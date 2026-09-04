(ns netz.content-provider
  "Bounded, expiring provider hints for portable content.

  Provider hints are routing aids, not authority claims.  This pure cache keeps
  the untrusted state bounded; the content hash remains the acceptance check."
  (:require [netz.content-wire :as content-wire]
            [netz.identity :as identity]
            [netz.wire :as wire]))

(def default-opts
  {:max-keys 4096
   :max-providers-per-key 20
   :max-keys-per-provider 256})

(defn make-cache
  ([] (make-cache {}))
  ([opts]
   (let [{:keys [max-keys max-providers-per-key max-keys-per-provider]
          :as opts} (merge default-opts opts)]
     (when-not (and (integer? max-keys) (<= 1 max-keys 65536)
                    (integer? max-providers-per-key)
                    (<= 1 max-providers-per-key content-wire/max-providers)
                    (integer? max-keys-per-provider)
                    (<= 1 max-keys-per-provider 65536))
       (throw (ex-info "invalid provider-cache bounds"
                       {:type :netz.content-provider/invalid-options})))
     {:opts opts
      :records {}
      :provider-counts {}
      :stats {:learned 0 :refused 0 :expired 0}})))

(defn- provider-counts [records]
  (frequencies (for [providers (vals records)
                     authority-key (keys providers)]
                 authority-key)))

(defn expire
  "Remove records whose exclusive expiry is at or before `now-ms`."
  [cache now-ms]
  (when-not (and (integer? now-ms) (<= 0 now-ms))
    (throw (ex-info "provider-cache time must be a non-negative integer"
                    {:type :netz.content-provider/invalid-time})))
  (let [before (reduce + 0 (map count (vals (:records cache))))
        records (reduce-kv
                 (fn [out cid-key providers]
                   (let [live (into (sorted-map)
                                    (filter (fn [[_ record]]
                                              (> (:expires-at record) now-ms)))
                                    providers)]
                     (if (seq live) (assoc out cid-key live) out)))
                 (sorted-map) (:records cache))
        after (reduce + 0 (map count (vals records)))]
    (-> cache
        (assoc :records records)
        (assoc :provider-counts (provider-counts records))
        (update-in [:stats :expired] + (- before after)))))

(defn- valid-record! [cid authority ttl-ms now-ms]
  (when-not (and (content-wire/valid-cid? cid)
                 (wire/byte-buffer? authority)
                 (= identity/peer-id-size (identity/buf-length authority))
                 (integer? ttl-ms)
                 (<= 0 ttl-ms content-wire/max-provider-ttl-ms)
                 (integer? now-ms) (<= 0 now-ms))
    (throw (ex-info "invalid provider record"
                    {:type :netz.content-provider/invalid-record}))))

(defn- remember-record [cache cid authority ttl-ms now-ms]
  (let [cid-key (identity/bytes->hex cid)
        authority-key (identity/bytes->hex authority)
        providers (get-in cache [:records cid-key] {})
        existing? (contains? providers authority-key)
        {:keys [max-keys max-providers-per-key max-keys-per-provider]}
        (:opts cache)
        refused? (or (and (not existing?)
                          (>= (count providers) max-providers-per-key))
                     (and (not existing?)
                          (not (contains? (:records cache) cid-key))
                          (>= (count (:records cache)) max-keys))
                     (and (not existing?)
                          (>= (get (:provider-counts cache) authority-key 0)
                              max-keys-per-provider)))]
    (cond
      (zero? ttl-ms) cache
      refused? (update-in cache [:stats :refused] inc)
      :else
      (cond-> (-> cache
                  (assoc-in [:records cid-key authority-key]
                            {:authority authority
                             :peer (identity/authority-routing-id authority)
                             :expires-at (+ now-ms ttl-ms)})
                  (update-in [:stats :learned] + (if existing? 0 1)))
        (not existing?) (update-in [:provider-counts authority-key]
                                   (fnil inc 0))))))

(defn remember
  "Remember one full Authority ID as a provider until `now-ms + ttl-ms`.

  Existing records may always refresh. New records that exceed a key, provider,
  or total-key bound are refused rather than evicting unrelated useful state."
  [cache cid authority ttl-ms now-ms]
  (valid-record! cid authority ttl-ms now-ms)
  (remember-record (expire cache now-ms) cid authority ttl-ms now-ms))

(defn remember-many
  "Remember canonical provider hints from one `content/found` response."
  [cache cid authorities ttl-ms now-ms]
  (let [cache (expire cache now-ms)]
    (reduce (fn [current authority]
              (valid-record! cid authority ttl-ms now-ms)
              (remember-record current cid authority ttl-ms now-ms))
            cache authorities)))

(defn remember-provider-cids
  "Remember that one authenticated provider announced several CIDs.

  Expiry is computed once and every record still passes the same per-CID,
  per-provider, and global limits as solicited provider hints."
  [cache cids authority ttl-ms now-ms]
  (let [cache (expire cache now-ms)]
    (reduce (fn [current cid]
              (valid-record! cid authority ttl-ms now-ms)
              (remember-record current cid authority ttl-ms now-ms))
            cache cids)))

(defn providers-for
  "Return live provider records in deterministic Authority-ID order."
  [cache cid now-ms]
  (when-not (content-wire/valid-cid? cid)
    (throw (ex-info "invalid provider lookup CID"
                    {:type :netz.content-provider/invalid-cid})))
  (let [cache (expire cache now-ms)]
    (vec (vals (get-in cache [:records (identity/bytes->hex cid)] {})))))
