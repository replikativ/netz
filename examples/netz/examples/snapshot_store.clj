(ns netz.examples.snapshot-store
  "Experimental, bounded, read-only Konserve view of a signed content catalogue.
   The catalogue is demo application data, not a new standard Netz wire profile."
  (:require [clojure.core.async :as a]
            [konserve.core :as k]
            [konserve.protocols :as p]
            [netz.content-wire :as content]
            [netz.identity :as id]
            [netz.snapshot-ref :as ref]
            [netz.wire :as wire]))

(def max-objects 1024)
(def catalogue-profile "netz/demo/konserve-catalog-v1")

(defn await! [ch]
  (let [[v port] (a/alts!! [ch (a/timeout 30000)])]
    (when-not (= port ch) (throw (ex-info "Snapshot operation timed out" {})))
    (when (instance? Throwable v) (throw v))
    v))

(defn channel [v] (doto (a/promise-chan) (a/put! v)))

(defn- read-only! [] (throw (ex-info "Snapshot store is read-only" {})))

(defrecord ReadOnlyStore [metadata read-value]
  p/PLockFreeStore
  (-lock-free? [_] true)
  p/PEDNKeyValueStore
  (-exists? [_ key _] (channel (contains? metadata key)))
  (-get-meta [_ key _] (a/thread (get metadata key)))
  (-get-in [_ path missing _]
    (a/thread
      (try
        (if (contains? metadata (first path))
          (let [v (read-value (first path))]
            (if (next path) (get-in v (next path) missing) v))
          missing)
        (catch Throwable e e))))
  (-assoc-in [_ _ _ _ _] (read-only!))
  (-update-in [_ _ _ _ _] (read-only!))
  (-dissoc [_ _ _] (read-only!))
  p/PKeyIterable
  (-keys [_ _] (channel (vec (vals metadata)))))

(defn view
  "Construct a read-only EDN view; read-value is a blocking, throwing function."
  [keys read-value]
  (->ReadOnlyStore
   (into {} (map (fn [key]
                   [key {:key key :type :edn :immutable? true
                         :last-write (java.util.Date. 0)}]) keys))
   read-value))

(defn export!
  "Capture explicitly selected immutable EDN keys as independent bounded blocks.
   Caller owns reachability selection and must prevent GC during capture."
  [source keys root-key root-statement authority encode-value parent-catalogues]
  (when-not (and (<= 1 (count keys) max-objects)
                 (= (count keys) (count (distinct keys)))
                 (some #{root-key} keys))
    (throw (ex-info "Invalid snapshot key selection" {})))
  (let [blocks (atom {})
        entries
        (mapv (fn [key]
                (when-not (await! (k/exists? source key))
                  (throw (ex-info "Snapshot object disappeared" {:key key})))
                (let [bytes (encode-value (await! (k/get source key)))
                      cid (content/raw-cid bytes)]
                  (swap! blocks assoc (id/bytes->hex cid) bytes)
                  {"key" (encode-value key) "cid" cid})) keys)
        catalogue {"v" 1 "type" "netz/demo/konserve-catalog"
                   "root" (encode-value root-key)
                   "root_ref" (ref/encode root-statement)
                   "entries" entries}
        bytes (wire/encode-canonical catalogue)
        cid (content/raw-cid bytes)
        statement (await! (ref/sign authority
                                    {:profile catalogue-profile
                                     :system (:netz/system root-statement)
                                     :ref (:netz/ref root-statement)
                                     :snapshot cid :parents parent-catalogues}))]
    (swap! blocks assoc (id/bytes->hex cid) bytes)
    {:statement statement :root-statement root-statement :blocks @blocks}))

(defn fetch-verified!
  "Cache only bytes that match the requested CID. A restart can reuse this cache."
  [cache fetch! cid]
  (let [key [:netz.demo/block (id/bytes->hex cid)]
        held (await! (k/get cache key))]
    (if (and held (content/verify-block? cid held))
      held
      (let [bytes (fetch! cid)]
        (when-not (content/verify-block? cid bytes)
          (throw (ex-info "Snapshot block failed verification" {:cid (id/bytes->hex cid)})))
        (await! (k/assoc cache key bytes))
        bytes))))

(defn open!
  "Verify the pinned publisher and stream before fetching or decoding app values.
   Returns a read-only Konserve source for konserve-sync, plus the signed DB root."
  [statement {:keys [publisher system ref-name]} cache fetch! decode-value]
  (when-not (and (= catalogue-profile (:netz/profile statement))
                 (content/valid-cid? (:netz/snapshot statement))
                 (= [catalogue-profile (id/bytes->hex system) (id/bytes->hex ref-name)]
                    (ref/stream-key statement))
                 (= publisher (:netz/publisher statement))
                 (await! (ref/verify statement)))
    (throw (ex-info "Unauthorized snapshot catalogue" {})))
  (let [catalogue (wire/decode
                   (fetch-verified! cache fetch! (:netz/snapshot statement)))
        entries (get catalogue "entries")]
    (when-not (and (= #{"v" "type" "root" "root_ref" "entries"} (set (keys catalogue)))
                   (= 1 (get catalogue "v"))
                   (= "netz/demo/konserve-catalog" (get catalogue "type"))
                   (vector? entries) (<= 1 (count entries) max-objects)
                   (every? #(and (= #{"key" "cid"} (set (keys %)))
                                 (wire/byte-buffer? (get % "key"))
                                 (content/valid-cid? (get % "cid"))) entries))
      (throw (ex-info "Invalid snapshot catalogue" {})))
    (let [root-ref (ref/decode (get catalogue "root_ref"))
          root-key (decode-value (get catalogue "root"))
          by-key (into {} (map (fn [entry]
                                 [(decode-value (get entry "key")) (get entry "cid")]) entries))]
      (when-not (and (= (count by-key) (count entries))
                     (contains? by-key root-key)
                     (= publisher (:netz/publisher root-ref))
                     (= (id/bytes->hex system) (id/bytes->hex (:netz/system root-ref)))
                     (= (id/bytes->hex ref-name) (id/bytes->hex (:netz/ref root-ref)))
                     (await! (ref/verify root-ref)))
        (throw (ex-info "Catalogue root binding is invalid" {})))
      {:root-key root-key :root-statement root-ref
       :source (view (keys by-key)
                     #(decode-value (fetch-verified! cache fetch! (get by-key %))))})))
