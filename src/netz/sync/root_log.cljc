(ns netz.sync.root-log
  "Reference `netz/root-log/1` synchronization profile.

  Urd supplies the writer and fence. Netz checks monotonic `(term,index)` and
  transports the content root; immutable block closure remains in
  `netz.content`."
  (:require [netz.sync.profile :as profile]))

(def profile-name "netz/root-log/1")

(defn position [record]
  [(or (:term record) -1) (or (:index record) -1)])

(defn newer? [a b]
  (pos? (compare (position a) (position b))))

(defn- valid-record? [writer verify record]
  (and (map? record)
       (= writer (:writer record))
       (int? (:term record))
       (int? (:index record))
       (some? (:root record))
       (verify record)))

(defrecord RootLogProfile [writer verify]
  profile/SyncProfile
  (-profile-id [_] profile-name)
  (-describe [_ value _topic] value)
  (-respond [_ value _topic remote-summary]
    (when (and value (not (valid-record? writer verify value)))
      (throw (ex-info "Local fenced root record is invalid"
                      {:type :netz.sync.root-log/invalid-local-record})))
    (when (and value remote-summary
               (= (position value) (position remote-summary))
               (not= (:root value) (:root remote-summary)))
      (throw (ex-info "Conflicting roots at one fence/index"
                      {:type :netz.sync.root-log/conflict
                       :local value :remote remote-summary})))
    (if (and value (or (nil? remote-summary) (newer? value remote-summary)))
      {:chunks [value] :checkpoint value}
      {:chunks [] :checkpoint remote-summary}))
  (-begin [_ value _topic] value)
  (-apply-chunk [_ staging _topic chunk]
    (when-not (valid-record? writer verify chunk)
      (throw (ex-info "Invalid fenced root record"
                      {:type :netz.sync.root-log/invalid-record
                       :record chunk})))
    (cond
      (nil? staging) chunk
      (newer? chunk staging) chunk
      (= (position chunk) (position staging))
      (if (= (:root chunk) (:root staging))
        staging
        (throw (ex-info "Conflicting roots at one fence/index"
                        {:type :netz.sync.root-log/conflict
                         :current staging :incoming chunk})))
      :else staging))
  (-commit [_ value _topic staging checkpoint]
    (when (and checkpoint staging (not= checkpoint staging))
      (throw (ex-info "Root checkpoint does not match staged value"
                      {:type :netz.sync.root-log/checkpoint-mismatch})))
    (if (or (nil? value) (and staging (newer? staging value))) staging value)))

(defn profile
  ([writer] (profile writer (constantly true)))
  ([writer verify] (->RootLogProfile writer verify)))
