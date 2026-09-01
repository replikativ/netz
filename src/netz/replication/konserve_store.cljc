(ns netz.replication.konserve-store
  "Portable Konserve implementation of the durable-blob transfer store.

  Pieces, installation markers and receipt records are immutable values in a
  caller-supplied Konserve store. The same namespace also holds the bounded,
  monotone lifecycle journal used by `netz.replication.lifecycle-runtime`.
  This is the Netz transfer repository, not a transparent wrapper over
  application keys. Applications publish their own key-to-object mapping only
  after the required receipts exist.

  A generic Konserve write completion cannot prove a POSIX fsync boundary, so
  this adapter supports only `netz/store/volatile/1` and
  `netz/store/best-effort/1`. Use `netz.replication.file-store` when an fsync
  receipt is required on the JVM."
  (:require [konserve.core :as k]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.inventory :as inventory]
            [netz.replication.lifecycle-storage :as lifecycle-storage]
            [netz.replication.storage :as storage]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]
            [clojure.core.async :as async]
            [superv.async :refer [<?- go-try-] :include-macros true]
            #?(:cljs [goog.crypt.Sha256]))
  #?(:clj (:import [java.security MessageDigest])))

(def ^:private marker-fields
  #{"v" "type" "manifest" "object" "descriptor" "persistence" "stored_at"})

(def ^:private receipt-record-fields
  #{"v" "type" "session" "object" "manifest" "replica" "layout_epoch"
    "attempt" "persistence" "receipt"})

(def ^:private event-fields
  #{:type :object-id :replica-id :layout-epoch :attempt
    :persistence :receipt :manifest-id})

(def ^:private persistence-profiles
  #{"netz/store/volatile/1" "netz/store/best-effort/1"})

(defn- bytes-n? [n value]
  (and (wire/byte-buffer? value) (= n (identity/buf-length value))))

(defn- same-bytes? [a b]
  (and (wire/byte-buffer? a)
       (wire/byte-buffer? b)
       (identity/bufs= a b)))

(defn- same-value? [a b]
  (same-bytes? (wire/encode-canonical a) (wire/encode-canonical b)))

(defn- safe-uint? [value]
  (and (integer? value) (<= 0 value wire/max-safe-integer)))

(defn- failure? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn- address-hex [value]
  (cond
    (bytes-n? 32 value) (identity/bytes->hex value)
    (and (string? value) (re-matches #"[0-9a-f]{64}" value)) value
    :else (throw (ex-info "Invalid Konserve blob-store address"
                          {:type :netz.replication/invalid-konserve-address}))))

(defn- entry-key [store kind address]
  [(:prefix store) kind (address-hex address)])

(defn- piece-key [store address]
  (entry-key store ::piece address))

(defn- marker-key [store manifest-id]
  (entry-key store ::manifest manifest-id))

(defn- receipt-key [store record]
  (entry-key store ::receipt
             (identity/sha256 (wire/encode-canonical record))))

(defn- lifecycle-key [store key]
  [(:prefix store) ::lifecycle key])

(defn- verify-context! [manifest-id manifest]
  (blob/validate-manifest manifest)
  (when-not (and (bytes-n? 32 manifest-id)
                 (same-bytes? manifest-id (blob/manifest-address manifest)))
    (throw (ex-info "Manifest does not match its Konserve address"
                    {:type :netz.replication/manifest-address-mismatch})))
  manifest)

(defn- expected-piece! [manifest index piece-id]
  (when-not (and (int? index)
                 (<= 0 index)
                 (< index (count (get manifest "pieces")))
                 (same-bytes? piece-id (nth (get manifest "pieces") index)))
    (throw (ex-info "Piece does not match its manifest position"
                    {:type :netz.replication/piece-address-mismatch
                     :index index})))
  piece-id)

(defn- immutable-assoc! [store key value]
  (k/assoc (:store store) key value {:immutable? true} {:sync? false}))

(defn- read-piece [store manifest index]
  (async/go
    (let [piece-id (nth (get manifest "pieces") index)
          data (async/<! (k/get (:store store) (piece-key store piece-id)
                                nil {:sync? false}))]
      (if (failure? data)
        data
        (when (and (wire/byte-buffer? data)
                   (= (blob/expected-piece-length manifest index)
                      (identity/buf-length data))
                   (blob/verify-piece? manifest index data))
          data)))))

(defn- held-pieces [store manifest]
  (go-try-
   (loop [index 0 held [] seen #{}]
     (if (= index (count (get manifest "pieces")))
       held
       (let [piece-id (nth (get manifest "pieces") index)
             hex (identity/bytes->hex piece-id)
             data (<?- (read-piece store manifest index))]
         (recur (inc index)
                (if (and data (not (contains? seen hex)))
                  (conj held piece-id)
                  held)
                (cond-> seen data (conj hex))))))))

(defn- new-object-digest []
  #?(:clj (MessageDigest/getInstance "SHA-256")
     :cljs (goog.crypt.Sha256.)))

(defn- update-digest! [digest bytes]
  #?(:clj (.update ^MessageDigest digest ^bytes bytes)
     :cljs (.update digest bytes))
  digest)

(defn- finish-digest [digest]
  #?(:clj (.digest ^MessageDigest digest)
     :cljs (js/Uint8Array. (.digest digest))))

(defn- verify-object! [store manifest]
  (go-try-
   (let [digest (new-object-digest)]
     (update-digest! digest (identity/utf8-bytes blob/object-address-tag))
     (loop [index 0 length 0]
       (if (= index (count (get manifest "pieces")))
         (do
           (when-not (and (= length (get manifest "length"))
                          (same-bytes? (finish-digest digest)
                                       (get manifest "object")))
             (throw (ex-info "Installed pieces do not match the object address"
                             {:type :netz.replication/object-mismatch})))
           true)
         (let [data (<?- (read-piece store manifest index))]
           (when-not data
             (throw (ex-info "Installed object has a missing or corrupt piece"
                             {:type :netz.replication/missing-piece
                              :index index})))
           (update-digest! digest data)
           (recur (inc index) (+ length (identity/buf-length data)))))))))

(defn- valid-marker? [marker manifest-id manifest]
  (try
    (verify-context! manifest-id manifest)
    (and (map? marker)
         (= marker-fields (set (keys marker)))
         (= 1 (get marker "v"))
         (= "blob/install" (get marker "type"))
         (same-bytes? manifest-id (get marker "manifest"))
         (same-bytes? (get manifest "object") (get marker "object"))
         (same-value? manifest (get marker "descriptor"))
         (contains? persistence-profiles (get marker "persistence"))
         (safe-uint? (get marker "stored_at")))
    (catch #?(:clj Throwable :cljs js/Error) _ false)))

(defn- load-marker [store manifest-id manifest]
  (go-try-
   (let [marker (<?- (k/get (:store store) (marker-key store manifest-id)
                            nil {:sync? false}))]
     (when marker
       (when-not (valid-marker? marker manifest-id manifest)
         (throw (ex-info "Invalid installed-object marker"
                         {:type :netz.replication/invalid-install-marker})))
       marker))))

(defn- marker->installed [marker]
  {:manifest (get marker "manifest")
   :object (get marker "object")
   :persistence (get marker "persistence")
   :stored-at (get marker "stored_at")})

(defn- load-staging! [store manifest-id manifest]
  (go-try-
   (verify-context! manifest-id manifest)
   (let [held (<?- (held-pieces store manifest))
         marker (<?- (load-marker store manifest-id manifest))
         installed (when (and marker
                              (= (count held)
                                 (count (distinct
                                         (map identity/bytes->hex
                                              (get manifest "pieces"))))))
                     (<?- (verify-object! store manifest))
                     (marker->installed marker))]
     {:held held :installed installed})))

(defn- stage-piece! [store manifest-id manifest index piece-id data]
  (go-try-
   (verify-context! manifest-id manifest)
   (expected-piece! manifest index piece-id)
   (when-not (blob/verify-piece? manifest index data)
     (throw (ex-info "Refusing to stage an invalid piece"
                     {:type :netz.replication/invalid-piece :index index})))
   (let [existing (<?- (read-piece store manifest index))]
     (when-not existing
       (<?- (immutable-assoc! store (piece-key store piece-id) data))))
   (when-not (<?- (read-piece store manifest index))
     (throw (ex-info "Piece failed verification after staging"
                     {:type :netz.replication/stage-verification-failed
                      :index index})))
   :ok))

(defn- install-object! [store manifest-id manifest persistence]
  (go-try-
   (verify-context! manifest-id manifest)
   (when-not (= persistence (:persistence store))
     (throw (ex-info "Requested persistence does not match the Konserve store"
                     {:type :netz.replication/persistence-mismatch
                      :requested persistence
                      :configured (:persistence store)})))
   (<?- (verify-object! store manifest))
   (if-let [existing (<?- (load-marker store manifest-id manifest))]
     (assoc (marker->installed existing) :result :ok)
     (let [stored-at ((:clock store))
           marker {"v" 1
                   "type" "blob/install"
                   "manifest" manifest-id
                   "object" (get manifest "object")
                   "descriptor" manifest
                   "persistence" persistence
                   "stored_at" stored-at}]
       (when-not (safe-uint? stored-at)
         (throw (ex-info "Konserve-store clock returned an invalid timestamp"
                         {:type :netz.replication/invalid-stored-at
                          :stored-at stored-at})))
       (<?- (immutable-assoc! store (marker-key store manifest-id) marker))
       (assoc (marker->installed marker) :result :ok)))))

(defn- load-installed-piece! [store manifest-id index piece-id]
  (go-try-
   (when-not (bytes-n? 32 manifest-id)
     (throw (ex-info "Invalid manifest address" {})))
   (let [marker (<?- (k/get (:store store) (marker-key store manifest-id)
                            nil {:sync? false}))]
     (when-not marker
       (throw (ex-info "Cannot send a piece from an uninstalled manifest"
                       {:type :netz.replication/manifest-not-installed})))
     (let [manifest (get marker "descriptor")]
       (when-not (valid-marker? marker manifest-id manifest)
         (throw (ex-info "Invalid installed-object marker"
                         {:type :netz.replication/invalid-install-marker})))
       (expected-piece! manifest index piece-id)
       (or (<?- (read-piece store manifest index))
           (throw (ex-info "Installed piece is missing or corrupt"
                           {:type :netz.replication/missing-piece
                            :index index})))))))

(defn- controller-event->record [session event]
  (let [envelope (:receipt event)
        statement (:statement (replication-wire/parse-envelope envelope))]
    (when-not (and (= event-fields (set (keys event)))
                   (= :replication/stored (:type event))
                   (bytes-n? 16 session)
                   (= (:object-id event)
                      (identity/bytes->hex (get statement "object")))
                   (= (:manifest-id event)
                      (identity/bytes->hex (get statement "manifest")))
                   (= (:replica-id event)
                      (identity/bytes->hex (get statement "replica")))
                   (= (:layout-epoch event) (get statement "layout_epoch"))
                   (= (:attempt event) (get statement "attempt"))
                   (= (:persistence event) (get statement "persistence")))
      (throw (ex-info "Controller event does not match its signed receipt"
                      {:type :netz.replication/receipt-record-mismatch})))
    {"v" 1
     "type" "blob/receipt-record"
     "session" session
     "object" (get statement "object")
     "manifest" (get statement "manifest")
     "replica" (get statement "replica")
     "layout_epoch" (get statement "layout_epoch")
     "attempt" (get statement "attempt")
     "persistence" (get statement "persistence")
     "receipt" envelope}))

(defn- valid-receipt-record? [record]
  (try
    (let [statement (:statement
                     (replication-wire/parse-envelope (get record "receipt")))]
      (and (map? record)
           (= receipt-record-fields (set (keys record)))
           (= 1 (get record "v"))
           (= "blob/receipt-record" (get record "type"))
           (= "blob/stored" (get statement "type"))
           (bytes-n? 16 (get record "session"))
           (same-bytes? (get record "object") (get statement "object"))
           (same-bytes? (get record "manifest") (get statement "manifest"))
           (same-bytes? (get record "replica") (get statement "replica"))
           (= (get record "layout_epoch") (get statement "layout_epoch"))
           (= (get record "attempt") (get statement "attempt"))
           (= (get record "persistence") (get statement "persistence"))))
    (catch #?(:clj Throwable :cljs js/Error) _ false)))

(defn- record->controller-event [record]
  {:type :replication/stored
   :object-id (identity/bytes->hex (get record "object"))
   :replica-id (identity/bytes->hex (get record "replica"))
   :layout-epoch (get record "layout_epoch")
   :attempt (get record "attempt")
   :persistence (get record "persistence")
   :receipt (get record "receipt")
   :manifest-id (identity/bytes->hex (get record "manifest"))})

(defn- receipt-entry? [store entry]
  (let [key (:key entry)]
    (and (vector? key)
         (= 3 (count key))
         (= (:prefix store) (nth key 0))
         (= ::receipt (nth key 1))
         (string? (nth key 2))
         (re-matches #"[0-9a-f]{64}" (nth key 2)))))

(defn- receipt-keys [store]
  (go-try-
   (let [entries (<?- (k/keys (:store store) {:sync? false}))
         result (into [] (comp (filter #(receipt-entry? store %))
                               (map :key))
                      entries)]
     (when (> (count result) (:max-receipts store))
       (throw (ex-info "Receipt journal exceeds its configured bound"
                       {:type :netz.replication/too-many-receipts
                        :limit (:max-receipts store)})))
     result)))

(defn- load-receipts! [store]
  (go-try-
   (let [keys (<?- (receipt-keys store))]
     (loop [remaining (seq keys) result []]
       (if-let [key (first remaining)]
         (let [record (<?- (k/get (:store store) key nil {:sync? false}))
               encoded (when record (wire/encode-canonical record))
               address (when encoded
                         (identity/bytes->hex (identity/sha256 encoded)))
               signature-ok? (when (valid-receipt-record? record)
                               (<?- (replication-wire/verify-envelope
                                     (get record "receipt"))))]
           (when-not (and (= address (nth key 2)) (true? signature-ok?))
             (throw (ex-info "Invalid durable receipt record"
                             {:type :netz.replication/invalid-receipt-record
                              :key key})))
           (recur (next remaining)
                  (conj result (record->controller-event record))))
         result)))))

(defn- lifecycle-entry? [store entry]
  (let [key (:key entry)]
    (and (vector? key)
         (= 3 (count key))
         (= (:prefix store) (nth key 0))
         (= ::lifecycle (nth key 1))
         (vector? (nth key 2))
         (= :blob/lifecycle (first (nth key 2)))
         (string? (second (nth key 2))))))

(defn- marker-entry? [store entry]
  (let [key (:key entry)]
    (and (vector? key)
         (= 3 (count key))
         (= (:prefix store) (nth key 0))
         (= ::manifest (nth key 1))
         (string? (nth key 2))
         (re-matches #"[0-9a-f]{64}" (nth key 2)))))

(defn- installed-addresses! [store]
  (go-try-
   (let [entries (<?- (k/keys (:store store) {:sync? false}))
         addresses (into [] (comp (filter #(marker-entry? store %))
                                  (map #(nth (:key %) 2)))
                         entries)]
     (when (> (count addresses) (:max-inventory-records store))
       (throw (ex-info "Installed-object inventory exceeds its configured bound"
                       {:type :netz.replication/too-many-installed-objects
                        :limit (:max-inventory-records store)})))
     addresses)))

(defn- scrub-object! [store manifest-id]
  (go-try-
   (let [marker (<?- (k/get (:store store) (marker-key store manifest-id)
                            nil {:sync? false}))]
     (if-not marker
       {:status :missing :manifest manifest-id}
       (let [manifest (get marker "descriptor")]
         (when-not (valid-marker? marker manifest-id manifest)
           (throw (ex-info "Invalid installed-object marker"
                           {:type :netz.replication/invalid-install-marker})))
         (<?- (verify-object! store manifest))
         {:status :healthy
          :manifest manifest-id
          :object (get marker "object")
          :persistence (get marker "persistence")
          :stored-at (get marker "stored_at")
          :length (get manifest "length")
          :pieces (count (get manifest "pieces"))})))))

(defn- load-installed-manifest! [store manifest-id]
  (go-try-
   (let [marker (<?- (k/get (:store store) (marker-key store manifest-id)
                            nil {:sync? false}))]
     (when marker
       (let [manifest (get marker "descriptor")]
         (when-not (valid-marker? marker manifest-id manifest)
           (throw (ex-info "Invalid installed-object marker"
                           {:type :netz.replication/invalid-install-marker})))
         manifest)))))

(defn- load-lifecycle-records! [store]
  (go-try-
   (let [entries (<?- (k/keys (:store store) {:sync? false}))
         keys (into [] (comp (filter #(lifecycle-entry? store %))
                             (map :key))
                    entries)]
     (when (> (count keys) (:max-lifecycle-records store))
       (throw (ex-info "Lifecycle journal exceeds its configured bound"
                       {:type :netz.replication/too-many-lifecycle-records
                        :limit (:max-lifecycle-records store)})))
     (loop [remaining (seq keys) result {}]
       (if-let [storage-key (first remaining)]
         (let [machine-key (nth storage-key 2)
               object-key (second machine-key)
               record (<?- (k/get (:store store) storage-key
                                  nil {:sync? false}))]
           (when-not (map? record)
             (throw (ex-info "Invalid durable lifecycle record"
                             {:type :netz.replication/invalid-lifecycle-record
                              :key machine-key})))
           (recur (next remaining) (assoc result object-key record)))
         result)))))

(defrecord KonserveBlobStore [store prefix persistence clock max-receipts
                              max-lifecycle-records max-inventory-records
                              delete-object!]
  storage/PBlobTransferStore

  (-load-staging [this _session manifest-id manifest]
    (go-try-
     (try
       (<?- (load-staging! this manifest-id manifest))
       (catch #?(:clj Throwable :cljs js/Error) cause cause))))

  (-stage-piece! [this _session manifest-id manifest index piece-id data]
    (go-try-
     (try
       (<?- (stage-piece! this manifest-id manifest index piece-id data))
       (catch #?(:clj Throwable :cljs js/Error) cause cause))))

  (-install-object! [this _session manifest-id manifest _retention requested]
    (go-try-
     (try
       (<?- (install-object! this manifest-id manifest requested))
       (catch #?(:clj Throwable :cljs js/Error) cause cause))))

  (-load-piece [this _session manifest-id index piece-id]
    (go-try-
     (try
       (<?- (load-installed-piece! this manifest-id index piece-id))
       (catch #?(:clj Throwable :cljs js/Error) cause cause))))

  (-persist-receipt! [this session event]
    (go-try-
     (try
       (let [record (controller-event->record session event)]
         (<?- (immutable-assoc! this (receipt-key this record) record))
         :ok)
       (catch #?(:clj Throwable :cljs js/Error) cause cause))))

  (-recorded-receipts [this]
    (go-try-
     (try
       (<?- (load-receipts! this))
       (catch #?(:clj Throwable :cljs js/Error) cause cause))))

  (-cleanup-session! [_this _session _manifest-id]
    (storage/chan-of true))

  lifecycle-storage/PBlobLifecycleStore

  (-load-lifecycle-records [this]
    (go-try-
     (try
       (<?- (load-lifecycle-records! this))
       (catch #?(:clj Throwable :cljs js/Error) cause cause))))

  (-persist-lifecycle! [this key value]
    (go-try-
     (try
       (<?- (k/assoc store (lifecycle-key this key) value
                     nil {:sync? false}))
       :ok
       (catch #?(:clj Throwable :cljs js/Error) cause cause))))

  (-delete-object! [_this key statement requested]
    (if delete-object!
      (try
        (delete-object! {:key key :statement statement
                         :persistence requested})
        (catch #?(:clj Throwable :cljs js/Error) cause
          (lifecycle-storage/chan-of cause)))
      (lifecycle-storage/chan-of
       (ex-info
        "Konserve object deletion requires an explicit erasure implementation"
        {:type :netz.replication/deletion-not-configured
         :key key}))))

  inventory/PBlobInventoryStore

  (-inventory-page [this cursor limit]
    (go-try-
     (try
       (inventory/page-addresses (<?- (installed-addresses! this))
                                 cursor limit)
       (catch #?(:clj Throwable :cljs js/Error) cause cause))))

  (-scrub-object! [this manifest-id]
    (go-try-
     (try
       (<?- (scrub-object! this manifest-id))
       (catch #?(:clj Throwable :cljs js/Error) cause cause))))

  inventory/PBlobProviderStore

  (-load-installed-manifest [this manifest-id]
    (go-try-
     (try
       (<?- (load-installed-manifest! this manifest-id))
       (catch #?(:clj Throwable :cljs js/Error) cause cause)))))

(defn new-konserve-blob-store
  "Use a connected Konserve store as a portable Netz transfer repository.

  `:persistence` is required and must be `netz/store/volatile/1` or
  `netz/store/best-effort/1`; a generic Konserve completion cannot justify an
  fsync receipt. `:prefix` isolates the repository in a shared store and
  defaults to `:netz.blob`. `:clock` returns Unix milliseconds.

  Staged pieces intentionally survive session cleanup. `:max-receipts` and
  `:max-lifecycle-records` independently bound restart scans (both default to
  100,000). `:max-inventory-records` bounds the first reference
  implementation's local marker scan (default 1,000,000); returned pages stay
  bounded to 1,024 addresses.

  Privacy deletion is intentionally fail-closed. Supply `:delete-object!` as a
  function from `{key statement persistence}` to a result channel only when it
  can erase every retained representation and return an honest
  `{result :ok, persistence ..., deleted-at ...}` proof. Without it, expiry is
  retained as a durable tombstone and retried, but no deletion receipt is
  issued."
  [store {:keys [prefix persistence clock max-receipts
                 max-lifecycle-records max-inventory-records delete-object!]
          :or {prefix :netz.blob
               clock #?(:clj #(System/currentTimeMillis)
                        :cljs #(.now js/Date))
               max-receipts 100000
               max-lifecycle-records 100000
               max-inventory-records 1000000}}]
  (when-not (and store
                 (some? prefix)
                 (contains? persistence-profiles persistence)
                 (ifn? clock)
                 (int? max-receipts)
                 (pos? max-receipts)
                 (int? max-lifecycle-records)
                 (pos? max-lifecycle-records)
                 (int? max-inventory-records)
                 (pos? max-inventory-records)
                 (or (nil? delete-object!) (ifn? delete-object!)))
    (throw (ex-info "Invalid Konserve blob-store options"
                    {:type :netz.replication/invalid-konserve-store-options
                     :persistence persistence
                     :max-receipts max-receipts
                     :max-lifecycle-records max-lifecycle-records
                     :max-inventory-records max-inventory-records})))
  (->KonserveBlobStore store prefix persistence clock max-receipts
                       max-lifecycle-records max-inventory-records
                       delete-object!))
