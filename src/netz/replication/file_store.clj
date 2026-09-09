(ns netz.replication.file-store
  "Crash-safe JVM filesystem adapter for durable blob transfers.

  Pieces are immutable files addressed by their profile-1 hash. Installation is
  an atomic, fsynced manifest marker written only after every piece and the full
  object have been verified. Staged pieces intentionally survive session
  cleanup so a fresh transfer can resume after disconnect or process loss.

  One process must own a store root. Calls are serialized within an adapter;
  sharing the same root between independent processes is unsupported. Blocking
  filesystem work runs on core.async's thread pool and exceptions are delivered
  as channel values."
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.controller-storage :as controller-storage]
            [netz.replication.directory-barrier :as directory-barrier]
            [netz.replication.inventory :as inventory]
            [netz.replication.intent :as intent]
            [netz.replication.storage :as storage]
            [netz.replication.store-validation :refer [verify-context! expected-piece!]]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire])
  (:import (java.nio ByteBuffer)
           (java.nio.channels FileChannel)
           (java.nio.file AtomicMoveNotSupportedException Files LinkOption
                          OpenOption Path Paths StandardCopyOption
                          StandardOpenOption)
           (java.security MessageDigest)))

(def ^:private marker-fields
  #{"v" "type" "manifest" "object" "descriptor" "persistence" "stored_at"})

(def ^:private receipt-record-fields
  #{"v" "type" "session" "object" "manifest" "replica" "layout_epoch"
    "attempt" "persistence" "receipt"})

(def ^:private file-persistence-profiles
  #{"netz/store/best-effort/1" "netz/store/fsync/1"})

(def ^:private no-links (make-array LinkOption 0))
(def ^:private max-marker-bytes (* 2 1024 1024))
(def ^:private max-receipt-bytes (* 64 1024))
(def ^:private max-controller-intent-bytes (* 5 1024 1024))

(defn- byte-array? [value]
  (wire/byte-buffer? value))

(defn- bytes-n? [n value]
  (and (byte-array? value) (= n (identity/buf-length value))))

(defn- same-bytes? [a b]
  (and (byte-array? a) (byte-array? b) (identity/bufs= a b)))

(defn- same-value? [a b]
  (same-bytes? (wire/encode-canonical a) (wire/encode-canonical b)))

(defn- safe-uint? [value]
  (and (integer? value) (<= 0 value wire/max-safe-integer)))

(defn- path
  ^Path [^Path root & segments]
  (reduce (fn [^Path current segment]
            (.resolve current ^String segment))
          root segments))

(defn- hex-address [value]
  (cond
    (bytes-n? 32 value) (identity/bytes->hex value)
    (and (string? value) (re-matches #"[0-9a-f]{64}" value)) value
    :else (throw (ex-info "Invalid file-store address"
                          {:type :netz.replication/invalid-file-address}))))

(defn- cluster-hex [value]
  (if (and (byte-array? value)
           (<= 1 (identity/buf-length value) 64))
    (identity/bytes->hex value)
    (throw (ex-info "Invalid file-store cluster"
                    {:type :netz.replication/invalid-file-cluster}))))

(defn- addressed-path
  ^Path [^Path root directory address suffix]
  (let [hex (hex-address address)]
    (path root directory (subs hex 0 2) (str (subs hex 2) suffix))))

(defn- piece-path [store address]
  (addressed-path (:root store) "pieces" address ".piece"))

(defn- marker-path [store manifest-id]
  (addressed-path (:root store) "manifests" manifest-id ".cbor"))

(defn- receipt-path [store record]
  (addressed-path (:root store) "receipts"
                  (identity/sha256 (wire/encode-canonical record)) ".cbor"))

(defn- controller-intent-root [store cluster]
  (path (:root store) "controller-intents" (cluster-hex cluster)))

(defn- controller-intent-path [store cluster object-id]
  (let [hex (hex-address object-id)]
    (path (controller-intent-root store cluster)
          (subs hex 0 2) (str (subs hex 2) ".cbor"))))

(defn- exists? [^Path value]
  (Files/exists value no-links))

(defn- create-parent! [store ^Path target]
  (if (:fsync? store)
    (directory-barrier/provision! (:directory-barrier store) (.getParent target))
    (Files/createDirectories (.getParent target)
                             (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- force-file! [^Path target]
  (with-open [channel (FileChannel/open
                       target
                       (into-array OpenOption [StandardOpenOption/WRITE]))]
    (.force channel true)))

(defn- force-directory! [store ^Path directory]
  (directory-barrier/sync! (:directory-barrier store) directory))

(defn- atomic-write! [store ^Path target bytes]
  (create-parent! store target)
  (let [directory (.getParent target)
        temporary (Files/createTempFile
                   directory ".netz-" ".tmp"
                   (make-array java.nio.file.attribute.FileAttribute 0))]
    (try
      (with-open [channel (FileChannel/open
                           temporary
                           (into-array OpenOption
                                       [StandardOpenOption/WRITE
                                        StandardOpenOption/TRUNCATE_EXISTING]))]
        (let [buffer (ByteBuffer/wrap bytes)]
          (while (.hasRemaining buffer)
            (.write channel buffer)))
        (when (:fsync? store) (.force channel true)))
      (try
        (Files/move temporary target
                    (into-array StandardCopyOption
                                [StandardCopyOption/ATOMIC_MOVE
                                 StandardCopyOption/REPLACE_EXISTING]))
        (catch AtomicMoveNotSupportedException cause
          (throw (ex-info "Filesystem does not support atomic blob install"
                          {:type :netz.replication/atomic-move-unsupported
                           :target (str target)}
                          cause))))
      (when (:fsync? store) (force-directory! store directory))
      target
      (finally
        (Files/deleteIfExists temporary)))))

(defn- read-bounded [^Path target limit]
  (when-not (Files/isRegularFile target no-links)
    (throw (ex-info "Blob-store entry is not a regular file"
                    {:type :netz.replication/invalid-file-entry
                     :path (str target)})))
  (let [size (Files/size target)]
    (when (> size limit)
      (throw (ex-info "Blob-store metadata exceeds its bound"
                      {:type :netz.replication/file-entry-too-large
                       :path (str target) :size size :limit limit})))
    (Files/readAllBytes target)))

(defn- read-piece! [store manifest index]
  (let [piece-id (nth (get manifest "pieces") index)
        target (piece-path store piece-id)
        expected-length (blob/expected-piece-length manifest index)]
    (when (exists? target)
      (let [data (read-bounded target blob/max-piece-size)]
        (when (and (= expected-length (identity/buf-length data))
                   (blob/verify-piece? manifest index data))
          data)))))

(defn- held-pieces [store manifest]
  (->> (get manifest "pieces")
       (keep-indexed (fn [index piece-id]
                       (when (read-piece! store manifest index) piece-id)))
       distinct
       vec))

(defn- verify-object! [store manifest]
  (let [digest (MessageDigest/getInstance "SHA-256")
        _ (.update digest (identity/utf8-bytes blob/object-address-tag))
        length (reduce (fn [total index]
                         (let [data (read-piece! store manifest index)]
                           (when-not data
                             (throw (ex-info "Installed object has a missing or corrupt piece"
                                             {:type :netz.replication/missing-piece
                                              :index index})))
                           (.update digest data)
                           (+ total (identity/buf-length data))))
                       0 (range (count (get manifest "pieces"))))]
    (when-not (and (= length (get manifest "length"))
                   (same-bytes? (.digest digest) (get manifest "object")))
      (throw (ex-info "Installed pieces do not match the object address"
                      {:type :netz.replication/object-mismatch})))
    true))

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
         (contains? file-persistence-profiles (get marker "persistence"))
         (safe-uint? (get marker "stored_at")))
    (catch Throwable _ false)))

(defn- load-marker! [store manifest-id manifest]
  (let [target (marker-path store manifest-id)]
    (when (exists? target)
      (let [marker (wire/decode (read-bounded target max-marker-bytes))]
        (when-not (valid-marker? marker manifest-id manifest)
          (throw (ex-info "Invalid installed-object marker"
                          {:type :netz.replication/invalid-install-marker
                           :path (str target)})))
        marker))))

(defn- marker->installed [marker]
  {:manifest (get marker "manifest")
   :object (get marker "object")
   :persistence (get marker "persistence")
   :stored-at (get marker "stored_at")})

(defn- load-staging! [store manifest-id manifest]
  (verify-context! manifest-id manifest)
  (let [held (held-pieces store manifest)
        marker (load-marker! store manifest-id manifest)
        installed (when (and marker
                             (= (count held)
                                (count (distinct (get manifest "pieces")))))
                    (verify-object! store manifest)
                    (marker->installed marker))]
    {:held held :installed installed}))

(defn- stage-piece!
  [store manifest-id manifest index piece-id data]
  (verify-context! manifest-id manifest)
  (expected-piece! manifest index piece-id)
  (when-not (blob/verify-piece? manifest index data)
    (throw (ex-info "Refusing to stage an invalid piece"
                    {:type :netz.replication/invalid-piece
                     :index index})))
  (let [target (piece-path store piece-id)]
    (if (and (exists? target) (read-piece! store manifest index))
      (when (:fsync? store)
        (create-parent! store target)
        (force-file! target)
        (force-directory! store (.getParent target)))
      (atomic-write! store target data))
    (when-not (read-piece! store manifest index)
      (throw (ex-info "Piece failed verification after staging"
                      {:type :netz.replication/stage-verification-failed
                       :index index}))))
  :ok)

(defn- force-pieces! [store manifest]
  (doseq [piece-id (distinct (get manifest "pieces"))]
    (let [target (piece-path store piece-id)]
      (create-parent! store target)
      (force-file! target)
      (force-directory! store (.getParent target)))))

(defn- install-object!
  [store manifest-id manifest persistence]
  (verify-context! manifest-id manifest)
  (when-not (= persistence (:persistence store))
    (throw (ex-info "Requested persistence does not match the file store"
                    {:type :netz.replication/persistence-mismatch
                     :requested persistence
                     :configured (:persistence store)})))
  (verify-object! store manifest)
  (let [existing (load-marker! store manifest-id manifest)]
    (if (and existing (= persistence (get existing "persistence")))
      (do
        (when (:fsync? store)
          (force-pieces! store manifest)
          (let [target (marker-path store manifest-id)]
            (create-parent! store target)
            (force-file! target)
            (force-directory! store (.getParent target))))
        (assoc (marker->installed existing) :result :ok))
      (do
        (when (:fsync? store) (force-pieces! store manifest))
        (let [stored-at ((:clock store))
              marker {"v" 1
                      "type" "blob/install"
                      "manifest" manifest-id
                      "object" (get manifest "object")
                      "descriptor" manifest
                      "persistence" persistence
                      "stored_at" stored-at}]
          (when-not (safe-uint? stored-at)
            (throw (ex-info "File-store clock returned an invalid timestamp"
                            {:type :netz.replication/invalid-stored-at
                             :stored-at stored-at})))
          (atomic-write! store (marker-path store manifest-id)
                         (wire/encode-canonical marker))
          (assoc (marker->installed marker) :result :ok))))))

(defn- load-installed-piece! [store manifest-id index piece-id]
  (when-not (bytes-n? 32 manifest-id)
    (throw (ex-info "Invalid manifest address" {})))
  (let [target (marker-path store manifest-id)]
    (when-not (exists? target)
      (throw (ex-info "Cannot send a piece from an uninstalled manifest"
                      {:type :netz.replication/manifest-not-installed})))
    (let [marker (wire/decode (read-bounded target max-marker-bytes))
          manifest (get marker "descriptor")]
      (when-not (valid-marker? marker manifest-id manifest)
        (throw (ex-info "Invalid installed-object marker"
                        {:type :netz.replication/invalid-install-marker})))
      (expected-piece! manifest index piece-id)
      (or (read-piece! store manifest index)
          (throw (ex-info "Installed piece is missing or corrupt"
                          {:type :netz.replication/missing-piece
                           :index index}))))))

(defn- controller-event->record [session event]
  (let [required #{:type :object-id :replica-id :layout-epoch :attempt
                   :persistence :receipt :manifest-id}
        envelope (:receipt event)
        statement (:statement (replication-wire/parse-envelope envelope))]
    (when-not (and (= required (set (keys event)))
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
    (catch Throwable _ false)))

(defn- record->controller-event [record]
  {:type :replication/stored
   :object-id (identity/bytes->hex (get record "object"))
   :replica-id (identity/bytes->hex (get record "replica"))
   :layout-epoch (get record "layout_epoch")
   :attempt (get record "attempt")
   :persistence (get record "persistence")
   :receipt (get record "receipt")
   :manifest-id (identity/bytes->hex (get record "manifest"))})

(defn- regular-cbor-files
  ([root limit]
   (regular-cbor-files root limit
                       :netz.replication/too-many-receipts
                       "Receipt journal"))
  ([^Path root limit type label]
   (if-not (exists? root)
     []
     (with-open [paths (Files/walk
                        root (make-array java.nio.file.FileVisitOption 0))]
       (let [files (->> (.iterator paths)
                        iterator-seq
                        (filter #(and (Files/isRegularFile ^Path % no-links)
                                      (str/ends-with? (str %) ".cbor")))
                        (take (inc limit))
                        vec)]
         (when (> (count files) limit)
           (throw (ex-info (str label " exceeds its configured bound")
                           {:type type :limit limit})))
         files)))))

(defn- sharded-address! [^Path root ^Path target entry-type error-type]
  (let [segments (vec (iterator-seq (.iterator (.relativize root target))))
        directory (some-> (first segments) str)
        filename (some-> (second segments) str)
        hex (when (and (= 2 (count segments))
                       (re-matches #"[0-9a-f]{2}" directory)
                       (str/ends-with? filename ".cbor"))
              (str directory (subs filename 0 (- (count filename) 5))))]
    (when-not (and hex (re-matches #"[0-9a-f]{64}" hex))
      (throw (ex-info (str "Invalid " entry-type " path")
                      {:type error-type
                       :entry-type entry-type
                       :path (str target)})))
    hex))

(defn- load-receipts! [store]
  (mapv (fn [^Path target]
          (let [bytes (read-bounded target max-receipt-bytes)
                record (wire/decode bytes)
                address (identity/bytes->hex (identity/sha256 bytes))
                expected-directory (subs address 0 2)
                expected-name (str (subs address 2) ".cbor")]
            (when-not (and (valid-receipt-record? record)
                           (= expected-directory
                              (str (.getFileName (.getParent target))))
                           (= expected-name (str (.getFileName target)))
                           (true? (async/<!!
                                   (replication-wire/verify-envelope
                                    (get record "receipt")))))
              (throw (ex-info "Invalid durable receipt record"
                              {:type :netz.replication/invalid-receipt-record
                               :path (str target)})))
            (record->controller-event record)))
        (regular-cbor-files (path (:root store) "receipts")
                            (:max-receipts store))))

(defn- load-controller-intents! [store cluster]
  (let [root (controller-intent-root store cluster)]
    (->> (regular-cbor-files
          root (:max-controller-intents store)
          :netz.replication/too-many-controller-intents
          "Controller intent journal")
         (map (fn [^Path target]
                (let [object-hex
                      (sharded-address!
                       root target "controller-intent"
                       :netz.replication/invalid-controller-intent-path)
                      record (wire/decode
                              (read-bounded target
                                            max-controller-intent-bytes))]
                  (when-not (and (intent/valid-record? record)
                                 (= object-hex
                                    (identity/bytes->hex
                                     (get record "object")))
                                 (same-bytes? cluster
                                              (get record "cluster")))
                    (throw (ex-info "Invalid durable controller intent"
                                    {:type
                                     :netz.replication/invalid-intent-record
                                     :path (str target)})))
                  [object-hex record])))
         (sort-by first)
         (mapv second))))

(defn- manifest-address! [^Path root ^Path target]
  (sharded-address! root target "installed-manifest"
                    :netz.replication/invalid-manifest-path))

(defn- installed-addresses! [store]
  (let [root (path (:root store) "manifests")]
    (mapv #(manifest-address! root %)
          (regular-cbor-files
           root (:max-inventory-records store)
           :netz.replication/too-many-installed-objects
           "Installed-object inventory"))))

(defn- scrub-object! [store manifest-id]
  (let [target (marker-path store manifest-id)]
    (if-not (exists? target)
      {:status :missing :manifest manifest-id}
      (let [marker (wire/decode (read-bounded target max-marker-bytes))
            manifest (get marker "descriptor")]
        (when-not (valid-marker? marker manifest-id manifest)
          (throw (ex-info "Invalid installed-object marker"
                          {:type :netz.replication/invalid-install-marker
                           :path (str target)})))
        (verify-object! store manifest)
        {:status :healthy
         :manifest manifest-id
         :object (get marker "object")
         :persistence (get marker "persistence")
         :stored-at (get marker "stored_at")
         :length (get manifest "length")
         :pieces (count (get manifest "pieces"))}))))

(defn- load-installed-manifest! [store manifest-id]
  (let [target (marker-path store manifest-id)]
    (when (exists? target)
      (let [marker (wire/decode (read-bounded target max-marker-bytes))
            manifest (get marker "descriptor")]
        (when-not (valid-marker? marker manifest-id manifest)
          (throw (ex-info "Invalid installed-object marker"
                          {:type :netz.replication/invalid-install-marker
                           :path (str target)})))
        manifest))))

(defn- run-io [operation]
  (async/thread
    (try
      (operation)
      (catch Throwable cause cause))))

(defrecord FileBlobStore [root persistence fsync? clock max-receipts
                          max-inventory-records max-controller-intents lock]
  storage/PBlobTransferStore

  (-load-staging [store _session manifest-id manifest]
    (run-io #(locking lock (load-staging! store manifest-id manifest))))

  (-stage-piece! [store _session manifest-id manifest index piece-id data]
    (run-io #(locking lock
               (stage-piece! store manifest-id manifest index piece-id data))))

  (-install-object! [store _session manifest-id manifest _retention requested]
    (run-io #(locking lock
               (install-object! store manifest-id manifest requested))))

  (-load-piece [store _session manifest-id index piece-id]
    (run-io #(locking lock
               (load-installed-piece! store manifest-id index piece-id))))

  (-persist-receipt! [store session event]
    (run-io #(locking lock
               (let [record (controller-event->record session event)]
                 (atomic-write! store (receipt-path store record)
                                (wire/encode-canonical record))
                 :ok))))

  (-recorded-receipts [store]
    (run-io #(locking lock (load-receipts! store))))

  (-cleanup-session! [_store _session _manifest-id]
    (storage/chan-of true))

  controller-storage/PBlobControllerStore

  (-load-controller-intents [store cluster]
    (run-io #(locking lock (load-controller-intents! store cluster))))

  (-persist-controller-intent! [store cluster object-id record]
    (run-io
     #(locking lock
        (when-not (and (intent/valid-record? record)
                       (same-bytes? cluster (get record "cluster"))
                       (same-bytes? object-id (get record "object")))
          (throw (ex-info "Invalid durable controller intent"
                          {:type :netz.replication/invalid-intent-record})))
        (atomic-write! store
                       (controller-intent-path store cluster object-id)
                       (wire/encode-canonical record))
        :ok)))

  inventory/PBlobInventoryStore

  (-inventory-page [store cursor limit]
    (run-io #(locking lock
               (inventory/page-addresses (installed-addresses! store)
                                         cursor limit))))

  (-scrub-object! [store manifest-id]
    (run-io #(locking lock (scrub-object! store manifest-id))))

  inventory/PBlobProviderStore

  (-load-installed-manifest [store manifest-id]
    (run-io #(locking lock
               (load-installed-manifest! store manifest-id)))))

(defn new-file-store
  "Create a JVM durable-blob store rooted at `root`.

  Options:

  - `:persistence` is `netz/store/fsync/1` (default) or
    `netz/store/best-effort/1`;
  - `:durable-ancestor` is required for fsync: an existing directory whose name
    is already durable, exclusively owned against concurrent filesystem changes.
    Fsync also requires Konserve's directory provisioning helper on the classpath;
  - `:clock` returns Unix milliseconds and is injectable for deterministic
  tests;
  - `:max-receipts` bounds restart scanning (default 100,000);
  - `:max-inventory-records` bounds local manifest scanning (default
    1,000,000); returned pages remain bounded to 1,024 addresses;
  - `:max-controller-intents` bounds the cluster-scoped durable controller
    outbox scan (default 100,000).

  Fsync mode forces every staged piece and each atomic install, receipt, or
  controller-intent record plus its containing directory. An unsupported
  atomic move fails rather than silently weakening the advertised persistence
  profile."
  ([root] (new-file-store root {}))
  ([root opts]
   (let [root (-> (if (instance? Path root)
                    root
                    (Paths/get (str root) (make-array String 0)))
                  .toAbsolutePath
                  .normalize)
         persistence (or (:persistence opts) "netz/store/fsync/1")
         clock (or (:clock opts) #(System/currentTimeMillis))
         max-receipts (or (:max-receipts opts) 100000)
         max-inventory-records (or (:max-inventory-records opts) 1000000)
         max-controller-intents (or (:max-controller-intents opts) 100000)]
     (when-not (and (contains? file-persistence-profiles persistence)
                    (ifn? clock)
                    (int? max-receipts)
                    (pos? max-receipts)
                    (int? max-inventory-records)
                    (pos? max-inventory-records)
                    (int? max-controller-intents)
                    (pos? max-controller-intents))
       (throw (ex-info "Invalid file blob-store options"
                       {:type :netz.replication/invalid-file-store-options})))
     (let [barrier (when (= persistence "netz/store/fsync/1")
                     (directory-barrier/open (:durable-ancestor opts)))]
       (if barrier
         (directory-barrier/provision! barrier root)
         (Files/createDirectories root
                                  (make-array java.nio.file.attribute.FileAttribute 0)))
       (assoc (->FileBlobStore root persistence
                               (= persistence "netz/store/fsync/1")
                               clock max-receipts max-inventory-records
                               max-controller-intents (Object.))
              :directory-barrier barrier)))))
