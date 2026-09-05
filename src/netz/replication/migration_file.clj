(ns netz.replication.migration-file
  "Experimental JVM checkpoint IO, separate from application/replica GC.

  Local HMAC authentication is not remote receipt verification or rollback
  protection. One externally fenced writer owns a path. Caller retains its key
  and trusted transition template outside this file. No deletion integration."
  (:require [clojure.edn :as edn]
            [netz.replication.migration :as migration])
  (:import [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.nio.file Files Path StandardOpenOption StandardCopyOption]
           [java.nio.file.attribute FileAttribute]
           [java.security MessageDigest]
           [java.util Arrays]
           [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]))

(def max-bytes (* 1024 1024))
(def max-counter 9007199254740991)
(def ^:dynamic *after-stage*
  "Fault-injection hook for IO-boundary tests; production callers leave unset."
  (fn [_] nil))
(defn- uint? [x] (and (integer? x) (<= 0 x max-counter)))
(defn- invalid [] (throw (ex-info "Invalid migration checkpoint" {:type ::invalid})))

(defn validate-record
  "Validate against a fresh, independently trusted migration/create template.
  The file cannot supply its own expected placement, policy, or authority."
  [template record]
  (let [base (migration/checkpoint template)
        dynamic #{:attempts :receipts :revision :halted?}
        attempts (:attempts record)
        receipts (:receipts record)]
    (when-not
     (and (map? record) (= (set (keys base)) (set (keys record)))
          (= (apply dissoc base dynamic) (apply dissoc record dynamic))
          (uint? (:revision record)) (boolean? (:halted? record))
          (map? attempts) (= (set (keys (:attempts base))) (set (keys attempts)))
          (every? uint? (vals attempts))
          (map? receipts)
          (every? (fn [[replica receipt]]
                    (and (contains? attempts replica) (map? receipt)
                         (= #{:attempt :expires} (set (keys receipt)))
                         (= (get attempts replica) (:attempt receipt))
                         (uint? (:expires receipt)) (pos? (:expires receipt))))
                  receipts))
      (invalid))
    record))

(defn- mac [key payload]
  (when-not (and (bytes? key) (>= (alength ^bytes key) 32))
    (throw (ex-info "Checkpoint key requires at least 32 random bytes" {})))
  (let [m (Mac/getInstance "HmacSHA256")]
    (.init m (SecretKeySpec. key "HmacSHA256"))
    (.update m (.getBytes "netz/local-migration-checkpoint/1\u0000" "UTF-8"))
    (.doFinal m ^bytes payload)))

(defn- decode [template key bytes minimum-revision]
  (when-not (and (uint? minimum-revision) (<= 33 (alength ^bytes bytes) max-bytes))
    (invalid))
  (let [tag (Arrays/copyOfRange ^bytes bytes 0 32)
        payload (Arrays/copyOfRange ^bytes bytes 32 (alength ^bytes bytes))]
    ;; Authenticate before parsing. The tag covers the exact stored bytes.
    (when-not (MessageDigest/isEqual tag (mac key payload)) (invalid))
    (let [record (validate-record template (edn/read-string (String. payload "UTF-8")))]
      (when (< (:revision record) minimum-revision) (invalid))
      (migration/restore record))))

(defn load!
  "Read/authenticate a bounded checkpoint and reject revisions below a caller's
  independently retained floor. Throws on missing files; never silently resets.
  Refresh authority and retention protection before using restored state."
  [^Path path template key minimum-revision]
  (with-open [in (Files/newInputStream path (make-array java.nio.file.OpenOption 0))]
    (let [bytes (.readNBytes in (inc max-bytes))]
      (decode template key bytes minimum-revision))))

(defn persist!
  "Write a unique temporary file, fsync, atomically replace, fsync directory,
  then authenticate read-back before returning model persistence completion.
  POSIX-like local filesystem required; unsupported atomic move/fsync fails.
  Directory must exist and be private to the trusted single writer."
  [^Path path template key state]
  (let [record (validate-record template (migration/checkpoint state))
        payload (.getBytes (pr-str record) "UTF-8")
        tag (mac key payload)
        _ (when (> (+ 32 (alength payload)) max-bytes) (invalid))
        parent (.getParent (.toAbsolutePath path))
        temp (Files/createTempFile parent ".migration-" ".tmp" (make-array FileAttribute 0))]
    (try
      (with-open [out (FileChannel/open temp (into-array java.nio.file.OpenOption
                                                         [StandardOpenOption/WRITE]))]
        (doseq [bytes [tag payload]]
          (let [buffer (ByteBuffer/wrap bytes)]
            (while (.hasRemaining buffer) (.write out buffer))))
        (.force out true))
      (*after-stage* :file-synced)
      (Files/move temp (.toAbsolutePath path)
                  (into-array java.nio.file.CopyOption
                              [StandardCopyOption/ATOMIC_MOVE StandardCopyOption/REPLACE_EXISTING]))
      (*after-stage* :renamed)
      (with-open [directory (FileChannel/open parent (into-array java.nio.file.OpenOption
                                                                 [StandardOpenOption/READ]))]
        (.force directory true))
      (*after-stage* :directory-synced)
      (let [restored (load! path template key (:revision record))]
        (when-not (= record (migration/checkpoint restored)) (invalid))
        (:state (migration/step state {:type :persisted :checkpoint record})))
      (finally (Files/deleteIfExists temp)))))
