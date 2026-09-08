(ns netz.replication.migration-konserve
  "Optional JVM checkpoint adapter using a dedicated Konserve file store.
  Requires authenticated-only AES-GCM and explicit directory provisioning
  (the latter introduced in Konserve #192).
  Single externally fenced writer; no network IO, GC or deletion integration."
  (:require [boring.core :as boring]
            [konserve.core :as k]
            [konserve.directory-sync :as directory-sync]
            [konserve.encryptor]
            [konserve.filestore :as fs]
            [netz.replication.migration :as migration]
            [netz.replication.migration-record :as record]))

(def ^:private checkpoint-key ::checkpoint)
(def max-checkpoint-bytes (* 1024 1024))

(defn- bounded! [bytes]
  (when-not (and (bytes? bytes) (<= 1 (alength ^bytes bytes) max-checkpoint-bytes))
    (throw (ex-info "Missing or oversized checkpoint" {:type ::invalid-size})))
  bytes)

(defn open!
  "Open a dedicated local checkpoint store. Serialization/authentication and
  crash-safe replacement belong to Konserve. Unsupported Konserve versions fail
  before opening a store; no plaintext or legacy-CBC compatibility reads.
  Requires an explicit, independently durable ancestor and exclusive ownership
  of the managed directory path during provisioning. Reopen repeats barriers."
  [path key {:keys [durable-ancestor]}]
  (when-not durable-ancestor
    (throw (ex-info "Checkpoint stores require an explicit durable ancestor"
                    {:type ::missing-durable-ancestor})))
  (when-not (ns-resolve 'konserve.encryptor 'validate-policy!)
    (throw (ex-info "Konserve authenticated-only read policy is required"
                    {:type ::unsupported-konserve})))
  (directory-sync/provision-directory! durable-ancestor path)
  {:store (fs/connect-fs-store
           (str path)
           :config {:sync-blob? true :in-place? false :lock-blob? true
                    :encoding {:serializer :BoringSerializer
                               :encryptor {:type :aes-gcm :key key
                                           :require-authenticated? true}}}
           :opts {:sync? true})})

(defn load!
  "Read against an independent template/revision floor; never reset on missing
  or invalid data. Encryption cannot detect rollback without an external floor."
  [checkpoint-store template minimum-revision]
  (let [store (:store checkpoint-store)
        sealed (k/bget store checkpoint-key
                       (fn [{:keys [input-stream]}]
                         (bounded! (.readNBytes ^java.io.InputStream input-stream
                                                (inc max-checkpoint-bytes))))
                       {:sync? true :raw? true :streaming? true})
        plaintext ((ns-resolve 'konserve.core 'unseal)
                   store checkpoint-key (bounded! sealed) {:sync? true})
        value (boring/decode plaintext {:stringref false :max-depth 32 :max-items 4096
                                        :tolerate-unknown-tags false :on-unknown-record :error})]
    (migration/restore (record/validate-record template value minimum-revision))))

(defn persist!
  "Acknowledge model persistence only after Konserve's configured durable write
  and validated read-back. Do not wrap a distributed Netz store in this adapter."
  [checkpoint-store template state]
  (let [value (record/validate-record template (migration/checkpoint state) 0)
        store (:store checkpoint-store)
        plaintext (bounded! (boring/encode value {:stringref false :max-depth 32}))
        sealed (bounded! ((ns-resolve 'konserve.core 'seal)
                          store checkpoint-key plaintext {:sync? true}))]
    (k/bassoc store checkpoint-key sealed {:sync? true :raw? true})
    (let [read-back (load! checkpoint-store template (:revision value))]
      (when-not (= value (migration/checkpoint read-back))
        (throw (ex-info "Checkpoint read-back mismatch" {:type ::read-back-mismatch})))
      (:state (migration/step state {:type :persisted :checkpoint value})))))
