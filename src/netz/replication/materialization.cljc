(ns netz.replication.materialization
  "Portable application-object bindings and explicit local materialization.

  A binding belongs to an already authenticated catalog or root closure. It
  binds an application key and codec to the exact Netz object and transfer
  manifest. This namespace verifies that binding against the reconciled signed
  retention statement, ensures the object is healthy in the local blob
  repository, reconstructs its logical bytes, and invokes an application-owned
  installer. Ordinary Konserve reads remain local and unchanged."
  (:require [clojure.string :as str]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.inventory :as inventory]
            [netz.replication.read-repair :as read-repair]
            [netz.replication.storage :as storage]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]
            #?(:clj [clojure.core.async :refer [go <!]]
               :cljs [clojure.core.async :refer [<!] :refer-macros [go]])))

(def binding-address-tag "netz/application-object/v1\u0000")
(def max-store-id-bytes 256)
(def max-application-key-bytes 4096)
(def max-codec-id-bytes 128)
(def default-max-materialization-bytes (* 64 1024 1024))

(def ^:private binding-fields
  #{"v" "type" "store_id" "application_key" "codec" "object" "manifest"})

(defn- bytes-n? [value n]
  (and (wire/byte-buffer? value)
       (= n (identity/buf-length value))))

(defn- bounded-id? [value limit]
  (and (string? value)
       (not (str/blank? value))
       (boolean (re-matches #"[!-~]+" value))
       (<= (identity/buf-length (identity/utf8-bytes value)) limit)))

(defn valid-binding?
  "True for the closed, language-neutral application-object binding schema."
  [binding]
  (and (map? binding)
       (= binding-fields (set (keys binding)))
       (= 1 (get binding "v"))
       (= "blob/application-object" (get binding "type"))
       (bounded-id? (get binding "store_id") max-store-id-bytes)
       (let [key (get binding "application_key")]
         (and (wire/byte-buffer? key)
              (pos? (identity/buf-length key))
              (<= (identity/buf-length key) max-application-key-bytes)))
       (bounded-id? (get binding "codec") max-codec-id-bytes)
       (bytes-n? (get binding "object") 32)
       (bytes-n? (get binding "manifest") 32)))

(defn validate-binding [binding]
  (when-not (valid-binding? binding)
    (throw (ex-info "Invalid application-object binding"
                    {:type :netz.replication/invalid-application-binding})))
  binding)

(defn application-binding
  "Create and validate one portable catalog entry.

  `application-key` is opaque bytes. Applications choose their own stable key
  encoding; for example a drive can use UTF-8 SHA-256 hex while a Datahike
  adapter can use deterministic-CBOR UUID bytes. `codec` identifies how the
  logical object bytes are installed, not how Netz transports them."
  [{:keys [store-id application-key codec object-id manifest-id]}]
  (validate-binding
   {"v" 1
    "type" "blob/application-object"
    "store_id" store-id
    "application_key" application-key
    "codec" codec
    "object" object-id
    "manifest" manifest-id}))

(defn binding-address
  "Domain-separated SHA-256 address of a validated canonical binding."
  [binding]
  (validate-binding binding)
  (identity/sha256
   (identity/concat-bufs
    (identity/utf8-bytes binding-address-tag)
    (wire/encode-canonical binding))))

(defn- failure? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn- validate-read-options! [store max-bytes]
  (when-not (and (satisfies? inventory/PBlobProviderStore store)
                 (satisfies? storage/PBlobTransferStore store))
    (throw (ex-info "Materialization requires an installed blob provider store"
                    {:type :netz.replication/materialization-not-supported})))
  (when-not (and (integer? max-bytes)
                 (pos? max-bytes)
                 (<= max-bytes wire/max-safe-integer))
    (throw (ex-info "Invalid materialization byte bound"
                    {:type :netz.replication/invalid-materialization-options}))))

(defn- retention-statement [retention-envelope]
  (let [statement (:statement
                   (replication-wire/parse-envelope retention-envelope))]
    (replication-wire/validate-statement statement)
    statement))

(defn- validate-context! [binding retention-envelope]
  (let [statement (retention-statement retention-envelope)]
    (when-not (and (= "blob/retention" (get statement "type"))
                   (identity/bufs= (get binding "object")
                                   (get statement "object"))
                   (identity/bufs= (get binding "manifest")
                                   (get statement "manifest")))
      (throw (ex-info "Application binding does not match retention"
                      {:type :netz.replication/application-binding-mismatch})))
    statement))

(defn read-installed!
  "Return a channel yielding the verified logical bytes for one local binding.

  This small-value materializer assembles the object in memory and defaults to
  a 64 MiB bound. Streaming application objects require the later bounded
  source/sink API and must not raise this limit merely to bypass backpressure."
  ([store binding] (read-installed! store binding {}))
  ([store binding {:keys [max-bytes]
                   :or {max-bytes default-max-materialization-bytes}}]
   (validate-binding binding)
   (validate-read-options! store max-bytes)
   (let [manifest-id (get binding "manifest")]
     (go
       (try
         (let [manifest (<! (inventory/installed-manifest store manifest-id))]
           (when (failure? manifest) (throw manifest))
           (when-not manifest
             (throw (ex-info "Application object is not installed locally"
                             {:type :netz.replication/manifest-not-installed})))
           (when-not (identity/bufs= (get binding "object")
                                     (get manifest "object"))
             (throw (ex-info "Installed manifest does not match application binding"
                             {:type :netz.replication/application-binding-mismatch})))
           (when (> (get manifest "length") max-bytes)
             (throw (ex-info "Application object exceeds the materialization bound"
                             {:type :netz.replication/materialization-too-large
                              :length (get manifest "length")
                              :limit max-bytes})))
           (let [pieces
                 (loop [index 0 result {}]
                   (if (= index (count (get manifest "pieces")))
                     result
                     (let [piece-id (nth (get manifest "pieces") index)
                           data (<! (storage/-load-piece
                                     store nil manifest-id index piece-id))]
                       (when (failure? data) (throw data))
                       (recur (inc index) (assoc result index data)))))]
             (blob/assemble manifest pieces)))
         (catch #?(:clj Throwable :cljs :default) cause cause))))))

(defn- failed-result [binding code cause ensured]
  {:type :replication/materialization
   :status :failed
   :code code
   :binding-id (identity/bytes->hex (binding-address binding))
   :store-id (get binding "store_id")
   :object-id (identity/bytes->hex (get binding "object"))
   :manifest-id (identity/bytes->hex (get binding "manifest"))
   :ensure ensured
   :cause cause})

(defn materialize!
  "Ensure and explicitly install one authenticated application-object binding.

  `binding` MUST come from an already authenticated and reconciled catalog or
  root closure. It is checked against `retention-envelope` before any network
  or application-store work. `install!` receives a map containing `:binding`,
  `:store-id`, opaque `:application-key`, `:codec`, and verified `:bytes`, and
  returns a channel. It must allowlist codecs, install idempotently, reject an
  existing different value at the same immutable key, and report success only
  after its requested local persistence boundary.

  The result status is `:materialized`, `:unavailable`, or `:failed`. A
  materialized result records whether bytes came from `:local` or `:fetched`.
  `:fetch-one!` and `:attempt` are forwarded to `ensure-local!`; `:max-bytes`
  bounds in-memory reconstruction."
  ([store replication-runtime providers retention-envelope binding install!]
   (materialize! store replication-runtime providers retention-envelope
                 binding install! {}))
  ([store replication-runtime providers retention-envelope binding install!
    {:keys [fetch-one! attempt max-bytes]
     :or {attempt 0 max-bytes default-max-materialization-bytes}}]
   (validate-binding binding)
   (validate-context! binding retention-envelope)
   (when-not (ifn? install!)
     (throw (ex-info "Materialization requires an application installer"
                     {:type :netz.replication/invalid-materializer})))
   (validate-read-options! store max-bytes)
   (when-not (and (integer? attempt) (<= 0 attempt wire/max-safe-integer)
                  (or (nil? fetch-one!) (ifn? fetch-one!)))
     (throw (ex-info "Invalid materialization options"
                     {:type :netz.replication/invalid-materialization-options})))
   (let [ensure-opts (cond-> {:attempt attempt}
                       fetch-one! (assoc :fetch-one! fetch-one!))
         ;; Construct outside `go` so malformed providers and unsupported
         ;; scrubbing fail synchronously before an application waits on a
         ;; channel that can never produce a meaningful result.
         ensure-channel (read-repair/ensure-local!
                         store replication-runtime providers
                         retention-envelope ensure-opts)]
     (go
       (let [ensured (<! ensure-channel)]
         (if-not (contains? #{:local :fetched} (:status ensured))
           {:type :replication/materialization
            :status :unavailable
            :binding-id (identity/bytes->hex (binding-address binding))
            :store-id (get binding "store_id")
            :object-id (identity/bytes->hex (get binding "object"))
            :manifest-id (identity/bytes->hex (get binding "manifest"))
            :ensure ensured}
           (let [bytes (<! (read-installed! store binding
                                            {:max-bytes max-bytes}))]
             (if (failure? bytes)
               (failed-result binding :local-read-failed bytes ensured)
               (let [installed
                     (try
                       (<! (install! {:binding binding
                                      :store-id (get binding "store_id")
                                      :application-key
                                      (get binding "application_key")
                                      :codec (get binding "codec")
                                      :bytes bytes}))
                       (catch #?(:clj Throwable :cljs :default) cause cause))]
                 (if (or (nil? installed) (failure? installed))
                   (failed-result binding :application-install-failed
                                  (or installed
                                      (ex-info
                                       "Application installer closed without a result"
                                       {:type
                                        :netz.replication/empty-install-result}))
                                  ensured)
                   {:type :replication/materialization
                    :status :materialized
                    :source (:status ensured)
                    :binding-id
                    (identity/bytes->hex (binding-address binding))
                    :store-id (get binding "store_id")
                    :application-key (get binding "application_key")
                    :codec (get binding "codec")
                    :object-id (identity/bytes->hex (get binding "object"))
                    :manifest-id (identity/bytes->hex
                                  (get binding "manifest"))
                    :install-result installed
                    :ensure ensured}))))))))))
