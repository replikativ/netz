(ns netz.replication.storage
  "Asynchronous storage boundary for durable-blob transfer machines.

  The protocol deliberately mirrors sender and receiver actions. Implementors
  may use files, Konserve, IndexedDB or another backend, but the returned
  channel must not yield success until the operation has reached the requested
  persistence boundary. Exceptions may be delivered on the channel; a runtime
  converts them to the corresponding machine failure event.

  Durable staging is addressed by manifest and piece hashes, not by volatile
  transfer sessions. `-cleanup-session!` releases only session-local resources,
  so a fresh session can resume verified pieces after a crash or disconnect."
  (:require [clojure.core.async :refer [chan close! put!]]))

(defprotocol PBlobTransferStore
  (-load-staging [store session manifest-id manifest]
    "Yield `{:held [piece-id ...] :installed map-or-nil}` after verifying the
    durable inventory for a receiver offer.")

  (-stage-piece! [store session manifest-id manifest index piece-id data]
    "Verify and durably stage one piece. Yield `:ok` only after persistence.")

  (-install-object! [store session manifest-id manifest retention persistence]
    "Verify the complete object and atomically publish its installation marker.
    Yield `{:result :ok :persistence text :stored-at unix-ms}` on success.")

  (-load-piece [store session manifest-id index piece-id]
    "Load and verify one installed piece for a sender. Yield its bytes.")

  (-persist-receipt! [store session controller-event]
    "Durably record one verified signed receipt before quorum accounting.
    Yield `:ok` after persistence.")

  (-recorded-receipts [store]
    "Yield verified local receipt records as `:replication/stored` events for
    restart replay. The replication controller still fences layout/attempt.")

  (-cleanup-session! [store session manifest-id]
    "Release volatile/session-local resources without deleting resumable
    content-addressed staging. Yield true."))

(defn chan-of
  "Return a closed channel containing `value`; nil closes it empty."
  [value]
  (let [result (chan 1)]
    (when (some? value) (put! result value))
    (close! result)
    result))

(def storage-ops
  "Action operations interpreted by `execute-action`."
  #{:load-staging :stage-piece :install-object :load-piece
    :persist-receipt :cleanup-staging})

(defn execute-action
  "Execute one sender/receiver storage action and return its result channel.

  Network sends, timers, signing and controller handoffs intentionally remain
  outside this adapter. Unknown operations fail immediately."
  [store [op & args :as action]]
  (case op
    :load-staging (apply -load-staging store args)
    :stage-piece (apply -stage-piece! store args)
    :install-object (apply -install-object! store args)
    :load-piece (apply -load-piece store args)
    :persist-receipt (apply -persist-receipt! store args)
    :cleanup-staging (apply -cleanup-session! store args)
    (throw (ex-info "Not a durable-blob storage action"
                    {:type :netz.replication/unknown-storage-action
                     :action action}))))

(defn- failure? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn completion-event
  "Translate a storage action result into its machine completion event.

  This is pure so runtimes in every host can share the exact same error and
  callback semantics. Cleanup has no completion event."
  [[op & args :as action] result]
  (case op
    :load-staging
    (let [[session] args]
      (if (failure? result)
        {:type :transfer/staging-loaded :session session :error result}
        (merge {:type :transfer/staging-loaded :session session} result)))

    :stage-piece
    (let [[session _manifest-id _manifest index] args]
      {:type :transfer/piece-staged
       :session session
       :index index
       :result (if (failure? result) :error result)})

    :install-object
    (let [[session] args]
      (if (failure? result)
        {:type :transfer/installed :session session :result :error}
        (merge {:type :transfer/installed :session session} result)))

    :load-piece
    (let [[session _manifest-id index] args]
      (if (failure? result)
        {:type :transfer/piece-loaded
         :session session :index index :result :error}
        {:type :transfer/piece-loaded
         :session session :index index :result :ok :data result}))

    :persist-receipt
    (let [[session] args]
      {:type :transfer/receipt-persisted
       :session session
       :result (if (failure? result) :error result)})

    :cleanup-staging nil

    (throw (ex-info "Not a durable-blob storage action"
                    {:type :netz.replication/unknown-storage-action
                     :action action}))))
