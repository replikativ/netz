(ns netz.replication.runtime
  "Capability-gated runtime for durable-blob sender and receiver machines.

  This adapter shares the authenticated Kabel/Noise carrier owned by
  `netz.overlay.runtime`. It preserves connection order while verifying signed
  retention/receipt envelopes, binds every frame to the negotiated remote full
  Authority ID, executes the portable storage actions, and keeps all machine
  transitions on one bounded event loop.

  Receiver policy remains explicit. `:authorize-offer!` must return a channel
  yielding true only after the controller signer/layout is authorized and the
  retention statement has crossed the durable lifecycle-journal boundary. The
  default rejects every offer."
  (:require [clojure.string :as str]
            [netz.identity :as identity]
            [netz.overlay.runtime :as overlay-runtime]
            [netz.replication.sender :as sender]
            [netz.replication.storage :as storage]
            [netz.replication.transfer :as transfer]
            [netz.replication.wire :as replication-wire]
            #?(:clj [clojure.core.async :as async
                     :refer [chan close! go go-loop <! timeout]]
               :cljs [clojure.core.async :as async
                      :refer [chan close! <! timeout]
                      :refer-macros [go go-loop]])))

(def ^:private extension-id ::durable-blob)
(def ^:private event-capacity 256)

(defn- failure? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn- blob-frame? [value]
  (and (map? value)
       (string? (get value "type"))
       (str/starts-with? (get value "type") "blob/")))

(defn- authority-bytes [facts]
  (let [value (:authority-id facts)]
    (when-not (and (string? value)
                   (re-matches #"[0-9a-f]{64}" value))
      (throw (ex-info "Extension received invalid authenticated Authority facts"
                      {:type :netz.replication/invalid-connection-authority})))
    (identity/hex->bytes value)))

(defn- submit! [ctx event]
  (and (not @(:stopped? ctx))
       (boolean (async/offer! (:events ctx) event))))

(declare advance! stop!)

(defn- callback! [ctx callback value stat]
  (try
    (when callback (callback value))
    (swap! (:stats ctx) update stat (fnil inc 0))
    (catch #?(:clj Throwable :cljs :default) _
      (swap! (:stats ctx) update :callback-errors inc))))

(defn- apply-action! [ctx side [op & args :as action]]
  (cond
    (= :send op)
    (let [[peer frame] args]
      (when-not (overlay-runtime/send-extension!
                 (:node ctx) peer replication-wire/capability frame)
        (swap! (:stats ctx) update :send-refused inc)))

    (= :timer op)
    (let [[delay payload] args]
      (go
        (<! (timeout (max 0 delay)))
        (submit! ctx {:kind :machine
                      :side side
                      :event {:type :timer :payload payload}})))

    (contains? storage/storage-ops op)
    (go
      (let [result (try
                     (<! (storage/execute-action (:store ctx) action))
                     (catch #?(:clj Throwable :cljs :default) error error))
            event (storage/completion-event action result)]
        (if event
          (submit! ctx {:kind :machine :side side :event event})
          (when (failure? result)
            (swap! (:stats ctx) update :storage-errors inc)))))

    (= :sign-receipt op)
    (let [[session statement] args]
      (go
        (let [envelope
              (try
                (<! (replication-wire/sign-statement
                     (:identity ctx) statement))
                (catch #?(:clj Throwable :cljs :default) error error))]
          (submit! ctx
                   {:kind :machine
                    :side :receiver
                    :event {:type :transfer/receipt-signed
                            :session session
                            :envelope envelope}}))))

    (= :replication-stored op)
    (callback! ctx (:on-stored! ctx) (first args) :stored)

    (= :transfer-failed op)
    (callback! ctx (:on-failed! ctx) (first args) :failed)

    :else
    (throw (ex-info "Unknown durable-blob runtime action"
                    {:type :netz.replication.runtime/unknown-action
                     :side side :action action}))))

(defn- advance! [ctx side event]
  (let [state-atom (get ctx (if (= side :sender)
                              :sender-state :receiver-state))
        handler (if (= side :sender) sender/handler transfer/handler)
        {next-state :state actions :actions}
        (handler @state-atom event {:now ((:now-fn ctx))})]
    (reset! state-atom next-state)
    (doseq [action actions] (apply-action! ctx side action))))

(defn- send-rejection! [ctx peer session code]
  (overlay-runtime/send-extension!
   (:node ctx) peer replication-wire/capability
   {"v" 1 "type" "blob/abort" "session" session "code" code}))

(defn- process-offer! [ctx from from-authority frame]
  (go
    (let [verified (<! (replication-wire/verify-envelope
                        (get frame "retention")))]
      (if-not (true? verified)
        (do
          (swap! (:stats ctx) update :invalid-signatures inc)
          (overlay-runtime/disconnect-peer! (:node ctx) from))
        (let [retention (:statement
                         (replication-wire/parse-envelope
                          (get frame "retention")))
              request {:from from
                       :from-authority from-authority
                       :frame frame
                       :retention retention}
              [decision authorization-error?]
              (try
                [(<! ((:authorize-offer! ctx) request)) false]
                (catch #?(:clj Throwable :cljs :default) _ [false true]))]
          (when authorization-error?
            (swap! (:stats ctx) update :authorization-errors inc))
          (if (true? decision)
            (advance! ctx :receiver
                      {:type :transfer/authorized-offer
                       :from from :from-authority from-authority :frame frame})
            (do
              (swap! (:stats ctx) update :unauthorized-offers inc)
              (send-rejection! ctx from (get frame "session")
                               "unauthorized"))))))))

(defn- session-side [ctx session]
  (let [key (identity/bytes->hex session)
        sender? (contains? (:sessions @(:sender-state ctx)) key)
        receiver? (contains? (:sessions @(:receiver-state ctx)) key)]
    (cond
      (and sender? receiver?) :ambiguous
      sender? :sender
      receiver? :receiver
      :else nil)))

(defn- process-wire! [ctx {:keys [from facts frame]}]
  (go
    (let [from-authority (authority-bytes facts)]
      (swap! (:stats ctx) update :inbound inc)
      (case (get frame "type")
        "blob/offer"
        (<! (process-offer! ctx from from-authority frame))

        "blob/piece"
        (advance! ctx :receiver
                  {:type :transfer/piece :from from
                   :from-authority from-authority :frame frame})

        "blob/commit"
        (advance! ctx :receiver
                  {:type :transfer/commit :from from
                   :from-authority from-authority :frame frame})

        "blob/need"
        (advance! ctx :sender
                  {:type :transfer/need :from from
                   :from-authority from-authority :frame frame})

        "blob/ack"
        (advance! ctx :sender
                  {:type :transfer/ack :from from
                   :from-authority from-authority :frame frame})

        "blob/stored"
        (let [verified (<! (replication-wire/verify-envelope
                            (get frame "receipt")))]
          (if (true? verified)
            (advance! ctx :sender
                      {:type :transfer/verified-stored :from from
                       :from-authority from-authority :frame frame})
            (do
              (swap! (:stats ctx) update :invalid-signatures inc)
              (overlay-runtime/disconnect-peer! (:node ctx) from))))

        "blob/abort"
        (case (session-side ctx (get frame "session"))
          :sender (advance! ctx :sender
                            {:type :transfer/abort :from from
                             :from-authority from-authority :frame frame})
          :receiver (advance! ctx :receiver
                              {:type :transfer/abort :from from
                               :from-authority from-authority :frame frame})
          :ambiguous (overlay-runtime/disconnect-peer! (:node ctx) from)
          nil)

        nil))))

(defn- run-loop! [ctx]
  (go-loop []
    (when-let [{:keys [kind side event] :as item} (<! (:events ctx))]
      (try
        (case kind
          :machine (advance! ctx side event)
          :wire (<! (process-wire! ctx item))
          :disconnected (do
                          (advance! ctx :sender
                                    {:type :disconnected :peer (:peer item)})
                          (advance! ctx :receiver
                                    {:type :disconnected :peer (:peer item)}))
          nil)
        (catch #?(:clj Throwable :cljs :default) _
          (swap! (:stats ctx) update :dropped inc)))
      (recur))))

(defn start!
  "Attach durable-blob transfer to an authenticated overlay node.

  Required option `:store` satisfies `PBlobTransferStore`. Optional callbacks:

  - `:authorize-offer!` returns a channel yielding true only after policy and
    durable lifecycle acceptance (default rejects);
  - `:on-stored!` receives replayed/new fenced `:replication/stored` events;
  - `:on-failed!` receives sender failure records;
  - `:sender-opts`, `:receiver-opts`, and `:now-fn` configure pure machines.

  Noise must be constructed with `netz/blob-replication/1` in its supported
  capability set on both peers. The capability is intentionally not in the
  default set."
  [{:keys [identity] :as node}
   {:keys [store authorize-offer! on-stored! on-failed!
           sender-opts receiver-opts now-fn]}]
  (when-not (satisfies? storage/PBlobTransferStore store)
    (throw (ex-info "Durable-blob runtime requires a transfer store"
                    {:type :netz.replication.runtime/missing-store})))
  (let [authority (identity/authority-id-bytes (:genesis identity))
        ctx {:node node
             :identity identity
             :store store
             :sender-state (atom (sender/make-state sender-opts))
             :receiver-state (atom (transfer/make-state authority receiver-opts))
             :authorize-offer! (or authorize-offer!
                                   (fn [_] (storage/chan-of false)))
             :on-stored! on-stored!
             :on-failed! on-failed!
             :now-fn (or now-fn #?(:clj #(System/currentTimeMillis)
                                   :cljs #(.getTime (js/Date.))))
             :events (chan event-capacity)
             :stopped? (atom false)
             :cleanup (atom nil)
             :stats (atom {:inbound 0 :stored 0 :failed 0
                           :invalid-signatures 0 :unauthorized-offers 0
                           :authorization-errors 0
                           :send-refused 0 :storage-errors 0
                           :callback-errors 0 :dropped 0})}
        cleanup
        (overlay-runtime/register-extension!
         node extension-id
         {:capability replication-wire/capability
          :match? blob-frame?
          :valid? replication-wire/valid-frame?
          :receive! (fn [from facts frame]
                      (submit! ctx {:kind :wire :from from
                                    :facts facts :frame frame}))
          :disconnected! (fn [peer]
                           (submit! ctx {:kind :disconnected :peer peer}))})]
    (reset! (:cleanup ctx) cleanup)
    (run-loop! ctx)
    (overlay-runtime/on-stop! node #(stop! ctx))
    (go
      (let [receipts (<! (storage/-recorded-receipts store))]
        (if (failure? receipts)
          (swap! (:stats ctx) update :storage-errors inc)
          (doseq [event receipts]
            (callback! ctx on-stored! event :stored)))))
    ctx))

(defn replicate!
  "Start one already-signed offer after the controller's durable outbox step.
  Returns false when the target connection did not negotiate the capability or
  the bounded runtime queue is full."
  [ctx peer authority offer]
  (try
    (replication-wire/validate-frame offer)
    (and (= "blob/offer" (get offer "type"))
         (overlay-runtime/connection-supports?
          (get-in ctx [:node :peer]) peer replication-wire/capability)
         (submit! ctx {:kind :machine
                       :side :sender
                       :event {:type :transfer/start
                               :peer peer :authority authority :frame offer}}))
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn states [ctx]
  {:sender @(:sender-state ctx)
   :receiver @(:receiver-state ctx)})

(defn stats [ctx] @(:stats ctx))

(defn stop!
  "Stop the blob runtime and remove its extension handler. Idempotent."
  [ctx]
  (when (compare-and-set! (:stopped? ctx) false true)
    (when-let [cleanup @(:cleanup ctx)] (cleanup))
    (close! (:events ctx))
    true))
