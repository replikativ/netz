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
            [netz.replication.inventory :as inventory]
            [netz.replication.sender :as sender]
            [netz.replication.storage :as storage]
            [netz.replication.transfer :as transfer]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]
            #?(:clj [clojure.core.async :as async
                     :refer [chan close! go go-loop <! put! timeout]]
               :cljs [clojure.core.async :as async
                      :refer [chan close! <! put! timeout]
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

(defn- same-value? [a b]
  (identity/bufs= (wire/encode-canonical a)
                  (wire/encode-canonical b)))

(defn- same-authority? [authority value]
  (and (wire/byte-buffer? value)
       (= 32 (identity/buf-length value))
       (identity/bufs= authority value)))

(defn- session-hex [session]
  (when (and (wire/byte-buffer? session)
             (= 16 (identity/buf-length session)))
    (identity/bytes->hex session)))

(defn- submit! [ctx event]
  (and (not @(:stopped? ctx))
       (boolean (async/offer! (:events ctx) event))))

(declare advance! stop! replicate!)

(defn- callback! [ctx callback value stat]
  (try
    (when callback (callback value))
    (swap! (:stats ctx) update stat (fnil inc 0))
    (catch #?(:clj Throwable :cljs :default) _
      (swap! (:stats ctx) update :callback-errors inc))))

(defn- fetch-failure [entry code]
  {:type :replication/fetch-failed
   :status :failed
   :session (:session entry)
   :object-id
   (identity/bytes->hex
    (get-in entry [:retention "statement" "object"]))
   :manifest-id
   (identity/bytes->hex
    (get-in entry [:retention "statement" "manifest"]))
   :provider-id (identity/bytes->hex (:authority entry))
   :code code})

(defn- deliver-fetch! [entry result]
  (when-let [result-channel (::result-channel entry)]
    (put! result-channel result (fn [_] (close! result-channel)))))

(defn- fail-fetch! [ctx session code]
  (when-let [key (session-hex session)]
    (let [entry (get @(:fetches ctx) key)]
      (when entry
        (swap! (:fetches ctx) dissoc key)
        (let [failure (fetch-failure entry code)]
          (deliver-fetch! entry failure)
          (callback! ctx (:on-fetch-failed! ctx) failure :fetch-failed))
        true))))

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

    (= :replica-installed op)
    (let [event (first args)
          key (session-hex (:session event))
          entry (when key (get @(:fetches ctx) key))]
      (when entry
        (swap! (:fetches ctx) dissoc key)
        (swap! (:stats ctx) update :fetched inc)
        (deliver-fetch!
         entry
         (assoc event
                :status :installed
                :provider-id (identity/bytes->hex (:authority entry)))))
      (callback! ctx (:on-installed! ctx) event :installed))

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
        (let [pending (get @(:fetches ctx)
                           (session-hex (get frame "session")))
              responding-provider?
              (and pending
                   (= from (:peer pending))
                   (identity/bufs= from-authority (:authority pending)))
              expected? (or (nil? pending)
                            (and responding-provider?
                                 (= (get frame "attempt")
                                    (:attempt pending))
                                 (same-value? (get frame "retention")
                                              (:retention pending))))
              retention (:statement
                         (replication-wire/parse-envelope
                          (get frame "retention")))]
          (if-not expected?
            (do
              (when responding-provider?
                (fail-fetch! ctx (get frame "session") :offer-mismatch))
              (overlay-runtime/disconnect-peer! (:node ctx) from))
            (let [request {:from from
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
                           :from from :from-authority from-authority
                           :frame frame})
                (do
                  (fail-fetch! ctx (get frame "session")
                               :unauthorized-offer)
                  (swap! (:stats ctx) update :unauthorized-offers inc)
                  (send-rejection! ctx from (get frame "session")
                                   "unauthorized"))))))))))

(defn- process-fetch! [ctx from from-authority frame]
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
              request {:from from :from-authority from-authority
                       :frame frame :retention retention}
              [decision authorization-error?]
              (try
                [(<! ((:authorize-fetch! ctx) request)) false]
                (catch #?(:clj Throwable :cljs :default) _ [false true]))]
          (when authorization-error?
            (swap! (:stats ctx) update :authorization-errors inc))
          (cond
            (not (true? decision))
            (do
              (swap! (:stats ctx) update :unauthorized-fetches inc)
              (send-rejection! ctx from (get frame "session")
                               "fetch-unauthorized"))

            (not (satisfies? inventory/PBlobProviderStore (:store ctx)))
            (do
              (swap! (:stats ctx) update :fetch-unavailable inc)
              (send-rejection! ctx from (get frame "session")
                               "fetch-unavailable"))

            :else
            (let [manifest
                  (try
                    (<! (inventory/installed-manifest
                         (:store ctx) (get retention "manifest")))
                    (catch #?(:clj Throwable :cljs :default) cause cause))]
              (if (or (failure? manifest) (nil? manifest))
                (do
                  (when (failure? manifest)
                    (swap! (:stats ctx) update :storage-errors inc))
                  (swap! (:stats ctx) update :fetch-unavailable inc)
                  (send-rejection! ctx from (get frame "session")
                                   "fetch-unavailable"))
                (let [offer {"v" 1 "type" "blob/offer"
                             "session" (get frame "session")
                             "attempt" (get frame "attempt")
                             "replica" from-authority
                             "retention" (get frame "retention")
                             "manifest" manifest}]
                  (if (replicate! ctx from from-authority offer)
                    (swap! (:stats ctx) update :fetch-served inc)
                    (do
                      (swap! (:stats ctx) update :fetch-unavailable inc)
                      (send-rejection! ctx from (get frame "session")
                                       "fetch-unavailable"))))))))))))

(defn- process-deletion! [ctx from from-authority frame]
  (go
    (let [envelope (get frame "deletion")
          verified? (try
                      (true? (<! (replication-wire/verify-envelope envelope)))
                      (catch #?(:clj Throwable :cljs :default) _ false))
          statement (when verified?
                      (:statement (replication-wire/parse-envelope envelope)))]
      (if-not (and statement
                   (same-authority? from-authority (get statement "signer")))
        (do
          (swap! (:stats ctx) update :invalid-lifecycle-frames inc)
          (overlay-runtime/disconnect-peer! (:node ctx) from))
        (let [accepted?
              (try
                (true? (<! ((:apply-deletion! ctx)
                            {:from from :from-authority from-authority
                             :frame frame :envelope envelope
                             :statement statement})))
                (catch #?(:clj Throwable :cljs :default) _ false))]
          (swap! (:stats ctx) update
                 (if accepted? :deletions-accepted :deletions-rejected) inc))))))

(defn- process-deletion-receipt! [ctx from from-authority frame]
  (go
    (let [envelope (get frame "receipt")
          verified? (try
                      (true? (<! (replication-wire/verify-envelope envelope)))
                      (catch #?(:clj Throwable :cljs :default) _ false))
          statement (when verified?
                      (:statement (replication-wire/parse-envelope envelope)))]
      (if-not (and statement
                   (same-authority? from-authority (get statement "replica"))
                   (same-authority? from-authority (get statement "signer")))
        (do
          (swap! (:stats ctx) update :invalid-lifecycle-frames inc)
          (overlay-runtime/disconnect-peer! (:node ctx) from))
        (callback! ctx (:on-deletion-receipt! ctx)
                   {:from from :from-authority from-authority
                    :frame frame :envelope envelope :statement statement}
                   :deletion-receipts)))))

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
        "blob/delete"
        (<! (process-deletion! ctx from from-authority frame))

        "blob/deleted"
        (<! (process-deletion-receipt! ctx from from-authority frame))

        "blob/fetch"
        (<! (process-fetch! ctx from from-authority frame))

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
          :receiver (do
                      (advance! ctx :receiver
                                {:type :transfer/abort :from from
                                 :from-authority from-authority :frame frame})
                      (let [entry (get @(:fetches ctx)
                                       (session-hex (get frame "session")))]
                        (when (and entry (= from (:peer entry))
                                   (identity/bufs= from-authority
                                                   (:authority entry)))
                          (fail-fetch! ctx (get frame "session")
                                       (get frame "code")))))
          :ambiguous (overlay-runtime/disconnect-peer! (:node ctx) from)
          (let [entry (get @(:fetches ctx)
                           (session-hex (get frame "session")))]
            (when (and entry (= from (:peer entry))
                       (identity/bufs= from-authority (:authority entry)))
              (fail-fetch! ctx (get frame "session")
                           (get frame "code")))))

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
                                    {:type :disconnected :peer (:peer item)})
                          (doseq [[_ entry] @(:fetches ctx)
                                  :when (= (:peer item) (:peer entry))]
                            (fail-fetch! ctx (:session entry) :disconnected)))
          :fetch-timeout (fail-fetch! ctx (:session item) :timeout)
          nil)
        (catch #?(:clj Throwable :cljs :default) _
          (swap! (:stats ctx) update :dropped inc)))
      (recur))))

(defn start!
  "Attach durable-blob transfer to an authenticated overlay node.

  Required option `:store` satisfies `PBlobTransferStore`. Optional callbacks:

  - `:authorize-offer!` returns a channel yielding true only after policy and
    durable lifecycle acceptance (default rejects);
  - `:authorize-fetch!` separately authorizes an authenticated reader before
    this peer serves an installed object (default rejects);
  - `:on-stored!` receives replayed/new fenced `:replication/stored` events;
  - `:on-installed!` receives locally installed replica events;
  - `:on-failed!` receives sender failure records;
  - `:on-fetch-failed!` receives requester-side fetch failures;
  - `:apply-deletion!` durably applies a verified, carrier-bound deletion and
    returns a channel yielding true (default rejects);
  - `:on-deletion-receipt!` receives verified receipts whose replica signer is
    the immediate authenticated remote Authority;
  - `:replay-receipts?` defaults true; set false when a controller runtime owns
    ordered intent-first receipt restoration from the same store;
  - `:sender-opts`, `:receiver-opts`, and `:now-fn` configure pure machines.

  Noise must be constructed with `netz/blob-replication/1` in its supported
  capability set on both peers. The capability is intentionally not in the
  default set."
  [{:keys [identity] :as node}
   {:keys [store authorize-offer! authorize-fetch! apply-deletion!
           on-stored! on-installed! on-failed! on-fetch-failed!
           on-deletion-receipt!
           sender-opts receiver-opts now-fn max-fetches fetch-timeout-ms
           replay-receipts?]
    :or {max-fetches 64 fetch-timeout-ms 30000 replay-receipts? true}}]
  (when-not (satisfies? storage/PBlobTransferStore store)
    (throw (ex-info "Durable-blob runtime requires a transfer store"
                    {:type :netz.replication.runtime/missing-store})))
  (when-not (and (int? max-fetches) (pos? max-fetches)
                 (int? fetch-timeout-ms) (pos? fetch-timeout-ms))
    (throw (ex-info "Invalid durable-blob fetch runtime options"
                    {:type :netz.replication.runtime/invalid-fetch-options})))
  (let [authority (identity/authority-id-bytes (:genesis identity))
        ctx {:node node
             :identity identity
             :store store
             :sender-state (atom (sender/make-state sender-opts))
             :receiver-state (atom (transfer/make-state authority receiver-opts))
             :authorize-offer! (or authorize-offer!
                                   (fn [_] (storage/chan-of false)))
             :authorize-fetch! (or authorize-fetch!
                                   (fn [_] (storage/chan-of false)))
             :apply-deletion! (or apply-deletion!
                                  (fn [_] (storage/chan-of false)))
             :on-stored! on-stored!
             :on-installed! on-installed!
             :on-failed! on-failed!
             :on-fetch-failed! on-fetch-failed!
             :on-deletion-receipt! on-deletion-receipt!
             :authority authority
             :fetches (atom {})
             :max-fetches max-fetches
             :fetch-timeout-ms fetch-timeout-ms
             :now-fn (or now-fn #?(:clj #(System/currentTimeMillis)
                                   :cljs #(.getTime (js/Date.))))
             :events (chan event-capacity)
             :stopped? (atom false)
             :cleanup (atom nil)
             :stats (atom {:inbound 0 :stored 0 :failed 0
                           :installed 0 :fetched 0 :fetch-failed 0
                           :fetch-served 0 :fetch-unavailable 0
                           :unauthorized-fetches 0
                           :invalid-signatures 0 :unauthorized-offers 0
                           :authorization-errors 0
                           :deletions-accepted 0 :deletions-rejected 0
                           :deletion-receipts 0 :invalid-lifecycle-frames 0
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
    (when replay-receipts?
      (go
        (let [receipts (<! (storage/-recorded-receipts store))]
          (if (failure? receipts)
            (swap! (:stats ctx) update :storage-errors inc)
            (doseq [event receipts]
              (callback! ctx on-stored! event :stored))))))
    ctx))

(defn fetch!
  "Ask one authenticated provider to push an installed object back to us.

  `retention-envelope` is the controller-signed statement already reconciled
  by the application. The provider still applies its independent
  `:authorize-fetch!` read policy, and this requester applies
  `:authorize-offer!` before accepting the response. Returns the fresh 16-byte
  session, or false when capability/limits/input reject the request.

  Optional `:attempt` defaults to zero and `:session` exists for deterministic
  tests only; production callers should let the runtime draw it from the CSPRNG."
  ([ctx peer authority retention-envelope]
   (fetch! ctx peer authority retention-envelope {}))
  ([ctx peer authority retention-envelope {:keys [attempt session]
                                           :or {attempt 0}
                                           :as opts}]
   (try
     (let [statement (:statement
                      (replication-wire/parse-envelope retention-envelope))
           session (or session (identity/random-bytes 16))
           key (session-hex session)
           frame {"v" 1 "type" "blob/fetch" "session" session
                  "attempt" attempt "retention" retention-envelope}
           entry {:session session :peer peer :authority authority
                  :attempt attempt :retention retention-envelope
                  ::result-channel (::result-channel opts)}]
       (replication-wire/validate-frame frame)
       (if-not (and (= "blob/retention" (get statement "type"))
                    key
                    (wire/byte-buffer? authority)
                    (= 32 (identity/buf-length authority))
                    (overlay-runtime/connection-supports?
                     (get-in ctx [:node :peer]) peer
                     replication-wire/capability))
         false
         (let [fetches (swap! (:fetches ctx)
                              (fn [current]
                                (if (or (contains? current key)
                                        (>= (count current)
                                            (:max-fetches ctx)))
                                  current
                                  (assoc current key entry))))]
           (if-not (= entry (get fetches key))
             false
             (if-not (overlay-runtime/send-extension!
                      (:node ctx) peer replication-wire/capability frame)
               (do (swap! (:fetches ctx) dissoc key) false)
               (do
                 (go
                   (<! (timeout (:fetch-timeout-ms ctx)))
                   (submit! ctx {:kind :fetch-timeout :session session}))
                 session))))))
     (catch #?(:clj Throwable :cljs :default) _ false))))

(defn fetch-result!
  "Start `fetch!` and return a one-value channel for its terminal outcome.

  Success yields the local `:replication/installed` event extended with
  `:status :installed` and the selected `:provider-id`. Remote abort,
  disconnect, timeout and shutdown yield `:replication/fetch-failed`. Immediate
  input, capacity or capability refusal yields `{:status :refused}`."
  ([ctx peer authority retention-envelope]
   (fetch-result! ctx peer authority retention-envelope {}))
  ([ctx peer authority retention-envelope opts]
   (let [result-channel (chan 1)]
     (when-not (fetch! ctx peer authority retention-envelope
                       (assoc opts ::result-channel result-channel))
       (put! result-channel {:type :replication/fetch-failed
                             :status :refused
                             :code :refused}
             (fn [_] (close! result-channel))))
     result-channel)))

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

(defn- unique-authority-peer [ctx authority-id]
  (when (and (string? authority-id)
             (re-matches #"[0-9a-f]{64}" authority-id))
    (let [peer (get-in ctx [:node :peer])
          matches (filterv
                   #(and (= authority-id
                            (:authority-id
                             (overlay-runtime/connection-info peer %)))
                         (overlay-runtime/portable-extension-connection?
                          peer % replication-wire/capability))
                   (overlay-runtime/connections peer))]
      (when (= 1 (count matches)) (first matches)))))

(defn- send-lifecycle-frame! [ctx remote-authority envelope
                              expected-type frame-type field]
  (try
    (let [statement (:statement (replication-wire/parse-envelope envelope))
          local (:authority ctx)
          peer (unique-authority-peer ctx remote-authority)
          frame {"v" 1 "type" frame-type field envelope}]
      (boolean
       (and peer
            (= expected-type (get statement "type"))
            (same-authority? local (get statement "signer"))
            (or (not= expected-type "blob/deleted")
                (same-authority? local (get statement "replica")))
            (replication-wire/valid-frame? frame)
            (overlay-runtime/send-extension!
             (:node ctx) peer replication-wire/capability frame))))
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn send-deletion!
  "Send a controller-signed deletion to exactly one connected replica.

  The envelope signer must be this node's Authority. `replica-id` is resolved
  to one authenticated portable carrier; zero or duplicate connections fail
  closed. The return value is transport acceptance, never erasure evidence."
  [ctx replica-id deletion-envelope]
  (send-lifecycle-frame! ctx replica-id deletion-envelope
                         "blob/deletion" "blob/delete" "deletion"))

(defn deletion-dispatcher
  "Return the callback shape consumed by `erasure-runtime/start!`."
  [ctx]
  (fn [{:keys [replica-id envelope]}]
    (send-deletion! ctx replica-id envelope)))

(defn send-deletion-receipt!
  "Return a locally signed deletion receipt to one authenticated controller."
  [ctx controller-id receipt-envelope]
  (send-lifecycle-frame! ctx controller-id receipt-envelope
                         "blob/deleted" "blob/deleted" "receipt"))

(defn deletion-receipt-dispatcher
  "Return the callback shape emitted by `lifecycle-runtime`.

  The controller Authority is recovered from the durable deletion record, not
  from mutable connection state."
  [ctx]
  (fn [{:keys [controller-id envelope]}]
    (send-deletion-receipt! ctx controller-id envelope)))

(defn states [ctx]
  {:sender @(:sender-state ctx)
   :receiver @(:receiver-state ctx)
   :fetches (reduce-kv (fn [result key entry]
                         (assoc result key (dissoc entry ::result-channel)))
                       {} @(:fetches ctx))})

(defn stats [ctx] @(:stats ctx))

(defn stop!
  "Stop the blob runtime and remove its extension handler. Idempotent."
  [ctx]
  (when (compare-and-set! (:stopped? ctx) false true)
    (when-let [cleanup @(:cleanup ctx)] (cleanup))
    (doseq [[_ entry] @(:fetches ctx)]
      (fail-fetch! ctx (:session entry) :stopped))
    (close! (:events ctx))
    true))
