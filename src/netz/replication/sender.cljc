(ns netz.replication.sender
  "Pure bounded sender for resumable durable-blob transfers.

  The machine starts only after the replication controller has durably stored
  its outgoing intent. Transport authentication, retention signing and receipt
  signature verification remain explicit runtime boundaries. In particular,
  a `:transfer/verified-stored` event means that the receipt signature and the
  authenticated remote Authority ID have already been checked.

  The sender keeps at most one piece in flight. This matches the receiver's
  persistence backpressure: another piece is not read until the previous one
  has produced a cumulative ACK. Piece bytes appear in an action only and are
  never retained in machine state. A verified receipt must also cross the
  `:persist-receipt` boundary before it is released to the quorum controller."
  (:require [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]))

(def default-opts
  {:max-sessions 16
   :timeout-ms 30000
   :accepted-persistence #{"netz/store/fsync/1"}})

(defn make-state
  ([] (make-state {}))
  ([opts]
   (let [opts (merge default-opts opts)]
     (when-not (and (int? (:max-sessions opts))
                    (pos? (:max-sessions opts))
                    (int? (:timeout-ms opts))
                    (pos? (:timeout-ms opts))
                    (set? (:accepted-persistence opts))
                    (seq (:accepted-persistence opts))
                    (every? replication-wire/persistence-profiles
                            (:accepted-persistence opts)))
       (throw (ex-info "Invalid transfer sender options"
                       {:type :netz.replication/invalid-sender-options})))
     {:sessions {}
      :opts opts
      :stats {:started 0
              :pieces-sent 0
              :acks 0
              :receipts 0
              :duplicates 0
              :aborted 0
              :rejected 0
              :timeouts 0}})))

(defn- same-bytes? [a b]
  (and (wire/byte-buffer? a)
       (wire/byte-buffer? b)
       (identity/bufs= a b)))

(defn- same-value? [a b]
  (identity/bufs= (wire/encode-canonical a) (wire/encode-canonical b)))

(defn- session-key [session]
  (when-not (and (wire/byte-buffer? session)
                 (= 16 (identity/buf-length session)))
    (throw (ex-info "Invalid transfer session"
                    {:type :netz.replication/invalid-session})))
  (identity/bytes->hex session))

(defn- retention-of [offer]
  (:statement (replication-wire/parse-envelope (get offer "retention"))))

(defn- timer-action [state session]
  [:timer (get-in state [:opts :timeout-ms])
   {:type :transfer/sender-timeout :session session}])

(defn- reject [state]
  {:state (update-in state [:stats :rejected] inc)
   :actions []})

(defn- failure [entry code]
  {:session (:session entry)
   :object-id (identity/bytes->hex (get-in entry [:manifest "object"]))
   :manifest-id (identity/bytes->hex (:manifest-id entry))
   :replica-id (identity/bytes->hex (:authority entry))
   :layout-epoch (get-in entry [:retention "layout_epoch"])
   :attempt (:attempt entry)
   :code code})

(defn- abort
  ([state session code] (abort state session code true))
  ([state session code notify?]
   (let [key (session-key session)
         entry (get-in state [:sessions key])]
     (if-not entry
       (reject state)
       {:state (-> state
                   (update :sessions dissoc key)
                   (update-in [:stats :aborted] inc))
        :actions (cond-> []
                   (and notify? (:peer entry))
                   (conj [:send (:peer entry)
                          {"v" 1 "type" "blob/abort"
                           "session" session "code" (name code)}])
                   true
                   (conj [:transfer-failed (failure entry code)]))}))))

(defn- connection-matches? [entry from from-authority]
  (and (= (:peer entry) from)
       (same-bytes? (:authority entry) from-authority)))

(defn- load-action [entry index]
  [:load-piece (:session entry) (:manifest-id entry) index
   (nth (get-in entry [:manifest "pieces"]) index)])

(defn- commit-action [entry]
  [:send (:peer entry)
   {"v" 1
    "type" "blob/commit"
    "session" (:session entry)
    "manifest" (:manifest-id entry)
    "count" (count (get-in entry [:manifest "pieces"]))}])

(defn- begin-transfer [state {:keys [peer authority frame]} now]
  (try
    (replication-wire/validate-frame frame)
    (let [session (get frame "session")
          key (session-key session)
          retention (retention-of frame)]
      (cond
        (not= "blob/offer" (get frame "type"))
        (reject state)

        (not (and (some? peer)
                  (wire/byte-buffer? authority)
                  (= 32 (identity/buf-length authority))
                  (same-bytes? authority (get frame "replica"))))
        (reject state)

        (<= (get retention "retain_until") now)
        (reject state)

        (contains? (:sessions state) key)
        (abort state session :duplicate-session)

        (>= (count (:sessions state)) (get-in state [:opts :max-sessions]))
        (reject state)

        :else
        (let [entry {:phase :awaiting-need
                     :peer peer
                     :authority authority
                     :session session
                     :attempt (get frame "attempt")
                     :manifest (get frame "manifest")
                     :manifest-id (get retention "manifest")
                     :retention retention}]
          {:state (-> state
                      (assoc-in [:sessions key] entry)
                      (update-in [:stats :started] inc))
           :actions [[:send peer frame]
                     (timer-action state session)]})))
    (catch #?(:clj Throwable :cljs :default) _
      (reject state))))

(defn- need [state {:keys [from from-authority frame]}]
  (try
    (replication-wire/validate-frame frame)
    (let [session (get frame "session")
          key (session-key session)
          {:keys [phase manifest manifest-id] :as entry}
          (get-in state [:sessions key])]
      (cond
        (not= "blob/need" (get frame "type"))
        (if entry (abort state session :wrong-frame) (reject state))

        (not entry)
        (reject state)

        (not (connection-matches? entry from from-authority))
        (abort state session :connection-mismatch)

        (not (same-bytes? manifest-id (get frame "manifest")))
        (abort state session :manifest-mismatch)

        (not= :awaiting-need phase)
        (if (and (:need entry) (same-bytes? (:need entry) (get frame "need")))
          {:state (update-in state [:stats :duplicates] inc) :actions []}
          (abort state session :unexpected-need))

        :else
        (let [indices (blob/needed-indices manifest (get frame "need"))
              entry (assoc entry :need (get frame "need") :remaining indices)]
          (if-let [index (first indices)]
            {:state (assoc-in state [:sessions key]
                              (assoc entry :phase :loading-piece
                                     :current index))
             :actions [(load-action entry index)]}
            {:state (assoc-in state [:sessions key]
                              (assoc entry :phase :awaiting-receipt))
             :actions [(commit-action entry)]}))))
    (catch #?(:clj Throwable :cljs :default) _
      (reject state))))

(defn- piece-loaded [state {:keys [session index result data]}]
  (try
    (let [key (session-key session)
          {:keys [phase current manifest manifest-id peer] :as entry}
          (get-in state [:sessions key])]
      (cond
        (not entry)
        (reject state)

        (or (not= :loading-piece phase) (not= current index))
        (abort state session :unexpected-piece-result)

        (not= :ok result)
        (abort state session :piece-load-failed)

        (not (blob/verify-piece? manifest index data))
        (abort state session :invalid-local-piece)

        :else
        {:state (-> state
                    (assoc-in [:sessions key] (assoc entry :phase :awaiting-ack))
                    (update-in [:stats :pieces-sent] inc))
         :actions [[:send peer
                    {"v" 1 "type" "blob/piece" "session" session
                     "manifest" manifest-id "index" index "data" data}]]}))
    (catch #?(:clj Throwable :cljs :default) _
      (reject state))))

(defn- ack [state {:keys [from from-authority frame]}]
  (try
    (replication-wire/validate-frame frame)
    (let [session (get frame "session")
          key (session-key session)
          {:keys [phase current manifest manifest-id remaining] :as entry}
          (get-in state [:sessions key])
          through (get frame "through")]
      (cond
        (not= "blob/ack" (get frame "type"))
        (if entry (abort state session :wrong-frame) (reject state))

        (not entry)
        (reject state)

        (not (connection-matches? entry from from-authority))
        (abort state session :connection-mismatch)

        (not (same-bytes? manifest-id (get frame "manifest")))
        (abort state session :manifest-mismatch)

        (>= through (count (get manifest "pieces")))
        (abort state session :invalid-ack)

        (<= through (or (:acked-through entry) -1))
        {:state (update-in state [:stats :duplicates] inc) :actions []}

        (and (= :awaiting-ack phase) (< through current))
        {:state (update-in state [:stats :duplicates] inc) :actions []}

        (not= :awaiting-ack phase)
        (abort state session :unexpected-ack)

        :else
        (let [remaining (filterv #(< through %) remaining)
              entry (assoc entry
                           :remaining remaining
                           :acked-through through)]
          (if-let [index (first remaining)]
            {:state (-> state
                        (assoc-in [:sessions key]
                                  (assoc entry :phase :loading-piece
                                         :current index))
                        (update-in [:stats :acks] inc))
             :actions [(load-action entry index)]}
            {:state (-> state
                        (assoc-in [:sessions key]
                                  (assoc entry :phase :awaiting-receipt
                                         :current nil))
                        (update-in [:stats :acks] inc))
             :actions [(commit-action entry)]}))))
    (catch #?(:clj Throwable :cljs :default) _
      (reject state))))

(defn- receipt-matches? [state entry statement]
  (let [retention (:retention entry)]
    (and (= "blob/stored" (get statement "type"))
         (same-bytes? (:authority entry) (get statement "replica"))
         (same-bytes? (:authority entry) (get statement "signer"))
         (same-bytes? (get retention "cluster") (get statement "cluster"))
         (same-bytes? (get retention "privacy_domain")
                      (get statement "privacy_domain"))
         (= (get retention "revision")
            (get statement "retention_revision"))
         (same-bytes? (:manifest-id entry) (get statement "manifest"))
         (same-bytes? (get-in entry [:manifest "object"])
                      (get statement "object"))
         (= (get retention "layout_epoch") (get statement "layout_epoch"))
         (= (:attempt entry) (get statement "attempt"))
         (= (get retention "retain_until") (get statement "retain_until"))
         (contains? (get-in state [:opts :accepted-persistence])
                    (get statement "persistence")))))

(defn- controller-event [entry envelope statement]
  {:type :replication/stored
   :object-id (identity/bytes->hex (get statement "object"))
   :replica-id (identity/bytes->hex (get statement "replica"))
   :layout-epoch (get statement "layout_epoch")
   :attempt (get statement "attempt")
   :persistence (get statement "persistence")
   :receipt envelope
   :manifest-id (identity/bytes->hex (:manifest-id entry))})

(defn- verified-stored
  [state {:keys [from from-authority frame]}]
  (try
    (replication-wire/validate-frame frame)
    (let [session (get frame "session")
          key (session-key session)
          {:keys [phase] :as entry} (get-in state [:sessions key])
          envelope (get frame "receipt")
          statement (:statement (replication-wire/parse-envelope envelope))]
      (cond
        (not= "blob/stored" (get frame "type"))
        (if entry (abort state session :wrong-frame) (reject state))

        (not entry)
        (reject state)

        (not (connection-matches? entry from from-authority))
        (abort state session :connection-mismatch)

        ;; An already installed replica can answer the offer with a fresh
        ;; receipt directly, without requesting pieces or a commit. The same
        ;; signature and exact intent-binding checks below still apply.
        (not (#{:awaiting-need :awaiting-receipt} phase))
        (if (and (= :persisting-receipt phase)
                 (same-value? envelope
                              (get-in entry [:controller-event :receipt])))
          {:state (update-in state [:stats :duplicates] inc) :actions []}
          (abort state session :unexpected-receipt))

        (not (receipt-matches? state entry statement))
        (abort state session :receipt-mismatch)

        :else
        (let [event (controller-event entry envelope statement)]
          {:state (assoc-in state [:sessions key]
                            (assoc entry :phase :persisting-receipt
                                   :controller-event event))
           :actions [[:persist-receipt session event]]})))
    (catch #?(:clj Throwable :cljs :default) _
      (reject state))))

(defn- receipt-persisted [state {:keys [session result]}]
  (try
    (let [key (session-key session)
          {:keys [phase controller-event] :as entry}
          (get-in state [:sessions key])]
      (cond
        (not entry)
        (reject state)

        (not= :persisting-receipt phase)
        (abort state session :unexpected-receipt-result)

        (not= :ok result)
        (abort state session :receipt-persist-failed false)

        :else
        {:state (-> state
                    (update :sessions dissoc key)
                    (update-in [:stats :receipts] inc))
         :actions [[:replication-stored controller-event]]}))
    (catch #?(:clj Throwable :cljs :default) _
      (reject state))))

(defn- remote-abort [state {:keys [from from-authority frame]}]
  (try
    (replication-wire/validate-frame frame)
    (let [session (get frame "session")
          key (session-key session)
          entry (get-in state [:sessions key])]
      (cond
        (not= "blob/abort" (get frame "type"))
        (if entry (abort state session :wrong-frame) (reject state))

        (not entry)
        (reject state)

        (not (connection-matches? entry from from-authority))
        (abort state session :connection-mismatch)

        :else
        (abort state session (get frame "code") false)))
    (catch #?(:clj Throwable :cljs :default) _
      (reject state))))

(defn- timeout [state session]
  (try
    (let [key (session-key session)]
      (if (contains? (:sessions state) key)
        (-> (abort state session :timeout)
            (update :state update-in [:stats :timeouts] inc))
        {:state state :actions []}))
    (catch #?(:clj Throwable :cljs :default) _
      (reject state))))

(defn- disconnected [state peer]
  (let [sessions (filter (fn [[_ entry]] (= peer (:peer entry)))
                         (:sessions state))]
    {:state (-> state
                (update :sessions #(apply dissoc % (map first sessions)))
                (update-in [:stats :aborted] + (count sessions)))
     :actions (mapv (fn [[_ entry]]
                      [:transfer-failed (failure entry :disconnected)])
                    sessions)}))

(defn handler
  "Advance the sender using `ctx.now` for lease-expiry checks.

  Inbound events must carry both the opaque transport peer in `:from` and its
  authenticated full Authority ID in `:from-authority`. Only a runtime that has
  cryptographically verified a signed receipt may emit
  `:transfer/verified-stored`."
  [state event ctx]
  (let [now (or (:now ctx) 0)]
    (case (:type event)
      :init {:state state :actions []}
      :transfer/start (begin-transfer state event now)
      :transfer/need (need state event)
      :transfer/piece-loaded (piece-loaded state event)
      :transfer/ack (ack state event)
      :transfer/verified-stored (verified-stored state event)
      :transfer/receipt-persisted (receipt-persisted state event)
      :transfer/abort (remote-abort state event)
      :timer (if (= :transfer/sender-timeout (get-in event [:payload :type]))
               (timeout state (get-in event [:payload :session]))
               {:state state :actions []})
      :transfer/sender-timeout (timeout state (:session event))
      :disconnected (disconnected state (:peer event))
      {:state state :actions []})))
