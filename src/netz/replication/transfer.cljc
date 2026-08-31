(ns netz.replication.transfer
  "Pure receiver for resumable durable-blob transfers.

  Cryptographic verification and IO are explicit boundaries. A runtime feeds
  `:transfer/authorized-offer` only after verifying the retention envelope and
  authenticated connection *and* durably accepting the retention statement in
  the lifecycle machine. The machine emits load, stage, install and sign
  actions, and advances only when their completion events arrive. It never
  treats network delivery as persistence."
  (:require [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]))

(def default-opts
  {:max-sessions 16
   :max-object-bytes (* 1024 1024 1024)
   :timeout-ms 30000
   :persistence "netz/store/fsync/1"
   :accepted-persistence #{"netz/store/fsync/1"}})

(defn make-state
  ([authority] (make-state authority {}))
  ([authority opts]
   (when-not (and (wire/byte-buffer? authority)
                  (= 32 (identity/buf-length authority)))
     (throw (ex-info "Transfer receiver requires a full Authority ID"
                     {:type :netz.replication/invalid-receiver-authority})))
   (let [opts (merge default-opts opts)]
     (when-not (and (int? (:max-sessions opts)) (pos? (:max-sessions opts))
                    (int? (:max-object-bytes opts))
                    (pos? (:max-object-bytes opts))
                    (int? (:timeout-ms opts)) (pos? (:timeout-ms opts))
                    (contains? replication-wire/persistence-profiles
                               (:persistence opts))
                    (set? (:accepted-persistence opts))
                    (seq (:accepted-persistence opts))
                    (every? replication-wire/persistence-profiles
                            (:accepted-persistence opts)))
       (throw (ex-info "Invalid transfer receiver options"
                       {:type :netz.replication/invalid-transfer-options})))
     {:authority authority
      :sessions {}
      :opts opts
      :stats {:offered 0 :resumed 0 :pieces-staged 0 :duplicates 0
              :installed 0 :receipts 0 :aborted 0 :rejected 0}})))

(defn- session-key [session]
  (when-not (and (wire/byte-buffer? session)
                 (= 16 (identity/buf-length session)))
    (throw (ex-info "Invalid transfer session"
                    {:type :netz.replication/invalid-session})))
  (identity/bytes->hex session))

(defn- same-bytes? [a b]
  (and (wire/byte-buffer? a) (wire/byte-buffer? b)
       (identity/bufs= a b)))

(defn- statement= [a b]
  (identity/bufs= (wire/encode-canonical a) (wire/encode-canonical b)))

(defn- timer-action [state session]
  [:timer (get-in state [:opts :timeout-ms])
   {:type :transfer/timeout :session session}])

(defn- abort
  ([state session code] (abort state session code true))
  ([state session code notify?]
   (let [key (session-key session)
         {:keys [peer manifest-id]} (get-in state [:sessions key])]
     {:state (-> state
                 (update :sessions dissoc key)
                 (update-in [:stats :aborted] inc))
      :actions (cond-> []
                 (and notify? peer)
                 (conj [:send peer
                        {"v" 1 "type" "blob/abort"
                         "session" session "code" (name code)}])
                 manifest-id
                 (conj [:cleanup-staging session manifest-id]))})))

(defn- retention-of [offer]
  (:statement (replication-wire/parse-envelope (get offer "retention"))))

(defn- indexes-by-piece [manifest]
  (reduce-kv (fn [result index address]
               (update result (identity/bytes->hex address) (fnil conj #{}) index))
             {} (get manifest "pieces")))

(defn- receipt-body [state session-entry persistence stored-at]
  (let [retention (:retention session-entry)]
    {"v" 1
     "type" "blob/stored"
     "cluster" (get retention "cluster")
     "privacy_domain" (get retention "privacy_domain")
     "retention_revision" (get retention "revision")
     "manifest" (get retention "manifest")
     "object" (get retention "object")
     "layout_epoch" (get retention "layout_epoch")
     "replica" (:authority state)
     "attempt" (:attempt session-entry)
     "persistence" persistence
     "stored_at" stored-at
     "retain_until" (get retention "retain_until")
     "signer" (:authority state)}))

(defn- begin-receipt [state key persistence stored-at now]
  (let [entry (get-in state [:sessions key])
        accepted (get-in state [:opts :accepted-persistence])
        retain-until (get-in entry [:retention "retain_until"])]
    (cond
      (not (contains? accepted persistence))
      (abort state (:session entry) :unacceptable-persistence)

      (not (and (integer? stored-at)
                (<= 0 stored-at retain-until wire/max-safe-integer)))
      (abort state (:session entry) :invalid-stored-at)

      (not (< now retain-until))
      (abort state (:session entry) :lease-expired)

      :else
      (let [receipt (receipt-body state entry persistence stored-at)]
        {:state (assoc-in state [:sessions key]
                          (assoc entry :phase :signing-receipt
                                 :receipt receipt))
         :actions [[:sign-receipt (:session entry) receipt]]}))))

(defn- authorized-offer [state {:keys [from frame]} now]
  (try
    (replication-wire/validate-frame frame)
    (let [session (get frame "session")
          key (session-key session)
          manifest (get frame "manifest")
          retention (retention-of frame)
          length (get manifest "length")]
      (cond
        (not= "blob/offer" (get frame "type"))
        {:state (update-in state [:stats :rejected] inc) :actions []}

        (not (same-bytes? (:authority state) (get frame "replica")))
        {:state (update-in state [:stats :rejected] inc) :actions []}

        (> length (get-in state [:opts :max-object-bytes]))
        {:state (update-in state [:stats :rejected] inc) :actions []}

        (<= (get retention "retain_until") now)
        {:state (update-in state [:stats :rejected] inc) :actions []}

        (contains? (:sessions state) key)
        (abort state session :duplicate-session)

        (>= (count (:sessions state)) (get-in state [:opts :max-sessions]))
        {:state (update-in state [:stats :rejected] inc) :actions []}

        :else
        (let [entry {:phase :loading
                     :peer from
                     :session session
                     :attempt (get frame "attempt")
                     :manifest manifest
                     :indexes-by-piece (indexes-by-piece manifest)
                     :manifest-id (get retention "manifest")
                     :retention retention}]
          {:state (-> state
                      (assoc-in [:sessions key] entry)
                      (update-in [:stats :offered] inc))
           :actions [[:load-staging session (:manifest-id entry) manifest]
                     (timer-action state session)]})))
    (catch #?(:clj Throwable :cljs :default) _
      {:state (update-in state [:stats :rejected] inc) :actions []})))

(defn- held-inventory [indexes-by-piece held]
  (let [held-ids (set (map (fn [address]
                             (when-not (and (wire/byte-buffer? address)
                                            (= 32 (identity/buf-length address)))
                               (throw (ex-info "Invalid staged piece address" {})))
                             (identity/bytes->hex address))
                           held))]
    (when-not (every? indexes-by-piece held-ids)
      (throw (ex-info "Staging contains a piece outside the manifest" {})))
    {:addresses held-ids
     :indices (into #{} (mapcat indexes-by-piece) held-ids)}))

(defn- staging-loaded
  [state {:keys [session held installed]} now]
  (let [key (session-key session)
        {:keys [phase manifest manifest-id peer indexes-by-piece] :as entry}
        (get-in state [:sessions key])]
    (if-not (= :loading phase)
      (abort state session :unexpected-staging-result)
      (try
        (let [{held :indices held-addresses :addresses}
              (held-inventory indexes-by-piece (or held []))
              needed (set (remove held (range (count (get manifest "pieces")))))]
          (if installed
            (if (and (same-bytes? manifest-id (:manifest installed))
                     (same-bytes? (get manifest "object") (:object installed)))
              (-> (begin-receipt state key (:persistence installed)
                                 (:stored-at installed) now)
                  (update :state update-in [:stats :resumed] inc))
              (abort state session :installed-object-mismatch))
            {:state (-> state
                        (assoc-in [:sessions key]
                                  (assoc entry :phase :receiving
                                         :held held :needed needed
                                         :pending nil))
                        (cond-> (seq held) (update-in [:stats :resumed] inc)))
             :actions [[:send peer
                        {"v" 1 "type" "blob/need" "session" session
                         "manifest" manifest-id
                         "need" (blob/need-bitmap
                                 manifest
                                 (fn [address]
                                   (contains? held-addresses
                                              (identity/bytes->hex address))))}]]}))
        (catch #?(:clj Throwable :cljs :default) _
          (abort state session :invalid-staging-inventory))))))

(defn- contiguous-through [held piece-count]
  (loop [index 0]
    (if (and (< index piece-count) (contains? held index))
      (recur (inc index))
      (dec index))))

(defn- piece [state {:keys [from frame]}]
  (try
    (replication-wire/validate-frame frame)
    (let [session (get frame "session")
          key (session-key session)
          {:keys [phase peer manifest manifest-id held needed pending
                  indexes-by-piece] :as entry}
          (get-in state [:sessions key])
          index (get frame "index")
          data (get frame "data")]
      (cond
        (not= "blob/piece" (get frame "type")) (abort state session :wrong-frame)
        (or (not= :receiving phase) (not= peer from))
        (abort state session :unknown-session)
        (not (same-bytes? manifest-id (get frame "manifest")))
        (abort state session :manifest-mismatch)
        (contains? held index)
        (let [through (contiguous-through held (count (get manifest "pieces")))]
          {:state (update-in state [:stats :duplicates] inc)
           :actions (if (neg? through) []
                        [[:send peer
                          {"v" 1 "type" "blob/ack" "session" session
                           "manifest" manifest-id "through" through}]])})
        pending (abort state session :piece-before-stage)
        (not (contains? needed index)) (abort state session :unrequested-piece)
        (not (blob/verify-piece? manifest index data))
        (abort state session :invalid-piece)
        :else
        {:state (assoc-in state [:sessions key]
                          (assoc entry :pending
                                 {:index index
                                  :indices (get indexes-by-piece
                                                (identity/bytes->hex
                                                 (nth (get manifest "pieces")
                                                      index)))}))
         :actions [[:stage-piece session manifest-id index
                    (nth (get manifest "pieces") index) data]]}))
    (catch #?(:clj Throwable :cljs :default) _
      {:state (update-in state [:stats :rejected] inc) :actions []})))

(defn- piece-staged [state {:keys [session index result]}]
  (let [key (session-key session)
        {:keys [phase peer manifest manifest-id pending] :as entry}
        (get-in state [:sessions key])]
    (cond
      (or (not= :receiving phase) (not= (:index pending) index))
      (abort state session :unexpected-stage-result)
      (not= :ok result) (abort state session :stage-failed)
      :else
      (let [entry (-> entry
                      (update :held into (:indices pending))
                      (update :needed #(apply disj % (:indices pending)))
                      (assoc :pending nil))
            through (contiguous-through (:held entry)
                                        (count (get manifest "pieces")))]
        {:state (-> state
                    (assoc-in [:sessions key] entry)
                    (update-in [:stats :pieces-staged] inc))
         :actions (if (neg? through) []
                      [[:send peer
                        {"v" 1 "type" "blob/ack" "session" session
                         "manifest" manifest-id "through" through}]])}))))

(defn- commit [state {:keys [from frame]}]
  (try
    (replication-wire/validate-frame frame)
    (let [session (get frame "session")
          key (session-key session)
          {:keys [phase peer manifest manifest-id needed pending retention] :as entry}
          (get-in state [:sessions key])]
      (cond
        (not= "blob/commit" (get frame "type")) (abort state session :wrong-frame)
        (or (not= :receiving phase) (not= peer from))
        (abort state session :unknown-session)
        (not (same-bytes? manifest-id (get frame "manifest")))
        (abort state session :manifest-mismatch)
        (or pending (seq needed)
            (not= (get frame "count") (count (get manifest "pieces"))))
        (abort state session :incomplete)
        :else
        {:state (assoc-in state [:sessions key] (assoc entry :phase :installing))
         :actions [[:install-object session manifest-id manifest retention
                    (get-in state [:opts :persistence])]]}))
    (catch #?(:clj Throwable :cljs :default) _
      {:state (update-in state [:stats :rejected] inc) :actions []})))

(defn- installed [state {:keys [session result persistence stored-at]} now]
  (let [key (session-key session)
        entry (get-in state [:sessions key])]
    (cond
      (not= :installing (:phase entry))
      (abort state session :unexpected-install-result)
      (not= :ok result) (abort state session :install-failed)
      :else (-> (begin-receipt state key persistence stored-at now)
                (update :state update-in [:stats :installed] inc)))))

(defn- receipt-signed [state {:keys [session envelope]}]
  (let [key (session-key session)
        {:keys [phase peer manifest-id receipt]} (get-in state [:sessions key])]
    (try
      (let [actual (:statement (replication-wire/parse-envelope envelope))]
        (if-not (and (= :signing-receipt phase) (statement= receipt actual))
          (abort state session :invalid-receipt)
          {:state (-> state
                      (update :sessions dissoc key)
                      (update-in [:stats :receipts] inc))
           :actions [[:send peer
                      {"v" 1 "type" "blob/stored" "session" session
                       "receipt" envelope}]
                     [:cleanup-staging session manifest-id]]}))
      (catch #?(:clj Throwable :cljs :default) _
        (abort state session :invalid-receipt)))))

(defn handler
  "Advance the receiver machine using `ctx.now` for lease-expiry checks."
  [state event ctx]
  (let [now (or (:now ctx) 0)]
    (case (:type event)
      :init {:state state :actions []}
      :transfer/authorized-offer (authorized-offer state event now)
      :transfer/staging-loaded (staging-loaded state event now)
      :transfer/piece (piece state event)
      :transfer/piece-staged (piece-staged state event)
      :transfer/commit (commit state event)
      :transfer/installed (installed state event now)
      :transfer/receipt-signed (receipt-signed state event)
      :timer (if (= :transfer/timeout (get-in event [:payload :type]))
               (if (contains? (:sessions state)
                              (session-key (get-in event [:payload :session])))
                 (abort state (get-in event [:payload :session]) :timeout)
                 {:state state :actions []})
               {:state state :actions []})
      :transfer/timeout (if (contains? (:sessions state)
                                       (session-key (:session event)))
                          (abort state (:session event) :timeout)
                          {:state state :actions []})
      :disconnected
      (let [sessions (filter (fn [[_ entry]] (= (:peer event) (:peer entry)))
                             (:sessions state))]
        {:state (-> state
                    (update :sessions #(apply dissoc % (map first sessions)))
                    (update-in [:stats :aborted] + (count sessions)))
         :actions (mapv (fn [[_ {:keys [session manifest-id]}]]
                          [:cleanup-staging session manifest-id])
                        sessions)})
      {:state state :actions []})))
