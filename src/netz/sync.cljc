(ns netz.sync
  "Pure, transport-independent synchronization sessions.

  The machine transfers opaque profile chunks point-to-point with bounded
  acknowledged windows. Application state is staged until `sync/done`, then
  committed atomically. Its handler has the same `(state,event,ctx)` shape as
  the rest of Netz and runs unchanged in the deterministic simulator."
  (:require [netz.identity :as identity]
            [netz.sync.profile :as profile]
            [netz.wire :as wire]))

(def default-opts
  {:window 4
   :timeout-ms 15000
   :max-chunks 4096
   :max-sessions 64
   :max-frontier-streams 1024
   :max-frontier-bytes (* 256 1024)})

(def max-safe-integer 9007199254740991)

(defn- safe-uint? [value]
  (and (integer? value) (<= 0 value max-safe-integer)))

(defn- validated-receipt
  [state topic {:keys [frontiers] :as receipt}]
  (when (and (map? receipt)
             (contains? receipt :application)
             (map? frontiers)
             (<= (count frontiers) (get-in state [:opts :max-frontier-streams])))
    (try
      (let [encoded (wire/encode-frontiers topic frontiers)
            bytes (identity/buf-length encoded)]
        (when (<= bytes (get-in state [:opts :max-frontier-bytes]))
          (assoc receipt :frontier-bytes bytes)))
      (catch #?(:clj Throwable :cljs :default) _ nil))))

(defn- validated-claim
  "Validate and normalize an untrusted sender frontier before profile code.

  Profile-1 peers send the canonical array. Native Clojure transports still
  send the historical map until capability negotiation is deployed; that path
  receives the same strict origin/topic/integer/count/encoded-byte checks."
  [state topic frontiers]
  (try
    (let [frontiers (if (vector? frontiers)
                      (wire/frontiers-from-wire topic frontiers)
                      frontiers)]
      (when (and (map? frontiers)
                 (<= (count frontiers)
                     (get-in state [:opts :max-frontier-streams])))
        (let [bytes (identity/buf-length (wire/encode-frontiers topic frontiers))]
          (when (<= bytes (get-in state [:opts :max-frontier-bytes]))
            frontiers))))
    (catch #?(:clj Throwable :cljs :default) _ nil)))

(defn make-state
  ([id] (make-state id {}))
  ([id {:keys [profiles applications frontiers] :as opts}]
   {:id id
    :profiles (or profiles {})
    :applications (or applications {})
    :frontiers (or frontiers {})
    :sessions {}
    :next-session 0
    :status {}
    :opts (merge default-opts (dissoc opts :profiles :applications :frontiers))
    :stats {:opened 0 :completed 0 :aborted 0 :chunks-sent 0
            :chunks-applied 0 :duplicates 0}}))

(defn application [state topic]
  (get-in state [:applications topic]))

(defn status [state topic]
  (get-in state [:status topic] :idle))

(defn frontiers [state topic]
  (get-in state [:frontiers topic] {}))

(defn- record-application-commit
  [state {:keys [origin epoch seq topic]}]
  (let [stream [origin epoch topic]
        current (get-in state [:frontiers topic stream] -1)]
    (cond
      (not (and (some? origin) (some? topic)
                (safe-uint? epoch) (safe-uint? seq)))
      state

      (<= seq current)
      state

      (= seq (inc current))
      (assoc-in state [:frontiers topic stream] seq)

      :else
      (assoc-in state [:status topic] :desynchronized))))

(defn- profile-for [state profile-id]
  (get-in state [:profiles profile-id]))

(defn- timer-action [state session]
  [:timer (get-in state [:opts :timeout-ms])
   {:type :sync/timeout :session session}])

(defn start
  "Open a pull session to `peer`. Returns `[state actions]`."
  ([state peer topic profile-id]
   (start state peer topic profile-id [(:id state) (:next-session state)]))
  ([state peer topic profile-id session]
   (when-not (profile-for state profile-id)
     (throw (ex-info "Unknown synchronization profile"
                     {:type :netz.sync/unknown-profile :profile profile-id})))
   (when (>= (count (:sessions state)) (get-in state [:opts :max-sessions]))
     (throw (ex-info "Too many synchronization sessions"
                     {:type :netz.sync/session-limit})))
   (let [impl (profile-for state profile-id)
         value (application state topic)
         summary (profile/describe impl value topic)
         receiver {:role :receiver :peer peer :topic topic :profile profile-id
                   :next-index 0 :staging (profile/begin impl value topic)}]
     [(-> state
          (assoc-in [:sessions session] receiver)
          (assoc-in [:status topic] :synchronizing)
          (update :next-session inc)
          (update-in [:stats :opened] inc))
      [[:send peer {:type :sync/open :session session :topic topic
                    :profile profile-id :summary summary}]
       (timer-action state session)]])))

(defn- sender-actions [state session sender]
  (let [{:keys [peer chunks next-to-send acked checkpoint frontiers topic]} sender
        window (get-in state [:opts :window])
        through (min (count chunks) (+ (inc acked) window))
        indexes (range next-to-send through)
        sends (mapv (fn [index]
                      [:send peer {:type :sync/chunk :session session
                                   :index index :data (nth chunks index)}])
                    indexes)
        done? (and (= acked (dec (count chunks)))
                   (= next-to-send (count chunks)))]
    [(assoc sender :next-to-send through)
     (cond-> sends
       done? (conj [:send peer {:type :sync/done :session session
                                :count (count chunks) :topic topic
                                :checkpoint checkpoint :frontiers frontiers}]))]))

(defn- abort [state session peer code]
  [(-> state
       (update :sessions dissoc session)
       (update-in [:stats :aborted] inc))
   [[:send peer {:type :sync/abort :session session :code code}]]])

(defn- on-open [state from payload _ctx]
  (let [{:keys [session topic summary] profile-id :profile} payload
        impl (profile-for state profile-id)]
    (cond
      (nil? impl) (abort state session from :unknown-profile)
      (contains? (:sessions state) session) (abort state session from :duplicate-session)
      (>= (count (:sessions state)) (get-in state [:opts :max-sessions]))
      (abort state session from :session-limit)
      :else
      (try
        (let [value (application state topic)
              {:keys [chunks checkpoint]} (profile/respond impl value topic summary)
              chunks (vec chunks)]
          (if (> (count chunks) (get-in state [:opts :max-chunks]))
            (abort state session from :too-many-chunks)
            (let [sender {:role :sender :peer from :topic topic :profile profile-id
                          :chunks chunks :checkpoint checkpoint
                          :frontiers (frontiers state topic)
                          :acked -1 :next-to-send 0}
                  [sender actions] (sender-actions state session sender)
                  done? (some #(= :sync/done (:type (nth % 2 nil))) actions)
                  state (-> state
                            (assoc-in [:sessions session] sender)
                            (update-in [:stats :chunks-sent]
                                       + (count (filter #(= :sync/chunk
                                                            (:type (nth % 2 nil)))
                                                        actions))))]
              [(cond-> state done? (update :sessions dissoc session))
               (cond-> actions
                 (seq chunks) (conj (timer-action state session)))])))
        (catch #?(:clj Throwable :cljs :default) _
          (abort state session from :profile-rejected))))))

(defn- on-chunk [state from {:keys [session index data]}]
  (let [{:keys [role peer topic profile next-index staging] :as receiver}
        (get-in state [:sessions session])]
    (cond
      (or (not= role :receiver) (not= peer from))
      (abort state session from :unknown-session)

      (< index next-index)
      [(update-in state [:stats :duplicates] inc)
       [[:send from {:type :sync/ack :session session
                     :through (dec next-index)}]]]

      (> index next-index)
      (abort state session from :out-of-order-chunk)

      :else
      (try
        (let [impl (profile-for state profile)
              staging (profile/apply-chunk impl staging topic data)]
          [(-> state
               (assoc-in [:sessions session]
                         (assoc receiver :staging staging :next-index (inc index)))
               (update-in [:stats :chunks-applied] inc))
           [[:send from {:type :sync/ack :session session :through index}]]])
        (catch #?(:clj Throwable :cljs :default) _
          (abort state session from :profile-rejected))))))

(defn- on-ack [state from {:keys [session through]}]
  (let [{:keys [role peer] :as sender} (get-in state [:sessions session])]
    (if (or (not= role :sender) (not= peer from))
      (abort state session from :unknown-session)
      (let [sender (assoc sender :acked (max (:acked sender) through))
            [sender actions] (sender-actions state session sender)
            done? (some #(= :sync/done (:type (nth % 2 nil))) actions)
            state (-> state
                      (assoc-in [:sessions session] sender)
                      (update-in [:stats :chunks-sent]
                                 + (count (filter #(= :sync/chunk
                                                      (:type (nth % 2 nil)))
                                                  actions))))]
        [(cond-> state done? (update :sessions dissoc session)) actions]))))

(defn- on-done [state from {:keys [session count checkpoint frontiers]}]
  (let [{:keys [role peer topic profile next-index staging]}
        (get-in state [:sessions session])]
    (cond
      (or (not= role :receiver) (not= peer from))
      (abort state session from :unknown-session)
      (not= count next-index)
      (abort state session from :incomplete)
      :else
      (try
        (let [frontiers (validated-claim state topic frontiers)
              _ (when-not frontiers
                  (throw (ex-info "invalid sender frontier claim" {})))
              impl (profile-for state profile)
              current (application state topic)
              receipt (validated-receipt
                       state topic
                       (profile/commit-result
                        impl current topic staging checkpoint frontiers))]
          (if-not receipt
            (abort state session from :invalid-committed-frontiers)
            [(-> state
                 (assoc-in [:applications topic] (:application receipt))
                 (update-in [:frontiers topic] #(merge-with max (or % {})
                                                            (:frontiers receipt)))
                 (assoc-in [:status topic] :synchronized)
                 (update :sessions dissoc session)
                 (update-in [:stats :completed] inc))
             [[:sync-committed (:frontiers receipt)]]]))
        (catch #?(:clj Throwable :cljs :default) _
          (abort state session from :commit-rejected))))))

(defn handler [state event ctx]
  (case (:type event)
    :init {:state state :actions []}
    :timer (let [{:keys [type session]} (:payload event)]
             (if (and (= type :sync/timeout) (contains? (:sessions state) session))
               (let [{:keys [role peer topic]} (get-in state [:sessions session])]
                 {:state (-> state
                             (update :sessions dissoc session)
                             (cond-> (= role :receiver)
                               (assoc-in [:status topic] :desynchronized))
                             (update-in [:stats :aborted] inc))
                  :actions [[:send peer {:type :sync/abort :session session
                                         :code :timeout}]]})
               {:state state :actions []}))
    :message
    (let [from (:from event) payload (:payload event)]
      (if (= from :app)
        (case (:type payload)
          :sync/start
          (let [[state actions] (if-let [session (:session payload)]
                                  (start state (:peer payload) (:topic payload)
                                         (:profile payload) session)
                                  (start state (:peer payload) (:topic payload)
                                         (:profile payload)))]
            {:state state :actions actions})

          :application-committed
          {:state (record-application-commit state (:publication payload))
           :actions []}

          {:state state :actions []})
        (let [[state actions]
              (case (:type payload)
                :sync/open (on-open state from payload ctx)
                :sync/chunk (on-chunk state from payload)
                :sync/ack (on-ack state from payload)
                :sync/done (on-done state from payload)
                :sync/abort (let [{:keys [role topic]}
                                  (get-in state [:sessions (:session payload)])]
                              [(-> state
                                   (update :sessions dissoc (:session payload))
                                   (cond-> (= role :receiver)
                                     (assoc-in [:status topic] :desynchronized))
                                   (update-in [:stats :aborted] inc)) []])
                [state []])]
          {:state state :actions actions})))
    {:state state :actions []}))
