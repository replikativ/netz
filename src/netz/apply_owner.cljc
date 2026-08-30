(ns netz.apply-owner
  "Pure ownership boundary between network delivery and profile mutation.

  Transport receipt, ordered release, and successful application are distinct
  facts. This machine advances a publication frontier only after the profile
  reports literal `{:ok true}`, and it never continues applying on a gapped or
  failed state. Runtimes execute the returned effects and feed results back as
  events; the same transitions are therefore usable in deterministic tests.")

(def default-opts
  {:max-queued-items 1024
   :max-queued-bytes (* 8 1024 1024)})

(defn make-state
  [{:keys [topic profile generation live-transport phase applied-frontiers opts]
    :or {live-transport :overlay phase :joining}}]
  {:topic topic
   :profile profile
   :generation generation
   :live-transport live-transport
   :phase phase
   :queue []
   :queued-items 0
   :queued-bytes 0
   :in-flight nil
   :sync-begin-pending? false
   :next-token 0
   :applied-frontiers (or applied-frontiers {})
   :sync-requested? false
   :ready? (= phase :live)
   :opts (merge default-opts opts)})

(defn stream-key
  [{:keys [origin epoch topic]}]
  (when (and (some? origin) (some? epoch) (some? topic))
    [origin epoch topic]))

(defn publication-id
  [{:keys [seq] :as publication}]
  (when-let [stream (stream-key publication)]
    (when (int? seq)
      (conj stream seq))))

(defn- publication-bytes [publication]
  (max 0 (or (:bytes publication) 0)))

(defn- expected-seq [state publication]
  (inc (get (:applied-frontiers state) (stream-key publication) -1)))

(defn- applied? [state publication]
  (when-let [stream (stream-key publication)]
    (<= (:seq publication) (get (:applied-frontiers state) stream -1))))

(defn- matching-entry? [entry publication]
  (let [id (publication-id publication)]
    (and id (= id (publication-id (:publication entry))))))

(defn- settle-effects [entry result]
  (mapv (fn [receipt] {:op :settle :receipt receipt :result result})
        (:receipts entry)))

(defn- request-sync
  [state source reason]
  (if (:sync-requested? state)
    [state []]
    [(assoc state :sync-requested? true)
     [{:op :request-sync :source source :topic (:topic state) :reason reason}]]))

(defn- desynchronize
  [state source reason transport]
  (let [[state effects] (request-sync
                         (assoc state :phase :desynchronized :ready? false)
                         source reason)]
    [state (cond-> effects
             (= transport :direct)
             (conj {:op :retire-direct :topic (:topic state) :reason reason}))]))

(defn- eligible?
  [state entry]
  (let [publication (:publication entry)]
    (or (nil? (stream-key publication))
        (= (:seq publication) (expected-seq state publication)))))

(defn- remove-queue-index [queue index]
  (into (subvec queue 0 index) (subvec queue (inc index))))

(declare advance)
(declare on-sync-commit)

(defn- start-entry
  [state index]
  (let [entry (nth (:queue state) index)
        token (:next-token state)
        bytes (publication-bytes (:publication entry))
        state (-> state
                  (assoc :queue (remove-queue-index (:queue state) index))
                  (update :queued-items dec)
                  (update :queued-bytes - bytes)
                  (assoc :in-flight (assoc entry :token token))
                  (update :next-token inc))]
    {:state state
     :effects [{:op :invoke-live
                :token token
                :topic (:topic state)
                :profile (:profile state)
                :publication (:publication entry)}]}))

(defn- advance
  [state]
  (cond
    (:in-flight state) {:state state :effects []}
    (not (contains? #{:live :draining} (:phase state)))
    {:state state :effects []}

    :else
    (if-let [index (first (keep-indexed (fn [i entry]
                                          (when (eligible? state entry) i))
                                        (:queue state)))]
      (start-entry state index)
      (cond
        (and (= :draining (:phase state)) (empty? (:queue state)))
        {:state (assoc state :phase :live :ready? true)
         :effects [{:op :ready :topic (:topic state)
                    :generation (:generation state)}]}

        (and (= :draining (:phase state)) (seq (:queue state)))
        (let [source (get-in state [:queue 0 :source])
              [state effects] (desynchronize state source :noncontiguous-suffix
                                             (:live-transport state))]
          {:state state :effects effects})

        :else {:state state :effects []}))))

(defn- enqueue
  [state event]
  (let [publication (:publication event)
        bytes (publication-bytes publication)
        max-items (get-in state [:opts :max-queued-items])
        max-bytes (get-in state [:opts :max-queued-bytes])]
    (if (or (>= (:queued-items state) max-items)
            (> (+ (:queued-bytes state) bytes) max-bytes))
      (let [[state effects] (desynchronize state (:source event) :apply-overflow
                                           (:transport event))]
        {:state state
         :effects (into [{:op :settle :receipt (:receipt event)
                          :result {:error :netz/apply-overflow}}]
                        effects)})
      (let [entry {:publication publication
                   :source (:source event)
                   :transport (:transport event)
                   :receipts (cond-> [] (:receipt event) (conj (:receipt event)))}
            state (-> state
                      (update :queue conj entry)
                      (update :queued-items inc)
                      (update :queued-bytes + bytes))
            advanced (advance state)]
        advanced))))

(defn- add-duplicate-receipt
  [state event]
  (let [publication (:publication event)
        receipt (:receipt event)]
    (cond
      (and (:in-flight state)
           (matching-entry? (:in-flight state) publication))
      {:state (cond-> state
                receipt (update-in [:in-flight :receipts] conj receipt))
       :effects []}

      :else
      (if-let [index (first (keep-indexed (fn [i entry]
                                            (when (matching-entry? entry publication) i))
                                          (:queue state)))]
        {:state (cond-> state
                  receipt (update-in [:queue index :receipts] conj receipt))
         :effects []}
        nil))))

(defn- on-live
  [state event]
  (let [publication (:publication event)
        receipt (:receipt event)]
    (cond
      (not= (:generation state) (:generation event))
      {:state state
       :effects [{:op :settle :receipt receipt
                  :result {:error :netz/stale-generation}}]}

      (not= (:live-transport state) (:transport event))
      {:state state
       :effects (cond-> [{:op :settle :receipt receipt
                          :result {:error :netz/wrong-live-transport}}]
                  (= :direct (:transport event))
                  (conj {:op :retire-direct :topic (:topic state)
                         :reason :wrong-live-transport}))}

      (contains? #{:closed :failed} (:phase state))
      {:state state
       :effects [{:op :settle :receipt receipt
                  :result {:error :netz/apply-owner-closed}}]}

      (applied? state publication)
      {:state state
       :effects [{:op :settle :receipt receipt
                  :result {:ok true :duplicate true}}]}

      :else
      (or (add-duplicate-receipt state event)
          (enqueue state event)))))

(defn- advance-frontier [state publication]
  (if-let [stream (stream-key publication)]
    (assoc-in state [:applied-frontiers stream] (:seq publication))
    state))

(defn- on-apply-result
  [state {:keys [token result]}]
  (let [entry (:in-flight state)]
    (if (or (nil? entry) (not= token (:token entry)))
      {:state state :effects []}
      (if (and (= true (:ok result)) (not (:error result)))
        (let [state (-> state
                        (assoc :in-flight nil)
                        (advance-frontier (:publication entry)))
              settled (settle-effects entry {:ok true})
              begin-sync? (:sync-begin-pending? state)
              state (cond-> state begin-sync? (assoc :sync-begin-pending? false))
              advanced (if begin-sync?
                         {:state state
                          :effects [{:op :begin-sync :topic (:topic state)
                                     :generation (:generation state)}]}
                         (advance state))]
          {:state (:state advanced)
           :effects (into settled (:effects advanced))})
        (let [[state recovery]
              (desynchronize (assoc state :in-flight nil)
                             (:source entry) :profile-apply-failed
                             (:transport entry))]
          {:state state
           :effects (into (settle-effects
                           entry {:error :netz/profile-apply-failed
                                  :result result})
                          recovery)})))))

(defn- at-or-below?
  [frontiers publication]
  (when-let [stream (stream-key publication)]
    (<= (:seq publication) (get frontiers stream -1))))

(defn- on-sync-commit
  [state {:keys [generation frontiers]}]
  (cond
    (not= (:generation state) generation)
    {:state state :effects []}

    (:in-flight state)
    {:state state
     :effects [{:op :sync-protocol-error :topic (:topic state)
                :reason :commit-during-apply}]}

    :else
    (let [frontiers (merge-with max (:applied-frontiers state) (or frontiers {}))
          [subsumed remaining]
          (reduce (fn [[subsumed remaining] entry]
                    (if (at-or-below? frontiers (:publication entry))
                      [(conj subsumed entry) remaining]
                      [subsumed (conj remaining entry)]))
                  [[] []]
                  (:queue state))
          state (-> state
                    (assoc :phase :draining
                           :ready? false
                           :sync-requested? false
                           :applied-frontiers frontiers
                           :queue (vec remaining)
                           :queued-items (count remaining)
                           :queued-bytes (reduce + 0 (map #(publication-bytes
                                                            (:publication %))
                                                          remaining))))
          settled (mapcat #(settle-effects % {:ok true :subsumed true}) subsumed)
          advanced (advance state)]
      {:state (:state advanced)
       :effects (into (vec settled) (:effects advanced))})))

(defn- on-sync-begin
  [state {:keys [generation]}]
  (if (not= (:generation state) generation)
    {:state state :effects []}
    (let [waiting? (some? (:in-flight state))]
      {:state (assoc state :phase :joining :ready? false
                     :sync-requested? false
                     :sync-begin-pending? waiting?)
       :effects (if waiting?
                  []
                  [{:op :begin-sync :topic (:topic state)
                    :generation (:generation state)}])})))

(defn- on-close
  [state {:keys [generation]}]
  (if (not= (:generation state) generation)
    {:state state :effects []}
    (let [entries (cond-> (:queue state) (:in-flight state) (conj (:in-flight state)))
          effects (mapcat #(settle-effects % {:error :netz/apply-owner-closed}) entries)]
      {:state (assoc state :phase :closed :ready? false :queue []
                     :queued-items 0 :queued-bytes 0 :in-flight nil
                     :sync-begin-pending? false)
       :effects (vec effects)})))

(defn transition
  "Advance an apply owner by one event and return `{:state … :effects […]}`.

  Supported events are `:live`, `:apply-result`, `:sync/begin`,
  `:sync/commit`, and `:close`."
  [state event]
  (case (:type event)
    :live (on-live state event)
    :apply-result (on-apply-result state event)
    :sync/begin (on-sync-begin state event)
    :sync/commit (on-sync-commit state event)
    :close (on-close state event)
    {:state state :effects []}))
