(ns netz.apply-owner
  "Pure ownership boundary between network delivery and profile mutation.

  Transport receipt, ordered release, and successful application are distinct
  facts. This machine advances a publication frontier only after the local
  profile reports literal `{:ok true}`. Snapshot frontiers are accepted only
  from the result of a generation- and session-scoped local profile operation;
  wire messages must never be translated directly into `:profile-sync-result`.
  Runtimes execute returned effects and feed their results back as events, so
  the same transitions are usable in deterministic tests.")

(def default-opts
  {:max-queued-items 1024
   :max-queued-bytes (* 8 1024 1024)
   :max-duplicate-waiters 64
   :max-frontier-streams 1024
   :max-frontier-bytes (* 256 1024)})

(def max-safe-integer
  "Largest integer represented exactly by both JVM integers and JS numbers."
  9007199254740991)

(defn- safe-uint? [value]
  (and (integer? value) (<= 0 value max-safe-integer)))

(defn- valid-stream? [topic stream]
  (and (vector? stream)
       (= 3 (count stream))
       (some? (nth stream 0))
       (safe-uint? (nth stream 1))
       (= topic (nth stream 2))))

(defn- valid-frontier-map? [topic opts frontiers]
  (and (map? frontiers)
       (<= (count frontiers) (:max-frontier-streams opts))
       (every? (fn [[stream seq-no]]
                 (and (valid-stream? topic stream)
                      (or (= -1 seq-no) (safe-uint? seq-no))))
               frontiers)))

(defn- valid-limit? [value]
  (safe-uint? value))

(defn make-state
  [{:keys [topic profile generation live-transport phase applied-frontiers opts]
    :or {live-transport :overlay phase :joining}}]
  (let [opts (merge default-opts opts)
        frontiers (or applied-frontiers {})]
    (when-not (and (some? topic)
                   (some? profile)
                   (some? generation)
                   (contains? #{:overlay :direct} live-transport)
                   (contains? #{:joining :draining :live
                                :desynchronized :closed :failed} phase)
                   (every? valid-limit? (vals (select-keys
                                               opts
                                               (keys default-opts))))
                   (valid-frontier-map? topic opts frontiers))
      (throw (ex-info "Invalid apply-owner configuration"
                      {:type :netz/invalid-apply-owner
                       :topic topic :profile profile :generation generation
                       :live-transport live-transport :phase phase})))
    {:topic topic
     :profile profile
     :generation generation
     :live-transport live-transport
     :phase phase
     :queue []
     :queued-items 0
     :queued-bytes 0
     :in-flight nil
     :active-sync nil
     :sync-begin-pending? false
     :next-token 0
     :next-sync-token 0
     :applied-frontiers frontiers
     :sync-requested? false
     :ready? (= phase :live)
     :opts opts}))

(defn stream-key
  [{:keys [origin epoch topic]}]
  (when (and (some? origin) (some? epoch) (some? topic))
    [origin epoch topic]))

(defn publication-id
  [{:keys [seq] :as publication}]
  (when-let [stream (stream-key publication)]
    (when (safe-uint? seq)
      (conj stream seq))))

(defn- exact-success? [result]
  (and (map? result)
       (= true (:ok result))
       (not (contains? result :error))))

(defn- publication-valid? [state publication]
  (and (publication-id publication)
       (valid-stream? (:topic state) (stream-key publication))
       (= (:topic state) (:topic publication))
       (safe-uint? (:bytes publication))))

(defn- publication-bytes [publication]
  (or (:bytes publication) 0))

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

(defn- settle-one [receipt result]
  (cond-> [] receipt (conj {:op :settle :receipt receipt :result result})))

(defn- all-entries [state]
  (cond-> (:queue state)
    (:in-flight state) (conj (:in-flight state))))

(defn- clear-work [state phase]
  (assoc state :phase phase :ready? false :queue []
         :queued-items 0 :queued-bytes 0 :in-flight nil
         :active-sync nil :sync-begin-pending? false))

(defn- fail-owner
  [state error reason]
  (let [effects (mapcat #(settle-effects % {:error error}) (all-entries state))
        effects (cond-> (vec effects)
                  (= :direct (:live-transport state))
                  (conj {:op :retire-direct :topic (:topic state)
                         :reason reason}))]
    {:state (clear-work state :failed)
     :effects effects}))

(defn- request-sync
  [state source reason]
  (if (:sync-requested? state)
    [state []]
    [(assoc state :sync-requested? true)
     [{:op :request-sync :source source :topic (:topic state) :reason reason}]]))

(defn- desynchronize
  [state source reason transport]
  (if (= transport :direct)
    (let [{:keys [state effects]}
          (fail-owner state :netz/direct-generation-failed reason)]
      [state effects])
    (request-sync (assoc state :phase :desynchronized :ready? false)
                  source reason)))

(defn- eligible? [state entry]
  (= (:seq (:publication entry))
     (expected-seq state (:publication entry))))

(defn- remove-queue-index [queue index]
  (into (subvec queue 0 index) (subvec queue (inc index))))

(declare advance)

(defn- apply-operation-id [state token]
  {:topic (:topic state)
   :profile (:profile state)
   :generation (:generation state)
   :token token})

(defn- start-entry
  [state index]
  (let [entry (nth (:queue state) index)
        token (:next-token state)
        operation-id (apply-operation-id state token)
        bytes (publication-bytes (:publication entry))
        state (-> state
                  (assoc :queue (remove-queue-index (:queue state) index))
                  (update :queued-items dec)
                  (update :queued-bytes - bytes)
                  (assoc :in-flight (assoc entry :operation-id operation-id))
                  (update :next-token inc))]
    {:state state
     :effects [{:op :invoke-live
                :operation-id operation-id
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
        {:state (assoc state :phase :live :ready? true :active-sync nil)
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
      (if (or (:active-sync state) (= :direct (:transport event)))
        (let [failed (fail-owner state :netz/apply-overflow :apply-overflow)]
          (update failed :effects #(into (settle-one (:receipt event)
                                                     {:error :netz/apply-overflow})
                                         %)))
        (let [[state effects] (desynchronize state (:source event) :apply-overflow
                                             (:transport event))]
          {:state state
           :effects (into (settle-one (:receipt event)
                                      {:error :netz/apply-overflow})
                          effects)}))
      (let [entry {:publication publication
                   :source (:source event)
                   :transport (:transport event)
                   :receipts (cond-> [] (:receipt event) (conj (:receipt event)))}
            state (-> state
                      (update :queue conj entry)
                      (update :queued-items inc)
                      (update :queued-bytes + bytes))]
        (advance state)))))

(defn- append-duplicate-receipt
  [state path receipt]
  (let [receipts (get-in state path)
        max-waiters (get-in state [:opts :max-duplicate-waiters])]
    (cond
      (nil? receipt) {:state state :effects []}
      (some #(= receipt %) receipts) {:state state :effects []}
      (>= (count receipts) max-waiters)
      {:state state
       :effects (settle-one receipt {:error :netz/duplicate-waiter-overflow})}
      :else {:state (update-in state path conj receipt) :effects []})))

(defn- add-duplicate-receipt
  [state event]
  (let [publication (:publication event)]
    (cond
      (and (:in-flight state)
           (matching-entry? (:in-flight state) publication))
      (append-duplicate-receipt state [:in-flight :receipts] (:receipt event))

      :else
      (when-let [index (first (keep-indexed (fn [i entry]
                                              (when (matching-entry? entry publication) i))
                                            (:queue state)))]
        (append-duplicate-receipt state [:queue index :receipts]
                                  (:receipt event))))))

(defn- on-live
  [state event]
  (let [publication (:publication event)
        receipt (:receipt event)]
    (cond
      (not= (:generation state) (:generation event))
      {:state state
       :effects (settle-one receipt {:error :netz/stale-generation})}

      (not (publication-valid? state publication))
      {:state state
       :effects (settle-one receipt {:error :netz/invalid-publication})}

      (not= (:live-transport state) (:transport event))
      {:state state
       :effects (cond-> (settle-one receipt {:error :netz/wrong-live-transport})
                  (= :direct (:transport event))
                  (conj {:op :retire-direct :topic (:topic state)
                         :reason :wrong-live-transport}))}

      (contains? #{:closed :failed} (:phase state))
      {:state state
       :effects (settle-one receipt {:error :netz/apply-owner-closed})}

      (applied? state publication)
      {:state state
       :effects (settle-one receipt {:ok true :duplicate true})}

      :else
      (or (add-duplicate-receipt state event)
          (enqueue state event)))))

(defn- advance-frontier [state publication]
  (assoc-in state [:applied-frontiers (stream-key publication)] (:seq publication)))

(defn- begin-sync-effect [state]
  {:op :begin-sync
   :operation-id (:active-sync state)
   :topic (:topic state)
   :profile (:profile state)})

(defn- on-apply-result
  [state {:keys [operation-id result]}]
  (let [entry (:in-flight state)]
    (if (or (nil? entry) (not= operation-id (:operation-id entry)))
      {:state state :effects []}
      (if (exact-success? result)
        (let [state (-> state
                        (assoc :in-flight nil)
                        (advance-frontier (:publication entry)))
              settled (settle-effects entry {:ok true})
              begin-sync? (:sync-begin-pending? state)
              state (cond-> state begin-sync? (assoc :sync-begin-pending? false))
              advanced (if begin-sync?
                         {:state state :effects [(begin-sync-effect state)]}
                         (advance state))]
          {:state (:state advanced)
           :effects (into settled (:effects advanced))})
        (if (and (:sync-begin-pending? state)
                 (= :direct (:transport entry)))
          (fail-owner state :netz/profile-apply-failed :profile-apply-failed)
          (let [begin-sync? (:sync-begin-pending? state)
                state (assoc state :in-flight nil)
                [state recovery]
                (if begin-sync?
                  [(assoc state :phase :joining :ready? false
                          :sync-begin-pending? false)
                   [(begin-sync-effect state)]]
                  (desynchronize state (:source entry) :profile-apply-failed
                                 (:transport entry)))]
            {:state state
             :effects (into (settle-effects
                             entry {:error :netz/profile-apply-failed
                                    :result result})
                            recovery)}))))))

(defn- at-or-below? [frontiers publication]
  (<= (:seq publication) (get frontiers (stream-key publication) -1)))

(defn- valid-frontiers? [state result]
  (and (valid-frontier-map? (:topic state) (:opts state) (:frontiers result))
       (safe-uint? (:frontier-bytes result))
       (<= (:frontier-bytes result)
           (get-in state [:opts :max-frontier-bytes]))))

(defn- commit-frontiers
  [state frontiers]
  (let [frontiers (merge-with max (:applied-frontiers state) frontiers)
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
                         :active-sync nil
                         :applied-frontiers frontiers
                         :queue (vec remaining)
                         :queued-items (count remaining)
                         :queued-bytes (reduce + 0 (map #(publication-bytes
                                                          (:publication %))
                                                        remaining))))
        settled (mapcat #(settle-effects % {:ok true :subsumed true}) subsumed)
        advanced (advance state)]
    {:state (:state advanced)
     :effects (into (vec settled) (:effects advanced))}))

(defn- on-profile-sync-result
  [state {:keys [operation-id result]}]
  (cond
    (or (nil? (:active-sync state))
        (not= operation-id (:active-sync state)))
    {:state state :effects []}

    (not= :joining (:phase state))
    {:state state
     :effects [{:op :sync-protocol-error :topic (:topic state)
                :reason :commit-outside-joining}]}

    (:in-flight state)
    {:state state
     :effects [{:op :sync-protocol-error :topic (:topic state)
                :reason :commit-during-apply}]}

    (not (exact-success? result))
    (fail-owner state :netz/sync-failed :profile-sync-failed)

    (not (valid-frontiers? state result))
    (fail-owner state :netz/invalid-sync-frontiers :invalid-sync-frontiers)

    :else
    (commit-frontiers state (:frontiers result))))

(defn- sync-operation-id [state sync-id]
  {:topic (:topic state)
   :profile (:profile state)
   :generation (:generation state)
   :attempt (:next-sync-token state)
   :sync-id sync-id})

(defn- on-sync-begin
  [state {:keys [generation sync-id]}]
  (let [operation-id (sync-operation-id state sync-id)]
    (cond
      (not= (:generation state) generation)
      {:state state :effects []}

      (nil? sync-id)
      {:state state
       :effects [{:op :sync-protocol-error :topic (:topic state)
                  :reason :missing-sync-id}]}

      (= sync-id (:sync-id (:active-sync state)))
      {:state state :effects []}

      (:active-sync state)
      {:state state
       :effects [{:op :sync-protocol-error :topic (:topic state)
                  :reason :sync-already-active}]}

      :else
      (let [waiting? (some? (:in-flight state))
            state (assoc state :phase :joining :ready? false
                         :sync-requested? false
                         :active-sync operation-id
                         :sync-begin-pending? waiting?
                         :next-sync-token (inc (:next-sync-token state)))]
        {:state state
         :effects (if waiting? [] [(begin-sync-effect state)])}))))

(defn- on-sync-aborted
  [state {:keys [operation-id]}]
  (if (and (:active-sync state)
           (= operation-id (:active-sync state)))
    (fail-owner state :netz/sync-aborted :profile-sync-aborted)
    {:state state :effects []}))

(defn- on-close
  [state {:keys [generation]}]
  (if (not= (:generation state) generation)
    {:state state :effects []}
    (let [effects (mapcat #(settle-effects % {:error :netz/apply-owner-closed})
                          (all-entries state))]
      {:state (clear-work state :closed)
       :effects (vec effects)})))

(defn transition
  "Advance an apply owner by one event and return `{:state ... :effects [...]}`.

  Network-facing events are `:live`, `:sync/begin`, and `:close`. Local runtime
  completions are `:apply-result`, `:profile-sync-result`, and `:sync/aborted`.
  A runtime must emit `:profile-sync-result` only from the local profile operation
  named by the matching `:begin-sync` effect, never from untrusted wire data."
  [state event]
  (if (and (contains? #{:closed :failed} (:phase state))
           (not= :live (:type event)))
    {:state state :effects []}
    (case (:type event)
      :live (on-live state event)
      :apply-result (on-apply-result state event)
      :sync/begin (on-sync-begin state event)
      :profile-sync-result (on-profile-sync-result state event)
      :sync/aborted (on-sync-aborted state event)
      :close (on-close state event)
      {:state state :effects []})))
