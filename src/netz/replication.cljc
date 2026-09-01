(ns netz.replication
  "Pure receipt controller for durable immutable objects.

  The controller does not perform IO. `:replication/begin` first emits a
  `:persist-intent` action. Only `:replication/intent-stored` releases network
  `:store` actions, making the durable-outbox crash boundary explicit in the
  API. A runtime reports successful remote durable installation back as
  `:replication/stored`. Only those events can produce `:durable` or `:complete`
  actions. Network delivery, an offer, or a write merely being queued is never
  a durability acknowledgement."
  (:require [netz.replication.placement :as placement]))

(defn make-state
  [layout policy]
  {:layout (placement/validate-layout layout)
   :policy (placement/validate-policy layout policy)
   :objects {}
   :stats {:begun 0
           :intents-stored 0
           :stored 0
           :duplicates 0
           :invalid-acks 0
           :durable 0
           :complete 0
           :lost 0
           :repair-scans 0
           :repaired 0}})

(defn- store-action [entry target]
  [:store target (:manifest entry)
   {:layout-epoch (:layout-epoch entry)
    :attempt (get-in entry [:attempts target])}])

(defn- object-receipt [entry status]
  {:object-id (get-in entry [:manifest :object-id])
   :layout-epoch (:layout-epoch entry)
   :target-replicas (count (:targets entry))
   :durable-acks (vec (filter (:acks entry) (:targets entry)))
   :signed-receipts (select-keys (:receipts entry) (:targets entry))
   :status status})

(defn- ack-members [state entry]
  (let [by-id (into {} (map (juxt :id identity))
                    (placement/eligible-members (:layout state)))]
    (keep by-id (:acks entry))))

(defn- durable? [state entry]
  (let [{:keys [write-acks minimum-domains]} (:policy state)]
    (and (>= (count (:acks entry)) write-acks)
         (placement/satisfies-domains? (ack-members state entry)
                                       minimum-domains))))

(defn- complete? [entry]
  (= (set (:targets entry)) (:acks entry)))

(defn- begin [state manifest]
  (let [object-id (:object-id manifest)]
    (when (nil? object-id)
      (throw (ex-info "Replication manifest has no object id"
                      {:type :netz.replication/invalid-manifest
                       :manifest manifest})))
    (if-let [existing (get-in state [:objects object-id])]
      (if (= manifest (:manifest existing))
        {:state state :actions []}
        (throw (ex-info "One object id was begun with conflicting manifests"
                        {:type :netz.replication/conflicting-manifest
                         :object-id object-id
                         :existing (:manifest existing)
                         :incoming manifest})))
      (let [targets (mapv :id
                          (placement/place (:layout state) (:policy state)
                                           object-id))
            entry {:manifest manifest
                   :layout-epoch (get-in state [:layout :epoch])
                   :targets targets
                   :attempts (zipmap targets (repeat 0))
                   :acks #{}
                   :receipts {}
                   :intent-stored? false
                   :durable-emitted? false
                   :complete-emitted? false
                   :repairing? false}]
        {:state (-> state
                    (assoc-in [:objects object-id] entry)
                    (update-in [:stats :begun] inc))
         :actions [[:persist-intent object-id entry]]}))))

(defn- intent-stored [state {:keys [object-id layout-epoch]}]
  (if-let [entry (get-in state [:objects object-id])]
    (cond
      (not= layout-epoch (:layout-epoch entry))
      {:state (update-in state [:stats :invalid-acks] inc) :actions []}

      (:intent-stored? entry)
      {:state state :actions []}

      :else
      (let [state (-> state
                      (assoc-in [:objects object-id :intent-stored?] true)
                      (update-in [:stats :intents-stored] inc))
            entry (get-in state [:objects object-id])]
        {:state state
         :actions (mapv #(store-action entry %) (:targets entry))}))
    {:state (update-in state [:stats :invalid-acks] inc) :actions []}))

(defn- stored
  [state {:keys [object-id replica-id layout-epoch attempt receipt]}]
  (if-let [entry (get-in state [:objects object-id])]
    (let [valid? (and (:intent-stored? entry)
                      (= layout-epoch (:layout-epoch entry))
                      (some #{replica-id} (:targets entry))
                      (= attempt (get-in entry [:attempts replica-id])))]
      (cond
        (not valid?)
        {:state (update-in state [:stats :invalid-acks] inc)
         :actions []}

        (contains? (:acks entry) replica-id)
        {:state (update-in state [:stats :duplicates] inc)
         :actions []}

        :else
        (let [state (cond-> (-> state
                                (update-in [:objects object-id :acks]
                                           conj replica-id)
                                (update-in [:stats :stored] inc))
                      receipt
                      (assoc-in [:objects object-id :receipts replica-id]
                                receipt))
              entry (get-in state [:objects object-id])
              emit-durable? (and (durable? state entry)
                                 (not (:durable-emitted? entry)))
              emit-complete? (and (complete? entry)
                                  (not (:complete-emitted? entry)))
              emit-repaired? (and (complete? entry)
                                  (:complete-emitted? entry)
                                  (:repairing? entry))
              state (cond-> state
                      emit-durable?
                      (-> (assoc-in [:objects object-id :durable-emitted?] true)
                          (update-in [:stats :durable] inc))
                      emit-complete?
                      (-> (assoc-in [:objects object-id :complete-emitted?] true)
                          (update-in [:stats :complete] inc))
                      emit-repaired?
                      (-> (assoc-in [:objects object-id :repairing?] false)
                          (update-in [:stats :repaired] inc)))
              entry (get-in state [:objects object-id])]
          {:state state
           :actions (cond-> []
                      emit-durable? (conj [:durable
                                           (object-receipt entry :quorum)])
                      emit-complete? (conj [:complete
                                            (object-receipt entry :complete)])
                      emit-repaired? (conj [:repaired
                                            (object-receipt entry :complete)]))})))
    {:state (update-in state [:stats :invalid-acks] inc)
     :actions []}))

(defn- retry-targets [state object-id targets]
  (let [state (reduce (fn [s target]
                        (update-in s [:objects object-id :attempts target] inc))
                      state targets)
        entry (get-in state [:objects object-id])]
    {:state state
     :actions (mapv #(store-action entry %) targets)}))

(defn- lost [state {:keys [object-id replica-id layout-epoch]}]
  (if-let [entry (get-in state [:objects object-id])]
    (if (and (:intent-stored? entry)
             (= layout-epoch (:layout-epoch entry))
             (some #{replica-id} (:targets entry)))
      (let [state (-> state
                      (update-in [:objects object-id :acks] disj replica-id)
                      (update-in [:objects object-id :receipts]
                                 dissoc replica-id)
                      (assoc-in [:objects object-id :repairing?]
                                (:complete-emitted? entry))
                      (update-in [:stats :lost] inc))]
        (retry-targets state object-id [replica-id]))
      {:state (update-in state [:stats :invalid-acks] inc) :actions []})
    {:state (update-in state [:stats :invalid-acks] inc) :actions []}))

(defn- repair [state {:keys [object-id]}]
  (if-let [entry (get-in state [:objects object-id])]
    (if (:intent-stored? entry)
      (let [missing (filterv #(not (contains? (:acks entry) %))
                             (:targets entry))
            state (update-in state [:stats :repair-scans] inc)]
        (retry-targets state object-id missing))
      {:state state :actions []})
    {:state state :actions []}))

(defn handler
  "Advance receipt state by one local event.

  Supported events:

  - `{:type :replication/begin :manifest {... :object-id ...}}`
  - `{:type :replication/intent-stored :object-id ... :layout-epoch ...}`
  - `{:type :replication/stored :object-id ... :replica-id ...
      :layout-epoch ... :attempt ... :receipt <verified-envelope>}`
  - `{:type :replication/lost :object-id ... :replica-id ...
      :layout-epoch ...}`
  - `{:type :replication/repair :object-id ...}`"
  [state {:keys [type manifest] :as event} _context]
  (case type
    :replication/begin (begin state manifest)
    :replication/intent-stored (intent-stored state event)
    :replication/stored (stored state event)
    :replication/lost (lost state event)
    :replication/repair (repair state event)
    (throw (ex-info "Unknown replication event"
                    {:type :netz.replication/unknown-event
                     :event event}))))
