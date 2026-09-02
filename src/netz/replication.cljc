(ns netz.replication
  "Pure receipt controller for durable immutable objects.

  The controller does not perform IO. `:replication/begin` and every retry
  generation first emit a `:persist-intent` action. Only the matching
  `:replication/intent-stored` revision releases network `:store` actions,
  making the durable-outbox and attempt-fencing crash boundaries explicit in
  the API. A runtime reports successful remote durable installation back as
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
           :revised 0
           :repair-scans 0
           :repaired 0}})

(defn make-empty-state
  "Create a controller with no global placement default.

  Every begin event must supply its own `:layout` and `:policy`. This shape is
  used by a multi-policy runtime which restores or validates each intent
  independently."
  []
  {:layout nil
   :policy nil
   :objects {}
   :stats {:begun 0
           :intents-stored 0
           :stored 0
           :duplicates 0
           :invalid-acks 0
           :durable 0
           :complete 0
           :lost 0
           :revised 0
           :repair-scans 0
           :repaired 0}})

(defn- store-action [entry target]
  [:store target (:manifest entry)
   {:layout-epoch (:layout-epoch entry)
    :attempt (get-in entry [:attempts target])}])

(defn- persist-action [object-id entry]
  [:persist-intent object-id entry])

(defn- object-receipt [entry status]
  {:object-id (get-in entry [:manifest :object-id])
   :layout-epoch (:layout-epoch entry)
   :target-replicas (count (:targets entry))
   :durable-acks (vec (filter (:acks entry) (:targets entry)))
   :signed-receipts (select-keys (:receipts entry) (:targets entry))
   :status status})

(defn- ack-members [entry]
  (let [by-id (into {} (map (juxt :id identity))
                    (placement/eligible-members (:layout entry)))]
    (keep by-id (:acks entry))))

(defn- durable? [entry]
  (let [{:keys [write-acks minimum-domains]} (:policy entry)]
    (and (>= (count (:acks entry)) write-acks)
         (placement/satisfies-domains? (ack-members entry)
                                       minimum-domains))))

(defn- complete? [entry]
  (= (set (:targets entry)) (:acks entry)))

(defn- begin [state manifest requested-layout requested-policy]
  (let [object-id (:object-id manifest)
        layout (placement/validate-layout (or requested-layout (:layout state)))
        policy (placement/validate-policy
                layout (or requested-policy (:policy state)))]
    (when (nil? object-id)
      (throw (ex-info "Replication manifest has no object id"
                      {:type :netz.replication/invalid-manifest
                       :manifest manifest})))
    (if-let [existing (get-in state [:objects object-id])]
      (if (= [manifest layout policy]
             [(:manifest existing) (:layout existing) (:policy existing)])
        {:state state :actions []}
        (throw (ex-info "One object id was begun with conflicting manifests"
                        {:type :netz.replication/conflicting-manifest
                         :object-id object-id
                         :existing (:manifest existing)
                         :incoming manifest})))
      (let [targets (mapv :id
                          (placement/place layout policy object-id))
            entry {:manifest manifest
                   :layout layout
                   :policy policy
                   :layout-epoch (:epoch layout)
                   :targets targets
                   :attempts (zipmap targets (repeat 0))
                   :acks #{}
                   :receipts {}
                   :intent-revision 0
                   :intent-stored? false
                   :pending-targets targets
                   :durable-emitted? false
                   :complete-emitted? false
                   :repairing? false}]
        {:state (-> state
                    (assoc-in [:objects object-id] entry)
                    (update-in [:stats :begun] inc))
         :actions [(persist-action object-id entry)]}))))

(defn- intent-stored
  [state {:keys [object-id layout-epoch intent-revision]}]
  (if-let [entry (get-in state [:objects object-id])]
    (cond
      (or (not= layout-epoch (:layout-epoch entry))
          (not= intent-revision (:intent-revision entry)))
      {:state (update-in state [:stats :invalid-acks] inc) :actions []}

      (:intent-stored? entry)
      {:state state :actions []}

      :else
      (let [state (-> state
                      (assoc-in [:objects object-id :intent-stored?] true)
                      (assoc-in [:objects object-id :pending-targets] [])
                      (update-in [:stats :intents-stored] inc))
            stored-entry (get-in state [:objects object-id])]
        {:state state
         :actions (mapv #(store-action stored-entry %)
                        (:pending-targets entry))}))
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
              emit-durable? (and (durable? entry)
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
  (if (seq targets)
    (let [state (-> (reduce (fn [s target]
                              (update-in s [:objects object-id :attempts target]
                                         inc))
                            state targets)
                    (update-in [:objects object-id :intent-revision] inc)
                    (assoc-in [:objects object-id :intent-stored?] false)
                    (assoc-in [:objects object-id :pending-targets] targets))
          entry (get-in state [:objects object-id])]
      {:state state
       :actions [(persist-action object-id entry)]})
    {:state state :actions []}))

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

(defn- revise [state intent]
  (let [{:keys [manifest layout policy attempts intent-revision]} intent
        object-id (:object-id manifest)]
    (if-let [existing (get-in state [:objects object-id])]
      (let [layout (placement/validate-layout layout)
            policy (placement/validate-policy layout policy)
            targets (mapv :id (placement/place layout policy object-id))]
        (when-not (and (:intent-stored? existing)
                       (:manifest-id manifest)
                       (= (:manifest-id (:manifest existing))
                          (:manifest-id manifest))
                       (<= (:layout-epoch existing) (:epoch layout))
                       (= intent-revision (inc (:intent-revision existing)))
                       (<= intent-revision 9007199254740991)
                       (= (set targets) (set (keys attempts)))
                       (every? (fn [target]
                                 (if-let [old-attempt
                                          (get (:attempts existing) target)]
                                   (= (inc old-attempt) (get attempts target))
                                   (= intent-revision (get attempts target))))
                               targets))
          (throw (ex-info "Invalid replication intent revision"
                          {:type :netz.replication/invalid-intent-revision
                           :object-id object-id})))
        (let [entry {:manifest manifest
                     :layout layout
                     :policy policy
                     :layout-epoch (:epoch layout)
                     :targets targets
                     :attempts attempts
                     :acks #{}
                     :receipts {}
                     :intent-revision intent-revision
                     :intent-stored? false
                     :pending-targets targets
                     :durable-emitted? false
                     :complete-emitted? false
                     :repairing? false}]
          {:state (-> state
                      (assoc-in [:objects object-id] entry)
                      (update-in [:stats :revised] inc))
           :actions [(persist-action object-id entry)]}))
      (throw (ex-info "Cannot revise an unknown replication intent"
                      {:type :netz.replication/missing-intent
                       :object-id object-id})))))

(defn- restored-entry
  [{:keys [manifest layout policy attempts intent-revision]}]
  (let [object-id (:object-id manifest)
        layout (placement/validate-layout layout)
        policy (placement/validate-policy layout policy)
        targets (mapv :id (placement/place layout policy object-id))]
    (when-not (and object-id
                   (map? attempts)
                   (= (set targets) (set (keys attempts)))
                   (every? #(and (integer? %) (<= 0 % 9007199254740991))
                           (vals attempts))
                   (integer? intent-revision)
                   (<= 0 intent-revision 9007199254740991))
      (throw (ex-info "Invalid durable replication intent"
                      {:type :netz.replication/invalid-durable-intent
                       :object-id object-id})))
    [object-id
     {:manifest manifest
      :layout layout
      :policy policy
      :layout-epoch (:epoch layout)
      :targets targets
      :attempts attempts
      :acks #{}
      :receipts {}
      :intent-revision intent-revision
      :intent-stored? true
      :pending-targets []
      :durable-emitted? false
      :complete-emitted? false
      :repairing? false}]))

(defn restore-state
  "Restore verified durable intents without releasing network work.

  `intents` is a bounded sequence of maps containing `:manifest`, `:layout`,
  `:policy`, exact per-target `:attempts`, and `:intent-revision`. A runtime
  replays durable signed receipts next and only then submits explicit repair
  events; those repairs cross a new persisted revision before any re-offer."
  [state intents]
  (reduce (fn [current intent]
            (let [[object-id entry] (restored-entry intent)]
              (when (contains? (:objects current) object-id)
                (throw (ex-info "Duplicate durable replication intent"
                                {:type :netz.replication/duplicate-durable-intent
                                 :object-id object-id})))
              (-> current
                  (assoc-in [:objects object-id] entry)
                  (update-in [:stats :intents-stored] inc))))
          state intents))

(defn handler
  "Advance receipt state by one local event.

  Supported events:

  - `{:type :replication/begin :manifest {... :object-id ...}}`
  - `{:type :replication/intent-stored :object-id ... :layout-epoch ...
      :intent-revision ...}`
  - `{:type :replication/stored :object-id ... :replica-id ...
      :layout-epoch ... :attempt ... :receipt <verified-envelope>}`
  - `{:type :replication/lost :object-id ... :replica-id ...
      :layout-epoch ...}`
  - `{:type :replication/repair :object-id ...}`
  - `{:type :replication/revise :intent <verified-successor-intent>}`"
  [state {:keys [type manifest layout policy intent] :as event} _context]
  (case type
    :replication/begin (begin state manifest layout policy)
    :replication/intent-stored (intent-stored state event)
    :replication/stored (stored state event)
    :replication/lost (lost state event)
    :replication/repair (repair state event)
    :replication/revise (revise state intent)
    (throw (ex-info "Unknown replication event"
                    {:type :netz.replication/unknown-event
                     :event event}))))
