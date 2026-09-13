(ns netz.replication.migration
  "Experimental trusted-controller migration model, not a wire protocol.

  Inputs must already be authenticated and authorized. No deletion effects are
  emitted. Every mutation requires a matching local persistence completion before
  further events or retirement eligibility. Callers retry events returning :busy.
  migration-konserve provides optional local JVM checkpoint IO; the distributed
  runtime adapter and authenticated wire format are not implemented."
  (:require [clojure.set :as set]
            [netz.replication.placement :as placement]))

(defn create
  "Prepare one transition. Binding carries exact cluster/object, old/new layout
  addresses, policy and controller fencing identity supplied by the trusted caller."
  [{:keys [binding layout policy old-holders retire-after]}]
  (when-not (and (map? binding)
                 (= #{:cluster :object :old-layout :new-layout :controller}
                    (set (keys binding)))
                 (every? #(and (string? %) (seq %)) (vals binding))
                 (not= (:old-layout binding) (:new-layout binding))
                 (set? old-holders)
                 (<= (count old-holders) placement/max-layout-members)
                 (every? #(and (string? %) (re-matches #"[0-9a-f]{64}" %)) old-holders)
                 (integer? retire-after) (<= 0 retire-after))
    (throw (ex-info "Invalid migration configuration" {:type ::invalid-config})))
  (let [policy (placement/validate-policy layout policy)
        targets (placement/place layout policy (:object binding))]
    {:binding (assoc binding :policy policy)
     :targets targets :old-holders old-holders :retire-after retire-after
     :attempts (zipmap (map :id targets) (repeat 0))
     :receipts {} :retirements {}
     :revision 0 :persisted -1 :halted? false}))

(defn checkpoint
  "Local checkpoint payload. Persist this exact value before acknowledging it.
  This is an internal data value, not an authenticated or portable wire format."
  [state]
  (dissoc state :persisted))

(defn restore
  "Restore an intact, trusted checkpoint read back from durable storage.
  Corruption detection and authentication are responsibilities of the adapter."
  [record]
  (assoc record :persisted (:revision record)))

(defn ready? [state] (= (:revision state) (:persisted state)))

(defn copy-candidates
  "Plan bounded copy work for the current durable transition, in placement order.
  Results bind replica, attempt and the exact transition. They are not network
  authorization: a runtime must validate current authority and signed layouts
  before issuing transfers. No work is released while a checkpoint is pending.
  Missing, obsolete-attempt and expired receipts require fresh evidence."
  [state {:keys [binding now]}]
  (if (and (ready? state) (not (:halted? state))
           (= binding (:binding state))
           (integer? now) (<= 0 now))
    (into []
          (keep (fn [{:keys [id]}]
                  (let [attempt (get-in state [:attempts id])
                        receipt (get-in state [:receipts id])]
                    (when-not (and (= attempt (:attempt receipt))
                                   (integer? (:expires receipt))
                                   (< now (:expires receipt)))
                      {:binding binding :replica id :attempt attempt}))))
          (:targets state))
    []))

(defn- covered? [state now]
  (and (every? (fn [[id attempt]]
                 (let [receipt (get-in state [:receipts id])]
                   (and (= attempt (:attempt receipt))
                        (number? (:expires receipt))
                        (< now (:expires receipt)))))
               (:attempts state))
       (placement/satisfies-domains?
        (:targets state)
        (get-in state [:binding :policy :placement-minimum-domains]))))

(defn retirement-candidates
  "Advisory only, never delete authority. Requires current exact binding, no
  pins/leases, elapsed grace, durable state and all N unexpired receipts.
  The future runtime must recheck these conditions atomically with retirement."
  [state {:keys [binding now protected-holders]}]
  (if (and (ready? state) (not (:halted? state))
           (= binding (:binding state))
           (number? now) (>= now (:retire-after state))
           (set? protected-holders) (covered? state now))
    (set/difference (:old-holders state)
                    (set (map :id (:targets state)))
                    protected-holders
                    (set (keys (:retirements state))))
    #{}))

(defn retirement-delete-candidates
  "Persisted deletion work that is still eligible under a fresh context.

  The returned generation is a one-shot fence for `:retired`. Callers must not
  cache this result: obtain it while serialized with protection changes and
  pass that exact generation to idempotent local deletion."
  [state {:keys [binding now protected-holders] :as context}]
  (if (and (ready? state) (not (:halted? state))
           (= binding (:binding state))
           (number? now) (>= now (:retire-after state))
           (set? protected-holders) (covered? state now))
    (->> (:retirements state)
         (keep (fn [[replica {:keys [phase generation]}]]
                 (when (and (= :deleting phase)
                            (contains? (:old-holders state) replica)
                            (not (contains? (set (map :id (:targets state))) replica))
                            (not (contains? protected-holders replica)))
                   {:binding binding :replica replica :generation generation})))
         (sort-by :replica)
         vec)
    []))

(defn- retirement-token-matches? [state {:keys [binding replica generation]} phase]
  (and (= binding (:binding state))
       (= {:phase phase :generation generation}
          (get-in state [:retirements replica]))))

(defn step
  "Apply trusted local events. :persisted must contain the exact :checkpoint
  written; :receipt/:lost bind replica+attempt and exact transition binding.
  :halt fences this transition permanently, including while persistence is pending."
  [state {:keys [type binding replica attempt expires checkpoint generation
                 now protected-holders deleted-at]
          :as event}]
  (cond
    (= type :halt)
    {:state (-> state (assoc :halted? true) (update :revision inc)) :status :persist}

    (= type :persisted)
    (if (= checkpoint (netz.replication.migration/checkpoint state))
      {:state (assoc state :persisted (:revision state)) :status :persisted}
      {:state state :status :stale})

    (not (ready? state)) {:state state :status :busy}

    ;; A successful physical deletion is a fact that must remain recordable
    ;; after a concurrent halt. Halting prevents new effects; it cannot undo an
    ;; effect which already crossed the storage boundary.
    (= type :retired)
    (if (and (retirement-token-matches? state event :deleting)
             (integer? deleted-at) (<= 0 deleted-at 9007199254740991))
      {:state (-> state
                  (assoc-in [:retirements replica]
                            {:phase :retired :generation generation
                             :deleted-at deleted-at})
                  (update :revision inc))
       :status :persist}
      {:state state :status :stale})

    (:halted? state) {:state state :status :halted}

    (= type :plan-retirement)
    (cond
      (contains? (:retirements state) replica)
      {:state state :status :duplicate}

      (contains? (retirement-candidates state
                                        {:binding binding :now now
                                         :protected-holders protected-holders})
                 replica)
      (let [generation (inc (:revision state))]
        {:state (-> state
                    (assoc-in [:retirements replica]
                              {:phase :planned :generation generation})
                    (update :revision inc))
         :status :persist})

      :else {:state state :status :ineligible})

    (= type :begin-retirement)
    (if (and (retirement-token-matches? state event :planned)
             (number? now)
             (set? protected-holders)
             (not (contains? protected-holders replica))
             (covered? state now)
             (>= now (:retire-after state)))
      {:state (-> state
                  (assoc-in [:retirements replica :phase] :deleting)
                  (update :revision inc))
       :status :persist}
      {:state state :status :ineligible})

    (not= binding (:binding state)) {:state state :status :stale}
    (not (contains? (:attempts state) replica)) {:state state :status :rejected}
    (not= attempt (get-in state [:attempts replica])) {:state state :status :stale}

    (= type :lost)
    {:state (-> state (update-in [:attempts replica] inc)
                (update :receipts dissoc replica) (update :revision inc))
     :status :persist}

    (and (= type :receipt) (integer? expires) (pos? expires))
    (let [receipt (select-keys event [:attempt :expires])]
      (if (= receipt (get-in state [:receipts replica]))
        {:state state :status :duplicate}
        {:state (-> state (assoc-in [:receipts replica] receipt)
                    (update :revision inc)) :status :persist}))

    :else {:state state :status :rejected}))
