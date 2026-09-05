(ns netz.replication.migration
  "Experimental trusted-controller migration model, not a wire protocol.

  Inputs must already be authenticated and authorized. No deletion effects are
  emitted. Every mutation requires a matching local persistence completion before
  further events or retirement eligibility. Callers retry events returning :busy.
  migration-file provides optional local JVM checkpoint IO; the distributed
  runtime adapter and authenticated wire format are not implemented."
  (:require [clojure.set :as set]
            [netz.replication.placement :as placement]
            [netz.replication.stable-placement :as stable]))

(defn create
  "Prepare one transition. Binding carries exact cluster/object, old/new layout
  addresses, policy and controller fencing identity supplied by the trusted caller."
  [{:keys [binding layout seed policy old-holders retire-after]}]
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
        targets (stable/place layout seed policy (:object binding))]
    {:binding (assoc binding :policy policy)
     :targets targets :old-holders old-holders :retire-after retire-after
     :attempts (zipmap (map :id targets) (repeat 0))
     :receipts {} :revision 0 :persisted -1 :halted? false}))

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

(defn- covered? [state now]
  (and (every? (fn [[id attempt]]
                 (let [receipt (get-in state [:receipts id])]
                   (and (= attempt (:attempt receipt))
                        (number? (:expires receipt))
                        (< now (:expires receipt)))))
               (:attempts state))
       (placement/satisfies-domains?
        (:targets state) (get-in state [:binding :policy :minimum-domains]))))

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
                    (set (map :id (:targets state))) protected-holders)
    #{}))

(defn step
  "Apply trusted local events. :persisted must contain the exact :checkpoint
  written; :receipt/:lost bind replica+attempt and exact transition binding.
  :halt fences this transition permanently, including while persistence is pending."
  [state {:keys [type binding replica attempt expires checkpoint] :as event}]
  (cond
    (= type :halt)
    {:state (-> state (assoc :halted? true) (update :revision inc)) :status :persist}

    (= type :persisted)
    (if (= checkpoint (netz.replication.migration/checkpoint state))
      {:state (assoc state :persisted (:revision state)) :status :persisted}
      {:state state :status :stale})

    (:halted? state) {:state state :status :halted}
    (not (ready? state)) {:state state :status :busy}
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
