(ns netz.overlay
  "L1 + L2 composed: membership below, dissemination above.

  This is the whole point of the two layers — seed a node with one address and
  it finds peers, forms a mesh, and publishes reach everyone on the topic,
  with no peer wired by hand. That is what replikativ needed and did not have:
  its P2P was explicit wiring of every pair (`replikativ`'s deleted
  `connect.cljc`), and this replaces it.

  The composition is deliberately mechanical. Both layers are pure
  `(state, event, ctx) -> {:state, :actions}` machines over disjoint message
  and timer vocabularies, so composing them is: run both, concatenate the
  actions, and feed membership's connection set into dissemination's peer set
  between the two. The rng is threaded through both so the whole node remains
  a pure function of its seed."
  (:require [netz.content :as c]
            [netz.topics]
            [netz.dissemination :as d]
            [netz.membership :as m]
            [netz.sim.rng :as rng]
            [netz.sync :as sync]))

(defn make-state
  "State for an overlay node.

  Options:
  - `:addresses`     — our own addresses, announced to peers on connect
  - `:authority-id`  — our full 256-bit Authority ID, announced with routing id
  - `:seeds`         — configured peers, `[{:peer-id … :addresses […] :group …}]`
  - `:topics`        — topics this node subscribes to
  - `:membership`    — options passed to `netz.membership/make-state`
  - `:dissemination` — options passed to `netz.dissemination/make-state`
  - `:content`       — options passed to `netz.content/make-state`
  - `:blocks`        — content we hold and can serve, `{key value}`
  - `:sync`          — profiles, application values, and sync session limits
  - `:carries`       — topic RANGES we relay, `#{[]}` (everything) by default
  - `:relay?`        — legacy boolean; false means carry nothing but our own"
  [id {:keys [authority-id addresses seeds topics membership dissemination content blocks sync
              relay? carries]
       :or {seeds [] topics #{} relay? true}}]
  {:id id
   :rng (rng/make-rng 0)
   ;; `:carries` and `:topics` have to reach membership too, or topic-aware
   ;; peer selection is inert: `candidates`' relevance scoring compares a
   ;; peer's advertised `:carries` against what WE want, and with `:topics`
   ;; empty it scored every peer identically while every record we gossiped
   ;; advertised `:carries []`. The whole "find your slice of the network
   ;; without a discovery protocol" story did nothing.
   :membership (m/make-state id seeds (assoc (or membership {})
                                             :authority-id authority-id
                                             :addresses (vec addresses)
                                             :topics topics
                                             :carries (cond
                                                        carries (netz.topics/normalise carries)
                                                        relay? #{netz.topics/everything}
                                                        :else #{})))
   :dissemination (assoc (d/make-state id topics
                                       (assoc (or dissemination {})
                                              :authority-id authority-id))
                         :carries (cond
                                    carries (netz.topics/normalise carries)
                                    relay? #{netz.topics/everything}
                                    :else #{}))
   :content (c/make-state id (or blocks {}) (or content {}))
   :sync (netz.sync/make-state id (or sync {}))})

(defn handler
  [state event ctx]
  (let [;; Membership first: its connection set is dissemination's input.
        ms (assoc (:membership state) :rng (:rng state) :id (:id state))
        {ms' :state ma :actions} (m/handler ms event ctx)

        peers (keys (:connections ms'))

        ds (assoc (:dissemination state) :rng (:rng ms') :id (:id state))
        [ds d-sync] (d/sync-peers ds peers (:now ctx))
        {ds' :state da :actions} (d/handler ds event ctx)

        cs (assoc (:content state) :rng (:rng ds') :id (:id state))
        [cs c-sync] (c/sync-peers cs peers)
        {cs' :state ca :actions} (c/handler cs event ctx)

        ss (:sync state)
        topic (get-in event [:payload :topic])
        {ss' :state sa :actions}
        (sync/handler ss event ctx)

        ;; A sync profile commits application state first. Its transport
        ;; frontiers then advance dissemination's ordered application cursor,
        ;; releasing only the newer contiguous live suffix.
        [ds'' committed sa']
        (reduce (fn [[ds actions external] action]
                  (if (= :sync-committed (first action))
                    (let [[ds released] (d/sync-committed ds (second action))]
                      [ds (into actions released) external])
                    [ds actions (conj external action)]))
                [ds' [] []]
                sa)]
    {:state (assoc state
                   :membership (dissoc ms' :rng)
                   :dissemination (dissoc ds'' :rng)
                   :content (dissoc cs' :rng)
                   :sync ss'
                   :rng (:rng cs'))
     :actions (vec (concat ma d-sync da c-sync ca sa' committed))}))

;; =============================================================================
;; Inspection
;; =============================================================================

(defn connections [state] (keys (get-in state [:membership :connections])))

(defn delivered [state] (get-in state [:dissemination :delivered]))

(defn stats [state]
  (merge (get-in state [:membership :stats])
         (get-in state [:dissemination :stats])
         (get-in state [:content :stats])))

(defn have?
  "Do we hold `k` locally?"
  [state k]
  (c/have? (:content state) k))

(defn blocks [state] (get-in state [:content :blocks]))

(defn synced-value [state topic]
  (sync/application (:sync state) topic))

(defn sync-status [state topic]
  (sync/status (:sync state) topic))
