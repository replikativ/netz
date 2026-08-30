(ns netz.pubsub-test
  "The claim under test: moving `kabel.pubsub` onto the peer-to-peer overlay is
  a SUBSTITUTION, not a rewrite.

  The same strategy object, the same `subscribe!`/`publish!` calls, the same
  assertions — run twice, once over a direct connection and once over the
  overlay. If a strategy cannot tell which transport carried it, then
  konserve-sync, datahike's tx-broadcast and spindel's signal-sync become
  federated by a config key, because all three already express both paths
  through `PSyncStrategy`."
  (:require [clojure.test :refer [deftest testing is]]
            [kabel.http-kit :as http-kit]
            [netz.identity :as id]
            [kabel.peer :as peer]
            [kabel.pubsub :as pubsub]
            [netz.apply-owner :as apply-owner]
            [netz.pubsub :as pso]
            [kabel.pubsub.protocol :as proto]
            [netz.overlay.runtime :as rt]
            [netz.dissemination :as d]
            [superv.async :refer [S <?? <? go-loop-try]]
            [clojure.core.async :as async :refer [timeout chan put! close!]]))

(def ^:private port-counter (atom 49200))
(defn- unique-port [] (swap! port-counter inc))

(defn- wait-for [ms pred]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (cond (pred) true
            (> (System/currentTimeMillis) deadline) false
            :else (do (<?? S (timeout 25)) (recur))))))

(defn- done [v] (let [c (chan 1)] (put! c v) (close! c) c))

(defn- activate-recovery-topic! [peer installed-subs topic opts]
  (swap! @installed-subs assoc topic opts)
  (swap! (get-in @peer [:pubsub :netz/recovery-control])
         assoc-in [:topics topic]
         {:active? true :intent-generation 1 :dirty-generation 0
          :pending-sources #{} :gated? false
          :status {:status :joining}}))

(defn- activate-managed-topic!
  [peer installed-subs topic opts]
  (let [generation 1
        state (apply-owner/make-state
               {:topic topic :profile "kabel/psync-strategy/0"
                :generation generation :live-transport :overlay
                :phase :joining})
        state (:state
               (apply-owner/transition
                state {:type :sync/begin :generation generation
                       :sync-id [:initial generation]}))]
    (swap! @installed-subs assoc topic opts)
    (swap! (get-in @peer [:pubsub :netz/recovery-control])
           assoc-in [:topics topic]
           {:active? true :intent-generation generation :dirty-generation 0
            :pending-sources #{} :gated? true :owner state
            :status {:status :joining}})))

(defn- recording-strategy
  "Records what it was handed. Deliberately the SAME implementation under both
  transports — the point is that it cannot tell them apart."
  [applied]
  (reify proto/PSyncStrategy
    (-init-client-state [_] (done {:since 0}))
    (-handshake-items [_ _] (done []))
    (-apply-handshake-item [_ _] (done {:ok true}))
    (-apply-publish [_ payload]
      (swap! applied conj payload)
      (done {:ok true}))))

;; =============================================================================
;; The seam itself
;; =============================================================================

(deftest absent-transport-is-todays-behaviour
  (testing "the released API is untouched when nobody opts in"
    ;; `:direct` is not a mode anyone selects — it is what happens when the
    ;; :transport key is absent, which is every existing consumer.
    (let [p (peer/client-peer S "test-peer" identity identity)]
      (is (nil? (get-in (:pubsub @p) [:transport]))
          "a fresh peer must not have a transport installed")
      (pubsub/set-transport! p {:publish! (fn [_ _ _] :routed)})
      (is (= :routed (pubsub/publish! p :t "x"))
          "publish! must delegate to an installed transport")
      (pubsub/set-transport! p nil)
      (is (nil? (get-in (:pubsub @p) [:transport]))
          "removing the transport must restore the direct path"))))

(deftest overlay-transport-rejects-identity-less-direct-live
  (let [p (peer/client-peer S "test-peer" identity identity)
        transport (pso/transport S p nil (atom {}) (atom {})
                                 (fn [_ _]))
        result (<?? S ((:receive-publish! transport)
                       p :t {:v 1} {:principal :legacy}))]
    (is (some? (:error result)))
    (is (= :netz/direct-live-disabled
           (:type (ex-data (:error result)))))))

(deftest kabel-unsubscribe-dispatches-through-netz-lifecycle
  (let [p (atom {:pubsub {}})
        withdrawn (atom [])
        drained (atom [])]
    (with-redefs [rt/set-deliver! (fn [_ _] true)
                  rt/set-state-sync! (fn [_ _] true)
                  rt/on-stop! (fn [_ _] true)
                  rt/unsubscribe-topics-ordered!
                  (fn [_ topics]
                    (swap! withdrawn conj topics)
                    (done true))
                  pubsub/direct-unsubscribe!
                  (fn [_ topics _]
                    (swap! drained conj topics)
                    (done {:ok true}))]
      (pso/install! S p {:peer-id :self})
      (is (= {:ok true} (<?? S (pubsub/unsubscribe! p #{:t}))))
      (is (= [#{:t}] @withdrawn))
      (is (= [#{:t}] @drained)))))

(deftest concurrent-subscribes-have-one-lifecycle-winner
  (let [lock (doto (chan 1) (async/offer! :available))
        control (atom {:topics {} :lifecycle-lock lock :stopped? false})
        subs (atom {})
        first-result (chan 1)
        direct-calls (atom 0)
        transport (pso/transport S (atom {}) :running subs (atom {})
                                 (fn [& _]) control)]
    (with-redefs [rt/subscribe-topics! (fn [_ _] true)
                  pubsub/direct-subscribe!
                  (fn [_ _ _]
                    (swap! direct-calls inc)
                    first-result)]
      (let [winner ((:subscribe! transport) (atom {}) #{:t} {:owner :first})
            _ (is (wait-for 1000 #(= 1 @direct-calls)))
            loser ((:subscribe! transport) (atom {}) #{:t} {:owner :second})]
        (<?? S (timeout 100))
        (is (= 1 @direct-calls)
            "the second call must wait outside Kabel's reservation")
        (put! first-result {:ok true})
        (is (= true (:ok (<?? S winner))))
        (is (= :netz/already-subscribed (:error (<?? S loser))))
        (is (= 1 @direct-calls))
        (is (= :first (get-in @subs [:t :owner])))
        (is (true? (get-in @control [:topics :t :active?])))))))

(deftest a-failed-subscribe-rolls-back-overlay-interest
  (let [lock (doto (chan 1) (async/offer! :available))
        control (atom {:topics {} :lifecycle-lock lock :stopped? false})
        subs (atom {})
        withdrawn (atom [])
        transport (pso/transport S (atom {}) :running subs (atom {})
                                 (fn [& _]) control)]
    (with-redefs [rt/subscribe-topics! (fn [_ _] true)
                  rt/unsubscribe-topics-ordered! (fn [_ topics]
                                                   (swap! withdrawn conj topics)
                                                   (done true))
                  pubsub/direct-subscribe! (fn [_ _ _]
                                             (done {:error :no-owner}))]
      (let [result (<?? S ((:subscribe! transport)
                           (atom {}) #{:t} {:owner :failed}))]
        (is (= :no-owner (:error result)))
        (is (true? (:interest-rollback-accepted? result)))
        (is (= [#{:t}] @withdrawn))
        (is (empty? @subs))
        (is (false? (get-in @control [:topics :t :active?])))))))

(deftest stop-wins-over-a-simultaneously-ready-subscribe-result
  (let [lock (doto (chan 1) (async/offer! :available))
        stop-signal (chan)
        out (chan 1)
        control (atom {:topics {} :lifecycle-lock lock
                       :stop-signal stop-signal :stopped? false})
        transport (pso/transport S (atom {}) :running (atom {}) (atom {})
                                 (fn [& _]) control)]
    (with-redefs [rt/subscribe-topics! (fn [_ _] true)
                  rt/unsubscribe-topics-ordered! (fn [_ _] (done false))
                  pubsub/subscriptions (fn [_] {:t {:out out}})
                  pubsub/direct-subscribe!
                  (fn [_ _ _]
                    ;; Make stop and successful attempt ready before transport's
                    ;; alts resumes. Either selection must settle as stopped.
                    (swap! control
                           (fn [state]
                             (-> state
                                 (assoc :stopped? true)
                                 (assoc-in [:topics :t :active?] false)
                                 (assoc-in [:topics :t :status]
                                           {:status :stopped}))))
                    (close! stop-signal)
                    (done {:ok true}))]
      (let [result (<?? S ((:subscribe! transport) (atom {}) #{:t} {}))]
        (is (= :netz/stopped (:error result)))
        (is (= :stopped (get-in @control [:topics :t :status :status])))
        (is (false? (async/>!! out :late-generation))
            "a Kabel success concurrent with stop must be retired")))))

;; =============================================================================
;; The same scenario, both transports
;; =============================================================================

(defn- run-direct
  "Two peers, one connection: today's pub/sub. The server owns the topic and
  publishes; the client subscribes."
  [applied strategy]
  (let [port (unique-port)
        url (str "ws://localhost:" port)
        server (peer/server-peer S (http-kit/create-http-kit-handler! S url "server")
                                 "server"
                                 (pubsub/make-pubsub-peer-middleware {}) identity)]
    (<?? S (peer/start server))
    (try
      (pubsub/register-topic! server :t {:strategy strategy})
      (let [client (peer/client-peer S "client"
                                     (pubsub/make-pubsub-peer-middleware {}) identity)]
        (<?? S (peer/connect S client url))
        (<?? S (pubsub/subscribe! client #{:t} {:strategies {:t strategy}}))
        (<?? S (timeout 500))
        (<?? S (pubsub/publish! server :t {:hello :world}))
        (is (wait-for 5000 #(seq @applied)) "direct: nothing was ever delivered")
        nil)
      (finally (<?? S (peer/stop server))))))

(defn- run-overlay
  "The same two peers and the same calls — but the middleware now carries the
  overlay as well, so the handshake stays point-to-point while the publish is
  disseminated. Returns the two runtimes for inspection."
  [applied strategy]
  (let [port (unique-port)
        url (str "ws://localhost:" port)
        server-kp (<?? S (id/generate-identity))
        client-kp (<?? S (id/generate-identity))
        server-id (id/peer-id (:genesis server-kp))
        client-id (id/peer-id (:genesis client-kp))
        [server-ov install-server!] (rt/deferred-middleware)
        ;; Composed, not replaced. `comp` applies right-to-left, so the overlay
        ;; sees the raw socket and passes everything it does not recognise
        ;; through to pub/sub — which is exactly the division of labour: the
        ;; overlay owns its own frames, pub/sub owns the handshake.
        server (peer/server-peer S (http-kit/create-http-kit-handler! S url server-id)
                                 server-id
                                 (comp (pubsub/make-pubsub-peer-middleware {}) server-ov)
                                 identity)
        server-run (<?? S (rt/start! S server {:identity server-kp
                                               :addresses [url]
                                               :topics #{}}))]
    (install-server! (:middleware server-run))
    (<?? S (peer/start server))
    (try
      (let [[client-ov install-client!] (rt/deferred-middleware)
            client (peer/client-peer S client-id
                                     (comp (pubsub/make-pubsub-peer-middleware {})
                                           client-ov)
                                     identity)
            client-run (<?? S (rt/start! S client
                                         {:identity client-kp
                                          :addresses []
                                          :topics #{}
                                          :seeds [{:peer-id server-id
                                                   :addresses [url]
                                                   :group "seed"}]}))]
        (install-client! (:middleware client-run))
        (pso/install! S server server-run)
        (pso/install! S client client-run)
        (pubsub/register-topic! server :t {:strategy strategy})
        (<?? S (peer/connect S client url))
        (is (wait-for 8000 #(contains? (rt/connections client) server-id))
            "the overlay never connected, so the comparison would be vacuous")
        (<?? S (pubsub/subscribe! client #{:t} {:strategies {:t strategy}}))
        (<?? S (timeout 500))
        (<?? S (pubsub/publish! server :t {:hello :world}))
        (is (wait-for 8000 #(seq @applied)) "overlay: nothing was ever delivered")
        {:client client-run :server server-run})
      (finally (<?? S (peer/stop server))))))

(deftest a-publish-reaches-the-strategy-on-either-transport
  (doseq [[label run] [[:direct run-direct] [:overlay run-overlay]]]
    (testing (name label)
      (let [applied (atom [])
            strategy (recording-strategy applied)
            runs (run applied strategy)]
        ;; The identical assertion under both transports. That it is identical
        ;; IS the result.
        (is (= [{:hello :world}] @applied)
            (str (name label) ": the strategy saw the wrong payloads"))
        (when runs
          ;; The atom is shared by both peers' strategies, so "something was
          ;; applied" alone would also be satisfied by a purely local delivery.
          ;; Assert the payload actually CROSSED.
          (let [c (:dissemination (rt/overlay-state (:client runs)))
                sv (:dissemination (rt/overlay-state (:server runs)))]
            (is (= 1 (get-in sv [:stats :published]))
                "the server should have published exactly once")
            (is (= 1 (get-in c [:stats :delivered]))
                "the CLIENT must be the peer that delivered — otherwise the
                 payload never left the publisher")
            (is (zero? (get-in sv [:stats :delivered] 0))
                "the publisher is not subscribed, so it must not deliver to
                 itself — this is the interest filter doing its job")))))))

;; =============================================================================
;; The horizon is a re-handshake
;; =============================================================================

(deftest a-gap-past-the-horizon-asks-the-application-for-a-state-sync
  ;; The transport's honest statement is "these messages no longer exist
  ;; anywhere, so no repair of mine can help you". That is a transport fact.
  ;; The application answers with a differential state sync, which is an
  ;; application fact. Neither has to understand the other's semantics — the
  ;; action is the whole interface between them.
  (let [;; A publisher whose store keeps only the last 5 of 20 messages, so
        ;; everything below seq 15 has fallen out of repair range.
        pub (reduce (fn [s i] (first (d/publish s :t i)))
                    (d/make-state :pub #{:t} {:store-size 5})
                    (range 20))
        ;; A peer that has seen only message 0 — hopelessly behind.
        behind (d/mark-seen (d/make-state :behind #{:t}) :pub 0 :t 0)
        {acts :actions} (d/handler behind
                                   {:type :message
                                    :from :pub
                                    :payload {:type :have
                                              :summary (d/summary pub)
                                              :horizon (d/horizon pub)}}
                                   {:now 0})
        syncs (filter #(= :state-sync (first %)) acts)]
    (is (= 1 (count syncs))
        "a stranded peer must be told, or it asks for the unfetchable forever")
    (is (= [:state-sync :pub [{:origin :pub :epoch 0 :topic :t
                               :missing-below 15 :through 19}]]
           (first syncs))
        "the signal names who reported it and exactly what cannot be repaired")
    ;; Both, not either: the reachable part is still repaired by gossip and
    ;; only the unreachable part escalates. A design that escalated the whole
    ;; gap would do a full state sync for a peer that missed three messages.
    (is (seq (filter #(and (= :send (first %)) (= :want (:type (nth % 2)))) acts))
        "the part that CAN still be repaired must still be requested"))

  (testing "a caught-up peer is never asked to state sync"
    (let [pub (reduce (fn [s i] (first (d/publish s :t i)))
                      (d/make-state :pub #{:t} {:store-size 5})
                      (range 20))
          current (reduce (fn [s i] (d/mark-seen s :pub 0 :t i))
                          (d/make-state :current #{:t}) (range 20))
          {acts :actions} (d/handler current
                                     {:type :message
                                      :from :pub
                                      :payload {:type :have
                                                :summary (d/summary pub)
                                                :horizon (d/horizon pub)}}
                                     {:now 0})]
      (is (empty? (filter #(= :state-sync (first %)) acts))))))

(deftest re-handshake-reuses-the-join-path
  ;; Recovery and join are the same problem — "give me the current state" — so
  ;; they must be the same code, or the rarely-exercised one rots.
  (let [inits (atom 0)
        completes (atom 0)
        applied (atom [])
        strategy (reify proto/PSyncStrategy
                   (-init-client-state [_] (swap! inits inc) (done {:since 0}))
                   (-handshake-items [_ _] (done []))
                   (-apply-handshake-item [_ _] (done {:ok true}))
                   (-apply-publish [_ p] (swap! applied conj p) (done {:ok true})))
        port (unique-port)
        url (str "ws://localhost:" port)
        server (peer/server-peer S (http-kit/create-http-kit-handler! S url "srv")
                                 "srv" (pubsub/make-pubsub-peer-middleware {}) identity)]
    (<?? S (peer/start server))
    (try
      (pubsub/register-topic! server :t {:strategy strategy})
      (let [client (peer/client-peer S "cli"
                                     (pubsub/make-pubsub-peer-middleware {}) identity)]
        (<?? S (peer/connect S client url))
        (<?? S (pubsub/subscribe! client #{:t} {:strategies {:t strategy}}))
        (is (wait-for 5000 #(= 1 @inits)) "the initial handshake never ran")
        (is (wait-for 5000 #(true? (:handshake-complete?
                                    (pubsub/subscription client :t))))
            "recovery must not replace an active v0 generation")
        ;; Now the horizon fires. No new message type, no new state: the peer
        ;; simply asks again, and -init-client-state bounds what that costs.
        ;; The opts the subscription was made with, as `install!` remembers
        ;; them — a re-handshake must reproduce the ORIGINAL subscription.
        (let [subs (atom {:t {:strategies {:t strategy}
                              :on-handshake-complete (fn [_] (swap! completes inc))}})]
          ;; nil `from`: no overlay registry in this direct-only fixture, so it
          ;; falls back to [:pubsub :out] -- the historical path.
          (pso/re-handshake! S client subs nil))
        (is (wait-for 5000 #(= 2 @inits))
            "a horizon gap must re-run the SAME differential sync the join used")
        (is (wait-for 5000 #(pos? @completes))
            "a re-handshake must not silently drop :on-handshake-complete")
        ;; The strategies survive the round trip — a re-handshake that lost
        ;; them would silently stop applying anything afterwards.
        (is (= #{:t} (set (keys (get-in @client [:pubsub :subscriptions]))))))
      (finally (<?? S (peer/stop server))))))

;; =============================================================================
;; A publish must not overtake the handshake it depends on
;; =============================================================================

(deftest publishes-are-held-until-the-handshake-completes
  ;; The handshake is point-to-point and publishes are disseminated, so they
  ;; arrive over different paths -- in a mesh, from different peers -- with no
  ;; ordering relation. A strategy whose -apply-publish is a DELTA on handshake
  ;; state (datahike tx-broadcast, spindel -apply-delta, konserve-sync key
  ;; writes) is silently wrong if a publish lands first.
  (let [applied (atom [])
        peer (atom {:pubsub {:subscriptions
                             {:t {:strategy (recording-strategy applied)
                                  :handshake-complete? false}}}})
        subs (atom {:t {}})
        pending (atom {})
        p1 {:origin :writer :epoch 0 :seq 0 :topic :t :payload {:n 1}}
        p2 {:origin :writer :epoch 0 :seq 1 :topic :t :payload {:n 2}}]
    (pso/deliver-to-strategy! S peer subs pending :t {:n 1} {:publication p1})
    (pso/deliver-to-strategy! S peer subs pending :t {:n 2} {:publication p2})
    (<?? S (timeout 200))
    (is (empty? @applied)
        "nothing may be applied before the base state exists")
    (is (= 2 (count (get @pending :t))) "both must be held, not dropped")

    ;; Handshake completes.
    (swap! peer assoc-in [:pubsub :subscriptions :t :handshake-complete?] true)
    (let [drained (<?? S (pso/drain-pending! S peer subs pending :t))]
      (is (= [p1 p2] (:committed-publications drained))
          "the snapshot drain retains identities for application receipts"))
    (<?? S (timeout 200))
    (is (= [{:n 1} {:n 2}] @applied)
        "the buffer must drain in ARRIVAL order")

    (testing "and afterwards delivery is immediate"
      (pso/deliver-to-strategy! S peer subs pending :t {:n 3})
      (<?? S (timeout 200))
      (is (= [{:n 1} {:n 2} {:n 3}] @applied)))))

(deftest a-gated-delivery-obeys-the-asynchronous-result-contract
  ;; The installed delivery owner always awaits this result. Returning the
  ;; buffered value directly kills that owner when `<?` treats it as a channel.
  (let [peer (atom {:pubsub {:subscriptions
                             {:t {:strategy (recording-strategy (atom []))
                                  :handshake-complete? false}}}})
        result (<?? S (pso/deliver-to-strategy!
                       S peer (atom {:t {}}) (atom {}) :t {:n 1}))]
    (is (map? result))
    (is (= [{:n 1}] (get result :t)))))

(deftest pending-values-are-owned-by-intent-and-recovery-generation
  (let [applied (atom [])
        peer (atom {:pubsub {:subscriptions
                             {:t {:strategy (recording-strategy applied)
                                  :handshake-complete? false}}}})
        subs (atom {:t {}})
        pending (atom {})
        control (atom {:topics
                       {:t {:active? true :intent-generation 1
                            :dirty-generation 1 :gated? true}}})]
    (<?? S (pso/deliver-to-strategy!
            S peer subs pending :t :old {:desynchronized control}))
    (swap! control assoc-in [:topics :t]
           {:active? true :intent-generation 2
            :dirty-generation 0 :gated? true})
    (<?? S (pso/deliver-to-strategy!
            S peer subs pending :t :new {:desynchronized control}))

    (<?? S (pso/drain-pending! S peer subs pending :t
                               {:intent-generation 2
                                :dirty-generation 0}))
    (is (= [:new] @applied)
        "a new snapshot drains only values admitted by its own intent")
    (<?? S (pso/drain-pending! S peer subs pending :t
                               {:intent-generation 1
                                :dirty-generation 1}))
    (is (= [:new :old] @applied)
        "taking one generation preserves, rather than deletes, another")))

(deftest the-pre-handshake-buffer-is-bounded
  ;; The sender controls the rate, so the buffer is attacker-influenced.
  ;; Dissemination has already marked these messages seen, so overflow must
  ;; request application state sync rather than pretending message repair can
  ;; recover them.
  (let [applied (atom [])
        overflows (atom [])
        peer (atom {:pubsub {:subscriptions
                             {:t {:strategy (recording-strategy applied)
                                  :handshake-complete? false}}}})
        subs (atom {:t {}})
        pending (atom {})
        desynchronized (atom #{})
        opts {:max-pending 4
              :desynchronized desynchronized
              :on-overflow #(do (swap! desynchronized conj %)
                                (swap! overflows conj %))}]
    (dotimes [i 10]
      (pso/deliver-to-strategy! S peer subs pending :t {:n i}
                                opts))
    (is (= 4 (count (get @pending :t))))
    (is (= [{:n 6} {:n 7} {:n 8} {:n 9}] (vec (get @pending :t)))
        "the oldest are dropped, so the newest state survives")
    (is (= 6 (count @overflows)) "every application-level loss is signalled")
    (is (= #{:t} @desynchronized))

    (testing "later deltas stay behind the repair gate"
      (swap! peer assoc-in [:pubsub :subscriptions :t :handshake-complete?] true)
      (pso/deliver-to-strategy! S peer subs pending :t {:n 10} opts)
      (<?? S (timeout 100))
      (is (empty? @applied))
      (is (= [{:n 7} {:n 8} {:n 9} {:n 10}]
             (vec (get @pending :t)))))

    (testing "a replacement snapshot may release its captured suffix"
      (swap! desynchronized disj :t)
      (is (:ok (<?? S (pso/drain-pending! S peer subs pending :t))))
      (is (= [{:n 7} {:n 8} {:n 9} {:n 10}] @applied)))))

(deftest a-topic-owner-is-not-made-to-wait
  ;; A peer that REGISTERED a topic has no subscription and so no handshake to
  ;; wait for. Buffering there would stall a publisher forever.
  (let [applied (atom [])
        peer (atom {:pubsub {:topics {:t {:strategy (recording-strategy applied)}}}})]
    (pso/deliver-to-strategy! S peer (atom {}) (atom {}) :t {:n 1})
    (<?? S (timeout 200))
    (is (= [{:n 1}] @applied))))

(deftest deliveries-are-applied-in-order-even-when-the-strategy-is-slow
  ;; `deliver-to-strategy!` returns a go block and the runtime's `:deliver`
  ;; action discarded it, so two publishes on one topic started two concurrent
  ;; blocks that raced inside `-apply-publish`. Ordering the pre-handshake
  ;; buffer fixed the JOIN and not the steady state.
  ;;
  ;; The strategy here takes a varying amount of time on purpose: with
  ;; concurrent blocks the later, faster publish overtakes the earlier one, and
  ;; the assertion below is exactly what a delta-applying consumer needs.
  (let [applied (atom [])
        slow-strategy
        (reify proto/PSyncStrategy
          (-init-client-state [_] (done {:since 0}))
          (-handshake-items [_ _] (done []))
          (-apply-handshake-item [_ _] (done {:ok true}))
          (-apply-publish [_ p]
            (let [c (chan 1)]
              (async/go
                ;; earlier messages are slower, so any concurrency reorders
                (async/<! (timeout (max 0 (- 40 (* 4 (:n p))))))
                (swap! applied conj (:n p))
                (put! c {:ok true})
                (close! c))
              c)))
        peer (atom {:pubsub {:subscriptions {:t {:strategy slow-strategy
                                                 :handshake-complete? true}}}})
        subs (atom {:t {}})
        pending (atom {})
        deliveries (chan 64)]
    ;; The same single-consumer arrangement `install!` builds.
    (go-loop-try S [d (<? S deliveries)]
                 (when d
                   (let [[topic payload] d]
                     (<? S (pso/deliver-to-strategy! S peer subs pending topic payload)))
                   (recur (<? S deliveries))))
    (dotimes [i 8] (async/offer! deliveries [:t {:n i}]))
    (is (wait-for 8000 #(= 8 (count @applied))) "not everything was applied")
    (is (= (range 8) (seq @applied))
        "applications must follow arrival order, not completion order")))

(deftest recovery-signals-are-coalesced-and-never-overlap-generations
  (let [state-sync! (atom nil)
        stop! (atom nil)
        installed-subs (atom nil)
        gates (atom [])
        calls (atom [])
        active (atom 0)
        maximum-active (atom 0)
        p (atom {:pubsub {}})
        running {:peer-id :self}]
    (with-redefs [rt/set-deliver! (fn [_ _] true)
                  rt/set-state-sync! (fn [_ f] (reset! state-sync! f) true)
                  rt/on-stop! (fn [_ f] (reset! stop! f) true)
                  rt/unsubscribe-topics-ordered! (fn [_ _] (done true))
                  pubsub/set-transport! (fn [_ _] true)
                  pso/transport (fn [_ _ _ subs _ _ _]
                                  (reset! installed-subs subs)
                                  {})
                  pso/re-handshake!
                  (fn [_ _ _ source {:keys [topics]}]
                    (let [gate (chan 1)
                          result (chan 1)]
                      (swap! calls conj {:source source :topics topics})
                      (swap! active inc)
                      (swap! maximum-active max @active)
                      (swap! gates conj gate)
                      (async/go
                        (let [value (async/<! gate)]
                          (swap! active dec)
                          (async/>! result value)
                          (close! result)))
                      result))]
      (pso/install! S p running)
      (activate-recovery-topic! p installed-subs :topic-a {})

      ;; Establish the first generation before producing the storm. This makes
      ;; the test independent of scheduler timing while still exercising the
      ;; atomic take of pending sources.
      (@state-sync! :source-a [{:topic :topic-a}])
      (is (wait-for 3000 #(= 1 (count @calls))))
      (is (= {:source :source-a :topics #{:topic-a}} (first @calls)))
      (is (= 1 @active))

      ;; A signal arriving while the first generation is still running must
      ;; wait. Ten thousand newer signals for the SAME topic occupy one source
      ;; set and one sliding control slot. The old completion must not clear
      ;; their newer application gate.
      (dotimes [_ 10000]
        (@state-sync! :source-c [{:topic :topic-a}]))
      (<?? S (timeout 100))
      (is (= 1 (count @calls)))
      (put! (first @gates) {:ok true})
      (is (wait-for 3000 #(= 2 (count @calls))))
      (is (= {:source :source-c :topics #{:topic-a}} (second @calls)))
      (is (= 1 @active))
      (is (= 1 @maximum-active))
      (is (= :recovering (:status (pso/recovery-status p :topic-a)))
          "an old successful drain cannot publish readiness for newer work")

      (put! (second @gates) {:ok true})
      (is (wait-for 3000 #(= :source-c
                             (:source (pso/recovery-status p :topic-a)))))
      (is (zero? @active))
      (is (= {:status :ready :source :source-c}
             (select-keys (pso/recovery-status p :topic-a)
                          [:status :source])))
      (@stop!))))

(deftest recovery-completes-only-after-the-replacement-drain
  (let [strategy (recording-strategy (atom []))
        out (chan 1)
        held {:t {:strategy strategy :out out}}
        subs (atom {:t {:strategies {:t strategy}}})
        handshake-callback (atom nil)
        drain-completion (atom nil)]
    (with-redefs [pubsub/subscriptions (fn [_] held)
                  pubsub/direct-unsubscribe! (fn [_ _ _] (done {:ok true}))
                  pubsub/direct-subscribe!
                  (fn [_ _ opts]
                    (reset! handshake-callback (:on-handshake-complete opts))
                    (done {:ok true}))
                  rt/connection-out (fn [_ _] out)]
      (let [result (pso/re-handshake!
                    S (atom {}) subs :source
                    {:topics #{:t}
                     :timeout-ms 3000
                     :enqueue-drain!
                     (fn [_ _ completion _valid? _owner]
                       (reset! drain-completion completion))})]
        (is (wait-for 1000 #(some? @handshake-callback)))
        (is (nil? (async/poll! result))
            "request enqueue must not complete the recovery generation")
        (@handshake-callback :t)
        (is (wait-for 1000 #(some? @drain-completion)))
        (is (nil? (async/poll! result))
            "handshake-ready must still wait for buffered application")
        (@drain-completion {:ok true})
        (is (= {:ok true} (<?? S result)))))))

(deftest recovery-normalizes-failure-and-retires-the-v0-connection
  (let [strategy (recording-strategy (atom []))]
    (doseq [[label operation expected]
            [[:closed (fn [] (doto (chan) close!)) :netz/recovery-closed]
             [:exception (fn [] (done (ex-info "boom" {})))
              :netz/recovery-exception]
             [:timeout (fn [] (chan)) :netz/recovery-timeout]]]
      (testing (name label)
        (let [out (chan 1)
              held {:t {:strategy strategy :out out}}]
          (with-redefs [pubsub/subscriptions (fn [_] held)
                        pubsub/direct-unsubscribe! (fn [_ _ _] (operation))
                        rt/connection-out (fn [_ _] nil)]
            (let [result (<?? S (pso/re-handshake!
                                 S (atom {}) (atom {}) nil
                                 {:topics #{:t} :timeout-ms 25}))]
              (is (= expected (:error result)))
              (is (false? (async/>!! out :late-frame))
                  "failure must retire the connection before another generation"))))))))

(deftest a-superseded-recovery-preserves-its-source-for-the-successor
  (let [strategy (recording-strategy (atom []))
        out (chan 1)
        held {:t {:strategy strategy :out out}}]
    (with-redefs [pubsub/subscriptions (fn [_] held)
                  pubsub/direct-unsubscribe! (fn [_ _ _] (done {:ok true}))
                  pubsub/direct-subscribe!
                  (fn [_ _ opts]
                    ((:on-handshake-complete opts) :t)
                    (done {:ok true}))]
      (let [result
            (<?? S (pso/re-handshake!
                    S (atom {}) (atom {:t {:strategies {:t strategy}}}) nil
                    {:topics #{:t}
                     :timeout-ms 1000
                     :valid? (constantly true)
                     :drain-valid? (constantly :superseded)
                     :drain-owner {:intent-generation 1 :dirty-generation 1}
                     :enqueue-drain!
                     (fn [_ _ completion _ _]
                       (completion {:error :netz/recovery-superseded}))}))]
        (is (= :netz/recovery-superseded (:error result)))
        (is (true? (async/>!! out :still-usable))
            "the successor must be able to reuse the same source connection")
        (close! out)))))

(deftest a-failed-recovery-does-not-kill-the-recovery-owner
  (let [state-sync! (atom nil)
        stop! (atom nil)
        installed-subs (atom nil)
        failures (atom [])
        calls (atom [])
        p (atom {:pubsub {}})]
    (with-redefs [rt/set-deliver! (fn [_ _] true)
                  rt/set-state-sync! (fn [_ f] (reset! state-sync! f) true)
                  rt/on-stop! (fn [_ f] (reset! stop! f) true)
                  rt/unsubscribe-topics-ordered! (fn [_ _] (done true))
                  pubsub/set-transport! (fn [_ _] true)
                  pso/transport (fn [_ _ _ subs _ _ _]
                                  (reset! installed-subs subs)
                                  {})
                  pso/re-handshake!
                  (fn [_ _ _ source _]
                    (swap! calls conj source)
                    (if (= source :bad-source)
                      (done (ex-info "failed generation" {}))
                      (done {:ok true})))]
      (pso/install! S p {:peer-id :self})
      (activate-recovery-topic!
       p installed-subs :t
       {:on-recovery-failed #(swap! failures conj [%1 %2])})
      (@state-sync! :bad-source [{:topic :t}])
      (is (wait-for 3000 #(= :failed (:status (pso/recovery-status p :t)))))
      (is (= :t (ffirst @failures))
          "terminal failure is explicitly visible to the subscription owner")
      (@state-sync! :good-source [{:topic :t}])
      (is (wait-for 3000 #(= {:status :ready :source :good-source}
                             (select-keys (pso/recovery-status p :t)
                                          [:status :source]))))
      (is (= [:bad-source :good-source] @calls))
      (@stop!))))

(deftest only-exact-application-success-emits-a-committed-publication-receipt
  (let [deliver! (atom nil)
        stop! (atom nil)
        installed-subs (atom nil)
        results (atom [{:ok true} {:error :apply-failed}])
        committed (atom [])
        applications (atom 0)
        p (atom {:pubsub {}})]
    (with-redefs [rt/set-deliver! (fn [_ f] (reset! deliver! f) true)
                  rt/set-state-sync! (fn [_ _] true)
                  rt/on-stop! (fn [_ f] (reset! stop! f) true)
                  rt/application-committed!
                  (fn [_ publication]
                    (swap! committed conj publication)
                    true)
                  pubsub/set-transport! (fn [_ _] true)
                  pso/transport (fn [_ _ _ subs _ _ _]
                                  (reset! installed-subs subs)
                                  {})
                  pso/re-handshake! (fn [& _] (done {:error :test/recovery}))
                  pso/deliver-to-strategy!
                  (fn [& _]
                    (swap! applications inc)
                    (let [[result & remaining] @results]
                      (reset! results (vec remaining))
                      (done result)))]
      (pso/install! S p {:peer-id :self})
      (activate-recovery-topic! p installed-subs :t {})
      (let [first-publication {:type :gossip :origin :writer :epoch 0 :seq 0
                               :topic :t :payload :first}
            failed-publication (assoc first-publication :seq 1 :payload :second)]
        (@deliver! :source :t :first first-publication)
        (is (wait-for 3000 #(= 1 @applications)))
        (is (wait-for 3000 #(= [first-publication] @committed)))

        (@deliver! :source :t :second failed-publication)
        (is (wait-for 3000 #(= 2 @applications)))
        (<?? S (timeout 50))
        (is (= [first-publication] @committed)
            "failed application must not enter a later sync checkpoint"))
      (@stop!))))

(deftest installed-apply-owner-serializes-snapshot-and-live-receipts
  (let [deliver! (atom nil)
        stop! (atom nil)
        installed-subs (atom nil)
        enqueue-drain! (atom nil)
        committed (atom [])
        applied (atom [])
        completion (chan 1)
        strategy (recording-strategy applied)
        p (atom {:pubsub {:subscriptions
                          {:t {:strategy strategy
                               :handshake-complete? true}}}})]
    (with-redefs [rt/set-deliver! (fn [_ f] (reset! deliver! f) true)
                  rt/set-state-sync! (fn [_ _] true)
                  rt/on-stop! (fn [_ f] (reset! stop! f) true)
                  rt/application-committed!
                  (fn [_ publication]
                    (swap! committed conj publication)
                    true)
                  pubsub/set-transport! (fn [_ _] true)
                  pso/transport (fn [_ _ _ subs _ enqueue _]
                                  (reset! installed-subs subs)
                                  (reset! enqueue-drain! enqueue)
                                  {})]
      (pso/install! S p {:peer-id :self})
      (activate-managed-topic! p installed-subs :t {:strategy strategy})
      (let [publication {:type :gossip :origin :writer :epoch 0 :seq 0
                         :topic :t :payload :live}
            control (get-in @p [:pubsub :netz/recovery-control])]
        (@deliver! :source :t :live publication)
        (<?? S (timeout 50))
        (is (empty? @applied)
            "live data waits behind the owner-controlled snapshot")
        (is (empty? @committed))

        (let [operation-id (get-in @control
                                   [:topics :t :owner :active-sync])]
          (@enqueue-drain! :t nil #(put! completion %)
                           (constantly true)
                           {:intent-generation 1
                            :dirty-generation 0
                            :operation-id operation-id}))
        (is (= {:ok true} (<?? S completion)))
        (is (= [:live] @applied))
        (is (= [(assoc publication :bytes 0)] @committed))
        (is (= 0 (get-in @control
                         [:topics :t :owner :applied-frontiers
                          [:writer 0 :t]])))
        (is (= :live (get-in @control [:topics :t :owner :phase]))))
      (@stop!))))

(deftest recovery-snapshot-waits-for-an-in-flight-managed-apply
  (let [deliver! (atom nil)
        state-sync! (atom nil)
        stop! (atom nil)
        installed-subs (atom nil)
        enqueue-drain! (atom nil)
        apply-gate (chan 1)
        recovery-gate (chan 1)
        recovery-calls (atom [])
        applications (atom 0)
        strategy (reify proto/PSyncStrategy
                   (-init-client-state [_] (done {}))
                   (-handshake-items [_ _] (done []))
                   (-apply-handshake-item [_ _] (done {:ok true}))
                   (-apply-publish [_ _]
                     (swap! applications inc)
                     apply-gate))
        p (atom {:pubsub {:subscriptions
                          {:t {:strategy strategy
                               :handshake-complete? true}}}})]
    (with-redefs [rt/set-deliver! (fn [_ f] (reset! deliver! f) true)
                  rt/set-state-sync! (fn [_ f] (reset! state-sync! f) true)
                  rt/on-stop! (fn [_ f] (reset! stop! f) true)
                  rt/application-committed! (fn [_ _] true)
                  pubsub/set-transport! (fn [_ _] true)
                  pso/transport (fn [_ _ _ subs _ enqueue _]
                                  (reset! installed-subs subs)
                                  (reset! enqueue-drain! enqueue)
                                  {})
                  pso/re-handshake!
                  (fn [_ _ _ source _]
                    (swap! recovery-calls conj source)
                    recovery-gate)]
      (pso/install! S p {:peer-id :self})
      (activate-managed-topic! p installed-subs :t {:strategy strategy})
      (let [control (get-in @p [:pubsub :netz/recovery-control])
            initial-completion (chan 1)
            initial-operation (get-in @control
                                      [:topics :t :owner :active-sync])]
        (@enqueue-drain! :t nil #(put! initial-completion %)
                         (constantly true)
                         {:intent-generation 1 :dirty-generation 0
                          :operation-id initial-operation})
        (is (= {:ok true} (<?? S initial-completion)))
        (is (= :live (get-in @control [:topics :t :owner :phase])))

        (is (@deliver! :source :t :live
                       {:type :gossip :origin :writer :epoch 0 :seq 0
                        :topic :t :payload :live}))
        (is (wait-for 3000
                      #(some? (get-in @control
                                      [:topics :t :owner :in-flight]))))
        (is (wait-for 3000 #(= 1 @applications)))
        (@state-sync! :repair-source [{:topic :t}])
        (<?? S (timeout 100))
        (is (empty? @recovery-calls)
            "snapshot capture cannot race the in-flight live mutation")

        (put! apply-gate {:ok true})
        (is (wait-for 3000 #(= [:repair-source] @recovery-calls)))
        (is (= 0 (get-in @control
                         [:topics :t :owner :applied-frontiers
                          [:writer 0 :t]])))
        (put! recovery-gate {:error :test/stop-recovery}))
      (@stop!))))

(deftest managed-unsubscribe-waits-for-the-apply-owner-to-close
  (let [deliver! (atom nil)
        stop! (atom nil)
        installed-subs (atom nil)
        enqueue-drain! (atom nil)
        apply-gate (chan 1)
        applications (atom 0)
        receipts (atom [])
        withdrawals (atom 0)
        direct-unsubscribes (atom 0)
        strategy (reify proto/PSyncStrategy
                   (-init-client-state [_] (done {}))
                   (-handshake-items [_ _] (done []))
                   (-apply-handshake-item [_ _] (done {:ok true}))
                   (-apply-publish [_ _]
                     (swap! applications inc)
                     apply-gate))
        p (atom {:pubsub {:subscriptions
                          {:t {:strategy strategy
                               :handshake-complete? true}}}})]
    (with-redefs [rt/set-deliver! (fn [_ f] (reset! deliver! f) true)
                  rt/set-state-sync! (fn [_ _] true)
                  rt/on-stop! (fn [_ f] (reset! stop! f) true)
                  rt/application-committed!
                  (fn [_ publication]
                    (swap! receipts conj publication)
                    true)
                  rt/unsubscribe-topics-ordered!
                  (fn [_ _]
                    (swap! withdrawals inc)
                    (done true))
                  pubsub/set-transport! (fn [_ _] true)
                  pubsub/direct-unsubscribe!
                  (fn [_ _ _]
                    (swap! direct-unsubscribes inc)
                    (done {:ok true}))
                  pso/transport (fn [_ _ _ subs _ enqueue _]
                                  (reset! installed-subs subs)
                                  (reset! enqueue-drain! enqueue)
                                  {})]
      (pso/install! S p {:peer-id :self})
      (activate-managed-topic! p installed-subs :t {:strategy strategy})
      (let [control (get-in @p [:pubsub :netz/recovery-control])
            initial-completion (chan 1)
            initial-operation (get-in @control
                                      [:topics :t :owner :active-sync])]
        (@enqueue-drain! :t nil #(put! initial-completion %)
                         (constantly true)
                         {:intent-generation 1 :dirty-generation 0
                          :operation-id initial-operation})
        (is (= {:ok true} (<?? S initial-completion)))

        (let [publication {:type :gossip :origin :writer :epoch 0 :seq 0
                           :topic :t :payload :live}]
          (is (@deliver! :source :t :live publication))
          (is (wait-for 3000 #(= 1 @applications)))
          (let [unsubscribed (pso/unsubscribe! p #{:t})]
            (<?? S (timeout 100))
            (is (zero? @withdrawals))
            (is (zero? @direct-unsubscribes)
                "unsubscribe cannot overtake the in-flight application")
            (put! apply-gate {:ok true})
            (is (= {:ok true} (<?? S unsubscribed))))

          (is (= [(assoc publication :bytes 0)] @receipts))
          (is (= 1 @withdrawals))
          (is (= 1 @direct-unsubscribes))
          (is (= :closed
                 (get-in @control [:topics :t :owner :phase])))))
      (@stop!))))

(deftest snapshot-drain-emits-receipts-for-the-buffered-publication-identities
  (let [deliver! (atom nil)
        stop! (atom nil)
        installed-subs (atom nil)
        committed (atom [])
        applied (atom [])
        strategy (recording-strategy applied)
        p (atom {:pubsub {:subscriptions
                          {:t {:strategy strategy
                               :handshake-complete? false}}}})]
    (with-redefs [rt/set-deliver! (fn [_ f] (reset! deliver! f) true)
                  rt/set-state-sync! (fn [_ _] true)
                  rt/on-stop! (fn [_ f] (reset! stop! f) true)
                  rt/application-committed!
                  (fn [_ publication]
                    (swap! committed conj publication)
                    true)
                  pubsub/set-transport! (fn [_ _] true)
                  pso/transport (fn [_ _ _ subs _ _ _]
                                  (reset! installed-subs subs)
                                  {})]
      (pso/install! S p {:peer-id :self})
      (activate-recovery-topic! p installed-subs :t {})
      (let [p1 {:type :gossip :origin :writer :epoch 0 :seq 0
                :topic :t :payload :first}
            p2 (assoc p1 :seq 1 :payload :second)]
        (@deliver! :source :t :first p1)
        (<?? S (timeout 50))
        (is (empty? @applied))
        (is (empty? @committed))

        (swap! p assoc-in [:pubsub :subscriptions :t :handshake-complete?] true)
        (swap! (get-in @p [:pubsub :netz/recovery-control])
               assoc-in [:topics :t :gated?] false)
        (@deliver! :source :t :second p2)
        (is (wait-for 3000 #(= [:first :second] @applied)))
        (is (wait-for 3000 #(= [p1 p2] @committed))))
      (@stop!))))

(deftest a-vanished-candidate-never-falls-back-to-an-unrelated-connection
  (let [strategy (recording-strategy (atom []))
        attempted? (atom false)]
    (with-redefs [pubsub/subscriptions (fn [_] {:t {:strategy strategy}})
                  pubsub/direct-unsubscribe! (fn [_ _ _]
                                               (reset! attempted? true)
                                               (done {:ok true}))
                  rt/connection-out (fn [_ _] nil)]
      (let [result (<?? S (pso/re-handshake!
                           S (atom {}) (atom {}) :gone
                           {:topics #{:t} :timeout-ms 100}))]
        (is (= :netz/recovery-source-unavailable (:error result)))
        (is (false? @attempted?)
            "Kabel's peer-wide fallback must not be used for a named source")))))

(deftest explicit-unsubscribe-cancels-intent-after-inflight-recovery
  (let [state-sync! (atom nil)
        stop! (atom nil)
        installed-subs (atom nil)
        deliver! (atom nil)
        recovery-gate (chan 1)
        recoveries (atom 0)
        drains (atom 0)
        applications (atom 0)
        p (atom {:pubsub {}})]
    (with-redefs [rt/set-deliver! (fn [_ f] (reset! deliver! f) true)
                  rt/set-state-sync! (fn [_ f] (reset! state-sync! f) true)
                  rt/on-stop! (fn [_ f] (reset! stop! f) true)
                  rt/unsubscribe-topics-ordered! (fn [_ _] (done true))
                  pubsub/set-transport! (fn [_ _] true)
                  pubsub/direct-unsubscribe! (fn [_ _ _]
                                               (swap! drains inc)
                                               (done {:ok true}))
                  pso/transport (fn [_ _ _ subs _ _ _]
                                  (reset! installed-subs subs)
                                  {})
                  pso/re-handshake! (fn [_ _ _ _ _]
                                      (swap! recoveries inc)
                                      recovery-gate)
                  pso/deliver-to-strategy! (fn [& _]
                                             (swap! applications inc)
                                             (done {:ok true}))]
      (pso/install! S p {:peer-id :self})
      (activate-recovery-topic! p installed-subs :t {})
      (@state-sync! nil [{:topic :t}])
      (is (wait-for 3000 #(= 1 @recoveries)))

      (let [unsubscribed (pso/unsubscribe! p #{:t})]
        (<?? S (timeout 100))
        (is (zero? @drains)
            "unsubscribe waits behind the recovery lifecycle owner")
        (put! recovery-gate {:ok true})
        (is (= {:ok true} (<?? S unsubscribed))))

      (is (= 1 @drains))
      (is (= :closed (:status (pso/recovery-status p :t))))
      (is (not (contains? @@installed-subs :t)))
      (@deliver! :late-source :t {:late true} nil)
      (<?? S (timeout 100))
      (is (zero? @applications)
          "late overlay frames cannot cross an explicit cancellation fence")
      (@state-sync! nil [{:topic :t}])
      (<?? S (timeout 100))
      (is (= 1 @recoveries)
          "a stale horizon signal cannot recreate cancelled intent")
      (@stop!))))

(deftest explicit-unsubscribe-is-bounded-and-retires-an-unacked-generation
  (let [installed-subs (atom nil)
        out (chan 1)
        p (atom {:pubsub {}})]
    (with-redefs [rt/set-deliver! (fn [_ _] true)
                  rt/set-state-sync! (fn [_ _] true)
                  rt/on-stop! (fn [_ _] true)
                  rt/unsubscribe-topics-ordered! (fn [_ _] (done true))
                  pubsub/set-transport! (fn [_ _] true)
                  pubsub/subscriptions (fn [_] {:t {:out out}})
                  pubsub/direct-unsubscribe! (fn [_ _ _] (chan))
                  pso/transport (fn [_ _ _ subs _ _ _]
                                  (reset! installed-subs subs)
                                  {})]
      (pso/install! S p {:peer-id :self})
      (activate-recovery-topic!
       p installed-subs :t {:recovery-timeout-ms 25})
      (let [result (<?? S (pso/unsubscribe! p #{:t}))]
        (is (= :netz/unsubscribe-timeout (:error result)))
        (is (false? (async/>!! out :late-ack))
            "a late v0 ack must not contaminate a later topic generation")
        (is (= :closed (:status (pso/recovery-status p :t))))))))

(deftest shutdown-rejects-every-lifecycle-waiter-without-opening-the-semaphore
  (let [stop! (atom nil)
        installed-subs (atom nil)
        raw-unsubscribes (atom 0)
        p (atom {:pubsub {}})]
    (with-redefs [rt/set-deliver! (fn [_ _] true)
                  rt/set-state-sync! (fn [_ _] true)
                  rt/on-stop! (fn [_ f] (reset! stop! f) true)
                  rt/unsubscribe-topics-ordered! (fn [_ _] (done true))
                  pubsub/set-transport! (fn [_ _] true)
                  pubsub/direct-unsubscribe! (fn [_ _ _]
                                               (swap! raw-unsubscribes inc)
                                               (done {:ok true}))
                  pso/transport (fn [_ _ _ subs _ _ _]
                                  (reset! installed-subs subs)
                                  {})]
      (pso/install! S p {:peer-id :self})
      (activate-recovery-topic! p installed-subs :t {})
      (let [control (get-in @p [:pubsub :netz/recovery-control])
            lock (:lifecycle-lock @control)]
        (is (= :available (<?? S lock)) "simulate a stalled lifecycle owner")
        (let [a (pso/unsubscribe! p #{:t})
              b (pso/unsubscribe! p #{:t})]
          (<?? S (timeout 50))
          (is (zero? @raw-unsubscribes)
              "both callers began before stop and are parked behind the owner")
          (@stop!)
          (is (= :netz/stopped (:error (<?? S a))))
          (is (= :netz/stopped (:error (<?? S b))))
          (is (zero? @raw-unsubscribes))
          (is (= :stopped (:status (pso/recovery-status p :t))))
          (async/offer! lock :available))))))
