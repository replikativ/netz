(ns netz.pubsub
  "Running `kabel.pubsub` over the peer-to-peer overlay.

  ## What changes, and what does not

  Only two things differ between pub/sub over one connection and pub/sub over a
  mesh: where a publish **goes**, and what a subscription **means**. Everything
  else — topics, `PSyncStrategy`, the batched ack-driven handshake,
  backpressure — is identical, so it is untouched.

  That is the whole design. `PSyncStrategy` already expresses both paths a
  replicated value needs:

      -apply-publish                          the live path
      -init-client-state → -handshake-items   the DIFFERENTIAL state sync
        → -apply-handshake-item

  and three consumers already implement it — konserve-sync's timestamp diff,
  datahike's tx-broadcast, spindel's signal-sync (which carries yggdrasil's
  `-join` and `-apply-delta` inside its strategy). None of them needs to change
  to become federated, because the semantics were never the transport's
  business.

  ## The three substitutions

  **Publish** becomes dissemination: multi-hop, signed at origin, verified at
  every hop, deduplicated by interval set, and repairable.

  **Subscribe** becomes topic interest. A relay carries a topic RANGE, so
  `[:tx-report]` or one store id is a thing a peer can agree to relay without
  carrying the network.

  **The handshake stays point-to-point**, because it is a bulk, acknowledged,
  backpressured transfer and should be. What changes is who with: today it is
  \"your server\", here it is any peer carrying the range — so state sync becomes
  multi-source.

  ## The horizon is a re-handshake

  `netz.dissemination/beyond-horizon` reports peers whose gap δ-repair cannot
  close, because the messages have fallen out of the repair store. The correct
  answer is not a bespoke state request: it is to **run the handshake again**,
  which is exactly the differential state sync `PSyncStrategy` already
  provides — and precisely replikativ's observation that a state sync per
  identity is much smaller than replaying every change that produced it.

  The transport says *I cannot fill this gap*, which is a transport fact. The
  application answers with a differential sync, which is an application fact.
  Neither needs to know the other's semantics."
  (:require [kabel.pubsub :as pubsub]
            [netz.apply-owner :as apply-owner]
            [netz.overlay.runtime :as rt]
            #?(:clj [superv.async :refer [<? go-try go-loop-try]])
            [clojure.core.async :as async :refer [chan put! close! timeout]])
  #?(:cljs (:require-macros [superv.async :refer [<? go-try go-loop-try]])))

(defn- ok [v]
  (let [ch (chan 1)] (put! ch v) (close! ch) ch))

(defn- exact-success? [result]
  (and (map? result)
       (= true (:ok result))
       (not (contains? result :error))))

(defn- commit-publications!
  [running publications]
  (every? true? (map #(rt/application-committed! running %) publications)))

(defn- subscribe-success? [result topics]
  (and (map? result)
       (not (contains? result :error))
       (or (= true (:ok result))
           (= (set topics) (set (:ok result))))))

(defn- throwable? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn- initial-apply-owner
  [topic generation]
  (let [state (apply-owner/make-state
               {:topic topic
                :profile "kabel/psync-strategy/0"
                :generation generation
                :live-transport :overlay
                :phase :joining})
        started (apply-owner/transition
                 state {:type :sync/begin
                        :generation generation
                        :sync-id [:initial generation]})]
    (:state started)))

(defn- transition-topic-owner!
  [recovery-control topic event]
  (let [result (volatile! {:state nil :effects []})]
    (swap! recovery-control update-in [:topics topic]
           (fn [topic-state]
             (if-let [state (:owner topic-state)]
               (let [next (apply-owner/transition state event)]
                 (vreset! result next)
                 (assoc topic-state :owner (:state next)))
               topic-state)))
    @result))

(defn recovery-status
  "Return the last observable recovery state for `topic`."
  [peer topic]
  (if-let [control (get-in @peer [:pubsub :netz/recovery-control])]
    (get-in @control [:topics topic :status])
    (get-in @peer [:pubsub :netz/recovery topic])))

(defn unsubscribe!
  "Cancel Netz subscription intent, then orderly-drain Kabel topics.

  Kabel dispatches this through the installed Netz transport, so callers may
  use either public namespace without bypassing overlay lifecycle ownership."
  [peer topics]
  (if-let [unsubscribe-fn (get-in @peer [:pubsub :netz/unsubscribe!])]
    (unsubscribe-fn topics)
    (pubsub/unsubscribe! peer topics)))

(defn- topic-gated? [gate-state topic]
  (let [state @gate-state]
    (if (set? state)
      (contains? state topic)
      (boolean (get-in state [:topics topic :gated?])))))

(defn- handshake-complete? [peer subs gate-state topic]
  (let [sub (pubsub/subscription peer topic)]
    ;; A topic remembered in `subs` is a remote subscription even during the
    ;; orderly unsubscribe/resubscribe gap used for recovery. Treating its
    ;; temporarily absent Kabel subscription as an owner would let overlay live
    ;; data overtake the replacement snapshot.
    (if (contains? @subs topic)
      (and (not (topic-gated? gate-state topic))
           (boolean (:handshake-complete? sub)))
      true)))

(defn- apply-one!
  [S peer subs topic payload]
  (let [sub (pubsub/subscription peer topic)
        topic-cfg (pubsub/get-topic-config peer topic)
        strategy (or (:strategy sub) (:strategy topic-cfg))
        on-publish (get-in @subs [topic :on-publish])]
    ;; Kabel owns the peer/topic application serializer. Going through its
    ;; public boundary makes overlay live values mutually exclusive with direct
    ;; live frames and snapshot application instead of maintaining a second
    ;; Netz-only strategy runner.
    (pubsub/apply-publish! S peer topic payload
                           {:strategy strategy
                            :on-publish on-publish})))

(defn- pending-entry [gate-state topic payload publication]
  (let [state (when (and gate-state (map? @gate-state))
                (get-in @gate-state [:topics topic]))]
    (if (or state publication)
      {::pending? true
       ::payload payload
       ::publication publication
       ::intent-generation (:intent-generation state)
       ::dirty-generation (:dirty-generation state)}
      payload)))

(defn- owned-pending? [entry owner]
  (if owner
    (and (::pending? entry)
         (= (:intent-generation owner) (::intent-generation entry))
         (= (:dirty-generation owner) (::dirty-generation entry)))
    true))

(defn- drop-intent-pending! [pending intent-generations]
  (swap! pending
         (fn [all]
           (reduce-kv
            (fn [all topic intent-generation]
              (let [remaining
                    (into []
                          (remove #(and (::pending? %)
                                        (= intent-generation
                                           (::intent-generation %))))
                          (get all topic))]
                (if (seq remaining)
                  (assoc all topic remaining)
                  (dissoc all topic))))
            all
            intent-generations))))

(defn drain-pending!
  "Apply anything buffered for `topic`, in arrival order.

  Called when the handshake completes, and again on every delivery, so a
  payload can never overtake the state it was meant to be applied on top of."
  ([S peer subs pending topic]
   (drain-pending! S peer subs pending topic nil))
  ([S peer subs pending topic owner]
   (let [[before _]
         (swap-vals!
          pending
          (fn [all]
            (let [remaining (into []
                                  (remove #(owned-pending? % owner))
                                  (get all topic))]
              (if (seq remaining)
                (assoc all topic remaining)
                (dissoc all topic)))))
         queued (filter #(owned-pending? % owner) (get before topic))]
    ;; Sequentially, awaiting each: `apply-one!` returns a go-block, so a
    ;; `doseq` that merely STARTS them applies the buffer in whatever order
    ;; the scheduler happens to finish — which is the exact defect this
    ;; buffer exists to prevent. Always returns a channel so callers can
    ;; chain on it.
     (go-try S
             (loop [values (seq queued)
                    applied 0
                    committed-publications []]
               (if-let [p (first values)]
                 (let [payload (if (::pending? p) (::payload p) p)
                       result (<? S (apply-one! S peer subs topic payload))]
                   (if (true? (:ok result))
                     (recur (next values) (inc applied)
                            (cond-> committed-publications
                              (::publication p) (conj (::publication p))))
                     result))
                 {:ok true
                  :applied applied
                  :committed-publications committed-publications}))))))

(defn deliver-to-strategy!
  "Hand a disseminated payload to the topic's strategy, and to `:on-publish`.

  Installed by `install!` as the runtime's delivery function: dissemination
  emits a `[:deliver source topic payload publication]` action rather than
  calling anything, so
  the state machine stays pure and the runtime decides what delivery means.
  Here it means what a one-hop publish has always meant — `-apply-publish`
  followed by `:on-publish`, in that order, exactly as
  `kabel.pubsub/handle-publish!` does on the direct path.

  ## Why this buffers

  The handshake is point-to-point and publishes are disseminated, so the two
  arrive over different paths — in a mesh, from different peers entirely —
  with no ordering relation of any kind. For a strategy whose `-apply-publish`
  is a DELTA on handshake state (datahike's tx-broadcast, spindel's
  `-apply-delta`, konserve-sync's key writes) applying a publish before the
  base state it depends on is a lost update or an error, and a silent one.

  So a payload for a topic whose handshake has not completed is held, in
  arrival order, and released when it has. `:handshake-complete?` already
  existed on the subscription; nothing was reading it.

  Bounded, because the sender controls the rate: past `max-pending` the oldest
  item is dropped and `on-overflow` is called. The adapter MUST answer that
  signal with a state sync: dissemination has already marked the publication
  seen, so message anti-entropy alone cannot repair an application-level drop."
  [S peer subs pending topic payload & [{:keys [max-pending on-overflow
                                                desynchronized publication]
                                         :or {max-pending 1024}}]]
  (let [desynchronized (or desynchronized (atom #{}))]
    (if-not (handshake-complete? peer subs desynchronized topic)
      (let [[before after]
            (swap-vals! pending update topic
                        (fn [q]
                          (let [q (conj (vec q)
                                        (pending-entry desynchronized
                                                       topic payload publication))]
                            (if (> (count q) max-pending)
                              (vec (subvec q 1))
                              q))))
            overflow? (> (inc (count (get before topic))) max-pending)]
        (when (and overflow? on-overflow) (on-overflow topic))
        (ok after))
    ;; Drain first, and AWAIT it, so a payload arriving just after the
    ;; handshake completed cannot overtake the buffer it was queued behind.
      (go-try S
              (let [drained (<? S (drain-pending! S peer subs pending topic))]
                (if (true? (:ok drained))
                  (let [result (<? S (apply-one! S peer subs topic payload))]
                    (if (exact-success? result)
                      (update result :committed-publications
                              (fnil into [])
                              (cond-> (:committed-publications drained)
                                publication (conj publication)))
                      result))
                  drained))))))

(def default-recovery-timeout-ms 30000)

(declare retire-recovery-outs!)

(defn transport
  "A `kabel.pubsub` transport backed by `running` (from
  `netz.overlay.runtime/start!`).

  Prefer `install!`, which wires delivery as well — a transport alone carries
  publishes across the network but nothing hands them to a strategy.

  `subs` is an atom of `{topic opts}`, remembered so that a horizon-triggered
  `re-handshake!` can reproduce the ORIGINAL subscription rather than a
  stripped-down one. `pending` is the pre-handshake buffer. `enqueue-drain!`
  serializes handshake completion with live delivery."
  ([S peer running subs pending enqueue-drain!]
   (transport S peer running subs pending enqueue-drain! nil))
  ([S peer running subs pending enqueue-drain! recovery-control]
   (let [lifecycle-lock
         (or (when recovery-control (:lifecycle-lock @recovery-control))
             (doto (chan 1) (async/offer! :available)))
         stop-signal (or (when recovery-control
                           (:stop-signal @recovery-control))
                         (chan))]
     {:receive-publish!
      (fn [_peer topic _payload _context]
     ;; Kabel v0 live frames have no origin/epoch/sequence identity. Once the
     ;; overlay owns live delivery, accepting them as a second path makes
     ;; duplicate detection impossible. Fail explicitly; Kabel retires that v0
     ;; connection while point-to-point handshake frames remain supported.
        (ok {:error (ex-info "Direct live delivery is disabled by the Netz overlay"
                             {:type :netz/direct-live-disabled
                              :topic topic})}))

      :publish!
      (fn [_peer topic payload]
     ;; `submit!` returns false when the runtime's event queue is full, and its
     ;; docstring is explicit that refusing is the point. Reporting {:ok true}
     ;; for a publish that was dropped would invert the only backpressure
     ;; signal the runtime has.
        (if (rt/publish! running topic payload)
          (ok {:ok true :transport :overlay})
          (ok {:error :netz/overloaded :transport :overlay})))

      :subscribe!
      (fn [peer topics opts]
        (go-try S
                (if (and recovery-control (:stopped? @recovery-control))
                  {:error :netz/stopped :transport :overlay}
                  (let [[token port]
                        (async/alts! [lifecycle-lock stop-signal])]
                    (if (or (= port stop-signal)
                            (not= :available token))
                      {:error :netz/stopped :transport :overlay}
                      (try
                        (if (and recovery-control (:stopped? @recovery-control))
                          {:error :netz/stopped :transport :overlay}
                          (if (some #(contains? @subs %) topics)
                            {:error :netz/already-subscribed :transport :overlay}
                            (let [original-opts opts
                                  intent-generations
                                  (when recovery-control
                                    (into {}
                                          (for [t topics]
                                            (let [[_ after]
                                                  (swap-vals!
                                                   recovery-control update-in
                                                   [:topics t]
                                                   (fn [state]
                                                     (let [generation
                                                           (inc (or (:intent-generation
                                                                     state)
                                                                    0))]
                                                       {:active? true
                                                        :intent-generation generation
                                                        :dirty-generation 0
                                                        :pending-sources #{}
                                                        :gated? true
                                                        :owner
                                                        (initial-apply-owner
                                                         t generation)
                                                        :status
                                                        {:status :joining}})))]
                                              [t (get-in after
                                                         [:topics t
                                                          :intent-generation])]))))
                                  valid-intent?
                                  (fn [topic]
                                    (if-let [generation
                                             (get intent-generations topic)]
                                      (let [state (get-in @recovery-control
                                                          [:topics topic])]
                                        (and (not (:stopped? @recovery-control))
                                             (:active? state)
                                             (= generation
                                                (:intent-generation state))))
                                      true))
                                  opts
                                  (update opts :on-handshake-complete
                                          (fn [callback]
                                            (fn [topic]
                                              (enqueue-drain!
                                               topic callback nil
                                               #(valid-intent? topic)
                                               (when recovery-control
                                                 {:intent-generation
                                                  (get intent-generations topic)
                                                  :dirty-generation 0})))))]
                        ;; Remember caller options before announcing interest,
                        ;; so a publish forwarded immediately after that event
                        ;; is buffered behind the joining generation.
                              (doseq [t topics]
                                (swap! subs assoc t original-opts))
                              (if-not (rt/subscribe-topics! running topics)
                                (do
                                  (swap! subs #(apply dissoc % topics))
                                  (drop-intent-pending! pending intent-generations)
                                  (when recovery-control
                                    (swap!
                                     recovery-control
                                     (fn [control]
                                       (if (:stopped? control)
                                         control
                                         (update control :topics
                                                 (fn [states]
                                                   (reduce
                                                    #(update %1 %2 assoc
                                                             :active? false
                                                             :gated? true
                                                             :status
                                                             {:status :failed
                                                              :result
                                                              {:error
                                                               :netz/overloaded}})
                                                    states topics)))))))
                                  {:error :netz/overloaded :transport :overlay})
                                (let [attempt (pubsub/direct-subscribe!
                                               peer topics opts)
                                      deadline
                                      (timeout (max 1
                                                    (or (:recovery-timeout-ms opts)
                                                        default-recovery-timeout-ms)))
                                      [value port]
                                      (async/alts! [attempt deadline stop-signal])
                                      result
                                      (cond
                                        (= port stop-signal)
                                        {:error :netz/stopped}

                                        (= port deadline)
                                        {:error :netz/subscribe-timeout}

                                        (nil? value)
                                        {:error :netz/subscribe-closed}

                                        (throwable? value)
                                        {:error :netz/subscribe-exception
                                         :exception value}

                                        :else value)
                                      ;; `attempt` and stop may become ready in
                                      ;; the same scheduler turn. Treat success
                                      ;; as committed only while this exact
                                      ;; intent is still live; that check is the
                                      ;; subscribe operation's linearization
                                      ;; point with shutdown.
                                      result
                                      (if (and (subscribe-success? result topics)
                                               (not-every? valid-intent? topics))
                                        {:error :netz/stopped
                                         :late-result result}
                                        result)]
                                  (if (subscribe-success? result topics)
                                    (assoc result :transport :overlay)
                                    (do
                                      (swap! subs #(apply dissoc % topics))
                                      (drop-intent-pending! pending intent-generations)
                                      (when recovery-control
                                        (swap!
                                         recovery-control
                                         (fn [control]
                                           (if (:stopped? control)
                                             control
                                             (update control :topics
                                                     (fn [states]
                                                       (reduce
                                                        #(update %1 %2 assoc
                                                                 :active? false
                                                                 :gated? true
                                                                 :status
                                                                 {:status :failed
                                                                  :result result})
                                                        states topics)))))))
                                      (when (#{:netz/stopped
                                               :netz/subscribe-timeout}
                                             (:error result))
                                        (retire-recovery-outs!
                                         (pubsub/subscriptions peer) topics nil))
                                ;; The interest was admitted before Kabel failed,
                                ;; so unwind it in the same lifecycle section.
                                      (let [rolled-back?
                                            (<? S (rt/unsubscribe-topics-ordered!
                                                   running topics))]
                                        (assoc result
                                               :transport :overlay
                                               :interest-rollback-accepted?
                                               rolled-back?)))))))))
                        (finally
                          (async/offer! lifecycle-lock :available))))))))})))

(defn- retire-recovery-outs! [held topics explicit-out]
  (doseq [out (into #{} (keep identity)
                    (cons explicit-out (map #(get-in held [% :out]) topics)))]
    (close! out)))

(defn re-handshake!
  "Answer a horizon gap by re-running the handshake for everything this peer
  subscribes to, against the peer that reported the gap.

  Deliberately not a bespoke \"state request\" message. `PSyncStrategy` already
  has exactly one differential state sync — `-init-client-state` →
  `-handshake-items` → `-apply-handshake-item` — and a peer that has fallen
  beyond the repair horizon is in precisely the position of a peer that just
  subscribed: it needs the CURRENT state, not the messages that produced it.
  Reusing the subscribe path means the recovery route is the same code the
  join route is, so it cannot rot separately.

  `from` is used rather than discarded: the peer that reported the horizon is
  by construction one that HAS the state we are missing, and on a mesh
  `[:pubsub :out]` would otherwise send this to whichever connection was
  accepted last. That is what makes state sync multi-source rather than
  nominally so.

  Reuses the ORIGINAL opts per topic. Passing only `{:strategies …}` would be
  the same code with a different argument: `init-subscription-state!` replaces
  the whole subscription map, so `:on-handshake-complete` would be silently
  dropped and never fire again."
  ([S peer subs from]
   (re-handshake! S peer subs from {}))
  ([S peer subs from {:keys [topics enqueue-drain! timeout-ms valid?
                             drain-valid? drain-owner]
                      :or {timeout-ms default-recovery-timeout-ms}}]
   (let [held (pubsub/subscriptions peer)
         topics (into #{}
                      (filter #(get-in held [% :strategy]))
                      (or topics (keys held)))
         out (when from (rt/connection-out peer from))]
     (cond
       (and from (nil? out))
       (ok {:error :netz/recovery-source-unavailable :source from})

       (not (seq topics))
       (ok {:ok true :skipped true})

       :else
       (let [ready (chan (max 1 (count topics)))
             attempt
             (go-try S
                     ;; Kabel v0 cannot correlate two generations of
                     ;; topic-only handshake frames. Drain the old generation
                     ;; before reserving a replacement.
                     (let [unsubscribed
                           (<? S (pubsub/direct-unsubscribe! peer topics {}))]
                       (if-not (exact-success? unsubscribed)
                         unsubscribed
                         (if (and valid? (not (valid?)))
                           {:error :netz/recovery-cancelled}
                           (loop [groups (seq (group-by #(get @subs %) topics))]
                             (if-let [[opts ts] (first groups)]
                               (let [opts (or opts
                                              {:strategies
                                               (into {}
                                                     (for [t ts]
                                                       [t (get-in held
                                                                  [t :strategy])]))})
                                     opts (if enqueue-drain!
                                            (update
                                             opts :on-handshake-complete
                                             (fn [callback]
                                               (fn [topic]
                                                 (enqueue-drain!
                                                  topic callback
                                                  (fn [result]
                                                    (put! ready
                                                          [topic result]))
                                                  (or drain-valid? valid?)
                                                  drain-owner))))
                                            opts)
                                     result (if (and valid? (not (valid?)))
                                              {:error :netz/recovery-cancelled}
                                              (<? S
                                                  (pubsub/direct-subscribe!
                                                   peer (set ts)
                                                   (cond-> opts
                                                     out (assoc :out out)))))]
                                 (if (subscribe-success? result ts)
                                   (recur (next groups))
                                   result))
                               (if-not enqueue-drain!
                                 {:ok true}
                                 (loop [remaining (count topics)]
                                   (if (zero? remaining)
                                     {:ok true}
                                     (let [[_ result] (async/<! ready)]
                                       (if (exact-success? result)
                                         (recur (dec remaining))
                                         (or result
                                             {:error
                                              :netz/recovery-drain-closed}))))))))))))
             deadline (timeout timeout-ms)]
         (go-try S
                 (let [[value port] (async/alts! [attempt deadline])
                       result (cond
                                (= port deadline)
                                {:error :netz/recovery-timeout}

                                (nil? value)
                                {:error :netz/recovery-closed}

                                (throwable? value)
                                {:error :netz/recovery-exception
                                 :exception value}

                                :else value)]
                   (when (and (not (exact-success? result))
                              (not= :netz/recovery-superseded
                                    (:error result)))
                     ;; A timed-out or failed v0 operation cannot be safely
                     ;; followed by another topic-only generation. Retire all
                     ;; connections that may still complete it late.
                     (close! ready)
                     (retire-recovery-outs! held topics out))
                   result)))))))

(defn install!
  "Wire `peer`'s pub/sub onto `running`, and route disseminated payloads into
  the topic strategies.

  Returns the peer."
  [S peer running]
  (let [;; The opts each topic was subscribed with. `kabel.pubsub` keeps
        ;; `:strategy` and `:on-handshake-complete` on the peer but not
        ;; `:on-publish`, and a re-handshake needs all of them.
        subs (atom {})
        ;; Publishes that arrived before their topic's handshake finished.
        pending (atom {})
        ;; Subscribe, explicit unsubscribe, and replacement subscribe share one
        ;; lifecycle token. It is deliberately never closed: a closed semaphore
        ;; makes every waiter receive nil and enter the critical section at once.
        recovery-lock (doto (chan 1) (async/offer! :available))
        ;; Cancellation broadcast is separate from the semaphore: closing it
        ;; wakes waiters without granting lifecycle ownership.
        stop-signal (chan)
        ;; Topics that lost an admitted application value. They remain behind
        ;; the snapshot gate until a replacement handshake completes; continuing
        ;; to apply later deltas on a gapped state would make repair meaningless.
        recovery-control (atom {:topics {}
                                :lifecycle-lock recovery-lock
                                :stop-signal stop-signal
                                :stopped? false})
        ;; ONE consumer, so applications happen in the order they arrived.
        ;;
        ;; `deliver-to-strategy!` returns a go block and the runtime's
        ;; `:deliver` action discards it, so two publishes on a topic started
        ;; two concurrent blocks that raced inside `-apply-publish`. For a
        ;; strategy whose apply is a DELTA -- datahike's tx-broadcast,
        ;; spindel's `-apply-delta`, konserve-sync's key writes -- that is a
        ;; lost update, silently. Ordering the buffer drain was not enough:
        ;; that fixed the join, not the steady state.
        ;;
        ;; Submission is already serial (the runtime is a single event loop),
        ;; so one consumer that AWAITS each application is all that is needed
        ;; to keep it that way.
        deliveries (chan 1024)
        ;; Recovery control is separate from application delivery because a
        ;; recovery completes only after a :drain action runs. A sliding slot
        ;; coalesces any number of wakeups while dirty-topics retains the work.
        recovery-wake (chan (async/sliding-buffer 1))
        ;; Gap reports are admitted onto the application lane with at most one
        ;; pending put. The atom retains every topic/source while that wake is
        ;; queued, avoiding core.async's bounded pending-put limit under storms.
        gap-control (atom {:queued? false :by-topic {}})
        queue-gap! (fn [source topics]
                     (let [[before _]
                           (swap-vals!
                            gap-control
                            (fn [state]
                              (-> state
                                  (assoc :queued? true)
                                  (update :by-topic
                                          (fn [by-topic]
                                            (reduce #(update %1 %2
                                                             (fnil conj #{})
                                                             source)
                                                    by-topic topics))))))]
                       (when-not (:queued? before)
                         (put! deliveries [:gap nil nil nil]))))
        enqueue-drain! (fn
                         ([topic callback]
                          (put! deliveries
                                [:drain nil topic {:callback callback}]))
                         ([topic callback completion]
                          (put! deliveries
                                [:drain nil topic {:callback callback
                                                   :completion completion}]))
                         ([topic callback completion valid?]
                          (put! deliveries
                                [:drain nil topic {:callback callback
                                                   :completion completion
                                                   :valid? valid?}]))
                         ([topic callback completion valid? owner]
                          (put! deliveries
                                [:drain nil topic {:callback callback
                                                   :completion completion
                                                   :valid? valid?
                                                   :owner owner}])))
        mark-dirty! (fn [source topic]
                      (let [sources (if (set? source) source #{source})
                            sources (into #{}
                                          (map #(when-not (= % (:peer-id running))
                                                  %))
                                          sources)
                            [before after]
                            (swap-vals!
                             recovery-control update-in [:topics topic]
                             (fn [state]
                               (if-not (:active? state)
                                 state
                                 (let [generation
                                       (inc (or (:dirty-generation state) 0))]
                                   (assoc state
                                          :dirty-generation generation
                                          :pending-sources
                                          (into (or (:pending-sources state) #{})
                                                sources)
                                          :gated? true
                                          :status {:status :pending
                                                   :generation generation})))))]
                        (if (= before after)
                          []
                          (let [state (get-in after [:topics topic])]
                            (if (:owner state)
                              (:effects
                               (transition-topic-owner!
                                recovery-control topic
                                {:type :sync/begin
                                 :generation (:intent-generation state)
                                 :sync-id [(:intent-generation state)
                                           (:dirty-generation state)]}))
                              (do
                                (async/offer! recovery-wake :wake)
                                []))))))
        run-owner-effects!
        (fn run-owner-effects! [topic initial-effects]
          (go-try S
                  (loop [effects (seq initial-effects)]
                    (if-let [{:keys [op] :as effect} (first effects)]
                      (case op
                        :invoke-live
                        (let [publication (:publication effect)
                              result (<? S (apply-one! S peer subs topic
                                                       (:payload publication)))
                              next-effects
                              (:effects
                               (transition-topic-owner!
                                recovery-control topic
                                {:type :apply-result
                                 :operation-id (:operation-id effect)
                                 :result result}))]
                          (recur (concat next-effects (next effects))))

                        :settle
                        (let [{:keys [publication source]} (:receipt effect)
                              result (:result effect)
                              committed?
                              (or (:duplicate result)
                                  (:subsumed result)
                                  (not (exact-success? result))
                                  (nil? publication)
                                  (rt/application-committed! running
                                                             publication))
                              recovery-effects
                              (if committed?
                                []
                                (mark-dirty! source topic))]
                          (recur (concat recovery-effects (next effects))))

                        :request-sync
                        (recur (concat (mark-dirty! (:source effect) topic)
                                       (next effects)))

                        :begin-sync
                        (do
                          (swap! recovery-control update-in [:topics topic]
                                 assoc
                                 :gated? true
                                 :sync-operation-id (:operation-id effect)
                                 :status {:status :pending
                                          :generation
                                          (get-in @recovery-control
                                                  [:topics topic
                                                   :dirty-generation])})
                          (async/offer! recovery-wake :wake)
                          (recur (next effects)))

                        :ready
                        (do
                          (swap! recovery-control update-in [:topics topic]
                                 assoc :gated? false)
                          (recur (next effects)))

                        :retire-direct
                        (do
                          (retire-recovery-outs!
                           (pubsub/subscriptions peer) #{topic} nil)
                          (recur (next effects)))

                        :sync-protocol-error
                        (if (= :sync-already-active (:reason effect))
                          (recur (next effects))
                          {:error :netz/apply-owner-protocol
                           :reason (:reason effect)})

                        (recur (next effects)))
                      {:ok true}))))
        close-topic-owner!
        (fn [topic]
          (if-let [generation (get-in @recovery-control
                                      [:topics topic :owner :generation])]
            (let [completion (chan 1)]
              (put! deliveries
                    [:owner-close nil topic
                     {:generation generation :completion completion}])
              completion)
            (ok {:ok true :skipped true})))
        cancel-and-unsubscribe!
        (fn [topics]
          (go-try S
                  (if (:stopped? @recovery-control)
                    {:error :netz/stopped}
                    (let [[token port]
                          (async/alts! [recovery-lock stop-signal])]
                      (if (or (= port stop-signal)
                              (not= :available token))
                        {:error :netz/stopped}
                        (try
                          (if (:stopped? @recovery-control)
                            {:error :netz/stopped}
                            (let [held (pubsub/subscriptions peer)
                                  remembered (select-keys @subs topics)
                                  _ (loop [remaining (seq topics)]
                                      (when-let [topic (first remaining)]
                                        (<? S (close-topic-owner! topic))
                                        (recur (next remaining))))
                                  intent-generations
                                  (into {}
                                        (keep (fn [topic]
                                                (when-let [generation
                                                           (get-in @recovery-control
                                                                   [:topics topic
                                                                    :intent-generation])]
                                                  [topic generation])))
                                        topics)
                                  timeout-ms
                                  (max 1
                                       (reduce min default-recovery-timeout-ms
                                               (keep :recovery-timeout-ms
                                                     (vals remembered))))
                                  _ (swap! recovery-control update :topics
                                           (fn [states]
                                             (reduce
                                              (fn [states topic]
                                                (update
                                                 states topic
                                                 (fn [state]
                                                   (-> state
                                                       (assoc :active? false
                                                              :gated? true
                                                              :pending-sources #{}
                                                              :status {:status :closed})
                                                       (update :intent-generation
                                                               (fnil inc 0))))))
                                              states topics)))
                                  _ (swap! subs #(apply dissoc % topics))
                                  _ (drop-intent-pending! pending
                                                          intent-generations)
                                  interest-accepted?
                                  (<? S (rt/unsubscribe-topics-ordered!
                                         running topics))
                                  attempt (pubsub/direct-unsubscribe!
                                           peer topics {})
                                  deadline (timeout timeout-ms)
                                  [value port] (async/alts! [attempt deadline])
                                  result (cond
                                           (= port deadline)
                                           {:error :netz/unsubscribe-timeout}

                                           (nil? value)
                                           {:error :netz/unsubscribe-closed}

                                           (throwable? value)
                                           {:error :netz/unsubscribe-exception
                                            :exception value}

                                           :else value)
                                  result (if (and interest-accepted?
                                                  (exact-success? result))
                                           result
                                           (if-not interest-accepted?
                                             {:error :netz/unsubscribe-interest-overloaded
                                              :kabel-result result}
                                             result))]
                              (when-not (exact-success? result)
                        ;; A v0 unsubscribe that can still acknowledge late
                        ;; cannot share its topic-only generation with a future
                        ;; subscribe. Retire every connection that owned one of
                        ;; the cancelled subscriptions.
                                (retire-recovery-outs! held topics nil))
                              result))
                          (finally
                            (async/offer! recovery-lock :available))))))))]
    (swap! peer assoc-in [:pubsub :netz/recovery-control] recovery-control)
    (swap! peer assoc-in [:pubsub :netz/unsubscribe!] cancel-and-unsubscribe!)
    (go-loop-try S [d (<? S deliveries)]
                 (when d
                   (let [[op source topic payload publication] d]
                     (case op
                       :deliver
                       (when-not
                        ;; Cancellation fences local application immediately.
                        ;; The interest-withdrawal event can be backpressured,
                        ;; so a peer may still forward a frame until its retry.
                        (false? (get-in @recovery-control
                                        [:topics topic :active?]))
                         (if-let [state (get-in @recovery-control
                                                [:topics topic :owner])]
                           (let [publication
                                 (-> (or publication
                                         {:origin source :epoch 0 :seq 0
                                          :topic topic :payload payload})
                                     (assoc :payload payload)
                                     (update :bytes #(or % 0)))
                                 owner-result
                                 (transition-topic-owner!
                                  recovery-control topic
                                  {:type :live
                                   :generation (:generation state)
                                   :transport :overlay
                                   :source source
                                   :receipt {:publication publication
                                             :source source}
                                   :publication publication})]
                             (<? S (run-owner-effects!
                                    topic (:effects owner-result))))
                           ;; Compatibility fallback for callers that install
                           ;; subscriptions before the owner-aware transport.
                           (let [result
                                 (<? S (deliver-to-strategy!
                                        S peer subs pending topic payload
                                        {:desynchronized recovery-control
                                         :publication publication
                                         :on-overflow
                                         (fn [overflow-topic]
                                           (let [effects
                                                 (mark-dirty!
                                                  source overflow-topic)]
                                             (run-owner-effects!
                                              overflow-topic effects)))}))]
                             (when (and (map? result) (:error result))
                               (run-owner-effects!
                                topic (mark-dirty! source topic)))
                             (when (exact-success? result)
                               (let [publications
                                     (or (seq (:committed-publications result))
                                         (when publication [publication]))]
                                 (when (and (seq publications)
                                            (not (commit-publications!
                                                  running publications)))
                                   (run-owner-effects!
                                    topic (mark-dirty! source topic))))))))

                       :drain
                       (let [{:keys [callback completion valid? owner]} payload
                             validation (if valid? (valid?) true)
                             valid? (or (= true validation)
                                        (= :valid validation))
                             managed? (some? (get-in @recovery-control
                                                     [:topics topic :owner]))
                             _ (when-not valid?
                                 (swap! pending
                                        (fn [all]
                                          (let [remaining
                                                (into []
                                                      (remove
                                                       #(owned-pending? % owner))
                                                      (get all topic))]
                                            (if (seq remaining)
                                              (assoc all topic remaining)
                                              (dissoc all topic))))))
                             drained
                             (cond
                               (not valid?)
                               {:error (if (= :superseded validation)
                                         :netz/recovery-superseded
                                         :netz/recovery-cancelled)}

                               managed?
                               (let [operation-id
                                     (or (:operation-id owner)
                                         (get-in @recovery-control
                                                 [:topics topic :owner
                                                  :active-sync]))
                                     ;; The snapshot is now committed. Open the
                                     ;; legacy Kabel gate before the owner
                                     ;; drains its queued live suffix.
                                     _ (swap! recovery-control
                                              assoc-in
                                              [:topics topic :gated?] false)
                                     owner-result
                                     (transition-topic-owner!
                                      recovery-control topic
                                      {:type :profile-sync-result
                                       :operation-id operation-id
                                       :result {:ok true
                                                :frontiers {}
                                                :frontier-bytes 1}})
                                     result
                                     (<? S (run-owner-effects!
                                            topic (:effects owner-result)))]
                                 (if (and (exact-success? result)
                                          (= :live
                                             (get-in @recovery-control
                                                     [:topics topic :owner
                                                      :phase])))
                                   {:ok true}
                                   (or result
                                       {:error :netz/apply-owner-not-ready})))

                               :else
                               (<? S (drain-pending!
                                      S peer subs pending topic owner)))
                             receipts-ok?
                             (or managed?
                                 (not (exact-success? drained))
                                 (commit-publications!
                                  running
                                  (:committed-publications drained)))
                             result
                             (cond
                               (not (exact-success? drained))
                               drained

                               (not receipts-ok?)
                               {:error :netz/application-receipt-overloaded}

                               :else
                               (try
                                 (when callback (callback topic))
                                 {:ok true}
                                 (catch #?(:clj Throwable :cljs :default) error
                                   {:error error})))]
                         (if (exact-success? result)
                           ;; Recovery drains release their gate only after the
                           ;; owning recovery worker validates its generation.
                           (when-not completion
                             ;; Initial-handshake drain does not own a recovery
                             ;; generation. It may observe a gate but can never
                             ;; clear one.
                             nil)
                           ;; A recovery completion reports failure to its
                           ;; owner. An initial-join drain has no owner and must
                           ;; schedule recovery here.
                           (when-not completion
                             (<? S (run-owner-effects!
                                    topic (mark-dirty! source topic)))))
                         (when completion (completion result)))

                       :owner-close
                       (let [{:keys [generation completion]} payload
                             owner-result
                             (transition-topic-owner!
                              recovery-control topic
                              {:type :close :generation generation})
                             result
                             (<? S (run-owner-effects!
                                    topic (:effects owner-result)))]
                         (when completion
                           (put! completion result)
                           (close! completion)))

                       :gap
                       (let [[batch _]
                             (swap-vals! gap-control
                                         (constantly {:queued? false
                                                      :by-topic {}}))]
                         (doseq [[gap-topic sources] (:by-topic batch)]
                           (<? S (run-owner-effects!
                                  gap-topic
                                  (mark-dirty! sources gap-topic))))))

                     (recur (<? S deliveries)))))
    ;; One recovery owner, independent of the delivery worker it waits on.
    (go-loop-try S [wake (<? S recovery-wake)]
                 (when wake
                   (let [[before _]
                         (swap-vals!
                          recovery-control update :topics
                          (fn [topics]
                            (reduce-kv
                             (fn [next-topics topic state]
                               (assoc next-topics topic
                                      (if (and (:active? state)
                                               (seq (:pending-sources state)))
                                        (assoc state
                                               :pending-sources #{}
                                               :status
                                               {:status :recovering
                                                :generation
                                                (:dirty-generation state)})
                                        state)))
                             {}
                             topics)))
                         tasks
                         (->> (:topics before)
                              (keep (fn [[topic state]]
                                      (when (and (:active? state)
                                                 (seq (:pending-sources state)))
                                        {:topic topic
                                         :intent-generation
                                         (:intent-generation state)
                                         :generation
                                         (:dirty-generation state)
                                         :sources (:pending-sources state)})))
                              (sort-by (comp str :topic))
                              seq)]
                     (loop [tasks tasks]
                       (when-let [{:keys [topic intent-generation generation
                                          sources]}
                                  (first tasks)]
                         (let [source (first (sort-by str sources))
                               managed? (some? (get-in @recovery-control
                                                       [:topics topic :owner]))
                               owner-start
                               (when managed?
                                 (transition-topic-owner!
                                  recovery-control topic
                                  {:type :sync/begin
                                   :generation intent-generation
                                   :sync-id [intent-generation generation]}))
                               owner-start-result
                               (if managed?
                                 (<? S (run-owner-effects!
                                        topic (:effects owner-start)))
                                 {:ok true})
                               operation-id
                               (when managed?
                                 (get-in @recovery-control
                                         [:topics topic :owner :active-sync]))
                               valid-intent?
                               #(let [state (get-in @recovery-control
                                                    [:topics topic])]
                                  (and (not (:stopped? @recovery-control))
                                       (:active? state)
                                       (= intent-generation
                                          (:intent-generation state))))
                               drain-valid?
                               #(let [state (get-in @recovery-control
                                                    [:topics topic])]
                                  (cond
                                    (not (valid-intent?)) false
                                    (if managed?
                                      (not= operation-id
                                            (get-in state
                                                    [:owner :active-sync]))
                                      (not= generation
                                            (:dirty-generation state)))
                                    :superseded
                                    :else true))
                               [token port]
                               (async/alts! [recovery-lock stop-signal])
                               value (if (or (not (exact-success?
                                                   owner-start-result))
                                             (and managed?
                                                  (nil? operation-id)))
                                       (do
                                         (when (= port recovery-lock)
                                           (async/offer! recovery-lock
                                                         :available))
                                         {:error :netz/apply-owner-not-ready})
                                       (if (or (= port stop-signal)
                                               (not= :available token))
                                         {:error :netz/stopped}
                                         (try
                                           (if (:stopped? @recovery-control)
                                             {:error :netz/stopped}
                                             (async/<! (re-handshake!
                                                        S peer subs source
                                                        {:topics #{topic}
                                                         :enqueue-drain!
                                                         enqueue-drain!
                                                         :valid? valid-intent?
                                                         :drain-valid?
                                                         drain-valid?
                                                         :drain-owner
                                                         {:intent-generation
                                                          intent-generation
                                                          :dirty-generation
                                                          generation
                                                          :operation-id
                                                          operation-id}})))
                                           (finally
                                             (async/offer! recovery-lock
                                                           :available)))))
                               result (cond
                                        (nil? value)
                                        {:error :netz/recovery-closed}

                                        (throwable? value)
                                        {:error :netz/recovery-exception
                                         :exception value}

                                        :else value)
                               failure (when-not (exact-success? result)
                                         {:status :failed
                                          :generation generation
                                          :source source
                                          :remaining-sources
                                          (disj sources source)
                                          :result result})
                               desired-status
                               (or failure
                                   {:status :ready
                                    :generation generation
                                    :source source})
                               [before after]
                               (swap-vals!
                                recovery-control update-in [:topics topic]
                                (fn [state]
                                  (if (and (:active? state)
                                           (= intent-generation
                                              (:intent-generation state))
                                           (= generation
                                              (:dirty-generation state)))
                                    (assoc state
                                           :gated? (boolean failure)
                                           :status desired-status)
                                    state)))
                               old-topic (get-in before [:topics topic])
                               committed?
                               (and (:active? old-topic)
                                    (= intent-generation
                                       (:intent-generation old-topic))
                                    (= generation
                                       (:dirty-generation old-topic))
                                    (= desired-status
                                       (get-in after
                                               [:topics topic :status])))]
                           ;; Validation, gate release, and status publication
                           ;; are one atomic per-topic transition. A newer dirty
                           ;; signal or explicit unsubscribe makes this a no-op.
                           (when (and committed? failure)
                             (when-let [callback
                                        (get-in @subs
                                                [topic :on-recovery-failed])]
                               (try
                                 (callback topic failure)
                                 (catch #?(:clj Throwable
                                           :cljs :default) _)))))
                         (recur (next tasks))))
                     ;; A signal received after the atomic batch take remains
                     ;; pending in the same controller. Its one-slot wake
                     ;; normally remains too; this covers a consumed wake.
                     (when (some (comp seq :pending-sources)
                                 (vals (:topics @recovery-control)))
                       (async/offer! recovery-wake :wake))
                     (recur (<? S recovery-wake)))))
    (rt/set-deliver! running
                     (fn [source topic payload publication]
                       ;; `offer!`, not a parking put: the delivery function is
                       ;; called from the runtime's own loop, and blocking it
                       ;; to deliver to ourselves would deadlock. A full queue
                       ;; means the application cannot keep up. It converts
                       ;; overload into state synchronization;
                       ;; silently dropping here is never correct because the
                       ;; dissemination layer has already recorded the message.
                       (or (async/offer! deliveries
                                         [:deliver source topic payload publication])
                           (do
                             ;; Queue the gap behind every application already
                             ;; admitted to the owner. This makes the gap itself
                             ;; the linearization point and prevents a stale
                             ;; recovery drain from applying after it.
                             (queue-gap! source #{topic})
                             false))))
    (rt/set-state-sync!
     running
     (fn [from stranded]
       ;; Close the application gate before queueing the recovery control item.
       ;; Live actions accepted afterwards are buffered until the replacement
       ;; snapshot's ordered drain callback clears the topic.
       (let [reported (into #{} (keep :topic) stranded)
             topics (if (seq reported)
                      (into #{} (filter #(contains? @subs %)) reported)
                      (set (keys @subs)))]
         (queue-gap! from topics))
       true))
    (rt/on-stop!
     running
     #(do
        (swap! recovery-control
               (fn [control]
                 (-> control
                     (assoc :stopped? true)
                     (update :topics
                             (fn [topics]
                               (reduce-kv
                                (fn [next-topics topic state]
                                  (let [owner (:owner state)
                                        closed
                                        (when owner
                                          (:state
                                           (apply-owner/transition
                                            owner
                                            {:type :close
                                             :generation
                                             (:generation owner)})))]
                                    (assoc next-topics topic
                                           (cond->
                                            (-> state
                                                (assoc :active? false
                                                       :gated? true
                                                       :pending-sources #{}
                                                       :status {:status :stopped})
                                                (update :intent-generation
                                                        (fnil inc 0)))
                                             closed (assoc :owner closed)))))
                                {}
                                topics))))))
        (close! deliveries)
        (close! recovery-wake)
        (close! stop-signal)))
    (pubsub/set-transport!
     peer
     (assoc (transport S peer running subs pending enqueue-drain!
                       recovery-control)
            :unsubscribe!
            (fn [_peer topics]
              (cancel-and-unsubscribe! topics))))
    peer))
