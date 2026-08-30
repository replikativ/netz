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
            [netz.overlay.runtime :as rt]
            #?(:clj [superv.async :refer [<? go-try go-loop-try]])
            [clojure.core.async :as async :refer [chan put! close!]])
  #?(:cljs (:require-macros [superv.async :refer [<? go-try go-loop-try]])))

(defn- ok [v]
  (let [ch (chan 1)] (put! ch v) (close! ch) ch))

(defn- handshake-complete? [peer subs desynchronized topic]
  (let [sub (pubsub/subscription peer topic)]
    ;; A topic remembered in `subs` is a remote subscription even during the
    ;; orderly unsubscribe/resubscribe gap used for recovery. Treating its
    ;; temporarily absent Kabel subscription as an owner would let overlay live
    ;; data overtake the replacement snapshot.
    (if (contains? @subs topic)
      (and (not (contains? @desynchronized topic))
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

(defn drain-pending!
  "Apply anything buffered for `topic`, in arrival order.

  Called when the handshake completes, and again on every delivery, so a
  payload can never overtake the state it was meant to be applied on top of."
  [S peer subs pending topic]
  (let [queued (get @pending topic)]
    (swap! pending dissoc topic)
    ;; Sequentially, awaiting each: `apply-one!` returns a go-block, so a
    ;; `doseq` that merely STARTS them applies the buffer in whatever order
    ;; the scheduler happens to finish — which is the exact defect this
    ;; buffer exists to prevent. Always returns a channel so callers can
    ;; chain on it.
    (go-try S
            (loop [values (seq queued)
                   applied 0]
              (if-let [p (first values)]
                (let [result (<? S (apply-one! S peer subs topic p))]
                  (if (true? (:ok result))
                    (recur (next values) (inc applied))
                    result))
                {:ok true :applied applied})))))

(defn deliver-to-strategy!
  "Hand a disseminated payload to the topic's strategy, and to `:on-publish`.

  Installed by `install!` as the runtime's delivery function: dissemination
  emits a `[:deliver topic payload]` action rather than calling anything, so
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
                                                desynchronized]
                                         :or {max-pending 1024}}]]
  (let [desynchronized (or desynchronized (atom #{}))]
    (if-not (handshake-complete? peer subs desynchronized topic)
      (let [overflow? (volatile! false)
            result (swap! pending update topic
                          (fn [q]
                            (let [q (conj (vec q) payload)]
                              (if (> (count q) max-pending)
                                (do (vreset! overflow? true) (vec (subvec q 1)))
                                q))))]
        (when (and @overflow? on-overflow) (on-overflow topic))
        result)
    ;; Drain first, and AWAIT it, so a payload arriving just after the
    ;; handshake completed cannot overtake the buffer it was queued behind.
      (go-try S
              (let [drained (<? S (drain-pending! S peer subs pending topic))]
                (if (true? (:ok drained))
                  (<? S (apply-one! S peer subs topic payload))
                  drained))))))

(defn transport
  "A `kabel.pubsub` transport backed by `running` (from
  `netz.overlay.runtime/start!`).

  Prefer `install!`, which wires delivery as well — a transport alone carries
  publishes across the network but nothing hands them to a strategy.

  `subs` is an atom of `{topic opts}`, remembered so that a horizon-triggered
  `re-handshake!` can reproduce the ORIGINAL subscription rather than a
  stripped-down one. `pending` is the pre-handshake buffer. `enqueue-drain!`
  serializes handshake completion with live delivery."
  [S peer running subs pending enqueue-drain!]
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
             ;; Interest FIRST. It is what makes dissemination forward these
             ;; topics to us at all; without it the handshake could complete
             ;; and no live publish would ever arrive. Anything that lands
             ;; before the handshake finishes is buffered rather than applied.
             (if-not (rt/subscribe-topics! running topics)
               ;; Refused means the interest was never registered, so this peer
               ;; would be permanently invisible to the mesh for these topics.
               ;; Reporting success here is the worst of the options.
               {:error :netz/overloaded :transport :overlay}
               (let [;; Release the buffer as soon as the handshake says so,
                     ;; rather than waiting for the next publish to notice.
                     opts (update opts :on-handshake-complete
                                  (fn [f]
                                    (fn [t]
                                      (enqueue-drain! t f))))]
                 (doseq [t topics] (swap! subs assoc t opts))
                 ;; Then the ordinary point-to-point handshake — the SAME code
                 ;; the direct transport runs. A handshake is a bulk,
                 ;; acknowledged, backpressured transfer between two peers;
                 ;; disseminating it would flood the network with one peer's
                 ;; catch-up. What the overlay changes is who you may handshake
                 ;; WITH, not how.
                 (assoc (<? S (pubsub/direct-subscribe! peer topics opts))
                        :transport :overlay)))))})

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
  [S peer subs from]
  (let [held (pubsub/subscriptions peer)
        topics (into #{} (filter #(get-in held [% :strategy]) (keys held)))
        out (when from (rt/connection-out peer from))]
    (when (seq topics)
      (go-try S
              ;; Kabel v0 cannot correlate two generations of topic-only
              ;; handshake frames. Drain the old generation before reserving a
              ;; replacement; duplicate subscribe is correctly rejected.
              (let [unsubscribed (<? S (pubsub/unsubscribe! peer topics))]
                (if-not (true? (:ok unsubscribed))
                  unsubscribed
                  (loop [groups (seq (group-by #(get @subs %) topics))]
                    (if-let [[opts ts] (first groups)]
                      (let [result
                            (<? S
                                (pubsub/direct-subscribe!
                                 peer (set ts)
                                 (cond-> (or opts
                                             {:strategies
                                              (into {}
                                                    (for [t ts]
                                                      [t (get-in held
                                                                 [t :strategy])]))})
                                   out (assoc :out out))))]
                        (if (true? (:ok result))
                          (recur (next groups))
                          result))
                      {:ok true}))))))))

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
        ;; Topics that lost an admitted application value. They remain behind
        ;; the snapshot gate until a replacement handshake completes; continuing
        ;; to apply later deltas on a gapped state would make repair meaningless.
        desynchronized (atom #{})
        ;; Sources whose accepted gossip could not be handed to the
        ;; application. Message repair cannot help after dissemination marked
        ;; it seen, so the worker schedules a differential state sync once the
        ;; ordered delivery backlog has drained.
        dirty-sources (atom #{})
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
        mark-dirty! (fn [source topic]
                      (swap! desynchronized conj topic)
                      (swap! dirty-sources conj
                             (when-not (= source (:peer-id running)) source)))]
    (go-loop-try S [d (<? S deliveries)]
                 (when d
                   (let [[op source topic payload] d]
                     (case op
                       :deliver
                       (let [result
                             (<? S (deliver-to-strategy!
                                    S peer subs pending topic payload
                                    {:desynchronized desynchronized
                                     :on-overflow
                                     (fn [overflow-topic]
                                       (mark-dirty! source overflow-topic))}))]
                         (when (and (map? result) (:error result))
                           (mark-dirty! source topic)))

                       :drain
                       (let [_ (swap! desynchronized disj topic)
                             result (<? S (drain-pending! S peer subs pending topic))]
                         (if (true? (:ok result))
                           (when payload (payload topic))
                           (mark-dirty! source topic)))

                       :recover
                       (re-handshake! S peer subs source))
                   ;; Recover only after the ordered backlog is empty. A
                   ;; re-subscribe marks the handshake incomplete, so anything
                   ;; arriving while recovery runs is buffered behind it.
                     (if-let [next-delivery (async/poll! deliveries)]
                       (recur next-delivery)
                       (do
                       ;; Atomically take the set. A concurrent failed offer
                       ;; belongs to the next drain and cannot be erased
                       ;; between a deref and reset.
                         (let [sources (first (swap-vals! dirty-sources
                                                          (constantly #{})))]
                         ;; One state source is enough. Starting concurrent v0
                         ;; generations is invalid and Kabel rejects it.
                           (when (seq sources)
                             (re-handshake! S peer subs
                                            (first (sort-by str
                                                            (remove nil? sources))))))
                         (recur (<? S deliveries)))))))
    (rt/set-deliver! running
                     (fn [source topic payload]
                       ;; `offer!`, not a parking put: the delivery function is
                       ;; called from the runtime's own loop, and blocking it
                       ;; to deliver to ourselves would deadlock. A full queue
                       ;; means the application cannot keep up. It converts
                       ;; overload into state synchronization;
                       ;; silently dropping here is never correct because the
                       ;; dissemination layer has already recorded the message.
                       (or (async/offer! deliveries [:deliver source topic payload])
                           (do (mark-dirty! source topic) false))))
    (rt/set-state-sync!
     running
     (fn [from _stranded]
       ;; Close the application gate before queueing the recovery control item.
       ;; Live actions accepted afterwards are buffered until the replacement
       ;; snapshot's ordered drain callback clears the topic.
       (swap! desynchronized into (keys @subs))
       (put! deliveries [:recover from nil nil])))
    (rt/on-stop! running #(close! deliveries))
    (pubsub/set-transport!
     peer
     (transport S peer running subs pending
                (fn [topic callback]
                  ;; A control item must not be lost behind a full data queue.
                  ;; `put!` is safe here: at most one is pending per active
                  ;; handshake, and the worker is independent of the runtime.
                  (put! deliveries [:drain nil topic callback]))))
    peer))
