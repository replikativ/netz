(ns netz.sim
  "A deterministic discrete-event simulator for overlay protocols.

  Every run is a pure function of its seed. There is no wall clock, no
  threads, no `core.async`, and no unseeded randomness anywhere — so a failing
  run is reproduced by rerunning the seed, and a run reproduces identically on
  the JVM and in ClojureScript.

  ## Why this exists before the protocols do

  The interesting bugs in membership and dissemination protocols live under
  churn: nodes joining and leaving *during* a lookup, views being read while
  they split, repair racing expiry. An integration test against a handful of
  live peers does not produce them and will pass while the design is wrong.

  ## The rule this simulator exists to enforce

  **Determinism comes from a seeded RNG, never from replacing the algorithm.**
  Each node carries its own rng in its state, seeded from the simulation seed
  and the node id, so the real peer-selection code runs — with reproducible
  draws — rather than a stub. Assert that an attack has **no effect**; never
  merely that a counter moved.

  ## Node model

  A node is a pure state machine, following the same transition shape used by
  Partisan's broadcast engine:

      (fn handler [state event ctx] -> {:state state' :actions [action ...]})

  where `event` is one of

      {:type :init}
      {:type :message      :from <id> :payload <any>}
      {:type :timer        :payload <any>}
      {:type :disconnected :peer <id>}

  `ctx` is `{:id <this node> :now <virtual ms>}`, and each action is

      [:send       <to-id> <payload>]
      [:timer      <delay-ms> <payload>]
      [:connect    <to-id> <address> <first-frame>]
      [:disconnect <to-id>]
      [:persist    <key> <value> [completion-payload]]
      [:deliver    <source-id> <topic> <payload> <publication>]

  `:connect` and `:disconnect` exist because opening a transport is not a
  message — in a deployment they are `kabel.peer/connect` against a URL
  followed by sending `<first-frame>`, and a socket close. Keeping them in the vocabulary means the protocol must hold an
  *address* to dial, which is the property the address book has to satisfy and
  which a message-only model quietly lets you skip.

  Handlers must not read a clock, generate randomness outside their state, or
  perform effects. Everything they need is in `state`, `event` and `ctx`.

  **Give the event dispatch a default clause.** The event vocabulary grows —
  `:disconnected` was added after the first handlers were written — and a
  `case` without a default turns a new event type into a crash rather than an
  ignored event."
  (:require [netz.sim.rng :as rng]))

;; =============================================================================
;; Event queue
;; =============================================================================
;; Ordered by [time, insertion-sequence]. The sequence number is what makes
;; the order total: two events scheduled for the same virtual millisecond must
;; have one deterministic order, or the simulation is only reproducible by
;; luck.

(def ^:private event-order
  (fn [a b]
    (compare [(:at a) (:seq a)] [(:at b) (:seq b)])))

(defn- empty-queue [] (sorted-set-by event-order))

;; =============================================================================
;; Construction
;; =============================================================================

(def default-opts
  {:seed 42
   ;; Message latency is drawn uniformly from this inclusive range.
   :latency-min 10
   :latency-max 50
   ;; Probability that a message is silently lost in the network.
   :drop-p 0.0
   ;; Probability of a second copy. This is a network fault above the ordered
   ;; transport and is intentionally allowed to arrive later.
   :duplicate-p 0.0
   ;; Kabel/WebSocket is FIFO per connection. Keep that transport guarantee by
   ;; default even though independently drawn latencies would otherwise let a
   ;; later frame overtake an earlier one on the same directed link. Tests for
   ;; unordered transports can disable it explicitly.
   :link-fifo? true
   ;; Optional directed-link overrides. A profile may set an inclusive
   ;; latency range and a shaping throughput. Throughput serializes frames;
   ;; unlike the global burst ceiling below, it does not drop them.
   :links {}
   ;; Resource ceilings. nil bandwidth means unlimited.
   :max-inflight 100000
   :bandwidth-bytes-per-ms nil
   ;; A persistence request lands after this virtual delay. Process crashes
   ;; invalidate requests that have not landed yet.
   :persist-latency-ms 1
   ;; Hard bound on events processed by `run-*`, so a protocol bug that
   ;; schedules work faster than it consumes it fails the test rather than
   ;; hanging it.
   :max-steps 1000000
   ;; Recording every delivery is useful and unbounded; a long run should turn
   ;; it off and assert on node state instead.
   :trace? true})

(def ^:private link-profile-fields
  #{:latency-min :latency-max :throughput-bytes-per-ms})

(defn- valid-link-profile? [opts [link profile]]
  (let [latency-min (get profile :latency-min (:latency-min opts))
        latency-max (get profile :latency-max (:latency-max opts))
        throughput (:throughput-bytes-per-ms profile)]
    (and (vector? link) (= 2 (count link))
         (map? profile)
         (every? link-profile-fields (keys profile))
         (integer? latency-min) (<= 0 latency-min)
         (integer? latency-max) (<= latency-min latency-max)
         (or (nil? throughput)
             (and (integer? throughput) (pos? throughput))))))

(defn make-sim
  "Create a simulation. See `default-opts` for the options."
  ([] (make-sim {}))
  ([opts]
   (let [opts (merge default-opts opts)]
     (when-not (and (map? (:links opts))
                    (every? #(valid-link-profile? opts %) (:links opts)))
       (throw (ex-info "Invalid simulated directed-link profiles"
                       {:type :netz.sim/invalid-link-profiles})))
     {:now 0
      :seq 0
      :steps 0
      :rng (rng/make-rng (:seed opts))
      :queue (empty-queue)
      :inflight 0
      :peak-inflight 0
      :nodes {}
      ;; nil means "one network". Otherwise a map of node-id -> group; nodes
      ;; can reach each other exactly when their groups are equal.
      :partition nil
      ;; Directed link -> delivery time of the last enqueued frame. Event
      ;; insertion sequence breaks ties, so equal timestamps remain FIFO.
      :link-next-at {}
      :link-ready-at {}
      :storage {}
      :bytes-sent {}
      :link-bytes {}
      :time-bytes {}
      :peak-time-bytes {}
      :opts opts
      :trace []
      :stats {:sent 0 :delivered 0 :dropped-partition 0
              :dropped-loss 0 :dropped-down 0 :dropped-overflow 0
              :dropped-bandwidth 0 :duplicated 0 :timers 0 :connects 0
              :persist-attempts 0 :persisted 0
              :persist-write-failed 0 :persist-full 0
              :disk-losses 0 :corruptions 0}})))

(def ^:private size-overhead 2)

(defn estimate-size
  "Portable deterministic structural size in notional bytes.

  It intentionally does not print values: host printers can differ between
  Clojure and ClojureScript. The estimate is depth-bounded and is used for
  relative traffic and bandwidth assertions, not as a wire encoder."
  ([x] (estimate-size x 0))
  ([x depth]
   (cond
     (> depth 32) 8
     (nil? x) 1
     (boolean? x) 1
     (number? x) 8
     (keyword? x) (+ size-overhead (count (name x))
                     (if-let [ns' (namespace x)] (count ns') 0))
     (string? x) (+ size-overhead (count x))
     (map? x) (reduce (fn [n [k v]]
                        (+ n (estimate-size k (inc depth))
                           (estimate-size v (inc depth))))
                      size-overhead x)
     (coll? x) (reduce (fn [n v] (+ n (estimate-size v (inc depth))))
                       size-overhead x)
     :else 16)))

(defn- node-seed
  "Derive a node's rng seed from the simulation seed and its id.

  Deterministic, and independent of map iteration order — `hash` differs
  across platforms, so the id is stringified and folded byte by byte."
  [sim id]
  (let [s (str id)]
    (reduce (fn [acc ch]
              (rng/u32 (bit-xor (rng/u32 (bit-shift-left acc 5))
                                (int #?(:clj ch :cljs (.charCodeAt ch 0))))))
            (rng/u32 (get-in sim [:opts :seed]))
            (seq s))))

(defn- enqueue [sim event]
  (-> sim
      (update :queue conj (assoc event :seq (:seq sim)))
      (update :seq inc)))

(defn add-node
  "Add a node running `handler` with `init-state`.

  The node receives an `{:type :init}` event at the current virtual time, so
  it can schedule its first timers. Its rng is seeded from the simulation seed
  and its id, and lives at `:rng` in its state."
  ([sim id handler init-state]
   (add-node sim id handler init-state {}))
  ([sim id handler init-state {:keys [restore]}]
   (let [decorate (fn [state]
                    (assoc state
                           :rng (rng/make-rng (node-seed sim id))
                           :id id))]
     (-> sim
         (assoc-in [:nodes id] {:handler handler
                                :up? true
                                :generation 0
                                :init-state init-state
                                :restore (or restore
                                             (fn [initial durable]
                                               (assoc initial :persisted durable)))
                                :state (decorate init-state)})
         (assoc-in [:storage id] {:durable {}
                                  :generation 0
                                  :writable? true
                                  :capacity-bytes nil})
         (enqueue {:at (:now sim) :kind :init :to id})))))

;; =============================================================================
;; Faults
;; =============================================================================

(defn- notify-disconnect
  "Tell every other node that `id`'s transport is gone.

  Broadcast rather than targeted because the simulator does not track
  connections — they are protocol state, not transport state. A node that was
  not connected to `id` simply drops a key it does not hold, so the extra
  events are inert.

  This matters for fidelity: when a real process dies its kernel closes every
  socket, so peers *do* find out. A simulator where a crash is silent lets a
  protocol pass tests while believing forever in connections that are gone."
  [sim id]
  (reduce (fn [s other]
            (if (= other id)
              s
              (enqueue s {:at (:now s) :kind :disconnected :to other :from id})))
          sim
          (keys (:nodes sim))))

(defn link-down
  "Drop the link between `a` and `b`: both learn the transport is gone.

  The targeted counterpart to `crash` — for testing a dropped connection
  between two peers that are both still running."
  [sim a b]
  (-> sim
      (enqueue {:at (:now sim) :kind :disconnected :to a :from b})
      (enqueue {:at (:now sim) :kind :disconnected :to b :from a})))

(defn pause
  "Pause `id` without losing memory. Use this for suspension or a temporarily
  unavailable process whose memory remains intact."
  [sim id]
  (-> sim
      (assoc-in [:nodes id :up?] false)
      (notify-disconnect id)))

(defn resume
  "Resume a paused node and let it re-arm timers."
  [sim id]
  (-> sim
      (assoc-in [:nodes id :up?] true)
      (enqueue {:at (:now sim) :kind :init :to id})))

(defn crash
  "Crash `id`: volatile state and unlanded persistence are lost.

  Durable values remain in `storage`; `restart` reconstructs through the
  node's restore function supplied to `add-node`."
  [sim id]
  (-> sim
      (assoc-in [:nodes id :up?] false)
      (assoc-in [:nodes id :state] nil)
      (update-in [:nodes id :generation] inc)
      (notify-disconnect id)))

(defn restart
  "Bring `id` back. A crashed node is rebuilt from durable state; a merely
  paused node keeps its memory."
  [sim id]
  (let [{:keys [state init-state restore]} (get-in sim [:nodes id])
        restored (when-not state
                   (assoc (restore init-state (get-in sim [:storage id :durable]))
                          :rng (rng/make-rng (node-seed sim id))
                          :id id))]
    (-> sim
        (cond-> restored (assoc-in [:nodes id :state] restored))
        (assoc-in [:nodes id :up?] true)
        (enqueue {:at (:now sim) :kind :init :to id}))))

(defn forget
  "Remove `id` entirely, state and all — a permanent departure.

  Peers are told the transport is gone, as they would be by a closing socket."
  [sim id]
  (-> sim
      (notify-disconnect id)
      (update :nodes dissoc id)
      (update :storage dissoc id)))

(defn set-disk-writable
  "Enable or disable durable writes for `id`. Reads and already durable values
  remain available. Pending writes observe the setting when they complete."
  [sim id writable?]
  (assoc-in sim [:storage id :writable?] (boolean writable?)))

(defn set-disk-capacity
  "Set the notional byte capacity for `id`, or nil for unlimited capacity."
  [sim id capacity]
  (when-not (or (nil? capacity)
                (and (int? capacity) (not (neg? capacity))))
    (throw (ex-info "Invalid simulated disk capacity"
                    {:type :netz.sim/invalid-disk-capacity
                     :capacity capacity})))
  (assoc-in sim [:storage id :capacity-bytes] capacity))

(defn lose-disk
  "Lose all durable values for `id` while leaving the process running.

  Incrementing the disk generation prevents a write already queued against the
  lost device from silently landing on its replacement. The node receives a
  `:disk-lost` event so a protocol can invalidate volatile inventory."
  [sim id]
  (let [generation (inc (get-in sim [:storage id :generation] 0))]
    (-> sim
        (assoc-in [:storage id :durable] {})
        (assoc-in [:storage id :generation] generation)
        (update-in [:stats :disk-losses] inc)
        (enqueue {:at (:now sim) :kind :disk-lost :to id}))))

(defn corrupt-durable
  "Replace one durable value without changing its key.

  The explicit replacement keeps corruption deterministic and lets a protocol
  prove that it verifies the content address rather than trusting the key."
  [sim id key replacement]
  (-> sim
      (assoc-in [:storage id :durable key] replacement)
      (update-in [:stats :corruptions] inc)
      (enqueue {:at (:now sim) :kind :disk-corrupt
                :to id :key key})))

(defn partition-network
  "Split the network into groups: a map of `node-id -> group`.

  Nodes reach each other exactly when their groups are equal. A node absent
  from the map is in group `nil` and so can reach every other absent node."
  [sim groups]
  (assoc sim :partition groups))

(defn heal
  "Remove any partition."
  [sim]
  (assoc sim :partition nil))

(defn reachable?
  "Can `from` currently send to `to`?"
  [sim from to]
  (let [p (:partition sim)]
    (or (nil? p) (= (get p from) (get p to)))))

(defn up? [sim id]
  (boolean (get-in sim [:nodes id :up?])))

;; =============================================================================
;; Scheduling
;; =============================================================================

(defn at
  "Schedule `f` — a `(fn [sim] -> sim)` — to run at virtual time `t`.

  This is how churn is injected: `(at sim 5000 #(crash % :n3))`."
  [sim t f]
  (enqueue sim {:at t :kind :call :f f}))

(defn send-message
  "Inject a message from outside, as if `from` had sent it now."
  [sim from to payload]
  (enqueue sim {:at (:now sim) :kind :message :from from :to to :payload payload}))

(defn- link-profile [sim from to]
  (get-in sim [:opts :links [from to]] {}))

(defn- draw-latency [sim from to]
  (let [profile (link-profile sim from to)
        latency-min (get profile :latency-min
                         (get-in sim [:opts :latency-min]))
        latency-max (get profile :latency-max
                         (get-in sim [:opts :latency-max]))
        [rng' ms] (rng/rand-range (:rng sim) latency-min latency-max)]
    [(assoc sim :rng rng') ms]))

(defn- drop-message? [sim]
  (let [p (get-in sim [:opts :drop-p])]
    (if (<= p 0.0)
      [sim false]
      (let [[rng' dropped?] (rng/rand-bool (:rng sim) p)]
        [(assoc sim :rng rng') dropped?]))))

(defn- duplicate-message? [sim]
  (let [p (get-in sim [:opts :duplicate-p])]
    (if (<= p 0.0)
      [sim false]
      (let [[rng' duplicate?] (rng/rand-bool (:rng sim) p)]
        [(assoc sim :rng rng') duplicate?]))))

(defn- charge-bytes [sim from to bytes]
  (let [{last-at :at current :bytes} (get-in sim [:time-bytes from])
        current (if (= last-at (:now sim)) (or current 0) 0)
        total (+ current bytes)]
    (-> sim
        (update-in [:bytes-sent from] (fnil + 0) bytes)
        (update-in [:link-bytes [from to]] (fnil + 0) bytes)
        (assoc-in [:time-bytes from] {:at (:now sim) :bytes total})
        (update-in [:peak-time-bytes from] (fnil max 0) total))))

(defn- over-bandwidth? [sim from bytes]
  (when-let [budget (get-in sim [:opts :bandwidth-bytes-per-ms])]
    (let [{last-at :at current :bytes} (get-in sim [:time-bytes from])
          current (if (= last-at (:now sim)) (or current 0) 0)]
      (> (+ current bytes) budget))))

(defn- enqueue-network [sim event]
  (if (>= (:inflight sim) (get-in sim [:opts :max-inflight]))
    (update-in sim [:stats :dropped-overflow] inc)
    (let [sim (-> sim
                  (enqueue (assoc event :network? true))
                  (update :inflight inc))]
      (update sim :peak-inflight max (:inflight sim)))))

(defn- transmit
  "Put `payload` on the wire from `from` to `to`, subject to reachability,
  loss and latency."
  [sim from to payload]
  (let [sim (update-in sim [:stats :sent] inc)
        bytes (estimate-size payload)]
    (cond
      (not (contains? (:nodes sim) to))
      (update-in sim [:stats :dropped-down] inc)

      (not (reachable? sim from to))
      (update-in sim [:stats :dropped-partition] inc)

      (over-bandwidth? sim from bytes)
      (-> sim
          (charge-bytes from to bytes)
          (update-in [:stats :dropped-bandwidth] inc))

      :else
      (let [sim (charge-bytes sim from to bytes)
            [sim dropped?] (drop-message? sim)]
        (if dropped?
          (update-in sim [:stats :dropped-loss] inc)
          (let [link [from to]
                profile (link-profile sim from to)
                throughput (:throughput-bytes-per-ms profile)
                transmission-ms (if throughput
                                  (max 1 (quot (+ bytes throughput -1)
                                               throughput))
                                  0)
                transmission-start (max (:now sim)
                                        (get-in sim [:link-ready-at link]
                                                (:now sim)))
                transmission-end (+ transmission-start transmission-ms)
                [sim ms] (draw-latency sim from to)
                candidate (+ transmission-end ms)
                at (if (get-in sim [:opts :link-fifo?])
                     (max candidate (get-in sim [:link-next-at link] candidate))
                     candidate)
                event {:at at :kind :message :from from :to to :payload payload}
                sim (-> sim
                        (assoc-in [:link-ready-at link] transmission-end)
                        (assoc-in [:link-next-at link] at)
                        (enqueue-network event))
                [sim duplicate?] (duplicate-message? sim)]
            (if duplicate?
              (-> sim
                  (assoc-in [:link-next-at link] (inc at))
                  (enqueue-network (assoc event :at (inc at) :duplicate? true))
                  (update-in [:stats :duplicated] inc))
              sim)))))))

(defn- apply-action
  [sim from [op & args]]
  (case op
    :send
    (let [[to payload] args]
      (transmit sim from to payload))

    ;; Opening and closing a transport are their own actions rather than
    ;; messages, because in a real deployment they are: `:connect` becomes
    ;; `kabel.peer/connect` against a URL, and `:disconnect` closes a socket.
    ;;
    ;; The simulator models both as ordinary frames, so the same protocol code
    ;; runs here and over a real wire. The address is carried but unused — the
    ;; simulator routes by node id, and the point of the action is that the
    ;; protocol had to *have* an address to dial, which is the property the
    ;; address book must satisfy.
    :connect
    (let [[to _address payload] args]
      (-> sim
          (update-in [:stats :connects] (fnil inc 0))
          (transmit from to (or payload {:type :dial}))))

    :deliver
    ;; Handing a payload to the application. The simulator has no application —
    ;; it counts the call so a test can assert delivery happened at the
    ;; protocol's boundary, which is the part the protocol is responsible for.
    (update-in sim [:stats :delivered-to-app] (fnil inc 0))

    :state-sync
    ;; "Your gap is older than anything I still hold." The simulator has no
    ;; application to answer it, so it counts the escalation — which is what a
    ;; test wants to assert anyway: that the transport gave up on repair
    ;; exactly once per unrepairable gap, rather than every tick.
    (update-in sim [:stats :state-syncs] (fnil inc 0))

    :sync-committed
    ;; The synchronization machine already committed its staged application
    ;; value atomically. Overlay composition consumes the attached transport
    ;; frontiers; a sync-only simulation records the boundary.
    (update-in sim [:stats :sync-committed] (fnil inc 0))

    :persist
    (let [[k v completion] args
          node-generation (get-in sim [:nodes from :generation])
          disk-generation (get-in sim [:storage from :generation])]
      (-> sim
          (update-in [:stats :persist-attempts] inc)
          (enqueue {:at (+ (:now sim) (get-in sim [:opts :persist-latency-ms]))
                    :kind :persist-complete
                    :to from
                    :node-generation node-generation
                    :disk-generation disk-generation
                    :key k
                    :value v
                    :completion completion})))

    :disconnect
    (let [[to] args]
      ;; The far side learns the transport is gone, exactly as a closing socket
      ;; tells it. The near side has already updated its own state — it is the
      ;; one that asked.
      (enqueue sim {:at (:now sim) :kind :disconnected :to to :from from}))

    :timer
    (let [[delay payload] args]
      (-> sim
          (update-in [:stats :timers] inc)
          (enqueue {:at (+ (:now sim) (max 0 delay))
                    :kind :timer
                    :to from
                    :payload payload})))

    (throw (ex-info "Unknown simulator action"
                    {:type :netz.sim/unknown-action :action op :from from}))))

(defn- record [sim event]
  (if (get-in sim [:opts :trace?])
    (update sim :trace conj (select-keys event [:at :kind :from :to :payload]))
    sim))

(defn- deliver-to-node
  [sim {:keys [to from payload kind] :as event}]
  (let [{:keys [handler state up?]} (get-in sim [:nodes to])]
    (if-not up?
      ;; Messages and timers for a down node are simply lost; when it comes
      ;; back up it gets a fresh :init rather than a backlog.
      (update-in sim [:stats :dropped-down] inc)
      (let [ev (case kind
                 :init {:type :init}
                 :message {:type :message :from from :payload payload}
                 :timer {:type :timer :payload payload}
                 :disconnected {:type :disconnected :peer from}
                 :disk-lost {:type :disk-lost}
                 :disk-corrupt {:type :disk-corrupt :key (:key event)}
                 :persist-result {:type :persist-result
                                  :key (:key event)
                                  :result (:result event)
                                  :payload payload})
            {new-state :state actions :actions}
            (handler state ev {:id to :now (:now sim)})
            sim (-> sim
                    (assoc-in [:nodes to :state] (or new-state state))
                    (record event))
            sim (if (= :message kind)
                  (update-in sim [:stats :delivered] inc)
                  sim)]
        (reduce (fn [s a] (apply-action s to a)) sim (or actions []))))))

(defn- durable-size [durable]
  (reduce-kv (fn [total key value]
               (+ total (estimate-size key) (estimate-size value)))
             0 durable))

(defn- persist-result [sim event result]
  (let [{:keys [to completion]} event]
    (if (and completion (up? sim to))
      (enqueue sim {:at (:now sim) :kind :persist-result :to to
                    :key (:key event) :result result
                    :payload completion})
      sim)))

(defn- complete-persist
  [sim {:keys [to node-generation disk-generation key value] :as event}]
  (let [same-process? (= node-generation (get-in sim [:nodes to :generation]))
        same-disk? (= disk-generation (get-in sim [:storage to :generation]))
        writable? (get-in sim [:storage to :writable?])
        durable (get-in sim [:storage to :durable])
        proposed (assoc durable key value)
        capacity (get-in sim [:storage to :capacity-bytes])]
    (cond
      (or (not same-process?) (not same-disk?)) sim

      (not writable?)
      (-> sim
          (update-in [:stats :persist-write-failed] inc)
          (persist-result event :write-failed))

      (and capacity (> (durable-size proposed) capacity))
      (-> sim
          (update-in [:stats :persist-full] inc)
          (persist-result event :full))

      :else
      (-> sim
          (assoc-in [:storage to :durable key] value)
          (update-in [:stats :persisted] inc)
          (record event)
          (persist-result event :ok)))))

;; =============================================================================
;; Running
;; =============================================================================

(defn step
  "Process the earliest pending event. Returns the sim unchanged if idle."
  [sim]
  (if-let [event (first (:queue sim))]
    (let [sim (-> sim
                  (update :queue disj event)
                  (assoc :now (max (:now sim) (:at event)))
                  (update :steps inc)
                  (cond-> (:network? event) (update :inflight dec)))]
      (case (:kind event)
        :call ((:f event) sim)
        :persist-complete (complete-persist sim event)
        (if (contains? (:nodes sim) (:to event))
          (deliver-to-node sim event)
          (update-in sim [:stats :dropped-down] inc))))
    sim))

(defn idle? [sim] (empty? (:queue sim)))

(defn- check-steps [sim]
  (when (> (:steps sim) (get-in sim [:opts :max-steps]))
    (throw (ex-info "Simulation exceeded :max-steps — a protocol is probably scheduling faster than it consumes"
                    {:type :netz.sim/step-limit
                     :steps (:steps sim)
                     :now (:now sim)
                     :queued (count (:queue sim))})))
  sim)

(defn run-until
  "Run until virtual time reaches `t`, or the simulation goes idle."
  [sim t]
  (loop [sim sim]
    (let [next-at (:at (first (:queue sim)))]
      (if (or (nil? next-at) (> next-at t))
        (assoc sim :now (max (:now sim) t))
        (recur (check-steps (step sim)))))))

(defn run-until-idle
  "Run until nothing is pending.

  Note that a protocol with a periodic timer is *never* idle; use `run-until`
  for those, which is most of them."
  [sim]
  (loop [sim sim]
    (if (idle? sim)
      sim
      (recur (check-steps (step sim))))))

(defn run-steps
  "Process at most `n` events."
  [sim n]
  (loop [sim sim
         n n]
    (if (or (zero? n) (idle? sim))
      sim
      (recur (step sim) (dec n)))))

;; =============================================================================
;; Inspection
;; =============================================================================

(defn node-state
  "The current state of `id`."
  [sim id]
  (get-in sim [:nodes id :state]))

(defn node-ids [sim] (set (keys (:nodes sim))))

(defn durable-state
  "Durable key/value state that survives a process crash for `id`."
  [sim id]
  (get-in sim [:storage id :durable] {}))

(defn bytes-sent
  "Notional bytes attempted by each sender."
  [sim]
  (:bytes-sent sim))

(defn link-bytes
  "Notional bytes charged to each reachable directed link.

  Includes modeled loss and burst rejection after reachability, but excludes
  sends rejected because the destination is down or partitioned."
  [sim]
  (:link-bytes sim))

(defn messages-to
  "Traced messages delivered to `id`."
  [sim id]
  (filter #(and (= :message (:kind %)) (= id (:to %))) (:trace sim)))

(defn payloads-to
  "Payloads of traced messages delivered to `id`."
  [sim id]
  (map :payload (messages-to sim id)))
