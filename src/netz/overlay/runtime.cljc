(ns netz.overlay.runtime
  "Runs `netz.overlay` on real kabel connections.

  Everything under `netz.overlay` is a pure state machine tested in
  `netz.sim`. This namespace is the part that touches the world: it turns the
  machine's actions into sockets, timers and frames, and turns arriving frames
  back into events.

  ## The split, and why

  kabel middleware is **per connection** — it is handed one `[in out]` pair and
  returns another. The overlay is **per peer**: one state machine, many
  connections, and it originates connections, which middleware by construction
  cannot do (it only ever sees a connection that already exists).

  So the two planes are separated:

  - **Data plane — `middleware`.** Per connection. Performs the identity
    handshake, registers the connection under the remote peer id, funnels
    overlay frames into the shared event channel, and passes every other
    message through untouched so it composes with the rest of a middleware
    stack.
  - **Control plane — the runtime.** One per peer. Drains events, steps the
    state machine, and interprets its actions.

  ## Why there has to be an identity handshake

  kabel connections are **anonymous**. `client-connect!` accepts a `peer-id`
  argument and never puts it on the wire, and `kabel.ring-ws/create-ws-handler!`
  names its copy `_peer-id` and ignores it. Neither end of a kabel connection
  learns who the other is, so an overlay that addresses peers by id has to
  establish that itself — which is what `netz.identity`'s signed records are
  for.

  This buys more than naming. A dial is issued against an *address* but
  addressed to a *peer id*; the pending frame is only released once a peer
  presenting that id has proved it holds the matching key. An impostor at the
  dialled address therefore cannot absorb the dial — it registers under its own
  id, our outbox never flushes, and the membership layer's dial timeout records
  a failure. That is the eclipse defence of `.internal/DHT_DESIGN.md` §2 falling
  out of self-certifying ids rather than being bolted on.

  ## Effects are injected

  The runtime takes an `effects` map rather than reaching for kabel directly,
  so the action interpreter and event loop are testable without opening a
  socket. `kabel-effects` supplies the real ones."
  (:refer-clojure :exclude [run!])
  (:require [replikativ.logging :as log]
            [netz.dissemination :as d]
            [netz.identity :as id]
            [netz.ratelimit :as rl]
            [netz.overlay :as overlay]
            [kabel.peer :as peer]
            [netz.sim.rng :as rng]
            [netz.store.protocol :as store]
            ;; `put?` is a plain function in both, while <?/>?/go-* are macros
            ;; that ClojureScript takes through :require-macros below. Missing
            ;; the :cljs branch compiles on the JVM and leaves an undeclared var
            ;; in the browser build — one platform only, as usual.
            #?(:clj [superv.async :refer [<? >? go-try go-loop-super put?]]
               :cljs [superv.async :refer [put?]])
            #?(:clj [clojure.core.async :as async
                     :refer [chan put! close! timeout go <!]]
               :cljs [clojure.core.async :as async
                      :refer [chan put! close! timeout <!] :refer-macros [go]]))
  #?(:cljs (:require-macros [superv.async :refer [<? >? go-try go-loop-super]])))

(defn- now-ms []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

;; =============================================================================
;; Frames
;; =============================================================================
;; Overlay traffic is tagged so it can share a wire with anything else a peer
;; is doing. Everything that is not ours passes through the middleware
;; untouched.

(def frame-type :netz.overlay/frame)
(def proof-type
  "Second leg of the handshake: proof that the peer holds its key NOW.

  A record alone is replayable -- see `netz.identity/challenge-bytes`. This
  carries a signature over the nonce WE chose, so registration means \"this
  connection is held by that identity\" rather than \"somebody showed me a
  record that identity once published\"."
  :netz.overlay/hello-proof)

(def hello-type :netz.overlay/hello)

(defn frame [payload] {:type frame-type :payload payload})

(defn overlay-message? [m]
  (and (map? m) (#{frame-type hello-type proof-type} (:type m))))

;; =============================================================================
;; Runtime
;; =============================================================================

(defn make-runtime
  "A runtime around an overlay state machine.

  `effects` must supply:

      :send!       (fn [to frame])
      :connect!    (fn [to address frame])
      :disconnect! (fn [to])
      :schedule!   (fn [delay-ms payload])
      :persist!    (fn [key value])       ; optional

  Delivery and state-sync are deliberately NOT here. They are installed later,
  with `set-deliver!` / `set-state-sync!`, because what they mean is the
  application's business and the application does not exist yet when the
  runtime is built — `start!` runs the machine before a consumer can wire
  itself in.

  Returns a context map. Feed it events with `submit!` and start it with
  `run!`."
  [{:keys [id state handler effects now-fn ratelimit]}]
  {:id id
   :state (atom state)
   :handler (or handler overlay/handler)
   :effects effects
   :now-fn (or now-fn now-ms)
   ;; Last address book written, so `persist-book!` only writes on a change.
   :book-cache (atom ::unwritten)
   ;; Late-bound application delivery. The effects map is fixed at
   ;; construction, but `netz.pubsub` can only wire delivery once BOTH
   ;; the overlay and the peer's pub/sub state exist — so this one is an atom.
   :deliver-fn (atom nil)
   ;; Late-bound too, and for the same reason: what a horizon gap MEANS is the
   ;; application's business, not the transport's.
   :state-sync-fn (atom nil)
   ;; Per-connection metering. Written as a pure state machine, kept in an atom
   ;; here because the middleware meters concurrently across connections.
   :limiter (atom (rl/make-state (or ratelimit {})))
   ;; Events dropped because a handler threw. A counter rather than silence:
   ;; a peer quietly discarding traffic is indistinguishable from a quiet
   ;; network, and that is the failure mode this guard could otherwise hide.
   :dropped (atom 0)
   :events (chan 1024)})

(defn submit!
  "Hand an event to the runtime. Returns false when the queue is full.

  `offer!` rather than `put!`, deliberately. `put!` on a full channel queues,
  and core.async throws past 1024 pending puts — so overload arrived as an
  exception rather than as a decision. Refusing here lets the caller do what
  replikativ did and drop the connection, which is the correct answer to a peer
  sending faster than we can process."
  [ctx event]
  (boolean (async/offer! (:events ctx) event)))

(defn- apply-action!
  [{:keys [effects] :as ctx} [op & args]]
  (case op
    :send (let [[to payload] args] ((:send! effects) to (frame payload)))
    :connect (let [[to address payload] args]
               ((:connect! effects) to address (frame payload)))
    :disconnect (let [[to] args] ((:disconnect! effects) to))
    :timer (let [[delay payload] args] ((:schedule! effects) delay payload))
    :persist (let [[k v] args]
               (when-let [f (:persist! effects)] (f k v)))
    :deliver (let [[topic payload] args]
               (when-let [f @(:deliver-fn ctx)]
                 (f topic payload)))

    :state-sync (let [[from stranded] args]
                  (when-let [f @(:state-sync-fn ctx)]
                    (f from stranded)))
    (throw (ex-info "Unknown overlay action"
                    {:type :netz.overlay.runtime/unknown-action :action op}))))

(defn- persist-book!
  "Write the address book when it changes.

  `start!` LOADS `:book` and, until this existed, nothing ever wrote it — so
  the book was empty on every start and stayed that way. That silently disabled
  the sticky/anchor-peer mitigation, which `netz.store.protocol` calls the
  thing without which the rest is \"worthless\": a peer that restarts with no
  memory of who behaved well before takes whoever answers first, which is the
  eclipse an attacker wants.

  It lives here rather than as an action because persistence policy is the
  runtime's business, and because emitting one on every book touch would write
  far more often than the book meaningfully changes. Debounced by comparing to
  what was last written."
  [{:keys [state book-cache] :as _ctx} persist-book]
  (when persist-book
    (let [book (get-in @state [:membership :book])]
      (when (not= book @book-cache)
        (reset! book-cache book)
        (persist-book book)))))

(defn step!
  "Apply one event: run the state machine, then its actions.

  Exposed for tests, which want to drive the machine one event at a time
  rather than through the loop."
  [{:keys [state handler now-fn id persist-book] :as ctx} event]
  (let [{new-state :state actions :actions}
        (handler @state event {:id id :now (now-fn)})]
    (reset! state (or new-state @state))
    (doseq [a (or actions [])]
      (apply-action! ctx a))
    (persist-book! ctx persist-book)
    ctx))

(defn run!
  "Drain events forever, stepping the machine for each.

  A single loop, so the state machine is never entered concurrently and needs
  no locking — the same guarantee the simulator gives it."
  [S ctx]
  (submit! ctx {:type :init})
  (go-loop-super S [ev (<? S (:events ctx))]
                 (when ev
                   ;; One event must never end the loop.
                   ;;
                   ;; `go-super` wraps the whole body in a try/catch that
                   ;; REPORTS and EXITS, so before this guard any exception
                   ;; from any handler killed the peer's overlay permanently --
                   ;; gossip, membership, repair, timers and content, all of it,
                   ;; from one malformed frame. And no wire field is
                   ;; type-checked, so producing one is easy.
                   ;;
                   ;; A schema gate at the ingress is still wanted, and is the
                   ;; better fix; this is the backstop that makes a missed case
                   ;; a dropped event instead of a dead node. `Throwable`
                   ;; deliberately: StackOverflowError from a deeply nested
                   ;; payload is an Error, and would otherwise not even be
                   ;; reported.
                   (try
                     (step! ctx ev)
                     (catch #?(:clj Throwable :cljs :default) e
                       (swap! (:dropped ctx) inc)
                       (log/warn :overlay/event-dropped
                                 {:type (:type ev)
                                  :payload-type (:type (:payload ev))
                                  :from (:from ev)
                                  :error #?(:clj (.getMessage ^Throwable e)
                                            :cljs (str e))})))
                   (recur (<? S (:events ctx))))))

;; =============================================================================
;; Publisher authentication
;; =============================================================================
;; Public-key authenticated pub/sub. A publish is signed by its origin and
;; checked at EVERY hop, so a relay cannot forge, alter or re-attribute one.
;;
;; The message carries the origin's public key, which costs ~192 bytes of hex
;; per publish and buys self-certification: since a peer id IS the hash of its
;; public key, a verifier needs no key lookup, no directory and no prior
;; contact with the origin. A message that has travelled five hops from a peer
;; we have never met is still checkable.
;;
;; This is deliberately kept OUT of the pure state machine. Signing and
;; verification are async on ClojureScript (WebCrypto), and a state machine
;; that had to await them would stop being a state machine. Handling it here
;; means everything reaching the machine is already authentic — the same
;; arrangement as TLS terminating below an application.

(defn sign-gossip
  "Attach publisher credentials to a message we are originating.

  The credentials carry the origin's **genesis record** as well as its signing
  key, which is what keeps verification self-certifying now that a peer id is
  the hash of a genesis rather than of a key. A verifier five hops away, who
  has never met the origin, still needs no lookup: the genesis hashes to the
  claimed origin, and the signing key must be one of its operational keys.

  The genesis is not separately signed and does not need to be — it is bound by
  `origin`, which *is* signed, and a substituted genesis would not hash to it."
  [S identity msg]
  (go-try S
          (let [{:keys [genesis operational]} identity
                sig (<? S (id/sign (:private operational) (d/signing-bytes msg)))]
            (assoc msg
                   :origin-genesis genesis
                   :origin-key (id/bytes->hex (:public operational))
                   :origin-sig (id/bytes->hex sig)))))

(defn verify-gossip
  "Is this publish genuinely from the peer it claims?

  All three are required: the signature must check out, the genesis must hash
  to the claimed origin, and the signing key must be one of that genesis's
  operational keys."
  [S msg]
  (go-try S
          (let [{:keys [origin origin-genesis origin-key origin-sig]} msg]
            (if-not (and (string? origin-key) (string? origin-sig))
              false
              (try
                (let [pk (id/hex->bytes origin-key)
                      sig (id/hex->bytes origin-sig)]
                  (and (= id/key-size (id/buf-length pk))
                       (= id/signature-size (id/buf-length sig))
                       ;; Genesis must hash to the claimed origin AND name this
                       ;; key as operational. Either check alone lets somebody
                       ;; publish under another peer's name.
                       (id/genesis-authorises? origin-genesis pk origin)
                       (<? S (id/verify pk (d/signing-bytes msg) sig))))
                (catch #?(:clj Exception :cljs js/Error) _ false))))))

;; =============================================================================
;; Connection registry
;; =============================================================================

(defn- registry
  "The connection registry for `peer`, created once.

  `swap!`-created rather than check-then-act: two connections arriving
  concurrently would each see no registry, each `assoc` their own, and the
  second would clobber the first -- orphaning any registration already made
  against it, so that connection stays unreachable for the life of the peer.
  A small window with a permanent failure is the worst combination."
  [peer]
  (or (:netz.overlay/connections @peer)
      (-> (swap! peer update :netz.overlay/connections
                 (fn [existing] (or existing (atom {:by-id {} :outbox {}}))))
          :netz.overlay/connections)))

(defn connections
  "Peer ids we currently hold a connection to."
  [peer]
  (set (keys (:by-id @(registry peer)))))

(defn connection-out
  "The channel that reaches `peer-id`, or nil if we hold no connection to it.

  This is what makes \"sync with the peer that told me\" expressible. Without it
  a caller can only reach whichever connection wrote `[:pubsub :out]` last."
  [peer peer-id]
  (get-in @(registry peer) [:by-id peer-id]))

(defn- register!
  "Bind `peer-id` to `out`, and flush anything queued for it.

  The outbox exists because a dial is issued before the remote's identity is
  known: `:connect` opens a socket to an *address*, and the frame can only be
  released once a peer presenting the expected *id* has proved it."
  [S peer peer-id out]
  (let [reg (registry peer)
        pending (get-in @reg [:outbox peer-id])]
    (swap! reg (fn [r] (-> r
                           (assoc-in [:by-id peer-id] out)
                           (update :outbox dissoc peer-id))))
    (doseq [m pending]
      (put? S out m))
    peer-id))

(defn- unregister!
  "Drop every binding pointing at `out`. Returns the peer ids removed.

  The ids matter: the state machine has to be told the transport is gone, or
  it goes on believing in a connection nobody is draining — and will never
  redial, because a peer it thinks it is connected to is not a dial
  candidate."
  [peer out]
  (let [reg (registry peer)
        removed (->> (:by-id @reg)
                     (filter (fn [[_ o]] (= o out)))
                     (mapv key))]
    (swap! reg update :by-id
           (fn [m] (apply dissoc m removed)))
    removed))

(defn- log-rejected
  "A frame refused by the rate limiter. Counted so an operator can see a peer
  being throttled, and otherwise dropped in silence — answering a flood is
  participating in it."
  [ctx from]
  (swap! (:limiter ctx) update-in [:stats :dropped-frames] (fnil inc 0))
  nil)

(def ^:private frame-schema
  "What each overlay message type must look like on the wire.

  A map of message type to `{field predicate}`. A field absent from the map is
  unconstrained; a field present must satisfy the predicate or the frame is
  refused. This is a GATE, not a parser -- it exists so a handler can assume
  the shapes it destructures, not to validate semantics.

  It is here rather than in the pure machines on purpose: the machines are
  driven by the simulator with well-formed input, and scattering `number?`
  guards through five namespaces is both larger and easy to leave incomplete.
  One table is also reviewable in a way that scattered checks are not."
  (let [iset? (fn [x] (and (sequential? x)
                           (every? #(and (sequential? %) (= 2 (count %))
                                         (every? int? %)) x)))
        summary? (fn [x] (and (map? x)
                              (every? (fn [[_ epochs]]
                                        (and (map? epochs)
                                             (every? (fn [[e rs]] (and (int? e) (iset? rs)))
                                                     epochs)))
                                      x)))
        horizon? (fn [x] (and (map? x)
                              (every? (fn [[k v]] (and (sequential? k) (int? v))) x)))
        ranges? (fn [x] (and (sequential? x)
                             (every? #(and (map? %) (int? (:lo %)) (int? (:hi %))) x)))]
    {:gossip {:epoch int? :seq int? :hops int?}
     :have {:summary summary? :horizon horizon?}
     :want {:ranges ranges?}
     :interests {:topics coll? :carries coll?}
     :peers {:peers coll?}
     :dial {:addresses coll?}
     :dial-refused {:peers coll?}
     :content/announce {:keys coll?}
     :content/found {:providers coll?}
     :content/want-tree {:have coll?}
     :content/tree-batch {:nodes coll?}}))

(defn well-formed?
  "Does `payload` satisfy `frame-schema` for its type?

  Unknown types pass: a peer running a newer version may legitimately send
  something this one does not know, and the handlers already fall through to
  `:else`. The gate refuses malformed KNOWN messages, which is the class that
  reached `interval-set` arithmetic and killed the loop."
  [payload]
  (let [fields (get frame-schema (:type payload))]
    (or (nil? fields)
        (every? (fn [[k pred]]
                  (let [v (get payload k ::absent)]
                    (or (= ::absent v) (pred v))))
                fields))))

(defn- accept-or-drop!
  "Submit an event, or close the connection if the machine cannot keep up.

  `submit!` refuses when the event queue is full, and a peer that fills it is
  sending faster than we can process no matter how politely we ask. replikativ
  dropped the connection in exactly this case, which is better than the
  alternative we had — queueing until core.async threw."
  [S ctx peer out from payload]
  (if-not (well-formed? payload)
    ;; Refused, not fatal, and counted: a malformed frame is a fact about the
    ;; sender, not a reason to stop serving everyone else.
    (do (swap! (:limiter ctx) update-in [:stats :malformed] (fnil inc 0))
        (log/warn :overlay/malformed-frame {:from from :type (:type payload)})
        false)
    (or (submit! ctx {:type :message :from from :payload payload})
        (do
          (swap! (:limiter ctx) update-in [:stats :overload-drops] (fnil inc 0))
          (close! out)
          false))))

(defn- send-frame!
  "Send to `peer-id`, queueing if the connection is not yet identified."
  [S peer peer-id m]
  (let [reg (registry peer)]
    (if-let [out (get-in @reg [:by-id peer-id])]
      (put? S out m)
      ;; Bounded twice over: at most 8 frames for any one peer, and at most 64
      ;; peers awaiting identification. A peer that never identifies must not
      ;; accumulate frames, and a stream of dials to peers that never answer
      ;; must not accumulate outboxes. The membership dial timeout fails the
      ;; dial long before either bound matters — the bounds are there for when
      ;; it does not.
      (swap! reg (fn [r]
                   (let [r (update-in r [:outbox peer-id]
                                      (fn [q] (vec (take-last 8 (conj (or q []) m)))))]
                     (if (> (count (:outbox r)) 64)
                       (update r :outbox dissoc (first (sort (keys (dissoc (:outbox r) peer-id)))))
                       r)))))))

;; =============================================================================
;; Middleware — the data plane
;; =============================================================================

(defn middleware
  "kabel middleware carrying the overlay.

  Per connection: announce our signed identity, verify theirs, register the
  connection under their peer id, and funnel overlay frames into the runtime.
  Everything else passes through."
  [{:keys [ctx identity addresses seq-no require-signed?]
    :or {require-signed? true}}]
  (fn [[S peer [in out]]]
    (let [pass-in (chan)
          pass-out (chan)
          ;; Set once this connection's PROOF verifies; nil until then, which
          ;; is exactly the "only frames on an identified connection become
          ;; events" rule.
          conn-peer-id (atom nil)
          ;; The challenge we chose for this connection. A peer's proof is
          ;; worth nothing unless it answers a nonce we picked ourselves.
          our-nonce (id/new-nonce)
          ;; Their record, verified but not yet proven live.
          pending-record (atom nil)]

      ;; Announce ourselves immediately. Both ends do this unconditionally, so
      ;; the handshake needs no notion of who dialled whom.
      (go-try S
              (let [record (<? S (id/sign-record identity addresses (or seq-no 0)))]
                ;; Hex-encoded: the default codec is pr-str/edn, which cannot
                ;; round-trip a byte array. See `netz.identity/record->wire`.
                (>? S out {:type hello-type
                           :record (id/record->wire record)
                           :nonce our-nonce})))

      (go-loop-super S [m (<? S in)]
                     (if m
                       (do
                         ;; The same guard as `run!`, and needed independently:
                         ;; this loop decodes and verifies the hello BEFORE any
                         ;; event reaches the machine, so a malformed
                         ;; `:record` kills the connection here regardless of
                         ;; what protects the event loop. `id/wire->record`
                         ;; and `verify-gossip` both run on wholly unvalidated
                         ;; wire data.
                         (try
                           (cond
                             (= hello-type (:type m))
                             (let [record (id/wire->record (:record m))]
                               ;; A verified record says who they CLAIM to be.
                               ;; It does not say they are here: it is signed
                               ;; over constants of the identity, so anyone who
                               ;; has seen it can replay it. Hold it, and
                               ;; answer their challenge; registration waits
                               ;; for them to answer ours.
                               (when (and record (<? S (id/verify-record record))
                                          (string? (:nonce m)))
                                 (reset! pending-record record)
                                 (>? S out {:type proof-type
                                            :sig (<? S (id/sign-challenge identity
                                                                          (:nonce m)))})))

                             (= proof-type (:type m))
                             (when-let [record @pending-record]
                               (when (<? S (id/verify-challenge record our-nonce (:sig m)))
                                 (reset! conn-peer-id
                                         (register! S peer (:netz/peer-id record) out))))

                             (= frame-type (:type m))
                             ;; The connection's own peer id, remembered when
                             ;; its hello verified. This used to invert the
                             ;; ENTIRE registry -- `(into {} (map …))` over
                             ;; every connection -- on every inbound frame,
                             ;; which was the largest per-frame allocation in
                             ;; the hot path and grew with the peer count. The
                             ;; loop has known the answer since registration.
                             (when-let [from @conn-peer-id]
                               (let [payload (:payload m)
                                     ;; `swap!`, not read-modify-write. The old
                                     ;; `(rl/check @limiter …)` then `reset!`
                                     ;; lost every concurrent update, so the
                                     ;; limit was bypassable by parallelism --
                                     ;; and the `reset!` also discarded the
                                     ;; drop counters other paths `swap!` in,
                                     ;; so the metering under-reported exactly
                                     ;; when it mattered most.
                                     verdict (::verdict
                                              (swap! (:limiter ctx)
                                                     (fn [st]
                                                       (let [[st' v] (rl/check st from (now-ms))]
                                                         (assoc st' ::verdict v)))))]
                               ;; Synapse's shape: slow, then queue, then
                               ;; reject. Sleeping here IS the backpressure —
                               ;; it stops draining `in`, which backs up the
                               ;; socket, so a busy peer is throttled rather
                               ;; than dropped and a hostile one is cut off.
                                 (case verdict
                                   :slow (<? S (timeout 25))
                                   :queue (<? S (timeout 250))
                                   nil)
                                 (if (= :reject verdict)
                                   (log-rejected ctx from)
                                   (if (and require-signed? (= :gossip (:type payload)))
                                 ;; Checked at EVERY hop, not only at the
                                 ;; destination: a single authorised relay
                                 ;; would otherwise be able to inject anything
                                 ;; into the rest of the network.
                                     (when (<? S (verify-gossip S payload))
                                       (accept-or-drop! S ctx peer out from payload))
                                     (accept-or-drop! S ctx peer out from payload)))))

                             :else
                             (>? S pass-in m))
                           (catch #?(:clj Throwable :cljs :default) e
                             (swap! (:dropped ctx) inc)
                             (log/warn :overlay/frame-dropped
                                       {:type (:type m)
                                        :error #?(:clj (.getMessage ^Throwable e)
                                                  :cljs (str e))})))
                         (recur (<? S in)))
                       ;; The socket closed. Tell the machine about every peer
                       ;; that was reachable through it, so membership drops
                       ;; the connection and its dial policy becomes the
                       ;; reconnect policy — kabel has no reconnect of its own.
                       (do (doseq [pid (unregister! peer out)]
                             (submit! ctx {:type :disconnected :peer pid}))
                           (close! pass-in))))

      (go-loop-super S [m (<? S pass-out)]
                     (when m
                       (>? S out m)
                       (recur (<? S pass-out))))

      [S peer [pass-in pass-out]])))

;; =============================================================================
;; Effects — the control plane's hands
;; =============================================================================

(defn- gossip? [m] (= :gossip (:type (:payload m))))

(defn- outgoing
  "Send `m` to `to`, signing it first if we are its origin.

  Only our OWN publishes are signed; a forwarded message already carries its
  origin's credentials and must travel untouched, or forwarding would destroy
  the very signature the next hop checks. Re-signing on every send (including
  repair serves) keeps the state machine free of credentials entirely, and
  Ed25519 is deterministic, so the same message always yields the same bytes."
  [S peer ctx identity to m]
  (if (and (gossip? m)
           (= (:origin (:payload m)) (:id ctx))
           (not (d/signed? (:payload m))))
    (go
      (let [signed (<? S (sign-gossip S identity (:payload m)))]
        (send-frame! S peer to (assoc m :payload signed))))
    (send-frame! S peer to m)))

(defn kabel-effects
  "Real effects: kabel connections, core.async timers, and — when a store is
  supplied — durable persistence of verified content."
  [S peer ctx identity store]
  {:send!
   (fn [to m] (outgoing S peer ctx identity to m))

   :connect!
   (fn [to address m]
     (if (contains? (connections peer) to)
       (outgoing S peer ctx identity to m)
       (go
         (try
           ;; Queue first: the frame is released by `register!` once a peer
           ;; presenting `to` has proved it holds the key. An impostor at
           ;; `address` registers under its own id and never sees this.
           (outgoing S peer ctx identity to m)
           (<? S (peer/connect S peer address))
           (catch #?(:clj Exception :cljs js/Error) _
             ;; A failed dial needs no event: the membership layer already
             ;; armed a dial timeout, and letting the timeout be the single
             ;; failure path keeps one code path instead of two that must
             ;; agree.
             nil)))))

   :disconnect!
   (fn [to]
     (let [reg (registry peer)]
       (when-let [out (get-in @reg [:by-id to])]
         ;; Close only. Do NOT dissoc here: the socket's close path calls
         ;; `unregister!`, which finds every peer id bound to this `out` and
         ;; submits `:disconnected` for each. Removing the binding first makes
         ;; that search come up empty, so membership never hears the connection
         ;; went away, keeps it in `:connections` forever, and never redials --
         ;; a deliberate disconnect would strand the peer permanently.
         (close! out))))

   :schedule!
   (fn [delay payload]
     (go
       (<! (timeout (max 0 delay)))
       (submit! ctx {:type :timer :payload payload})))

   :persist!
   (fn [k v]
     (when store
       (go
         (try
           ;; Marked immutable: it verified against its own content address, so
           ;; it is safe to cache, serve and re-verify. That is the same
           ;; distinction konserve records in its own metadata.
           (<? S (store/-store! store [:blocks k] v))
           (catch #?(:clj Exception :cljs js/Error) _ nil)))))})

;; =============================================================================
;; Assembly
;; =============================================================================

(defn deferred-middleware
  "Returns `[middleware-fn install!]` for the chicken-and-egg at startup.

  `start!` needs a peer, and a peer needs its middleware, so one of them has to
  come first. Pass `middleware-fn` when constructing the peer, call `start!`,
  then `install!` the middleware it returns.

  This is not merely convenient — it is required on the server side, and the
  asymmetry is easy to get wrong:

  - `kabel.peer/connect` reads `:middleware` out of the **peer atom** each time
    a client connects, so swapping it after construction works there;
  - `kabel.peer/server-peer` closes over its `middleware` **argument** in the
    `go-loop-super` that accepts connections, so swapping the atom has no
    effect at all and every inbound connection silently runs the middleware the
    peer was built with.

  Until it is installed, the placeholder is identity middleware, so a
  connection accepted in the gap passes through unwrapped rather than failing."
  []
  (let [a (atom (fn [args] args))]
    [(fn [args] (@a args))
     (fn [mw] (reset! a mw))]))

(defn start!
  "Start an overlay on `peer`.

  Options:
  - `:identity`  — from `netz.identity/generate-identity`; loaded from the
                   store, or generated and stored, if absent
  - `:addresses` — our own addresses, announced to peers
  - `:seeds`     — configured peers `[{:peer-id … :addresses […] :group …}]`
  - `:topics`    — topics to subscribe to
  - `:store`     — a `netz.store.protocol/PPeerStore`; optional
  - `:seed`      — rng seed; derived from the peer id when absent

  Returns a channel yielding a context map with `:ctx` (the runtime) and
  `:middleware` (to install on the peer)."
  [S peer {:keys [identity addresses seeds topics store seed overlay-opts
                  require-signed?]}]
  (go-try S
          (let [identity (or identity
                             (when store (<? S (store/-load store :identity)))
                             (let [i (<? S (id/generate-identity))]
                               (when store (<? S (store/-store! store :identity i)))
                               i))
                peer-id (id/peer-id (:genesis identity))
                prev-epoch (when store (<? S (store/-load store :epoch)))
                epoch (store/monotonic-epoch prev-epoch)
                _ (when store (<? S (store/-store! store :epoch epoch)))
                book (when store (<? S (store/-load store :book)))
                state (-> (overlay/make-state peer-id
                                              (merge {:addresses addresses
                                                      :seeds seeds
                                                      :topics topics}
                                                     overlay-opts))
                          (assoc :rng (rng/make-rng (or seed (hash (str peer-id)))))
                          (assoc-in [:dissemination :epoch] epoch)
                          ;; A persisted book is the sticky/anchor-peer
                          ;; mitigation: peers that behaved well before start
                          ;; ahead of strangers after a restart.
                          (cond-> book (assoc-in [:membership :book] book)))
                ctx (make-runtime {:id peer-id :state state :effects nil})
                ctx (assoc ctx :effects (kabel-effects S peer ctx identity store))
                ;; The book is durable membership state, not content, so it
                ;; does not go through `:persist!` (which is content-shaped and
                ;; writes under `[:blocks …]`).
                ctx (cond-> ctx
                      store (assoc :persist-book
                                   (fn [book]
                                     (go (try (<? S (store/-store! store :book book))
                                              (catch #?(:clj Exception :cljs js/Error) _
                                                nil))))))]
            (run! S ctx)
            {:ctx ctx
             :peer-id peer-id
             :identity identity
             :epoch epoch
             :middleware (middleware {:ctx ctx
                                      :identity identity
                                      :addresses (vec addresses)
                                      :seq-no epoch
                                      :require-signed? (if (nil? require-signed?)
                                                         true
                                                         require-signed?)})})))

(defn set-deliver!
  "Install the application delivery function — what `[:deliver topic payload]`
  means. `netz.pubsub` sets this to `-apply-publish`."
  [{:keys [ctx]} f]
  (reset! (:deliver-fn ctx) f)
  nil)

(defn set-state-sync!
  "Install what a horizon gap means. `netz.pubsub` sets this to
  re-running the handshake, which IS the differential state sync."
  [{:keys [ctx]} f]
  (reset! (:state-sync-fn ctx) f)
  nil)

(defn subscribe-topics!
  "Add `topics` to what this peer subscribes to, so dissemination delivers them.

  Interest, not a request: nothing is asked of anybody. A peer forwards to us
  because we said we want it, and relays carry ranges covering it."
  [{:keys [ctx]} topics]
  (submit! ctx {:type :message
                :from :app
                :payload {:type :subscribe :topics (set topics)}}))

(defn publish!
  "Publish on a topic through a running overlay."
  [{:keys [ctx] :as _running} topic payload]
  (submit! ctx {:type :message
                :from :app
                :payload {:type :publish :topic topic :payload payload}}))

(defn warm!
  "Load `ks` from the durable store into the servable working set.

  A peer that restarts has its identity and address book back, but an empty
  content set — so it silently stops being a provider for everything it still
  holds on disk. Warming is what makes it a provider again, and it is
  deliberately explicit and caller-driven rather than automatic: the working
  set is bounded, and which of a large store's keys are worth holding resident
  is the caller's decision, not ours.

  Returns a channel yielding the number of values loaded."
  [S {:keys [ctx] :as _running} store ks]
  (go-try S
          (loop [ks (seq ks) n 0]
            (if-not ks
              n
              (let [k (first ks)
                    v (<? S (store/-load store [:blocks k]))]
                (if (some? v)
                  (do (submit! ctx {:type :message :from :app
                                    :payload {:type :content/loaded :key k :value v}})
                      (recur (next ks) (inc n)))
                  (recur (next ks) n)))))))

(defn overlay-state
  "Current overlay state — for inspection and tests."
  [{:keys [ctx] :as _running}]
  @(:state ctx))
