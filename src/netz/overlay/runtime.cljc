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
  a failure. The eclipse defence falls out of self-certifying ids rather than
  being bolted on.

  ## Effects are injected

  The runtime takes an `effects` map rather than reaching for kabel directly,
  so the action interpreter and event loop are testable without opening a
  socket. `kabel-effects` supplies the real ones."
  (:refer-clojure :exclude [run!])
  (:require [clojure.string :as str]
            [replikativ.logging :as log]
            [netz.content :as content]
            [netz.dissemination :as d]
            [netz.identity :as id]
            [netz.publication :as publication]
            [netz.ratelimit :as rl]
            [netz.overlay :as overlay]
            [netz.sync :as sync]
            [netz.wire :as wire]
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
  "Second leg of the handshake: proof that the peer signed this transcript.

  A record alone is replayable -- see `netz.identity/challenge-bytes`. This
  carries a signature over both identities and both fresh nonces. It rejects
  stale proofs and the simple cross-connection signing oracle, but is not a
  transport channel binding: a live adversary can still relay the complete
  handshake. Untrusted transports require the additional session-integrity
  mechanism tracked in RELEASE.md."
  :netz.overlay/hello-proof)

(def hello-type :netz.overlay/hello)

(defn frame [payload] {:type frame-type :payload payload})

(def extension-carrier-capability
  "Capability selecting the portable named-extension carrier."
  "netz/extension-cbor/1")

(def extension-carrier-type
  "Reserved Kabel value identifying one portable extension envelope."
  extension-carrier-capability)

(def extension-envelope-type "netz/extension")

(defn- capability-name? [value]
  (and (string? value)
       (<= 1 (count value) 128)
       (every? (fn [ch]
                 (let [n #?(:clj (int ch)
                            :cljs (.charCodeAt ch 0))]
                   (<= 0x21 n 0x7e)))
               value)))

(defn encode-extension-envelope
  "Encode one named extension payload as deterministic Netz-CBOR bytes.

  These bytes are carried inside `extension-carrier-type`; keeping the closed
  text-key envelope canonical makes its representation identical in Clojure,
  ClojureScript and independent implementations even though Kabel's general
  Clojure-value serializer is intentionally not a canonicalization boundary."
  [capability payload]
  (when-not (capability-name? capability)
    (throw (ex-info "Invalid extension capability"
                    {:type :netz.overlay.runtime/invalid-capability
                     :capability capability})))
  (wire/encode-canonical
   {"v" 1
    "type" extension-envelope-type
    "capability" capability
    "payload" payload}))

(defn decode-extension-envelope
  "Decode and validate deterministic bytes produced by
  `encode-extension-envelope`. Returns the named capability and payload."
  [bytes]
  (let [envelope (wire/decode bytes)
        capability (get envelope "capability")]
    (when-not (and (map? envelope)
                   (= #{"v" "type" "capability" "payload"}
                      (set (keys envelope)))
                   (= 1 (get envelope "v"))
                   (= extension-envelope-type (get envelope "type"))
                   (capability-name? capability))
      (throw (ex-info "Invalid portable extension envelope"
                      {:type :netz.overlay.runtime/invalid-extension-envelope})))
    {:capability capability :payload (get envelope "payload")}))

(defn- cbor-head-bytes [length]
  (cond
    (< length 24) 1
    (<= length 0xff) 2
    (<= length 0xffff) 3
    (<= length 0xffffffff) 5
    :else 9))

(defn extension-carrier
  "Build the reserved Kabel value for one portable extension envelope."
  [capability payload]
  (let [inner (encode-extension-envelope capability payload)
        marker-bytes (id/buf-length (id/utf8-bytes extension-carrier-type))
        inner-bytes (id/buf-length inner)
        ;; The secure carrier's 5 MiB limit includes Kabel's four-byte
        ;; serializer prefix. The outer array adds one head for itself and one
        ;; each for the marker and inner byte string.
        envelope-bytes (+ 4 1
                          (cbor-head-bytes marker-bytes) marker-bytes
                          (cbor-head-bytes inner-bytes) inner-bytes)]
    (when (> envelope-bytes wire/max-frame-bytes)
      (throw (ex-info "Portable extension exceeds the Kabel frame limit"
                      {:type :netz.overlay.runtime/extension-frame-too-large
                       :bytes envelope-bytes
                       :max wire/max-frame-bytes})))
    [extension-carrier-type inner]))

(defn extension-carrier?
  "Whether `value` has the unambiguous portable extension carrier shape."
  [value]
  (and (vector? value)
       (= 2 (count value))
       (= extension-carrier-type (first value))
       (wire/byte-buffer? (second value))))

(defn overlay-message? [m]
  (or (extension-carrier? m)
      (and (map? m) (#{frame-type hello-type proof-type} (:type m)))))

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
   ;; Optional capability-gated protocols sharing the authenticated overlay
   ;; carrier. Each handler owns its own bounded event queue/state machines;
   ;; the overlay supplies connection identity, rate limiting and routing.
   :extensions (atom {})
   ;; Per-connection metering. Written as a pure state machine, kept in an atom
   ;; here because the middleware meters concurrently across connections.
   :limiter (atom (rl/make-state (or ratelimit {})))
   ;; Events dropped because a handler threw. A counter rather than silence:
   ;; a peer quietly discarding traffic is indistinguishable from a quiet
   ;; network, and that is the failure mode this guard could otherwise hide.
   :dropped (atom 0)
   ;; Lifecycle belongs to the overlay rather than to whichever socket happened
   ;; to be installed last. Timers and adapters consult this before submitting
   ;; more work, and adapters register their own cleanup functions below.
   :stopped? (atom false)
   :stop-fns (atom [])
   :events (chan 1024)})

(defn submit!
  "Hand an event to the runtime. Returns false when the queue is full.

  `offer!` rather than `put!`, deliberately. `put!` on a full channel queues,
  and core.async throws past 1024 pending puts — so overload arrived as an
  exception rather than as a decision. Refusing here lets the caller do what
  replikativ did and drop the connection, which is the correct answer to a peer
  sending faster than we can process."
  [ctx event]
  (and (not @(:stopped? ctx))
       (boolean (async/offer! (:events ctx) event))))

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
    :deliver (let [[from topic payload publication] args]
               (when-let [f @(:deliver-fn ctx)]
                 (f from topic payload publication)))

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
                msg (assoc msg :origin-authority (id/authority-id genesis))
                msg (cond-> msg
                      (publication/portable? msg)
                      (assoc :wire-version 1))
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
          (let [{:keys [origin origin-authority origin-genesis origin-key origin-sig]} msg]
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
                       (id/genesis-authorises? origin-genesis pk origin-authority)
                       (id/genesis-routes? origin-genesis origin)
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
                 (fn [existing]
                   (or existing (atom {:by-id {} :facts {} :outbox {}}))))
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

(defn connection-info
  "Authenticated facts retained for the current connection to `peer-id`.

  The map is deliberately transport-neutral:

      :authenticated? true
      :transport      :noise | :signed-hello
      :routing-id     Kabel's compact routing projection
      :authority-id   the full 32-byte Authority ID as lowercase hex
      :capabilities   the cryptographically negotiated capability set
      :identity-record the verified Netz identity record
      :handshake-hash  the Noise transcript hash when Noise is used

  A legacy signed-hello connection has an empty capability set.  Callers MUST
  therefore check an exact capability here before changing a wire format; the
  fact that a peer authenticated is not evidence that it understands the
  portable profile.  Returns nil when there is no current connection."
  [peer peer-id]
  (get-in @(registry peer) [:facts peer-id]))

(defn connection-supports?
  "Whether the current authenticated connection negotiated `capability`."
  [peer peer-id capability]
  (boolean (contains? (:capabilities (connection-info peer peer-id)) capability)))

(defn portable-sync-connection?
  "Whether `peer-id` selected every capability needed for portable sync."
  [peer peer-id]
  (every? #(connection-supports? peer peer-id %)
          [wire/profile-capability extension-carrier-capability
           sync/capability]))

(defn register-extension!
  "Register one capability-gated direct protocol on a running node.

  `handler` contains `:capability`, `:match?`, `:valid?`, `:receive!`, and an
  optional `:disconnected!`. `:receive!` receives the routing ID, immutable
  authenticated connection facts, and payload; returning false applies
  backpressure by closing that connection. Returns an idempotent cleanup fn."
  [{:keys [ctx]} extension-id
   {:keys [capability match? valid? receive!] :as handler}]
  (when-not (and (some? extension-id)
                 (string? capability) (seq capability)
                 (ifn? match?) (ifn? valid?) (ifn? receive!))
    (throw (ex-info "Invalid overlay extension handler"
                    {:type :netz.overlay.runtime/invalid-extension
                     :extension extension-id})))
  (when (= sync/capability capability)
    (throw (ex-info "The portable sync capability is reserved by Netz"
                    {:type :netz.overlay.runtime/reserved-extension-capability
                     :extension extension-id
                     :capability capability})))
  (swap! (:extensions ctx)
         (fn [extensions]
           (when (contains? extensions extension-id)
             (throw (ex-info "Overlay extension is already registered"
                             {:type :netz.overlay.runtime/duplicate-extension
                              :extension extension-id})))
           (assoc extensions extension-id handler)))
  (let [removed? (atom false)]
    (fn []
      (when (compare-and-set! removed? false true)
        (swap! (:extensions ctx) dissoc extension-id)
        true))))

(defn send-extension!
  "Send a direct extension payload only on a connection that negotiated its
  exact capability. Returns false instead of queueing across an unauthenticated
  or incompatible connection. A peer that also negotiated the language-neutral
  wire profile and dedicated extension-carrier capability receives the portable
  carrier; older Clojure peers retain the transitional native overlay frame."
  [{:keys [ctx peer]} to capability payload]
  (if (or (= sync/capability capability)
          (not (connection-supports? peer to capability)))
    false
    (do ((get-in ctx [:effects :send!])
         to
         (if (and (connection-supports? peer to wire/profile-capability)
                  (connection-supports? peer to extension-carrier-capability))
           (extension-carrier capability payload)
           (frame payload)))
        true)))

(defn disconnect-peer!
  "Close the current connection to `peer-id`. The normal socket cleanup path
  removes authenticated facts and notifies the overlay and every extension."
  [{:keys [ctx]} peer-id]
  ((get-in ctx [:effects :disconnect!]) peer-id))

(declare bounded-value?)

(defn- matching-extension [ctx capability payload]
  (reduce-kv (fn [found _id handler]
               (if (and (or (nil? capability)
                            (= capability (:capability handler)))
                        ((:match? handler) payload))
                 (if found
                   (throw (ex-info "More than one extension accepted a frame"
                                   {:type :netz.overlay.runtime/ambiguous-extension}))
                   handler)
                 found))
             nil @(:extensions ctx)))

(defn- dispatch-extension! [ctx peer from capability payload]
  (try
    (if-let [{handler-capability :capability :keys [valid? receive!]}
             (matching-extension ctx capability payload)]
      (let [facts (connection-info peer from)]
        (if (and (contains? (:capabilities facts) handler-capability)
                 (bounded-value? payload)
                 (valid? payload)
                 (receive! from facts payload))
          :accepted
          :rejected))
      :unmatched)
    (catch #?(:clj Throwable :cljs :default) e
      ;; Predicates and handlers see hostile wire input. A throw must have the
      ;; same fail-closed result as `false`, while remaining observable.
      (swap! (:dropped ctx) inc)
      (log/warn :overlay/extension-rejected
                {:from from
                 :capability capability
                 :error #?(:clj (.getMessage ^Throwable e)
                           :cljs (str e))})
      :rejected)))

(defn- meter-frame! [S ctx from]
  (go-try S
          (let [verdict (::verdict
                         (swap! (:limiter ctx)
                                (fn [st]
                                  (let [[st' v] (rl/check st from (now-ms))]
                                    (assoc st' ::verdict v)))))]
            (case verdict
              :slow (<? S (timeout 25))
              :queue (<? S (timeout 250))
              nil)
            verdict)))

(defn- extension-disconnected! [ctx peer-id]
  (doseq [{:keys [disconnected!]} (vals @(:extensions ctx))]
    (when disconnected! (disconnected! peer-id))))

(defn- register!
  "Bind `peer-id` to `out`, and flush anything queued for it.

  The outbox exists because a dial is issued before the remote's identity is
  known: `:connect` opens a socket to an *address*, and the frame can only be
  released once a peer presenting the expected *id* has proved it."
  ([S peer peer-id out]
   (register! S peer peer-id out nil))
  ([S peer peer-id out facts]
   (let [reg (registry peer)
         pending (get-in @reg [:outbox peer-id])]
     (swap! reg (fn [r]
                  (cond-> (-> r
                              (assoc-in [:by-id peer-id] out)
                              (update :outbox dissoc peer-id))
                    facts (assoc-in [:facts peer-id] facts)
                    (nil? facts) (update :facts dissoc peer-id))))
     (doseq [m pending]
       (put? S out m))
     peer-id)))

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
    (swap! reg (fn [r]
                 (-> r
                     (update :by-id (fn [m] (apply dissoc m removed)))
                     (update :facts (fn [m] (apply dissoc m removed))))))
    removed))

(defn- log-rejected
  "A frame refused by the rate limiter. Counted so an operator can see a peer
  being throttled, and otherwise dropped in silence — answering a flood is
  participating in it."
  [ctx from]
  (swap! (:limiter ctx) update-in [:stats :dropped-frames] (fnil inc 0))
  nil)

(def ^:private max-wire-items 16384)
(def ^:private max-wire-depth 64)
(def ^:private max-wire-string-bytes (* 1024 1024))
(def ^:private max-wire-bytes (* 5 1024 1024))

#?(:clj (def ^:private byte-array-class (Class/forName "[B")))

(defn- byte-count
  [x]
  #?(:clj (when (= (class x) byte-array-class) (alength ^bytes x))
     :cljs (when (instance? js/Uint8Array x) (.-byteLength x))))

(defn- text-byte-count
  [x]
  #?(:clj (alength (.getBytes ^String x java.nio.charset.StandardCharsets/UTF_8))
     :cljs (.-byteLength (.encode (js/TextEncoder.) x))))

(defn- bounded-value?
  "Bound decoded structure before a handler hashes, reduces or stores it.

  The transport must still enforce a raw frame-byte ceiling before decoding;
  this is the second line of defence and is portable across JVM and JS."
  [value]
  (letfn [(walk [stack remaining-items remaining-bytes]
            (if-let [[v depth] (peek stack)]
              (if (or (not (pos? remaining-items)) (> depth max-wire-depth))
                false
                (let [stack (pop stack)]
                  (cond
                    (string? v)
                    (let [n (text-byte-count v)]
                      (and (<= n max-wire-string-bytes)
                           (<= n remaining-bytes)
                           (walk stack (dec remaining-items)
                                 (- remaining-bytes n))))

                    (some? (byte-count v))
                    (let [n (byte-count v)]
                      (and (<= n max-wire-bytes)
                           (<= n remaining-bytes)
                           (walk stack (dec remaining-items) (- remaining-bytes n))))

                    (map? v)
                    (let [entries (seq v)
                          n (bounded-count (inc remaining-items) entries)]
                      (if (> n remaining-items)
                        false
                        (walk (into stack
                                    (mapcat (fn [[k x]] [[k (inc depth)] [x (inc depth)]])
                                            entries))
                              (dec remaining-items) remaining-bytes)))

                    (coll? v)
                    (let [xs (seq v)
                          n (bounded-count (inc remaining-items) xs)]
                      (and (<= n remaining-items)
                           (walk (into stack (map #(vector % (inc depth)) xs))
                                 (dec remaining-items) remaining-bytes)))

                    :else (walk stack (dec remaining-items) remaining-bytes))))
              true))]
    (boolean (walk [[value 0]] max-wire-items max-wire-bytes))))

(def ^:private frame-schema
  "Required and optional fields for every remotely accepted overlay message.

  Unknown message types are rejected. Protocol evolution happens through an
  explicit version/capability negotiation, not by feeding unknown input into a
  collection of state machines and hoping every one ignores it. Extra fields
  on a known type remain allowed for compatible extensions."
  (let [bounded-coll? (fn [n]
                        (fn [x] (and (coll? x)
                                     (<= (bounded-count (inc n) x) n))))
        addresses? (fn [x] (and ((bounded-coll? 16) x)
                                (every? #(and (string? %) (<= (count %) 2048)) x)))
        authority? (fn [x] (and (string? x)
                                (boolean (re-matches #"(?i)[0-9a-f]{64}" x))))
        entries? (fn [x] (and ((bounded-coll? 64) x) (every? map? x)))
        iset? (fn [x] (and ((bounded-coll? 512) x)
                           (every? #(and (sequential? %) (= 2 (count %))
                                         (every? int? %)) x)))
        summary? (fn [x] (and (map? x) (<= (count x) 512)
                              (every? (fn [[_ epochs]]
                                        (and (map? epochs) (<= (count epochs) 32)
                                             (every? (fn [[e streams]]
                                                       (and (int? e)
                                                            (map? streams)
                                                            (<= (count streams) 256)
                                                            (every? (fn [[_ rs]] (iset? rs))
                                                                    streams)))
                                                     epochs)))
                                      x)))
        horizon? (fn [x] (and (map? x) (<= (count x) 512)
                              (every? (fn [[k v]]
                                        (and (sequential? k) (= 3 (count k)) (int? v))) x)))
        ranges? (fn [x] (and ((bounded-coll? 256) x)
                             (every? #(and (map? %) (contains? % :topic)
                                           (int? (:lo %)) (int? (:hi %))) x)))
        nodes? (fn [x] (and ((bounded-coll? 256) x)
                            (every? #(and (map? %) (contains? % :key)
                                          (contains? % :value)) x)))]
    {:gossip {:required {:origin some? :origin-authority string?
                         :epoch int? :seq int? :topic some?
                         :hops int? :payload (constantly true)}}
     :have {:required {:summary summary? :horizon horizon?}}
     :want {:required {:ranges ranges?}}
     :interests {:required {:topics (bounded-coll? 1024)}
                 :optional {:carries (bounded-coll? 1024) :relay? boolean?}}
     :dial {:required {:addresses addresses?}
            :optional {:authority-id authority? :carries (bounded-coll? 1024)}}
     :dial-ok {:required {:addresses addresses?}
               :optional {:authority-id authority? :carries (bounded-coll? 1024)}}
     :dial-refused {:required {:reason keyword?}
                    :optional {:peers entries?}}
     :peers {:required {:entries entries?}}
     :disconnect {:required {}}
     :content/announce {:required {:keys (bounded-coll? 256)}}
     :content/find {:required {:key some?}}
     :content/found {:required {:key some? :providers (bounded-coll? 64)}
                     :optional {:ttl int?}}
     :content/want {:required {:key some?}}
     :content/block {:required {:key some? :value (constantly true)}}
     :content/dont-have {:required {:key some?}}
     :content/want-tree {:required {:root some? :have (bounded-coll? 4096)}}
     :content/tree-batch {:required {:root some? :nodes nodes?}}
     :content/tree-done {:required {:root some? :count int?
                                    :frontier (bounded-coll? 4096)}}
     :sync/open {:required {:session some? :topic some? :profile string?
                            :summary (constantly true)}}
     :sync/chunk {:required {:session some? :index int? :data (constantly true)}}
     :sync/ack {:required {:session some? :through int?}}
     :sync/done {:required {:session some? :count int? :topic some?
                            :checkpoint (constantly true) :frontiers horizon?}}
     :sync/abort {:required {:session some? :code some?}}}))

(defn well-formed?
  "Does `payload` satisfy the bounded schema for its known wire type?"
  [payload]
  (let [{:keys [required optional] :as schema} (get frame-schema (:type payload))]
    (boolean
     (and (map? payload)
          schema
          (bounded-value? payload)
          (every? (fn [[k pred]]
                    (and (contains? payload k) (pred (get payload k))))
                  required)
          (every? (fn [[k pred]]
                    (or (not (contains? payload k)) (pred (get payload k))))
                  optional)))))

(defn- sync-payload? [payload]
  (contains? #{:sync/open :sync/chunk :sync/ack :sync/done :sync/abort}
             (:type payload)))

(defn- accept-or-drop!
  "Submit an event, or close the connection if the machine cannot keep up.

  `submit!` refuses when the event queue is full, and a peer that fills it is
  sending faster than we can process no matter how politely we ask. replikativ
  dropped the connection in exactly this case, which is better than the
  alternative we had — queueing until core.async threw."
  [S ctx peer out from payload encoded-bytes]
  (if-not (well-formed? payload)
    ;; Refused, not fatal, and counted: a malformed frame is a fact about the
    ;; sender, not a reason to stop serving everyone else.
    (do (swap! (:limiter ctx) update-in [:stats :malformed] (fnil inc 0))
        (log/warn :overlay/malformed-frame {:from from :type (:type payload)})
        false)
    (or (submit! ctx (cond-> {:type :message :from from :payload payload}
                       encoded-bytes (assoc :encoded-bytes encoded-bytes)))
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
  [{:keys [ctx identity addresses seq-no require-signed? handshake-max-attempts
           handshake-timeout-ms authenticated-context-fn authorize-sync?]
    :or {require-signed? true handshake-max-attempts 4
         handshake-timeout-ms 15000
         authorize-sync? (constantly false)}}]
  (fn [[S peer [in out] :as connection]]
    (let [authorize-sync? (or authorize-sync? (constantly false))
          pass-in (chan)
          pass-out (chan)
          authenticated-context (when authenticated-context-fn
                                  (authenticated-context-fn connection))
          local-peer-id (id/peer-id (:genesis identity))
          local-authority-id (id/authority-id (:genesis identity))
          ;; Kabel preserves channel order, so make the two Netz producers
          ;; establish that order explicitly: the inbound loop cannot enqueue
          ;; a proof until the startup loop has enqueued our hello.
          hello-sent (chan)
          ;; Set once this connection's PROOF verifies; nil until then, which
          ;; is exactly the "only frames on an identified connection become
          ;; events" rule.
          conn-peer-id (atom nil)
          ;; The challenge we chose for this connection. A peer's proof is
          ;; worth nothing unless it answers a nonce we picked ourselves.
          our-nonce (id/new-nonce)
          ;; Their record, verified but not yet proven live.
          pending-record (atom nil)
          ;; Their challenge is part of the signed two-sided transcript. Keep
          ;; it beside the verified record so an independently obtained proof
          ;; cannot be substituted. A live full-transcript relay remains a
          ;; release blocker until the session itself is cryptographically
          ;; bound and subsequent frames have integrity.
          pending-nonce (atom nil)
          handshake-state (atom :pending)
          handshake-attempts (atom 0)
          ;; Application messages may race the identity exchange on a fresh
          ;; socket. They are held, not trusted, until proof completes.
          pending-app (atom [])
          attempt! (fn []
                     (let [n (swap! handshake-attempts inc)]
                       (if (<= n handshake-max-attempts)
                         true
                         (do (reset! handshake-state :failed)
                             (close! out)
                             false))))
          complete! (fn [record prover-nonce signature]
                      (go-try S
                              (when (and (= :pending @handshake-state)
                                         (<? S (id/verify-challenge
                                                record prover-nonce
                                                local-authority-id our-nonce
                                                signature)))
                                (reset! conn-peer-id
                                        (register!
                                         S peer (:netz/peer-id record) out
                                         {:authenticated? true
                                          :transport :signed-hello
                                          :routing-id (:netz/peer-id record)
                                          :authority-id (:netz/authority-id record)
                                          :capabilities #{}
                                          :identity-record record}))
                                (reset! handshake-state :complete)
                                (doseq [app @pending-app]
                                  (>? S pass-in app))
                                (reset! pending-app [])
                                true)))
          complete-authenticated-transport!
          (fn []
            (when (and authenticated-context
                       (= :pending @handshake-state))
              (let [facts @authenticated-context
                    record (:netz.transport.noise/authenticated-record facts)
                    routing-id (:netz.transport.noise/authenticated-routing-id facts)
                    authority (:kabel.transport/authenticated-authority facts)]
                (cond
                  (:netz.transport.noise/error facts)
                  (do (reset! handshake-state :failed) (close! out) false)

                  (and record routing-id authority
                       (= routing-id (:netz/peer-id record))
                       (= authority (:netz/authority-id record)))
                  (do
                    (reset! conn-peer-id
                            (register!
                             S peer routing-id out
                             {:authenticated? true
                              :transport :noise
                              :routing-id routing-id
                              :authority-id authority
                              :capabilities
                              (set (or (:kabel.transport/negotiated-capabilities
                                        facts)
                                       #{}))
                              :identity-record record
                              :handshake-hash
                              (:netz.transport.noise/handshake-hash facts)}))
                    (reset! handshake-state :complete)
                    (go-try S
                            (doseq [app @pending-app]
                              (>? S pass-in app))
                            (reset! pending-app []))
                    true)

                  :else false))))]

      ;; Announce ourselves immediately. Both ends do this unconditionally, so
      ;; the handshake needs no notion of who dialled whom.
      (if authenticated-context
        ;; Noise already exchanged and verified the same identity record inside
        ;; its transcript. Poll only until that lower middleware atomically
        ;; publishes it; no second public challenge is put on the wire.
        (go-loop-super S []
                       (when (= :pending @handshake-state)
                         (when-not (complete-authenticated-transport!)
                           (<? S (timeout 5))
                           (recur))))
        (go-try S
                (let [record (<? S (id/sign-record identity addresses (or seq-no 0)))]
                  ;; Hex-encoded: the default codec is pr-str/edn, which cannot
                  ;; round-trip a byte array. See `netz.identity/record->wire`.
                  (>? S out {:type hello-type
                             :record (id/record->wire record)
                             :nonce our-nonce})
                  (close! hello-sent))))

      ;; A peer that never finishes authentication must not retain a socket,
      ;; challenge, record and buffered application messages indefinitely.
      (go
        (<! (timeout handshake-timeout-ms))
        (when (= :pending @handshake-state)
          (reset! handshake-state :failed)
          (close! out)))

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
                           ;; A decrypted application frame cannot arrive until
                           ;; Noise has published the authenticated context. Do
                           ;; the registration synchronously too, closing the
                           ;; tiny scheduler race with the polling loop above.
                           (when authenticated-context
                             (complete-authenticated-transport!))
                           (cond
                             (= hello-type (:type m))
                             (if authenticated-context
                               (close! out)
                               (if-not (and (= :pending @handshake-state)
                                            (attempt!)
                                            (bounded-value? m))
                                 (close! out)
                                 (let [record (id/wire->record (:record m))]
                               ;; A verified record says who they CLAIM to be.
                               ;; It does not say they are here: it is signed
                               ;; over constants of the identity, so anyone who
                               ;; has seen it can replay it. Hold it, and
                               ;; answer their challenge; registration waits
                               ;; for them to answer ours.
                                   (when (and record (<? S (id/verify-record record))
                                              (string? (:nonce m))
                                              (= 64 (count (:nonce m))))
                                     (reset! pending-record record)
                                     (reset! pending-nonce (:nonce m))
                                     (<? S hello-sent)
                                     (>? S out {:type proof-type
                                                :sig (<? S (id/sign-challenge
                                                            identity our-nonce
                                                            (:netz/authority-id record)
                                                            (:nonce m)))})))))

                             (= proof-type (:type m))
                             (if authenticated-context
                               (close! out)
                               (if-not (and (= :pending @handshake-state)
                                            (attempt!)
                                            (bounded-value? m))
                                 (close! out)
                                 (when-let [record @pending-record]
                                   (<? S (complete! record @pending-nonce (:sig m))))))

                             ;; A reserved carrier is claimed only after the
                             ;; authenticated session negotiated profile 1.
                             ;; Other vectors (including ordinary application
                             ;; values) still pass through untouched.
                             (and (vector? m)
                                  (= extension-carrier-type (first m))
                                  (when-let [from @conn-peer-id]
                                    (and (connection-supports?
                                          peer from wire/profile-capability)
                                         (connection-supports?
                                          peer from
                                          extension-carrier-capability))))
                             (if-not (extension-carrier? m)
                               (close! out)
                               (when-let [from @conn-peer-id]
                                 (let [{:keys [capability payload] :as decoded}
                                       (try
                                         (decode-extension-envelope (second m))
                                         (catch #?(:clj Throwable :cljs :default) e
                                           ;; The reserved marker means this
                                           ;; is protocol input, not an
                                           ;; application value. Decoding
                                           ;; failure is therefore fatal to
                                           ;; the offending connection.
                                           (close! out)
                                           (throw e)))
                                       verdict (<? S (meter-frame! S ctx from))]
                                   (when decoded
                                     (if (= :reject verdict)
                                       (log-rejected ctx from)
                                       (if (= sync/capability capability)
                                         (if-not (connection-supports?
                                                  peer from sync/capability)
                                           (close! out)
                                           (let [frame
                                                 (sync/with-transport-mode
                                                   (try
                                                     (wire/sync-frame-from-wire
                                                      payload)
                                                     (catch #?(:clj Throwable
                                                               :cljs :default) e
                                                       (close! out)
                                                       (throw e)))
                                                   :portable)
                                                 authorized?
                                                 (or
                                                  (not= :sync/open (:type frame))
                                                  (try
                                                    (true?
                                                     (authorize-sync?
                                                      {:peer-id from
                                                       :connection
                                                       (connection-info peer from)
                                                       :topic (:topic frame)
                                                       :profile (:profile frame)}))
                                                    (catch #?(:clj Throwable
                                                              :cljs :default) e
                                                      (close! out)
                                                      (throw e))))]
                                             (if authorized?
                                               (accept-or-drop!
                                                S ctx peer out from frame
                                                (:kabel/encoded-bytes (meta m)))
                                               ;; Supporting a protocol is not
                                               ;; permission to read an
                                               ;; application's state. Avoid
                                               ;; disclosing whether the topic
                                               ;; or profile exists.
                                               (close! out))))
                                         (case (dispatch-extension!
                                                ctx peer from capability payload)
                                           :accepted true
                                         ;; A named portable extension can
                                         ;; never be an overlay or application
                                         ;; frame. Unknown, malformed and
                                           ;; handler-refused values fail closed.
                                           (:rejected :unmatched)
                                           (close! out))))))))

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
                                     verdict (<? S (meter-frame! S ctx from))]
                                 (if (= :reject verdict)
                                   (log-rejected ctx from)
                                   (if (and (sync-payload? payload)
                                            (portable-sync-connection? peer from))
                                     ;; Once selected, the strict portable
                                     ;; carrier is the only sync grammar on
                                     ;; this connection. Native input would be
                                     ;; a schema downgrade.
                                     (close! out)
                                     (case (dispatch-extension!
                                            ctx peer from nil payload)
                                       :accepted true
                                       :rejected (close! out)
                                       :unmatched
                                       (let [payload
                                             (if (sync-payload? payload)
                                               (sync/with-transport-mode
                                                 payload :native)
                                               payload)]
                                         (if (and require-signed?
                                                  (= :gossip (:type payload)))
                                           ;; Checked at EVERY hop, not only at the
                                           ;; destination: a single authorised relay
                                           ;; would otherwise be able to inject anything
                                           ;; into the rest of the network.
                                           (when (<? S (verify-gossip S payload))
                                             (accept-or-drop!
                                              S ctx peer out from payload
                                              (:kabel/encoded-bytes (meta m))))
                                           (accept-or-drop!
                                            S ctx peer out from payload
                                            (:kabel/encoded-bytes (meta m))))))))))

                             :else
                             (if (= :complete @handshake-state)
                               (>? S pass-in m)
                               (if (and (bounded-value? m)
                                        (< (count @pending-app) 32))
                                 (swap! pending-app conj m)
                                 (do (reset! handshake-state :failed)
                                     (close! out)))))
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
                             (extension-disconnected! ctx pid)
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

(defn- close-peer-connection! [peer peer-id]
  (when-let [out (get-in @(registry peer) [:by-id peer-id])]
    (close! out))
  false)

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
    (let [payload (:payload m)]
      (if-not (and (= frame-type (:type m)) (sync-payload? payload))
        (send-frame! S peer to m)
        ;; A synchronization session pins its carrier when it opens. Never
        ;; reinterpret a queued or later frame using mutable reconnect facts.
        (case (sync/transport-mode payload)
          :portable
          (if (portable-sync-connection? peer to)
            (send-frame! S peer to
                         (extension-carrier
                          sync/capability
                          (wire/sync-frame->wire payload)))
            (close-peer-connection! peer to))

          :native
          (if (and (connection-info peer to)
                   (not (portable-sync-connection? peer to)))
            (send-frame! S peer to m)
            (close-peer-connection! peer to))

          ;; Runtime-originated sync always carries a pinned mode. An
          ;; unmarked frame is an implementation error, not permission to
          ;; guess a wire format.
          (close-peer-connection! peer to))))))

(defn websocket-address?
  "Is `address` an absolute ws/wss URL without embedded credentials?

  This deliberately does not pretend to solve DNS rebinding or decide whether
  private networks are legitimate; P2P deployments commonly use them. Pass a
  stricter `:allow-address?` policy to `start!` at a public trust boundary."
  [address]
  (boolean
   (try
     (when (string? address)
       #?(:clj
          (let [u (java.net.URI. address)]
            (and (contains? #{"ws" "wss"} (some-> (.getScheme u) str/lower-case))
                 (some? (.getHost u))
                 (nil? (.getRawUserInfo u))
                 (nil? (.getRawFragment u))))
          :cljs
          (let [u (js/URL. address)]
            (and (contains? #{"ws:" "wss:"} (str/lower-case (.-protocol u)))
                 (seq (.-hostname u))
                 (empty? (.-username u))
                 (empty? (.-password u))
                 (empty? (.-hash u))))))
     (catch #?(:clj Exception :cljs :default) _ false))))

(defn configured-address-policy
  "Build a strict dial policy from configured peer/address records.

  The result has the `:allow-address?` shape accepted by `start!` and permits
  only an exact `(peer-id, address)` pair present in `peers`. It is the safe
  public-gateway policy when discovery is useful for routing information but
  must not turn untrusted peer exchange into an SSRF primitive.

  This intentionally does not resolve DNS. Operators accepting arbitrary
  discovered hostnames still need resolver/IP pinning at the socket adapter;
  a pre-dial hostname check cannot prevent DNS rebinding."
  ([peers] (configured-address-policy peers {}))
  ([peers {:keys [require-tls?] :or {require-tls? false}}]
   (let [pairs (set (for [{:keys [peer-id addresses]} peers
                          address addresses]
                      [peer-id address]))
         secure? (fn [address]
                   (if require-tls?
                     (boolean (re-matches #"(?i)wss://.*" address))
                     true))]
     (when (or (empty? pairs)
               (some (fn [[peer-id address]]
                       (or (nil? peer-id)
                           (not (websocket-address? address))
                           (not (secure? address))))
                     pairs))
       (throw (ex-info "configured dial policy requires valid peer/address pairs"
                       {:type :netz.overlay/invalid-address-policy
                        :require-tls? require-tls?})))
     (fn [{:keys [peer-id address]}]
       (contains? pairs [peer-id address])))))

(defn public-gateway-address-policy
  "Exact operator-configured public dial policy.

  Only listed `(Authority routing id, wss address)` pairs pass. In particular,
  a signed peer-exchange record cannot make the gateway resolve or connect to
  an attacker-selected hostname, private IP, or redirect target. The operator
  remains responsible for the DNS of its own allowlist; dynamic public
  discovery requires a socket adapter that validates and pins resolved IPs."
  [peers]
  (configured-address-policy peers {:require-tls? true}))

(defn kabel-effects
  "Real effects: kabel connections, core.async timers, and — when a store is
  supplied — durable persistence of verified content."
  [S peer ctx identity store {:keys [allow-address?]
                              :or {allow-address? (fn [{:keys [address]}]
                                                    (websocket-address? address))}}]
  {:send!
   (fn [to m] (outgoing S peer ctx identity to m))

   :connect!
   (fn [to address m]
     (if-not (allow-address? {:peer-id to :address address})
       (swap! (:limiter ctx) update-in [:stats :address-refused] (fnil inc 0))
       (if (contains? (connections peer) to)
         (outgoing S peer ctx identity to m)
         (go
           (try
           ;; Queue first: the frame is released by `register!` once a peer
           ;; presenting `to` has proved it holds the key. An impostor at
           ;; `address` registers under its own id and never sees this.
             (outgoing S peer ctx identity to m)
             (let [expected-authority
                   (get-in @(:state ctx) [:membership :book to :authority-id])]
               (if expected-authority
                 (<? S (peer/connect S peer address
                                     {:kabel.transport/expected-target
                                      expected-authority}))
                 (<? S (peer/connect S peer address))))
             (catch #?(:clj Exception :cljs js/Error) _
             ;; A failed dial needs no event: the membership layer already
             ;; armed a dial timeout, and letting the timeout be the single
             ;; failure path keeps one code path instead of two that must
             ;; agree.
               nil))))))

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
       (when-not @(:stopped? ctx)
         (submit! ctx {:type :timer :payload payload}))))

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
  - `:recovery-store` — a separate store for rotation/revocation secrets
  - `:allow-address?` — `(fn [{:keys [peer-id address]}] ...)`; defaults to
                        absolute credential-free ws/wss URLs
  - `:handshake-max-attempts` — signature-bearing hello/proof messages allowed
  - `:handshake-timeout-ms` — deadline for connection identity proof
  - `:authenticated-context-fn` — secure transport context accessor; when set,
                                  use its verified identity instead of the
                                  transitional public hello/proof exchange
  - `:authorize-sync?` — synchronous portable `sync/open` authorization over
                         authenticated connection facts, topic and profile;
                         omitted or non-true results reject the connection
  - `:seed`      — rng seed; derived from the peer id when absent

  Returns a channel yielding a context map with `:ctx` (the runtime) and
  `:middleware` (to install on the peer)."
  [S peer {:keys [identity addresses seeds topics store recovery-store seed
                  overlay-opts require-signed? allow-address?
                  handshake-max-attempts handshake-timeout-ms
                  authenticated-context-fn authorize-sync?]}]
  (go-try S
          (let [stored-identity (when (and (nil? identity) store)
                                  (<? S (store/-load store :identity)))
                generated? (and (nil? identity) (nil? stored-identity))
                identity (or identity stored-identity (<? S (id/generate-identity)))
                recovery (when generated? (id/recovery-material identity))
                ;; Ordinary storage gets only the hot key. Recovery material
                ;; may be written only when the caller supplied a distinct
                ;; storage boundary explicitly.
                _ (when (and generated? store)
                    (<? S (store/-store! store :identity (id/hot-identity identity))))
                _ (when (and generated? recovery-store recovery)
                    (<? S (store/-store! recovery-store :identity/recovery recovery)))
                peer-id (id/peer-id (:genesis identity))
                prev-epoch (when store (<? S (store/-load store :epoch)))
                epoch (store/monotonic-epoch prev-epoch)
                _ (when store (<? S (store/-store! store :epoch epoch)))
                book (when store (<? S (store/-load store :book)))
                state (-> (overlay/make-state peer-id
                                              (merge {:addresses addresses
                                                      :authority-id (id/authority-id
                                                                     (:genesis identity))
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
                ctx (assoc ctx :effects
                           (kabel-effects S peer ctx identity store
                                          {:allow-address? (or allow-address?
                                                               (fn [{:keys [address]}]
                                                                 (websocket-address? address)))})
                           :peer peer)
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
             :peer peer
             :peer-id peer-id
             :identity identity
             :recovery-material (when (and generated? (nil? recovery-store)) recovery)
             :epoch epoch
             :middleware (middleware {:ctx ctx
                                      :identity identity
                                      :addresses (vec addresses)
                                      :seq-no epoch
                                      :authenticated-context-fn
                                      authenticated-context-fn
                                      :authorize-sync?
                                      (or authorize-sync? (constantly false))
                                      :handshake-max-attempts (or handshake-max-attempts 4)
                                      :handshake-timeout-ms (or handshake-timeout-ms 15000)
                                      :require-signed? (if (nil? require-signed?)
                                                         true
                                                         require-signed?)})})))

(defn on-stop!
  "Register a zero-argument cleanup function owned by an adapter.

  If the overlay is already stopped, runs `f` immediately."
  [{:keys [ctx]} f]
  (if @(:stopped? ctx)
    (f)
    (swap! (:stop-fns ctx) conj f))
  nil)

(defn stop!
  "Stop a running overlay and release its in-process resources.

  This closes the event loop, adapter workers and every registered connection;
  timers may still wake, but observe `:stopped?` and submit no work. Idempotent;
  returns true only for the call that performed the transition. The owning
  Kabel server/listener remains the caller's lifecycle responsibility."
  [{:keys [ctx peer]}]
  (when (compare-and-set! (:stopped? ctx) false true)
    (doseq [f @(:stop-fns ctx)]
      (try (f) (catch #?(:clj Throwable :cljs :default) _ nil)))
    (let [reg (registry peer)]
      (doseq [out (distinct (vals (:by-id @reg)))]
        (close! out))
      (reset! reg {:by-id {} :facts {} :outbox {}}))
    (close! (:events ctx))
    true))

(defn set-deliver!
  "Install the application delivery function — what
  `[:deliver source topic payload publication]` means. `netz.pubsub` sets this to an
  ordered, bounded `-apply-publish` worker. The source exists so overload can
  recover through application state sync rather than becoming silent loss;
  the authenticated publication envelope lets the application receipt name
  the exact `(origin, epoch, topic, sequence)` it committed."
  [{:keys [ctx]} f]
  (reset! (:deliver-fn ctx) f)
  nil)

(defn application-committed!
  "Record an exact successful application receipt in the overlay event order.

  This is deliberately separate from transport delivery. A callback calls it
  only after the local profile has returned `{:ok true}`; slow, failed, or
  merely admitted publications therefore cannot enter a sync checkpoint."
  [{:keys [ctx]} publication]
  (submit! ctx {:type :message
                :from :app
                :payload {:type :application-committed
                          :publication (select-keys publication
                                                    [:origin :epoch :seq
                                                     :topic])}}))

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

(defn unsubscribe-topics!
  "Remove local topic interests and announce the reduced set to peers."
  [{:keys [ctx]} topics]
  (submit! ctx {:type :message
                :from :app
                :payload {:type :unsubscribe :topics (set topics)}}))

(defn unsubscribe-topics-ordered!
  "Queue local interest removal without best-effort overload loss.

  Returns a channel yielding true once the event owns a place in the runtime
  queue, or false if shutdown closes the queue first. Lifecycle callers await
  this result, so at most one such pending put exists behind their serializer."
  [{:keys [ctx]} topics]
  (let [result (chan 1)
        event {:type :message
               :from :app
               :payload {:type :unsubscribe :topics (set topics)}}
        finish! (fn [accepted?]
                  (put! result (boolean accepted?))
                  (close! result))]
    (if @(:stopped? ctx)
      (finish! false)
      (async/put! (:events ctx) event finish!))
    result))

(defn publish!
  "Publish on a topic through a running overlay."
  [{:keys [ctx] :as _running} topic payload]
  (submit! ctx {:type :message
                :from :app
                :payload {:type :publish :topic topic :payload payload}}))

(defn start-sync!
  "Open a transport-independent synchronization session. Returns the same
  immediate bounded-queue acceptance boolean as `publish!`. A live
  authenticated connection is required because the negotiated carrier is
  pinned for the lifetime of the session."
  [{:keys [ctx peer] :as _running} remote topic profile & [session]]
  (if-not (connection-info peer remote)
    false
    (let [portable? (portable-sync-connection? peer remote)
          mode (if portable? :portable :native)
          session (or session (when portable? (id/random-bytes 16)))]
      (if (and portable?
               (not (and (wire/byte-buffer? session)
                         (= 16 (id/buf-length session)))))
        false
        (submit! ctx {:type :message
                      :from :app
                      :payload
                      (sync/with-transport-mode
                        (cond-> {:type :sync/start :peer remote :topic topic
                                 :profile profile}
                          session (assoc :session session))
                        mode)})))))

(defn provide!
  "Make an immutable content-addressed `value` servable under `key`.

  Refuses a key/value mismatch before it reaches the state machine. Returns the
  same immediate acceptance boolean as `publish!`."
  [{:keys [ctx]} key value]
  (and (content/verified? key value)
       (submit! ctx {:type :message :from :app
                     :payload {:type :content/loaded :key key :value value
                               :immutable? true}})))

(defn fetch!
  "Begin fetching one content-addressed block. Returns whether the request was
  accepted by the runtime; completion is observable in `overlay-state`."
  [{:keys [ctx]} key]
  (submit! ctx {:type :message :from :app
                :payload {:type :content/fetch :key key}}))

(defn fetch-tree!
  "Begin fetching the DAG at `root`, pruning subtrees in `have`."
  [{:keys [ctx]} root have]
  (submit! ctx {:type :message :from :app
                :payload {:type :content/fetch-tree :root root
                          :have (vec have)}}))

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
