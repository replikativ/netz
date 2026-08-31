(ns netz.transport.noise
  "Authenticated direct-stream binding for Kabel.

  This middleware sits at Kabel's transport boundary, outside serialization.
  Noise XX authenticates a fresh per-connection X25519 static key; an
  operational Ed25519 key in the peer's signed identity record binds that
  static key to the full 32-byte Netz Authority ID.  Capability negotiation is
  part of the Noise transcript and every post-handshake serialized Kabel
  envelope is carried inside Noise transport encryption.

  The implementation deliberately owns each Noise CipherState from one
  core.async loop.  Callers may write concurrently to the returned Kabel
  channel, but encryption and nonces remain strictly serialized."
  (:require [clojure.core.async :as async :refer [chan close!]]
            [clojure.set :as set]
            [kabel.binary.table :as binary-table]
            [kabel.transport :as transport]
            [netz.identity :as identity]
            [netz.wire :as wire]
            [org.replikativ.geheimnis.codec :as codec]
            [org.replikativ.geheimnis.dh :as dh]
            [org.replikativ.geheimnis.noise :as noise]))

(def protocol-name "Noise_XX_25519_AESGCM_SHA256")
(def default-capabilities
  #{"netz/1" "transport/kabel-noise-cbor/1"})
(def default-required default-capabilities)

(def ^:private prologue (codec/str->bytes "netz/kabel-noise/v1\u0000"))
(def ^:private binding-domain
  (codec/str->bytes "netz/noise-static-binding/v1\u0000"))
(def ^:private handshake-kind 1)
(def ^:private transport-kind 2)
(def ^:private fragment-single 0)
(def ^:private fragment-start 1)
(def ^:private fragment-middle 2)
(def ^:private fragment-end 3)
(def ^:private fragment-data-limit (- noise/max-message-length 16 1))
(def ^:private max-envelope-bytes (* 5 1024 1024))

(defn- error-value? [x]
  #?(:clj (instance? Throwable x) :cljs (instance? js/Error x)))

(defn- fail [message data]
  (throw (ex-info message (assoc data :type :netz.transport/noise-error))))

(defn- unwrap [x context]
  (if (error-value? x)
    (throw (ex-info context {:type :netz.transport/crypto-error} x))
    x))

(defn- exact-keys? [m expected]
  (and (map? m) (= expected (set (keys m)))))

(defn- bytes-of-length? [n value]
  (and (wire/byte-buffer? value) (= n (identity/buf-length value))))

(defn- canonical-capabilities [values]
  (let [v (vec (sort values))]
    (when-not (and (<= (count v) 64)
                   (= (count v) (count (distinct v)))
                   (every? #(and (string? %) (<= 1 (count %) 128)) v))
      (fail "invalid capability set" {:capabilities values}))
    v))

(defn- checked-capabilities [values]
  (when-not (vector? values)
    (fail "capabilities must be a canonical array" {}))
  (let [canonical (canonical-capabilities values)]
    (when-not (= values canonical)
      (fail "capabilities are not sorted and unique" {}))
    (set canonical)))

(defn- genesis-from-wire [value]
  (when-not (exact-keys? value #{"v" "rotation" "threshold"
                                 "operational" "revoke"})
    (fail "malformed genesis object" {}))
  (let [rotation (get value "rotation")
        operational (get value "operational")
        threshold (get value "threshold")
        revoke (get value "revoke")]
    (when-not (and (= 1 (get value "v"))
                   (vector? rotation) (seq rotation)
                   (vector? operational) (seq operational)
                   (every? #(bytes-of-length? 32 %) rotation)
                   (every? #(bytes-of-length? 32 %) operational)
                   (integer? threshold) (pos? threshold)
                   (<= threshold (count rotation))
                   (bytes-of-length? 32 revoke))
      (fail "invalid genesis fields" {}))
    (identity/make-genesis
     {:rotation-keys (mapv identity/bytes->hex rotation)
      :rotation-threshold threshold
      :operational-keys (mapv identity/bytes->hex operational)
      :revocation-commit (identity/bytes->hex revoke)})))

(defn- record->profile [{genesis :netz/genesis public :netz/public-key
                         addresses :netz/addresses seq-no :netz/seq-no
                         signature :netz/signature}]
  {"v" 1
   "genesis" (identity/genesis->wire genesis)
   "authority" (identity/authority-id-bytes genesis)
   "operational" public
   "addresses" addresses
   "seq" seq-no
   "signature" signature})

(defn- profile->record [value]
  (when-not (exact-keys? value #{"v" "genesis" "authority" "operational"
                                 "addresses" "seq" "signature"})
    (fail "malformed identity record" {}))
  (let [genesis (genesis-from-wire (get value "genesis"))
        authority (get value "authority")
        public (get value "operational")
        addresses (get value "addresses")
        seq-no (get value "seq")
        signature (get value "signature")]
    (when-not (and (= 1 (get value "v"))
                   (bytes-of-length? 32 authority)
                   (identity/bufs= authority
                                   (identity/authority-id-bytes genesis))
                   (bytes-of-length? 32 public)
                   (vector? addresses) (<= (count addresses) 16)
                   (every? #(and (string? %) (<= (count %) 2048)) addresses)
                   (= addresses (vec (sort addresses)))
                   (integer? seq-no) (<= 0 seq-no wire/max-safe-integer)
                   (bytes-of-length? 64 signature))
      (fail "invalid identity record fields" {}))
    {:netz/genesis genesis
     :netz/public-key public
     :netz/authority-id (identity/authority-id genesis)
     :netz/peer-id (identity/peer-id genesis)
     :netz/addresses addresses
     :netz/seq-no seq-no
     :netz/signature signature}))

(defn- binding-bytes [authority static-key role capabilities required selected]
  (identity/concat-bufs
   binding-domain
   (wire/encode-canonical
    {"v" 1
     "protocol" protocol-name
     "authority" authority
     "noise_static" static-key
     "role" (name role)
     "capabilities" (canonical-capabilities capabilities)
     "required" (canonical-capabilities required)
     "selected" (canonical-capabilities selected)})))

(defn- offer-bytes [capabilities required]
  (wire/encode-canonical
   {"v" 1
    "type" "netz/noise/offer"
    "capabilities" (canonical-capabilities capabilities)
    "required" (canonical-capabilities required)}))

(defn- parse-offer [payload]
  (let [offer (wire/decode payload)]
    (when-not (and (exact-keys? offer #{"v" "type" "capabilities" "required"})
                   (= 1 (get offer "v"))
                   (= "netz/noise/offer" (get offer "type")))
      (fail "malformed Noise offer" {}))
    (let [capabilities (checked-capabilities (get offer "capabilities"))
          required (checked-capabilities (get offer "required"))]
      (when-not (set/subset? required capabilities)
        (fail "offered requirements are not capabilities" {}))
      {:capabilities capabilities :required required})))

(defn- selected-capabilities [local remote]
  (let [selected (set/intersection (:capabilities local) (:capabilities remote))]
    (when-not (and (seq selected)
                   (set/subset? (:required local) selected)
                   (set/subset? (:required remote) selected))
      (fail "no capability selection satisfies both peers"
            {:local-required (:required local)
             :remote-required (:required remote)
             :shared selected}))
    selected))

(defn- identity-payload [identity record static-key role capabilities required selected]
  (async/go
    (try
      (let [authority (identity/authority-id-bytes (:genesis identity))
            signature (unwrap
                       (async/<!
                        (identity/sign
                         (get-in identity [:operational :private])
                         (binding-bytes authority static-key role capabilities
                                        required selected)))
                       "cannot sign Noise static binding")]
        (wire/encode-canonical
         {"v" 1
          "type" "netz/noise/identity"
          "record" (record->profile record)
          "noise_static" static-key
          "role" (name role)
          "capabilities" (canonical-capabilities capabilities)
          "required" (canonical-capabilities required)
          "selected" (canonical-capabilities selected)
          "signature" signature}))
      (catch #?(:clj Throwable :cljs :default) e e))))

(defn- expected-authority-hex [value]
  (cond
    (nil? value) nil
    (and (string? value) (re-matches #"(?i)[0-9a-f]{64}" value))
    (.toLowerCase value)
    (bytes-of-length? 32 value) (identity/bytes->hex value)
    :else (fail "expected target must be a full 32-byte Authority ID"
                {:expected-target value})))

(defn- validate-identity-payload
  [payload remote-static expected-role local-offer expected-offer expected-authority]
  (async/go
    (try
      (let [value (wire/decode payload)]
        (when-not
         (and (exact-keys? value #{"v" "type" "record" "noise_static" "role"
                                   "capabilities" "required" "selected"
                                   "signature"})
              (= 1 (get value "v"))
              (= "netz/noise/identity" (get value "type"))
              (= (name expected-role) (get value "role"))
              (bytes-of-length? 32 (get value "noise_static"))
              (identity/bufs= remote-static (get value "noise_static"))
              (bytes-of-length? 64 (get value "signature")))
          (fail "malformed or mismatched Noise identity payload" {}))
        (let [record (profile->record (get value "record"))
              authority (identity/authority-id-bytes (:netz/genesis record))
              remote-offer {:capabilities
                            (checked-capabilities (get value "capabilities"))
                            :required (checked-capabilities (get value "required"))}
              selected (checked-capabilities (get value "selected"))
              expected-selected (selected-capabilities local-offer remote-offer)
              authority-hex (identity/bytes->hex authority)]
          (when (and expected-offer (not= expected-offer remote-offer))
            (fail "identity capabilities differ from the transcript offer" {}))
          (when-not (= selected expected-selected)
            (fail "peer selected a downgraded capability set"
                  {:selected selected :expected expected-selected}))
          (when (and expected-authority
                     (not= expected-authority authority-hex))
            (fail "remote Authority ID does not match the dial target"
                  {:expected expected-authority :actual authority-hex}))
          (when-not (true? (async/<! (identity/verify-record record)))
            (fail "remote identity record did not verify" {}))
          (when-not
           (true?
            (async/<!
             (identity/verify
              (:netz/public-key record)
              (binding-bytes authority remote-static expected-role
                             (:capabilities remote-offer)
                             (:required remote-offer) selected)
              (get value "signature"))))
            (fail "remote Noise static binding did not verify" {}))
          {:record record
           :authority authority-hex
           :routing-id (:netz/peer-id record)
           :capabilities selected}))
      (catch #?(:clj Throwable :cljs :default) e e))))

(defn- one-byte [n] (identity/seq->bytes [n]))

(defn- byte-at [bytes index]
  (bit-and 0xff #?(:clj (aget ^bytes bytes (int index))
                   :cljs (aget bytes index))))

(defn- wrap-frame [kind payload]
  {:kabel/serialization :binary
   :kabel/payload (identity/concat-bufs (one-byte kind) payload)})

(defn- unwrap-frame [frame]
  (when-not (and (map? frame)
                 (= :binary (:kabel/serialization frame))
                 (wire/byte-buffer? (:kabel/payload frame))
                 (pos? (identity/buf-length (:kabel/payload frame))))
    (fail "secure transport accepts only binary carrier frames" {}))
  (let [payload (:kabel/payload frame)]
    {:kind (byte-at payload 0)
     :payload (identity/sub-buf payload 1 (identity/buf-length payload))
     :host (:kabel/host frame)}))

(defn- send-raw! [raw-out frame]
  (async/go
    (try
      (when-not (async/>! raw-out frame)
        (fail "underlying Kabel connection closed" {}))
      true
      (catch #?(:clj Throwable :cljs :default) e e))))

(defn- read-handshake! [raw-in]
  (async/go
    (try
      (let [frame (async/<! raw-in)]
        (when-not frame (fail "connection closed during Noise handshake" {}))
        (let [{:keys [kind payload]} (unwrap-frame frame)]
          (when-not (= handshake-kind kind)
            (fail "application frame arrived before Noise completed" {:kind kind}))
          payload))
      (catch #?(:clj Throwable :cljs :default) e e))))

(defn- encode-envelope [{:keys [kabel/serialization kabel/payload]}]
  (when-not (and (= :cbor serialization) (wire/byte-buffer? payload))
    (fail "the secure profile accepts only encoded Kabel CBOR envelopes"
          {:serialization serialization}))
  (let [id (binary-table/encoding-for serialization)
        header (identity/seq->bytes [(bit-and 0xff (bit-shift-right id 24))
                                     (bit-and 0xff (bit-shift-right id 16))
                                     (bit-and 0xff (bit-shift-right id 8))
                                     (bit-and 0xff id)])
        encoded (identity/concat-bufs header payload)]
    (when (> (identity/buf-length encoded) max-envelope-bytes)
      (fail "encoded Kabel envelope exceeds secure binding limit"
            {:bytes (identity/buf-length encoded)
             :maximum max-envelope-bytes}))
    encoded))

(defn- decode-envelope [encoded host]
  (when (< (identity/buf-length encoded) 4)
    (fail "truncated Kabel envelope" {}))
  (let [id (+ (* (byte-at encoded 0) 0x1000000)
              (* (byte-at encoded 1) 0x10000)
              (* (byte-at encoded 2) 0x100)
              (byte-at encoded 3))
        serialization (binary-table/decoding-for id)
        result {:kabel/serialization serialization
                :kabel/payload (identity/sub-buf encoded 4
                                                 (identity/buf-length encoded))}]
    (when-not (= :cbor serialization)
      (fail "peer sent an unnegotiated Kabel serialization"
            {:serialization serialization}))
    (cond-> result host (assoc :kabel/host host))))

(defn- encrypt-envelope! [cipher raw-out envelope]
  (async/go
    (try
      (let [encoded (encode-envelope envelope)
            total (identity/buf-length encoded)]
        (loop [cipher cipher offset 0]
          (let [remaining (- total offset)
                amount (min fragment-data-limit remaining)
                end (+ offset amount)
                flag (cond
                       (and (zero? offset) (= end total)) fragment-single
                       (zero? offset) fragment-start
                       (= end total) fragment-end
                       :else fragment-middle)
                plaintext (identity/concat-bufs
                           (one-byte flag) (identity/sub-buf encoded offset end))
                encrypted (unwrap (async/<! (noise/encrypt-transport cipher plaintext))
                                  "Noise transport encryption failed")]
            (async/<! (send-raw! raw-out
                                 (wrap-frame transport-kind (:message encrypted))))
            (if (= end total)
              (:cipher encrypted)
              (recur (:cipher encrypted) end)))))
      (catch #?(:clj Throwable :cljs :default) e e))))

(defn- append-fragment [assembly payload]
  (let [total (+ (or (:total assembly) 0) (identity/buf-length payload))]
    (when (> total max-envelope-bytes)
      (fail "fragmented Kabel envelope exceeds secure binding limit"
            {:bytes total :maximum max-envelope-bytes}))
    {:parts (conj (or (:parts assembly) []) payload) :total total}))

(defn- complete-fragments [assembly]
  (apply identity/concat-bufs (:parts assembly)))

(defn- receive-loop! [cipher raw-in plain-in]
  (async/go
    (try
      (loop [cipher cipher assembly nil]
        (if-let [outer (async/<! raw-in)]
          (let [{:keys [kind payload host]} (unwrap-frame outer)]
            (when-not (= transport-kind kind)
              (fail "handshake or unknown frame after Noise completion" {:kind kind}))
            (let [decrypted (unwrap
                             (async/<! (noise/decrypt-transport cipher payload))
                             "Noise transport authentication failed")
                  plaintext (:payload decrypted)]
              (when (zero? (identity/buf-length plaintext))
                (fail "empty Noise transport fragment" {}))
              (let [flag (byte-at plaintext 0)
                    data (identity/sub-buf plaintext 1
                                           (identity/buf-length plaintext))]
                (case flag
                  0 (do
                      (when assembly (fail "single fragment interrupted an envelope" {}))
                      (when-not (async/>! plain-in (decode-envelope data host))
                        (fail "secure inbound channel closed" {}))
                      (recur (:cipher decrypted) nil))
                  1 (do
                      (when assembly (fail "nested envelope start" {}))
                      (recur (:cipher decrypted) (append-fragment nil data)))
                  2 (do
                      (when-not assembly (fail "middle fragment without start" {}))
                      (recur (:cipher decrypted) (append-fragment assembly data)))
                  3 (do
                      (when-not assembly (fail "end fragment without start" {}))
                      (let [complete (complete-fragments
                                      (append-fragment assembly data))]
                        (when-not (async/>! plain-in (decode-envelope complete host))
                          (fail "secure inbound channel closed" {})))
                      (recur (:cipher decrypted) nil))
                  (fail "unknown secure fragment kind" {:fragment-kind flag})))))
          (when assembly
            (fail "connection closed with a partial envelope" {}))))
      (catch #?(:clj Throwable :cljs :default) e e))))

(defn- install-context! [context handshake-status remote state]
  (when-not (compare-and-set! handshake-status :pending :authenticated)
    (fail "Noise handshake was cancelled" {}))
  (transport/update!
   context
   {::transport/authenticated-authority (:authority remote)
    ::transport/negotiated-capabilities (:capabilities remote)
    ::authenticated-record (:record remote)
    ::authenticated-routing-id (:routing-id remote)
    ::handshake-hash (identity/bytes->hex (noise/handshake-hash state))})
  remote)

(defn- handshake!
  [role raw-in raw-out context handshake-status
   {:keys [identity addresses seq-no capabilities required]}]
  (async/go
    (try
      (let [capabilities (set (or capabilities default-capabilities))
            required (set (or required default-required))
            local-offer {:capabilities (checked-capabilities
                                        (canonical-capabilities capabilities))
                         :required (checked-capabilities
                                    (canonical-capabilities required))}
            _ (when-not (set/subset? required capabilities)
                (fail "local requirements are not capabilities" {}))
            static (unwrap (async/<! (dh/generate-keypair))
                           "cannot generate Noise connection-static key")
            record (unwrap (async/<! (identity/sign-record identity
                                                           (vec addresses) seq-no))
                           "cannot sign identity record")
            expected (expected-authority-hex (::transport/expected-target @context))
            state-0 (noise/initialize role static {:prologue prologue})]
        (if (= role :initiator)
          (let [m1 (unwrap (async/<! (noise/write-message
                                      state-0 (offer-bytes capabilities required)))
                           "cannot write Noise message 1")
                _ (async/<! (send-raw! raw-out
                                       (wrap-frame handshake-kind (:message m1))))
                m2-bytes (unwrap (async/<! (read-handshake! raw-in))
                                 "cannot receive Noise message 2")
                m2 (unwrap (async/<! (noise/read-message (:state m1) m2-bytes))
                           "cannot read Noise message 2")
                selected (selected-capabilities
                          local-offer
                          ;; Payload validation re-parses and checks this exact
                          ;; value; decoding here is only to construct message 3.
                          (let [v (wire/decode (:payload m2))]
                            {:capabilities (checked-capabilities
                                            (get v "capabilities"))
                             :required (checked-capabilities (get v "required"))}))
                remote (unwrap
                        (async/<! (validate-identity-payload
                                   (:payload m2)
                                   (noise/remote-static-key (:state m2))
                                   :responder local-offer nil expected))
                        "responder identity validation failed")
                payload-3 (unwrap
                           (async/<! (identity-payload
                                      identity record (:public static) :initiator
                                      capabilities required selected))
                           "cannot build initiator identity payload")
                m3 (unwrap (async/<! (noise/write-message (:state m2) payload-3))
                           "cannot write Noise message 3")
                _ (async/<! (send-raw! raw-out
                                       (wrap-frame handshake-kind (:message m3))))]
            {:state (:state m3)
             :remote (install-context! context handshake-status remote (:state m3))})
          (let [m1-bytes (unwrap (async/<! (read-handshake! raw-in))
                                 "cannot receive Noise message 1")
                m1 (unwrap (async/<! (noise/read-message state-0 m1-bytes))
                           "cannot read Noise message 1")
                initiator-offer (parse-offer (:payload m1))
                selected (selected-capabilities local-offer initiator-offer)
                payload-2 (unwrap
                           (async/<! (identity-payload
                                      identity record (:public static) :responder
                                      capabilities required selected))
                           "cannot build responder identity payload")
                m2 (unwrap (async/<! (noise/write-message (:state m1) payload-2))
                           "cannot write Noise message 2")
                _ (async/<! (send-raw! raw-out
                                       (wrap-frame handshake-kind (:message m2))))
                m3-bytes (unwrap (async/<! (read-handshake! raw-in))
                                 "cannot receive Noise message 3")
                m3 (unwrap (async/<! (noise/read-message (:state m2) m3-bytes))
                           "cannot read Noise message 3")
                remote (unwrap
                        (async/<! (validate-identity-payload
                                   (:payload m3)
                                   (noise/remote-static-key (:state m3))
                                   :initiator local-offer initiator-offer expected))
                        "initiator identity validation failed")]
            {:state (:state m3)
             :remote (install-context! context handshake-status remote (:state m3))})))
      (catch #?(:clj Throwable :cljs :default) e e))))

(defn middleware
  "Create Kabel transport middleware for one local Netz identity.

  Options:

  * `:identity` is a hot identity returned by `netz.identity/generate-identity`;
  * `:addresses` and `:seq-no` form its signed connection identity record;
  * `:capabilities` is the supported set, and `:required` the subset without
    which the connection is refused; and
  * `:handshake-timeout-ms` defaults to 15000 and is bounded at 60000.

  The initiator may seed Kabel's connection context with
  `::kabel.transport/expected-target`; it must be the full Authority ID (hex or
  32 bytes), never the compact routing UUID."
  [{:keys [identity addresses seq-no handshake-timeout-ms] :as opts
    :or {addresses [] seq-no 0 handshake-timeout-ms 15000}}]
  (when-not (and identity (map? identity))
    (fail "secure transport requires a local identity" {}))
  (when-not (and (integer? handshake-timeout-ms)
                 (<= 1 handshake-timeout-ms 60000))
    (fail "Noise handshake timeout must be between 1 and 60000 ms"
          {:handshake-timeout-ms handshake-timeout-ms}))
  (fn [[S peer [raw-in raw-out] :as connection]]
    (let [context (transport/connection-context connection)
          plain-in (chan 16)
          plain-out (chan 16)]
      (when-not context
        (fail "secure transport requires Kabel connection context" {}))
      (let [role (::transport/role @context)]
        (async/go
          (let [handshake-status (atom :pending)
                operation (handshake! role raw-in raw-out context handshake-status
                                      (assoc opts :addresses addresses
                                             :seq-no seq-no))
                timer (async/timeout handshake-timeout-ms)
                [value port] (async/alts! [operation timer])
                result (if (= port timer)
                         (if (compare-and-set! handshake-status
                                               :pending :cancelled)
                           (ex-info "Noise handshake timed out"
                                    {:type :netz.transport/handshake-timeout
                                     :timeout-ms handshake-timeout-ms})
                           ;; The operation won the authentication transition
                           ;; while both ports became ready. Await its result
                           ;; rather than reporting a false timeout after the
                           ;; peer context was installed.
                           (async/<! operation))
                         value)]
            (if (error-value? result)
              (do
                (transport/update! context {::error result})
                (close! plain-in)
                (close! plain-out)
                (close! raw-in)
                (close! raw-out))
              (let [{:keys [send receive]}
                    (noise/transport-ciphers (:state result))]
                ;; Exactly one loop owns each CipherState. Even if many callers
                ;; put concurrently, a nonce cannot be reused or reordered.
                (async/go-loop [cipher send]
                  (if-let [envelope (async/<! plain-out)]
                    (let [next-cipher (async/<!
                                       (encrypt-envelope! cipher raw-out envelope))]
                      (if (error-value? next-cipher)
                        (do (transport/update! context {::error next-cipher})
                            (close! raw-in)
                            (close! raw-out))
                        (recur next-cipher)))
                    (close! raw-out)))
                (async/go
                  (let [receive-result (async/<! (receive-loop! receive raw-in plain-in))]
                    (when (error-value? receive-result)
                      (transport/update! context {::error receive-result})
                      (close! raw-in)
                      (close! raw-out))
                    (close! plain-in))))))))
      (transport/with-context [S peer [plain-in plain-out]] context))))
