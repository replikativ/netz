(ns netz.publication
  "Portable, self-verifying dissemination publications.

  A profile-1 publication signs deterministic control data containing the
  exact application byte string. Relays may alter only forwarding metadata;
  they never decode or reconstruct the application value."
  (:require [netz.identity :as identity]
            [netz.wire :as wire]))

(def signing-domain "netz/gossip/v1\u0000")
(def max-topic-bytes 1024)
(def max-payload-bytes (* 1024 1024))
(def max-safe-integer 9007199254740991)

(defn- safe-uint? [value]
  (and (integer? value) (<= 0 value max-safe-integer)))

(defn- authority-bytes [value]
  (let [bytes (cond
                (wire/byte-buffer? value) value
                (and (string? value)
                     (re-matches #"(?i)[0-9a-f]{64}" value))
                (identity/hex->bytes value)
                :else nil)]
    (when-not (and bytes (= identity/peer-id-size (identity/buf-length bytes)))
      (throw (ex-info "publication origin must be a full Authority ID"
                      {:type :netz.publication/invalid-origin})))
    bytes))

(defn- checked-topic [value]
  (when-not (and (string? value)
                 (pos? (count value))
                 (<= (identity/buf-length (identity/utf8-bytes value))
                     max-topic-bytes))
    (throw (ex-info "publication topic must be bounded non-empty UTF-8 text"
                    {:type :netz.publication/invalid-topic})))
  value)

(defn- checked-payload [value]
  (when-not (and (wire/byte-buffer? value)
                 (<= (identity/buf-length value) max-payload-bytes))
    (throw (ex-info "profile-1 publication payload must be bounded opaque bytes"
                    {:type :netz.publication/invalid-payload})))
  value)

(defn signing-object
  "Return the exact deterministic-CBOR object covered by the origin signature."
  [{:keys [origin-authority epoch seq topic payload]}]
  (when-not (and (safe-uint? epoch) (safe-uint? seq))
    (throw (ex-info "publication epoch and sequence must be safe uints"
                    {:type :netz.publication/invalid-sequence})))
  {"v" 1
   "type" "gossip"
   "origin" (authority-bytes origin-authority)
   "epoch" epoch
   "seq" seq
   "topic" (checked-topic topic)
   "payload" (checked-payload payload)})

(defn signing-bytes
  "Language-neutral bytes signed by a profile-1 publication."
  [publication]
  (identity/concat-bufs
   (identity/utf8-bytes signing-domain)
   (wire/encode-canonical (signing-object publication))))

(defn portable?
  "Can `publication` cross the strict profile-1 boundary?"
  [publication]
  (try
    (signing-object publication)
    true
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn- genesis-from-wire [value]
  (when-not (and (map? value)
                 (= #{"v" "rotation" "threshold" "operational" "revoke"}
                    (set (keys value)))
                 (= 1 (get value "v")))
    (throw (ex-info "invalid publication genesis object"
                    {:type :netz.publication/invalid-genesis})))
  (let [rotation (get value "rotation")
        operational (get value "operational")
        threshold (get value "threshold")
        revoke (get value "revoke")]
    (when-not (and (vector? rotation) (seq rotation)
                   (vector? operational) (seq operational)
                   (every? #(and (wire/byte-buffer? %)
                                 (= identity/key-size (identity/buf-length %)))
                           (concat rotation operational [revoke]))
                   (integer? threshold) (pos? threshold)
                   (<= threshold (count rotation)))
      (throw (ex-info "invalid publication genesis fields"
                      {:type :netz.publication/invalid-genesis})))
    (identity/make-genesis
     {:rotation-keys (mapv identity/bytes->hex rotation)
      :rotation-threshold threshold
      :operational-keys (mapv identity/bytes->hex operational)
      :revocation-commit (identity/bytes->hex revoke)})))

(defn ->wire
  "Convert a signed internal publication to its strict profile-1 envelope."
  [{:keys [origin-authority origin-genesis origin-key origin-sig hops repair?]
    :as publication}]
  (let [signed (signing-object publication)
        operational (if (wire/byte-buffer? origin-key)
                      origin-key (identity/hex->bytes origin-key))
        signature (if (wire/byte-buffer? origin-sig)
                    origin-sig (identity/hex->bytes origin-sig))]
    (when-not (and (identity/genesis? origin-genesis)
                   (= identity/key-size (identity/buf-length operational))
                   (= identity/signature-size (identity/buf-length signature))
                   (identity/genesis-authorises? origin-genesis operational
                                                 origin-authority)
                   (safe-uint? hops))
      (throw (ex-info "invalid profile-1 publication credentials or forwarding data"
                      {:type :netz.publication/invalid-envelope})))
    (cond-> (assoc signed
                   "genesis" (identity/genesis->wire origin-genesis)
                   "operational" operational
                   "signature" signature
                   "hops" hops)
      repair? (assoc "repair" true))))

(defn from-wire
  "Validate and convert a decoded profile-1 publication to runtime form.

  Cryptographic verification remains async and is performed by the runtime."
  [value]
  (let [required #{"v" "type" "origin" "epoch" "seq" "topic" "payload"
                   "genesis" "operational" "signature" "hops"}
        allowed (conj required "repair")]
    (when-not (and (map? value)
                   (= 1 (get value "v")) (= "gossip" (get value "type"))
                   (every? allowed (keys value))
                   (every? #(contains? value %) required)
                   (or (not (contains? value "repair"))
                       (true? (get value "repair"))))
      (throw (ex-info "invalid profile-1 publication schema"
                      {:type :netz.publication/invalid-envelope})))
    (let [genesis (genesis-from-wire (get value "genesis"))
          authority (authority-bytes (get value "origin"))
          operational (get value "operational")
          signature (get value "signature")
          publication (cond-> {:type :gossip
                               :wire-version 1
                               :origin (identity/peer-id genesis)
                               :origin-authority (identity/bytes->hex authority)
                               :origin-genesis genesis
                               :origin-key (identity/bytes->hex operational)
                               :origin-sig (identity/bytes->hex signature)
                               :epoch (get value "epoch")
                               :seq (get value "seq")
                               :topic (get value "topic")
                               :payload (get value "payload")
                               :hops (get value "hops")}
                        (true? (get value "repair")) (assoc :repair? true))]
      (signing-object publication)
      (when-not (and (wire/byte-buffer? operational)
                     (= identity/key-size (identity/buf-length operational))
                     (wire/byte-buffer? signature)
                     (= identity/signature-size (identity/buf-length signature))
                     (safe-uint? (:hops publication)))
        (throw (ex-info "invalid profile-1 publication fields"
                        {:type :netz.publication/invalid-envelope})))
      publication)))

(defn encode [publication]
  (wire/encode-canonical (->wire publication)))

(defn decode [bytes]
  (from-wire (wire/decode bytes)))
