(ns netz.content-wire
  "Strict language-neutral single-block content discovery and transfer.

  Profile 1 uses CIDv1/raw/SHA-256 over opaque bytes.  It deliberately does
  not assign meaning to those bytes and does not expose the native Hasch keys
  used by `netz.content`."
  (:require [netz.identity :as identity]
            [netz.wire :as wire]))

(def capability "netz/content/1")
(def cid-prefix (identity/seq->bytes [0x01 0x55 0x12 0x20]))
(def cid-size 36)
(def request-size 16)
(def max-announced-cids 256)
(def max-providers 64)
(def max-block-bytes (* 1024 1024))
(def max-provider-ttl-ms (* 20 60 1000))

(defn raw-cid
  "The profile-1 CIDv1 for an opaque block."
  [data]
  (when-not (and (wire/byte-buffer? data)
                 (<= (identity/buf-length data) max-block-bytes))
    (throw (ex-info "block data must be bounded bytes"
                    {:type :netz.content-wire/invalid-block})))
  (identity/concat-bufs cid-prefix (identity/sha256 data)))

(defn valid-cid?
  [value]
  (and (wire/byte-buffer? value)
       (= cid-size (identity/buf-length value))
       (identity/bufs= cid-prefix (identity/sub-buf value 0 4))))

(defn- cid! [value]
  (when-not (valid-cid? value)
    (throw (ex-info "CID must be CIDv1/raw/SHA-256"
                    {:type :netz.content-wire/invalid-cid})))
  value)

(defn verify-block?
  "Whether opaque block bytes exactly match a profile-1 CID."
  [cid data]
  (boolean
   (try
     (and (valid-cid? cid) (wire/byte-buffer? data)
          (<= (identity/buf-length data) max-block-bytes)
          (identity/bufs= cid (raw-cid data)))
     (catch #?(:clj Throwable :cljs :default) _ false))))

(defn- exact-bytes! [value size message type]
  (when-not (and (wire/byte-buffer? value)
                 (= size (identity/buf-length value)))
    (throw (ex-info message {:type type})))
  value)

(defn- request! [value]
  (exact-bytes! value request-size "request must be exactly 16 bytes"
                :netz.content-wire/invalid-request))

(defn- authority! [value]
  (exact-bytes! value identity/peer-id-size
                "provider must be a full 32-byte Authority ID"
                :netz.content-wire/invalid-provider))

(defn- ordered-unique-bytes? [values]
  (or (empty? values)
      (loop [previous (identity/bytes->hex (first values))
             remaining (next values)]
        (if-let [value (first remaining)]
          (let [current (identity/bytes->hex value)]
            (and (neg? (compare previous current))
                 (recur current (next remaining))))
          true))))

(defn- canonical-bytes [validate values limit message type]
  (let [by-hex (reduce (fn [out value]
                         (let [value (validate value)]
                           (assoc out (identity/bytes->hex value) value)))
                       (sorted-map) values)]
    (when (> (count by-hex) limit)
      (throw (ex-info message {:type type})))
    (vec (vals by-hex))))

(defn- base-wire [{:keys [type request cid]} wire-type]
  (when-not (= type (keyword wire-type))
    (throw (ex-info "invalid content frame type"
                    {:type :netz.content-wire/invalid-frame})))
  {"v" 1 "type" wire-type "request" (request! request) "cid" (cid! cid)})

(defn frame->wire
  "Convert an internal portable content frame to its closed profile-1 map."
  [{:keys [type cids providers ttl-ms data] :as frame}]
  (case type
    :content/announce
    {"v" 1 "type" "content/announce"
     "cids" (canonical-bytes cid! cids max-announced-cids
                             "too many announced CIDs"
                             :netz.content-wire/announce-limit)}

    :content/find (base-wire frame "content/find")
    :content/want (base-wire frame "content/want")
    :content/dont-have (base-wire frame "content/dont-have")

    :content/found
    (do
      (when-not (and (integer? ttl-ms)
                     (<= 0 ttl-ms max-provider-ttl-ms))
        (throw (ex-info "provider TTL is outside the profile limit"
                        {:type :netz.content-wire/invalid-ttl})))
      (assoc (base-wire frame "content/found")
             "providers" (canonical-bytes authority! providers max-providers
                                          "too many providers"
                                          :netz.content-wire/provider-limit)
             "ttl_ms" ttl-ms))

    :content/block
    (do
      (when-not (verify-block? (:cid frame) data)
        (throw (ex-info "block data does not match CID"
                        {:type :netz.content-wire/hash-mismatch})))
      (assoc (base-wire frame "content/block") "data" data))

    (throw (ex-info "unknown portable content frame"
                    {:type :netz.content-wire/unknown-frame
                     :frame-type type}))))

(defn- base-from-wire [frame expected-type expected-keys]
  (when-not (and (= expected-keys (set (keys frame)))
                 (= expected-type (get frame "type")))
    (throw (ex-info "invalid content frame schema"
                    {:type :netz.content-wire/invalid-frame})))
  {:type (keyword expected-type)
   :request (request! (get frame "request"))
   :cid (cid! (get frame "cid"))})

(defn frame-from-wire
  "Validate one decoded closed profile-1 map and return internal data."
  [frame]
  (when-not (and (map? frame) (= 1 (get frame "v")))
    (throw (ex-info "invalid portable content frame"
                    {:type :netz.content-wire/invalid-frame})))
  (let [common #{"v" "type" "request" "cid"}]
    (case (get frame "type")
      "content/announce"
      (let [cids (get frame "cids")]
        (when-not (and (= #{"v" "type" "cids"} (set (keys frame)))
                       (vector? cids) (<= (count cids) max-announced-cids)
                       (every? valid-cid? cids)
                       (ordered-unique-bytes? cids))
          (throw (ex-info "invalid or noncanonical content announcement"
                          {:type :netz.content-wire/invalid-announcement})))
        {:type :content/announce :cids cids})

      "content/find" (base-from-wire frame "content/find" common)
      "content/want" (base-from-wire frame "content/want" common)
      "content/dont-have" (base-from-wire frame "content/dont-have" common)

      "content/found"
      (let [base (base-from-wire frame "content/found"
                                 (conj common "providers" "ttl_ms"))
            providers (get frame "providers")
            ttl (get frame "ttl_ms")]
        (when-not (and (vector? providers) (<= (count providers) max-providers)
                       (every? #(and (wire/byte-buffer? %)
                                     (= identity/peer-id-size
                                        (identity/buf-length %))) providers)
                       (ordered-unique-bytes? providers)
                       (integer? ttl) (<= 0 ttl max-provider-ttl-ms))
          (throw (ex-info "invalid content provider response"
                          {:type :netz.content-wire/invalid-found})))
        (assoc base :providers providers :ttl-ms ttl))

      "content/block"
      (let [base (base-from-wire frame "content/block" (conj common "data"))
            data (get frame "data")]
        (when-not (verify-block? (:cid base) data)
          (throw (ex-info "block data does not match CID"
                          {:type :netz.content-wire/hash-mismatch})))
        (assoc base :data data))

      (throw (ex-info "unknown portable content frame"
                      {:type :netz.content-wire/unknown-frame
                       :frame-type (get frame "type")})))))

(defn encode-frame [frame]
  (wire/encode-canonical (frame->wire frame)))

(defn decode-frame [bytes]
  (frame-from-wire (wire/decode bytes)))
