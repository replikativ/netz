(ns netz.wire
  "Language-neutral bytes for Netz wire profile 1.

  Protocol state remains ordinary Clojure data internally. Values crossing a
  standards-compatible boundary use maps with TEXT keys and the portable CBOR
  subset documented in `spec/netz-draft-00.md`; no keywords, sets, ratios,
  records or implementation-specific tags are accepted by that profile."
  (:require [boring.core :as boring]
            [netz.identity :as identity]))

(def profile-version 1)

(def ^:private canonical-options
  {:profile :canonical
   :stringref false
   :max-depth 64})

(def ^:private decode-options
  {:stringref false
   :max-depth 64
   :max-items 16384
   :tolerate-unknown-tags false
   :on-unknown-record :error})

(def ^:private max-frame-bytes (* 5 1024 1024))

#?(:clj (def ^:private byte-array-class (Class/forName "[B")))

(defn- byte-buffer?
  [value]
  #?(:clj (= byte-array-class (class value))
     :cljs (instance? js/Uint8Array value)))

(defn- int64?
  [value]
  #?(:clj (and (integer? value)
               (<= Long/MIN_VALUE value Long/MAX_VALUE))
     :cljs (or (and (number? value) (js/Number.isSafeInteger value))
               (and (= "bigint" (js* "typeof ~{}" value))
                    (<= (js/BigInt "-9223372036854775808") value)
                    (<= value (js/BigInt "9223372036854775807"))))))

(defn- portable-value?
  [value]
  (cond
    (or (nil? value) (boolean? value) (string? value) (byte-buffer? value)) true
    (int64? value) true
    (sequential? value) (every? portable-value? value)
    (map? value) (every? (fn [[k v]]
                           (and (string? k) (portable-value? v)))
                         value)
    :else false))

(defn- assert-portable!
  [value]
  (when-not (portable-value? value)
    (throw (ex-info "value is outside the Netz profile-1 CBOR subset"
                    {:type :netz.wire/non-portable-value
                     :value-type (type value)})))
  value)

(defn encode-canonical
  "Encode a wire-profile value using RFC 8949 deterministic CBOR."
  [value]
  (boring/encode (assert-portable! value) canonical-options))

(defn decode
  "Decode exactly one deterministic profile-1 value.

  Refuses oversized input, tags/extensions, excessive depth/item counts,
  non-text map keys, floats, out-of-range integers, trailing bytes and
  non-deterministic encodings. Message-schema validation remains the caller's
  responsibility."
  [bytes]
  (when (> (identity/buf-length bytes) max-frame-bytes)
    (throw (ex-info "Netz CBOR frame exceeds the profile-1 limit"
                    {:type :netz.wire/frame-too-large
                     :bytes (identity/buf-length bytes)})))
  (let [value (assert-portable! (boring/decode bytes decode-options))
        canonical (encode-canonical value)]
    (when-not (and (= (identity/buf-length bytes)
                      (identity/buf-length canonical))
                   (identity/bufs= bytes canonical))
      (throw (ex-info "CBOR is not deterministic profile-1 encoding"
                      {:type :netz.wire/non-canonical})))
    value))
