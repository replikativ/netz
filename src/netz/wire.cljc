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

(defn byte-buffer?
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

(def max-safe-integer 9007199254740991)
(def max-frontier-streams 1024)
(def max-frontier-bytes (* 256 1024))

(defn- safe-uint? [value]
  (and (integer? value) (<= 0 value max-safe-integer)))

(defn- origin-bytes
  [origin]
  (let [bytes (cond
                (byte-buffer? origin) origin
                (and (string? origin)
                     (re-matches #"(?i)[0-9a-f]{64}" origin))
                (identity/hex->bytes origin)
                :else nil)]
    (when-not (and bytes (= 32 (identity/buf-length bytes)))
      (throw (ex-info "frontier origin must be 32 bytes"
                      {:type :netz.wire/invalid-frontier-origin})))
    bytes))

(defn- frontier-entry
  [topic [[origin epoch stream-topic :as stream] through]]
  (when-not (and (vector? stream)
                 (= 3 (count stream))
                 (= topic stream-topic)
                 (string? topic)
                 (safe-uint? epoch)
                 (safe-uint? through))
    (throw (ex-info "invalid committed frontier"
                    {:type :netz.wire/invalid-frontier
                     :stream stream :through through :topic topic})))
  (let [origin (origin-bytes origin)]
    {:order [(identity/bytes->hex origin) epoch topic]
     :wire {"origin" origin
            "epoch" epoch
            "topic" topic
            "through" through}}))

(defn frontiers->wire
  "Convert internal `{[origin epoch topic] through}` frontiers to the canonical
  profile-1 array. Internal origins may be 32-byte values or 64-digit hex."
  [topic frontiers]
  (when-not (and (map? frontiers)
                 (<= (count frontiers) max-frontier-streams))
    (throw (ex-info "too many committed frontiers"
                    {:type :netz.wire/frontier-limit})))
  (let [entries (sort-by :order (map #(frontier-entry topic %) frontiers))]
    (when-not (= (count entries) (count (distinct (map :order entries))))
      (throw (ex-info "duplicate committed frontier"
                      {:type :netz.wire/duplicate-frontier})))
    (mapv :wire entries)))

(defn frontiers-from-wire
  "Validate a canonical profile-1 frontier array and return the internal map."
  [topic entries]
  (when-not (and (vector? entries)
                 (string? topic)
                 (<= (count entries) max-frontier-streams))
    (throw (ex-info "invalid committed frontier array"
                    {:type :netz.wire/invalid-frontiers})))
  (let [bytes (identity/buf-length (encode-canonical entries))]
    (when (> bytes max-frontier-bytes)
      (throw (ex-info "committed frontier encoding exceeds its byte limit"
                      {:type :netz.wire/frontier-byte-limit
                       :bytes bytes}))))
  (loop [remaining entries previous nil result {}]
    (if-let [entry (first remaining)]
      (let [origin (get entry "origin")
            epoch (get entry "epoch")
            entry-topic (get entry "topic")
            through (get entry "through")
            order [(when (byte-buffer? origin) (identity/bytes->hex origin))
                   epoch entry-topic]]
        (when-not (and (map? entry)
                       (= #{"origin" "epoch" "topic" "through"}
                          (set (keys entry)))
                       (byte-buffer? origin)
                       (= 32 (identity/buf-length origin))
                       (safe-uint? epoch)
                       (= topic entry-topic)
                       (safe-uint? through)
                       (or (nil? previous) (neg? (compare previous order))))
          (throw (ex-info "invalid or noncanonical committed frontier"
                          {:type :netz.wire/invalid-frontier-array})))
        (recur (next remaining) order
               (assoc result [(first order) epoch topic] through)))
      result)))

(defn encode-frontiers
  "Canonical encoded frontier bytes, enforcing the profile-1 byte ceiling."
  [topic frontiers]
  (let [encoded (encode-canonical (frontiers->wire topic frontiers))]
    (when (> (identity/buf-length encoded) max-frontier-bytes)
      (throw (ex-info "committed frontier encoding exceeds its byte limit"
                      {:type :netz.wire/frontier-byte-limit
                       :bytes (identity/buf-length encoded)})))
    encoded))
