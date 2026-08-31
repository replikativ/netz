(ns netz.replication.blob
  "Portable immutable-blob descriptor and resumable piece selection.

  This deliberately does not reuse `netz.chunk`, whose addresses are Hasch
  UUIDs over Clojure values. The durable-blob boundary is language-neutral:
  it hashes opaque bytes and a deterministic-CBOR text-key descriptor."
  (:require [netz.identity :as identity]
            [netz.wire :as wire]))

(def object-address-tag "netz/blob-object/v1\u0000")
(def piece-address-tag "netz/blob-piece/v1\u0000")
(def manifest-address-tag "netz/blob-manifest/v1\u0000")

(def default-opts
  {:piece-size 65536
   :max-pieces 16384})

(def max-piece-size (* 1024 1024))

(defn- tagged-hash [tag bytes]
  (identity/sha256
   (identity/concat-bufs (identity/utf8-bytes tag) bytes)))

(defn object-address [bytes]
  (tagged-hash object-address-tag bytes))

(defn piece-address [bytes]
  (tagged-hash piece-address-tag bytes))

(defn manifest-address [manifest]
  (tagged-hash manifest-address-tag (wire/encode-canonical manifest)))

(defn- positive-uint? [value]
  (and (int? value) (pos? value)))

(defn- bytes-n? [value n]
  (and (wire/byte-buffer? value)
       (= n (identity/buf-length value))))

(defn valid-manifest?
  [manifest]
  (and (map? manifest)
       (= #{"v" "type" "object" "length" "piece_size" "pieces"}
          (set (keys manifest)))
       (= 1 (get manifest "v"))
       (= "blob/manifest" (get manifest "type"))
       (bytes-n? (get manifest "object") 32)
       (int? (get manifest "length"))
       (not (neg? (get manifest "length")))
       (positive-uint? (get manifest "piece_size"))
       (<= (get manifest "piece_size") max-piece-size)
       (vector? (get manifest "pieces"))
       (<= (count (get manifest "pieces")) (:max-pieces default-opts))
       (every? #(bytes-n? % 32) (get manifest "pieces"))
       (= (count (get manifest "pieces"))
          (quot (+ (get manifest "length")
                   (get manifest "piece_size") -1)
                (get manifest "piece_size")))))

(defn validate-manifest [manifest]
  (when-not (valid-manifest? manifest)
    (throw (ex-info "Invalid portable blob manifest"
                    {:type :netz.replication/invalid-blob-manifest})))
  manifest)

(defn split
  "Split opaque bytes into a portable descriptor and indexed pieces."
  ([bytes] (split bytes {}))
  ([bytes opts]
   (when-not (wire/byte-buffer? bytes)
     (throw (ex-info "Blob value must be bytes"
                     {:type :netz.replication/blob-not-bytes})))
   (let [{:keys [piece-size max-pieces]} (merge default-opts opts)
         length (identity/buf-length bytes)]
     (when-not (and (positive-uint? piece-size)
                    (<= piece-size max-piece-size)
                    (positive-uint? max-pieces)
                    (<= max-pieces (:max-pieces default-opts)))
       (throw (ex-info "Invalid blob chunking options"
                       {:type :netz.replication/invalid-blob-options})))
     (let [piece-count (quot (+ length piece-size -1) piece-size)]
       (when (> piece-count max-pieces)
         (throw (ex-info "Blob exceeds the maximum piece count"
                         {:type :netz.replication/too-many-pieces
                          :pieces piece-count :max max-pieces})))
       (let [pieces (mapv (fn [index]
                            (let [data (identity/sub-buf
                                        bytes (* index piece-size)
                                        (min length (* (inc index) piece-size)))]
                              {:index index
                               :id (piece-address data)
                               :data data}))
                          (range piece-count))
             manifest {"v" 1
                       "type" "blob/manifest"
                       "object" (object-address bytes)
                       "length" length
                       "piece_size" piece-size
                       "pieces" (mapv :id pieces)}]
         {:object-id (get manifest "object")
          :manifest-id (manifest-address manifest)
          :manifest manifest
          :pieces pieces})))))

(defn expected-piece-length [manifest index]
  (validate-manifest manifest)
  (let [piece-count (count (get manifest "pieces"))]
    (when-not (and (int? index) (<= 0 index) (< index piece-count))
      (throw (ex-info "Piece index is outside the manifest"
                      {:type :netz.replication/invalid-piece-index
                       :index index})))
    (if (= index (dec piece-count))
      (- (get manifest "length")
         (* index (get manifest "piece_size")))
      (get manifest "piece_size"))))

(defn verify-piece?
  [manifest index data]
  (and (wire/byte-buffer? data)
       (= (expected-piece-length manifest index) (identity/buf-length data))
       (identity/bufs= (nth (get manifest "pieces") index)
                       (piece-address data))))

(defn- bitmap-size [piece-count]
  (quot (+ piece-count 7) 8))

(defn need-bitmap
  "A compact bitset whose set indices are pieces the receiver still needs.

  Bits are most-significant first within each byte, so index 0 is 0x80. `held`
  is a set or predicate over piece addresses."
  [manifest held]
  (validate-manifest manifest)
  (let [addresses (get manifest "pieces")
        octets (reduce (fn [result [index address]]
                         (if (held address)
                           result
                           (let [octet-index (quot index 8)
                                 mask (bit-shift-left 1 (- 7 (mod index 8)))]
                             (assoc result octet-index
                                    (bit-or (nth result octet-index) mask)))))
                       (vec (repeat (bitmap-size (count addresses)) 0))
                       (map-indexed vector addresses))]
    (identity/seq->bytes octets)))

(defn needed-indices
  "Decode and validate a need bitmap for `manifest`."
  [manifest bitmap]
  (validate-manifest manifest)
  (let [piece-count (count (get manifest "pieces"))
        expected (bitmap-size piece-count)]
    (when-not (and (wire/byte-buffer? bitmap)
                   (= expected (identity/buf-length bitmap)))
      (throw (ex-info "Need bitmap has the wrong size"
                      {:type :netz.replication/invalid-need-bitmap})))
    (let [indices (filterv (fn [index]
                             (not (zero?
                                   (bit-and
                                    (bit-and 0xff (aget bitmap (quot index 8)))
                                    (bit-shift-left 1 (- 7 (mod index 8)))))))
                           (range piece-count))
          unused (- (* expected 8) piece-count)]
      (when (and (pos? unused)
                 (not (zero?
                       (bit-and (bit-and 0xff (aget bitmap (dec expected)))
                                (dec (bit-shift-left 1 unused))))))
        (throw (ex-info "Need bitmap sets bits beyond the manifest"
                        {:type :netz.replication/invalid-need-bitmap})))
      indices)))

(defn assemble
  "Verify indexed pieces, assemble the object, then verify its full address."
  [manifest pieces]
  (validate-manifest manifest)
  (let [piece-count (count (get manifest "pieces"))
        ordered (mapv (fn [index]
                        (let [data (get pieces index)]
                          (when-not (verify-piece? manifest index data)
                            (throw (ex-info "Missing or invalid blob piece"
                                            {:type :netz.replication/invalid-piece
                                             :index index})))
                          data))
                      (range piece-count))
        bytes (apply identity/concat-bufs ordered)]
    (when-not (and (= (get manifest "length") (identity/buf-length bytes))
                   (identity/bufs= (get manifest "object")
                                   (object-address bytes)))
      (throw (ex-info "Assembled blob does not match its object address"
                      {:type :netz.replication/object-mismatch})))
    bytes))
