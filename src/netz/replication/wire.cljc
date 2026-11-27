(ns netz.replication.wire
  "Portable signed statements and transfer frames for durable blob profile 1.

  A transport ACK is never a durability receipt.  The signed `blob/stored`
  statement below may only be produced after complete-object verification,
  atomic installation and the flush represented by its persistence profile.
  Application bytes remain opaque and are not decoded by this namespace."
  (:require [clojure.core.async :as async :refer [chan close! put!]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.layout :as layout]
            [netz.wire :as wire]))

(def capability "netz/blob-replication/1")
(def signing-domain "netz/blob-statement/v1\u0000")

(def persistence-profiles
  #{"netz/store/volatile/1"
    "netz/store/best-effort/1"
    "netz/store/fsync/1"})

(def ^:private statement-fields
  {"blob/retention"
   #{"v" "type" "cluster" "privacy_domain" "revision" "manifest"
     "object" "layout_epoch" "replicas" "write_acks"
     "placement_minimum_domains" "ack_minimum_domains"
     "issued_at" "retain_until" "signer"}

   "blob/lease"
   #{"v" "type" "cluster" "privacy_domain" "revision" "manifest"
     "object" "layout_epoch" "issued_at" "retain_until" "signer"}

   "blob/deletion"
   #{"v" "type" "cluster" "privacy_domain" "revision" "manifest"
     "object" "layout_epoch" "issued_at" "delete_after" "signer"}

   "blob/retirement"
   #{"v" "type" "cluster" "privacy_domain" "revision" "manifest"
     "object" "layout_epoch" "layout" "previous_layout" "replica"
     "retention" "receipts" "issued_at" "retire_after" "signer"}

   "blob/stored"
   #{"v" "type" "cluster" "privacy_domain" "retention_revision"
     "manifest" "object" "layout_epoch" "replica" "attempt"
     "persistence" "stored_at" "retain_until" "signer"}

   "blob/deleted"
   #{"v" "type" "cluster" "privacy_domain" "deletion_revision"
     "manifest" "object" "layout_epoch" "replica" "persistence"
     "deleted_at" "signer"}})

(def ^:private frame-fields
  {"blob/offer"
   #{"v" "type" "session" "attempt" "replica" "retention" "manifest"}
   "blob/fetch" #{"v" "type" "session" "attempt" "retention"}
   "blob/need" #{"v" "type" "session" "manifest" "need"}
   "blob/piece" #{"v" "type" "session" "manifest" "index" "data"}
   "blob/ack" #{"v" "type" "session" "manifest" "through"}
   "blob/commit" #{"v" "type" "session" "manifest" "count"}
   "blob/stored" #{"v" "type" "session" "receipt"}
   "blob/abort" #{"v" "type" "session" "code"}
   "blob/delete" #{"v" "type" "deletion"}
   "blob/deleted" #{"v" "type" "receipt"}})

(defn- safe-uint? [value]
  (and (integer? value) (<= 0 value wire/max-safe-integer)))

(defn- positive-uint? [value]
  (and (safe-uint? value) (pos? value)))

(defn- bytes-n? [n value]
  (and (wire/byte-buffer? value) (= n (identity/buf-length value))))

(defn- namespace-id? [value]
  (and (wire/byte-buffer? value)
       (<= 1 (identity/buf-length value) 64)))

(defn- exact-map? [fields value]
  (and (map? value) (= fields (set (keys value)))))

(defn- bounded-text? [limit value]
  (and (string? value)
       (pos? (count value))
       (<= (identity/buf-length (identity/utf8-bytes value)) limit)))

(declare parse-envelope)

(defn- nested-statement [envelope expected-type]
  (try
    (let [statement (:statement (parse-envelope envelope))]
      (when (= expected-type (get statement "type")) statement))
    (catch #?(:clj Throwable :cljs :default) _ nil)))

(defn- common-statement? [statement]
  (and (= 1 (get statement "v"))
       (namespace-id? (get statement "cluster"))
       (namespace-id? (get statement "privacy_domain"))
       (bytes-n? 32 (get statement "manifest"))
       (bytes-n? 32 (get statement "object"))
       (safe-uint? (get statement "layout_epoch"))
       (bytes-n? 32 (get statement "signer"))))

(defn valid-statement?
  "Whether `statement` is one exact profile-1 signed-statement body."
  [statement]
  (let [type (get statement "type")
        fields (get statement-fields type)
        replica (get statement "replica")
        signer (get statement "signer")]
    (if (= layout/statement-type type)
      (layout/valid-statement? statement)
      (and fields
           (exact-map? fields statement)
           (common-statement? statement)
           (case type
             "blob/retention"
             (let [replicas (get statement "replicas")
                   write-acks (get statement "write_acks")
                   placement-minima (get statement "placement_minimum_domains")
                   ack-minima (get statement "ack_minimum_domains")]
               (and (positive-uint? (get statement "revision"))
                    (positive-uint? replicas) (<= replicas 5)
                    (positive-uint? write-acks) (<= write-acks replicas)
                    (map? placement-minima)
                    (every? #(bounded-text? 128 %) (keys placement-minima))
                    (every? #(and (positive-uint? %) (<= % replicas))
                            (vals placement-minima))
                    (map? ack-minima)
                    (every? #(bounded-text? 128 %) (keys ack-minima))
                    (every? #(and (positive-uint? %) (<= % write-acks))
                            (vals ack-minima))
                    (safe-uint? (get statement "issued_at"))
                    (safe-uint? (get statement "retain_until"))
                    (<= (get statement "issued_at")
                        (get statement "retain_until"))))

             "blob/lease"
             (and (positive-uint? (get statement "revision"))
                  (safe-uint? (get statement "issued_at"))
                  (safe-uint? (get statement "retain_until"))
                  (<= (get statement "issued_at")
                      (get statement "retain_until")))

             "blob/deletion"
             (and (positive-uint? (get statement "revision"))
                  (safe-uint? (get statement "issued_at"))
                  (safe-uint? (get statement "delete_after"))
                  (<= (get statement "issued_at")
                      (get statement "delete_after")))

             "blob/retirement"
             (let [retention (nested-statement (get statement "retention")
                                               "blob/retention")
                   receipts (get statement "receipts")
                   receipt-statements (when (and (vector? receipts)
                                                 (<= 1 (count receipts) 5))
                                        (mapv #(nested-statement % "blob/stored")
                                              receipts))
                   receipt-replicas (when (every? some? receipt-statements)
                                      (mapv #(identity/bytes->hex
                                              (get % "replica"))
                                            receipt-statements))]
               (and (positive-uint? (get statement "revision"))
                    (bytes-n? 32 (get statement "layout"))
                    (bytes-n? 32 (get statement "previous_layout"))
                    (not= (identity/bytes->hex (get statement "layout"))
                          (identity/bytes->hex (get statement "previous_layout")))
                    (bytes-n? 32 (get statement "replica"))
                    retention
                    receipt-replicas
                    (= receipt-replicas (vec (sort receipt-replicas)))
                    (= (count receipt-replicas) (count (set receipt-replicas)))
                    (safe-uint? (get statement "issued_at"))
                    (safe-uint? (get statement "retire_after"))
                    (<= (get statement "issued_at")
                        (get statement "retire_after"))))

             "blob/stored"
             (and (positive-uint? (get statement "retention_revision"))
                  (bytes-n? 32 replica)
                  (identity/bufs= replica signer)
                  (safe-uint? (get statement "attempt"))
                  (contains? persistence-profiles
                             (get statement "persistence"))
                  (safe-uint? (get statement "stored_at"))
                  (safe-uint? (get statement "retain_until"))
                  (<= (get statement "stored_at")
                      (get statement "retain_until")))

             "blob/deleted"
             (and (positive-uint? (get statement "deletion_revision"))
                  (bytes-n? 32 replica)
                  (identity/bufs= replica signer)
                  (contains? persistence-profiles
                             (get statement "persistence"))
                  (safe-uint? (get statement "deleted_at")))

             false)))))

(defn validate-statement [statement]
  (when-not (valid-statement? statement)
    (throw (ex-info "Invalid durable-blob signed statement"
                    {:type :netz.replication/invalid-statement
                     :statement-type (get statement "type")})))
  statement)

(defn signing-bytes
  "Exact domain-separated bytes signed for every durable-blob statement."
  [statement]
  (identity/concat-bufs
   (identity/utf8-bytes signing-domain)
   (wire/encode-canonical (validate-statement statement))))

(defn envelope
  "Construct a strict self-verifying envelope around `statement` and signature."
  [genesis operational statement signature]
  (let [statement (validate-statement statement)
        signer (identity/bytes->hex (get statement "signer"))]
    (when-not (and (identity/genesis-authorises? genesis operational signer)
                   (bytes-n? 32 operational)
                   (bytes-n? identity/signature-size signature))
      (throw (ex-info "Statement credentials do not authorize its signer"
                      {:type :netz.replication/invalid-statement-credentials})))
    {"v" 1
     "type" "blob/signed"
     "statement" statement
     "genesis" (identity/genesis->wire genesis)
     "operational" operational
     "signature" signature}))

(defn parse-envelope
  "Validate an envelope structurally and return verification inputs."
  [value]
  (when-not (exact-map? #{"v" "type" "statement" "genesis"
                          "operational" "signature"} value)
    (throw (ex-info "Invalid durable-blob signed envelope"
                    {:type :netz.replication/invalid-envelope})))
  (let [statement (validate-statement (get value "statement"))
        genesis (wire/genesis-from-wire (get value "genesis") :netz.replication/invalid-genesis)
        operational (get value "operational")
        signature (get value "signature")
        signer (identity/bytes->hex (get statement "signer"))]
    (when-not (and (= 1 (get value "v"))
                   (= "blob/signed" (get value "type"))
                   (bytes-n? 32 operational)
                   (bytes-n? identity/signature-size signature)
                   (identity/genesis-authorises? genesis operational signer))
      (throw (ex-info "Invalid durable-blob envelope credentials"
                      {:type :netz.replication/invalid-envelope})))
    {:statement statement
     :genesis genesis
     :operational operational
     :signature signature}))

(defn sign-statement
  "Sign `statement` with a Netz identity. Returns a core.async channel."
  [{:keys [genesis operational]} statement]
  (let [ch (chan 1)]
    (async/go
      (try
        (let [signature (async/<! (identity/sign (:private operational)
                                                 (signing-bytes statement)))]
          (put! ch (if (instance? #?(:clj Exception :cljs js/Error) signature)
                     signature
                     (envelope genesis (:public operational)
                               statement signature))))
        (catch #?(:clj Exception :cljs js/Error) e
          (put! ch e)))
      (close! ch))
    ch))

(defn verify-envelope
  "Verify one self-contained statement envelope. Malformed input yields false."
  [value]
  (let [ch (chan 1)]
    (async/go
      (put! ch
            (boolean
             (try
               (let [{:keys [statement operational signature]}
                     (parse-envelope value)]
                 (async/<! (identity/verify operational
                                            (signing-bytes statement)
                                            signature)))
               (catch #?(:clj Exception :cljs js/Error) _ false))))
      (close! ch))
    ch))

(defn- valid-offer? [frame]
  (try
    (let [parsed (parse-envelope (get frame "retention"))
          retention (:statement parsed)
          manifest (blob/validate-manifest (get frame "manifest"))]
      (and (= "blob/retention" (get retention "type"))
           (bytes-n? 32 (get frame "replica"))
           (safe-uint? (get frame "attempt"))
           (identity/bufs= (get retention "manifest")
                           (blob/manifest-address manifest))
           (identity/bufs= (get retention "object")
                           (get manifest "object"))))
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn- valid-fetch? [frame]
  (try
    (let [retention (:statement (parse-envelope (get frame "retention")))]
      (and (= "blob/retention" (get retention "type"))
           (safe-uint? (get frame "attempt"))))
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn valid-frame?
  "Whether `frame` has the exact bounded durable-blob transfer schema."
  [frame]
  (let [type (get frame "type")
        fields (get frame-fields type)]
    (and fields
         (exact-map? fields frame)
         (= 1 (get frame "v"))
         (case type
           "blob/offer" (and (bytes-n? 16 (get frame "session"))
                             (valid-offer? frame))
           "blob/fetch" (and (bytes-n? 16 (get frame "session"))
                             (valid-fetch? frame))
           "blob/need" (and (bytes-n? 16 (get frame "session"))
                            (bytes-n? 32 (get frame "manifest"))
                            (wire/byte-buffer? (get frame "need"))
                            (<= (identity/buf-length (get frame "need")) 2048))
           "blob/piece" (and (bytes-n? 16 (get frame "session"))
                             (bytes-n? 32 (get frame "manifest"))
                             (safe-uint? (get frame "index"))
                             (wire/byte-buffer? (get frame "data"))
                             (<= (identity/buf-length (get frame "data"))
                                 blob/max-piece-size))
           "blob/ack" (and (bytes-n? 16 (get frame "session"))
                           (bytes-n? 32 (get frame "manifest"))
                           (safe-uint? (get frame "through")))
           "blob/commit" (and (bytes-n? 16 (get frame "session"))
                              (bytes-n? 32 (get frame "manifest"))
                              (safe-uint? (get frame "count")))
           "blob/stored" (and
                          (bytes-n? 16 (get frame "session"))
                          (try
                            (= "blob/stored"
                               (get-in (parse-envelope (get frame "receipt"))
                                       [:statement "type"]))
                            (catch #?(:clj Throwable :cljs :default) _ false)))
           "blob/abort" (and (bytes-n? 16 (get frame "session"))
                             (bounded-text? 128 (get frame "code")))
           "blob/delete" (try
                           (= "blob/deletion"
                              (get-in (parse-envelope (get frame "deletion"))
                                      [:statement "type"]))
                           (catch #?(:clj Throwable :cljs :default) _ false))
           "blob/deleted" (try
                            (= "blob/deleted"
                               (get-in (parse-envelope (get frame "receipt"))
                                       [:statement "type"]))
                            (catch #?(:clj Throwable :cljs :default) _ false))
           false))))

(defn validate-frame [frame]
  (when-not (valid-frame? frame)
    (throw (ex-info "Invalid durable-blob transfer frame"
                    {:type :netz.replication/invalid-frame
                     :frame-type (get frame "type")})))
  frame)

(defn encode-frame [frame]
  (wire/encode-canonical (validate-frame frame)))

(defn decode-frame [bytes]
  (validate-frame (wire/decode bytes)))
