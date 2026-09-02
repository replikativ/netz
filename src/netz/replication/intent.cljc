(ns netz.replication.intent
  "Closed portable records for a controller's durable replication outbox.

  Records retain the exact signed layout and retention envelopes, transfer
  manifest, per-target attempt counters and local intent revision needed to
  reconstruct the pure controller after a crash. Signature verification is
  asynchronous and remains separate from structural decoding."
  (:require [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.layout :as layout]
            [netz.replication.placement :as placement]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]
            #?(:clj [clojure.core.async :refer [go <!]]
               :cljs [clojure.core.async :refer [<!]
                      :refer-macros [go]])))

(def record-type "blob/replication-intent")
(def ^:private record-fields
  #{"v" "type" "cluster" "object" "manifest" "layout" "retention"
    "intent_revision" "attempts"})
(def ^:private attempt-fields #{"replica" "attempt"})

(defn- bytes-n? [n value]
  (and (wire/byte-buffer? value) (= n (identity/buf-length value))))

(defn- same-bytes? [a b]
  (and (wire/byte-buffer? a) (wire/byte-buffer? b)
       (identity/bufs= a b)))

(defn- same-value? [a b]
  (same-bytes? (wire/encode-canonical a) (wire/encode-canonical b)))

(defn- safe-uint? [value]
  (and (integer? value) (<= 0 value wire/max-safe-integer)))

(defn- cluster-id? [value]
  (and (wire/byte-buffer? value)
       (<= 1 (identity/buf-length value) 64)))

(defn- exact-map? [fields value]
  (and (map? value) (= fields (set (keys value)))))

(defn- statement [envelope expected-type]
  (let [value (:statement (replication-wire/parse-envelope envelope))]
    (when-not (= expected-type (get value "type"))
      (throw (ex-info "Unexpected durable intent statement type"
                      {:type :netz.replication/invalid-intent-statement
                       :expected expected-type
                       :actual (get value "type")})))
    value))

(defn- policy [retention]
  {:replicas (get retention "replicas")
   :write-acks (get retention "write_acks")
   :minimum-domains (get retention "minimum_domains")})

(defn- target-ids [layout-statement retention]
  (mapv :id
        (placement/place (layout/placement-layout layout-statement)
                         (policy retention)
                         (get retention "object"))))

(defn- attempts-map [attempts]
  (into {}
        (map (fn [entry]
               [(identity/bytes->hex (get entry "replica"))
                (get entry "attempt")]))
        attempts))

(defn valid-record?
  "Whether `record` is one closed, internally consistent intent record.

  This verifies structure, addresses, policy and deterministic targets, but not
  envelope signatures or deployment authorization; use `verify-record` at a
  trust boundary."
  [record]
  (try
    (let [layout-envelope (get record "layout")
          retention-envelope (get record "retention")
          layout-statement (statement layout-envelope layout/statement-type)
          retention (statement retention-envelope "blob/retention")
          manifest (blob/validate-manifest (get record "manifest"))
          attempts (get record "attempts")
          replica-hexes (mapv #(identity/bytes->hex (get % "replica")) attempts)
          expected-targets (target-ids layout-statement retention)]
      (and (exact-map? record-fields record)
           (= 1 (get record "v"))
           (= record-type (get record "type"))
           (cluster-id? (get record "cluster"))
           (bytes-n? 32 (get record "object"))
           (safe-uint? (get record "intent_revision"))
           (vector? attempts)
           (every? #(and (exact-map? attempt-fields %)
                         (bytes-n? 32 (get % "replica"))
                         (safe-uint? (get % "attempt")))
                   attempts)
           (= replica-hexes (vec (sort replica-hexes)))
           (= (count replica-hexes) (count (set replica-hexes)))
           (= (set expected-targets) (set replica-hexes))
           (same-bytes? (get record "cluster")
                        (get layout-statement "cluster"))
           (same-bytes? (get record "cluster") (get retention "cluster"))
           (same-bytes? (get record "object") (get manifest "object"))
           (same-bytes? (get record "object") (get retention "object"))
           (same-bytes? (blob/manifest-address manifest)
                        (get retention "manifest"))
           (= (get layout-statement "epoch")
              (get retention "layout_epoch"))))
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn validate-record [record]
  (when-not (valid-record? record)
    (throw (ex-info "Invalid durable replication intent record"
                    {:type :netz.replication/invalid-intent-record})))
  record)

(defn make-record
  "Build a canonical durable intent record from signed envelopes and attempts.

  `attempts` maps lowercase full-Authority hex IDs to safe unsigned counters."
  [layout-envelope retention-envelope manifest attempts intent-revision]
  (let [retention (statement retention-envelope "blob/retention")
        attempt-records
        (->> attempts
             (map (fn [[replica attempt]]
                    {"replica" (identity/hex->bytes replica)
                     "attempt" attempt}))
             (sort-by #(identity/bytes->hex (get % "replica")))
             vec)]
    (validate-record
     {"v" 1
      "type" record-type
      "cluster" (get retention "cluster")
      "object" (get retention "object")
      "manifest" manifest
      "layout" layout-envelope
      "retention" retention-envelope
      "intent_revision" intent-revision
      "attempts" attempt-records})))

(defn make-initial-record
  "Build revision zero with attempt zero for every deterministic target."
  [layout-envelope retention-envelope manifest]
  (let [layout-statement (statement layout-envelope layout/statement-type)
        retention (statement retention-envelope "blob/retention")
        targets (target-ids layout-statement retention)]
    (make-record layout-envelope retention-envelope manifest
                 (zipmap targets (repeat 0)) 0)))

(defn valid-successor-record?
  "Whether `successor` is a monotone replacement for one durable intent.

  The immutable object and controller stream remain fixed. A higher signed
  retention revision may extend the deadline and change policy on the same
  layout, or move to a later already-reconciled layout. Every retained target
  advances its attempt; a newly selected target starts at the new local intent
  revision so an earlier remove/re-add cycle cannot collide."
  [current successor]
  (try
    (let [current (validate-record current)
          successor (validate-record successor)
          old-layout (statement (get current "layout") layout/statement-type)
          new-layout (statement (get successor "layout")
                                layout/statement-type)
          old-retention (statement (get current "retention") "blob/retention")
          new-retention (statement (get successor "retention")
                                   "blob/retention")
          old-revision (get current "intent_revision")
          new-revision (get successor "intent_revision")
          old-attempts (attempts-map (get current "attempts"))
          new-attempts (attempts-map (get successor "attempts"))
          expected-attempts
          (into {}
                (map (fn [target]
                       [target (if-let [attempt (get old-attempts target)]
                                 (inc attempt)
                                 new-revision)]))
                (keys new-attempts))]
      (and (same-bytes? (get current "cluster") (get successor "cluster"))
           (same-bytes? (get current "object") (get successor "object"))
           (same-value? (get current "manifest") (get successor "manifest"))
           (same-bytes? (get old-retention "privacy_domain")
                        (get new-retention "privacy_domain"))
           (same-bytes? (get old-retention "manifest")
                        (get new-retention "manifest"))
           (same-bytes? (get old-retention "signer")
                        (get new-retention "signer"))
           (< (get old-retention "revision")
              (get new-retention "revision"))
           (<= (get old-retention "issued_at")
               (get new-retention "issued_at"))
           (<= (get old-retention "retain_until")
               (get new-retention "retain_until"))
           (= new-revision (inc old-revision))
           (= expected-attempts new-attempts)
           (<= (get old-layout "epoch") (get new-layout "epoch"))
           (or (< (get old-layout "epoch") (get new-layout "epoch"))
               (same-bytes? (layout/address old-layout)
                            (layout/address new-layout)))))
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn make-successor-record
  "Replace one intent with a higher full retention revision.

  `layout-envelope` must name the same or a later signed layout. The manifest
  bytes are reused unchanged; this operation migrates authority and placement,
  not content."
  [current layout-envelope retention-envelope]
  (let [current (validate-record current)
        next-revision (inc (get current "intent_revision"))
        new-layout (statement layout-envelope layout/statement-type)
        new-retention (statement retention-envelope "blob/retention")
        new-targets (target-ids new-layout new-retention)
        old-attempts (attempts-map (get current "attempts"))
        attempts (into {}
                       (map (fn [target]
                              [target (if-let [attempt (get old-attempts target)]
                                        (inc attempt)
                                        next-revision)]))
                       new-targets)
        successor (make-record layout-envelope retention-envelope
                               (get current "manifest") attempts next-revision)]
    (when-not (valid-successor-record? current successor)
      (throw (ex-info "Invalid durable intent successor"
                      {:type :netz.replication/invalid-intent-successor})))
    successor))

(defn record->intent
  "Convert one structurally valid record to the pure controller restore shape."
  [record]
  (validate-record record)
  (let [layout-envelope (get record "layout")
        retention-envelope (get record "retention")
        layout-statement (statement layout-envelope layout/statement-type)
        retention (statement retention-envelope "blob/retention")]
    {:manifest {:object-id (identity/bytes->hex (get record "object"))
                :manifest-id
                (identity/bytes->hex (get retention "manifest"))
                :wire-manifest (get record "manifest")
                :layout-envelope layout-envelope
                :retention-envelope retention-envelope}
     :layout (layout/placement-layout layout-statement)
     :policy (policy retention)
     :attempts (attempts-map (get record "attempts"))
     :intent-revision (get record "intent_revision")}))

(defn- authority-hex [value]
  (cond
    (bytes-n? 32 value) (identity/bytes->hex value)
    (and (string? value) (re-matches #"[0-9a-f]{64}" value)) value
    :else nil))

(defn verify-record
  "Verify signatures, controller authorization and return a pure intent.

  The result channel yields the restore map or the failure value. Acceptance
  against the current non-forked layout is a runtime concern because layouts
  can advance after this record was written."
  [record controllers]
  (go
    (try
      (validate-record record)
      (let [layout-envelope (get record "layout")
            retention-envelope (get record "retention")
            layout-statement (statement layout-envelope layout/statement-type)
            retention (statement retention-envelope "blob/retention")
            controllers (set (keep authority-hex controllers))
            layout-signer (identity/bytes->hex (get layout-statement "signer"))
            retention-signer (identity/bytes->hex (get retention "signer"))
            layout-ok? (<! (replication-wire/verify-envelope layout-envelope))
            retention-ok? (<! (replication-wire/verify-envelope
                               retention-envelope))]
        (when-not (and (seq controllers)
                       (contains? controllers layout-signer)
                       (contains? controllers retention-signer)
                       (true? layout-ok?)
                       (true? retention-ok?))
          (throw (ex-info "Unauthorized durable replication intent"
                          {:type :netz.replication/unauthorized-intent})))
        (record->intent record))
      (catch #?(:clj Throwable :cljs :default) cause cause))))

(defn same-record?
  "Canonical-value equality for storage idempotence checks."
  [a b]
  (and (valid-record? a) (valid-record? b) (same-value? a b)))
