(ns netz.replication.layout
  "Authenticated, hash-chained replica layouts for durable blob profile 1.

  This namespace reconciles statement bodies only after their Netz envelope
  has been verified. Discovery never changes the accepted member set."
  (:require [clojure.string :as str]
            [netz.identity :as identity]
            [netz.replication.placement :as placement]
            [netz.wire :as wire]))

(def statement-type "blob/layout")
(def address-domain "netz/blob-layout/v1\u0000")
(def max-domains-per-member 16)
(def default-max-conflicts 32)
(def ^:private zero-address-hex (apply str (repeat 64 "0")))

(def ^:private statement-fields
  #{"v" "type" "cluster" "epoch" "previous" "members" "issued_at"
    "signer"})
(def ^:private member-fields #{"authority" "domains" "enabled"})

(defn- safe-uint? [value]
  (and (integer? value) (<= 0 value wire/max-safe-integer)))

(defn- bytes-n? [n value]
  (and (wire/byte-buffer? value) (= n (identity/buf-length value))))

(defn- cluster-id? [value]
  (and (wire/byte-buffer? value)
       (<= 1 (identity/buf-length value) 64)))

(defn- visible-ascii? [limit value]
  (and (string? value)
       (<= (count value) limit)
       (boolean (re-matches #"[\x21-\x7e]+" value))))

(defn- exact-map? [fields value]
  (and (map? value) (= fields (set (keys value)))))

(defn- valid-domains? [domains]
  (and (map? domains)
       (<= (count domains) max-domains-per-member)
       (every? #(visible-ascii? 128 %) (keys domains))
       (every? #(visible-ascii? 128 %) (vals domains))))

(defn- valid-member? [member]
  (and (exact-map? member-fields member)
       (bytes-n? 32 (get member "authority"))
       (valid-domains? (get member "domains"))
       (boolean? (get member "enabled"))))

(defn- authority-hex [value]
  (cond
    (and (string? value) (re-matches #"(?i)[0-9a-f]{64}" value))
    (str/lower-case value)

    (bytes-n? 32 value)
    (identity/bytes->hex value)

    :else
    (throw (ex-info "Layout authority must be a full Authority ID"
                    {:type :netz.replication/invalid-layout-authority}))))

(defn valid-statement?
  "Whether `statement` is one exact canonical signed layout body.

  Members must be sorted by full Authority ID so a membership set has only one
  signed representation. Epoch zero uses the all-zero previous address; later
  predecessor links are checked by `reconcile`."
  [statement]
  (try
    (let [members (get statement "members")
          authorities (mapv #(get % "authority") members)
          authority-hexes (mapv identity/bytes->hex authorities)
          epoch (get statement "epoch")
          previous (get statement "previous")]
      (and (exact-map? statement-fields statement)
           (= 1 (get statement "v"))
           (= statement-type (get statement "type"))
           (cluster-id? (get statement "cluster"))
           (safe-uint? epoch)
           (bytes-n? 32 previous)
           (vector? members)
           (<= 1 (count members) placement/max-layout-members)
           (every? valid-member? members)
           (= authority-hexes (vec (sort authority-hexes)))
           (= (count authority-hexes) (count (set authority-hexes)))
           (if (zero? epoch)
             (= zero-address-hex (identity/bytes->hex previous))
             true)
           (safe-uint? (get statement "issued_at"))
           (bytes-n? 32 (get statement "signer"))))
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn validate-statement [statement]
  (when-not (valid-statement? statement)
    (throw (ex-info "Invalid durable-blob layout statement"
                    {:type :netz.replication/invalid-layout-statement})))
  statement)

(defn address
  "Return the domain-separated address of a canonical layout statement."
  [statement]
  (identity/sha256
   (identity/concat-bufs
    (identity/utf8-bytes address-domain)
    (wire/encode-canonical (validate-statement statement)))))

(defn placement-layout
  "Convert a verified signed layout to the placement engine's local shape."
  [statement]
  (validate-statement statement)
  {:epoch (get statement "epoch")
   :members
   (mapv (fn [member]
           {:id (identity/bytes->hex (get member "authority"))
            :domains (get member "domains")
            :enabled? (get member "enabled")})
         (get statement "members"))})

(defn make-state
  "Create a pure layout-chain reconciler for one cluster and controller set."
  ([cluster controllers]
   (make-state cluster controllers {}))
  ([cluster controllers opts]
   (let [controllers (set (map authority-hex controllers))
         max-conflicts (get opts :max-conflicts default-max-conflicts)]
     (when-not (and (cluster-id? cluster)
                    (seq controllers)
                    (integer? max-conflicts)
                    (pos? max-conflicts))
       (throw (ex-info "Invalid layout reconciliation options"
                       {:type :netz.replication/invalid-layout-options})))
     {:cluster cluster
      :controllers controllers
      :current nil
      :conflicts []
      :max-conflicts max-conflicts})))

(defn- same-bytes? [left right]
  (and (wire/byte-buffer? left)
       (wire/byte-buffer? right)
       (identity/bufs= left right)))

(defn- conflict [state statement statement-address]
  (let [conflicting (identity/bytes->hex statement-address)
        proof {:epoch (get statement "epoch")
               :accepted (identity/bytes->hex (get-in state [:current :address]))
               :conflicting conflicting
               :statement statement}
        conflicts (if (some #(= conflicting (:conflicting %))
                            (:conflicts state))
                    (:conflicts state)
                    (->> (conj (:conflicts state) proof)
                         (take-last (:max-conflicts state))
                         vec))]
    {:state (assoc state :conflicts conflicts)
     :status :conflict}))

(defn reconcile
  "Reconcile one already signature-verified layout statement.

  Returns `{:state next-state :status keyword}`. Accepted chains start at epoch
  zero and then advance one epoch at a time through the exact previous layout
  address. Same-epoch forks are retained as bounded conflict evidence."
  [state statement]
  (try
    (let [statement (validate-statement statement)
          signer (authority-hex (get statement "signer"))
          epoch (get statement "epoch")
          statement-address (address statement)
          current (:current state)
          current-epoch (get-in current [:statement "epoch"])]
      (cond
        (not (contains? (:controllers state) signer))
        {:state state :status :rejected}

        (not (same-bytes? (:cluster state) (get statement "cluster")))
        {:state state :status :rejected}

        (nil? current)
        (if (zero? epoch)
          {:state (assoc state :current {:statement statement
                                         :address statement-address})
           :status :accepted}
          {:state state :status :gap})

        (< epoch current-epoch)
        {:state state :status :stale}

        (= epoch current-epoch)
        (if (same-bytes? statement-address (:address current))
          {:state state :status :duplicate}
          (conflict state statement statement-address))

        (seq (:conflicts state))
        {:state state :status :forked}

        (> epoch (inc current-epoch))
        {:state state :status :gap}

        (not (same-bytes? (get statement "previous") (:address current)))
        {:state state :status :rejected}

        :else
        {:state (assoc state :current {:statement statement
                                       :address statement-address})
         :status :accepted}))
    (catch #?(:clj Throwable :cljs :default) _
      {:state state :status :rejected})))

(defn retention-authorized?
  "Whether the current layout selects `local-authority` for `retention`.

  The signed retention policy and object address feed the deterministic
  placement function. Any absent, stale or malformed layout fails closed."
  [state local-authority retention]
  (try
    (let [statement (get-in state [:current :statement])
          local (authority-hex local-authority)
          policy {:replicas (get retention "replicas")
                  :write-acks (get retention "write_acks")
                  :minimum-domains (get retention "minimum_domains")}
          targets (placement/place (placement-layout statement)
                                   policy
                                   (get retention "object"))]
      (and (= "blob/retention" (get retention "type"))
           (empty? (:conflicts state))
           (same-bytes? (:cluster state) (get retention "cluster"))
           (= (get statement "epoch") (get retention "layout_epoch"))
           (boolean (some #(= local (:id %)) targets))))
    (catch #?(:clj Throwable :cljs :default) _ false)))
