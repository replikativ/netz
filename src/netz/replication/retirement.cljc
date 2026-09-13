(ns netz.replication.retirement
  "Independent verification of one controller-authorized replica retirement.

  A command embeds the current retention envelope and all N replacement
  receipts. Verification is portable and side-effect free apart from
  asynchronous signature checks. It authorizes consideration of local
  deletion, never deletion by itself: the lifecycle runtime must additionally
  recheck its local retention/pin state atomically with storage mutation."
  (:require [clojure.string :as str]
            [netz.identity :as identity]
            [netz.replication.layout :as layout]
            [netz.replication.placement :as placement]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]
            #?(:clj [clojure.core.async :refer [go <!]]
               :cljs [clojure.core.async :refer [<!]
                      :refer-macros [go]])))

(defn- same-bytes? [a b]
  (and (wire/byte-buffer? a) (wire/byte-buffer? b)
       (identity/bufs= a b)))

(defn- authority-hex [value]
  (cond
    (and (string? value) (re-matches #"(?i)[0-9a-f]{64}" value))
    (str/lower-case value)
    (and (wire/byte-buffer? value) (= 32 (identity/buf-length value)))
    (identity/bytes->hex value)
    :else nil))

(defn- matching-receipt? [command retention targets accepted-persistence now receipt]
  (let [replica (authority-hex (get receipt "replica"))]
    (and (= "blob/stored" (get receipt "type"))
         (contains? targets replica)
         (contains? accepted-persistence (get receipt "persistence"))
         (same-bytes? (get command "cluster") (get receipt "cluster"))
         (same-bytes? (get command "privacy_domain")
                      (get receipt "privacy_domain"))
         (same-bytes? (get command "manifest") (get receipt "manifest"))
         (same-bytes? (get command "object") (get receipt "object"))
         (= (get command "layout_epoch") (get receipt "layout_epoch"))
         (= (get retention "revision") (get receipt "retention_revision"))
         (= (get retention "retain_until") (get receipt "retain_until"))
         (<= (get receipt "stored_at") now)
         (< now (get receipt "retain_until")))))

(defn verify-command
  "Yield verified retirement evidence or the failure value.

  Options require a reconciled `:layout-state`, the local full Authority,
  authorized controller Authorities, accepted stable persistence profiles and
  current integer time. The exact current layout must directly follow the
  command's previous layout address. Every selected target must supply one
  fresh, independently valid signed receipt."
  [envelope {:keys [layout-state local-authority controllers
                    accepted-persistence now]}]
  (go
    (try
      (when-not (and (true? (<! (replication-wire/verify-envelope envelope)))
                     (map? layout-state)
                     (empty? (:conflicts layout-state))
                     (set? accepted-persistence)
                     (seq accepted-persistence)
                     (every? replication-wire/persistence-profiles
                             accepted-persistence)
                     (not (contains? accepted-persistence
                                     "netz/store/volatile/1"))
                     (integer? now) (<= 0 now wire/max-safe-integer))
        (throw (ex-info "Invalid retirement verification context"
                        {:type ::invalid-context})))
      (let [command (:statement (replication-wire/parse-envelope envelope))
            current (get-in layout-state [:current :statement])
            retention-envelope (get command "retention")
            retention-ok? (<! (replication-wire/verify-envelope retention-envelope))
            retention (:statement
                       (replication-wire/parse-envelope retention-envelope))
            receipt-envelopes (get command "receipts")
            receipts
            (loop [remaining (seq receipt-envelopes) result []]
              (if-let [receipt-envelope (first remaining)]
                (if (true? (<! (replication-wire/verify-envelope receipt-envelope)))
                  (recur (next remaining)
                         (conj result
                               (:statement
                                (replication-wire/parse-envelope receipt-envelope))))
                  (throw (ex-info "Invalid replacement receipt signature"
                                  {:type ::invalid-receipt})))
                result))
            policy {:replicas (get retention "replicas")
                    :write-acks (get retention "write_acks")
                    :minimum-domains (get retention "minimum_domains")}
            selected (placement/place (layout/placement-layout current)
                                      policy (get command "object"))
            targets (set (map :id selected))
            receipt-targets (set (map #(authority-hex (get % "replica")) receipts))
            controller-set (set (keep authority-hex controllers))
            signer (authority-hex (get command "signer"))
            local (authority-hex local-authority)]
        (when-not
         (and (= "blob/retirement" (get command "type"))
              retention-ok?
              (= controller-set (:controllers layout-state))
              (contains? controller-set signer)
              (contains? controller-set
                         (authority-hex (get current "signer")))
              (= signer (authority-hex (get retention "signer")))
              (= local (authority-hex (get command "replica")))
              (not (contains? targets local))
              (same-bytes? (:cluster layout-state) (get command "cluster"))
              (same-bytes? (get-in layout-state [:current :address])
                           (layout/address current))
              (same-bytes? (layout/address current) (get command "layout"))
              (same-bytes? (get current "previous")
                           (get command "previous_layout"))
              (pos? (get current "epoch"))
              (= (get current "epoch") (get command "layout_epoch"))
              (= (get command "layout_epoch") (get retention "layout_epoch"))
              (every? #(same-bytes? (get command %) (get retention %))
                      ["cluster" "privacy_domain" "manifest" "object"])
              (< (get retention "revision") (get command "revision"))
              (<= (get retention "issued_at") (get command "issued_at"))
              (<= (get command "issued_at") now)
              (<= (get command "retire_after") now)
              (< now (get retention "retain_until"))
              (= (count receipts) (get retention "replicas"))
              (= targets receipt-targets)
              (every? #(matching-receipt? command retention targets
                                          accepted-persistence now %)
                      receipts)
              (placement/satisfies-domains? selected
                                            (get retention "minimum_domains")))
          (throw (ex-info "Retirement evidence is not currently sufficient"
                          {:type ::ineligible})))
        {:statement command :retention retention
         :targets targets
         :receipts (into {}
                         (map (fn [receipt]
                                [(authority-hex (get receipt "replica"))
                                 receipt]))
                         receipts)})
      (catch #?(:clj Throwable :cljs :default) cause cause))))
