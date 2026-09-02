(ns netz.replication.lifecycle
  "Pure crash-safe retention, expiry and deletion state.

  Verified controller statements are journaled before they take effect.
  Expiry/deletion tombstones are journaled before delete IO, and a signed
  deletion receipt is journaled before emission.  This ordering makes restart
  replay idempotent and prevents an offline replica from resurrecting an
  object after privacy deletion."
  (:require [clojure.string :as str]
            [netz.identity :as identity]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]))

(def default-opts
  {:retry-ms 30000
   :persistence "netz/store/fsync/1"
   :accepted-persistence #{"netz/store/fsync/1"}
   :max-records 100000
   :max-equivocations 64})

(defn- authority-hex [value]
  (cond
    (and (string? value) (re-matches #"(?i)[0-9a-f]{64}" value))
    (str/lower-case value)

    (and (wire/byte-buffer? value) (= 32 (identity/buf-length value)))
    (identity/bytes->hex value)

    :else
    (throw (ex-info "Lifecycle authority must be a full Authority ID"
                    {:type :netz.replication/invalid-controller}))))

(defn make-state
  ([local-authority controllers]
   (make-state local-authority controllers {}))
  ([local-authority controllers opts]
   (let [controllers (set (map authority-hex controllers))
         opts (merge default-opts opts)]
     (when-not (and (seq controllers)
                    (int? (:retry-ms opts)) (pos? (:retry-ms opts))
                    (int? (:max-records opts)) (pos? (:max-records opts))
                    (int? (:max-equivocations opts))
                    (pos? (:max-equivocations opts))
                    (contains? replication-wire/persistence-profiles
                               (:persistence opts))
                    (set? (:accepted-persistence opts))
                    (seq (:accepted-persistence opts))
                    (every? replication-wire/persistence-profiles
                            (:accepted-persistence opts)))
       (throw (ex-info "Invalid lifecycle options"
                       {:type :netz.replication/invalid-lifecycle-options})))
     {:local-authority (identity/hex->bytes (authority-hex local-authority))
      :controllers controllers
      :records {}
      :pending {}
      :equivocations []
      :opts opts
      :stats {:validated 0 :duplicates 0 :stale 0 :rejected 0
              :equivocations 0 :expired 0 :delete-attempts 0
              :deleted 0 :receipts 0 :persistence-failures 0}})))

(defn- object-key [statement]
  (str (identity/bytes->hex (get statement "cluster")) "/"
       (identity/bytes->hex (get statement "privacy_domain")) "/"
       (identity/bytes->hex (get statement "object"))))

(defn- revision [statement]
  (get statement "revision"))

(defn- fingerprint [statement]
  (identity/bytes->hex
   (identity/sha256 (replication-wire/signing-bytes statement))))

(def ^:private lifecycle-statuses
  #{:retained :deletion-pending :deleting :expiring :deletion-failed
    :deleted})

(defn- safe-time? [value]
  (and (integer? value) (<= 0 value wire/max-safe-integer)))

(defn- same-value? [a b]
  (identity/bufs= (wire/encode-canonical a) (wire/encode-canonical b)))

(defn- valid-restored-receipt? [state record]
  (let [statement (:statement record)
        body (:receipt-body record)
        envelope (:receipt-envelope record)]
    (and (if envelope body true)
         (if-not body
           true
           (and (= "blob/deletion" (get statement "type"))
                (= "blob/deleted" (get body "type"))
                (replication-wire/valid-statement? body)
                (= (get statement "revision")
                   (get body "deletion_revision"))
                (= (:deleted-at record) (get body "deleted_at"))
                (= (:delete-persistence record) (get body "persistence"))
                (identity/bufs= (:local-authority state)
                                (get body "replica"))
                (every? true?
                        (map #(identity/bufs= (get statement %) (get body %))
                             ["cluster" "privacy_domain" "manifest"
                              "object"]))
                (= (get statement "layout_epoch")
                   (get body "layout_epoch"))))
         (if-not envelope
           true
           (try
             (same-value?
              body (:statement (replication-wire/parse-envelope envelope)))
             (catch #?(:clj Throwable :cljs :default) _ false))))))

(defn- valid-restored-status? [state record]
  (let [status (:status record)
        statement (:statement record)
        type (get statement "type")]
    (case status
      :retained (contains? #{"blob/retention" "blob/lease"} type)
      :deletion-pending (= "blob/deletion" type)
      :deleting (= "blob/deletion" type)
      :expiring (and (contains? #{"blob/retention" "blob/lease"} type)
                     (safe-time? (:expired-at record)))
      :deletion-failed (safe-time? (:retry-at record))
      :deleted (and (safe-time? (:deleted-at record))
                    (contains? (get-in state [:opts :accepted-persistence])
                               (:delete-persistence record))
                    (valid-restored-receipt? state record))
      false)))

(defn- valid-restored-record? [state key record]
  (try
    (let [statement (:statement record)
          signer (authority-hex (get statement "signer"))]
      (replication-wire/validate-statement statement)
      (and (map? record)
           (= key (object-key statement))
           (= signer (:controller record))
           (contains? (:controllers state) signer)
           (= (revision statement) (:revision record))
           (= (fingerprint statement) (:fingerprint record))
           (contains? lifecycle-statuses (:status record))
           (valid-restored-status? state record)))
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn restore-state
  "Restore previously journaled lifecycle records into a fresh state.

  Corrupt, foreign-controller and over-limit journals fail closed. The record
  fingerprint detects accidental mutation of a previously accepted statement;
  the journal remains inside the trusted local persistence boundary."
  [state records]
  (let [records (or records {})]
    (when-not (and (map? records)
                   (<= (count records) (get-in state [:opts :max-records]))
                   (every? (fn [[key record]]
                             (valid-restored-record? state key record))
                           records))
      (throw (ex-info "Invalid durable lifecycle journal"
                      {:type :netz.replication/invalid-lifecycle-journal})))
    (assoc state :records records)))

(defn- same-bytes? [a b]
  (and (wire/byte-buffer? a) (wire/byte-buffer? b)
       (identity/bufs= a b)))

(defn- same-object-stream? [current incoming]
  (and (same-bytes? (get-in current [:statement "manifest"])
                    (get incoming "manifest"))
       (= (:controller current)
          (identity/bytes->hex (get incoming "signer")))))

(defn- valid-layout-transition? [current incoming]
  (let [current-epoch (get-in current [:statement "layout_epoch"])
        incoming-epoch (get incoming "layout_epoch")]
    (case (get incoming "type")
      "blob/lease" (= current-epoch incoming-epoch)
      "blob/retention" (<= current-epoch incoming-epoch)
      "blob/deletion" (<= current-epoch incoming-epoch)
      false)))

(defn- persist-action [key record]
  [:persist [:blob/lifecycle key] record
   {:type :lifecycle/persisted :key key}])

(defn- queue-persist [state key record after]
  (if (contains? (:pending state) key)
    {:state (update-in state [:stats :rejected] inc) :actions []}
    {:state (assoc-in state [:pending key] {:record record :after after})
     :actions [(persist-action key record)]}))

(defn- timer-action [state at]
  [:timer (max 0 (- at (:now state))) {:type :lifecycle/tick}])

(defn- record-equivocation [state key current incoming]
  (let [proof {:key key
               :revision (revision incoming)
               :accepted (:fingerprint current)
               :conflicting (fingerprint incoming)}
        limit (get-in state [:opts :max-equivocations])
        proofs (->> (conj (:equivocations state) proof)
                    (take-last limit) vec)]
    {:state (-> state
                (assoc :equivocations proofs)
                (update-in [:stats :equivocations] inc))
     :actions [[:persist [:blob/equivocations key] proofs]]}))

(defn- accepted-record [statement now]
  (let [type (get statement "type")
        delete-at (get statement "delete_after")
        retain-until (get statement "retain_until")]
    {:statement statement
     :controller (identity/bytes->hex (get statement "signer"))
     :revision (revision statement)
     :fingerprint (fingerprint statement)
     :status (cond
               (= type "blob/deletion")
               (if (<= delete-at now) :deleting :deletion-pending)

               (<= retain-until now) :expiring
               :else :retained)}))

(defn- after-for-record [record]
  (case (:status record)
    :retained [:schedule (get-in record [:statement "retain_until"])]
    :deletion-pending [:schedule (get-in record [:statement "delete_after"])]
    :deleting [:delete]
    :expiring [:delete]
    nil))

(defn- accept-initial [state statement now]
  (let [key (object-key statement)
        record (accepted-record statement now)]
    (-> (queue-persist state key record (after-for-record record))
        (update :state update-in [:stats :validated] inc))))

(defn- accept-update [state key current statement now]
  (let [incoming-revision (revision statement)
        current-revision (:revision current)
        type (get statement "type")]
    (cond
      (< incoming-revision current-revision)
      {:state (update-in state [:stats :stale] inc) :actions []}

      (= incoming-revision current-revision)
      (if (= (:fingerprint current) (fingerprint statement))
        {:state (update-in state [:stats :duplicates] inc) :actions []}
        (record-equivocation state key current statement))

      (and (not= type "blob/deletion")
           (#{:deleting :expiring :deleted} (:status current)))
      {:state (update-in state [:stats :rejected] inc) :actions []}

      (not (same-object-stream? current statement))
      {:state (update-in state [:stats :rejected] inc) :actions []}

      (not (valid-layout-transition? current statement))
      {:state (update-in state [:stats :rejected] inc) :actions []}

      (< (get statement "issued_at")
         (get-in current [:statement "issued_at"]))
      {:state (update-in state [:stats :rejected] inc) :actions []}

      (and (not= type "blob/deletion")
           (< (get statement "retain_until")
              (get-in current [:statement "retain_until"] 0)))
      {:state (update-in state [:stats :rejected] inc) :actions []}

      (and (not= type "blob/deletion")
           (<= (get statement "retain_until") now))
      {:state (update-in state [:stats :rejected] inc) :actions []}

      :else
      (let [record (accepted-record statement now)]
        (-> (queue-persist state key record (after-for-record record))
            (update :state update-in [:stats :validated] inc))))))

(defn- verified-statement [state {:keys [statement]} now]
  (try
    (replication-wire/validate-statement statement)
    (let [type (get statement "type")
          signer (identity/bytes->hex (get statement "signer"))
          key (object-key statement)
          current (get-in state [:records key])]
      (cond
        (not (contains? #{"blob/retention" "blob/lease" "blob/deletion"}
                        type))
        {:state (update-in state [:stats :rejected] inc) :actions []}

        (not (contains? (:controllers state) signer))
        {:state (update-in state [:stats :rejected] inc) :actions []}

        (and (nil? current) (= type "blob/lease"))
        {:state (update-in state [:stats :rejected] inc) :actions []}

        (and (nil? current)
             (>= (+ (count (:records state)) (count (:pending state)))
                 (get-in state [:opts :max-records])))
        {:state (update-in state [:stats :rejected] inc) :actions []}

        (nil? current) (accept-initial state statement now)
        :else (accept-update state key current statement now)))
    (catch #?(:clj Throwable :cljs :default) _
      {:state (update-in state [:stats :rejected] inc) :actions []})))

(defn- execute-after [state key record after]
  (let [[op value] after]
    (case op
      :schedule {:state state :actions [(timer-action state value)]}
      :delete {:state (update-in state [:stats :delete-attempts] inc)
               :actions [[:delete-object key (:statement record)
                          (get-in state [:opts :persistence])]]}
      :sign {:state state :actions [[:sign-deletion-receipt key value]]}
      :emit {:state (update-in state [:stats :receipts] inc)
             :actions [[:emit-deletion-receipt key value]]}
      {:state state :actions []})))

(defn- persisted [state {:keys [key result]}]
  (if-let [{:keys [record after]} (get-in state [:pending key])]
    (if (= :ok result)
      (execute-after (-> state
                         (assoc-in [:records key] record)
                         (update :pending dissoc key))
                     key record after)
      {:state (-> state
                  (update :pending dissoc key)
                  (update-in [:stats :persistence-failures] inc))
       :actions []})
    {:state state :actions []}))

(defn- due-transition [state key record now]
  (cond
    (contains? (:pending state) key) {:state state :actions []}

    (and (= :retained (:status record))
         (<= (get-in record [:statement "retain_until"]) now))
    (let [record (assoc record :status :expiring :expired-at now)]
      (-> (queue-persist state key record [:delete])
          (update :state update-in [:stats :expired] inc)))

    (and (= :deletion-pending (:status record))
         (<= (get-in record [:statement "delete_after"]) now))
    (queue-persist state key (assoc record :status :deleting) [:delete])

    (= :deletion-failed (:status record))
    (if (<= (:retry-at record) now)
      (queue-persist state key (-> record
                                   (assoc :status
                                          (if (:expired-at record)
                                            :expiring :deleting))
                                   (dissoc :retry-at))
                     [:delete])
      {:state state :actions []})

    :else {:state state :actions []}))

(defn- tick [state now]
  (reduce (fn [{current :state actions :actions} [key record]]
            (let [{next-state :state more :actions}
                  (due-transition current key record now)]
              {:state next-state :actions (into actions more)}))
          {:state state :actions []}
          (:records state)))

(defn- deletion-receipt [state record persistence deleted-at]
  (let [statement (:statement record)]
    {"v" 1 "type" "blob/deleted"
     "cluster" (get statement "cluster")
     "privacy_domain" (get statement "privacy_domain")
     "deletion_revision" (get statement "revision")
     "manifest" (get statement "manifest")
     "object" (get statement "object")
     "layout_epoch" (get statement "layout_epoch")
     "replica" (:local-authority state)
     "persistence" persistence
     "deleted_at" deleted-at
     "signer" (:local-authority state)}))

(defn- delete-result [state {:keys [key result persistence deleted-at]}]
  (if-let [record (get-in state [:records key])]
    (if (= :ok result)
      (if-not (and (contains? (get-in state [:opts :accepted-persistence])
                              persistence)
                   (integer? deleted-at)
                   (<= 0 deleted-at wire/max-safe-integer))
        (let [retry-at (+ (:now state) (get-in state [:opts :retry-ms]))]
          (queue-persist state key
                         (assoc record :status :deletion-failed
                                :retry-at retry-at)
                         [:schedule retry-at]))
        (let [controller-deletion? (= "blob/deletion"
                                      (get-in record [:statement "type"]))
              receipt (when controller-deletion?
                        (deletion-receipt state record persistence deleted-at))
              deleted (cond-> (assoc record :status :deleted
                                     :deleted-at deleted-at
                                     :delete-persistence persistence)
                        receipt (assoc :receipt-body receipt))]
          (-> (queue-persist state key deleted
                             (when receipt [:sign receipt]))
              (update :state update-in [:stats :deleted] inc))))
      (let [retry-at (+ (:now state) (get-in state [:opts :retry-ms]))]
        (queue-persist state key
                       (assoc record :status :deletion-failed
                              :retry-at retry-at)
                       [:schedule retry-at])))
    {:state state :actions []}))

(defn- receipt-signed [state {:keys [key envelope]}]
  (try
    (let [record (get-in state [:records key])
          expected (:receipt-body record)
          actual (:statement (replication-wire/parse-envelope envelope))]
      (if-not (and (= :deleted (:status record))
                   expected
                   (identity/bufs= (wire/encode-canonical expected)
                                   (wire/encode-canonical actual)))
        {:state (update-in state [:stats :rejected] inc) :actions []}
        (queue-persist state key (assoc record :receipt-envelope envelope)
                       [:emit envelope])))
    (catch #?(:clj Throwable :cljs :default) _
      {:state (update-in state [:stats :rejected] inc) :actions []})))

(defn- initialize [state now]
  (let [state (assoc state :now now)
        {state :state actions :actions} (tick state now)]
    (reduce (fn [{current :state effects :actions} [key record]]
              (cond
                (contains? (:pending current) key)
                {:state current :actions effects}

                (:receipt-envelope record)
                {:state current
                 :actions (conj effects
                                [:emit-deletion-receipt key
                                 (:receipt-envelope record)])}

                (and (= :deleted (:status record)) (:receipt-body record))
                {:state current
                 :actions (conj effects
                                [:sign-deletion-receipt key
                                 (:receipt-body record)])}

                (= :retained (:status record))
                {:state current
                 :actions (conj effects
                                (timer-action
                                 current
                                 (get-in record [:statement "retain_until"])))}

                (= :deletion-pending (:status record))
                {:state current
                 :actions (conj effects
                                (timer-action
                                 current
                                 (get-in record [:statement "delete_after"])))}

                (= :deletion-failed (:status record))
                {:state current
                 :actions (conj effects
                                (timer-action current (:retry-at record)))}

                (#{:deleting :expiring} (:status record))
                {:state (update-in current [:stats :delete-attempts] inc)
                 :actions (conj effects
                                [:delete-object key (:statement record)
                                 (get-in current [:opts :persistence])])}

                :else {:state current :actions effects}))
            {:state state :actions actions}
            (:records state))))

(defn handler [state event ctx]
  (let [now (or (:now ctx) 0)
        state (assoc state :now now)]
    (case (:type event)
      :init (initialize state now)
      :timer (if (= :lifecycle/tick (get-in event [:payload :type]))
               (tick state now)
               {:state state :actions []})
      :persist-result
      (if (= :lifecycle/persisted (get-in event [:payload :type]))
        (persisted state {:key (get-in event [:payload :key])
                          :result (:result event)})
        {:state state :actions []})
      :lifecycle/verified-statement (verified-statement state event now)
      :lifecycle/persisted (persisted state event)
      :lifecycle/tick (tick state now)
      :lifecycle/delete-result (delete-result state event)
      :lifecycle/receipt-signed (receipt-signed state event)
      {:state state :actions []})))

(defn status [state statement]
  (get-in state [:records (object-key statement) :status] :unknown))

(defn accepted-retention?
  "Whether this exact retention statement is durably active in `state`.

  This deliberately compares the canonical statement fingerprint, not just
  the object key or revision. Runtime authorization uses it after executing
  the lifecycle journal action, so an equivocation or failed persistence can
  never turn into permission to receive bytes."
  [state statement]
  (try
    (let [record (get-in state [:records (object-key statement)])]
      (and (= "blob/retention" (get statement "type"))
           (= :retained (:status record))
           (= (fingerprint statement) (:fingerprint record))))
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn accepted-statement?
  "Whether this exact controller statement crossed the durable journal.

  Unlike `accepted-retention?`, this also recognizes leases and deletion
  markers in their later lifecycle phases. It is intended for acknowledging a
  control-plane statement, not for authorizing an object transfer."
  [state statement]
  (try
    (let [record (get-in state [:records (object-key statement)])]
      (and (contains? #{"blob/retention" "blob/lease" "blob/deletion"}
                      (get statement "type"))
           (= (fingerprint statement) (:fingerprint record))))
    (catch #?(:clj Throwable :cljs :default) _ false)))
