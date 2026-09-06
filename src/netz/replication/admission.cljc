(ns netz.replication.admission
  "Pure, explicit writer-admission budget for unconfirmed immutable snapshots.
   Applications serialize/persist reservations BEFORE invoking their writer.
   This is not a post-commit hook and cannot roll back committed work."
  (:require [netz.wire :as wire]))

(defn- uint? [n] (and (integer? n) (<= 0 n wire/max-safe-integer)))

(defn make-state [{:keys [max-pending max-bytes max-age-ms] :as limits}]
  (when-not (every? #(and (uint? %) (pos? %)) [max-pending max-bytes max-age-ms])
    (throw (ex-info "Invalid replication admission limits" {})))
  {:limits limits :pending {}})

(defn reserve
  "Reserve an application operation ID and conservative byte budget.
   Repeated identical reservations are idempotent. Age bounds stop NEW writes;
   they cannot make an existing unconfirmed write disappear during a partition."
  [state operation-id bytes now]
  (when-not (and (string? operation-id) (<= 1 (count operation-id) 256)
                 (uint? bytes) (uint? now))
    (throw (ex-info "Invalid replication admission request" {})))
  (let [pending (:pending state)
        prior (get pending operation-id)
        {:keys [max-pending max-bytes max-age-ms]} (:limits state)
        used (reduce + 0 (map :bytes (vals pending)))
        reason (cond
                 prior (if (= bytes (:bytes prior)) :exists :conflict)
                 (some #(or (< now (:at %)) (>= (- now (:at %)) max-age-ms)) (vals pending)) :lag
                 (>= (count pending) max-pending) :count
                 (> bytes (- max-bytes used)) :bytes)]
    {:state (if reason state
                (assoc-in state [:pending operation-id] {:bytes bytes :at now}))
     :admitted? (or (nil? reason) (= :exists reason)) :reason reason}))

(defn bind-object
  "Bind a reservation to its immutable snapshot archive after local commit."
  [state operation-id object-id]
  (let [entry (get-in state [:pending operation-id])]
    (when-not (and entry (string? object-id) (re-matches #"[0-9a-f]{64}" object-id)
                   (or (nil? (:object-id entry)) (= object-id (:object-id entry))))
      (throw (ex-info "Invalid admission object binding" {})))
    (assoc-in state [:pending operation-id :object-id] object-id)))

(defn confirm
  "Release credit using CURRENT controller durability-status for this exact
   object. The application supplies trusted local controller output, not a
   network message. Expired evidence cannot release credit."
  [state operation-id status now]
  (when-not (uint? now) (throw (ex-info "Invalid confirmation time" {})))
  (let [entry (get-in state [:pending operation-id])]
    (if (and entry (:object-id entry) (= (:object-id entry) (:object-id status))
             (= :confirmed (:status status)) (uint? (:retain-until status))
             (< now (:retain-until status)))
      (update state :pending dissoc operation-id)
      state)))

(defn cancel-before-write
  "Release a reservation only when the application KNOWS no write ran.
   A bound or ambiguously committed operation must be recovered, never cancelled."
  [state operation-id]
  (when (get-in state [:pending operation-id :object-id])
    (throw (ex-info "Cannot cancel a committed replication reservation" {})))
  (update state :pending dissoc operation-id))
