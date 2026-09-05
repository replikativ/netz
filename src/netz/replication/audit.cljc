(ns netz.replication.audit
  "Bounded periodic revalidation of historical durable receipts.
   Pure scheduling only: no failure detection, membership or storage authority."
  (:require [netz.wire :as wire]))

(defn make-state
  ([] (make-state {}))
  ([{:keys [interval-ms max-per-scan max-entries]
     :or {interval-ms 300000 max-per-scan 4 max-entries 10000}}]
   (when-not (and (integer? interval-ms) (<= 1 interval-ms wire/max-safe-integer)
                  (integer? max-per-scan) (<= 1 max-per-scan 1024)
                  (integer? max-entries) (<= max-per-scan max-entries 1000000))
     (throw (ex-info "Invalid receipt audit options" {})))
   {:interval-ms interval-ms :max-per-scan max-per-scan :max-entries max-entries
    :entries {}}))

(defn observations [controller-state]
  (for [[object-id entry] (sort-by key (:objects controller-state))
        :when (:intent-stored? entry)
        replica-id (sort (:acks entry))]
    {:object-id object-id :replica-id replica-id
     :layout-epoch (:layout-epoch entry) :intent-revision (:intent-revision entry)
     :attempt (get-in entry [:attempts replica-id])}))

(defn scan
  "Returns {:state ... :revalidate [fenced observations ...]}.
   Bound overflow fails closed instead of silently starving later objects.
   A refused operation becomes eligible after the audit interval; lost copies
   are retried more frequently by the existing known-missing repair scheduler.
   Restart resets the audit grace period, not durable replica evidence."
  [state controller-state now]
  (when-not (and (integer? now) (<= 0 now wire/max-safe-integer))
    (throw (ex-info "Invalid audit time" {})))
  (let [observed (vec (take (inc (:max-entries state)) (observations controller-state)))
        _ (when (> (count observed) (:max-entries state))
            (throw (ex-info "Receipt audit capacity exceeded" {:type ::capacity})))
        deadline (min wire/max-safe-integer (+ now (:interval-ms state)))
        fingerprint #(dissoc % :intent-revision)
        pending (into #{} (keep (fn [[object-id entry]]
                                  (when-not (:intent-stored? entry) object-id)))
                      (:objects controller-state))
        entries (into (into {} (filter #(contains? pending (:object-id (key %))))
                            (:entries state))
                      (map (fn [o] [(fingerprint o)
                                    (get (:entries state) (fingerprint o) deadline)]) observed))
        _ (when (> (count entries) (:max-entries state))
            (throw (ex-info "Receipt audit capacity exceeded" {:type ::capacity})))
        candidates (->> observed
                        (filter #(<= (get entries (fingerprint %)) now))
                        (sort-by #(vector (get entries (fingerprint %))
                                          (:object-id %) (:replica-id %))))
        ;; One operation changes the object's intent revision. Leave its other
        ;; targets due for the next scan instead of issuing stale observations.
        due (:selected
             (reduce (fn [{:keys [objects selected] :as result} o]
                       (if (or (contains? objects (:object-id o))
                               (>= (count selected) (:max-per-scan state)))
                         result
                         {:objects (conj objects (:object-id o))
                          :selected (conj selected o)}))
                     {:objects #{} :selected []} candidates))]
    {:state (assoc state :entries
                   (reduce #(assoc %1 (fingerprint %2) deadline) entries due))
     :revalidate due}))
