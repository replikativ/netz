(ns netz.replication.repair-scheduler
  "Pure bounded policy for retrying known-missing durable replicas.

  This machine never discovers authority, changes placement, or claims that a
  successful transfer proves future retention. It observes the durable
  controller's current objects, waits an initial grace period so attempt-zero
  transfers can finish, and emits bounded explicit repair operations. Backoff
  state is volatile; the attempt fence it advances is durable in the controller
  intent record."
  (:require [clojure.set :as set]
            [netz.wire :as wire]))

(def default-opts
  {:scan-ms 5000
   :initial-grace-ms 30000
   :backoff-ms [30000 60000 300000 1800000]
   :max-repairs-per-scan 32
   :max-in-flight 32})

(defn- positive-safe-int? [value]
  (and (integer? value) (pos? value) (<= value wire/max-safe-integer)))

(defn validate-opts [opts]
  (let [{:keys [scan-ms initial-grace-ms backoff-ms max-repairs-per-scan
                max-in-flight]
         :as opts}
        (merge default-opts opts)]
    (when-not (and (positive-safe-int? scan-ms)
                   (positive-safe-int? initial-grace-ms)
                   (vector? backoff-ms)
                   (seq backoff-ms)
                   (every? positive-safe-int? backoff-ms)
                   (integer? max-repairs-per-scan)
                   (pos? max-repairs-per-scan)
                   (<= max-repairs-per-scan 1024)
                   (integer? max-in-flight)
                   (pos? max-in-flight)
                   (<= max-in-flight 1024))
      (throw (ex-info "Invalid repair scheduler options"
                      {:type :netz.replication/invalid-repair-scheduler-options
                       :options opts})))
    opts))

(defn make-state
  ([] (make-state {}))
  ([opts]
   {:opts (validate-opts opts)
    :entries {}
    :stats {:scans 0
            :discovered 0
            :scheduled 0
            :completed 0
            :busy 0
            :failed 0
            :blocked 0}}))

(defn controller-objects
  "Return the scheduler's bounded snapshot from a controller state.

  Durable intents include their target and receipt evidence. A controller
  generation crossing its outbox boundary is represented as `:pending?` so a
  concurrent scan neither schedules nor forgets its prior scheduler entry. An
  absent acknowledgement is not a fresh remote inventory claim."
  [controller-state]
  (->> (:objects controller-state)
       (map (fn [[object-id entry]]
              (if (:intent-stored? entry)
                {:object-id object-id
                 :layout-epoch (:layout-epoch entry)
                 :intent-revision (:intent-revision entry)
                 :targets (set (:targets entry))
                 :acks (set (:acks entry))}
                {:object-id object-id :pending? true
                 :layout-epoch (:layout-epoch entry)
                 :intent-revision (:intent-revision entry)
                 :targets (set (:targets entry)) :acks (set (:acks entry))})))
       (sort-by :object-id)
       vec))

(defn- object-fingerprint [object]
  [(:layout-epoch object) (:intent-revision object) (:targets object)])

(defn- incomplete? [object]
  (seq (set/difference (:targets object) (:acks object))))

(defn- add-time [now delay]
  (min wire/max-safe-integer (+ now delay)))

(defn- reconcile-entry [state now object]
  (let [object-id (:object-id object)
        previous (get-in state [:entries object-id])
        fingerprint (object-fingerprint object)
        successor? (and (:in-flight? previous)
                        (= (:layout-epoch object)
                           (first (:fingerprint previous)))
                        (= (:targets object)
                           (nth (:fingerprint previous) 2))
                        (= (:intent-revision object)
                           (inc (second (:fingerprint previous)))))
        changed? (and previous (not= fingerprint (:fingerprint previous)))
        progress? (and previous (> (count (:acks object))
                                   (:ack-count previous)))
        entry (cond
                (nil? previous)
                {:fingerprint fingerprint
                 :ack-count (count (:acks object))
                 :rounds 0
                 :next-at (add-time now
                                    (get-in state [:opts :initial-grace-ms]))
                 :in-flight? false
                 :blocked? false}

                successor?
                (assoc previous :fingerprint fingerprint)

                changed?
                {:fingerprint fingerprint
                 :ack-count (count (:acks object))
                 :rounds 0
                 :next-at (add-time now
                                    (get-in state [:opts :initial-grace-ms]))
                 :in-flight? false
                 :blocked? false}

                progress?
                (assoc previous :ack-count (count (:acks object)) :rounds 0)

                :else previous)]
    [(assoc-in state [:entries object-id] entry)
     (nil? previous)]))

(defn- scan [state objects now]
  (when-not (and (integer? now) (<= 0 now wire/max-safe-integer)
                 (vector? objects))
    (throw (ex-info "Invalid repair scheduler scan"
                    {:type :netz.replication/invalid-repair-scan})))
  (let [pending-ids (into #{} (comp (filter :pending?) (map :object-id)) objects)
        durable (filterv (complement :pending?) objects)
        objects-by-id (into {} (map (juxt :object-id identity)) durable)
        incomplete (filterv incomplete? durable)
        ;; A first audit can withdraw an acknowledged copy and then fail its
        ;; outbox write before this scheduler ever saw a missing target. Admit
        ;; that pending generation as retry work too. repair! persists the same
        ;; candidate; it cannot dispatch until that write succeeds.
        new-pending (filterv #(and (:pending? %) (some? (:layout-epoch %))
                                   (some? (:intent-revision %)) (incomplete? %)
                                   (not (contains? (:entries state) (:object-id %)))) objects)
        live-ids (set (map :object-id incomplete))
        completed (count (filter (fn [object-id]
                                   (when-let [object (get objects-by-id object-id)]
                                     (not (incomplete? object))))
                                 (keys (:entries state))))
        [state discovered]
        (reduce (fn [[current discoveries] object]
                  (let [[next-state new?]
                        (reconcile-entry current now object)]
                    [next-state (+ discoveries (if new? 1 0))]))
                [(update state :entries select-keys (into live-ids pending-ids)) 0]
                (into incomplete new-pending))
        in-flight (count (filter :in-flight? (vals (:entries state))))
        available (max 0 (- (get-in state [:opts :max-in-flight]) in-flight))
        issue-limit (min available
                         (get-in state [:opts :max-repairs-per-scan]))
        due (->> (:entries state)
                 (keep (fn [[object-id entry]]
                         (when (and (not (:in-flight? entry))
                                    (not (:blocked? entry))
                                    (<= (:next-at entry) now))
                           object-id)))
                 sort
                 (take issue-limit)
                 vec)
        state (reduce (fn [current object-id]
                        (-> current
                            (assoc-in [:entries object-id :in-flight?] true)
                            (assoc-in [:entries object-id :operation]
                                      (get-in current
                                              [:entries object-id
                                               :fingerprint]))))
                      state due)]
    {:state (-> state
                (update-in [:stats :scans] inc)
                (update-in [:stats :discovered] + discovered)
                (update-in [:stats :completed] + completed)
                (update-in [:stats :scheduled] + (count due)))
     :actions (into (mapv #(vector :repair %
                                   (get-in state [:entries % :operation]))
                          due)
                    [[:timer (get-in state [:opts :scan-ms]) :repair-scan]])}))

(defn- retry-delay [state rounds]
  (let [backoff (get-in state [:opts :backoff-ms])]
    (nth backoff (min rounds (dec (count backoff))))))

(defn- repair-result [state object-id fingerprint result now]
  (if-let [entry (get-in state [:entries object-id])]
    (cond
      (or (not (:in-flight? entry))
          (not= fingerprint (:operation entry)))
      {:state state :actions []}

      (= :ok result)
      (let [entry (if (= fingerprint (:fingerprint entry))
                    (update-in entry [:fingerprint 1] inc)
                    entry)]
        {:state (-> state
                    (assoc-in [:entries object-id]
                              (-> entry
                                  (assoc :in-flight? false)
                                  (dissoc :operation)
                                  (assoc :next-at
                                         (add-time now
                                                   (retry-delay state
                                                                (:rounds entry))))
                                  (update :rounds inc))))
         :actions []})

      (= :busy result)
      {:state (-> state
                  (assoc-in [:entries object-id :in-flight?] false)
                  (update-in [:entries object-id] dissoc :operation)
                  (assoc-in [:entries object-id :next-at]
                            (add-time now (get-in state [:opts :scan-ms])))
                  (update-in [:stats :busy] inc))
       :actions []}

      (contains? #{:missing :stale-layout} result)
      {:state (-> state
                  (assoc-in [:entries object-id :in-flight?] false)
                  (update-in [:entries object-id] dissoc :operation)
                  (assoc-in [:entries object-id :blocked?] true)
                  (update-in [:stats :blocked] inc))
       :actions []}

      :else
      {:state (-> state
                  (assoc-in [:entries object-id]
                            (-> entry
                                (assoc :in-flight? false)
                                (dissoc :operation)
                                (assoc :next-at
                                       (add-time now
                                                 (retry-delay state
                                                              (:rounds entry))))
                                (update :rounds inc)))
                  (update-in [:stats :failed] inc))
       :actions []})
    {:state state :actions []}))

(defn handler
  "Advance the repair policy.

  Events are `:repair-scheduler/init`, `:repair-scheduler/scan` with a bounded
  `:objects` snapshot, and `:repair-scheduler/result` with `:object-id`, the
  action `:fingerprint`, and `:result`. Actions are
  `[:timer delay :repair-scan]` and `[:repair object-id fingerprint]`."
  [state {:keys [type objects object-id fingerprint result]} {:keys [now]}]
  (case type
    :repair-scheduler/init
    {:state state :actions [[:timer 0 :repair-scan]]}

    :repair-scheduler/scan
    (scan state objects now)

    :repair-scheduler/result
    (repair-result state object-id fingerprint result now)

    (throw (ex-info "Unknown repair scheduler event"
                    {:type :netz.replication/unknown-repair-scheduler-event
                     :event-type type}))))
