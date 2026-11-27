(ns netz.replication-lifecycle-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.lifecycle :as lifecycle]
            [netz.replication.lifecycle-storage :as lifecycle-storage]
            [netz.replication.wire :as replication-wire]
            [netz.sim :as sim]))

(def public-hex
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
(def genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "aa"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "cc"))}))
(def controller (identity/authority-id-bytes genesis))
(def replica-genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "dd"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "ee"))}))
(def replica (identity/authority-id-bytes replica-genesis))
(def cluster (identity/hex->bytes (apply str (repeat 16 "10"))))
(def privacy-domain (identity/hex->bytes (apply str (repeat 16 "20"))))
(def split (blob/split (identity/seq->bytes (range 20)) {:piece-size 8}))

(def retention
  {"v" 1 "type" "blob/retention"
   "cluster" cluster "privacy_domain" privacy-domain
   "revision" 1 "manifest" (:manifest-id split) "object" (:object-id split)
   "layout_epoch" 7 "replicas" 3 "write_acks" 2
   "placement_minimum_domains" {"owner" 2}
   "ack_minimum_domains" {"owner" 2} "issued_at" 1000
   "retain_until" 2000 "signer" controller})

(deftest lifecycle-storage-records-have-a-closed-portable-shape
  (let [key (str (apply str (repeat 16 "10")) "/"
                 (apply str (repeat 16 "20")) "/"
                 (apply str (repeat 32 "30")))
        record {:statement retention
                :controller (identity/bytes->hex controller)
                :revision 1 :fingerprint "fingerprint" :status :retained}
        encoded (lifecycle-storage/record->wire key record)]
    (is (= [key record] (lifecycle-storage/wire->record encoded)))
    (is (thrown? #?(:clj Throwable :cljs js/Error)
                 (lifecycle-storage/wire->record
                  (assoc-in encoded ["record" "future_field"] true))))))

(defn lease [revision retain-until]
  {"v" 1 "type" "blob/lease"
   "cluster" cluster "privacy_domain" privacy-domain
   "revision" revision "manifest" (:manifest-id split)
   "object" (:object-id split) "layout_epoch" 7
   "issued_at" 1100 "retain_until" retain-until "signer" controller})

(defn deletion [revision delete-after]
  {"v" 1 "type" "blob/deletion"
   "cluster" cluster "privacy_domain" privacy-domain
   "revision" revision "manifest" (:manifest-id split)
   "object" (:object-id split) "layout_epoch" 7
   "issued_at" 1200 "delete_after" delete-after "signer" controller})

(defn state [] (lifecycle/make-state replica [controller]))

(defn step
  ([state event] (step state event 1000))
  ([state event now] (lifecycle/handler state event {:now now})))

(defn accept [state statement now]
  (step state {:type :lifecycle/verified-statement :statement statement} now))

(defn land
  ([state actions] (land state actions :ok 1000))
  ([state actions result now]
   (step state {:type :lifecycle/persisted
                :key (get-in actions [0 1 1]) :result result}
         now)))

(defn fake-envelope [statement]
  (replication-wire/envelope
   replica-genesis (identity/hex->bytes public-hex) statement
   (identity/seq->bytes (repeat identity/signature-size 0))))

(defn- runtime-actions [actions]
  (mapv (fn [[op & args :as action]]
          (case op
            :delete-object
            (let [[key statement persistence] args]
              [:persist [:blob/delete key]
               {:object (get statement "object") :deleted? true}
               {:type :demo/delete-result :key key
                :persistence persistence}])

            :sign-deletion-receipt
            (let [[key receipt] args]
              [:timer 0 {:type :demo/receipt-signed :key key
                         :envelope (fake-envelope receipt)}])

            :emit-deletion-receipt
            (let [[_key envelope] args]
              [:deliver :app :deletion envelope nil])

            action))
        actions))

(defn- lifecycle-node [state event ctx]
  (let [transition
        (cond
          (and (= :message (:type event)) (= :app (:from event)))
          (lifecycle/handler state
                             {:type :lifecycle/verified-statement
                              :statement (get-in event [:payload :statement])}
                             ctx)

          (and (= :persist-result (:type event))
               (= :demo/delete-result (get-in event [:payload :type])))
          (lifecycle/handler
           state {:type :lifecycle/delete-result
                  :key (get-in event [:payload :key])
                  :result (:result event)
                  :persistence (get-in event [:payload :persistence])
                  :deleted-at (:now ctx)} ctx)

          (and (= :timer (:type event))
               (= :demo/receipt-signed (get-in event [:payload :type])))
          (lifecycle/handler
           state {:type :lifecycle/receipt-signed
                  :key (get-in event [:payload :key])
                  :envelope (get-in event [:payload :envelope])} ctx)

          :else (lifecycle/handler state event ctx))]
    (update transition :actions runtime-actions)))

(defn- restore-lifecycle [initial durable]
  (lifecycle/restore-state
   initial
   (into {}
         (keep (fn [[[kind key] value]]
                 (when (= :blob/lifecycle kind) [key value])))
         durable)))

(deftest statements-take-effect-only-after-the-journal-lands
  (let [{pending :state actions :actions} (accept (state) retention 1000)]
    (is (= [:persist] (mapv first actions)))
    (is (= :unknown (lifecycle/status pending retention)))
    (is (seq (:pending pending)))
    (testing "a crash before persistence loses no accepted authority state"
      (let [restarted (lifecycle/restore-state (state) {})]
        (is (= :unknown (lifecycle/status restarted retention)))))
    (let [{accepted :state timers :actions} (land pending actions :ok 1000)]
      (is (= :retained (lifecycle/status accepted retention)))
      (is (= [:timer] (mapv first timers)))
      (is (empty? (:pending accepted)))
      (let [{restart-actions :actions}
            (step (lifecycle/restore-state (state) (:records accepted))
                  {:type :init} 1500)]
        (is (= 500 (get-in restart-actions [0 1]))
            "restart restores the lease-expiry timer from durable state")))))

(deftest lease-revisions-are-monotone-and-persistence-fenced
  (let [{pending :state actions :actions} (accept (state) retention 1000)
        {retained :state} (land pending actions)
        renewal (lease 2 3000)
        {renewing :state renewal-actions :actions} (accept retained renewal 1100)
        {failed :state} (land renewing renewal-actions :full 1100)]
    (is (= :retained (lifecycle/status failed retention)))
    (is (= 1 (-> failed :records vals first :revision))
        "a failed journal write cannot advance the accepted revision")
    (let [{retrying :state retry-actions :actions} (accept failed renewal 1100)
          {renewed :state} (land retrying retry-actions :ok 1100)
          {stale :state} (accept renewed retention 1200)
          conflict (assoc renewal "retain_until" 3100)
          {equivocated :state} (accept renewed conflict 1200)]
      (is (= 2 (-> renewed :records vals first :revision)))
      (is (= 1 (get-in stale [:stats :stale])))
      (is (= 1 (get-in equivocated [:stats :equivocations])))
      (is (= 1 (count (:equivocations equivocated)))))))

(deftest full-retention-can-advance-layout-but-lease-cannot
  (let [{pending :state actions :actions} (accept (state) retention 1000)
        {retained :state} (land pending actions)
        migrated-statement (assoc retention
                                  "revision" 2
                                  "layout_epoch" 8
                                  "issued_at" 1100
                                  "retain_until" 3000)
        {migrating :state migration-actions :actions}
        (accept retained migrated-statement 1100)
        {migrated :state} (land migrating migration-actions :ok 1100)
        old-layout-lease (lease 3 4000)
        {lease-rejected :state} (accept migrated old-layout-lease 1200)
        regressed-retention (assoc retention
                                   "revision" 3
                                   "issued_at" 1200
                                   "retain_until" 4000)
        {regression-rejected :state}
        (accept migrated regressed-retention 1200)
        delete-new-layout (assoc (deletion 3 1300) "layout_epoch" 8)
        {deleting :state delete-actions :actions}
        (accept migrated delete-new-layout 1200)]
    (is (= [:persist] (mapv first migration-actions)))
    (is (= 8 (-> migrated :records vals first :statement
                 (get "layout_epoch"))))
    (is (empty? (:actions (accept migrated old-layout-lease 1200))))
    (is (= 1 (get-in lease-rejected [:stats :rejected])))
    (is (= 1 (get-in regression-rejected [:stats :rejected])))
    (is (= [:persist] (mapv first delete-actions)))
    (is (= 8 (-> deleting :pending vals first :record :statement
                 (get "layout_epoch"))))))

(deftest expiry-is-journaled-before-delete-and-is-absorbing
  (let [{pending :state actions :actions} (accept (state) retention 1000)
        {retained :state} (land pending actions)
        {expiring :state persist-expiry :actions}
        (step retained {:type :lifecycle/tick} 2000)]
    (is (= :retained (lifecycle/status expiring retention))
        "delete cannot begin before the expiry tombstone is durable")
    (is (= [:persist] (mapv first persist-expiry)))
    (let [{deleting :state deletes :actions}
          (land expiring persist-expiry :ok 2000)]
      (is (= :expiring (lifecycle/status deleting retention)))
      (is (= [:delete-object] (mapv first deletes)))
      (let [{deleted-pending :state deleted-persist :actions}
            (step deleting {:type :lifecycle/delete-result
                            :key (get-in deletes [0 1]) :result :ok
                            :persistence "netz/store/fsync/1"
                            :deleted-at 2001} 2001)
            {deleted :state post-delete :actions}
            (land deleted-pending deleted-persist :ok 2001)
            {rejected :state resurrection-actions :actions}
            (accept deleted (lease 2 4000) 2100)]
        (is (= :deleted (lifecycle/status deleted retention)))
        (is (empty? post-delete) "expiry has no controller deletion receipt")
        (is (empty? resurrection-actions))
        (is (= :deleted (lifecycle/status rejected retention)))))))

(deftest a-scheduled-or-failed-deletion-cannot-be-renewed
  (let [marker (deletion 1 1300)
        {pending :state actions :actions} (accept (state) marker 1200)
        {waiting :state} (land pending actions :ok 1200)
        {due :state writes :actions} (step waiting {:type :lifecycle/tick} 1300)
        {deleting :state deletes :actions} (land due writes :ok 1300)
        {failing :state changes :actions}
        (step deleting {:type :lifecycle/delete-result :key (get-in deletes [0 1])
                        :result :error} 1301)
        {failed :state} (land failing changes :ok 1301)]
    (doseq [current [waiting failed]
            renewal [(assoc (lease 2 4000) "issued_at" 1301)
                     (assoc retention "revision" 2 "issued_at" 1301)]]
      (let [result (accept current renewal 1301)]
        (is (empty? (:actions result)))
        (is (= (:records current) (:records (:state result))))))))

(deftest deletion-receipt-is-signed-journaled-and-replayable
  (let [marker (deletion 3 1300)
        {pending :state persist-marker :actions} (accept (state) marker 1200)
        {waiting :state timer :actions} (land pending persist-marker :ok 1200)
        {due :state persist-due :actions}
        (step waiting {:type :lifecycle/tick} 1300)
        {deleting :state delete-actions :actions}
        (land due persist-due :ok 1300)
        key (get-in delete-actions [0 1])
        {deleted-pending :state persist-deleted :actions}
        (step deleting {:type :lifecycle/delete-result :key key :result :ok
                        :persistence "netz/store/fsync/1" :deleted-at 1301}
              1301)
        {deleted :state sign-actions :actions}
        (land deleted-pending persist-deleted :ok 1301)
        receipt (get-in sign-actions [0 2])
        {signed-pending :state persist-signed :actions}
        (step deleted {:type :lifecycle/receipt-signed :key key
                       :envelope (fake-envelope receipt)} 1301)
        {complete :state emitted :actions}
        (land signed-pending persist-signed :ok 1301)]
    (is (= [:timer] (mapv first timer)))
    (is (= [:persist] (mapv first persist-due)))
    (is (= [:delete-object] (mapv first delete-actions)))
    (is (= "blob/deleted" (get receipt "type")))
    (is (= 3 (get receipt "deletion_revision")))
    (is (= [:persist] (mapv first persist-signed))
        "the signed receipt is durable before it is emitted")
    (is (= [:emit-deletion-receipt] (mapv first emitted)))
    (let [{:keys [actions]}
          (step (lifecycle/restore-state (state) (:records complete))
                {:type :init} 2000)]
      (is (= [:emit-deletion-receipt] (mapv first actions))
          "restart safely replays the durable receipt"))))

(deftest failed-deletion-retries-without-losing-the-tombstone
  (let [marker (deletion 1 1200)
        {pending :state persist-marker :actions} (accept (state) marker 1200)
        {deleting :state deletes :actions} (land pending persist-marker :ok 1200)
        key (get-in deletes [0 1])
        {failed-pending :state persist-failed :actions}
        (step deleting {:type :lifecycle/delete-result :key key :result :full}
              1201)
        {failed :state retry-timer :actions}
        (land failed-pending persist-failed :ok 1201)
        early (step failed {:type :lifecycle/tick} 2000)
        {retrying :state persist-retry :actions}
        (step failed {:type :lifecycle/tick} 31201)
        {again :state delete-again :actions}
        (land retrying persist-retry :ok 31201)]
    (is (= :deletion-failed (lifecycle/status failed marker)))
    (is (= [:timer] (mapv first retry-timer)))
    (is (empty? (:actions early)))
    (let [{restart-actions :actions}
          (step (lifecycle/restore-state (state) (:records failed))
                {:type :init} 2000)]
      (is (= 29201 (get-in restart-actions [0 1]))
          "restart restores the deletion retry timer"))
    (is (= [:persist] (mapv first persist-retry)))
    (is (= [:delete-object] (mapv first delete-again)))
    (is (= :deleting (lifecycle/status again marker)))))

(deftest unauthorized-controller-and-orphan-lease-are-rejected
  (let [stranger (identity/hex->bytes (apply str (repeat 32 "ff")))
        unauthorised (assoc retention "signer" stranger)
        {a :state} (accept (state) unauthorised 1000)
        {b :state} (accept (state) (lease 1 3000) 1000)]
    (is (= 1 (get-in a [:stats :rejected])))
    (is (= 1 (get-in b [:stats :rejected])))
    (is (empty? (:records a)))
    (is (empty? (:records b)))))

(deftest simulator-proves-crash-recovery-and-bounded-bulk-expiry
  (let [initial (state)
        base (-> (sim/make-sim {:persist-latency-ms 5 :trace? false})
                 (sim/add-node :replica lifecycle-node initial
                               {:restore restore-lifecycle})
                 (sim/send-message :app :replica {:statement retention})
                 sim/step
                 sim/step)
        crashed (sim/crash base :replica)
        restarted (-> crashed (sim/run-until 10) (sim/restart :replica)
                      (sim/run-until 20))]
    (is (= :unknown
           (lifecycle/status (sim/node-state restarted :replica) retention))
        "the journal write in flight at crash did not become authority state")
    (let [objects (mapv (fn [index]
                          (assoc retention
                                 "object" (identity/sha256
                                           (identity/utf8-bytes (str index)))
                                 "manifest" (identity/sha256
                                             (identity/utf8-bytes
                                              (str "manifest-" index)))))
                        (range 64))
          loaded (reduce (fn [simulation statement]
                           (sim/send-message simulation :app :replica
                                             {:statement statement}))
                         restarted objects)
          retained (sim/run-until loaded 100)
          expired (sim/run-until retained 2100)
          node (sim/node-state expired :replica)]
      (is (= 64 (count (:records node))))
      (is (every? #(= :deleted (lifecycle/status node %)) objects))
      (is (<= (:peak-inflight expired) 64))
      (is (= 64 (get-in node [:stats :expired])))
      (is (= 64 (get-in node [:stats :deleted]))))))
