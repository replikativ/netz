(ns netz.replication-admission-test
  (:require [netz.replication.admission :as admission]
            #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is]])))

(def object-id (apply str (repeat 64 "a")))
(def initial (admission/make-state {:max-pending 2 :max-bytes 100 :max-age-ms 10}))

(deftest bounded-writer-admission-and-idempotency
  (let [first (:state (admission/reserve initial "tx1" 60 0))]
    (is (= :exists (:reason (admission/reserve first "tx1" 60 1))))
    (is (= :conflict (:reason (admission/reserve first "tx1" 61 1))))
    (is (= :bytes (:reason (admission/reserve first "tx2" 41 1))))
    (is (= :lag (:reason (admission/reserve first "tx2" 1 10))))
    (let [second (:state (admission/reserve first "tx2" 40 1))]
      (is (= :count (:reason (admission/reserve second "tx3" 0 2)))))))

(deftest exact-confirmation-required-to-release-credit
  (let [reserved (:state (admission/reserve initial "tx" 60 0))
        bound (admission/bind-object reserved "tx" object-id)
        status {:object-id object-id :status :confirmed :retain-until 100}]
    (doseq [invalid [(assoc status :object-id "wrong")
                     (assoc status :status :timeout)
                     (assoc status :retain-until 1)]]
      (is (= bound (admission/confirm bound "tx" invalid 2))))
    (is (empty? (:pending (admission/confirm bound "tx" status 2))))
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (admission/cancel-before-write bound "tx")))
    (is (empty? (:pending (admission/cancel-before-write reserved "tx"))))))
