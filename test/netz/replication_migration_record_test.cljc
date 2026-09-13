(ns netz.replication-migration-record-test
  (:require [clojure.test :refer [deftest is]]
            [netz.replication.migration :as migration]
            [netz.replication.migration-record :as record]
            [netz.replication-migration-test :as model]))

(deftest checkpoint-record-is-closed-and-bound-to-independent-template
  (let [template (model/initial)
        value (migration/checkpoint template)]
    (is (= value (record/validate-record template value 0)))
    (doseq [bad [(assoc value :unexpected true)
                 (assoc value :targets [])
                 (assoc value :revision -1)
                 (assoc value :halted? nil)
                 (assoc value :attempts {})
                 (assoc value :retirements {(model/id "0")
                                            {:phase :retired :generation 1}})
                 (assoc value :retirements {(model/id "1")
                                            {:phase :planned :generation 1}})
                 (assoc value :retirements {(model/id "0")
                                            {:phase :unknown :generation 1}})
                 (assoc value :retirements {(model/id "0")
                                            {:phase :planned :generation 1}})
                 (assoc-in value [:binding :controller] "other")
                 (assoc value :receipts {"unknown" {:attempt 0 :expires 100}})]]
      (is (try (record/validate-record template bad 0) false
               (catch #?(:clj Exception :cljs :default) _ true))))
    (is (try (record/validate-record template value 1) false
             (catch #?(:clj Exception :cljs :default) _ true)))))

(deftest valid-retirement-phases-survive-checkpoint-validation
  (let [template (model/initial)
        complete (model/fill template)
        replica (model/id "0")
        context (model/retirement-context complete)
        planned (model/persist
                 (:state (migration/step complete
                                         (merge context {:type :plan-retirement
                                                         :replica replica}))))
        generation (get-in planned [:retirements replica :generation])
        token {:binding (:binding planned) :replica replica :generation generation}
        deleting (model/persist
                  (:state (migration/step planned
                                          (merge context token
                                                 {:type :begin-retirement}))))
        retired (model/persist
                 (:state (migration/step deleting
                                         (merge token {:type :retired
                                                       :deleted-at 21}))))]
    (doseq [state [planned deleting retired]]
      (is (= (migration/checkpoint state)
             (record/validate-record template (migration/checkpoint state) 0))))))
