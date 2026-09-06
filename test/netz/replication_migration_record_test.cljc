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
                 (assoc-in value [:binding :controller] "other")
                 (assoc value :receipts {"unknown" {:attempt 0 :expires 100}})]]
      (is (try (record/validate-record template bad 0) false
               (catch #?(:clj Exception :cljs :default) _ true))))
    (is (try (record/validate-record template value 1) false
             (catch #?(:clj Exception :cljs :default) _ true)))))
