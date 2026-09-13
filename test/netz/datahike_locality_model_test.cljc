(ns netz.datahike-locality-model-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.datahike-locality-model :as model]))

(deftest producer-placement-and-consumer-caches-remain-distinct
  (let [{:keys [hash-only producer-planned]}
        (model/compare-plans model/example-scenario
                             "datahike/catalog/example")]
    (testing "hard producer policy survives one complete site loss at W"
      (is (not (:placement-domains-satisfied? hash-only)))
      (is (:placement-domains-satisfied? producer-planned))
      (is (= 3 (count (:target-sites producer-planned))))
      (is (:write-threshold-survives-one-site-loss? producer-planned)))
    (testing "static locality planning improves the configured cold-read mix"
      (is (<= (:demand-weighted-source-rtt-ms producer-planned)
              (:demand-weighted-source-rtt-ms hash-only))))
    (testing "demand caches improve reads without manufacturing receipts"
      (is (zero? (:cached-source-rtt-ms producer-planned)))
      (is (pos? (:opportunistic-cache-count producer-planned)))
      (is (= 3 (:durable-copy-count producer-planned))))))

(deftest acknowledgement-and-placement-domain-minima-are-independent
  (let [scenario (assoc model/example-scenario
                        :placement-minimum-domains {"site" 3}
                        :ack-minimum-domains {"site" 2})
        result (model/report scenario (model/planned-targets scenario))]
    (is (= 3 (count (:target-sites result))))
    (is (< (:durable-ms result) (:all-targets-ms result)))))
