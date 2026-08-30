(ns netz.scale-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.dissemination-test :as dissemination-test]
            [netz.overlay :as overlay]
            [netz.sim :as sim]))

(deftest dissemination-scale-matrix
  (doseq [n [10 100 1000]]
    (testing (str n " logical peers")
      (let [s (-> (dissemination-test/overlay-network
                   n {:seed 3 :trace? false :max-steps 20000000} {})
                  (sim/run-until 60000)
                  (sim/send-message :app :n0
                                    {:type :publish :topic :t :payload :scale})
                  (sim/run-until 90000))
            delivered (count
                       (filter #(= [:scale]
                                   (vec (overlay/delivered
                                         (sim/node-state s %))))
                               (sim/node-ids s)))]
        (is (= n delivered))
        ;; This includes 90 seconds of membership and anti-entropy control
        ;; traffic, not just the publication. It is deliberately generous but
        ;; catches accidental quadratic flooding at the first useful scales.
        (is (< (get-in s [:stats :sent]) (* 250 n))
            (str "traffic amplification at " n " peers: " (:stats s)))
        (is (< (:peak-inflight s) (* 20 n))
            (str "in-flight queue grew unexpectedly: " (:peak-inflight s)))))))
