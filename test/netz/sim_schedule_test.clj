(ns netz.sim-schedule-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.properties :as prop]
            [netz.sim :as sim]
            [netz.sim.schedule :as schedule]
            [netz.sim.schedule-gen :as schedule-gen]
            [netz.sim-test :as subject]))

(def peers [:n0 :n1 :n2 :n3 :n4 :n5])

(deftest schedules-are-readable-data
  (let [steps [[:advance 3] [:pause :n2] [:isolate :n3]
               [:heal] [:resume :n2]]
        rendered (schedule/describe steps)]
    (is (re-find #"PAUSE :n2" rendered))
    (is (re-find #"isolate :n3" rendered))
    (is (= steps (read-string (pr-str steps))))))

(defspec healed-schedules-preserve-eventual-flooding 50
  (prop/for-all
   [steps (schedule-gen/gen-schedule peers {:max-steps 10 :settle-ms 1000})]
   (let [prefix (vec (butlast steps))
         settle (last steps)
         s (-> (subject/ring-network {:seed 19 :latency-min 1 :latency-max 20} 6)
               (schedule/run prefix)
               (sim/send-message :external :n0 {:mid 99 :body :after-faults})
               (schedule/step settle))]
     (or (= #{:after-faults}
            (subject/received s :n5))
         (do
           (println "Failing schedule:\n" (schedule/describe steps))
           false)))))
