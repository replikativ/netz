(ns netz.sim.schedule
  "Fault schedules as portable data.

  Seeds still choose low-level delay, loss and duplication. Human-meaningful
  events are vectors so property testing can delete and simplify them into a
  readable minimal counterexample."
  (:require [clojure.string :as str]
            [netz.sim :as sim]))

(defn step
  "Apply one schedule operation."
  [s [op & args]]
  (case op
    :advance (sim/run-until s (+ (:now s) (or (first args) 1)))
    :run-until (sim/run-until s (first args))
    :pause (sim/pause s (first args))
    :resume (sim/resume s (first args))
    :crash (sim/crash s (first args))
    :restart (sim/restart s (first args))
    :partition (sim/partition-network s (first args))
    :isolate (let [id (first args)]
               (sim/partition-network
                s (into {} (map (fn [peer] [peer (if (= peer id) :isolated :main)]))
                        (sim/node-ids s))))
    :heal (sim/heal s)
    :link-down (sim/link-down s (first args) (second args))
    :inject (sim/send-message s (first args) (second args) (nth args 2))
    (throw (ex-info "Unknown simulator schedule operation"
                    {:type :netz.sim.schedule/unknown-operation
                     :operation op :args args}))))

(defn run [s schedule]
  (reduce step s schedule))

(defn describe
  "Human-readable schedule for failure reports."
  [schedule]
  (str/join
   "\n"
   (map (fn [[op & args]]
          (case op
            :advance (str "  advance " (first args) " ms")
            :pause (str "  PAUSE " (first args))
            :resume (str "  resume " (first args))
            :crash (str "  CRASH " (first args))
            :restart (str "  restart " (first args))
            :isolate (str "  isolate " (first args))
            :heal "  heal"
            (str "  " (name op) " " (pr-str args))))
        schedule)))
