(ns netz.sim.schedule-gen
  "Shrinkable test.check generators for Netz simulator schedules."
  (:require [clojure.test.check.generators :as gen]))

(defn gen-advance
  ([] (gen-advance 1 5000))
  ([lo hi] (gen/fmap (fn [n] [:advance n]) (gen/choose lo hi))))

(defn gen-fault [peers]
  (let [peers (vec peers)]
    (gen/one-of
     [(gen/fmap (fn [p] [:isolate p]) (gen/elements peers))
      (gen/return [:heal])
      (gen/fmap (fn [p] [:pause p]) (gen/elements peers))
      (gen/fmap (fn [p] [:resume p]) (gen/elements peers))])))

(defn gen-schedule
  "Interleave time and faults, then restore reachability and allow settling."
  ([peers] (gen-schedule peers {}))
  ([peers {:keys [max-steps settle-ms]
           :or {max-steps 12 settle-ms 30000}}]
   (let [peers (vec peers)]
     (gen/fmap (fn [steps]
                 (vec (concat steps
                              [[:heal]]
                              (mapv (fn [p] [:resume p]) peers)
                              [[:advance settle-ms]])))
               (gen/vector (gen/one-of [(gen-advance) (gen-fault peers)])
                           0 max-steps)))))

(defn gen-crash-cycles
  "Paired crash/restart histories, so every generated crash is recoverable."
  [peers]
  (let [peers (vec peers)]
    (gen/fmap
     (fn [cycles]
       (vec (concat (mapcat (fn [[p before after]]
                              [[:crash p] [:advance before]
                               [:restart p] [:advance after]])
                            cycles)
                    [[:heal] [:advance 30000]])))
     (gen/vector (gen/tuple (gen/elements peers)
                            (gen/choose 1 1000)
                            (gen/choose 1 1000))
                 0 6))))
