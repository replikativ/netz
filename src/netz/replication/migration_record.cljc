(ns netz.replication.migration-record
  "Storage-independent validation of trusted local migration checkpoints."
  (:require [netz.replication.migration :as migration]))

(def max-counter 9007199254740991)
(defn- uint? [x] (and (integer? x) (<= 0 x max-counter)))

(defn validate-record
  "The independently trusted template supplies placement, policy and authority.
  A checkpoint is not permitted to define its own expected transition."
  [template record minimum-revision]
  (let [base (migration/checkpoint template)
        dynamic #{:attempts :receipts :revision :halted?}
        attempts (:attempts record)]
    (when-not
     (and (map? record) (= (set (keys base)) (set (keys record)))
          (= (apply dissoc base dynamic) (apply dissoc record dynamic))
          (uint? minimum-revision) (uint? (:revision record))
          (<= minimum-revision (:revision record))
          (boolean? (:halted? record))
          (map? attempts) (= (set (keys (:attempts base))) (set (keys attempts)))
          (every? uint? (vals attempts))
          (map? (:receipts record))
          (every? (fn [[replica receipt]]
                    (and (contains? attempts replica) (map? receipt)
                         (= #{:attempt :expires} (set (keys receipt)))
                         (= (get attempts replica) (:attempt receipt))
                         (uint? (:expires receipt)) (pos? (:expires receipt))))
                  (:receipts record)))
      (throw (ex-info "Invalid migration checkpoint" {:type ::invalid})))
    record))
