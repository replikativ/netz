(ns netz.mixed-identity
  (:require [netz.identity :as id]))

(defn test-identity []
  ;; Public test vector from netz-kabel-noise-binding-1.json, never production keys.
  (let [public "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c"]
    {:genesis (id/make-genesis {:rotation-keys [public] :rotation-threshold 1
                                :operational-keys [public]
                                :revocation-commit (apply str (repeat 32 "21"))})
     :operational {:public (id/hex->bytes public)
                   :private (id/hex->bytes (apply str (repeat 32 "01")))}}))
