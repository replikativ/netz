(ns netz.mixed-sync-profile
  "Shared JVM/Node test profile; application chunks remain canonical opaque bytes."
  (:require [netz.sync.profile :as sync-profile]
            [netz.wire :as wire]))

(defn portable-join-profile []
  (reify sync-profile/SyncProfile
    (-profile-id [_] "netz/join-semilattice/1")
    (-describe [_ value _topic]
      (wire/encode-canonical (vec (sort value))))
    (-respond [_ value _topic remote-summary]
      (let [remote (set (wire/decode remote-summary))
            missing (vec (sort (remove remote value)))]
        {:chunks (cond-> [] (seq missing)
                         (conj (wire/encode-canonical missing)))
         :checkpoint (wire/encode-canonical (vec (sort value)))}))
    (-begin [_ value _topic] (set value))
    (-apply-chunk [_ staging _topic chunk]
      (into staging (wire/decode chunk)))
    (-commit [_ value _topic staging checkpoint]
      (let [described (set (wire/decode checkpoint))]
        (when-not (every? staging described)
          (throw (ex-info "checkpoint was not fully staged" {})))
        (into (set value) staging)))))
