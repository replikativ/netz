(ns netz.sync.join-semilattice
  "Reference `netz/join-semilattice/1` profile using mathematical sets.

  It is intentionally small: production Spindel/Yggdrasil adapters replace the
  set summary with their causal frontier and encode deltas or snapshots as
  chunks, while retaining the same join laws."
  (:require [clojure.set :as set]
            [netz.sync.profile :as profile]))

(def profile-name "netz/join-semilattice/1")

(defrecord JoinSemilatticeProfile [chunk-size]
  profile/SyncProfile
  (-profile-id [_] profile-name)
  (-describe [_ value _topic] (set value))
  (-respond [_ value _topic remote-summary]
    (let [missing (sort-by pr-str (set/difference (set value)
                                                  (set remote-summary)))]
      {:chunks (mapv set (partition-all chunk-size missing))
       :checkpoint (set value)}))
  (-begin [_ value _topic] (set value))
  (-apply-chunk [_ staging _topic chunk] (set/union (set staging) (set chunk)))
  (-commit [_ value _topic staging checkpoint]
    (set/union (set value) (set staging) (set checkpoint))))

(defn profile
  ([] (profile 64))
  ([chunk-size] (->JoinSemilatticeProfile chunk-size)))
