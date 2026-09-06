(ns netz.replication.stable-placement
  "Experimental placement profile 2; not accepted by the signed-layout runtime.

  A stable pool seed replaces the authority epoch in rendezvous scores. Changing
  the epoch fences control operations without gratuitously moving object data.
  Activation requires a future authenticated layout schema and safe migration."
  (:require [netz.identity :as identity]
            [netz.replication.placement :as placement]
            [netz.wire :as wire]))

(defn- validate-seed [seed]
  (when-not (and (string? seed) (re-matches #"[0-9a-f]{64}" seed))
    (throw (ex-info "Placement seed must be 32 bytes as lowercase hex"
                    {:type :netz.replication/invalid-placement-seed})))
  seed)

(defn score [seed object-id member-id]
  (identity/sha256
   (wire/encode-canonical
    {"seed" (validate-seed seed)
     "member" member-id
     "object" object-id
     "profile" "netz/blob-placement/2"})))

(defn rank
  "Rank eligible members using an explicit stable pool seed, never the epoch."
  [layout seed object-id]
  (placement/validate-layout layout)
  (validate-seed seed)
  (->> (placement/eligible-members layout)
       (mapv #(assoc % ::score (score seed object-id (:id %))))
       (sort (fn [a b]
               (let [c (placement/compare-bytes (::score b) (::score a))]
                 (if (zero? c) (compare (:id a) (:id b)) c))))
       (mapv #(dissoc % ::score))))

(defn place
  "Experimental deterministic N-target placement with profile-1 domain rules.

  Unconstrained membership additions/removals preserve unaffected target sets.
  Domain constraints can require additional movement; no stronger claim is made."
  [layout seed policy object-id]
  (let [policy (placement/validate-policy layout policy)]
    (placement/select-ranked (rank layout seed object-id) policy)))
