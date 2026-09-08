(ns netz.replication.stable-placement
  "Experimental placement profile 2; not accepted by the signed-layout runtime.

  A stable pool seed replaces the authority epoch in rendezvous scores. Changing
  the epoch fences control operations without gratuitously moving object data.
  Activation requires a future authenticated layout schema and safe migration."
  (:require [netz.replication.placement :as placement]))

(defn score [seed object-id member-id]
  (placement/stable-score seed object-id member-id))

(defn rank
  "Rank eligible members using an explicit stable pool seed, never the epoch."
  [layout seed object-id]
  (placement/stable-rank layout seed object-id))

(defn place
  "Experimental deterministic N-target placement with profile-1 domain rules.

  Unconstrained membership additions/removals preserve unaffected target sets.
  Domain constraints can require additional movement; no stronger claim is made."
  [layout seed policy object-id]
  (let [policy (placement/validate-policy layout policy)]
    (placement/select-ranked (rank layout seed object-id) policy)))
