(ns netz.sync.profile
  "Application semantics used by the transport-independent sync machine.")

(defprotocol SyncProfile
  (-profile-id [profile])
  (-describe [profile value topic]
    "Opaque summary of the committed local value.")
  (-respond [profile value topic remote-summary]
    "Return {:chunks [...] :checkpoint x} needed by `remote-summary`.")
  (-begin [profile value topic]
    "Create isolated staging state for an inbound session.")
  (-apply-chunk [profile staging topic chunk]
    "Apply one ordered chunk to staging state.")
  (-commit [profile value topic staging checkpoint]
    "Atomically produce the new committed value."))

(defprotocol CommittedFrontierProfile
  (-commit-with-frontiers
    [profile value topic staging checkpoint claimed-frontiers]
    "Atomically commit staged state and return locally validated receipt data.

    The result is `{:application value :frontiers map}`. Netz computes the
    deterministic profile-1 byte count locally; an adapter-supplied count is
    ignored.
    `claimed-frontiers` came from the network and is untrusted. Implementations
    must return only the subset cryptographically or causally bound to the
    committed checkpoint. Merely echoing the claim is a correctness bug."))

(defn profile-id [profile] (-profile-id profile))
(defn describe [profile value topic] (-describe profile value topic))
(defn respond [profile value topic summary] (-respond profile value topic summary))
(defn begin [profile value topic] (-begin profile value topic))
(defn apply-chunk [profile staging topic chunk]
  (-apply-chunk profile staging topic chunk))
(defn commit [profile value topic staging checkpoint]
  (-commit profile value topic staging checkpoint))

(defn commit-result
  "Commit a synchronization session and return its local receipt.

  Legacy profiles remain safe: they commit their application value but claim
  no publication frontier. They therefore cannot suppress buffered live data
  on the strength of an untrusted `sync/done` field."
  [profile value topic staging checkpoint claimed-frontiers]
  (if (satisfies? CommittedFrontierProfile profile)
    (-commit-with-frontiers profile value topic staging checkpoint
                            claimed-frontiers)
    {:application (commit profile value topic staging checkpoint)
     :frontiers {}}))
