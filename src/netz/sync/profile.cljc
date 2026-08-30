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

(defn profile-id [profile] (-profile-id profile))
(defn describe [profile value topic] (-describe profile value topic))
(defn respond [profile value topic summary] (-respond profile value topic summary))
(defn begin [profile value topic] (-begin profile value topic))
(defn apply-chunk [profile staging topic chunk]
  (-apply-chunk profile staging topic chunk))
(defn commit [profile value topic staging checkpoint]
  (-commit profile value topic staging checkpoint))
