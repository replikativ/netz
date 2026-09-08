(ns netz.replication.layout-v2
  "Experimental stable-placement layout bodies; not runtime-admissible yet.
  Version-1 validation, signing bytes and addresses remain unchanged."
  (:require [netz.identity :as identity]
            [netz.replication.layout :as layout]
            [netz.replication.stable-placement :as stable]
            [netz.wire :as wire]))

(def placement-profile "netz/blob-placement/2")
(def address-domain "netz/blob-layout/v2\u0000")

(defn- base-statement [statement]
  (-> statement (dissoc "placement" "seed") (assoc "v" 1)))

(defn valid-statement?
  "Validate an exact version-2 body, reusing version-1 member and chain fields.
  This does not verify a signature, predecessor, or controller authority."
  [statement]
  (and (map? statement)
       (= 2 (get statement "v"))
       (= placement-profile (get statement "placement"))
       (wire/byte-buffer? (get statement "seed"))
       (= 32 (identity/buf-length (get statement "seed")))
       (layout/valid-statement? (base-statement statement))))

(defn validate-statement [statement]
  (when-not (valid-statement? statement)
    (throw (ex-info "Invalid experimental stable-placement layout"
                    {:type :netz.replication/invalid-layout-v2})))
  statement)

(defn address
  "Hash the exact version-2 body under its separate layout address domain."
  [statement]
  (identity/sha256
   (identity/concat-bufs (identity/utf8-bytes address-domain)
                         (wire/encode-canonical (validate-statement statement)))))

(defn place
  "Calculate stable targets from a validated body and caller-supplied policy.
  Callers must authenticate the body before treating these targets as authority."
  [statement policy object-id]
  (validate-statement statement)
  (stable/place (layout/placement-layout (base-statement statement))
                (identity/bytes->hex (get statement "seed")) policy object-id))
