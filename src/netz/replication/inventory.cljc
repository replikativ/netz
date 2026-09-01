(ns netz.replication.inventory
  "Portable local inventory and integrity-scrub boundary.

  Inventory is evidence that an installation marker exists, not a durability
  receipt and not proof that every piece is still healthy. `-scrub-object!`
  performs that stronger local check. Repair controllers can therefore compare
  placement receipts, paginated inventory and scrub results without learning a
  backend's private keys or paths."
  (:require [clojure.string :as str]
            [netz.identity :as identity]
            [netz.wire :as wire]))

(def max-page-size 1024)

(defprotocol PBlobInventoryStore
  (-inventory-page [store cursor limit]
    "Yield `{:manifests [32-byte-address ...] :next-cursor hex-or-nil}`.
    `cursor` is the last manifest hex returned by the preceding page and is
    exclusive. Implementations must sort by lowercase address bytes.")

  (-scrub-object! [store manifest-id]
    "Re-read an installed marker and every referenced piece, verifying the
    manifest, piece and full-object addresses. Yield `{:status :healthy ...}`
    or `{:status :missing :manifest ...}`. Corruption and IO failures are
    delivered as exception values; callers must not count them as replicas."))

(defprotocol PBlobProviderStore
  (-load-installed-manifest [store manifest-id]
    "Yield the validated transfer-manifest descriptor for an installed marker,
    or nil when absent. This is the provider-side source for an authenticated
    fetch; it does not itself authorize serving the object."))

(defn valid-cursor? [cursor]
  (or (nil? cursor)
      (and (string? cursor) (re-matches #"[0-9a-f]{64}" cursor))))

(defn valid-limit? [limit]
  (and (int? limit) (<= 1 limit max-page-size)))

(defn address-hex [value]
  (cond
    (and (wire/byte-buffer? value) (= 32 (identity/buf-length value)))
    (identity/bytes->hex value)

    (and (string? value) (re-matches #"(?i)[0-9a-f]{64}" value))
    (str/lower-case value)

    :else
    (throw (ex-info "Invalid blob inventory address"
                    {:type :netz.replication/invalid-inventory-address}))))

(defn page-addresses
  "Build one deterministic page from a bounded collection of address values."
  [addresses cursor limit]
  (when-not (and (valid-cursor? cursor) (valid-limit? limit))
    (throw (ex-info "Invalid blob inventory page request"
                    {:type :netz.replication/invalid-inventory-page
                     :cursor cursor :limit limit})))
  (let [eligible (cond->> (map address-hex addresses)
                   true distinct
                   true sort
                   cursor (drop-while #(not (pos? (compare % cursor)))))
        window (vec (take (inc limit) eligible))
        more? (> (count window) limit)
        page (if more? (pop window) window)]
    {:manifests (mapv identity/hex->bytes page)
     :next-cursor (when more? (peek page))}))

(defn inventory-page
  "Validated public wrapper around `PBlobInventoryStore/-inventory-page`."
  [store cursor limit]
  (when-not (satisfies? PBlobInventoryStore store)
    (throw (ex-info "Store does not expose blob inventory"
                    {:type :netz.replication/inventory-not-supported})))
  (when-not (and (valid-cursor? cursor) (valid-limit? limit))
    (throw (ex-info "Invalid blob inventory page request"
                    {:type :netz.replication/invalid-inventory-page
                     :cursor cursor :limit limit})))
  (-inventory-page store cursor limit))

(defn installed-manifest
  "Load the validated descriptor behind one local installed-object marker.

  This is local inventory, not network retrieval and not read authorization.
  A missing marker yields nil; malformed input fails synchronously."
  [store manifest-id]
  (when-not (satisfies? PBlobProviderStore store)
    (throw (ex-info "Store cannot provide installed blob manifests"
                    {:type :netz.replication/provider-not-supported})))
  (-load-installed-manifest
   store (identity/hex->bytes (address-hex manifest-id))))

(defn scrub-object!
  "Request a complete local integrity scrub of one installed manifest."
  [store manifest-id]
  (when-not (satisfies? PBlobInventoryStore store)
    (throw (ex-info "Store does not expose blob scrubbing"
                    {:type :netz.replication/scrub-not-supported})))
  ;; Normalize here so malformed public input fails synchronously and each
  ;; backend receives the same canonical byte representation.
  (-scrub-object! store (identity/hex->bytes (address-hex manifest-id))))
