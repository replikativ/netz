(ns netz.replication.store-validation
  "Shared content-address checks for transfer repositories, independent of IO.
  These checks do not establish persistence, receipt authority or retention."
  (:require [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.wire :as wire]))

(defn- same-bytes? [a b]
  (and (wire/byte-buffer? a) (wire/byte-buffer? b)
       (identity/bufs= a b)))

(defn verify-context! [manifest-id manifest]
  (blob/validate-manifest manifest)
  (when-not (and (wire/byte-buffer? manifest-id)
                 (= 32 (identity/buf-length manifest-id))
                 (same-bytes? manifest-id (blob/manifest-address manifest)))
    (throw (ex-info "Manifest does not match its storage address"
                    {:type :netz.replication/manifest-address-mismatch})))
  manifest)

(defn expected-piece! [manifest index piece-id]
  (when-not (and (int? index)
                 (<= 0 index)
                 (< index (count (get manifest "pieces")))
                 (same-bytes? piece-id (nth (get manifest "pieces") index)))
    (throw (ex-info "Piece does not match its manifest position"
                    {:type :netz.replication/piece-address-mismatch
                     :index index})))
  piece-id)
