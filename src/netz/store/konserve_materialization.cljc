(ns netz.store.konserve-materialization
  "Fail-closed binary installer for explicit Netz materialization into Konserve.

  This optional adapter is intentionally narrower than a Konserve wrapper. The
  application configures one logical store ID, an allowlist of codec IDs, and a
  decoder from the authenticated opaque application key to its local Konserve
  key. Existing different bytes are never overwritten."
  (:require [konserve.binary :as binary]
            [konserve.core :as k]
            [netz.identity :as identity]
            [netz.wire :as wire]
            #?(:clj [clojure.core.async :refer [chan go <! put!]]
               :cljs [clojure.core.async :refer [chan <! put!]
                      :refer-macros [go]])))

(defn- failure? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn- read-bytes! [store key]
  (k/bget store key (binary/to-bytes) {:sync? false}))

(defn binary-installer
  "Return an installer callback for `netz.replication.materialization`.

  Options require `:store-id` and `:decode-key`, a function from the installer
  request to a local Konserve key. The codec allowlist defaults to only the
  `bytes/identity` codec. The callback checks the authenticated logical store and
  codec, refuses nil keys, treats identical existing bytes as idempotent, and
  rejects a conflicting immutable value. A new binary value is read back and
  compared before `:committed` is returned. Route every write to these keys
  through the same installer instance. Its read/check/write gate is local to
  the callback, and Konserve binary writes provide no conditional create."
  [store {:keys [store-id decode-key codecs]
          :or {codecs #{"bytes/identity"}}}]
  (when-not (and store
                 (string? store-id)
                 (not-empty store-id)
                 (ifn? decode-key)
                 (set? codecs)
                 (seq codecs)
                 (every? string? codecs))
    (throw (ex-info "Invalid Konserve materializer options"
                    {:type :netz.replication/invalid-konserve-materializer})))
  (let [gate (chan 1)]
    (put! gate true)
    (fn [{request-store-id :store-id codec :codec bytes :bytes :as request}]
      (go
        (<! gate)
        (try
          (try
            (when-not (= store-id request-store-id)
              (throw (ex-info "Application binding targets another logical store"
                              {:type :netz.replication/materialization-store-mismatch
                               :expected store-id :actual request-store-id})))
            (when-not (contains? codecs codec)
              (throw (ex-info "Application binding uses an unsupported codec"
                              {:type :netz.replication/unsupported-materialization-codec
                               :codec codec})))
            (when-not (wire/byte-buffer? bytes)
              (throw (ex-info "Materializer requires verified byte input"
                              {:type :netz.replication/materialization-not-bytes})))
            (let [key (decode-key request)]
              (when (nil? key)
                (throw (ex-info "Application key decoder returned nil"
                                {:type
                                 :netz.replication/invalid-application-key})))
              (let [existing (<! (read-bytes! store key))]
                (when (failure? existing) (throw existing))
                (if existing
                  (if (identity/bufs= existing bytes)
                    {:result :already-present :key key}
                    (throw
                     (ex-info "Immutable application key already has other bytes"
                              {:type :netz.replication/application-key-conflict
                               :key key})))
                  (let [stored (<! (k/bassoc store key bytes {:sync? false}))]
                    (when (failure? stored) (throw stored))
                    (let [verified (<! (read-bytes! store key))]
                      (when (failure? verified) (throw verified))
                      (when-not (and verified (identity/bufs= verified bytes))
                        (throw
                         (ex-info
                          "Konserve bytes differ after materialization"
                          {:type
                           :netz.replication/materialization-verification-failed
                           :key key})))
                      {:result :committed :key key})))))
            (catch #?(:clj Throwable :cljs :default) cause cause))
          (finally (put! gate true)))))))
