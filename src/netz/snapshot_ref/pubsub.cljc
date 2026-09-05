(ns netz.snapshot-ref.pubsub
  "Kabel snapshot/live strategy for signed snapshot-reference streams."
  (:require [clojure.core.async :as async :refer [chan close! put!]]
            [kabel.pubsub.protocol :as pubsub]
            [netz.identity :as identity]
            [netz.snapshot-ref :as ref]))

(def ^:const max-summary-heads 1024)
(def ^:const default-max-handshake-items 4096)

(defn- value-channel [value]
  (let [ch (chan 1)]
    (put! ch value)
    (close! ch)
    ch))

(defn- expected-stream? [{:keys [profile system ref]} statement]
  (= [profile
      (identity/bytes->hex system)
      (identity/bytes->hex ref)]
     (ref/stream-key statement)))

(defn- accept-bytes!
  [state expected payload]
  (let [ch (chan 1)]
    (async/go
      (try
        (let [statement (ref/decode payload)]
          (if-not (expected-stream? expected statement)
            (put! ch {:ok false :error :wrong-stream})
            (if-not (async/<! (ref/verify statement))
              (put! ch {:ok false :error :invalid-signature})
              (let [outcome (volatile! nil)
                    after (swap! state
                                 (fn [current]
                                   (let [[next-state result]
                                         (ref/accept current statement)]
                                     (vreset! outcome result)
                                     next-state)))]
                (put! ch (if (#{:accepted :duplicate} @outcome)
                           {:ok true :outcome @outcome :state after}
                           {:ok false :error @outcome}))))))
        (catch #?(:clj Throwable :cljs :default) error
          (put! ch {:ok false :error error})))
      (close! ch))
    ch))

(defn- head-id? [value]
  (and (string? value)
       (<= 2 (count value) (* 2 ref/max-id-bytes))
       (even? (count value))
       (boolean (re-matches #"(?i)[0-9a-f]+" value))))

(defrecord SnapshotRefStrategy [state expected max-handshake-items]
  pubsub/PSyncStrategy
  (-init-client-state [_]
    (let [local-heads (vec (sort (ref/heads @state
                                            (:profile expected)
                                            (:system expected)
                                            (:ref expected))))]
      (value-channel
       (if (<= (count local-heads) max-summary-heads)
         local-heads
         (ex-info "snapshot-reference head summary exceeds limit"
                  {:type :netz.snapshot-ref/summary-limit
                   :heads (count local-heads)})))))
  (-handshake-items [_ remote-heads]
    (try
      (when-not (and (vector? remote-heads)
                     (<= (count remote-heads) max-summary-heads)
                     (every? head-id? remote-heads))
        (throw (ex-info "invalid snapshot-reference head summary"
                        {:type :netz.snapshot-ref/invalid-summary})))
      (let [statements (ref/statements-since @state
                                             (:profile expected)
                                             (:system expected)
                                             (:ref expected)
                                             (set remote-heads))]
        (when (> (count statements) max-handshake-items)
          (throw (ex-info "snapshot-reference catch-up exceeds item limit"
                          {:type :netz.snapshot-ref/handshake-limit
                           :items (count statements)})))
        (value-channel (mapv ref/encode statements)))
      (catch #?(:clj Throwable :cljs :default) error
        (value-channel error))))
  (-apply-handshake-item [_ payload]
    (accept-bytes! state expected payload))
  (-apply-publish [_ payload]
    (accept-bytes! state expected payload)))

(defn strategy
  "Create a `PSyncStrategy` for exactly one `(profile, system, ref)` stream.

  `state` is an atom containing `netz.snapshot-ref/make-state`. If `publisher`
  is supplied, it is pinned before any network value is accepted. The strategy
  verifies every handshake and live item; accepted and duplicate deliveries
  return Kabel's exact `{:ok true}` shape, while conflicts and capacity errors
  fail the application lane visibly."
  [{:keys [state profile system ref publisher max-handshake-items]
    :or {state (atom (ref/make-state))
         max-handshake-items default-max-handshake-items}}]
  (when publisher
    (swap! state ref/pin profile system ref publisher))
  (->SnapshotRefStrategy state {:profile profile :system system :ref ref}
                         max-handshake-items))
