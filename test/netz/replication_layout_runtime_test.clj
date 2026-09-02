(ns netz.replication-layout-runtime-test
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :as async :refer [<!! go <!]]
            [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.layout :as layout]
            [netz.replication.layout-runtime :as layout-runtime]
            [netz.replication.layout-storage :as layout-storage]
            [netz.replication.wire :as replication-wire]))

(def public-hex
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
(def private-hex
  "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
(def genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "aa"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "cc"))}))
(def controller (identity/authority-id-bytes genesis))
(def signer
  {:genesis genesis
   :operational {:public (identity/hex->bytes public-hex)
                 :private (identity/hex->bytes private-hex)}})
(def cluster (identity/hex->bytes (apply str (repeat 16 "10"))))
(def member (identity/hex->bytes (apply str (repeat 32 "01"))))
(def layout0
  {"v" 1 "type" "blob/layout" "cluster" cluster "epoch" 0
   "previous" (identity/hex->bytes (apply str (repeat 32 "00")))
   "members" [{"authority" member "domains" {} "enabled" true}]
   "issued_at" 1000 "signer" controller})

(defn successor [statement]
  (assoc statement "epoch" (inc (get statement "epoch"))
         "previous" (layout/address statement)
         "issued_at" (inc (get statement "issued_at"))))

(defn await [channel]
  (let [value (<!! channel)]
    (if (instance? Throwable value) (throw value) value)))

(defn sign [statement]
  (await (replication-wire/sign-statement signer statement)))

(defrecord MemoryLayoutStore [transcript gate fail?]
  layout-storage/PBlobLayoutStore
  (-load-layout-transcript [_ _cluster]
    (layout-storage/chan-of @transcript))
  (-persist-layout-transcript! [_ _cluster envelopes]
    (go
      (when gate (<! gate))
      (if @fail?
        (ex-info "layout journal failed" {})
        (do (reset! transcript envelopes) :ok)))))

(defn memory-store
  ([] (memory-store (atom nil) nil))
  ([transcript gate]
   (->MemoryLayoutStore transcript gate (atom false))))

(defn start [store]
  (layout-runtime/start! {:store store :cluster cluster
                          :controllers [controller]}))

(deftest activation-crosses-the-durable-transcript-first
  (let [gate (async/chan)
        store (memory-store (atom nil) gate)
        runtime (start store)
        envelope (sign layout0)]
    (is (true? (await (layout-runtime/ready runtime))))
    (let [result (layout-runtime/apply-envelope! runtime envelope)]
      (is (nil? (async/poll! result)))
      (is (nil? (:current (layout-runtime/state runtime))))
      (async/>!! gate true)
      (is (= :accepted (await result)))
      (is (= 0 (get-in (layout-runtime/state runtime)
                       [:current :statement "epoch"])))
      (is (= 1 (count @(:transcript store)))))
    (layout-runtime/stop! runtime)))

(deftest restart-verifies-and-replays-the-complete-chain
  (let [durable (atom nil)
        first-run (start (memory-store durable nil))
        envelope0 (sign layout0)
        envelope1 (sign (successor layout0))]
    (await (layout-runtime/ready first-run))
    (is (= :accepted (await (layout-runtime/apply-envelope!
                             first-run envelope0))))
    (is (= :accepted (await (layout-runtime/apply-envelope!
                             first-run envelope1))))
    (layout-runtime/stop! first-run)
    (let [restarted (start (memory-store durable nil))]
      (is (true? (await (layout-runtime/ready restarted))))
      (is (= 1 (get-in (layout-runtime/state restarted)
                       [:current :statement "epoch"])))
      (is (= 2 (count (layout-runtime/transcript restarted))))
      (layout-runtime/stop! restarted))))

(deftest persistence-failure-and-corrupt-restart-fail-closed
  (testing "failed persistence never activates a layout"
    (let [store (memory-store)
          _ (reset! (:fail? store) true)
          runtime (start store)]
      (await (layout-runtime/ready runtime))
      (is (= :persistence-failed
             (await (layout-runtime/apply-envelope! runtime (sign layout0)))))
      (is (nil? (:current (layout-runtime/state runtime))))
      (layout-runtime/stop! runtime)))
  (testing "tampered durable envelopes fail startup"
    (let [envelope (assoc-in (sign layout0) ["statement" "issued_at"] 1001)
          runtime (start (memory-store (atom [envelope]) nil))]
      (is (instance? Throwable (<!! (layout-runtime/ready runtime))))
      (is (= :unavailable
             (await (layout-runtime/apply-envelope! runtime (sign layout0)))))
      (layout-runtime/stop! runtime))))

(deftest an-unpersisted-valid-fork-still-freezes-the-live-process
  (let [store (memory-store)
        runtime (start store)]
    (await (layout-runtime/ready runtime))
    (is (= :accepted
           (await (layout-runtime/apply-envelope! runtime (sign layout0)))))
    (reset! (:fail? store) true)
    (is (= :persistence-failed
           (await (layout-runtime/apply-envelope!
                   runtime (sign (assoc layout0 "issued_at" 1001))))))
    (is (= 1 (count (:conflicts (layout-runtime/state runtime)))))
    (is (= :forked
           (await (layout-runtime/apply-envelope!
                   runtime (sign (successor layout0))))))
    (layout-runtime/stop! runtime)))

(deftest transcript-capacity-fails-before-persistence-or-activation
  (let [store (memory-store)
        runtime (layout-runtime/start!
                 {:store store :cluster cluster :controllers [controller]
                  :max-records 1})]
    (await (layout-runtime/ready runtime))
    (is (= :accepted
           (await (layout-runtime/apply-envelope! runtime (sign layout0)))))
    (is (= :full
           (await (layout-runtime/apply-envelope!
                   runtime (sign (successor layout0))))))
    (is (= 0 (get-in (layout-runtime/state runtime)
                     [:current :statement "epoch"])))
    (is (= 1 (count @(:transcript store))))
    (layout-runtime/stop! runtime)))

(deftest a-durable-controller-fork-survives-restart-and-freezes-the-chain
  (let [durable (atom nil)
        runtime (start (memory-store durable nil))
        fork (assoc layout0 "issued_at" 1001)
        fork2 (assoc layout0 "issued_at" 1002)]
    (await (layout-runtime/ready runtime))
    (is (= :accepted
           (await (layout-runtime/apply-envelope! runtime (sign layout0)))))
    (is (= :conflict
           (await (layout-runtime/apply-envelope! runtime (sign fork)))))
    (is (= :conflict
           (await (layout-runtime/apply-envelope! runtime (sign fork2)))))
    (is (= :forked
           (await (layout-runtime/apply-envelope!
                   runtime (sign (successor layout0))))))
    (layout-runtime/stop! runtime)
    (let [restarted (start (memory-store durable nil))]
      (is (true? (await (layout-runtime/ready restarted))))
      (is (= 2 (count (:conflicts (layout-runtime/state restarted)))))
      (is (= :forked
             (await (layout-runtime/apply-envelope!
                     restarted (sign (successor layout0))))))
      (layout-runtime/stop! restarted))))
