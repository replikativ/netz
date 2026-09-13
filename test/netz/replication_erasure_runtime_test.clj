(ns netz.replication-erasure-runtime-test
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :as async :refer [<!! go]]
            [clojure.test :refer [deftest is]]
            [netz.identity :as identity]
            [netz.replication.erasure :as erasure]
            [netz.replication.erasure-runtime :as erasure-runtime]
            [netz.replication.erasure-storage :as erasure-storage]
            [netz.replication.lifecycle-runtime :as lifecycle-runtime]
            [netz.replication.lifecycle-storage :as lifecycle-storage]
            [netz.replication.wire :as replication-wire]))

(def public-hex
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
(def private-hex
  "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")

(defn test-identity [rotation-byte revoke-byte]
  (let [genesis (identity/make-genesis
                 {:rotation-keys [(apply str (repeat 32 rotation-byte))]
                  :rotation-threshold 1
                  :operational-keys [public-hex]
                  :revocation-commit (apply str (repeat 32 revoke-byte))})]
    {:genesis genesis
     :operational {:public (identity/hex->bytes public-hex)
                   :private (identity/hex->bytes private-hex)}}))

(def controller-identity (test-identity "aa" "ab"))
(def replica-a-identity (test-identity "ba" "bb"))
(def replica-b-identity (test-identity "ca" "cb"))
(def controller
  (identity/authority-id-bytes (:genesis controller-identity)))
(def replica-a
  (identity/authority-id-bytes (:genesis replica-a-identity)))
(def replica-b
  (identity/authority-id-bytes (:genesis replica-b-identity)))
(def cluster (identity/hex->bytes (apply str (repeat 16 "10"))))
(def privacy-domain (identity/hex->bytes (apply str (repeat 16 "20"))))
(def manifest (identity/hex->bytes (apply str (repeat 32 "30"))))
(def object (identity/hex->bytes (apply str (repeat 32 "40"))))

(def deletion
  {"v" 1 "type" "blob/deletion" "cluster" cluster
   "privacy_domain" privacy-domain "revision" 4 "manifest" manifest
   "object" object "layout_epoch" 8 "issued_at" 1000
   "delete_after" 1000 "signer" controller})

(defn await [channel]
  (let [value (<!! channel)]
    (if (instance? Throwable value) (throw value) value)))

(defn signed [identity statement]
  (await (replication-wire/sign-statement identity statement)))

(defn receipt [identity replica]
  (signed identity
          {"v" 1 "type" "blob/deleted" "cluster" cluster
           "privacy_domain" privacy-domain "deletion_revision" 4
           "manifest" manifest "object" object "layout_epoch" 8
           "replica" replica "persistence" "netz/store/fsync/1"
           "deleted_at" 1100 "signer" replica}))

(defrecord MemoryErasureStore [records fail?]
  erasure-storage/PBlobErasureStore
  (-load-erasure-records [_ _cluster]
    (erasure-storage/chan-of @records))
  (-persist-erasure-record! [_ _cluster key record]
    (if @fail?
      (erasure-storage/chan-of (ex-info "unavailable" {}))
      (do (swap! records assoc key record)
          (erasure-storage/chan-of :ok)))))

(defrecord MemoryReplicaStore [records deletes]
  lifecycle-storage/PBlobLifecycleStore
  (-load-lifecycle-records [_] (lifecycle-storage/chan-of @records))
  (-persist-lifecycle! [_ key value]
    (go
      (when (= :blob/lifecycle (first key))
        (swap! records assoc (second key) value))
      :ok))
  (-delete-object! [_ key _statement persistence]
    (swap! deletes conj key)
    (lifecycle-storage/chan-of
     {:result :ok :persistence persistence :deleted-at 1100})))

(defn runtime [store dispatched progress]
  (erasure-runtime/start!
   {:cluster cluster :controllers [controller] :store store
    :dispatch-deletion! #(do (swap! dispatched conj %) true)
    :on-progress! #(swap! progress conj %)
    :scan-ms 60000 :now-fn (constantly 1000)}))

(deftest deletion-converges-only-after-every-explicit-holder-acknowledges
  (let [records (atom {})
        store (->MemoryErasureStore records (atom false))
        dispatched (atom []) progress (atom [])
        ctx (runtime store dispatched progress)
        deletion-envelope (signed controller-identity deletion)]
    (is (true? (await (erasure-runtime/ready ctx))))
    (is (true? (await (erasure-runtime/begin!
                       ctx deletion-envelope [replica-a replica-b]))))
    (is (= #{(identity/bytes->hex replica-a)
             (identity/bytes->hex replica-b)}
           (set (map :replica-id @dispatched))))
    (let [key (:key (first (erasure/statuses (erasure-runtime/state ctx))))]
      (is (= :pending (:status (erasure-runtime/status ctx key))))
      (is (true? (await (erasure-runtime/accept-receipt!
                         ctx (receipt replica-a-identity replica-a)))))
      (is (= 1 (:acknowledged (erasure-runtime/status ctx key))))
      (is (= :pending (:status (erasure-runtime/status ctx key))))
      (is (true? (await (erasure-runtime/accept-receipt!
                         ctx (receipt replica-b-identity replica-b)))))
      (is (= :complete (:status (erasure-runtime/status ctx key))))
      (is (= :complete (:status (last @progress))))
      (erasure-runtime/stop! ctx)

      (let [replayed-dispatches (atom [])
            restarted (runtime (->MemoryErasureStore records (atom false))
                               replayed-dispatches (atom []))]
        (is (true? (await (erasure-runtime/ready restarted))))
        (is (= :complete (:status (erasure-runtime/status restarted key))))
        (is (empty? @replayed-dispatches))
        (erasure-runtime/stop! restarted)))))

(deftest late-holder-reopens-a-complete-campaign
  (let [store (->MemoryErasureStore (atom {}) (atom false))
        dispatched (atom [])
        ctx (runtime store dispatched (atom []))
        deletion-envelope (signed controller-identity deletion)]
    (await (erasure-runtime/ready ctx))
    (is (true? (await (erasure-runtime/begin!
                       ctx deletion-envelope [replica-a]))))
    (is (true? (await (erasure-runtime/accept-receipt!
                       ctx (receipt replica-a-identity replica-a)))))
    (let [key (:key (first (erasure/statuses (erasure-runtime/state ctx))))]
      (is (= :complete (:status (erasure-runtime/status ctx key))))
      (reset! dispatched [])
      (is (true? (await (erasure-runtime/begin!
                         ctx deletion-envelope [replica-b]))))
      (is (= [(identity/bytes->hex replica-b)]
             (mapv :replica-id @dispatched)))
      (is (= :pending (:status (erasure-runtime/status ctx key))))
      (is (= [(identity/bytes->hex replica-b)]
             (:pending (erasure-runtime/status ctx key))))
      (is (true? (await (erasure-runtime/accept-receipt!
                         ctx (receipt replica-b-identity replica-b)))))
      (is (= :complete (:status (erasure-runtime/status ctx key)))))
    (erasure-runtime/stop! ctx)))

(deftest validly-signed-but-unrelated-receipt-is-rejected
  (let [store (->MemoryErasureStore (atom {}) (atom false))
        ctx (runtime store (atom []) (atom []))
        deletion-envelope (signed controller-identity deletion)]
    (await (erasure-runtime/ready ctx))
    (await (erasure-runtime/begin! ctx deletion-envelope [replica-a]))
    (let [unrelated (assoc (:statement
                            (replication-wire/parse-envelope
                             (receipt replica-a-identity replica-a)))
                           "object" (identity/hex->bytes
                                     (apply str (repeat 32 "ff"))))]
      (is (false? (await (erasure-runtime/accept-receipt!
                          ctx (signed replica-a-identity unrelated))))))
    (is (= 0 (:acknowledged
              (first (erasure/statuses (erasure-runtime/state ctx))))))
    (erasure-runtime/stop! ctx)))

(deftest retry-is-persisted-before-redelivery
  (let [ctx (runtime (->MemoryErasureStore (atom {}) (atom false))
                     (atom []) (atom []))
        envelope (signed controller-identity deletion)]
    (await (erasure-runtime/ready ctx))
    (await (erasure-runtime/begin! ctx envelope [replica-a]))
    (is (empty? (:actions (erasure/handler
                           (erasure-runtime/state ctx)
                           {:type :erasure/tick} {:now 30999}))))
    (let [{pending :state [persist] :actions}
          (erasure/handler (erasure-runtime/state ctx)
                           {:type :erasure/tick} {:now 31000})
          [_ key _record] persist
          {actions :actions}
          (erasure/handler pending
                           {:type :erasure/persisted :key key :result :ok}
                           {:now 31000})]
      (is (= :persist-erasure (first persist)))
      (is (= [:dispatch-deletion] (mapv first actions))))
    (erasure-runtime/stop! ctx)))

(defn wait-until [predicate]
  (loop [remaining 100]
    (cond
      (predicate) true
      (zero? remaining) false
      :else (do (<!! (async/timeout 10))
                (recur (dec remaining))))))

(deftest compliant-replicas-erase-and-return-receipts-automatically
  (let [coordinator (atom nil)
        make-replica
        (fn [identity]
          (let [store (->MemoryReplicaStore (atom {}) (atom []))
                runtime
                (lifecycle-runtime/start!
                 {:identity identity :store store :controllers [controller]
                  :on-deletion-receipt!
                  #(when-let [ctx @coordinator]
                     (erasure-runtime/accept-receipt! ctx (:envelope %)))})]
            (await (lifecycle-runtime/ready runtime))
            {:runtime runtime :store store}))
        a (make-replica replica-a-identity)
        b (make-replica replica-b-identity)
        replicas {(identity/bytes->hex replica-a) (:runtime a)
                  (identity/bytes->hex replica-b) (:runtime b)}
        ctx (erasure-runtime/start!
             {:cluster cluster :controllers [controller]
              :store (->MemoryErasureStore (atom {}) (atom false))
              :dispatch-deletion!
              (fn [{:keys [replica-id envelope]}]
                (if-let [replica-runtime (get replicas replica-id)]
                  (do (lifecycle-runtime/apply-envelope!
                       replica-runtime envelope)
                      true)
                  false))})]
    (reset! coordinator ctx)
    (is (true? (await (erasure-runtime/ready ctx))))
    (is (true? (await (erasure-runtime/begin!
                       ctx (signed controller-identity deletion)
                       [replica-a replica-b]))))
    (is (true? (wait-until
                #(= :complete
                    (:status (first (erasure/statuses
                                     (erasure-runtime/state ctx))))))))
    (is (= 1 (count @(:deletes (:store a)))))
    (is (= 1 (count @(:deletes (:store b)))))
    (is (= 2 (:acknowledged
              (first (erasure/statuses (erasure-runtime/state ctx))))))
    (erasure-runtime/stop! ctx)
    (lifecycle-runtime/stop! (:runtime a))
    (lifecycle-runtime/stop! (:runtime b))))

(deftest failed-journal-never-dispatches-or-claims-a-campaign
  (let [store (->MemoryErasureStore (atom {}) (atom true))
        dispatched (atom [])
        ctx (runtime store dispatched (atom []))]
    (is (true? (await (erasure-runtime/ready ctx))))
    (is (false? (await (erasure-runtime/begin!
                        ctx (signed controller-identity deletion)
                        [replica-a]))))
    (is (empty? @dispatched))
    (is (empty? (:records (erasure-runtime/state ctx))))
    (erasure-runtime/stop! ctx)))
