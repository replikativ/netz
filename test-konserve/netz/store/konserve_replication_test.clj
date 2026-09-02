(ns netz.store.konserve-replication-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [netz.store.konserve-replication :as durable]
            [netz.store.protocol :as peer-store]))

(deftest local-failure-prevents-network-replication
  (let [store (<!! (new-mem-store))
        cause (ex-info "disk full" {:type :test/disk-full})
        calls (atom [])
        result
        (with-redefs [k/assoc (fn [& _] (peer-store/chan-of cause))]
          (<!! (durable/assoc-immutable!
                store #(do (swap! calls conj %) (peer-store/chan-of :never))
                :node 42)))]
    (is (identical? cause result))
    (is (empty? @calls))))

(deftest thrown-replicator-failure-is-delivered-on-the-result-channel
  (let [store (<!! (new-mem-store))
        cause (ex-info "outbox failed" {:type :test/outbox-failed})
        result (<!! (durable/assoc-immutable!
                     store (fn [_] (throw cause)) :node 42))]
    (is (identical? cause result))
    (is (= 42 (<!! (k/get store :node nil {:sync? false}))))))

(deftest immutable-write-awaits-local-commit-and-replica-receipt
  (let [store (<!! (new-mem-store))
        calls (atom [])
        replicate! (fn [request]
                     (swap! calls conj request)
                     (peer-store/chan-of
                      {:status :quorum :durable-acks ["alice" "bob"]}))
        result (<!! (durable/assoc-immutable!
                     store replicate! :node {:payload 1} {:privacy :friends}))]
    (is (= {:payload 1} (<!! (k/get store :node nil {:sync? false}))))
    (is (true? (:immutable? (<!! (k/get-meta store :node nil
                                              {:sync? false})))))
    (is (= {:privacy :friends :immutable? true} (:meta (first @calls))))
    (is (= :quorum (get-in result [:receipt :status])))
    (is (= [nil {:payload 1}] (:local-result result)))))

(deftest quorum-failure-still-reports-the-local-fact
  (testing "a remote failure cannot rewrite a successful local commit"
    (let [store (<!! (new-mem-store))
          replicate! (fn [_]
                       (peer-store/chan-of
                        {:error :netz.replication/quorum-unavailable
                         :local-committed? true
                         :durable-replicas 1
                         :required 2}))
          result (<!! (durable/assoc-immutable! store replicate! :node 42))]
      (is (= 42 (<!! (k/get store :node nil {:sync? false}))))
      (is (true? (get-in result [:receipt :local-committed?])))
      (is (= :netz.replication/quorum-unavailable
             (get-in result [:receipt :error]))))))
