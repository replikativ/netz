(ns netz.snapshot-ref-pubsub-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [kabel.pubsub.protocol :as pubsub]
            [netz.identity :as identity]
            [netz.snapshot-ref :as ref]
            [netz.snapshot-ref.pubsub :as ref-pubsub]))

(defn- utf8 [value] (identity/utf8-bytes value))

(def stream
  {:profile "datahike/commit-v1"
   :system (utf8 "store-a")
   :ref (utf8 "main")})

(defn- signed [authority snapshot parents]
  (<!! (ref/sign authority
                 (assoc stream
                        :snapshot (utf8 snapshot)
                        :parents (mapv utf8 parents)))))

(defn- install! [state statement]
  (let [[next-state outcome] (<!! (ref/accept-verified @state statement))]
    (reset! state next-state)
    (is (= :accepted outcome))))

(deftest kabel-strategy-catches-up-and-then-applies-live-references
  (let [authority (<!! (identity/generate-identity))
        publisher (identity/authority-id (:genesis authority))
        base (signed authority "base" [])
        child (signed authority "child" ["base"])
        live (signed authority "live" ["child"])
        sender-state (atom (ref/make-state))
        receiver-state (atom (ref/make-state))]
    (install! sender-state base)
    (install! sender-state child)
    (let [sender (ref-pubsub/strategy
                  (assoc stream :state sender-state :publisher publisher))
          receiver (ref-pubsub/strategy
                    (assoc stream :state receiver-state :publisher publisher))
          summary (<!! (pubsub/-init-client-state receiver))
          items (<!! (pubsub/-handshake-items sender summary))]
      (is (= [] summary))
      (is (= 2 (count items)))
      (doseq [item items]
        (is (true? (:ok (<!! (pubsub/-apply-handshake-item receiver item))))))
      (is (= (ref/heads @sender-state (:profile stream) (:system stream) (:ref stream))
             (ref/heads @receiver-state (:profile stream) (:system stream)
                        (:ref stream))))
      (testing "the same verified state transition handles the live path"
        (is (true? (:ok (<!! (pubsub/-apply-publish receiver (ref/encode live))))))
        (is (= #{(ref/snapshot-key live)}
               (ref/heads @receiver-state (:profile stream) (:system stream)
                          (:ref stream))))))

    (testing "catch-up refuses an unbounded snapshot in one handshake"
      (let [limited (ref-pubsub/strategy
                     (assoc stream :state sender-state :publisher publisher
                            :max-handshake-items 1))]
        (is (instance? Exception
                       (<!! (pubsub/-handshake-items limited []))))))))

(deftest kabel-strategy-rejects-a-valid-statement-for-another-stream
  (let [authority (<!! (identity/generate-identity))
        publisher (identity/authority-id (:genesis authority))
        strategy (ref-pubsub/strategy
                  (assoc stream :publisher publisher))
        other (<!! (ref/sign authority
                             {:profile (:profile stream)
                              :system (:system stream)
                              :ref (utf8 "other")
                              :snapshot (utf8 "c0")
                              :parents []}))]
    (is (= {:ok false :error :wrong-stream}
           (<!! (pubsub/-apply-publish strategy (ref/encode other)))))))
