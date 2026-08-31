(ns netz.roots-test
  (:require [clojure.test :refer [deftest testing is]]
            #?(:clj [clojure.core.async :refer [<!!]])
            [netz.identity :as identity]
            [netz.roots :as roots]
            [netz.topics :as tp]))

(defn- rec [v root prev]
  (roots/make-record {:database "db" :version v :root root :prev prev
                      :publisher :alice}))

(defn- accept-all
  "Feed records in order, returning [state outcomes]."
  [state records]
  (reduce (fn [[st outs] r]
            (let [[st' o] (roots/accept st r)]
              [st' (conj outs o)]))
          [state []]
          records))

(deftest portable-root-signing-known-answer
  (let [publisher "dadc6166b57499e9a4934c894f012a4acd29d851521f5be9dc04def7937467ca"
        record (roots/make-record
                {:database "example/db"
                 :version 7
                 :root (identity/hex->bytes (apply str (repeat 32 "11")))
                 :prev (identity/hex->bytes (apply str (repeat 32 "22")))
                 :publisher publisher})]
    (is (= "6e65747a2f726f6f742d7265636f72642f763100a661760164707265765820222222222222222222222222222222222222222222222222222222222222222264726f6f74582011111111111111111111111111111111111111111111111111111111111111116776657273696f6e076864617461626173656a6578616d706c652f6462697075626c69736865725820dadc6166b57499e9a4934c894f012a4acd29d851521f5be9dc04def7937467ca"
           (identity/bytes->hex (roots/record-signing-bytes record))))))

;; =============================================================================
;; The chain
;; =============================================================================

(deftest a-valid-chain-advances-the-head
  (let [[st outs] (accept-all (roots/make-state)
                              [(rec 0 "r0" nil)
                               (rec 1 "r1" "r0")
                               (rec 2 "r2" "r1")])]
    (is (= [:first :accepted :accepted] outs))
    (is (= {:version 2 :root "r2" :publisher :alice} (roots/head st "db")))

    (testing "the verifier holds ONE entry, not a history"
      ;; This is the whole saving: atproto's relay went from 16 TB to ~21 GB by
      ;; keeping one hash per producer rather than archiving repositories.
      (is (= 1 (count (:heads st)))))))

(deftest a-replayed-root-is-refused
  (testing "an old signed root is still a valid signature, and must not win"
    ;; The failure a signature cannot catch: a provider serving version 1 when
    ;; version 3 exists forges nothing. Only monotone pinning detects it, and
    ;; it is local — no clock, no quorum, no cooperation from anyone.
    (let [[st _] (accept-all (roots/make-state)
                             [(rec 0 "r0" nil) (rec 1 "r1" "r0") (rec 2 "r2" "r1")])
          [st' o] (roots/accept st (rec 1 "r1" "r0"))]
      (is (= :stale o))
      (is (= 2 (:version (roots/head st' "db"))) "the head went backwards"))))

(deftest a-broken-chain-is-refused
  (testing "the right version with the wrong predecessor does not join up"
    (let [[st _] (accept-all (roots/make-state) [(rec 0 "r0" nil)])
          [st' o] (roots/accept st (rec 1 "r1" "SOMETHING-ELSE"))]
      (is (= :not-successor o))
      (is (= 0 (:version (roots/head st' "db")))))))

(deftest a-gap-is-named-rather-than-papered-over
  (testing "too far ahead to verify inductively"
    ;; Not fatal — it is a fetch. But a verifier that silently accepted across
    ;; a gap would have abandoned the property the chain exists to provide,
    ;; and this is exactly the hole non-archival relays created for atproto.
    (let [[st _] (accept-all (roots/make-state) [(rec 0 "r0" nil)])
          [st' o] (roots/accept st (rec 5 "r5" "r4"))]
      (is (= :gap o))
      (is (= 0 (:version (roots/head st' "db"))))

      (testing "and the missing range is reported so it can be closed"
        (is (= {:database "db" :from 1 :to 4}
               (roots/missing-versions st' "db" 5))))))

  (testing "no gap when the next version is in hand"
    (let [[st _] (accept-all (roots/make-state) [(rec 0 "r0" nil)])]
      (is (nil? (roots/missing-versions st "db" 1))))))

;; =============================================================================
;; Equivocation
;; =============================================================================

(deftest two-roots-at-one-version-are-evidence
  (testing "a fork is retained as proof, not arbitrated"
    ;; did:plc resolves this with a Postgres row lock. We have no serialiser,
    ;; and pretending otherwise would mean picking a winner by luck of arrival.
    (let [[st _] (accept-all (roots/make-state) [(rec 0 "r0" nil) (rec 1 "r1" "r0")])
          [st' o] (roots/accept st (rec 1 "DIFFERENT" "r0"))]
      (is (= :fork o))
      (is (roots/compromised? st' "db"))
      (is (= 1 (count (get-in st' [:equivocations "db"])))
          "the proof was not retained")))

  (testing "compromise is absorbing — later records cannot clear it"
    ;; The rule two independent reviews converged on: highest-version-wins
    ;; loses to an attacker who simply keeps incrementing.
    (let [[st _] (accept-all (roots/make-state) [(rec 0 "r0" nil) (rec 1 "r1" "r0")])
          [st _] (roots/accept st (rec 1 "DIFFERENT" "r0"))
          [st' o] (roots/accept st (rec 2 "r2" "r1"))]
      (is (= :fork o))
      (is (roots/compromised? st' "db"))
      (is (= 1 (:version (roots/head st' "db")))
          "the head advanced after a proven compromise")))

  (testing "the same root at the same version is merely stale, not a fork"
    ;; A duplicate delivery must not be mistaken for equivocation.
    (let [[st _] (accept-all (roots/make-state) [(rec 0 "r0" nil) (rec 1 "r1" "r0")])
          [st' o] (roots/accept st (rec 1 "r1" "r0"))]
      (is (= :stale o))
      (is (not (roots/compromised? st' "db")))))

  (testing "equivocation storage is bounded"
    (let [st (reduce (fn [st i]
                       (let [db (str "db" i)
                             mk (fn [v r p] (roots/make-record
                                             {:database db :version v :root r
                                              :prev p :publisher :alice}))
                             [st _] (roots/accept st (mk 0 "a" nil))
                             [st _] (roots/accept st (mk 0 "b" nil))]
                         st))
                     (roots/make-state {:max-equivocations 5})
                     (range 40))]
      (is (<= (count (:equivocations st)) 6)))))

;; =============================================================================
;; Publisher
;; =============================================================================

(deftest only-the-owner-may-advance-a-database
  (testing "a different key is refused even with a well-formed chain"
    (let [[st _] (accept-all (roots/make-state) [(rec 0 "r0" nil)])
          [st' o] (roots/accept st (roots/make-record
                                    {:database "db" :version 1 :root "r1"
                                     :prev "r0" :publisher :mallory}))]
      (is (= :wrong-publisher o))
      (is (= :alice (:publisher (roots/head st' "db"))))))

  (testing "pinning removes the trust-on-first-use window"
    ;; A caller who already knows the owner should say so, and then no first
    ;; record can establish somebody else.
    (let [st (roots/pin (roots/make-state) "db" :alice)
          [st' o] (roots/accept st (roots/make-record
                                    {:database "db" :version 0 :root "r0"
                                     :prev nil :publisher :mallory}))]
      (is (= :wrong-publisher o))
      (is (nil? (:root (roots/head st' "db"))))))

  (testing "and a pinned owner still starts from version 0"
    (let [st (roots/pin (roots/make-state) "db" :alice)
          [st' o] (roots/accept st (rec 0 "r0" nil))]
      (is (= :accepted o))
      (is (= {:version 0 :root "r0" :publisher :alice} (roots/head st' "db")))))

  (testing "first use can be refused outright"
    (let [[st o] (roots/accept (roots/make-state {:trust-on-first-use? false})
                               (rec 0 "r0" nil))]
      (is (= :wrong-publisher o))
      (is (nil? (roots/head st "db"))))))

(deftest malformed-records-are-refused
  (doseq [bad [{} nil "a string"
               {:netz/kind "netz/root/v1"}
               (assoc (rec 0 "r0" nil) :netz/version -1)
               (dissoc (rec 0 "r0" nil) :netz/root)]]
    (let [[_ o] (roots/accept (roots/make-state) bad)]
      (is (= :malformed o) (str "accepted " (pr-str bad))))))

#?(:clj
   (deftest portable-root-records-bind-the-publisher-signature
     (let [identity (<!! (identity/generate-identity))
           root (identity/hex->bytes (apply str (repeat 32 "11")))
           signed (<!! (roots/sign-record identity
                                          {:database "db"
                                           :version 0
                                           :root root
                                           :prev nil}))]
       (is (true? (<!! (roots/verify-record signed))))
       (let [[state outcome] (<!! (roots/accept-verified
                                   (roots/make-state) signed))]
         (is (= :first outcome))
         (is (= (identity/authority-id (:genesis identity))
                (:publisher (roots/head state "db")))))

       (testing "all chain fields are covered"
         (doseq [tampered [(assoc signed :netz/version 1)
                           (assoc signed :netz/root
                                  (identity/hex->bytes (apply str (repeat 32 "22"))))
                           (assoc signed :netz/database "other")]]
           (is (false? (<!! (roots/verify-record tampered))))))

       (testing "an invalid record cannot advance the pure verifier"
         (let [[state outcome]
               (<!! (roots/accept-verified
                     (roots/make-state)
                     (assoc signed :netz/signature (identity/byte-buf 64))))]
           (is (= :invalid-signature outcome))
           (is (nil? (roots/head state "db")))
           (is (= 1 (get-in state [:stats :invalid-signature]))))))))

;; =============================================================================
;; Topics
;; =============================================================================

(deftest roots-live-under-a-relayable-path
  (testing "a relay can carry one publisher's databases without the network"
    (is (= [:netz/roots :alice "db"] (roots/topic-for :alice "db")))
    (is (roots/covers-database? #{[:netz/roots :alice]} :alice "db"))
    (is (roots/covers-database? #{[:netz/roots]} :alice "db"))
    (is (roots/covers-database? #{tp/everything} :alice "db"))
    (is (not (roots/covers-database? #{[:netz/roots :bob]} :alice "db")))
    (is (not (roots/covers-database? #{[:rooms]} :alice "db")))))
