(ns netz.snapshot-ref-test
  (:require [clojure.test :refer [deftest is testing]]
            #?(:clj [clojure.core.async :refer [<!!]])
            [netz.identity :as identity]
            [netz.snapshot-ref :as ref]))

(defn- utf8 [text] (identity/utf8-bytes text))
(def alice (apply str (repeat 64 "a")))
(def mallory (apply str (repeat 64 "b")))

(defn- statement
  ([snapshot parents] (statement snapshot parents alice))
  ([snapshot parents publisher]
   (ref/make-statement
    {:profile "datahike/commit-v1"
     :system (utf8 "store-a")
     :ref (utf8 "main")
     :snapshot (utf8 snapshot)
     :parents (mapv utf8 parents)
     :publisher publisher})))

(defn- accept-all [state statements]
  (reduce (fn [[state outcomes] item]
            (let [[state outcome] (ref/accept state item)]
              [state (conj outcomes outcome)]))
          [state []]
          statements))

(defn- permutations [xs]
  (if (empty? xs)
    [[]]
    (mapcat (fn [x] (map #(cons x %) (permutations (remove #{x} xs)))) xs)))

(deftest late-ancestry-bridges-prune-already-known-ancestor-heads
  ;; A and C can both be heads until B arrives and reveals A <- B <- C.
  ;; B is itself covered by C, but its arrival must still retire A.
  (doseq [parents [[[] ["a"] ["b"] ["c"]]
                   [[] ["a"] ["a"] ["b" "c"]]]
          order (permutations (range 4))]
    (let [items (mapv statement ["a" "b" "c" "d"] parents)
          [state outcomes] (accept-all (ref/make-state) (mapv items order))]
      (is (every? #{:accepted} outcomes))
      (is (= #{"64"} (ref/heads state "datahike/commit-v1" (utf8 "store-a") (utf8 "main")))
          (str "delivery order " order " parents " parents)))))

(deftest parents-are-canonical-set-semantics
  (let [item (statement "c" ["b" "a" "b"])]
    (is (= ["61" "62"]
           (mapv identity/bytes->hex (:netz/parents item))))
    (is (ref/statement? item))))

(deftest signing-bytes-match-the-python-reference
  (let [item (ref/make-statement
              {:profile "datahike/commit-v1"
               :system (utf8 "store-a")
               :ref (utf8 "main")
               :snapshot (utf8 "child")
               :parents [(utf8 "parent-b") (utf8 "parent-a")
                         (utf8 "parent-b")]
               :publisher (apply str (repeat 64 "a"))})]
    (is (= (str "6e65747a2f736e617073686f742d7265662f763100"
                "a861760163726566446d61696e64747970656c736e617073686f742d726566"
                "6673797374656d4773746f72652d6167706172656e74738248706172656e742d61"
                "48706172656e742d626770726f66696c65726461746168696b652f636f6d6d6974"
                "2d763168736e617073686f74456368696c64697075626c69736865725820"
                (apply str (repeat 64 "a")))
           (identity/bytes->hex (ref/signing-bytes item))))))

(deftest out-of-order-delivery-does-not-regress-the-head
  (let [child (statement "c1" ["c0"])
        parent (statement "c0" [])
        [[state outcomes]] [(accept-all (ref/make-state) [child parent])]]
    (is (= [:accepted :accepted] outcomes))
    (is (= #{"6331"}
           (ref/heads state "datahike/commit-v1" (utf8 "store-a")
                      (utf8 "main"))))
    (is (empty? (ref/missing-parents state "datahike/commit-v1"
                                     (utf8 "store-a") (utf8 "main"))))))

(deftest missing-parents-and-concurrent-heads-are-explicit
  (let [[state outcomes]
        (accept-all (ref/make-state)
                    [(statement "left" ["base"])
                     (statement "right" ["base"])])]
    (is (= [:accepted :accepted] outcomes))
    (is (= #{"62617365"}
           (ref/missing-parents state "datahike/commit-v1"
                                (utf8 "store-a") (utf8 "main"))))
    (is (= #{"6c656674" "7269676874"}
           (ref/heads state "datahike/commit-v1" (utf8 "store-a")
                      (utf8 "main"))))
    (is (= #{"6c656674" "7269676874"}
           (set (map ref/snapshot-key
                     (ref/statements-since
                      state "datahike/commit-v1" (utf8 "store-a")
                      (utf8 "main") #{})))))))

(deftest merge-collapses-known-concurrent-heads
  (let [[state _]
        (accept-all (ref/make-state)
                    [(statement "base" [])
                     (statement "left" ["base"])
                     (statement "right" ["base"])
                     (statement "merge" ["left" "right"])])]
    (is (= #{"6d65726765"}
           (ref/heads state "datahike/commit-v1" (utf8 "store-a")
                      (utf8 "main"))))))

(deftest duplicate-is-idempotent-but-redefined-snapshot-is-a-conflict
  (let [first (statement "same" ["a"])
        duplicate (statement "same" ["a"])
        conflicting (statement "same" ["b"])
        [state outcomes] (accept-all (ref/make-state)
                                     [first duplicate conflicting])]
    (is (= [:accepted :duplicate :conflict] outcomes))
    (is (= 1 (count (get-in state [:conflicts (ref/stream-key first)]))))))

(deftest cycles-and-capacity-are-rejected
  (let [[state _] (ref/accept (ref/make-state) (statement "child" ["parent"]))
        [_ outcome] (ref/accept state (statement "parent" ["child"]))]
    (is (= :cycle outcome)))
  (let [[state _] (ref/accept
                   (ref/make-state {:max-nodes-per-stream 1})
                   (statement "one" []))
        [state outcome] (ref/accept state (statement "two" []))]
    (is (= :capacity outcome))
    (is (= #{"6f6e65"}
           (ref/heads state "datahike/commit-v1" (utf8 "store-a")
                      (utf8 "main"))))))

(deftest publisher-can-be-pinned-before-first-contact
  (let [base (ref/make-state)
        state (ref/pin base "datahike/commit-v1" (utf8 "store-a")
                       (utf8 "main") alice)
        [_ outcome] (ref/accept state (statement "c0" [] mallory))]
    (is (= :wrong-publisher outcome))))

(deftest durable-dag-events-have-an-explicit-portable-boundary
  (let [fields (ref/dag-event-fields
                "datahike/commit-v1"
                {:system-id "store-a"
                 :snapshot-id "550e8400-e29b-41d4-a716-446655440000"
                 :parent-ids #{"parent-b" "parent-a"}
                 :ref "main"
                 :durable? true})]
    (is (= "73746f72652d61" (identity/bytes->hex (:system fields))))
    (is (= #{"706172656e742d61" "706172656e742d62"}
           (set (map identity/bytes->hex (:parents fields))))))
  (is (thrown-with-msg?
       #?(:clj clojure.lang.ExceptionInfo :cljs js/Error) #"non-durable"
       (ref/dag-event-fields
        "datahike/commit-v1"
        {:system-id "store-a" :snapshot-id "c1" :parent-ids #{}
         :ref "main" :durable? false}))))

#?(:clj
   (deftest signed-wire-round-trip-and-tamper-rejection
     (let [authority (<!! (identity/generate-identity))
           signed (<!! (ref/sign authority
                                 {:profile "datahike/commit-v1"
                                  :system (utf8 "store-a")
                                  :ref (utf8 "main")
                                  :snapshot (utf8 "c1")
                                  :parents [(utf8 "c0")]
                                  :ordering (utf8 "urd-fence-placeholder")}))
           decoded (ref/decode (ref/encode signed))]
       (is (true? (<!! (ref/verify decoded))))
       (is (= "6331" (identity/bytes->hex (:netz/snapshot decoded))))
       (is (= :accepted
              (second (<!! (ref/accept-verified (ref/make-state) decoded)))))
       (testing "the signature binds causality"
         (is (false? (<!! (ref/verify
                           (assoc decoded :netz/parents [(utf8 "other")])))))))))
