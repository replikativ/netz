(ns netz.replication-blob-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]))

(defn payload [n]
  (identity/seq->bytes (map #(mod (+ % 17) 251) (range n))))

(deftest portable-blob-roundtrip
  (let [bytes (payload 1000)
        {:keys [object-id manifest-id manifest pieces]}
        (blob/split bytes {:piece-size 256})
        indexed (into {} (map (juxt :index :data)) pieces)]
    (is (= 4 (count pieces)))
    (is (= 32 (identity/buf-length object-id)))
    (is (= 32 (identity/buf-length manifest-id)))
    (is (every? (fn [{:keys [index data]}]
                  (blob/verify-piece? manifest index data))
                pieces))
    (is (identity/bufs= bytes (blob/assemble manifest indexed)))))

(deftest manifest-known-answer-matches-the-python-fixture
  (let [bytes (identity/seq->bytes (range 20))
        {:keys [object-id manifest-id manifest pieces]}
        (blob/split bytes {:piece-size 8})
        held #{(:id (nth pieces 1))}]
    (is (= "c959701fbfcec8aeb4795de542de04c74d60e2736ce017c39f148c50604f53e0"
           (identity/bytes->hex object-id)))
    (is (= "b40648f31978841528e7a438f42b99de987fa22c75f2f1dca334c358dd5da23c"
           (identity/bytes->hex manifest-id)))
    (is (= ["7422949cf24dad0c8e1930e21f23f569ac5757abf3a5419e675727014d344c40"
            "0564776d59c7216f8ec6f1cb4417e7bcc2e5191b406e4fb704b1275b6c88e70f"
            "dc78eddb39d54bfc8d8afc97b4f06b317ebfbfcf7609f980119e500ef837e261"]
           (mapv #(identity/bytes->hex (:id %)) pieces)))
    (is (= "a0" (identity/bytes->hex (blob/need-bitmap manifest held))))
    (is (= [0 2] (blob/needed-indices manifest
                                      (blob/need-bitmap manifest held))))))

(deftest need-bitmap-is-compact-deterministic-and-resumable
  (let [{:keys [manifest pieces]} (blob/split (payload 2500) {:piece-size 256})
        held (set (map :id [(nth pieces 0) (nth pieces 3) (nth pieces 8)]))
        bitmap (blob/need-bitmap manifest held)]
    (is (= 2 (identity/buf-length bitmap)) "ten pieces need two bitmap bytes")
    (is (= [1 2 4 5 6 7 9] (blob/needed-indices manifest bitmap)))
    (is (= "6f40" (identity/bytes->hex bitmap))))

  (testing "unused low bits cannot smuggle indices beyond the manifest"
    (let [{:keys [manifest]} (blob/split (payload 10) {:piece-size 4})]
      (is (thrown? #?(:clj Throwable :cljs :default)
                   (blob/needed-indices manifest (identity/hex->bytes "1f")))))))

(deftest corruption-and-shape-errors-fail-closed
  (let [{:keys [manifest pieces]} (blob/split (payload 700) {:piece-size 256})
        indexed (into {} (map (juxt :index :data)) pieces)]
    (is (not (blob/verify-piece? manifest 0
                                 (identity/seq->bytes (repeat 256 0)))))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (blob/assemble manifest (dissoc indexed 1))))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (blob/validate-manifest (assoc manifest "extension" true))))))

(deftest empty-blobs-have-an-empty-manifest-and-roundtrip
  (let [empty-bytes (identity/byte-buf 0)
        {:keys [manifest pieces]} (blob/split empty-bytes)]
    (is (empty? pieces))
    (is (empty? (get manifest "pieces")))
    (is (zero? (identity/buf-length (blob/need-bitmap manifest #{}))))
    (is (identity/bufs= empty-bytes (blob/assemble manifest {})))))
