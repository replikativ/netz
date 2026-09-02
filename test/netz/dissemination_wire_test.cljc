(ns netz.dissemination-wire-test
  (:require [clojure.test :refer [deftest is testing]]
            #?(:clj [clojure.data.json :as json])
            [netz.dissemination-wire :as dissemination-wire]
            [netz.identity :as identity]
            [netz.publication-test :as publication-test]
            [netz.wire :as wire]))

(def other-authority
  "0101010101010101010101010101010101010101010101010101010101010101")

(def interests-hex
  "a5617601647479706569696e7465726573747366746f70696373826864622f726f6f74736a64656d6f2f7461736b736763617272696573816a64656d6f2f7461736b736972656c61795f616c6cf5")
(def have-hex
  "a3617601647479706564686176656773747265616d7382a56565706f63680165666c6f6f720965746f7069636864622f726f6f7473666f726967696e582001010101010101010101010101010101010101010101010101010101010101016672616e6765738182090ca56565706f63680365666c6f6f720165746f7069636a64656d6f2f7461736b73666f726967696e5820dadc6166b57499e9a4934c894f012a4acd29d851521f5be9dc04def7937467ca6672616e67657382820002820505")
(def want-hex
  "a361760164747970656477616e746672616e67657382a56268690d626c6f0d6565706f63680165746f7069636864622f726f6f7473666f726967696e58200101010101010101010101010101010101010101010101010101010101010101a562686904626c6f036565706f63680365746f7069636a64656d6f2f7461736b73666f726967696e5820dadc6166b57499e9a4934c894f012a4acd29d851521f5be9dc04def7937467ca")

(def interests
  {:type :interests
   :topics #{"demo/tasks" "db/roots"}
   :carries #{[] ["demo/tasks"]}})

(def have
  {:type :have
   :summary {publication-test/authority-hex
             {3 {"demo/tasks" [[0 2] [5 5]]}}
             other-authority
             {1 {"db/roots" [[9 12]]}}}
   :horizon {[publication-test/authority-hex 3 "demo/tasks"] 1
             [other-authority 1 "db/roots"] 9}})

(def want
  {:type :want
   :ranges [{:origin other-authority :epoch 1
             :topic "db/roots" :lo 13 :hi 13}
            {:origin publication-test/authority-hex :epoch 3
             :topic "demo/tasks" :lo 3 :hi 4}]})

(deftest portable-dissemination-round-trip
  #?(:clj
     (testing "the JVM reads the independently consumed shared fixture"
       (let [fixture (json/read-str
                      (slurp "spec/fixtures/dissemination-control-1.json"))]
         (is (= dissemination-wire/capability (get fixture "profile")))
         (is (= interests-hex (get fixture "interests_cbor_hex")))
         (is (= have-hex (get fixture "have_cbor_hex")))
         (is (= want-hex (get fixture "want_cbor_hex"))))))

  (testing "interest sets have a canonical array representation"
    (is (= interests-hex
           (identity/bytes->hex (dissemination-wire/encode-frame interests))))
    (is (= interests
           (dissemination-wire/decode-frame
            (dissemination-wire/encode-frame interests)))))

  (testing "repair summaries use full Authority IDs and flattened streams"
    (is (= have-hex
           (identity/bytes->hex (dissemination-wire/encode-frame have))))
    (is (= have
           (dissemination-wire/decode-frame
            (dissemination-wire/encode-frame have)))))

  (testing "want ranges use the same full stream identity"
    (is (= want-hex
           (identity/bytes->hex (dissemination-wire/encode-frame want))))
    (is (= want
           (dissemination-wire/decode-frame
            (dissemination-wire/encode-frame want)))))

  (testing "the signed publication envelope is the gossip frame"
    (let [decoded (dissemination-wire/decode-frame
                   (dissemination-wire/encode-frame publication-test/signed))]
      (is (= (dissoc publication-test/signed :payload)
             (dissoc decoded :payload)))
      (is (identity/bufs= (:payload publication-test/signed)
                          (:payload decoded))))))

(deftest portable-dissemination-is-closed-and-canonical
  (let [encoded (dissemination-wire/encode-frame interests)
        frame (wire/decode encoded)]
    (testing "unknown fields and duplicate or unsorted arrays are rejected"
      (doseq [invalid [(assoc frame "extension" true)
                       (assoc frame "topics" ["demo/tasks" "db/roots"])
                       (assoc frame "topics" ["db/roots" "db/roots"])]]
        (is (thrown? #?(:clj Throwable :cljs :default)
                     (dissemination-wire/frame-from-wire invalid)))))

    (testing "native-only topic shapes do not leak across the boundary"
      (is (thrown? #?(:clj Throwable :cljs :default)
                   (dissemination-wire/frame->wire
                    {:type :interests :topics #{:db/roots} :carries #{}})))
      (is (thrown? #?(:clj Throwable :cljs :default)
                   (dissemination-wire/frame->wire
                    {:type :interests :topics #{"db/roots"}
                     :carries #{[:db "alice"]}}))))

    (testing "interval sets and want ranges are bounded canonical data"
      (is (thrown? #?(:clj Throwable :cljs :default)
                   (dissemination-wire/frame-from-wire
                    {"v" 1 "type" "have"
                     "streams" [{"origin" (identity/hex->bytes other-authority)
                                 "epoch" 0 "topic" "db/roots"
                                 "ranges" [[0 1] [2 3]] "floor" 0}]})))
      (is (thrown? #?(:clj Throwable :cljs :default)
                   (dissemination-wire/frame-from-wire
                    {"v" 1 "type" "want"
                     "ranges" [{"origin" (identity/hex->bytes other-authority)
                                "epoch" 0 "topic" "db/roots"
                                "lo" 4 "hi" 3}]}))))))

(deftest portable-dissemination-producers-cannot-emit-invalid-frames
  (testing "normalized Authority IDs and want ranges must stay unique"
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (dissemination-wire/frame->wire
                  {:type :have
                   :summary {publication-test/authority-hex
                             {0 {"db/roots" [[0 0]]}}
                             (.toUpperCase publication-test/authority-hex)
                             {0 {"db/roots" [[2 2]]}}}
                   :horizon {}})))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (dissemination-wire/frame->wire
                  {:type :want :ranges [(first (:ranges want))
                                        (first (:ranges want))]}))))

  (testing "aggregate intervals remain below the profile-wide item budget"
    (let [intervals (mapv (fn [n] [(* 2 n) (* 2 n)]) (range 512))
          summary {other-authority
                   {0 {"a" intervals} 1 {"b" intervals} 2 {"c" intervals}}}]
      (is (thrown? #?(:clj Throwable :cljs :default)
                   (dissemination-wire/frame->wire
                    {:type :have :summary summary :horizon {}}))))))
