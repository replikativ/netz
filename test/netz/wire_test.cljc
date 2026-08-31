(ns netz.wire-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as id]
            [netz.wire :as wire]))

(def hello-fixture
  {"v" 1
   "type" "hello"
   "peer" "netz1-example"
   "nonce" (id/hex->bytes "00010203")
   "features" ["sync/1" "content/1"]})

(def hello-fixture-hex
  "a561760164706565726d6e65747a312d6578616d706c6564747970656568656c6c6f656e6f6e63654400010203686665617475726573826673796e632f3169636f6e74656e742f31")

(def sync-open-fixture
  {"v" 1
   "type" "sync/open"
   "session" (id/hex->bytes "000102030405060708090a0b0c0d0e0f")
   "topic" "demo/tasks"
   "profile" "netz/join-semilattice/1"
   "summary" (id/hex->bytes "8265616c7068616462657461")})

(def sync-open-fixture-hex
  "a661760164747970656973796e632f6f70656e65746f7069636a64656d6f2f7461736b736770726f66696c65776e65747a2f6a6f696e2d73656d696c6174746963652f316773657373696f6e50000102030405060708090a0b0c0d0e0f6773756d6d6172794c8265616c7068616462657461")

(def sync-done-frontiers
  [{"origin" (id/hex->bytes
              "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
    "epoch" 3
    "topic" "demo/tasks"
    "through" 7}
   {"origin" (id/hex->bytes
              "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f")
    "epoch" 1
    "topic" "demo/tasks"
    "through" 4}])

(def sync-done-fixture
  {"v" 1
   "type" "sync/done"
   "session" (id/hex->bytes "000102030405060708090a0b0c0d0e0f")
   "count" 2
   "topic" "demo/tasks"
   "checkpoint" (id/hex->bytes "8265616c7068616462657461")
   "frontiers" sync-done-frontiers})

(def sync-done-fixture-hex
  "a761760164747970656973796e632f646f6e6565636f756e740265746f7069636a64656d6f2f7461736b736773657373696f6e50000102030405060708090a0b0c0d0e0f6966726f6e746965727382a46565706f63680365746f7069636a64656d6f2f7461736b73666f726967696e5820000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f677468726f75676807a46565706f63680165746f7069636a64656d6f2f7461736b73666f726967696e5820202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f677468726f756768046a636865636b706f696e744c8265616c7068616462657461")

(deftest canonical-cbor-known-answer
  (testing "JVM and JS emit the exact bytes published for foreign clients"
    (is (= hello-fixture-hex
           (id/bytes->hex (wire/encode-canonical hello-fixture))))
    (let [decoded (wire/decode (wire/encode-canonical hello-fixture))]
      (is (= (dissoc hello-fixture "nonce") (dissoc decoded "nonce")))
      (is (id/bufs= (get hello-fixture "nonce") (get decoded "nonce"))))))

(deftest canonical-sync-open-known-answer
  (testing "opaque profile bytes and the session id have one portable encoding"
    (is (= sync-open-fixture-hex
           (id/bytes->hex (wire/encode-canonical sync-open-fixture))))
    (let [decoded (wire/decode (wire/encode-canonical sync-open-fixture))]
      (is (= (dissoc sync-open-fixture "session" "summary")
             (dissoc decoded "session" "summary")))
      (is (id/bufs= (get sync-open-fixture "session")
                    (get decoded "session")))
      (is (id/bufs= (get sync-open-fixture "summary")
                    (get decoded "summary"))))))

(deftest strict-sync-frame-boundary-known-answer
  (testing "the Clojure state-machine shape converts to the shared fixture"
    (let [internal {:type :sync/open
                    :session (get sync-open-fixture "session")
                    :topic "demo/tasks"
                    :profile "netz/join-semilattice/1"
                    :summary (get sync-open-fixture "summary")}
          encoded (wire/encode-sync-frame internal)
          decoded (wire/decode-sync-frame encoded)]
      (is (= sync-open-fixture-hex (id/bytes->hex encoded)))
      (is (= (dissoc internal :session :summary)
             (dissoc decoded :session :summary)))
      (is (id/bufs= (:session internal) (:session decoded)))
      (is (id/bufs= (:summary internal) (:summary decoded)))))

  (testing "opaque native values and unknown fields are not stringified"
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/encode-sync-frame
                  {:type :sync/chunk :session (id/hex->bytes
                                               "000102030405060708090a0b0c0d0e0f")
                   :index 0 :data #{:not-bytes}})))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/sync-frame-from-wire (assoc sync-open-fixture
                                                   "extension" true))))))

(deftest canonical-sync-done-frontier-known-answer
  (testing "JVM, JS, and Python agree on the bounded frontier array"
    (is (= sync-done-fixture-hex
           (id/bytes->hex (wire/encode-canonical sync-done-fixture))))
    (let [internal (wire/frontiers-from-wire "demo/tasks"
                                             sync-done-frontiers)
          roundtrip (wire/frontiers->wire "demo/tasks" internal)]
      (is (= 2 (count internal)))
      (is (= (mapv #(dissoc % "origin") sync-done-frontiers)
             (mapv #(dissoc % "origin") roundtrip)))
      (is (every? true?
                  (map #(id/bufs= (get %1 "origin") (get %2 "origin"))
                       sync-done-frontiers roundtrip)))))

  (let [internal {:type :sync/done
                  :session (get sync-done-fixture "session")
                  :count 2
                  :topic "demo/tasks"
                  :checkpoint (get sync-done-fixture "checkpoint")
                  :frontiers (wire/frontiers-from-wire "demo/tasks"
                                                       sync-done-frontiers)}
        encoded (wire/encode-sync-frame internal)
        decoded (wire/decode-sync-frame encoded)]
    (is (= sync-done-fixture-hex (id/bytes->hex encoded)))
    (is (= (:frontiers internal) (:frontiers decoded))))

  (testing "noncanonical ordering and foreign topics are rejected"
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/frontiers-from-wire "demo/tasks"
                                           (vec (reverse sync-done-frontiers)))))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/frontiers-from-wire "other" sync-done-frontiers)))))

(deftest profile-decoder-is-closed
  (testing "implementation-specific values cannot cross the portable boundary"
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/encode-canonical {:keyword :not-portable})))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/encode-canonical {"float" 1.5})))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/encode-canonical
                  {"oversized" (id/byte-buf (inc (* 1024 1024)))}))))

  (testing "valid CBOR still has to be deterministic and consume the frame"
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/decode (id/hex->bytes "1801"))))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/decode (id/hex->bytes "0102"))))))
