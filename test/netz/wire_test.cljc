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

(deftest profile-decoder-is-closed
  (testing "implementation-specific values cannot cross the portable boundary"
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/encode-canonical {:keyword :not-portable})))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/encode-canonical {"float" 1.5}))))

  (testing "valid CBOR still has to be deterministic and consume the frame"
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/decode (id/hex->bytes "1801"))))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (wire/decode (id/hex->bytes "0102"))))))
