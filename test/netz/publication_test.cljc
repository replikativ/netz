(ns netz.publication-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.publication :as publication]
            [netz.wire :as wire]
            #?(:clj [clojure.core.async :refer [<!!]])))

(def private-hex
  "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
(def public-hex
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
(def authority-hex
  "dadc6166b57499e9a4934c894f012a4acd29d851521f5be9dc04def7937467ca")
(def signing-hex
  "6e65747a2f676f737369702f763100a76176016373657107647479706566676f737369706565706f63680365746f7069636a64656d6f2f7461736b73666f726967696e5820dadc6166b57499e9a4934c894f012a4acd29d851521f5be9dc04def7937467ca677061796c6f616449a26178016179820203")
(def signature-hex
  "a48b674305d2450605148dbe33530c337f42b93e1f214cccf487e71d0c296c480daf1393a3209962c5ed7c13464c40aa9052387076738cc24e0d3b3fa7789701")
(def envelope-hex
  "ab617601637365710764686f707300647479706566676f737369706565706f63680365746f7069636a64656d6f2f7461736b73666f726967696e5820dadc6166b57499e9a4934c894f012a4acd29d851521f5be9dc04def7937467ca6767656e65736973a5617601667265766f6b655820cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc68726f746174696f6e815820aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa697468726573686f6c64016b6f7065726174696f6e616c815820d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a677061796c6f616449a26178016179820203697369676e61747572655840a48b674305d2450605148dbe33530c337f42b93e1f214cccf487e71d0c296c480daf1393a3209962c5ed7c13464c40aa9052387076738cc24e0d3b3fa77897016b6f7065726174696f6e616c5820d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")

(def genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "aa"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "cc"))}))

(def unsigned
  {:type :gossip
   :wire-version 1
   :origin-authority authority-hex
   :epoch 3
   :seq 7
   :topic "demo/tasks"
   :payload (identity/hex->bytes "a26178016179820203")
   :hops 0})

(def signed
  (assoc unsigned
         :origin (identity/peer-id genesis)
         :origin-genesis genesis
         :origin-key public-hex
         :origin-sig signature-hex))

(deftest portable-publication-known-answer
  (testing "JVM, JavaScript, and Python sign the exact opaque payload bytes"
    (is (publication/portable? unsigned))
    (is (= signing-hex
           (identity/bytes->hex (publication/signing-bytes unsigned))))
    (is (= envelope-hex
           (identity/bytes->hex (publication/encode signed)))))

  (testing "the strict wire boundary round-trips without decoding the payload"
    (let [decoded (publication/decode (publication/encode signed))]
      (is (= (dissoc signed :payload)
             (dissoc decoded :payload)))
      (is (identity/bufs= (:payload signed) (:payload decoded)))))

  #?(:clj
     (testing "the fixture signature verifies under the operational key"
       (is (true? (<!! (identity/verify
                        (identity/hex->bytes public-hex)
                        (publication/signing-bytes unsigned)
                        (identity/hex->bytes signature-hex))))))))

(deftest portable-publication-is-a-closed-byte-boundary
  (testing "native values, unsafe counters, and unknown fields are refused"
    (is (not (publication/portable? (assoc unsigned :payload {:x 1}))))
    (is (not (publication/portable? (assoc unsigned :seq 9007199254740992))))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (publication/from-wire
                  (assoc (wire/decode (publication/encode signed))
                         "extension" true))))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (publication/from-wire
                  (assoc (wire/decode (publication/encode signed))
                         "origin" (identity/hex->bytes
                                   (apply str (repeat 64 "0"))))))))

  (testing "forwarding metadata does not alter the origin signature"
    (is (= signing-hex
           (identity/bytes->hex
            (publication/signing-bytes (assoc unsigned :hops 8 :repair? true))))))

  (testing "application bytes are covered exactly"
    (let [tampered (assoc unsigned :payload
                          (identity/hex->bytes "a1617801"))]
      (is (not= signing-hex
                (identity/bytes->hex
                 (publication/signing-bytes tampered)))))))
