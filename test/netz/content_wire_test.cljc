(ns netz.content-wire-test
  (:require [clojure.test :refer [deftest is testing]]
            #?(:clj [clojure.data.json :as json])
            [netz.content-wire :as content-wire]
            [netz.identity :as identity]
            [netz.wire :as wire]))

(def data (identity/utf8-bytes "portable block"))
(def cid (content-wire/raw-cid data))
(def request (identity/seq->bytes (range 16)))
(def provider (identity/seq->bytes (repeat 32 1)))

(def frames
  {:announce {:type :content/announce :cids [cid]}
   :find {:type :content/find :request request :cid cid}
   :found {:type :content/found :request request :cid cid
           :providers [provider] :ttl-ms 60000}
   :want {:type :content/want :request request :cid cid}
   :block {:type :content/block :request request :cid cid :data data}
   :dont_have {:type :content/dont-have :request request :cid cid}})

(deftest portable-content-shared-vectors
  #?(:clj
     (let [fixture (json/read-str
                    (slurp "spec/fixtures/content-block-1.json"))]
       (is (= content-wire/capability (get fixture "profile")))
       (is (= (identity/bytes->hex cid) (get fixture "cid_hex")))
       (doseq [[name frame] frames]
         (is (= (get fixture (str (clojure.core/name name) "_cbor_hex"))
                (identity/bytes->hex (content-wire/encode-frame frame)))))))

  (doseq [[_ frame] frames]
    (let [decoded (content-wire/decode-frame
                   (content-wire/encode-frame frame))]
      (is (= (dissoc frame :request :cid :data :providers :cids)
             (dissoc decoded :request :cid :data :providers :cids)))
      (doseq [key [:request :cid :data]
              :when (get frame key)]
        (is (identity/bufs= (get frame key) (get decoded key))))))
  (is (content-wire/verify-block? cid data)))

(deftest portable-content-rejects-unsafe-input
  (testing "blocks are verified before crossing the boundary"
    (is (false? (content-wire/verify-block?
                 cid (identity/utf8-bytes "tampered"))))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (content-wire/frame->wire
                  (assoc (:block frames) :data
                         (identity/utf8-bytes "tampered"))))))

  (testing "decoded schemas are closed and canonical"
    (let [announce (content-wire/frame->wire (:announce frames))
          found (content-wire/frame->wire (:found frames))]
      (is (thrown? #?(:clj Throwable :cljs :default)
                   (content-wire/frame-from-wire
                    (assoc announce "extension" true))))
      (is (thrown? #?(:clj Throwable :cljs :default)
                   (content-wire/frame-from-wire
                    (assoc announce "cids" [cid cid]))))
      (is (thrown? #?(:clj Throwable :cljs :default)
                   (content-wire/frame-from-wire
                    (assoc found "providers" [provider provider]))))))

  (testing "request and provider identities have fixed widths"
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (content-wire/frame->wire
                  (assoc (:find frames) :request
                         (identity/sub-buf request 0 15)))))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (content-wire/frame->wire
                  (assoc (:found frames) :providers [request]))))))
