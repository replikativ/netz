(ns netz.content-provider-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.content-provider :as provider]
            [netz.content-wire :as content-wire]
            [netz.identity :as identity]))

(def cid-a (content-wire/raw-cid (identity/utf8-bytes "a")))
(def cid-b (content-wire/raw-cid (identity/utf8-bytes "b")))
(def cid-c (content-wire/raw-cid (identity/utf8-bytes "c")))
(def authority-a (identity/seq->bytes (repeat 32 1)))
(def authority-b (identity/seq->bytes (repeat 32 2)))
(def authority-c (identity/seq->bytes (repeat 32 3)))

(deftest provider-records-are-bounded-and-expire
  (let [cache (-> (provider/make-cache {:max-keys 2
                                        :max-providers-per-key 2
                                        :max-keys-per-provider 1})
                  (provider/remember cid-a authority-b 10 100)
                  (provider/remember cid-a authority-a 20 100)
                  (provider/remember cid-a authority-c 20 100)
                  (provider/remember cid-b authority-a 20 100)
                  (provider/remember cid-b authority-c 20 100)
                  (provider/remember cid-c authority-b 20 100))]
    (is (= [authority-a authority-b]
           (mapv :authority (provider/providers-for cache cid-a 100))))
    (is (= [authority-c]
           (mapv :authority (provider/providers-for cache cid-b 100))))
    (is (empty? (provider/providers-for cache cid-c 100)))
    (is (= {:learned 3 :refused 3 :expired 0} (:stats cache)))
    (is (= [authority-a]
           (mapv :authority (provider/providers-for cache cid-a 110))))
    (is (empty? (provider/providers-for cache cid-a 120)))))

(deftest refresh-and-zero-ttl-do-not-consume-new-capacity
  (let [cache (-> (provider/make-cache {:max-keys 1
                                        :max-providers-per-key 1})
                  (provider/remember cid-a authority-a 5 0)
                  (provider/remember cid-a authority-a 20 3)
                  (provider/remember cid-b authority-b 0 3))]
    (is (= 23 (:expires-at (first (provider/providers-for cache cid-a 3)))))
    (is (= {:learned 1 :refused 0 :expired 0} (:stats cache)))))

(deftest one-provider-announcement-obeys-every-cache-bound
  (let [cache (provider/remember-provider-cids
               (provider/make-cache {:max-keys 2
                                     :max-keys-per-provider 1})
               [cid-a cid-b cid-c] authority-a 10 100)]
    (is (= [authority-a]
           (mapv :authority (provider/providers-for cache cid-a 100))))
    (is (empty? (provider/providers-for cache cid-b 100)))
    (is (empty? (provider/providers-for cache cid-c 100)))
    (is (= {:learned 1 :refused 2 :expired 0} (:stats cache)))
    (is (empty? (provider/providers-for cache cid-a 110)))))

(deftest malformed-records-fail-closed
  (testing "CID, Authority, TTL and clock are exact"
    (doseq [args [[(identity/seq->bytes [1]) authority-a 1 0]
                  [cid-a (identity/seq->bytes [1]) 1 0]
                  [cid-a authority-a (inc content-wire/max-provider-ttl-ms) 0]
                  [cid-a authority-a 1 -1]]]
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (apply provider/remember (provider/make-cache) args))))))

(deftest hostile-provider-volume-remains-bounded
  (let [authorities (mapv #(identity/seq->bytes (repeat 32 %)) (range 1 65))
        cids (mapv #(content-wire/raw-cid
                     (identity/utf8-bytes (str "block-" %)))
                   (range 128))
        cache (reduce (fn [cache [cid authority]]
                        (provider/remember cache cid authority 1000 0))
                      (provider/make-cache {:max-keys 32
                                            :max-providers-per-key 4
                                            :max-keys-per-provider 4})
                      (for [cid cids authority authorities]
                        [cid authority]))
        records (:records cache)]
    (is (= 32 (count records)))
    (is (every? #(<= (count %) 4) (vals records)))
    (is (every? #(<= % 4)
                (vals (frequencies
                       (for [providers (vals records)
                             authority-key (keys providers)]
                         authority-key)))))
    (is (pos? (get-in cache [:stats :refused])))))
