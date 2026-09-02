(ns netz.replication-read-repair-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.inventory :as inventory]
            [netz.replication.read-repair :as repair]
            [netz.replication.storage :as storage]
            [netz.replication.wire :as replication-wire]
            #?(:clj [clojure.core.async :refer [<!!]]
               :cljs [cljs.core.async :refer [<!] :refer-macros [go]])
            #?(:cljs [cljs.test :refer-macros [async]])))

(def public-hex
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
(def genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "aa"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "cc"))}))
(def authority (identity/authority-id-bytes genesis))
(def second-authority (identity/hex->bytes (apply str (repeat 32 "bb"))))
(def split (blob/split (identity/utf8-bytes "repairable") {:piece-size 4}))
(def retention
  {"v" 1 "type" "blob/retention"
   "cluster" (identity/seq->bytes (range 16))
   "privacy_domain" (identity/seq->bytes (range 16 32))
   "revision" 1 "manifest" (:manifest-id split)
   "object" (:object-id split) "layout_epoch" 1
   "replicas" 2 "write_acks" 1 "minimum_domains" {}
   "issued_at" 1000 "retain_until" 5000 "signer" authority})
(def retention-envelope
  (replication-wire/envelope
   genesis (identity/hex->bytes public-hex) retention
   (identity/seq->bytes (repeat 64 0))))
(def providers
  [{:peer :first :authority authority}
   {:peer :second :authority second-authority}])

(defrecord ScrubStore [health]
  inventory/PBlobInventoryStore
  (-inventory-page [_ _ _]
    (storage/chan-of {:manifests [] :next-cursor nil}))
  (-scrub-object! [_ manifest-id]
    (storage/chan-of (assoc @health :manifest manifest-id))))

(defn await-result [channel]
  #?(:clj (<!! channel)
     :cljs channel))

#?(:clj
   (deftest ensure-local-prefers-a-healthy-local-konserve-object
     (let [health (atom {:status :healthy})
           calls (atom [])
           result
           (await-result
            (repair/ensure-local!
             (->ScrubStore health) nil providers retention-envelope
             {:fetch-one! (fn [& args]
                            (swap! calls conj args)
                            (storage/chan-of {:status :installed}))}))]
       (is (= :local (:status result)))
       (is (empty? @calls))
       (is (= (identity/bytes->hex (:manifest-id split))
              (:manifest-id result))))))

#?(:clj
   (deftest ensure-local-tries-providers-and-scrubs-the-install
     (let [health (atom {:status :missing})
           calls (atom [])
           result
           (await-result
            (repair/ensure-local!
             (->ScrubStore health) nil providers retention-envelope
             {:fetch-one!
              (fn [{:keys [peer]} _]
                (swap! calls conj peer)
                (if (= :first peer)
                  (storage/chan-of
                   {:type :replication/fetch-failed
                    :status :failed :code :disconnected})
                  (do
                    (reset! health {:status :healthy})
                    (storage/chan-of {:status :installed
                                      :receipt :verified}))))}))]
       (is (= :fetched (:status result)))
       (is (= [:first :second] @calls))
       (is (= (identity/bytes->hex second-authority)
              (:provider-id result)))
       (is (= [:disconnected] (mapv :code (:failures result)))))))

#?(:clj
   (deftest failed-post-fetch-scrubs-never-claim-locality
     (let [health (atom {:status :missing})
           result
           (await-result
            (repair/ensure-local!
             (->ScrubStore health) nil providers retention-envelope
             {:fetch-one! (fn [_ _]
                            (storage/chan-of {:status :installed}))}))]
       (is (= :unavailable (:status result)))
       (is (= 2 (count (:failures result))))
       (is (every? #(= :post-fetch-scrub-failed (:code %))
                   (:failures result))))))

#?(:cljs
   (deftest cljs-ensure-local-retries-and-verifies
     (async done
            (go
              (let [health (atom {:status :missing})
                    result
                    (<! (repair/ensure-local!
                         (->ScrubStore health) nil providers retention-envelope
                         {:fetch-one!
                          (fn [{:keys [peer]} _]
                            (if (= :first peer)
                              (storage/chan-of {:status :failed
                                                :code :disconnected})
                              (do (reset! health {:status :healthy})
                                  (storage/chan-of {:status :installed}))))}))]
                (is (= :fetched (:status result)))
                (is (= (identity/bytes->hex second-authority)
                       (:provider-id result)))
                (done))))))

(deftest provider-input-is-bounded-and-unambiguous
  (testing "duplicate authenticated destinations are rejected"
    (is (thrown? #?(:clj Throwable :cljs js/Error)
                 (repair/ensure-local!
                  (->ScrubStore (atom {:status :missing})) nil
                  [(first providers) (first providers)]
                  retention-envelope))))
  (testing "provider work cannot be unbounded"
    (is (thrown? #?(:clj Throwable :cljs js/Error)
                 (repair/ensure-local!
                  (->ScrubStore (atom {:status :missing})) nil
                  (vec (repeat (inc repair/max-providers) (first providers)))
                  retention-envelope)))))
