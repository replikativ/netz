(ns netz.replication-materialization-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.inventory :as inventory]
            [netz.replication.materialization :as materialization]
            [netz.replication.storage :as storage]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]
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
(def split (blob/split (identity/utf8-bytes "portable drive bytes")
                       {:piece-size 5}))
(def retention
  {"v" 1 "type" "blob/retention"
   "cluster" (identity/seq->bytes (range 16))
   "privacy_domain" (identity/seq->bytes (range 16 32))
   "revision" 1 "manifest" (:manifest-id split)
   "object" (:object-id split) "layout_epoch" 1
   "replicas" 1 "write_acks" 1 "minimum_domains" {}
   "issued_at" 1000 "retain_until" 5000 "signer" authority})
(def retention-envelope
  (replication-wire/envelope
   genesis (identity/hex->bytes public-hex) retention
   (identity/seq->bytes (repeat 64 0))))
(def app-binding
  (materialization/application-binding
   {:store-id "dvergr/drive"
    :application-key
    (identity/utf8-bytes
     "7f4f01f01e9d69c53b9ff760719559321a026ce37dbb11f4d6f291e829f5a09f")
    :codec "bytes/identity"
    :object-id (:object-id split)
    :manifest-id (:manifest-id split)}))
(def providers [{:peer :provider :authority authority}])

(def fixture-binding
  (materialization/application-binding
   {:store-id "dvergr/drive"
    :application-key
    (identity/hex->bytes
     "7f4f01f01e9d69c53b9ff760719559321a026ce37dbb11f4d6f291e829f5a09f")
    :codec "bytes/identity"
    :object-id
    (identity/hex->bytes
     "c959701fbfcec8aeb4795de542de04c74d60e2736ce017c39f148c50604f53e0")
    :manifest-id
    (identity/hex->bytes
     "b40648f31978841528e7a438f42b99de987fa22c75f2f1dca334c358dd5da23c")}))

(def fixture-cbor-hex
  "a7617601647479706577626c6f622f6170706c69636174696f6e2d6f626a65637465636f6465636e62797465732f6964656e74697479666f626a6563745820c959701fbfcec8aeb4795de542de04c74d60e2736ce017c39f148c50604f53e0686d616e69666573745820b40648f31978841528e7a438f42b99de987fa22c75f2f1dca334c358dd5da23c6873746f72655f69646c6476657267722f64726976656f6170706c69636174696f6e5f6b657958207f4f01f01e9d69c53b9ff760719559321a026ce37dbb11f4d6f291e829f5a09f")

(defrecord InstalledStore [health]
  inventory/PBlobInventoryStore
  (-inventory-page [_ _ _]
    (storage/chan-of {:manifests [(:manifest-id split)] :next-cursor nil}))
  (-scrub-object! [_ manifest-id]
    (storage/chan-of (assoc @health :manifest manifest-id)))

  inventory/PBlobProviderStore
  (-load-installed-manifest [_ _manifest-id]
    (storage/chan-of (when (= :healthy (:status @health)) (:manifest split))))

  storage/PBlobTransferStore
  (-load-staging [_ _ _ _] (storage/chan-of nil))
  (-stage-piece! [_ _ _ _ _ _ _] (storage/chan-of :ok))
  (-install-object! [_ _ _ _ _ _] (storage/chan-of nil))
  (-load-piece [_ _ _ index _]
    (storage/chan-of (:data (nth (:pieces split) index))))
  (-persist-receipt! [_ _ _] (storage/chan-of :ok))
  (-recorded-receipts [_] (storage/chan-of []))
  (-cleanup-session! [_ _ _] (storage/chan-of true)))

(defn await-result [channel]
  #?(:clj (<!! channel)
     :cljs channel))

(deftest application-binding-is-closed-and-addressed
  (is (materialization/valid-binding? app-binding))
  (is (= 32 (identity/buf-length
             (materialization/binding-address app-binding))))
  (is (not (materialization/valid-binding?
            (assoc app-binding "extra" true))))
  (is (not (materialization/valid-binding?
            (assoc app-binding "store_id" "dvergr/drive/é"))))
  (is (thrown? #?(:clj Throwable :cljs js/Error)
               (materialization/application-binding
                {:store-id "dvergr/drive"
                 :application-key (identity/seq->bytes [])
                 :codec "bytes/identity"
                 :object-id (:object-id split)
                 :manifest-id (:manifest-id split)}))))

(deftest application-binding-matches-the-python-cbor-fixture
  (is (= fixture-cbor-hex
         (identity/bytes->hex (wire/encode-canonical fixture-binding))))
  (is (= "b8825524e7384c5e2188752e4c2b78cd79a5043579912054da15daaefb1e997e"
         (identity/bytes->hex
          (materialization/binding-address fixture-binding)))))

#?(:clj
   (deftest healthy-local-object-materializes-at-the-authenticated-key
     (let [installed (atom nil)
           result
           (await-result
            (materialization/materialize!
             (->InstalledStore (atom {:status :healthy})) nil providers
             retention-envelope app-binding
             (fn [request]
               (reset! installed request)
               (storage/chan-of {:result :committed}))))]
       (is (= :materialized (:status result)))
       (is (= :local (:source result)))
       (is (= "dvergr/drive" (:store-id @installed)))
       (is (= "bytes/identity" (:codec @installed)))
       (is (identity/bufs= (identity/utf8-bytes "portable drive bytes")
                           (:bytes @installed)))
       (is (identity/bufs= (get app-binding "application_key")
                           (:application-key @installed))))))

#?(:clj
   (deftest fetch-precedes-materialization-and-post-install-scrub
     (let [health (atom {:status :missing})
           order (atom [])
           result
           (await-result
            (materialization/materialize!
             (->InstalledStore health) nil providers retention-envelope
             app-binding
             (fn [_]
               (swap! order conj :materialize)
               (storage/chan-of :committed))
             {:fetch-one!
              (fn [_ _]
                (swap! order conj :fetch)
                (reset! health {:status :healthy})
                (storage/chan-of {:status :installed}))}))]
       (is (= :materialized (:status result)))
       (is (= :fetched (:source result)))
       (is (= [:fetch :materialize] @order)))))

#?(:clj
   (deftest mismatched-catalog-binding-cannot-write-an-application-key
     (let [calls (atom 0)
           wrong (assoc app-binding "object"
                        (identity/seq->bytes (repeat 32 7)))]
       (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"does not match retention"
            (materialization/materialize!
             (->InstalledStore (atom {:status :healthy})) nil providers
             retention-envelope wrong
             (fn [_] (swap! calls inc) (storage/chan-of :bad)))))
       (is (zero? @calls)))))

#?(:clj
   (deftest materialization-size-and-installer-failures-are-explicit
     (let [store (->InstalledStore (atom {:status :healthy}))
           too-large (await-result
                      (materialization/materialize!
                       store nil providers retention-envelope app-binding
                       (fn [_] (storage/chan-of :never))
                       {:max-bytes 4}))
           cause (ex-info "immutable key conflict" {:type :test/conflict})
           conflict (await-result
                     (materialization/materialize!
                      store nil providers retention-envelope app-binding
                      (fn [_] (storage/chan-of cause))))
           empty-result
           (await-result
            (materialization/materialize!
             store nil providers retention-envelope app-binding
             (fn [_] (storage/chan-of nil))))]
       (is (= :local-read-failed (:code too-large)))
       (is (= :netz.replication/materialization-too-large
              (:type (ex-data (:cause too-large)))))
       (is (= :application-install-failed (:code conflict)))
       (is (identical? cause (:cause conflict)))
       (is (= :application-install-failed (:code empty-result)))
       (is (= :netz.replication/empty-install-result
              (:type (ex-data (:cause empty-result))))))))

#?(:cljs
   (deftest cljs-materialization-keeps-the-same-explicit-boundary
     (async done
            (go
              (let [installed (atom nil)
                    result
                    (<! (materialization/materialize!
                         (->InstalledStore (atom {:status :healthy}))
                         nil providers retention-envelope app-binding
                         (fn [request]
                           (reset! installed request)
                           (storage/chan-of :committed))))]
                (is (= :materialized (:status result)))
                (is (= :local (:source result)))
                (is (identity/bufs= (identity/utf8-bytes
                                     "portable drive bytes")
                                    (:bytes @installed)))
                (done))))))
