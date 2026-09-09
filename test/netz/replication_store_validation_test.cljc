(ns netz.replication-store-validation-test
  (:require [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.store-validation :as validation]
            #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is]])))

(defn- error-data [f]
  (try (f) nil
       (catch #?(:clj Exception :cljs js/Error) e (ex-data e))))

(deftest content-address-validation-is-backend-independent
  (let [{:keys [manifest manifest-id pieces]}
        (blob/split (identity/seq->bytes (range 20)) {:piece-size 8})
        piece-id (:id (first pieces))]
    (is (= manifest (validation/verify-context! manifest-id manifest)))
    (is (identity/bufs= piece-id
                        (validation/expected-piece! manifest 0 piece-id)))
    (doseq [address [nil "not-bytes" (identity/seq->bytes [1]) piece-id]]
      (is (= :netz.replication/manifest-address-mismatch
             (:type (error-data #(validation/verify-context! address manifest))))))
    (doseq [[index address] [[-1 piece-id] [3 piece-id] [0.5 piece-id]
                             [nil piece-id] [0 nil] [0 manifest-id]]]
      (is (= {:type :netz.replication/piece-address-mismatch :index index}
             (error-data #(validation/expected-piece! manifest index address)))))))
