(ns netz.store.konserve-materialization-cljs-test
  (:require [cljs.core.async :as async]
            [cljs.test :refer-macros [async deftest is]]
            [konserve.memory :refer [new-mem-store]]
            [netz.identity :as identity]
            [netz.store.konserve-materialization :as materialization]))

(deftest cljs-binary-installer-is-idempotent-and-refuses-conflicts
  (async done
    (async/go
      (let [store (async/<! (new-mem-store))
            installer
            (materialization/binary-installer
             store {:store-id "dvergr/drive"
                    :decode-key (constantly "sha256-hex")})
            request {:store-id "dvergr/drive"
                     :application-key (identity/utf8-bytes "sha256-hex")
                     :codec "bytes/identity"
                     :bytes (identity/utf8-bytes "drive payload")}
            first-result (async/<! (installer request))
            second-result (async/<! (installer request))
            conflict (async/<!
                      (installer
                       (assoc request :bytes (identity/utf8-bytes "other"))))]
        (is (= :committed (:result first-result)))
        (is (= :already-present (:result second-result)))
        (is (instance? js/Error conflict))
        (is (= :netz.replication/application-key-conflict
               (:type (ex-data conflict))))
        (done)))))
