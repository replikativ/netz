(ns netz.store.konserve-materialization-cljs-test
  (:require ["fs" :as fs]
            ["os" :as os]
            ["path" :as path]
            [cljs.core.async :as async]
            [cljs.test :refer-macros [async deftest is]]
            [konserve.memory :refer [new-mem-store]]
            [konserve.node-filestore :as filestore]
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

(deftest node-file-installer-reads-back-streamed-bytes
  (async done
    (async/go
      (let [directory (fs/mkdtempSync (path/join (os/tmpdir) "netz-materialize-"))]
        (try
          (let [store (async/<! (filestore/connect-fs-store directory))
                installer (materialization/binary-installer
                           store {:store-id "drive" :decode-key (constantly :file)})
                request {:store-id "drive" :codec "bytes/identity"
                         :bytes (identity/utf8-bytes "file contents")}
                [result] (async/alts! [(installer request) (async/timeout 5000)])]
            (is (= :committed (:result result))))
          (finally
            (fs/rmSync directory #js {:recursive true :force true})
            (done)))))))
