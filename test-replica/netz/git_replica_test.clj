(ns netz.git-replica-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.string :as str]
            [konserve.memory :as memory]
            [netz.examples.git-replica :as git]
            [netz.examples.snapshot-store :as store]
            [netz.examples.snapshot-archive :as archive]
            [netz.examples.snapshot-transfer :as transfer]
            [netz.identity :as id]
            [netz.replication.blob :as blob]
            [netz.replication.file-store :as file-store]
            [netz.replication.materialization :as materialization]
            [yggdrasil.adapters.git :as ygg])
  (:import (java.nio.file Files)))

(defn temp-dir []
  (str (Files/createTempDirectory "netz-git-replica-" (make-array java.nio.file.attribute.FileAttribute 0))))

(deftest yggdrasil-git-closure-installs-offline-without-advancing-refs
  (let [source (temp-dir)
        target (temp-dir)
        _ (git/git! source "init" "--initial-branch=main")
        _ (git/git! target "init" "--bare" "--initial-branch=main")
        file (str/trim (git/git! source "hash-object" "-w" "--stdin" :in "replicated file\n"))
        tree (str/trim (git/git! source "mktree" :in (str "100644 blob " file "\tnotes.txt\n")))
        commit (str/trim (git/git! source "-c" "user.name=Netz" "-c" "user.email=demo@localhost"
                                   "commit-tree" tree "-m" "initial"))
        _ (git/git! source "update-ref" "refs/heads/main" commit)
        system (ygg/create source {:system-name "git-replica-test"})
        authority (store/await! (id/generate-identity))
        adapter (git/adapter system target)
        event (git/event system)
        captured (transfer/capture! adapter event authority git/encode [])
        archive-path (temp-dir)
        packed (archive/stage-local! (file-store/new-file-store archive-path) (archive/prepare captured))
        restored-bytes (store/await! (materialization/read-installed!
                                      (file-store/new-file-store archive-path) (:binding packed)))
        export (archive/restore restored-bytes (:statement captured))
        statement (:statement export)
        target-store (store/await! (memory/new-mem-store))
        cache (store/await! (memory/new-mem-store))
        opened (store/open! statement
                            {:publisher (:netz/publisher statement)
                             :system (:netz/system statement) :ref-name (:netz/ref statement)}
                            cache #(get (:blocks export) (id/bytes->hex %)) git/decode)]
    ;; Make the source repository inaccessible to the importer, without deleting it.
    (Files/move (.toPath (java.io.File. (str source "/.git")))
                (.toPath (java.io.File. (str source "/offline.git")))
                (make-array java.nio.file.CopyOption 0))
    (let [view (transfer/install! (git/adapter nil target) opened target-store git/encode)]
      (is (= commit (:commit view)))
      (is (= "replicated file\n" (git/git! target "show" (str commit ":notes.txt"))))
      (is (= "initial" (str/trim (git/git! target "show" "-s" "--format=%s" commit)))))
    (is (id/bufs= (:object-id (:split packed)) (blob/object-address (:bytes packed))))
    (is (thrown? Exception (archive/prepare (update captured :blocks dissoc
                                                    (first (keys (:blocks captured)))))))
    (is (str/blank? (git/git! target "for-each-ref")))
    (is (thrown? Exception
                 (transfer/install! (git/adapter nil target)
                                    (assoc-in opened [:root-statement :netz/profile] "datahike/commit-v1")
                                    target-store git/encode)))))
