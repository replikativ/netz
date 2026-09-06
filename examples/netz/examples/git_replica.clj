(ns netz.examples.git-replica
  "Second Yggdrasil snapshot adapter: native Git commit closure in a pack.
   No Git fetch connection, checkout, hooks, or mutable ref advancement.
   Small trusted repositories only; external Git must be installed."
  (:require [boring.core :as boring]
            [clojure.java.shell :as shell]
            [konserve.core :as k]
            [netz.examples.snapshot-store :as store]
            [netz.examples.snapshot-transfer :as transfer]
            [netz.identity :as id]
            [yggdrasil.protocols :as ygg])
  (:import (java.io ByteArrayInputStream)))

(def profile "netz/demo/git-pack-v1")
(def max-pack-bytes (* 900 1024))
(defn encode [v] (boring/encode v {:stringref false :profile :canonical}))
(defn decode [v] (boring/decode v))

(defn git! [path & args]
  (let [result (apply shell/sh "git" "--no-replace-objects" "-C" path args)]
    (when-not (zero? (:exit result))
      (throw (ex-info "Git snapshot operation failed" (select-keys result [:exit :err]))))
    (:out result)))

(defn commit-id? [id]
  (and (string? id) (boolean (re-matches #"(?:[0-9a-f]{40}|[0-9a-f]{64})" id))))

(defn event
  "Capture identity and metadata through Yggdrasil. A committed Git snapshot
   is not itself a Netz fsync receipt. Source owner must prevent concurrent GC."
  [system]
  (let [snapshot (str (ygg/snapshot-id system))
        metadata (ygg/snapshot-meta system snapshot)]
    (when-not (and (commit-id? snapshot) (= snapshot (:snapshot-id metadata))
                   (set? (:parent-ids metadata)))
      (throw (ex-info "Git snapshot metadata is unavailable" {})))
    {:system-id (ygg/system-id system) :snapshot-id snapshot
     :parent-ids (set (map str (:parent-ids metadata)))
     :ref (str "refs/heads/" (name (ygg/current-branch system))) :durable? true}))

(defn adapter
  "source-system is a Yggdrasil Git system. target-path is an existing isolated
   Git repository, created by the application; native object format must match."
  [source-system target-path]
  (reify transfer/SnapshotTransfer
    (select-snapshot! [_ event]
      (let [commit (:snapshot-id event)]
        (when-not (commit-id? commit) (throw (ex-info "Invalid Git commit ID" {})))
        (let [view (ygg/as-of source-system commit)
              source (:repo-path view)
              _ (git! source "cat-file" "-e" (str commit "^{commit}"))
              pack (git! source "pack-objects" "--stdout" "--revs" "--threads=1" "--window=0"
                         :in (str commit "\n") :out-enc :bytes)
              _ (when (> (alength ^bytes pack) max-pack-bytes)
                  (throw (ex-info "Git demo pack exceeds bound" {})))
              root {"commit" commit "pack" pack}]
          {:profile profile :root-key commit :keys [commit]
           :source (store/view [commit] (constantly root))})))
    (validate-root! [_ root statement]
      (when-not (and (= profile (:netz/profile statement)) (commit-id? root)
                     (id/bufs= (id/utf8-bytes root) (:netz/snapshot statement)))
        (throw (ex-info "Git commit and signed reference disagree" {}))))
    (open-snapshot! [_ target root-key]
      (let [root (store/await! (k/get target root-key))
            pack (get root "pack")]
        (when-not (and (= #{"commit" "pack"} (set (keys root)))
                       (= root-key (get root "commit"))
                       (bytes? pack) (<= (alength ^bytes pack) max-pack-bytes))
          (throw (ex-info "Invalid Git snapshot value" {})))
        (git! target-path "index-pack" "--stdin" "--strict" :in (ByteArrayInputStream. pack))
        (git! target-path "cat-file" "-e" (str root-key "^{commit}"))
        (git! target-path "fsck" "--strict" "--no-reflogs" root-key)
        {:repo-path target-path :commit root-key}))))
