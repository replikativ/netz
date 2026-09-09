(ns netz.replication-test-store
  "Filesystem fixtures: caller-created temporary roots model durable ancestors.
  These tests verify barrier ordering, not temporary-directory power-loss safety."
  (:require [netz.replication.file-store :as file-store]))

(defn new-file-store
  ([root] (new-file-store root {}))
  ([root opts]
   (file-store/new-file-store root (merge {:durable-ancestor root} opts))))
