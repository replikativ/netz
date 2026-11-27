(ns netz.replication-test-store
  "Filesystem fixtures: caller-created temporary roots model durable ancestors.
  These tests verify barrier ordering, not temporary-directory power-loss safety."
  (:require [netz.replication.file-store :as file-store])
  (:import (java.nio.file Files Path)
           (java.util Comparator)))

(defn new-file-store
  ([root] (new-file-store root {}))
  ([root opts]
   (file-store/new-file-store root (merge {:durable-ancestor root} opts))))

(defn temporary-directory []
  (Files/createTempDirectory
   "netz-blob-runtime-test-"
   (make-array java.nio.file.attribute.FileAttribute 0)))

(defn delete-tree! [^Path root]
  (when (Files/exists root (make-array java.nio.file.LinkOption 0))
    (with-open [paths (Files/walk
                       root
                       (make-array java.nio.file.FileVisitOption 0))]
      (doseq [target (sort (Comparator/reverseOrder)
                           (iterator-seq (.iterator paths)))]
        (Files/deleteIfExists ^Path target)))))

(defmacro with-store-roots
  [[source-binding source-expression target-binding target-expression] & body]
  `(let [~source-binding ~source-expression]
     (try
       (let [~target-binding ~target-expression]
         (try
           ~@body
           (finally (delete-tree! ~target-binding))))
       (finally (delete-tree! ~source-binding)))))
