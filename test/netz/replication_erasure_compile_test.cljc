(ns netz.replication-erasure-compile-test
  (:require [clojure.test :refer [deftest is]]
            [netz.replication.erasure :as erasure]
            [netz.replication.erasure-runtime :as erasure-runtime]
            [netz.replication.erasure-storage :as erasure-storage]))

(deftest portable-erasure-api-loads
  (is (ifn? erasure/make-state))
  (is (ifn? erasure-runtime/start!))
  (is (some? erasure-storage/PBlobErasureStore)))
