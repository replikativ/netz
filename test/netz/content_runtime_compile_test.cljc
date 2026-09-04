(ns netz.content-runtime-compile-test
  (:require [clojure.test :refer [deftest is]]
            [netz.content-runtime :as content-runtime]))

(deftest portable-content-runtime-loads-on-both-platforms
  (is (= 64 (:queue-size content-runtime/default-opts)))
  (is (thrown? #?(:clj Throwable :cljs :default)
               (content-runtime/start-service! {} {}))))
