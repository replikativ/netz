(ns netz.replication-connectivity-runtime-test
  (:require [clojure.test :refer [deftest is]]
            [netz.replication.connectivity-runtime :as connectivity]))

(deftest rejects-invalid-coordinator-configuration
  ;; Also loads the complete portable coordinator dependency graph on Node.
  (doseq [options [nil {} {:overlay {} :controllers []}
                   {:overlay {} :controllers (vec (repeat 33 {}))}]]
    (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                 (connectivity/start! options)))))
