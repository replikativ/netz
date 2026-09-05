(ns netz.snapshot-ref-pubsub-compile-test
  (:require [clojure.test :refer [deftest is]]
            [netz.identity :as identity]
            [netz.snapshot-ref :as ref]
            [netz.snapshot-ref.pubsub :as ref-pubsub]))

(deftest strategy-is-portable
  (is (some? (ref-pubsub/strategy
              {:state (atom (ref/make-state))
               :profile "datahike/commit-v1"
               :system (identity/utf8-bytes "store-a")
               :ref (identity/utf8-bytes "main")}))))
