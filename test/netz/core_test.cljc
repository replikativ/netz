(ns netz.core-test
  (:require [clojure.test :refer [deftest is]]
            [netz.content :as content]
            [netz.core :as netz]
            [netz.overlay :as overlay]
            [netz.overlay.runtime :as runtime]))

(defn- running []
  (let [ctx (runtime/make-runtime
             {:id :me
              :state (overlay/make-state :me {})
              :handler overlay/handler
              :effects {:send! (fn [_ _]) :connect! (fn [_ _ _])
                        :disconnect! (fn [_]) :schedule! (fn [_ _])}})]
    {:ctx ctx :peer (atom {}) :peer-id :me}))

(deftest curated-content-api-validates-before-submission
  (let [running (running)
        value {:portable "block"}
        key (content/content-key value)]
    (is (true? (netz/provide! running key value)))
    (is (false? (netz/provide! running :wrong value)))
    (is (true? (netz/fetch! running key)))
    (is (true? (netz/fetch-tree! running key [])))
    (is (true? (netz/stop! running)))))
