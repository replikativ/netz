(ns netz.dissemination-adapter-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.dissemination :as dissemination]
            [netz.dissemination-adapter :as adapter]
            [netz.dissemination-wire :as dissemination-wire]
            [netz.identity :as identity]))

(def authority-a (apply str (repeat 64 "a")))
(def authority-b (apply str (repeat 64 "b")))

(deftest portable-adapter-round-trips-control-streams
  (let [routing-a (identity/authority-routing-id authority-a)
        state (-> (dissemination/make-state :me)
                  (assoc :authority-ids {routing-a authority-a}
                         :portable-streams #{[routing-a 2 "db/roots"]}))
        have {:type :have
              :summary {routing-a {2 {"db/roots" [[0 3]]}}}
              :horizon {[routing-a 2 "db/roots"] 1}}
        wire-have (adapter/to-wire state have)
        restored (adapter/from-wire (dissemination/make-state :other) wire-have)
        receiver (:state (dissemination/handler
                          (dissemination/make-state :other)
                          {:type :message :from :peer :payload restored}
                          {:now 0}))
        want (-> (dissemination/gaps-against receiver (:summary restored)
                                             (:horizon restored))
                 (#(hash-map :type :want :ranges %)))
        wire-want (adapter/to-wire receiver want)]
    (is (identity/bufs= (identity/hex->bytes authority-a)
                        (get-in wire-have ["streams" 0 "origin"])))
    (is (= {2 {"db/roots" [[0 3]]}}
           (get-in restored [:summary routing-a])))
    (is (= authority-a (get-in receiver [:authority-ids routing-a])))
    (is (identity/bufs= (identity/hex->bytes authority-a)
                        (get-in wire-want ["ranges" 0 "origin"])))
    (is (= want (adapter/from-wire receiver wire-want)))))

(deftest portable-adapter-filters-native-history-and-collisions
  (let [routing-a (identity/authority-routing-id authority-a)
        state (assoc (dissemination/make-state :me)
                     :authority-ids {routing-a authority-a}
                     :portable-streams #{})
        wire-have (adapter/to-wire
                   state
                   {:type :have
                    :summary {routing-a {0 {:native [[0 4]]}}}
                    :horizon {[routing-a 0 :native] 0}})]
    (is (= [] (get wire-have "streams"))))

  (testing "different Authorities may never alias through the routing UUID"
    (let [prefix (apply str (repeat 32 "c"))
          first-authority (str prefix (apply str (repeat 32 "0")))
          second-authority (str prefix (apply str (repeat 32 "1")))
          wire-have (dissemination-wire/frame->wire
                     {:type :have
                      :summary {first-authority {0 {"a" [[0 0]]}}
                                second-authority {0 {"b" [[0 0]]}}}
                      :horizon {}})]
      (is (thrown? #?(:clj Throwable :cljs :default)
                   (adapter/from-wire (dissemination/make-state :me)
                                      wire-have))))))
