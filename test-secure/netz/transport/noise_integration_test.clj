(ns netz.transport.noise-integration-test
  "The authenticated carrier over a real Kabel/http-kit WebSocket."
  (:require [clojure.core.async :refer [timeout]]
            [clojure.test :refer [deftest is testing]]
            [kabel.http-kit :as http-kit]
            [kabel.middleware.cbor :refer [cbor]]
            [kabel.peer :as peer]
            [kabel.transport :as transport]
            [netz.identity :as id]
            [netz.overlay :as overlay]
            [netz.overlay.runtime :as rt]
            [netz.sync.join-semilattice :as join]
            [netz.sync.profile :as sync-profile]
            [netz.sync.root-log :as root-log]
            [netz.transport.noise :as secure]
            [netz.transport.secure :as secure-overlay]
            [superv.async :refer [<?? S]]))

(def ^:private port-counter (atom 48700))

(defn- wait-for [ms pred]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (cond
        (pred) true
        (> (System/currentTimeMillis) deadline) false
        :else (do (<?? S (timeout 25)) (recur))))))

(defn- secure-contexts [p]
  (vals (transport/connections p)))

(def ^:private join-profile (join/profile 1))
(def ^:private join-profile-id (sync-profile/profile-id join-profile))
(def ^:private root-profile (root-log/profile :writer))
(def ^:private root-profile-id (sync-profile/profile-id root-profile))

(defn- sync-options [document root]
  {:profiles {join-profile-id join-profile
              root-profile-id root-profile}
   :applications {:doc document :db root}})

(defn- start-secure-client [identity server-id server-authority url applications]
  (let [client-id (id/peer-id (:genesis identity))
        [app install!] (rt/deferred-middleware)
        socket (peer/client-peer
                S client-id app cbor (atom {}) (atom {})
                {:transport-middleware
                 (secure-overlay/middleware {:identity identity
                                             :addresses []
                                             :seq-no 1})})
        node (<?? S (secure-overlay/start!
                     S socket
                     {:identity identity
                      :topics #{:doc :db}
                      :seeds [{:peer-id server-id
                               :authority-id server-authority
                               :addresses [url]
                               :group "seed"}]
                      :overlay-opts
                      {:membership {:require-authority? true
                                    :dial-interval-ms 20}
                       :sync applications}}))]
    (install! (:middleware node))
    {:id client-id :peer socket :node node}))

(defn- stop-node! [{:keys [node peer]}]
  (rt/stop! node)
  (<?? S (peer/stop peer)))

(deftest authenticated-overlay-crosses-a-real-websocket
  (let [port (swap! port-counter inc)
        url (str "ws://localhost:" port)
        server-identity (<?? S (id/generate-identity))
        client-identity (<?? S (id/generate-identity))
        server-id (id/peer-id (:genesis server-identity))
        client-id (id/peer-id (:genesis client-identity))
        server-authority (id/authority-id (:genesis server-identity))
        client-authority (id/authority-id (:genesis client-identity))
        [server-app install-server!] (rt/deferred-middleware)
        server-peer
        (peer/server-peer
         S (http-kit/create-http-kit-handler! S url server-id)
         server-id server-app cbor (atom {}) (atom {})
         {:transport-middleware
          (secure-overlay/middleware {:identity server-identity
                                      :addresses [url]
                                      :seq-no 1})})
        server (<?? S (secure-overlay/start!
                       S server-peer
                       {:identity server-identity
                        :addresses [url]
                        :topics #{:secure}
                        :overlay-opts
                        {:membership {:require-authority? true}}}))]
    (install-server! (:middleware server))
    (<?? S (peer/start server-peer))
    (try
      (let [[client-app install-client!] (rt/deferred-middleware)
            client-peer
            (peer/client-peer
             S client-id client-app cbor (atom {}) (atom {})
             {:transport-middleware
              (secure-overlay/middleware {:identity client-identity
                                          :addresses []
                                          :seq-no 1})})
            client (<?? S (secure-overlay/start!
                           S client-peer
                           {:identity client-identity
                            :topics #{:secure}
                            :seeds [{:peer-id server-id
                                     :authority-id server-authority
                                     :addresses [url]
                                     :group "seed"}]
                            :overlay-opts
                            {:membership {:require-authority? true
                                          :dial-interval-ms 20}}}))]
        (install-client! (:middleware client))
        (try
          (testing "membership's dial carries the full expected Authority ID"
            (is (wait-for 10000
                          #(and (contains? (rt/connections client-peer) server-id)
                                (contains? (rt/connections server-peer) client-id)))
                "Noise plus the transitional overlay proof never completed"))

          (testing "both physical connection contexts publish authenticated facts"
            (is (= #{server-authority}
                   (set (map #(get @% ::transport/authenticated-authority)
                             (secure-contexts client-peer)))))
            (is (= #{client-authority}
                   (set (map #(get @% ::transport/authenticated-authority)
                             (secure-contexts server-peer)))))
            (is (every? #(= secure/default-capabilities
                            (get @% ::transport/negotiated-capabilities))
                        (concat (secure-contexts client-peer)
                                (secure-contexts server-peer)))))

          (testing "an ordinary overlay publication survives CBOR and Noise"
            (rt/publish! client :secure {:message "encrypted"})
            (is (wait-for 10000
                          #(some #{{:message "encrypted"}}
                                 (overlay/delivered (rt/overlay-state server))))))
          (finally
            (rt/stop! client)
            (<?? S (peer/stop client-peer)))))
      (finally
        (rt/stop! server)
        (<?? S (peer/stop server-peer))))))

(deftest three-peer-profiles-rejoin-over-real-noise-websockets
  (let [port (swap! port-counter inc)
        url (str "ws://localhost:" port)
        a-identity (<?? S (id/generate-identity))
        b-identity (<?? S (id/generate-identity))
        c-identity (<?? S (id/generate-identity))
        a-id (id/peer-id (:genesis a-identity))
        a-authority (id/authority-id (:genesis a-identity))
        old-root {:writer :writer :term 1 :index 1 :root :old}
        current-root {:writer :writer :term 2 :index 4 :root :current}
        [a-app install-a!] (rt/deferred-middleware)
        a-peer
        (peer/server-peer
         S (http-kit/create-http-kit-handler! S url a-id)
         a-id a-app cbor (atom {}) (atom {})
         {:transport-middleware
          (secure-overlay/middleware {:identity a-identity
                                      :addresses [url]
                                      :seq-no 1})})
        a-node (<?? S (secure-overlay/start!
                       S a-peer
                       {:identity a-identity
                        :addresses [url]
                        :topics #{:doc :db}
                        :overlay-opts
                        {:membership {:require-authority? true}
                         :sync (sync-options #{:a} current-root)}}))]
    (install-a! (:middleware a-node))
    (<?? S (peer/start a-peer))
    (try
      (let [b (start-secure-client b-identity a-id a-authority url
                                   (sync-options #{:b} old-root))
            c (start-secure-client c-identity a-id a-authority url
                                   (sync-options #{:c} old-root))]
        (try
          (is (wait-for 10000 #(= #{(:id b) (:id c)}
                                   (rt/connections a-peer)))
              "three-peer secure star did not assemble")

          ;; C goes offline with its own contribution and stale writer root.
          (stop-node! c)
          (is (wait-for 5000 #(not (contains? (rt/connections a-peer) (:id c))))
              "partitioned peer remained registered")

          ;; The connected side converges both application profiles.
          (rt/start-sync! a-node (:id b) :doc join-profile-id)
          (is (wait-for 10000 #(= #{:a :b}
                                   (overlay/synced-value
                                    (rt/overlay-state a-node) :doc))))
          (rt/start-sync! (:node b) a-id :doc join-profile-id)
          (rt/start-sync! (:node b) a-id :db root-profile-id)
          (is (wait-for 10000
                        #(and (= #{:a :b}
                                 (overlay/synced-value
                                  (rt/overlay-state (:node b)) :doc))
                              (= current-root
                                 (overlay/synced-value
                                  (rt/overlay-state (:node b)) :db)))))

          ;; Recreate C with the application state it durably held before the
          ;; partition. Identity is stable, while the runtime epoch is fresh.
          (let [c2 (start-secure-client c-identity a-id a-authority url
                                        (sync-options #{:c} old-root))]
            (try
              (is (wait-for 10000 #(contains? (rt/connections a-peer) (:id c2)))
                  "partitioned peer did not rejoin through Noise")
              (rt/start-sync! a-node (:id c2) :doc join-profile-id)
              (is (wait-for 10000 #(= #{:a :b :c}
                                       (overlay/synced-value
                                        (rt/overlay-state a-node) :doc))))
              (rt/start-sync! (:node c2) a-id :doc join-profile-id)
              (rt/start-sync! (:node c2) a-id :db root-profile-id)
              (rt/start-sync! (:node b) a-id :doc join-profile-id)
              (is (wait-for 10000
                            #(every?
                              true?
                              [(= #{:a :b :c}
                                  (overlay/synced-value
                                   (rt/overlay-state (:node b)) :doc))
                               (= #{:a :b :c}
                                  (overlay/synced-value
                                   (rt/overlay-state (:node c2)) :doc))
                               (= current-root
                                  (overlay/synced-value
                                   (rt/overlay-state (:node c2)) :db))]))
                  "real carrier rejoin did not converge both profiles")
              (finally (stop-node! c2))))
          (finally (stop-node! b))))
      (finally
        (rt/stop! a-node)
        (<?? S (peer/stop a-peer))))))
