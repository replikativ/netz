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
