(ns netz.mixed-dissemination-client
  (:require [clojure.core.async :as a]
            [kabel.middleware.cbor :refer [cbor]]
            [kabel.peer :as peer]
            [netz.dissemination-wire :as dissemination]
            [netz.identity :as id]
            [netz.mixed-identity :refer [test-identity]]
            [netz.overlay.runtime :as runtime]
            [netz.transport.noise :as noise]
            [netz.transport.secure :as secure]
            [netz.wire :as wire]
            [superv.async :refer [S]]))

(def topic "datahike/roots/node-demo")

(defn main [url authority]
  (let [deadline (js/setTimeout #(do (js/console.error "Node publication timed out")
                                     (js/process.exit 1)) 25000)]
    (a/go
      (try
        (let [identity (test-identity)
              capabilities (conj noise/default-capabilities wire/profile-capability
                                 runtime/extension-carrier-capability dissemination/capability)
              [app install!] (runtime/deferred-middleware)
              socket (peer/client-peer
                      S (id/peer-id (:genesis identity)) app cbor (atom {}) (atom {})
                      {:transport-middleware
                       (secure/middleware {:identity identity :addresses [] :seq-no 1
                                           :capabilities capabilities :required capabilities})})
              node (a/<! (secure/start!
                          S socket {:identity identity :topics #{topic}
                                    :seeds [{:peer-id (id/authority-routing-id authority)
                                             :authority-id authority :addresses [url] :group "seed"}]
                                    :authorize-dissemination?
                                    (fn [context]
                                      (and (= authority (:authority-id context))
                                           (or (not= :gossip (:frame-type context))
                                               (and (= authority (:origin-authority context))
                                                    (= topic (:topic context))))))
                                    :overlay-opts {:membership {:require-authority? true
                                                                :dial-interval-ms 20}}}))
              delivered (a/promise-chan)
              applied (atom nil)]
          (runtime/set-deliver! node
                                (fn [_source received-topic payload publication]
                                  (if (and (= topic received-topic)
                                           (= authority (:origin-authority publication))
                                           (id/bufs= (id/utf8-bytes "root-42") payload))
                                    (do
                                      (reset! applied payload)
                                      (runtime/application-committed! node publication)
                                      (a/put! delivered publication))
                                    (a/put! delivered :invalid))))
          (install! (:middleware node))
          (try
            (let [publication (a/<! delivered)]
              (when (= :invalid publication)
                (throw (ex-info "Unexpected publication delivery" {})))
              (loop [n 0]
                (when-not (= (:seq publication)
                             (get-in (runtime/overlay-state node)
                                     [:sync :frontiers topic
                                      [(:origin publication) (:epoch publication) topic]]))
                  (when (= n 200) (throw (ex-info "Application receipt was not recorded" {})))
                  (a/<! (a/timeout 25))
                  (recur (inc n)))))
            (finally (runtime/stop! node) (a/<! (peer/stop socket))))
          (js/clearTimeout deadline)
          (println "NETZ_MIXED_DISSEMINATION_OK root-42")
          (js/process.exit 0))
        (catch :default error
          (js/clearTimeout deadline)
          (js/console.error error)
          (js/process.exit 1))))))
