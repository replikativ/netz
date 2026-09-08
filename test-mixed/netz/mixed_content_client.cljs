(ns netz.mixed-content-client
  (:require [clojure.core.async :as a]
            [kabel.middleware.cbor :refer [cbor]]
            [kabel.peer :as peer]
            [netz.content-runtime :as content]
            [netz.content-wire :as content-wire]
            [netz.identity :as id]
            [netz.overlay.runtime :as runtime]
            [netz.transport.noise :as noise]
            [netz.transport.secure :as secure]
            [netz.wire :as wire]
            [superv.async :refer [S]]))

(defn main [_uri-option url _authority-option authority-hex]
  (let [deadline (js/setTimeout
                  #(do (js/console.error "Node content conformance timed out")
                       (js/process.exit 1)) 25000)]
    (a/go
      (try
        (let [identity (a/<! (id/generate-identity))
              authority (id/hex->bytes authority-hex)
              remote (id/authority-routing-id authority)
              capabilities (conj noise/default-capabilities
                                 content-wire/capability
                                 runtime/extension-carrier-capability
                                 wire/profile-capability)
              [app install!] (runtime/deferred-middleware)
              socket (peer/client-peer
                      S (id/peer-id (:genesis identity)) app cbor (atom {}) (atom {})
                      {:transport-middleware
                       (secure/middleware {:identity identity :addresses [] :seq-no 1
                                           :capabilities capabilities :required capabilities})})
              node (a/<! (secure/start!
                          S socket {:identity identity :topics #{}
                                    :seeds [{:peer-id remote :authority-id authority-hex
                                             :addresses [url] :group "seed"}]
                                    :overlay-opts
                                    {:membership {:require-authority? true
                                                  :dial-interval-ms 20}}}))
              requester (content/start-requester! node {:timeout-ms 5000})]
          (install! (:middleware node))
          (try
            (loop [attempt 0]
              (when-not (runtime/connection-info socket remote)
                (when (= attempt 400)
                  (throw (ex-info "Authenticated JVM connection missing" {})))
                (a/<! (a/timeout 25))
                (recur (inc attempt))))
            (let [expected (id/utf8-bytes "portable content over authenticated Kabel")
                  result (a/<! ((:fetch! requester) remote (content-wire/raw-cid expected)))]
              (when-not (and (= :ok (:status result))
                             (id/bufs= expected (:data result)))
                (throw (ex-info "JVM content fetch failed" {:result result}))))
            (finally
              ((:stop! requester))
              (runtime/stop! node)
              (a/<! (peer/stop socket))))
          (js/clearTimeout deadline)
          (println "NETZ_MIXED_CONTENT_OK")
          (js/process.exit 0))
        (catch :default error
          (js/clearTimeout deadline)
          (js/console.error error)
          (js/process.exit 1))))))
