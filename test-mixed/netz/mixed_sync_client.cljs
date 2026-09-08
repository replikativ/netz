(ns netz.mixed-sync-client
  (:require [clojure.core.async :as a]
            [kabel.middleware.cbor :refer [cbor]]
            [kabel.peer :as peer]
            [netz.identity :as id]
            [netz.mixed-sync-profile :refer [portable-join-profile]]
            [netz.overlay.runtime :as runtime]
            [netz.sync :as sync]
            [netz.transport.noise :as noise]
            [netz.transport.secure :as secure]
            [netz.wire :as wire]
            [superv.async :refer [S]]))

(defn- test-identity []
  ;; Public test vector from netz-kabel-noise-binding-1.json, never production keys.
  (let [public "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c"]
    {:genesis (id/make-genesis {:rotation-keys [public] :rotation-threshold 1
                                :operational-keys [public]
                                :revocation-commit (apply str (repeat 32 "21"))})
     :operational {:public (id/hex->bytes public)
                   :private (id/hex->bytes (apply str (repeat 32 "01")))}}))

(defn main [url authority & [mode]]
  (let [deadline (js/setTimeout #(do (js/console.error "Node sync timed out")
                                     (js/process.exit 1)) 25000)]
    (a/go
      (try
        (let [identity (test-identity)
              remote (id/authority-routing-id authority)
              capabilities (conj noise/default-capabilities wire/profile-capability
                                 runtime/extension-carrier-capability sync/capability)
              [app install!] (runtime/deferred-middleware)
              socket (peer/client-peer
                      S (id/peer-id (:genesis identity)) app cbor (atom {}) (atom {})
                      {:transport-middleware
                       (secure/middleware {:identity identity :addresses [] :seq-no 1
                                           :capabilities capabilities :required capabilities})})
              node (a/<! (secure/start!
                          S socket {:identity identity :topics #{}
                                    :authorize-sync?
                                    (fn [{:keys [connection topic profile]}]
                                      (and (= mode "serve")
                                           (= authority (:authority-id connection))
                                           (= "demo/join" topic)
                                           (= "netz/join-semilattice/1" profile)))
                                    :seeds [{:peer-id remote :authority-id authority
                                             :addresses [url] :group "seed"}]
                                    :overlay-opts
                                    {:membership {:require-authority? true :dial-interval-ms 20}
                                     :sync {:profiles {"netz/join-semilattice/1" (portable-join-profile)}
                                            :applications {"demo/join" #{"node"}}}}}))]
          (install! (:middleware node))
          (try
            (loop [n 0]
              (when-not (runtime/connection-info socket remote)
                (when (= n 400) (throw (ex-info "JVM authentication timed out" {})))
                (a/<! (a/timeout 25))
                (recur (inc n))))
            (when-not (runtime/portable-sync-connection? socket remote)
              (throw (ex-info "Portable sync was not negotiated" {})))
            (if (= mode "serve")
              (let [done (a/promise-chan)
                    input (.createInterface (js/require "readline")
                                            #js {:input (.-stdin js/process)})]
                (try
                  (.once input "line" #(a/put! done %))
                  (when-not (= "done" (a/<! done))
                    (throw (ex-info "JVM did not confirm sync commit" {})))
                  (let [state (:sync (runtime/overlay-state node))]
                    (when-not (and (= 1 (get-in state [:stats :chunks-sent]))
                                   (empty? (:sessions state)))
                      (throw (ex-info "Node sender did not finish" {}))))
                  (finally (.close input))))
              (do
                (when-not (runtime/start-sync! node remote "demo/join" "netz/join-semilattice/1")
                  (throw (ex-info "Sync admission failed" {})))
                (loop [n 0]
                  (let [state (:sync (runtime/overlay-state node))]
                    (when-not (and (= #{"node" "jvm"} (get-in state [:applications "demo/join"]))
                                   (= :synchronized (get-in state [:status "demo/join"]))
                                   (empty? (:sessions state)))
                      (when (= n 400) (throw (ex-info "Node did not commit JVM state" {})))
                      (a/<! (a/timeout 25))
                      (recur (inc n)))))))
            (finally
              (runtime/stop! node)
              (a/<! (peer/stop socket))))
          (js/clearTimeout deadline)
          (println (if (= mode "serve") "NETZ_MIXED_SYNC_SERVED"
                       "NETZ_MIXED_SYNC_OK jvm,node"))
          (js/process.exit 0))
        (catch :default error
          (js/clearTimeout deadline)
          (js/console.error error)
          (js/process.exit 1))))))
