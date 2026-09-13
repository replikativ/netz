(ns netz.datahike-replica-process-fixture
  "Owned storage JVM for the static-bootstrap Datahike experiment."
  (:refer-clojure :exclude [await run!])
  (:require [clojure.core.async :as async]
            [clojure.edn :as edn]
            [netz.examples.static-bootstrap :as bootstrap]
            [netz.identity :as identity]
            [netz.replication.file-store :as file-store]
            [netz.replication.layout :as layout]
            [netz.replication.runtime :as transfer]
            [netz.replication.storage :as storage]
            [netz.replication-process-fixture :as replication-fixture]
            [netz.replication-runtime-integration-test :as carrier]))

(defn await [channel]
  (let [[value port] (async/alts!! [channel (async/timeout 30000)])]
    (when-not (= port channel)
      (throw (ex-info "Replica process operation timed out" {})))
    (if (instance? Throwable value) (throw value) value)))

(defn- identity-from [{:keys [genesis private public]}]
  {:genesis genesis
   :operational {:private (identity/hex->bytes private)
                 :public (identity/hex->bytes public)}})

(defn- load-bootstrap! [path trust]
  (await (bootstrap/load-and-verify!
          path (System/currentTimeMillis) trust)))

(defn- marker! [value]
  (println (str "NETZ-DATAHIKE-REPLICA " (pr-str value)))
  (flush))

(defn run! [config-path]
  (let [{:keys [root url bootstrap-path expected-network
                controller-authorities] :as config}
        (edn/read-string (slurp config-path))
        credentials (identity-from config)
        authority (identity/authority-id (:genesis credentials))
        trust {:network expected-network
               :controllers (set controller-authorities)}
        accepted (atom (load-bootstrap! bootstrap-path trust))
        node (carrier/start-server url credentials replication-fixture/capabilities)
        root-path (java.nio.file.Paths/get root (make-array String 0))
        store (file-store/new-file-store root-path
                                         {:durable-ancestor root-path})
        runtime
        (transfer/start!
         (:node node)
         {:store store
          :authorize-offer!
          (fn [{:keys [retention]}]
            (storage/chan-of
             (layout/retention-authorized?
              (:layout-state @accepted) authority retention)))})]
    (try
      (marker! [:ready authority])
      (loop []
        (when-let [command (read-line)]
          (case command
            "reload"
            (do (reset! accepted (load-bootstrap! bootstrap-path trust))
                (marker! [:reloaded
                          (get-in @accepted
                                  [:layout-state :current :statement "epoch"])]))
            "stop" nil
            (throw (ex-info "Unknown replica process command"
                            {:command command})))
          (when-not (= "stop" command) (recur))))
      (finally
        (transfer/stop! runtime)
        (carrier/stop-node! node)))))

(defn -main [config-path]
  (try
    (run! config-path)
    (System/exit 0)
    (catch Throwable cause
      (.printStackTrace cause)
      (System/exit 1))))
