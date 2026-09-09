(ns netz.replication-process-integration-test
  "Live four-replica topology with a forcibly terminated, file-backed writer."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [netz.identity :as identity]
            [netz.replication.file-store :as file-store]
            [netz.replication.inventory :as inventory]
            [netz.replication.layout :as layout]
            [netz.replication-process-fixture :as fixture]
            [netz.replication-runtime-integration-test :as carrier]
            [netz.replication.runtime :as transfer]
            [netz.replication.storage :as storage])
  (:import (java.net ServerSocket)
           (java.nio.file Files Path)
           (java.util.concurrent TimeUnit)))

(defn- temp-root []
  (Files/createTempDirectory "netz-process-replication-"
                             (make-array java.nio.file.attribute.FileAttribute 0)))

(defn- child-directory [^Path root name]
  (Files/createDirectory (.resolve root name)
                         (make-array java.nio.file.attribute.FileAttribute 0)))

(defn- free-url []
  (with-open [socket (ServerSocket. 0)]
    (str "ws://localhost:" (.getLocalPort socket))))

(defn- stop-process! [^Process process]
  (when (.isAlive process)
    (.destroyForcibly process)
    (fixture/require! (.waitFor process 10 TimeUnit/SECONDS) "Owned child JVM did not stop")))

(defn- launch! [^Path root config-path mode]
  (let [java (str (System/getProperty "java.home") java.io.File/separator "bin"
                  java.io.File/separator "java")
        log (.resolve root (str mode ".log"))
        process (.start
                 (doto (ProcessBuilder.
                        ^java.util.List
                        [java "-Xmx512m" "-Xss4m"
                         (str "-Djava.io.tmpdir=" (System/getProperty "java.io.tmpdir"))
                         "-cp" (System/getProperty "java.class.path")
                         "clojure.main" "-m" "netz.replication-process-fixture"
                         (str config-path) mode])
                   (.redirectErrorStream true)
                   (.redirectOutput (.toFile log))))]
    {:process process :log log}))

(defn- await-marker! [{:keys [^Process process log]} marker]
  (let [deadline (+ (System/currentTimeMillis) 120000)
        expected (str "NETZ-PROCESS " (pr-str marker))]
    (loop []
      (let [output (slurp (str log))]
        (cond
          (str/includes? output expected) true
          (not (.isAlive process))
          (throw (ex-info "Writer process exited before milestone"
                          {:marker marker :log (str log) :output output}))
          (>= (System/currentTimeMillis) deadline)
          (throw (ex-info "Writer process milestone timeout"
                          {:marker marker :log (str log) :output output}))
          :else (do (Thread/sleep 50) (recur)))))))

(deftest signed-three-replica-transition-survives-writer-process-termination
  (let [root (temp-root)
        nodes (atom [])
        runtimes (atom [])
        processes (atom [])]
    (try
      (let [writer (fixture/await (identity/generate-identity))
            receivers
            (mapv (fn [index]
                    (let [credentials (fixture/await (identity/generate-identity))
                          url (free-url)
                          node (carrier/start-server url credentials fixture/capabilities)
                          store-root (child-directory root (str "replica-" index))
                          store (file-store/new-file-store
                                 store-root {:durable-ancestor store-root
                                             :clock (constantly 1200)})]
                      (swap! nodes conj node)
                      {:node node :store store :url url :peer-id (:id node)
                       :authority (identity/authority-id (:genesis credentials))}))
                  (range 4))
            source-root (child-directory root "writer")
            config {:root (str source-root) :genesis (:genesis writer)
                    :private (identity/bytes->hex (get-in writer [:operational :private]))
                    :public (identity/bytes->hex (get-in writer [:operational :public]))
                    :receivers (mapv #(select-keys % [:url :peer-id :authority]) receivers)}
            data (fixture/fixture-data config)
            current (atom (fixture/accepted-state data false))
            config-path (.resolve root "fixture.edn")]
        (doseq [{:keys [node store authority]} receivers]
          (swap! runtimes conj
                 (transfer/start!
                  (:node node)
                  {:store store :now-fn (constantly 1100)
                   :authorize-offer!
                   (fn [{:keys [retention]}]
                     (storage/chan-of (layout/retention-authorized?
                                       @current authority retention)))})))
        ;; Only generated test credentials are written, inside an owned temp directory.
        (spit (str config-path) (pr-str config))
        (let [first-run (launch! root config-path "initial")]
          (swap! processes conj (:process first-run))
          (await-marker! first-run :initial-complete)
          (await-marker! first-run :successor-persisted)
          (is (= :missing
                 (:status (fixture/await (inventory/scrub-object!
                                          (:store (nth receivers 3))
                                          (get-in data [:split :manifest-id]))))))
          (stop-process! (:process first-run))
          (is (not (.isAlive ^Process (:process first-run)))))
        (reset! current (fixture/accepted-state data true))
        (let [second-run (launch! root config-path "recover")]
          (swap! processes conj (:process second-run))
          (await-marker! second-run :recovered-under-replicated)
          (await-marker! second-run :successor-complete)
          (fixture/require! (.waitFor ^Process (:process second-run) 30 TimeUnit/SECONDS)
                            "Recovered writer did not exit")
          (is (= 0 (.exitValue ^Process (:process second-run)))))
        (doseq [index [1 2 3]]
          (is (= :healthy
                 (:status (fixture/await (inventory/scrub-object!
                                          (:store (nth receivers index))
                                          (get-in data [:split :manifest-id])))))))
        ;; Retirement is not automatic: the replaced holder keeps its valid copy.
        (is (= :healthy
               (:status (fixture/await (inventory/scrub-object!
                                        (:store (first receivers))
                                        (get-in data [:split :manifest-id])))))))
      (finally
        (doseq [process @processes] (stop-process! process))
        (doseq [runtime @runtimes] (transfer/stop! runtime))
        (doseq [node @nodes] (carrier/stop-node! node))
        ;; Retain bounded logs and stores for crash-test diagnosis; they contain
        ;; generated fixture keys only. Print the exact owned directory.
        (println "Process replication test artifacts:" (str root))))))
