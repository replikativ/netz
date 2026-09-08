(ns netz.mixed-runtime-integration-test
  "Real bidirectional Python/JVM durable-blob interoperability."
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :refer [timeout]]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [kabel.http-kit :as http-kit]
            [kabel.middleware.cbor :refer [cbor]]
            [kabel.peer :as peer]
            [netz.dissemination-wire :as dissemination-wire]
            [netz.identity :as identity]
            [netz.overlay.runtime :as overlay-runtime]
            [netz.replication.blob :as blob]
            [netz.replication.file-store :as file-store]
            [netz.replication.runtime :as replication-runtime]
            [netz.replication.storage :as storage]
            [netz.replication.wire :as replication-wire]
            [netz.sync :as sync]
            [netz.mixed-sync-profile :refer [portable-join-profile]]
            [netz.transport.noise :as noise]
            [netz.transport.secure :as secure-overlay]
            [netz.wire :as wire]
            [superv.async :refer [<?? S]])
  (:import (java.nio.file Files Path)
           (java.util Comparator)
           (java.util.concurrent TimeUnit)))

(def ^:private port-counter (atom 49300))

(def ^:private python-authority
  (identity/hex->bytes
   "4531a4acb860ec3ecef33da9dfd6e17657285f75ffe1576237f58cd863f93e87"))

(def ^:private python-sync-authority
  "dba4644c8e9b0ded79d5ef00569cb3841053b197c2b528b55ad8e1857085e56a")

(def ^:private python-sync-peer-id
  #uuid "dba4644c-8e9b-0ded-79d5-ef00569cb384")

(defn- mixed-payload []
  (let [payload (byte-array (+ (bit-shift-left 1 20) 137))]
    (dotimes [index (alength payload)]
      (aset-byte payload index (unchecked-byte (mod index 251))))
    payload))

(defn- wait-for [ms pred]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (cond
        (pred) true
        (> (System/currentTimeMillis) deadline) false
        :else (do (<?? S (timeout 25)) (recur))))))

(defn- await [channel]
  (let [value (<?? S channel)]
    (if (instance? Throwable value) (throw value) value)))

(defn- temporary-directory []
  (Files/createTempDirectory
   "netz-mixed-runtime-test-"
   (make-array java.nio.file.attribute.FileAttribute 0)))

(defn- delete-tree! [^Path root]
  (when (Files/exists root (make-array java.nio.file.LinkOption 0))
    (with-open [paths (Files/walk
                       root
                       (make-array java.nio.file.FileVisitOption 0))]
      (doseq [target (sort (Comparator/reverseOrder)
                           (iterator-seq (.iterator paths)))]
        (Files/deleteIfExists ^Path target)))))

(defmacro ^{:clj-kondo/lint-as 'clojure.core/let} with-store-roots
  [[receipt-binding target-binding] & body]
  `(let [~receipt-binding (temporary-directory)
         ~target-binding (temporary-directory)]
     (try
       ~@body
       (finally
         (delete-tree! ~receipt-binding)
         (delete-tree! ~target-binding)))))

(defn- start-server
  [url identity capabilities & [overlay-opts authorize-sync?
                                authorize-dissemination?]]
  (let [id (identity/peer-id (:genesis identity))
        [app install!] (overlay-runtime/deferred-middleware)
        socket
        (peer/server-peer
         S (http-kit/create-http-kit-handler! S url id)
         id app cbor (atom {}) (atom {})
         {:transport-middleware
          (secure-overlay/middleware {:identity identity
                                      :addresses [url]
                                      :seq-no 1
                                      :capabilities capabilities})})
        node (<?? S (secure-overlay/start!
                     S socket
                     {:identity identity
                      :addresses [url]
                      :topics #{:blob-test}
                      :authorize-sync? authorize-sync?
                      :authorize-dissemination? authorize-dissemination?
                      :overlay-opts
                      (merge {:membership {:require-authority? true}}
                             overlay-opts)}))]
    (install! (:middleware node))
    (<?? S (peer/start socket))
    {:peer socket :node node}))

(defn- stop-node! [{:keys [node peer]}]
  (overlay-runtime/stop! node)
  (<?? S (peer/stop peer)))

(defn- stop-process! [^Process process]
  (when (.isAlive process)
    (.destroy process)
    (when-not (.waitFor process 1 TimeUnit/SECONDS)
      (.destroyForcibly process)
      (.waitFor process 5 TimeUnit/SECONDS))))

(defn- run-python-sender [url server-authority receipt-path timeout-seconds]
  (let [python (or (System/getenv "NETZ_PYTHON") "python3")
        arguments
        [python "test-mixed/python/mixed_blob_sender.py"
         "--uri" url
         "--remote-authority" (identity/bytes->hex server-authority)
         "--receipt" (str receipt-path)]
        builder (doto (ProcessBuilder. ^java.util.List arguments)
                  (.redirectErrorStream true))
        process (.start builder)
        stream (.getInputStream process)
        output (future (slurp stream))]
    (try
      (let [finished? (.waitFor process timeout-seconds TimeUnit/SECONDS)]
        (when-not finished? (stop-process! process))
        {:exit (when finished? (.exitValue process))
         :output (deref output 5000 "process output did not close")
         :timed-out? (not finished?)})
      (finally
        (stop-process! process)
        (.close stream)
        (future-cancel output)))))

(defn- run-python-receiver
  [url server-authority store-path completion-path timeout-seconds
   {:keys [attempt stop-after-pieces]
    :or {attempt 0 stop-after-pieces 0}}]
  (let [python (or (System/getenv "NETZ_PYTHON") "python3")
        arguments
        [python "test-mixed/python/mixed_blob_receiver.py"
         "--uri" url
         "--remote-authority" (identity/bytes->hex server-authority)
         "--store" (str store-path)
         "--completion" (str completion-path)
         "--attempt" (str attempt)
         "--stop-after-pieces" (str stop-after-pieces)]
        builder (doto (ProcessBuilder. ^java.util.List arguments)
                  (.redirectErrorStream true))
        process (.start builder)
        stream (.getInputStream process)
        output (future (slurp stream))]
    (try
      (let [finished? (.waitFor process timeout-seconds TimeUnit/SECONDS)]
        (when-not finished? (stop-process! process))
        {:exit (when finished? (.exitValue process))
         :output (deref output 5000 "process output did not close")
         :timed-out? (not finished?)})
      (finally
        (stop-process! process)
        (.close stream)
        (future-cancel output)))))

(defn- install-source! [store session split]
  (doseq [{:keys [index id data]} (:pieces split)]
    (await (storage/-stage-piece!
            store session (:manifest-id split) (:manifest split)
            index id data)))
  (await (storage/-install-object!
          store session (:manifest-id split) (:manifest split) {}
          "netz/store/fsync/1")))

(defn- run-python-sync [url server-authority timeout-seconds]
  (let [python (or (System/getenv "NETZ_PYTHON") "python3")
        arguments
        [python "test-mixed/python/mixed_sync_receiver.py"
         "--uri" url
         "--remote-authority" (identity/bytes->hex server-authority)]
        builder (doto (ProcessBuilder. ^java.util.List arguments)
                  (.redirectErrorStream true))
        process (.start builder)
        stream (.getInputStream process)
        output (future (slurp stream))]
    (try
      (let [finished? (.waitFor process timeout-seconds TimeUnit/SECONDS)]
        (when-not finished? (stop-process! process))
        {:exit (when finished? (.exitValue process))
         :output (deref output 5000 "process output did not close")
         :timed-out? (not finished?)})
      (finally
        (stop-process! process)
        (.close stream)
        (future-cancel output)))))

(defn- run-python-sync-sender [url server-authority timeout-seconds]
  (let [python (or (System/getenv "NETZ_PYTHON") "python3")
        arguments
        [python "test-mixed/python/mixed_sync_sender.py"
         "--uri" url
         "--remote-authority" (identity/bytes->hex server-authority)]
        builder (doto (ProcessBuilder. ^java.util.List arguments)
                  (.redirectErrorStream true))
        process (.start builder)
        stream (.getInputStream process)
        output (future (slurp stream))]
    (try
      (let [finished? (.waitFor process timeout-seconds TimeUnit/SECONDS)]
        (when-not finished? (stop-process! process))
        {:exit (when finished? (.exitValue process))
         :output (deref output 5000 "process output did not close")
         :timed-out? (not finished?)})
      (finally
        (stop-process! process)
        (.close stream)
        (future-cancel output)))))

(defn- run-python-dissemination [url server-authority timeout-seconds]
  (let [python (or (System/getenv "NETZ_PYTHON") "python3")
        arguments
        [python "test-mixed/python/mixed_dissemination_receiver.py"
         "--uri" url
         "--remote-authority" (identity/bytes->hex server-authority)]
        builder (doto (ProcessBuilder. ^java.util.List arguments)
                  (.redirectErrorStream true))
        process (.start builder)
        stream (.getInputStream process)
        output (future (slurp stream))]
    (try
      (let [finished? (.waitFor process timeout-seconds TimeUnit/SECONDS)]
        (when-not finished? (stop-process! process))
        {:exit (when finished? (.exitValue process))
         :output (deref output 5000 "process output did not close")
         :timed-out? (not finished?)})
      (finally
        (stop-process! process)
        (.close stream)
        (future-cancel output)))))

(deftest python-sender-transfers-into-jvm-durable-store
  (with-store-roots [receipt-root target-root]
    (let [port (swap! port-counter inc)
          url (str "ws://localhost:" port)
          capabilities (conj noise/default-capabilities
                             replication-wire/capability
                             overlay-runtime/extension-carrier-capability
                             wire/profile-capability)
          server-identity (<?? S (identity/generate-identity))
          server (start-server url server-identity capabilities)
          server-authority
          (identity/authority-id-bytes (:genesis server-identity))
          target (file-store/new-file-store target-root
                                            {:clock (constantly 1200)})
          accepted (atom [])
          installed (atom [])
          runtime
          (replication-runtime/start!
           (:node server)
           {:store target
            :now-fn (constantly 1100)
            :authorize-offer!
            (fn [{:keys [from-authority retention] :as request}]
              ;; This mixed test begins at an already-authorized offer. The
              ;; production lifecycle runtime's journal-before-authorization
              ;; ordering has its own persistence-fault suite.
              (swap! accepted conj request)
              (storage/chan-of
               (identity/bufs= from-authority (get retention "signer"))))
            :on-installed! #(swap! installed conj %)})
          split (blob/split (mixed-payload)
                            {:piece-size (bit-shift-left 1 20)})
          receipt-path (.resolve receipt-root "receipt.cbor")]
      (try
        (let [wrong-authority (identity/seq->bytes (repeat 32 255))
              rejected (run-python-sender
                        url wrong-authority receipt-path 30)]
          (is (false? (:timed-out? rejected)) (:output rejected))
          (is (not= 0 (:exit rejected))
              "a Python dial with the wrong target Authority succeeded")
          (is (empty? @accepted)
              "target mismatch reached blob authorization")
          (is (empty? @installed)
              "target mismatch reached durable storage"))
        (let [{:keys [exit output timed-out?]}
              (run-python-sender url server-authority receipt-path 30)]
          (is (false? timed-out?) output)
          (is (= 0 exit) output)
          (is (= (str "NETZ_MIXED_OK "
                      (identity/bytes->hex (:manifest-id split)))
                 (last (remove str/blank? (str/split-lines output)))))
          (let [receipt-exists?
                (Files/isRegularFile
                 receipt-path (make-array java.nio.file.LinkOption 0))]
            (is receipt-exists?
                "Python did not persist the signed JVM receipt")
            (when receipt-exists?
              (let [persisted (wire/decode (Files/readAllBytes receipt-path))
                    receipt (get persisted "receipt")]
                (is (true? (await (replication-wire/verify-envelope receipt))))
                (is (identity/bufs=
                     server-authority
                     (get-in receipt ["statement" "signer"]))))))
          (is (wait-for 10000 #(= 1 (count @installed)))
              "JVM did not report a durably installed Python object")
          (is (= 1 (count @accepted)))
          (is (identity/bufs=
               (:from-authority (first @accepted))
               (get-in (first @accepted) [:retention "signer"])))
          (let [inventory
                (await (storage/-load-staging
                        target (identity/seq->bytes (range 16 32))
                        (:manifest-id split) (:manifest split)))]
            (is (= "netz/store/fsync/1"
                   (get-in inventory [:installed :persistence])))
            (is (= (count (:pieces split)) (count (:held inventory)))))
          (is (= 1 (:installed (replication-runtime/stats runtime))))
          (is (= 0 (:invalid-signatures
                    (replication-runtime/stats runtime)))))
        (finally
          (replication-runtime/stop! runtime)
          (stop-node! server))))))

(deftest jvm-provider-transfers-into-python-durable-store
  (with-store-roots [source-root target-root]
    (let [port (swap! port-counter inc)
          url (str "ws://localhost:" port)
          capabilities (conj noise/default-capabilities
                             replication-wire/capability
                             overlay-runtime/extension-carrier-capability
                             wire/profile-capability)
          server-identity (<?? S (identity/generate-identity))
          server (start-server url server-identity capabilities)
          server-authority
          (identity/authority-id-bytes (:genesis server-identity))
          source (file-store/new-file-store source-root
                                            {:clock (constantly 1100)})
          split (blob/split (mixed-payload)
                            {:piece-size (bit-shift-left 1 20)})
          fetches (atom [])
          stored (atom [])
          failed (atom [])
          completion-path (.resolve source-root "receipt-journaled")
          admission-path
          (.resolve target-root ".netz-test-lifecycle-admission.cbor")
          runtime
          (replication-runtime/start!
           (:node server)
           {:store source
            :now-fn (constantly 1100)
            :authorize-fetch!
            (fn [{:keys [from-authority retention] :as request}]
              (swap! fetches conj request)
              (storage/chan-of
               (and
                (identity/bufs= python-authority from-authority)
                (identity/bufs= python-authority (get retention "signer"))
                (identity/bufs= (identity/seq->bytes (range 16))
                                (get retention "cluster"))
                (identity/bufs= (:manifest-id split)
                                (get retention "manifest"))
                (= 1 (get retention "layout_epoch")))))
            :on-stored!
            #(do
               (swap! stored conj %)
               (Files/write completion-path (byte-array [1])
                            (make-array java.nio.file.OpenOption 0)))
            :on-failed! #(swap! failed conj %)})]
      (install-source! source (identity/seq->bytes (range 16 32)) split)
      (try
        (let [{:keys [exit output timed-out?]}
              (run-python-receiver
               url server-authority target-root completion-path 30
               {:attempt 0 :stop-after-pieces 1})]
          (is (false? timed-out?) output)
          (is (= 0 exit) output)
          (is (= (str "NETZ_MIXED_FETCH_INTERRUPTED "
                      (identity/bytes->hex (:manifest-id split)))
                 (last (remove str/blank? (str/split-lines output)))))
          (is (wait-for 10000 #(= 1 (count @failed)))
              "JVM sender did not observe the interrupted Python process")
          (is (= :disconnected (:code (first @failed))))
          (let [python-store (file-store/new-file-store target-root)
                inventory
                (await (storage/-load-staging
                        python-store (identity/seq->bytes (range 32 48))
                        (:manifest-id split) (:manifest split)))]
            (is (nil? (:installed inventory)))
            (is (= 1 (count (:held inventory)))
                "the interrupted process did not leave one durable piece")))
        (let [{:keys [exit output timed-out?]}
              (run-python-receiver
               url server-authority target-root completion-path 30
               {:attempt 1})]
          (is (false? timed-out?) output)
          (is (= 0 exit) output)
          (is (= (str "NETZ_MIXED_FETCH_OK "
                      (identity/bytes->hex (:manifest-id split)))
                 (last (remove str/blank? (str/split-lines output)))))
          (is (wait-for 10000 #(= 1 (count @stored)))
              "JVM did not durably persist the Python replica receipt")
          (is (= 2 (count @fetches)))
          (is (= [0 1] (mapv #(get-in % [:frame "attempt"]) @fetches)))
          (is (not (identity/bufs=
                    (get-in (first @fetches) [:frame "session"])
                    (get-in (last @fetches) [:frame "session"])))
              "the retry reused its failed transfer session")
          (is (identity/bufs=
               python-authority (:from-authority (last @fetches))))
          (is (Files/isRegularFile
               admission-path (make-array java.nio.file.LinkOption 0))
              "Python did not durably record its admitted controller intent")
          (let [admitted (wire/decode (Files/readAllBytes admission-path))
                fetched (get-in (last @fetches) [:frame "retention"])]
            (is (true? (await (replication-wire/verify-envelope admitted))))
            (is (identity/bufs=
                 (wire/encode-canonical fetched)
                 (wire/encode-canonical admitted))
                "the fetch differed from Python's admitted retention envelope"))
          (let [python-store (file-store/new-file-store target-root)
                inventory
                (await (storage/-load-staging
                        python-store (identity/seq->bytes (range 32 48))
                        (:manifest-id split) (:manifest split)))]
            (is (= "netz/store/fsync/1"
                   (get-in inventory [:installed :persistence])))
            (is (= (count (:pieces split)) (count (:held inventory)))))
          (let [receipts (await (storage/-recorded-receipts source))]
            (is (= 1 (count receipts)))
            (is (= 1 (get-in (first receipts)
                             [:receipt "statement" "attempt"]))))
          (is (= 2 (:fetch-served (replication-runtime/stats runtime))))
          (is (= 1 (:stored (replication-runtime/stats runtime))))
          (is (= 1 (:failed (replication-runtime/stats runtime))))
          (is (= 2 (get-in (replication-runtime/states runtime)
                           [:sender :stats :pieces-sent]))
              "the resumed session retransmitted an already durable piece")
          (is (= 0 (:invalid-signatures
                    (replication-runtime/stats runtime)))))
        (finally
          (replication-runtime/stop! runtime)
          (stop-node! server))))))

(deftest node-receiver-pulls-portable-sync-from-jvm
  (let [url (str "ws://localhost:" (swap! port-counter inc))
        capabilities (conj noise/default-capabilities wire/profile-capability
                           overlay-runtime/extension-carrier-capability sync/capability)
        server-identity (<?? S (identity/generate-identity))
        authorized (atom 0)
        server (start-server
                url server-identity capabilities
                {:sync {:profiles {"netz/join-semilattice/1" (portable-join-profile)}
                        :applications {"demo/join" #{"jvm"}}}}
                (fn [{:keys [connection topic profile]}]
                  (let [allowed? (and (= "a8386e4e777c12bb11c3344e3ac87b490b1cc13f0128301d28d658d4651791f4"
                                         (:authority-id connection))
                                      (= "demo/join" topic)
                                      (= "netz/join-semilattice/1" profile))]
                    (when allowed? (swap! authorized inc))
                    allowed?)))]
    (try
      (let [process (.start (doto (ProcessBuilder.
                                   ^java.util.List
                                   [(or (System/getenv "NETZ_NODE") "node")
                                    "target/mixed-sync-client.js" url
                                    (identity/authority-id (:genesis server-identity))])
                              (.redirectErrorStream true)))
            output (future (slurp (.getInputStream process)))]
        (try
          (let [finished? (.waitFor process 30 TimeUnit/SECONDS)]
            (is finished? "Node sync process timed out")
            (when finished?
              (let [text (deref output 5000 "Node output did not close")]
                (is (= 0 (.exitValue process)) text)
                (is (= "NETZ_MIXED_SYNC_OK jvm,node"
                       (last (remove str/blank? (str/split-lines text)))) text))))
          (is (= 1 @authorized))
          (is (wait-for 5000 #(empty? (get-in (overlay-runtime/overlay-state (:node server))
                                              [:sync :sessions]))))
          (is (= 1 (get-in (overlay-runtime/overlay-state (:node server))
                           [:sync :stats :chunks-sent])))
          (finally (stop-process! process))))
      (finally (stop-node! server)))))

(deftest python-receiver-pulls-portable-sync-from-jvm
  (let [port (swap! port-counter inc)
        url (str "ws://localhost:" port)
        capabilities (conj noise/default-capabilities
                           wire/profile-capability
                           overlay-runtime/extension-carrier-capability
                           sync/capability)
        server-identity (<?? S (identity/generate-identity))
        server (start-server
                url server-identity capabilities
                {:sync {:profiles {"netz/join-semilattice/1"
                                   (portable-join-profile)}
                        :applications {"demo/join" #{"jvm"}}}}
                (fn [{:keys [connection topic profile]}]
                  (and (= python-sync-authority
                          (:authority-id connection))
                       (= "demo/join" topic)
                       (= "netz/join-semilattice/1" profile))))
        server-authority
        (identity/authority-id-bytes (:genesis server-identity))]
    (try
      (let [{:keys [exit output timed-out?]}
            (run-python-sync url server-authority 30)]
        (is (false? timed-out?) output)
        (is (= 0 exit) output)
        (is (= "NETZ_MIXED_SYNC_OK jvm,python"
               (last (remove str/blank? (str/split-lines output)))))
        (is (wait-for
             5000
             #(empty? (get-in (overlay-runtime/overlay-state (:node server))
                              [:sync :sessions])))
            "JVM portable sync sender did not complete")
        (is (= 1 (get-in (overlay-runtime/overlay-state (:node server))
                         [:sync :stats :chunks-sent]))))
      (finally
        (stop-node! server)))))

(deftest jvm-receiver-pulls-portable-sync-from-python
  (let [port (swap! port-counter inc)
        url (str "ws://localhost:" port)
        capabilities (conj noise/default-capabilities
                           wire/profile-capability
                           overlay-runtime/extension-carrier-capability
                           sync/capability)
        server-identity (<?? S (identity/generate-identity))
        server (start-server
                url server-identity capabilities
                {:sync {:profiles {"netz/join-semilattice/1"
                                   (portable-join-profile)}
                        :applications {"demo/join" #{"jvm"}}}})
        server-authority
        (identity/authority-id-bytes (:genesis server-identity))]
    (try
      (let [python-result
            (future (run-python-sync-sender url server-authority 30))]
        (is (wait-for
             10000
             #(contains? (overlay-runtime/connections (:peer server))
                         python-sync-peer-id))
            "Python sync sender did not authenticate")
        (let [facts (overlay-runtime/connection-info
                     (:peer server) python-sync-peer-id)]
          (is (= python-sync-authority (:authority-id facts)))
          (is (every? (:capabilities facts)
                      [wire/profile-capability
                       overlay-runtime/extension-carrier-capability
                       sync/capability])))
        (is (true? (overlay-runtime/start-sync!
                    (:node server) python-sync-peer-id "demo/join"
                    "netz/join-semilattice/1")))
        (is (wait-for
             10000
             #(= #{"jvm" "python"}
                 (get-in (overlay-runtime/overlay-state (:node server))
                         [:sync :applications "demo/join"])))
            "JVM receiver did not commit Python state")
        (let [{:keys [exit output timed-out?]}
              (deref python-result 35000
                     {:exit nil :output "Python sender future timed out"
                      :timed-out? true})]
          (is (false? timed-out?) output)
          (is (= 0 exit) output)
          (is (= "NETZ_MIXED_SYNC_SERVED"
                 (last (remove str/blank? (str/split-lines output))))))
        (is (= :synchronized
               (get-in (overlay-runtime/overlay-state (:node server))
                       [:sync :status "demo/join"])))
        (is (empty? (get-in (overlay-runtime/overlay-state (:node server))
                            [:sync :sessions]))))
      (finally
        (stop-node! server)))))

(deftest jvm-receiver-pulls-portable-sync-from-node
  (let [url (str "ws://localhost:" (swap! port-counter inc))
        capabilities (conj noise/default-capabilities wire/profile-capability
                           overlay-runtime/extension-carrier-capability sync/capability)
        server-identity (<?? S (identity/generate-identity))
        node-authority "a8386e4e777c12bb11c3344e3ac87b490b1cc13f0128301d28d658d4651791f4"
        node-peer (identity/authority-routing-id node-authority)
        server (start-server
                url server-identity capabilities
                {:sync {:profiles {"netz/join-semilattice/1" (portable-join-profile)}
                        :applications {"demo/join" #{"jvm"}}}})]
    (try
      (let [process (.start (doto (ProcessBuilder.
                                   ^java.util.List
                                   [(or (System/getenv "NETZ_NODE") "node")
                                    "target/mixed-sync-client.js" url
                                    (identity/authority-id (:genesis server-identity)) "serve"])
                              (.redirectErrorStream true)))
            output (future (slurp (.getInputStream process)))]
        (try
          (is (wait-for 10000 #(overlay-runtime/connection-info (:peer server) node-peer)))
          (let [facts (overlay-runtime/connection-info (:peer server) node-peer)]
            (is (= node-authority (:authority-id facts)))
            (is (overlay-runtime/portable-sync-connection? (:peer server) node-peer)))
          (is (overlay-runtime/start-sync! (:node server) node-peer "demo/join"
                                           "netz/join-semilattice/1"))
          (is (wait-for 10000 #(let [state (:sync (overlay-runtime/overlay-state (:node server)))]
                                 (and (= #{"jvm" "node"} (get-in state [:applications "demo/join"]))
                                      (= :synchronized (get-in state [:status "demo/join"]))
                                      (empty? (:sessions state))))))
          (let [out (.getOutputStream process)]
            (.write out (.getBytes "done\n" java.nio.charset.StandardCharsets/UTF_8))
            (.flush out))
          (let [finished? (.waitFor process 10 TimeUnit/SECONDS)]
            (is finished? "Node sender did not stop")
            (when finished?
              (let [text (deref output 5000 "Node output did not close")]
                (is (= 0 (.exitValue process)) text)
                (is (= "NETZ_MIXED_SYNC_SERVED"
                       (last (remove str/blank? (str/split-lines text)))) text))))
          (finally (stop-process! process))))
      (finally (stop-node! server)))))

(deftest python-client-repairs-an-offline-jvm-publication
  (let [port (swap! port-counter inc)
        url (str "ws://localhost:" port)
        topic "datahike/roots/python-demo"
        capabilities (conj noise/default-capabilities
                           wire/profile-capability
                           overlay-runtime/extension-carrier-capability
                           dissemination-wire/capability)
        server-identity (<?? S (identity/generate-identity))
        authorize? (fn [{:keys [authority-id]}]
                     (= python-sync-authority authority-id))
        server (start-server url server-identity capabilities nil nil authorize?)
        server-authority
        (identity/authority-id-bytes (:genesis server-identity))]
    (try
      ;; There is no subscriber yet, so success proves initial have/want repair
      ;; rather than merely exercising the live forwarding path.
      (is (overlay-runtime/publish!
           (:node server) topic (identity/utf8-bytes "root-42")))
      (let [{:keys [exit output timed-out?]}
            (run-python-dissemination url server-authority 30)]
        (is (false? timed-out?) output)
        (is (= 0 exit) output)
        (is (= "NETZ_MIXED_DISSEMINATION_OK root-42"
               (last (remove str/blank? (str/split-lines output))))))
      (finally
        (stop-node! server)))))
