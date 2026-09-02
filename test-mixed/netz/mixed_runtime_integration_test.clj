(ns netz.mixed-runtime-integration-test
  "A real Python sender interoperating with the JVM Netz receiver runtime."
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :refer [timeout]]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [kabel.http-kit :as http-kit]
            [kabel.middleware.cbor :refer [cbor]]
            [kabel.peer :as peer]
            [netz.identity :as identity]
            [netz.overlay.runtime :as overlay-runtime]
            [netz.replication.blob :as blob]
            [netz.replication.file-store :as file-store]
            [netz.replication.runtime :as replication-runtime]
            [netz.replication.storage :as storage]
            [netz.replication.wire :as replication-wire]
            [netz.transport.noise :as noise]
            [netz.transport.secure :as secure-overlay]
            [netz.wire :as wire]
            [superv.async :refer [<?? S]])
  (:import (java.nio.file Files Path)
           (java.util Comparator)
           (java.util.concurrent TimeUnit)))

(def ^:private port-counter (atom 49300))

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

(defn- start-server [url identity capabilities]
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
                      :overlay-opts
                      {:membership {:require-authority? true}}}))]
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
