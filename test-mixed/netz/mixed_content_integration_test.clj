(ns netz.mixed-content-integration-test
  (:require [clojure.core.async :refer [timeout]]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [kabel.http-kit :as http-kit]
            [kabel.middleware.cbor :refer [cbor]]
            [kabel.peer :as peer]
            [netz.content-runtime :as content-runtime]
            [netz.content-wire :as content-wire]
            [netz.identity :as identity]
            [netz.overlay.runtime :as overlay-runtime]
            [netz.transport.noise :as noise]
            [netz.transport.secure :as secure-overlay]
            [netz.wire :as wire]
            [superv.async :refer [<?? S]])
  (:import (java.util.concurrent TimeUnit)))

(def ^:private port-counter (atom 49400))
(def ^:private data
  (identity/utf8-bytes "portable content over authenticated Kabel"))
(def ^:private cid (content-wire/raw-cid data))

(defn- stop-process! [^Process process]
  (when (.isAlive process)
    (.destroy process)
    (when-not (.waitFor process 1 TimeUnit/SECONDS)
      (.destroyForcibly process))))

(deftest python-fetches-verified-jvm-content
  (let [port (swap! port-counter inc)
        url (str "ws://localhost:" port)
        capabilities (conj noise/default-capabilities
                           content-wire/capability
                           overlay-runtime/extension-carrier-capability
                           wire/profile-capability)
        server-identity (<?? S (identity/generate-identity))
        server-id (identity/peer-id (:genesis server-identity))
        server-authority
        (identity/authority-id-bytes (:genesis server-identity))
        [app install!] (overlay-runtime/deferred-middleware)
        socket (peer/server-peer
                S (http-kit/create-http-kit-handler! S url server-id)
                server-id app cbor (atom {}) (atom {})
                {:transport-middleware
                 (secure-overlay/middleware
                  {:identity server-identity :addresses [url] :seq-no 1
                   :capabilities capabilities :required capabilities})})
        node (<?? S (secure-overlay/start!
                     S socket {:identity server-identity :addresses [url]
                               :topics #{}
                               :overlay-opts
                               {:membership {:require-authority? true}}}))
        authorized (atom [])
        stop-content
        (content-runtime/start-service!
         node {:authorize? (fn [context]
                             (swap! authorized conj context)
                             (= :content/read (:op context)))
               :get-block (fn [requested]
                            (when (identity/bufs= cid requested) data))})]
    (install! (:middleware node))
    (<?? S (peer/start socket))
    (let [python (or (System/getenv "NETZ_PYTHON") "python3")
          process (.start
                   (doto (ProcessBuilder.
                          ^java.util.List
                          [python "test-mixed/python/mixed_content_fetcher.py"
                           "--uri" url "--remote-authority"
                           (identity/bytes->hex server-authority)])
                     (.redirectErrorStream true)))
          output (future (slurp (.getInputStream process)))]
      (try
        (is (.waitFor process 30 TimeUnit/SECONDS))
        (is (= 0 (.exitValue process)) @output)
        (is (= "NETZ_MIXED_CONTENT_OK"
               (last (remove str/blank? (str/split-lines @output)))))
        (is (= 1 (count @authorized)))
        (finally
          (stop-process! process)
          (stop-content)
          (overlay-runtime/stop! node)
          (<?? S (peer/stop socket))
          (<?? S (timeout 50)))))))
