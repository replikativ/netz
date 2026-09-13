(ns netz.datahike-process-experiment-test
  "Multi-process Datahike snapshot durability experiment over static EDN bootstrap."
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.store :as datahike-store]
            [konserve.memory :as memory]
            [netz.examples.datahike-replica :as demo]
            [netz.examples.snapshot-archive :as archive]
            [netz.examples.snapshot-store :as snapshot]
            [netz.examples.static-bootstrap :as bootstrap]
            [netz.identity :as identity]
            [netz.overlay.runtime :as overlay]
            [netz.replication.controller-runtime :as controller]
            [netz.replication.file-store :as file-store]
            [netz.replication.layout :as layout]
            [netz.replication.materialization :as materialization]
            [netz.replication.runtime :as transfer]
            [netz.replication.wire :as replication-wire]
            [netz.replication-process-fixture :as replication-fixture]
            [netz.replication-runtime-integration-test :as carrier]
            [netz.wire :as wire]
            [yggdrasil.adapters.datahike :as yggdrasil]
            [yggdrasil.hooks :as hooks])
  (:import (java.net ServerSocket)
           (java.nio.file Files Path)
           (java.util.concurrent TimeUnit)))

(defn- await [channel]
  (let [[value port] (async/alts!! [channel (async/timeout 60000)])]
    (when-not (= port channel) (throw (ex-info "Experiment operation timed out" {})))
    (if (instance? Throwable value) (throw value) value)))

(defn- wait-for! [message pred]
  (let [deadline (+ (System/currentTimeMillis) 60000)]
    (loop []
      (if (pred)
        true
        (do
          (when-not (< (System/currentTimeMillis) deadline)
            (throw (ex-info message {})))
          (Thread/sleep 25)
          (recur))))))

(defn- temp-root []
  (Files/createTempDirectory "netz-datahike-process-"
                             (make-array java.nio.file.attribute.FileAttribute 0)))

(defn- child-directory [^Path root name]
  (Files/createDirectory (.resolve root name)
                         (make-array java.nio.file.attribute.FileAttribute 0)))

(defn- free-url []
  (with-open [socket (ServerSocket. 0)]
    (str "ws://127.0.0.1:" (.getLocalPort socket))))

(defn- credentials-map [credentials]
  {:genesis (:genesis credentials)
   :private (identity/bytes->hex (get-in credentials [:operational :private]))
   :public (identity/bytes->hex (get-in credentials [:operational :public]))})

(defn- peer-config [index credentials url]
  {:authority-id (identity/authority-id (:genesis credentials))
   :peer-id (str (identity/peer-id (:genesis credentials)))
   :addresses [url]
   :group (str "site-" index)
   :domains {"owner" (str "operator-" index)
             "site" (str "site-" index)}
   :roles [:replica]})

(defn- layout-member [peer]
  {"authority" (identity/hex->bytes (:authority-id peer))
   "domains" (:domains peer) "enabled" true})

(defn- bootstrap-map [network writer-authority peers layout-envelopes now]
  {:v 1 :network network :issued-at (- now 1000)
   :expires-at (+ now (* 60 60 1000))
   :controllers [writer-authority]
   :peers peers
   :layout-chain-cbor-hex
   (mapv #(identity/bytes->hex (wire/encode-canonical %)) layout-envelopes)})

(defn- bootstrap-trust [network writer-authority]
  {:network network :controllers #{writer-authority}})

(defn- replica-config [network writer-authority]
  {:expected-network network :controller-authorities [writer-authority]})

(defn- write-bootstrap! [path value]
  (spit (str path) (pr-str value)))

(defn- launch-replica! [^Path root index config]
  (let [config-path (.resolve root (str "replica-" index ".edn"))
        log (.resolve root (str "replica-" index ".log"))
        java (str (System/getProperty "java.home") java.io.File/separator
                  "bin" java.io.File/separator "java")]
    (spit (str config-path) (pr-str config))
    (let [process (.start
                   (doto (ProcessBuilder.
                          ^java.util.List
                          [java "-Xmx256m" "-Xss4m"
                           (str "-Djava.io.tmpdir=" (System/getProperty "java.io.tmpdir"))
                           "-cp" (System/getProperty "java.class.path")
                           "clojure.main" "-m"
                           "netz.datahike-replica-process-fixture"
                           (str config-path)])
                     (.redirectErrorStream true)
                     (.redirectOutput (.toFile log))))
          input (io/writer (.getOutputStream process))]
      {:process process :input input :log log :index index})))

(defn- await-marker! [{:keys [^Process process log]} marker]
  (let [expected (str "NETZ-DATAHIKE-REPLICA " (pr-str marker))
        deadline (+ (System/currentTimeMillis) 120000)]
    (loop []
      (let [output (if (Files/exists log (make-array java.nio.file.LinkOption 0))
                     (slurp (str log)) "")]
        (cond
          (str/includes? output expected) true
          (not (.isAlive process))
          (throw (ex-info "Replica exited before milestone"
                          {:marker marker :output output}))
          (>= (System/currentTimeMillis) deadline)
          (throw (ex-info "Replica milestone timeout"
                          {:marker marker :output output}))
          :else (do (Thread/sleep 50) (recur)))))))

(defn- command! [{:keys [^java.io.Writer input]} command]
  (.write input (str command "\n"))
  (.flush input))

(defn- stop-process! [{:keys [^Process process input]}]
  (when (.isAlive process)
    (try (command! {:input input} "stop") (catch Throwable _))
    (when-not (.waitFor process 15 TimeUnit/SECONDS)
      (.destroyForcibly process)
      (.waitFor process 15 TimeUnit/SECONDS)))
  (try (.close ^java.io.Writer input) (catch Throwable _)))

(defn- kill-process! [{:keys [^Process process]}]
  (when (.isAlive process)
    (.destroyForcibly process)
    (.waitFor process 15 TimeUnit/SECONDS)))

(defn- connected-authorities [node]
  (into #{}
        (keep #(some-> (overlay/connection-info (:peer node) %) :authority-id))
        (overlay/connections (:peer node))))

(defn- retention [writer-authority cluster packed epoch revision now]
  {"v" 1 "type" "blob/retention" "cluster" cluster
   "privacy_domain" (identity/seq->bytes (range 16 32))
   "revision" revision
   "manifest" (get-in packed [:split :manifest-id])
   "object" (get-in packed [:split :object-id])
   "layout_epoch" epoch "replicas" 3 "write_acks" 2
   "placement_minimum_domains" {"owner" 3 "site" 3}
   "ack_minimum_domains" {"owner" 2 "site" 2}
   "issued_at" now "retain_until" (+ now (* 60 60 1000))
   "signer" (identity/hex->bytes writer-authority)})

(deftest static-bootstrap-replicates-a-queryable-datahike-snapshot-across-processes
  (let [root (temp-root)
        bootstrap-path (.resolve root "bootstrap.edn")
        processes (atom [])
        writer-node (atom nil)
        source-runtime (atom nil)
        controller-runtime (atom nil)
        connection (atom nil)
        system (atom nil)
        hook (atom nil)]
    (try
      (let [now (System/currentTimeMillis)
            writer (await (identity/generate-identity))
            writer-authority (identity/authority-id (:genesis writer))
            receiver-identities (vec (repeatedly 4 #(await (identity/generate-identity))))
            urls (vec (repeatedly 4 free-url))
            peers (mapv peer-config (range 4) receiver-identities urls)
            cluster (identity/seq->bytes (range 16))
            members (fn [indices]
                      (->> indices (map #(layout-member (nth peers %)))
                           (sort-by #(identity/bytes->hex (get % "authority"))) vec))
            initial-layout {"v" 1 "type" "blob/layout" "cluster" cluster
                            "epoch" 0 "placement" "netz/blob-placement/2"
                            "seed" (identity/seq->bytes (repeat 32 42))
                            "previous" (identity/seq->bytes (repeat 32 0))
                            "members" (members [0 1 2])
                            "issued_at" (- now 1000)
                            "signer" (identity/hex->bytes writer-authority)}
            initial-envelope (await (replication-wire/sign-statement writer initial-layout))
            initial-bootstrap (bootstrap-map "datahike-process-experiment"
                                             writer-authority peers [initial-envelope] now)
            receiver-roots (mapv #(child-directory root (str "store-" %)) (range 4))]
        (write-bootstrap! bootstrap-path initial-bootstrap)
        (testing "the fetched file cannot replace the local trust root"
          (let [rejected
                (async/<!!
                 (bootstrap/load-and-verify!
                  bootstrap-path now
                  {:network "another-network"
                   :controllers #{writer-authority}}))]
            (is (= :netz.bootstrap/trust-mismatch
                   (:type (ex-data rejected))))))
        (doseq [index [0 1 2]]
          (let [process
                (launch-replica!
                 root index
                 (merge (credentials-map (nth receiver-identities index))
                        {:root (str (nth receiver-roots index))
                         :url (nth urls index)
                         :bootstrap-path (str bootstrap-path)}
                        (replica-config "datahike-process-experiment"
                                        writer-authority)))]
            (swap! processes conj process)
            (await-marker! process [:ready (:authority-id (nth peers index))])))
        (let [trust (bootstrap-trust "datahike-process-experiment"
                                     writer-authority)
              loaded (await (bootstrap/load-and-verify!
                             bootstrap-path (System/currentTimeMillis) trust))
              node (carrier/start-client nil writer {:seeds (bootstrap/seeds loaded)}
                                         replication-fixture/capabilities)
              source-root (child-directory root "writer-store")
              source (file-store/new-file-store source-root
                                                {:durable-ancestor source-root})
              config (demo/source-config)
              _ (d/create-database config)
              conn (d/connect config)
              reader-config (:config @conn)
              ygg (yggdrasil/create conn {:system-name "netz-process-experiment"})
              events (atom [])
              installed-hook (hooks/install-commit-hook! nil ygg #(swap! events conj %))]
          (reset! writer-node node)
          (reset! connection conn)
          (reset! system ygg)
          (reset! hook installed-hook)
          (d/transact conn (mapv #(hash-map :note/text (str "note-" %)) (range 200)))
          (let [export (demo/capture! conn (last @events) writer [])
                packed (archive/prepare export)
                _ (archive/stage-local! source packed)
                layout-state (atom (:layout-state loaded))
                failures (atom [])
                controller-ref (atom nil)
                active-node (atom node)
                active-sender (atom nil)
                sender
                (transfer/start!
                 (:node node)
                 {:store source :replay-receipts? false
                  :on-stored! #(when-let [ctx @controller-ref]
                                 (controller/notify-stored! ctx %))
                  :on-failed! #(swap! failures conj %)})
                ctx
                (controller/start!
                 {:store source :cluster cluster
                  :controllers [(identity/hex->bytes writer-authority)]
                  :layout-state-fn #(deref layout-state)
                  :dispatch-offer!
                  (fn [{:keys [authority offer]}]
                    (let [node @active-node
                          sender @active-sender
                          target (identity/bytes->hex authority)
                          connections
                          (filter #(= target
                                      (:authority-id
                                       (overlay/connection-info (:peer node) %)))
                                  (overlay/connections (:peer node)))]
                      (and (= 1 (count connections))
                           (transfer/replicate! sender (first connections)
                                                authority offer))))})
                initial-retention (retention writer-authority cluster packed 0 1 now)
                initial-retention-envelope
                (await (replication-wire/sign-statement writer initial-retention))
                object-id (identity/bytes->hex (get-in packed [:split :object-id]))]
            (reset! source-runtime sender)
            (reset! active-sender sender)
            (reset! controller-runtime ctx)
            (reset! controller-ref ctx)
            (is (true? (await (controller/ready ctx))))
            (wait-for! "writer did not connect to the three bootstrap replicas"
                       #(every? (connected-authorities node)
                                (map :authority-id (take 3 peers))))
            (is (= :ok (await (controller/begin!
                               ctx initial-envelope initial-retention-envelope
                               (get-in packed [:split :manifest])))))
            (let [confirmed (await (controller/await-durable!
                                    ctx object-id {:timeout-ms 30000 :poll-ms 20}))]
              (is (= :confirmed (:status confirmed)))
              (is (= 2 (:required confirmed)))
              (is (<= 2 (count (:acknowledged confirmed)))))
            (wait-for! "initial layout never converged to N=3"
                       #(:complete? (controller/durability-status ctx object-id)))

            (testing "one failed site is replaced under a signed successor layout"
              (kill-process! (first @processes))
              (let [successor (assoc initial-layout
                                     "epoch" 1
                                     "previous" (layout/address initial-layout)
                                     "members" (members [1 2 3])
                                     "issued_at" (System/currentTimeMillis))
                    successor-envelope
                    (await (replication-wire/sign-statement writer successor))
                    successor-bootstrap
                    (bootstrap-map "datahike-process-experiment" writer-authority
                                   peers [initial-envelope successor-envelope]
                                   (System/currentTimeMillis))]
                (write-bootstrap! bootstrap-path successor-bootstrap)
                (doseq [process (rest @processes)]
                  (command! process "reload")
                  (await-marker! process [:reloaded 1]))
                (let [replacement
                      (launch-replica!
                       root 3
                       (merge (credentials-map (nth receiver-identities 3))
                              {:root (str (nth receiver-roots 3))
                               :url (nth urls 3)
                               :bootstrap-path (str bootstrap-path)}
                              (replica-config "datahike-process-experiment"
                                              writer-authority)))]
                  (swap! processes conj replacement)
                  (await-marker! replacement [:ready (:authority-id (nth peers 3))]))
                ;; Restart the writer overlay from the new static bootstrap.
                ;; This avoids treating an old failed-dial backoff as topology
                ;; state and exercises controller/receipt survival separately
                ;; from transport connection survival.
                (transfer/stop! @active-sender)
                (carrier/stop-node! @active-node)
                (let [successor-loaded
                      (await (bootstrap/load-and-verify!
                              bootstrap-path (System/currentTimeMillis) trust))
                      next-node
                      (carrier/start-client
                       nil writer {:seeds (bootstrap/seeds successor-loaded)}
                       replication-fixture/capabilities)
                      next-sender
                      (transfer/start!
                       (:node next-node)
                       {:store source :replay-receipts? false
                        :on-stored! #(when-let [controller @controller-ref]
                                       (controller/notify-stored! controller %))
                        :on-failed! #(swap! failures conj %)})]
                  (reset! active-node next-node)
                  (reset! active-sender next-sender)
                  (reset! writer-node next-node)
                  (reset! source-runtime next-sender))
                (let [accepted (layout/reconcile @layout-state successor)
                      successor-retention
                      (retention writer-authority cluster packed 1 2
                                 (System/currentTimeMillis))
                      successor-retention-envelope
                      (await (replication-wire/sign-statement
                              writer successor-retention))]
                  (is (= :accepted (:status accepted)))
                  (reset! layout-state (:state accepted))
                  (wait-for! "writer did not connect to replacement replica"
                             #(contains? (connected-authorities @active-node)
                                         (:authority-id (nth peers 3))))
                  (is (= :ok (await (controller/revise!
                                     ctx successor-envelope
                                     successor-retention-envelope))))
                  (wait-for! "successor layout never converged to N=3"
                             #(let [status (controller/durability-status ctx object-id)]
                                (and (= 1 (:layout-epoch status))
                                     (:complete? status))))
                  (is (empty? @failures)))))

            (testing "a surviving replica restores a queryable DB without the writer"
              (controller/stop! ctx)
              (reset! controller-runtime nil)
              (transfer/stop! @active-sender)
              (reset! source-runtime nil)
              (carrier/stop-node! @active-node)
              (reset! writer-node nil)
              (hooks/remove-commit-hook! nil ygg installed-hook)
              (reset! hook nil)
              (d/release conn)
              (reset! connection nil)
              (d/delete-database config)
              (let [survivor (second @processes)]
                (stop-process! survivor)
                (let [store-root (nth receiver-roots 1)
                      store (file-store/new-file-store
                             store-root {:durable-ancestor store-root})
                      bytes (await (materialization/read-installed!
                                    store (:binding packed)))
                      restored (archive/restore bytes (:statement export))
                      cache (await (memory/new-mem-store))
                      target (datahike-store/add-cache-and-handlers
                              (await (memory/new-mem-store)) reader-config)
                      opened (snapshot/open!
                              (:statement restored)
                              {:publisher (:netz/publisher (:statement export))
                               :system (:netz/system (:statement export))
                               :ref-name (:netz/ref (:statement export))}
                              cache
                              #(get (:blocks restored) (identity/bytes->hex %))
                              demo/decode)
                      database (demo/install! opened target)]
                  (is (= 200 (d/q '[:find (count ?e) .
                                     :where [?e :note/text]] database)))))))))
      (finally
        (when (and @hook @system) (hooks/remove-commit-hook! nil @system @hook))
        (when @connection (d/release @connection))
        (when @controller-runtime (controller/stop! @controller-runtime))
        (when @source-runtime (transfer/stop! @source-runtime))
        (when @writer-node (carrier/stop-node! @writer-node))
        (doseq [process @processes] (stop-process! process))
        (println "Datahike process experiment artifacts:" (str root))))))
