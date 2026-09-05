(ns netz.examples.datahike-replica
  "Two independently stored Datahike snapshots over authenticated Netz peers."
  (:require [boring.core :as boring]
            [clojure.core.async :as a]
            [clojure.set :as set]
            [datahike.api :as d]
            [datahike.cbor :as dcbor]
            [datahike.store :as ds]
            [kabel.http-kit :as http-kit]
            [kabel.middleware.cbor :refer [cbor]]
            [kabel.peer :as peer]
            [kabel.pubsub :as ps]
            [kabel.pubsub.protocol :as protocol]
            [konserve.core :as k]
            [konserve.filestore :as filestore]
            [konserve-sync.pubsub :as ks]
            [konserve-sync.walkers.datahike :as walker]
            [netz.content-runtime :as content]
            [netz.content-wire :as content-wire]
            [netz.examples.snapshot-store :as snapshot]
            [netz.identity :as id]
            [netz.overlay.runtime :as rt]
            [netz.pubsub :as mesh]
            [netz.snapshot-ref :as ref]
            [netz.snapshot-ref.pubsub :as refs]
            [netz.transport.noise :as noise]
            [netz.transport.secure :as secure]
            [yggdrasil.adapters.datahike :as ygg]
            [yggdrasil.hooks :as hooks]
            [superv.async :refer [S]])
  (:import (java.net ServerSocket)
           (java.nio.file Files)))

(def topic "netz/demo/datahike-roots")
(def await! snapshot/await!)
(def registry (dcbor/registry {:resolve-storage (constantly nil)}))
(defn encode [value] (boring/encode value {:registry registry :stringref false :profile :canonical}))
(defn decode [bytes] (boring/decode bytes {:registry registry}))

(defn capture!
  "Capture one exact commit, without following the mutable branch head.
   This prototype supports primary PSS indexes only, with no store-ref blobs."
  [connection event authority previous]
  (let [store (:store @connection)
        commit (java.util.UUID/fromString (:snapshot-id event))
        raw (await! (k/get store commit))
        schema (or (:schema raw)
                   (:schema (await! (k/get store (:schema-meta-key raw)))))]
    (when (or (seq (:secondary-index-keys raw))
              (some #(= :db.type/store-ref (:db/valueType %)) (vals schema)))
      (throw (ex-info "Demo walker does not cover secondary indexes or store-ref blobs" {})))
    (let [pinned (snapshot/view [:db :branches]
                                #(case % :db raw :branches #{:db}))
          ;; The walker reads root cells from the pinned view and nodes from
          ;; the original source. It must never chase the changing :db pointer.
          view (assoc pinned :read-value
                      #(case % :db raw :branches #{:db} (await! (k/get store %)))
                      :metadata (merge (:metadata pinned)
                                       (into {} (map (juxt :key identity)
                                                     (await! (k/keys store))))))
          keys (->> (await! (walker/datahike-walk-fn view {}))
                    (remove #{:db :branches}) (cons commit) distinct vec)
          signed (await! (ref/sign-dag-event authority "datahike/commit-v1" event))]
      (snapshot/export! store keys commit signed authority encode previous))))

(defn install!
  "Use konserve-sync to pull from the Netz-backed view. Commit record goes last.
   No branch pointer is advanced; the caller explicitly chooses a verified DAG head."
  [opened target]
  (let [{:keys [source root-key root-statement]} opened]
    (when-not (and (= "datahike/commit-v1" (:netz/profile root-statement))
                   (= (str root-key) (String. ^bytes (:netz/snapshot root-statement) "UTF-8")))
      (throw (ex-info "Datahike commit and signed reference disagree" {})))
    (let [sender (ks/server-store-strategy source
                                           {:key-sort-fn #(if (= % root-key) 1 0)})
          receiver (ks/store-sync-strategy target {})
          produced (protocol/-handshake-items sender {})
          ;; Released konserve-sync 0.1.40 uses the legacy item channel. The
          ;; signed finite catalogue independently proves completeness below.
          stream (if (map? produced) produced {:items produced})
          expected-keys (set (map :key (await! (k/keys source))))
          seen (atom #{})]
      (try
        (loop []
          (when-let [item (await! (:items stream))]
            (when (or (not (contains? expected-keys (:key item)))
                      (contains? @seen (:key item)))
              (throw (ex-info "Unexpected or duplicate snapshot key" {:key (:key item)})))
            (when (and (= root-key (:key item))
                       (not= @seen (disj expected-keys root-key)))
              (throw (ex-info "Snapshot root arrived before its complete closure" {})))
            ;; An upstream channel-delivered error must not be treated as a value.
            (when (instance? Throwable (:value item)) (throw (:value item)))
            (if (await! (k/exists? target (:key item)))
              (when-not (id/bufs= (encode (:value item))
                                  (encode (await! (k/get target (:key item)))))
                (throw (ex-info "Conflicting immutable snapshot key" {:key (:key item)})))
              (let [result (await! (protocol/-apply-handshake-item receiver item))]
                (when-not (= true (:ok result))
                  (throw (ex-info "Konserve snapshot install failed" {:result result})))
                (when-not (id/bufs= (encode (:value item))
                                    (encode (await! (k/get target (:key item)))))
                  (throw (ex-info "Snapshot write failed read-back verification" {:key (:key item)})))))
            (swap! seen conj (:key item))
            (recur)))
        (when-let [completion (:completion stream)]
          (let [result (await! completion)]
            (when-not (= true (:ok result))
              (throw (ex-info "Konserve snapshot source failed" {:result result})))))
        (when-not (= expected-keys @seen)
          (throw (ex-info "Incomplete snapshot source" {:missing (set/difference expected-keys @seen)})))
        (finally (a/close! (:items stream)))))
    (let [db (d/commit-as-db target root-key)]
      (when-not (= root-key (get-in db [:meta :datahike/commit-id]))
        (throw (ex-info "Installed database has the wrong commit" {})))
      ;; Force all primary datoms before declaring the snapshot usable.
      (doall (d/datoms db :eavt))
      db)))

(defn start-node! [identity url seed]
  (let [peer-id (id/peer-id (:genesis identity))
        [overlay install-overlay!] (rt/deferred-middleware)
        app (comp (ps/make-pubsub-peer-middleware {}) overlay)
        opts {:transport-middleware
              (secure/middleware {:identity identity :addresses (if url [url] [])
                                  :seq-no 1
                                  :capabilities (conj noise/default-capabilities
                                                      content-wire/capability)})}
        socket (if url
                 (peer/server-peer S (http-kit/create-http-kit-handler! S url peer-id)
                                   peer-id app cbor (atom {}) (atom {}) opts)
                 (peer/client-peer S peer-id app cbor (atom {}) (atom {}) opts))
        running (await! (secure/start!
                         S socket {:identity identity :addresses (if url [url] [])
                                   :topics #{topic}
                                   :seeds (if seed [seed] [])
                                   :overlay-opts {:membership {:require-authority? true
                                                               :dial-interval-ms 20}}}))]
    (install-overlay! (:middleware running))
    (mesh/install! S socket running)
    (when url (await! (peer/start socket)))
    {:id peer-id :identity identity :peer socket :running running}))

(defn stop-node! [{:keys [running peer]}]
  (rt/stop! running)
  (await! (peer/stop peer)))

(defn wait-for! [pred]
  (let [deadline (+ (System/currentTimeMillis) 15000)]
    (loop []
      (when-not (pred)
        (when (> (System/currentTimeMillis) deadline)
          (throw (ex-info "Peer convergence timed out" {})))
        (Thread/sleep 20)
        (recur)))))

(defn free-url []
  (with-open [socket (ServerSocket. 0)]
    (str "ws://127.0.0.1:" (.getLocalPort socket))))

(defn source-config []
  {:store {:backend :file :id (random-uuid)
           :path (str (.resolve (Files/createTempDirectory
                                 "netz-datahike-demo-"
                                 (make-array java.nio.file.attribute.FileAttribute 0)) "db"))}
   :schema-flexibility :read :keep-history? true})

(defn run-demo!
  "Run actual WebSocket/Noise peers, then stop the publisher and query B locally."
  []
  (let [config (source-config)
        _ (d/create-database config)
        connection (d/connect config)
        system (ygg/create connection {:system-name "netz-replica-demo"})
        events (atom [])
        hook (hooks/install-commit-hook! nil system #(swap! events conj %))
        authority (await! (id/generate-identity))
        reader-id (await! (id/generate-identity))
        url (free-url)
        server (start-node! authority url nil)
        publisher (id/authority-id (:genesis authority))
        seed {:peer-id (:id server) :authority-id publisher :addresses [url] :group "seed"}
        client (atom (start-node! reader-id nil seed))
        blocks (atom {})
        server-stopped? (atom false)
        source-released? (atom false)]
    (try
      (d/transact connection (mapv #(hash-map :note/text (str "note-" %)) (range 1200)))
      (let [export (capture! connection (last @events) authority [])
            statement (:statement export)
            system-id (:netz/system statement)
            ref-name (:netz/ref statement)
            expected {:publisher publisher :system system-id :ref-name ref-name}
            sender-state (atom (ref/make-state))
            receiver-state (atom (ref/make-state))
            strategy-opts {:profile snapshot/catalogue-profile :system system-id
                           :ref ref-name :publisher publisher}
            sender (refs/strategy (assoc strategy-opts :state sender-state))
            receiver (refs/strategy (assoc strategy-opts :state receiver-state))
            cache-path (:path (:store (source-config)))
            target-path (:path (:store (source-config)))
            reader-config (:config @connection)
            open-target #(ds/add-cache-and-handlers
                          (await! (filestore/connect-fs-store target-path)) reader-config)
            cache (atom (await! (filestore/connect-fs-store cache-path)))
            target (atom (open-target))
            requester (atom (content/start-requester! (:running @client)))
            fetches (atom 0)
            fetch! (fn [cid]
                     (swap! fetches inc)
                     (let [result (await! ((:fetch! @requester) (:id server) cid))]
                       (when-not (= :ok (:status result))
                         (throw (ex-info "Netz object fetch failed" result)))
                       (:data result)))
            subscribe! (fn []
                         (wait-for! #(contains? (rt/connections (:peer @client)) (:id server)))
                         (let [result (await! (ps/subscribe! (:peer @client) #{topic}
                                                             {:strategies {topic receiver}}))]
                           (when (:error result)
                             (throw (ex-info "Root subscription failed" result)))))
            install-head! (fn [expected-statement]
                            (let [key (ref/snapshot-key expected-statement)]
                              (wait-for! #(= #{key}
                                             (ref/heads @receiver-state snapshot/catalogue-profile
                                                        system-id ref-name)))
                              (let [received (first (filter #(= key (ref/snapshot-key %))
                                                            (ref/statements-since @receiver-state
                                                                                  snapshot/catalogue-profile
                                                                                  system-id ref-name #{})))]
                                (install! (snapshot/open! received expected @cache fetch! decode) @target))))
            publish! (fn [export]
                       (swap! blocks merge (:blocks export))
                       (let [statement (:statement export)
                             [next-state outcome] (await! (ref/accept-verified @sender-state statement))]
                         (when-not (= :accepted outcome)
                           (throw (ex-info "Publisher rejected its catalogue" {:outcome outcome})))
                         (reset! sender-state next-state)
                         (ps/publish! (:peer server) topic (ref/encode statement))
                         statement))]
        (reset! blocks (:blocks export))
        (content/start-service! (:running server)
                                {:authorize? #(= (id/authority-id (:genesis reader-id))
                                                 (:authority-id %))
                                 :get-block #(get @blocks (id/bytes->hex %))})
        (reset! sender-state (first (await! (ref/accept-verified @sender-state statement))))
        (ps/register-topic! (:peer server) topic {:strategy sender})
        (subscribe!)
        (let [first-db (install-head! statement)
              count-before (d/q '[:find (count ?e) . :where [?e :note/text]] first-db)
              _ (d/transact connection [{:note/text "live"}])
              live-ref (publish! (capture! connection (last @events) authority [(:netz/snapshot statement)]))
              live-db (install-head! live-ref)
              live-count (d/q '[:find (count ?e) . :where [?e :note/text]] live-db)]
          (stop-node! @client)
          (reset! client nil)
          ;; Two commits while B is offline. Both signed references remain in
          ;; A's bounded catch-up history, even though live delivery is lost.
          (d/transact connection [{:note/text "offline-1"}])
          (let [missed (publish! (capture! connection (last @events) authority [(:netz/snapshot live-ref)]))
                _ (d/transact connection [{:note/text "offline-2"}])
                latest (publish! (capture! connection (last @events) authority [(:netz/snapshot missed)]))]
            (reset! receiver-state (ref/pin (ref/make-state) snapshot/catalogue-profile
                                            system-id ref-name publisher))
            (reset! cache (await! (filestore/connect-fs-store cache-path)))
            (reset! target (open-target))
            (reset! client (start-node! reader-id nil seed))
            (reset! requester (content/start-requester! (:running @client)))
            (subscribe!)
            (let [db (install-head! latest)
                  catch-up-count (d/q '[:find (count ?e) . :where [?e :note/text]] db)]
          ;; The reader must remain independent after source and network shutdown.
              (stop-node! @client)
              (reset! client nil)
              (stop-node! server)
              (reset! server-stopped? true)
              (hooks/remove-commit-hook! nil system hook)
              (d/release connection)
              (reset! source-released? true)
              (d/delete-database config)
              (let [cold-db (d/commit-as-db (open-target) (get-in db [:meta :datahike/commit-id]))
                    result {:rows-before count-before
                            :rows-live live-count
                            :rows-caught-up catch-up-count
                            :rows-offline (d/q '[:find (count ?e) . :where [?e :note/text]] cold-db)
                            :objects-fetched @fetches
                            :replica-path target-path
                            :commit-id (get-in cold-db [:meta :datahike/commit-id])}]
                (when-not (= [1200 1201 1203 1203]
                             (mapv result [:rows-before :rows-live :rows-caught-up :rows-offline]))
                  (throw (ex-info "Replica query differs from source" result)))
                result)))))
      (finally
        (try (when @client (stop-node! @client))
             (finally
               (when-not @server-stopped? (stop-node! server))
               (when-not @source-released?
                 (hooks/remove-commit-hook! nil system hook)
                 (d/release connection)
                 (d/delete-database config))))))))

(defn -main [& _]
  (try (prn (run-demo!))
       (finally (shutdown-agents))))
