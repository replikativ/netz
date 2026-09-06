(ns netz.replica-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :as a]
            [datahike.api :as d]
            [datahike.store :as ds]
            [kabel.pubsub.protocol :as protocol]
            [konserve.core :as k]
            [konserve.filestore :as filestore]
            [konserve.memory :as memory]
            [netz.examples.datahike-replica :as demo]
            [netz.examples.snapshot-store :as snapshot]
            [netz.content-wire :as content]
            [netz.identity :as id]
            [netz.snapshot-ref :as ref]
            [yggdrasil.adapters.datahike :as ygg]
            [yggdrasil.hooks :as hooks]))

(def await! snapshot/await!)

(defn fixture! [f]
  (let [config (demo/source-config)
        _ (d/create-database config)
        conn (d/connect config)
        system (ygg/create conn {:system-name "replica-recovery-test"})
        events (atom [])
        hook (hooks/install-commit-hook! nil system #(swap! events conj %))
        authority (await! (id/generate-identity))]
    (try
      (d/transact conn (mapv #(hash-map :note/text (str "note-" %)) (range 1200)))
      (f {:connection conn :events events :authority authority
          :export (demo/capture! conn (last @events) authority [])})
      (finally
        (hooks/remove-commit-hook! nil system hook)
        (d/release conn)
        (d/delete-database config)))))

(defn expected [statement]
  {:publisher (:netz/publisher statement)
   :system (:netz/system statement) :ref-name (:netz/ref statement)})

(deftest authenticated-network-replica-remains-queryable-offline
  (let [result (demo/run-demo!)]
    (is (= 1200 (:rows-before result)))
    (is (= 1201 (:rows-live result)))
    (is (= 1203 (:rows-caught-up result) (:rows-offline result)))
    (is (> (:objects-fetched result) 3))
    (is (uuid? (:commit-id result)))))

(deftest unauthorized-and-corrupt-catalogues-do-not-expose-a-store
  (fixture!
   (fn [{:keys [export]}]
     (let [statement (:statement export)
           cache (await! (memory/new-mem-store))
           reads (atom 0)
           fetch! #(do (swap! reads inc) (get (:blocks export) (id/bytes->hex %)))]
       (is (thrown-with-msg? Exception #"Unauthorized"
                             (snapshot/open! statement
                                             (assoc (expected statement) :publisher (apply str (repeat 64 "0")))
                                             cache fetch! demo/decode)))
       (is (zero? @reads))
       (is (thrown-with-msg? Exception #"verification"
                             (snapshot/open! statement (expected statement) cache
                                             (constantly (id/utf8-bytes "corrupt")) demo/decode)))
       (is (empty? (await! (k/keys cache))))))))

(deftest interrupted-install-reuses-verified-objects-and-never-publishes-a-partial-root
  (fixture!
   (fn [{:keys [connection export]}]
     (let [statement (:statement export)
           cache-path (get-in (demo/source-config) [:store :path])
           cache (await! (filestore/connect-fs-store cache-path))
           target (ds/add-cache-and-handlers (await! (memory/new-mem-store)) (:config @connection))
           reads (atom [])
           fail? (atom true)
           fetch! (fn [cid]
                    (swap! reads conj (id/bytes->hex cid))
                    (when (and @fail? (= 3 (count @reads)))
                      (throw (ex-info "disconnected" {})))
                    (get (:blocks export) (id/bytes->hex cid)))
           opened (snapshot/open! statement (expected statement) cache fetch! demo/decode)]
       (is (thrown? Exception (demo/install! opened target)))
       (is (not (await! (k/exists? target (:root-key opened)))))
       (is (not (await! (k/exists? target :db))))
       (reset! fail? false)
       ;; Recreate the reader/source; only its verified object cache survives.
       (let [restored-cache (await! (filestore/connect-fs-store cache-path))
             reopened (snapshot/open! statement (expected statement) restored-cache fetch! demo/decode)
             db (demo/install! reopened target)]
         (is (= 1200 (d/q '[:find (count ?e) . :where [?e :note/text]] db)))
         (is (= 1 (count (filter #{(id/bytes->hex (:netz/snapshot statement))} @reads)))))))))

(deftest missed-commits-and-concurrent-announcements-preserve-explicit-selection
  (fixture!
   (fn [{:keys [connection events authority export]}]
     (let [first-ref (:statement export)
           previous [(:netz/snapshot first-ref)]
           cache (await! (memory/new-mem-store))]
       (d/transact connection [{:note/text "offline-1"}])
       (d/transact connection [{:note/text "offline-2"}])
       (let [latest (demo/capture! connection (last @events) authority previous)
             latest-ref (:statement latest)
             state (first (await! (ref/accept-verified (ref/make-state) latest-ref)))
             state (first (await! (ref/accept-verified state first-ref)))
             target (ds/add-cache-and-handlers (await! (memory/new-mem-store)) (:config @connection))
             db (demo/install!
                 (snapshot/open! latest-ref (expected latest-ref) cache
                                 #(get (:blocks latest) (id/bytes->hex %)) demo/decode)
                 target)]
         (is (= 1202 (d/q '[:find (count ?e) . :where [?e :note/text]] db)))
         (is (= #{(ref/snapshot-key latest-ref)}
                (ref/heads state snapshot/catalogue-profile (:netz/system latest-ref) (:netz/ref latest-ref))))
         (testing "a concurrent catalogue is retained; arrival order does not select a DB"
           (let [fork (await! (ref/sign authority
                                        {:profile snapshot/catalogue-profile
                                         :system (:netz/system latest-ref) :ref (:netz/ref latest-ref)
                                         :snapshot (content/raw-cid (id/utf8-bytes "other-catalogue")) :parents previous}))
                 state (first (await! (ref/accept-verified state fork)))]
             (is (= 2 (count (ref/heads state snapshot/catalogue-profile
                                        (:netz/system latest-ref) (:netz/ref latest-ref)))))
             (is (not (await! (k/exists? target :db)))))))))))

(deftest truncated-legacy-source-cannot-declare-success-or-install-a-root-early
  (fixture!
   (fn [{:keys [connection export]}]
     (let [cache (await! (memory/new-mem-store))
           statement (:statement export)
           opened (snapshot/open! statement (expected statement) cache
                                  #(get (:blocks export) (id/bytes->hex %)) demo/decode)
           target (ds/add-cache-and-handlers (await! (memory/new-mem-store)) (:config @connection))]
       (with-redefs [protocol/-handshake-items (fn [_ _] (a/to-chan! []))]
         (is (thrown-with-msg? Exception #"Incomplete snapshot" (demo/install! opened target))))
       (with-redefs [protocol/-handshake-items
                     (fn [_ _]
                       (a/to-chan! [{:key (:root-key opened)
                                     :value (await! (k/get (:source opened) (:root-key opened)))}]))]
         (is (thrown-with-msg? Exception #"root arrived before" (demo/install! opened target))))
       (is (not (await! (k/exists? target (:root-key opened)))))))))

(deftest conflicting-local-object-is-never-overwritten
  (fixture!
   (fn [{:keys [connection export]}]
     (let [cache (await! (memory/new-mem-store))
           statement (:statement export)
           opened (snapshot/open! statement (expected statement) cache
                                  #(get (:blocks export) (id/bytes->hex %)) demo/decode)
           target (ds/add-cache-and-handlers (await! (memory/new-mem-store)) (:config @connection))
           key (first (remove #{(:root-key opened)} (map :key (await! (k/keys (:source opened))))))]
       (await! (k/assoc target key :conflicting-local-value))
       (is (thrown-with-msg? Exception #"Conflicting immutable" (demo/install! opened target)))
       (is (= :conflicting-local-value (await! (k/get target key))))
       (is (not (await! (k/exists? target (:root-key opened)))))))))
