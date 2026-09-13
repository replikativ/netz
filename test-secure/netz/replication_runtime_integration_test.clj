(ns netz.replication-runtime-integration-test
  "Durable blob transfer over the released Kabel/http-kit/Noise carrier."
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :refer [timeout]]
            [clojure.test :refer [deftest is testing]]
            [kabel.http-kit :as http-kit]
            [kabel.middleware.cbor :refer [cbor]]
            [kabel.peer :as peer]
            [netz.identity :as identity]
            [netz.overlay.runtime :as overlay-runtime]
            [netz.replication.blob :as blob]
            [netz.replication.audit-runtime :as audit-runtime]
            [netz.replication.controller-runtime :as controller-runtime]
            [netz.replication.connectivity-runtime :as connectivity-runtime]
            [netz.replication.erasure :as erasure]
            [netz.replication.erasure-runtime :as erasure-runtime]
            [netz.replication-test-store :as file-store]
            [netz.replication.inventory :as inventory]
            [netz.replication.layout :as layout]
            [netz.replication.lifecycle-runtime :as lifecycle-runtime]
            [netz.replication.read-repair :as read-repair]
            [netz.replication.runtime :as replication-runtime]
            [netz.replication.storage :as storage]
            [netz.replication.wire :as replication-wire]
            [netz.transport.noise :as noise]
            [netz.transport.secure :as secure-overlay]
            [netz.wire :as wire]
            [superv.async :refer [<?? S]])
  (:import (java.nio.file Files Path)
           (java.util Comparator)))

(def ^:private port-counter (atom 49100))

(defn- wait-for [ms pred]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (cond
        (pred) true
        (> (System/currentTimeMillis) deadline) false
        :else (do (<?? S (timeout 25)) (recur))))))

(defn- temporary-directory []
  (Files/createTempDirectory
   "netz-blob-runtime-test-"
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
  [[source-binding source-expression target-binding target-expression] & body]
  `(let [~source-binding ~source-expression
         ~target-binding ~target-expression]
     (try
       ~@body
       (finally
         (delete-tree! ~source-binding)
         (delete-tree! ~target-binding)))))

(defn start-server [url identity capabilities]
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
    {:id id :peer socket :node node}))

(defn start-client [url identity server capabilities]
  (let [id (identity/peer-id (:genesis identity))
        [app install!] (overlay-runtime/deferred-middleware)
        socket
        (peer/client-peer
         S id app cbor (atom {}) (atom {})
         {:transport-middleware
          (secure-overlay/middleware {:identity identity
                                      :addresses []
                                      :seq-no 1
                                      :capabilities capabilities})})
        node (<?? S (secure-overlay/start!
                     S socket
                     {:identity identity
                      :topics #{:blob-test}
                      :seeds (or (:seeds server)
                                 [{:peer-id (:id server)
                                   :authority-id
                                   (identity/authority-id
                                    (:genesis (:identity server)))
                                   :addresses [url]
                                   :group "seed"}])
                      :overlay-opts
                      {:membership (merge {:require-authority? true
                                           :dial-interval-ms 20}
                                          (:membership server))}}))]
    (install! (:middleware node))
    {:id id :peer socket :node node}))

(defn stop-node! [{:keys [node peer]}]
  (overlay-runtime/stop! node)
  (<?? S (peer/stop peer)))

(defn- await [channel]
  (let [value (<?? S channel)]
    (if (instance? Throwable value) (throw value) value)))

(defn- install-source! [store session split]
  (doseq [{:keys [index id data]} (:pieces split)]
    (is (= :ok
           (await (storage/-stage-piece!
                   store session (:manifest-id split) (:manifest split)
                   index id data)))))
  (await (storage/-install-object!
          store session (:manifest-id split) (:manifest split) {}
          "netz/store/fsync/1")))

(defn- piece-file-path [^Path root piece-id]
  (let [hex (identity/bytes->hex piece-id)]
    (.resolve (.resolve (.resolve root "pieces") (subs hex 0 2))
              (str (subs hex 2) ".piece"))))

(defn- signed-offer [identity replica session split]
  (let [authority (identity/authority-id-bytes (:genesis identity))
        retention
        {"v" 1
         "type" "blob/retention"
         "cluster" (identity/seq->bytes (range 16))
         "privacy_domain" (identity/seq->bytes (range 16 32))
         "revision" 1
         "manifest" (:manifest-id split)
         "object" (:object-id split)
         "layout_epoch" 1
         "replicas" 1
         "write_acks" 1
         "placement_minimum_domains" {} "ack_minimum_domains" {}
         "issued_at" 1000
         "retain_until" 5000
         "signer" authority}]
    {"v" 1
     "type" "blob/offer"
     "session" session
     "attempt" 0
     "replica" replica
     "retention" (await (replication-wire/sign-statement identity retention))
     "manifest" (:manifest split)}))

(deftest placement-demand-selects-the-authenticated-target
  (with-store-roots [source-root (temporary-directory)
                     unused-root (temporary-directory)]
    (let [target-url (str "ws://localhost:" (swap! port-counter inc))
          ordinary-url (str "ws://localhost:" (swap! port-counter inc))
          capabilities (conj noise/default-capabilities
                             replication-wire/capability
                             overlay-runtime/extension-carrier-capability
                             wire/profile-capability)
          target-identity (<?? S (identity/generate-identity))
          ordinary-identity (<?? S (identity/generate-identity))
          client-identity (<?? S (identity/generate-identity))
          target (assoc (start-server target-url target-identity capabilities)
                        :identity target-identity)
          ordinary (assoc (start-server ordinary-url ordinary-identity capabilities)
                          :identity ordinary-identity)
          target-authority (identity/authority-id-bytes (:genesis target-identity))
          client-authority (identity/authority-id-bytes (:genesis client-identity))
          cluster (identity/seq->bytes (range 16))
          split (blob/split (identity/utf8-bytes "connection demand") {:piece-size 8})
          statement {"v" 1 "type" "blob/layout" "cluster" cluster "epoch" 0
                     "placement" "netz/blob-placement/2"
                     "seed" (identity/seq->bytes (repeat 32 7))
                     "previous" (identity/seq->bytes (repeat 32 0))
                     "members" [{"authority" target-authority
                                 "domains" {"site" "target"} "enabled" true}]
                     "issued_at" 900 "signer" client-authority}
          retention {"v" 1 "type" "blob/retention" "cluster" cluster
                     "privacy_domain" (identity/seq->bytes (range 16 32))
                     "revision" 1 "manifest" (:manifest-id split)
                     "object" (:object-id split) "layout_epoch" 0
                     "replicas" 1 "write_acks" 1
                     "placement_minimum_domains" {"site" 1}
                     "ack_minimum_domains" {"site" 1}
                     "issued_at" 1000 "retain_until" 5000 "signer" client-authority}
          layout-envelope (await (replication-wire/sign-statement client-identity statement))
          retention-envelope (await (replication-wire/sign-statement client-identity retention))
          current-layout (atom (:state (layout/reconcile
                                        (layout/make-state cluster [client-authority]) statement)))
          source (file-store/new-file-store source-root {:clock (constantly 1100)})
          controller (controller-runtime/start!
                      {:store source :cluster cluster :controllers [client-authority]
                       :layout-state-fn #(deref current-layout)
                       :now-fn (constantly 1100) :dispatch-offer! (constantly false)})]
      (try
        (is (true? (await (controller-runtime/ready controller))))
        (is (= :ok (await (controller-runtime/begin!
                           controller layout-envelope retention-envelope (:manifest split)))))
        (let [seeds [{:peer-id (:id ordinary)
                      :authority-id (identity/authority-id (:genesis ordinary-identity))
                      :addresses [ordinary-url] :group "ordinary"}
                     {:peer-id (:id target)
                      :authority-id (identity/authority-id (:genesis target-identity))
                      :addresses [target-url] :group "target"}]
              client (assoc (start-client target-url client-identity
                                          {:seeds seeds
                                           :membership {:max-peers 1 :max-parallel 1
                                                        :dial-interval-ms 1000}}
                                          capabilities)
                            :identity client-identity)
              coordinator (connectivity-runtime/start!
                           {:overlay (get-in client [:node :ctx])
                            :controllers [controller]})]
          (try
            (is (wait-for 10000
                          #(= #{(:id target)}
                              (overlay-runtime/connections (:peer client))))
                "the placement-selected Authority did not win the one outbound slot")
            (is (not (contains? (overlay-runtime/connections (:peer client)) (:id ordinary))))
            (is (= (identity/authority-id (:genesis target-identity))
                   (:authority-id
                    (overlay-runtime/connection-info (:peer client) (:id target)))))
            (finally
              (await (connectivity-runtime/stop! coordinator))
              (stop-node! client))))
        (finally
          (controller-runtime/stop! controller)
          (stop-node! ordinary)
          (stop-node! target))))))

(deftest durable-controller-drives-a-real-authenticated-transfer
  (with-store-roots [source-root (temporary-directory)
                     target-root (temporary-directory)]
    (let [port (swap! port-counter inc)
          url (str "ws://localhost:" port)
          capabilities (conj noise/default-capabilities
                             replication-wire/capability
                             overlay-runtime/extension-carrier-capability
                             wire/profile-capability)
          server-identity (<?? S (identity/generate-identity))
          client-identity (<?? S (identity/generate-identity))
          server (assoc (start-server url server-identity capabilities)
                        :identity server-identity)
          client (assoc (start-client url client-identity server capabilities)
                        :identity client-identity)
          source (file-store/new-file-store source-root
                                            {:clock (constantly 1100)})
          target (file-store/new-file-store target-root
                                            {:clock (constantly 1200)})
          server-authority
          (identity/authority-id-bytes (:genesis server-identity))
          client-authority
          (identity/authority-id-bytes (:genesis client-identity))
          cluster (identity/seq->bytes (range 16))
          split (blob/split (identity/utf8-bytes
                             "controller-owned durable transfer")
                            {:piece-size 8})
          layout-statement
          {"v" 1 "type" "blob/layout" "cluster" cluster "epoch" 0
           "placement" "netz/blob-placement/2"
           "seed" (identity/hex->bytes (apply str (repeat 32 "aa")))
           "previous" (identity/seq->bytes (repeat 32 0))
           "members" [{"authority" server-authority
                       "domains" {"site" "server"}
                       "enabled" true}]
           "issued_at" 900 "signer" client-authority}
          retention
          {"v" 1 "type" "blob/retention" "cluster" cluster
           "privacy_domain" (identity/seq->bytes (range 16 32))
           "revision" 1 "manifest" (:manifest-id split)
           "object" (:object-id split) "layout_epoch" 0
           "replicas" 1 "write_acks" 1
           "placement_minimum_domains" {"site" 1}
           "ack_minimum_domains" {"site" 1}
           "issued_at" 1000 "retain_until" 5000
           "signer" client-authority}
          layout-envelope
          (await (replication-wire/sign-statement client-identity
                                                  layout-statement))
          retention-envelope
          (await (replication-wire/sign-statement client-identity retention))
          current-layout
          (atom (:state (layout/reconcile
                         (layout/make-state cluster [client-authority])
                         layout-statement)))
          controller-ref (atom nil)
          complete (atom [])
          stored (atom [])
          failures (atom [])
          target-runtime
          (replication-runtime/start!
           (:node server)
           {:store target
            :now-fn (constantly 1100)
            :authorize-offer!
            (fn [{:keys [retention]}]
              (storage/chan-of
               (layout/retention-authorized? @current-layout server-authority
                                             retention)))})
          source-runtime
          (replication-runtime/start!
           (:node client)
           {:store source
            :now-fn (constantly 1100)
            :replay-receipts? false
            :on-stored! #(do
                           (swap! stored conj %)
                           (when-let [controller @controller-ref]
                             (controller-runtime/notify-stored! controller %)))
            :on-failed! #(swap! failures conj %)})
          controller
          (controller-runtime/start!
           {:store source :cluster cluster :controllers [client-authority]
            :layout-state-fn #(deref current-layout)
            :now-fn (constantly 1100)
            :on-complete! #(swap! complete conj %)
            :on-failed! #(swap! failures conj %)
            :dispatch-offer!
            (fn [{:keys [authority offer]}]
              (let [authority-hex (identity/bytes->hex authority)
                    peers (filterv
                           #(= authority-hex
                               (:authority-id
                                (overlay-runtime/connection-info
                                 (:peer client) %)))
                           (overlay-runtime/connections (:peer client)))]
                (and (= 1 (count peers))
                     (replication-runtime/replicate!
                      source-runtime (first peers) authority offer))))})]
      (reset! controller-ref controller)
      (try
        (is (true? (await (controller-runtime/ready controller))))
        (is (wait-for 10000
                      #(contains? (overlay-runtime/connections (:peer client))
                                  (:id server))))
        (install-source! source (identity/seq->bytes (range 32 48)) split)
        (is (= :ok
               (await (controller-runtime/begin!
                       controller layout-envelope retention-envelope
                       (:manifest split)))))
        (is (wait-for 10000 #(= 1 (count @complete)))
            "controller did not observe a durably journaled target receipt")
        (is (= 1 (count @stored)))
        (is (= 1 (count (await (storage/-recorded-receipts source)))))
        (is (= 1 (:stored (replication-runtime/stats source-runtime))))
        (is (= 0 (:event-errors (controller-runtime/stats controller))))
        (is (= #{(identity/bytes->hex server-authority)}
               (get-in (controller-runtime/state controller)
                       [:objects (identity/bytes->hex (:object-id split))
                        :acks])))
        (is (empty? @failures))
        (testing "active revalidation repairs an actually missing replica piece"
          (let [piece (identity/bytes->hex (:id (first (:pieces split))))
                piece-path (.resolve target-root
                                     (str "pieces/" (subs piece 0 2) "/" (subs piece 2) ".piece"))
                _ (Files/move piece-path (.resolveSibling piece-path (str (.getFileName piece-path) ".offline"))
                              (make-array java.nio.file.CopyOption 0))
                clock (atom 0)
                auditor (audit-runtime/start!
                         {:controller controller :scan-ms 10 :now-fn #(deref clock)
                          :audit-opts {:interval-ms 10}})]
            (try
              (await (audit-runtime/ready auditor))
              (is (wait-for 1000 #(seq (:entries @(:state auditor)))))
              (reset! clock 10)
              (is (wait-for 10000 #(>= (count @stored) 2))
                  (pr-str {:failures @failures :audit (audit-runtime/stats auditor)
                           :source (replication-runtime/stats source-runtime)
                           :target (replication-runtime/stats target-runtime)}))
              (is (= :confirmed
                     (:status (await (controller-runtime/await-durable!
                                      controller (identity/bytes->hex (:object-id split))
                                      {:timeout-ms 1000 :poll-ms 10})))))
              (is (= :healthy (:status (await (inventory/scrub-object! target (:manifest-id split))))))
              (is (= 1 (:attempt (last @stored))))
              (finally (await (audit-runtime/stop! auditor))))))
        (testing "a signed successor obtains fresh receipts over the live transport"
          (let [next-layout (assoc layout-statement "epoch" 1
                                   "previous" (layout/address layout-statement)
                                   "issued_at" 1000)
                next-retention (assoc retention "layout_epoch" 1 "revision" 2)
                next-envelope (await (replication-wire/sign-statement
                                      client-identity next-layout))
                next-retention-envelope (await (replication-wire/sign-statement
                                                client-identity next-retention))
                accepted (layout/reconcile @current-layout next-layout)
                object-id (identity/bytes->hex (:object-id split))]
            (is (true? (await (replication-wire/verify-envelope next-envelope))))
            (is (= :accepted (:status accepted)))
            (reset! current-layout (:state accepted))
            (is (= :ok (await (controller-runtime/revise!
                               controller next-envelope next-retention-envelope))))
            (is (wait-for 10000
                          #(some (fn [event] (= 1 (:layout-epoch event))) @stored)))
            (is (= :confirmed
                   (:status (await (controller-runtime/await-durable!
                                    controller object-id
                                    {:timeout-ms 2000 :poll-ms 10})))))
            (is (= 1 (get-in (controller-runtime/state controller)
                             [:objects object-id :layout-epoch])))
            (is (empty? @failures))))
        (finally
          (controller-runtime/stop! controller)
          (replication-runtime/stop! source-runtime)
          (replication-runtime/stop! target-runtime)
          (stop-node! client)
          (stop-node! server))))))

(deftest explicitly-negotiated-blob-runtime-transfers-and-records-a-receipt
  (with-store-roots [source-root (temporary-directory)
                     target-root (temporary-directory)]
    (let [port (swap! port-counter inc)
          url (str "ws://localhost:" port)
          capabilities (conj noise/default-capabilities
                             replication-wire/capability
                             overlay-runtime/extension-carrier-capability
                             wire/profile-capability)
          server-identity (<?? S (identity/generate-identity))
          client-identity (<?? S (identity/generate-identity))
          server (assoc (start-server url server-identity capabilities)
                        :identity server-identity)
          client (assoc (start-client url client-identity server capabilities)
                        :identity client-identity)
          source (file-store/new-file-store source-root
                                            {:clock (constantly 1100)})
          target (file-store/new-file-store target-root
                                            {:clock (constantly 1200)})
          session (identity/seq->bytes (range 16))
          split (blob/split (identity/utf8-bytes
                             "one ordered blob over Kabel and Noise")
                            {:piece-size 8})
          server-authority
          (identity/authority-id-bytes (:genesis server-identity))
          client-authority
          (identity/authority-id-bytes (:genesis client-identity))
          allow-offers? (atom true)
          allow-fetches? (atom true)
          accepted (atom [])
          fetch-requests (atom [])
          stored (atom [])
          provider-stored (atom [])
          installed (atom [])
          failures (atom [])
          fetch-failures (atom [])
          server-runtime
          (replication-runtime/start!
           (:node server)
           {:store target
            :now-fn (constantly 1000)
            :authorize-offer!
            (fn [{:keys [from-authority retention] :as request}]
              ;; A real policy adapter durably records this exact accepted
              ;; statement before its channel yields true.
              (swap! accepted conj request)
              (storage/chan-of
               (and @allow-offers?
                    (identity/bufs= client-authority from-authority)
                    (identity/bufs= client-authority
                                    (get retention "signer")))))
            :authorize-fetch!
            (fn [{:keys [from-authority retention] :as request}]
              (swap! fetch-requests conj request)
              (storage/chan-of
               (and @allow-fetches?
                    (identity/bufs= client-authority from-authority)
                    (identity/bufs= client-authority
                                    (get retention "signer")))))
            :on-stored! #(swap! provider-stored conj %)})
          client-runtime
          (replication-runtime/start!
           (:node client)
           {:store source
            :now-fn (constantly 1000)
            :authorize-offer!
            (fn [{:keys [from-authority retention]}]
              (storage/chan-of
               (and (identity/bufs= server-authority from-authority)
                    (identity/bufs= client-authority
                                    (get retention "signer")))))
            :on-stored! #(swap! stored conj %)
            :on-installed! #(swap! installed conj %)
            :on-failed! #(swap! failures conj %)
            :on-fetch-failed! #(swap! fetch-failures conj %)})]
      (try
        (is (wait-for 10000
                      #(and (contains? (overlay-runtime/connections
                                        (:peer client))
                                       (:id server))
                            (contains? (overlay-runtime/connections
                                        (:peer server))
                                       (:id client))))
            "authenticated peers did not connect")

        (testing "the extension is present only after explicit negotiation"
          (is (overlay-runtime/connection-supports?
               (:peer client) (:id server) replication-wire/capability))
          (is (= capabilities
                 (:capabilities
                  (overlay-runtime/connection-info
                   (:peer client) (:id server))))))

        (install-source! source session split)
        (let [offer (signed-offer client-identity server-authority
                                  session split)]
          (is (replication-runtime/replicate!
               client-runtime (:id server) server-authority offer))
          (is (wait-for 10000 #(= 1 (count @stored)))
              "durable signed receipt never reached the sender controller")

          (testing "authorization sees the authenticated full Authority"
            (is (= 1 (count @accepted)))
            (is (identity/bufs=
                 client-authority (:from-authority (first @accepted)))))

          (testing "the target installed and reverified the complete object"
            (let [inventory
                  (await (storage/-load-staging
                          target (identity/seq->bytes (range 16 32))
                          (:manifest-id split) (:manifest split)))]
              (is (= "netz/store/fsync/1"
                     (get-in inventory [:installed :persistence])))
              (is (= (count (:pieces split)) (count (:held inventory))))))

          (testing "receipt evidence is durable before the callback"
            (let [records (await (storage/-recorded-receipts source))]
              (is (= 1 (count records)))
              (is (= (dissoc (first @stored) :receipt)
                     (dissoc (first records) :receipt)))
              (is (identity/bufs=
                   (wire/encode-canonical (:receipt (first @stored)))
                   (wire/encode-canonical (:receipt (first records)))))))

          (is (empty? @failures))
          (is (empty? (get-in (replication-runtime/states client-runtime)
                              [:sender :sessions])))
          (is (empty? (get-in (replication-runtime/states server-runtime)
                              [:receiver :sessions])))

          (testing "an authenticated reader pulls an installed object"
            (let [provider-session (identity/seq->bytes (range 80 96))
                  fetch-session (identity/seq->bytes (range 96 112))
                  provider-split
                  (blob/split (identity/utf8-bytes
                               "provider-side immutable Konserve value")
                              {:piece-size 7})
                  retention-envelope
                  (get (signed-offer client-identity client-authority
                                     fetch-session provider-split)
                       "retention")]
              (install-source! target provider-session provider-split)
              (is (= :fetched
                     (:status
                      (await
                       (read-repair/ensure-local!
                        source client-runtime
                        [{:peer (:id server) :authority server-authority}]
                        retention-envelope)))))
              (is (wait-for 10000 #(= 1 (count @installed)))
                  "requester never installed the fetched object")
              (is (wait-for 10000 #(= 1 (count @provider-stored)))
                  "provider never durably recorded the requester receipt")
              (is (= 1 (count @fetch-requests)))
              (is (identity/bufs=
                   client-authority
                   (:from-authority (first @fetch-requests))))
              (is (= (identity/bytes->hex (:manifest-id provider-split))
                     (:manifest-id (first @installed))))
              (is (identity/bufs=
                   (wire/encode-canonical (:manifest provider-split))
                   (wire/encode-canonical
                    (await (inventory/installed-manifest
                            source (:manifest-id provider-split))))))
              (is (empty? @fetch-failures))
              (is (= 1 (:fetched
                        (replication-runtime/stats client-runtime))))
              (is (= 1 (:fetch-served
                        (replication-runtime/stats server-runtime))))

              (reset! allow-fetches? false)
              (let [denied-fetch-session
                    (identity/seq->bytes (range 112 128))
                    denied-result
                    (await
                     (replication-runtime/fetch-result!
                      client-runtime (:id server) server-authority
                      retention-envelope {:session denied-fetch-session}))]
                (is (= "fetch-unauthorized" (:code denied-result)))
                (is (wait-for 10000 #(= 1 (count @fetch-failures)))
                    "requester did not observe provider read-policy rejection")
                (is (= "fetch-unauthorized"
                       (:code (first @fetch-failures))))
                (is (empty? (:fetches
                             (replication-runtime/states client-runtime)))))))

          (testing "a valid but unauthorized offer is aborted without storage"
            (reset! allow-offers? false)
            (let [denied-session (identity/seq->bytes (range 32 48))
                  denied-split (blob/split
                                (identity/utf8-bytes "policy denied")
                                {:piece-size 4})
                  denied-offer (signed-offer
                                client-identity server-authority
                                denied-session denied-split)]
              (install-source! source denied-session denied-split)
              (is (replication-runtime/replicate!
                   client-runtime (:id server) server-authority denied-offer))
              (is (wait-for 10000 #(= 1 (count @failures)))
                  "sender did not observe the receiver's policy abort")
              (is (= "unauthorized" (:code (first @failures))))
              (is (nil?
                   (:installed
                    (await (storage/-load-staging
                            target (identity/seq->bytes (range 48 64))
                            (:manifest-id denied-split)
                            (:manifest denied-split)))))
                  "denied bytes reached the target store")
              (is (empty?
                   (get-in (replication-runtime/states client-runtime)
                           [:sender :sessions])))))

          (testing "a structurally valid frame with a bad signature is hostile"
            (reset! allow-offers? true)
            (let [bad-session (identity/seq->bytes (range 64 80))
                  bad-split (blob/split (identity/utf8-bytes "tampered")
                                        {:piece-size 4})
                  tampered
                  (update-in
                   (signed-offer client-identity server-authority
                                 bad-session bad-split)
                   ["retention" "statement" "retain_until"] inc)]
              (is (replication-wire/valid-frame? tampered)
                  "the signature check, not structural validation, is tested")
              (is (false?
                   (await (replication-wire/verify-envelope
                           (get tampered "retention")))))
              (is (overlay-runtime/send-extension!
                   (:node client) (:id server)
                   replication-wire/capability tampered))
              (is (wait-for
                   10000
                   #(= 1 (:invalid-signatures
                          (replication-runtime/stats server-runtime))))
                  "bad signature did not reach the fail-closed path"))))
        (finally
          (replication-runtime/stop! client-runtime)
          (replication-runtime/stop! server-runtime)
          (stop-node! client)
          (stop-node! server))))))

(deftest unsupported-connection-refuses-replication-before-creating-a-session
  (with-store-roots [source-root (temporary-directory)
                     target-root (temporary-directory)]
    (let [port (swap! port-counter inc)
          url (str "ws://localhost:" port)
          server-identity (<?? S (identity/generate-identity))
          client-identity (<?? S (identity/generate-identity))
          server (assoc (start-server url server-identity
                                      noise/default-capabilities)
                        :identity server-identity)
          client (assoc (start-client url client-identity server
                                      noise/default-capabilities)
                        :identity client-identity)
          store (file-store/new-file-store source-root)
          target-store (file-store/new-file-store target-root)
          source-runtime (replication-runtime/start! (:node client)
                                                     {:store store})
          target-runtime (replication-runtime/start! (:node server)
                                                     {:store target-store})
          session (identity/seq->bytes (range 16))
          split (blob/split (identity/utf8-bytes "not sent")
                            {:piece-size 4})
          server-authority
          (identity/authority-id-bytes (:genesis server-identity))
          client-authority
          (identity/authority-id-bytes (:genesis client-identity))]
      (try
        (is (wait-for 10000
                      #(contains? (overlay-runtime/connections (:peer client))
                                  (:id server))))
        (install-source! store session split)
        (is (false?
             (replication-runtime/replicate!
              source-runtime (:id server) server-authority
              (signed-offer client-identity server-authority session split))))
        (is (= :refused
               (:status
                (await
                 (replication-runtime/fetch-result!
                  source-runtime (:id server) server-authority
                  (get (signed-offer client-identity client-authority
                                     session split)
                       "retention"))))))
        (is (empty? (get-in (replication-runtime/states source-runtime)
                            [:sender :sessions])))
        (is (zero? (:inbound (replication-runtime/stats target-runtime))))
        (finally
          (replication-runtime/stop! source-runtime)
          (replication-runtime/stop! target-runtime)
          (stop-node! client)
          (stop-node! server))))))

(deftest erasure-converges-over-the-authenticated-portable-carrier
  (with-store-roots [controller-root (temporary-directory)
                     replica-root (temporary-directory)]
    (let [url (str "ws://localhost:" (swap! port-counter inc))
          capabilities (conj noise/default-capabilities
                             replication-wire/capability
                             overlay-runtime/extension-carrier-capability
                             wire/profile-capability)
          controller-identity (<?? S (identity/generate-identity))
          replica-identity (<?? S (identity/generate-identity))
          replica (assoc (start-server url replica-identity capabilities)
                         :identity replica-identity)
          controller (assoc (start-client url controller-identity replica
                                          capabilities)
                            :identity controller-identity)
          controller-authority
          (identity/authority-id-bytes (:genesis controller-identity))
          replica-authority
          (identity/authority-id-bytes (:genesis replica-identity))
          replica-id (identity/bytes->hex replica-authority)
          cluster (identity/seq->bytes (range 16))
          privacy-domain (identity/seq->bytes (range 16 32))
          split (blob/split (identity/seq->bytes (range 40))
                            {:piece-size 8})
          deletion {"v" 1 "type" "blob/deletion" "cluster" cluster
                    "privacy_domain" privacy-domain "revision" 2
                    "manifest" (:manifest-id split)
                    "object" (:object-id split) "layout_epoch" 1
                    "issued_at" 2000 "delete_after" 2000
                    "signer" controller-authority}
          deletion-envelope
          (await (replication-wire/sign-statement controller-identity deletion))
          controller-store (file-store/new-file-store controller-root
                                                      {:clock (constantly 2100)})
          replica-store (file-store/new-file-store replica-root
                                                   {:clock (constantly 2100)})
          lifecycle
          (lifecycle-runtime/start!
           {:identity replica-identity :store replica-store
            :controllers [controller-authority] :now-fn (constantly 2100)})
          replica-runtime
          (replication-runtime/start!
           (:node replica)
           {:store replica-store
            :apply-deletion!
            #(lifecycle-runtime/apply-envelope! lifecycle (:envelope %))})
          coordinator-ref (atom nil)
          controller-runtime
          (replication-runtime/start!
           (:node controller)
           {:store controller-store
            :on-deletion-receipt!
            #(when-let [coordinator @coordinator-ref]
               (erasure-runtime/accept-receipt! coordinator (:envelope %)))})
          coordinator
          (erasure-runtime/start!
           {:cluster cluster :controllers [controller-authority]
            :store controller-store :scan-ms 60000
            :now-fn (constantly 2100)
            :dispatch-deletion!
            (replication-runtime/deletion-dispatcher controller-runtime)})]
      (lifecycle-runtime/set-deletion-receipt-handler!
       lifecycle (replication-runtime/deletion-receipt-dispatcher
                  replica-runtime))
      (reset! coordinator-ref coordinator)
      (try
        (install-source! replica-store
                         (identity/seq->bytes (range 32 48)) split)
        (is (= :healthy
               (:status (await (inventory/scrub-object!
                                replica-store (:manifest-id split))))))
        (is (true? (await (lifecycle-runtime/ready lifecycle))))
        (is (true? (await (erasure-runtime/ready coordinator))))
        (is (wait-for 10000
                      #(contains? (overlay-runtime/connections
                                   (:peer controller)) (:id replica))))
        (is (false?
             (replication-runtime/send-deletion!
              controller-runtime replica-id
              (await (replication-wire/sign-statement
                      replica-identity
                      (assoc deletion "signer" replica-authority)))))
            "the carrier must not send a command signed by another Authority")
        (is (false?
             (replication-runtime/send-deletion!
              controller-runtime (apply str (repeat 64 "f"))
              deletion-envelope))
            "an unconnected Authority is not a routing fallback")
        (is (true? (await (erasure-runtime/begin!
                           coordinator deletion-envelope [replica-authority]))))
        (is (wait-for
             10000
             #(= :complete
                 (:status (first (erasure/statuses
                                  (erasure-runtime/state coordinator)))))))
        (is (= :missing
               (:status (await (inventory/scrub-object!
                                replica-store (:manifest-id split))))))
        (doseq [piece (:pieces split)]
          (is (not (Files/exists
                    (piece-file-path replica-root (:id piece))
                    (make-array java.nio.file.LinkOption 0)))))
        (is (= 1 (:deletions-accepted
                  (replication-runtime/stats replica-runtime))))
        (is (= 1 (:deletion-receipts
                  (replication-runtime/stats controller-runtime))))
        (finally
          (erasure-runtime/stop! coordinator)
          (replication-runtime/stop! controller-runtime)
          (replication-runtime/stop! replica-runtime)
          (lifecycle-runtime/stop! lifecycle)
          (stop-node! controller)
          (stop-node! replica))))))
