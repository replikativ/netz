(ns netz.replication-file-store-test
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.controller-storage :as controller-storage]
            [netz.replication.directory-barrier :as directory-barrier]
            [netz.replication-test-store :as file-store]
            [netz.replication.inventory :as inventory]
            [netz.replication.intent :as intent]
            [netz.replication.sender :as sender]
            [netz.replication.storage :as storage]
            [netz.replication.transfer :as transfer]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire])
  (:import (java.nio.file Files OpenOption Path StandardOpenOption)
           (java.util Comparator)))

(def public-hex
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
(def private-hex
  "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
(def genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "aa"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "cc"))}))
(def authority (identity/authority-id-bytes genesis))
(def session (identity/hex->bytes (apply str (repeat 16 "30"))))
(def session-2 (identity/hex->bytes (apply str (repeat 16 "31"))))
(def split (blob/split (identity/seq->bytes (range 20)) {:piece-size 8}))

(defn await [channel]
  (let [result (<!! channel)]
    (if (instance? Throwable result) (throw result) result)))

(defn temporary-directory []
  (Files/createTempDirectory
   "netz-blob-store-test-"
   (make-array java.nio.file.attribute.FileAttribute 0)))

(defn delete-tree! [^Path root]
  (when (Files/exists root (make-array java.nio.file.LinkOption 0))
    (with-open [paths (Files/walk
                       root
                       (make-array java.nio.file.FileVisitOption 0))]
      (doseq [target (sort (Comparator/reverseOrder)
                           (iterator-seq (.iterator paths)))]
        (Files/deleteIfExists ^Path target)))))

(defmacro ^{:clj-kondo/lint-as 'clojure.core/let} with-store-root
  [[binding expression] & body]
  `(let [~binding ~expression]
     (try
       ~@body
       (finally (delete-tree! ~binding)))))

(defn piece-path [^Path root piece-id]
  (let [hex (identity/bytes->hex piece-id)]
    (.resolve (.resolve (.resolve root "pieces") (subs hex 0 2))
              (str (subs hex 2) ".piece"))))

(defn marker-path [^Path root manifest-id]
  (let [hex (identity/bytes->hex manifest-id)]
    (.resolve (.resolve (.resolve root "manifests") (subs hex 0 2))
              (str (subs hex 2) ".cbor"))))

(defn receipt-files [^Path root]
  (let [receipts (.resolve root "receipts")]
    (if-not (Files/exists receipts (make-array java.nio.file.LinkOption 0))
      []
      (with-open [paths (Files/walk
                         receipts
                         (make-array java.nio.file.FileVisitOption 0))]
        (->> (iterator-seq (.iterator paths))
             (filter #(and (Files/isRegularFile
                            ^Path % (make-array java.nio.file.LinkOption 0))
                           (str/ends-with? (str %) ".cbor")))
             vec)))))

(defn controller-intent-files [^Path root cluster]
  (let [intents (.resolve (.resolve root "controller-intents")
                          (identity/bytes->hex cluster))]
    (if-not (Files/exists intents (make-array java.nio.file.LinkOption 0))
      []
      (with-open [paths (Files/walk
                         intents
                         (make-array java.nio.file.FileVisitOption 0))]
        (->> (iterator-seq (.iterator paths))
             (filter #(Files/isRegularFile
                       ^Path % (make-array java.nio.file.LinkOption 0)))
             vec)))))

(defn stage! [store piece]
  (await (storage/-stage-piece!
          store session (:manifest-id split) (:manifest split)
          (:index piece) (:id piece) (:data piece))))

(defn install! [store persistence]
  (await (storage/-install-object!
          store session (:manifest-id split) (:manifest split)
          {} persistence)))

(defn signed-receipt []
  (let [statement
        {"v" 1
         "type" "blob/stored"
         "cluster" (identity/hex->bytes (apply str (repeat 16 "10")))
         "privacy_domain" (identity/hex->bytes (apply str (repeat 16 "20")))
         "retention_revision" 1
         "manifest" (:manifest-id split)
         "object" (:object-id split)
         "layout_epoch" 7
         "replica" authority
         "attempt" 0
         "persistence" "netz/store/fsync/1"
         "stored_at" 1200
         "retain_until" 5000
         "signer" authority}]
    (await (replication-wire/sign-statement
            {:genesis genesis
             :operational {:public (identity/hex->bytes public-hex)
                           :private (identity/hex->bytes private-hex)}}
            statement))))

(defn sign-statement [statement]
  (await (replication-wire/sign-statement
          {:genesis genesis
           :operational {:public (identity/hex->bytes public-hex)
                         :private (identity/hex->bytes private-hex)}}
          statement)))

(def retention
  {"v" 1 "type" "blob/retention"
   "cluster" (identity/hex->bytes (apply str (repeat 16 "10")))
   "privacy_domain" (identity/hex->bytes (apply str (repeat 16 "20")))
   "revision" 1 "manifest" (:manifest-id split)
   "object" (:object-id split) "layout_epoch" 0
   "replicas" 1 "write_acks" 1 "minimum_domains" {}
   "issued_at" 1000 "retain_until" 5000 "signer" authority})

(def layout0
  {"v" 1 "type" "blob/layout"
   "cluster" (get retention "cluster") "epoch" 0
   "previous" (identity/hex->bytes (apply str (repeat 32 "00")))
   "members" [{"authority" authority "domains" {} "enabled" true}]
   "issued_at" 900 "signer" authority})

(defn durable-intent [intent-revision attempt]
  (intent/make-record
   (sign-statement layout0) (sign-statement retention) (:manifest split)
   {(identity/bytes->hex authority) attempt} intent-revision))

(defn controller-event [receipt]
  {:type :replication/stored
   :object-id (identity/bytes->hex (:object-id split))
   :replica-id (identity/bytes->hex authority)
   :layout-epoch 7
   :attempt 0
   :persistence "netz/store/fsync/1"
   :receipt receipt
   :manifest-id (identity/bytes->hex (:manifest-id split))})

(deftest provisioning-failure-prevents-root-and-piece-success
  (with-store-root [root (temporary-directory)]
    (let [nested (.resolve root "nested/store")
          error (java.nio.file.AccessDeniedException. "injected")]
      (is (thrown? clojure.lang.ExceptionInfo
                   (file-store/new-file-store nested {:durable-ancestor nil})))
      (is (not (Files/exists (.getParent nested) (make-array java.nio.file.LinkOption 0))))
      (with-redefs [directory-barrier/provision! (fn [& _] (throw error))]
        (is (thrown? java.nio.file.AccessDeniedException
                     (file-store/new-file-store nested {:durable-ancestor root}))))
      (is (not (Files/exists nested (make-array java.nio.file.LinkOption 0))))
      (let [store (file-store/new-file-store nested {:durable-ancestor root})
            piece (first (:pieces split))]
        (doseq [_ (range 2)]
          (with-redefs [directory-barrier/provision! (fn [& _] (throw error))]
            (is (thrown? java.nio.file.AccessDeniedException (stage! store piece))))
          (is (= :ok (stage! store piece))))))))

(deftest staged-pieces-survive-cleanup-and-restart
  (with-store-root [root (temporary-directory)]
    (let [store (file-store/new-file-store root {:clock (constantly 1200)})
          piece-0 (first (:pieces split))]
      (is (= {:held [] :installed nil}
             (await (storage/-load-staging
                     store session (:manifest-id split) (:manifest split)))))
      (is (= :ok (stage! store piece-0)))
      (is (true? (await (storage/-cleanup-session!
                         store session (:manifest-id split)))))

      (let [restarted (file-store/new-file-store root {:clock (constantly 1200)})
            inventory (await (storage/-load-staging
                              restarted session-2 (:manifest-id split)
                              (:manifest split)))]
        (is (= 1 (count (:held inventory))))
        (is (identity/bufs= (:id piece-0) (first (:held inventory))))
        (is (nil? (:installed inventory)))
        (doseq [piece (rest (:pieces split))] (stage! restarted piece))
        (is (= {:result :ok
                :manifest (:manifest-id split)
                :object (:object-id split)
                :persistence "netz/store/fsync/1"
                :stored-at 1200}
               (install! restarted "netz/store/fsync/1")))
        (is (= (str/trim
                (slurp "spec/fixtures/blob-file-store-marker-1.hex"))
               (identity/bytes->hex
                (Files/readAllBytes
                 (marker-path root (:manifest-id split))))))

        (let [{:keys [held installed]}
              (await (storage/-load-staging
                      restarted session-2 (:manifest-id split)
                      (:manifest split)))]
          (is (= (count (:pieces split)) (count held)))
          (is (= "netz/store/fsync/1" (:persistence installed)))
          (is (= 1200 (:stored-at installed))))

        (let [loaded (await (storage/-load-piece
                             restarted session-2 (:manifest-id split) 1
                             (:id (second (:pieces split)))))]
          (is (identity/bufs= (:data (second (:pieces split))) loaded)))))))

(deftest partial-and-corrupt-content-is-never-installed
  (with-store-root [root (temporary-directory)]
    (let [store (file-store/new-file-store root {:clock (constantly 1200)})]
      (doseq [piece (butlast (:pieces split))] (stage! store piece))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"missing or corrupt piece"
                            (install! store "netz/store/fsync/1")))
      (is (nil? (:installed
                 (await (storage/-load-staging
                         store session (:manifest-id split) (:manifest split))))))

      (stage! store (last (:pieces split)))
      (install! store "netz/store/fsync/1")
      (let [piece-1 (second (:pieces split))
            target (piece-path root (:id piece-1))]
        (Files/write target
                     (identity/seq->bytes (repeat 8 0))
                     (into-array OpenOption
                                 [StandardOpenOption/WRITE
                                  StandardOpenOption/TRUNCATE_EXISTING]))
        (let [{:keys [held installed]}
              (await (storage/-load-staging
                      store session-2 (:manifest-id split) (:manifest split)))]
          (is (nil? installed))
          (is (= 2 (count held)))
          (is (not-any? #(identity/bufs= (:id piece-1) %) held)))

        (is (= :ok (stage! store piece-1)))
        (is (= "netz/store/fsync/1"
               (:persistence (install! store "netz/store/fsync/1"))))))))

(deftest invalid-piece-and-persistence-claims-fail-closed
  (with-store-root [root (temporary-directory)]
    (let [store (file-store/new-file-store
                 root {:persistence "netz/store/best-effort/1"
                       :clock (constantly 1200)})
          piece (first (:pieces split))]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"invalid piece"
           (await (storage/-stage-piece!
                   store session (:manifest-id split) (:manifest split)
                   0 (:id piece) (identity/seq->bytes [0])))))
      (doseq [value (:pieces split)] (stage! store value))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"does not match"
                            (install! store "netz/store/fsync/1")))
      (is (= "netz/store/best-effort/1"
             (:persistence
              (install! store "netz/store/best-effort/1")))))))

(deftest signed-receipts-are-durable-and-cryptographically-replayed
  (with-store-root [root (temporary-directory)]
    (let [store (file-store/new-file-store root)
          receipt (signed-receipt)
          event (controller-event receipt)]
      (is (true? (await (replication-wire/verify-envelope receipt))))
      (is (= :ok (await (storage/-persist-receipt!
                         store session event))))
      (let [restarted (file-store/new-file-store root)
            replayed (first (await (storage/-recorded-receipts restarted)))]
        (is (= (dissoc event :receipt) (dissoc replayed :receipt)))
        (is (identity/bufs= (wire/encode-canonical receipt)
                            (wire/encode-canonical (:receipt replayed)))))

      (testing "the journal scan is bounded"
        (is (= :ok (await (storage/-persist-receipt!
                           store session-2 event))))
        (let [bounded (file-store/new-file-store root {:max-receipts 1})]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"configured bound"
                                (await (storage/-recorded-receipts bounded)))))))))

(deftest controller-intents-are-atomically-replaced-and-replayed
  (with-store-root [root (temporary-directory)]
    (let [store (file-store/new-file-store root)
          cluster (get retention "cluster")
          initial (durable-intent 0 0)
          retry (durable-intent 1 1)]
      (is (= :ok
             (await (controller-storage/-persist-controller-intent!
                     store cluster (:object-id split) initial))))
      (is (intent/same-record?
           initial
           (first
            (await (controller-storage/-load-controller-intents
                    (file-store/new-file-store root) cluster)))))
      (is (= :ok
             (await (controller-storage/-persist-controller-intent!
                     store cluster (:object-id split) retry))))
      (let [loaded (await (controller-storage/-load-controller-intents
                           (file-store/new-file-store root) cluster))]
        (is (= 1 (count loaded)))
        (is (intent/same-record? retry (first loaded))))

      (testing "record identity and bounded restart scans fail closed"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"Invalid durable controller intent"
             (await (controller-storage/-persist-controller-intent!
                     store cluster
                     (identity/hex->bytes (apply str (repeat 32 "ff")))
                     retry))))
        (let [other-split (blob/split (identity/seq->bytes (range 21))
                                      {:piece-size 8})
              other-retention (assoc retention
                                     "object" (:object-id other-split)
                                     "manifest" (:manifest-id other-split))
              other (intent/make-record
                     (sign-statement layout0)
                     (sign-statement other-retention)
                     (:manifest other-split)
                     {(identity/bytes->hex authority) 0} 0)]
          (is (= :ok
                 (await (controller-storage/-persist-controller-intent!
                         store cluster (:object-id other-split) other))))
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo #"configured bound"
               (await (controller-storage/-load-controller-intents
                       (file-store/new-file-store
                        root {:max-controller-intents 1})
                       cluster))))
          (Files/write (first (controller-intent-files root cluster))
                       (identity/seq->bytes [0 1 2])
                       (into-array OpenOption
                                   [StandardOpenOption/WRITE
                                    StandardOpenOption/TRUNCATE_EXISTING]))
          (is (thrown? Throwable
                       (await (controller-storage/-load-controller-intents
                               store cluster)))))))))

(deftest corrupted-receipt-records-never-become-quorum-events
  (with-store-root [root (temporary-directory)]
    (let [store (file-store/new-file-store root)
          event (controller-event (signed-receipt))]
      (await (storage/-persist-receipt! store session event))
      (let [target (first (receipt-files root))]
        (Files/write target
                     (identity/seq->bytes [0 1 2])
                     (into-array OpenOption
                                 [StandardOpenOption/WRITE
                                  StandardOpenOption/TRUNCATE_EXISTING]))
        (is (thrown? Throwable
                     (await (storage/-recorded-receipts store))))))))

(deftest installed-inventory-is-paginated-and-scrubbed
  (with-store-root [root (temporary-directory)]
    (let [store (file-store/new-file-store root {:clock (constantly 1200)})
          second-split (blob/split (identity/seq->bytes (range 30 55))
                                   {:piece-size 8})
          install-split!
          (fn [value]
            (doseq [piece (:pieces value)]
              (await (storage/-stage-piece!
                      store session (:manifest-id value) (:manifest value)
                      (:index piece) (:id piece) (:data piece))))
            (await (storage/-install-object!
                    store session (:manifest-id value) (:manifest value)
                    {} "netz/store/fsync/1")))]
      (install-split! split)
      (install-split! second-split)
      (let [first-page (await (inventory/inventory-page store nil 1))
            second-page (await (inventory/inventory-page
                                store (:next-cursor first-page) 1))
            addresses (mapv identity/bytes->hex
                            (concat (:manifests first-page)
                                    (:manifests second-page)))]
        (is (= (sort [(identity/bytes->hex (:manifest-id split))
                      (identity/bytes->hex (:manifest-id second-split))])
               addresses))
        (is (string? (:next-cursor first-page)))
        (is (nil? (:next-cursor second-page))))

      (let [healthy (await (inventory/scrub-object!
                            store (:manifest-id split)))]
        (is (= :healthy (:status healthy)))
        (is (= (get-in split [:manifest "length"]) (:length healthy)))
        (is (identity/bufs= (:object-id split) (:object healthy))))
      (is (identity/bufs=
           (wire/encode-canonical (:manifest split))
           (wire/encode-canonical
            (await (inventory/installed-manifest
                    store (:manifest-id split))))))
      (is (nil? (await (inventory/installed-manifest
                        store (identity/sha256
                               (identity/utf8-bytes "absent"))))))
      (is (= :missing
             (:status (await (inventory/scrub-object!
                              store (identity/sha256
                                     (identity/utf8-bytes "absent")))))))

      (let [piece (first (:pieces split))]
        (Files/write (piece-path root (:id piece))
                     (identity/seq->bytes (repeat 8 0))
                     (into-array OpenOption
                                 [StandardOpenOption/WRITE
                                  StandardOpenOption/TRUNCATE_EXISTING]))
        (is (instance? Throwable
                       (<!! (inventory/scrub-object!
                             store (:manifest-id split)))))))))

(defn machine-step [side state event]
  ((if (= side :sender) sender/handler transfer/handler)
   state event {:now 1000}))

(defn queue-actions [queue side actions]
  (into queue (map #(vector side %) actions)))

(defn route-frame [side frame]
  (if (= side :sender)
    (case (get frame "type")
      "blob/offer" {:type :transfer/authorized-offer
                    :from :sender :from-authority authority :frame frame}
      "blob/piece" {:type :transfer/piece
                    :from :sender :from-authority authority :frame frame}
      "blob/commit" {:type :transfer/commit
                     :from :sender :from-authority authority :frame frame})
    (case (get frame "type")
      "blob/need" {:type :transfer/need
                   :from :receiver :from-authority authority :frame frame}
      "blob/ack" {:type :transfer/ack
                  :from :receiver :from-authority authority :frame frame}
      "blob/stored" (do
                      (is (true? (await (replication-wire/verify-envelope
                                         (get frame "receipt")))))
                      {:type :transfer/verified-stored
                       :from :receiver :from-authority authority :frame frame})
      "blob/abort" {:type :transfer/abort
                    :from :receiver :from-authority authority :frame frame})))

(defn drive-file-transfer
  [source target sender-state receiver-state initial-actions]
  (loop [states {:sender sender-state :receiver receiver-state}
         queue (queue-actions [] :sender initial-actions)
         controller-event nil
         steps 0]
    (when (> steps 100)
      (throw (ex-info "File transfer did not quiesce" {:states states})))
    (if-let [[side action] (first queue)]
      (let [queue (subvec queue 1)
            op (first action)]
        (cond
          (= :timer op)
          (recur states queue controller-event (inc steps))

          (= :send op)
          (let [other (if (= side :sender) :receiver :sender)
                event (route-frame side (nth action 2))
                {next-state :state actions :actions}
                (machine-step other (get states other) event)]
            (recur (assoc states other next-state)
                   (queue-actions queue other actions)
                   controller-event (inc steps)))

          (contains? storage/storage-ops op)
          (let [store (if (= side :sender) source target)
                result (await (storage/execute-action store action))
                event (storage/completion-event action result)]
            (if event
              (let [{next-state :state actions :actions}
                    (machine-step side (get states side) event)]
                (recur (assoc states side next-state)
                       (queue-actions queue side actions)
                       controller-event (inc steps)))
              (recur states queue controller-event (inc steps))))

          (= :sign-receipt op)
          (let [[_ transfer-session statement] action
                envelope (sign-statement statement)
                {next-state :state actions :actions}
                (machine-step :receiver (:receiver states)
                              {:type :transfer/receipt-signed
                               :session transfer-session
                               :envelope envelope})]
            (recur (assoc states :receiver next-state)
                   (queue-actions queue :receiver actions)
                   controller-event (inc steps)))

          (= :replication-stored op)
          (recur states queue (second action) (inc steps))

          (= :replica-installed op)
          (recur states queue controller-event (inc steps))

          (= :transfer-failed op)
          (throw (ex-info "Unexpected transfer failure" {:failure action}))

          :else
          (throw (ex-info "Unknown transfer action" {:side side :action action}))))
      {:states states :controller-event controller-event :steps steps})))

(deftest real-file-stores-drive-the-pure-transfer-machines
  (with-store-root [source-root (temporary-directory)]
    (with-store-root [target-root (temporary-directory)]
      (let [source (file-store/new-file-store source-root
                                              {:clock (constantly 1100)})
            target (file-store/new-file-store target-root
                                              {:clock (constantly 1200)})
            _ (doseq [piece (:pieces split)] (stage! source piece))
            _ (install! source "netz/store/fsync/1")
            retention
            {"v" 1
             "type" "blob/retention"
             "cluster" (identity/hex->bytes (apply str (repeat 16 "10")))
             "privacy_domain" (identity/hex->bytes (apply str (repeat 16 "20")))
             "revision" 1
             "manifest" (:manifest-id split)
             "object" (:object-id split)
             "layout_epoch" 7
             "replicas" 1
             "write_acks" 1
             "minimum_domains" {}
             "issued_at" 1000
             "retain_until" 5000
             "signer" authority}
            offer {"v" 1
                   "type" "blob/offer"
                   "session" session
                   "attempt" 0
                   "replica" authority
                   "retention" (sign-statement retention)
                   "manifest" (:manifest split)}
            {sender-state :state initial-actions :actions}
            (sender/handler (sender/make-state)
                            {:type :transfer/start
                             :peer :receiver
                             :authority authority
                             :frame offer}
                            {:now 1000})
            result (drive-file-transfer
                    source target sender-state
                    (transfer/make-state authority) initial-actions)
            event (:controller-event result)
            target-inventory (await (storage/-load-staging
                                     target session-2 (:manifest-id split)
                                     (:manifest split)))
            recorded (await (storage/-recorded-receipts source))]
        (is (= :replication/stored (:type event)))
        (is (= 0 (:attempt event)))
        (is (= "netz/store/fsync/1" (:persistence event)))
        (is (= "netz/store/fsync/1"
               (get-in target-inventory [:installed :persistence])))
        (is (= 1 (count recorded)))
        (is (= (dissoc event :receipt)
               (dissoc (first recorded) :receipt)))
        (is (empty? (get-in result [:states :sender :sessions])))
        (is (empty? (get-in result [:states :receiver :sessions])))
        (is (< (:steps result) 30))))))
