(ns netz.replication-file-store-test
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.file-store :as file-store]
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

(defn controller-event [receipt]
  {:type :replication/stored
   :object-id (identity/bytes->hex (:object-id split))
   :replica-id (identity/bytes->hex authority)
   :layout-epoch 7
   :attempt 0
   :persistence "netz/store/fsync/1"
   :receipt receipt
   :manifest-id (identity/bytes->hex (:manifest-id split))})

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

(defn machine-step [side state event]
  ((if (= side :sender) sender/handler transfer/handler)
   state event {:now 1000}))

(defn queue-actions [queue side actions]
  (into queue (map #(vector side %) actions)))

(defn route-frame [side frame]
  (if (= side :sender)
    (case (get frame "type")
      "blob/offer" {:type :transfer/authorized-offer
                    :from :sender :frame frame}
      "blob/piece" {:type :transfer/piece :from :sender :frame frame}
      "blob/commit" {:type :transfer/commit :from :sender :frame frame})
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
