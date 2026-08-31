(ns netz.replication-sender-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication :as replication]
            [netz.replication.blob :as blob]
            [netz.replication.sender :as sender]
            [netz.replication.transfer :as transfer]
            [netz.replication.wire :as replication-wire]))

(def public-hex
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
(def genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "aa"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "cc"))}))
(def authority (identity/authority-id-bytes genesis))
(def authority-hex (identity/bytes->hex authority))
(def session (identity/hex->bytes (apply str (repeat 16 "30"))))
(def session-2 (identity/hex->bytes (apply str (repeat 16 "31"))))
(def cluster (identity/hex->bytes (apply str (repeat 16 "10"))))
(def privacy-domain (identity/hex->bytes (apply str (repeat 16 "20"))))
(def split (blob/split (identity/seq->bytes (range 20)) {:piece-size 8}))

(def retention
  {"v" 1
   "type" "blob/retention"
   "cluster" cluster
   "privacy_domain" privacy-domain
   "revision" 1
   "manifest" (:manifest-id split)
   "object" (:object-id split)
   "layout_epoch" 7
   "replicas" 1
   "write_acks" 1
   "minimum_domains" {}
   "issued_at" 1000
   "retain_until" 5000
   "signer" authority})

(defn envelope [statement]
  (replication-wire/envelope
   genesis (identity/hex->bytes public-hex) statement
   (identity/seq->bytes (repeat identity/signature-size 0))))

(def offer
  {"v" 1
   "type" "blob/offer"
   "session" session
   "attempt" 0
   "replica" authority
   "retention" (envelope retention)
   "manifest" (:manifest split)})

(defn sender-step [state event]
  (sender/handler state event {:now 1000}))

(defn receiver-step [state event]
  (transfer/handler state event {:now 1000}))

(defn inbound [type frame]
  {:type type
   :from :receiver
   :from-authority authority
   :frame frame})

(defn start-sender
  ([] (start-sender (sender/make-state) offer))
  ([state frame]
   (sender-step state {:type :transfer/start
                       :peer :receiver
                       :authority authority
                       :frame frame})))

(defn receipt-statement
  ([] (receipt-statement {}))
  ([overrides]
   (merge
    {"v" 1
     "type" "blob/stored"
     "cluster" cluster
     "privacy_domain" privacy-domain
     "retention_revision" 1
     "manifest" (:manifest-id split)
     "object" (:object-id split)
     "layout_epoch" 7
     "replica" authority
     "attempt" 0
     "persistence" "netz/store/fsync/1"
     "stored_at" 1200
     "retain_until" 5000
     "signer" authority}
    overrides)))

(defn stored-frame
  ([transfer-session] (stored-frame transfer-session {}))
  ([transfer-session overrides]
   {"v" 1
    "type" "blob/stored"
    "session" transfer-session
    "receipt" (envelope (receipt-statement overrides))}))

(deftest sender-and-receiver-complete-one-durable-transfer
  (let [{sender-0 :state start-actions :actions} (start-sender)
        offer-frame (get-in start-actions [0 2])
        {receiver-0 :state} (receiver-step
                             (transfer/make-state authority)
                             {:type :transfer/authorized-offer
                              :from :sender
                              :frame offer-frame})
        held-piece (:id (second (:pieces split)))
        {receiver-1 :state need-actions :actions}
        (receiver-step receiver-0
                       {:type :transfer/staging-loaded
                        :session session
                        :held [held-piece]})
        need-frame (get-in need-actions [0 2])
        {sender-1 :state load-0-actions :actions}
        (sender-step sender-0 (inbound :transfer/need need-frame))
        piece-0 (first (:pieces split))
        {sender-2 :state piece-0-actions :actions}
        (sender-step sender-1
                     {:type :transfer/piece-loaded
                      :session session
                      :index 0
                      :result :ok
                      :data (:data piece-0)})
        piece-0-frame (get-in piece-0-actions [0 2])
        {receiver-2 :state stage-0-actions :actions}
        (receiver-step receiver-1
                       {:type :transfer/piece
                        :from :sender
                        :frame piece-0-frame})
        {receiver-3 :state ack-0-actions :actions}
        (receiver-step receiver-2
                       {:type :transfer/piece-staged
                        :session session
                        :index 0
                        :result :ok})
        ack-0-frame (get-in ack-0-actions [0 2])
        {sender-3 :state load-2-actions :actions}
        (sender-step sender-2 (inbound :transfer/ack ack-0-frame))
        piece-2 (nth (:pieces split) 2)
        {sender-4 :state piece-2-actions :actions}
        (sender-step sender-3
                     {:type :transfer/piece-loaded
                      :session session
                      :index 2
                      :result :ok
                      :data (:data piece-2)})
        {receiver-4 :state}
        (receiver-step receiver-3
                       {:type :transfer/piece
                        :from :sender
                        :frame (get-in piece-2-actions [0 2])})
        {receiver-5 :state ack-2-actions :actions}
        (receiver-step receiver-4
                       {:type :transfer/piece-staged
                        :session session
                        :index 2
                        :result :ok})
        {sender-5 :state commit-actions :actions}
        (sender-step sender-4
                     (inbound :transfer/ack (get-in ack-2-actions [0 2])))
        {receiver-6 :state install-actions :actions}
        (receiver-step receiver-5
                       {:type :transfer/commit
                        :from :sender
                        :frame (get-in commit-actions [0 2])})
        {receiver-7 :state sign-actions :actions}
        (receiver-step receiver-6
                       {:type :transfer/installed
                        :session session
                        :result :ok
                        :persistence "netz/store/fsync/1"
                        :stored-at 1200})
        receipt-body (get-in sign-actions [0 2])
        {receiver-8 :state stored-actions :actions}
        (receiver-step receiver-7
                       {:type :transfer/receipt-signed
                        :session session
                        :envelope (envelope receipt-body)})
        stored (get-in stored-actions [0 2])
        {sender-6 :state persist-actions :actions}
        (sender-step sender-5
                     (inbound :transfer/verified-stored stored))
        controller-event (get-in persist-actions [0 2])
        {sender-7 :state controller-actions :actions}
        (sender-step sender-6
                     {:type :transfer/receipt-persisted
                      :session session
                      :result :ok})]
    (is (= [:send :timer] (mapv first start-actions)))
    (is (= [0 2]
           (blob/needed-indices (:manifest split) (get need-frame "need"))))
    (is (= [:load-piece session (:manifest-id split) 0 (:id piece-0)]
           (first load-0-actions)))
    (is (nil? (-> sender-2 :sessions vals first :data))
        "piece bytes are not retained while awaiting an ACK")
    (is (= 1 (get ack-0-frame "through")))
    (is (= 2 (get-in load-2-actions [0 3]))
        "the cumulative ACK skips a repeated or already-held position")
    (is (= "blob/commit" (get-in commit-actions [0 2 "type"])))
    (is (= :install-object (ffirst install-actions)))
    (is (= :persist-receipt (ffirst persist-actions)))
    (is (= :persisting-receipt
           (-> sender-6 :sessions vals first :phase)))
    (is (= :replication/stored (:type controller-event)))
    (is (= authority-hex (:replica-id controller-event)))
    (is (= 0 (:attempt controller-event)))
    (is (= [:replication-stored] (mapv first controller-actions)))
    (is (empty? (:sessions sender-7)))
    (is (empty? (:sessions receiver-8)))
    (is (= 1 (get-in sender-7 [:stats :receipts])))))

(deftest signed-receipt-crosses-persistence-before-quorum
  (let [layout {:epoch 7
                :members [{:id authority-hex :domains {"party" "remote"}}]}
        policy {:replicas 1 :write-acks 1 :minimum-domains {}}
        manifest {:object-id (identity/bytes->hex (:object-id split))
                  :manifest-id (identity/bytes->hex (:manifest-id split))}
        controller-0 (replication/make-state layout policy)
        {controller-1 :state} (replication/handler
                               controller-0
                               {:type :replication/begin :manifest manifest}
                               {})
        {controller-2 :state} (replication/handler
                               controller-1
                               {:type :replication/intent-stored
                                :object-id (:object-id manifest)
                                :layout-epoch 7}
                               {})
        {sender-0 :state} (start-sender)
        all-held (blob/need-bitmap (:manifest split) (constantly true))
        {sender-1 :state} (sender-step
                           sender-0
                           (inbound :transfer/need
                                    {"v" 1 "type" "blob/need"
                                     "session" session
                                     "manifest" (:manifest-id split)
                                     "need" all-held}))
        {sender-2 :state persist-actions :actions}
        (sender-step sender-1
                     (inbound :transfer/verified-stored
                              (stored-frame session)))
        controller-event (get-in persist-actions [0 2])
        {sender-3 :state released :actions}
        (sender-step sender-2
                     {:type :transfer/receipt-persisted
                      :session session
                      :result :ok})
        {controller-3 :state quorum-actions :actions}
        (replication/handler controller-2 (second (first released)) {})]
    (is (= :persist-receipt (ffirst persist-actions)))
    (is (= :persisting-receipt
           (-> sender-2 :sessions vals first :phase)))
    (is (empty? (get-in controller-2
                        [:objects (:object-id manifest) :acks]))
        "the verified but unrecorded receipt is not yet a quorum vote")
    (is (= [:durable :complete] (mapv first quorum-actions)))
    (is (= #{authority-hex}
           (get-in controller-3 [:objects (:object-id manifest) :acks])))
    (is (= (:receipt controller-event)
           (get-in controller-3
                   [:objects (:object-id manifest) :receipts authority-hex])))
    (is (= (:receipt controller-event)
           (get-in quorum-actions [0 1 :signed-receipts authority-hex])))
    (is (empty? (:sessions sender-3)))
    (is (= controller-event (second (first released))))))

(deftest cumulative-acks-enforce-one-piece-in-flight
  (let [repeated (blob/split (identity/seq->bytes [1 2 1 2]) {:piece-size 2})
        repeated-retention (assoc retention
                                  "manifest" (:manifest-id repeated)
                                  "object" (:object-id repeated))
        repeated-offer (assoc offer
                              "retention" (envelope repeated-retention)
                              "manifest" (:manifest repeated))
        {started :state} (start-sender (sender/make-state) repeated-offer)
        need-frame {"v" 1 "type" "blob/need" "session" session
                    "manifest" (:manifest-id repeated)
                    "need" (blob/need-bitmap (:manifest repeated) #{})}
        {loading :state actions :actions}
        (sender-step started (inbound :transfer/need need-frame))
        {waiting :state sent :actions}
        (sender-step loading
                     {:type :transfer/piece-loaded
                      :session session
                      :index 0
                      :result :ok
                      :data (:data (first (:pieces repeated)))})
        ack-frame {"v" 1 "type" "blob/ack" "session" session
                   "manifest" (:manifest-id repeated) "through" 1}
        {committing :state commit :actions}
        (sender-step waiting (inbound :transfer/ack ack-frame))
        duplicate (sender-step committing (inbound :transfer/ack ack-frame))]
    (is (= [0] (mapv #(get-in % [3]) actions)))
    (is (= [:send] (mapv first sent)))
    (is (= "blob/commit" (get-in commit [0 2 "type"])))
    (is (= :awaiting-receipt
           (-> committing :sessions vals first :phase)))
    (is (= 1 (get-in committing [:stats :pieces-sent])))
    (is (= :awaiting-receipt
           (-> duplicate :state :sessions vals first :phase)))
    (is (empty? (:actions duplicate)))
    (is (= 1 (get-in (:state duplicate) [:stats :duplicates])))))

(deftest stale-attempts-wrong-connections-and-bounds-fail-closed
  (testing "a timeout releases the bounded slot and cannot acknowledge a retry"
    (let [{old :state} (start-sender)
          {timed-out :state failure-actions :actions}
          (sender-step old {:type :transfer/sender-timeout :session session})
          retry-offer (assoc offer "session" session-2 "attempt" 1)
          {retrying :state} (start-sender timed-out retry-offer)
          stale (sender-step retrying
                             (inbound :transfer/verified-stored
                                      (stored-frame session)))]
      (is (= [:send :transfer-failed] (mapv first failure-actions)))
      (is (= :timeout (get-in failure-actions [1 1 :code])))
      (is (= 1 (count (:sessions (:state stale)))))
      (is (= 1 (get-in (:state stale) [:stats :rejected])))
      (is (= 1 (get-in (:state stale) [:stats :timeouts])))))

  (testing "an authenticated connection cannot speak for another replica"
    (let [{started :state} (start-sender)
          wrong (identity/hex->bytes (apply str (repeat 32 "ff")))
          need-frame {"v" 1 "type" "blob/need" "session" session
                      "manifest" (:manifest-id split)
                      "need" (blob/need-bitmap (:manifest split) #{})}
          {stopped :state actions :actions}
          (sender-step started
                       {:type :transfer/need
                        :from :receiver
                        :from-authority wrong
                        :frame need-frame})]
      (is (empty? (:sessions stopped)))
      (is (= "connection-mismatch" (get-in actions [0 2 "code"])))
      (is (= :connection-mismatch (get-in actions [1 1 :code])))))

  (testing "session count is bounded"
    (let [{one :state} (start-sender (sender/make-state {:max-sessions 1}) offer)
          second-offer (assoc offer "session" session-2)
          {bounded :state actions :actions}
          (start-sender one second-offer)]
      (is (empty? actions))
      (is (= 1 (count (:sessions bounded))))
      (is (= 1 (get-in bounded [:stats :rejected]))))))

(deftest invalid-local-data-and-receipts-never-reach-the-controller
  (let [{started :state} (start-sender)
        need-frame {"v" 1 "type" "blob/need" "session" session
                    "manifest" (:manifest-id split)
                    "need" (blob/need-bitmap (:manifest split) #{})}
        {loading :state} (sender-step started
                                      (inbound :transfer/need need-frame))
        {failed :state actions :actions}
        (sender-step loading
                     {:type :transfer/piece-loaded
                      :session session
                      :index 0
                      :result :ok
                      :data (identity/seq->bytes [0])})]
    (is (empty? (:sessions failed)))
    (is (= :invalid-local-piece (get-in actions [1 1 :code]))))
  (let [{started :state} (start-sender)
        all-held (blob/need-bitmap (:manifest split) (constantly true))
        {waiting :state} (sender-step
                          started
                          (inbound :transfer/need
                                   {"v" 1 "type" "blob/need"
                                    "session" session
                                    "manifest" (:manifest-id split)
                                    "need" all-held}))
        {rejected :state actions :actions}
        (sender-step waiting
                     (inbound :transfer/verified-stored
                              (stored-frame
                               session
                               {"persistence" "netz/store/best-effort/1"})))]
    (is (empty? (:sessions rejected)))
    (is (= [:send :transfer-failed] (mapv first actions)))
    (is (not-any? #(= :replication-stored (first %)) actions))))

(deftest receipt-persistence-failure-retries-through-the-controller
  (let [{started :state} (start-sender)
        all-held (blob/need-bitmap (:manifest split) (constantly true))
        {waiting :state} (sender-step
                          started
                          (inbound :transfer/need
                                   {"v" 1 "type" "blob/need"
                                    "session" session
                                    "manifest" (:manifest-id split)
                                    "need" all-held}))
        {persisting :state} (sender-step
                             waiting
                             (inbound :transfer/verified-stored
                                      (stored-frame session)))
        {failed :state actions :actions}
        (sender-step persisting
                     {:type :transfer/receipt-persisted
                      :session session
                      :result :disk-full})]
    (is (empty? (:sessions failed)))
    (is (= [:transfer-failed] (mapv first actions)))
    (is (= :receipt-persist-failed (get-in actions [0 1 :code])))
    (is (not-any? #(= :replication-stored (first %)) actions))))

(deftest sender-restart-resumes-receiver-staging-with-a-fresh-attempt
  (let [{old-sender-0 :state old-start :actions} (start-sender)
        {receiver-0 :state} (receiver-step
                             (transfer/make-state authority)
                             {:type :transfer/authorized-offer
                              :from :sender
                              :frame (get-in old-start [0 2])})
        {receiver-1 :state old-need :actions}
        (receiver-step receiver-0
                       {:type :transfer/staging-loaded
                        :session session
                        :held []})
        {old-sender-1 :state old-load :actions}
        (sender-step old-sender-0
                     (inbound :transfer/need (get-in old-need [0 2])))
        piece-0 (first (:pieces split))
        {old-sender-2 :state old-piece :actions}
        (sender-step old-sender-1
                     {:type :transfer/piece-loaded
                      :session session
                      :index 0
                      :result :ok
                      :data (:data piece-0)})
        {receiver-2 :state} (receiver-step
                             receiver-1
                             {:type :transfer/piece
                              :from :sender
                              :frame (get-in old-piece [0 2])})
        {receiver-3 :state dropped-ack :actions}
        (receiver-step receiver-2
                       {:type :transfer/piece-staged
                        :session session
                        :index 0
                        :result :ok})
        retry-offer (assoc offer "session" session-2 "attempt" 1)
        {new-sender-0 :state retry-start :actions}
        ;; The old volatile sender state is intentionally discarded here.
        (start-sender (sender/make-state) retry-offer)
        {receiver-4 :state} (receiver-step
                             receiver-3
                             {:type :transfer/authorized-offer
                              :from :sender
                              :frame (get-in retry-start [0 2])})
        {receiver-5 :state retry-need :actions}
        (receiver-step receiver-4
                       {:type :transfer/staging-loaded
                        :session session-2
                        :held [(:id piece-0)]})
        delayed-old-ack (sender-step
                         new-sender-0
                         (inbound :transfer/ack (get-in dropped-ack [0 2])))
        retry-ready (sender-step
                     (:state delayed-old-ack)
                     (inbound :transfer/need (get-in retry-need [0 2])))]
    (is (= :awaiting-ack
           (-> old-sender-2 :sessions vals first :phase)))
    (is (= [1 2]
           (blob/needed-indices (:manifest split)
                                (get-in retry-need [0 2 "need"]))))
    (is (= 1 (count (:sessions (:state delayed-old-ack))))
        "a delayed frame for the pre-crash session cannot disturb the retry")
    (is (= 1 (get-in (:state delayed-old-ack) [:stats :rejected])))
    (is (= [:load-piece session-2 (:manifest-id split) 1
            (:id (second (:pieces split)))]
           (first (:actions retry-ready))))
    (is (= 2 (count (:sessions receiver-5)))
        "receiver staging can outlive either volatile session until cleanup")))
