(ns netz.replication-transfer-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
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
(def session (identity/hex->bytes (apply str (repeat 16 "30"))))
(def cluster (identity/hex->bytes (apply str (repeat 16 "10"))))
(def privacy-domain (identity/hex->bytes (apply str (repeat 16 "20"))))
(def split (blob/split (identity/seq->bytes (range 20)) {:piece-size 8}))

(def retention
  {"v" 1 "type" "blob/retention"
   "cluster" cluster "privacy_domain" privacy-domain
   "revision" 1 "manifest" (:manifest-id split) "object" (:object-id split)
   "layout_epoch" 7 "replicas" 3 "write_acks" 2
   "minimum_domains" {"owner" 2} "issued_at" 1000
   "retain_until" 5000 "signer" authority})

(defn envelope [statement]
  (replication-wire/envelope
   genesis (identity/hex->bytes public-hex) statement
   (identity/seq->bytes (repeat identity/signature-size 0))))

(def offer
  {"v" 1 "type" "blob/offer" "session" session "attempt" 0
   "replica" authority "retention" (envelope retention)
   "manifest" (:manifest split)})

(defn step [state event]
  (transfer/handler
   state
   (cond-> event
     (:from event) (assoc :from-authority authority))
   {:now 1000}))

(defn started []
  (:state (step (transfer/make-state authority)
                {:type :transfer/authorized-offer :from :coordinator
                 :frame offer})))

(deftest durable-receiver-happy-path
  (let [{offered :state offer-actions :actions}
        (step (transfer/make-state authority)
              {:type :transfer/authorized-offer :from :coordinator :frame offer})
        held-piece (:id (second (:pieces split)))
        {loaded :state load-actions :actions}
        (step offered {:type :transfer/staging-loaded :session session
                       :held [held-piece]})
        need (get-in load-actions [0 2])
        first-piece (first (:pieces split))
        {pending-0 :state stage-0 :actions}
        (step loaded {:type :transfer/piece :from :coordinator
                      :frame {"v" 1 "type" "blob/piece" "session" session
                              "manifest" (:manifest-id split) "index" 0
                              "data" (:data first-piece)}})
        {staged-0 :state ack-0 :actions}
        (step pending-0 {:type :transfer/piece-staged :session session
                         :index 0 :result :ok})
        last-piece (nth (:pieces split) 2)
        {pending-2 :state}
        (step staged-0 {:type :transfer/piece :from :coordinator
                        :frame {"v" 1 "type" "blob/piece" "session" session
                                "manifest" (:manifest-id split) "index" 2
                                "data" (:data last-piece)}})
        {staged-2 :state ack-2 :actions}
        (step pending-2 {:type :transfer/piece-staged :session session
                         :index 2 :result :ok})
        {installing :state install-actions :actions}
        (step staged-2 {:type :transfer/commit :from :coordinator
                        :frame {"v" 1 "type" "blob/commit" "session" session
                                "manifest" (:manifest-id split) "count" 3}})
        {signing :state sign-actions :actions}
        (step installing {:type :transfer/installed :session session
                          :result :ok :persistence "netz/store/fsync/1"
                          :stored-at 1200})
        receipt (get-in sign-actions [0 2])
        {done :state done-actions :actions}
        (step signing {:type :transfer/receipt-signed :session session
                       :envelope (envelope receipt)})]
    (is (= :load-staging (ffirst offer-actions)))
    (is (= [0 2] (blob/needed-indices (:manifest split) (get need "need"))))
    (is (= :stage-piece (ffirst stage-0)))
    (is (= 1 (get-in ack-0 [0 2 "through"]))
        "the already staged middle piece extends the contiguous ACK")
    (is (= 2 (get-in ack-2 [0 2 "through"])))
    (is (= :install-object (ffirst install-actions)))
    (is (= "blob/stored" (get receipt "type")))
    (is (= 0 (get receipt "attempt")))
    (is (= "netz/store/fsync/1" (get receipt "persistence")))
    (is (= "blob/stored" (get-in done-actions [0 2 "type"])))
    (is (= :cleanup-staging (first (second done-actions))))
    (is (= :replica-installed (first (nth done-actions 2))))
    (is (= :replication/installed (get-in done-actions [2 1 :type])))
    (is (empty? (:sessions done)))
    (is (= 1 (get-in done [:stats :receipts])))))

(deftest no-piece-is-acknowledged-before-staging-completes
  (let [{loaded :state}
        (step (started) {:type :transfer/staging-loaded :session session :held []})
        piece-0 (first (:pieces split))
        {pending :state actions :actions}
        (step loaded {:type :transfer/piece :from :coordinator
                      :frame {"v" 1 "type" "blob/piece" "session" session
                              "manifest" (:manifest-id split) "index" 0
                              "data" (:data piece-0)}})]
    (is (= [:stage-piece] (mapv first actions)))
    (is (not-any? #(= :send (first %)) actions))
    (testing "a sender violating backpressure aborts rather than buffering"
      (let [piece-1 (second (:pieces split))
            {aborted :state abort-actions :actions}
            (step pending {:type :transfer/piece :from :coordinator
                           :frame {"v" 1 "type" "blob/piece" "session" session
                                   "manifest" (:manifest-id split) "index" 1
                                   "data" (:data piece-1)}})]
        (is (empty? (:sessions aborted)))
        (is (= "piece-before-stage" (get-in abort-actions [0 2 "code"])))))))

(deftest commit-and-receipt-require-the-durable-boundaries
  (let [{loaded :state}
        (step (started) {:type :transfer/staging-loaded :session session
                         :held (mapv :id (:pieces split))})
        commit-frame {"v" 1 "type" "blob/commit" "session" session
                      "manifest" (:manifest-id split) "count" 3}
        {installing :state actions :actions}
        (step loaded {:type :transfer/commit :from :coordinator
                      :frame commit-frame})]
    (is (= [:install-object] (mapv first actions)))
    (let [{failed :state failed-actions :actions}
          (step installing {:type :transfer/installed :session session
                            :result :full})]
      (is (empty? (:sessions failed)))
      (is (= "install-failed" (get-in failed-actions [0 2 "code"]))))
    (let [{rejected :state rejected-actions :actions}
          (step installing {:type :transfer/installed :session session
                            :result :ok
                            :persistence "netz/store/best-effort/1"
                            :stored-at 1200})]
      (is (empty? (:sessions rejected)))
      (is (= "unacceptable-persistence"
             (get-in rejected-actions [0 2 "code"]))))
    (let [{expired :state expired-actions :actions}
          (transfer/handler
           installing {:type :transfer/installed :session session
                       :result :ok :persistence "netz/store/fsync/1"
                       :stored-at 1200}
           {:now 5000})]
      (is (empty? (:sessions expired)))
      (is (= "lease-expired" (get-in expired-actions [0 2 "code"]))))))

(deftest restart-can-receipt-an-already-installed-object
  (let [{signing :state actions :actions}
        (step (started)
              {:type :transfer/staging-loaded :session session :held []
               :installed {:manifest (:manifest-id split)
                           :object (:object-id split)
                           :persistence "netz/store/fsync/1"
                           :stored-at 1100}})]
    (is (= :sign-receipt (ffirst actions)))
    (is (= :signing-receipt
           (-> signing :sessions vals first :phase)))
    (is (= 1 (get-in signing [:stats :resumed])))))

(deftest one-staged-address-satisfies-every-repeated-piece-position
  (let [repeated (blob/split (identity/seq->bytes [1 2 1 2]) {:piece-size 2})
        repeated-retention (assoc retention
                                  "manifest" (:manifest-id repeated)
                                  "object" (:object-id repeated))
        repeated-offer (assoc offer
                              "retention" (envelope repeated-retention)
                              "manifest" (:manifest repeated))
        {offered :state}
        (step (transfer/make-state authority)
              {:type :transfer/authorized-offer :from :coordinator
               :frame repeated-offer})
        {loaded :state actions :actions}
        (step offered {:type :transfer/staging-loaded :session session
                       :held [(:id (first (:pieces repeated)))]})]
    (is (= 2 (count (:pieces repeated))))
    (is (empty? (blob/needed-indices (:manifest repeated)
                                     (get-in actions [0 2 "need"]))))
    (is (empty? (-> loaded :sessions vals first :needed)))))

(deftest session-bounds-timeout-and-target-authority-fail-closed
  (let [wrong-offer (assoc offer "replica"
                           (identity/hex->bytes (apply str (repeat 32 "ff"))))
        {rejected :state actions :actions}
        (step (transfer/make-state authority)
              {:type :transfer/authorized-offer :from :coordinator
               :frame wrong-offer})]
    (is (empty? actions))
    (is (empty? (:sessions rejected)))
    (is (= 1 (get-in rejected [:stats :rejected]))))
  (let [{expired :state actions :actions}
        (transfer/handler
         (transfer/make-state authority)
         {:type :transfer/authorized-offer :from :coordinator
          :from-authority authority :frame offer}
         {:now 5000})]
    (is (empty? actions))
    (is (empty? (:sessions expired)))
    (is (= 1 (get-in expired [:stats :rejected]))))
  (let [{timed-out :state actions :actions}
        (step (started) {:type :transfer/timeout :session session})]
    (is (empty? (:sessions timed-out)))
    (is (= "timeout" (get-in actions [0 2 "code"])))
    (is (= :cleanup-staging (first (second actions))))))

(deftest remote-abort-cleans-receiver-state-without-an-abort-loop
  (let [{state :state actions :actions}
        (step (started)
              {:type :transfer/abort
               :from :coordinator
               :frame {"v" 1
                       "type" "blob/abort"
                       "session" session
                       "code" "piece-load-failed"}})]
    (is (empty? (:sessions state)))
    (is (= [[:cleanup-staging session (:manifest-id split)]] actions))
    (is (= 1 (get-in state [:stats :aborted])))))

(deftest every-remote-frame-is-bound-to-the-offers-full-authority
  (let [{loaded :state}
        (step (started) {:type :transfer/staging-loaded
                         :session session :held []})
        piece-0 (first (:pieces split))
        wrong-authority (identity/hex->bytes (apply str (repeat 32 "ff")))
        {aborted :state actions :actions}
        (transfer/handler
         loaded
         {:type :transfer/piece
          :from :coordinator
          :from-authority wrong-authority
          :frame {"v" 1 "type" "blob/piece" "session" session
                  "manifest" (:manifest-id split) "index" 0
                  "data" (:data piece-0)}}
         {:now 1000})]
    (is (empty? (:sessions aborted)))
    (is (= "unknown-session" (get-in actions [0 2 "code"])))
    (is (= :cleanup-staging (first (second actions))))))
