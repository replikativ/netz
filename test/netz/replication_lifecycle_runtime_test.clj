(ns netz.replication-lifecycle-runtime-test
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :as async :refer [<!! go <!]]
            [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.layout :as layout]
            [netz.replication.lifecycle :as lifecycle]
            [netz.replication.lifecycle-runtime :as lifecycle-runtime]
            [netz.replication.lifecycle-storage :as lifecycle-storage]
            [netz.replication.wire :as replication-wire]))

(def public-hex
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
(def private-hex
  "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
(def controller-genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "aa"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "cc"))}))
(def controller (identity/authority-id-bytes controller-genesis))
(def controller-identity
  {:genesis controller-genesis
   :operational {:public (identity/hex->bytes public-hex)
                 :private (identity/hex->bytes private-hex)}})
(def replica-genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "dd"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "ee"))}))
(def replica (identity/authority-id-bytes replica-genesis))
(def replica-identity
  {:genesis replica-genesis
   :operational {:public (identity/hex->bytes public-hex)
                 :private (identity/hex->bytes private-hex)}})
(def split (blob/split (identity/seq->bytes (range 20)) {:piece-size 8}))

(def retention
  {"v" 1 "type" "blob/retention"
   "cluster" (identity/hex->bytes (apply str (repeat 16 "10")))
   "privacy_domain" (identity/hex->bytes (apply str (repeat 16 "20")))
   "revision" 1 "manifest" (:manifest-id split)
   "object" (:object-id split) "layout_epoch" 7
   "replicas" 3 "write_acks" 2
   "placement_minimum_domains" {"owner" 2}
   "ack_minimum_domains" {"owner" 2}
   "issued_at" 1000 "retain_until" 5000 "signer" controller})

(defn await [channel]
  (let [value (<!! channel)]
    (if (instance? Throwable value) (throw value) value)))

(defrecord MemoryLifecycleStore [records persist-gate fail-persist? deletes]
  lifecycle-storage/PBlobLifecycleStore

  (-load-lifecycle-records [_]
    (lifecycle-storage/chan-of @records))

  (-persist-lifecycle! [_ key value]
    (go
      (when persist-gate (<! persist-gate))
      (if @fail-persist?
        (ex-info "journal unavailable" {})
        (do
          (when (= :blob/lifecycle (first key))
            (swap! records assoc (second key) value))
          :ok))))

  (-delete-object! [_ key _statement persistence]
    (swap! deletes conj key)
    (lifecycle-storage/chan-of
     {:result :ok :persistence persistence :deleted-at 5001})))

(defn memory-store
  ([] (memory-store (atom {}) nil))
  ([records gate]
   (->MemoryLifecycleStore records gate (atom false) (atom []))))

(defn start-runtime
  ([store] (start-runtime store {}))
  ([store opts]
   (lifecycle-runtime/start!
    (merge {:identity replica-identity
            :store store
            :controllers [controller]
            :lifecycle-opts
            {:persistence "netz/store/best-effort/1"
             :accepted-persistence #{"netz/store/best-effort/1"}}
            :now-fn (constantly 1000)}
           opts))))

(defn request
  ([] (request retention replica))
  ([statement target]
   {:from :controller
    :from-authority controller
    :retention statement
    :frame {"replica" target}}))

(defn reconciled-layout [authorities]
  (let [members (->> authorities
                     (sort-by identity/bytes->hex)
                     (map-indexed
                      (fn [index authority]
                        {"authority" authority
                         "domains" {"owner" (str "owner-" index)}
                         "enabled" true}))
                     vec)
        statement {"v" 1 "type" "blob/layout"
                   "placement" "netz/blob-placement/2"
                   "seed" (identity/hex->bytes (apply str (repeat 32 "aa")))
                   "cluster" (get retention "cluster")
                   "epoch" 0
                   "previous" (identity/hex->bytes
                               (apply str (repeat 32 "00")))
                   "members" members "issued_at" 900
                   "signer" controller}]
    (:state (layout/reconcile
             (layout/make-state (get retention "cluster") [controller])
             statement))))

(deftest authorization-crosses-the-durable-journal-before-success
  (let [gate (async/chan)
        store (memory-store (atom {}) gate)
        runtime (start-runtime store)]
    (is (true? (await (lifecycle-runtime/ready runtime))))
    (let [decision (lifecycle-runtime/authorize-offer! runtime (request))]
      (is (nil? (async/poll! decision))
          "authorization remains pending while journal IO is pending")
      (async/>!! gate true)
      (is (true? (await decision)))
      (is (= :retained
             (lifecycle/status (lifecycle-runtime/state runtime) retention)))
      (is (= 1 (:authorized (lifecycle-runtime/stats runtime)))))
    (lifecycle-runtime/stop! runtime)))

(deftest restart-replays-journal-and-policy-fails-closed
  (let [records (atom {})
        first-store (memory-store records nil)
        first-runtime (start-runtime first-store)]
    (await (lifecycle-runtime/ready first-runtime))
    (is (true? (await (lifecycle-runtime/authorize-offer!
                       first-runtime (request)))))
    (lifecycle-runtime/stop! first-runtime)

    (let [restarted (start-runtime (memory-store records nil))]
      (is (true? (await (lifecycle-runtime/ready restarted))))
      (is (= :retained
             (lifecycle/status (lifecycle-runtime/state restarted) retention)))
      (is (true? (await (lifecycle-runtime/authorize-offer!
                         restarted (request))))
          "an exact durable duplicate is authorized without weakening policy")
      (is (false? (await (lifecycle-runtime/authorize-offer!
                          restarted
                          (request retention
                                   (identity/hex->bytes
                                    (apply str (repeat 32 "ff"))))))))
      (is (false? (await (lifecycle-runtime/authorize-offer!
                          restarted
                          (request (assoc retention "retain_until" 6000)
                                   replica))))
          "same-revision equivocation is never authorized")
      (lifecycle-runtime/stop! restarted))))

(deftest failed-journal-and-deployment-policy-never-authorize
  (testing "persistence failure"
    (let [store (memory-store)
          _ (reset! (:fail-persist? store) true)
          runtime (start-runtime store)]
      (await (lifecycle-runtime/ready runtime))
      (is (false? (await (lifecycle-runtime/authorize-offer!
                          runtime (request)))))
      (is (= :unknown
             (lifecycle/status (lifecycle-runtime/state runtime) retention)))
      (lifecycle-runtime/stop! runtime)))

  (testing "operator admission runs before journal IO"
    (let [store (memory-store)
          runtime (start-runtime store {:admit-offer? (constantly false)})]
      (await (lifecycle-runtime/ready runtime))
      (is (false? (await (lifecycle-runtime/authorize-offer!
                          runtime (request)))))
      (is (empty? @(:records store)))
      (lifecycle-runtime/stop! runtime))))

(deftest reconciled-layout-fences-retention-before-journal-io
  (let [statement (assoc retention "layout_epoch" 0)
        included (reconciled-layout
                  [replica
                   (identity/hex->bytes (apply str (repeat 32 "01")))
                   (identity/hex->bytes (apply str (repeat 32 "02")))])
        excluded (reconciled-layout
                  (mapv #(identity/hex->bytes (apply str (repeat 32 %)))
                        ["01" "02" "03"]))]
    (testing "a selected member accepts the retained object"
      (let [runtime (start-runtime
                     (memory-store)
                     {:layout-state-fn (constantly included)})]
        (await (lifecycle-runtime/ready runtime))
        (is (true? (await (lifecycle-runtime/authorize-offer!
                           runtime (request statement replica)))))
        (lifecycle-runtime/stop! runtime)))
    (testing "an authenticated but unselected member fails before persistence"
      (let [store (memory-store)
            runtime (start-runtime
                     store {:layout-state-fn (constantly excluded)})]
        (await (lifecycle-runtime/ready runtime))
        (is (false? (await (lifecycle-runtime/authorize-offer!
                            runtime (request statement replica)))))
        (is (false? (await (lifecycle-runtime/apply-envelope!
                            runtime
                            (await (replication-wire/sign-statement
                                    controller-identity statement)))))
            "control-stream retention cannot retain on a removed replica")
        (is (empty? @(:records store)))
        (lifecycle-runtime/stop! runtime)))))

(deftest corrupt-restored-journals-fail-startup-closed
  (let [key (str (identity/bytes->hex (get retention "cluster")) "/"
                 (identity/bytes->hex (get retention "privacy_domain")) "/"
                 (identity/bytes->hex (get retention "object")))
        corrupt (atom {key {:statement retention
                            :controller (identity/bytes->hex controller)
                            :revision 1
                            :fingerprint "not-the-statement-hash"
                            :status :retained}})
        runtime (start-runtime (memory-store corrupt nil))
        ready-result (<!! (lifecycle-runtime/ready runtime))]
    (is (instance? Throwable ready-result))
    (is (false? (await (lifecycle-runtime/authorize-offer!
                        runtime (request)))))
    (is (= 1 (:startup-errors (lifecycle-runtime/stats runtime))))
    (lifecycle-runtime/stop! runtime)))

(deftest signed-lease-control-records-verify-and-journal
  (let [store (memory-store)
        runtime (start-runtime store)
        lease {"v" 1 "type" "blob/lease"
               "cluster" (get retention "cluster")
               "privacy_domain" (get retention "privacy_domain")
               "revision" 2 "manifest" (:manifest-id split)
               "object" (:object-id split) "layout_epoch" 7
               "issued_at" 1100 "retain_until" 6000
               "signer" controller}]
    (await (lifecycle-runtime/ready runtime))
    (is (true? (await (lifecycle-runtime/authorize-offer!
                       runtime (request)))))
    (let [envelope (await (replication-wire/sign-statement
                           controller-identity lease))]
      (is (true? (await (lifecycle-runtime/apply-envelope!
                         runtime envelope))))
      (is (= 2 (-> (lifecycle-runtime/state runtime)
                   :records vals first :revision)))
      (is (false? (await (lifecycle-runtime/apply-envelope!
                          runtime
                          (assoc-in envelope ["statement" "retain_until"]
                                    7000))))
          "a structurally valid but tampered envelope is rejected"))
    (lifecycle-runtime/stop! runtime)))

(deftest deletion-is-erased-signed-journaled-and-replayed
  (let [records (atom {})
        store (memory-store records nil)
        emitted (atom [])
        opts {:on-deletion-receipt! #(swap! emitted conj %)}
        runtime (start-runtime store opts)
        deletion {"v" 1 "type" "blob/deletion"
                  "cluster" (get retention "cluster")
                  "privacy_domain" (get retention "privacy_domain")
                  "revision" 2 "manifest" (:manifest-id split)
                  "object" (:object-id split) "layout_epoch" 7
                  "issued_at" 1000 "delete_after" 1000
                  "signer" controller}]
    (await (lifecycle-runtime/ready runtime))
    (is (true? (await (lifecycle-runtime/authorize-offer!
                       runtime (request)))))
    (let [envelope (await (replication-wire/sign-statement
                           controller-identity deletion))]
      (is (true? (await (lifecycle-runtime/apply-envelope!
                         runtime envelope)))))
    (is (= :deleted
           (lifecycle/status (lifecycle-runtime/state runtime) deletion)))
    (is (= 1 (count @(:deletes store))))
    (is (= 1 (count @emitted)))
    (is (true? (await (replication-wire/verify-envelope
                       (:envelope (first @emitted))))))
    (lifecycle-runtime/stop! runtime)

    (let [replayed (atom [])
          restarted (start-runtime (memory-store records nil)
                                   {:on-deletion-receipt!
                                    #(swap! replayed conj %)})]
      (is (true? (await (lifecycle-runtime/ready restarted))))
      (is (= 1 (count @replayed))
          "a signed durable deletion receipt is re-emitted after restart")
      (lifecycle-runtime/stop! restarted))))
