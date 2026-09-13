(ns netz.replication-process-fixture
  "Owned child JVM for the live durable-controller restart test. Not a public API."
  (:refer-clojure :exclude [await run!])
  (:require [clojure.core.async :as async]
            [clojure.edn :as edn]
            [netz.identity :as identity]
            [netz.overlay.runtime :as overlay]
            [netz.replication.blob :as blob]
            [netz.replication.controller-runtime :as controller]
            [netz.replication.file-store :as file-store]
            [netz.replication.layout :as layout]
            [netz.replication.runtime :as transfer]
            [netz.replication.storage :as storage]
            [netz.replication.wire :as replication-wire]
            [netz.replication-runtime-integration-test :as carrier]
            [netz.transport.noise :as noise]
            [netz.wire :as wire]))

(def capabilities
  (conj noise/default-capabilities replication-wire/capability
        overlay/extension-carrier-capability wire/profile-capability))

(defn await [channel]
  (let [[value port] (async/alts!! [channel (async/timeout 30000)])]
    (when-not (= port channel) (throw (ex-info "Fixture operation timed out" {})))
    (if (instance? Throwable value) (throw value) value)))

(defn require! [condition message]
  (when-not condition (throw (ex-info message {}))))

(defn fixture-data [{:keys [private public genesis receivers]}]
  (let [signer {:genesis genesis :operational {:private (identity/hex->bytes private)
                                               :public (identity/hex->bytes public)}}
        authority (identity/authority-id-bytes genesis)
        cluster (identity/seq->bytes (range 16))
        split (blob/split (identity/utf8-bytes "three replicas across a writer process crash")
                          {:piece-size 8})
        members (fn [indices]
                  (->> indices
                       (map (fn [i] {"authority" (identity/hex->bytes (:authority (nth receivers i)))
                                     "domains" {"owner" (str i)} "enabled" true}))
                       (sort-by #(identity/bytes->hex (get % "authority"))) vec))
        initial {"v" 1 "type" "blob/layout" "cluster" cluster "epoch" 0
                 "placement" "netz/blob-placement/2"
                 "seed" (identity/seq->bytes (repeat 32 42))
                 "previous" (identity/seq->bytes (repeat 32 0))
                 "members" (members [0 1 2]) "issued_at" 900 "signer" authority}
        successor (assoc initial "epoch" 1 "previous" (layout/address initial)
                         "members" (members [1 2 3]) "issued_at" 1000)
        retention {"v" 1 "type" "blob/retention" "cluster" cluster
                   "privacy_domain" (identity/seq->bytes (range 16 32))
                   "revision" 1 "manifest" (:manifest-id split) "object" (:object-id split)
                   "layout_epoch" 0 "replicas" 3 "write_acks" 2
                   "placement_minimum_domains" {"owner" 3}
                   "ack_minimum_domains" {"owner" 2} "issued_at" 1000
                   "retain_until" 5000 "signer" authority}]
    {:signer signer :authority authority :cluster cluster :split split
     :initial initial :successor successor :retention retention}))

(defn accepted-state [{:keys [cluster authority initial successor]} recover?]
  (let [initial-state (:state (layout/reconcile (layout/make-state cluster [authority]) initial))]
    (if recover? (:state (layout/reconcile initial-state successor)) initial-state)))

(defn milestone! [value]
  (println (str "NETZ-PROCESS " (pr-str value)))
  (flush))

(defn run! [config recover?]
  (let [{:keys [signer authority cluster split initial successor retention] :as data}
        (fixture-data config)
        store (file-store/new-file-store (:root config) {:durable-ancestor (:root config)
                                                         :clock (constantly 1200)})
        state (atom (accepted-state data recover?))
        seeds (mapv (fn [{:keys [authority url peer-id]}]
                      {:peer-id peer-id
                       :authority-id authority :addresses [url] :group authority})
                    (:receivers config))
        node (carrier/start-client nil signer {:seeds seeds} capabilities)
        controller-ref (atom nil)
        dispatch? (atom true)
        failures (atom [])
        sender (transfer/start!
                (:node node) {:store store :now-fn (constantly 1100) :replay-receipts? false
                              :on-failed! #(swap! failures conj %)
                              :on-stored! #(when-let [ctx @controller-ref]
                                             (controller/notify-stored! ctx %))})
        ctx (controller/start!
             {:store store :cluster cluster :controllers [authority]
              :layout-state-fn #(deref state) :now-fn (constantly 1100)
              :dispatch-offer!
              (fn [{:keys [authority offer]}]
                (if-not @dispatch?
                  true
                  (let [id (identity/bytes->hex authority)
                        peers (filter #(= id (:authority-id (overlay/connection-info (:peer node) %)))
                                      (overlay/connections (:peer node)))]
                    (and (= 1 (count peers))
                         (transfer/replicate! sender (first peers) authority offer)))))})
        object-id (identity/bytes->hex (:object-id split))
        sign #(await (replication-wire/sign-statement signer %))]
    (reset! controller-ref ctx)
    (try
      (require! (true? (await (controller/ready ctx))) "Controller did not restore")
      (let [deadline (+ (System/currentTimeMillis) 20000)
            required (set (map #(identity/bytes->hex (get % "authority"))
                               (get-in @state [:current :statement "members"])))]
        (loop []
          (let [connected (set (map #(:authority-id (overlay/connection-info (:peer node) %))
                                    (overlay/connections (:peer node))))]
            (when-not (every? connected required)
              (when-not (< (System/currentTimeMillis) deadline)
                (throw (ex-info "Selected replica connection timeout"
                                {:required required :connected connected})))
              (Thread/sleep 25)
              (recur)))))
      (if recover?
        (do
          (require! (= :under-replicated (:status (controller/durability-status ctx object-id)))
                    "Old receipts confirmed the successor")
          (require! (empty? (:acknowledged (controller/durability-status ctx object-id)))
                    "Old receipts entered successor accounting")
          (milestone! :recovered-under-replicated)
          (require! (= :ok (await (controller/repair! ctx object-id))) "Repair rejected"))
        (do
          (doseq [{:keys [index id data]} (:pieces split)]
            (require! (= :ok (await (storage/-stage-piece!
                                     store (identity/seq->bytes (repeat 16 7))
                                     (:manifest-id split) (:manifest split) index id data)))
                      "Source staging failed"))
          (await (storage/-install-object! store (identity/seq->bytes (repeat 16 7))
                                           (:manifest-id split) (:manifest split) {}
                                           "netz/store/fsync/1"))
          (require! (= :ok (await (controller/begin! ctx (sign initial) (sign retention)
                                                     (:manifest split)))) "Begin rejected")))
      (let [deadline (+ (System/currentTimeMillis) 20000)]
        (loop []
          (when-not (:complete? (controller/durability-status ctx object-id))
            (require! (empty? @failures) (str "Transfer failures: " @failures))
            (require! (< (System/currentTimeMillis) deadline) "Replication completion timeout")
            (Thread/sleep 25)
            (recur))))
      (if recover?
        (milestone! :successor-complete)
        (do
          (milestone! :initial-complete)
          (reset! dispatch? false)
          (reset! state (accepted-state data true))
          (require! (= :ok (await (controller/revise!
                                   ctx (sign successor)
                                   (sign (assoc retention "revision" 2 "layout_epoch" 1)))))
                    "Successor persistence failed")
          (milestone! :successor-persisted)
          ;; The parent forcibly terminates this owned process at this boundary.
          (read-line)))
      (finally
        (controller/stop! ctx)
        (transfer/stop! sender)
        (carrier/stop-node! node)))))

(defn -main [config-path mode]
  (try
    (run! (edn/read-string (slurp config-path)) (= mode "recover"))
    (System/exit 0)
    (catch Throwable cause
      (.printStackTrace cause)
      (System/exit 1))))
