(ns netz.content-runtime-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [netz.content-provider :as content-provider]
            [netz.content-runtime :as content-runtime]
            [netz.content-wire :as content-wire]
            [netz.identity :as identity]
            [netz.overlay.runtime :as overlay-runtime]))

(def peer-id #uuid "01010101-0101-0101-0101-010101010101")
(def authority (identity/seq->bytes (repeat 32 1)))
(def request (identity/seq->bytes (range 16)))
(def data (identity/utf8-bytes "runtime block"))
(def cid (content-wire/raw-cid data))

(defn- fake-running []
  {:ctx {:extensions (atom {}) :stopped? (atom false) :stop-fns (atom [])}})

(defn- service-handler [running]
  (first (vals @(:extensions (:ctx running)))))

(defn- offer! [handler frame]
  ((:receive! handler) peer-id
                       {:authority-id (identity/bytes->hex authority)
                        :capabilities #{content-wire/capability}}
                       (content-wire/frame->wire frame)))

(deftest portable-content-service-authorizes-before-storage
  (let [running (fake-running)
        events (atom [])
        sent (async/promise-chan)]
    (with-redefs [overlay-runtime/send-extension!
                  (fn [_ to capability payload]
                    (async/put! sent [to capability payload]) true)
                  overlay-runtime/disconnect-peer! (fn [& _] false)]
      (let [stop! (content-runtime/start-service!
                   running
                   {:authorize? (fn [context]
                                  (swap! events conj [:authorize (:op context)])
                                  true)
                    :get-block (fn [requested]
                                 (swap! events conj [:read requested])
                                 data)})
            handler (service-handler running)]
        (is (offer! handler {:type :content/want :request request :cid cid}))
        (let [[to capability payload] (async/<!! sent)
              response (content-wire/frame-from-wire payload)]
          (is (= peer-id to))
          (is (= content-wire/capability capability))
          (is (= [:authorize :read] (mapv first @events)))
          (is (= :content/block (:type response)))
          (is (identity/bufs= data (:data response))))
        (is (true? (stop!)))
        (is (nil? (stop!)))))))

(deftest denial-never-consults-content-backends
  (let [running (fake-running)
        reads (atom 0)
        disconnected (async/promise-chan)]
    (with-redefs [overlay-runtime/send-extension! (fn [& _] true)
                  overlay-runtime/disconnect-peer!
                  (fn [_ peer] (async/put! disconnected peer) false)]
      (let [stop! (content-runtime/start-service!
                   running
                   {:authorize? (constantly false)
                    :get-block (fn [_] (swap! reads inc) data)
                    :find-providers (fn [_] (swap! reads inc) [authority])})
            handler (service-handler running)]
        (is (offer! handler {:type :content/want :request request :cid cid}))
        (is (= peer-id (async/<!! disconnected)))
        (is (zero? @reads))
        (stop!)))))

(deftest denied-announcements-never-reach-admission-or-cache
  (let [running (fake-running)
        admissions (atom 0)
        cache (atom (content-provider/make-cache))
        disconnected (async/promise-chan)]
    (with-redefs [overlay-runtime/disconnect-peer!
                  (fn [_ peer] (async/put! disconnected peer) false)]
      (let [stop! (content-runtime/start-service!
                   running
                   {:authorize? (constantly false)
                    :get-block (constantly nil)
                    :accept-announcement!
                    (fn [_] (swap! admissions inc) true)
                    :provider-cache cache
                    :now-fn (constantly 100)})
            handler (service-handler running)]
        (is (offer! handler {:type :content/announce :cids [cid]}))
        (is (= peer-id (async/<!! disconnected)))
        (is (zero? @admissions))
        (is (empty? (content-provider/providers-for @cache cid 100)))
        (stop!)))))

(deftest backend-results-must-pass-wire-integrity-checks
  (testing "a corrupt provider result closes the requesting connection"
    (let [running (fake-running)
          disconnected (async/promise-chan)]
      (with-redefs [overlay-runtime/send-extension! (fn [& _] true)
                    overlay-runtime/disconnect-peer!
                    (fn [_ peer] (async/put! disconnected peer) false)]
        (let [stop! (content-runtime/start-service!
                     running
                     {:authorize? (constantly true)
                      :get-block (constantly (identity/utf8-bytes "wrong"))})
              handler (service-handler running)]
          (is (offer! handler {:type :content/want
                               :request request :cid cid}))
          (is (= peer-id (async/<!! disconnected)))
          (stop!))))))

(deftest provider-discovery-supports-asynchronous-hooks
  (let [running (fake-running)
        sent (async/promise-chan)
        decision (doto (async/promise-chan) (async/put! true))
        providers (doto (async/promise-chan) (async/put! [authority]))]
    (with-redefs [overlay-runtime/send-extension!
                  (fn [_ _ _ payload] (async/put! sent payload) true)
                  overlay-runtime/disconnect-peer! (fn [& _] false)]
      (let [stop! (content-runtime/start-service!
                   running {:authorize? (constantly decision)
                            :get-block (constantly nil)
                            :find-providers (constantly providers)
                            :provider-ttl-ms 10000})
            handler (service-handler running)]
        (is (offer! handler {:type :content/find :request request :cid cid}))
        (let [response (content-wire/frame-from-wire (async/<!! sent))]
          (is (= :content/found (:type response)))
          (is (= 10000 (:ttl-ms response)))
          (is (identity/bufs= authority (first (:providers response)))))
        (stop!)))))

(deftest authorized-announcements-feed-bounded-two-hop-finds
  (let [running (fake-running)
        cache (atom (content-provider/make-cache))
        events (atom [])
        called (async/promise-chan)
        accepted (async/promise-chan)
        sent (async/promise-chan)]
    (with-redefs [overlay-runtime/send-extension!
                  (fn [_ _ _ payload] (async/put! sent payload) true)
                  overlay-runtime/disconnect-peer! (fn [& _] false)]
      (let [stop! (content-runtime/start-service!
                   running
                   {:authorize? (fn [context]
                                  (swap! events conj [:authorize (:op context)])
                                  true)
                    :get-block (constantly nil)
                    :accept-announcement!
                    (fn [context]
                      (swap! events conj [:accept (:cids context)])
                      (async/put! called true)
                      accepted)
                    :provider-cache cache
                    :announcement-ttl-ms 50
                    :provider-ttl-ms 40
                    :now-fn (constantly 100)})
            handler (service-handler running)]
        (is (offer! handler {:type :content/announce :cids [cid]}))
        (is (true? (async/<!! called)))
        (is (= [[:authorize :content/announce] [:accept [cid]]]
               @events))
        (async/>!! accepted true)
        ;; FIFO service processing ensures the accepted announcement is cached
        ;; before the following lookup is served.
        (is (offer! handler {:type :content/find :request request :cid cid}))
        (let [response (content-wire/frame-from-wire (async/<!! sent))]
          (is (= :content/found (:type response)))
          (is (= 40 (:ttl-ms response)))
          (is (identity/bufs= authority (first (:providers response)))))
        (stop!)))))

(deftest portable-requester-correlates-and-completes-fetches
  (let [running (fake-running)
        outbound (atom nil)]
    (with-redefs [overlay-runtime/send-extension!
                  (fn [_ to capability payload]
                    (reset! outbound [to capability payload]) true)]
      (let [{:keys [fetch! stop! pending]}
            (content-runtime/start-requester! running)
            result (fetch! peer-id cid)
            handler (service-handler running)
            [_ capability request-wire] @outbound
            request-frame (content-wire/frame-from-wire request-wire)
            response {:type :content/block
                      :request (:request request-frame) :cid cid :data data}]
        (is (= content-wire/capability capability))
        (is (= :content/want (:type request-frame)))
        (is ((:receive! handler) peer-id {}
                                 (content-wire/frame->wire response)))
        (let [completed (async/<!! result)]
          (is (= :ok (:status completed)))
          (is (identity/bufs= data (:data completed))))
        (is ((:receive! handler) peer-id {}
                                 (content-wire/frame->wire response))
            "a duplicate response is ignored without closing the connection")
        (is (empty? @pending))
        (stop!)))))

(deftest portable-requester-bounds-timeout-and-disconnect
  (testing "one in-flight slot is explicit and expires"
    (let [running (fake-running)]
      (with-redefs [overlay-runtime/send-extension! (fn [& _] true)]
        (let [{:keys [fetch! stop!]}
              (content-runtime/start-requester!
               running {:max-outstanding 1 :timeout-ms 5})
              first-result (fetch! peer-id cid)
              refused (fetch! peer-id cid)]
          (is (= :unavailable (:status (async/<!! refused))))
          (is (= :timeout (:status (async/<!! first-result))))
          (stop!)))))

  (testing "disconnect resolves every request for that peer"
    (let [running (fake-running)]
      (with-redefs [overlay-runtime/send-extension! (fn [& _] true)]
        (let [{:keys [fetch! stop!]}
              (content-runtime/start-requester! running)
              result (fetch! peer-id cid)
              handler (service-handler running)]
          ((:disconnected! handler) peer-id)
          (is (= :disconnected (:status (async/<!! result))))
          (stop!))))))

(deftest concurrent-requesters-cannot-race-past-the-outstanding-bound
  (let [running (fake-running)]
    (with-redefs [overlay-runtime/send-extension! (fn [& _] true)]
      (let [{:keys [fetch! stop! pending]}
            (content-runtime/start-requester!
             running {:max-outstanding 8 :timeout-ms 30000})
            results (->> (range 100)
                         (mapv (fn [_] (future (fetch! peer-id cid))))
                         (mapv deref))
            immediate (keep async/poll! results)]
        (is (= 8 (count @pending)))
        (is (= 92 (count immediate)))
        (is (every? #(= :overloaded (:reason %)) immediate))
        (stop!)))))

(deftest repeated-request-ids-fail-boundedly
  (let [running (fake-running)]
    (with-redefs [identity/random-bytes (fn [_] request)
                  overlay-runtime/send-extension! (fn [& _] true)]
      (let [{:keys [fetch! stop!]}
            (content-runtime/start-requester!
             running {:max-outstanding 2 :timeout-ms 30000})
            first-result (fetch! peer-id cid)
            collision (async/<!! (fetch! peer-id cid))]
        (is (= :request-id-exhausted (:reason collision)))
        (stop!)
        (is (= :stopped (:status (async/<!! first-result))))))))

(deftest portable-requester-learns-expires-and-fails-over-providers
  (let [running (fake-running)
        now (atom 100)
        outbound (async/chan 8)
        authority-a (identity/seq->bytes (repeat 32 2))
        authority-b (identity/seq->bytes (repeat 32 3))
        peer-a (identity/authority-routing-id authority-a)
        peer-b (identity/authority-routing-id authority-b)]
    (with-redefs [overlay-runtime/send-extension!
                  (fn [_ to _ payload]
                    (async/offer! outbound
                                  [to (content-wire/frame-from-wire payload)]))]
      (let [{:keys [find! fetch-any! providers-for stop!]}
            (content-runtime/start-requester!
             running {:now-fn #(deref now) :max-provider-attempts 2
                      :provider-order-fn identity})
            handler (service-handler running)
            found-result (find! peer-id cid)
            [_ find-frame] (async/<!! outbound)]
        (is ((:receive! handler)
             peer-id {}
             (content-wire/frame->wire
              {:type :content/found :request (:request find-frame) :cid cid
               :providers [authority-a authority-b] :ttl-ms 50})))
        (is (= :found (:status (async/<!! found-result))))
        (is (= [peer-a peer-b] (mapv :peer (providers-for cid))))

        (let [result (fetch-any! cid)
              [first-peer first-want] (async/<!! outbound)]
          (is (= peer-a first-peer))
          (is ((:receive! handler)
               first-peer {}
               (content-wire/frame->wire
                {:type :content/dont-have :request (:request first-want)
                 :cid cid})))
          (let [[second-peer second-want] (async/<!! outbound)]
            (is (= peer-b second-peer))
            (is ((:receive! handler)
                 second-peer {}
                 (content-wire/frame->wire
                  {:type :content/block :request (:request second-want)
                   :cid cid :data data}))))
          (let [completed (async/<!! result)]
            (is (= :ok (:status completed)))
            (is (= [:not-found :ok] (mapv :status (:attempts completed))))
            (is (identity/bufs= authority-b (:provider-authority completed)))))

        (reset! now 150)
        (is (empty? (providers-for cid)))
        (is (= :no-providers (:reason (async/<!! (fetch-any! cid)))))
        (stop!)))))

(deftest connected-discovery-is-capability-filtered-bounded-and-fetches
  (let [running (fake-running)
        peer-a #uuid "02020202-0202-0202-0202-020202020202"
        peer-b #uuid "03030303-0303-0303-0303-030303030303"
        peer-c #uuid "04040404-0404-0404-0404-040404040404"
        provider-authority (identity/seq->bytes (repeat 32 9))
        provider-peer (identity/authority-routing-id provider-authority)
        outbound (async/chan 8)]
    (with-redefs [overlay-runtime/portable-extension-connection?
                  (fn [_ peer _] (not= peer peer-c))
                  overlay-runtime/send-extension!
                  (fn [_ to _ payload]
                    (async/offer! outbound
                                  [to (content-wire/frame-from-wire payload)]))]
      (let [{:keys [announce-connected! discover! discover-and-fetch! stop!]}
            (content-runtime/start-requester!
             running {:connected-peers-fn #(vector peer-a peer-b peer-c)
                      :peer-order-fn identity
                      :provider-order-fn identity
                      :discovery-parallelism 1
                      :discovery-min-providers 1
                      :max-announcement-peers 1})
            handler (service-handler running)
            announcement (announce-connected! [cid])
            [announced-peer announce-frame] (async/<!! outbound)]
        (is (= :ok (:status announcement)))
        (is (= [peer-a] (:attempted announcement)))
        (is (= peer-a announced-peer))
        (is (= :content/announce (:type announce-frame)))

        (let [result (discover! cid)
              [first-peer first-find] (async/<!! outbound)]
          (is (= peer-a first-peer))
          (is ((:receive! handler)
               first-peer {}
               (content-wire/frame->wire
                {:type :content/found :request (:request first-find) :cid cid
                 :providers [] :ttl-ms 50})))
          (let [[second-peer second-find] (async/<!! outbound)]
            (is (= peer-b second-peer))
            (is ((:receive! handler)
                 second-peer {}
                 (content-wire/frame->wire
                  {:type :content/found :request (:request second-find) :cid cid
                   :providers [provider-authority] :ttl-ms 50}))))
          (let [discovered (async/<!! result)]
            (is (= :found (:status discovered)))
            (is (= [peer-a peer-b] (:queried discovered)))
            (is (= [provider-peer] (mapv :peer (:providers discovered))))))

        (let [result (discover-and-fetch! cid)
              [to want] (async/<!! outbound)]
          (is (= provider-peer to))
          (is (= :content/want (:type want)))
          (is ((:receive! handler)
               to {}
               (content-wire/frame->wire
                {:type :content/block :request (:request want)
                 :cid cid :data data})))
          (let [fetched (async/<!! result)]
            (is (= :ok (:status fetched)))
            (is (empty? (get-in fetched [:discovery :queried])))))
        (stop!)))))

(deftest discovery-fanout-remains-bounded-with-a-large-peer-view
  (let [running (fake-running)
        peers (mapv #(java.util.UUID. 0 %) (range 1 101))
        outbound (async/chan 16)
        pending-ref (atom nil)
        max-in-flight (atom 0)]
    (with-redefs [overlay-runtime/portable-extension-connection?
                  (fn [& _] true)
                  overlay-runtime/send-extension!
                  (fn [_ to _ payload]
                    (when-let [pending @pending-ref]
                      (swap! max-in-flight max (count @pending)))
                    (async/offer! outbound
                                  [to (content-wire/frame-from-wire payload)]))]
      (let [{:keys [discover! pending stop!]}
            (content-runtime/start-requester!
             running {:connected-peers-fn (constantly peers)
                      :peer-order-fn identity
                      :max-discovery-peers 8
                      :discovery-parallelism 2
                      :timeout-ms 30000})
            _ (reset! pending-ref pending)
            result (discover! cid)
            handler (service-handler running)]
        (dotimes [_ 8]
          (let [[peer find-frame] (async/<!! outbound)]
            (is ((:receive! handler)
                 peer {}
                 (content-wire/frame->wire
                  {:type :content/found :request (:request find-frame)
                   :cid cid :providers [] :ttl-ms 50})))))
        (let [completed (async/<!! result)]
          (is (= :not-found (:status completed)))
          (is (= (subvec peers 0 8) (:queried completed)))
          (is (= 2 @max-in-flight))
          (is (nil? (async/poll! outbound))))
        (stop!)))))
