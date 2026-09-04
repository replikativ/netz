(ns netz.content-runtime
  "Authorization-first serving of portable content over an overlay carrier.

  This adapter deliberately owns no storage. Callers inject block and provider
  lookups which may answer immediately or through core.async channels. The
  overlay supplies authenticated connection facts, routing and backpressure."
  (:require [clojure.core.async :as async :refer [chan close! timeout]]
            [kabel.authorize :as authz]
            [netz.content-provider :as content-provider]
            [netz.content-wire :as content-wire]
            [netz.identity :as identity]
            [netz.overlay.runtime :as overlay-runtime]))

(def default-opts
  {:queue-size 64
   :authorization-timeout-ms 5000
   :backend-timeout-ms 30000
   :provider-ttl-ms content-wire/max-provider-ttl-ms
   :announcement-ttl-ms content-wire/max-provider-ttl-ms})

(def ^:private max-request-id-attempts 8)

(defn- result-channel [value]
  (if (authz/channel? value)
    (async/map (fn [result] {:value result}) [value])
    (doto (async/promise-chan) (async/put! {:value value}))))

(defn- valid-wire-frame? [payload]
  (boolean
   (try
     (contains? #{:content/announce :content/find :content/want}
                (:type (content-wire/frame-from-wire payload)))
     (catch #?(:clj Throwable :cljs :default) _ false))))

(defn- wire-type? [types payload]
  (and (map? payload) (contains? types (get payload "type"))))

(defn- authorization-context [peer facts frame]
  {:op (case (:type frame)
         :content/announce :content/announce
         :content/find :content/find
         :content/want :content/read)
   :peer-id peer
   :authority-id (:authority-id facts)
   :connection facts
   :cid (:cid frame)
   :cids (:cids frame)
   :frame-type (:type frame)
   :frame frame})

(defn- response-frame [frame value provider-ttl-ms]
  (case (:type frame)
    :content/find
    {:type :content/found
     :request (:request frame)
     :cid (:cid frame)
     :providers (or value [])
     :ttl-ms provider-ttl-ms}

    :content/want
    (if (nil? value)
      {:type :content/dont-have :request (:request frame) :cid (:cid frame)}
      {:type :content/block :request (:request frame) :cid (:cid frame)
       :data value})))

(defn start-service!
  "Serve portable announce/find/want requests on a running overlay.

  Required options:
  - `:authorize?` receives authenticated connection facts and defaults nowhere;
  - `:get-block` receives a CID and returns bytes/nil or a channel thereof.

  `:find-providers` and `:accept-announcement!` are optional. Supplying the
  same `:provider-cache` atom to service and requester enables bounded two-hop
  routing automatically. Authorization always completes before callbacks or
  cache mutation. Returns an idempotent stop function."
  [running {:keys [authorize? get-block find-providers accept-announcement!
                   provider-cache now-fn]
            :as opts}]
  (when-not (and (ifn? authorize?) (ifn? get-block)
                 (or (nil? find-providers) (ifn? find-providers))
                 (or (nil? accept-announcement!)
                     (ifn? accept-announcement!)))
    (throw (ex-info "portable content service requires authorization and storage hooks"
                    {:type :netz.content-runtime/invalid-options})))
  (let [{:keys [queue-size authorization-timeout-ms backend-timeout-ms
                provider-ttl-ms announcement-ttl-ms]}
        (merge default-opts opts)
        _ (when-not (and (integer? queue-size) (pos? queue-size)
                         (integer? authorization-timeout-ms)
                         (pos? authorization-timeout-ms)
                         (integer? backend-timeout-ms) (pos? backend-timeout-ms)
                         (integer? provider-ttl-ms)
                         (<= 0 provider-ttl-ms
                             content-wire/max-provider-ttl-ms)
                         (integer? announcement-ttl-ms)
                         (<= 0 announcement-ttl-ms
                             content-wire/max-provider-ttl-ms)
                         (or (nil? now-fn) (ifn? now-fn)))
            (throw (ex-info "invalid portable content service bounds"
                            {:type :netz.content-runtime/invalid-options})))
        requests (chan queue-size)
        stopped? (atom false)
        now-fn (or now-fn #?(:clj #(System/currentTimeMillis)
                             :cljs #(.now js/Date)))
        find-providers
        (or find-providers
            (when provider-cache
              (fn [cid]
                (let [now (now-fn)
                      current (swap! provider-cache content-provider/expire now)]
                  (mapv :authority
                        (content-provider/providers-for current cid now)))))
            (constantly []))
        accept-announcement! (or accept-announcement! (constantly true))
        cleanup (overlay-runtime/register-extension!
                 running ::service
                 {:capability content-wire/capability
                  :match? #(wire-type? #{"content/announce" "content/find"
                                         "content/want"} %)
                  :valid? valid-wire-frame?
                  :receive!
                  (fn [peer facts payload]
                    (and (not @stopped?)
                         (async/offer! requests
                                       {:peer peer :facts facts
                                        :frame (content-wire/frame-from-wire
                                                payload)})))})
        stop! (fn []
                (when (compare-and-set! stopped? false true)
                  (cleanup)
                  (close! requests)
                  true))]
    (async/go-loop []
      (when-let [{:keys [peer facts frame]} (async/<! requests)]
        (try
          (let [decision (authz/decision
                          (authorize? (authorization-context peer facts frame)))
                deadline (timeout authorization-timeout-ms)
                [allowed port] (async/alts! [decision deadline] :priority true)]
            (if-not (and (= port decision) (true? allowed))
              (overlay-runtime/disconnect-peer! running peer)
              (let [announcement? (= :content/announce (:type frame))
                    lookup (cond
                             announcement? accept-announcement!
                             (= :content/find (:type frame)) find-providers
                             :else get-block)
                    argument (if announcement?
                               (authorization-context peer facts frame)
                               (:cid frame))
                    result (result-channel (lookup argument))
                    deadline (timeout backend-timeout-ms)
                    [wrapped port] (async/alts! [result deadline] :priority true)
                    completed? (and (= port result)
                                    (not (and (map? wrapped)
                                              (instance?
                                               #?(:clj Throwable :cljs js/Error)
                                               (:value wrapped)))))]
                (if-not completed?
                  (overlay-runtime/disconnect-peer! running peer)
                  (if announcement?
                    (when (and provider-cache (true? (:value wrapped)))
                      (let [authority (identity/hex->bytes
                                       (:authority-id facts))]
                        (swap! provider-cache
                               content-provider/remember-provider-cids
                               (:cids frame) authority announcement-ttl-ms
                               (now-fn))))
                    (let [response (response-frame frame
                                                   (when (map? wrapped)
                                                     (:value wrapped))
                                                   provider-ttl-ms)]
                      ;; `frame->wire` verifies block bytes before the send.
                      (if-not (overlay-runtime/send-extension!
                               running peer content-wire/capability
                               (content-wire/frame->wire response))
                        (overlay-runtime/disconnect-peer! running peer))))))))
          (catch #?(:clj Throwable :cljs :default) _
            (overlay-runtime/disconnect-peer! running peer)))
        (recur)))
    (overlay-runtime/on-stop! running stop!)
    stop!))

(defn- valid-response-wire? [payload]
  (boolean
   (try
     (contains? #{:content/found :content/block :content/dont-have}
                (:type (content-wire/frame-from-wire payload)))
     (catch #?(:clj Throwable :cljs :default) _ false))))

(defn- finish! [entry result]
  (async/offer! (:result entry) result)
  (close! (:result entry)))

(defn start-requester!
  "Install bounded portable find/fetch response handling.

  Direct `:fetch!`/`:find!` operations address one peer. `:discover!` asks a
  bounded, capability-filtered snapshot of connected peers and caches their
  answers; it never dials. `:fetch-any!` tries bounded live provider hints and
  `:discover-and-fetch!` composes both phases. `:announce-connected!` sends an
  authenticated provider hint to a bounded connected-peer snapshot.

  Asynchronous operations immediately return a one-value channel. The result
  status is `:ok`, `:found`, `:not-found`, `:timeout`, `:disconnected`,
  `:unavailable`, or `:stopped`."
  [running & [{:keys [max-outstanding timeout-ms max-provider-attempts
                      provider-cache provider-cache-opts provider-order-fn
                      max-discovery-peers discovery-parallelism
                      discovery-min-providers max-announcement-peers
                      peer-order-fn connected-peers-fn now-fn]
               :or {max-outstanding 16 timeout-ms 5000
                    max-provider-attempts 4 provider-cache-opts {}
                    provider-order-fn shuffle max-discovery-peers 8
                    discovery-parallelism 2 discovery-min-providers 1
                    max-announcement-peers 8 peer-order-fn shuffle}}]]
  (when-not (and (integer? max-outstanding) (<= 1 max-outstanding 1024)
                 (integer? timeout-ms) (<= 1 timeout-ms 300000)
                 (integer? max-provider-attempts)
                 (<= 1 max-provider-attempts content-wire/max-providers)
                 (ifn? provider-order-fn)
                 (integer? max-discovery-peers)
                 (<= 1 max-discovery-peers 64)
                 (integer? discovery-parallelism)
                 (<= 1 discovery-parallelism max-discovery-peers)
                 (integer? discovery-min-providers)
                 (<= 1 discovery-min-providers content-wire/max-providers)
                 (integer? max-announcement-peers)
                 (<= 1 max-announcement-peers 64)
                 (ifn? peer-order-fn)
                 (or (nil? connected-peers-fn) (ifn? connected-peers-fn))
                 (or (nil? now-fn) (ifn? now-fn)))
    (throw (ex-info "invalid portable content requester bounds"
                    {:type :netz.content-runtime/invalid-options})))
  (let [now-fn (or now-fn #?(:clj #(System/currentTimeMillis)
                             :cljs #(.now js/Date)))
        provider-cache (or provider-cache
                           (atom (content-provider/make-cache
                                  provider-cache-opts)))
        connected-peers-fn
        (or connected-peers-fn
            #(overlay-runtime/connections (:peer running)))
        pending (atom {})
        stopped? (atom false)
        remove-entry!
        (fn [request-key]
          (let [removed (atom nil)]
            (swap! pending
                   (fn [entries]
                     (if-let [entry (get entries request-key)]
                       (do (reset! removed entry) (dissoc entries request-key))
                       entries)))
            @removed))
        fail-peer!
        (fn [peer status]
          (doseq [[request-key entry] @pending
                  :when (= peer (:peer entry))]
            (when-let [removed (remove-entry! request-key)]
              (finish! removed {:status status :peer peer :cid (:cid removed)}))))
        receive!
        (fn [peer _facts payload]
          (let [frame (content-wire/frame-from-wire payload)
                request-key (identity/bytes->hex (:request frame))]
            (if-let [entry (get @pending request-key)]
              (let [expected (if (= :content/want (:operation entry))
                               #{:content/block :content/dont-have}
                               #{:content/found})]
                (if-not (and (= peer (:peer entry))
                             (identity/bufs= (:cid frame) (:cid entry))
                             (contains? expected (:type frame)))
                  false
                  (if-let [removed (remove-entry! request-key)]
                    (do
                      (finish!
                       removed
                       (case (:type frame)
                         :content/block
                         {:status :ok :peer peer :cid (:cid frame)
                          :data (:data frame)}
                         :content/found
                         (do
                           (swap! provider-cache
                                  content-provider/remember-many
                                  (:cid frame) (:providers frame)
                                  (:ttl-ms frame) (now-fn))
                           {:status :found :peer peer :cid (:cid frame)
                            :providers (:providers frame)
                            :ttl-ms (:ttl-ms frame)})
                         :content/dont-have
                         {:status :not-found :peer peer :cid (:cid frame)}))
                      true)
                    ;; Timeout/disconnect won the removal race.
                    true)))
              ;; Duplicate and retired-connection responses are harmless.
              true)))
        cleanup (overlay-runtime/register-extension!
                 running ::requester
                 {:capability content-wire/capability
                  :match? #(wire-type? #{"content/found" "content/block"
                                         "content/dont-have"} %)
                  :valid? valid-response-wire?
                  :receive! receive!
                  :disconnected! #(fail-peer! % :disconnected)})
        open!
        (fn [operation peer cid]
          (let [result (chan 1)]
            (cond
              (not (content-wire/valid-cid? cid))
              (do (finish! {:result result}
                           {:status :unavailable :peer peer :cid cid
                            :reason :invalid-cid}) result)

              :else
              (do
                (loop [request-id-attempt 0]
                  (let [entries @pending]
                    (cond
                      @stopped?
                      (finish! {:result result}
                               {:status :stopped :peer peer :cid cid})

                      (>= (count entries) max-outstanding)
                      (finish! {:result result}
                               {:status :unavailable :peer peer :cid cid
                                :reason :overloaded})

                      :else
                      (let [request (identity/random-bytes
                                     content-wire/request-size)
                            request-key (identity/bytes->hex request)
                            frame {:type operation :request request :cid cid}
                            entry {:peer peer :cid cid :operation operation
                                   :result result}]
                        (if (contains? entries request-key)
                          (if (= (inc request-id-attempt)
                                 max-request-id-attempts)
                            (finish! {:result result}
                                     {:status :unavailable :peer peer :cid cid
                                      :reason :request-id-exhausted})
                            (recur (inc request-id-attempt)))
                          (if-not (compare-and-set!
                                   pending entries
                                   (assoc entries request-key entry))
                            (recur request-id-attempt)
                            (if (overlay-runtime/send-extension!
                                 running peer content-wire/capability
                                 (content-wire/frame->wire frame))
                              (async/go
                                (async/<! (timeout timeout-ms))
                                (when-let [expired (remove-entry! request-key)]
                                  (finish! expired
                                           {:status :timeout :peer peer
                                            :cid cid})))
                              (when-let [unavailable
                                         (remove-entry! request-key)]
                                (finish! unavailable
                                         {:status :unavailable :peer peer
                                          :cid cid})))))))))
                result))))
        providers-for!
        (fn [cid]
          (let [now (now-fn)
                current (swap! provider-cache content-provider/expire now)]
            (content-provider/providers-for current cid now)))
        eligible-peers!
        (fn [limit]
          (->> (connected-peers-fn)
               (filter #(overlay-runtime/portable-extension-connection?
                         (:peer running) % content-wire/capability))
               peer-order-fn
               (take limit)
               vec))
        announce!
        (fn [peer cids]
          (and (not @stopped?)
               (overlay-runtime/send-extension!
                running peer content-wire/capability
                (content-wire/frame->wire
                 {:type :content/announce :cids (vec cids)}))))
        announce-connected!
        (fn [cids]
          ;; Canonicalize and validate before sending the first copy.
          (let [wire-frame (content-wire/frame->wire
                            {:type :content/announce :cids (vec cids)})
                peers (if @stopped?
                        []
                        (eligible-peers! max-announcement-peers))
                announced (reduce (fn [sent peer]
                                    (if (overlay-runtime/send-extension!
                                         running peer content-wire/capability
                                         wire-frame)
                                      (conj sent peer)
                                      sent))
                                  [] peers)]
            {:status (if @stopped? :stopped :ok)
             :attempted peers
             :announced announced}))
        discover!
        (fn [cid]
          (let [result (chan 1)]
            (cond
              @stopped?
              (finish! {:result result} {:status :stopped :cid cid})

              (not (content-wire/valid-cid? cid))
              (finish! {:result result}
                       {:status :unavailable :cid cid :reason :invalid-cid})

              :else
              (let [peers (eligible-peers! max-discovery-peers)]
                (async/go-loop [remaining (seq peers)
                                active {}
                                attempts []
                                queried []]
                  (let [providers (providers-for! cid)]
                    (cond
                      (>= (count providers) discovery-min-providers)
                      (finish! {:result result}
                               {:status :found :cid cid :providers providers
                                :queried queried :attempts attempts})

                      (and (empty? remaining) (empty? active))
                      (finish! {:result result}
                               {:status (if (seq providers)
                                          :found :not-found)
                                :cid cid :providers providers
                                :reason (when (empty? providers)
                                          :providers-not-found)
                                :queried queried :attempts attempts})

                      :else
                      (let [[remaining active queried]
                            (loop [remaining remaining
                                   active active
                                   queried queried]
                              (if (and (seq remaining)
                                       (< (count active)
                                          discovery-parallelism))
                                (let [peer (first remaining)
                                      response (open! :content/find peer cid)]
                                  (recur (next remaining)
                                         (assoc active response peer)
                                         (conj queried peer)))
                                [remaining active queried]))]
                        (if (empty? active)
                          (recur remaining active attempts queried)
                          (let [[attempt response]
                                (async/alts! (vec (keys active)))
                                peer (get active response)]
                            (recur remaining
                                   (dissoc active response)
                                   (conj attempts
                                         (assoc
                                          (select-keys attempt
                                                       [:status :reason])
                                          :peer peer))
                                   queried)))))))))
            result))
        fetch-any!
        (fn [cid]
          (let [result (chan 1)]
            (cond
              @stopped?
              (finish! {:result result} {:status :stopped :cid cid})

              (not (content-wire/valid-cid? cid))
              (finish! {:result result}
                       {:status :unavailable :cid cid :reason :invalid-cid})

              :else
              (let [providers (vec (take max-provider-attempts
                                         (provider-order-fn
                                          (providers-for! cid))))]
                (if (empty? providers)
                  (finish! {:result result}
                           {:status :unavailable :cid cid
                            :reason :no-providers :attempts []})
                  (async/go-loop [remaining providers attempts []]
                    (if-let [{:keys [peer authority]} (first remaining)]
                      (let [attempt (async/<! (open! :content/want peer cid))
                            attempts (conj attempts
                                           (select-keys attempt
                                                        [:status :peer :reason]))]
                        (cond
                          (= :ok (:status attempt))
                          (finish! {:result result}
                                   (assoc attempt :provider-authority authority
                                          :attempts attempts))

                          (= :stopped (:status attempt))
                          (finish! {:result result}
                                   {:status :stopped :cid cid
                                    :attempts attempts})

                          :else (recur (next remaining) attempts)))
                      (finish! {:result result}
                               {:status :unavailable :cid cid
                                :reason :providers-exhausted
                                :attempts attempts}))))))
            result))
        discover-and-fetch!
        (fn [cid]
          (let [result (chan 1)]
            (async/go
              (let [discovery (async/<! (discover! cid))]
                (if (= :found (:status discovery))
                  (let [fetched (async/<! (fetch-any! cid))]
                    (finish! {:result result}
                             (assoc fetched :discovery discovery)))
                  (finish! {:result result}
                           {:status (if (= :stopped (:status discovery))
                                      :stopped :unavailable)
                            :cid cid :reason :discovery-failed
                            :discovery discovery}))))
            result))
        stop! (fn []
                (when (compare-and-set! stopped? false true)
                  (cleanup)
                  (doseq [[request-key _] @pending]
                    (when-let [entry (remove-entry! request-key)]
                      (finish! entry {:status :stopped :peer (:peer entry)
                                      :cid (:cid entry)})))
                  true))]
    (overlay-runtime/on-stop! running stop!)
    {:fetch! #(open! :content/want %1 %2)
     :find! #(open! :content/find %1 %2)
     :announce! announce!
     :announce-connected! announce-connected!
     :discover! discover!
     :discover-and-fetch! discover-and-fetch!
     :fetch-any! fetch-any!
     :providers-for providers-for!
     :stop! stop!
     :pending pending
     :provider-cache provider-cache}))
