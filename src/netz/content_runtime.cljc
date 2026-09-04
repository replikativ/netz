(ns netz.content-runtime
  "Authorization-first serving of portable content over an overlay carrier.

  This adapter deliberately owns no storage. Callers inject block and provider
  lookups which may answer immediately or through core.async channels. The
  overlay supplies authenticated connection facts, routing and backpressure."
  (:require [clojure.core.async :as async :refer [chan close! timeout]]
            [kabel.authorize :as authz]
            [netz.content-wire :as content-wire]
            [netz.identity :as identity]
            [netz.overlay.runtime :as overlay-runtime]))

(def default-opts
  {:queue-size 64
   :authorization-timeout-ms 5000
   :backend-timeout-ms 30000
   :provider-ttl-ms content-wire/max-provider-ttl-ms})

(defn- result-channel [value]
  (if (authz/channel? value)
    (async/map (fn [result] {:value result}) [value])
    (doto (async/promise-chan) (async/put! {:value value}))))

(defn- valid-wire-frame? [payload]
  (boolean
   (try
     (contains? #{:content/find :content/want}
                (:type (content-wire/frame-from-wire payload)))
     (catch #?(:clj Throwable :cljs :default) _ false))))

(defn- wire-type? [types payload]
  (and (map? payload) (contains? types (get payload "type"))))

(defn- authorization-context [peer facts frame]
  {:op (case (:type frame)
         :content/find :content/find
         :content/want :content/read)
   :peer-id peer
   :authority-id (:authority-id facts)
   :connection facts
   :cid (:cid frame)
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
  "Serve portable find/want requests on a running overlay.

  Required options:
  - `:authorize?` receives authenticated connection facts and defaults nowhere;
  - `:get-block` receives a CID and returns bytes/nil or a channel thereof.

  `:find-providers` is optional and defaults to no hints. Authorization always
  completes before either lookup runs. Returns an idempotent stop function."
  [running {:keys [authorize? get-block find-providers] :as opts}]
  (when-not (and (ifn? authorize?) (ifn? get-block)
                 (or (nil? find-providers) (ifn? find-providers)))
    (throw (ex-info "portable content service requires authorization and storage hooks"
                    {:type :netz.content-runtime/invalid-options})))
  (let [{:keys [queue-size authorization-timeout-ms backend-timeout-ms
                provider-ttl-ms]}
        (merge default-opts opts)
        _ (when-not (and (integer? queue-size) (pos? queue-size)
                         (integer? authorization-timeout-ms)
                         (pos? authorization-timeout-ms)
                         (integer? backend-timeout-ms) (pos? backend-timeout-ms)
                         (integer? provider-ttl-ms)
                         (<= 0 provider-ttl-ms
                             content-wire/max-provider-ttl-ms))
            (throw (ex-info "invalid portable content service bounds"
                            {:type :netz.content-runtime/invalid-options})))
        requests (chan queue-size)
        stopped? (atom false)
        find-providers (or find-providers (constantly []))
        cleanup (overlay-runtime/register-extension!
                 running ::service
                 {:capability content-wire/capability
                  :match? #(wire-type? #{"content/find" "content/want"} %)
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
              (let [lookup (if (= :content/find (:type frame))
                             find-providers get-block)
                    result (result-channel (lookup (:cid frame)))
                    deadline (timeout backend-timeout-ms)
                    [wrapped port] (async/alts! [result deadline] :priority true)]
                (if-not (and (= port result)
                             (not (and (map? wrapped)
                                       (instance?
                                        #?(:clj Throwable :cljs js/Error)
                                        (:value wrapped)))))
                  (overlay-runtime/disconnect-peer! running peer)
                  (let [response (response-frame frame
                                                 (when (map? wrapped)
                                                   (:value wrapped))
                                                 provider-ttl-ms)]
                    ;; `frame->wire` verifies block bytes before the send.
                    (if-not (overlay-runtime/send-extension!
                             running peer content-wire/capability
                             (content-wire/frame->wire response))
                      (overlay-runtime/disconnect-peer! running peer)))))))
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

  Returns `{:fetch! f :find! f :stop! f :pending atom}`. Each operation takes
  a routing peer ID and CID and immediately returns a one-value channel. The
  result status is `:ok`, `:found`, `:not-found`, `:timeout`, `:disconnected`,
  `:unavailable`, or `:stopped`."
  [running & [{:keys [max-outstanding timeout-ms]
               :or {max-outstanding 16 timeout-ms 5000}}]]
  (when-not (and (integer? max-outstanding) (<= 1 max-outstanding 1024)
                 (integer? timeout-ms) (<= 1 timeout-ms 300000))
    (throw (ex-info "invalid portable content requester bounds"
                    {:type :netz.content-runtime/invalid-options})))
  (let [pending (atom {})
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
                         {:status :found :peer peer :cid (:cid frame)
                          :providers (:providers frame) :ttl-ms (:ttl-ms frame)}
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
              @stopped?
              (do (finish! {:result result}
                           {:status :stopped :peer peer :cid cid}) result)

              (not (content-wire/valid-cid? cid))
              (do (finish! {:result result}
                           {:status :unavailable :peer peer :cid cid
                            :reason :invalid-cid}) result)

              (>= (count @pending) max-outstanding)
              (do (finish! {:result result}
                           {:status :unavailable :peer peer :cid cid
                            :reason :overloaded}) result)

              :else
              (let [request (identity/random-bytes content-wire/request-size)
                    request-key (identity/bytes->hex request)
                    frame {:type operation :request request :cid cid}
                    entry {:peer peer :cid cid :operation operation
                           :result result}]
                (swap! pending assoc request-key entry)
                (if (overlay-runtime/send-extension!
                     running peer content-wire/capability
                     (content-wire/frame->wire frame))
                  (async/go
                    (async/<! (timeout timeout-ms))
                    (when-let [expired (remove-entry! request-key)]
                      (finish! expired {:status :timeout :peer peer :cid cid})))
                  (when-let [unavailable (remove-entry! request-key)]
                    (finish! unavailable
                             {:status :unavailable :peer peer :cid cid})))
                result))))
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
     :stop! stop!
     :pending pending}))
