(ns netz.content-runtime
  "Authorization-first serving of portable content over an overlay carrier.

  This adapter deliberately owns no storage. Callers inject block and provider
  lookups which may answer immediately or through core.async channels. The
  overlay supplies authenticated connection facts, routing and backpressure."
  (:require [clojure.core.async :as async :refer [chan close! timeout]]
            [kabel.authorize :as authz]
            [netz.content-wire :as content-wire]
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
                  :match? #(and (map? %) (string? (get % "type"))
                                (.startsWith (get % "type") "content/"))
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
