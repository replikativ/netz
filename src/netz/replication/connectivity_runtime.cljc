(ns netz.replication.connectivity-runtime
  "Bounded refresh of aggregate local replication connection demand.

  One coordinator owns demand for an overlay. It queues no offers, evicts no
  connections and never interprets demand admission as replication success."
  (:require [netz.identity :as identity]
            [netz.overlay.runtime :as overlay]
            [netz.replication.controller-runtime :as controller]
            #?(:clj [clojure.core.async :as async :refer [go-loop <!]]
               :cljs [clojure.core.async :as async :refer [<!]
                      :refer-macros [go-loop]])))

(defn start!
  "Refresh current missing targets from ready controllers every second.

  Supply exactly one coordinator per overlay; controllers are a fixed vector
  (at most 32). Overflow rejects the whole snapshot instead of silently
  substituting replicas. Demand expires after five seconds without refresh.
  Await each controller's readiness before starting. Stop this coordinator
  before replacing it. Existing connections remain ordinary overlay peers."
  [{:keys [overlay controllers now-fn]}]
  (when-not (and overlay (vector? controllers) (<= 1 (count controllers) 32))
    (throw (ex-info "Invalid connectivity coordinator options" {:type ::invalid-options})))
  (let [stop (async/chan)
        done (async/promise-chan)
        stats (atom {:submitted 0 :refused 0 :errors 0})
        now-fn (or now-fn #?(:clj #(System/currentTimeMillis) :cljs #(.now js/Date)))
        publish! (fn [targets]
                   (let [demand (into {} (map (fn [authority]
                                                [(identity/authority-routing-id authority) authority]) targets))
                         limit (overlay/max-connection-demand overlay)]
                     (and (<= (count demand) limit)
                          (= (count demand) (count targets))
                          (overlay/submit! overlay {:type :connection-demand
                                                    :demand demand :until (+ (now-fn) 5000)}))))]
    (go-loop []
      (try
        (let [targets (reduce into #{} (map controller/connection-targets controllers))]
          (swap! stats update (if (publish! targets) :submitted :refused) inc))
        (catch #?(:clj Throwable :cljs :default) _ (swap! stats update :errors inc)))
      (let [[_ port] (async/alts! [stop (async/timeout 1000)] :priority true)]
        (if (= port stop)
          (do
            ;; FIFO after this coordinator's final refresh. If the bounded
            ;; queue refuses release, the last snapshot still expires.
            (try (publish! #{}) (catch #?(:clj Throwable :cljs :default) _ nil))
            (async/>! done true)
            (async/close! done))
          (recur))))
    {:stop stop :done done :stats stats}))

(defn stop!
  "Stop refresh, best-effort release demand, and return the completion channel."
  [ctx]
  (async/close! (:stop ctx))
  (:done ctx))

(defn stats [ctx] @(:stats ctx))
