(ns netz.datahike-locality-model
  "Small deterministic cost model for Datahike snapshot placement.

  This is deliberately a test-path experiment, not a placement wire profile.
  Durable targets are producer-authorized; read caches are populated by demand
  and never count as durability evidence."
  (:require [netz.replication.placement :as placement]))

(defn combinations [values n]
  (letfn [(step [prefix remaining needed]
            (cond
              (zero? needed) [prefix]
              (< (count remaining) needed) []
              :else
              (into (step (conj prefix (first remaining))
                          (subvec remaining 1) (dec needed))
                    (step prefix (subvec remaining 1) needed))))]
    (step [] (vec values) n)))

(defn domain-counts [members minima]
  (into {}
        (for [domain (keys minima)]
          [domain (count (into #{} (keep #(get-in % [:domains domain])) members))])))

(defn satisfies-domains? [members minima]
  (let [counts (domain-counts members minima)]
    (every? (fn [[domain required]]
              (>= (get counts domain 0) required))
            minima)))

(defn latency [scenario from to]
  (if (= from to)
    0
    (or (get-in scenario [:latency-ms [from to]])
        (get-in scenario [:latency-ms [to from]])
        (throw (ex-info "Missing simulated latency"
                        {:from from :to to})))))

(defn- receipt-ms [scenario member]
  (+ (:persist-ms scenario)
     (* 2 (latency scenario (:producer-site scenario)
                   (get-in member [:domains "site"])))))

(defn- acknowledgement-ms [scenario targets]
  (let [w (:write-acks scenario)
        valid (filter #(satisfies-domains? % (:ack-minimum-domains scenario))
                      (combinations targets w))]
    (when (empty? valid)
      (throw (ex-info "Targets cannot satisfy acknowledgement domains"
                      {:targets (mapv :id targets)})))
    (apply min (map #(apply max (map (partial receipt-ms scenario) %)) valid))))

(defn- weighted-source-rtt-ms [scenario targets]
  (let [[latency-sum demand-sum]
        (reduce-kv
         (fn [[total weight] consumer-site demand]
           [(+ total
               (* demand
                  (apply min
                         (map #(latency scenario consumer-site
                                        (get-in % [:domains "site"]))
                              targets))))
            (+ weight demand)])
         [0 0]
         (:consumer-demand scenario))]
    (/ latency-sum demand-sum)))

(defn report
  "Report the producer durability path and consumer locality separately.

  A cold consumer downloads from its nearest durable target. After a successful
  verified install it has a local opportunistic cache; that cache improves warm
  reads but does not alter targets, acknowledgements, or outage coverage."
  [scenario targets]
  (let [target-sites (set (map #(get-in % [:domains "site"]) targets))
        consumer-sites (set (keys (:consumer-demand scenario)))
        cache-sites (remove target-sites consumer-sites)
        site-counts (frequencies (map #(get-in % [:domains "site"]) targets))
        remaining-after-site-loss
        (apply min (map #(- (count targets) %) (vals site-counts)))]
    {:targets (mapv :id targets)
     :target-sites (vec (sort target-sites))
     :placement-domains-satisfied?
     (satisfies-domains? targets (:placement-minimum-domains scenario))
     :durable-ms (acknowledgement-ms scenario targets)
     :all-targets-ms (apply max (map (partial receipt-ms scenario) targets))
     :demand-weighted-source-rtt-ms (weighted-source-rtt-ms scenario targets)
     :cached-source-rtt-ms 0
     :durable-copy-count (count targets)
     :opportunistic-cache-count (count cache-sites)
     :cache-fill-bytes (* (:snapshot-bytes scenario) (count cache-sites))
     :minimum-durable-copies-after-one-site-loss remaining-after-site-loss
     :write-threshold-survives-one-site-loss?
     (>= remaining-after-site-loss (:write-acks scenario))}))

(defn hash-plan [scenario object-id]
  (placement/place
   (:layout scenario)
   {:replicas (:replicas scenario)
    :write-acks (:write-acks scenario)
    ;; The current profile has one domain map for both N and W. Use the ACK
    ;; constraint here; the report exposes whether the resulting full set has
    ;; the stronger producer placement diversity desired by the experiment.
    :minimum-domains (:ack-minimum-domains scenario)}
   object-id))

(defn planned-targets
  "Choose a stable producer plan under separate hard N-placement and W-ACK
  constraints. The objective combines configured demand locality with write
  confirmation latency. No live consumer observation enters this calculation."
  [scenario]
  (let [candidates (sort-by :id
                            (filter #(not= false (:enabled? %))
                                    (get-in scenario [:layout :members])))
        valid (filter
               (fn [targets]
                 (and (satisfies-domains?
                       targets (:placement-minimum-domains scenario))
                      (some #(satisfies-domains?
                              % (:ack-minimum-domains scenario))
                            (combinations targets (:write-acks scenario)))))
               (combinations candidates (:replicas scenario)))]
    (when (empty? valid)
      (throw (ex-info "No producer placement satisfies the scenario"
                      {:replicas (:replicas scenario)})))
    (first
     (sort-by
      (fn [targets]
        [(+ (* (:read-weight scenario) (weighted-source-rtt-ms scenario targets))
            (* (:write-weight scenario) (acknowledgement-ms scenario targets)))
         (vec (sort (map :id targets)))])
      valid))))

(defn compare-plans [scenario object-id]
  {:hash-only (report scenario (hash-plan scenario object-id))
   :producer-planned (report scenario (planned-targets scenario))})

(defn repeated-hex [pair]
  (apply str (repeat 32 pair)))

(def example-scenario
  {:producer-site "eu-central"
   :persist-ms 3
   :snapshot-bytes (* 128 1024 1024)
   :replicas 3
   :write-acks 2
   :placement-minimum-domains {"owner" 3 "site" 3}
   :ack-minimum-domains {"owner" 2 "site" 2}
   :read-weight 1
   :write-weight 2
   ;; Static product intent, not observed per-user requests.
   :consumer-demand {"eu-central" 50 "eu-west" 20
                     "us-east" 20 "ap-southeast" 10}
   :latency-ms {["eu-central" "eu-west"] 20
                ["eu-central" "us-east"] 80
                ["eu-central" "ap-southeast"] 160
                ["eu-west" "us-east"] 90
                ["eu-west" "ap-southeast"] 170
                ["us-east" "ap-southeast"] 120}
   :layout
   {:placement "netz/blob-placement/2"
    :seed (apply str (repeat 64 "a"))
    :epoch 1
    :members
    [{:id (repeated-hex "aa")
      :domains {"owner" "operator-a" "site" "eu-central"}}
     {:id (repeated-hex "bb")
      :domains {"owner" "operator-b" "site" "eu-central"}}
     {:id (repeated-hex "cc")
      :domains {"owner" "operator-c" "site" "eu-west"}}
     {:id (repeated-hex "dd")
      :domains {"owner" "operator-d" "site" "us-east"}}
     {:id (repeated-hex "ee")
      :domains {"owner" "operator-e" "site" "ap-southeast"}}]}})

(defn -main [& _]
  (prn (compare-plans example-scenario "datahike/catalog/example")))
