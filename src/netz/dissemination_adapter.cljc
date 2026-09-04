(ns netz.dissemination-adapter
  "Pure adaptation between portable full-Authority frames and the transitional
  UUID-routed dissemination machine. Routing projections never authorize data."
  (:require [netz.dissemination-wire :as wire]
            [netz.identity :as identity]))

(defn- route [authority]
  (identity/authority-routing-id authority))

(defn- claim-authority
  [known routing authority]
  (if-let [existing (get known routing)]
    (if (= existing authority)
      known
      (throw (ex-info "Authority IDs collide at the routing projection"
                      {:type :netz.dissemination-adapter/routing-collision
                       :routing-id routing})))
    (assoc known routing authority)))

(defn- inbound-have [state frame]
  (let [summary (:summary frame)
        horizon (:horizon frame)
        authorities (reduce (fn [known authority]
                              (claim-authority known (route authority) authority))
                            (:authority-ids state)
                            (keys summary))
        streams (for [[authority epochs] summary
                      [epoch topics] epochs
                      [topic _] topics]
                  [(route authority) epoch topic])]
    (assoc frame
           :summary (reduce-kv
                     (fn [result authority epochs]
                       (assoc result (route authority) epochs))
                     {} summary)
           :horizon (into {}
                          (map (fn [[[authority epoch topic] floor]]
                                 [[(route authority) epoch topic] floor]))
                          horizon)
           :origin-authorities
           (select-keys authorities (map first streams))
           :portable-streams (set streams))))

(defn- inbound-want [state frame]
  (update frame :ranges
          (fn [ranges]
            (mapv (fn [{:keys [origin] :as range}]
                    (let [routing (route origin)]
                      (when-not (= origin (get-in state [:authority-ids routing]))
                        (throw (ex-info "want names an unknown Authority stream"
                                        {:type :netz.dissemination-adapter/unknown-stream})))
                      (assoc range :origin routing)))
                  ranges))))

(defn from-wire
  "Adapt one already schema-checked wire map for the pure state machine."
  [state frame]
  (let [frame (wire/frame-from-wire frame)]
    (case (:type frame)
      :have (inbound-have state frame)
      :want (inbound-want state frame)
      frame)))

(defn- outbound-have [state frame]
  (let [portable (:portable-streams state)
        authorities (:authority-ids state)
        streams (for [[origin epochs] (:summary frame)
                      [epoch topics] epochs
                      [topic ranges] topics
                      :when (contains? portable [origin epoch topic])
                      :let [authority (get authorities origin)]
                      :when authority]
                  [authority epoch topic ranges])
        summary (reduce (fn [result [authority epoch topic ranges]]
                          (assoc-in result [authority epoch topic] ranges))
                        {} streams)
        horizon (into {}
                      (map (fn [[authority epoch topic _]]
                             [[authority epoch topic]
                              (get (:horizon frame)
                                   [(route authority) epoch topic] 0)]))
                      streams)]
    (assoc frame :summary summary :horizon horizon)))

(defn- outbound-want [state frame]
  (update frame :ranges
          (fn [ranges]
            (mapv (fn [{:keys [origin] :as range}]
                    (if-let [authority (get-in state [:authority-ids origin])]
                      (assoc range :origin authority)
                      (throw (ex-info "want lacks a full stream Authority ID"
                                      {:type :netz.dissemination-adapter/unknown-stream}))))
                  ranges))))

(defn to-wire
  "Adapt one state-machine frame and return its strict profile-1 map."
  [state frame]
  (wire/frame->wire
   (case (:type frame)
     :have (outbound-have state frame)
     :want (outbound-want state frame)
     frame)))
