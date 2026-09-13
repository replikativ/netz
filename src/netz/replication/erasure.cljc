(ns netz.replication.erasure
  "Pure controller-side convergence for global blob deletion.

  Campaigns retain the explicit set of replicas known to have held a copy.
  A campaign is complete only after every holder has returned a matching,
  signed deletion receipt; reachability and layout membership never substitute
  for erasure evidence. Effects are emitted only after the corresponding
  campaign record has crossed its durability boundary."
  (:require [clojure.string :as str]
            [netz.identity :as identity]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]))

(def default-opts
  {:retry-ms 30000 :max-records 100000 :max-holders 1024})

(defn- safe-time? [value]
  (and (integer? value) (<= 0 value wire/max-safe-integer)))

(defn- authority-hex [value]
  (cond
    (and (string? value) (re-matches #"(?i)[0-9a-f]{64}" value))
    (str/lower-case value)
    (and (wire/byte-buffer? value) (= 32 (identity/buf-length value)))
    (identity/bytes->hex value)
    :else (throw (ex-info "Invalid erasure authority"
                          {:type ::invalid-authority}))))

(defn- same-bytes? [a b]
  (and (wire/byte-buffer? a) (wire/byte-buffer? b)
       (identity/bufs= a b)))

(defn- same-value? [a b]
  (same-bytes? (wire/encode-canonical a) (wire/encode-canonical b)))

(defn- parse [envelope]
  (:statement (replication-wire/parse-envelope envelope)))

(defn- erasure-key [statement]
  (str (identity/bytes->hex (get statement "cluster")) "/"
       (identity/bytes->hex (get statement "privacy_domain")) "/"
       (identity/bytes->hex (get statement "object"))))

(defn- statement-fingerprint [statement]
  (identity/bytes->hex
   (identity/sha256 (replication-wire/signing-bytes statement))))

(defn make-state
  ([cluster controllers] (make-state cluster controllers {}))
  ([cluster controllers opts]
   (let [opts (merge default-opts opts)
         controllers (set (map authority-hex controllers))]
     (when-not (and (wire/byte-buffer? cluster)
                    (<= 1 (identity/buf-length cluster) 64)
                    (seq controllers)
                    (safe-time? (:retry-ms opts)) (pos? (:retry-ms opts))
                    (int? (:max-records opts)) (pos? (:max-records opts))
                    (int? (:max-holders opts)) (pos? (:max-holders opts)))
       (throw (ex-info "Invalid erasure coordinator options"
                       {:type ::invalid-options})))
     {:cluster cluster :controllers controllers :records {} :pending {}
      :opts opts
      :stats {:started 0 :dispatched 0 :receipts 0 :duplicates 0
              :rejected 0 :persistence-failures 0}})))

(defn- receipt-matches? [record receipt]
  (let [deletion (:statement record)]
    (and (= "blob/deleted" (get receipt "type"))
         (= (get deletion "revision") (get receipt "deletion_revision"))
         (= (get deletion "layout_epoch") (get receipt "layout_epoch"))
         (every? true? (map #(same-bytes? (get deletion %) (get receipt %))
                            ["cluster" "privacy_domain" "manifest" "object"]))
         (same-bytes? (get receipt "replica") (get receipt "signer"))
         (contains? (set (:holders record))
                    (authority-hex (get receipt "replica"))))))

(defn- complete? [record]
  (= (set (:holders record)) (set (keys (:receipts record)))))

(defn- public-status [record]
  (let [acknowledged (set (keys (:receipts record)))
        pending (remove acknowledged (:holders record))]
    {:status (if (complete? record) :complete :pending)
     :key (:key record)
     :deletion-revision (get-in record [:statement "revision"])
     :holders (count (:holders record))
     :acknowledged (count acknowledged)
     :pending (vec pending)
     :receipts (:receipts record)}))

(defn status
  "Return controller evidence for `key`, or `{:status :unknown}`."
  [state key]
  (if-let [record (get-in state [:records key])]
    (public-status record)
    {:status :unknown :key key}))

(defn statuses [state]
  (mapv (comp public-status val) (sort-by key (:records state))))

(defn- persist-action [state key record after]
  {:state (assoc-in state [:pending key] {:record record :after after})
   :actions [[:persist-erasure key record]]})

(defn- retry-actions [state record now]
  (let [outstanding (remove #(contains? (:receipts record) %) (:holders record))
        due (filter #(let [attempted (get-in record [:attempted-at %])]
                       (or (nil? attempted)
                           (<= (+ attempted (get-in state [:opts :retry-ms]))
                               now)))
                    outstanding)
        attempted (reduce #(assoc-in %1 [:attempted-at %2] now) record due)
        actions (mapv #(vector :dispatch-deletion % (:deletion-envelope record)) due)]
    [attempted actions]))

(defn- begin [state {:keys [deletion-envelope holders]} now]
  (try
    (let [statement (parse deletion-envelope)
          holder-ids (->> holders (map authority-hex) distinct sort vec)
          key (erasure-key statement)
          current (get-in state [:records key])]
      (cond
        (not= "blob/deletion" (get statement "type"))
        {:state (update-in state [:stats :rejected] inc) :actions []}
        (not (contains? (:controllers state)
                        (authority-hex (get statement "signer"))))
        {:state (update-in state [:stats :rejected] inc) :actions []}
        (not (same-bytes? (:cluster state) (get statement "cluster")))
        {:state (update-in state [:stats :rejected] inc) :actions []}
        (or (empty? holder-ids)
            (> (count holder-ids) (get-in state [:opts :max-holders])))
        {:state (update-in state [:stats :rejected] inc) :actions []}
        (contains? (:pending state) key)
        {:state (update-in state [:stats :rejected] inc) :actions []}
        current
        (if (= (statement-fingerprint statement) (:fingerprint current))
          (let [expanded (vec (sort (into (set (:holders current))
                                          holder-ids)))]
            (cond
              (> (count expanded) (get-in state [:opts :max-holders]))
              {:state (update-in state [:stats :rejected] inc) :actions []}
              (= expanded (:holders current))
              {:state (update-in state [:stats :duplicates] inc) :actions []}
              :else
              (let [record (assoc current :holders expanded)
                    [record actions] (retry-actions state record now)]
                (persist-action state key record
                                (conj actions [:erasure-progress key])))))
          {:state (update-in state [:stats :rejected] inc) :actions []})
        (>= (count (:records state)) (get-in state [:opts :max-records]))
        {:state (update-in state [:stats :rejected] inc) :actions []}
        :else
        (let [record {:key key :statement statement
                      :fingerprint (statement-fingerprint statement)
                      :deletion-envelope deletion-envelope
                      :holders holder-ids :receipts {} :attempted-at {}}
              [record actions] (retry-actions state record now)]
          (-> (persist-action state key record
                              (conj actions [:erasure-progress key]))
              (update :state update-in [:stats :started] inc)))))
    (catch #?(:clj Throwable :cljs :default) _
      {:state (update-in state [:stats :rejected] inc) :actions []})))

(defn- accept-receipt [state envelope]
  (try
    (let [receipt (parse envelope)
          key (erasure-key receipt)
          record (get-in state [:records key])
          replica (authority-hex (get receipt "replica"))]
      (cond
        (or (nil? record) (contains? (:pending state) key)
            (not (receipt-matches? record receipt)))
        {:state (update-in state [:stats :rejected] inc) :actions []}
        (contains? (:receipts record) replica)
        {:state (update-in state [:stats :duplicates] inc) :actions []}
        :else
        (let [updated (assoc-in record [:receipts replica] envelope)]
          (persist-action state key updated [[:erasure-progress key]]))))
    (catch #?(:clj Throwable :cljs :default) _
      {:state (update-in state [:stats :rejected] inc) :actions []})))

(defn- tick [state now]
  (reduce
   (fn [{current :state actions :actions} [key record]]
     (if (or (contains? (:pending current) key) (complete? record))
       {:state current :actions actions}
       (let [[updated dispatches] (retry-actions current record now)]
         (if (empty? dispatches)
           {:state current :actions actions}
           (let [{next-state :state more :actions}
                 (persist-action current key updated dispatches)]
             {:state next-state :actions (into actions more)})))))
   {:state state :actions []}
   (:records state)))

(defn- persisted [state key result]
  (if-let [{:keys [record after]} (get-in state [:pending key])]
    (if (= :ok result)
      {:state (-> state (assoc-in [:records key] record)
                  (update :pending dissoc key))
       :actions (vec after)}
      {:state (-> state (update :pending dissoc key)
                  (update-in [:stats :persistence-failures] inc))
       :actions []})
    {:state state :actions []}))

(defn restore-state [state records]
  (when-not (and (map? records)
                 (<= (count records) (get-in state [:opts :max-records])))
    (throw (ex-info "Invalid erasure journal" {:type ::invalid-journal})))
  ;; Replay through the same structural checks but do not depend on signatures:
  ;; the runtime verifies both deletion and receipt envelopes before calling it.
  (doseq [[key record] records]
    (let [statement (parse (:deletion-envelope record))]
      (when-not (and (= #{:key :statement :fingerprint :deletion-envelope
                          :holders :receipts :attempted-at}
                        (set (keys record)))
                     (= key (:key record) (erasure-key statement))
                     (same-value? statement (:statement record))
                     (= "blob/deletion" (get statement "type"))
                     (= (:fingerprint record) (statement-fingerprint statement))
                     (contains? (:controllers state)
                                (authority-hex (get statement "signer")))
                     (same-bytes? (:cluster state) (get statement "cluster"))
                     (vector? (:holders record))
                     (<= 1 (count (:holders record))
                         (get-in state [:opts :max-holders]))
                     (= (:holders record) (vec (sort (distinct (:holders record)))))
                     (every? #(try (authority-hex %) true
                                   (catch #?(:clj Throwable :cljs :default) _
                                     false))
                             (:holders record))
                     (map? (:attempted-at record))
                     (every? (fn [[replica-id attempted-at]]
                               (and (contains? (set (:holders record)) replica-id)
                                    (safe-time? attempted-at)))
                             (:attempted-at record))
                     (map? (:receipts record))
                     (every? (fn [[replica-id envelope]]
                               (let [receipt (parse envelope)]
                                 (and (receipt-matches? record receipt)
                                      (= replica-id
                                         (authority-hex
                                          (get receipt "replica"))))))
                             (:receipts record)))
        (throw (ex-info "Invalid erasure journal record"
                        {:type ::invalid-journal :key key})))))
  (assoc state :records records))

(defn handler [state event ctx]
  (let [now (or (:now ctx) 0)]
    (case (:type event)
      :erasure/begin (begin state event now)
      :erasure/receipt (accept-receipt state (:envelope event))
      :erasure/tick (tick state now)
      :erasure/persisted (persisted state (:key event) (:result event))
      :init (tick state now)
      {:state state :actions []})))
