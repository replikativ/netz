(ns netz.replication.placement
  "Deterministic replica placement for the resilient blob profile.

  Placement is deliberately independent of discovery. A signed layout says who
  is eligible to store data; rendezvous hashing says which eligible members own
  one object. Learning about another Netz peer never grants it storage authority.

  Profile 1 targets small, explicitly administered clusters. The common path is
  cheap: take the first N rendezvous-ranked members. Only when that set violates
  a failure-domain constraint do we search later ranked combinations. The search
  is bounded so a trusted but mistaken layout cannot turn every write into an
  unbounded combinatorial operation."
  (:require [netz.identity :as identity]
            [netz.wire :as wire]))

(def max-layout-members 32)
(def max-replicas 5)
(def max-constrained-combinations 250000)

(defn- safe-uint? [x]
  (and (int? x) (not (neg? x))))

(defn- member-id? [x]
  ;; Full lowercase Authority ID, never the truncated routing projection.
  ;; Strict spelling prevents collation and authorization differences between
  ;; runtimes from entering placement.
  (and (string? x) (boolean (re-matches #"[0-9a-f]{64}" x))))

(defn- domains? [x]
  (and (map? x)
       (every? string? (keys x))
       (every? #(and (string? %) (not (empty? %))) (vals x))))

(defn eligible-members [layout]
  (filterv #(not= false (:enabled? %)) (:members layout)))

(defn validate-layout
  "Validate and return `layout`.

  A layout contains `:epoch` and `:members` with Authority IDs and domains.
  The main ranking path additionally requires `:placement` and the explicit
  lowercase-hex `:seed`. Member order carries no meaning."
  [layout]
  (let [members (:members layout)
        ids (map :id members)]
    (when-not (and (map? layout)
                   (safe-uint? (:epoch layout))
                   (vector? members)
                   (<= (count members) max-layout-members)
                   (every? #(and (map? %)
                                 (member-id? (:id %))
                                 (domains? (or (:domains %) {}))
                                 (or (nil? (:enabled? %))
                                     (boolean? (:enabled? %))))
                           members)
                   (= (count ids) (count (set ids))))
      (throw (ex-info "Invalid replica layout"
                      {:type :netz.replication/invalid-layout
                       :layout layout
                       :max-members max-layout-members})))
    layout))

(defn validate-policy
  "Validate and normalize placement/durability policy.

  `:minimum-domains` maps a domain name to the number of distinct values which
  the selected replica set must contain. The same constraint is later applied
  to the acknowledgements required for a durability receipt."
  [layout policy]
  (validate-layout layout)
  (let [policy (merge {:minimum-domains {}} policy)
        n (:replicas policy)
        w (:write-acks policy)
        minima (:minimum-domains policy)
        eligible (eligible-members layout)]
    (when-not (and (safe-uint? n) (pos? n) (<= n max-replicas)
                   (safe-uint? w) (pos? w) (<= w n)
                   (<= n (count eligible))
                   (map? minima)
                   (every? string? (keys minima))
                   ;; A durability receipt is emitted at W acknowledgements.
                   ;; Requiring more distinct domains than W would make the
                   ;; configured threshold misleading: it could only succeed
                   ;; after more than W replicas had replied.
                   (every? #(and (safe-uint? %) (pos? %) (<= % w))
                           (vals minima)))
      (throw (ex-info "Invalid replica policy"
                      {:type :netz.replication/invalid-policy
                       :policy policy
                       :eligible (count eligible)
                       :max-replicas max-replicas})))
    (doseq [[domain minimum] minima]
      (let [values (into #{} (keep #(get (:domains %) domain)) eligible)]
        (when (< (count values) minimum)
          (throw (ex-info "Replica layout cannot satisfy failure-domain policy"
                          {:type :netz.replication/insufficient-domains
                           :domain domain
                           :required minimum
                           :available (count values)})))))
    policy))

(defn- unsigned-byte [buf i]
  (bit-and 0xff (aget buf i)))

(defn compare-bytes
  "Unsigned lexicographic comparison of JVM byte arrays and JavaScript
  Uint8Arrays."
  [a b]
  (let [an (identity/buf-length a)
        bn (identity/buf-length b)
        n (min an bn)]
    (loop [i 0]
      (if (= i n)
        (compare an bn)
        (let [c (compare (unsigned-byte a i) (unsigned-byte b i))]
          (if (zero? c) (recur (inc i)) c))))))

(defn score
  "Portable rendezvous score bound to the explicit pool seed, never the epoch."
  [seed object-id member-id]
  (when-not (and (string? seed) (re-matches #"[0-9a-f]{64}" seed))
    (throw (ex-info "Placement seed must be 32 bytes as lowercase hex"
                    {:type :netz.replication/invalid-placement-seed})))
  (identity/sha256
   (wire/encode-canonical
    {"seed" seed "member" member-id "object" object-id
     "profile" "netz/blob-placement/2"})))

(defn rank-by-score
  "Shared eligibility and deterministic tie-breaking for a supplied score function."
  [layout score-fn]
  (validate-layout layout)
  (let [scored (mapv (fn [member]
                       (assoc member ::score
                              (score-fn (:id member))))
                     (eligible-members layout))]
    (->> scored
         (sort (fn [a b]
                 (let [c (compare-bytes (::score b) (::score a))]
                   (if (zero? c)
                     (compare (:id a) (:id b))
                     c))))
         (mapv #(dissoc % ::score)))))

(defn rank
  "Eligible layout members in descending rendezvous order for `object-id`."
  [layout object-id]
  (when-not (= "netz/blob-placement/2" (:placement layout))
    (throw (ex-info "Unsupported placement profile"
                    {:type :netz.replication/invalid-placement-profile})))
  (score (:seed layout) object-id "")
  (rank-by-score layout #(score (:seed layout) object-id %)))

(defn domain-counts
  "Distinct declared values for each constrained domain among `members`."
  [members minimum-domains]
  (into {}
        (for [domain (keys minimum-domains)]
          [domain (count (into #{} (keep #(get (:domains %) domain)) members))])))

(defn satisfies-domains?
  [members minimum-domains]
  (let [counts (domain-counts members minimum-domains)]
    (every? (fn [[domain minimum]]
              (>= (get counts domain 0) minimum))
            minimum-domains)))

(defn- combinations
  "Rank-preserving k-combinations."
  [xs k]
  (letfn [(step [prefix remaining need]
            (lazy-seq
             (cond
               (zero? need)
               (list prefix)

               (< (count remaining) need) nil

               :else
               (concat (step (conj prefix (first remaining))
                             (subvec remaining 1) (dec need))
                       (step prefix (subvec remaining 1) need)))))]
    (step [] (vec xs) k)))

(defn- constrained-placement [ranked replicas minimum-domains]
  ;; Do not put the work limit around a lazy `filter`: when no candidate
  ;; matches, filter has to search without yielding and the outer limit never
  ;; gets a chance to stop it. Count candidates explicitly instead.
  (loop [candidates (combinations ranked replicas)
         examined 0]
    (cond
      (empty? candidates) nil
      (>= examined max-constrained-combinations)
      (throw (ex-info "Constrained placement search exhausted"
                      {:type :netz.replication/placement-search-exhausted
                       :examined examined
                       :limit max-constrained-combinations}))
      (satisfies-domains? (first candidates) minimum-domains)
      (first candidates)
      :else
      (recur (rest candidates) (inc examined)))))

(defn select-ranked
  "Internal shared selection engine. Callers must validate the layout/policy
  and supply its eligible members in profile-specific rank order."
  [ranked {:keys [replicas minimum-domains]}]
  (let [preferred (subvec ranked 0 replicas)]
    (if (satisfies-domains? preferred minimum-domains)
      preferred
      (if-let [result (constrained-placement ranked replicas minimum-domains)]
        result
          ;; Individual domain availability was checked above. Reaching here
          ;; means the constraints cannot be satisfied simultaneously in N
          ;; slots.
        (throw (ex-info "No replica set satisfies all failure domains"
                        {:type :netz.replication/unsatisfiable-domains
                         :replicas replicas
                         :minimum-domains minimum-domains}))))))

(defn place
  "Select deterministic targets using profile-1 scores and validated policy."
  [layout policy object-id]
  (let [policy (validate-policy layout policy)]
    (select-ranked (rank layout object-id) policy)))
