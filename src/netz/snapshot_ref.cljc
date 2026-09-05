(ns netz.snapshot-ref
  "Signed, transport-independent references to immutable DAG snapshots.

  A reference announces causality; it does not claim that the referenced
  object closure is locally materialized. Statements are safe to receive out
  of order and a stream may have several concurrent heads. Applications decide
  how snapshot and parent byte strings map to their own object graph."
  (:require [clojure.core.async :as async :refer [chan close! put!]]
            [netz.identity :as identity]
            [netz.topics :as topics]
            [netz.wire :as wire]))

(def ^:const statement-version "netz/snapshot-ref/v1")
(def ^:const signing-domain "netz/snapshot-ref/v1\u0000")
(def ^:const max-profile-bytes 128)
(def ^:const max-id-bytes 256)
(def ^:const max-parents 64)
(def ^:const max-ordering-bytes 512)

(defn- bounded-text? [value max-bytes]
  (and (string? value)
       (pos? (count value))
       (<= (identity/buf-length (identity/utf8-bytes value)) max-bytes)))

(defn- bounded-bytes? [value max-bytes]
  (and (wire/byte-buffer? value)
       (pos? (identity/buf-length value))
       (<= (identity/buf-length value) max-bytes)))

(defn- hex32? [value]
  (and (string? value) (boolean (re-matches #"(?i)[0-9a-f]{64}" value))))

(defn- canonical-parents [parents]
  (->> parents
       (sort-by identity/bytes->hex)
       (reduce (fn [result parent]
                 (if (and (seq result)
                          (identity/bufs= parent (peek result)))
                   result
                   (conj result parent)))
               [])))

(defn make-statement
  "Create the unsigned, canonical statement value.

  `system`, `ref`, `snapshot` and every parent are opaque byte strings. The
  application profile owns their interpretation. `ordering`, when present, is
  likewise opaque signed data reserved for a profile-defined fence (for
  example an Urd term/index); Netz does not compare it."
  [{:keys [profile system ref snapshot parents publisher ordering]}]
  (cond-> {:netz/kind statement-version
           :netz/profile profile
           :netz/system system
           :netz/ref ref
           :netz/snapshot snapshot
           :netz/parents (canonical-parents (or parents []))
           :netz/publisher publisher}
    (some? ordering) (assoc :netz/ordering ordering)))

(defn statement?
  [statement]
  (and (map? statement)
       (= statement-version (:netz/kind statement))
       (bounded-text? (:netz/profile statement) max-profile-bytes)
       (every? #(bounded-bytes? % max-id-bytes)
               [(:netz/system statement)
                (:netz/ref statement)
                (:netz/snapshot statement)])
       (vector? (:netz/parents statement))
       (<= (count (:netz/parents statement)) max-parents)
       (every? #(bounded-bytes? % max-id-bytes) (:netz/parents statement))
       (not-any? #(identity/bufs= (:netz/snapshot statement) %)
                 (:netz/parents statement))
       (= (:netz/parents statement)
          (canonical-parents (:netz/parents statement)))
       (hex32? (:netz/publisher statement))
       (or (not (contains? statement :netz/ordering))
           (bounded-bytes? (:netz/ordering statement) max-ordering-bytes))))

(defn signing-object
  "Return the deterministic-CBOR object covered by the signature."
  [statement]
  (when-not (statement? statement)
    (throw (ex-info "snapshot reference is outside the portable profile"
                    {:type :netz.snapshot-ref/non-portable-statement})))
  (cond-> {"v" 1
           "type" "snapshot-ref"
           "profile" (:netz/profile statement)
           "system" (:netz/system statement)
           "ref" (:netz/ref statement)
           "snapshot" (:netz/snapshot statement)
           "parents" (:netz/parents statement)
           "publisher" (identity/hex->bytes (:netz/publisher statement))}
    (contains? statement :netz/ordering)
    (assoc "ordering" (:netz/ordering statement))))

(defn signing-bytes [statement]
  (identity/concat-bufs
   (identity/utf8-bytes signing-domain)
   (wire/encode-canonical (signing-object statement))))

(defn sign
  "Sign a reference with an Authority's operational key. Returns a channel."
  [{:keys [genesis operational]} fields]
  (let [statement (make-statement
                   (assoc fields :publisher (identity/authority-id genesis)))
        ch (chan 1)]
    (async/go
      (try
        (let [signature (async/<! (identity/sign (:private operational)
                                                 (signing-bytes statement)))]
          (put! ch (if (instance? #?(:clj Exception :cljs js/Error) signature)
                     signature
                     (assoc statement
                            :netz/publisher-genesis genesis
                            :netz/publisher-key (:public operational)
                            :netz/signature signature))))
        (catch #?(:clj Exception :cljs js/Error) error
          (put! ch error)))
      (close! ch))
    ch))

(defn dag-event-fields
  "Map a self-contained Yggdrasil-style durable commit event to wire fields.

  This is deliberately structural and does not depend on Yggdrasil. String IDs
  are encoded as exact UTF-8; a profile must specify any stronger normalization
  (for example lowercase canonical UUID text). An ordering fence is accepted
  only as caller-supplied bytes because Netz cannot canonically encode an
  application-specific ordering map without defining its semantics."
  ([profile event] (dag-event-fields profile event {}))
  ([profile {:keys [system-id snapshot-id parent-ids ref durable?]}
    {:keys [ordering-bytes]}]
   (when-not (and (bounded-text? profile max-profile-bytes)
                  (true? durable?)
                  (bounded-text? system-id max-id-bytes)
                  (bounded-text? snapshot-id max-id-bytes)
                  (bounded-text? ref max-id-bytes)
                  (set? parent-ids)
                  (<= (count parent-ids) max-parents)
                  (every? #(bounded-text? % max-id-bytes) parent-ids)
                  (or (nil? ordering-bytes)
                      (bounded-bytes? ordering-bytes max-ordering-bytes)))
     (throw (ex-info "incomplete or non-durable DAG commit event"
                     {:type :netz.snapshot-ref/invalid-dag-event})))
   (cond-> {:profile profile
            :system (identity/utf8-bytes system-id)
            :ref (identity/utf8-bytes ref)
            :snapshot (identity/utf8-bytes snapshot-id)
            :parents (mapv identity/utf8-bytes parent-ids)}
     ordering-bytes (assoc :ordering ordering-bytes))))

(defn sign-dag-event
  "Sign a complete local DAG event for publication. Returns a channel."
  ([authority profile event]
   (sign authority (dag-event-fields profile event)))
  ([authority profile event opts]
   (sign authority (dag-event-fields profile event opts))))

(defn verify
  "Verify a signed reference without prior contact. Returns true/false on a channel."
  [statement]
  (let [ch (chan 1)]
    (async/go
      (put! ch
            (boolean
             (try
               (let [genesis (:netz/publisher-genesis statement)
                     public-key (:netz/publisher-key statement)
                     signature (:netz/signature statement)
                     publisher (:netz/publisher statement)]
                 (and (= identity/key-size (identity/buf-length public-key))
                      (= identity/signature-size (identity/buf-length signature))
                      (identity/genesis-authorises? genesis public-key publisher)
                      (async/<! (identity/verify public-key
                                                 (signing-bytes statement)
                                                 signature))))
               (catch #?(:clj Exception :cljs js/Error) _ false))))
      (close! ch))
    ch))

(defn- genesis-from-wire [value]
  (when-not (and (map? value)
                 (= #{"v" "rotation" "threshold" "operational" "revoke"}
                    (set (keys value)))
                 (= 1 (get value "v")))
    (throw (ex-info "invalid snapshot-reference genesis"
                    {:type :netz.snapshot-ref/invalid-envelope})))
  (let [rotation (get value "rotation")
        operational (get value "operational")
        threshold (get value "threshold")
        revoke (get value "revoke")]
    (when-not (and (vector? rotation) (seq rotation)
                   (vector? operational) (seq operational)
                   (every? #(and (wire/byte-buffer? %)
                                 (= identity/key-size (identity/buf-length %)))
                           (concat rotation operational [revoke]))
                   (integer? threshold) (pos? threshold)
                   (<= threshold (count rotation)))
      (throw (ex-info "invalid snapshot-reference genesis fields"
                      {:type :netz.snapshot-ref/invalid-envelope})))
    (identity/make-genesis
     {:rotation-keys (mapv identity/bytes->hex rotation)
      :rotation-threshold threshold
      :operational-keys (mapv identity/bytes->hex operational)
      :revocation-commit (identity/bytes->hex revoke)})))

(defn ->wire [statement]
  (let [signed (signing-object statement)
        public-key (:netz/publisher-key statement)
        signature (:netz/signature statement)]
    (when-not (and (identity/genesis? (:netz/publisher-genesis statement))
                   (= identity/key-size (identity/buf-length public-key))
                   (= identity/signature-size (identity/buf-length signature))
                   (identity/genesis-authorises?
                    (:netz/publisher-genesis statement) public-key
                    (:netz/publisher statement)))
      (throw (ex-info "invalid snapshot-reference credentials"
                      {:type :netz.snapshot-ref/invalid-envelope})))
    (assoc signed
           "genesis" (identity/genesis->wire (:netz/publisher-genesis statement))
           "operational" public-key
           "signature" signature)))

(defn from-wire [value]
  (let [required #{"v" "type" "profile" "system" "ref" "snapshot" "parents"
                   "publisher" "genesis" "operational" "signature"}
        allowed (conj required "ordering")]
    (when-not (and (map? value)
                   (= required (set (filter #(contains? value %) required)))
                   (every? allowed (keys value))
                   (= 1 (get value "v"))
                   (= "snapshot-ref" (get value "type")))
      (throw (ex-info "invalid snapshot-reference envelope"
                      {:type :netz.snapshot-ref/invalid-envelope})))
    (let [genesis (genesis-from-wire (get value "genesis"))
          publisher-bytes (get value "publisher")
          public-key (get value "operational")
          signature (get value "signature")
          statement (cond->
                     (make-statement
                      {:profile (get value "profile")
                       :system (get value "system")
                       :ref (get value "ref")
                       :snapshot (get value "snapshot")
                       :parents (get value "parents")
                       :publisher (when (wire/byte-buffer? publisher-bytes)
                                    (identity/bytes->hex publisher-bytes))})
                      (contains? value "ordering")
                      (assoc :netz/ordering (get value "ordering")))
          statement (assoc statement
                           :netz/publisher-genesis genesis
                           :netz/publisher-key public-key
                           :netz/signature signature)]
      (when-not (and (statement? statement)
                     (= identity/peer-id-size (identity/buf-length publisher-bytes))
                     (= identity/key-size (identity/buf-length public-key))
                     (= identity/signature-size (identity/buf-length signature))
                     (identity/genesis-authorises?
                      genesis public-key (:netz/publisher statement)))
        (throw (ex-info "invalid snapshot-reference fields"
                        {:type :netz.snapshot-ref/invalid-envelope})))
      statement)))

(defn encode [statement] (wire/encode-canonical (->wire statement)))
(defn decode [bytes] (from-wire (wire/decode bytes)))

(defn stream-key [statement]
  [(:netz/profile statement)
   (identity/bytes->hex (:netz/system statement))
   (identity/bytes->hex (:netz/ref statement))])

(defn snapshot-key [statement]
  (identity/bytes->hex (:netz/snapshot statement)))

(defn make-state
  "Create receiver state. Ownership is independently pinned per profile/system/ref."
  ([] (make-state {}))
  ([opts]
   {:opts (merge {:trust-on-first-use? true
                  :max-conflicts 64
                  :max-streams 1024
                  :max-nodes-per-stream 100000}
                 opts)
    :owners {}
    :nodes {}
    :heads {}
    :conflicts {}
    :stats {:accepted 0 :duplicates 0 :conflicts 0 :cycles 0 :capacity 0
            :wrong-publisher 0
            :invalid-signature 0 :malformed 0}}))

(defn pin
  [state profile system ref publisher]
  (assoc-in state [:owners [profile (identity/bytes->hex system)
                            (identity/bytes->hex ref)]] publisher))

(defn heads
  "Return the current, possibly multi-head snapshot IDs as hex strings."
  [state profile system ref]
  (get-in state [:heads [profile (identity/bytes->hex system)
                         (identity/bytes->hex ref)]] #{}))

(defn missing-parents
  "Return parent IDs named by accepted statements but not yet received."
  [state profile system ref]
  (let [stream [profile (identity/bytes->hex system) (identity/bytes->hex ref)]
        nodes (get-in state [:nodes stream] {})]
    (->> (vals nodes)
         (mapcat :parents)
         (remove #(contains? nodes %))
         set)))

(defn known-snapshots
  "Return the snapshot IDs retained for one stream as hex strings."
  [state profile system ref]
  (set (keys (get-in state
                     [:nodes [profile (identity/bytes->hex system)
                              (identity/bytes->hex ref)]]
                     {}))))

(defn statements-since
  "Return signed statements reachable from local heads until `remote-heads`.

  Results are deterministic but need not be parent-first; receivers accept the
  DAG out of order. A remote head unknown locally cannot suppress any local
  history."
  [state profile system ref remote-heads]
  (let [stream [profile (identity/bytes->hex system) (identity/bytes->hex ref)]
        nodes (get-in state [:nodes stream] {})
        stop (set remote-heads)]
    (loop [todo (seq (sort (get-in state [:heads stream] #{})))
           selected #{}]
      (if-let [snapshot (first todo)]
        (if (or (stop snapshot)
                (selected snapshot)
                (not (contains? nodes snapshot)))
          (recur (next todo) selected)
          (recur (concat (next todo) (sort (get-in nodes [snapshot :parents])))
                 (conj selected snapshot)))
        (mapv #(get-in nodes [% :statement]) (sort selected))))))

(defn- ancestor?
  [nodes ancestor descendant]
  (loop [todo (seq (get-in nodes [descendant :parents]))
         seen #{}]
    (if-let [candidate (first todo)]
      (cond
        (= candidate ancestor) true
        (seen candidate) (recur (next todo) seen)
        :else (recur (concat (next todo) (get-in nodes [candidate :parents]))
                     (conj seen candidate)))
      false)))

(defn- semantic-node [statement]
  {:snapshot (snapshot-key statement)
   :parents (set (map identity/bytes->hex (:netz/parents statement)))
   :ordering (some-> (:netz/ordering statement) identity/bytes->hex)
   :statement statement})

(defn- add-head [head-set nodes snapshot]
  (let [covered? (some #(ancestor? nodes snapshot %) head-set)
        ;; A late intermediate node can connect two previously disconnected
        ;; heads. Even if a known descendant covers this node, its newly known
        ;; ancestors must be removed from the frontier.
        remaining (set (remove #(ancestor? nodes % snapshot) head-set))]
    (if covered?
      remaining
      (conj remaining snapshot))))

(defn accept
  "Apply an already authenticated statement. Returns `[state outcome]`.

  Delivery order never determines causality: a child received before its
  parent stays the head when the parent later arrives. Concurrent children are
  retained as multiple heads."
  [state statement]
  (if-not (statement? statement)
    [(update-in state [:stats :malformed] inc) :malformed]
    (let [stream (stream-key statement)
          publisher (:netz/publisher statement)
          owner (get-in state [:owners stream])
          snapshot (snapshot-key statement)
          node (semantic-node statement)
          nodes (get-in state [:nodes stream] {})
          current (get nodes snapshot)]
      (cond
        (and owner (not= owner publisher))
        [(update-in state [:stats :wrong-publisher] inc) :wrong-publisher]

        (and (nil? owner) (not (get-in state [:opts :trust-on-first-use?])))
        [(update-in state [:stats :wrong-publisher] inc) :wrong-publisher]

        (= (dissoc current :statement) (dissoc node :statement))
        [(update-in state [:stats :duplicates] inc) :duplicate]

        current
        [(-> state
             (update-in [:conflicts stream]
                        (fn [items]
                          (let [items (conj (vec (or items []))
                                            [(:statement current) statement])]
                            (vec (take-last (get-in state [:opts :max-conflicts])
                                            items)))))
             (update-in [:stats :conflicts] inc))
         :conflict]

        (some #(ancestor? nodes snapshot %) (:parents node))
        [(update-in state [:stats :cycles] inc) :cycle]

        (or (and (not (contains? (:nodes state) stream))
                 (>= (count (:nodes state)) (get-in state [:opts :max-streams])))
            (>= (count nodes) (get-in state [:opts :max-nodes-per-stream])))
        [(update-in state [:stats :capacity] inc) :capacity]

        :else
        (let [state (-> state
                        (assoc-in [:owners stream] publisher)
                        (assoc-in [:nodes stream snapshot] node))
              nodes (get-in state [:nodes stream])]
          [(-> state
               (update-in [:heads stream] #(add-head (or % #{}) nodes snapshot))
               (update-in [:stats :accepted] inc))
           :accepted])))))

(defn accept-verified [state statement]
  (let [ch (chan 1)]
    (async/go
      (if (async/<! (verify statement))
        (put! ch (accept state statement))
        (put! ch [(update-in state [:stats :invalid-signature] inc)
                  :invalid-signature]))
      (close! ch))
    ch))

(defn topic-for
  "Hierarchical pub/sub topic for one signed ref stream."
  [publisher profile system ref]
  [:netz/snapshot-refs publisher profile
   (identity/bytes->hex system) (identity/bytes->hex ref)])

(defn covers-stream? [ranges publisher profile system ref]
  (topics/covered? ranges (topic-for publisher profile system ref)))
