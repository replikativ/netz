(ns netz.dissemination-wire
  "Strict language-neutral frames for signed dissemination and bounded repair.

  The pure dissemination state machine still uses idiomatic Clojure data. This
  namespace is its standards boundary: text-key maps, full Authority IDs,
  deterministic arrays instead of sets/maps with host-specific keys, and
  opaque publication payload bytes."
  (:require [netz.identity :as identity]
            [netz.publication :as publication]
            [netz.topics :as topics]
            [netz.wire :as wire]))

(def capability "netz/dissemination/1")

(def max-interests 1024)
(def max-streams 1024)
(def max-intervals-per-stream 512)
(def max-total-intervals 1024)
(def max-want-ranges 16)

(def ^:private safe-max 9007199254740991)

(defn- safe-uint? [value]
  (and (integer? value) (<= 0 value safe-max)))

(defn- topic? [value]
  (and (string? value)
       (pos? (count value))
       (<= (identity/buf-length (identity/utf8-bytes value))
           publication/max-topic-bytes)))

(defn- authority-bytes [value]
  (let [bytes (cond
                (wire/byte-buffer? value) value
                (and (string? value)
                     (re-matches #"(?i)[0-9a-f]{64}" value))
                (identity/hex->bytes value)
                :else nil)]
    (when-not (and bytes (= identity/peer-id-size (identity/buf-length bytes)))
      (throw (ex-info "dissemination origin must be a full Authority ID"
                      {:type :netz.dissemination-wire/invalid-origin})))
    bytes))

(defn- topic-order [topic]
  (identity/bytes->hex (identity/utf8-bytes topic)))

(defn- ordered-unique?
  [order-key values]
  (or (empty? values)
      (loop [previous (order-key (first values))
             remaining (next values)]
        (if-let [value (first remaining)]
          (let [current (order-key value)]
            (and (neg? (compare previous current))
                 (recur current (next remaining))))
          true))))

(defn- interval? [value]
  (and (vector? value)
       (= 2 (count value))
       (safe-uint? (first value))
       (safe-uint? (second value))
       (<= (first value) (second value))))

(defn- intervals? [values]
  (and (vector? values)
       (<= (count values) max-intervals-per-stream)
       (every? interval? values)
       ;; Canonical interval sets are sorted, disjoint and non-adjacent.
       (or (empty? values)
           (loop [previous (first values)
                  remaining (next values)]
             (if-let [[lo :as current] (first remaining)]
               (and (< (inc (second previous)) lo)
                    (recur current (next remaining)))
               true)))))

(defn- exact-carries->wire [ranges]
  (let [ranges (or ranges #{})]
    (when-not (and (coll? ranges)
                   (every? #(or (= topics/everything %)
                                (and (vector? %) (= 1 (count %))
                                     (topic? (first %))))
                           ranges))
      (throw (ex-info
              "profile-1 carries are relay-all or exact portable topics"
              {:type :netz.dissemination-wire/invalid-carries})))
    {:relay-all (boolean (some #(= topics/everything %) ranges))
     :topics (vec (sort-by topic-order
                           (into #{} (keep #(when (= 1 (count %)) (first %)))
                                 ranges)))}))

(defn- interests->wire [{:keys [topics carries] :as frame}]
  (when-not (and (= :interests (:type frame))
                 (coll? topics) (<= (count topics) max-interests)
                 (every? topic? topics))
    (throw (ex-info "invalid portable interests frame"
                    {:type :netz.dissemination-wire/invalid-interests})))
  (let [{carry-topics :topics relay-all :relay-all}
        (exact-carries->wire carries)]
    (when (> (count carry-topics) max-interests)
      (throw (ex-info "too many portable carry topics"
                      {:type :netz.dissemination-wire/interest-limit})))
    {"v" 1
     "type" "interests"
     "topics" (vec (sort-by topic-order (set topics)))
     "carries" carry-topics
     "relay_all" relay-all}))

(defn- stream-entry
  [horizon [origin epoch topic ranges]]
  (let [origin (authority-bytes origin)
        floor (get horizon [(identity/bytes->hex origin) epoch topic]
                   (get horizon [origin epoch topic] 0))]
    (when-not (and (safe-uint? epoch) (topic? topic) (intervals? (vec ranges))
                   (safe-uint? floor))
      (throw (ex-info "invalid portable have stream"
                      {:type :netz.dissemination-wire/invalid-have-stream})))
    {:order [(identity/bytes->hex origin) epoch (topic-order topic)]
     :wire {"origin" origin "epoch" epoch "topic" topic
            "ranges" (vec ranges) "floor" floor}}))

(defn- flatten-summary [summary]
  (for [[origin epochs] summary
        [epoch streams] epochs
        [topic ranges] streams]
    [origin epoch topic ranges]))

(defn- have->wire [{:keys [summary horizon] :as frame}]
  (when-not (and (= :have (:type frame)) (map? summary) (map? horizon))
    (throw (ex-info "invalid portable have frame"
                    {:type :netz.dissemination-wire/invalid-have})))
  (let [entries (mapv #(stream-entry horizon %) (flatten-summary summary))]
    (when (or (> (count entries) max-streams)
              (> (reduce + (map #(count (get-in % [:wire "ranges"])) entries))
                 max-total-intervals)
              (not (ordered-unique? :order (sort-by :order entries))))
      (throw (ex-info "too many or duplicate portable have streams"
                      {:type :netz.dissemination-wire/have-limit})))
    {"v" 1 "type" "have"
     "streams" (mapv :wire (sort-by :order entries))}))

(defn- want-entry [{:keys [origin epoch topic lo hi] :as value}]
  (let [origin (authority-bytes origin)]
    (when-not (and (= #{:origin :epoch :topic :lo :hi} (set (keys value)))
                   (safe-uint? epoch) (topic? topic)
                   (safe-uint? lo) (safe-uint? hi) (<= lo hi))
      (throw (ex-info "invalid portable want range"
                      {:type :netz.dissemination-wire/invalid-want-range})))
    {:order [(identity/bytes->hex origin) epoch (topic-order topic) lo hi]
     :wire {"origin" origin "epoch" epoch "topic" topic "lo" lo "hi" hi}}))

(defn- want->wire [{:keys [ranges] :as frame}]
  (when-not (and (= :want (:type frame)) (coll? ranges)
                 (<= (count ranges) max-want-ranges))
    (throw (ex-info "invalid portable want frame"
                    {:type :netz.dissemination-wire/invalid-want})))
  (let [entries (mapv want-entry ranges)]
    (when-not (ordered-unique? :order (sort-by :order entries))
      (throw (ex-info "duplicate portable want range"
                      {:type :netz.dissemination-wire/invalid-want})))
    {"v" 1 "type" "want"
     "ranges" (mapv :wire (sort-by :order entries))}))

(defn frame->wire
  "Convert a portable dissemination frame to its closed profile-1 map."
  [frame]
  (case (:type frame)
    :interests (interests->wire frame)
    :have (have->wire frame)
    :want (want->wire frame)
    :gossip (publication/->wire frame)
    (throw (ex-info "unknown portable dissemination frame"
                    {:type :netz.dissemination-wire/unknown-frame
                     :frame-type (:type frame)}))))

(defn- wire-interests [frame]
  (let [topics (get frame "topics")
        carries (get frame "carries")]
    (when-not (and (= #{"v" "type" "topics" "carries" "relay_all"}
                      (set (keys frame)))
                   (vector? topics) (<= (count topics) max-interests)
                   (every? topic? topics) (ordered-unique? topic-order topics)
                   (vector? carries) (<= (count carries) max-interests)
                   (every? topic? carries) (ordered-unique? topic-order carries)
                   (boolean? (get frame "relay_all")))
      (throw (ex-info "invalid or noncanonical portable interests"
                      {:type :netz.dissemination-wire/invalid-interests})))
    {:type :interests
     :topics (set topics)
     :carries (into (if (get frame "relay_all") #{topics/everything} #{})
                    (map vector carries))}))

(defn- stream-order [value]
  [(identity/bytes->hex (get value "origin"))
   (get value "epoch")
   (topic-order (get value "topic"))])

(defn- wire-stream? [value]
  (and (map? value)
       (= #{"origin" "epoch" "topic" "ranges" "floor"} (set (keys value)))
       (wire/byte-buffer? (get value "origin"))
       (= identity/peer-id-size (identity/buf-length (get value "origin")))
       (safe-uint? (get value "epoch"))
       (topic? (get value "topic"))
       (intervals? (get value "ranges"))
       (safe-uint? (get value "floor"))))

(defn- wire-have [frame]
  (let [streams (get frame "streams")]
    (when-not (and (= #{"v" "type" "streams"} (set (keys frame)))
                   (vector? streams) (<= (count streams) max-streams)
                   (every? wire-stream? streams)
                   (<= (reduce + (map #(count (get % "ranges")) streams))
                       max-total-intervals)
                   (ordered-unique? stream-order streams))
      (throw (ex-info "invalid or noncanonical portable have"
                      {:type :netz.dissemination-wire/invalid-have})))
    {:type :have
     :summary (reduce (fn [summary stream]
                        (assoc-in summary
                                  [(identity/bytes->hex (get stream "origin"))
                                   (get stream "epoch") (get stream "topic")]
                                  (get stream "ranges")))
                      {} streams)
     :horizon (into {}
                    (map (fn [stream]
                           [[(identity/bytes->hex (get stream "origin"))
                             (get stream "epoch") (get stream "topic")]
                            (get stream "floor")]))
                    streams)}))

(defn- wire-want-entry? [value]
  (and (map? value)
       (= #{"origin" "epoch" "topic" "lo" "hi"} (set (keys value)))
       (wire/byte-buffer? (get value "origin"))
       (= identity/peer-id-size (identity/buf-length (get value "origin")))
       (safe-uint? (get value "epoch")) (topic? (get value "topic"))
       (safe-uint? (get value "lo")) (safe-uint? (get value "hi"))
       (<= (get value "lo") (get value "hi"))))

(defn- wire-want-order [value]
  (conj (stream-order value) (get value "lo") (get value "hi")))

(defn- wire-want [frame]
  (let [ranges (get frame "ranges")]
    (when-not (and (= #{"v" "type" "ranges"} (set (keys frame)))
                   (vector? ranges) (<= (count ranges) max-want-ranges)
                   (every? wire-want-entry? ranges)
                   (ordered-unique? wire-want-order ranges))
      (throw (ex-info "invalid or noncanonical portable want"
                      {:type :netz.dissemination-wire/invalid-want})))
    {:type :want
     :ranges (mapv (fn [value]
                     {:origin (identity/bytes->hex (get value "origin"))
                      :epoch (get value "epoch") :topic (get value "topic")
                      :lo (get value "lo") :hi (get value "hi")})
                   ranges)}))

(defn frame-from-wire
  "Validate a decoded closed profile-1 map and return portable internal data."
  [frame]
  (when-not (and (map? frame) (= 1 (get frame "v")))
    (throw (ex-info "invalid portable dissemination frame"
                    {:type :netz.dissemination-wire/invalid-frame})))
  (case (get frame "type")
    "interests" (wire-interests frame)
    "have" (wire-have frame)
    "want" (wire-want frame)
    "gossip" (publication/from-wire frame)
    (throw (ex-info "unknown portable dissemination frame"
                    {:type :netz.dissemination-wire/unknown-frame
                     :frame-type (get frame "type")}))))

(defn encode-frame [frame]
  (wire/encode-canonical (frame->wire frame)))

(defn decode-frame [bytes]
  (frame-from-wire (wire/decode bytes)))
