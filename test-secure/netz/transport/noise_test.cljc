(ns netz.transport.noise-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer-macros [deftest is testing async]])
            [clojure.core.async :as a]
            [kabel.transport :as transport]
            [netz.identity :as identity]
            [netz.transport.noise :as secure]
            [netz.transport.secure :as secure-overlay]
            [netz.wire :as wire]))

(defn- error-value? [x]
  #?(:clj (instance? Throwable x) :cljs (instance? js/Error x)))

(defn- verify! [condition label]
  (when-not condition
    (throw (ex-info (str "secure transport test failed: " label)
                    {:check label})))
  true)

(defn- take-with-timeout [ch]
  (a/go
    (let [timer (a/timeout 5000)
          [value port] (a/alts! [ch timer])]
      (if (= port timer) ::timeout value))))

(defn- wait-for [pred]
  (a/go-loop [attempt 0]
    (cond
      (pred) true
      (= attempt 500) false
      :else (do (a/<! (a/timeout 10)) (recur (inc attempt))))))

(defn- pair
  ([initiator responder] (pair initiator responder {}))
  ([initiator responder {:keys [initiator-context responder-context
                                initiator-options responder-options
                                a->b b->a]
                         :or {initiator-context {}
                              responder-context {}
                              initiator-options {}
                              responder-options {}}}]
   (let [a->b (or a->b (a/chan 128))
         b->a (or b->a (a/chan 128))
         ic (transport/new-context :initiator initiator-context)
         rc (transport/new-context :responder responder-context)
         im (secure/middleware
             (merge {:identity initiator :addresses [] :seq-no 1}
                    initiator-options))
         rm (secure/middleware
             (merge {:identity responder :addresses [] :seq-no 1}
                    responder-options))
         [_ _ [i-in i-out]]
         (im (transport/with-context [::supervisor (atom {}) [b->a a->b]] ic))
         [_ _ [r-in r-out]]
         (rm (transport/with-context [::supervisor (atom {}) [a->b b->a]] rc))]
     {:initiator-context ic :responder-context rc
      :initiator-in i-in :initiator-out i-out
      :responder-in r-in :responder-out r-out
      :a->b a->b :b->a b->a})))

(defn- close-pair! [{:keys [initiator-out responder-out a->b b->a]}]
  (a/close! initiator-out)
  (a/close! responder-out)
  (a/close! a->b)
  (a/close! b->a))

(defn- envelope [n]
  {:kabel/serialization :cbor
   :kabel/payload (wire/encode-canonical {"v" 1 "type" "test" "n" n})})

(deftest portable-text-profile-is-unambiguous
  (is (fn? secure-overlay/start!))
  (testing "capability identifiers are ASCII protocol tokens"
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (secure/middleware
                  {:identity {} :addresses [] :seq-no 1
                   :capabilities #{"netz/1" "sync/🕸"}}))))
  (testing "signed transport addresses are normalized to printable ASCII"
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (secure/middleware
                  {:identity {} :addresses ["wss://münich.example/netz"]
                   :seq-no 1})))))

(defn- decoded-n [value]
  (get (wire/decode (:kabel/payload value)) "n"))

(defn- exercise-roundtrip-and-concurrent-send []
  (a/go
    (try
      (let [initiator (a/<! (identity/generate-identity))
            responder (a/<! (identity/generate-identity))
            expected (identity/authority-id (:genesis responder))
            connection (pair initiator responder
                             {:initiator-context
                              {::transport/expected-target expected}})
            ic (:initiator-context connection)
            rc (:responder-context connection)]
        (verify! (a/<! (wait-for
                        #(and (::transport/authenticated-authority @ic)
                              (::transport/authenticated-authority @rc))))
                 "both contexts authenticate")
        (verify! (= expected (::transport/authenticated-authority @ic))
                 "initiator authenticates the expected full authority")
        (verify! (= (identity/authority-id (:genesis initiator))
                    (::transport/authenticated-authority @rc))
                 "responder authenticates the initiator")
        (verify! (= secure/default-capabilities
                    (::transport/negotiated-capabilities @ic))
                 "capabilities are negotiated into Kabel context")
        (verify! (some? (::secure/authenticated-record @ic))
                 "verified Ed25519 identity record is retained")
        (verify! (some? (::secure/handshake-hash @ic))
                 "Noise channel binding is retained")

        ;; Concurrent callers only enqueue. One transport loop owns and
        ;; advances the Noise send CipherState for every envelope.
        (doseq [n (range 32)]
          (a/put! (:initiator-out connection) (envelope n)))
        (let [received
              (loop [values []]
                (if (= 32 (count values))
                  values
                  (let [value (a/<! (take-with-timeout
                                     (:responder-in connection)))]
                    (verify! (not= ::timeout value) "concurrent send completed")
                    (verify! (map? value)
                             (str "concurrent receive stayed open: "
                                  (::secure/error @rc)))
                    (recur (conj values (decoded-n value))))))]
          (verify! (= (set (range 32)) (set received))
                   "all concurrent envelopes decrypt once"))

        ;; One logical Kabel envelope may exceed Noise's 65535-byte message
        ;; bound; the binding fragments inside AEAD and reconstructs before
        ;; exposing anything to serialization.
        (let [large (identity/byte-buf 150000)]
          (a/>! (:initiator-out connection)
                {:kabel/serialization :cbor :kabel/payload large})
          (let [received (a/<! (take-with-timeout (:responder-in connection)))]
            (verify! (not= ::timeout received) "fragmented envelope completed")
            (verify! (identity/bufs= large (:kabel/payload received))
                     "fragmented envelope is byte-identical")))
        (close-pair! connection)
        true)
      (catch #?(:clj Throwable :cljs :default) e e))))

(deftest authenticated-roundtrip-fragmentation-and-concurrent-send
  #?(:clj (is (true? (a/<!! (exercise-roundtrip-and-concurrent-send))))
     :cljs (async done
                  (a/go
                    (is (true? (a/<! (exercise-roundtrip-and-concurrent-send))))
                    (done)))))

(defn- exercise-policy-failures []
  (a/go
    (try
      (let [initiator (a/<! (identity/generate-identity))
            responder (a/<! (identity/generate-identity))
            wrong (apply str (repeat 64 "0"))
            wrong-pair (pair initiator responder
                             {:initiator-context
                              {::transport/expected-target wrong}})
            wrong-context (:initiator-context wrong-pair)]
        (verify! (a/<! (wait-for #(some? (::secure/error @wrong-context))))
                 "wrong expected authority closes the handshake")
        (verify! (nil? (::transport/authenticated-authority @wrong-context))
                 "wrong authority is never published to context")
        (close-pair! wrong-pair)

        (let [raw-in (a/chan 1)
              raw-out (a/chan 1)
              context (transport/new-context :initiator)
              _ ((secure/middleware {:identity initiator
                                     :addresses [] :seq-no 1
                                     :handshake-timeout-ms 20})
                 (transport/with-context
                   [::supervisor (atom {}) [raw-in raw-out]] context))]
          (verify! (a/<! (wait-for #(some? (::secure/error @context))))
                   "an incomplete handshake times out")
          (verify! (= :netz.transport/handshake-timeout
                      (:type (ex-data (::secure/error @context))))
                   "timeout has a stable error type")
          (verify! (nil? (::transport/authenticated-authority @context))
                   "a timed-out operation cannot authenticate later"))

        (let [downgrade-pair
              (pair initiator responder
                    {:initiator-options
                     {:capabilities (conj secure/default-capabilities "sync/1")
                      :required (conj secure/default-required "sync/1")}})
              ic (:initiator-context downgrade-pair)
              rc (:responder-context downgrade-pair)]
          (verify! (a/<! (wait-for #(or (::secure/error @ic)
                                        (::secure/error @rc))))
                   "missing required capability aborts rather than downgrades")
          (verify! (and (nil? (::transport/authenticated-authority @ic))
                        (nil? (::transport/authenticated-authority @rc)))
                   "downgraded connection is never registered")
          (close-pair! downgrade-pair))
        true)
      (catch #?(:clj Throwable :cljs :default) e e))))

(deftest wrong-target-and-capability-downgrade-are-rejected
  #?(:clj (is (true? (a/<!! (exercise-policy-failures))))
     :cljs (async done
                  (a/go
                    (is (true? (a/<! (exercise-policy-failures))))
                    (done)))))

(defn- flip-last [bytes]
  #?(:clj (let [copy (java.util.Arrays/copyOf ^bytes bytes (alength ^bytes bytes))
                i (dec (alength ^bytes copy))]
            (aset copy i (unchecked-byte (bit-xor 1 (aget copy i))))
            copy)
     :cljs (let [copy (.slice bytes 0)
                 i (dec (.-byteLength copy))]
             (aset copy i (bit-xor 1 (aget copy i)))
             copy)))

(defn- byte-at [bytes index]
  (bit-and 0xff #?(:clj (aget ^bytes bytes (int index))
                   :cljs (aget bytes index))))

(defn- proxy-a->b [source target transform]
  (a/go-loop [transport-count 0]
    (if-let [frame (a/<! source)]
      (let [payload (:kabel/payload frame)
            kind (when (and payload (pos? (identity/buf-length payload)))
                   (byte-at payload 0))
            transport? (= 2 kind)
            index (if transport? transport-count nil)
            frame' (transform frame index)]
        (when frame' (a/>! target frame'))
        (recur (if transport? (inc transport-count) transport-count)))
      (a/close! target))))

(defn- exercise-tamper-and-replay []
  (a/go
    (try
      (let [initiator (a/<! (identity/generate-identity))
            responder (a/<! (identity/generate-identity))
            wire-out (a/chan 128)
            wire-in (a/chan 128)
            back (a/chan 128)
            ic (transport/new-context :initiator)
            rc (transport/new-context :responder)
            [_ _ [i-in i-out]]
            ((secure/middleware {:identity initiator :addresses [] :seq-no 1})
             (transport/with-context [::s (atom {}) [back wire-out]] ic))
            [_ _ [r-in r-out]]
            ((secure/middleware {:identity responder :addresses [] :seq-no 1})
             (transport/with-context [::s (atom {}) [wire-in back]] rc))]
        (proxy-a->b wire-out wire-in
                    (fn [frame index]
                      (if (= 0 index)
                        (update frame :kabel/payload flip-last)
                        frame)))
        (verify! (a/<! (wait-for #(and (::transport/authenticated-authority @ic)
                                       (::transport/authenticated-authority @rc))))
                 "tamper pair authenticated before transport data")
        (a/>! i-out (envelope 1))
        (verify! (a/<! (wait-for #(some? (::secure/error @rc))))
                 "tampered transport ciphertext is rejected")
        (verify! (nil? (a/<! (take-with-timeout r-in)))
                 "tampered plaintext is never exposed")
        (a/close! i-out) (a/close! r-out)

        (let [wire-out (a/chan 128)
              wire-in (a/chan 128)
              back (a/chan 128)
              ic (transport/new-context :initiator)
              rc (transport/new-context :responder)
              [_ _ [_ i-out]]
              ((secure/middleware {:identity initiator :addresses [] :seq-no 2})
               (transport/with-context [::s (atom {}) [back wire-out]] ic))
              [_ _ [r-in r-out]]
              ((secure/middleware {:identity responder :addresses [] :seq-no 2})
               (transport/with-context [::s (atom {}) [wire-in back]] rc))]
          (proxy-a->b wire-out wire-in
                      (fn [frame index]
                        (when (= 0 index)
                          ;; Ordered duplicate: first decrypts at nonce N, the
                          ;; second is checked at N+1 and must fail.
                          (a/put! wire-in frame))
                        frame))
          (verify! (a/<! (wait-for #(and (::transport/authenticated-authority @ic)
                                         (::transport/authenticated-authority @rc))))
                   "replay pair authenticated")
          (a/>! i-out (envelope 7))
          (let [first-value (a/<! (take-with-timeout r-in))]
            (verify! (map? first-value)
                     (str "original ciphertext arrived: " (::secure/error @rc)))
            (verify! (= 7 (decoded-n first-value))
                     "original ciphertext is delivered"))
          (verify! (a/<! (wait-for #(some? (::secure/error @rc))))
                   "replayed ciphertext is rejected at the next nonce")
          (a/close! i-out) (a/close! r-out)))
      true
      (catch #?(:clj Throwable :cljs :default) e e))))

(deftest transport-tamper-and-replay-are-rejected
  #?(:clj (is (true? (a/<!! (exercise-tamper-and-replay))))
     :cljs (async done
                  (a/go
                    (is (true? (a/<! (exercise-tamper-and-replay))))
                    (done)))))
