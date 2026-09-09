(ns netz.overlay.runtime-test
  "Unit tests for the overlay runtime's action interpreter and event loop.

  Effects are injected, so these run without opening a socket. The real-wire
  behaviour is covered by `netz.overlay.integration-test`."
  (:require [boring.core :as boring]
            [clojure.test :refer [deftest testing is are]]
            [netz.dissemination-adapter :as dissemination-adapter]
            [netz.dissemination-wire :as dissemination-wire]
            [netz.identity :as id]
            [netz.overlay.runtime :as rt]
            [netz.sync :as sync]
            [netz.wire :as wire]
            #?@(:clj [[netz.dissemination :as d]
                      [superv.async :refer [S <??]]
                      [clojure.core.async :as async
                       :refer [alts!! chan <!! >!! timeout close!]]])))

(defn- recording-effects
  "Effects that record instead of acting."
  [log]
  {:send! (fn [to m] (swap! log conj [:send to m]))
   :connect! (fn [to address m] (swap! log conj [:connect to address m]))
   :disconnect! (fn [to] (swap! log conj [:disconnect to]))
   :schedule! (fn [delay payload] (swap! log conj [:timer delay payload]))})

(defn- runtime-with
  "A runtime whose handler emits `actions` for any event."
  [actions log]
  (rt/make-runtime {:id :me
                    :state {:seen []}
                    :handler (fn [state event _ctx]
                               {:state (update state :seen conj (:type event))
                                :actions actions})
                    :effects (recording-effects log)
                    :now-fn (constantly 1000)}))

(deftest actions-become-effects
  (testing ":send is wrapped in an overlay frame"
    ;; The wrapping is what lets overlay traffic share a wire with everything
    ;; else a peer is doing; an unwrapped payload would be indistinguishable
    ;; from an application message.
    (let [log (atom [])
          ctx (runtime-with [[:send :p1 {:type :dial}]] log)]
      (rt/step! ctx {:type :init})
      (is (= [[:send :p1 {:type rt/frame-type :payload {:type :dial}}]] @log))))

  (testing ":connect carries the address and the first frame"
    (let [log (atom [])
          ctx (runtime-with [[:connect :p1 "ws://p1" {:type :dial}]] log)]
      (rt/step! ctx {:type :init})
      (is (= [[:connect :p1 "ws://p1"
               {:type rt/frame-type :payload {:type :dial}}]]
             @log))))

  (testing ":timer and :disconnect pass straight through"
    (let [log (atom [])
          ctx (runtime-with [[:timer 500 :tick] [:disconnect :p2]] log)]
      (rt/step! ctx {:type :init})
      (is (= [[:timer 500 :tick] [:disconnect :p2]] @log))))

  (testing "an unknown action is refused rather than ignored"
    ;; A typo in an action name must not be a silent no-op — that is how a
    ;; defence ends up declared and inert.
    (let [log (atom [])
          ctx (runtime-with [[:sned :p1 "oops"]] log)]
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (rt/step! ctx {:type :init}))))))

(deftest state-advances-per-event
  (testing "the machine's returned state is retained between events"
    (let [log (atom [])
          ctx (runtime-with [] log)]
      (rt/step! ctx {:type :init})
      (rt/step! ctx {:type :timer :payload :tick})
      (is (= [:init :timer] (:seen @(:state ctx))))))

  (testing "a handler returning no state leaves the old state in place"
    (let [ctx (rt/make-runtime {:id :me
                                :state {:kept true}
                                :handler (fn [_ _ _] {:state nil :actions []})
                                :effects (recording-effects (atom []))})]
      (rt/step! ctx {:type :init})
      (is (= {:kept true} @(:state ctx))))))

(deftest websocket-address-policy
  (testing "only absolute credential-free WebSocket URLs pass by default"
    (is (rt/websocket-address? "ws://localhost:8080/netz"))
    (is (rt/websocket-address? "wss://peer.example/netz"))
    (is (not (rt/websocket-address? "http://peer.example/netz")))
    (is (not (rt/websocket-address? "ws://user:secret@peer.example/netz")))
    (is (not (rt/websocket-address? "ws://peer.example/netz#fragment")))
    (is (not (rt/websocket-address? "not a url")))))

(deftest configured-addresses-cannot-be-rebound-by-peer-exchange
  (let [alice :alice
        bob :bob
        allow? (rt/configured-address-policy
                [{:peer-id alice :addresses ["wss://alice.example/netz"]}
                 {:peer-id bob :addresses ["wss://bob.example/netz"]}])]
    (is (allow? {:peer-id alice :address "wss://alice.example/netz"}))
    (is (allow? {:peer-id bob :address "wss://bob.example/netz"}))
    (is (not (allow? {:peer-id alice :address "wss://bob.example/netz"})))
    (is (not (allow? {:peer-id alice :address "ws://127.0.0.1:8080/admin"})))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (rt/configured-address-policy
                  [{:peer-id alice :addresses ["https://not-websocket.example"]}])))))

(deftest public-gateway-dials-only-configured-tls-peers
  (let [alice :alice
        allow? (rt/public-gateway-address-policy
                [{:peer-id alice :addresses ["wss://alice.example/netz"]}])]
    (is (allow? {:peer-id alice :address "wss://alice.example/netz"}))
    (is (not (allow? {:peer-id alice :address "wss://elsewhere.example/netz"})))
    (is (not (allow? {:peer-id :untrusted
                      :address "wss://alice.example/netz"})))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (rt/public-gateway-address-policy
                  [{:peer-id alice :addresses ["ws://alice.example/netz"]}])))))

(deftest stopping-a-runtime-is-idempotent-and-refuses-new-work
  (let [ctx (rt/make-runtime {:id :me :state {}
                              :handler (fn [s _ _] {:state s :actions []})
                              :effects (recording-effects (atom []))})
        cleaned (atom 0)
        running {:ctx ctx :peer (atom {})}]
    (rt/on-stop! running #(swap! cleaned inc))
    (is (true? (rt/stop! running)))
    (is (= 1 @cleaned))
    (is (false? (rt/submit! ctx {:type :init})))
    (is (nil? (rt/stop! running)))
    (is (= 1 @cleaned))))

(deftest extensions-are-explicitly-registered-and-capability-gated
  (let [log (atom [])
        ctx (runtime-with [] log)
        peer (atom {})
        node {:ctx ctx :peer peer}
        handler {:capability "example/1"
                 :match? #(= "example" (get % "type"))
                 :valid? map?
                 :receive! (fn [_ _ _] true)}
        cleanup (rt/register-extension! node :example handler)]
    (testing "a local handler cannot send before remote negotiation"
      (is (false? (rt/send-extension!
                   node :remote "example/1" {"type" "example"})))
      (is (empty? @log)))

    ;; Public registry access initializes the per-peer connection registry.
    (rt/connections peer)
    (swap! (:netz.overlay/connections @peer)
           assoc-in [:facts :remote :capabilities] #{"example/1"})

    (testing "a negotiated extension shares the normal framed send path"
      (is (true? (rt/send-extension!
                  node :remote "example/1" {"type" "example"})))
      (is (= [[:send :remote
               {:type rt/frame-type :payload {"type" "example"}}]]
             @log)))

    (testing "wire profile alone preserves the transitional native carrier"
      (swap! (:netz.overlay/connections @peer)
             update-in [:facts :remote :capabilities]
             conj wire/profile-capability)
      (reset! log [])
      (is (true? (rt/send-extension!
                  node :remote "example/1" {"type" "example"})))
      (is (= rt/frame-type (:type (nth (first @log) 2)))))

    (testing "dedicated carrier negotiation selects the portable envelope"
      (swap! (:netz.overlay/connections @peer)
             update-in [:facts :remote :capabilities]
             conj rt/extension-carrier-capability)
      (reset! log [])
      (is (true? (rt/send-extension!
                  node :remote "example/1" {"type" "example"})))
      (let [[_ _ carrier] (first @log)
            decoded (rt/decode-extension-envelope (second carrier))]
        (is (rt/extension-carrier? carrier))
        (is (= "example/1" (:capability decoded)))
        (is (= {"type" "example"} (:payload decoded)))))

    (testing "registration and cleanup are unambiguous and idempotent"
      (is (thrown? #?(:clj Throwable :cljs :default)
                   (rt/register-extension! node :example handler)))
      (is (true? (cleanup)))
      (is (nil? (cleanup)))
      (let [cleanup-again (rt/register-extension! node :example handler)]
        (is (true? (cleanup-again)))))

    (testing "the built-in portable sync capability cannot be shadowed"
      (is (thrown-with-msg?
           #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
           #"reserved"
           (rt/register-extension!
            node :fake-sync (assoc handler :capability "netz/sync/1")))))

    (testing "the generic sender cannot bypass the built-in sync machine"
      (swap! (:netz.overlay/connections @peer)
             update-in [:facts :remote :capabilities]
             conj "netz/sync/1")
      (reset! log [])
      (is (false? (rt/send-extension!
                   node :remote "netz/sync/1" {"type" "sync/open"})))
      (is (empty? @log)))

    (testing "extensions use the overlay's ordinary disconnect effect"
      (rt/disconnect-peer! node :remote)
      (is (= [:disconnect :remote] (last @log))))))

#?(:clj
   (deftest synchronization-carrier-is-pinned-before-queueing
     (let [ctx (runtime-with [] (atom []))
           peer (atom {})
           node {:ctx ctx :peer peer}
           capabilities #{wire/profile-capability
                          rt/extension-carrier-capability
                          "netz/sync/1"}]
       (testing "a disconnected request is refused instead of queued"
         (is (false? (rt/start-sync! node :remote "doc" "example/sync/1"))))

       (rt/connections peer)
       (swap! (:netz.overlay/connections @peer)
              assoc-in [:facts :remote]
              {:authenticated? true :capabilities capabilities})
       (testing "the live negotiation selects and pins portable mode"
         (is (true? (rt/start-sync! node :remote "doc" "example/sync/1")))
         (let [event (<!! (:events ctx))
               payload (:payload event)]
           (is (= :portable (sync/transport-mode payload)))
           (is (= 16 (id/buf-length (:session payload))))
           (swap! (:netz.overlay/connections @peer)
                  assoc-in [:facts :remote :capabilities]
                  #{wire/profile-capability "netz/sync/1"})
           (is (= :portable (sync/transport-mode payload))
               "later capability changes cannot reinterpret the session")))

       (testing "portable mode rejects a legacy caller-supplied session"
         (swap! (:netz.overlay/connections @peer)
                assoc-in [:facts :remote :capabilities]
                capabilities)
         (is (false? (rt/start-sync! node :remote "doc" "example/sync/1"
                                     :legacy)))))))

#?(:clj
   (deftest outbound-sync-refuses-a-reconnected-carrier-change
     (let [ctx (runtime-with [] (atom []))
           peer (atom {})
           out (chan 1)
           capabilities #{wire/profile-capability
                          rt/extension-carrier-capability
                          "netz/sync/1"}
           effects (rt/kabel-effects S peer ctx nil nil {})
           session (id/seq->bytes (range 16))]
       (rt/connections peer)
       (swap! (:netz.overlay/connections @peer)
              (fn [registry]
                (-> registry
                    (assoc-in [:by-id :remote] out)
                    (assoc-in [:facts :remote]
                              {:authenticated? true
                               :capabilities capabilities}))))
       ((:send! effects)
        :remote
        (rt/frame
         (sync/with-transport-mode
           {:type :sync/open :session session :topic "doc"
            :profile "example/sync/1" :summary (id/seq->bytes [])}
           :portable)))
       (is (rt/extension-carrier? (<!! out)))

       ;; The session remains portable even if the same routing id reconnects
       ;; with a different selection. Its next frame closes instead of being
       ;; emitted through the legacy grammar.
       (swap! (:netz.overlay/connections @peer)
              assoc-in [:facts :remote :capabilities]
              #{wire/profile-capability "netz/sync/1"})
       ((:send! effects)
        :remote
        (rt/frame
         (sync/with-transport-mode
           {:type :sync/ack :session session :through 0}
           :portable)))
       (is (nil? (<!! out))))))

#?(:clj
   (deftest outbound-dissemination-selects-the-negotiated-portable-carrier
     (let [identity (<!! (id/generate-identity))
           local-id (id/peer-id (:genesis identity))
           authority (id/authority-id (:genesis identity))
           state (d/make-state local-id #{"db/roots"}
                               {:authority-id authority})
           remote-authority (apply str (repeat 64 "b"))
           seen-policy (atom nil)
           ctx (rt/make-runtime
                {:id local-id :state {:dissemination state}
                 :handler (fn [s _ _] {:state s :actions []})
                 :effects nil
                 :authorize-dissemination?
                 (fn [context] (reset! seen-policy context) true)})
           peer (atom {})
           out (chan 1)
           capabilities #{wire/profile-capability
                          rt/extension-carrier-capability
                          dissemination-wire/capability}
           effects (rt/kabel-effects S peer ctx identity nil {})
           unsigned {:type :gossip :topic "db/roots" :origin local-id
                     :epoch 0 :seq 0 :hops 0
                     :payload (id/seq->bytes [1 2 3])}
           signed (<!! (rt/sign-gossip S identity unsigned))]
       (rt/connections peer)
       (swap! (:netz.overlay/connections @peer)
              (fn [registry]
                (-> registry
                    (assoc-in [:by-id :remote] out)
                    (assoc-in [:facts :remote]
                              {:authenticated? true
                               :authority-id remote-authority
                               :capabilities capabilities}))))
       ((:send! effects) :remote (rt/frame signed))
       (let [carrier (<!! out)
             envelope (rt/decode-extension-envelope (second carrier))
             restored (dissemination-adapter/from-wire
                       (d/make-state :receiver) (:payload envelope))]
         (is (rt/extension-carrier? carrier))
         (is (= dissemination-wire/capability (:capability envelope)))
         (is (= "db/roots" (:topic restored)))
         (is (= authority (:origin-authority restored)))
         (is (= {:op :publish :direction :outbound :peer-id :remote
                 :authority-id remote-authority
                 :origin-authority authority :topic "db/roots"}
                (select-keys @seen-policy
                             [:op :direction :peer-id :authority-id
                              :origin-authority :topic])))
         (is (true? (<!! (rt/verify-gossip S restored))))))))

#?(:clj
   (deftest portable-dissemination-awaits-policy-and-fails-closed
     (let [local (<!! (id/generate-identity))
           remote (<!! (id/generate-identity))
           remote-record (<!! (id/sign-record remote [] 1))
           remote-id (id/peer-id (:genesis remote))
           remote-authority (id/authority-id (:genesis remote))
           capabilities #{wire/profile-capability
                          rt/extension-carrier-capability
                          dissemination-wire/capability}
           authenticated-context
           (atom {:netz.transport.noise/authenticated-record remote-record
                  :netz.transport.noise/authenticated-routing-id remote-id
                  :kabel.transport/authenticated-authority remote-authority
                  :kabel.transport/negotiated-capabilities capabilities})
           unsigned {:type :gossip :topic "db/roots" :origin remote-id
                     :epoch 0 :seq 0 :hops 0
                     :payload (id/seq->bytes [4 5 6])}
           signed (<!! (rt/sign-gossip S remote unsigned))
           carrier (rt/extension-carrier
                    dissemination-wire/capability
                    (dissemination-wire/frame->wire signed))]
       (testing "an asynchronous allow decision preserves order into the machine"
         (let [seen (atom nil)
               ctx (runtime-with [] (atom []))
               peer (atom {})
               in (chan)
               out (chan)]
           ((rt/middleware
             {:ctx ctx :identity local :addresses [] :seq-no 1
              :authenticated-context-fn (constantly authenticated-context)
              :authorize-dissemination?
              (fn [context]
                (reset! seen context)
                (async/go (async/<! (timeout 10)) true))})
            [S peer [in out]])
           (>!! in carrier)
           (is (= {:type :dissemination-connected :peer remote-id}
                  (<!! (:events ctx))))
           (let [[event port] (alts!! [(:events ctx) (timeout 1000)])]
             (is (= (:events ctx) port))
             (is (= :message (:type event)))
             (is (= "db/roots" (get-in event [:payload :topic])))
             (is (= {:op :publish :direction :inbound
                     :frame-type :gossip :authority-id remote-authority
                     :origin-authority remote-authority
                     :topic "db/roots"}
                    (select-keys @seen
                                 [:op :direction :frame-type
                                  :authority-id :origin-authority :topic]))))
           (close! in)
           (close! out)))

       (testing "a policy that never settles is bounded and denies"
         (let [ctx (runtime-with [] (atom []))
               peer (atom {})
               in (chan)
               out (chan)]
           ((rt/middleware
             {:ctx ctx :identity local :addresses [] :seq-no 1
              :authenticated-context-fn (constantly authenticated-context)
              :authorize-dissemination? (fn [_] (chan))
              :authorization-timeout-ms 20})
            [S peer [in out]])
           (>!! in carrier)
           (is (= {:type :dissemination-connected :peer remote-id}
                  (<!! (:events ctx))))
           (let [[value port] (alts!! [out (timeout 1000)])]
             (is (= out port))
             (is (nil? value)))
           (is (nil? (async/poll! (:events ctx))))
           (close! in))))))

#?(:clj
   (deftest noise-registration-is-single-owner-before-inbound-delivery
     (let [local (<!! (id/generate-identity))
           remote (<!! (id/generate-identity))
           record (<!! (id/sign-record remote [] 1))
           remote-id (id/peer-id (:genesis remote))
           authority (id/authority-id (:genesis remote))
           capabilities #{wire/profile-capability rt/extension-carrier-capability
                          dissemination-wire/capability}
           facts (atom {:netz.transport.noise/authenticated-record record
                        :netz.transport.noise/authenticated-routing-id remote-id
                        :kabel.transport/authenticated-authority authority
                        :kabel.transport/negotiated-capabilities capabilities})
           signed (<!! (rt/sign-gossip S remote
                                       {:type :gossip :topic "db/roots" :origin remote-id
                                        :epoch 0 :seq 0 :hops 0 :payload (id/seq->bytes [7])}))
           carrier (rt/extension-carrier dissemination-wire/capability
                                         (dissemination-wire/frame->wire signed))
           ctx (runtime-with [] (atom []))
           peer (atom {}) in (chan) out (chan)
           entered (promise) release (promise) calls (atom 0)
           original @#'rt/register!]
       (with-redefs-fn
         {#'rt/register!
          (fn [& args]
            (swap! calls inc)
            (deliver entered true)
            (when-not (deref release 5000 false)
              (throw (ex-info "Test registration latch timed out" {})))
            (apply original args))}
         (fn []
           (try
             ((rt/middleware {:ctx ctx :identity local :addresses [] :seq-no 1
                              :authenticated-context-fn (constantly facts)
                              :authorize-dissemination? (constantly true)})
              [S peer [in out]])
             (is (true? (deref entered 2000 false)) "Poller must enter registration first")
             (is (= [true in] (alts!! [[in carrier] (timeout 2000)]))
                 "Inbound loop must take the frame while registration is held")
             ;; Keep the first completion in-flight while the frame loop races it.
             (<!! (timeout 100))
             (is (= 1 @calls) "Only one completion may enter registration")
             (is (nil? (async/poll! (:events ctx)))
                 "No event may precede registration completion")
             (deliver release true)
             (let [[connected _] (alts!! [(:events ctx) (timeout 2000)])
                   [message _] (alts!! [(:events ctx) (timeout 2000)])]
               (is (= {:type :dissemination-connected :peer remote-id} connected))
               (is (= :message (:type message)))
               (is (= "db/roots" (get-in message [:payload :topic])))
               (is (= 1 @calls)))
             (finally
               (deliver release true)
               (close! in)
               (close! out))))))))

(def extension-fixture
  {"v" 1
   "type" "blob/need"
   "session" (id/hex->bytes "000102030405060708090a0b0c0d0e0f")
   "attempt" 0
   "need" (id/hex->bytes "05")})

(def extension-envelope-hex
  "a461760164747970656e6e65747a2f657874656e73696f6e677061796c6f6164a5617601646e6565644105647479706569626c6f622f6e65656467617474656d7074006773657373696f6e50000102030405060708090a0b0c0d0e0f6a6361706162696c697479776e65747a2f626c6f622d7265706c69636174696f6e2f31")

(def extension-carrier-hex
  "82756e65747a2f657874656e73696f6e2d63626f722f31587fa461760164747970656e6e65747a2f657874656e73696f6e677061796c6f6164a5617601646e6565644105647479706569626c6f622f6e65656467617474656d7074006773657373696f6e50000102030405060708090a0b0c0d0e0f6a6361706162696c697479776e65747a2f626c6f622d7265706c69636174696f6e2f31")

(def extension-carrier-indefinite-hex
  "9f7f756e65747a2f657874656e73696f6e2d63626f722f31ff5f587fa461760164747970656e6e65747a2f657874656e73696f6e677061796c6f6164a5617601646e6565644105647479706569626c6f622f6e65656467617474656d7074006773657373696f6e50000102030405060708090a0b0c0d0e0f6a6361706162696c697479776e65747a2f626c6f622d7265706c69636174696f6e2f31ffff")

(deftest portable-extension-envelope-known-answer
  (let [capability "netz/blob-replication/1"
        encoded (rt/encode-extension-envelope capability extension-fixture)
        carrier (rt/extension-carrier capability extension-fixture)
        decoded (rt/decode-extension-envelope encoded)]
    (is (= extension-envelope-hex (id/bytes->hex encoded)))
    (is (= extension-carrier-hex
           (id/bytes->hex (wire/encode-canonical carrier))))
    (is (= capability (:capability decoded)))
    (is (= (dissoc extension-fixture "session" "need")
           (dissoc (:payload decoded) "session" "need")))
    (is (id/bufs= (get extension-fixture "session")
                  (get-in decoded [:payload "session"])))
    (is (id/bufs= (get extension-fixture "need")
                  (get-in decoded [:payload "need"]))))

  (testing "the portable boundary is closed and capability names are ASCII"
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (rt/encode-extension-envelope "example/é" {})))
    (is (thrown? #?(:clj Throwable :cljs :default)
                 (rt/decode-extension-envelope
                  (wire/encode-canonical
                   {"v" 1 "type" rt/extension-envelope-type
                    "capability" "example/1" "payload" {}
                    "extension" true}))))))

(deftest portable-extension-outer-cbor-is-a-semantic-carrier
  (testing "JVM and JavaScript accept preferred, non-minimal and indefinite forms"
    (doseq [hex [extension-carrier-hex
                 (str "9802" (subs extension-carrier-hex 2))
                 extension-carrier-indefinite-hex]]
      (let [carrier (boring/decode (id/hex->bytes hex) {:stringref false})
            decoded (rt/decode-extension-envelope (second carrier))]
        (is (rt/extension-carrier? carrier))
        (is (= "netz/blob-replication/1" (:capability decoded)))))))

#?(:clj
   (defn- assert-portable-extension-closes!
     [handler capabilities carrier]
     (let [local (<!! (id/generate-identity))
           remote (<!! (id/generate-identity))
           remote-record (<!! (id/sign-record remote [] 1))
           remote-id (id/peer-id (:genesis remote))
           authenticated-context
           (atom {:netz.transport.noise/authenticated-record remote-record
                  :netz.transport.noise/authenticated-routing-id remote-id
                  :kabel.transport/authenticated-authority
                  (id/authority-id (:genesis remote))
                  :kabel.transport/negotiated-capabilities capabilities})
           ctx (runtime-with [] (atom []))
           peer (atom {})
           in (chan)
           out (chan)
           node {:ctx ctx :peer peer}]
       (rt/register-extension! node :example handler)
       ((rt/middleware
         {:ctx ctx :identity local :addresses [] :seq-no 1
          :authenticated-context-fn (constantly authenticated-context)})
        [S peer [in out]])
       (>!! in carrier)
       (let [[value port] (alts!! [out (timeout 1000)])]
         (is (= out port) "hostile portable carrier did not close its connection")
         (is (nil? value))))))

#?(:clj
   (defn- assert-native-sync-admitted! [capabilities payload]
     (let [local (<!! (id/generate-identity))
           remote (<!! (id/generate-identity))
           remote-record (<!! (id/sign-record remote [] 1))
           remote-id (id/peer-id (:genesis remote))
           authenticated-context
           (atom {:netz.transport.noise/authenticated-record remote-record
                  :netz.transport.noise/authenticated-routing-id remote-id
                  :kabel.transport/authenticated-authority
                  (id/authority-id (:genesis remote))
                  :kabel.transport/negotiated-capabilities capabilities})
           ctx (runtime-with [] (atom []))
           peer (atom {})
           in (chan)
           out (chan)]
       ((rt/middleware
         {:ctx ctx :identity local :addresses [] :seq-no 1
          :authenticated-context-fn (constantly authenticated-context)})
        [S peer [in out]])
       ;; Allow the authenticated transport facts to be registered before the
       ;; test frame enters the data plane.
       (loop [attempt 0]
         (when (and (< attempt 200)
                    (nil? (rt/connection-info peer remote-id)))
           (Thread/sleep 5)
           (recur (inc attempt))))
       (is (some? (rt/connection-info peer remote-id)))
       (>!! in {:type rt/frame-type :payload payload})
       (let [[event port] (alts!! [(:events ctx) (timeout 1000)])]
         (is (= (:events ctx) port) "legacy native sync was not admitted")
         (is (= :native
                (sync/transport-mode (:payload event))))))))

#?(:clj
   (deftest public-start-defaults-portable-sync-authorization-to-deny
     (let [local (<!! (id/generate-identity))
           remote (<!! (id/generate-identity))
           remote-record (<!! (id/sign-record remote [] 1))
           remote-id (id/peer-id (:genesis remote))
           capabilities #{wire/profile-capability
                          rt/extension-carrier-capability
                          "netz/sync/1"}
           authenticated-context
           (atom {:netz.transport.noise/authenticated-record remote-record
                  :netz.transport.noise/authenticated-routing-id remote-id
                  :kabel.transport/authenticated-authority
                  (id/authority-id (:genesis remote))
                  :kabel.transport/negotiated-capabilities capabilities})
           peer (atom {})
           node (<!! (rt/start!
                      S peer
                      {:identity local :addresses []
                       :authenticated-context-fn
                       (constantly authenticated-context)}))
           in (chan)
           out (chan)]
       (try
         ((:middleware node) [S peer [in out]])
         (loop [attempt 0]
           (when (and (< attempt 200)
                      (nil? (rt/connection-info peer remote-id)))
             (Thread/sleep 5)
             (recur (inc attempt))))
         (>!! in
              (rt/extension-carrier
               "netz/sync/1"
               (wire/sync-frame->wire
                {:type :sync/open
                 :session (id/seq->bytes (range 16))
                 :topic "private/document"
                 :profile "example/sync/1"
                 :summary (id/seq->bytes [])})))
         (let [[value port] (alts!! [out (timeout 1000)])]
           (is (= out port))
           (is (nil? value))
           (is (zero? @(:dropped (:ctx node)))
               "default denial must not be an exception path"))
         (finally
           (rt/stop! node))))))

#?(:clj
   (deftest hostile-portable-extension-input-fails-closed
     (let [base-capabilities
           #{wire/profile-capability rt/extension-carrier-capability}
           payload {"type" "example"}
           normal-handler
           {:capability "example/1"
            :match? #(= "example" (get % "type"))
            :valid? map?
            :receive! (fn [_ _ _] true)}]
       (testing "the named application capability is independently required"
         (assert-portable-extension-closes!
          normal-handler base-capabilities
          (rt/extension-carrier "example/1" payload)))

       (testing "the reserved marker never falls through as application data"
         (assert-portable-extension-closes!
          normal-handler #{}
          (rt/extension-carrier "example/1" payload)))

       (testing "portable dissemination is deny-by-default"
         (assert-portable-extension-closes!
          normal-handler
          (conj base-capabilities dissemination-wire/capability)
          (rt/extension-carrier
           dissemination-wire/capability
           (dissemination-wire/frame->wire
            {:type :interests :topics #{"db/roots"}
             :carries #{["db/roots"]}}))))

       (testing "negotiated portable dissemination rejects the native downgrade"
         (assert-portable-extension-closes!
          normal-handler
          (conj base-capabilities dissemination-wire/capability)
          {:type rt/frame-type
           :payload {:type :interests :topics #{"db/roots"}
                     :carries #{["db/roots"]}}}))

       (testing "unknown named handlers are rejected"
         (assert-portable-extension-closes!
          normal-handler (conj base-capabilities "unknown/1")
          (rt/extension-carrier "unknown/1" {"type" "unknown"})))

       (testing "malformed deterministic inner envelopes are rejected"
         (assert-portable-extension-closes!
          normal-handler (conj base-capabilities "example/1")
          [rt/extension-carrier-type (id/hex->bytes "00")]))

       (testing "malformed negotiated portable sync closes the connection"
         (assert-portable-extension-closes!
          normal-handler (conj base-capabilities "netz/sync/1")
          (rt/extension-carrier "netz/sync/1"
                                {"v" 1 "type" "sync/ack"
                                 "session" (id/seq->bytes (range 16))
                                 "through" -1})))

       (testing "portable sync reads are denied unless explicitly authorized"
         (assert-portable-extension-closes!
          normal-handler (conj base-capabilities "netz/sync/1")
          (rt/extension-carrier
           "netz/sync/1"
           (wire/sync-frame->wire
            {:type :sync/open
             :session (id/seq->bytes (range 16))
             :topic "private/document"
             :profile "example/sync/1"
             :summary (id/seq->bytes [])}))))

       (testing "negotiated portable sync rejects the native schema downgrade"
         (assert-portable-extension-closes!
          normal-handler (conj base-capabilities "netz/sync/1")
          {:type rt/frame-type
           :payload {:type :sync/open :session :legacy
                     :topic :doc :profile "example/sync/1"
                     :summary #{}}}))

       (testing "a peer missing a portable capability retains native sync"
         (assert-native-sync-admitted!
          #{wire/profile-capability "netz/sync/1"}
          {:type :sync/open :session :legacy :topic :doc
           :profile "example/sync/1" :summary #{}}))

       (testing "malformed reserved outer carrier shapes are rejected"
         (assert-portable-extension-closes!
          normal-handler (conj base-capabilities "example/1")
          [rt/extension-carrier-type]))

       (testing "a handler refusal closes the connection"
         (assert-portable-extension-closes!
          (assoc normal-handler :receive! (fn [_ _ _] false))
          (conj base-capabilities "example/1")
          (rt/extension-carrier "example/1" payload)))

       (testing "extension predicate exceptions close instead of looping"
         (assert-portable-extension-closes!
          (assoc normal-handler :match? (fn [_] (throw (ex-info "boom" {}))))
          (conj base-capabilities "example/1")
          (rt/extension-carrier "example/1" payload))))))

#?(:clj
   (deftest publisher-authentication
     (let [kp (<!! (id/generate-identity))
           peer-id (id/peer-id (:genesis kp))
           msg {:type :gossip :topic :db/roots :origin peer-id
                :epoch 7 :seq 3 :hops 0 :payload {:root "abc"}}
           signed (<!! (rt/sign-gossip S kp msg))]

       (testing "a genuine publish verifies"
         (is (d/signed? signed))
         (is (true? (<!! (rt/verify-gossip S signed)))))

       (testing "an unsigned publish does not"
         (is (false? (<!! (rt/verify-gossip S msg)))))

       (testing "altering the payload breaks it"
         ;; The point of the whole exercise for db roots: a relay must not be
         ;; able to substitute a different root.
         (is (false? (<!! (rt/verify-gossip S (assoc signed :payload {:root "evil"}))))))

       (testing "altering the topic breaks it"
         (is (false? (<!! (rt/verify-gossip S (assoc signed :topic :other))))))

       (testing "replaying it at a different sequence number breaks it"
         ;; Without `seq` in the signed bytes, a relay could replay an old
         ;; publish as a newer one — which for a database root is a rollback.
         (is (false? (<!! (rt/verify-gossip S (assoc signed :seq 99)))))
         (is (false? (<!! (rt/verify-gossip S (assoc signed :epoch 1))))))

       (testing "re-attributing it to another origin breaks it"
         (let [other (<!! (id/generate-identity))]
           (is (false? (<!! (rt/verify-gossip
                             S (assoc signed :origin (id/peer-id (:genesis other)))))))))

       (testing "a valid signature under a key that does not derive the origin is refused"
         ;; Both halves are required. A signature that checks out over a
         ;; mismatched origin would let anyone publish under another's name.
         (let [other (<!! (id/generate-identity))
               forged (assoc msg :origin (id/peer-id (:genesis other)))
               forged (<!! (rt/sign-gossip S other forged))]
           (is (true? (<!! (rt/verify-gossip S forged))) "sanity: self-consistent")
           (is (false? (<!! (rt/verify-gossip
                             S (assoc forged :origin-key (id/bytes->hex (:public (:operational kp))))))))))

       (testing "garbage credentials are refused, not thrown"
         (is (false? (<!! (rt/verify-gossip S (assoc signed :origin-sig "zz")))))
         (is (false? (<!! (rt/verify-gossip S (assoc signed :origin-key "00")))))
         (is (false? (<!! (rt/verify-gossip S (assoc signed :origin-key 42)))))))))

#?(:clj
   (deftest hello-is-enqueued-before-proof
     (let [local (<!! (id/generate-identity))
           remote (<!! (id/generate-identity))
           ctx (rt/make-runtime
                {:id (id/peer-id (:genesis local))
                 :state {}
                 :handler (fn [state _ _] {:state state :actions []})
                 :effects (recording-effects (atom []))})
           peer (atom {})
           in (chan)
           out (chan)
           _ ((rt/middleware {:ctx ctx :identity local :addresses [] :seq-no 0})
              [S peer [in out]])
           remote-nonce (id/new-nonce)
           remote-record (<!! (id/sign-record remote [] 0))]
       (testing "concurrent Netz producers preserve the protocol order Kabel ships"
         ;; Feed their hello before consuming our output. Without the
         ;; hello-sent gate, the reply producer can win the race to `out`.
         (>!! in {:type rt/hello-type
                  :record (id/record->wire remote-record)
                  :nonce remote-nonce})
         (is (= rt/hello-type (:type (<!! out))))
         (is (= rt/proof-type (:type (<!! out))))))))

(deftest frames-are-recognisable
  (testing "overlay frames are distinguishable from application messages"
    (is (rt/overlay-message? (rt/frame {:type :dial})))
    (is (rt/overlay-message? {:type rt/hello-type :record {}}))
    (is (rt/overlay-message?
         (rt/extension-carrier "example/1" {"type" "example"})))
    (is (not (rt/overlay-message? {:type :app/thing})))
    (is (not (rt/overlay-message? ["application/vector" (id/hex->bytes "00")])))
    (is (not (rt/overlay-message? "a string")))
    (is (not (rt/overlay-message? nil)))))

(deftest overload-refuses-rather-than-throws
  (testing "submit! reports a full queue instead of queueing until it breaks"
    ;; `put!` on a full channel queues, and core.async throws past 1024 pending
    ;; puts — so overload used to arrive as an exception rather than a decision.
    ;; replikativ dropped the connection in exactly this case, which is the
    ;; right answer to a peer sending faster than we can process.
    (let [ctx (rt/make-runtime {:id :me :state {}
                                :handler (fn [s _ _] {:state s :actions []})
                                :effects {:send! (fn [_ _]) :connect! (fn [_ _ _])
                                          :disconnect! (fn [_]) :schedule! (fn [_ _])}})
          ;; nothing is draining :events, so it fills at its buffer
          accepted (count (take-while true?
                                      (repeatedly 5000 #(rt/submit! ctx {:type :init}))))]
      (is (< accepted 5000) "the queue accepted everything, so it is unbounded")
      (is (pos? accepted))
      (testing "and keeps refusing rather than throwing"
        (is (false? (rt/submit! ctx {:type :init})))))))

#?(:clj
   (deftest ordered-interest-removal-waits-for-one-runtime-slot
     (let [ctx (rt/make-runtime
                {:id :me :state {}
                 :handler (fn [s _ _] {:state s :actions []})
                 :effects {:send! (fn [_ _]) :connect! (fn [_ _ _])
                           :disconnect! (fn [_]) :schedule! (fn [_ _])}})]
       (while (rt/submit! ctx {:type :init}))
       (let [settled (rt/unsubscribe-topics-ordered! {:ctx ctx} #{:secret})]
         (is (nil? (clojure.core.async/poll! settled))
             "cleanup must not claim success while the queue is full")
         (<!! (:events ctx))
         (is (true? (<?? S settled))
             "cleanup settles only after it owns a queue position")))))

(deftest one-bad-event-must-not-end-the-loop
  ;; `go-super` wraps the loop body in a try/catch that REPORTS and EXITS, so
  ;; before the guard any handler exception killed the peer's overlay
  ;; permanently -- and no wire field is type-checked, so producing one is a
  ;; single frame. Reproduced adversarially: a `:have` whose summary holds a
  ;; string throws IllegalArgumentException out of the interval-set code.
  (let [seen (atom [])
        ctx (rt/make-runtime
             {:id :me
              :state {}
              :handler (fn [state ev _ctx]
                         (swap! seen conj (:type ev))
                         (if (= :boom (:type ev))
                           (throw (ex-info "handler blew up" {}))
                           {:state state :actions []}))
              :effects {:send! (fn [_ _]) :connect! (fn [_ _ _])
                        :disconnect! (fn [_]) :schedule! (fn [_ _])}})]
    (rt/step! ctx {:type :fine})
    (is (= [:fine] @seen))

    (testing "a throwing event is dropped and counted, not fatal"
      ;; step! itself still throws -- the guard lives in the LOOP, which is
      ;; what must survive. This asserts the contract the loop relies on.
      (is (thrown? #?(:clj Throwable :cljs :default)
                   (rt/step! ctx {:type :boom}))))

    (testing "and the machine still works afterwards"
      (rt/step! ctx {:type :after})
      (is (= [:fine :boom :after] @seen)
          "state and handler must be intact after a throwing event"))))

(deftest the-dropped-counter-exists-so-silence-is-visible
  ;; A peer quietly discarding traffic is indistinguishable from a quiet
  ;; network. The guard must count what it swallows.
  (let [ctx (rt/make-runtime {:id :me :state {} :handler (fn [s _ _] {:state s :actions []})
                              :effects {:send! (fn [_ _]) :connect! (fn [_ _ _])
                                        :disconnect! (fn [_]) :schedule! (fn [_ _])}})]
    (is (zero? @(:dropped ctx)))))

(deftest the-schema-gate-refuses-the-frames-that-killed-the-loop
  ;; Each of these was reproduced adversarially as a one-frame remote kill:
  ;; no wire field was type-checked, and an exception out of any handler ends
  ;; the event loop permanently. The gate refuses them at ingress; the loop
  ;; guard is the backstop for whatever the gate misses.
  (testing "malformed known messages are refused"
    (are [payload] (false? (rt/well-formed? payload))
      {:type :have :summary {:o {0 "abc"}}}
      {:type :have :summary "xxx"}
      {:type :have :summary {:o [1 2 3]}}
      {:type :have :summary {}}
      {:type :have :summary {} :horizon {[:o 0] "hi"}}
      {:type :want :ranges [{:lo "x" :hi 5}]}
      {:type :want :ranges 42}
      ;; :hops is deliberately OUTSIDE the signature so relays can increment
      ;; it -- which means any relay could corrupt a validly signed message and
      ;; kill everything downstream. This is the one that needed no key.
      {:type :gossip :origin :o :epoch 0 :seq 1 :hops "x"}
      {:type :gossip :origin :o :epoch "x" :seq 1 :hops 0}
      {:type :gossip :origin :o :epoch 0 :seq nil :hops 0}
      {:type :interests :topics 5}
      {:type :content/found :providers 5}
      {:type :content/want-tree :have 5}
      {:type :sync/open :session :s :topic :t :summary {}}
      {:type :sync/chunk :session :s :index "0" :data {}}
      {:type :dial :addresses 42}
      {:type :dial :addresses [] :authority-id "truncated"}
      {:type :dial-refused :peers 42}
      {:type :peers :peers []}
      {:type :peers :entries 42}
      {:type :something/new :whatever 42}))

  (testing "well-formed messages pass"
    (are [payload] (true? (rt/well-formed? payload))
      {:type :have :summary {:o {0 {:t [[0 5]]}}} :horizon {[:o 0 :t] 3}}
      {:type :want :ranges [{:topic :t :lo 0 :hi 5}]}
      {:type :gossip :origin :o
       :origin-authority (apply str (repeat 64 "0"))
       :epoch 0 :seq 1 :hops 2 :topic :t :payload "x"}
      {:type :sync/open :session :s :topic :t
       :profile "netz/join-semilattice/1" :summary {}}
      {:type :sync/chunk :session :s :index 0 :data {}}
      {:type :sync/ack :session :s :through 0}
      {:type :sync/done :session :s :count 1 :topic :t
       :checkpoint {} :frontiers {[:o 0 :t] 3}}
      {:type :sync/abort :session :s :code :timeout}
      {:type :interests :topics #{:t} :carries #{}}
      {:type :dial :addresses ["wss://peer.example/netz"]
       :authority-id (apply str (repeat 64 "a"))}
      {:type :peers :entries []}))

  (testing "required fields must be present"
    (is (false? (rt/well-formed? {:type :have})))
    (is (false? (rt/well-formed? {:type :gossip :origin :o}))))

  (testing "unknown types require explicit protocol negotiation"
    (is (false? (rt/well-formed? {:type :something/new :whatever 42}))))

  (testing "decoded values are bounded before handlers process them"
    (is (false? (rt/well-formed?
                 {:type :gossip :origin :o :epoch 0 :seq 1 :hops 0
                  :topic :t :payload (apply str (repeat 1048577 "x"))})))
    (is (false? (rt/well-formed?
                 {:type :gossip :origin :o :epoch 0 :seq 1 :hops 0
                  :topic :t :payload (apply str (repeat 262145 "🕸"))}))
        "text limits count UTF-8 bytes, not host-language characters")))
