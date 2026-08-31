(ns netz.overlay.integration-test
  "The overlay over a real WebSocket, with real kabel peers.

  Everything below this level is tested in the simulator against pure state
  machines. These tests exist to check the part the simulator cannot: that the
  identity handshake completes over a real wire, that connections register
  under the right peer id, and that a publish crosses a socket."
  (:require [clojure.test :refer [deftest testing is]]
            [kabel.http-kit :as http-kit]
            [netz.identity :as id]
            [netz.overlay :as overlay]
            [netz.overlay.runtime :as rt]
            [kabel.peer :as peer]
            [superv.async :refer [<?? S]]
            [clojure.core.async :refer [timeout chan >!! <! go-loop]]))

(def ^:private port-counter (atom 48300))

(defn- unique-port
  "Ports from a monotone counter, not `rand-int`.

  Random ports collide, and the collision got much more likely once membership
  started *retrying* dials: a client whose server has been stopped keeps
  redialling that port, so a later test that lands on it inherits a stranger's
  connection attempts. One flaky run is one too many for a race that a counter
  removes outright."
  []
  (swap! port-counter inc))

(defn- wait-for
  "Poll `pred` until it holds or `ms` elapses. Returns whether it held.

  Real sockets need a settling window; the simulator's virtual clock is what
  makes the *protocol* tests deterministic, so these need only be patient
  rather than precise."
  [ms pred]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (cond
        (pred) true
        (> (System/currentTimeMillis) deadline) false
        :else (do (<?? S (timeout 50)) (recur))))))

(deftest handshake-identifies-both-ends
  (testing "two real peers connect, identify each other, and register"
    (let [port (unique-port)
          url (str "ws://localhost:" port)
          server-kp (<?? S (id/generate-identity))
          client-kp (<?? S (id/generate-identity))
          server-id (id/peer-id (:genesis server-kp))
          client-id (id/peer-id (:genesis client-kp))

          [server-mw install-server!] (rt/deferred-middleware)
          server-peer (peer/server-peer
                       S (http-kit/create-http-kit-handler! S url server-id)
                       server-id server-mw identity)
          server (<?? S (rt/start! S server-peer
                                   {:identity server-kp
                                    :addresses [url]
                                    :topics #{:t}}))]
      ;; server-peer captured server-mw lexically, so the middleware must be
      ;; installed through the indirection rather than swapped onto the atom.
      (install-server! (:middleware server))
      (<?? S (peer/start server-peer))
      (try
        (let [[client-mw install-client!] (rt/deferred-middleware)
              client-peer (peer/client-peer S client-id client-mw identity)
              client (<?? S (rt/start! S client-peer
                                       {:identity client-kp
                                        :addresses []
                                        :topics #{:t}
                                        :seeds [{:peer-id server-id
                                                 :addresses [url]
                                                 :group "seed"}]}))]
          (try
            (install-client! (:middleware client))
            (<?? S (peer/connect S client-peer url))

            (testing "each side learns the other's peer id from the signed hello"
              (is (wait-for 5000 #(contains? (rt/connections client-peer) server-id))
                  "client never registered the server")
              (is (wait-for 5000 #(contains? (rt/connections server-peer) client-id))
                  "server never registered the client"))

            (testing "and the ids are the ones derived from the public keys"
              ;; Not merely "some id" — the whole point of a self-certifying id
              ;; is that it is derivable from the key and cannot be asserted.
              (is (= #{server-id} (rt/connections client-peer)))
              (is (= #{client-id} (rt/connections server-peer))))

            (testing "membership records the connection"
              (is (wait-for 5000
                            #(overlay/connections (rt/overlay-state client))))
              (is (some #{server-id}
                        (overlay/connections (rt/overlay-state client)))))
            (finally
              (rt/stop! client)
              (<?? S (peer/stop client-peer)))))
        (finally
          (rt/stop! server)
          (<?? S (peer/stop server-peer)))))))

(deftest publish-crosses-a-real-socket
  (testing "a publish on one peer is delivered on the other"
    (let [port (unique-port)
          url (str "ws://localhost:" port)
          server-kp (<?? S (id/generate-identity))
          client-kp (<?? S (id/generate-identity))
          server-id (id/peer-id (:genesis server-kp))
          client-id (id/peer-id (:genesis client-kp))

          [server-mw install-server!] (rt/deferred-middleware)
          server-peer (peer/server-peer
                       S (http-kit/create-http-kit-handler! S url server-id)
                       server-id server-mw identity)
          server (<?? S (rt/start! S server-peer
                                   {:identity server-kp
                                    :addresses [url]
                                    :topics #{:t}}))]
      (install-server! (:middleware server))
      (<?? S (peer/start server-peer))
      (try
        (let [[client-mw install-client!] (rt/deferred-middleware)
              client-peer (peer/client-peer S client-id client-mw identity)
              client (<?? S (rt/start! S client-peer
                                       {:identity client-kp
                                        :addresses []
                                        :topics #{:t}
                                        :seeds [{:peer-id server-id
                                                 :addresses [url]
                                                 :group "seed"}]}))]
          (try
            (install-client! (:middleware client))
            (<?? S (peer/connect S client-peer url))
            (is (wait-for 5000 #(contains? (rt/connections server-peer) client-id)))

            (testing "client to server"
              (rt/publish! client :t "from-client")
              (is (wait-for 5000
                            #(= ["from-client"]
                                (overlay/delivered (rt/overlay-state server))))
                  (str "server delivered "
                       (overlay/delivered (rt/overlay-state server)))))

            (testing "server to client"
              (rt/publish! server :t "from-server")
              (is (wait-for 5000
                            #(some #{"from-server"}
                                   (overlay/delivered (rt/overlay-state client))))
                  (str "client delivered "
                       (overlay/delivered (rt/overlay-state client)))))
            (finally
              (rt/stop! client)
              (<?? S (peer/stop client-peer)))))
        (finally
          (rt/stop! server)
          (<?? S (peer/stop server-peer)))))))

(deftest closing-a-socket-reaches-the-state-machine
  (testing "when the server goes away, the client stops believing in it"
    ;; The gap this closes: without feeding the close back in, membership holds
    ;; a connection nobody is draining, and — worse — never redials, because a
    ;; peer it believes it is connected to is not a dial candidate. kabel has
    ;; no reconnect of its own, so this path *is* the reconnect.
    (let [port (unique-port)
          url (str "ws://localhost:" port)
          server-kp (<?? S (id/generate-identity))
          client-kp (<?? S (id/generate-identity))
          server-id (id/peer-id (:genesis server-kp))
          server-authority (id/authority-id (:genesis server-kp))
          client-id (id/peer-id (:genesis client-kp))

          [server-mw install-server!] (rt/deferred-middleware)
          server-peer (peer/server-peer
                       S (http-kit/create-http-kit-handler! S url server-id)
                       server-id server-mw identity)
          server (<?? S (rt/start! S server-peer
                                   {:identity server-kp
                                    :addresses [url]
                                    :topics #{:t}}))]
      (install-server! (:middleware server))
      (<?? S (peer/start server-peer))
      (try
        (let [[client-mw install-client!] (rt/deferred-middleware)
              client-peer (peer/client-peer S client-id client-mw identity)
              client (<?? S (rt/start! S client-peer
                                       {:identity client-kp
                                        :addresses []
                                        :topics #{:t}
                                        :seeds [{:peer-id server-id
                                                 :addresses [url]
                                                 :group "seed"}]}))]
          (try
            (install-client! (:middleware client))
            (<?? S (peer/connect S client-peer url))
            (is (wait-for 5000 #(contains? (rt/connections client-peer) server-id))
                "never connected, so the teardown proves nothing")
            (is (wait-for 5000 #(some #{server-id}
                                      (overlay/connections (rt/overlay-state client)))))
            (is (= {:authenticated? true
                    :transport :signed-hello
                    :routing-id server-id
                    :authority-id server-authority
                    :capabilities #{}}
                   (select-keys (rt/connection-info client-peer server-id)
                                [:authenticated? :transport :routing-id
                                 :authority-id :capabilities])))

            (<?? S (peer/stop server-peer))

            (testing "the transport registry drops it"
              (is (wait-for 5000
                            #(not (contains? (rt/connections client-peer)
                                             server-id)))
                  "the registry still holds a dead connection")
              (is (nil? (rt/connection-info client-peer server-id))
                  "the registry still holds dead authentication facts"))

            (testing "and so does membership"
              (is (wait-for 5000
                            #(not (some #{server-id}
                                        (overlay/connections
                                         (rt/overlay-state client)))))
                  "membership still believes in a dead connection"))
            (finally
              (rt/stop! client)
              (<?? S (peer/stop client-peer)))))
        (finally
          (rt/stop! server)
          (<?? S (peer/stop server-peer)))))))

(deftest forged-publishes-never-reach-the-state-machine
  (testing "a peer cannot publish under somebody else's identity"
    ;; The db-root case: an authenticated, connected peer tries to announce a
    ;; new root *as if* it came from the database's owner. Authorisation alone
    ;; would not stop this — the attacker is a legitimate member of the
    ;; network. Only the signature does.
    (let [attacker-kp (<?? S (id/generate-identity))
          victim-kp (<?? S (id/generate-identity))
          honest-kp (<?? S (id/generate-identity))
          attacker-id (id/peer-id (:genesis attacker-kp))
          honest-authority (id/authority-id (:genesis honest-kp))
          victim-id (id/peer-id (:genesis victim-kp))

          seen (atom [])
          ctx (rt/make-runtime {:id (id/peer-id (:genesis honest-kp))
                                :state {}
                                :handler (fn [st ev _] (swap! seen conj ev)
                                           {:state st :actions []})
                                :effects {:send! (fn [_ _])
                                          :connect! (fn [_ _ _])
                                          :disconnect! (fn [_])
                                          :schedule! (fn [_ _])}})
          _ (rt/run! S ctx)

          peer (peer/client-peer S :honest identity identity)
          in (chan 16)
          out (chan 16)
          _ ((rt/middleware {:ctx ctx :identity honest-kp :addresses [] :seq-no 0})
             [S peer [in out]])
          ;; The middleware announces itself on construction; its hello carries
          ;; the challenge we must answer to be registered.
          our-hello (<?? S out)
          ;; Then drain, or the rest of what it sends blocks the loop.
          _ (go-loop [] (when (<! out) (recur)))

          gossip (fn [origin] {:type :gossip :origin origin :epoch 0 :seq 1
                               :topic :db/roots :hops 0 :payload {:root "x"}})]

      (is (string? (:nonce our-hello))
          "the hello must carry a challenge, or a record is replayable")

      ;; The attacker completes a perfectly valid identity handshake — it is a
      ;; real peer, not an outsider, and it holds its own key. That is the
      ;; point: the defence being tested is not "keep strangers out".
      (let [attacker-nonce (id/new-nonce)]
        (>!! in {:type rt/hello-type
                 :record (id/record->wire
                          (<?? S (id/sign-record attacker-kp [] 0)))
                 :nonce attacker-nonce})
        (>!! in {:type rt/proof-type
                 :sig (<?? S (id/sign-challenge attacker-kp attacker-nonce
                                                honest-authority
                                                (:nonce our-hello)))}))
      (is (wait-for 3000 #(contains? (rt/connections peer) attacker-id))
          "the attacker never connected, so the test proves nothing")

      (testing "a publish signed by the attacker but claiming the victim's id is dropped"
        (>!! in {:type rt/frame-type
                 :payload (<?? S (rt/sign-gossip S attacker-kp (gossip victim-id)))})
        (<?? S (timeout 300))
        (is (not-any? #(= victim-id (get-in % [:payload :origin])) @seen)
            "a forged publish reached the state machine"))

      (testing "an unsigned publish is dropped too"
        (>!! in {:type rt/frame-type :payload (gossip attacker-id)})
        (<?? S (timeout 300))
        (is (empty? (filter #(= :gossip (get-in % [:payload :type])) @seen))))

      (testing "a tampered payload is dropped"
        (let [signed (<?? S (rt/sign-gossip S attacker-kp (gossip attacker-id)))]
          (>!! in {:type rt/frame-type
                   :payload (assoc signed :payload {:root "tampered"})})
          (<?? S (timeout 300))
          (is (empty? (filter #(= :gossip (get-in % [:payload :type])) @seen)))))

      (testing "but the attacker's own genuine publish is accepted"
        ;; The check is authentication, not exclusion: a member may publish
        ;; under its own name. Whether it is *allowed* to set that topic is a
        ;; separate question, and that is what :authorize-fn is for.
        (>!! in {:type rt/frame-type
                 :payload (<?? S (rt/sign-gossip S attacker-kp (gossip attacker-id)))})
        (is (wait-for 3000
                      #(some (fn [e] (and (= :gossip (get-in e [:payload :type]))
                                          (= attacker-id (get-in e [:payload :origin]))))
                             @seen))
            "a genuine publish was rejected")))))

(deftest epoch-advances-across-restarts
  (testing "a fresh runtime never reuses an epoch"
    ;; The epoch is what keeps a restarted peer's sequence numbers from being
    ;; suppressed as duplicates of its previous run.
    (let [kp (<?? S (id/generate-identity))
          peer-id (id/peer-id (:genesis kp))
          p1 (peer/client-peer S peer-id identity identity)
          a (<?? S (rt/start! S p1 {:identity kp :addresses [] :topics #{:t}}))
          _ (<?? S (timeout 5))
          p2 (peer/client-peer S peer-id identity identity)
          b (<?? S (rt/start! S p2 {:identity kp :addresses [] :topics #{:t}}))]
      (is (< (:epoch a) (:epoch b)))
      (is (= peer-id (:peer-id a) (:peer-id b))
          "identity must survive a restart; the epoch must not"))))

(deftest a-captured-hello-cannot-be-replayed
  ;; Before the challenge, an identity record was signed only over constants of
  ;; the identity — genesis, key, seq-no, addresses — so it was replayable by
  ;; anyone who had ever seen one. Capture a peer's hello, send it verbatim to
  ;; somebody else, and be registered as that peer: their membership, content
  ;; and repair frames all become attributable to you. Gossip signatures still
  ;; stopped you forging their publishes, which is why this was easy to miss.
  (let [victim-kp (<?? S (id/generate-identity))
        victim-id (id/peer-id (:genesis victim-kp))
        victim-record (id/record->wire (<?? S (id/sign-record victim-kp [] 0)))
        our-kp (<?? S (id/generate-identity))
        our-authority (id/authority-id (:genesis our-kp))
        attacker-kp (<?? S (id/generate-identity))
        attacker-id (id/peer-id (:genesis attacker-kp))
        victim-nonce (id/new-nonce)
        peer (atom {})
        ctx (rt/make-runtime {:id :me :state (overlay/make-state :me {})
                              :effects {:send! (fn [_ _]) :connect! (fn [_ _ _])
                                        :disconnect! (fn [_]) :schedule! (fn [_ _])}})
        in (chan 16)
        out (chan 16)
        _ ((rt/middleware {:ctx ctx :identity our-kp :addresses [] :seq-no 0
                           :handshake-max-attempts 8})
           [S peer [in out]])
        our-hello (<?? S out)]
    (go-loop [] (when (<! out) (recur)))

    (testing "the victim's record alone does not register it"
      ;; This is the whole attack: the attacker HAS the record and nothing else.
      (>!! in {:type rt/hello-type :record victim-record :nonce victim-nonce})
      (<?? S (timeout 400))
      (is (not (contains? (rt/connections peer) victim-id))
          "a replayed record must not bind the connection to that peer"))

    (testing "and a proof signed by the wrong key does not either"
      ;; The attacker answers our challenge with the only key it actually has.
      (>!! in {:type rt/proof-type
               :sig (<?? S (id/sign-challenge attacker-kp victim-nonce
                                              our-authority (:nonce our-hello)))})
      (<?? S (timeout 400))
      (is (not (contains? (rt/connections peer) victim-id))
          "the proof must be checked against the record's key, not any key"))

    (testing "a proof over a DIFFERENT nonce does not either"
      ;; Even the real victim's proof, captured from another connection, is
      ;; worthless here — which is what makes the nonce load-bearing.
      (>!! in {:type rt/proof-type
               :sig (<?? S (id/sign-challenge victim-kp (id/new-nonce)
                                              our-authority (:nonce our-hello)))})
      (<?? S (timeout 400))
      (is (not (contains? (rt/connections peer) victim-id))
          "a proof is only valid for the challenge WE chose"))

    (testing "a chosen-nonce signature from a different transcript does not register"
      ;; The old proof covered only our challenge, so an attacker could ask the
      ;; victim to sign that challenge on another socket and relay the result.
      ;; Binding both roles and both challenges rejects that simpler oracle.
      ;; A live relay can still splice the *whole* public transcript, so public
      ;; transports additionally need channel binding and frame integrity.
      (>!! in {:type rt/proof-type
               :sig (<?? S (id/sign-challenge victim-kp (id/new-nonce)
                                              (id/authority-id
                                               (:genesis attacker-kp))
                                              (:nonce our-hello)))})
      (<?? S (timeout 400))
      (is (not (contains? (rt/connections peer) victim-id))
          "a proof obtained for another verifier must not authenticate this socket"))

    (testing "but the real holder of the key registers"
      (>!! in {:type rt/proof-type
               :sig (<?? S (id/sign-challenge victim-kp victim-nonce
                                              our-authority (:nonce our-hello)))})
      (is (wait-for 3000 #(contains? (rt/connections peer) victim-id))
          "possession of the key on THIS connection is what registration means"))))
