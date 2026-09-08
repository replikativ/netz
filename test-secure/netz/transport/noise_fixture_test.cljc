(ns netz.transport.noise-fixture-test
  "JVM and Node consumers of the shared Python/Kabel/Noise known answer."
  (:require [clojure.core.async :as async]
            #?(:clj [clojure.data.json :as json])
            #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is async]])
            [netz.identity :as identity]
            [netz.transport.noise :as secure]
            [netz.wire :as wire]
            [org.replikativ.geheimnis.dh :as dh]
            [org.replikativ.geheimnis.noise :as noise]))

(defn- fixture []
  (#?(:clj json/read-str :cljs #(js->clj (js/JSON.parse %)))
   #?(:clj (slurp "spec/fixtures/netz-kabel-noise-binding-1.json")
      :cljs (.readFileSync (js/require "fs")
                           "spec/fixtures/netz-kabel-noise-binding-1.json" "utf8"))))

(defn- hex [value] (identity/hex->bytes value))

(defn- keypair [private-hex]
  (async/go
    (let [private (hex private-hex)
          basepoint (hex (str "09" (apply str (repeat 62 "0"))))]
      {:private private
       :public (async/<! (dh/key-agreement private basepoint))})))

(defn- exercise-known-answer []
  (async/go
    (try
      (let [f (fixture)
            initiator (get f "initiator")
            responder (get f "responder")
            handshake (get f "handshake")
            first-transport (get f "first_transport")
            initiator-payload (hex (get initiator "identity_payload_hex"))
            responder-payload (hex (get responder "identity_payload_hex"))
            initiator-value (wire/decode initiator-payload)
            responder-value (wire/decode responder-payload)
            defaults secure/default-capabilities
            initiator-offer {:capabilities (conj defaults "sync/1")
                             :required defaults}
            responder-offer {:capabilities (conj defaults "content/1" "sync/1")
                             :required defaults}
            selected (conj defaults "sync/1")
            offer (#'secure/offer-bytes (:capabilities initiator-offer)
                                        (:required initiator-offer))
            initiator-binding
            (#'secure/binding-bytes
             (get-in initiator-value ["record" "authority"])
             (get initiator-value "noise_static") :initiator
             (:capabilities initiator-offer) (:required initiator-offer) selected)
            responder-binding
            (#'secure/binding-bytes
             (get-in responder-value ["record" "authority"])
             (get responder-value "noise_static") :responder
             (:capabilities responder-offer) (:required responder-offer) selected)
            initiator-result
            (async/<! (#'secure/validate-identity-payload
                       initiator-payload (get initiator-value "noise_static")
                       :initiator responder-offer initiator-offer
                       (get initiator "authority_hex")))
            responder-result
            (async/<! (#'secure/validate-identity-payload
                       responder-payload (get responder-value "noise_static")
                       :responder initiator-offer nil
                       (get responder "authority_hex")))
            prologue (hex (get f "prologue_hex"))
            initiator-0
            (noise/initialize
             :initiator (async/<! (keypair (get initiator "noise_static_private_hex")))
             {:prologue prologue
              :ephemeral-keypair
              (async/<! (keypair (get initiator "noise_ephemeral_private_hex")))})
            responder-0
            (noise/initialize
             :responder (async/<! (keypair (get responder "noise_static_private_hex")))
             {:prologue prologue
              :ephemeral-keypair
              (async/<! (keypair (get responder "noise_ephemeral_private_hex")))})
            initiator-1 (async/<! (noise/write-message initiator-0 offer))
            responder-1
            (async/<! (noise/read-message responder-0 (:message initiator-1)))
            responder-2
            (async/<! (noise/write-message (:state responder-1) responder-payload))
            initiator-2
            (async/<! (noise/read-message (:state initiator-1)
                                          (:message responder-2)))
            initiator-3
            (async/<! (noise/write-message (:state initiator-2) initiator-payload))
            responder-3
            (async/<! (noise/read-message (:state responder-2)
                                          (:message initiator-3)))
            transport
            (async/<!
             (noise/encrypt-transport
              (:send (noise/transport-ciphers (:state initiator-3)))
              (hex (get first-transport "fragment_plaintext_hex"))))
            outer (identity/concat-bufs (identity/seq->bytes [0 0 0 0 2])
                                        (:message transport))]
        (is (= (get f "offer_cbor_hex") (identity/bytes->hex offer)))
        (is (= (get initiator "binding_bytes_hex")
               (identity/bytes->hex initiator-binding)))
        (is (= (get responder "binding_bytes_hex")
               (identity/bytes->hex responder-binding)))
        (is (= (get initiator "authority_hex") (:authority initiator-result)))
        (is (= (get responder "authority_hex") (:authority responder-result)))
        (is (= selected (:capabilities initiator-result)
               (:capabilities responder-result)))
        (is (= (get handshake "message_1_hex")
               (identity/bytes->hex (:message initiator-1))))
        (is (= (get handshake "message_2_hex")
               (identity/bytes->hex (:message responder-2))))
        (is (= (get handshake "message_3_hex")
               (identity/bytes->hex (:message initiator-3))))
        (is (= (get handshake "hash_hex")
               (identity/bytes->hex (noise/handshake-hash (:state initiator-3)))
               (identity/bytes->hex (noise/handshake-hash (:state responder-3)))))
        (is (= (get first-transport "ciphertext_hex")
               (identity/bytes->hex (:message transport))))
        (is (= (get first-transport "outer_hex") (identity/bytes->hex outer))))
      (catch #?(:clj Throwable :cljs :default) error
        (is false (str "Noise fixture failed: " error))))))

(deftest shared-python-jvm-js-noise-binding-known-answer
  #?(:clj (async/<!! (exercise-known-answer))
     :cljs (async done
                  (async/go
                    (async/<! (exercise-known-answer))
                    (done)))))
