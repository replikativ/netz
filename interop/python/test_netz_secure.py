import asyncio
import json
import unittest
import warnings
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric import ed25519, x25519

from netz_protocol import dumps, loads
from netz_protocol.crypto import (
    DEFAULT_CAPABILITIES,
    DEFAULT_REQUIRED,
    NOISE_PROLOGUE,
    NOISE_PROTOCOL_NAME,
    NOISE_STATIC_DOMAIN,
    CryptoError,
    capability_offer,
    make_identity_payload,
    noise_static_binding_bytes,
    normalize_websocket_uri,
    offer_bytes,
    parse_offer,
    selected_capabilities,
    sign_identity_record,
    verify_identity_payload,
    verify_identity_record,
)
from netz_protocol.identity import genesis
from netz_protocol.secure import (
    FRAGMENT_DATA_LIMIT,
    KABEL_BINARY_SERIALIZER_ID,
    MAX_WEBSOCKET_MESSAGE,
    LocalIdentity,
    NoiseXXSession,
    SecureTransportError,
    SecureWebSocket,
    accept_secure_websocket,
    open_secure_websocket,
)


ROOT = Path(__file__).resolve().parents[2]


def raw_public(private: bytes) -> bytes:
    return ed25519.Ed25519PrivateKey.from_private_bytes(private).public_key().public_bytes_raw()


def identity(seed: int, *extra_capabilities: str,
             required: tuple[str, ...] = DEFAULT_CAPABILITIES) -> LocalIdentity:
    private = bytes([seed]) * 32
    public = raw_public(private)
    value = genesis([public], 1, [public], bytes([seed + 32]) * 32)
    return LocalIdentity(
        genesis=value,
        operational_private=private,
        addresses=(f"wss://peer-{seed}.example/netz",),
        sequence=7,
        capabilities=DEFAULT_CAPABILITIES + extra_capabilities,
        required=required,
    )


class MemoryWebSocket:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[bytes] = asyncio.Queue()
        self.remote = None
        self.sent: list[bytes] = []
        self.closed = False

    async def send(self, value: bytes) -> None:
        self.sent.append(value)
        await self.remote.incoming.put(value)

    async def recv(self) -> bytes:
        return await self.incoming.get()

    async def close(self) -> None:
        self.closed = True


class SecureTest(unittest.TestCase):
    def test_capability_tokens_are_visible_ascii(self):
        self.assertEqual(("netz/1",), capability_offer(("netz/1",), ()).capabilities)
        for invalid in ("contains space", "caf\N{LATIN SMALL LETTER E WITH ACUTE}/1",
                        "line\nbreak", "\x7f"):
            with self.subTest(invalid=invalid), self.assertRaises(CryptoError):
                capability_offer((invalid,), ())

    def test_signed_addresses_are_normalized_ascii_websocket_uris(self):
        self.assertEqual(
            "wss://xn--bcher-kva.example/na%C3%AFve%20path?q=%E2%9C%93",
            normalize_websocket_uri(
                "WSS://B\N{LATIN SMALL LETTER U WITH DIAERESIS}CHER.Example/"
                "na\N{LATIN SMALL LETTER I WITH DIAERESIS}ve path?q="
                "\N{CHECK MARK}"
            ),
        )
        self.assertEqual("ws://[2001:db8::1]:8080/",
                         normalize_websocket_uri("ws://[2001:0DB8::1]:8080"))
        private = bytes([9]) * 32
        public = raw_public(private)
        authority = genesis([public], 1, [public], bytes([41]) * 32)
        record = sign_identity_record(
            authority, private,
            ("WSS://B\N{LATIN SMALL LETTER U WITH DIAERESIS}CHER.Example/sync",), 1
        )
        self.assertEqual(["wss://xn--bcher-kva.example/sync"], record["addresses"])
        verify_identity_record(record)

        for invalid in ("https://peer.example/", "wss://user@peer.example/",
                        "wss://peer.example/#fragment", "wss://peer.example/%zz"):
            with self.subTest(invalid=invalid), self.assertRaises(CryptoError):
                normalize_websocket_uri(invalid)

    def test_verifier_rejects_noncanonical_signed_address(self):
        private = bytes([10]) * 32
        public = raw_public(private)
        authority = genesis([public], 1, [public], bytes([42]) * 32)
        record = sign_identity_record(authority, private,
                                      ("wss://peer.example/sync",), 1)
        record["addresses"] = ["WSS://PEER.EXAMPLE/sync"]
        with self.assertRaises(CryptoError):
            verify_identity_record(record)

    def test_normative_kabel_noise_binding_fixture(self):
        from noise.connection import Keypair, NoiseConnection

        fixture = json.loads(
            (ROOT / "spec/fixtures/netz-kabel-noise-binding-1.json").read_text()
        )
        self.assertEqual(NOISE_PROTOCOL_NAME.decode(), fixture["protocol_name"])
        self.assertEqual(NOISE_PROLOGUE.hex(), fixture["prologue_hex"])
        self.assertEqual(NOISE_STATIC_DOMAIN.hex(), fixture["binding_domain_hex"])

        init_offer = capability_offer(
            DEFAULT_CAPABILITIES + ("sync/1",), DEFAULT_REQUIRED
        )
        resp_offer = capability_offer(
            DEFAULT_CAPABILITIES + ("content/1", "sync/1"), DEFAULT_REQUIRED
        )
        selected = selected_capabilities(init_offer, resp_offer)
        self.assertEqual(fixture["selected"], list(selected))
        offer = offer_bytes(init_offer)
        self.assertEqual(fixture["offer_cbor_hex"], offer.hex())

        generated = {}
        for role, offer_value, address, sequence in (
            ("initiator", init_offer, "wss://alice.example/netz", 7),
            ("responder", resp_offer, "wss://bob.example/netz", 8),
        ):
            side = fixture[role]
            private = bytes.fromhex(side["operational_private_hex"])
            genesis_value = loads(bytes.fromhex(side["genesis_cbor_hex"]))
            record = sign_identity_record(
                genesis_value, private, [address], sequence
            )
            self.assertEqual(side["record_cbor_hex"], dumps(record).hex())
            static_private = bytes.fromhex(side["noise_static_private_hex"])
            static_public = (x25519.X25519PrivateKey
                             .from_private_bytes(static_private)
                             .public_key().public_bytes_raw())
            binding = noise_static_binding_bytes(
                bytes.fromhex(side["authority_hex"]), static_public, role,
                offer_value.capabilities, offer_value.required, selected,
            )
            self.assertEqual(side["binding_bytes_hex"], binding.hex())
            payload = make_identity_payload(
                record, private, static_public, role,
                offer_value.capabilities, offer_value.required, selected,
            )
            self.assertEqual(side["identity_payload_hex"], payload.hex())
            generated[role] = (payload, static_private)

        initiator = NoiseConnection.from_name(NOISE_PROTOCOL_NAME)
        responder = NoiseConnection.from_name(NOISE_PROTOCOL_NAME)
        for session, role in ((initiator, "initiator"),
                              (responder, "responder")):
            side = fixture[role]
            session.set_prologue(NOISE_PROLOGUE)
            (session.set_as_initiator if role == "initiator"
             else session.set_as_responder)()
            session.set_keypair_from_private_bytes(
                Keypair.STATIC, generated[role][1]
            )
            session.set_keypair_from_private_bytes(
                Keypair.EPHEMERAL,
                bytes.fromhex(side["noise_ephemeral_private_hex"]),
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                session.start_handshake()

        message1 = bytes(initiator.write_message(offer))
        self.assertEqual(fixture["handshake"]["message_1_hex"], message1.hex())
        self.assertEqual(offer, bytes(responder.read_message(message1)))
        message2 = bytes(responder.write_message(generated["responder"][0]))
        self.assertEqual(fixture["handshake"]["message_2_hex"], message2.hex())
        self.assertEqual(generated["responder"][0],
                         bytes(initiator.read_message(message2)))
        message3 = bytes(initiator.write_message(generated["initiator"][0]))
        self.assertEqual(fixture["handshake"]["message_3_hex"], message3.hex())
        self.assertEqual(generated["initiator"][0],
                         bytes(responder.read_message(message3)))
        self.assertEqual(fixture["handshake"]["hash_hex"],
                         initiator.get_handshake_hash().hex())

        transport = fixture["first_transport"]
        plaintext = bytes.fromhex(transport["fragment_plaintext_hex"])
        ciphertext = initiator.encrypt(plaintext)
        self.assertEqual(transport["ciphertext_hex"], ciphertext.hex())
        self.assertEqual(plaintext, responder.decrypt(ciphertext))
        outer = b"\x00\x00\x00\x00\x02" + ciphertext
        self.assertEqual(transport["outer_hex"], outer.hex())

    def test_shared_geheimnis_cacophony_transcript(self):
        from noise.connection import Keypair, NoiseConnection

        fixture = json.loads(
            (ROOT / "spec/fixtures/noise-xx-aesgcm-1.json").read_text()
        )
        initiator = NoiseConnection.from_name(fixture["protocol_name"].encode())
        responder = NoiseConnection.from_name(fixture["protocol_name"].encode())
        for session, role in ((initiator, "initiator"),
                              (responder, "responder")):
            session.set_prologue(bytes.fromhex(fixture["prologue_hex"]))
            (session.set_as_initiator if role == "initiator"
             else session.set_as_responder)()
            session.set_keypair_from_private_bytes(
                Keypair.STATIC,
                bytes.fromhex(fixture[f"{role}_static_private_hex"]),
            )
            session.set_keypair_from_private_bytes(
                Keypair.EPHEMERAL,
                bytes.fromhex(fixture[f"{role}_ephemeral_private_hex"]),
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                session.start_handshake()

        for index, message in enumerate(fixture["messages"]):
            sender, receiver = ((initiator, responder) if index % 2 == 0
                                else (responder, initiator))
            payload = bytes.fromhex(message["payload_hex"])
            if sender.handshake_finished:
                ciphertext = sender.encrypt(payload)
                plaintext = receiver.decrypt(ciphertext)
            else:
                ciphertext = bytes(sender.write_message(payload))
                plaintext = bytes(receiver.read_message(ciphertext))
            self.assertEqual(message["ciphertext_hex"], ciphertext.hex())
            self.assertEqual(payload, plaintext)

        self.assertEqual(fixture["handshake_hash_hex"],
                         initiator.get_handshake_hash().hex())
        self.assertEqual(initiator.get_handshake_hash(),
                         responder.get_handshake_hash())

    def test_signed_record_and_noise_binding(self):
        local = identity(1, "sync/1")
        record = sign_identity_record(
            local.genesis, local.operational_private, local.addresses, local.sequence
        )
        authority = verify_identity_record(record)
        static = bytes(range(32))
        local_offer = capability_offer(local.capabilities, local.required)
        remote_offer = capability_offer(
            DEFAULT_CAPABILITIES + ("content/1", "sync/1"),
            DEFAULT_CAPABILITIES,
        )
        selected = selected_capabilities(local_offer, remote_offer)
        payload = make_identity_payload(
            record, local.operational_private, static, "initiator",
            local.capabilities, local.required, selected,
        )
        peer = verify_identity_payload(
            payload, static, expected_role="initiator",
            local_offer=remote_offer, expected_offer=local_offer,
            expected_authority=authority,
        )
        self.assertEqual(authority, peer.authority)
        self.assertEqual(tuple(sorted(DEFAULT_CAPABILITIES + ("sync/1",))),
                         peer.capabilities)
        with self.assertRaises(CryptoError):
            verify_identity_payload(
                payload, b"x" * 32, expected_role="initiator",
                local_offer=remote_offer,
            )

        downgraded = tuple(sorted(DEFAULT_CAPABILITIES))
        downgraded_payload = make_identity_payload(
            record, local.operational_private, static, "initiator",
            local.capabilities, local.required, downgraded,
        )
        with self.assertRaises(CryptoError):
            verify_identity_payload(
                downgraded_payload, static, expected_role="initiator",
                local_offer=remote_offer, expected_offer=local_offer,
            )

    def test_offer_and_downgrade_rules_are_exact(self):
        local = capability_offer(
            DEFAULT_CAPABILITIES + ("sync/1",), DEFAULT_CAPABILITIES
        )
        remote = capability_offer(
            DEFAULT_CAPABILITIES + ("content/1", "sync/1"),
            DEFAULT_CAPABILITIES,
        )
        self.assertEqual(local, parse_offer(offer_bytes(local)))
        self.assertEqual(tuple(sorted(DEFAULT_CAPABILITIES + ("sync/1",))),
                         selected_capabilities(local, remote))
        with self.assertRaises(CryptoError):
            capability_offer(("sync/1",), ("required-but-missing/1",))
        with self.assertRaises(CryptoError):
            parse_offer(dumps({
                "v": 1,
                "type": "netz/noise/offer",
                "capabilities": list(reversed(DEFAULT_CAPABILITIES)),
                "required": list(DEFAULT_CAPABILITIES),
            }))

    def sessions(self):
        alice = identity(1, "dissemination/1", "sync/1")
        bob = identity(2, "sync/1")
        expected_bob = verify_identity_record(sign_identity_record(
            bob.genesis, bob.operational_private, bob.addresses, bob.sequence
        ))
        initiator = NoiseXXSession(
            initiator=True, static_private=b"a" * 32,
            local_identity=alice, expected_authority=expected_bob,
        )
        responder = NoiseXXSession(
            initiator=False, static_private=b"b" * 32,
            local_identity=bob,
        )
        responder.read_handshake(initiator.write_handshake())
        initiator.read_handshake(responder.write_handshake())
        responder.read_handshake(initiator.write_handshake())
        return initiator, responder

    def test_missing_required_capability_aborts(self):
        alice = identity(
            1, "sync/1", required=DEFAULT_CAPABILITIES + ("sync/1",)
        )
        bob = identity(2)
        initiator = NoiseXXSession(
            initiator=True, static_private=b"a" * 32, local_identity=alice
        )
        responder = NoiseXXSession(
            initiator=False, static_private=b"b" * 32, local_identity=bob
        )
        with self.assertRaises(SecureTransportError):
            responder.read_handshake(initiator.write_handshake())
        self.assertTrue(responder.closed)

    def test_noise_xx_identity_and_transport(self):
        initiator, responder = self.sessions()
        self.assertEqual(initiator.handshake_hash, responder.handshake_hash)
        self.assertNotEqual(initiator.remote_peer.authority,
                            responder.remote_peer.authority)
        ciphertext = initiator.encrypt(b"ordered application bytes")
        self.assertEqual(b"ordered application bytes", responder.decrypt(ciphertext))
        reverse = responder.encrypt(b"response")
        self.assertEqual(b"response", initiator.decrypt(reverse))

    def test_noise_authentication_failure_closes_session(self):
        initiator, responder = self.sessions()
        ciphertext = bytearray(initiator.encrypt(b"message"))
        ciphertext[-1] ^= 1
        with self.assertRaises(SecureTransportError):
            responder.decrypt(bytes(ciphertext))
        self.assertTrue(responder.closed)

    def test_secure_websocket_fragments_and_reassembles(self):
        async def scenario():
            initiator, responder = self.sessions()
            left, right = MemoryWebSocket(), MemoryWebSocket()
            left.remote, right.remote = right, left
            sender = SecureWebSocket(left, initiator)
            receiver = SecureWebSocket(right, responder)
            value = b"z" * (FRAGMENT_DATA_LIMIT * 2 + 17)
            await sender.send_frame(value)
            first_outer = left.sent[0]
            self.assertEqual(KABEL_BINARY_SERIALIZER_ID,
                             int.from_bytes(first_outer[:4], "big"))
            self.assertEqual(2, first_outer[4])
            self.assertEqual(value, await receiver.recv_frame())

        asyncio.run(scenario())

    def test_malformed_fragment_closes_carrier(self):
        async def scenario():
            initiator, responder = self.sessions()
            left, right = MemoryWebSocket(), MemoryWebSocket()
            left.remote, right.remote = right, left
            receiver = SecureWebSocket(right, responder)
            # A middle fragment without a start is a protocol error. The flag
            # is encrypted; only the peer advancing the correct Noise nonce can
            # construct this authenticated malformed frame.
            ciphertext = initiator.encrypt(b"\x02orphan")
            await left.send(b"\x00\x00\x00\x00\x02" + ciphertext)
            with self.assertRaises(SecureTransportError):
                await receiver.recv_frame()
            self.assertTrue(responder.closed)
            self.assertTrue(right.closed)

        asyncio.run(scenario())

    def test_incomplete_handshake_times_out_and_closes(self):
        async def scenario():
            websocket = MemoryWebSocket()
            with self.assertRaisesRegex(SecureTransportError, "timed out"):
                await accept_secure_websocket(
                    websocket, identity=identity(2), static_private=b"b" * 32,
                    handshake_timeout=0.001,
                )
            self.assertTrue(websocket.closed)

        asyncio.run(scenario())

    def test_real_async_websocket_dial_and_accept(self):
        async def scenario():
            from websockets.asyncio.server import serve

            alice = identity(1, "sync/1")
            bob = identity(2, "sync/1")
            expected_bob = verify_identity_record(sign_identity_record(
                bob.genesis, bob.operational_private, bob.addresses, bob.sequence
            ))
            accepted = asyncio.get_running_loop().create_future()

            async def handler(websocket):
                secure = await accept_secure_websocket(
                    websocket, identity=bob, static_private=b"b" * 32
                )
                accepted.set_result(secure.peer)
                await secure.send_frame(await secure.recv_frame())

            async with serve(handler, "127.0.0.1", 0,
                             max_size=MAX_WEBSOCKET_MESSAGE) as server:
                port = server.sockets[0].getsockname()[1]
                secure = await open_secure_websocket(
                    f"ws://127.0.0.1:{port}", identity=alice,
                    expected_authority=expected_bob,
                    static_private=b"a" * 32,
                )
                await secure.send_frame(b"round trip")
                self.assertEqual(b"round trip", await secure.recv_frame())
                remote_alice = await accepted
                self.assertEqual(
                    verify_identity_record(sign_identity_record(
                        alice.genesis, alice.operational_private,
                        alice.addresses, alice.sequence
                    )),
                    remote_alice.authority,
                )
                await secure.websocket.close()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
