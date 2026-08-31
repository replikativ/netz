import asyncio
import json
import unittest
import warnings
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric import ed25519

from netz_protocol.crypto import (
    CryptoError,
    make_identity_payload,
    sign_identity_record,
    verify_identity_payload,
    verify_identity_record,
)
from netz_protocol.identity import genesis
from netz_protocol.secure import (
    LocalIdentity,
    NOISE_MAX_PLAINTEXT,
    NoiseXXSession,
    SecureTransportError,
    SecureWebSocket,
    accept_secure_websocket,
    open_secure_websocket,
)


ROOT = Path(__file__).resolve().parents[2]


def raw_public(private: bytes) -> bytes:
    return ed25519.Ed25519PrivateKey.from_private_bytes(private).public_key().public_bytes_raw()


def identity(seed: int, *capabilities: str) -> LocalIdentity:
    private = bytes([seed]) * 32
    public = raw_public(private)
    value = genesis([public], 1, [public], bytes([seed + 32]) * 32)
    return LocalIdentity(
        genesis=value,
        operational_private=private,
        addresses=(f"wss://peer-{seed}.example/netz",),
        sequence=7,
        capabilities=capabilities,
    )


class MemoryWebSocket:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[bytes] = asyncio.Queue()
        self.remote = None

    async def send(self, value: bytes) -> None:
        await self.remote.incoming.put(value)

    async def recv(self) -> bytes:
        return await self.incoming.get()

    async def close(self) -> None:
        return None


class SecureTest(unittest.TestCase):
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
        payload = make_identity_payload(
            record, local.operational_private, static, local.capabilities
        )
        peer = verify_identity_payload(payload, static, authority)
        self.assertEqual(authority, peer.authority)
        self.assertEqual(("sync/1",), peer.capabilities)
        with self.assertRaises(CryptoError):
            verify_identity_payload(payload, b"x" * 32)

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
            value = b"z" * (NOISE_MAX_PLAINTEXT * 2 + 17)
            await sender.send_frame(value)
            self.assertEqual(value, await receiver.recv_frame())

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
                             max_size=65535) as server:
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
