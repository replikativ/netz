import json
import unittest
from hashlib import sha256
from importlib.util import find_spec
from pathlib import Path

from netz_codec import dumps
from netz_protocol.blob import (
    assemble,
    need_bitmap,
    needed_indices,
    split_blob,
)
from netz_protocol.replication import ReplicationError, place, rank, score
from netz_protocol.replication_layout import (
    LayoutError,
    ZERO_ADDRESS,
    layout_address,
    make_state as make_layout_state,
    placement_layout,
    reconcile,
    retention_authorized,
    validate_statement as validate_layout_statement,
)
from netz_protocol.replication_retirement import (
    RetirementError,
    verify_command as verify_retirement_command,
)
from netz_protocol.identity import authority_id, genesis
from netz_protocol.materialization import (
    MaterializationError,
    application_binding,
    binding_address,
    validate_binding,
    validate_retention_binding,
)
from netz_protocol.replication_wire import (
    ReplicationWireError,
    encode_frame,
    make_envelope,
    sign_statement,
    signing_bytes,
    validate_frame,
    validate_statement,
    verify_envelope,
)


ROOT = Path(__file__).resolve().parents[2]


class ReplicationPlacementTest(unittest.TestCase):
    def fixture(self):
        fixture = json.loads(
            (ROOT / "spec/fixtures/blob-placement-1.json").read_text())
        diagnostic = fixture["diagnostic"]
        layout = {
            "seed": diagnostic["seed"],
            "placement": "netz/blob-placement/2",
            "epoch": diagnostic["epoch"],
            "members": [
                {"id": member["id"],
                 "domains": {"owner": member["owner"], "site": member["site"]}}
                for member in diagnostic["members"]
            ],
        }
        return fixture, layout, diagnostic["policy"], bytes.fromhex(
            diagnostic["object_id_hex"])

    def test_shared_known_answer(self):
        fixture, layout, policy, object_id = self.fixture()
        first = layout["members"][0]["id"]
        score_input = dumps({
            "seed": layout["seed"], "member": first, "object": object_id,
            "profile": "netz/blob-placement/2",
        })
        self.assertEqual(fixture["first_score_input_cbor_hex"], score_input.hex())
        for member in layout["members"]:
            self.assertEqual(fixture["scores"][member["id"]],
                             score(layout["seed"], object_id,
                                   member["id"]).hex())
        self.assertEqual(fixture["ranked_ids"],
                         [member["id"] for member in rank(layout, object_id)])
        self.assertEqual(fixture["selected_ids"],
                         [member["id"] for member in place(
                             layout, policy, object_id)])

    def test_layout_order_and_disabled_members_do_not_change_authority(self):
        _, layout, policy, object_id = self.fixture()
        expected = [member["id"] for member in place(layout, policy, object_id)]
        for epoch in (0, 1, 999):
            self.assertEqual(expected, [member["id"] for member in place(
                {**layout, "epoch": epoch}, policy, object_id)])
        for field in ("seed", "placement"):
            invalid = dict(layout)
            del invalid[field]
            with self.assertRaises(ReplicationError):
                rank(invalid, object_id)
        reversed_layout = {**layout, "members": list(reversed(layout["members"]))}
        self.assertEqual(expected, [member["id"] for member in place(
            reversed_layout, policy, object_id)])
        disabled = {**layout, "members": layout["members"] + [
            {"id": "ff" * 32, "enabled": False,
             "domains": {"owner": "mallory", "site": "elsewhere"}}
        ]}
        self.assertNotIn("ff" * 32,
                         [member["id"] for member in rank(disabled, object_id)])

    def test_policy_cannot_promise_more_domain_independence_than_w(self):
        _, layout, _, object_id = self.fixture()
        with self.assertRaises(ReplicationError):
            place(layout, {"replicas": 3, "write_acks": 1,
                           "minimum_domains": {"owner": 2}}, object_id)


class SignedLayoutTest(unittest.TestCase):
    def values(self):
        fixture = json.loads(
            (ROOT / "spec/fixtures/blob-layout-1.json").read_text())
        diagnostic = fixture["diagnostic"]
        public_key = bytes.fromhex(diagnostic["public_key_hex"])
        identity_genesis = genesis(
            [bytes.fromhex(diagnostic["rotation_key_hex"])], 1,
            [public_key],
            bytes.fromhex(diagnostic["revocation_commit_hex"]))
        statement = {
            "v": 1,
            "type": "blob/layout",
            "placement": diagnostic["placement"],
            "seed": bytes.fromhex(diagnostic["seed_hex"]),
            "cluster": bytes.fromhex(diagnostic["cluster_hex"]),
            "epoch": diagnostic["epoch"],
            "previous": bytes.fromhex(diagnostic["previous_hex"]),
            "members": [
                {
                    "authority": bytes.fromhex(member["authority_hex"]),
                    "domains": member["domains"],
                    "enabled": member["enabled"],
                }
                for member in diagnostic["members"]
            ],
            "issued_at": diagnostic["issued_at"],
            "signer": bytes.fromhex(diagnostic["signer_hex"]),
        }
        return fixture, diagnostic, identity_genesis, statement

    def test_shared_layout_address_and_signature_fixture(self):
        fixture, diagnostic, identity_genesis, statement = self.values()
        self.assertIs(statement, validate_layout_statement(statement))
        self.assertIs(statement, validate_statement(statement))
        self.assertEqual(fixture["canonical_cbor_hex"], dumps(statement).hex())
        self.assertEqual(fixture["layout_address_hex"],
                         layout_address(statement).hex())
        if find_spec("cryptography"):
            envelope = sign_statement(
                identity_genesis,
                bytes.fromhex(diagnostic["private_key_hex"]), statement)
            self.assertEqual(fixture["signature_hex"],
                             envelope["signature"].hex())
            self.assertTrue(verify_envelope(envelope))
            self.assertFalse(verify_envelope({
                **envelope, "statement": {**statement, "seed": b"\x01" * 32},
            }))

    def test_schema_is_closed_sorted_and_ascii(self):
        _, _, _, statement = self.values()
        for field in ("seed", "placement"):
            missing = dict(statement)
            del missing[field]
            with self.assertRaises(LayoutError):
                validate_layout_statement(missing)
        for changes in ({"seed": "aa" * 32}, {"seed": b"a"},
                        {"placement": "netz/blob-placement/1"}):
            with self.assertRaises(LayoutError):
                validate_layout_statement({**statement, **changes})
        with self.assertRaises(LayoutError):
            validate_layout_statement({**statement, "extension": True})
        with self.assertRaises(LayoutError):
            validate_layout_statement({
                **statement, "members": list(reversed(statement["members"]))})
        malformed = {**statement, "members": [
            {**statement["members"][0], "domains": {"site": "Montréal"}},
            *statement["members"][1:],
        ]}
        with self.assertRaises(LayoutError):
            validate_layout_statement(malformed)

    def test_chain_reconciliation_and_placement_authorization(self):
        _, _, _, genesis_layout = self.values()
        accepted = reconcile(
            make_layout_state(genesis_layout["cluster"],
                              [genesis_layout["signer"]]),
            genesis_layout)
        self.assertEqual("accepted", accepted["status"])
        state = accepted["state"]
        self.assertEqual("duplicate", reconcile(state, genesis_layout)["status"])
        fork = {**genesis_layout, "issued_at": 1001}
        conflict = reconcile(state, fork)
        self.assertEqual("conflict", conflict["status"])
        self.assertEqual(1, len(conflict["state"]["conflicts"]))
        successor = {
            **genesis_layout,
            "epoch": 1,
            "previous": layout_address(genesis_layout),
            "issued_at": 1001,
        }
        self.assertEqual("accepted", reconcile(state, successor)["status"])
        self.assertEqual("rejected", reconcile(state, {
            **successor, "seed": b"\x01" * 32,
        })["status"])
        self.assertEqual("forked",
                         reconcile(conflict["state"], successor)["status"])
        self.assertEqual("gap", reconcile(state, {
            **successor, "epoch": 2,
            "previous": layout_address(successor),
        })["status"])
        self.assertEqual("rejected", reconcile(state, {
            **successor, "previous": b"\xff" * 32,
        })["status"])

        object_id = b"\xa0" * 32
        retention = {
            "type": "blob/retention",
            "cluster": genesis_layout["cluster"],
            "layout_epoch": 0,
            "object": object_id,
            "replicas": 2,
            "write_acks": 2,
            "minimum_domains": {"owner": 2},
        }
        targets = place(
            placement_layout(genesis_layout),
            {"replicas": 2, "write_acks": 2,
             "minimum_domains": {"owner": 2}}, object_id)
        selected = bytes.fromhex(targets[0]["id"])
        unselected = next(
            member["authority"] for member in genesis_layout["members"]
            if member["authority"].hex() not in {target["id"] for target in targets})
        self.assertTrue(retention_authorized(state, selected, retention))
        self.assertFalse(retention_authorized(
            conflict["state"], selected, retention))
        self.assertFalse(retention_authorized(state, unselected, retention))
        self.assertFalse(retention_authorized(
            state, selected, {**retention, "layout_epoch": 1}))


class PortableBlobTest(unittest.TestCase):
    def test_shared_manifest_and_resume_fixture(self):
        fixture = json.loads(
            (ROOT / "spec/fixtures/blob-manifest-1.json").read_text())
        diagnostic = fixture["diagnostic"]
        split = split_blob(bytes.fromhex(diagnostic["value_hex"]),
                           diagnostic["piece_size"])
        self.assertEqual(fixture["object_id_hex"], split["object_id"].hex())
        self.assertEqual(fixture["manifest_id_hex"], split["manifest_id"].hex())
        self.assertEqual(fixture["manifest_cbor_hex"],
                         dumps(split["manifest"]).hex())
        self.assertEqual(fixture["pieces"], [
            {"id_hex": piece["id"].hex(), "data_hex": piece["data"].hex()}
            for piece in split["pieces"]
        ])
        held = {split["pieces"][index]["id"]
                for index in diagnostic["held_piece_indices"]}
        bitmap = need_bitmap(split["manifest"], held)
        self.assertEqual(fixture["need_bitmap_hex"], bitmap.hex())
        self.assertEqual(fixture["needed_piece_indices"],
                         needed_indices(split["manifest"], bitmap))
        self.assertEqual(bytes.fromhex(diagnostic["value_hex"]),
                         assemble(split["manifest"], {
                             piece["index"]: piece["data"]
                             for piece in split["pieces"]
                         }))


class ApplicationMaterializationBindingTest(unittest.TestCase):
    def binding_fixture(self):
        fixture = json.loads((
            ROOT / "spec/fixtures/application-object-binding-1.json"
        ).read_text())
        diagnostic = fixture["diagnostic"]
        binding = application_binding(
            store_id=diagnostic["store_id"],
            application_key=bytes.fromhex(diagnostic["application_key_hex"]),
            codec=diagnostic["codec"],
            object_id=bytes.fromhex(diagnostic["object_id_hex"]),
            manifest_id=bytes.fromhex(diagnostic["manifest_id_hex"]),
        )
        return fixture, binding

    def test_shared_binding_fixture(self):
        fixture, binding = self.binding_fixture()
        self.assertIs(binding, validate_binding(binding))
        self.assertEqual(fixture["canonical_cbor_hex"], dumps(binding).hex())
        self.assertEqual(fixture["binding_id_hex"],
                         binding_address(binding).hex())

    def test_binding_is_closed_and_must_match_authenticated_retention(self):
        _, binding = self.binding_fixture()
        retention = {
            "type": "blob/retention",
            "object": binding["object"],
            "manifest": binding["manifest"],
        }
        self.assertIs(binding, validate_retention_binding(binding, retention))
        with self.assertRaises(MaterializationError):
            validate_binding({**binding, "extension": True})
        with self.assertRaises(MaterializationError):
            validate_binding({**binding, "store_id": "dvergr/drive/é"})
        with self.assertRaises(MaterializationError):
            validate_retention_binding(
                binding, {**retention, "object": b"\x00" * 32})


class SignedBlobProtocolTest(unittest.TestCase):
    def values(self):
        fixture = json.loads(
            (ROOT / "spec/fixtures/blob-statements-1.json").read_text())
        diagnostic = fixture["diagnostic"]
        public_key = bytes.fromhex(diagnostic["public_key_hex"])
        identity_genesis = genesis(
            [bytes.fromhex(diagnostic["rotation_key_hex"])], 1,
            [public_key], bytes.fromhex(diagnostic["revocation_commit_hex"]))
        split = split_blob(bytes.fromhex(diagnostic["value_hex"]),
                           diagnostic["piece_size"])
        statement = {
            "v": 1,
            "type": "blob/retention",
            "cluster": bytes.fromhex(diagnostic["cluster_hex"]),
            "privacy_domain": bytes.fromhex(diagnostic["privacy_domain_hex"]),
            "revision": 1,
            "manifest": split["manifest_id"],
            "object": split["object_id"],
            "layout_epoch": 7,
            "replicas": 3,
            "write_acks": 2,
            "minimum_domains": {"owner": 2, "site": 2},
            "issued_at": 1000,
            "retain_until": 2000,
            "signer": authority_id(identity_genesis),
        }
        return fixture, diagnostic, identity_genesis, public_key, split, statement

    def test_shared_statement_and_offer_fixture(self):
        values = self.values()
        fixture, diagnostic, identity_genesis, public_key, split, statement = values
        self.assertIs(statement, validate_statement(statement))
        self.assertEqual(fixture["signing_bytes_hex"], signing_bytes(statement).hex())
        envelope = make_envelope(
            identity_genesis, public_key, statement,
            bytes.fromhex(fixture["signature_hex"]))
        offer = {
            "v": 1,
            "type": "blob/offer",
            "session": bytes.fromhex(diagnostic["session_hex"]),
            "attempt": 0,
            "replica": authority_id(identity_genesis),
            "retention": envelope,
            "manifest": split["manifest"],
        }
        self.assertIs(offer, validate_frame(offer))
        self.assertEqual(fixture["offer_cbor_sha256_hex"],
                         sha256(encode_frame(offer)).hexdigest())
        fetch = {
            "v": 1,
            "type": "blob/fetch",
            "session": bytes.fromhex(diagnostic["session_hex"]),
            "attempt": 0,
            "retention": envelope,
        }
        self.assertIs(fetch, validate_frame(fetch))
        self.assertEqual(fixture["fetch_cbor_sha256_hex"],
                         sha256(encode_frame(fetch)).hexdigest())

    def test_statement_and_frame_schemas_fail_closed(self):
        values = self.values()
        fixture, diagnostic, identity_genesis, public_key, split, statement = values
        with self.assertRaises(ReplicationWireError):
            validate_statement({**statement, "extension": True})
        with self.assertRaises(ReplicationWireError):
            validate_statement({**statement, "write_acks": 4})
        envelope = make_envelope(identity_genesis, public_key, statement,
                                 bytes.fromhex(fixture["signature_hex"]))
        bad_manifest = {**split["manifest"], "object": b"\xff" * 32}
        with self.assertRaises(ReplicationWireError):
            validate_frame({
                "v": 1, "type": "blob/offer",
                "session": bytes.fromhex(diagnostic["session_hex"]),
                "attempt": 0, "replica": authority_id(identity_genesis),
                "retention": envelope, "manifest": bad_manifest,
            })

    @unittest.skipUnless(find_spec("cryptography"),
                         "optional crypto dependency is not installed")
    def test_shared_signature_and_tamper_rejection(self):
        fixture, diagnostic, identity_genesis, _, _, statement = self.values()
        signed = sign_statement(
            identity_genesis, bytes.fromhex(diagnostic["private_key_hex"]),
            statement)
        self.assertEqual(fixture["signature_hex"], signed["signature"].hex())
        self.assertTrue(verify_envelope(signed))
        tampered = {**signed, "statement": {
            **signed["statement"], "retain_until": 2001}}
        self.assertFalse(verify_envelope(tampered))


@unittest.skipUnless(find_spec("cryptography"),
                     "optional crypto dependency is not installed")
class SignedRetirementCommandTest(unittest.TestCase):
    @staticmethod
    def identity(seed: int):
        from cryptography.hazmat.primitives.asymmetric.ed25519 import (
            Ed25519PrivateKey,
        )

        private_key = bytes([seed]) * 32
        public_key = (Ed25519PrivateKey.from_private_bytes(private_key)
                      .public_key().public_bytes_raw())
        return private_key, genesis(
            [public_key], 1, [public_key], bytes([seed + 32]) * 32)

    def values(self):
        controller_key, controller = self.identity(1)
        _, retired = self.identity(2)
        replacements = [self.identity(seed) for seed in (3, 4, 5)]
        controller_id = authority_id(controller)
        retired_id = authority_id(retired)
        replacement_ids = [authority_id(value) for _, value in replacements]
        cluster = bytes(range(16))
        split = split_blob(b"portable retirement evidence", 8)

        def members(authorities):
            return sorted(({
                "authority": authority,
                "domains": {"site": f"site-{index}"},
                "enabled": True,
            } for index, authority in enumerate(authorities)),
                          key=lambda member: member["authority"])

        old_layout = {
            "v": 1, "type": "blob/layout", "cluster": cluster, "epoch": 0,
            "placement": "netz/blob-placement/2", "seed": b"\x09" * 32,
            "previous": ZERO_ADDRESS,
            "members": members([retired_id, *replacement_ids[:2]]),
            "issued_at": 900, "signer": controller_id,
        }
        current_layout = {
            **old_layout,
            "epoch": 1,
            "previous": layout_address(old_layout),
            "members": members(replacement_ids),
            "issued_at": 1000,
        }
        state = make_layout_state(cluster, [controller_id])
        state = reconcile(state, old_layout)["state"]
        state = reconcile(state, current_layout)["state"]
        retention = {
            "v": 1, "type": "blob/retention", "cluster": cluster,
            "privacy_domain": bytes(range(16, 32)), "revision": 2,
            "manifest": split["manifest_id"], "object": split["object_id"],
            "layout_epoch": 1, "replicas": 3, "write_acks": 2,
            "minimum_domains": {"site": 2}, "issued_at": 1000,
            "retain_until": 5000, "signer": controller_id,
        }
        retention_envelope = sign_statement(
            controller, controller_key, retention)
        receipts = []
        for (private_key, replica), replica_id in zip(
                replacements, replacement_ids):
            receipts.append(sign_statement(replica, private_key, {
                "v": 1, "type": "blob/stored", "cluster": cluster,
                "privacy_domain": retention["privacy_domain"],
                "retention_revision": 2, "manifest": split["manifest_id"],
                "object": split["object_id"], "layout_epoch": 1,
                "replica": replica_id, "attempt": 1,
                "persistence": "netz/store/fsync/1", "stored_at": 1100,
                "retain_until": 5000, "signer": replica_id,
            }))
        receipts.sort(key=lambda value: value["statement"]["replica"])
        command = {
            "v": 1, "type": "blob/retirement", "cluster": cluster,
            "privacy_domain": retention["privacy_domain"], "revision": 3,
            "manifest": split["manifest_id"], "object": split["object_id"],
            "layout_epoch": 1, "layout": layout_address(current_layout),
            "previous_layout": layout_address(old_layout),
            "replica": retired_id, "retention": retention_envelope,
            "receipts": receipts, "issued_at": 1150, "retire_after": 1200,
            "signer": controller_id,
        }
        options = {
            "layout_state": state, "local_authority": retired_id,
            "controllers": [controller_id],
            "accepted_persistence": {"netz/store/fsync/1"}, "now": 1200,
        }
        return controller, controller_key, command, options

    def test_all_n_replacement_receipts_are_required(self):
        controller, private_key, command, options = self.values()
        envelope = sign_statement(controller, private_key, command)
        verified = verify_retirement_command(envelope, **options)
        self.assertEqual(3, len(verified["targets"]))
        self.assertEqual(3, len(verified["receipts"]))

        incomplete = {**command, "receipts": command["receipts"][:-1]}
        with self.assertRaises(RetirementError):
            verify_retirement_command(
                sign_statement(controller, private_key, incomplete), **options)
        bad_receipt = {
            **command["receipts"][0],
            "signature": b"\x00" * 64,
        }
        tampered = {
            **command,
            "receipts": [bad_receipt, *command["receipts"][1:]],
        }
        with self.assertRaises(RetirementError):
            verify_retirement_command(
                sign_statement(controller, private_key, tampered), **options)
        with self.assertRaises(RetirementError):
            verify_retirement_command(envelope, **{**options, "now": 5000})
        with self.assertRaises(RetirementError):
            verify_retirement_command(envelope, **{
                **options,
                "accepted_persistence": {"netz/store/volatile/1"},
            })
        with self.assertRaises(RetirementError):
            verify_retirement_command(envelope, **{
                **options,
                "layout_state": {**options["layout_state"],
                                 "controllers": set()},
            })


if __name__ == "__main__":
    unittest.main()
