"""Public Python API for the Netz protocol reference core.

The package is deliberately transport-independent in draft-00. Applications
can use the deterministic CBOR codec and ordered synchronization receiver while
identity and WebSocket negotiation remain optional layers.
"""

from netz_codec import CBORError, dumps, loads
from netz_sync import (
    JOIN_PROFILE,
    ROOT_PROFILE,
    CommitReceipt,
    JoinSetProfile,
    RootLogProfile,
    SyncError,
    SyncProfile,
    SyncReceiver,
    SyncResponse,
    SyncSender,
    open_frame,
)
from .framing import (
    FramingError,
    KABEL_CBOR_SERIALIZER_ID,
    decode_kabel,
    decode_websocket,
    encode_kabel,
    encode_websocket,
)
from .identity import (
    IdentityError,
    authority_id,
    genesis,
    genesis_bytes,
    identity_record_bytes,
    routing_id,
)
from .publication import (
    PublicationError,
    decode_publication,
    encode_publication,
    sign_publication,
    signing_bytes as publication_signing_bytes,
    verify_publication,
)
from .dissemination import OrderedReceiver, Publisher, ReceiveResult
from .replication import (
    ReplicationError,
    domain_counts,
    place as place_replicas,
    rank as rank_replicas,
    score as replica_score,
)
from .blob import (
    BlobError,
    assemble as assemble_blob,
    manifest_address as blob_manifest_address,
    need_bitmap as blob_need_bitmap,
    needed_indices as blob_needed_indices,
    object_address as blob_object_address,
    piece_address as blob_piece_address,
    split_blob,
)

__all__ = [
    "CBORError",
    "BlobError",
    "CommitReceipt",
    "FramingError",
    "IdentityError",
    "JOIN_PROFILE",
    "JoinSetProfile",
    "KABEL_CBOR_SERIALIZER_ID",
    "OrderedReceiver",
    "PublicationError",
    "Publisher",
    "ReceiveResult",
    "ReplicationError",
    "ROOT_PROFILE",
    "RootLogProfile",
    "SyncError",
    "SyncProfile",
    "SyncReceiver",
    "SyncResponse",
    "SyncSender",
    "authority_id",
    "assemble_blob",
    "blob_manifest_address",
    "blob_need_bitmap",
    "blob_needed_indices",
    "blob_object_address",
    "blob_piece_address",
    "dumps",
    "decode_kabel",
    "decode_websocket",
    "decode_publication",
    "encode_kabel",
    "encode_websocket",
    "encode_publication",
    "genesis",
    "genesis_bytes",
    "identity_record_bytes",
    "loads",
    "open_frame",
    "place_replicas",
    "publication_signing_bytes",
    "rank_replicas",
    "replica_score",
    "domain_counts",
    "routing_id",
    "sign_publication",
    "split_blob",
    "verify_publication",
]
