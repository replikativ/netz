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

__all__ = [
    "CBORError",
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
    "ROOT_PROFILE",
    "RootLogProfile",
    "SyncError",
    "SyncProfile",
    "SyncReceiver",
    "authority_id",
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
    "publication_signing_bytes",
    "routing_id",
    "sign_publication",
    "verify_publication",
]
