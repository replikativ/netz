"""Bounded transport-independent sessions for ``netz/content/1``.

Networking adapters own connection lifecycle and retries.  These classes own
the security-sensitive seam: correlate replies before delivery, expire every
request, and authorize before consulting storage or provider indexes.
"""

from __future__ import annotations

import asyncio
import inspect
import os
from dataclasses import dataclass
from typing import Awaitable, Callable, Mapping, cast

from .content import (
    MAX_PROVIDER_TTL_MS,
    ContentError,
    block_frame,
    dont_have_frame,
    find_frame,
    found_frame,
    validate_cid,
    validate_frame,
    want_frame,
)

Authority = bytes
Authorize = Callable[[Mapping[str, object]], bool | Awaitable[bool]]
GetBlock = Callable[[bytes], bytes | None | Awaitable[bytes | None]]
FindProviders = Callable[[bytes], tuple[bytes, ...] | Awaitable[tuple[bytes, ...]]]


class ContentSessionError(RuntimeError):
    """A content session violated correlation, policy, or resource bounds."""


@dataclass(frozen=True)
class PendingRequest:
    request: bytes
    cid: bytes
    provider: Authority
    deadline_ms: int
    operation: str


@dataclass(frozen=True)
class ContentResult:
    request: bytes
    cid: bytes
    provider: Authority
    data: bytes | None = None
    providers: tuple[Authority, ...] = ()
    ttl_ms: int | None = None
    reason: str | None = None


def _authority(value: object) -> bytes:
    if not isinstance(value, bytes) or len(value) != 32:
        raise ContentSessionError("peer must be a full 32-byte Authority ID")
    return value


class ContentRequester:
    """Correlate bounded direct block requests on identified connections."""

    def __init__(self, *, timeout_ms: int = 5_000,
                 max_outstanding: int = 16) -> None:
        if not 1 <= timeout_ms <= 300_000 or not 1 <= max_outstanding <= 1024:
            raise ValueError("invalid content requester bounds")
        self.timeout_ms = timeout_ms
        self.max_outstanding = max_outstanding
        self.pending: dict[bytes, PendingRequest] = {}

    @staticmethod
    def _now(value: object) -> int:
        if type(value) is not int or value < 0:
            raise ValueError("now_ms must be a non-negative integer")
        return value

    def _open(self, provider: Authority, cid: bytes, *, now_ms: int,
              request: bytes | None, operation: str) -> tuple[bytes, bytes]:
        provider, cid = _authority(provider), validate_cid(cid)
        now_ms = self._now(now_ms)
        if len(self.pending) >= self.max_outstanding:
            raise ContentSessionError("too many outstanding content requests")
        request = os.urandom(16) if request is None else request
        # Frame construction supplies the exact request-width validation.
        want_frame(request, cid)
        if request in self.pending:
            raise ContentSessionError("content request ID is already outstanding")
        self.pending[request] = PendingRequest(
            request, cid, provider, now_ms + self.timeout_ms, operation)
        return request, cid

    def want(self, provider: Authority, cid: bytes, *, now_ms: int,
             request: bytes | None = None) -> dict[str, object]:
        request, cid = self._open(provider, cid, now_ms=now_ms,
                                  request=request, operation="content/want")
        return want_frame(request, cid)

    def find(self, provider: Authority, cid: bytes, *, now_ms: int,
             request: bytes | None = None) -> dict[str, object]:
        request, cid = self._open(provider, cid, now_ms=now_ms,
                                  request=request, operation="content/find")
        return find_frame(request, cid)

    def receive(self, provider: Authority, frame: object, *,
                now_ms: int) -> ContentResult | None:
        provider = _authority(provider)
        now_ms = self._now(now_ms)
        checked = validate_frame(frame)
        if checked["type"] not in {
                "content/block", "content/dont-have", "content/found"}:
            raise ContentSessionError("frame is not a content response")
        request = cast(bytes, checked["request"])
        pending = self.pending.get(request)
        if pending is None:
            # A duplicate or a reply from a retired connection can arrive
            # after completion/expiry. It has no authority to recreate state.
            return None
        if now_ms >= pending.deadline_ms:
            del self.pending[request]
            return None
        if provider != pending.provider or checked["cid"] != pending.cid:
            raise ContentSessionError("content response correlation failed")
        expected = ({"content/block", "content/dont-have"}
                    if pending.operation == "content/want"
                    else {"content/found"})
        if checked["type"] not in expected:
            raise ContentSessionError("content response operation mismatch")
        del self.pending[request]
        if checked["type"] == "content/block":
            return ContentResult(request, pending.cid, provider,
                                 data=cast(bytes, checked["data"]))
        if checked["type"] == "content/found":
            return ContentResult(request, pending.cid, provider,
                                 providers=tuple(cast(list[bytes],
                                                      checked["providers"])),
                                 ttl_ms=cast(int, checked["ttl_ms"]),
                                 reason="providers-found")
        return ContentResult(request, pending.cid, provider,
                             reason="provider-does-not-have-block")

    def expire(self, *, now_ms: int) -> tuple[ContentResult, ...]:
        now_ms = self._now(now_ms)
        expired = [entry for entry in self.pending.values()
                   if now_ms >= entry.deadline_ms]
        for entry in expired:
            del self.pending[entry.request]
        return tuple(ContentResult(entry.request, entry.cid, entry.provider,
                                   reason="timeout") for entry in expired)

    def disconnected(self, provider: Authority) -> tuple[ContentResult, ...]:
        provider = _authority(provider)
        failed = [entry for entry in self.pending.values()
                  if entry.provider == provider]
        for entry in failed:
            del self.pending[entry.request]
        return tuple(ContentResult(entry.request, entry.cid, entry.provider,
                                   reason="disconnected") for entry in failed)


class ContentService:
    """Authorize and answer direct find/want requests without existence leaks."""

    def __init__(self, *, authorize: Authorize, get_block: GetBlock,
                 find_providers: FindProviders = lambda _cid: (),
                 authorization_timeout: float = 5.0,
                 provider_ttl_ms: int = MAX_PROVIDER_TTL_MS) -> None:
        if not all(callable(value) for value in
                   (authorize, get_block, find_providers)):
            raise TypeError("content service hooks must be callable")
        if not 0 < authorization_timeout <= 60:
            raise ValueError("invalid authorization timeout")
        if (type(provider_ttl_ms) is not int
                or not 0 <= provider_ttl_ms <= MAX_PROVIDER_TTL_MS):
            raise ValueError("invalid provider TTL")
        self.authorize = authorize
        self.get_block = get_block
        self.find_providers = find_providers
        self.authorization_timeout = authorization_timeout
        self.provider_ttl_ms = provider_ttl_ms

    async def _call(self, function, *args):
        value = function(*args)
        if inspect.isawaitable(value):
            return await value
        return value

    async def receive(self, peer: Authority, frame: object) -> dict[str, object]:
        peer = _authority(peer)
        checked = validate_frame(frame)
        frame_type = checked["type"]
        if frame_type not in {"content/find", "content/want"}:
            raise ContentSessionError("service accepts only content requests")
        context = {"op": "content/find" if frame_type == "content/find"
                   else "content/read", "peer_authority": peer,
                   "cid": checked["cid"], "frame": checked}
        try:
            allowed = await asyncio.wait_for(
                self._call(self.authorize, context),
                timeout=self.authorization_timeout)
        except Exception as error:
            raise ContentSessionError("content authorization failed") from error
        if allowed is not True:
            raise ContentSessionError("content authorization denied")
        request, cid = cast(bytes, checked["request"]), cast(bytes, checked["cid"])
        try:
            if frame_type == "content/find":
                providers = await self._call(self.find_providers, cid)
                return found_frame(request, cid, providers,
                                   ttl_ms=self.provider_ttl_ms)
            data = await self._call(self.get_block, cid)
            return (dont_have_frame(request, cid) if data is None
                    else block_frame(request, cid, data))
        except (ContentError, ContentSessionError):
            raise
        except Exception as error:
            raise ContentSessionError("content service backend failed") from error
