import unittest

from netz_protocol.content import block_frame, raw_cid
from netz_protocol.content_session import (
    ContentRequester,
    ContentService,
    ContentSessionError,
)


PEER = bytes([1]) * 32
OTHER = bytes([2]) * 32
REQUEST = bytes(range(16))
DATA = b"portable session block"
CID = raw_cid(DATA)


class ContentRequesterTest(unittest.TestCase):
    def test_only_correlated_verified_response_is_delivered(self):
        requester = ContentRequester(timeout_ms=100)
        want = requester.want(PEER, CID, now_ms=10, request=REQUEST)
        result = requester.receive(
            PEER, block_frame(REQUEST, CID, DATA), now_ms=20)
        self.assertEqual("content/want", want["type"])
        self.assertEqual(DATA, result.data)
        self.assertFalse(requester.pending)

    def test_cross_peer_late_duplicate_and_disconnect_fail_closed(self):
        requester = ContentRequester(timeout_ms=100)
        requester.want(PEER, CID, now_ms=10, request=REQUEST)
        with self.assertRaises(ContentSessionError):
            requester.receive(OTHER, block_frame(REQUEST, CID, DATA), now_ms=20)
        self.assertEqual("timeout", requester.expire(now_ms=110)[0].reason)
        with self.assertRaises(ContentSessionError):
            requester.receive(PEER, block_frame(REQUEST, CID, DATA), now_ms=112)

        requester.want(PEER, CID, now_ms=200, request=REQUEST)
        self.assertEqual("disconnected",
                         requester.disconnected(PEER)[0].reason)

    def test_outstanding_table_is_bounded(self):
        requester = ContentRequester(max_outstanding=1)
        requester.want(PEER, CID, now_ms=0, request=REQUEST)
        with self.assertRaises(ContentSessionError):
            requester.want(OTHER, CID, now_ms=0, request=bytes([9]) * 16)

    def test_find_and_want_responses_cannot_be_confused(self):
        requester = ContentRequester()
        requester.find(PEER, CID, now_ms=0, request=REQUEST)
        with self.assertRaisesRegex(ContentSessionError, "operation mismatch"):
            requester.receive(
                PEER, block_frame(REQUEST, CID, DATA), now_ms=1)

    def test_many_in_flight_requests_may_complete_in_any_order(self):
        requester = ContentRequester(max_outstanding=64)
        expected = {}
        for index in range(64):
            data = f"block-{index}".encode()
            cid = raw_cid(data)
            request = index.to_bytes(16, "big")
            requester.want(PEER, cid, now_ms=0, request=request)
            expected[request] = (cid, data)
        for request in reversed(tuple(expected)):
            cid, data = expected[request]
            result = requester.receive(
                PEER, block_frame(request, cid, data), now_ms=1)
            self.assertEqual(data, result.data)
        self.assertFalse(requester.pending)


class ContentServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_authorizes_before_read_and_returns_verified_block(self):
        events = []

        async def authorize(context):
            events.append(("authorize", context["op"]))
            return True

        async def get_block(cid):
            events.append(("read", cid))
            return DATA

        service = ContentService(authorize=authorize, get_block=get_block)
        requester = ContentRequester()
        want = requester.want(PEER, CID, now_ms=0, request=REQUEST)
        response = await service.receive(OTHER, want)
        self.assertEqual(["authorize", "read"], [event[0] for event in events])
        self.assertEqual(DATA,
                         requester.receive(PEER, response, now_ms=1).data)

    async def test_denial_does_not_consult_storage_or_provider_index(self):
        calls = []
        service = ContentService(
            authorize=lambda _context: False,
            get_block=lambda _cid: calls.append("read"),
            find_providers=lambda _cid: calls.append("find"),
        )
        requester = ContentRequester()
        want = requester.want(PEER, CID, now_ms=0, request=REQUEST)
        with self.assertRaises(ContentSessionError):
            await service.receive(OTHER, want)
        self.assertEqual([], calls)

    async def test_find_returns_bounded_provider_hints(self):
        service = ContentService(
            authorize=lambda _context: True,
            get_block=lambda _cid: None,
            find_providers=lambda _cid: (OTHER,),
            provider_ttl_ms=10_000,
        )
        requester = ContentRequester()
        find = requester.find(PEER, CID, now_ms=0, request=REQUEST)
        result = requester.receive(
            PEER, await service.receive(OTHER, find), now_ms=1)
        self.assertEqual((OTHER,), result.providers)
        self.assertEqual(10_000, result.ttl_ms)


if __name__ == "__main__":
    unittest.main()
