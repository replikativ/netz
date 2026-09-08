# Stable-placement layout body (experimental)

This draft defines a candidate body and address, not an activated signed wire
protocol. The current blob statement parser and layout runtime reject it.
Do not advertise support or use it to authorize transfers or retirement yet.

The body preserves all fields and validation rules of the version-1
`blob/layout` statement, except `v` is `2`, and adds exactly:

- `placement`: the text `netz/blob-placement/2`.
- `seed`: a 32-byte CBOR byte string naming the stable pool seed.

Unknown fields and placement profiles are rejected. Member order, authority
lengths, member/domain limits, epoch bounds, and epoch-zero predecessor rules
remain those of version 1. The address is SHA-256 of the UTF-8 domain
`netz/blob-layout/v2` followed by a zero byte and the deterministic CBOR body.
No version-1 address or signature preimage changes.

Placement uses the existing profile-2 scoring algorithm: lowercase hexadecimal
seed text in the score input, together with object ID, member ID and the exact
placement-profile text. Converting the layout seed to hexadecimal is deliberate
and preserves the existing JVM/JavaScript/Python stable-placement vectors.
Epoch is excluded from placement scoring but remains bound by the layout address.

## Required before activation

Specify and implement the signature-envelope version/domain and capability
negotiation. Define the profile-1-to-2 predecessor transition, prohibit implicit
downgrades, and make seed changes explicit redistribution operations. A new
epoch alone must not silently change the seed. Verify these rules during both
live reconciliation and durable replay.

Add shared exact CBOR/address/signature fixtures and independent Python
validation. Route intent placement through one profile dispatch after successful
layout verification; reuse the existing controller successor-intent persistence
and receipt matching. Test forks, stale receipts, authority loss and crash/retry.
The copy planner and advisory retirement API confer no deletion authority.
