# Initial stable-placement layout format (pre-release work)

Netz has not released its layout format. Replace the prototype format directly;
do not maintain a legacy parser, profile-1-to-2 layout migration, or a parallel
layout-v2 implementation. Keep explicit versioning for future released changes.

The initial signed blob/layout body remains version 1, with its existing
address and signature domains, and gains two required fields:

- placement: the text netz/blob-placement/2, identifying the already tested
  stable-seed scoring algorithm.
- seed: a 32-byte CBOR byte string naming the stable pool seed.

These fields are included in the canonical signed body and layout address.
Absent fields and unknown profiles are rejected; never synthesize a seed from
the epoch or an implicit default. Existing prototype fixtures must be regenerated
together across JVM, JavaScript and Python. No released-byte compatibility is
claimed or required. The /2 scoring identifier distinguishes the existing
experimental score vectors from the earlier epoch-based scoring algorithm;
it does not require a second layout schema.

The seed is converted to lowercase hexadecimal in the existing stable scoring
input. Epoch remains an authority fence and is excluded from scoring. Ordinary
successor layouts preserve the seed. Deliberate seed rotation, if added later,
must be an explicit redistribution operation with copy/retention safeguards.

## Implementation checklist

- Require the fields in shared layout validators and include them in all signed
  fixtures, builders and examples.
- Carry profile and seed through the local placement layout. Use the same
  dispatch/scoring path for initial intent admission, replay, revisions, repair
  and replica-side retention authorization.
- Update Python independently and compare exact canonical bodies, addresses,
  signatures and selected targets across all three runtimes.
- Test missing/unknown fields, seed tampering, epoch-only stability, seed-change
  rejection, membership/domain changes, forks, stale receipts and crash/replay.
- Reuse the existing controller's persisted successor intents and transfers.
  Physical replica movement still needs safe retention and atomic retirement
  checks, even though pre-release wire-format migration is unnecessary.

This document records the intended consolidated format. Runtime activation is
not complete until the checklist is implemented and verified.

Current draft: JVM/CLJS and Python validators require both fields, chain
reconciliation rejects ordinary successor seed changes, and the shared signed
layout fixture has been regenerated. Local placement layouts carry the fields.
The main placement engine still uses epoch scoring; switching that engine and
its pure-model fixtures is the next required step. Do not deploy this draft as
an activated stable-placement protocol.
