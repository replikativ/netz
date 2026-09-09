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
The main placement engines now require the profile and seed and use stable
scoring for intent admission, controller revisions and replica authorization.
The placement fixture has also been regenerated and the pure-model tests use
explicit seeds. Full release and interoperability gates remain required before
this draft is ready for deployment.

## Verification and remaining scope

The consolidated engine passed the full JVM suite (512 tests, 11,628
assertions), full Node suite (376 tests, 10,763 assertions), and 114 isolated
installed-wheel tests. The fresh mixed-runtime run passed 13 tests and 120
assertions using rebuilt Node clients and the installed Python wheel. These
mixed tests cover content, synchronization, dissemination and JVM/Python blob
transfer; they do not prove live three-runtime signed-layout reconfiguration.
Shared fixtures separately check the exact signed body, address, signature,
scores and target ordering.

One earlier full JVM run reported an error whose detailed output was truncated;
the subsequent complete logged run passed. The first error remains unexplained,
not a demonstrated fix. Retain full logs for subsequent release runs.

Controller transition coverage now includes restart from the persisted successor,
rejection of an old-layout receipt, and confirmation by a fresh successor
receipt. The live JVM Kabel/Noise test also advances a signed layout and obtains
a fresh receipt from an already installed replica using layout-based admission.
This exposed and fixed an overly restrictive sender phase check in JVM/CLJS and
Python: the protocol allows an installed replica to answer an offer directly.

An N=3/W=2 controller recovery test now uses four distinct signing identities
and replaces one member. It checks persisted successor recovery, rejection of
old receipts and a removed member, quorum after two fresh receipts without
premature completion, repair directed only to the missing third target, and
completion recovery on a second restart. Its journal is in-memory and offer
dispatch is captured: this establishes controller accounting, not process-crash
durability or multi-device availability.

A live JVM process test now keeps four replica peers running while forcibly
terminating an owned writer JVM after its successor intent is fsync-persisted
but before successor offers are dispatched. A fresh JVM reopens that journal,
observes the successor as under-replicated despite the old receipts, and repairs
the new three-member target set over Kabel/Noise. Independent scrubs verify all
three successor copies and that the removed replica still retains its copy.
The receiver peers share a parent JVM and all stores are on one host: this
tests writer-process loss, not independent-device or power-loss durability.
The fixture supplies the authorized layout chain as local configuration; it
does not test layout-distribution recovery.

Remaining work includes retirement/protection safety, target connection
management and independent-device qualification. The current tests do not
establish safe automatic retirement.

The live process fixture gives each simulated independent owner a distinct
discovery group and waits for the exact selected Authorities, not merely a
connection count. Production placement must be reconciled with discovery-group
limits and connection budgets: selecting a storage target does not by itself
reserve a connection to it. Explicit target reachability/connection management
remains part of the backend integration work.
