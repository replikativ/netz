# Snapshot replication policy (experimental)

Signed announcements, complete local snapshots and durable remote retention are
separate states. This increment composes the existing blob receipt, outbox and
lifecycle mechanisms; it does not change ordinary Konserve operations.

## Two adapters, one transfer boundary

The optional `:replica-demo` gate exercises Datahike and Yggdrasil's native Git
adapter. `netz.examples.snapshot-transfer/SnapshotTransfer` has three blocking
JVM operations: select the exact immutable closure, validate its signed root
binding, and validate/open the installed native snapshot without moving refs.
Both adapters share the catalogue and konserve-sync installation loop.

Datahike retains its primary-PSS-only restriction. Git exports a self-contained
native pack including ancestry, imports with `index-pack --strict`, and checks
connectivity. Git must be installed, object formats must match, and the source
owner must prevent concurrent GC. Packs are capped at 900 KiB **after creation**:
this small trusted-repository example is not a process/resource sandbox. LFS
payloads and submodule repositories are outside the Git object closure. Pack
bytes need not be canonical across Git versions; immutable conflicts fail closed.

This is not yet Geschichte support or a new Yggdrasil protocol. The interface
remains in examples while asynchronous operation and GC ownership are settled.

## Complete-snapshot durability

`snapshot-archive/prepare` packages the signed catalogue and its exact blocks
as one chunked blob and an existing application-object binding.
`stage-local!` installs that archive in the controller's blob repository at the
fsync boundary. Use the split's manifest/object IDs in a signed finite
`blob/retention` intent, then call `controller-runtime/begin!`. The receiver's
existing lifecycle must durably authorize retention before admitting offers.

The application must authenticate the archive binding. Existing
`materialization/read-installed!` and archive `restore` recover the catalogue;
`snapshot-store/open!` still verifies publisher, stream and signed root before
decoding application values. The archive is bounded to 5 MiB total; the
catalogue's 1 MiB per-block and 1,024-object limits also remain.

One archive means W receipts cover W **complete snapshots on the same peers**.
Per-object quorums on unrelated peer sets would not establish that property.
The tradeoff is reduced cross-snapshot deduplication. Keeping a newer snapshot
does not automatically preserve every older snapshot.

## Confirmation and writer admission

`controller-runtime/durability-status` reports `:confirmed`,
`:under-replicated`, `:pending`, `:inactive`, `:missing` or `:unavailable`.
It applies count/domain policy and excludes pending generations, expired
retention and stale/forked layouts. `:complete?` requires all selected targets.
Local storage counts only as an explicitly selected, receipted replica.

`await-durable!` waits asynchronously, with bounded waiter count. A timeout
returns `:last-status` and current evidence; it neither undoes local work nor
cancels replication. Confirmation is not authoritative root selection or proof
that all replicas are reachable now.

`netz.replication.admission` provides pure reservation, object binding and
confirmation transitions with pending-count, byte and age budgets. Applications
must serialize and persist reservations **before** invoking their writer,
restore outstanding reservations before reopening admission, and reserve a
conservative maximum export size. Ambiguous writes require recovery, not
cancellation. An age limit blocks new writes; it cannot remove already exposed
unconfirmed writes. No transparent Datahike writer wrapper is installed here.

## Active audit and repair

Run the existing `repair-scheduler-runtime` for known-missing targets, plus:

```clojure
(def auditor
  (audit-runtime/start!
    {:controller controller
     :scan-ms 5000
     :audit-opts {:interval-ms 300000 :max-per-scan 4 :max-entries 10000}}))
```

After a grace period, audits request freshly fenced durable receipts even for
previously complete objects. They withdraw old receipt credit, persist a new
attempt and re-offer to the exact selected peer. Receivers verify held pieces
and request missing/corrupt ones. Known-missing repair scheduling retries failed
attempts with backoff. Stale observations cannot invalidate newer evidence.
Pending first-audit outbox writes are discoverable retry work too: a storage
failure cannot leave a formerly complete object permanently outside repair.

Audit work is sequential/bounded and rotates targets. Scheduling is volatile:
restart resets its grace period, not durable attempts/receipts. Watch runtime
failure statistics; capacity overflow stops auditing instead of silently
ignoring excess work. Leases still need explicit renewal.

Auditing temporarily reduces confirmation credit, without asserting data loss.
With W=N, routine audits can delay confirmations; use N>W where appropriate.
Receipts remain trusted-custodian evidence, not Byzantine storage proofs.

## Permanent failure and replacement

A returning replica or missing piece can be repaired in place. A permanently
dead target needs an authorized, hash-chained successor layout and higher full
retention revisions for affected archives. Existing `controller-runtime/revise!`
persists the new intent before offers and requires freshly fenced receipts.
Count, domain and privacy constraints must continue to hold.

An operator or explicitly delegated placement controller can automate these
decisions. Gossip alone never authorizes replacement. This increment does not
autonomously sign layouts, choose new parties, renew leases or delete retired
copies. Old copies retain their previous finite lease; existing lifecycle/GC
enforces it. Repair always needs a surviving authorized source, locally held
or restored through the existing read-repair path.

## Tests and remaining gates

Coverage includes persistence before re-offer, stale observations, obsolete
receipt rejection after restart, expiry/layout conflicts, timeout semantics,
admission limits and audit fairness. The secure gate removes a real replica
piece and checks repair over Kabel/Noise. The adapter gate checks native Git
offline reads, Datahike reconnect/offline queries and fsync archive reopen.

This is not yet a killed-process/power-loss campaign, a many-node deployment
benchmark, a persisted application admission wrapper, or an unattended
placement manager.
