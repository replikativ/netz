# Placement-target connectivity

Placement selects storage Authorities; ordinary overlay discovery does not
necessarily connect to them. The local connectivity coordinator bridges this
gap by giving current missing replicas dial priority. It does not change
placement, admit transfers, or establish durability.

## Integration

After controller readiness, start `netz.replication.connectivity-runtime/start!`
with `{:overlay overlay-runtime :controllers [controller ...]}`. Exactly one
coordinator owns the aggregate demand for an overlay. Keep the existing repair
scheduler running: a refused offer is retried through the controller's durable
outbox, not queued by the connectivity coordinator.

The coordinator snapshots missing targets once per second. Only persisted,
current-layout, unexpired controller intents contribute targets. Each demand
contains the full Authority and its routing projection. An address-book entry
must match that full Authority to receive priority. The secure transport still
authenticates the actual endpoint; discovery is not authorization.

Demand expires after five seconds without refresh. Await `stop!` before starting
a replacement coordinator. Stop attempts to enqueue an empty snapshot; expiry
also removes priority if the event queue refuses that release. Existing sockets
are not closed, and a dial already in progress is not cancelled. The transfer
controller independently rechecks eligibility before admitting an offer.

## Bounds and diagnostics

The default demand limit is 32 Authorities, matching the current bounded layout
pool. Aggregate overflow is refused, not silently truncated. Routing projection
collisions also reject the snapshot. The coordinator records submissions,
refusals and snapshot errors; submission means only event-queue admission.

`netz.membership/connection-demand-status` reports routing state: unknown route,
Authority mismatch, missing address, backoff, group limit, capacity, ready,
dialing, connected or expired. Connected here is membership state, not a signed
storage acknowledgement. Hard limits and group diversity are unchanged; pending
dials now reserve group capacity, including within a single parallel batch.

This increment does not evict ordinary connections to make room for demanded
replicas, bypass outbound targets, discover addresses from signed layouts, or
promise immediate repair after connection establishment. Operators must provide
reachable configured/discovered addresses and sufficient connection capacity.

## Qualification still required

Pure policy and controller/coordinator lifecycle tests cover the initial wiring,
including full-queue refusal and nonblocking shutdown. The CLJS build loads the
coordinator and passes the Node suite (380 tests, 10,795 assertions); controller
and coordinator JVM tests pass (16 tests, 128 assertions).

The secure integration gate additionally presents two authenticated Noise peers
to one available outbound slot and verifies that the current placement target,
not the ordinary candidate, is connected. Controller tests verify that expiry,
layout conflict, successor-layout advancement and shutdown remove stale targets.

Before final release qualification, exercise sustained high-cardinality intent
loads and concurrent coordinator replacement. The present test proves
demand-driven connection selection, but it does not claim automatic eviction of
an ordinary connection that already occupies every configured slot.
