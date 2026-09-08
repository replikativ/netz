# Deployment models and topology

Status: design direction, not additional runtime guarantees. See the
[backend roadmap](blob-backend-roadmap.md) for implemented boundaries and gates.

## One data plane, different service contracts

Netz should share authenticated, bounded, content-verified transfer across these
deployments without treating every reachable peer as an equally durable replica.

| Use case | Durable placement | Latency strategy | Failure contract |
| --- | --- | --- | --- |
| Small cloud storage pool | Authorized capacity across hosts/sites | Nearby verified reads; bounded background repair | Explicit W acknowledgements and domain minima; block or report pending when unmet |
| Independent parties sharing database snapshots | Policy-approved operators with independent retention | Local materialized snapshot; fetch missing immutable data | Previously replicated snapshots remain usable, not necessarily the latest root |
| Personal devices, including a home Raspberry Pi | Explicit trusted devices, often intermittently online | Local-first access and opportunistic transfer | Offline progress is pending replication, not a remote durability acknowledgement |
| Collaborative applications and Git-like histories | Durable home pool plus optional edge caches | Local state, root notifications and missing-content fetch | Application-specific merge/conflict semantics; blob replication does not select a winning root |

Datahike/Yggdrasil snapshot consumers and Simmis/Spindel-style collaboration can
use this split. Urd may fence writers or replicate placement authority later;
neither ordinary blob reads nor all P2P participants need consensus membership.

## Keep three decisions separate

1. **Durable placement:** stable, authorized owners selected under hard policy.
   Include eligible operators, allowed locations, replication count and failure
   domains. Capacity weights are a future versioned placement input, not a local
   reinterpretation of today's scores.
2. **Read and transfer scheduling:** choose among authorized reachable sources
   using observed latency, throughput, load and transfer cost. Verify content and
   fall back on failure. Nearby caches help but do not count toward W unless they
   satisfy the same storage, retention and authority contract.
3. **Overlay connectivity:** maintain useful connections for discovery,
   dissemination and transfer. Prefer locality while preserving independent
   cross-site paths. A connection graph is neither an ownership map nor evidence
   of independent storage.

Hard constraints precede soft optimization. Never trade away required site or
operator diversity to improve measured latency. Likewise, do not move durable
ownership on each RTT fluctuation: use stable signed placement, explicit capacity
planning, hysteresis and a migration budget. Measurements are scheduling hints,
not authority to remove a peer or delete its data. An unreachable site is not
proof of lost bytes.

## Make latency and durability tradeoffs visible

For N=3/W=2, two local acknowledgements do not promise survival of that site's
loss. Requiring two sites at acknowledgement forces a remote durable write onto
the completion path. Alternatively return local acceptance immediately and expose
a separate bounded wait for cross-site durability. Never label those outcomes
identically. Reads can use one accessible verified immutable copy; freshness of
the selected mutable root remains a separate application policy.

Replication thresholds are mechanisms, not an SLA by themselves. A service
contract must also specify retention, repair assumptions, available spare capacity,
operator trust, key availability, allowed outage and acknowledgement semantics.
Report local acceptance, W/domain coverage, N convergence, last successful audit,
repair backlog and blocked repair separately. A past receipt is not continuous
proof of current availability.

## Scale out without pretending the current pool is unbounded

The current placement model is bounded to 32 members and five replicas. First
make one such pool reliable. A larger P2P population can connect applications,
caches and independent pools without giving every participant every pool's
membership or inventory. Federation/sharding will need explicit object-to-pool
routing, authority, discovery and cross-pool migration contracts. A DHT could
locate providers later; it would not establish authorization or durability.

Before adding that layer, measure inventory size, controller memory, scheduling
cost and repair traffic. Do not claim federation is implemented or that the
current per-object bounded selection algorithm is a global placement planner.

## Validation campaign

Extend deterministic simulation with sites/operators, directed link latency and
bandwidth, partitions, intermittent devices, capacity and foreground load. Keep
logical time and a fixed seed; verify invariants before optimizing performance.

* A fast single site cannot satisfy a two-site acknowledgement policy.
* Loss of a site permits reads when a verified reachable copy exists; it does not
  create replacement authority or retirement permission.
* RTT changes alter source choice without changing durable placement.
* Full disks and slow links bound queued bytes and expose pending/blocked work;
  endangered objects take priority without permanently starving other work.
* Rejoining devices refresh authority; stale receipts and roots do not overwrite
  current control state, while authorized old content can aid recovery.
* Measure p50/p95/p99 completion latency, inter-site bytes, movement per layout,
  time below W/N, repair convergence and queue maxima. Compare against a simple
  non-adaptive baseline; simulated results are not production SLA evidence.

Implementation order remains: qualify local durability, integrate authorized
migration and retention, run the three-peer failure/replacement demo, then add
topology-aware scheduling and simulated performance comparisons. No wire or
placement-profile change is authorized merely by this design document.
