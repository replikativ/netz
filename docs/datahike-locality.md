# Datahike replica locality experiment

Status: developer cost model, not a new wire-profile guarantee.

The Datahike integration should separate two kinds of copies:

1. **Producer-planned durable replicas** are authorized by a signed layout and
   retention policy. They count toward `W` only after durable, verified receipt
   persistence. Hard operator/site constraints take precedence over latency.
2. **Consumer-populated caches** fetch immutable snapshot content from a nearby
   verified provider. They may serve later reads and announcements, but remain
   evictable and never count toward `W` without separately accepting the same
   retention contract.

This allows a P2P deployment to adapt its read locality without letting request
traffic silently rewrite its availability policy. It also avoids sending raw
per-user demand telemetry to the producer. A later controller may consume
coarse, delayed regional demand aggregates, but should apply minimum observation
windows, hysteresis and movement budgets before signing a new layout.

Run the deterministic comparison with:

```sh
clojure -M:locality-sim
```

The model compares today's hash-first placement with a producer plan using a
static regional demand forecast. It reports acknowledgement and all-target
latency, demand-weighted source RTT, cache-fill traffic, durable versus cache
copy counts, and the copies remaining after any one selected site disappears.
It deliberately does not model query execution or bulk-transfer time. The
numbers are planning evidence, not an SLA or benchmark.

## Policy distinction exercised by the model

The experimental placement profile keeps the selected `N` targets distinct
from the `W` receipts needed for confirmation. Those are related but different
requirements. For example:

```clojure
{:replicas 3
 :write-acks 2
 :placement-minimum-domains {"site" 3 "owner" 3}
 :ack-minimum-domains {"site" 2 "owner" 2}}
```

This layout survives any one selected site disappearing while still allowing
the writer to return after two independently placed receipts. Deterministic
placement additionally proves that at least one exactly-W subset of the chosen
N targets can meet all acknowledgement minima.

Before making locality part of the signed standard, evaluate three
producer-side options:

- restrict each database to a statically curated replica pool and retain
  ordinary rendezvous hashing;
- add stable capacity/locality weights to a new placement profile;
- sign explicit per-database service-region intent and deterministically plan
  targets from it.

The first option is the smallest Datahike Server experiment. The third is the
clearest eventual public interface. Weighted rendezvous is useful for large
heterogeneous pools, but it introduces more movement and interoperability rules
than this first deployment needs.

## Coordination node

An optional Netz node can provide a stable `wss` seed, relay signed root
publications, accept authorized provider announcements, and answer bounded
content-provider searches. It is a rendezvous and routing accelerator, not a
root authority or durability controller. Deploy at least two seeds for an
availability-oriented setup; already connected peers continue direct transfer
when one seed is absent.

The first server integration should expose explicit roles:

- `writer`: publish signed durable Datahike commit references and plan replicas;
- `replica`: retain complete snapshots and serve local Datahike reads;
- `cache`: fetch and serve verified immutable blocks without durability credit;
- `gateway`: seed, relay and provider-index service, optionally without data.

No role is enabled by merely placing Netz on the server classpath.

## Related systems

The closest established architecture is an object store plus an edge cache.
The producer chooses durable regions and acknowledgement behavior; demand fills
nearby caches which can disappear without changing the durability claim. Google
Cloud similarly separates bucket-region replication from an optional Anywhere
Cache, while Azure distinguishes geo-redundant copies from copies exposed for
read access. S3 exposes replication lag and missed-threshold metrics rather than
pretending asynchronous cross-region completion happened with the source write.

Ceph's CRUSH map is the stronger model for our producer plane: placement policy
maps replicas across explicit device/location failure domains without consulting
individual readers. Netz needs the same separation between replica-set diversity
and write acknowledgement diversity, but applies it to authenticated independent
operators rather than one storage administrator's OSD fleet.

IPFS is the closer model for our consumer plane. Bitswap requests CID-addressed
blocks from connected providers and falls back to content routing; pinning is a
separate persistence decision. IPFS Cluster then adds an orchestration plane
which allocates and repairs a replicated global pin set. Netz currently has much
less discovery and swarming machinery, but stronger application-specific signed
references, explicit retention statements and durable receipt evidence in a
small authorized pool.

BitTorrent is optimized for a temporary demand-driven swarm. Hash-verified
pieces and rarest-first selection make downloaders useful uploaders, but the
swarm itself does not promise long-term retention, choose an authoritative
mutable database root, or prove failure-domain placement. Netz can later borrow
parallel block scheduling and provider scoring without borrowing those missing
service guarantees.

DynamoDB global tables show the mutable-database alternative: asynchronous
multi-region replication favors write latency, while strong multi-region mode
puts remote agreement on the write path. Our initial Datahike profile is simpler:
one authoritative writer produces immutable commits, and callers explicitly
choose whether local acceptance or remote `W` confirmation completes a write.

Primary references:

- [IPFS Bitswap](https://docs.ipfs.tech/concepts/bitswap/),
  [data lifecycle](https://docs.ipfs.tech/concepts/lifecycle/), and
  [IPFS Cluster](https://docs.ipfs.tech/install/server-infrastructure/)
- [BitTorrent protocol economics and piece selection](https://www.bittorrent.org/bittorrentecon.pdf)
- [Ceph CRUSH maps](https://docs.ceph.com/en/quincy/rados/operations/crush-map/)
- [Google Cloud Storage locations](https://docs.cloud.google.com/storage/docs/bucket-locations),
  [Azure redundancy](https://learn.microsoft.com/en-us/azure/storage/common/storage-redundancy), and
  [S3 Replication Time Control](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-time-control.html)
- [DynamoDB global-table consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html)

## Incremental Datahike Server path

1. Add Netz only to Datahike's `:http-server` alias so the server uberjar can
   enable it without adding it to embedded Datahike applications.
2. Start with a configured replica pool, static service regions and two or more
   redundant `wss` gateways. This needs no global DHT.
3. Publish signed commit references from the durable Datahike listener and
   expose `announced`, `queryable` and `durable` status independently.
4. Install complete verified snapshots on replica-role servers and route reads
   to a local/near queryable commit. Cache-role servers retain only best-effort
   verified content.
5. Split placement-domain and acknowledgement-domain constraints in the
   experimental wire profile, then add automatic repair/replacement.
6. Add stable capacity weights or producer-declared service-region intent only
   after the static-pool experiment produces real latency, movement and storage
   measurements.

Live demand should influence provider selection immediately, cache retention
over a moderate time window, and durable placement only through coarse delayed
aggregates. Individual requests must never be allowed to trigger signed replica
migration directly: that leaks behavior, enables adversarial thrashing and can
trade away independent failure domains for transient latency.
