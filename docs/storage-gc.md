# Application GC and Netz replica retention

Status: integration design, not an implemented distributed collector. Observations
refer to the sibling Datahike/Yggdrasil sources, potentially ahead of Netz's deps.

## Three separate decisions

| Decision | Authority | What it may remove |
| --- | --- | --- |
| Application reachability | Application owner | Snapshots and logical references it no longer retains |
| Pool retention | Authorized retention owners | Their own claims on exact blobs/closures, subject to leases and pins |
| Replica retirement | Placement controller | Surplus physical copies after verified replacement and protection checks |

A local Datahike sweep must not become a pool-wide delete broadcast. A layout
update must not drop an application's retained snapshot. One owner's release
cannot erase a deduplicated blob another owner still retains. Store deletion,
logical-key deletion and CID-byte deletion need separate namespace/authority rules.

## Datahike

`datahike.gc/gc-storage!` walks all branch heads and retained history. Heads survive
the cutoff; the default epoch cutoff keeps history on active branches.
`remove-before` changes history retention, not replication.

`datahike.gc-guard/safe-point` protects the values-before-pointer write window from
the local collector. It is in-memory and assumes writers/collectors share a JVM.
It is not a distributed lease, nor a pin on already-existing old snapshot objects
for a remote reader/exporter. Root-last publication alone does not prevent GC
before the root lands.

The sibling also exposes `gc/reachable-store-refs`: a mark-only live set for
`:db.type/store-ref` values across branches and retained history. This is useful
for attachment-blob retention, but is not the complete database/index closure or
a Netz CID inventory. Resolve logical keys via authenticated materialization
bindings and release only the owning application's claims. Check release/dependency
compatibility before using this API; it is not wired into Netz here.

## Yggdrasil

`GarbageCollectable` exposes `gc-roots` and `gc-sweep!`. The coordinator delegates
to native collectors. Its Datahike adapter explicitly ignores candidate snapshot
IDs and calls Datahike's own collector with the cutoff. A registry entry or
arbitrary coordinator candidate set therefore does not install a native storage
pin. A reachability scan is not an atomic view of all remote parties either.

Use Yggdrasil to select snapshots and the finite transfer adapter to enumerate
their full closure. Never point a native application sweep at Netz's controller,
retention, manifest or piece namespace.

## Integration order

1. Protect the exact source snapshot before selecting/enumerating it until capture
   has verified and durably staged the full closure. Today callers must exclude
   concurrent source GC. A temporary native branch is a possible future Datahike
   pin, but needs creation/export/cleanup race tests before adoption.
2. Persist a retention/export claim covering the catalogue, root, manifests and
   pieces before a distributed collector can consider them. A listener event or
   signed root alone is neither a storage pin nor a replication acknowledgement.
3. Protect partial imports until their verified closure and native root are live.
   Crash leftovers need durable resumable claims or explicit conservative expiry.
4. Track W-qualified write completion separately from N-target convergence.
   Migration retirement requires N replacement coverage plus current protection
   and authority checks; it does not release namespace retention claims.
5. Application collection releases only that application's claim through an
   authenticated, versioned, replay-safe operation. Physical collection requires
   no remaining owner claim, transfer protection or migration obligation.
   Unknown/unreachable owners do not mean zero references. Finite leases must
   explicitly specify renewal, offline duration and expiry consequences.

A source collecting local history should not erase an independently retained
remote backup. Conversely, local purge plus GC is not confirmed remote erasure.
Offline/untrusted copies and retained keys require separate deletion/encryption
policy; no legal compliance guarantee is made here.

## Required tests

* Pause before root publication and attempt GC: retain every protected object,
  including objects that already existed before the transfer began.
* Capture an old commit while the source advances and requests cutoff GC: complete
  under protection or fail explicitly, never announce a partial closure as success.
* Two owners share a CID: releasing one claim, delayed duplicates and restarts do
  not erase the other claim; stale release cannot cancel renewal.
* Partition through lease renewal/expiry and verify the declared policy, without
  treating a partial reachability scan as a global live set.
* Race migration, GC, import completion and authority changes; retain protected
  recovery sources. Reopen crashed imports/exports without prematurely sweeping.

The demo's no-concurrent-GC restriction remains. No Datahike/Yggdrasil behavior was
changed by this increment.
