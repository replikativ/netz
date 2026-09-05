# Signed Datahike replica demo

Run `clojure -M:replica-demo` from the Netz checkout. This JVM example uses
released Datahike 0.8.1877, Yggdrasil 0.3.43 and konserve-sync 0.1.40. It starts
two loopback WebSocket peers with Noise authentication, creates a temporary
file-backed Datahike database, replicates 1,200 notes, follows one live update,
and catches up two more updates after disconnecting and restarting the reader.
It then stops and deletes the source database, reopens the receiver's disk
store, and queries all 1,203 notes locally.

The result reports the commit ID, fetched object count, row counts and the
temporary replica path, retained for inspection. It creates only temporary
demo data. This is an optional
example; Datahike and konserve-sync do not enter Netz's normal dependencies.

## How the pieces fit

1. A durable Datahike commit produces a Yggdrasil DAG event. The local hook only
   queues the event; export happens outside the writer callback.
2. The exporter signs the Datahike root reference, pins that exact commit for
   the reachability walk, and captures its primary index objects and schema.
3. Each Konserve value becomes a bounded CBOR block addressed by
   CIDv1/raw/SHA-256. A catalogue maps the original Konserve keys to those CIDs
   and embeds the signed Datahike root reference. The publisher signs the
   catalogue CID using a demo-specific snapshot-reference profile.
4. Kabel's snapshot strategy disseminates the signed catalogue reference over
   Netz. The receiver pins the publisher and stream before requesting content.
5. A read-only Konserve view fetches verified objects through Netz's content
   runtime. konserve-sync consumes that view locally: it does not open another
   connection to the Datahike writer. A verified byte cache allows retries to
   reuse completed downloads.
6. The installer writes objects before the commit record, checks stored values,
   opens the exact commit and forces its primary datoms before returning a
   queryable DB. It never updates a mutable branch pointer.

The catalogue is necessary because a signed Datahike commit UUID does not by
itself specify a SHA-256 transport address for every stored object. Its signed
key-to-CID mapping lets a provider serve bytes without being trusted to choose
the values behind those keys. The demo pins one authorized provider; broader
provider discovery can use the same existing Netz content protocol.

## Deliberate limits

The catalogue is **experimental application data**, not a new standardized
wire profile. It uses the existing signed-reference and content protocols.
Application values use Datahike's Boring/CBOR registry; Netz control messages
do not inspect their tags.

The example supports primary persistent-sorted-set indexes. It rejects
secondary indexes and `:db.type/store-ref` schemas because the released generic
walker cannot prove those closures complete. The exporter must run without
concurrent GC; this demo owns its source and never runs GC. Memory-only source
databases are unsuitable because Datahike does not flush their index objects.

Exports are bounded to 1,024 objects and 1 MiB per encoded block (including the
catalogue). Larger values need the existing chunked blob materialization path.
This initial demo does not promise a replication quorum or automatic repair.

Signed announcements and queryable snapshots are separate states. Concurrent
catalogue heads remain visible, and applications explicitly select which one
to materialize. No LWW, branch overwrite, or Urd fencing is implied. Receiving
ancestry metadata also does not fetch every historical database closure.

Run the focused gate with:

```sh
clojure -X:replica-demo:test :dirs '["test-replica"]'
```

The tests cover authenticated network catch-up and live delivery, an actual
reader disconnect/reconnect, cold disk-store reopen followed by offline queries,
publisher rejection before content access, corrupt bytes, interrupted fetch
and cache reopen/reuse, missed commits, and concurrent announcements. The
interrupted-fetch test injects failure at the object boundary and recreates
the adapter; it is not a kill-the-JVM crash test.

The released konserve-sync strategy exposes a legacy channel without an
independent completion result. The demo therefore checks that every key in the
signed catalogue was received exactly once and read back successfully, and
that the root arrived last. A closed channel alone never declares success.
