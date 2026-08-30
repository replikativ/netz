# Synchronization model

Reliable collaboration needs three distinct recovery ranges. Treating them as
one mechanism either floods too much state or silently loses updates.

## 1. Live dissemination

A publication is signed at its origin, forwarded across interested peers,
deduplicated by `(origin, epoch, sequence)`, and delivered in order by the
adapter. `publish!` succeeding means the local bounded runtime accepted the
item; it does not promise that every peer is online or has applied it.

## 2. Recent-message repair

Peers exchange bounded interval summaries. A detected hole still inside a
peer's repair horizon is requested as an exact missing range. This is cheap and
preserves the publication stream, but only while the sender retains the
messages.

## 3. Application state sync

An old gap, a buffer overflow, or a new subscription requires current state,
not an unbounded replay. Netz opens a point-to-point, acknowledged,
backpressured synchronization session and delegates opaque summaries, chunks,
application, and commit to a named profile.

The current Kabel adapter maps this to `PSyncStrategy`:

| Portable profile operation | Kabel operation |
|---|---|
| Describe local state | `-init-client-state` |
| Produce differential chunks | `-handshake-items` |
| Apply a chunk | `-apply-handshake-item` |
| Apply a live value | `-apply-publish` |

Live values received before the initial handshake completes are bounded and
ordered. Overflow marks the topic desynchronized and reruns the handshake;
message repair cannot recover a value that dissemination already marked seen.

## Initial profiles

### `netz/root-log/1`

For Datahike/Konserve with an Urd-selected writer. A summary contains writer
identity, Raft term or fencing token, log index, and content root. A receiver
accepts only a root with a valid publisher binding and a monotonically newer
fence/index. Missing immutable blocks are then fetched by content id.

This is single-writer application state even though transport and storage are
peer-to-peer.

### `netz/join-semilattice/1`

For Spindel/Yggdrasil state. A summary carries a causal frontier; chunks carry
missing deltas or a snapshot. Application is commutative, associative, and
idempotent, so multiple authorized writers can converge without leader
selection.

## Encryption

Netz hashes and moves opaque blocks. With storage encryption, the content id
names ciphertext, including its nonce and envelope. The profile should expose
stable signed manifests that reference encrypted blocks and identify the key
epoch. Capability/key exchange must be authenticated separately; discovering
a ciphertext block never grants the ability to decrypt it.

## Programming-model target

The application-facing layer can converge on an asynchronous shape such as:

```text
workspace = connect(configuration)
document = workspace.open(topic, profile)
document.watch(callback)
document.change(operation)
document.status() -> joining | synchronized | desynchronized | synchronizing
```

Clojure and ClojureScript can offer protocols and macros over this interface;
Python and other languages should expose promises, futures, async iterators, or
their native equivalents. This layer belongs above Netz so the same networking
standard can carry Datahike roots, CRDT deltas, or another state model.
