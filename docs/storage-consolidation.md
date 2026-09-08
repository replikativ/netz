# Storage consolidation status

This increment preserves protocol scores and safety boundaries while reducing
duplication. It does not activate automatic placement migration or deletion.

## Implemented

* Placement profiles 1 and 2 share bounded target selection. Their distinct score
  inputs, known-answer vectors and authority semantics remain unchanged.
* File and Konserve blob adapters share manifest-address and piece-position
  validation in `store-validation.cljc`. Error types and index data are preserved;
  the manifest mismatch message is now backend-neutral. Persistence and IO remain
  separate, with no expansion of accepted receipt profiles.
* Checkpoint validation lives in `migration-record.cljc`, used by both adapters.
* `migration-konserve` replaces the experimental JVM `migration-file`: sealed
  CBOR bytes, AES-GCM with authenticated-only reads, explicit
  synced/non-in-place file configuration, and validated read-back before model
  persistence acknowledgement. Its streaming binary read caps sealed input at
  1 MiB before Konserve unseals it and Boring decodes it with depth/item limits.
  Konserve owns the file write path and cryptography. The bespoke EDN/HMAC adapter
  and its custom IO fault hooks have been removed; checkpoint tests now use
  Konserve's crash simulator directly.
* Konserve PR #150 now has opt-in `:require-authenticated? true`, rejects unknown
  encryptor names, and tests encrypted CBOR checkpoint recovery using its existing
  six-point crash simulator. Plaintext/legacy read compatibility remains the
  default elsewhere; the Netz adapter requires the strict policy.

Run the opt-in integration gate with:

```
clojure -X:checkpoint-test :dirs '["test-checkpoint"]'
```

Both optional Konserve aliases on this branch use released Konserve 0.9.394.
The separate checkpoint-provisioning PR temporarily pins Konserve #192 for its
new provisioning API; replace that pin after the API is released.
Older Konserve versions without the required policy are refused by the adapter before
opening a store. No files are automatically migrated between checkpoint formats.

## Equivalence gates still open

Normal Konserve `get` does not supply the old pre-decoding input bound here;
the new adapter therefore uses `bget` with `:streaming? true` and bounded reads,
plus Konserve `seal`/`unseal`. It requires independent revision floors and fresh
authority; authenticated storage is not rollback prevention. It is explicitly
single-writer, not consensus-backed. No automatic import of the old prototype's
checkpoint files is provided; the formats use separate namespaces and storage.

Before retiring Netz's separate filesystem blob backend, qualify a supported
Konserve backend/configuration for the fsync receipt profile. Generic Konserve
completion must not be relabeled fsync. Preserve immutable-write conflict checks,
bounded reads, install-marker ordering, piece/object verification and receipt
replay. Share validators and run the same conformance suite against both backends
before removing either implementation.

### Released file-backend qualification findings

The optional dependencies now use Konserve 0.9.394. Version 0.9.393 fixes the
directory-open suppression described below: the default path fails closed, with
a native Windows barrier on JDK 22+. This resolves the error-propagation issue,
not the remaining initial-provisioning, bounded-input or generic-store
qualification requirements. The findings below record the 0.9.392 baseline.

Inspection of Konserve 0.9.392's artifact confirms the write sequence with
`:sync-blob? true` and `:in-place? false`: write segments, force blob, close,
atomic move, then sync store directory. This is necessary but not a complete
qualification of the generic Netz adapter:

* `filestore/sync-base` skips custom filesystems and catches directory-open
  `AccessDeniedException` without checking the operating system. Despite the
  Windows-oriented comment, this can also suppress an access-denied directory
  sync on POSIX. A strict qualified path must propagate that failure; do not
  infer an fsync guarantee from configuration alone.
* The generic adapter uses decoded `get` for pieces and records. Post-decoding
  size verification does not replace bounded input before allocation/decoding.
* Qualification must use an owned supported backend/configuration, not an
  arbitrary caller store or a caller-supplied `:fsync?` assertion. Include initial
  directory creation, immutable conflict behavior, and restart ordering in tests.

Next implement shared backend conformance tests and a strict directory-sync
failure test at the storage layer, then qualify a bounded file adapter. Until
those pass, keep `netz/store/fsync/1` unavailable on generic Konserve stores and
retain Netz's existing filesystem implementation. Simulated crash coverage alone
does not qualify an OS/filesystem's power-loss behavior.

Konserve's fault/crash harness belongs at the storage layer. Netz's pure/network
simulator remains responsible for delivery, partitions, authority and migration.
Do not combine these into a universal runtime abstraction. Actual process-kill
and power-loss tests remain distinct from simulated crashes.

## Remaining order

### Initial directory provisioning: implementation boundary

Do not treat a successful sync of the leaf directory as proof that a newly
created chain of ancestor names survives a crash. This applies to Netz's direct
file adapter as well as the owned Konserve checkpoint store.

Konserve #192 implements the shared JVM helper; Netz's separate checkpoint
increment adopts it. The blob adapters have not adopted it yet. They need an
explicit, pre-provisioned durable
ancestor as a trust boundary. Provision descendants one component at a time and
persist each parent entry before descending. On retry, repeat the barriers for
the entire managed path, including components that now exist: an existence test
does not prove a previous barrier completed. Do not require write access to the
volume root, silently expand the managed boundary, or make read-only reconnects
perform writes. Shared/renamed paths and symlinks need an explicit policy before
this becomes a supported blob API. The helper excludes concurrent filesystem
mutation and rejects symlink components inside its managed path. It does not
establish the durability of the ancestor's own name.

The blob increment must cover these existing paths, not only store creation:

| Path | Required barrier placement |
| --- | --- |
| Store construction | Provision the root beneath the explicitly trusted ancestor before returning a writable fsync store. |
| New pieces, manifests, receipts, controller intents | Provision the complete parent chain before creating a temporary file; keep the file force, atomic rename, and final parent force. |
| Existing-piece retry | Repeat parent-chain barriers before forcing and accepting the existing piece. |
| Existing install-marker retry | Repeat barriers for every referenced piece and the marker before reporting installation success. |
| Restart/replay | Do not infer a completed persistence barrier from an existing name or valid signature; distinguish replay of previously durable evidence from requalification of locally retained writes. |

Use Konserve's `provision-directory!` and `sync-directory!` for the JVM path,
including its Windows binding, rather than maintaining another native barrier.
Resolve this optional storage dependency only when constructing an fsync file
store; loading Netz's networking namespaces must not require Konserve. Missing
helper support must fail before filesystem mutation, never fall back to the
legacy leaf-only path. `:durable-ancestor` is a caller trust assertion, not a
path discovered by walking upward until an existing directory is found.

The equivalent Python work includes `blob_store.py` **and** `frontier_store.py`:
both currently create parent chains and sync only their leaf. Python cannot call
the JVM helper, so implement the same barrier contract using its supported OS
primitives and run matching failure/retry cases. Do not advertise cross-platform
fsync merely because POSIX tests pass. Best-effort mode remains explicitly
distinct; do not silently downgrade a requested fsync profile.

Update the filesystem test fixtures and demos to provide an explicit ancestor.
A temporary directory can model this trust boundary for sequencing tests, but
its existence does not certify power-loss durability. Existing one-argument
fsync construction will require a migration note rather than implicit trust in
the store root. Keep this change separate from the wire grammar: the persistence
profile already requires the guarantee; the adapter must actually establish it.

Tests must inject failure after each mkdir and parent barrier, reconnect/retry
with existing-but-unconfirmed components, and verify no durable receipt can
escape before every required barrier completes. Include nested paths, a
pre-existing store, read-only reconnects, and Windows permission failures.
Process-kill coverage must remain distinct from OS/power-loss qualification.

### Execution order

1. Keep the released Konserve checkpoint integration gate passing.
2. Retain explicit strict-read and bounded-input requirements on future upgrades.
3. Qualify Konserve durability and share blob-store validation/conformance tests.
4. Review Konserve #188 as the publication/GC fence before inventing a Netz one.
5. Complete migration runtime, retention protections and end-to-end crash tests.

Keep wire validators' type-specific schemas and signature domains separate.
Likewise keep transfer, controller and lifecycle persistence barriers explicit;
deduplication must not hide their different authorization and failure semantics.
