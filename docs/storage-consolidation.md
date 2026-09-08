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

The generic Konserve alias uses released 0.9.394. The checkpoint test alias
temporarily pins the provisioning increment from Konserve #192; it will return
to a released version when that API is published.
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

The optional dependencies now use Konserve 0.9.393. That release fixes the
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

### Initial directory provisioning: proposed boundary

Do not treat a successful sync of the leaf directory as proof that a newly
created chain of ancestor names survives a crash. This applies to Netz's direct
file adapter as well as the owned Konserve checkpoint store.

The next storage increment should expose an explicit, pre-provisioned durable
ancestor as a trust boundary. Provision descendants one component at a time and
persist each parent entry before descending. On retry, repeat the barriers for
the entire managed path, including components that now exist: an existence test
does not prove a previous barrier completed. Do not require write access to the
volume root, silently expand the managed boundary, or make read-only reconnects
perform writes. Shared/renamed paths and symlinks need an explicit policy before
this becomes an API. This is a proposal, not an implemented persistence guarantee.

Tests must inject failure after each mkdir and parent barrier, reconnect/retry
with existing-but-unconfirmed components, and verify no durable receipt can
escape before every required barrier completes. Include nested paths, a
pre-existing store, read-only reconnects, and Windows permission failures.
Process-kill coverage must remain distinct from OS/power-loss qualification.

### Execution order

The experimental `migration-konserve/open!` integration now requires a third
argument, `{:durable-ancestor path}`. It provisions and rechecks the managed
path before opening Konserve, including on reconnect; a failed barrier prevents
store opening and checkpoint acknowledgement. This deliberately changes the
experimental two-argument API rather than silently asserting an ancestor was
durable. The administrator owns the durable ancestor and excludes concurrent
filesystem mutation. The ordinary Konserve connect API is unchanged.

The checkpoint-only test alias temporarily pins Konserve #192's exact revision;
replace it with the corresponding release after publication. Other optional
store integrations remain on released Konserve. This does not yet provision
the direct blob repository's nested directories or qualify generic Konserve
blob receipts, and is not proof of power-loss survival.

1. Keep the released Konserve checkpoint integration gate passing.
2. Retain explicit strict-read and bounded-input requirements on future upgrades.
3. Qualify Konserve durability and share blob-store validation/conformance tests.
4. Review Konserve #188 as the publication/GC fence before inventing a Netz one.
5. Complete migration runtime, retention protections and end-to-end crash tests.

Keep wire validators' type-specific schemas and signature domains separate.
Likewise keep transfer, controller and lifecycle persistence barriers explicit;
deduplication must not hide their different authorization and failure semantics.
