# Storage consolidation status

This increment preserves protocol scores and safety boundaries while reducing
duplication. It does not activate automatic placement migration or deletion.

## Implemented

* Placement profiles 1 and 2 share bounded target selection. Their distinct score
  inputs, known-answer vectors and authority semantics remain unchanged.
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

The alias pins the reviewed Konserve PR commit; it does not alter Netz's released
Konserve dependency. Older Konserve versions are refused by the new adapter before
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

Konserve's fault/crash harness belongs at the storage layer. Netz's pure/network
simulator remains responsible for delivery, partitions, authority and migration.
Do not combine these into a universal runtime abstraction. Actual process-kill
and power-loss tests remain distinct from simulated crashes.

## Remaining order

1. Review refreshed Konserve #150 CI and resolve its release dependency pin.
2. Replace the test-only dependency pin with a release after Konserve validation.
3. Qualify Konserve durability and share blob-store validation/conformance tests.
4. Review Konserve #188 as the publication/GC fence before inventing a Netz one.
5. Complete migration runtime, retention protections and end-to-end crash tests.

Keep wire validators' type-specific schemas and signature domains separate.
Likewise keep transfer, controller and lifecycle persistence barriers explicit;
deduplication must not hide their different authorization and failure semantics.
