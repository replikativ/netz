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

The JVM file-store and checkpoint test aliases use released Konserve 0.9.395
for directory provisioning. All four coordinates use the same release; no Git
dependency preparation is required.
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

The provisioning test dependencies now use Konserve 0.9.395. Version 0.9.393 fixes the
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

Konserve #192 implements the shared JVM helper, adopted here by both the
checkpoint store and JVM blob adapter. Python's blob and frontier stores use
the equivalent POSIX contract. Each requires an explicit, pre-provisioned durable
ancestor as a trust boundary. Provision descendants one component at a time and
persist each parent entry before descending. On retry, repeat the barriers for
the entire managed path, including components that now exist: an existence test
does not prove a previous barrier completed. Do not require write access to the
volume root, silently expand the managed boundary, or make read-only reconnects
perform writes. Shared/renamed paths and symlinks need an explicit policy before
this becomes a supported blob API. The helper excludes concurrent filesystem
mutation and rejects symlink components inside its managed path. It does not
establish the durability of the ancestor's own name.

The JVM file adapter now adopts these barriers for root construction, new writes,
existing-piece retries and existing-marker retries. Fsync construction requires
`:durable-ancestor`; Konserve is resolved lazily and missing support fails before
mutation. Best-effort construction does not load Konserve. Tests use explicitly
owned temporary ancestors as sequencing fixtures, not power-loss evidence.

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

The equivalent Python implementation covers `blob_store.py` and `frontier_store.py`:
both use `directory_sync.py` for parent-chain barriers and explicit ancestors.
Its tests cover failed barriers, existing-entry retries and rejected symlinks.
Python cannot call the JVM helper and currently uses POSIX primitives only.
Do not advertise cross-platform
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

The experimental `migration-konserve/open!` integration now requires a third
argument, `{:durable-ancestor path}`. It provisions and rechecks the managed
path before opening Konserve, including on reconnect; a failed barrier prevents
store opening and checkpoint acknowledgement. This deliberately changes the
experimental two-argument API rather than silently asserting an ancestor was
durable. The administrator owns the durable ancestor and excludes concurrent
filesystem mutation. The ordinary Konserve connect API is unchanged.

The checkpoint and blob test aliases share released Konserve 0.9.395, which
includes #192. Direct blob-directory provisioning is implemented, but generic
Konserve blob receipts remain unqualified. Successful barrier calls and these
failure-injection tests are not proof of arbitrary power-loss survival.

1. Keep the released Konserve checkpoint integration gate passing.
2. Retain explicit strict-read and bounded-input requirements on future upgrades.
3. Qualify Konserve durability and share blob-store validation/conformance tests.
4. Review Konserve #188 as the publication/GC fence before inventing a Netz one.
5. Complete migration runtime, retention protections and end-to-end crash tests.

The migration model now exposes `copy-candidates` for runtime planning. It emits
placement-ordered replica/attempt/binding records only for the exact current,
persisted transition; missing or expired receipts remain work, and a pending
checkpoint or halted transition releases none. Restored checkpoints reproduce
the same attempts. This API does not send data or grant authority. The runtime
still needs signed-layout and controller-fence validation, bounded transfer
scheduling, receipt verification followed by checkpoint persistence, and an
atomic protection recheck before any retirement. Retirement plans now cross
distinct durable planned/deleting/retired generations and are replay-safe. A
portable signed command independently proves the current layout and all N
replacement receipts, but its verified result remains advisory until a runtime
binds it to the checkpoint and serializes protection mutation with idempotent
physical deletion. It must never be wired directly to an unfenced delete
callback.

### Reuse the existing controller for migration execution

Do not confuse the experimental profile-2 planner with the existing profile-1
runtime. `controller-runtime/revise!` already accepts a verified successor
layout/retention intent, persists it before dispatching offers, and requires
fresh receipts for the new retention revision. `current-record?` compares the
exact layout address and rejects a forked layout state. `receipt-matches?`
checks cluster, privacy domain, retention revision, manifest, object, epoch,
replica, attempt, expiry and persistence against the durable intent. Reuse these
boundaries rather than introducing a second receipt or transfer engine.

Signed `blob/layout` now requires the stable seed and placement profile, and
`intent` uses the main stable-placement engine. The experimental duplicate
placement modules have been removed. The implementation and remaining safety
requirements are:

1. Consolidate the initial, unreleased signed layout representation to bind the
   exact placement profile and 32-byte stable pool seed. Regenerate prototype
   fixtures; no legacy parser or pre-release format migration is needed. Keep
   explicit versioning for future releases and reject unknown profiles.
2. Explicit transition rules: ordinary epoch advances preserve the seed;
   changing it is deliberate redistribution, not an incidental fence update.
   Physical replica movement still requires copy and retention safeguards.
3. One profile-dispatch point in intent placement, used by live admission and
   replay. Reuse the current revision/persistence/receipt machinery after that
   dispatch, with cross-runtime fixtures for exact bytes and selected targets.
4. Tests for profile/seed tampering, unsupported-profile rejection, epoch-only
   stability, membership/domain changes, crash/replay of successor intents,
   stale receipts, forks and authority loss while a copy is pending.

Existing `blob/stored` receipts do not contain a layout address or stable seed.
They can be interpreted only through the exact persisted intent and accepted
layout chain; an epoch alone must never select between conflicting layouts.
Whether a future receipt version additionally binds the layout address is a
wire-design decision, not something to infer from the current envelope.
Retirement still requires a separate atomic retention/pin check; successful
copy execution does not itself authorize removal of old replicas.

Keep wire validators' type-specific schemas and signature domains separate.
Likewise keep transfer, controller and lifecycle persistence barriers explicit;
deduplication must not hide their different authorization and failure semantics.
