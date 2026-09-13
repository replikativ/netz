# Multi-process Datahike durability experiment

Status: explicit developer gate; it does not publish Netz or modify Datahike.

The experiment composes the signed Datahike snapshot archive with Netz's real
durable-blob controller. One archive contains the exact authenticated catalogue
closure, so one `blob/stored` receipt represents one independently queryable
snapshot rather than a scattering of unrelated index objects.

Run the current checkout against the released Datahike dependency:

```sh
clojure -X:replica-demo:test \
  :dirs '["test-replica"]' \
  :patterns '["netz.datahike-process-experiment-test"]'
```

To test an unpublished Datahike checkout as well, compile its hand-written Java
interfaces once and override only the developer dependency:

```sh
cd /path/to/datahike
clojure -T:build prep-lib
cd /path/to/netz
clojure \
  -Sdeps '{:override-deps {org.replikativ/datahike {:local/root "/path/to/datahike"}}}' \
  -X:replica-demo:test \
  :dirs '["test-replica"]' \
  :patterns '["netz.datahike-process-experiment-test"]'
```

The gate starts three storage JVMs, creates a file-backed Datahike database in
the test JVM, captures its signed Yggdrasil commit closure and durably replicates
the archive under this policy:

```clojure
{:replicas 3
 :write-acks 2
 :placement-minimum-domains {"owner" 3 "site" 3}
 :ack-minimum-domains {"owner" 2 "site" 2}}
```

It observes `W=2`, waits for `N=3`, forcibly terminates one replica process,
signs an epoch-1 layout replacing that Authority, reloads the two surviving
processes, launches the replacement process and waits for three fresh fenced
receipts. It then stops the writer and deletes its Datahike database. Finally it
reconstructs the authenticated archive from one surviving replica and queries
all 200 records offline.

Temporary stores, credentials and per-process logs are retained under the
printed `/tmp/netz-datahike-process-*` directory for diagnosis. Connection
refusals for the intentionally offline spare and forcibly killed replica are
expected; they demonstrate that overlay reachability is not durability proof.

## Static EDN bootstrap

The shared EDN file is a small-deployment operator format, not a wire standard.
It has a closed schema containing:

- issue and expiry times;
- controller Authority IDs;
- peer Authority IDs, Kabel peer IDs, addresses, roles and declared domains;
- a bounded sequence of canonical-CBOR signed layout envelopes.

Each node pins the expected network name and controller Authority IDs in local
configuration; those trust anchors must not be learned from this fetched file.
Consequently, replacing the file cannot substitute an attacker's controller
and signed layout. The gate includes a negative check for this boundary.

The full layout chain is necessary. A new process cannot safely start from an
epoch greater than zero using only the latest layout because it cannot verify
the predecessor link, detect a missing epoch or distinguish a fork. Every
layout member must also have exactly matching domains in the EDN peer table.

HTTPS should distribute this document, while locally pinned controller IDs,
Noise-authenticated peers and signed layouts provide the security boundary.
The EDN server is neither a relay nor a liveness oracle. A production deployment
should mirror the file, refresh before expiry and retain the last accepted
layout chain locally.

## What remains outside this proof

- integration into Datahike HTTP Server configuration and lifecycle;
- secondary-index and `:db.type/store-ref` closure support;
- kill-during-file-write and machine/power-loss testing;
- periodic health evidence and automatic policy-authorized layout planning;
- parallel multi-provider reads, cache eviction policy and browser transports;
- mutable-head selection or writer fencing, which remains an application/Urd
  responsibility.
