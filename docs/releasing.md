# Releasing Netz

Netz publishes two distributions from every green `main` commit:

- `org.replikativ/netz` on Clojars;
- `netz-protocol` on PyPI.

Both use `0.1.<commit-count>`, where the final component is Git's total
reachable revision count. Alpha describes the maturity of the `0.1` series; it
is not a manually incremented version suffix. A larger release line is started
by changing the single `VERSION_PREFIX` file, for example from `0.1` to `0.2`.
We intentionally do not create a tag or GitHub release for every alpha build.

`build.clj` and `build_version.py` independently derive the same version from
the same checkout. The JVM verifier validates the generated POM, license and
required JAR entries; the Python CI job compares both derivations. Python
source-tree builds derive the count from Git; an sdist carries its derived package
metadata into the wheel build. For any other exported source tree, pass the
exact already-derived value as `NETZ_BUILD_VERSION=0.1.N`; the override is
validated against `VERSION_PREFIX` and must never be used to reuse a registry
version.

## One-time CircleCI setup

Keep the existing `docker-deploy` context and grant the project these two
additional contexts:

- `pypi-deploy`, used as an OIDC policy guard and containing **no PyPI
  credentials**. Restrict it to the Netz project and the expression
  `pipeline.git.branch == "main" and not job.ssh.enabled`;
- `clojars-deploy`, containing the Clojars credentials used by the other
  replikativ projects.

PyPI publication uses CircleCI Trusted Publishing. Before the first upload,
create a pending publisher for the unclaimed `netz-protocol` name from PyPI's
account Publishing page. Copy the organization ID, project ID and pipeline
definition ID from CircleCI, and preferably also the `pypi-deploy` context ID.
Bind the publisher to `github.com/replikativ/netz` and `refs/heads/main` as
additional restrictions. Twine 6.1 or newer detects CircleCI's OIDC credential
and exchanges it for a short-lived PyPI token; do not add `TWINE_USERNAME` or
`TWINE_PASSWORD` to the context.

Restrict both publication contexts to the maintainer group. Branch filters,
context restrictions and unique versions prevent pull requests from publishing,
while context access is the publication boundary. PyPI rejects SSH reruns of an
OIDC publishing job; fix or rerun the workflow normally instead.

## Publication flow

1. Merge a reviewed change to `main`.
2. CircleCI runs the full JVM, JavaScript, Python, secure transport, mixed
   runtime, checkpoint, Konserve and Datahike-replica gates.
3. A green pipeline publishes the Python wheel first and the JVM artifact
   second. Each registry receives the identical `0.1.<commit-count>` version.
4. Verify the new coordinates from clean consumer environments when making a
   significant release-line change.

If PyPI succeeds and Clojars fails, rerun only the failed CircleCI jobs so the
successful immutable Python version is not uploaded again. Never reuse a
version after either registry has accepted it. A failed `main` pipeline can be
fixed by another commit: the new commit receives a new count and therefore a
new version.

The repository must be public before the packages are presented as a public
standard implementation. Repository visibility and registry credentials are
maintainer operations; routine alpha publication and version calculation are
automatic.
