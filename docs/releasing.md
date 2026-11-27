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
the same full-history checkout. If Git reports a shallow repository, fetch the
remaining history with `git fetch --unshallow` before building. The JVM verifier
validates the generated POM, license and required JAR entries. Python CI
checks the package version against the repository prefix and revision count. Python
source-tree builds derive the count from Git; an sdist carries its derived package
metadata into the wheel build. For any other exported source tree, pass the
exact already-derived value as `NETZ_BUILD_VERSION=0.1.N`; the override is
validated against `VERSION_PREFIX` and must never be used to reuse a registry
version.

## One-time registry setup

Register a GitHub pending publisher on the [PyPI account Publishing page](https://pypi.org/manage/account/publishing/)
before the first upload. For an existing project, use its Publishing settings.

| PyPI field | Value |
|---|---|
| Project name | `netz-protocol` |
| Owner | `replikativ` |
| Repository | `netz` |
| Workflow filename | `publish-python.yml` |
| Environment | `pypi` |

Enable GitHub Actions and configure the repository's `pypi` environment for
deployments from `main`. The upload job exchanges a GitHub OIDC token for a
short-lived PyPI credential. The separate wheel build has read-only access.

CircleCI needs its existing `docker-deploy` context for CI and its restricted
`clojars-deploy` context for JVM publication.

## Publication flow

1. Merge a reviewed change to `main`. GitHub Actions waits for the
   `ci/circleci: build` status on that exact commit.
2. CircleCI runs formatting and all JVM, JavaScript, Python, secure transport,
   mixed-runtime, checkpoint, Konserve and Datahike-replica tests. The final
   build job requires those checks and verifies the JAR.
3. GitHub Actions builds the wheel from the full commit history and uploads it
   using the `pypi` environment. CircleCI checks the version-specific PyPI
   endpoint before uploading the matching JAR to Clojars.
4. Verify both coordinates from clean consumer environments before announcing.

If the PyPI upload fails, inspect the GitHub Actions publish job. After correcting
the publisher configuration, rerun the failed job on the same commit. If the
CircleCI Clojars job timed out waiting for that version, rerun it after the wheel
appears on PyPI. If just Clojars fails, rerun the failed CircleCI job. Published
versions are immutable.

The repository must be public before the packages are presented as a public
standard implementation. Repository visibility and registry credentials are
maintainer operations; routine alpha publication and version calculation are
automatic.
