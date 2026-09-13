# Releasing Netz

Netz publishes two distributions from the same commit:

- `io.replikativ/netz` on Clojars;
- `netz-protocol` on PyPI.

The JVM and Python versions are intentionally written using their ecosystems'
normal syntax (`0.1.0-alpha.1` and `0.1.0a1`). `clojure -T:build verify` checks
that both declarations, the user-facing documentation, the changelog, the
generated POM, the Apache-2.0 license, and required jar namespaces agree.

## One-time CircleCI setup

Keep the existing `docker-deploy` context and grant the project these two
additional contexts:

- `pypi-deploy`, containing `TWINE_USERNAME=__token__` and `TWINE_PASSWORD`.
  The first upload needs a short-lived account-scoped PyPI token because the
  project does not exist yet; replace it with a project-scoped token
  immediately after that bootstrap release;
- `clojars-deploy`, containing the Clojars credentials already used by the
  other replikativ projects.

Restrict both publication contexts to the maintainer group. The pipeline
parameter is an intentional trigger, not an authorization boundary; context
access is what prevents an ordinary contributor from using registry secrets.

Ordinary pushes and pull requests never publish. The release workflow exists
only when a maintainer explicitly triggers a pipeline with the boolean
pipeline parameter `release` set to `true`, and its publishing jobs run only
for `main`.

## Release procedure

1. Replace `unreleased` in the `CHANGELOG.md` version heading with the release
   date, confirm that it describes the release, and confirm that both intended
   versions are still unused on Clojars and PyPI.
2. Make the GitHub repository public and re-check every metadata/documentation
   link from a logged-out browser.
3. Merge the release commit to `main` and record its full commit SHA.
4. In CircleCI, choose **Run Pipeline**, select `main`, add the boolean
   parameter `release=true`, and run it. This repeats every conformance gate.
5. Python is published first and Clojars second. If Clojars fails after PyPI
   succeeds, use CircleCI's **Rerun failed tests** operation; do not start a new
   pipeline that would try to upload the immutable PyPI version again.
6. Verify both registry artifacts from clean consumer environments.
7. Create the immutable tag and pre-release from the exact published source
   commit:

   ```sh
   git tag -s v0.1.0-alpha.1 <published-commit-sha>
   git push origin v0.1.0-alpha.1
   gh release create v0.1.0-alpha.1 --verify-tag --prerelease \
     --title "Netz 0.1.0-alpha.1" --generate-notes
   ```

Do not reuse a version after either registry accepted it. A partial registry
release is completed at the same commit; it is not overwritten.

The repository must be public before presenting the packages as a public
standard implementation. Repository visibility, registry credentials, and the
signed release tag are deliberate maintainer actions rather than CI side
effects.
