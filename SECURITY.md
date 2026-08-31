# Security policy

Netz is pre-release software. No current version should be exposed to
untrusted peers until every item in the corresponding section of `RELEASE.md`
is complete.

Please report vulnerabilities privately through the repository's GitHub
Security Advisory interface. Include the affected revision, trust assumptions,
reproduction steps and expected impact. Do not include live credentials or
third-party personal data.

Security-sensitive protocol changes require JVM and ClojureScript regression
tests and, once wire profile 1 is active, a language-neutral fixture.

## Dial and SSRF policy

The default dial policy validates absolute credential-free `ws`/`wss` syntax;
it deliberately permits private networks because many P2P deployments require
them. A public gateway should instead pass
`(netz.overlay.runtime/configured-address-policy peers)` as `:allow-address?`.
That policy permits only exact configured Authority-routing/address pairs, so a
gossiped record cannot redirect a known peer to an internal service.

An open deployment that accepts previously unknown hostnames needs enforcement
in the WebSocket adapter as well: resolve once, reject disallowed IP ranges,
pin the resolved address through connect, retain TLS hostname verification, and
disable cross-origin redirects. Checking a hostname before the adapter resolves
it is not a DNS-rebinding defense. This transport hook remains a release blocker
for arbitrary public discovery; an exact configured policy is safe without it.
