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
