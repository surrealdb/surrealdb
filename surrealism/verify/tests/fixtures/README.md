# Verification fixtures

Every fixture is real output of the tool that would produce it in production,
committed as produced. The conformance suite modified some of its bundles to
make its failing cases; every other negative case is made by the tests
themselves, in code, from a valid fixture.

## `MANIFEST`

A manifest in the archive's exact format. Its SHA-256 is the `DIGEST` that the
`private/` bundles and the `ssh/` signatures sign.

## `conformance/`

Rekor v2 bundles from the [Sigstore conformance suite][conformance]
(`test/assets/bundle-verify/rekor2-*`, commit
`a4495afe376729269e600680fa1c747cafb22d3c`), licensed under the Apache License
2.0. Each `<case>.sigstore.json` is that case's `bundle.sigstore.json`; a
`_fail` suffix marks a bundle a verifier must refuse.

Sigstore's staging infrastructure produced them for the GitHub Actions workflow
`sigstore-conformance/extremely-dangerous-public-oidc-beacon` (repository ID
`632596897`, owner ID `131804563`). They sign the SHA-256 digest
`a0cfc712…01bf`, not `MANIFEST`.

- `trusted_root.json`: the staging trusted root, from which the tests take the
  `ca`, `log` and `tsa` anchors.
- `trusted_root_local_tsa.json`: the same root with a short-lived local
  timestamp authority. Used by `timestamp-with-expired-cert-chain` and
  `timestamp-outside-tsa-cert-validity_fail`.

Cases from the suite that are not here: the DSSE cases other than
`dsse-happy-path` (DSSE is refused outright), and
`timestamp-outside-trust-root-tsa-validity_fail`, whose failure comes from a
validity window in the trusted root. A `SIGNER` carries certificates, not
validity windows; certificate validity is still checked.

## `private/` and `ssh/`

Generated once by `generate.py`, which documents what each case holds. The
private keys existed only in a temporary directory during generation.

- `private/<case>/bundle.sigstore.json` and `signer.json`: a private Sigstore
  (P-384 CA, `openssl ts` timestamp authority, Rekor v2 log) signing `MANIFEST`.
  Cases: `ed25519-log`, `ecdsa-log`, `email-identity` (no repository IDs) and
  `expired` (a certificate that expired two hours before the timestamp).
- `ssh/`: `ssh-keygen -Y sign` signatures over `MANIFEST`, with the public keys.
  `ed25519-git-namespace.sig` and `ed25519-sha512.sig` are valid signatures that
  a verifier must refuse for their namespace and hash.

[conformance]: https://github.com/sigstore/sigstore-conformance
