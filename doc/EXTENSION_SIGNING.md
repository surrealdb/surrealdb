# Signing extensions

`surreal extension sign` writes a signature into a `.surli` archive, as the
file `surrealism/SIGNATURE`. The signature covers `sha256(surrealism/MANIFEST)`.
That digest is the extension's `DIGEST`. It does not cover the archive bytes.

- Signing re-packs the archive. `MANIFEST` and every other entry stay
  byte-for-byte the same, so `DIGEST` does not change.
- An archive holds at most one signature. Signing again replaces it.
- An archive without `surrealism/MANIFEST` is refused. Rebuild it with
  `surreal extension build`.
- `-o FILE` writes the signed archive to `FILE` and leaves the input as it is.
  Without `-o`, the input is replaced. A failed run leaves it unchanged.

There are three ways to sign.

## Keyless, from CI (default)

```bash
surreal extension sign target/acme-color-1.0.0.surli
```

This uses Sigstore and needs no key. The command does these steps:

1. It reads `surrealism/MANIFEST` and computes `sha256(MANIFEST)`.
2. It gets the CI identity token (audience `sigstore`):
   - `SIGSTORE_ID_TOKEN`, if it is set (GitLab CI, or any provider by hand);
   - otherwise GitHub Actions, through `ACTIONS_ID_TOKEN_REQUEST_URL`. The job
     must have `id-token: write`.
3. It generates a P-256 key in memory. It sends the token and the public key
   to Fulcio, which returns a certificate that is valid for 10 minutes and names
   the workflow.
4. It signs `MANIFEST` (ECDSA P-256, SHA-256) with that key.
5. It gets an RFC 3161 timestamp over the signature from the timestamp
   authority. A Rekor v2 entry records no time, so this timestamp is the signing
   time that a verifier compares with the certificate's validity.
6. It records the signature in Rekor v2. Rekor returns the entry with its
   inclusion proof and a signed checkpoint.
7. It reduces the Sigstore trust root (`trusted_root.json`) to the certificate
   authority, Rekor key and timestamp authority that verify this bundle, and
   checks the result offline.
8. It writes `surrealism/SIGNATURE` as one JSON envelope:

   ```json
   {
     "mediaType": "application/vnd.surrealdb.extension-signature.v1+json",
     "bundle": { "…": "the Sigstore bundle v0.3, unchanged" },
     "trustedRoot": { "…": "the trusted_root.json entries that verify it" }
   }
   ```

The envelope lets anyone verify the archive offline, with no trust root built
into SurrealDB or the CLI, the way an SSH signature carries its key. The
embedded root records which anchors the signer used; it does not make them
trusted. A client shows them, and the person installing decides; the server
verifies only against the anchors in the definition's `SIGNER` and never reads
the embedded root.

The trust root is fetched when signing. For the public instance it comes from
Sigstore's TUF repository (`https://tuf-repo-cdn.sigstore.dev`) through the
`sigstore-trust-root` TUF client, which verifies the repository's metadata from
Sigstore's TUF root. It is fetched before anything is signed, so a failed fetch
costs no log entry. Signing itself (the identity token, Fulcio, the TSA and
Rekor v2) goes through the official `sigstore-rust` crates.

The default services are Sigstore's public instance. Use these flags to choose
other services, for example a private Sigstore:

| Flag             | Default                                           |
|------------------|---------------------------------------------------|
| `--fulcio-url`   | `https://fulcio.sigstore.dev`                     |
| `--rekor-url`    | `https://log2025-1.rekor.sigstore.dev` (Rekor v2) |
| `--tsa-url`      | `https://timestamp.sigstore.dev/api/v1/timestamp` |
| `--trusted-root` | none: fetched through Sigstore's TUF repository   |

A private Sigstore (any of the first three flags changed) needs
`--trusted-root FILE`, its own `trusted_root.json`.

The token and the generated key are never written to disk or to the log.

### GitHub Actions

```yaml
permissions:
  id-token: write
  contents: read

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: surreal extension build
      - run: surreal extension sign target/acme-color-1.0.0.surli
      - run: surreal extension publish target/acme-color-1.0.0.surli
```

The certificate names the workflow, for example:

```
Issuer       https://token.actions.githubusercontent.com
Identity     https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.0.0
```

### GitLab CI

```yaml
release:
  id_tokens:
    SIGSTORE_ID_TOKEN:
      aud: sigstore
  script:
    - surreal extension build
    - surreal extension sign target/acme-color-1.0.0.surli
    - surreal extension publish target/acme-color-1.0.0.surli
```

The certificate names the pipeline, for example:

```
Issuer       https://gitlab.com
Identity     https://gitlab.com/acme/color//.gitlab-ci.yml@refs/tags/v1.0.0
```

## With an SSH key

```bash
surreal extension sign --key ~/.ssh/id_ed25519 pkg.surli
```

This writes an SSH signature (the `SSHSIG` format that git also uses). The
signed message is the `MANIFEST` bytes, with namespace `surrealism` and hash
algorithm `sha256`. The signature's `H(message)` is therefore
`sha256(MANIFEST)`, the extension's `DIGEST`. The command runs
`ssh-keygen -Y sign -O hashalg=sha256`, so it needs OpenSSH 8.9 or later on
`PATH`. Encrypted keys, `ssh-agent` and hardware keys work as they do for
`git commit -S`.

To check a key signature by hand, extract the archive and run:

```bash
ssh-keygen -Y check-novalidate -n surrealism -s surrealism/SIGNATURE < surrealism/MANIFEST
```

## With a bundle from another Sigstore tool

Sign the extracted `MANIFEST` as a blob, then import the bundle:

```bash
tar --zstd -xf pkg.surli surrealism/MANIFEST
cosign sign-blob --new-bundle-format --bundle bundle.json surrealism/MANIFEST
surreal extension sign --bundle bundle.json --trusted-root trusted_root.json pkg.surli
```

`--trusted-root` is required: the bundle is written in the same envelope as a
keyless signature, with the root reduced to the anchors that verify it.

Before it writes the bundle, the command checks that the bundle's message digest
is `sha256(MANIFEST)` of this archive. It also checks the digest in every
`hashedrekord` log entry that it can read. If a digest does not match, the
command refuses the bundle and does not change the archive. It also refuses a
DSSE bundle (from `cosign attest-blob`) and the older cosign bundle format.

`sign` verifies the bundle against the root you give it, so a signature it
writes always verifies offline. It does not decide whether that root, or the
signer's identity, should be trusted. The server checks the signature against
the `SIGNER` clause of the definition.
