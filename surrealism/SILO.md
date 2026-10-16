# Silo package host contract

Silo hosts Surrealism packages (`.surli` archives) under an organisation and a
package name. Two parties read from it:

- **The server** resolves `FROM silo::{org}::{pkg}<x.y.z>` to one archive and
  loads it (`surrealdb/core/src/surrealism/silo.rs`).
- **Clients** (the CLI, Studio, the SDKs) resolve what a person asked for to an
  exact version, collect the details the install prompt shows, and write the
  `DEFINE EXTENSION` statement (`surrealdb/server/src/cli/extension/silo.rs`).

Every URL below is relative to the endpoint, `https://silo.surrealdb.com`. There
is one registry: neither the server nor the CLI can be pointed at another one in
a release build. An archive served from anywhere else is defined from a bucket
(`FROM f"bucket:/path.surli"`), not as a `silo::` package. Builds with debug
assertions read `SURREAL_SURREALISM_SILO_ENDPOINT`, so the integration tests can
serve packages from a local registry.

## Names

`{org}` and `{pkg}` are each one URL path segment: non-empty, and only ASCII
letters, digits, `-` and `_`. The server and the client both refuse any other
value before they build a URL, so a name cannot reach outside its own path.

A version is `{major}.{minor}.{patch}`: three decimal integers, with no
pre-release or build suffix. A `DEFINE` statement has no way to express a suffix,
so a version that carries one cannot be installed, and a client rejects it.

## The archive

```http
GET {endpoint}/{org}/{pkg}/{major}.{minor}.{patch}.surli
```

- `200`: the archive bytes. The body for one version never changes.
- `404`: the package or the version does not exist.
- Any other status is an error.

This URL is the source of truth, and it is the only URL the server uses. A mirror
needs to serve only this path. The server accepts at most 256 MiB.

## The package API

The API is optional. It lets a client resolve "latest" and show the install
prompt without downloading the archive. Every response is a JSON object.
Clients ignore fields they do not know, so the API can add fields without a new
version prefix.

### List versions

```http
GET {endpoint}/api/v1/packages/{org}/{pkg}
```

```json
{
  "latest": "1.2.0",
  "versions": [
    { "version": "1.2.0", "digest": "sha256:5d1a…", "yanked": false, "published": "2026-10-01T12:00:00Z" },
    { "version": "1.1.0", "digest": "sha256:0b7e…", "yanked": true,  "published": "2026-09-14T08:30:00Z" }
  ]
}
```

| Field | Type | Meaning |
|-------|------|---------|
| `latest` | string or `null` | The highest version that is not yanked. `null` when every version is yanked. |
| `versions[].version` | string | An exact version. |
| `versions[].digest` | string | `sha256:` and 64 lowercase hex digits: the SHA-256 of the archive's `surrealism/MANIFEST`. |
| `versions[].yanked` | bool | The publisher withdrew this version. |
| `versions[].published` | string | When it was published, RFC 3339. |

### Version details

```http
GET {endpoint}/api/v1/packages/{org}/{pkg}/{major}.{minor}.{patch}
```

```json
{
  "digest": "sha256:5d1a…",
  "signer": {
    "issuer": "https://token.actions.githubusercontent.com",
    "identity": "https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.2.0",
    "repository_id": "812345678",
    "owner_id": "98765432",
    "registered_publisher": true
  },
  "requests": {
    "net": ["api.color.io:443"],
    "functions": ["http::get"],
    "queries": false,
    "scripting": false,
    "write": true,
    "memory": 67108864,
    "timeout": "5s",
    "strict_timeout": true
  },
  "exports": [
    { "name": "blend", "args": [{ "name": "a", "kind": "string" }, { "name": "b", "kind": "string" }],
      "returns": "string", "writes": false, "comment": "Blend two colours" },
    { "name": "recolor", "args": [{ "name": "id", "kind": "record<image>" }],
      "returns": "none", "writes": true, "comment": null }
  ],
  "yanked": false,
  "advisories": [
    { "id": "SILO-2026-0007", "summary": "blend() panics on empty input", "url": "https://silo.surrealdb.com/advisories/SILO-2026-0007" }
  ]
}
```

| Field | Type | Meaning |
|-------|------|---------|
| `digest` | string | As in the version list. |
| `signer` | object or `null` | Who signed the archive's manifest. `null` when the archive has no signature. |
| `signer.issuer` | string | The OIDC issuer in the signing certificate. |
| `signer.identity` | string | The certificate's subject: for CI, the workflow and its ref. |
| `signer.repository_id` | string or `null` | The source repository's numeric ID, when the issuer provides one. |
| `signer.owner_id` | string or `null` | The repository owner's numeric ID, when the issuer provides one. |
| `signer.key` | string or absent | For an SSH signature: the public key, `<type> <base64> [comment]`. The Sigstore fields are then absent. |
| `signer.registered_publisher` | bool | The signer matches a publisher registered for this package on Silo. |
| `requests` | object | The archive's `[capabilities]`, in the field names of the `CAPABILITIES` clause. A request, never a grant. Absent fields take the defaults shown in the `CAPABILITIES` documentation: nothing granted, `strict_timeout: true`. |
| `requests.write` | bool | `true` when at least one export writes. |
| `requests.memory` | integer or `null` | Bytes. |
| `requests.timeout` | string or `null` | A SurrealQL duration. |
| `exports[].name` | string or `null` | `null` for the default export. |
| `exports[].args` | array | Each argument's name and SurrealQL type. |
| `exports[].returns` | string | SurrealQL type. |
| `exports[].writes` | bool | The function may write. |
| `exports[].comment` | string or `null` | The function's `COMMENT`. |
| `yanked` | bool | The publisher withdrew this version. |
| `advisories[]` | array | Published security or correctness advisories: `id`, `summary`, and an optional `url`. |

### Status codes

- `200`: the JSON above.
- `404`: Silo has no such package or version, or the endpoint does not serve the
  API at all (a plain mirror). The client cannot tell these apart from the API
  alone, so it treats `404` as "no API answer" and asks the archive URL (see
  below). Only a `404` from the archive URL means the version does not exist.
- Any other status is an error. The client does not fall back, because a
  failing API on a Silo that has one should be visible, not hidden.

## Client rules

1. **Statements hold exact versions.** The client resolves every request to one
   `x.y.z` before it writes `DEFINE EXTENSION`. A statement never contains a
   range or "latest", so the same statement always means the same code.
2. **"Latest" needs the API.** The client takes `latest` from the version list.
   Without the API there is no list, and the client asks for an exact version.
3. **The JSON is optional.** For an exact version without an API answer, the
   client downloads the archive and inspects it locally:
   - It reads the archive with the server's own loader, which refuses it
     unless `surrealism/MANIFEST` lists every file except itself and
     `surrealism/SIGNATURE` with a matching hash, no path appears twice, every
     path is canonical, and every entry is a regular file or a directory. The
     digest is `sha256:` and the SHA-256 of the `surrealism/MANIFEST` bytes.
   - It reads `surrealism/package.toml`, or `surrealism/surrealism.toml` when
     that is the only one, for the requested capabilities, and refuses the
     archive unless that file names the requested organisation, package and
     version.
   - It reads `surrealism/exports.toml` for the exports.
   - It keeps `surrealism/SIGNATURE` as raw bytes for signature verification. A
     locally inspected archive has no `registered_publisher`, which only Silo
     can answer.
4. **The client cross-checks what it can.** When the version list and the
   details both give a digest for one version, they must match, and the
   archive's own manifest digest must match the details.
5. **An installing client verifies the signature itself.** It downloads the
   archive even when the API describes the release, verifies
   `surrealism/SIGNATURE` with the verifier the server runs, and records in
   `SIGNER` only an anchor that proved it. The API's `signer` is shown beside
   the proved one, never trusted in its place, and `registered_publisher` is
   shown only when the API names exactly the signer the signature proved.
6. **The server trusts neither the API nor the client.** The server fetches the
   archive from the static URL, and refuses a definition whose `DIGEST` does
   not match the archive's manifest or whose `SIGNER` its `SIGNATURE` does not
   prove. What the person approved is therefore
   bound to what runs, even if the API or a mirror is wrong.

## Error semantics

- **Not found** is a `404` from the archive URL, or a `404` from the version
  list when "latest" is requested. The client stops with an error that names the
  package and the endpoint.
- **Yanked** is a warning, never a refusal. A yanked version still resolves when
  it is asked for by exact version, and the client prints the warning in the
  install prompt. A yanked release stays downloadable so that existing
  definitions keep loading.
- **Advisories** are shown in the install prompt. They do not block an install.
- A malformed response (invalid JSON, a bad digest, a version with a suffix)
  is an error.

## Later work

**Publishing.** `surreal extension publish` uploads an archive with a
short-lived CI identity token (OIDC), which Silo matches against the
publishers registered for the package. Its request shape, the checks Silo runs
on upload, and the management endpoints (yank, transfer, publisher settings)
are specified separately.
