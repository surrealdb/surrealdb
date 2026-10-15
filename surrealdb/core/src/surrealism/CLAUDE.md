# SurrealDB Core — Surrealism Integration

Essential context for AI assistants working with the core-side Surrealism integration.

## Purpose

This module bridges the `surrealism-runtime` crate into the SurrealDB query engine. It provides the concrete `InvocationContext` implementation (`Host`) and the module cache (`SurrealismCache`).

## Key Components

### `Host` (`host.rs`)

Implements `surrealism_runtime::host::InvocationContext` for use inside SurrealDB. Holds the query context (`FrozenContext`), options (`Options`), and current document (`CursorDoc`).

**Host calls:**

- `sql(query, vars)` — parses the query string as a SurrealQL expression, computes it against a module-scoped context.
- `run(fnc, version, args)` — resolves the function name (silo, ml, or built-in), builds an `Expr::FunctionCall`, and evaluates it against a module-scoped context.

Both take `&self` and return `HostCallResult`, classifying failures as
`PermissionDenied` (capability denial), `InvalidRequest` (parse/resolve),
`Datastore` (execution failures) or
`Other`. `&self` rather than `&mut self` because a guest may hold several calls
in flight.
- `stdout` / `stderr` — routes to `tracing` with the configured log level.

**Capability scoping:** Both `sql()` and `run()` create a derived `Context` whose `Capabilities` are narrowed to the module's declared permissions via `module_scoped_capabilities()`. This ensures a module cannot exceed its declared `allow_scripting`, `allow_functions`, or `allow_net` even if the server is more permissive. The server's deny-lists are always preserved.

### `SurrealismCache` (`cache.rs`)

Caches compiled `Runtime` instances using `quick_cache`. An entry is keyed by its source and the `DIGEST` it was built for, so two definitions that name one source but pin different archives keep separate modules. Source variants:

- `File(ns, db, bucket, key)` — for user-uploaded modules
- `Silo(ns, db, org, pkg, major, minor, patch)` — for silo-hosted packages

Both variants are scoped by namespace and database. A `Runtime` pools
controllers whose WASM linear memory survives between invocations, so an entry
shared by two databases would leak module state across the tenant boundary. Two
databases resolving the same silo package therefore compile and hold it
separately.

Weight is based on WASM binary size (in MB); larger modules are evicted first under memory pressure. Default budget is `SURREALISM_CACHE_SIZE * 100 MB`.

A zero-allocation lookup type (`SurrealismCacheLookup`) avoids cloning strings on cache hits.

### Silo resolution (`silo.rs`)

A `silo::` executable names an organisation, a package and an exact version, which map onto one immutable object at `{surrealism_silo_endpoint}/{org}/{pkg}/{major}.{minor}.{patch}.surli` (default endpoint `https://silo.surrealdb.com`). Point the endpoint at a mirror to serve packages from elsewhere; no `DEFINE MODULE` statement changes.

Both names come straight from the parser, where a backtick-quoted identifier can hold `/` or `..`, so `package_url` restricts them to ASCII alphanumerics, `-` and `_` before they reach a URL path segment.

A silo fetch is first-party package resolution, not user-directed traffic, so it needs no `allow_net` grant: the client is built allowing only the endpoint's host. The server's deny-list is still passed through, so an operator who explicitly denies that host blocks the fetch.

## Definition clauses

`DEFINE EXTENSION` takes three trailing clauses beside `COMMENT` and `PERMISSIONS`, in any order, each at most once (`StoredModuleDefinition` revision 3):

- `DIGEST "sha256:…"` pins the code. When the statement leaves it out, executing the statement fetches the archive and records `sha256(surrealism/MANIFEST)` (`extension::archive::fetch_manifest_digest`) before the definition is written. A build without the `surrealism` feature cannot read archives, so there the statement must give `DIGEST`. Nothing is recorded at load time.
- `SIGNER { sigstore: { … } }` or `SIGNER { key: "…" }` names the anchor and identity rule a signature must satisfy; `UNSIGNED` waives the check. Every definition has exactly one of the two. The parser checks their shape; `extension/trust.rs` checks a signature against them.
- `CAPABILITIES { … }` is everything the module may do; an absent clause grants nothing. `extension::effective_capabilities` intersects it with the server's capabilities.

## Trust: DIGEST and SIGNER

`Context::get_surrealism_module(lookup, trust)` takes the `Trust` (`DIGEST`, `SIGNER`) of the definition being called and returns a module only when the archive may run under it (`TrustStore::ensure` in `extension/trust.rs`): its manifest digest equals `DIGEST`, and with a `SIGNER` a `SIGNATURE` proves it. The check runs before the archive is compiled into the cache and again on every call, because one cache entry (keyed by source) serves every definition that names that source. A cache entry whose digest is not the pinned one is dropped and the source fetched again.

The `TrustStore` lives in `SurrealismCache`, one per datastore. Only the `SIGNATURE` of the definition's own source counts: a cached module keeps the signature its archive carried, and every load verifies it against the loading definition's `SIGNER`, so a copy without its own signature is refused whatever this process has verified before. Proofs are cached per (`DIGEST`, `signer_fingerprint`, SHA-256 of the signature). Before refusing, the definition's own source is fetched once more for its current `SIGNATURE`; when that signature proves the `SIGNER`, the cached module is dropped so the next load rebuilds it with the new signature. Without a `SIGNER`, an archive runs only when the definition is `UNSIGNED`. A refusal, from that check or from building the module (a fetch error, a digest mismatch), is recorded per (`ns`, `db`, source, `DIGEST`, signer fingerprint) and repeated without any refetch or verification for `REFETCH_AFTER_REFUSAL` (10 s). It is consulted only for the same source of the same database, so one tenant's failing source never refuses another tenant's definition or shows it the reason; a successful `DEFINE` clears it. A `Trust` carries its signer's fingerprint, computed once per call. On a cache fill, only `MANIFEST` and `SIGNATURE` are read before the check; the archive is unpacked only after it passes. `DEFINE` builds the module from the archive it just checked, so the `LoadModule` after it does not fetch again.

`DEFINE EXTENSION` runs the same check through `StoreInstr::VerifyModule` (`extension::archive::verify_definition`), pinning `DIGEST` when the statement gave none. An archive it cannot fetch fails the statement without `DIGEST`, and is accepted with one (an import restores before its archives exist); the load-time check still applies. During an import, a definition without `DIGEST` is one a release before revision 3 wrote (an export writes `DIGEST` for every definition that records one): it is restored as a definition to reinstall, with no capabilities, and nothing is fetched.

A definition stored before revision 3 decodes with all three fields `None`. It is listed and removable, `INFO … STRUCTURE` carries a reinstall warning, and a call into it is refused before its archive is fetched.

## Capability Enforcement Flow

1. **Load time:** `validate_surrealism_capabilities()` (in `dbs/capabilities.rs`) checks that the module's declared capabilities are a subset of the server's.
2. **Runtime:** `module_scoped_capabilities()` (in `host.rs`) clones the server caps and narrows them to the module's declared set. This scoped `Capabilities` is installed on a derived `Context` for every `sql()` and `run()` call.

## Key Files

| File | Purpose |
|------|---------|
| `host.rs` | `Host` struct, `InvocationContext` impl, `module_scoped_capabilities` |
| `cache.rs` | `SurrealismCache`, cache key/lookup types, weight function |
| `silo.rs` | Package URL construction and the `.surli` download |
| `config.rs` | Server-side ceilings and the silo endpoint |
| `mod.rs` | Module declarations, `validate_surrealism_capabilities` |
