# SurrealDB Core — Surrealism Integration

Essential context for AI assistants working with the core-side Surrealism integration.

## Purpose

This module bridges extensions into the SurrealDB query engine. It provides the runner interface (`ExtensionRunner` / `PreparedExtension`, `runner.rs`) with the wasmtime implementation `SurrealismRunner`, the concrete `InvocationContext` implementation (`Host`) and the cache (`SurrealismCache`).

### Runners (`runner.rs`)

The runner is installed through `Builder::with_extension_runner`, or by the composer's `TransactionBuilderFactory::extension_runner`. The `surreal` binary and `CommunityComposer` install `SurrealismRunner`. Without one, definitions are accepted and invocations fail with `NO_RUNNER`.

- `prepare(VerifiedSource)` — once per `ManifestDigest` (`sha256(surrealism/MANIFEST)`, checked against the definition's `DIGEST` and `SIGNER` before preparing).
- `PreparedExtension::invoke(Invocation)` — one invocation in fresh memory, with the definition's `RunnerCapabilities` and `Limits { deadline, depth }`.
- Nesting: `Context::extension_depth` counts enclosing invocations; `Host::module_context` adds one. `run_on_runtime` refuses depth `>= MAX_EXTENSION_DEPTH` (8), and a nested invocation never waits for a slot.

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

**Capability scoping:** Both `sql()` and `run()` create a derived `Context` whose `Capabilities` are the invocation's `RunnerCapabilities::scoped`: the server's capabilities narrowed to the definition's `CAPABILITIES`. `sql()` also requires `queries` in the grant, and `run()` requires the function in its `functions`. The server's deny lists are always preserved. The archive's own `[capabilities]` request in `package.toml` is never read here.

### `SurrealismCache` (`cache.rs`)

Three `quick_cache` maps:

- **Prepared**, keyed by `ManifestDigest`: the runner's compiled extension. It holds no tenant state, so every definition whose archive has the same manifest digest shares one entry, in any namespace or database.
- **Sources**, keyed by `File(ns, db, bucket, key)` / `Silo(ns, db, org, pkg, major, minor, patch)` and the `DIGEST` the definition pins: that the source delivered that archive. Two definitions that name one source but pin different archives keep separate entries. Removing a definition evicts only its source entry.
- **Grants**, keyed by a definition's `CAPABILITIES` and the datastore's capabilities `Arc` (compared by identity): the `RunnerCapabilities` an invocation of such a definition gets. `Datastore::set_capabilities` swaps in a new `Arc`, so grants built from the old one are never served again and age out.

A definition with no source entry for its `DIGEST`, or whose prepared entry was evicted, re-fetches the archive, so a definition always runs the archive it pins. Each map holds at most `SURREALISM_CACHE_SIZE` entries. A zero-allocation lookup type (`SurrealismCacheLookup`) avoids cloning strings on hits.

### Silo resolution (`silo.rs`)

A `silo::` executable names an organisation, a package and an exact version, which map onto one immutable object at `https://silo.surrealdb.com/{org}/{pkg}/{major}.{minor}.{patch}.surli` (`config::SILO_ENDPOINT`). There is one registry, so a release build cannot change the endpoint; an archive served from elsewhere is defined from a bucket. A build with debug assertions reads `SURREAL_SURREALISM_SILO_ENDPOINT`, so the integration tests can serve packages from a local registry.

Both names come straight from the parser, where a backtick-quoted identifier can hold `/` or `..`, so `package_url` restricts them to ASCII alphanumerics, `-` and `_` before they reach a URL path segment.

A silo fetch is first-party package resolution, not user-directed traffic, so it needs no `allow_net` grant: the client is built allowing only the endpoint's host. The server's deny-list is still passed through, so an operator who explicitly denies that host blocks the fetch.

## Definition clauses

`DEFINE EXTENSION` takes four trailing clauses beside `COMMENT` and `PERMISSIONS`, in any order, each at most once (`StoredModuleDefinition` revision 4):

- `DIGEST "sha256:…"` pins the code. When the statement leaves it out, executing the statement fetches the archive and records `sha256(surrealism/MANIFEST)` (`extension::archive::fetch_manifest_digest`) before the definition is written. A build without the `surrealism` feature cannot read archives, so there the statement must give `DIGEST`. Nothing is recorded at load time.
- `SIGNER { sigstore: { … } }` or `SIGNER { key: "…" }` names the anchor and identity rule a signature must satisfy; `UNSIGNED` waives the check. Every definition has exactly one of the two. The parser checks their shape; `extension/trust.rs` checks a signature against them.
- `CAPABILITIES { … }` is everything the module may do; an absent clause grants nothing. `extension::effective_capabilities` intersects it with the server's capabilities.
- `BINDINGS { name: expression, … }` gives the values the guest reads through its `binding` import. The definition also records the `auth_limit` of the session that wrote it (a template hole, `session_auth_limit`). See *Bindings* below.

## Bindings (`bindings.rs`)

`StoreInstr::VerifyModule` runs `check_definition` against the prepared archive's `package.toml` when it has the archive. An archive whose ABI lacks `Features::BINDINGS` (ABI 4) takes no `BINDINGS`, and its `[bindings]` table is not read. Otherwise, against that table: a bound name the package does not declare, or a declared name whose kind cannot be `NONE` left unbound, refuses the statement. A definition restored before its archive exists is not checked there.

`run_on_runtime` lends every invocation a `DefinitionBindings`. On the first read of a name in that invocation it evaluates the definition's expression through `exec::entry` in `Context::new_isolated` of the caller's context (so `LET` bindings, request parameters and the row are out of scope; `$auth`, `$session`, `$token`, `$access` are in), with `opt.limited_by(auth_limit).new_for_extension_binding()`: permissions off, the caller's level capped at the definer's (never raised), and `NoWriteFrame::ExtensionBinding`, which refuses writes, external writes, state-advancing and blocking builtins, and every call into an extension (`NoWriteFrame::refuses_extension_call`, judged where the call is made, so a stored function redefined after the `DEFINE` cannot reach one). `DEFINE` also judges each binding as a schema clause (`ClauseCheck::refuse_clause("BINDINGS", …)`). The value is coerced to the declared kind and kept for the rest of the invocation. Every failure names the binding and the extension only, never the value or the evaluation's error.

## Trust: DIGEST and SIGNER

`Context::get_surrealism_module(lookup, trust)` takes the `Trust` (`DIGEST`, `SIGNER`) of the definition being called and returns a module only when the archive may run under it (`TrustStore::ensure` in `extension/trust.rs`): its manifest digest equals `DIGEST`, and with a `SIGNER` a `SIGNATURE` proves it. The check runs before the archive is prepared and again on every call, because one prepared entry (keyed by digest) serves every definition whose archive has that digest, and those definitions can name different signers. Before preparing, only `surrealism/MANIFEST` and `surrealism/SIGNATURE` are read (`trust_files_from_reader`); the runner then unpacks the archive, which checks every file against the manifest, and refuses a package whose manifest digest is not the verified one. A failure to load is cached per database, source, `DIGEST` and signer for `REFETCH_AFTER_REFUSAL`, so a failing definition is not fetched on every call.

The `TrustStore` lives in `SurrealismCache`, one per datastore. Only the `SIGNATURE` of the definition's own source counts: a cached module keeps the signature its archive carried, and every load verifies it against the loading definition's `SIGNER`, so a copy without its own signature is refused whatever this process has verified before. Proofs are cached per (`DIGEST`, `signer_fingerprint`, SHA-256 of the signature). Before refusing, the definition's own source is fetched once more for its current `SIGNATURE`; when that signature proves the `SIGNER`, the cached module is dropped so the next load rebuilds it with the new signature. Without a `SIGNER`, an archive runs only when the definition is `UNSIGNED`. A refusal, from that check or from building the module (a fetch error, a digest mismatch), is recorded per (`ns`, `db`, source, `DIGEST`, signer fingerprint) and repeated without any refetch or verification for `REFETCH_AFTER_REFUSAL` (10 s). It is consulted only for the same source of the same database, so one tenant's failing source never refuses another tenant's definition or shows it the reason; a successful `DEFINE` clears it. A `Trust` carries its signer's fingerprint, computed once per call. On a cache fill, only `MANIFEST` and `SIGNATURE` are read before the check; the archive is unpacked only after it passes. `DEFINE` builds the module from the archive it just checked, so the `LoadModule` after it does not fetch again.

`DEFINE EXTENSION` runs the same check through `StoreInstr::VerifyModule` (`extension::archive::verify_definition`), pinning `DIGEST` when the statement gave none. An archive it cannot fetch fails the statement without `DIGEST`, and is accepted with one (an import restores before its archives exist); the load-time check still applies. During an import, a definition without `DIGEST` is one a release before revision 3 wrote (an export writes `DIGEST` for every definition that records one): it is restored as a definition to reinstall, with no capabilities, and nothing is fetched.

A definition stored before revision 3 decodes with all three fields `None`. It is listed and removable, `INFO … STRUCTURE` carries a reinstall warning, and a call into it is refused before its archive is fetched.

## Capability Enforcement Flow

1. **First call per (`CAPABILITIES`, server capabilities):** `run_on_runtime` asks `SurrealismCache::grant`, which builds `RunnerCapabilities` with `host::runner_capabilities(effective_capabilities(definition, server), …)`: the scoped `Capabilities`, the network policy, the HTTP client and the limits. `server` is `Context::server_capabilities`, the datastore's capabilities, never the context's. An extension that another extension calls through `tx.run` runs with its own definition's grant, even where that is more than its caller's; whether the caller may call it at all is checked against the caller's grant before the definition is read.
2. **Runtime:** the scoped `Capabilities` is installed on a derived `Context` for every `sql()` and `run()` call; the runner applies the network policy and limits to the fresh instance.

## Key Files

| File | Purpose |
|------|---------|
| `host.rs` | `Host` struct, `InvocationContext` impl, `runner_capabilities` |
| `bindings.rs` | `DefinitionBindings` (the `binding` import's answers), `check_definition` |
| `runner.rs` | `ExtensionRunner`, `PreparedExtension`, `Invocation`, `RunnerCapabilities`, `SurrealismRunner` |
| `cache.rs` | `SurrealismCache`: prepared by digest, sources by tenant, grants by `CAPABILITIES` |
| `silo.rs` | Package URL construction and the `.surli` download |
| `config.rs` | Server-side ceilings, and the silo endpoint a debug build may override |
| `mod.rs` | Module declarations |
