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
- `kv()` — returns the per-module `BTreeMapStore`.
- `stdout` / `stderr` — routes to `tracing` with the configured log level.

**Capability scoping:** Both `sql()` and `run()` create a derived `Context` whose `Capabilities` are narrowed to the module's declared permissions via `module_scoped_capabilities()`. This ensures a module cannot exceed its declared `allow_scripting`, `allow_functions`, or `allow_net` even if the server is more permissive. The server's deny-lists are always preserved.

### `SurrealismCache` (`cache.rs`)

Caches compiled `Runtime` instances using `quick_cache`. Key variants:

- `File(ns, db, bucket, key)` — for user-uploaded modules
- `Silo(org, pkg, major, minor, patch)` — for silo-hosted packages

Weight is based on WASM binary size (in MB); larger modules are evicted first under memory pressure. Default budget is `SURREALISM_CACHE_SIZE * 100 MB`.

A zero-allocation lookup type (`SurrealismCacheLookup`) avoids cloning strings on cache hits.

### Silo resolution (`silo.rs`)

A `silo::` executable names an organisation, a package and an exact version, which map onto one immutable object at `{surrealism_silo_endpoint}/{org}/{pkg}/{major}.{minor}.{patch}.surli` (default endpoint `https://silo.surrealdb.com`). Point the endpoint at a mirror to serve packages from elsewhere; no `DEFINE MODULE` statement changes.

Both names come straight from the parser, where a backtick-quoted identifier can hold `/` or `..`, so `package_url` restricts them to ASCII alphanumerics, `-` and `_` before they reach a URL path segment.

A silo fetch is first-party package resolution, not user-directed traffic, so it needs no `allow_net` grant: the client is built allowing only the endpoint's host. The server's deny-list is still passed through, so an operator who explicitly denies that host blocks the fetch.

## Signing

Nothing is signature-verified yet, so `DEFINE MODULE` requires a trailing `UNSIGNED` clause — unordered with `COMMENT` and `PERMISSIONS` — and stores that choice on the definition (`StoredModuleDefinition::unsigned`, revision 2; definitions written before it decode as unsigned). Requiring the opt-out now means a definition written today keeps its meaning once Silo signs on upload and the keyword starts gating a real check.

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
