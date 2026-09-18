# Surrealism WASM Plugin System

Essential context for AI assistants working with the Surrealism codebase.

## Architecture Overview

Surrealism is SurrealDB's WASM plugin system. Guest modules are compiled to WASM components (WASI Preview 2) and run inside the host SurrealDB process. Communication uses WIT interfaces and FlatBuffers serialization.

## Crate Relationships

| Crate | Role |
|-------|------|
| **surrealism** | Guest SDK. Used by plugin authors. Provides the `#[surrealism]` attr and the `Transaction` handle through which a plugin reaches `sql` / `run`. |
| **surrealism-runtime** | Host. Compiles, instantiates, and executes guest modules. Integrates with SurrealDB. |
| **surrealism-types** | Shared types and encoding. Used by both host and guest. |
| **surrealism-macros** | Proc macros. Expands `#[surrealism]` into WIT exports and inventory registration. |

## Build / Test / Deploy Workflow

- **Build:** `surreal module build` — compiles the Rust project to `wasm32-wasip2`, optimizes with wasm-opt, packs into a `.surli` archive.
- **Run:** `surreal module run <file.surli> [--fn <name>] [args...]` — loads the archive, invokes the specified function (or default).
- **Info:** `surreal module info <file.surli>` — inspects exports without executing.

Archives are self-contained; load them via `SurrealismPackage::from_file(path)`.

## Loading a module from SurrealQL

A module executable is either a `.surli` in a bucket or a Silo package named by exact version:

```surql
DEFINE MODULE mod::color FROM f"modules:/color.surli" UNSIGNED;
DEFINE MODULE silo::surrealdb::color::<1.0.0> UNSIGNED;
```

A silo package is downloaded from `{surrealism_silo_endpoint}/{org}/{pkg}/{major}.{minor}.{patch}.surli`, so it needs no local build and no bucket. The named form takes a `mod::` name; the silo form is keyed by its package coordinates and takes none.

The keyword before the executable is `FROM`, not `AS`: what follows is the module's source, and `AS` would invert the sense it carries everywhere else in SurrealQL, where the alias sits on the right.

`UNSIGNED` is a trailing clause, unordered with `COMMENT` and `PERMISSIONS`, and mandatory on both forms. No module source is signature-verified yet, so every module opts out of verification explicitly; once Silo signs on upload, the keyword becomes the opt-out it names and definitions written today keep their meaning.

## Security Model

- **Capabilities-based:** Modules declare needs in `surrealism.toml` (`[capabilities]`). Server validates at load time and can further restrict.
- **Non-ambient host access:** `sql` and `run` are methods on a `transaction` handle the host lends for one `invoke` call and revokes on return. An export lent no handle — `init` and the introspection exports — has no route to the datastore, and a retained handle traps. This replaces the old swap-a-context-on-the-store scheme.
- **Runtime scoping:** When a module calls `sql()` or `run()`, the host creates a derived context whose capabilities are narrowed to the module's declared `allow_scripting`, `allow_functions`, and `allow_net`. Server deny-lists are always preserved.
- **Read-only is declared, not yet enforced:** a function's transaction parameter sets its `writeable` flag — `Transaction` (the default) reports `false`, `Transaction<ReadWrite>` reports `true`. The flag feeds planning and scheduling (`SurrealismModuleExec::access_mode`), but module calls run through the legacy `run_on_runtime` path and no read-only datastore transaction is opened for them, so a read-only function that issues `CREATE` currently succeeds. `module_read_only_write_refused` in `tests/surrealism_integration.rs` is the ignored acceptance test for closing this. Moving the flag from an attribute to the parameter type is what makes it trustworthy enough to enforce on.
- **`init` cannot query:** it runs once per instance, under whichever request created it, and its effects persist in linear memory for every later request. It is lent no handle, and the macro rejects `async fn` on it.
- **Deny-by-default for functions:** `allow_functions` defaults to empty (deny all). Use `["*"]` to allow all, or specific patterns like `["http::*", "fn::user_exists"]`.
- **Module isolation:** WASM linear memory is per-instance, and a module is cached per namespace/database, so its instances and statics never span a tenant boundary.
- **`strict_timeout = false`:** Disables epoch-based timeout enforcement entirely. The module can run indefinitely and monopolise a thread. Only trusted modules should set this. Future code-signing support will gate which modules may request it; until then the server operator accepts full responsibility.

## Archive Format

`.surli` = zstd-compressed tar archive containing:

- `surrealism/mod.wasm` — WASM component binary
- `surrealism/surrealism.toml` — config (package meta, capabilities, ABI)
- `surrealism/exports.toml` — function signatures (args/returns) extracted at build
- `surrealism/fs/*` — optional read-only filesystem attachment

## ABI Versioning

ABI version 3 is current. Stored in `surrealism.toml`. Build tool stamps `abi = 3`; user values are ignored. Older `p1`/`1` for Preview 1 is legacy.

An archive whose `abi` does not match is rejected at load with `UnsupportedAbi`, so a module built against an older SDK must be rebuilt rather than failing later with a missing-import error from the linker.

The number moves at most once per release that changes the world, not once per
change. Inside a release cycle the world may move several times under one ABI
number, and an archive built earlier in that cycle passes the version gate and
then fails in `instantiate_pre` with a linker import mismatch — rebuild it. That
window is between unreleased commits only, which is the whole point of spending
one number rather than one per change: the gate exists for archives that exist
in the world, and only a release puts one there.

## Async model

`invoke` is lifted async, so a plugin's `async fn` is driven by the component
model itself: the host re-enters the guest as each pending host call resolves.
Two `tx` reads awaited together genuinely overlap.

The host orders those calls by what each one carries, not by the mode marker on
the handle. A statement that may write runs alone; a read runs alongside other
reads; a `tx.run` is opaque, so it counts as writing. They all execute on the
caller's single transaction, whose savepoints are a stack with no handle, and a
second writer entering before the first has left would unwind the wrong one. A
handle may hold at most 64 calls in flight; beyond that they are refused.

There is no executor in the guest. The SDK has no tokio dependency and modules
need no build flags. A plugin may still build its own runtime for a dependency
that demands one, but must never await a `tx` method inside a nested `block_on`:
a `tx` call is a component-model subtask that can only complete once the guest
yields back to the host, which a blocking executor never does.

## Networking

A module reaches the network through ordinary Rust crates and `std::net`, not
through a bespoke host binding. Two checks gate it, both driven by `allow_net`:

- **By name.** `surrealism_runtime::dns` shadows `wasi:sockets/ip-name-lookup`
  and matches each name against the module's rules before resolving. Names are
  matched exactly and case-insensitively; `api.example.com` does not cover
  `sub.api.example.com`.
- **By address.** WASI's `socket_addr_check` gates every connect.

Hostnames are **not** resolved at module load. A stale pre-resolved address
cannot be refreshed without reloading the module, so names are matched as names
and the addresses the shim vends are recorded for the subsequent connect.

A resolved address that is loopback, private, link-local, multicast or otherwise
special is dropped unless an explicit IP rule covers it — so `allow_net =
["localhost"]` alone reaches nothing, and `["localhost", "127.0.0.1"]` reaches
loopback. This stops a name someone else controls from pointing at a metadata
endpoint.

### Library support on `wasm32-wasip2`

Measured against the filtering shim, using each crate's own documented API. "No
workarounds" means no `--cfg` flags, no patched crates, no hand-written protocol
code, and no pinning a hostname to a literal address.

| Crate | Status | Notes |
|---|---|---|
| `std::net` | **works** | Hostnames included. The baseline. |
| `ureq` 3 | **works** | Full sync HTTP client with TLS. Needs `default-features = false, features = ["rustls"]`. |
| `rustls` 0.23 | **works** with `ring` | The default `aws-lc-sys` provider is C and will not cross-compile; select `ring` explicitly. |
| `hyper` 1 | compiles | Runtime-agnostic; needs an IO adapter, so not exercised end to end. |
| `wstd` 0.6 | partial | Panics on an IPv6 address (`todo!()` in `wstd::net::TcpStream`). Fine over IPv4, which is what the special-address filter leaves for `localhost`. |
| `tokio` (`net`) | **needs `--cfg tokio_unstable`** | Sockets work once compiled, but only when connecting to a `SocketAddr`. Its `(host, port)` resolution goes through `spawn_blocking`, which needs threads and panics on wasm. |
| `reqwest` 0.13 | **does not work** | Needs the tokio flag, and then panics in `hyper-util`'s resolver for the same reason. |

`--cfg tokio_unstable` is the plugin's to set, not the SDK's: a module that
wants a tokio-based client opts in itself — and will still hit the resolution
panic, so prefer a sync client or `wstd` until that is fixed upstream.

Reading a response body to EOF (an HTTP/1.0 or `Connection: close` reply) fails
with WASI `EIO` on the readiness-driven path. Responses carrying
`Content-Length` or chunked encoding are unaffected, which covers nearly every
real API. This is a wasi-libc/std issue, not a host one: a
`wasmtime_wasi=trace` dump shows every host call returning `Ok`.

## Common Commands

```bash
surreal module build              # build from current dir
surreal module build -o out.surli  # custom output
surreal module run pkg.surli      # run default export
surreal module run pkg.surli --fn "mymod::greet" "hello"  # named export + args
surreal module info pkg.surli     # list exports
```

## Project Structure

- `surrealism/` — guest SDK
- `surrealism/runtime/` — host runtime
- `surrealism/macros/` — proc macros
- `surrealism/types/` — shared types
- `surrealism/wit/` — WIT interfaces
- `surrealism/demo/` — example plugin
