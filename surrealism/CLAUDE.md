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
| **surrealism-macros** | Proc macros. Expands `#[surrealism]` into WIT exports and inventory registration. Functions added at run time go through `surrealism::register` instead. |
| **surrealism-verify** | Archive signature verification: `verify(DIGEST, SIGNATURE, SIGNER)`. Pure, no I/O or clock, builds for wasm32. Accepts Sigstore bundle v0.3 (Rekor v2 + RFC 3161 timestamp) and SSH signatures. |

## Build / Test / Deploy Workflow

- **Build:** `surreal extension build` — compiles the Rust project to `wasm32-wasip2`, optimizes with wasm-opt, pre-initialises it by snapshotting `init` (see Security Model), packs into a `.surli` archive.
- **Run:** `surreal module run <file.surli> [--fn <name>] [args...]` — loads the archive, invokes the specified function (or default).
- **Info:** `surreal module info <file.surli>` — inspects exports without executing.

Archives are self-contained; load them via `SurrealismPackage::from_file(path)`.

## Loading a module from SurrealQL

An extension's archive is either a `.surli` in a bucket or a Silo package named by exact version:

```surql
DEFINE EXTENSION ext::color FROM f"modules:/color.surli" SIGNER { key: "ssh-ed25519 AAAA… release@acme" };
DEFINE EXTENSION ext::palette FROM silo::surrealdb::palette<1.0.0> UNSIGNED;
```

A silo package is downloaded from `https://silo.surrealdb.com/{org}/{pkg}/{major}.{minor}.{patch}.surli`, the one registry, so it needs no local build and no bucket. That URL and the optional package API that clients use to resolve and describe a version are specified in `SILO.md`.

The keyword before the executable is `FROM`, not `AS`: what follows is the extension's source, and `AS` would invert the sense it carries everywhere else in SurrealQL, where the alias sits on the right.

Every definition pins `DIGEST` (`sha256(surrealism/MANIFEST)`, computed at `DEFINE` when the statement leaves it out) and states at most one of `SIGNER` and `UNSIGNED`. Outside an import it must state one; an import stores a definition with neither, or without `DIGEST`, as a legacy row that does not run until it is defined again. The server runs an archive only when its manifest digest equals `DIGEST` and, with a `SIGNER`, a `surrealism/SIGNATURE` proves that signer through `surrealism-verify`. It checks at `DEFINE` and before every load; see `surrealdb/core/src/extension/trust.rs`. `UNSIGNED` waives the signature only.

## Security Model

- **Capabilities-based:** only the definition grants anything: `DEFINE EXTENSION … CAPABILITIES { … }`, intersected with the server's capabilities on every invocation. An absent clause grants nothing. The `[capabilities]` table in `package.toml` is the author's request, which the server never reads; it documents what the extension needs. An extension that another one calls through `tx.run` runs with its own definition's grant.
- **Non-ambient host access:** `sql` and `run` are methods on a `transaction` handle the host lends for one `invoke` call and revokes on return. An export lent no handle — `init` and the introspection exports — has no route to the datastore, and a retained handle traps. This replaces the old swap-a-context-on-the-store scheme.
- **Runtime scoping:** When a module calls `sql()` or `run()`, the host creates a derived context whose capabilities are narrowed to the definition's `scripting`, `functions` and `net`. `sql()` also needs `queries`, and a writing export without `write` gets the read-only frame. Server deny-lists are always preserved.
- **Read-only is enforced:** a function declares whether it needs write access, which reaches the host as the `writeable` flag in the exports manifest. When it is `false`, `Host::new` installs `NoWriteFrame::SurrealismReadOnly`, and a data-modifying statement is refused where it is reached — the same enforcement point that stops a `SELECT` `PERMISSIONS` predicate and a `COMPUTED` body from writing (`legacy/expr/expression.rs`). A definition-time check could not stand in for it: the guest composes its query as a string at runtime, so only reaching the statement settles whether it writes. How the declaration is spelled is the guest language's business — in the Rust SDK it is `Transaction` versus `Transaction<ReadWrite>`. Covered by `module_read_only_write_refused`.
- **`init` cannot query:** `surreal extension build` runs it once and ships the memory it leaves as the module's initial state, so it runs before any datastore or request exists. It is lent no handle, and the macro rejects `async fn` on it. The build runs it against a stub WASI: the attached `fs/` is preopened, network use fails the build, a WASI resource still open when it returns fails the build (writing to stdout or stderr from `init` is one), and reads of randomness or the clock are allowed and listed in the build output. Every value `init` reads is frozen into the archive and so into every instance: a random seed, a clock reading, and the random keys of a `HashMap` built in `init` are the same in every invocation and on every server. The server refuses a component that still exports `init`.
- **Deny-by-default for functions:** `functions` defaults to none. Use `true` for whatever the server allows, or patterns like `["http::*", "fn::user_exists"]`.
- **Fresh memory per invocation:** every invocation instantiates afresh from the compiled component into a pooling-allocator slot and drops the instance on return. Statics, heap and WASI handles never survive a call, so compiled code is shared across tenants by archive digest while state is not. `SURREAL_SURREALISM_MAX_CONCURRENT_INVOCATIONS` is the slot count; a nested invocation (`tx.run` into an extension) never waits for a slot, and nesting stops at depth 8.
- **`strict_timeout: false`:** Disables epoch-based timeout enforcement entirely. The module can run indefinitely and monopolise a thread. A definition can ask for it, but it takes effect only when the operator sets `SURREAL_SURREALISM_ALLOW_UNBOUNDED_EXECUTION=true`; otherwise every invocation runs on the guarded engine.

## Archive Format

`.surli` = zstd-compressed tar archive containing:

- `surrealism/mod.wasm` — WASM component binary
- `surrealism/package.toml` — package file (package meta, capabilities, ABI)
- `surrealism/exports.toml` — function signatures (args/returns) extracted at build
- `surrealism/fs/*` — optional read-only filesystem attachment
- `surrealism/MANIFEST` — every other file with its SHA-256 hash; its own hash is the archive's digest
- `surrealism/SIGNATURE` — optional; the only file the manifest does not list. Either an armored SSH signature, or for Sigstore a JSON envelope (`application/vnd.surrealdb.extension-signature.v1+json`) holding the bundle and a `trusted_root.json` reduced to the anchors that verify it, so the archive verifies offline (`surrealism_verify::verify_offline`). The server ignores the embedded root and verifies only against the definition's `SIGNER`

The manifest's bytes are exact, because the digest hashes them: UTF-8 with no
BOM, one line per file, `sha256:` + 64 lowercase hex + two spaces + the path,
LF after every line, paths sorted by raw bytes. `manifest.rs` writes and parses
it; `Manifest::parse` accepts only what `Manifest::from_entries` writes.

`SurrealismPackage::from_reader` hashes each file in its single pass and returns
the package only if the archive passes four rules:

1. The manifest lists every file except `SIGNATURE`, and every listed path is a file in the archive with a matching hash.
2. No path appears twice.
3. Paths are canonical: no `./`, no `..`, no empty segment, no backslash, no `:` and no Windows device name (`CON`, `NUL`, `COM1`, …) as a segment, and no PAX or long-name header that renames an entry (a GNU long name whose header holds the truncated path is allowed). `mod.wasm` is read only up to `MAX_WASM_BYTES`.
4. Entries are regular files or directories only, and every directory holds a file the manifest lists (`pack` writes no empty directory).

Every reader decompresses at most `archive_byte_limit(max_fs_bytes)` bytes: `MAX_WASM_BYTES`, the server's attached-file bound, and 64 MiB for the rest.

`digest()` is `sha256(MANIFEST)`; `signature()` returns the raw `SIGNATURE`
bytes without interpreting them. `pack` writes the manifest and returns the
digest.

The package file has a legacy name, `surrealism.toml`. Both names resolve the same way (`PackageFile::locate` in a project, `SurrealismPackage::from_reader` in an archive):

- `package.toml` is read if it exists, otherwise `surrealism.toml`.
- When both exist, `package.toml` wins and `surrealism.toml` is not parsed. `surreal extension build` prints a warning naming the ignored file.
- `surreal extension build` always writes `surrealism/package.toml`, and `surreal extension init` scaffolds only `package.toml`.

## ABI Versioning

ABI version 4 is current: WIT package `surrealism:extension`, world `extension`, and an archive with a `MANIFEST`. Stored in `package.toml`. Build tool stamps `abi = 4`; user values are ignored. Older `p1`/`1` for Preview 1 is legacy.

An archive whose `abi` does not match is rejected at load with `UnsupportedAbi`, which tells the user to rebuild with the 4.0 SDK, rather than failing later with a missing-import error from the linker.

The number moves at most once per release that changes the world, not once per
change. Inside a release cycle the world may move several times under one ABI
number, and an archive built earlier in that cycle passes the version gate and
then fails in `instantiate_pre` with a linker import mismatch — rebuild it. That
window is between unreleased commits only, which is the whole point of spending
one number rather than one per change: the gate exists for archives that exist
in the world, and only a release puts one there.

## Registering functions at run time

`#[surrealism]` fills the function table at compile time. A module that learns
its functions only when it runs — an engine that compiles scripts from the
attached `fs/` — adds them from `init` with `surrealism::register`:

```rust
use surrealdb_types::{Kind, Value};
use surrealism::{FunctionMeta, register, surrealism};

#[surrealism(init)]
fn init() -> anyhow::Result<()> {
    register(
        Some("notes::add"),                     // None = the default export
        FunctionMeta {
            args: vec![("text".into(), Kind::String)],
            returns: Kind::String,
            writeable: true,                    // what Transaction<ReadWrite> says
            declares_transaction: true,         // the handler gets Some(tx)
            comment: Some("Store a note.".into()),
        },
        |tx, args| async move {
            let tx = tx.expect("declares_transaction is set");
            tx.sql_with_vars("CREATE ONLY note SET text = $t RETURN VALUE text", [("t", args[0].clone())]).await
        },
    )
}
```

- **One table.** Every export — `invoke`, `list-functions` and the
  `function-*` introspection — sees the macro's entries followed by the
  registered ones in registration order.
- **Only from `init`.** `register` returns an error before `init` runs and once
  it has returned. The build snapshots memory as `init` leaves it and reads the
  table from the snapshot into `exports.toml`, and the host plans every call
  from that file, so a function added later would never be callable.
- **Unique names.** A name already held by a `#[surrealism]` function or an
  earlier `register` call is an error naming both definitions by `file:line`.
- **The metadata is the signature.** The host checks arguments against `args`,
  lends a handle only when `declares_transaction` is set, and installs the
  read-only frame unless `writeable` is set. The handle's type is
  `Transaction<Declared>`: its mode is that flag, not a type parameter.
- **Linear memory only.** The table is a plain static. The host keeps no copy:
  the build-time snapshot carries it into every instance.

The demo registers `registered::shout`, `registered::add_note` and
`registered::sneaky_write`.

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
need no build flags.

This covers **host imports only**. A future driven by the component model is
one whose waker the host calls, which means `tx.sql()` and `tx.run()`. Guest
network I/O is not in that set: WASI p2 socket calls are sync-lowered, so
`std::net` and sync clients block the task (correct, and no executor needed),
and a tokio-shaped crate still needs its own runtime. That half becomes
host-driven when `wasi:io@0.3` arrives, not before.

### Bringing your own runtime

A plugin may run its own executor for a crate that demands one. Two rules:

- Such an export must **not** take a `Transaction`. A `tx` call is a
  component-model subtask that completes only once the guest returns
  `CallbackCode::Wait` to the host, and a blocking executor never returns. The
  await would hang.
- Declare the export `fn`, not `async fn`. From the host's side it *is*
  synchronous — it blocks until done — and `async` would imply it yields.

Blocking there is legal: a guest-side executor parks on `wasi:io/poll`, an
ordinary synchronous host call, and never touches the component model's
`waitable-set.wait`, which is what would trap.

`#[surrealism]` preserves the attributes you put on a function, so
`#[tokio::main(flavor = "current_thread")]` composes with it. Order matters —
`tokio::main` must be the outer attribute, so it strips `async` before
`#[surrealism]` sees the signature:

```rust
#[tokio::main(flavor = "current_thread")]   // outermost: runs first
#[surrealism]                                // sees a plain `fn`
async fn fetch(url: String) -> anyhow::Result<String> { ... }
```

The default `multi_thread` flavor needs threads and will not work.

This is the form to reach for. *Using reqwest* below builds the runtime by hand
instead, which is the same thing spelled out: it is written that way because the
export there is already a plain `fn`, so the shape this section is about — no
`Transaction`, not `async` — is visible without knowing what `tokio::main`
rewrites. Either compiles to the same thing.

### Using reqwest

reqwest works, with two adjustments it cannot make for us. Uncomment the
`tokio_unstable` line in `.cargo/config.toml`, then:

```toml
reqwest = { version = "0.13", default-features = false, features = ["rustls-no-provider"] }
rustls  = { version = "0.23", default-features = false, features = ["ring", "std", "tls12"] }
tokio = { version = "1", default-features = false, features = ["rt", "net", "io-util", "time"] }
```

`rustls-no-provider` matters: reqwest's default `rustls` feature pulls
`aws-lc-sys`, which is C and will not cross-compile. `ring` will.

Root certificates need no entry of their own. `rustls-no-provider` pulls
`rustls-platform-verifier`, which on `wasm32` depends on `webpki-root-certs` and
uses it — there is no platform trust store to ask.

reqwest's default resolver is `hyper-util`'s `GaiResolver`, which hands the
lookup to `tokio::task::spawn_blocking`. There are no threads, so it panics.
Replace it — `std`'s resolver works here, going through
`wasi:sockets/ip-name-lookup`, and the guest is single-threaded anyway:

```rust
struct WasiResolver;

impl reqwest::dns::Resolve for WasiResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs = std::net::ToSocketAddrs::to_socket_addrs(&(host.as_str(), 0))?;
            Ok(Box::new(addrs) as reqwest::dns::Addrs)
        })
    }
}

#[surrealism]
fn fetch(url: String) -> anyhow::Result<String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    rt.block_on(async {
        let client = reqwest::Client::builder()
            .dns_resolver(std::sync::Arc::new(WasiResolver))
            .build()?;
        Ok(client.get(&url).send().await?.text().await?)
    })
}
```

Measured working over HTTPS. Prefer a sync client such as `ureq` when you can:
it needs none of the above and no executor at all.

## Networking

A module reaches the network through ordinary Rust crates and `std::net`, not
through a bespoke host binding. Two checks gate it, both driven by the `net` targets the definition grants (`true` is spelled `*`: any name and any address that is not special):

- **By name.** `surrealism_runtime::dns` shadows `wasi:sockets/ip-name-lookup`
  and matches each name against the granted rules before resolving. Names are
  matched exactly and case-insensitively; `api.example.com` does not cover
  `sub.api.example.com`.
- **By address.** WASI's `socket_addr_check` gates every connect.

Hostnames are **not** resolved at module load. A stale pre-resolved address
cannot be refreshed without reloading the module, so names are matched as names
and the addresses the shim vends are recorded for the subsequent connect.

A resolved address that is loopback, private, link-local, multicast or otherwise
special is dropped unless an explicit IP rule covers it — so `net:
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
| `reqwest` 0.13 | **works** with setup | Needs the tokio flag, a `rustls`/`ring` provider, and a custom `Resolve`. See *Using reqwest*. |

`--cfg tokio_unstable` is the plugin's to set, not the SDK's: a module that
wants a tokio-based client opts in by uncommenting the line `surreal module
init` leaves in `.cargo/config.toml`.

Reading a response body to EOF (an HTTP/1.0 or `Connection: close` reply) fails
with WASI `EIO` on the readiness-driven path. Responses carrying
`Content-Length` or chunked encoding are unaffected, which covers nearly every
real API. This is a wasi-libc/std issue, not a host one: a
`wasmtime_wasi=trace` dump shows every host call returning `Ok`.

## Common Commands

```bash
surreal extension build              # build from current dir
surreal extension build -o out.surli  # custom output
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
