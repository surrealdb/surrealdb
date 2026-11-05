<br>

<p align="center">
    <img width=120 src="https://raw.githubusercontent.com/surrealdb/icons/main/surreal.svg" />
</p>

<h1 align="center">@surrealdb/wasm-native</h1><br/>
<p align="center">WebAssembly bindings to an embedded SurrealDB engine</p>

<br>

<p align="center">
    <img width=74 src="https://raw.githubusercontent.com/surrealdb/icons/main/webassembly.svg" />
</p>

<br>

<p align="center">
    <a href="https://github.com/surrealdb/surrealdb"><img src="https://img.shields.io/badge/status-stable-ff00bb.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://surrealdb.com/docs/sdk/javascript"><img src="https://img.shields.io/badge/docs-view-44cc11.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://www.npmjs.com/package/@surrealdb/wasm-native"><img src="https://img.shields.io/npm/v/@surrealdb/wasm-native?style=flat-square"></a>
</p>

<p align="center">
    <a href="https://surrealdb.com/discord"><img src="https://img.shields.io/discord/902568124350599239?label=discord&style=flat-square&color=5a66f6"></a>
    &nbsp;
    <a href="https://twitter.com/surrealdb"><img src="https://img.shields.io/badge/twitter-follow_us-1d9bf0.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://www.linkedin.com/company/surrealdb/"><img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg?style=flat-square"></a>
</p>

## You probably want `@surrealdb/wasm`

**This is not the package to install.** It is the WebAssembly module that
[`@surrealdb/wasm`](https://www.npmjs.com/package/@surrealdb/wasm) is built on, published
separately because the module is compiled from the SurrealDB engine while the SDK engine that wraps
it is maintained with the JavaScript SDK.

To embed SurrealDB in a browser, install `@surrealdb/wasm` and follow the
[embedded engines](https://surrealdb.com/docs/languages/javascript/concepts/embedded-engines) guide.
It depends on this package for you.

## What this package is

A WebAssembly module exposing one embedded SurrealDB instance over the RPC protocol — the same
protocol the server speaks over WebSocket. A caller encodes an RPC request as CBOR, hands it to
`execute`, and decodes the CBOR reply.

```ts
import init, { SurrealWasmEngine } from "@surrealdb/wasm-native";

await init();

const engine = await SurrealWasmEngine.connect("mem://");
const reply = await engine.execute(cborEncodedRequest);
engine.free();
```

| Export | Description |
| --- | --- |
| `init` | Instantiates the module; must resolve before anything else is called |
| `SurrealWasmEngine` | One embedded instance: `connect`, `execute`, `notifications`, `export`, `import`, `version`, `close`, `persisted`, `free` |
| `ConnectionOptions` | Capabilities, timeouts, and the namespace/database created on new storage |
| `./worker` | `connect(endpoint, options)`: starts a dedicated worker, hosts the engine in it and returns a `WorkerEngine` with the same methods, for `surrealkv://` |
| `./module` | The `.wasm` file itself, for a caller that resolves the module source itself |

Storage backends are selected by the endpoint: `mem://` and `surrealkv://name`, the latter backed by
SurrealKV over the browser's Origin Private File System (OPFS). The two are covered in
[Persistent storage in the browser](#persistent-storage-in-the-browser).

### Persistent storage in the browser

`surrealkv://name` keeps the database in the origin private file system, in a database called
`name` that belongs to the origin. `mem://` keeps nothing and works on any thread.

**The module must run in a dedicated worker.** The synchronous file handles that SurrealKV needs
exist only in dedicated workers, so a `surrealkv://` database cannot be opened from the main
thread of a page, from a shared worker or from a service worker. `SurrealWasmEngine.connect`
rejects such a call before it touches storage. The `./worker` export is the bridge: `connect` starts a dedicated worker, loads this module in it
(`dist/worker.js` beside the loader), connects the engine there and returns a `WorkerEngine` whose
`execute`, `queryStream`, `notifications`, `export`, `import`, `version`, `persisted` and `close`
mirror the engine's. Byte payloads are copied or transferred and `ReadableStream`s are transferred,
so a reader that stops reading still stops the engine's producer.

```ts
import { connect } from "@surrealdb/wasm-native/worker";

const engine = await connect("surrealkv://my-database");
// ... engine.execute(request) ...
await engine.close(); // before the application ends
```

A caller that hosts the module in its own worker uses `SurrealWasmEngine` directly: it calls `init`
and `connect` there and carries requests, replies and notifications by message itself. The module needs no threads, no async runtime and no `SharedArrayBuffer`.

**One tab per database.** SurrealKV takes an exclusive lock on the database. A second tab (or
worker) that connects to the same name waits a few seconds for a holder that is closing and then
`connect` rejects with a message that says the database is open in another tab or worker. The engine does not retry or take the lock over: the
page decides, for example by telling the user to close the other tab, or by connecting again later.
After a reload without a `close()` the browser can take longer than the wait to release the lock,
so the same rejection can appear for a short time.

**Close the engine when the application ends.** A worker cannot wait for an asynchronous close
inside a page-unload handler. Call `await engine.close()` as early as the application knows it is
ending, for example when the page sends a message to the worker. On a `WorkerEngine`, `close()`
closes the datastore, frees the engine and ends the worker. On a `SurrealWasmEngine`, call
`await engine.close()` and then `engine.free()`. `close()` closes the SurrealKV tree and then
releases its files and the lock; it takes longer the more files the database has. A
`SurrealWasmEngine` that is freed without `close()` closes its datastore on a task of its own, so the
lock is released shortly after `free()` returns rather than before. A database that was not closed is recovered the next time it is
opened. Nothing may use the engine after `close()` resolves, other than `free()`.

**Ask for persistent storage from the page.** Unless the origin has been granted persistent
storage, the browser may evict its data when the disk fills. Only a page can ask: call
`navigator.storage.persist()` there. Inside the worker, `await SurrealWasmEngine.persisted()`
reports whether the request was granted; it resolves to `false` until then.

**Data from the browser's earlier storage moves over by itself.** At every `surrealkv://name` connect,
before the database is opened, the module copies the rows of an earlier browser database called
`name` into the origin private file system, unless a database `name` exists there already. The
earlier database is only read: it is never changed or deleted, and the application removes it with
`indexedDB.deleteDatabase(name)` once it has checked the copy. The copy is verified, and one that
was interrupted is made again at the next connect, which then opens no database until the copy
completes. A copy that fails makes `connect` reject, and the database is not opened.

Limits: Chrome is the only browser that is tested. Encryption, checkpoints and the migrators of
earlier engines are not available in the browser. `Durability` in a browser is an ordering guarantee of
the file handle's `flush()` and not a promise about a power cut or an operating-system crash. A
worker has an address space of at most 4 GiB, and the default memtable and block cache sizes
assume the 1 GiB that is assumed when the memory of the host cannot be read.

The `browser/` directory holds two pages that exercise this in a browser: `smoke.html` drives the
`./worker` bridge (persistence across close and reopen, the second-tab rejection) and `drop.html`
frees an engine without closing it and reconnects. Build, serve this directory with any static
server, and open them in Chrome; each leaves its outcome in `window.__results` or `window.__drop`.

### Where `init` reads the module from

`init()` with no argument resolves `index_bg.wasm` against the loader's own URL, which is right
only while the loader and the module sit beside each other. A bundler that copies the loader
elsewhere — Vite's dependency pre-bundling moves it into `node_modules/.vite/deps/` — leaves the
fetch pointing at a path the dev server answers with its HTML fallback, and instantiation fails on
the bytes not being a module (`expected magic word 00 61 73 6d, found 3c 21 64 6f`, which is
`<!do`).

`init` therefore accepts an explicit module source, and this package exports the `.wasm` under
`./module` so a caller can own the resolution rather than inherit the loader's location:

```ts
import init from "@surrealdb/wasm-native";
import wasmUrl from "@surrealdb/wasm-native/module?url";

await init({ module_or_path: wasmUrl });
```

Both are part of this package's surface and will keep working: the argument form is wasm-bindgen's
own `--target web` entry point, and `./module` is a subpath export declared here. Leaving the loader
where it was put works too, with Vite:

```ts
// vite.config.ts
optimizeDeps: { exclude: ["@surrealdb/wasm", "@surrealdb/wasm-native"] }
```

A reply is the method's value; only a failure is wrapped, in an `{ error }` envelope.
`notifications()` returns a `ReadableStream` of encoded live-query notifications, which ends when
the engine is freed. Every instance must be released with `free()`.

**Nothing may touch the handle after `free()`.** wasm-bindgen's generated wrapper fails on the
freed pointer from inside its async trampoline, so an `await engine.execute(…)` that follows a
`free()` never settles — it hangs rather than rejecting, and there is no error to catch. A caller
has to gate its own methods on whether it has freed the engine, which is what `@surrealdb/wasm`
does. Dropping the engine also leaves the datastore's maintenance timers armed until each next
tick, which a browser does not notice but which keeps a Node or Bun process from exiting.

## Requirements

- ES modules (`import`) — CommonJS (`require`) is not supported
- A browser, or any host with `WebAssembly`; `surrealkv://` additionally needs a dedicated worker with the origin private file system

## Contributing

This package lives in the [SurrealDB](https://github.com/surrealdb/surrealdb) repository, under
`surrealdb/wasm`, and is released with the engine it embeds — every published version pairs with the
identically versioned SurrealDB release. See the
[contributing guide](https://github.com/surrealdb/surrealdb/blob/main/CONTRIBUTING.md) for
repository-wide conventions.

The database behaviour is shared with `@surrealdb/node-native` in the `surrealdb-embedded` crate;
only the FFI lives here.

Building requires a Rust toolchain, [Bun](https://bun.sh), and two Cargo-installed tools:

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-opt --version 0.116.1
```

SurrealKV links zstd, which is C code, so the build needs a C compiler and an archiver that target
WebAssembly. On macOS the compiler and archiver that ship with the system do not, so use LLVM's:

```sh
brew install llvm
export CC_wasm32_unknown_unknown="$(brew --prefix llvm)/bin/clang"
export AR_wasm32_unknown_unknown="$(brew --prefix llvm)/bin/llvm-ar"
```

On Linux, `clang` and `llvm-ar` serve the same purpose.

`wasm-bindgen-cli` has to match the `wasm-bindgen` version in the workspace lockfile exactly —
the two share a schema that changes between patch releases. `bun run build` checks this before
it does anything and prints the `cargo install` line to run, so there is no version to copy from
here and get wrong.

```sh
cd surrealdb/wasm
bun install
bun run build            # compiles the module into dist/
bun run build -- --debug # unstripped, with panics reported to the console
bun test                 # exercises the built module directly
```

The Rust half is a normal member of the Cargo workspace, but it compiles to nothing off wasm — use
the target explicitly:

```sh
cargo clippy -p surrealdb-wasm --features kv-mem,kv-surrealkv --target wasm32-unknown-unknown
```
