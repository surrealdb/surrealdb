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
| `SurrealWasmEngine` | One embedded instance: `connect`, `execute`, `notifications`, `export`, `import`, `version`, `free` |
| `ConnectionOptions` | Capabilities, timeouts, and the namespace/database created on new storage |

Storage backends are selected by the endpoint: `mem://` and `indxdb://name`, the latter backed by
the browser's IndexedDB.

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
- A browser, or any host with `WebAssembly`; `indxdb://` additionally needs IndexedDB

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
cargo clippy -p surrealdb-wasm --features kv-mem,kv-indxdb --target wasm32-unknown-unknown
```
