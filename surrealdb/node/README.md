<br>

<p align="center">
    <img width=120 src="https://raw.githubusercontent.com/surrealdb/icons/main/surreal.svg" />
</p>

<h1 align="center">@surrealdb/node-native</h1><br/>
<p align="center">Native NAPI bindings to an embedded SurrealDB engine</p>

<br>

<p align="center">
    <img width=74 src="https://raw.githubusercontent.com/surrealdb/icons/main/nodejs.svg" />
</p>

<br>

<p align="center">
    <a href="https://github.com/surrealdb/surrealdb"><img src="https://img.shields.io/badge/status-stable-ff00bb.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://surrealdb.com/docs/sdk/javascript"><img src="https://img.shields.io/badge/docs-view-44cc11.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://www.npmjs.com/package/@surrealdb/node-native"><img src="https://img.shields.io/npm/v/@surrealdb/node-native?style=flat-square"></a>
</p>

<p align="center">
    <a href="https://surrealdb.com/discord"><img src="https://img.shields.io/discord/902568124350599239?label=discord&style=flat-square&color=5a66f6"></a>
    &nbsp;
    <a href="https://twitter.com/surrealdb"><img src="https://img.shields.io/badge/twitter-follow_us-1d9bf0.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://www.linkedin.com/company/surrealdb/"><img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg?style=flat-square"></a>
</p>

## You probably want `@surrealdb/node`

**This is not the package to install.** It is the native addon that
[`@surrealdb/node`](https://www.npmjs.com/package/@surrealdb/node) is built on, published separately
because the addon is compiled from the SurrealDB engine while the SDK engine that wraps it is
maintained with the JavaScript SDK.

To embed SurrealDB in Node.js, Bun, or Deno, install `@surrealdb/node` and follow the
[embedded engines](https://surrealdb.com/docs/languages/javascript/concepts/embedded-engines) guide.
It depends on this package for you.

## What this package is

A NAPI addon exposing one embedded SurrealDB instance over the RPC protocol — the same protocol the
server speaks over WebSocket. A caller encodes an RPC request as CBOR, hands it to `execute`, and
decodes the CBOR reply.

```ts
import { SurrealNodeEngine } from "@surrealdb/node-native";

const engine = await SurrealNodeEngine.connect("mem://");
const reply = await engine.execute(cborEncodedRequest);
await engine.free();
```

| Export | Description |
| --- | --- |
| `SurrealNodeEngine` | One embedded instance: `connect`, `execute`, `notifications`, `export`, `import`, `version`, `free` |
| `NotificationReceiver` | Live-query notifications, drained one at a time with `recv` |
| `ConnectionOptions` | Capabilities, timeouts, and the namespace/database created on new storage |

Storage backends are selected by the endpoint: `mem://`, `rocksdb://path` and `surrealkv://path`.
SurrealKV's MVCC versioning is a query parameter on that endpoint —
`surrealkv://path?versioned=true` — not a scheme of its own; the `surrealkv+versioned://` spelling
was removed engine-wide and now reports what to use instead.

`free()` closes the datastore, not just this handle: it resolves once the storage is closed, so a
file-backed path is unlocked and can be reopened as soon as the promise settles.

A reply is the method's value; only a failure is wrapped, in an `{ error }` envelope. Every instance
must be released with `free()`, after which each method reports a closed engine rather than
panicking — the addon is built with `panic = "abort"`, so a panic would end the host process.

### Streaming query frames

`query_stream` answers with a sequence of frames rather than one reply, each an object tagged by a
`stream` key: one `begin`, then `rows` / `value` and `finished` frames per statement, then exactly
one terminal `end`. They are the frames the server sends over WebSocket, so a client rebuilds a
result the same way on either transport.

The `begin` frame carries a `version`, which is the whole of the protocol's negotiation. While it
does not change: a `stream` tag is never repurposed, new tags and fields may be added — ignore the
ones you do not know rather than failing on them — and every `finished` frame carries `single`,
which is what tells a statement whose value is one bare value from one whose value is a list.

## Requirements

- ES modules (`import`) — CommonJS (`require`) is not supported
- Node.js, Bun, or Deno on a supported platform

## What gets installed

The native binary is around 45 MB and there is one per platform, so they are not shipped inside this
package. Each is published as `@surrealdb/node-native-<platform>` carrying `os`/`cpu`/`libc`, and
this package lists all of them under `optionalDependencies`: an install downloads only the one its
host can use, and the loader requires exactly that package. Nothing has to be configured — npm,
Bun, pnpm and Yarn all skip the platform packages they cannot install.

## Contributing

This package lives in the [SurrealDB](https://github.com/surrealdb/surrealdb) repository, under
`surrealdb/node`, and is released with the engine it embeds — every published version pairs with the
identically versioned SurrealDB release. See the
[contributing guide](https://github.com/surrealdb/surrealdb/blob/main/CONTRIBUTING.md) for
repository-wide conventions.

The database behaviour is shared with `@surrealdb/wasm` in the `surrealdb-embedded` crate; only the
FFI lives here.

Building requires a Rust toolchain and [Bun](https://bun.sh):

```sh
cd surrealdb/node
bun install
bun run build   # compiles the addon for the host platform into dist/
bun test        # exercises the built addon directly
```

The Rust half is a normal member of the Cargo workspace, so `cargo check -p surrealdb-node` and
`cargo clippy -p surrealdb-node` work from the repository root.
