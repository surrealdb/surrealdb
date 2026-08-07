<br>

<p align="center">
    <img width=120 src="https://raw.githubusercontent.com/surrealdb/icons/main/surreal.svg" />
</p>

<h1 align="center">@surrealdb/node</h1><br/>
<p align="center">Embedded SurrealDB engine for Node.js, Bun, and Deno</p>

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
    <a href="https://www.npmjs.com/package/@surrealdb/node"><img src="https://img.shields.io/npm/v/@surrealdb/node?style=flat-square"></a>
</p>

<p align="center">
    <a href="https://surrealdb.com/discord"><img src="https://img.shields.io/discord/902568124350599239?label=discord&style=flat-square&color=5a66f6"></a>
    &nbsp;
    <a href="https://twitter.com/surrealdb"><img src="https://img.shields.io/badge/twitter-follow_us-1d9bf0.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://www.linkedin.com/company/surrealdb/"><img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg?style=flat-square"></a>
</p>

## Documentation

View the JavaScript SDK documentation [here](https://surrealdb.com/docs/sdk/javascript), including the [embedded engines](https://surrealdb.com/docs/languages/javascript/concepts/embedded-engines) concept page and the [Node.js engine reference](https://surrealdb.com/docs/languages/javascript/engines/node).

## What is this package?

**`@surrealdb/node`** is a native engine plugin for the SurrealDB JavaScript SDK. It embeds SurrealDB directly in your server-side JavaScript runtime using a NAPI addon, so you can run databases in-process without a separate server.

Despite the name, this package works in **Node.js**, **Bun**, and **Deno**. It is not used in browsers - use [`@surrealdb/wasm`](https://www.npmjs.com/package/@surrealdb/wasm) for that.

Install it alongside [`surrealdb`](https://www.npmjs.com/package/surrealdb) and register the engine when constructing your client.

## How to install

This package has a peer dependency on `surrealdb`. Install both:

```sh
# using npm
npm i surrealdb @surrealdb/node

# or using pnpm
pnpm i surrealdb @surrealdb/node

# or using yarn
yarn add surrealdb @surrealdb/node

# or using bun
bun add surrealdb @surrealdb/node
```

## Getting started

Register the Node.js engine when you create a `Surreal` client, then connect to an embedded endpoint:

```ts
import { createNodeEngines } from "@surrealdb/node";
import { Surreal } from "surrealdb";

const db = new Surreal({
    engines: createNodeEngines(),
});

await db.connect("mem://");

await db.use({ namespace: "test", database: "test" });

await db.query("CREATE person SET name = 'Tobie'");
```

To connect to remote SurrealDB instances as well as embedded databases, combine the Node.js engine with the SDK's remote engines:

```ts
import { createNodeEngines } from "@surrealdb/node";
import { Surreal, createRemoteEngines } from "surrealdb";

const db = new Surreal({
    engines: {
        ...createRemoteEngines(),
        ...createNodeEngines(),
    },
});
```

### Storage backends

The engine supports in-memory and on-disk storage:

```ts
// In-memory (data is lost when the process exits)
await db.connect("mem://");

// RocksDB persistence
await db.connect("rocksdb://path/to/storage.db");

// SurrealKV persistence
await db.connect("surrealkv://path/to/storage.db");

// SurrealKV with versioned storage for temporal queries
await db.connect("surrealkv+versioned://path/to/storage.db");
```

### Connection options

Pass optional engine configuration to `createNodeEngines`:

```ts
const db = new Surreal({
    engines: createNodeEngines({
        strict: true,
        query_timeout: 30_000,
        capabilities: {
            scripting: true,
        },
    }),
});
```

### Closing the connection

When using the embedded engine, call `.close()` when you are done to shut down the database cleanly:

```ts
await db.close();
```

## Package contents

| Export | Description |
| --- | --- |
| `createNodeEngines(options?)` | Registers `mem`, `rocksdb`, `surrealkv`, and `surrealkv+versioned` engine factories |
| `NodeEngine` | The underlying engine implementation (advanced use) |

## Requirements

- ES modules (`import`) - CommonJS (`require`) is not supported
- A compatible `surrealdb` SDK version (see `peerDependencies` in `package.json`)
- Node.js, Bun, or Deno on a supported platform (native binaries are published per OS/architecture)

## Contributing

This package lives in the [SurrealDB](https://github.com/surrealdb/surrealdb) repository, under `surrealdb/node`, and is released with the engine it embeds — every published version pairs with the identically versioned SurrealDB release. See the [contributing guide](https://github.com/surrealdb/surrealdb/blob/main/CONTRIBUTING.md) for repository-wide conventions.

Building the package requires a Rust toolchain and [Bun](https://bun.sh):

```sh
cd surrealdb/node
bun install
bun run build   # compiles the addon for the host platform into dist/
bun test        # exercises the built package through the JavaScript SDK
```

The Rust half is a normal member of the Cargo workspace, so `cargo check -p surrealdb-node` and `cargo clippy -p surrealdb-node` work from the repository root.
