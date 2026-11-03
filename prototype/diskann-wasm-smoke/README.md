# DiskANN on 32-bit wasm: smoke tests

Each test runs the engine in memory, builds a DiskANN index over 2000 8-dim
vectors, and compares KNN answers against the brute-force `<|k,EUCLIDEAN|>`
from the pending set, from the graph after compaction, and after deleting the
nearest records.

- `web`: `wasm32-unknown-unknown`, the edge (Cloudflare) target. A
  `wasm-bindgen` export run from node, with no tokio runtime.
- `guest` + `host`: `wasm32-wasip2`, run under wasmtime. Not in CI.

Both take `diskann` from the `surrealdb-diskann` crate pinned in the workspace
`Cargo.toml`, and need the wasm arm of the `diskann` cfg in the `idx`, `core`
and `datastore` build scripts.

## wasm32-unknown-unknown (node)

CI runs this test as `Test Wasm (DiskANN in node)` in `.github/workflows/ci.yml`.
`web` is its own workspace with a committed `Cargo.lock`, which
`check-lockfiles.sh` checks against its manifests. It needs
`wasm-bindgen-cli` at the `wasm-bindgen` version in that lockfile.

```bash
cd prototype/diskann-wasm-smoke/web && RUSTFLAGS='-D warnings --cfg getrandom_backend="wasm_js" -C link-arg=-zstack-size=4194304' cargo build --locked --target wasm32-unknown-unknown
```

```bash
cd prototype/diskann-wasm-smoke/web && wasm-bindgen --target nodejs --out-dir pkg target/wasm32-unknown-unknown/debug/diskann_web_guest.wasm && node --stack-size=8000 run.cjs
```

`run.cjs` exits non-zero when the test fails, and also when node's event loop
empties before the test settles, which is how a deadlock on the JavaScript
executor shows.

## wasm32-wasip2 (wasmtime)

```bash
cp Cargo.lock prototype/diskann-wasm-smoke/guest/
cd prototype/diskann-wasm-smoke/guest && CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_DEV_OPT_LEVEL=1 RUSTFLAGS='-C link-arg=-zstack-size=8388608' cargo +1.95 build --offline --target wasm32-wasip2
```

```bash
cp Cargo.lock prototype/diskann-wasm-smoke/host/
cd prototype/diskann-wasm-smoke/host && cargo +1.95 build --offline --release
```

```bash
prototype/diskann-wasm-smoke/host/target/release/diskann-wasi-host prototype/diskann-wasm-smoke/guest/target/wasm32-wasip2/debug/diskann-wasi-guest.wasm
```

A debug-info build of the WASI guest is over 1 GiB, past wasmtime's module
size limit, hence `debug = 0`.
