# surrealdb-kvs-test

The shared behaviour test suite for SurrealDB key-value store backends.
Any backend implementing the `surrealdb-kvs` `TransactionBuilder` /
`Transactable` traits — first-party or external — runs the same contract
tests through this crate.

## Running the suite against a backend

Add a `harness = false` test target whose `main` registers one
`TestBackend` per backend and calls `run`:

```rust
fn main() -> std::process::ExitCode {
    let backends = vec![
        TestBackend::new("mybackend", || async {
            TestDs::from_builder(my_transaction_builder().await)
        }),
    ];
    surrealdb_kvs_test::run(backends)
}
```

The harness speaks the libtest CLI, so filtering, `--list`, `--exact`, and
per-test reporting work under both `cargo test` and `cargo nextest`. Tests
are named `{backend}::{module}::{test}`.

The first-party backends run this suite from `surrealdb-kvs-any`'s `kvs`
test target.

## Running on wasm

The libtest harness needs threads and a blockable executor, neither of
which exist in a browser — and backends like IndexedDB only make progress
when control returns to the JavaScript event loop. Wasm consumers instead
await `run_all` from a single `#[wasm_bindgen_test]`:

```rust
#[wasm_bindgen_test]
async fn kvs_suite() {
    let backends = [TestBackend::new("indxdb", || async { /* ... */ })];
    let ran = surrealdb_kvs_test::run_all(&backends, |line| console_log!("{line}")).await;
    assert!(ran > 0);
}
```

Per-test progress goes through the logging callback; a failing test panics
and ends the run (wasm has no unwinding), identified by the `running` line
logged immediately before it. The first-party IndexedDB backend runs the
suite this way from `surrealdb-kvs-indxdb`'s `suite` test target.

## Writing tests

A test is a private `async fn(&TestBackend)` registered with `kvs_test!`.
Tests run against **every** backend by default; a test that exercises
backend-specific behaviour opts in or out by backend name. Names are an
open vocabulary — unknown names simply never match, so tests may reference
backends that live in other repositories (e.g. `surrealds`):

```rust
async fn same_key_conflict(b: &TestBackend) { /* ... */ }
kvs_test!(same_key_conflict, except = [tikv]);
```

Combinations that don't apply are reported as *ignored*, so every backend
shows the same test count.
