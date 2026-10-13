# surrealdb-kvs-indxdb

The IndexedDB (browser/WASM) key-value store backend for SurrealDB.

> [!WARNING]
> This crate is SurrealDB internal API. It does not adhere to SemVer and its
> API is free to change and break code even between patch versions. If you are
> looking for a stable interface to the SurrealDB library please have a look at
> the [Rust SDK](https://crates.io/crates/surrealdb).

## Design

The backend is implemented directly on top of the browser's IndexedDB API
through a small JavaScript driver (`src/js/db.js`), bound with `wasm-bindgen`.

IndexedDB transactions go inactive whenever an `await` crosses a
non-IndexedDB microtask, so a native IndexedDB transaction cannot back a kvs
transaction. Instead each kvs transaction buffers its read-set and write-set
in memory and commits through a single JavaScript call that re-validates
every observed read (point reads, existence checks and scanned entries) and
applies the write-set inside one IndexedDB `readwrite` transaction. That
transaction only ever awaits IndexedDB requests, which keeps it active. This
amounts to optimistic concurrency control: conflicting transactions fail on
commit with a retryable `TransactionConflict` error.

### Isolation caveats

Commit-time validation covers only *individually observed* keys, and there is
no per-transaction snapshot, which is weaker than the snapshot isolation of
the native backends in two ways:

- **Phantoms.** A range scan records the entries it returned, not the absence
  of other keys in the range. A key concurrently inserted into an
  already-scanned range is not detected at commit.
- **Read-only transactions.** Reads run in short-lived IndexedDB transactions
  and read-only kvs transactions never commit, so nothing validates them: two
  scans in the same read-only transaction can observe different database
  states when a write commits in between.

Point reads and existence checks *are* repeatable within a transaction (they
are served from the read cache) and are re-validated at commit in write
transactions, so key-level invariants such as unique-index enforcement are
unaffected.

To keep the number of wasm ↔ js crossings low, bulk data moves in packed
`Uint8Array` buffers with `Uint32Array` offset tables (one crossing per scan
batch and one per commit) instead of per-entry marshalled objects.

The transaction type is `Send + Sync`: WASM without the `atomics` target
feature is single-threaded, making the manual `Send`/`Sync` declarations on
the JavaScript handles sound.

The database layout matches the layout created by the previously used
`indxdb` crate — a single object store named `"kv"` (database version 1) with
out-of-line binary keys and `Uint8Array` values — so existing databases open
without migration.

### Performance caveats.

In order to replicate some form of consistency this implementation needs to
keep every read key in memory in the transaction to ensure that the key does
not change while the transaction is active. This means that the memory use of 
transactions grow linearly with the amount of reads, and commits also require 
a consistency check with scales linearly with the amount of keys read.


## Testing

The test suite runs in a browser via `wasm-bindgen-test`:

```bash
cd surrealdb/kvs-indxdb
wasm-pack test --headless --chrome   # or --firefox
```
