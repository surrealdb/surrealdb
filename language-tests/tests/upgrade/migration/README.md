# Migration tests

Tests with `migration = true` belong to the RocksDB-to-SurrealKV axis
(`upgrade --backend rocksdb-migration`): the released binary writes
`rocksdb://<dir>`, the current build opens the same directory as
`surrealkv://<dir>`, which migrates the data in place. No other axis runs them.

A test carrying `[env] versioned = true` runs only under `--versioned`, where
both stores are opened with `?versioned=true`. Every other test runs in both
modes. See `../README.md` for the axes and how to bootstrap expectations.
