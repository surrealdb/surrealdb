These tests are compiled into a single `it` integration-test binary (the
individual test files live in `tests/it/` as modules). This keeps the crate
from re-linking `surrealdb-core` once per test file.

Run the whole suite:
```bash
cargo test -p surrealdb_core --features kv-mem --test it -- --nocapture
```

Run a single test module (e.g. `define`) by filtering on its module path:
```bash
cargo test -p surrealdb_core --features kv-mem --test it -- define:: --nocapture
```
