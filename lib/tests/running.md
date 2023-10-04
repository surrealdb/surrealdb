These tests are run with the following command

Individual test files (not api module):
```bash
cargo test -p surrealdb --features kv-mem --test bootstrap -- --nocapture
```

Api module:
```bash
TODO
cargo test -p surrealdb --features kv-rocksdb --test api api_integration::file::delete_record_range
```

