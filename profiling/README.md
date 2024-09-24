# Profiling with DTRACE (MacOS / FreeBSD)

## Prerequisites:

- [DTrace](https://dtrace.org)
- [rustfilt](https://crates.io/crates/rustfilt)

### 1. Build Surreal

Build Surreal with the bench profile (preserving symbols).

```
cargo build --profile bench
```

### 2. Start profiling

Start SurrealDB with profiling info.

```
sudo profiling/profile.d -c "./target/release/surreal" | rustfilt
```

### 3. Stop profiling

Type Ctrl/D to stop SurrealDB and see the result of the profiling.

### 4. Check the results

Each line is a function, with the following columns:

1. Name of the function
2. Average CPU times:
3. Total CPU times (sum of )
