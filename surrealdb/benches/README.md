# Benchmarks

This directory contains some micro-benchmarks that can help objectively
establish the performance implications of a change, and also benchmarks that
test the performance of different datastores using both the library and the SDK

## Manual usage

### Common

Execute the following command at the top level of the repository:

```console
$ cargo make bench
```

### Specific datastore
Execute the following commands at the top level of the repository:

* Memory datastore using the lib or the SDK
```console
$ cargo make bench-lib-mem
$ cargo make bench-sdk-mem
```

* RocksDB datastore using the lib or the SDK
```console
$ cargo make bench-lib-rocksdb
$ cargo make bench-sdk-rocksdb
```

* WebSocket remote server using the SDK
    * Start SurrealDB server
    ```
    $ cargo make build
    $ ./target/release/surreal start
    ```
    * Run the benchmarks
    ```console
    $ cargo make bench-sdk-ws
    ```


## Profiling

For CPU profiling and flamegraphs, use the language-tests benchmark harness,
which records profiles with `samply` (see `language-tests/README.md` >
Benchmarking):

```console
cargo make bench -- <bench-filter> --profile
```
