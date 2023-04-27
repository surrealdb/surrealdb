# Benchmarks

This directory contains some micro-benchmarks that can help objectively
establish the performance implications of a change.

## Building

The benchmarks use the unstable `test` feature and therefore require
a `nightly` Rust compiler.

```console
cargo +nightly bench --package surrealdb --jobs 1 --features kv-mem
```