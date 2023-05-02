# Benchmarks

This directory contains some micro-benchmarks that can help objectively
establish the performance implications of a change.

## Manual usage

Execute the following command at the top level of the repository:

```console
cargo bench --package surrealdb --no-default-features --features kv-mem
```