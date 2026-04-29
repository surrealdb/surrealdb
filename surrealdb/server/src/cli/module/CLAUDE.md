# CLI Module Subcommands

Essential context for AI assistants working with the `surreal module` CLI.

## Overview

The `surreal module` command provides tooling for developing, building, and inspecting Surrealism WASM modules. All subcommands are defined in `mod.rs` via `clap::Subcommand`.

## Subcommands

### `init` (`init_cmd.rs`)

Scaffolds a new Surrealism module project.

- Creates `Cargo.toml`, `surrealism.toml`, and `src/lib.rs` with starter code.
- Supports interactive and headless (`--headless --org <org>`) modes.
- Pins the `surrealism` SDK dependency to `SURREALISM_VERSION` and `surrealdb` to `SURREALDB_VERSION`.
- Only Rust target is currently supported.

### `build` (`build.rs`)

Compiles a Rust project into a distributable `.surli` archive.

Pipeline:
1. Load and validate `surrealism.toml` config.
2. Read `cargo metadata` to find the WASM target artifact.
3. Run `cargo build --target wasm32-wasip2` (release or debug).
4. Validate the `surrealism` SDK version matches the server's expected version.
5. Run `wasm-opt` to optimize the binary (skip in debug mode).
6. Instantiate with a `DemoHost` to extract the exports manifest (function signatures).
7. Stamp `abi = 2` into the config.
8. Pack into a `.surli` archive via `SurrealismPackage`.

### `run` (`run.rs`)

Loads a `.surli` archive and invokes a function.

- Uses `DemoHost` (see below) as the `InvocationContext`.
- If `--fnc` is not specified, uses the first (or only) export.
- Prints the return value as SurrealQL.

### `info` (`info.rs`)

Prints module metadata and exports without executing.

- Displays package name, version, SDK version, ABI version.
- Lists all exported functions with their argument and return types.

### `sig` (`sig.rs`)

Prints a single function's signature (argument types and return type).

## `DemoHost` (`host.rs`)

A minimal `InvocationContext` implementation for CLI use. When a module calls `sql()` or `run()`, it prints the query/function to stdout and reads a result from stdin. This enables interactive testing of modules outside the database.

## Key Files

| File | Purpose |
|------|---------|
| `mod.rs` | `ModuleCommand` enum (clap), dispatcher, `parse_value` helper |
| `init_cmd.rs` | Project scaffolding |
| `build.rs` | Build pipeline: compile, optimize, extract exports, pack |
| `run.rs` | Load archive, invoke function, print result |
| `info.rs` | Display module metadata and exports |
| `sig.rs` | Display single function signature |
| `host.rs` | `DemoHost` — stdin/stdout `InvocationContext` for CLI testing |
