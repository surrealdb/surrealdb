# Surrealism WASM Plugin System

Essential context for AI assistants working with the Surrealism codebase.

## Architecture Overview

Surrealism is SurrealDB's WASM plugin system. Guest modules are compiled to WASM components (WASI Preview 2) and run inside the host SurrealDB process. Communication uses WIT interfaces and FlatBuffers serialization.

## Crate Relationships

| Crate | Role |
|-------|------|
| **surrealism** | Guest SDK. Used by plugin authors. Provides `#[surrealism]` attr, bindings to host imports (sql, run, kv-*). |
| **surrealism-runtime** | Host. Compiles, instantiates, and executes guest modules. Integrates with SurrealDB. |
| **surrealism-types** | Shared types and encoding. Used by both host and guest. |
| **surrealism-macros** | Proc macros. Expands `#[surrealism]` into WIT exports and inventory registration. |

## Build / Test / Deploy Workflow

- **Build:** `surreal module build` — compiles the Rust project to `wasm32-wasip2`, optimizes with wasm-opt, packs into a `.surli` archive.
- **Run:** `surreal module run <file.surli> [--fn <name>] [args...]` — loads the archive, invokes the specified function (or default).
- **Info:** `surreal module info <file.surli>` — inspects exports without executing.

Archives are self-contained; load them via `SurrealismPackage::from_file(path)`.

## Security Model

- **Capabilities-based:** Modules declare needs in `surrealism.toml` (`[capabilities]`). Server validates at load time and can further restrict.
- **Runtime scoping:** When a module calls `sql()` or `run()`, the host creates a derived context whose capabilities are narrowed to the module's declared `allow_scripting`, `allow_functions`, and `allow_net`. Server deny-lists are always preserved.
- **Deny-by-default for functions:** `allow_functions` defaults to empty (deny all). Use `["*"]` to allow all, or specific patterns like `["http::*", "fn::user_exists"]`.
- **Module isolation:** Each module gets its own KV store; no cross-module access. WASM linear memory is per-instance.
- **`strict_timeout = false`:** Disables epoch-based timeout enforcement entirely. The module can run indefinitely and monopolise a thread. Only trusted modules should set this. Future code-signing support will gate which modules may request it; until then the server operator accepts full responsibility.

## Archive Format

`.surli` = zstd-compressed tar archive containing:

- `surrealism/mod.wasm` — WASM component binary
- `surrealism/surrealism.toml` — config (package meta, capabilities, ABI)
- `surrealism/exports.toml` — function signatures (args/returns) extracted at build
- `surrealism/fs/*` — optional read-only filesystem attachment

## ABI Versioning

ABI version 2 is current. Stored in `surrealism.toml`. Build tool stamps `abi = 2`; user values are ignored. Older `p1`/`1` for Preview 1 is legacy.

## Common Commands

```bash
surreal module build              # build from current dir
surreal module build -o out.surli  # custom output
surreal module run pkg.surli      # run default export
surreal module run pkg.surli --fn "mymod::greet" "hello"  # named export + args
surreal module info pkg.surli     # list exports
```

## Project Structure

- `surrealism/` — guest SDK
- `surrealism/runtime/` — host runtime
- `surrealism/macros/` — proc macros
- `surrealism/types/` — shared types
- `surrealism/wit/` — WIT interfaces
- `surrealism/demo/` — example plugin
