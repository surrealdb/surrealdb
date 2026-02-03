# CLAUDE.md

This file provides essential context for AI assistants working with the SurrealDB codebase.

## Project Overview

SurrealDB is a multi-model database built in Rust supporting document, graph, relational, time-series, geospatial, and key-value data models. It can run embedded, in browser (WASM), at the edge, or as a distributed cluster.

## Project Structure

```
surrealdb/           # Main SDK crate
surrealdb/core/      # Core database engine (query execution, storage)
surrealdb/server/    # HTTP, WebSocket, gRPC server
surrealdb/types/     # Public types and derive macros
language-tests/      # SurrealQL test suite (.surql files)
tests/               # Integration tests (CLI, HTTP, WebSocket, GraphQL)
```

## Common Commands

```bash
# Build and run dev server
cargo run --no-default-features --features storage-mem,http,scripting -- start --log trace --user root --pass root memory

# Format code (REQUIRED before commits)
cargo make fmt

# Run clippy lints
cargo make ci-clippy

# Run all tests
cargo test

# Run language tests
cd language-tests && cargo run run

# Run specific language test
cd language-tests && cargo run run -- --test path/to/test.surql

# Auto-generate test results
cd language-tests && cargo run run -- --results accept path/to/test.surql
```

## Testing Conventions

### Language Tests (`language-tests/tests/*.surql`)

Test SurrealQL queries with expected results. Bug reproductions go in `language-tests/tests/reproductions/ISSUE_NUMBER_description.surql`.

**Test file format:**
```surql
/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Description of what this tests"
issue = 1234  # Optional GitHub issue

[[test.results]]
value = "expected_result"
*/

-- SurrealQL queries here
```

### SDK/Integration Tests

Located in `surrealdb/tests/` and `tests/`. Follow standard Rust testing conventions.

## Code Quality Rules

- Use `anyhow::Result` for fallible APIs, `thiserror` for domain errors
- Never use `.unwrap_or_default()` when debugging - it masks errors
- Propagate datastore errors via `crate::err::Error`
- Performance matters: think about impact of every change
- WASM compatibility: maintain `#[cfg_attr]` patterns (e.g., `async_trait(?Send)`)
- Don't add dependencies without confirmation
- Instrument public async functions with `#[instrument(...)]`
- Never log sensitive user data or credentials

## Bug Investigation Protocol

**Never assume bug reports are correct.** Always:

1. Check existing language tests in `language-tests/tests/` for related functionality
2. Verify expected behavior against SurrealQL docs (https://surrealdb.com/docs)
3. Create minimal reproduction test
4. Consider if this is user error, SDK issue, or actual bug
5. Create `language-tests/tests/reproductions/ISSUE_NUMBER_summary.surql` regardless of outcome

## Documentation References

- SurrealQL docs: https://surrealdb.com/docs
- SurrealDB University: https://surrealdb.com/learn
- Detailed cursor rules: `.cursor/rules/`
- Contributing guide: `CONTRIBUTING.md`
- Building instructions: `doc/BUILDING.md`
