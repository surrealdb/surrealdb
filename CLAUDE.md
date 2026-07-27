# CLAUDE.md

This file provides essential context for AI assistants working with the SurrealDB codebase.

## Project Overview

SurrealDB is a multi-model database built in Rust supporting document, graph, relational, time-series, geospatial, and key-value data models. It can run embedded, in browser (WASM), at the edge, or as a distributed cluster.

## Project Structure

```
surrealdb/           # Main SDK crate
surrealdb/core/      # Core database engine (query execution, storage)
surrealdb/mcp/       # Model Context Protocol server (stdio + HTTP)
surrealdb/server/    # HTTP, WebSocket, gRPC server
surrealdb/types/     # Public types and derive macros
surrealism/          # Surrealism (WASM plugin system) crates
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
# Note: all paths are relative to the language-tests/tests directory
cd language-tests && cargo run run path/to/test.surql

# Auto-generate test results
# Note: The test results must be empty for the auto-generation to work.
cd language-tests && cargo run run --results accept path/to/test.surql

# Benchmark a language-test bench (measures by default; --profile records a flamegraph)
# Pass the bench filter and flags after `--`. See language-tests/README.md > Benchmarking.
cargo make bench -- scans/where_integer_in_many_full --save
cargo make bench -- scans/where_integer_in_many_full --profile --dataset indexed
```

## Testing Conventions

### Language Tests (`language-tests/tests/*.surql`, `*.gql`)

Test SurrealQL queries with expected results (`.gql` files test the GQL dialect). Bug reproductions go in `language-tests/tests/reproductions/ISSUE_NUMBER_description.surql`.

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

### Parser Tests (`surrealdb/parser/src/test/files/*.surql`)

The parser crate has its own file-based test suite. When adding a parser test, add a `.surql`
file under `surrealdb/parser/src/test/files/` (or `files_quirk/` for quirk-mode parsing) instead
of writing a test function in `src/test/mod.rs`. These tests verify that the source parses and
that the generated AST matches the expected block embedded in the file after
`/* ===== result =====`. Sources that fail to parse are also supported: the expected block then
contains the rendered parsing error, so error messages are pinned too.

Generate or update expected output with the `RESULT` environment variable:

```bash
# Fill in expectations for files that don't have one yet
RESULT=ACCEPT cargo test -p surrealdb-parser text_test

# Rewrite expectations that have changed (review the diff before committing)
RESULT=OVERWRITE cargo test -p surrealdb-parser text_test
```

Only add a function test in `src/test/mod.rs` for uncommon parser functionality that a plain
source-file test can't express.

### SDK/Integration Tests

Located in `surrealdb/tests/` and `tests/`. Follow standard Rust testing conventions.

## Code Quality Rules

- Use `anyhow::Result` for fallible APIs, `thiserror` for domain errors
- Never use `.unwrap_or_default()` when debugging - it masks errors
- Propagate datastore errors via `crate::err::Error`
- Performance matters: think about impact of every change
- Use `web_time::Instant` and `web_time::SystemTime` instead of `std::time` equivalents (WASM-safe, enforced by clippy)
- Don't add dependencies without confirmation
- Instrument public async functions with `#[instrument(...)]`
- Never log sensitive user data or credentials

## Comment discipline — describe the contract, not the narrative

Applies to in-source comments and doc comments, and to standalone docs in this repo.

**Describe what the code currently does and why it must be that way** — the invariants it upholds, the ordering it relies on, and the inputs, outputs, and error modes callers must respect. The reader is someone understanding the code as it is now, not reconstructing how it got here.

**Never** bake transient development context into long-lived comments or docs:

- No change narration — "previously did X, now does Y", "the old behaviour was…", "this used to…", "no longer gated".
- No mention of a bug or behaviour that has since been fixed. Once a fix lands, drop the mention and describe the correct behaviour.
- No references to a specific PR, review comment, branch, ticket, or commit — the git log already records that; readers don't have it loaded.
- No one-off empirical numbers from a single run (e.g. "37/42 entries were stale") — those belong in the commit message that introduced the change.
- No in-flight refactoring scaffolding ("for now…", "until the X migration lands…"). If it's the current behaviour, document that; if it's a genuine temporary, leave a TODO with a tracking link and document the contract the temporary upholds.

The right home for change/PR/incident narrative is the **commit message** or **PR description**. A comment should read the same six months and ten unrelated PRs later as it does today. When you touch a comment that already drifts into narrative, rewrite it into contract form rather than appending another layer.

## Bug Investigation Protocol

**Never assume bug reports are correct.** Always:

1. Check existing language tests in `language-tests/tests/` for related functionality
2. Verify expected behavior against SurrealQL docs (https://surrealdb.com/docs)
3. Create minimal reproduction test
4. Consider if this is user error, SDK issue, or actual bug
5. Create `language-tests/tests/reproductions/ISSUE_NUMBER_summary.surql` regardless of outcome

## Documentation References

Before changing code that touches authentication, sessions, permissions, RPC/HTTP
transport, the SurrealQL parser, function execution, storage keys, or import/export,
consult `SECURITY_GUIDE.md` and confirm the relevant invariants still hold.

- Security model and review invariants: `SECURITY_GUIDE.md`
- General code review checklist: `REVIEW.md`
- SurrealQL docs: https://surrealdb.com/docs
- SurrealDB University: https://surrealdb.com/learn
- Detailed cursor rules: `.cursor/rules/`
- Contributing guide: `CONTRIBUTING.md`
- Building instructions: `doc/BUILDING.md`
