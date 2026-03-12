# Code Review Guidelines

## Always check

- **Performance**: flag allocations in hot paths, unnecessary clones, recursion without stack guards, and blocking work on async tasks. This is a database, performance and memory usage regressions matter.
- **Security**: credentials and tokens must not appear in logs or error messages. New endpoints and operations must enforce authentication and authorization checks. User input must be validated before reaching the storage layer.
- **Error handling**: errors must not be silently swallowed. No `.unwrap_or_default()` in non-test code, no `let _ =` on fallible calls without justification. Datastore errors should propagate through `crate::err::Error` before wrapping in `anyhow`.
- **Concurrency**: background tasks must have cancellation paths tied to their owners. Lock ordering must be documented when multiple locks are held. No `.await` while holding synchronous locks.
- **Revisioned structs**: changes to types that derive `Revision` must be backwards compatible.
- **Test coverage**: bug fixes should include a language test (`language-tests/tests/`) or SDK test (`surrealdb/tests/`, `tests/`). New SurrealQL functionality needs corresponding `.surql` test files. Functionality that can be tested with language tests should be preferred over writing custom rust language tests.
- **Dependency changes**: new external crates require justification. Prefer workspace-managed dependencies in the root `Cargo.toml`.

## Skip

- Formatting and style (enforced by `cargo make fmt` and nightly rustfmt in CI)
- Clippy warnings (enforced by `cargo make ci-clippy` and `ci-clippy-release` in CI)
- `std::time::Instant` / `std::time::SystemTime` imports (enforced by `cargo make ci-check-imports` in CI)
- Unused dependencies (enforced by CI check)
- Files under `target/`, `fuzz/`, and generated code
- `revision.lock` file contents (validated by the `revision-lock` tool in CI)
