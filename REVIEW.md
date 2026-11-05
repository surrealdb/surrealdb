# Code Review Guidelines

## Always check

- **Performance**: flag allocations in hot paths, unnecessary clones, recursion without stack guards, and blocking work on async tasks. This is a database, performance and memory usage regressions matter.
- **Security**: credentials and tokens must not appear in logs or error messages. New endpoints and operations must enforce authentication and authorization checks. User input must be validated before reaching the storage layer. See [SECURITY_GUIDE.md](SECURITY_GUIDE.md) for detailed invariants and review triggers per subsystem.
- **Error handling**: errors must not be silently swallowed. No `.unwrap_or_default()` in non-test code, no `let _ =` on fallible calls without justification. Datastore errors should propagate through `crate::err::Error` before wrapping in `anyhow`.
- **Concurrency**: background tasks must have cancellation paths tied to their owners. Lock ordering must be documented when multiple locks are held. No `.await` while holding synchronous locks.
- **Revisioned structs**: changes to types that derive `Revision` must be backwards compatible.
- **Test coverage**: bug fixes should include a language test (`language-tests/tests/`) or SDK test (`surrealdb/tests/`, `tests/`). New SurrealQL functionality needs corresponding `.surql` test files. Functionality that can be tested with language tests should be preferred over writing custom rust language tests.
- **Dependency changes**: new external crates require justification. Prefer workspace-managed dependencies in the root `Cargo.toml`.
- **New releases**: a change that tags or ships a version must add it to all three of `LANG_UPGRADE_VERSIONS`, `LANG_UPGRADE_SEEDS` and `LANG_UPGRADE_CHAIN` in [Makefile.ci.toml](Makefile.ci.toml) — in the first and third, before the trailing `..`; the seeds list has no terminator. `LANG_UPGRADE_CHAIN` omits any release that demands a manual repair, so check its comment before adding a prerelease. Omitting it silently widens the last upgrade-test hop into a multi-release jump instead of failing. See [language-tests/CLAUDE.md](language-tests/CLAUDE.md) > Upgrade Tests.
- **Upgrade-test repairs**: a change that makes an upgrade require a manual step (a `REBUILD INDEX`, a migration an operator has to run) must declare it as `[[test.repair]]` on the affected upgrade test, never write the statement into the test body. A repair in the body reads back bytes the new binary just wrote, so it hides exactly what the test is there to check.

## Skip

- Formatting and style (enforced by `cargo make fmt` and nightly rustfmt in CI)
- Clippy warnings (enforced by `cargo make ci-clippy` and `ci-clippy-release` in CI)
- `std::time::Instant` / `std::time::SystemTime` imports (enforced by `cargo make ci-check-imports` in CI)
- Unused dependencies (enforced by CI check)
- Files under `target/`, `fuzz/`, and generated code
- `revision.lock` file contents (validated by the `revision-lock` tool in CI)
