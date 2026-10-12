# SurrealDB review guide

SurrealDB is a multi-model database written in Rust. Its users trust it to
return the right rows, keep committed data, and enforce permissions. A bug that
does any of those wrongly can go unnoticed for a long time. Your main job is to
find those bugs.

## Priorities

Spend your effort in this order:

1. **Correctness.** Wrong results, lost or corrupted data, a broken invariant, a
   panic that input can reach, or an error that is reported wrongly.
2. **Security.** See the section below.
3. **Concurrency and durability.** Races, cancellation, crash consistency, and
   whether data written by older releases can still be read.
4. **Performance** on hot paths.
5. **Code hygiene.** Readability, naming, duplication, comments.

Look hardest for bugs that pass review and CI: problems that only appear for a
particular input, interleaving, failure point or upgrade path. A finding the
author would not have spotted alone is worth far more than one they would. Do
not let hygiene comments take the place of correctness analysis. If you found
no correctness problems, say so in the review summary, and say what you
checked.

## How to review

Read the whole changed function, not only the changed lines. Follow the
changed code to its callers and the code it calls, as far as the PR and repo
let you. For each change, ask:

- **Inputs.** What happens with an empty input, one element, the maximum
  value, `NONE` versus `NULL`, a missing field, a nested or recursive value,
  non-ASCII text, a negative or zero count, or the boundary of a range?
- **All paths.** Does every path through the code keep the invariant? Check
  early returns, `?`, fast paths, cache hits, and `match` arms added for new
  variants. A fast path must do everything the slow path it replaces does,
  including permission checks, field processing and events.
- **Failure halfway.** If this fails, is cancelled or times out partway
  through, what state is left behind? Is any side effect performed before the
  transaction commits, or repeated when the operation is retried?
- **Concurrency.** What if another transaction, live query, background task or
  node acts on the same key between two steps here? Is a value read in one
  transaction and relied on in another?
- **Agreement.** When two pieces of code must agree (encode and decode, a
  check and the action it guards, the index path and the table-scan path,
  `can_coerce` and `coerce`, plan time and execution time), does the change
  update both?
- **Ordering.** Do sort orders, key orders, and the order of side effects stay
  the same as before? Key encodings must sort in the same order as the values
  they encode.

A finding is only worth posting if you can describe how it goes wrong: the
input or state, and the wrong result, panic, leak or corruption it causes. If
you can't, check more of the code or leave it out.

## Classes of subtle bug in this codebase

These have all occurred before. Check for them whenever the diff is nearby.

- **Index and planner paths that disagree with a table scan.** An index scan,
  pushed-down filter, sort elimination or `LIMIT` pushdown must return exactly
  the rows, and order, that a full scan would. A mismatch still returns rows,
  just the wrong ones, so the user sees no error.
- **Checks skipped at plan time.** Expressions evaluated or folded while
  planning must still be subject to the permission, capability and validation
  checks that apply at execution.
- **State reused across executions.** Per-query, per-row or per-subquery state
  (caches, `TopK` thresholds, iterators, cursors) must be reset when the same
  plan runs again, for example once per outer row.
- **Escaped and raw names.** Catalog names and record keys must use the raw
  name, not the escaped text produced by `Display`. Mixing them breaks lookups
  for names that need escaping.
- **Transaction visibility.** Code may wrongly assume it can see its own
  uncommitted writes, writes from transactions that started earlier, or a
  snapshot the storage engine does not provide. Savepoints and rolled-back
  statements must also undo buffered side effects.
- **Write-once keys and soft deletes.** Removing a definition must clear every
  key it owns. Otherwise, a later definition with the same name inherits stale
  data.
- **Background work.** Index builds, compaction, changefeed GC and async events
  can race with user writes and schema changes. Check what happens if the
  definition changes or is removed while the task runs.
- **Coercion and casting.** Numeric overflow, decimal precision, float-to-int
  conversion, and datetime precision or timezone changes.

## Persisted formats and upgrades (usually CRITICAL)

- A change to a storage key, an index layout, or a type that derives `Revision`
  must still decode data written by every earlier release. An incompatible
  change is CRITICAL even if every test passes.
- A change to a persisted format needs a matching pair in
  `language-tests/tests/upgrade/`. If the upgrade requires an operator step
  such as `REBUILD INDEX`, the test must declare it as `[[test.repair]]`. The
  statement must never appear in the test body.
- A change that ships a release must add it to all three of
  `LANG_UPGRADE_VERSIONS`, `LANG_UPGRADE_SEEDS` and `LANG_UPGRADE_CHAIN` in
  `Makefile.ci.toml`.

## Security (see `SECURITY_GUIDE.md`)

Prefix security findings with "Security:". Use the same severities as
`SECURITY_GUIDE.md`:

- **CRITICAL**: auth bypass, privilege escalation, data leaking across a
  namespace or database boundary, or code execution from user-supplied data.
- **HIGH**: a missing or incomplete permission check, user-controlled resource
  use with no bound, internal details leaking through error messages, or a
  session isolation failure.
- **MEDIUM**: a defense-in-depth gap or an overly broad capability default.

Specific things to check:

- Credentials, tokens and user data must never appear in logs, traces or error
  messages.
- New endpoints, RPC methods and statements must check authentication and
  permissions before they read or write anything. Check every path that
  reaches the data, not just the obvious one: subqueries, graph traversal,
  `FETCH`, record links, computed fields, events, views, live queries, export.
- Any recursion, allocation or loop that user input can drive needs a bound.
- A code path that intentionally breaks an invariant must carry a
  `// SECURITY:` comment that explains why. Flag a deviation that has no such
  comment.

## Concurrency and async

- Flag blocking I/O or CPU-heavy work on an async task.
- Flag a `.await` while a synchronous lock is held.
- A background task needs a cancellation path tied to its owner, and its loop
  must end on a condition that shutdown can reach.
- When code holds more than one lock, the lock order should be documented.
- A retry loop must not re-apply a side effect that already happened.

## Error handling

- Do not swallow errors. Flag `.unwrap_or_default()` in non-test code, and
  `let _ =` on a fallible call that has no comment justifying it. A swallowed
  error in a read path often becomes a wrong result.
- Datastore errors should propagate through `crate::err::Error` before being
  wrapped in `anyhow`. Use `thiserror` for domain error enums.
- Flag `unwrap()`, `expect()`, indexing and slicing on values that user input
  or stored data can make fail. Do not flag them on invariants the surrounding
  code has already established.

## Performance

Weigh performance findings by where the code runs:

- **Hot paths**, such as query execution, the document pipeline, key encoding,
  index scans and transaction commit. Here, extra allocations, clones and
  repeated lookups deserve MEDIUM. Work that grows with data size, such as an
  accidental full scan or a quadratic loop, deserves HIGH.
- **Cold paths**, such as DDL, CLI commands, startup and tests. Only comment on
  performance here when the cost grows with data size.

## Tests

- A bug fix should come with a test. Prefer a SurrealQL language test in
  `language-tests/tests/` over a Rust test whenever the behaviour can be seen
  from a query.
- New SurrealQL functionality needs `.surql` tests.
- Treat a weak test as a correctness problem. A test that would still pass with
  the fix reverted, that only checks the happy path, or whose expected output
  encodes the bug, proves nothing. Name the case the test misses.

## Code hygiene (LOW)

Hygiene comments are welcome but come last:

- Mark them LOW and keep each to a sentence or two.
- Post at most three per review, and only after you have finished the
  correctness analysis above.
- Skip anything a reader would fix by reflex, such as a slightly better name.

Comments in code should describe what the code does now and why: its
invariants, ordering, inputs, outputs and error modes. Flag a new comment that
narrates the change ("previously X, now Y", "no longer"), refers to a PR,
ticket or commit, or mentions a bug the same change fixes. Your own
suggestions must follow the same rule.

A new external crate needs a justification in the PR. Prefer workspace-managed
dependencies in the root `Cargo.toml`.

## What CI already checks

Every PR is built and tested on the pinned toolchain, so do not comment on any
of the following:

- **Compilation.** Do not say code "will not compile", a method "does not
  exist", or a module "is not in scope". CI builds every PR. If something looks
  missing, it is almost always defined somewhere outside the diff.
- **Rust version.** The toolchain is pinned in `rust-toolchain.toml` (currently
  1.95) and there is no MSRV. Never suggest replacing a stable std API with an
  older equivalent for compatibility. For example, do not swap
  `Option::is_none_or` for `map_or(true, ...)`; clippy rejects that rewrite.
- **Formatting.** Nightly rustfmt runs with `hard_tabs = true` and
  `comment_width = 100`. Indent suggestion blocks with tabs.
- **Clippy lints**, unused dependencies, and `std::time::{Instant, SystemTime}`
  imports (code must use `web_time`, and CI enforces it).
- **`revision.lock` and `Cargo.lock` contents**, which are generated and
  validated by tools.

## How to write findings

- Start with the failure: what input or state, and what goes wrong. Then
  explain why, then suggest a fix.
- Do not post hypotheticals that depend on code you cannot see, such as "if
  the format of X ever changes". Only post them when the diff makes that input
  reachable.
- Do not write comments that only describe the diff, such as "this check was
  moved up".
- One comment per issue. Do not post the same point on every call site.
