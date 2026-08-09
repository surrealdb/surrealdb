# CLAUDE.md

Conventions for the SurrealQL test suite in `language-tests/`.

## Commands

```bash
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

## Language Tests (`language-tests/tests/*.surql`, `*.gql`)

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

## Upgrade Tests (`language-tests/tests/upgrade/*.surql`)

Each test is a pair: `X_import.surql` runs on an old released binary and writes
data, `X.surql` runs on a newer one and asserts what comes back. The versions
hopped through live in `LANG_UPGRADE_VERSIONS` in `Makefile.ci.toml`.

**Every new release must be appended to `LANG_UPGRADE_VERSIONS` before the `..`
token.** The chain runs *adjacent pairs*, so a release that is missing does not
merely go untested — it widens the final hop into a multi-release jump, and the
version users are actually upgrading from stops being covered anywhere. Nothing
fails when this is forgotten; the chain just quietly tests less than it appears
to. Binaries must be published at `https://download.surrealdb.com/v{ver}/` for
every entry.

`;` separates independent chains and `,` separates versions within one. Hops are
made only within a chain, so a pair that cannot work — one spanning a defect in a
released binary that no gate can express — is excluded by breaking the chain
there, leaving every other pair covered.

List stable releases only. A beta belongs in the chain while its minor is
unreleased, and must be replaced by the stable that supersedes it.

A released binary with a defect cannot be fixed retroactively, so a hop
targeting one stays red. Gate the affected test with `version` (bounds the
upgrade target) or `importing-version` (bounds the source) rather than removing
the release from the chain, so the remaining tests still cover that hop.

The import phase defines the namespace and database explicitly before replaying
a test's imports. Do not make it depend on a write implicitly creating them:
that is not dependable across the released binaries the chain spans, and when it
does not happen every import statement fails against a store that was never
written to.
