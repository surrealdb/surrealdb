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

**Expectations that span lines:** a value whose text runs over several lines —
what an `EXPLAIN` renders — is recorded as a TOML multi-line literal, so the
expectation reads as the plan tree it is rather than as one line of escapes:

```surql
[[test.results]]
value = '''
'Limit [ctx: Db] [limit: 0]
    CountScan [ctx: Db] [source: sale]
''''
```

The literal holds SurrealQL source, so the string inside keeps its own quotes,
and the newline straight after the opening delimiter belongs to TOML rather than
to the value. `--results accept` writes this form for every string carrying a
newline and the single-line form for everything else, including arrays and
objects with newlines in a nested string. Text a literal cannot hold — text
carrying a literal delimiter of its own, or a carriage return — stays on one
line.

### Query traces

`[env] trace = ["relations", "links", …]` runs the query as a traced request,
and a table-form result states the statement's trace under `trace`. The
statement's trace and its value are checked together:

```surql
[[test.results]]
value = "[person:b]"
trace = "{ relations: [[person:a, knows:ab, person:b]] }"
```

A result with no `trace` expects none, so a traced test fixes every
statement's trace, the empty ones too. The tests live in
`tests/language/trace/`. `--no-results --results accept` writes what the
engine sends now, which can include its bugs. Read each recorded trace against
the query before keeping it, and check that it holds no record, edge or link
the session cannot read. `README.md > Traces` has the comparison rules.

### Which engine a test runs on

Every `.surql` and `.gql` file runs once per backend, on the default engine:
the bytecode VM. There is no per-test engine selector, so a plan assertion
(`EXPLAIN`) pins the VM's plan and nothing else.

### Running against a server

`run --endpoint <url>` drives the suite through the SDK against a real server
instead of an in-process datastore. Tests run concurrently there, isolated by a
namespace each (`lt_<run>_<n>`), so a test that reaches outside one — a root
user or access method, a namespace, `ALTER SYSTEM`, or an assertion on the
literal name of its own namespace — must declare `[env] remote-exclusive` and is
given the endpoint to itself. Undeclared root-level state fails the test by name
rather than racing; `README.md > [env.remote-exclusive]` has the message and the
rules.

**The suite is destructive to whatever it is pointed at.** It drops and creates
namespaces and removes root configs, so point it only at an instance nobody else
is using.

### Tagging tests by engine feature

A test that cannot pass without scripting, GraphQL, DiskANN, change feeds or the regex value
type declares it with `[test] requires = ["scripting"]`, so a run against an engine without
the feature (`--without scripting`) skips it instead of failing it. Tag the test whenever it
depends on one of them. `README.md > Features a run can be without` has the list and the rules.

### Naming and companions

Reproductions are flat in `tests/reproductions/`, named for the behaviour they
hold rather than for where they came from: `a_counted_value_is_the_count_itself`,
not a PR or branch name. A GitHub issue number is the one prefix that earns its
place (`ISSUE_NUMBER_description.surql`), because it is an address a reader can
look up. Put the provenance in `issue = 1234` or in `reason`, not in the
filename.

Two suffixes are load-bearing. A new test should carry no other:

| Suffix | Is |
|---|---|
| `_import` | Setup only, run as owner by the files that import it. Names the tests it serves in its `reason`. |
| `_plan` | The `EXPLAIN` companion of the test of the same name — the plan its rows were produced by. |

`imports = [...]` entries are paths from the `tests/` root
(`"reproductions/x_import.surql"`); `./` and `../` resolve against the importing
file's own directory. Imports are transitive and de-duplicated, and an import
must itself be a `.surql` file.

### Prove the test catches its own bug

A reproduction that passes against the unfixed code tests nothing. Revert the
fix, watch the test fail, restore it, watch it pass — in both directions.

Generating expectations has a trap: `--results accept` fills only blocks that
are **empty**, and numeric expectations are type-blind: `3` is satisfied by
`3f` and `3dec`, so pin a type with `type::is_int(...)` rather than by writing
the number.

### Testing denied permissions

**Permission tests must assert the effect on state, never the mere absence of an
error.** A write denied by table-level `PERMISSIONS` returns `OK` with an
empty/filtered result — it does not throw (see `SECURITY_GUIDE.md` § Data Write
Operations). A test that discards the result and checks only "did it throw" cannot
tell that response apart from a `WHERE` that matched nothing, or (since `SELECT`
permission on a write is checked after the write completes) a write that succeeded
but is unreadable to the caller — so such a test passes in all of these cases,
including the one it means to catch. Assert what the write actually did instead.
A test session is fixed for the whole file (`[env].auth`/`signin`), so re-reading
as a different, more privileged principal isn't available mid-test. Use one of:
- grant the test principal itself enough `SELECT` permission to observe the
  written record or field directly (e.g.
  `access/record/create_permission_vs_data_clause.surql` does this with
  `SELECT * FROM pcond` to confirm a conditionally-denied create left no row),
  or
- a side effect that only runs *after* the write is stored, such as a
  `DEFINE EVENT` on the target table: the `Create` pipeline runs table-level
  `PERMISSIONS FOR create` (`CheckCreatePermissions`) and stores the record
  (`StoreRecordData`) before firing events (`ProcessTableEvents`), so a
  denied create cannot trigger one. Prefer this when granting the principal
  `SELECT` on the target would itself change the scenario under test.

Either way, a side effect is only as good as *when* it runs. A `VALUE`/`SET`
data clause with a side effect (e.g. `CREATE pnone:1 SET spawn = (CREATE
log).id`) is not a substitute for either technique above: a conditional
(`WHERE`-based) permission evaluates its predicate only after the data clause
has already run, so the side effect fires whether the write is ultimately
denied or accepted (see the same test's `pcond` case) — it proves the data
clause ran, never whether the target write took effect.

## Upgrade Tests (`language-tests/tests/upgrade/*.surql`)

`tests/upgrade/README.md` is the full description of the harness: the axes, the
frontmatter, how to bootstrap a new test's expectations, and how to run it
locally. This section covers the conventions a change to the suite has to
respect. **Read both before adding a test.**

When you must touch this suite at all: a persisted format changes, a durable
construct is added and a release can now write it, or a release is cut. The rule
and its rationale are in the root `CLAUDE.md`.

Two traps worth knowing before anything else. A file under `tests/upgrade/`
without `upgrade = true` is silently an ordinary language test, so it can look
like coverage while proving nothing. And a test written without a
`[[test.results]]` block is never scheduled — `--results accept` on its own does
nothing here, because unspecified tests are filtered out before it runs; pass
`--no-results` alongside it to bootstrap.

Three axes run over the same tree, and a fourth over its own `migration/` tests, driven by `LANG_UPGRADE_VERSIONS` and
`LANG_UPGRADE_SEEDS` in `Makefile.ci.toml`. Each proves something the others
cannot, so none of them is redundant:

| Axis | Invocation | Property |
|---|---|---|
| Adjacent pairs | `upgrade --from A --to B` | release B reads what release A wrote |
| Seeds | `upgrade --from A,B,C --to ..` | the current checkout reads what an *old* release wrote, with nothing in between |
| Cumulative walk | `upgrade --chain A,B,C,..` | a structure created once stays correct while every release in turn maintains it |
| RocksDB migration | `upgrade --backend rocksdb-migration [--versioned] --from A,B --to ..` | the current checkout opens as `surrealkv://` a directory a release wrote as `rocksdb://`; runs only `[test] migration = true` tests (see `tests/upgrade/README.md`) |

The spawned servers run with maintenance tasks live, and graph adjacency folding
is on by default: a traversal over a vertex with enough edges can fold its scope
mid-test, rewriting the storage layout (never the query results). Assert only on
query results in upgrade tests, never on raw key layout or storage shape.

`LANG_UPGRADE_CHAIN` is a separate list from `LANG_UPGRADE_VERSIONS`: a walk must
not pass through a release that demands a manual repair, since the repair
replaces the very structure the walk is following. The runner refuses to
schedule a chain test whose repair would fire mid-walk.

**Every new release must be added to all three of `LANG_UPGRADE_VERSIONS`,
`LANG_UPGRADE_SEEDS` and `LANG_UPGRADE_CHAIN`** — before the trailing `..` in the
first and third; the seeds list has no terminator.
`LANG_UPGRADE_CHAIN` deliberately omits releases that demand a manual repair, so
read its comment before adding a prerelease; forget it entirely and the
cumulative walk quietly stops one release short of HEAD, with nothing red. The pair axis hops adjacent
entries, so a missing release does not merely go untested — it widens the final
hop into a multi-release jump, and the version users actually upgrade from stops
being covered anywhere. Nothing fails when this is forgotten; the chain just
quietly tests less than it appears to. Binaries must be published at
`https://download.surrealdb.com/v{ver}/` for every entry.

List stable releases only. A beta belongs in the chain while its minor is
unreleased, and must be replaced by the stable that supersedes it.

Release order is not version order. A patch on an older line can ship after a
newer line has released and carry a catalog revision that line's early releases
cannot decode, so its semver-adjacent successor is not a valid upgrade target.
Before adding a release, run its hop both ways and place it by what actually
reads it: `3.1.6` writes `TableDefinition` at revision 3 and can only be
upgraded onto 3.2.2 or later, so it forms its own chain and is absent from the
cumulative walk entirely.

`;` separates independent chains and `,` separates versions within one. Hops are
made only within a chain, so a pair that cannot work — one spanning a defect in
a released binary that no gate can express — is excluded by breaking the chain
there, leaving every other pair covered. A released binary with a defect cannot
be fixed retroactively, so a hop targeting one stays red; gate the affected test
with `version` (bounds the upgrade target) or `importing-version` (bounds the
source) rather than removing the release from the chain.

### File naming

Every file under `tests/upgrade/` opens with the release that introduced the
feature it covers, zero-padded to two digits per component:
`3.01.00_index_diskann.surql`. The padding is what keeps a plain listing
chronological — unpadded, `3.1.1` sorts after `3.10.0`. `3.00.00` means the
feature predates the oldest binary in the matrix, which is most of them.

The prefix mirrors `[test] introduced`, and a unit test
(`filename_prefix_tests` in `src/cmd/upgrade/mod.rs`) fails when the two
disagree — a naming convention nothing checks drifts, which has already happened
once in this suite. Helper files (`_import`, `_setup`, `_mutate`, `_rebuild`)
carry their parent test's prefix so the pair stays adjacent.

`introduced` states the release that can first *create* the state under test. It
is load-bearing: pairwise runs skip hops whose source binary predates it, and a
chain walk takes it as the default `[test.chain] since`. Keep it distinct from
`version`/`importing-version`, which exclude a hop because a *released binary is
defective* — same shape, opposite intent.

### Pairwise tests

A pair: `X_import.surql` runs on the old binary and writes data, `X.surql` runs
on the newer one with `upgrade = true` and asserts what comes back.

The import phase defines the namespace and database explicitly before replaying
a test's imports. Do not make it depend on a write implicitly creating them:
that is not dependable across the released binaries the chain spans, and when it
does not happen every import statement fails against a store that was never
written to.

The runner always connects as root over its own spawned endpoint, so
`env.auth`, `env.signin`, `env.signup`, `env.versioned`, `env.backend` and
`env.timeout` are rejected rather than ignored — a test configured with them
would assert against a session it never had.

A released binary's SurrealQL is not always the current one. `DEFINE MODULE`
spells its source `AS f"bucket:/key"` on every release in the matrix and gained
`FROM ... UNSIGNED` afterwards, so an import writing one cannot also be a file
the current parser reads. Such a script declares `[test] legacy-syntax`, which
takes it out of the parser's language-test corpus check. Only a script replayed
against a spawned release may set it; the runner refuses it on a test body or a
chain `mutate`, both of which run on the upgrade target.

`[env.capabilities]` is rejected too, with one exception: `allow-experimental`
is passed to the spawned binary through `SURREAL_CAPS_ALLOW_EXPERIMENTAL`, which
every release in the chain honours. That is what lets `DEFINE BUCKET` and file
values be tested. A target an older binary does not recognise makes it exit
during startup, which surfaces as a failure to open the connection.

### Cumulative tests

A test declaring `[test.chain]` is walked through the whole version list against
one datastore. See `tests/upgrade/chain/README.md` for the file format. Matchers
see `$hop`, `$hops`, `$version` and `$chain`; the `LET` prelude prepended to
`setup`, `mutate` and imports binds only the first three.

### Assert the read, not the repair

**Never write a repair statement such as `REBUILD INDEX` into a test body.** The
loader rejects it, because it destroys the thing the test exists to check: the
statement repopulates the structure from the base records, so every assertion
after it reads bytes the *new* binary just wrote, and a total failure to read
the old layout passes. This is not hypothetical — it is how the move to a
table-level shared doc-ID space stopped being observable.

When an upgrade genuinely requires a manual step, declare it:

```toml
[[test.repair]]
since = ">=3.3.0-beta.1, <3.4.0"   # upgrade targets that need the step
script = "upgrade/define/3.00.00_index_fulltext_rebuild.surql"
unrepaired = "broken"              # or "degraded"
reason = "Why an operator has to do this; this is release-note material."
```

The runner then brackets the body: it runs once before the repair and must show
the declared damage (`broken` = a statement errors, `degraded` = everything
completes but a result differs), the repair runs, and the body runs again and is
graded normally. Both directions are checkable — an undeclared requirement fails
because the body breaks with no repair to apply, and a declaration that has
stopped reproducing fails because the pre-repair run no longer shows the damage
it claims. A stale "you must rebuild" cannot sit in the suite pretending to be
coverage.

**A body with a declared repair must be idempotent.** The bracket runs it twice
against the same store, so `CREATE x:1` succeeds on the first pass and fails
with "already exists" on the second. Use `UPSERT` for any write the body makes.

### Assert the plan, not just the rows

An index that silently becomes unusable falls back to a table scan and returns
exactly the same rows; `WITH INDEX` is an allow-list, not a force. An upgrade
test covering an index needs an `EXPLAIN` assertion naming the operator, or it
cannot tell a working index from a missing one.

`EXPLAIN` output is not stable across the 3.x line — older releases return an
array of plan objects with entirely different operator names, and older
releases reject or render the statement differently. A plan assertion therefore
lives in its own `*_current.surql` file, gated with `version = ">=3.4.0-a"` to bound
the upgrade target to the current checkout. The
seed axis sends every released version straight to the current checkout, so the
gate costs no coverage of the hops that matter. Keep the data assertions in the
ungated sibling, where they run on every hop.
