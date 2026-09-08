# Upgrade tests

These tests prove one property: **state written by a released SurrealDB binary
is still read correctly by a later one.**

Nothing else in the test suite proves it. Ordinary language tests write and read
with the same binary, so a format change is invisible to them — both sides move
together. Unit fixtures under `surrealdb/core/src/kvs/compat/` decode frozen
bytes, which covers the decoder but not the read path around it: indexes, the
planner, and maintenance tasks. Only these tests spawn a real old server, let it
write, stop it, and spawn a real new server on the same data directory.

The failure they exist to catch is silent. A structure that stops being readable
usually does not error — an index that cannot be read falls back to a table scan
and returns the same rows; a dropped adjacency entry just returns a shorter list.

## The three axes

Each proves something the other two cannot, so none is redundant.

| Axis | Invocation | Property |
|---|---|---|
| Adjacent pairs | `upgrade --from A --to B` | release B reads what release A wrote |
| Seeds | `upgrade --from A,B,C --to ..` | the current checkout reads what an *old* release wrote, with no binary in between to migrate it |
| Cumulative walk | `upgrade --chain A,B,C,..` | a structure created once stays correct while every release in turn maintains it |

The pair axis is the widest but shallowest: the oldest writer HEAD ever sees is
the release immediately before it. The seed axis is the only one that sends a
3.0.x writer straight to today's build. The walk is the only one that shows a
structure surviving repeated maintenance rather than a single hop.

`--from` and `--to` are a cartesian product, so `--from a,b --to x,y` schedules
four runs per test. `--chain` conflicts with both: one invocation runs one axis.

CI drives all three from `Makefile.ci.toml` (`tasks.test-lang-upgrade`) over the
version lists defined at the top of that file.

## `..`, and the version lists

`..` is not a special token. Versions that fail to parse as semver are treated as
filesystem paths, and because the CI script runs from `language-tests/`, `..` is
the repo root. A path version is resolved by running `cargo metadata --no-deps`
there and reading the version of the workspace package named `surrealdb` — the
SDK crate, not the root binary package, which is named `surreal`. It is then
built with `cargo build --bin surreal`. Any other checkout works the same way:
`--to /path/to/another/clone`.

Everything else is a tag, downloaded from
`https://download.surrealdb.com/v{ver}/surreal-v{ver}.{platform}` and cached in
`language-tests/.binary_cache/`. The CDN is used rather than GitHub Releases
because it carries tags that were never mirrored there. `--allow-download`
waives the confirmation prompt and, as a side effect, lets the downloads run in
parallel.

The binary splits version lists on `,` and nothing else. The `;` that appears in
`LANG_UPGRADE_VERSIONS` is bash, split by the CI script before the binary is
called; it marks a hard boundary that no hop crosses. The chain axis erases it
(`${LANG_UPGRADE_CHAIN//;/,}`), because a walk cannot skip a version. Passing a
`;` straight to `--from` produces a path version and a confusing failure from
`cargo metadata`.

## Layout

```
tests/upgrade/
  3.00.00_basic_data.surql          a few tests live at the root
  define/    schema and catalog definitions   (see define/README.md)
  chain/     cumulative-walk tests            (see chain/README.md)
  value/     value encodings                  (see value/README.md)
```

The directory names are for humans. The runner only ever uses a path as a
substring filter, so nothing about behaviour follows from which folder a test is
in.

Two location rules do bite:

- Every `.surql` file anywhere under `tests/upgrade/` must carry a
  `<major>.<NN>.<NN>_` filename prefix. The `filename_prefix_tests` unit test in
  `src/cmd/upgrade/mod.rs` enforces it and checks it against `[test] introduced`.
  You cannot park an ordinary language test in this tree.
- A file here **without `upgrade = true` is silently just a language test.**
  Nothing warns. This is the easiest way to add something that looks like
  upgrade coverage and is not.

The prefix is zero-padded per component so a plain listing sorts chronologically
— unpadded, `3.1.1` sorts after `3.10.0`. `3.00.00` means the feature predates
the oldest binary in the matrix, which is true of most of them.

## Anatomy

A **pair** is two files. `X_import.surql` runs on the old binary and writes;
`X.surql` runs on the new one and asserts.

```
/**                                   ← X_import.surql
[test]
reason = "Setup for X.surql: the old binary writes the definition."
run = false
*/
DEFINE TABLE widget SCHEMAFULL;
```

```
/**                                   ← X.surql
[env]
imports = ["upgrade/define/3.00.00_widget_import.surql"]

[test]
upgrade = true
reason = "What this proves that the other axes cannot."

[[test.results]]
value = "..."
*/
INFO FOR DB;
```

A **chain** test adds `[test.chain]` with `setup` and `mutate` scripts, and is
picked up only by `--chain`. See `chain/README.md`.

Points that are easy to get wrong:

- Import paths are relative to the tests root and **include the `upgrade/`
  segment**.
- Namespace and database both default to `test` and are provisioned by the
  runner. No `[env]` block is needed for them.
- Exactly **one `[[test.results]]` entry per statement**. `DEFINE …` returns
  `NONE` and still consumes one.
- The `/** … */` block must be the only one in the file — a second one anywhere,
  including after the body, is a load error.
- `run = false` marks a helper as not-a-test-in-its-own-right. It also removes
  the file from the upgrade runner, so never put it on a body.
- The runner always connects as root over its own endpoint, so `env.auth`,
  `env.signin`, `env.signup`, `env.versioned`, `env.backend` and `env.timeout`
  are rejected rather than ignored.
- `[env.capabilities]` is rejected except for `allow-experimental`, which is
  passed to the spawned binary. Writing the table at all without that key is a
  load error. It must sit on the graded case; on a helper it is inert.

## Which release runs what

Three keys look alike and mean different things.

| Key | Bounds | Meaning |
|---|---|---|
| `introduced` | the writing side | the first release that can *create* this state |
| `importing-version` | the writing side | exclude a hop because the *writer* is defective |
| `version` | the reading side | exclude a hop because the *reader* is defective |

`introduced` is the feature gate and is load-bearing: pair runs skip hops whose
source binary predates it, and a chain walk takes it as the default
`[test.chain] since`. The other two exist to route around a defect in a binary
that has already shipped and cannot be fixed. Same shape, opposite intent.

On an import file `version` flips sides, since the import runs on the source.

## The repair contract

**Never write a repair statement such as `REBUILD INDEX` into a test body.** The
loader rejects it. It repopulates the structure from the base records, so every
assertion after it reads bytes the *new* binary just wrote and a total failure to
read the old layout still passes.

When an upgrade genuinely requires a manual step, declare it:

```toml
[[test.repair]]
since = ">=3.3.0-beta.1, <3.4.0"   # upgrade targets that need the step
script = "upgrade/define/3.00.00_index_fulltext_rebuild.surql"
unrepaired = "broken"              # or "degraded"
reason = "Why an operator has to do this; this is release-note material."
```

The runner then brackets the body: it runs once and must show the declared damage
(`broken` = a statement errors, `degraded` = everything completes but a result
differs), the repair runs, and the body runs again and is graded normally. Both
directions are checked — an undeclared requirement fails because the body breaks
with no repair to apply, and a declaration that has stopped reproducing fails
because the pre-repair run no longer shows the damage it claims.

Because the body runs twice against the same store, **a body carrying a repair
must be read-only.** Put any post-upgrade write in a `*_current.surql` sibling.

## Writing one

1. Write the `_import.surql` using only SurrealQL the **oldest** binary that will
   run it accepts. A released binary's SurrealQL is not today's.
2. Write the body with a `reason` that says what this proves that the other axes
   cannot, and one comment per query.
3. Bootstrap the expectations. `--results accept` alone does nothing here,
   because unspecified tests are not scheduled — you need both flags, and one
   `--from`/`--to` pair at a time, since each report rewrites the whole file:

   ```bash
   cd language-tests && cargo run --locked --features upgrade -- --color always upgrade --no-results --results accept --backend surrealkv --allow-download --from 3.2.4 --to .. define/3.00.00_widget
   ```

4. Re-run **without** `--no-results` to confirm it is actually scheduled and green.
5. Run it on more than one seed. Traversal and scan order follow the writer's key
   layout and differ between releases, so assert membership and cardinality —
   sort before comparing — never order, and never storage shape.

Assert query results only. Adjacency folding can rewrite the storage layout
mid-test without changing any result, so an assertion about layout is invalid by
construction.

An index needs an `EXPLAIN` assertion naming the operator, or the test cannot
tell a working index from one that silently fell back to a scan. `EXPLAIN` output
is not stable across the 3.x line, so that assertion lives in its own
`*_current.surql` gated with `version = ">=3.4.0-a"`.

## Running locally

```bash
cd language-tests && cargo run --locked --features upgrade -- --color always upgrade --backend surrealkv --allow-download --no-wip --jobs 8 --from 3.2.4 --to ..
```

- `--features upgrade` is required; the subcommand bails without it.
- Run from `language-tests/`. `./tests`, `.binary_cache`, `.module_cache` and the
  meaning of `..` are all relative to it.
- `--color` is the only argument that belongs *before* `upgrade`. Everything else
  follows the subcommand.
- Narrow with the **positional** filter, a plain substring match on the test path.
  There is no `--filter` flag, and `--path` re-roots every import and breaks the
  run.
- `--backend` accepts `surrealkv` (default) and `rocksdb` only. `rocksdb` needs
  the `backend-rocksdb` feature as well; `upgrade` already implies surrealkv.
- The first `..` hop is a full debug build of the server.
- Any run over the default tree builds the Surrealism wasm fixture, because one
  import references it; the positional filter does not prevent this. Without a
  wasm toolchain you get a warning and lose those tests. CI sets
  `LANG_UPGRADE_REQUIRE_MODULES=1` so that becomes a failure instead.

The harness's own unit tests — including the filename-prefix guard — run with
`cargo test --features upgrade` and nothing else in CI executes them.

## Adding a release

Add it to **all three** lists in `Makefile.ci.toml`, before the trailing `..` in
the first and third; the seeds list has no terminator. Nothing turns red if you
forget: the pair axis silently widens its last hop into a multi-release jump, and
the walk quietly stops short of HEAD.

The lists are deliberately different. Stable releases only — a beta belongs only
while its minor is unreleased. The walk additionally omits any release that
demands a manual repair, and any release that cannot be upgraded onto its
successor in the list.

**Release order is not version order.** A patch on an older line can ship after a
newer line and carry a catalog revision that line's early releases cannot decode,
which makes its semver-adjacent successor an invalid target. Run the hop both
ways before placing the entry. `3.1.6` is the standing example: it writes
`TableDefinition` at revision 3, so 3.2.0 and 3.2.1 reject it and its first valid
target is 3.2.2.

Every numeric entry needs a published binary on the CDN, and a new release also
wants a frozen catalog snapshot under `surrealdb/core/src/kvs/compat/`.

## What is not covered here

- **Subscriptions.** They do not survive a restart at all, so an assertion would
  observe an empty set whether or not decoding works. See `define/README.md`.
- **New syntax no released binary can write.** Until a release ships it, no axis
  can reach it — `introduced` gates a pair to zero runs and turns a chain test
  into a load error. Ordinary language tests are its home until then.
- **Authenticated round-trips**, rolling upgrades, and TiKV.
