# Upgrading SurrealDB

This document records per-release operational notes for SurrealDB
administrators: wire-format rotations, removed features, configuration
changes, and other impacts that aren't apparent from version numbers
alone. Read this before deploying a new release onto an existing
database.

For the release **process** (how releases are cut and shipped), see
[`RELEASING.md`](./RELEASING.md). For end-user release notes, see
<https://surrealdb.com/releases>.

## v3.3.0 — datastore version stamp, startup data migrations, the sequence keyspace move, module re-keying, and rollbacks reported as `cancelled`

**Impact**:

- The first v3.3.0+ node to start records data migrations it has applied; a
  build that lacks one of them refuses to start.
- Sequence definitions gain a second home, and a sequence created mid-rollout
  is not visible to older nodes.
- A silo module defined by v3.2.4 or earlier is unreachable under v3.3.0+ until
  migration 2 runs, and once it has run, older nodes stop resolving that
  module. `mod::` modules are unaffected.
- Dashboards that count transactions with
  `surrealdb_transaction_total{outcome="success"}` will read lower after the
  upgrade. Nothing about transaction behaviour changes — only how a rollback is
  labelled.

### Startup now applies data migrations

A v3.3.0+ node stamps the datastore with its full semantic version at `/!vs`,
appends a version-history entry at `/!vh`, and records each data migration it
applies at `/!mg{id}`. Migration selection reads that ledger, not the stamp: a
migration runs when its ledger entry is absent and its declared version is at or
below the running build.

**Binary downgrade is one-way once a migration has been applied.** A build whose
registry does not contain a migration the ledger records exits at startup with
`The data stored on disk was migrated by a newer version of SurrealDB … and
cannot be read by this one`. The `MajorVersion` guard does not catch this, for
the same reason it does not catch the v3.1.0 rotation: the migration happens
inside the existing major version.

If you need the option to roll back, take and verify a logical backup
(`surreal export`) **before** running v3.3.0+ against live data.

### Sequence definitions moved out of the table band

`DEFINE SEQUENCE` definitions were stored at `/*{ns}*{db}*sq{name}`, which is
the same byte position a table name occupies. A sequence named `foo` therefore
shared its key with the keyspace root of a table named `sqfoo`, and listing
sequences on a database containing any table whose name begins with `sq` read
that table's rows and failed — taking `INFO FOR DB`, `surreal export`, and
`REMOVE DATABASE`/`REMOVE NAMESPACE` with it.

Definitions now live at `/*{ns}*{db}!sd{name}`. Migration 1 **copies** each
definition to the new location and leaves the original in place, so a node still
on the previous release keeps resolving its existing sequences throughout a
rolling upgrade.

What still works during a mixed-version rollout:

- Existing sequences, on nodes of either version. `sequence::nextval` continues
  from the pre-upgrade watermark; allocator state is not touched by the
  migration.
- Every other statement.

What doesn't:

- **A sequence created after the rollout begins is visible only to v3.3.0+
  nodes.** They write the definition to `!sd` alone; an older node resolves `*sq`
  and will not find it. Avoid `DEFINE SEQUENCE` until every node is upgraded, or
  re-run it afterwards.
- Rolling back to a pre-v3.3.0 build after any v3.3.0+ node has started, per the
  downgrade note above.

The retained `*sq` keys are inert under v3.3.0+ — nothing reads them — and a
later release will delete them once no supported upgrade path starts below
v3.3.0.

### Time-travel reads of sequences predate the move

On a versioned backend (`?versioned=true`), `INFO FOR DB VERSION <t>` and
`sequence::nextval` resolved at a timestamp *before* the migration committed
return no sequences: the `!sd` keys do not exist at those timestamps, and the
read path does not consult the retained `*sq` keys. Sequences at the present
timestamp are unaffected, as is every other catalog type.

### If a migration cannot finish

Migrations run inside the server's startup budget
(`--startup-operation-timeout`, default 60s). Migration 1's cost is proportional
to the number of tables whose names begin with `sq`, not to the number of
sequences, so it is fast on ordinary databases. If a start times out during
migration, raise that flag rather than restarting into the same timeout.

### Why a silo module written by an older release cannot be reached

A module definition is stored at `/*{ns}*{db}!md{name}`, where `{name}` is not
part of the value but derived from it. `REMOVE`, `ALTER`, `DEFINE … OVERWRITE`
and a call to one of the module's exports all re-derive that name and read the
one key it spells.

The silo derivation gained a `::` before its version segment in v3.3.0, so
releases up to and including v3.2.4 wrote

```
silo::acme::widgets<1.2.3>
```

where v3.3.0+ derives

```
silo::acme::widgets::<1.2.3>
```

Nothing reported the mismatch. `INFO FOR DB` lists modules by scanning the band
and recomputing each name from the decoded value, so the definition rendered
exactly as it should while `REMOVE MODULE` on the name `INFO` had just printed
answered `The module '…' does not exist`. `DEFINE MODULE … OVERWRITE` then wrote
a *second* row at the derived key: `INFO FOR DB STRUCTURE` showed the module
twice, and `INFO FOR DB` still showed it once, because both rows recompute to
the same map key.

### Migration 2 moves the definition

Migration 2 walks the module band of every database and moves each row whose key
disagrees with the name its own value derives. Values are rewritten unchanged,
so permissions, comments and the `unsigned` flag carry across untouched.

Where both rows already exist — the store of an operator who worked around the
break with `DEFINE MODULE … OVERWRITE` — the addressable row wins and the
unreachable one is dropped, leaving one definition where there were two.

Unlike migration 1, this one **moves** rather than copies. The two keys hold the
same module, and the listing paths scan the band rather than addressing keys, so
leaving both behind would make every consumer of the module list report the
module twice. That is the state the migration exists to end.

What still works during a mixed-version rollout:

- Every `mod::` module, on nodes of either version. `mod::{name}` has never
  changed, and the migration does not touch those rows.
- Every other statement.

What doesn't:

- **A silo module stops resolving on nodes older than v3.3.0 once the migration
  runs.** No arrangement of the band avoids this: the two builds parse and derive
  different names for the same package, so no single key satisfies both. Upgrade
  every node before relying on a silo module, or re-run `DEFINE MODULE` once the
  rollout is complete.
- Rolling back to a pre-v3.3.0 build after any v3.3.0+ node has started, per the
  v3.3.0 downgrade note below.

### Time-travel reads of a silo module predate the move

On a versioned backend (`?versioned=true`), a silo module *call* resolved at a
timestamp before the migration committed fails to find the module: the derived
key does not exist at those timestamps. `INFO FOR DB VERSION <t>` still lists
the module, because listing scans the band and recomputes each name from the
value it finds. Silo modules at the present timestamp are unaffected, as is
every `mod::` module at any timestamp.

The migration's cost is proportional to the number of module definitions a
database holds, so it is negligible on any ordinary store.

### A rolled-back transaction is reported as `cancelled`

`outcome` on the `surrealdb.transaction` family previously took `success` for a
transaction that committed *and* for one that rolled back, because both report
the same result to the metrics layer. The two are now distinguished: `success`
means committed, `cancelled` means rolled back, and `error` stays reserved for a
commit or cancel that itself failed.

Rollbacks are routine — read-only work and internal probes end that way — so the
shift is not small on a busy node. To keep counting every transaction that
finished, admit both values:

```
surrealdb_transaction_total{outcome=~"success|cancelled"}
```

To count only work that reached durability, which is what the label now means,
keep filtering on `outcome="success"`. See the metric catalogue in
[`OBSERVABILITY.md`](./OBSERVABILITY.md) for the full attribute set.

## v3.1.0 — rev-2 on-disk wire format for `Value`, `Array`, `Object`, `Set`

**Impact**: irreversible on-disk format rotation for every row that
contains a `Value`, `Array`, `Object`, or `Set`.

**Important**: once a v3.1.0+ binary writes a row of any of these types,
the bytes are in the new `revision(2, optimised)` format. A pre-v3.1.0
binary cannot read them. **Binary downgrade is one-way** for any
database the new build has touched, even with no other schema change.
The runtime `MajorVersion` guard
([`surrealdb/core/src/kvs/ds.rs::check_version`](../surrealdb/core/src/kvs/ds.rs))
does **not** catch this — the rotation happens inside the existing
major version, so an older binary opening a v3.1.0-written database
will see the expected `MajorVersion` and then fail at the per-row
`deserialize_revisioned` step when it encounters the unrecognised
rev-2 envelope.

What still works:

- Reading existing rev-1 rows under v3.1.0+ (forward migration is
  fully supported; the multi-revision walker handles both encodings
  transparently).
- All SurrealQL semantics — no query-level behaviour change.
- Replication and failover between v3.1.0+ nodes.

What doesn't:

- Restoring a backup taken from v3.1.0+ onto a pre-v3.1.0 binary.
- Failover from v3.1.0+ data to a pre-v3.1.0 node in a mixed cluster.
- Re-mounting a v3.1.0+ on-disk store with a pre-v3.1.0 build.

If you need the option to roll back, take and verify a logical backup
(`surreal export`) **before** running v3.1.0+ against the live data —
that backup is plain SurrealQL and can be replayed onto any binary
that supports its statements.

See the rationale block at the top of the `Value` enum in
[`surrealdb/core/src/val/mod.rs`](../surrealdb/core/src/val/mod.rs) for
the implementation-side explanation, and `doc/UPGRADING.md` will gain
a new section per future release that introduces another operational
impact of this kind.
