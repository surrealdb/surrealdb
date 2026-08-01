# Upgrading SurrealDB

This document records per-release operational notes for SurrealDB
administrators: wire-format rotations, removed features, configuration
changes, and other impacts that aren't apparent from version numbers
alone. Read this before deploying a new release onto an existing
database.

For the release **process** (how releases are cut and shipped), see
[`RELEASING.md`](./RELEASING.md). For end-user release notes, see
<https://surrealdb.com/releases>.

## v3.3.0 — datastore version stamp, startup data migrations, and the sequence keyspace move

**Impact**: the first v3.3.0+ node to start records data migrations it has
applied; a build that lacks one of them refuses to start. Sequence definitions
gain a second home, and a sequence created mid-rollout is not visible to older
nodes.

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
