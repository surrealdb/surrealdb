//! Data migrations applied to an existing datastore on startup.
//!
//! # Adding a migration
//!
//! Write an `async fn(&Datastore) -> Result<()>` and add one entry to
//! [`MIGRATIONS`]:
//!
//! ```ignore
//! Migration {
//!     id: 1,
//!     name: "move sequence definitions out of the table band",
//!     version: (3, 3, 0),
//!     run: |ds| Box::pin(sequences::move_definitions_to_sd_band(ds)),
//! },
//! ```
//!
//! `id` is a stable handle for the ledger entry at `/!mg{id}`. It is never
//! reused and never renumbered, because a datastore that has already applied
//! migration 4 will skip whatever migration 4 later becomes. `version` is the
//! first release that contains the migration.
//!
//! # What a migration must guarantee
//!
//! * **Idempotent.** The ledger entry is written in a transaction of its own, after the migration's
//!   work, so a crash in between leaves the migration eligible to run again on the next startup.
//! * **Safe to run concurrently with itself.** The task lease keeps one node running migrations in
//!   the normal case, but a lease can expire under a migration that outlives it, at which point a
//!   second node may start the same work.
//! * **Readable by the previous release.** A migration runs as soon as the first upgraded node
//!   starts, while other nodes may still be on the older build. Anything it rewrites must stay
//!   legible to them, or the release notes must say the upgrade is not rolling-safe. Migration 1 is
//!   the worked example: it copies rather than moves, and a later release deletes what it left
//!   behind once no supported upgrade path starts below it.
//!
//! # On-demand entries
//!
//! A migration with `on_demand` set is a ledger entry and no work. It is written by the code that
//! first stores the bytes it describes, in the same transaction as those bytes, and never by
//! startup: it is not a candidate for [`candidate_ids`], so it never runs, and a datastore this
//! start created does not record it in its baseline. It stays in the registry so that
//! `check_downgrade` knows the id, and so that [`describe`] can name it.
//!
//! # Ledger classes
//!
//! A ledger id carries a compatibility class in its top two bits (see
//! [`crate::kvs::version::ledger`]), so a build reads what it may do with an entry it has no
//! registry row for from the key alone:
//!
//! * an **incompatible** or reserved-class entry records bytes this build cannot read, and the
//!   build refuses the datastore;
//! * a **read-only compatible** entry records bytes this build can read but not maintain, and the
//!   build opens the datastore read-only: [`Datastore::open_read_only`] refuses every write
//!   transaction except this node's own bookkeeping, and nothing here runs a migration, advances
//!   the stamp or writes any other state;
//! * a **compatible** entry records bytes every build can ignore, and the build opens the datastore
//!   read-write.
//!
//! Every migration a 4.x build adds at startup takes the compatible class.
//! [`judge_ledger`] is the one place that reads the classes. An id this build has a registry row
//! for is never judged by its class. [`LEDGER`] lists the same rows as the registry without the
//! work, so a caller that reads the ledger and runs nothing can pass it in place of [`MIGRATIONS`].
//!
//! # Version history
//!
//! The version history (`/!vh`) records one entry per transition. The stamp only rises, so a
//! release below it that opens the datastore appends a **downward note** (`from` is the stamp,
//! `to` is the running release), once per `(from, to)` however often it restarts, and changes
//! nothing else.
//!
//! A release that later runs against a stamp at or below its own redoes what the earlier releases
//! left unmaintained, through [`RECHECKS`]. The notes recorded since the last upward entry that
//! reached the stamp are the evidence: the lowest `to` among them is the floor, every recheck with
//! `floor < since <= current` runs, and a run at the stamp's own release then appends an upward
//! entry from that floor to itself, which is what retires the notes. A run that advances the stamp
//! appends the entry that retires them.
//!
//! # Selection
//!
//! A migration runs when `version <= current`, it is not on-demand, and its `/!mg{id}` entry is
//! absent. The ledger is the whole gate: it is what makes a migration run once,
//! what lets an interrupted run resume rather than restart, and what keeps a
//! datastore created fresh off the historical migrations — such a store records
//! the entire registry as applied before it serves anything.
//!
//! There is deliberately no lower bound on the datastore's stamped version. A
//! fix authored on a release branch carries that branch's version, so a store
//! already stamped past it would skip it forever if the stamp gated selection.

mod doc_lookup_erased;
mod doc_lookup_identity;
mod field_keys;
mod kinds_as_text;
mod module_keys;
mod sequence_defs;
mod store_defaults;
mod stored_records;

use std::future::Future;
use std::pin::Pin;

use anyhow::{Result, bail};
use tracing::{info, warn};

use crate::catalog::TaskLease;
use crate::key::schema::{MigrationKey, StorageVersionKey, TaskLeaseKey, VersionHistoryKey};
use crate::kvs::ds::Datastore;
use crate::kvs::tasklease::{LeaseHandler, TaskLeaseType};
use crate::kvs::version::ledger::{
	LedgerClass, LedgerId, RECORD_FORMAT_V1, RECORD_FORMAT_V1_NAME, RECORD_FORMAT_V1_VERSION,
};
use crate::kvs::version::{MigrationRecord, StorageVersion, VersionHistoryEntry};
use crate::kvs::{DatastoreError, Release, TransactionType};

const TARGET: &str = "surrealdb::core::kvs::migration";

/// How long a migration lease is held before another node may take it over.
/// Renewed between migrations, and between the batches of one that walks user
/// data, so this bounds the time between renewals, not the whole run.
///
/// Every release that takes the lease uses this duration, and
/// [`clear_lease_of_finished_holder`] relies on that to recover when a holder
/// last wrote it.
const LEASE_DURATION: std::time::Duration = std::time::Duration::from_secs(60);

/// How long a node that lost the lease waits for the holder to finish before
/// giving up and failing startup.
const WAIT_FOR_HOLDER: std::time::Duration = std::time::Duration::from_secs(300);

/// How often a waiting node re-reads the ledger.
const WAIT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

type MigrationFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

/// One data migration, keyed in the ledger by its `id`.
pub(crate) struct Migration {
	/// Stable ledger id. Never reused, never renumbered.
	pub id: u32,
	/// Human-readable name, recorded in the ledger and in logs.
	pub name: &'static str,
	/// The first release containing this migration, as `(major, minor, patch)`.
	pub version: (u64, u64, u64),
	/// The work itself.
	pub run: for<'a> fn(&'a Datastore) -> MigrationFuture<'a>,
	/// Whether the entry is written on demand by the code that first stores what it describes,
	/// and never selected, run or baselined by startup.
	pub on_demand: bool,
}

/// Work a later release redoes after an earlier one wrote to the store, for state the earlier
/// release does not maintain.
///
/// Same contract as a migration: idempotent, and safe to run concurrently with itself, because
/// several nodes of the later release may start together. A recheck is not recorded in the ledger;
/// the version history decides when it runs (see the module docs).
pub(crate) struct Recheck {
	/// The first release whose writes maintain the state this recheck repairs. The recheck runs
	/// when an earlier release wrote to the store since the stamp last reached `since`.
	pub since: (u64, u64, u64),
	/// Human-readable name, recorded in logs.
	pub name: &'static str,
	/// The work itself.
	pub run: for<'a> fn(&'a Datastore) -> MigrationFuture<'a>,
}

/// Every recheck, in the order they run.
pub(crate) static RECHECKS: &[Recheck] = &[];

/// Every migration, in ascending sequence order, which is the order they are applied.
///
/// An id carries its class in its top bits, so ids are not ordered by sequence across classes.
///
/// Id 5 is retired and never reused. It marks a datastore written in a record envelope no release
/// reads; this build has no entry for it, so `check_downgrade` refuses such a datastore.
pub(crate) static MIGRATIONS: &[Migration] = &[
	Migration {
		id: 1,
		name: "copy sequence definitions out of the table band",
		version: (3, 3, 0),
		run: |ds| Box::pin(sequence_defs::copy_definitions_out_of_the_table_band(ds)),
		on_demand: false,
	},
	Migration {
		id: 2,
		name: "re-key module definitions to their derived name",
		version: (3, 3, 0),
		run: |ds| Box::pin(module_keys::rekey_definitions_to_their_derived_name(ds)),
		on_demand: false,
	},
	Migration {
		id: 3,
		name: "rebuild forward doc-ID mappings under an injective key",
		version: (3, 3, 0),
		run: |ds| Box::pin(doc_lookup_identity::rebuild_forward_doc_ids_under_an_injective_key(ds)),
		on_demand: false,
	},
	Migration {
		id: 4,
		name: "re-save field and function kinds stored as an AST as text",
		version: (3, 4, 0),
		run: |ds| Box::pin(kinds_as_text::resave_kinds_as_text(ds)),
		on_demand: false,
	},
	Migration {
		id: 6,
		name: "mark the store as opened by a build that reads the v1 record envelope",
		version: (3, 4, 0),
		run: |ds| Box::pin(stored_records::mark_records_stored_value_encoded(ds)),
		on_demand: false,
	},
	Migration {
		id: RECORD_FORMAT_V1.raw(),
		name: RECORD_FORMAT_V1_NAME,
		version: RECORD_FORMAT_V1_VERSION,
		run: |_| Box::pin(async { Ok(()) }),
		on_demand: true,
	},
	Migration {
		id: STORE_DEFAULTS.raw(),
		name: "record the database defaults of the release that created the datastore",
		version: (3, 4, 0),
		run: |ds| Box::pin(store_defaults::record_the_defaults_of_an_earlier_release(ds)),
		on_demand: false,
	},
	Migration {
		id: field_keys::LEDGER_ID.raw(),
		name: "re-key field definitions to their storage name",
		version: (3, 4, 0),
		run: |ds| Box::pin(field_keys::rekey_definitions_to_their_storage_name(ds)),
		on_demand: false,
	},
	Migration {
		id: doc_lookup_erased::LEDGER_ID.raw(),
		name: "retire the type-erased forward doc-ID mappings",
		version: (3, 4, 0),
		run: |ds| Box::pin(doc_lookup_erased::retire_the_type_erased_forward_doc_ids(ds)),
		on_demand: false,
	},
];

/// The ledger facts of one migration, readable without the work.
pub(crate) trait LedgerFacts {
	/// Stable ledger id.
	fn id(&self) -> u32;
	/// Human-readable name, recorded in the ledger and in logs.
	fn name(&self) -> &'static str;
	/// The first release containing the migration, as `(major, minor, patch)`.
	fn version(&self) -> (u64, u64, u64);
	/// Whether the entry is written on demand and never selected, run or baselined by startup.
	fn on_demand(&self) -> bool;
}

impl LedgerFacts for Migration {
	fn id(&self) -> u32 {
		self.id
	}

	fn name(&self) -> &'static str {
		self.name
	}

	fn version(&self) -> (u64, u64, u64) {
		self.version
	}

	fn on_demand(&self) -> bool {
		self.on_demand
	}
}

/// One entry of [`LEDGER`]: the ledger facts of a [`Migration`] without its `run`.
pub(crate) struct LedgerRow {
	/// Stable ledger id. Never reused, never renumbered.
	pub id: u32,
	/// Human-readable name, recorded in the ledger and in logs.
	pub name: &'static str,
	/// The first release containing the migration, as `(major, minor, patch)`.
	pub version: (u64, u64, u64),
	/// Whether the entry is written on demand and never selected, run or baselined by startup.
	pub on_demand: bool,
}

impl LedgerFacts for LedgerRow {
	fn id(&self) -> u32 {
		self.id
	}

	fn name(&self) -> &'static str {
		self.name
	}

	fn version(&self) -> (u64, u64, u64) {
		self.version
	}

	fn on_demand(&self) -> bool {
		self.on_demand
	}
}

/// The ledger facts of every entry of [`MIGRATIONS`], without the work. Code that reads or writes
/// the ledger without running a migration names this table, so it links no migration body. It
/// lists the same rows as `MIGRATIONS` in the same order.
pub(crate) static LEDGER: &[LedgerRow] = &[
	LedgerRow {
		id: 1,
		name: "copy sequence definitions out of the table band",
		version: (3, 3, 0),
		on_demand: false,
	},
	LedgerRow {
		id: 2,
		name: "re-key module definitions to their derived name",
		version: (3, 3, 0),
		on_demand: false,
	},
	LedgerRow {
		id: 3,
		name: "rebuild forward doc-ID mappings under an injective key",
		version: (3, 3, 0),
		on_demand: false,
	},
	LedgerRow {
		id: 4,
		name: "re-save field and function kinds stored as an AST as text",
		version: (3, 4, 0),
		on_demand: false,
	},
	LedgerRow {
		id: 6,
		name: "mark the store as opened by a build that reads the v1 record envelope",
		version: (3, 4, 0),
		on_demand: false,
	},
	LedgerRow {
		id: RECORD_FORMAT_V1.raw(),
		name: RECORD_FORMAT_V1_NAME,
		version: RECORD_FORMAT_V1_VERSION,
		on_demand: true,
	},
	LedgerRow {
		id: STORE_DEFAULTS.raw(),
		name: "record the database defaults of the release that created the datastore",
		version: (3, 4, 0),
		on_demand: false,
	},
	LedgerRow {
		id: field_keys::LEDGER_ID.raw(),
		name: "re-key field definitions to their storage name",
		version: (3, 4, 0),
		on_demand: false,
	},
	LedgerRow {
		id: doc_lookup_erased::LEDGER_ID.raw(),
		name: "retire the type-erased forward doc-ID mappings",
		version: (3, 4, 0),
		on_demand: false,
	},
];

/// Checks that a store an edge build attaches needs nothing that [`Datastore::check_version`]
/// does: no migration to run, no stamp to advance, no feature level to raise.
///
/// A store this start created is baselined and stamped as [`run`] does for a new store. Any other
/// store must already be at this release with every migration applied, or the attach fails with
/// [`DatastoreError::NotAttachable`]. Names [`LEDGER`] only, so it links no migration body.
pub(crate) async fn attach(ds: &Datastore, is_new: bool) -> Result<()> {
	let current = StorageVersion::current();
	let stored = ds.storage_version().await?;
	if (is_new || !ds.feature_cell().level().is_legacy()) && stored.is_none() {
		record_baseline(ds, LEDGER).await?;
		return advance_stamp(ds, None, &current).await;
	}

	let pending = unapplied(ds, &candidate_ids(LEDGER, &current)).await?;
	if !pending.is_empty() {
		bail!(DatastoreError::NotAttachable {
			stored: stored.as_ref().map(|s| s.to_string()).unwrap_or_else(|| "unknown".to_owned()),
			running: current.to_string(),
			reason: format!("data migrations have not run: {}", describe(LEDGER, &pending)),
		});
	}
	match &stored {
		Some(s) if s.triple() == current.triple() => Ok(()),
		_ => bail!(DatastoreError::NotAttachable {
			stored: stored.as_ref().map(|s| s.to_string()).unwrap_or_else(|| "unknown".to_owned()),
			running: current.to_string(),
			reason: "the store's version stamp is not this release".into(),
		}),
	}
}

/// The entry that records the database defaults of a datastore an earlier release created. It
/// is compatible: a release that does not know it ignores the defaults it records.
pub(crate) const STORE_DEFAULTS: LedgerId = LedgerId::new(LedgerClass::Compatible, 8);

/// Brings the datastore's version stamp up to the running build, applying any
/// data migrations the gap calls for.
///
/// Called from [`Datastore::check_version`] once the major-version gate has
/// passed, so the datastore is already known to be readable by this build.
pub(crate) async fn run(ds: &Datastore, is_new: bool) -> Result<()> {
	run_with(ds, MIGRATIONS, RECHECKS, &StorageVersion::current(), is_new).await
}

/// Whether the datastore owes this build nothing: stamped at its version, with
/// every migration the build ships in its ledger and none the build lacks.
///
/// True exactly when [`run`] would find nothing to apply and nothing to stamp.
/// Read without a lease, so a datastore another node is migrating reads as
/// unsettled until that node stamps it, which is the answer a caller deferring
/// work until the migrations have run wants.
pub(crate) async fn settled(ds: &Datastore) -> Result<bool> {
	settled_with(ds, MIGRATIONS, &StorageVersion::current()).await
}

/// [`settled`] against an explicit registry and running version.
async fn settled_with(
	ds: &Datastore,
	registry: &[Migration],
	current: &StorageVersion,
) -> Result<bool> {
	if ds.storage_version().await?.as_ref() != Some(current) {
		return Ok(false);
	}
	// A migration in the ledger that this build lacks is what `check_downgrade`
	// refuses the start over; the datastore is not this build's to run.
	let unknown = ds
		.applied_migration_ids()
		.await?
		.iter()
		.any(|(id, _)| !registry.iter().any(|m| m.id == *id));
	if unknown {
		return Ok(false);
	}
	Ok(unapplied(ds, &candidate_ids(registry, current)).await?.is_empty())
}

/// [`run`] against an explicit registry and running version, so the driver can
/// be exercised without depending on what the shipped registry happens to
/// contain or on what version the tree is currently at.
async fn run_with(
	ds: &Datastore,
	registry: &[Migration],
	rechecks: &[Recheck],
	current: &StorageVersion,
	is_new: bool,
) -> Result<()> {
	let stored = ds.storage_version().await?;

	// A datastore created at a stored feature level has nothing from before the
	// version stamp to migrate, so its whole registry is recorded as already
	// applied before anything else runs. Checking the feature level alongside
	// `is_new` covers a restart or a concurrent node that arrives after `!v` was
	// committed but before the version stamp advanced: a store that genuinely
	// predates the stamp also predates stored feature levels and stands at
	// `FeatureLevel::Legacy` until after migrations complete.
	if (is_new || !ds.feature_cell().level().is_legacy()) && stored.is_none() {
		record_baseline(ds, registry).await?;
	}

	if let Opening::ReadOnly = check_downgrade(ds, registry, stored.as_ref(), current).await? {
		return Ok(());
	}

	let pending = unapplied(ds, &candidate_ids(registry, current)).await?;
	if !pending.is_empty() {
		apply(ds, registry, &pending).await?;
	}

	// A stamp above this release means an earlier release is writing to a store a later one
	// advanced: the stamp stays, and the note tells the later release what to recheck. A stamp at
	// or below this release may owe rechecks of what such notes record.
	let owed = match stored.as_ref() {
		Some(from) if from.triple() > current.triple() => {
			return note_earlier_writer(ds, from, current).await;
		}
		Some(from) => recheck_after_earlier_writer(ds, rechecks, from, current).await?,
		None => None,
	};

	advance_stamp(ds, stored.clone(), current).await?;

	// A run that advanced the stamp has an entry of its own to retire the notes; a run at the
	// stamp's own release appends one. It sorts after the notes it retires even when the nodes
	// that wrote them read a clock ahead of this one.
	match (owed, stored) {
		(Some(owed), Some(from)) if from.triple() == current.triple() => {
			record_history(
				ds,
				&VersionHistoryEntry {
					from: Some(owed.floor),
					to: current.clone(),
					node: ds.id(),
					timestamp: ds.clock_now().value.max(owed.latest.saturating_add(1)),
				},
			)
			.await
		}
		_ => Ok(()),
	}
}

/// Records every migration in the registry as applied in a single transaction,
/// without running any.
///
/// Used only for a newly created datastore: it has never held any layout a
/// migration exists to repair, so the ledger starts full rather than empty. An
/// on-demand entry describes bytes the store does not hold yet, so it is not
/// recorded.
async fn record_baseline<M: LedgerFacts>(ds: &Datastore, registry: &[M]) -> Result<()> {
	let timestamp = ds.clock_now().value;
	let node = ds.id();
	let txn = ds.transaction(TransactionType::Write).await?;
	let result = async {
		for migration in registry.iter().filter(|m| !m.on_demand()) {
			let record =
				MigrationRecord::applied(migration.name(), migration.version(), node, timestamp);
			txn.set_key(&MigrationKey::new(migration.id()), &record).await?;
		}
		txn.commit().await
	}
	.await;
	match result {
		Ok(()) => Ok(()),
		Err(e) if lost_the_race(&e) => {
			let _ = txn.cancel().await;
			Ok(())
		}
		Err(e) => {
			let _ = txn.cancel().await;
			Err(e)
		}
	}
}

/// What the ledger allows this build to do with a datastore.
pub(crate) enum LedgerVerdict {
	/// Every entry is one this build knows or may ignore: the datastore opens read-write.
	Open,
	/// An entry records bytes this build can read but not maintain: the datastore opens
	/// read-only. `features` names those entries by id and recorded name.
	ReadOnly {
		features: String,
	},
	/// An entry records bytes this build cannot read: the datastore is refused. `migrations`
	/// names those entries by id and recorded name.
	Refused {
		migrations: String,
	},
}

/// Judges the datastore's ledger with [`judge_ledger`].
///
/// The ids come from the keys and the descriptions from the values, and a value that will not
/// decode still yields its id: a record written in a revision this build cannot read is the
/// strongest possible evidence that the datastore has moved past it, so it must not turn into a
/// decode error.
pub(crate) async fn ledger_verdict<M: LedgerFacts>(
	ds: &Datastore,
	registry: &[M],
) -> Result<LedgerVerdict> {
	let entries = ds.applied_migration_ids().await?.into_iter().map(|(id, described)| {
		let text = match described {
			Some(record) => format!("{} ({}, id {id})", record.name, record.version),
			None => format!("id {id}, unreadable record"),
		};
		(id, text)
	});
	Ok(judge_ledger(entries, registry))
}

/// Judges the ledger entries that `registry` has no row for, by class. Each entry is a ledger id
/// and the text that names it in the verdict.
///
/// The ledger is the evidence, not the version stamp: a migration that ran and was then rolled
/// away from is by definition absent from this build's registry, so comparing versions against
/// that registry could never name it. Matched on the ledger id, which is a migration's identity.
/// The recorded name is documentation and may be reworded between releases, so comparing on it
/// would read a rename as an unknown migration and refuse the start. The class is read from the
/// id, never from the value.
///
/// An incompatible or reserved-class entry makes the verdict [`LedgerVerdict::Refused`], whatever
/// else the ledger holds; failing that, a read-only compatible entry makes it
/// [`LedgerVerdict::ReadOnly`]. Entries that are compatible are logged and otherwise ignored.
pub(crate) fn judge_ledger<M: LedgerFacts>(
	entries: impl IntoIterator<Item = (u32, String)>,
	registry: &[M],
) -> LedgerVerdict {
	let mut refused = Vec::new();
	let mut read_only = Vec::new();
	let mut ignored = Vec::new();
	for (id, text) in entries {
		if registry.iter().any(|m| m.id() == id) {
			continue;
		}
		match LedgerId::from_raw(id).class() {
			LedgerClass::Incompatible | LedgerClass::Reserved => refused.push(text),
			LedgerClass::ReadOnlyCompatible => read_only.push(text),
			LedgerClass::Compatible => ignored.push(text),
		}
	}
	if !refused.is_empty() {
		return LedgerVerdict::Refused {
			migrations: refused.join(", "),
		};
	}
	if !read_only.is_empty() {
		return LedgerVerdict::ReadOnly {
			features: read_only.join(", "),
		};
	}
	if !ignored.is_empty() {
		info!(
			target: TARGET,
			entries = %ignored.join(", "),
			"The ledger holds entries this build does not know and may ignore"
		);
	}
	LedgerVerdict::Open
}

/// Whether the ledger lets this build write to the datastore.
enum Opening {
	/// The datastore opens read-write: migrations run and the stamp advances.
	ReadWrite,
	/// The datastore opens read-only: nothing is migrated, stamped or recorded.
	ReadOnly,
}

/// Refuses to start when the datastore carries a ledger entry this build cannot read, and marks
/// the datastore read-only when it carries one this build cannot maintain.
///
/// A stamp above the running version with no such entry behind it is allowed, with a warning,
/// because nothing on disk has changed shape.
async fn check_downgrade(
	ds: &Datastore,
	registry: &[Migration],
	stored: Option<&StorageVersion>,
	current: &StorageVersion,
) -> Result<Opening> {
	match ledger_verdict(ds, registry).await? {
		LedgerVerdict::Refused {
			migrations,
		} => bail!(DatastoreError::MigratedBeyondStorageVersion {
			stored: stored.map(|s| s.to_string()).unwrap_or_else(|| "unknown".to_owned()),
			running: current.to_string(),
			migrations,
		}),
		LedgerVerdict::ReadOnly {
			features,
		} => {
			ds.open_read_only(features);
			return Ok(Opening::ReadOnly);
		}
		LedgerVerdict::Open => {}
	}

	let Some(from) = stored else {
		return Ok(Opening::ReadWrite);
	};
	if from.triple() <= current.triple() {
		return Ok(Opening::ReadWrite);
	}

	warn!(
		target: TARGET,
		stored = %from,
		running = %current,
		"A newer release last advanced this datastore; this release opens it. \
		 Running a mixed set of releases is not supported."
	);
	Ok(Opening::ReadWrite)
}

/// Every migration this build ships that a datastore could still owe, in the
/// order they must be applied. An on-demand entry is never a candidate.
///
/// Bounded above by the running version and nothing else. There is deliberately
/// no lower bound on the datastore's stamp: a fix authored on a release branch
/// carries that branch's version, so a store already stamped past it would skip
/// it forever. What keeps a migration from running twice, and what keeps a
/// newly created datastore off the historical ones, is the ledger.
fn candidate_ids<M: LedgerFacts>(registry: &[M], current: &StorageVersion) -> Vec<u32> {
	registry
		.iter()
		.filter(|m| !m.on_demand() && m.version() <= current.triple())
		.map(|m| m.id())
		.collect()
}

/// Of the given migrations, those with no ledger entry yet.
///
/// Presence of the key is the whole answer, so this deliberately does not
/// decode the value: a record written by a later release, in a revision this
/// build cannot read, still means the migration ran.
async fn unapplied(ds: &Datastore, ids: &[u32]) -> Result<Vec<u32>> {
	if ids.is_empty() {
		return Ok(Vec::new());
	}
	let txn = ds.transaction(TransactionType::Read).await?;
	let mut outstanding = Vec::new();
	for id in ids {
		let applied = catch!(txn, txn.exists_key(&MigrationKey::new(*id), None).await);
		if !applied {
			outstanding.push(*id);
		}
	}
	txn.cancel().await?;
	Ok(outstanding)
}

/// Applies the pending migrations, or waits for whichever node is applying
/// them.
///
/// One node holds the lease and does the work; the others poll the ledger. A
/// waiter re-attempts the lease on every pass, so if the holder dies mid-run
/// its lease expires and a waiter picks up whatever is left rather than
/// blocking until the deadline. Holding the lease is the normal path, not a
/// guarantee: it can expire under a migration that outruns it, which is why
/// migrations must tolerate a second node repeating them.
///
/// A waiter only waits on a holder that may still be migrating. A holder that
/// has finished its pass releases the lease, and a lease left behind by a
/// holder that did not release it is cleared once that holder's version
/// history entry proves its pass is over.
async fn apply(ds: &Datastore, registry: &[Migration], pending: &[u32]) -> Result<()> {
	let lease = lease(ds)?;
	let deadline = web_time::Instant::now() + WAIT_FOR_HOLDER;

	loop {
		let outstanding = unapplied(ds, pending).await?;
		if outstanding.is_empty() {
			return Ok(());
		}

		clear_lease_of_finished_holder(ds).await?;
		if lease.has_lease().await? {
			for id in outstanding {
				let migration = registry
					.iter()
					.find(|m| m.id == id)
					.expect("pending ids are taken from the registry");
				info!(
					target: TARGET,
					id = migration.id,
					name = migration.name,
					"Applying data migration"
				);
				(migration.run)(ds).await?;
				record_applied(ds, migration).await?;
				// Renew between migrations so a long run keeps the lease. A lost
				// renewal is not fatal, so the result is deliberately ignored.
				let _ = lease.try_maintain_lease().await;
			}
			// A lease left in place would hold off a node with migrations this
			// build does not know about until it expired. A failed release only
			// delays that node until expiry, so it does not fail startup.
			if let Err(e) = release_lease(ds).await {
				warn!(target: TARGET, error = %e, "Failed to release the data migration lease");
			}
			return Ok(());
		}

		if web_time::Instant::now() >= deadline {
			bail!(DatastoreError::MigrationTimedOut {
				migrations: describe(registry, &outstanding),
			});
		}
		info!(
			target: TARGET,
			count = outstanding.len(),
			"Another node is applying data migrations; waiting for it to finish"
		);
		common::time::sleep(WAIT_POLL_INTERVAL).await;
	}
}

/// This node's handle on the migration lease.
///
/// Every handle names the same stored lease, so a migration that runs long can
/// renew it from a handle of its own between batches.
pub(super) fn lease(ds: &Datastore) -> Result<LeaseHandler> {
	ds.lease_handler(TaskLeaseType::DataMigration, LEASE_DURATION, None)
}

/// The stored migration lease, if any.
async fn read_lease(ds: &Datastore) -> Result<Option<TaskLease>> {
	let txn = ds.transaction(TransactionType::Read).await?;
	let key = TaskLeaseKey {
		task_id: TaskLeaseType::DataMigration,
	};
	let lease = catch!(txn, txn.get_key(&key, None).await);
	txn.cancel().await?;
	Ok(lease)
}

/// Deletes the migration lease if it is exactly `expected`.
///
/// The compare makes this a no-op when another node renewed or replaced the
/// lease since it was read; that lost race is reported as success, because
/// the lease then belongs to a node that is actively holding it.
async fn delete_lease_if_unchanged(ds: &Datastore, expected: &TaskLease) -> Result<()> {
	let txn = ds.transaction(TransactionType::Write).await?;
	let key = TaskLeaseKey {
		task_id: TaskLeaseType::DataMigration,
	};
	let result = async {
		txn.del_compare_key(&key, Some(expected)).await?;
		txn.commit().await
	}
	.await;
	match result {
		Ok(()) => Ok(()),
		Err(e) if lost_the_race(&e) => {
			let _ = txn.cancel().await;
			Ok(())
		}
		Err(e) => {
			let _ = txn.cancel().await;
			Err(e)
		}
	}
}

/// Gives up this node's migration lease once its pass is complete.
///
/// Only a lease this node still owns is removed; one that expired and was
/// taken over belongs to its new holder.
async fn release_lease(ds: &Datastore) -> Result<()> {
	match read_lease(ds).await? {
		Some(held) if held.owner == ds.id() => delete_lease_if_unchanged(ds, &held).await,
		_ => Ok(()),
	}
}

/// Clears a migration lease whose holder has finished its migration pass but
/// did not release the lease.
///
/// A version history entry is appended only after [`apply`] returns (a stamp
/// advance, a downward note or an entry that retires notes), so one naming the
/// holder, recorded no earlier than the holder last wrote the lease, proves
/// the holder is past its migrations and will never renew the lease again. Both readings come from
/// the holder's own clock, so the comparison is unaffected by skew between nodes. The tenure start
/// is the lease's expiry less [`LEASE_DURATION`]. Without such an entry the holder may
/// still be migrating, and the lease is left for it to finish or let expire.
async fn clear_lease_of_finished_holder(ds: &Datastore) -> Result<()> {
	let Some(held) = read_lease(ds).await? else {
		return Ok(());
	};
	if held.owner == ds.id() {
		return Ok(());
	}
	let Ok(duration) = chrono::Duration::from_std(LEASE_DURATION) else {
		return Ok(());
	};
	let tenure_start = (held.expiration - duration).timestamp_millis();
	let finished = ds
		.version_history()
		.await?
		.iter()
		.any(|entry| entry.node == held.owner && entry.timestamp as i64 >= tenure_start);
	if !finished {
		return Ok(());
	}
	info!(
		target: TARGET,
		holder = %held.owner,
		"The data migration lease is held by a node that has finished its migrations; clearing it"
	);
	delete_lease_if_unchanged(ds, &held).await
}

/// Writes a migration's ledger entry.
///
/// A lost race here is benign: another node applied the same migration and
/// recorded it, which is the state this call was trying to reach.
async fn record_applied<M: LedgerFacts>(ds: &Datastore, migration: &M) -> Result<()> {
	let record = MigrationRecord::applied(
		migration.name(),
		migration.version(),
		ds.id(),
		ds.clock_now().value,
	);
	let txn = ds.transaction(TransactionType::Write).await?;
	let key = MigrationKey::new(migration.id());
	match run!(txn, txn.set_key(&key, &record).await) {
		Err(e) if lost_the_race(&e) => Ok(()),
		other => other,
	}
}

/// Moves the version stamp up to the running build and appends the matching
/// history entry.
///
/// The stamp only ever advances, and it tracks the release triple: a rebuild
/// that changes nothing but the pre-release tag is not a transition and records
/// no history. The conditional write keeps a node that read a stale stamp from
/// overwriting a newer one on backends where a blind write would simply win; a
/// lost race means another node already recorded at least this version, so
/// there is nothing left to do.
async fn advance_stamp(
	ds: &Datastore,
	stored: Option<StorageVersion>,
	current: &StorageVersion,
) -> Result<()> {
	if stored.as_ref().is_some_and(|s| s.triple() >= current.triple()) {
		return Ok(());
	}

	let timestamp = ds.clock_now().value;
	let entry = VersionHistoryEntry {
		from: stored.clone(),
		to: current.clone(),
		node: ds.id(),
		timestamp,
	};

	let txn = ds.transaction(TransactionType::Write).await?;
	let result = async {
		txn.put_compare_key(&StorageVersionKey {}, current, stored.as_ref()).await?;
		txn.set_key(&VersionHistoryKey::new(timestamp, ds.id()), &entry).await?;
		// The commit is inside the guarded block because a backend that
		// validates the condition at commit rather than at write time reports
		// the lost race from here, and an optimistic backend reports a plain
		// write conflict. Both mean the same thing: another node got there
		// first, which is not this node's problem to fail over.
		txn.commit().await
	}
	.await;

	match result {
		Ok(()) => {
			if let Some(from) = &entry.from {
				info!(target: TARGET, %from, to = %current, "Datastore version advanced");
			} else {
				info!(target: TARGET, version = %current, "Datastore version recorded");
			}
			Ok(())
		}
		Err(e) if lost_the_race(&e) => {
			// Another node stamped the datastore between our read and this
			// write. It recorded at least this version, so we are done.
			let _ = txn.cancel().await;
			Ok(())
		}
		Err(e) => {
			let _ = txn.cancel().await;
			Err(e)
		}
	}
}

/// Whether `entry` is a downward note: a release below the stamp opened the datastore.
///
/// Upward entries, which advance the stamp or retire notes, and the entry that first stamps a
/// datastore, which has no `from`, are not.
pub(crate) fn is_downward_note(entry: &VersionHistoryEntry) -> bool {
	entry.from.as_ref().is_some_and(|from| from.triple() > entry.to.triple())
}

/// Records that `current`, a release below the stamp `stored`, opened the datastore.
///
/// Deduplicated against the latest history entry, so a release that restarts adds one note, not one
/// per start. Two nodes of that release starting together may each append one; notes are
/// idempotent evidence, so that is harmless. The stamp is not touched.
async fn note_earlier_writer(
	ds: &Datastore,
	stored: &StorageVersion,
	current: &StorageVersion,
) -> Result<()> {
	let history = ds.version_history().await?;
	let already_noted = history.last().is_some_and(|last| {
		last.from.as_ref().map(StorageVersion::triple) == Some(stored.triple())
			&& last.to.triple() == current.triple()
	});
	if already_noted {
		return Ok(());
	}
	record_history(
		ds,
		&VersionHistoryEntry {
			from: Some(stored.clone()),
			to: current.clone(),
			node: ds.id(),
			timestamp: ds.clock_now().value,
		},
	)
	.await?;
	info!(
		target: TARGET,
		stored = %stored,
		running = %current,
		"Recorded that a release below the datastore's version opened it"
	);
	Ok(())
}

/// What an earlier writer left for a later release to recheck.
struct Owed {
	/// The version of the lowest release that wrote since the stamp was last reached.
	floor: StorageVersion,
	/// The timestamp of the newest history entry.
	latest: u64,
}

/// The lowest release among the downward notes recorded since the last upward entry that reached
/// `stored`, when `current` is at or above `stored`.
///
/// The notes before that entry were retired by it. A history with no such entry (a stamp that
/// predates the history) leaves every note outstanding. A note whose release is not below `current`
/// owes nothing.
fn recheck_floor(
	history: &[VersionHistoryEntry],
	stored: &StorageVersion,
	current: &StorageVersion,
) -> Option<StorageVersion> {
	let from = history
		.iter()
		.rposition(|entry| !is_downward_note(entry) && entry.to.triple() >= stored.triple())
		.map_or(0, |reached| reached + 1);
	history[from..]
		.iter()
		.filter(|entry| is_downward_note(entry) && entry.to.triple() < current.triple())
		.map(|entry| &entry.to)
		.min_by_key(|to| to.triple())
		.cloned()
}

/// Runs the rechecks the version history says an earlier release has made necessary, and returns
/// what they covered.
///
/// `stored` is at or below `current`. Every recheck with `floor < since <= current` runs, in
/// registry order, where `floor` is [`recheck_floor`]. A failing recheck fails the run before the
/// stamp advances or the notes are retired, so the next start runs it again.
async fn recheck_after_earlier_writer(
	ds: &Datastore,
	rechecks: &[Recheck],
	stored: &StorageVersion,
	current: &StorageVersion,
) -> Result<Option<Owed>> {
	let history = ds.version_history().await?;
	let Some(floor) = recheck_floor(&history, stored, current) else {
		return Ok(None);
	};
	for recheck in rechecks {
		if recheck.since <= floor.triple() || recheck.since > current.triple() {
			continue;
		}
		info!(
			target: TARGET,
			name = recheck.name,
			floor = %floor,
			"Rechecking state a release below the datastore's version does not maintain"
		);
		(recheck.run)(ds).await?;
	}
	let latest = history.last().map_or(0, |entry| entry.timestamp);
	Ok(Some(Owed {
		floor,
		latest,
	}))
}

/// Appends one entry to the version history.
///
/// A lost race is benign: the entry is keyed by its timestamp and node, and the state it
/// records is already there.
async fn record_history(ds: &Datastore, entry: &VersionHistoryEntry) -> Result<()> {
	let txn = ds.transaction(TransactionType::Write).await?;
	let key = VersionHistoryKey::new(entry.timestamp, entry.node);
	match run!(txn, txn.set_key(&key, entry).await) {
		Err(e) if lost_the_race(&e) => Ok(()),
		other => other,
	}
}

/// The release a migration's `version` belongs to, for comparison with a feature level.
///
/// The 3.4 development line is the 4.0 release, and an older version names the release of its major
/// and minor. A number beyond what a release can name saturates.
pub(crate) fn release_of(version: (u64, u64, u64)) -> Release {
	let (major, minor, _) = version;
	if (major, minor) == (3, 4) {
		return Release::FIRST_STORED;
	}
	let narrow = |n: u64| u16::try_from(n).unwrap_or(u16::MAX);
	Release::new(narrow(major), narrow(minor))
}

/// Names the given migrations for an operator-facing message.
fn describe<M: LedgerFacts>(registry: &[M], ids: &[u32]) -> String {
	ids.iter()
		.map(|id| match registry.iter().find(|m| m.id() == *id) {
			Some(m) => format!("{} ({})", m.name(), id),
			None => id.to_string(),
		})
		.collect::<Vec<_>>()
		.join(", ")
}

/// Whether a create-only write failed because the key was already there.
pub(super) fn already_exists(err: &anyhow::Error) -> bool {
	matches!(
		err.downcast_ref::<crate::kvs::Error>(),
		Some(crate::kvs::Error::TransactionKeyAlreadyExists)
	) || matches!(
		err.downcast_ref::<crate::err::Error>(),
		Some(crate::err::Error::Kvs(crate::kvs::Error::TransactionKeyAlreadyExists))
	)
}

/// Whether the error says another node stamped the datastore first.
///
/// Two shapes mean that: the conditional write's precondition failed, and a
/// plain write conflict from an optimistic backend where both writers passed
/// their own snapshot check and only one commit can win.
fn lost_the_race(err: &anyhow::Error) -> bool {
	crate::kvs::is_conditional_write_conflict(err)
		|| crate::kvs::is_retryable_transaction_conflict(err)
}

#[cfg(test)]
mod tests;
