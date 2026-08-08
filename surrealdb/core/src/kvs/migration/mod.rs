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
//! # Selection
//!
//! A migration runs when `version <= current` and its `/!mg{id}` entry is
//! absent. The ledger is the whole gate: it is what makes a migration run once,
//! what lets an interrupted run resume rather than restart, and what keeps a
//! datastore created fresh off the historical migrations — such a store records
//! the entire registry as applied before it serves anything.
//!
//! There is deliberately no lower bound on the datastore's stamped version. A
//! fix authored on a release branch carries that branch's version, so a store
//! already stamped past it would skip it forever if the stamp gated selection.

mod sequence_defs;

use std::future::Future;
use std::pin::Pin;

use anyhow::{Result, bail};
use tracing::{info, warn};

use crate::key::schema::{MigrationKey, StorageVersionKey, VersionHistoryKey};
use crate::kvs::ds::Datastore;
use crate::kvs::tasklease::{LeaseHandler, TaskLeaseType};
use crate::kvs::version::{MigrationRecord, StorageVersion, VersionHistoryEntry};
use crate::kvs::{DatastoreError, TransactionType};

const TARGET: &str = "surrealdb::core::kvs::migration";

/// How long a migration lease is held before another node may take it over.
/// Renewed between migrations, so this bounds one migration's runtime, not the
/// whole run.
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
}

/// Every migration, in ascending `id` order.
pub(crate) static MIGRATIONS: &[Migration] = &[Migration {
	id: 1,
	name: "copy sequence definitions out of the table band",
	version: (3, 3, 0),
	run: |ds| Box::pin(sequence_defs::copy_definitions_out_of_the_table_band(ds)),
}];

/// Brings the datastore's version stamp up to the running build, applying any
/// data migrations the gap calls for.
///
/// Called from [`Datastore::check_version`] once the major-version gate has
/// passed, so the datastore is already known to be readable by this build.
pub(crate) async fn run(ds: &Datastore, is_new: bool) -> Result<()> {
	run_with(ds, MIGRATIONS, &StorageVersion::current(), is_new).await
}

/// [`run`] against an explicit registry and running version, so the driver can
/// be exercised without depending on what the shipped registry happens to
/// contain or on what version the tree is currently at.
async fn run_with(
	ds: &Datastore,
	registry: &[Migration],
	current: &StorageVersion,
	is_new: bool,
) -> Result<()> {
	let stored = ds.storage_version().await?;

	// A datastore this start created has nothing to migrate, so its whole
	// registry is recorded as already applied before anything else runs. That
	// baseline is durable, unlike `is_new` itself, which is true only on the
	// start that wrote `!v`: without it, a crash between creating the datastore
	// and stamping it would leave a brand-new store looking like an upgrade from
	// before the stamp existed.
	if is_new && stored.is_none() {
		record_baseline(ds, registry).await?;
	}

	check_downgrade(ds, registry, stored.as_ref(), current).await?;

	let pending = unapplied(ds, &candidate_ids(registry, current)).await?;
	if !pending.is_empty() {
		apply(ds, registry, &pending).await?;
	}

	advance_stamp(ds, stored, current).await
}

/// Records every migration in the registry as applied, without running any.
///
/// Used only for a datastore this start created: it has never held any layout a
/// migration exists to repair, so the ledger starts full rather than empty.
async fn record_baseline(ds: &Datastore, registry: &[Migration]) -> Result<()> {
	for migration in registry {
		record_applied(ds, migration).await?;
	}
	Ok(())
}

/// Refuses to start when the datastore carries a migration this build has no
/// code for.
///
/// The ledger is the evidence, not the version stamp: a migration that ran and
/// was then rolled away from is by definition absent from this build's registry,
/// so comparing versions against that registry could never name it. A stamp
/// above the running version with no unknown migration behind it is allowed,
/// with a warning, because nothing on disk has changed shape.
async fn check_downgrade(
	ds: &Datastore,
	registry: &[Migration],
	stored: Option<&StorageVersion>,
	current: &StorageVersion,
) -> Result<()> {
	// Matched on the ledger id, which is a migration's identity. The recorded
	// name is documentation and may be reworded between releases, so comparing
	// on it would read a rename as an unknown migration and refuse the start.
	//
	// The ids come from the keys and the descriptions from the values, and a
	// value that will not decode still yields its id: a record written in a
	// revision this build cannot read is the strongest possible evidence that
	// the datastore has moved past it, so it must not turn into a decode error.
	let unknown: Vec<String> = ds
		.applied_migration_ids()
		.await?
		.into_iter()
		.filter(|(id, _)| !registry.iter().any(|m| m.id == *id))
		.map(|(id, described)| match described {
			Some(record) => format!("{} ({}, id {id})", record.name, record.version),
			None => format!("id {id}, unreadable record"),
		})
		.collect();

	if !unknown.is_empty() {
		bail!(DatastoreError::MigratedBeyondStorageVersion {
			stored: stored.map(|s| s.to_string()).unwrap_or_else(|| "unknown".to_owned()),
			running: current.to_string(),
			migrations: unknown.join(", "),
		});
	}

	let Some(from) = stored else {
		return Ok(());
	};
	if from.triple() <= current.triple() {
		return Ok(());
	}

	warn!(
		target: TARGET,
		stored = %from,
		running = %current,
		"This datastore was last written by a newer version of SurrealDB. \
		 No data migration separates the two, so startup continues, but \
		 running a mixed set of versions is not supported."
	);
	Ok(())
}

/// Every migration this build ships that a datastore could still owe, in the
/// order they must be applied.
///
/// Bounded above by the running version and nothing else. There is deliberately
/// no lower bound on the datastore's stamp: a fix authored on a release branch
/// carries that branch's version, so a store already stamped past it would skip
/// it forever. What keeps a migration from running twice, and what keeps a
/// newly created datastore off the historical ones, is the ledger.
fn candidate_ids(registry: &[Migration], current: &StorageVersion) -> Vec<u32> {
	registry.iter().filter(|m| m.version <= current.triple()).map(|m| m.id).collect()
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
async fn apply(ds: &Datastore, registry: &[Migration], pending: &[u32]) -> Result<()> {
	let lease = LeaseHandler::new(
		ds.sequences().clone(),
		ds.id(),
		ds.transaction_factory().clone(),
		TaskLeaseType::DataMigration,
		LEASE_DURATION,
	)?;
	let deadline = web_time::Instant::now() + WAIT_FOR_HOLDER;

	loop {
		let outstanding = unapplied(ds, pending).await?;
		if outstanding.is_empty() {
			return Ok(());
		}

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

/// Writes a migration's ledger entry.
///
/// A lost race here is benign: another node applied the same migration and
/// recorded it, which is the state this call was trying to reach.
async fn record_applied(ds: &Datastore, migration: &Migration) -> Result<()> {
	let record = MigrationRecord {
		name: migration.name.to_owned(),
		version: StorageVersion::from_triple(migration.version),
		node: ds.id(),
		timestamp: ds.clock_now().value,
	};
	let txn = ds.transaction(TransactionType::Write).await?;
	let key = MigrationKey::new(migration.id);
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

/// Names the given migrations for an operator-facing message.
fn describe(registry: &[Migration], ids: &[u32]) -> String {
	ids.iter()
		.map(|id| match registry.iter().find(|m| m.id == *id) {
			Some(m) => format!("{} ({})", m.name, id),
			None => id.to_string(),
		})
		.collect::<Vec<_>>()
		.join(", ")
}

/// Whether a create-only write failed because the key was already there.
fn already_exists(err: &anyhow::Error) -> bool {
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
