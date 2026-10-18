//! Fault injection for storage-layer tests.
//!
//! Some failures cannot be provoked from outside: a commit conflict at a precise
//! point in the concurrent index build, a non-retryable error in the middle of a
//! reservation release. Production code consults this registry at those points and
//! a test arms it beforehand, keyed by site and node so concurrent tests do not
//! see each other's injections.
//!
//! Behind the `test-hooks` feature, so the registry and its lookups compile out of
//! a normal build entirely. The crate above enables it as a dev-dependency, which
//! is how its tests reach the same registry the code under test reads.

// Only compiled under `test-hooks`, which nothing but a test build enables. A
// poisoned registry mutex means a test panicked while holding it, and carrying on
// would inject into a half-updated map - so panicking here is the correct
// behaviour, not an oversight. In core this module was `#[cfg(test)]` and got the
// same allowance from `.clippy.toml`.
#![allow(clippy::unwrap_used)]
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use anyhow::Result;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RetryableConflictSite {
	ConcurrentIndexInitialCleanup,
	ConcurrentIndexInitialBatch,
	ConcurrentIndexReservationRelease,
	/// A conflict on the transaction that ends a stopped build's generation
	/// in `Error`. Raised at the start of each attempt, before the build state
	/// is read, so whatever the attempt would find, it holds a builder that
	/// has stopped, and with it the outcome a statement waiting on the build
	/// has yet to hear.
	ConcurrentIndexGenerationEnd,
	/// A conflict on the transaction that publishes a build `Online`, raised
	/// after the build's last batch and before it commits. Held, it parks the
	/// build task between its writes and its arrival at the release gate.
	ConcurrentIndexOnlinePublish,
	/// A conflict on the transaction that releases a published build's
	/// ownership and stages the compaction request it owes, raised in place of
	/// its commit. A release that owes no request never reaches it.
	ConcurrentIndexOwnershipRelease,
	/// A conflict on the read of `!bs` that opens an owned build-state
	/// transition, raised in place of that read.
	ConcurrentIndexBuildStateRead,
	IndexCompactionQueueCleanup,
	FullTextCompaction,
	CountCompaction,
	HnswCompaction,
	/// A conflict on a compaction pass's DiskANN apply, raised in place of the
	/// apply because the apply commits its own transaction.
	DiskAnnCompaction,
	/// A conflict on the commit that queues a compaction pass's fresh request
	/// for its index, raised in place of that commit.
	IndexCompactionRequeue,
	/// A conflict on the commit that ends a queued async event — the one that
	/// makes the event's writes durable and removes its queue entry together.
	/// Raised in place of that commit, and leaving the transaction finished as
	/// a real commit failure does.
	AsyncEventCommit,
	/// A conflict on the commit of one batch of the migration that copies the
	/// table-scoped doc-ID mappings under their injective key.
	DocLookupIdentityMigration,
	/// A conflict on the commit of one batch of the migration that retires the
	/// type-erased forward doc-ID mappings.
	DocLookupErasedMigration,
	/// A conflict on the commit of a transaction that ran a LIVE or KILL
	/// statement: one run in a transaction of its own, or a transaction block
	/// ended by COMMIT (consulted at every such COMMIT, so a test arms it only
	/// around a block that runs LIVE or KILL). Raised in place of that commit,
	/// discarding its writes as a real lost commit does.
	LiveKillCommit,
	/// A conflict on the commit of a statement that left a blocking index
	/// build to await after it (a standalone, blocking `REBUILD INDEX`).
	/// Raised in place of that commit, once the build has started, discarding
	/// the statement's writes as a real lost commit does.
	AwaitedBuildCommit,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
// Site names follow the `RetryableConflictSite` convention of naming the
// subsystem and injection point, even when the prefixes coincide.
#[allow(clippy::enum_variant_names)]
pub enum NonRetryableErrorSite {
	ConcurrentIndexAfterReservationRegistration,
	ConcurrentIndexReservationRelease,
	ConcurrentIndexCountTailCommitted,
	ConcurrentIndexInitialBatchCommit,
	/// Simulates the commit that publishes a build `Online` applying but
	/// reporting `CommitOutcomeUnknown`: raised after that commit succeeds, so
	/// the published state is durable while the builder sees a failure.
	ConcurrentIndexOnlineCommitAppliedUnknown,
	/// Simulates an initial-scan batch commit interrupted by datastore
	/// shutdown (the `Shutdown` error the storage engines surface once
	/// graceful shutdown has begun).
	ConcurrentIndexInitialBatchShutdown,
	/// Simulates the initial scan crossing the process memory threshold
	/// (the error `Building::is_beyond_threshold` raises under memory
	/// pressure).
	ConcurrentIndexInitialBatchMemoryThreshold,
	/// Simulates the process being over its memory threshold when a context
	/// makes the deep check of whether its query should stop: raised in place
	/// of that check's memory measurement, with the error it reports.
	ContextDeepCheckMemoryThreshold,
	/// Simulates the commit that releases a published build's ownership, and
	/// stages the compaction request it owes, failing: raised in place of that
	/// commit, leaving the transaction finished and its writes discarded. A
	/// release that owes no request never reaches it.
	ConcurrentIndexOwnershipReleaseCommit,
	/// Simulates the cancel that ends an owned build-state transition whose
	/// outcome is already decided failing: raised in place of that cancel's
	/// result.
	ConcurrentIndexBuildStateCancel,
	/// Simulates a storage layer that will not serve the version bootstrap a
	/// transaction at all — what a distributed backend does while a view
	/// change is in flight. Non-retryable by classification, transient in
	/// fact, which is the combination the bootstrap has to survive.
	VersionBootstrapBegin,
	/// Simulates the version bootstrap being interrupted between committing
	/// its sentinel and stamping `!v`, which is the half-initialised state a
	/// later start has to recognise and finish.
	VersionBootstrapStamp,
	/// Simulates the commit that ends a queued async event applying but
	/// reporting `CommitOutcomeUnknown`: raised after that commit succeeds, so
	/// the event's writes and the removal of its queue entry are durable.
	AsyncEventCommitAppliedUnknown,
	/// Simulates that commit reporting `CommitOutcomeUnknown` without having
	/// applied: raised in place of it, leaving the transaction finished and its
	/// writes discarded.
	AsyncEventCommitDiscardedUnknown,
	/// Simulates the commit that queues a compaction pass's fresh request for
	/// its index failing: raised in place of that commit.
	IndexCompactionRequeue,
	/// Simulates a full-text compaction apply failing on an error no retry
	/// cures: raised in place of the commit of a plan the pass applied.
	FullTextCompaction,
	/// The HNSW counterpart of [`Self::FullTextCompaction`], raised where the
	/// retryable site of the same name is.
	HnswCompaction,
	/// The DiskANN counterpart, raised in place of the apply whose own commit
	/// would have failed.
	DiskAnnCompaction,
}

static RETRYABLE_CONFLICTS: OnceLock<Mutex<HashMap<(RetryableConflictSite, Uuid), usize>>> =
	OnceLock::new();
static NON_RETRYABLE_ERRORS: OnceLock<Mutex<HashMap<(NonRetryableErrorSite, Uuid), usize>>> =
	OnceLock::new();

fn retryable_conflicts() -> &'static Mutex<HashMap<(RetryableConflictSite, Uuid), usize>> {
	RETRYABLE_CONFLICTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn non_retryable_errors() -> &'static Mutex<HashMap<(NonRetryableErrorSite, Uuid), usize>> {
	NON_RETRYABLE_ERRORS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn inject_retryable_conflict(
	site: RetryableConflictSite,
	node_id: Uuid,
) -> RetryableConflictGuard {
	inject_retryable_conflicts(site, node_id, 1)
}

pub fn inject_retryable_conflicts(
	site: RetryableConflictSite,
	node_id: Uuid,
	count: usize,
) -> RetryableConflictGuard {
	assert!(count > 0);
	retryable_conflicts().lock().unwrap().insert((site, node_id), count);
	RetryableConflictGuard {
		site,
		node_id,
	}
}

pub fn maybe_inject_retryable_conflict(site: RetryableConflictSite, node_id: Uuid) -> Result<()> {
	let mut conflicts = retryable_conflicts().lock().unwrap();
	let Some(remaining) = conflicts.get_mut(&(site, node_id)) else {
		return Ok(());
	};
	*remaining -= 1;
	if *remaining == 0 {
		conflicts.remove(&(site, node_id));
	}
	Err(surrealdb_kvs::Error::TransactionConflict(format!("injected conflict at {site:?}")).into())
}

pub fn retryable_conflict_count(site: RetryableConflictSite, node_id: Uuid) -> usize {
	retryable_conflicts().lock().unwrap().get(&(site, node_id)).copied().unwrap_or(0)
}

/// How many injected errors at `site` on `node_id` are still to be raised.
pub fn non_retryable_error_count(site: NonRetryableErrorSite, node_id: Uuid) -> usize {
	non_retryable_errors().lock().unwrap().get(&(site, node_id)).copied().unwrap_or(0)
}

pub fn inject_non_retryable_error(
	site: NonRetryableErrorSite,
	node_id: Uuid,
) -> NonRetryableErrorGuard {
	inject_non_retryable_errors(site, node_id, 1)
}

pub fn inject_non_retryable_errors(
	site: NonRetryableErrorSite,
	node_id: Uuid,
	count: usize,
) -> NonRetryableErrorGuard {
	assert!(count > 0);
	non_retryable_errors().lock().unwrap().insert((site, node_id), count);
	NonRetryableErrorGuard {
		site,
		node_id,
	}
}

pub fn maybe_inject_non_retryable_error(site: NonRetryableErrorSite, node_id: Uuid) -> Result<()> {
	let mut errors = non_retryable_errors().lock().unwrap();
	let Some(remaining) = errors.get_mut(&(site, node_id)) else {
		return Ok(());
	};
	*remaining -= 1;
	if *remaining == 0 {
		errors.remove(&(site, node_id));
	}
	// Transient-interruption sites reproduce the exact errors the builder
	// classifies specially, so tests exercise those paths end to end.
	match site {
		// The error a batch commit surfaces once graceful shutdown has
		// begun: the pre-apply gate refuses the commit with `Shutdown`.
		NonRetryableErrorSite::ConcurrentIndexInitialBatchShutdown => {
			Err(surrealdb_kvs::Error::Shutdown.into())
		}
		// The error the builder raises when the process crosses the
		// memory threshold.
		NonRetryableErrorSite::ConcurrentIndexInitialBatchMemoryThreshold
		| NonRetryableErrorSite::ContextDeepCheckMemoryThreshold => {
			Err(crate::error::DatastoreError::QueryBeyondMemoryThreshold.into())
		}
		// The error a storage engine reports when it cannot tell whether a
		// commit applied.
		NonRetryableErrorSite::AsyncEventCommitAppliedUnknown
		| NonRetryableErrorSite::AsyncEventCommitDiscardedUnknown
		| NonRetryableErrorSite::ConcurrentIndexOnlineCommitAppliedUnknown => {
			Err(surrealdb_kvs::Error::CommitOutcomeUnknown(format!("injected at {site:?}")).into())
		}
		_ => {
			Err(surrealdb_kvs::Error::Internal(format!("injected non-retryable error at {site:?}"))
				.into())
		}
	}
}

pub struct RetryableConflictGuard {
	site: RetryableConflictSite,
	node_id: Uuid,
}

impl Drop for RetryableConflictGuard {
	fn drop(&mut self) {
		retryable_conflicts().lock().unwrap().remove(&(self.site, self.node_id));
	}
}

pub struct NonRetryableErrorGuard {
	site: NonRetryableErrorSite,
	node_id: Uuid,
}

impl Drop for NonRetryableErrorGuard {
	fn drop(&mut self) {
		non_retryable_errors().lock().unwrap().remove(&(self.site, self.node_id));
	}
}
