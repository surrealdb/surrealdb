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
	IndexCompactionQueueCleanup,
	FullTextCompaction,
	CountCompaction,
	HnswCompaction,
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
	/// Simulates an initial-scan batch commit interrupted by datastore
	/// shutdown (the `Shutdown` error the storage engines surface once
	/// graceful shutdown has begun).
	ConcurrentIndexInitialBatchShutdown,
	/// Simulates the initial scan crossing the process memory threshold
	/// (the error `Building::is_beyond_threshold` raises under memory
	/// pressure).
	ConcurrentIndexInitialBatchMemoryThreshold,
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
		NonRetryableErrorSite::ConcurrentIndexInitialBatchMemoryThreshold => {
			Err(crate::error::DatastoreError::QueryBeyondMemoryThreshold.into())
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
