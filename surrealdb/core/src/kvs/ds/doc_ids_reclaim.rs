//! Finishing a table's shared doc-ID reclaim for a statement that waits on it.
//!
//! Removing a table's last doc-ID index queues its shared doc-ID space for
//! background reclaim ([`crate::kvs::Transaction::del_tb_doc_ids_deferred`]).
//! Until the reaper's first page lands, `DEFINE INDEX` simply cancels the queued
//! reclaim and adopts the space. Once a page has landed the space may be torn,
//! so the claim ([`crate::kvs::Transaction::claim_tb_doc_ids_reclaim`]) refuses
//! a new consumer until every entry has drained. On a large table that drain
//! runs for minutes after the reclaim grace, behind the reaper's per-pass
//! budget, and a `DEFINE INDEX` issued in that window used to fail outright.
//!
//! A refused statement instead raises [`DocIdsReclaimPending`], a retryable
//! transaction conflict naming the table, and the executor finishes the drain
//! itself ([`Datastore::finish_tb_doc_ids_reclaim_step`]) before it runs the
//! statement again in a fresh transaction, whose claim then finds nothing
//! queued.
//!
//! The drain here skips the reclaim grace. The grace protects a reader whose
//! snapshot predates the removal from the out-of-transaction range destroy,
//! which these kinds never take ([`crate::key::reclaim::ReclaimKind::is_doc_id`]):
//! the drain deletes page by page inside transactions, which MVCC keeps from
//! such a reader. It runs only for a set that has started — a cursor written by
//! a page, the empty cursor an entry repaired from an unreadable state carries,
//! or a sibling already retired. A set nothing has started is left to the
//! claim, which cancels it.

use std::borrow::Cow;
use std::fmt;

use anyhow::Result;
use surrealdb_kvs::TransactionType::{Read, Write};
use surrealdb_strand::TableName;
use tokio_util::sync::CancellationToken;

use super::{Datastore, ReclaimMode, ReclaimOutcome};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::reclaim::{ALL_DOC_ID_KINDS, ReclaimState};
use crate::key::schema::{ReclaimKey, ReclaimTbPrefix};
use crate::key::{KVKeyDecode, KVValue, Key, Resumable};
use crate::kvs::tasklease::TaskLeaseType;
use crate::kvs::{
	Direction, RECLAIM_BATCH_SIZE, is_conditional_write_conflict, is_retryable_transaction_conflict,
};

/// The table whose shared doc-ID space a refused `DEFINE INDEX` or
/// `REBUILD INDEX` found part-way through a reclaim.
///
/// Raised as the context of a retryable
/// [`TransactionConflict`](crate::kvs::Error::TransactionConflict), so the
/// refusal reaches a client as a conflict an SDK retries, and the executor can
/// tell which table's reclaim to finish before it runs the statement again.
#[derive(Clone, Debug)]
pub(crate) struct DocIdsReclaimPending {
	pub(crate) ns: NamespaceId,
	pub(crate) db: DatabaseId,
	pub(crate) tb: TableName,
}

impl fmt::Display for DocIdsReclaimPending {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "The shared document-ID space for table `{}` is still being reclaimed", self.tb)
	}
}

impl DocIdsReclaimPending {
	/// The retryable error a statement refused by the claim raises.
	pub(crate) fn into_error(self, statement: &str) -> anyhow::Error {
		let conflict = crate::kvs::Error::TransactionConflict(format!(
			"The shared document-ID space for table `{}` is still being reclaimed after its last \
			 doc-ID index was removed; {statement} runs once the reclaim completes",
			self.tb
		));
		anyhow::Error::new(conflict).context(self)
	}

	/// The pending reclaim `error` was raised for, if it was.
	pub(crate) fn of(error: &anyhow::Error) -> Option<&Self> {
		error.downcast_ref::<Self>()
	}
}

/// What one step of [`Datastore::finish_tb_doc_ids_reclaim_step`] left.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DocIdsReclaimStep {
	/// Nothing torn is queued for the table: a claim made now adopts the space
	/// or cancels a reclaim no page has touched.
	Claimable,
	/// The step deleted what its budget allowed, or lost a race to a concurrent
	/// page; another step continues.
	Progressed,
	/// Nothing this node can do now: another node holds the reclaim lease, or an
	/// entry's state cannot be read until the reaper repairs it. Waiting lets
	/// the reaper finish.
	Waiting,
}

impl Datastore {
	/// Advance the reclaim of table `tb`'s shared doc-ID space by at most
	/// `budget` keys, when a page of it has already landed.
	///
	/// Runs under the reclaim task's lease, so a node reclaiming elsewhere is
	/// waited for rather than raced, and through the same paged, cursor
	/// compare-and-set deletes as the reaper, so a concurrent pass of the
	/// reaper on this node, or another statement finishing the same reclaim,
	/// costs a page instead of deleting twice. Where the two collide, one page
	/// or retirement loses: as a failed compare, or, on a backend that detects
	/// write-write conflicts at commit, as a retryable conflict. Either way it
	/// applied nothing, and the step reports [`DocIdsReclaimStep::Progressed`]
	/// so the next step reads what the winner left. The loss is never the
	/// step's error: the statement waiting on it has no conflict of its own.
	///
	/// `canceller` stops the step, its lease check's retries included, so a
	/// caller can hold it to the query's cancellation and deadline.
	pub(crate) async fn finish_tb_doc_ids_reclaim_step(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		mut budget: u64,
		canceller: &CancellationToken,
	) -> Result<DocIdsReclaimStep> {
		// Every queued entry of the table's set, read the way the claim reads it.
		let mut queued = [false; ALL_DOC_ID_KINDS.len()];
		let mut entries = Vec::new();
		let txn = self.transaction(Read).await?;
		let scanned = async {
			for (slot, kind) in ALL_DOC_ID_KINDS.into_iter().enumerate() {
				let mut window = ReclaimTbPrefix {
					kind,
					ns,
					db,
					tb: Cow::Borrowed(tb),
				}
				.range()?;
				loop {
					let items = txn.scan_raw(window.clone(), RECLAIM_BATCH_SIZE, 0, None).await?;
					let Some((last, _)) = items.last() else {
						break;
					};
					let last = last.clone();
					let exhausted = items.len() < RECLAIM_BATCH_SIZE as usize;
					queued[slot] = true;
					entries.extend(items);
					if exhausted {
						break;
					}
					window = window.resume_after(&last, Direction::Forward);
				}
			}
			Ok::<_, anyhow::Error>(())
		}
		.await;
		let _ = txn.cancel().await;
		scanned?;
		let mut states = Vec::with_capacity(entries.len());
		for (key, value) in entries {
			// A state that cannot be read may hold a cursor, and is repaired only
			// by the reaper, under its grace.
			let Ok(state) = ReclaimState::kv_decode_value(&value, ()) else {
				return Ok(DocIdsReclaimStep::Waiting);
			};
			states.push((key, state));
		}
		// Started means a page has landed: a cursor survives, or a sibling has
		// been drained and retired. Anything else is left to the claim.
		let started = states.iter().any(|(_, state)| state.cursor.is_some())
			|| (queued.iter().any(|&q| q) && !queued.iter().all(|&q| q));
		if !started {
			return Ok(DocIdsReclaimStep::Claimable);
		}
		let lh = self.lease_handler(
			TaskLeaseType::ReclaimTombstones,
			self.engine_options.reclaim_interval * 2,
			Some(canceller),
		)?;
		if !lh.has_lease().await? {
			return Ok(DocIdsReclaimStep::Waiting);
		}
		for (key, state) in states {
			Self::ensure_not_cancelled(canceller)?;
			let rc = ReclaimKey::decode_key(&key)?;
			// What a page records for an entry with no stamp: its sighting, or now.
			// A started entry always carries an observation, so the reaper never
			// holds one back as untouched.
			let observed_ms = match state.observed_ms {
				0 => self
					.reclaim_sightings
					.lock()
					.get(&key)
					.copied()
					.unwrap_or_else(|| self.wall_clock_ms().max(1)),
				stamped => stamped,
			};
			let outcome = match self
				.reclaim_decoded(
					&rc,
					&state,
					observed_ms,
					ReclaimMode::Leased(&lh),
					canceller,
					&mut budget,
				)
				.await
			{
				// A page lost its commit to a concurrent page of the entry.
				Err(e) if is_retryable_transaction_conflict(&e) => {
					return Ok(DocIdsReclaimStep::Progressed);
				}
				outcome => outcome?,
			};
			match outcome {
				ReclaimOutcome::Complete => self.retire_reclaim_entry(&key).await?,
				ReclaimOutcome::Incomplete => return Ok(DocIdsReclaimStep::Progressed),
				// A concurrent page advanced or retired the entry; the next step
				// reads what it left.
				ReclaimOutcome::Cancelled => {}
				ReclaimOutcome::LeaseLost => return Ok(DocIdsReclaimStep::Waiting),
			}
			if budget == 0 {
				return Ok(DocIdsReclaimStep::Progressed);
			}
		}
		Ok(DocIdsReclaimStep::Progressed)
	}

	/// Delete the reclaim queue entry at `key` whose prefix is now empty,
	/// guarded on the value stored, as the reaper retires one. A retirement that
	/// loses to a concurrent page or retirement of the entry, by a failed
	/// compare or a retryable conflict, is left to the next step.
	async fn retire_reclaim_entry(&self, key: &[u8]) -> Result<()> {
		let txn = self.transaction(Write).await?;
		let current = match txn.get(Key::from(key), None).await {
			Ok(Some(current)) => current,
			Ok(None) => {
				let _ = txn.cancel().await;
				return Ok(());
			}
			Err(e) => {
				let _ = txn.cancel().await;
				return Err(e);
			}
		};
		if let Err(e) = txn.del_compare(Key::from(key), Some(&current)).await {
			let _ = txn.cancel().await;
			return if lost_to_a_concurrent_write(&e) {
				Ok(())
			} else {
				Err(e)
			};
		}
		match txn.commit().await {
			Ok(()) => Ok(()),
			// A concurrent page or retirement changed the entry: the next step
			// reads what it left.
			Err(e) if lost_to_a_concurrent_write(&e) => {
				let _ = txn.cancel().await;
				Ok(())
			}
			Err(e) => {
				let _ = txn.cancel().await;
				Err(e)
			}
		}
	}
}

/// Whether a write to a reclaim queue entry failed because a concurrent write
/// to it landed first: a compare that no longer holds, or a conflict detected
/// at commit.
fn lost_to_a_concurrent_write(e: &anyhow::Error) -> bool {
	is_conditional_write_conflict(e) || is_retryable_transaction_conflict(e)
}
