//! Bounded, committed deletion of a key range.
//!
//! A range delete whose cardinality follows user data — a reclaimed prefix, a
//! changefeed retention backlog — cannot be a single transaction: the write
//! batch, and so the memory the job holds, would grow with the data. Such a
//! delete is instead paged here, in transactions of at most one page each, every
//! one committed before the next page is scanned. The job's memory is then a
//! function of the page size alone.
//!
//! Pages are scanned inside the transaction that deletes them, so the page and
//! its deletions cannot disagree, and every deleted key is charged to that
//! transaction's write set — which is what makes the write-cardinality guard and
//! the transaction metrics see the work at all. A whole-range `delr` reports
//! neither.
//!
//! A pass carries a key budget so that one large range cannot spend the whole
//! pass and starve the ranges behind it. Running out of budget, or losing the
//! task lease, ends the delete with every page so far committed, which is what
//! lets the next pass continue rather than restart.

use anyhow::Result;
use surrealdb_kvs::TransactionType::{Read, Write};
use tokio_util::sync::CancellationToken;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::reclaim::ReclaimState;
use crate::key::schema::ReclaimKey;
use crate::key::{AnyRange, Key, Resumable};
use crate::kvs::tasklease::LeaseHandler;
use crate::kvs::{Datastore, Direction};

/// How a bounded range delete ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PagedOutcome {
	/// The range is empty. The page that emptied it reports this, so a caller
	/// that holds a claim on the range can retire it without another pass.
	Complete,
	/// Keys remain because the budget ran out. Every page deleted so far is
	/// committed.
	Incomplete,
	/// Keys remain because the task lease passed to another node. Every page
	/// deleted so far is committed, and the caller must stop rather than move on
	/// to its next range: a lease check is throttled to one datastore read per
	/// maintenance period, so this answer consumed the read the caller's own
	/// re-check depends on and that re-check would report the lease held without
	/// asking.
	LeaseLost,
	/// The claim authorising the delete was withdrawn while it ran, so nothing
	/// further may be deleted from the range.
	Cancelled,
}

/// What rides in each page's transaction besides the page's own deletions.
pub(crate) enum PageCompanion<'a> {
	/// Nothing. The committed deletions are the only record of progress, so a
	/// later pass resumes by scanning the range from its head and landing on the
	/// first key that survives. Correct only where the range's lower bound is
	/// fixed — everything below it has already been deleted, so rescanning
	/// re-reads no live key.
	None,
	/// A reclaim queue entry. Its presence is the claim that the range is
	/// orphaned, and its cursor advances with every page so a later pass resumes
	/// at the last durable deletion instead of rescanning the prefix.
	ReclaimEntry {
		rc: &'a ReclaimKey<'a>,
		/// The exact state this page is allowed to advance. Updating it after
		/// every commit turns the cursor write into a compare-and-set, so a
		/// concurrent cancellation or page wins instead of being overwritten.
		state: ReclaimState,
		/// When the entry was first observed: what a cursor write records while
		/// `state` carries no stamp of its own.
		observed_ms: u64,
	},
	/// A changefeed retention fence. The window's upper bound is only the keys
	/// the database's retention has made stale, and that policy is catalog
	/// state a concurrent `DEFINE DATABASE`, `DEFINE TABLE` or `ALTER TABLE`
	/// can extend. Every such write bumps the database's fence, so the fence is
	/// armed inside every page transaction and compared with the value the
	/// sweep discovered the retention under, and the delete stops if it has
	/// moved — a deleted changefeed entry cannot be recovered.
	ChangefeedRetention {
		ns: NamespaceId,
		db: DatabaseId,
		/// The names the database is defined under, resolved again by every
		/// page: a database removed, or defined again under them, is reclaimed
		/// rather than collected.
		ns_name: &'a str,
		db_name: &'a str,
		/// The fence as the sweep read it when it discovered the retention.
		fence: Option<u64>,
	},
}

/// One bounded range delete.
pub(crate) struct PagedDelete<'a, R> {
	/// The keys to delete. A caller resuming from a durable cursor passes a
	/// window already advanced past it.
	pub(crate) window: R,
	pub(crate) companion: PageCompanion<'a>,
	/// Whether each key is cleared of every MVCC version rather than
	/// soft-deleted.
	pub(crate) expunge: bool,
	/// Maximum keys deleted per committed page.
	pub(crate) page: u32,
}

impl<R: AnyRange + Resumable + Clone> PagedDelete<'_, R> {
	/// Delete the window in committed pages, spending at most `budget` keys.
	///
	/// `budget` is decremented by what was deleted, so a caller sharing one
	/// pass across several ranges can hand the remainder to the next.
	///
	/// `lease` is the task lease checked before every page; `None` runs every
	/// page unleased, for a caller whose host runs one pass at a time and whose
	/// companion's compare-and-set keeps concurrent deletes apart.
	pub(crate) async fn run(
		mut self,
		ds: &Datastore,
		lease: Option<&LeaseHandler>,
		canceller: &CancellationToken,
		budget: &mut u64,
	) -> Result<PagedOutcome> {
		loop {
			Datastore::ensure_not_cancelled(canceller)?;
			if *budget == 0 {
				// Whether the range is finished is a question about the range,
				// not about the budget: the page that spent the last of it can
				// also be the page that emptied the range, and a full page gives
				// no sign either way. One single-key read settles it, so a
				// drained range is reported finished now instead of holding a
				// claim that names nothing until a later pass looks. Reached at
				// most once per range, and only where the last page landed
				// exactly on the budget — a short page has already answered it
				// below.
				let txn = ds.transaction(Read).await?;
				let rest = txn.keys_raw(self.window.clone(), 1, 0, None).await;
				let _ = txn.cancel().await;
				return Ok(match rest?.is_empty() {
					true => PagedOutcome::Complete,
					false => PagedOutcome::Incomplete,
				});
			}
			// A delete can span many passes, so handing the range to the next
			// lease holder costs nothing: every page so far is committed and the
			// next holder resumes from what survives.
			if let Some(lh) = lease
				&& !lh.try_maintain_lease().await?
			{
				return Ok(PagedOutcome::LeaseLost);
			}
			// At least one key however small the page: a zero limit scans
			// nothing, and an empty page is how this loop reports a drained
			// range, which would retire a claim over data still present.
			let limit = (*budget).min(self.page.max(1) as u64) as u32;
			let txn = ds.transaction(Write).await?;
			if let PageCompanion::ChangefeedRetention {
				ns,
				db,
				ns_name,
				db_name,
				fence,
			} = &self.companion
			{
				// The fence of the retention that made this window stale is
				// armed inside every page transaction, so a policy extended
				// before this read stops the delete here rather than deleting
				// entries the new policy keeps, and one committed between this
				// read and the commit below rejects the page on a
				// conflict-serializing backend; on a last-writer-wins backend
				// the window is only as fresh as this read.
				let covered = catch!(
					txn,
					crate::cf::gc::retention_still_reaches(
						&txn, ns_name, db_name, *ns, *db, *fence
					)
					.await
				);
				if !covered {
					let _ = txn.cancel().await;
					return Ok(PagedOutcome::Cancelled);
				}
			}
			// One bounded page, scanned inside the transaction that deletes it
			// so the page and its deletions cannot disagree. Keys only: a delete
			// needs no value.
			let keys = catch!(txn, txn.keys_raw(self.window.clone(), limit, 0, None).await);
			// A page the backend could not fill is the end of the range, so the
			// page that empties a range also reports it. Waiting for a following
			// empty page instead would spend a whole transaction and scan per
			// range to learn nothing, and — where that page is also the one that
			// spends the budget — would report a range as unfinished after
			// emptying it, keeping a claim naming nothing for another tick.
			let exhausted = keys.len() < limit as usize;
			let Some(last) = keys.last().cloned() else {
				let _ = txn.cancel().await;
				return Ok(PagedOutcome::Complete);
			};
			// Every window is a reclaimed prefix or an expired changefeed
			// range, neither of which holds a live definition.
			for key in &keys {
				catch!(txn, txn.del_reclaimed(Key::from(key), self.expunge).await);
			}
			let advanced = if let PageCompanion::ReclaimEntry {
				rc,
				state,
				observed_ms,
			} = &self.companion
			{
				let advanced = ReclaimState {
					observed_ms: if state.observed_ms == 0 {
						*observed_ms
					} else {
						state.observed_ms
					},
					cursor: Some(last.clone()),
				};
				// The queue entry is the claim authorising these deletes. Advance
				// only the exact state this page started from: unlike a blind set,
				// this neither resurrects a cancelled claim nor overwrites a cursor
				// committed by another node on a last-writer-wins backend.
				match txn.put_compare_key(*rc, &advanced, Some(state)).await {
					Ok(()) => Some(advanced),
					Err(e) if super::is_conditional_write_conflict(&e) => {
						let _ = txn.cancel().await;
						return Ok(PagedOutcome::Cancelled);
					}
					Err(e) => {
						let _ = txn.cancel().await;
						return Err(e);
					}
				}
			} else {
				None
			};
			#[cfg(test)]
			if advanced.is_some()
				&& let Err(e) = crate::kvs::testing::maybe_inject_retryable_conflict(
					crate::kvs::testing::RetryableConflictSite::ReclaimPageCommit,
					ds.id(),
				) {
				let _ = txn.cancel().await;
				return Err(e);
			}
			match txn.commit().await {
				Ok(()) => {}
				Err(e) if super::is_conditional_write_conflict(&e) => {
					let _ = txn.cancel().await;
					return Ok(PagedOutcome::Cancelled);
				}
				Err(e) => {
					let _ = txn.cancel().await;
					return Err(e);
				}
			}
			if let (
				Some(advanced),
				PageCompanion::ReclaimEntry {
					state,
					..
				},
			) = (advanced, &mut self.companion)
			{
				*state = advanced;
			}
			*budget = budget.saturating_sub(keys.len() as u64);
			if exhausted {
				return Ok(PagedOutcome::Complete);
			}
			self.window = self.window.resume_after(&last, Direction::Forward);
			yield_now!();
		}
	}
}

/// Deletes the range of each of `targets` in committed pages, sharing `budget`
/// across them, and reports whether the lease is still held.
///
/// Every target is granted a share of what the budget has left, across the
/// targets still to spend from it, so each makes progress however many there
/// are. A share is floored at one key rather than one page: the page size caps
/// a transaction rather than setting a minimum, and flooring at a page would
/// let the leading `budget / page` targets spend the whole budget and leave
/// the rest unvisited.
///
/// What a target does not spend of its share stays in the budget, and the
/// targets that spent the whole of theirs with keys still in range are visited
/// again with what is left, round after round. A pass therefore ends with keys
/// in range only once its budget is spent, so a caller reads an unspent budget
/// as every range emptied or withdrawn. A target is revisited only after
/// spending at least one key, so the rounds end.
///
/// `budget` is decremented by what was deleted. A lost lease ends the pass at
/// once and is reported as `false`: the delete's own outcome is the only fresh
/// answer to it (see [`PagedOutcome::LeaseLost`]).
pub(crate) async fn delete_sharing_budget<T, R>(
	ds: &Datastore,
	lh: &LeaseHandler,
	canceller: &CancellationToken,
	budget: &mut u64,
	targets: Vec<T>,
	delete: impl for<'t> Fn(&'t T) -> PagedDelete<'t, R>,
) -> Result<bool>
where
	R: AnyRange + Resumable + Clone,
{
	let mut pending = targets;
	while !pending.is_empty() && *budget > 0 {
		let mut remaining = pending.len() as u64;
		let mut unfinished = Vec::new();
		for target in pending {
			Datastore::ensure_not_cancelled(canceller)?;
			// This target's slice of what the budget has left, across the
			// targets still to spend from it, capped at the budget so the tail
			// of a round hands out only what remains.
			let share = (*budget / remaining.max(1)).max(1).min(*budget);
			remaining = remaining.saturating_sub(1);
			let mut left = share;
			let outcome = delete(&target).run(ds, Some(lh), canceller, &mut left).await?;
			// Charge what the target deleted, not what it was offered.
			*budget = budget.saturating_sub(share.saturating_sub(left));
			match outcome {
				PagedOutcome::LeaseLost => return Ok(false),
				PagedOutcome::Incomplete => unfinished.push(target),
				PagedOutcome::Complete | PagedOutcome::Cancelled => {}
			}
			if *budget == 0 {
				return Ok(true);
			}
			yield_now!();
		}
		pending = unfinished;
	}
	Ok(true)
}
