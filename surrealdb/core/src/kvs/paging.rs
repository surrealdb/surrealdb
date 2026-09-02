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
	},
	/// A changefeed retention watermark. The window's upper bound is only the
	/// keys the database's retention has made stale, and that policy is catalog
	/// state a concurrent `DEFINE DATABASE` or `DEFINE TABLE` can extend. The
	/// watermark is therefore recomputed inside every page transaction, and the
	/// delete stops if the policy now reaches below the bound the window was
	/// built from — a deleted changefeed entry cannot be recovered.
	ChangefeedRetention {
		ns: NamespaceId,
		db: DatabaseId,
		/// The encoded watermark this window's upper bound was built from.
		watermark: &'a [u8],
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
	pub(crate) async fn run(
		mut self,
		ds: &Datastore,
		lh: &LeaseHandler,
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
			if !lh.try_maintain_lease().await? {
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
				watermark,
			} = &self.companion
			{
				// The retention that made this window stale is read inside every
				// page transaction, so a policy extended before this read stops
				// the delete here rather than deleting entries the new policy
				// keeps. Reading the catalog here is also what arms the
				// write-conflict check on a conflict-serializing backend, for a
				// policy committed between this read and the commit below; on a
				// last-writer-wins backend the window is only as fresh as this
				// read.
				let covered = catch!(
					txn,
					crate::cf::gc::retention_still_reaches(&txn, *ns, *db, watermark).await
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
			for key in &keys {
				if self.expunge {
					catch!(txn, txn.clr(Key::from(key)).await);
				} else {
					catch!(txn, txn.del(Key::from(key)).await);
				}
			}
			let advanced = if let PageCompanion::ReclaimEntry {
				rc,
				state,
			} = &self.companion
			{
				let advanced = ReclaimState {
					observed_ms: state.observed_ms,
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
