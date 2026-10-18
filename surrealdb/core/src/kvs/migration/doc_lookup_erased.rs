//! Retires the type-erased forward doc-ID band `!di`.
//!
//! A table's shared doc-ID space maps a record to its doc-ID under `!dj`, keyed
//! by the record's identity, and back under `!dd`. `!di` spells the forward
//! direction under `IndexFormat`, which drops a number's kind, so ids that
//! differ only there share one key. This release reads and writes `!dj` and
//! `!dd` alone, and adopts a `!dj` hit without reading `!dd` back. What that
//! needs of the stored space, this migration establishes, table by table:
//!
//! 1. **Reconcile.** Every record `!dd` maps has a `!dj` naming a doc-ID whose `!dd` names that
//!    record. A `!dj` that is absent, or names a doc-ID whose `!dd` names another record or none,
//!    is overwritten with the doc-ID `!dd` maps the record to. One that `!dd` backs is kept, even
//!    where a second `!dd` names the same record: it is the doc-ID every release resolves the
//!    record to, so it is the one the record's postings reference.
//! 2. **Prune.** Every `!dj` whose doc-ID `!dd` does not map back to its record is deleted.
//! 3. **Delete `!di`.**
//!
//! # Where an unbacked `!dj` comes from
//!
//! A 3.3 pre-release resolves doc-IDs through `!di` alone and purges a record by
//! deleting `!di` and `!dd`, so a `!dj` copied for that record outlives it. The
//! copy migration 3 makes is create-only, so it never replaces such an entry.
//! 3.3.0 and later write and delete `!dj` together with `!dd`, as this release
//! does: once this migration has run, no supported writer leaves one.
//!
//! # Rolling upgrade
//!
//! A 3.3.x node still running resolves through `!dj` first, verified against
//! `!dd`, so it resolves every record this release maps, and it writes and
//! deletes `!dj` together with `!dd`. It also keeps writing `!di`, which this
//! release never reads; those rows go with the table's doc-ID space when its
//! last doc-ID consumer is dropped. A 3.3.x node that restarts refuses the
//! store, as it refuses any migration it does not know. A 3.3 pre-release node
//! resolves through `!di` alone and must not run alongside this release.
//!
//! # All or nothing
//!
//! Readers on this release rely on the reconcile and the prune having finished,
//! so nothing here is skipped. A batch that keeps losing its commit fails the
//! migration, which leaves it unrecorded and runs it again on the next start.
//! A forward entry whose key does not decode is deleted, since no reader can
//! derive that key; a reverse entry that does not decode backs nothing.
//!
//! # Re-entrancy
//!
//! Every pass works in bounded batches, each in its own transaction, re-reading
//! inside it what it decides on, and writing conditionally on what it read. A
//! re-run after an interruption converges, as do two nodes running it at once,
//! and a concurrent 3.3.x writer's mapping is never clobbered: the conditional
//! write turns that race into a conflict, and the retried batch sees the
//! writer's mapping.
//!
//! # Cost
//!
//! Three passes over each table's doc-ID space, at startup, inside the version
//! check, so they count against the server's startup operation timeout. The
//! migration lease is renewed between batches.

use std::borrow::Cow;
use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use tracing::{debug, trace, warn};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::schema::{
	DocKeyKey, DocLookupIdentityKey, DocLookupIdentityPrefix, DocLookupPrefix,
};
use crate::key::{KVKeyDecode, KVValue, Key, Resumable};
use crate::kvs::ds::Datastore;
use crate::kvs::tasklease::LeaseHandler;
#[cfg(test)]
use crate::kvs::testing::{RetryableConflictSite, maybe_inject_retryable_conflict};
use crate::kvs::version::ledger::{LedgerClass, LedgerId};
use crate::kvs::{Direction, TransactionType};
use crate::val::{RecordIdKey, RecordIdentity, TableName};

const TARGET: &str = "surrealdb::core::kvs::migration";

/// The ledger entry this migration is recorded under, at `/!mg{id}`.
///
/// Compatible, as every startup migration a 4.x build adds is: a build that
/// does not know the entry reads the doc-ID space this leaves as it reads any
/// other, so it may open the store read-write.
pub(super) const LEDGER_ID: LedgerId = LedgerId::new(LedgerClass::Compatible, 10);

/// How many entries one batch reads and writes.
const BATCH: usize = 500;

/// How many times one batch is run before its conflict fails the migration.
const WRITE_ATTEMPTS: usize = 5;

/// Delay before the first retry; doubled on each attempt, plus jitter.
const RETRY_BACKOFF: Duration = Duration::from_millis(10);

/// Reconciles `!dj` from `!dd`, prunes what `!dd` does not back, and deletes
/// `!di`, for every table in the datastore.
pub(super) async fn retire_the_type_erased_forward_doc_ids(ds: &Datastore) -> Result<()> {
	let lease = super::lease(ds)?;
	let (mut published, mut pruned, mut erased) = (0usize, 0usize, 0usize);
	for (ns, db, tb) in super::doc_lookup_identity::tables(ds).await? {
		published += reconcile_table(ds, &lease, ns, db, &tb).await?;
		pruned += prune_table(ds, &lease, ns, db, &tb).await?;
		erased += erase_table(ds, &lease, ns, db, &tb).await?;
	}
	if published + pruned + erased > 0 {
		debug!(
			target: TARGET,
			published, pruned, erased,
			"Retired the type-erased forward doc-ID mappings"
		);
	}
	Ok(())
}

/// Whether `e` is a commit lost to a concurrent writer: a retryable
/// transaction conflict, or a conditional write whose condition a racing
/// writer broke.
fn is_write_conflict(e: &anyhow::Error) -> bool {
	surrealdb_datastore::is_retryable_transaction_conflict(e)
		|| crate::kvs::is_conditional_write_conflict(e)
}

/// A forward mapping as the prune pass read it: its raw key, and its record and
/// doc-ID, or `None` when the entry does not decode.
type ForwardEntry = (Vec<u8>, Option<(RecordIdKey, u64)>);

/// One batch of one pass, over one table's doc-ID space.
enum Batch<'a> {
	/// [`reconcile_batch`] over reverse mappings `(raw key, doc-ID, record)`.
	Reconcile(&'a [(Vec<u8>, u64, RecordIdKey)]),
	/// [`prune_batch`] over forward mappings `(raw key, record and doc-ID)`.
	Prune(&'a [ForwardEntry]),
	/// [`erase_page`].
	Erase,
}

/// Runs `batch` in a write transaction of its own, returning how many entries
/// it wrote, and runs it again on a write conflict up to [`WRITE_ATTEMPTS`]
/// times. The conflict that outlasts them is returned.
async fn write_with_retry(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	batch: Batch<'_>,
) -> Result<usize> {
	for attempt in 1..=WRITE_ATTEMPTS {
		let txn = ds.transaction(TransactionType::Write).await?;
		let done = async {
			#[cfg(test)]
			maybe_inject_retryable_conflict(
				RetryableConflictSite::DocLookupErasedMigration,
				ds.id(),
			)?;
			match &batch {
				Batch::Reconcile(b) => reconcile_batch(&txn, ns, db, tb, b).await,
				Batch::Prune(b) => prune_batch(&txn, ns, db, tb, b).await,
				Batch::Erase => erase_page(&txn, ns, db, tb).await,
			}
		}
		.await;
		let outcome = match done {
			Ok(n) => txn.commit().await.map(|()| n),
			Err(e) => {
				txn.cancel().await?;
				Err(e)
			}
		};
		match outcome {
			Err(e) if attempt < WRITE_ATTEMPTS && is_write_conflict(&e) => {
				trace!(
					target: TARGET,
					"Retrying a doc-ID retirement batch after a write conflict (attempt {attempt})"
				);
				// The jitter keeps two nodes migrating the same table from
				// retrying in step.
				let base = RETRY_BACKOFF * (1 << (attempt - 1));
				let jitter = Duration::from_millis(rand::random::<u64>() % 20);
				crate::common::time::sleep(base + jitter).await;
			}
			other => return other,
		}
	}
	unreachable!("the final attempt returns rather than looping")
}

/// Whether the reverse mapping `stored` decodes to a record `id` addresses.
/// A reverse mapping that does not decode backs nothing.
fn backs(stored: Option<&[u8]>, id: &RecordIdKey) -> bool {
	stored
		.and_then(|raw| RecordIdKey::kv_decode_value(raw, ()).ok())
		.is_some_and(|mapped| mapped.addresses_same_record(id))
}

/// Publishes the `!dj` of every record `!dd` maps whose `!dj` is absent or
/// unbacked, returning how many were written.
async fn reconcile_table(
	ds: &Datastore,
	lease: &LeaseHandler,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
) -> Result<usize> {
	let mut published = 0usize;
	let mut resume: Option<Vec<u8>> = None;
	loop {
		let Some((batch, last)) =
			super::doc_lookup_identity::read_batch(ds, ns, db, tb, resume.as_deref()).await?
		else {
			return Ok(published);
		};
		resume = Some(last);
		published += write_with_retry(ds, ns, db, tb, Batch::Reconcile(&batch)).await?;
		// A lost renewal is not fatal: the batches are safe to repeat on
		// another node that takes the lease over.
		let _ = lease.try_maintain_lease().await;
	}
}

/// One batch of [`reconcile_table`], given the reverse mappings `(raw key,
/// doc-ID, record)` an earlier snapshot read.
///
/// Each mapping is re-read here first: a record deleted since owns no doc-ID,
/// and one re-created since owns its newer one.
async fn reconcile_batch(
	txn: &crate::kvs::Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	batch: &[(Vec<u8>, u64, RecordIdKey)],
) -> Result<usize> {
	let forward = |id: &RecordIdKey| {
		DocLookupIdentityKey::new(ns, db, Cow::Borrowed(tb), RecordIdentity(id.clone()))
	};
	let reverse = |doc_id: u64| DocKeyKey::new(ns, db, Cow::Borrowed(tb), doc_id);
	let current =
		txn.getm_raw(batch.iter().map(|(_, doc_id, _)| reverse(*doc_id)).collect(), None).await?;
	let claims =
		txn.get_many_key(batch.iter().map(|(_, _, id)| forward(id)).collect(), None).await?;
	// The reverse mapping behind each claim that names another doc-ID decides
	// whether that claim stands.
	let contested: Vec<u64> = batch
		.iter()
		.zip(&claims)
		.filter_map(|((_, doc_id, _), claim)| claim.filter(|c| c != doc_id))
		.collect();
	let mut behind =
		txn.getm_raw(contested.iter().map(|c| reverse(*c)).collect(), None).await?.into_iter();
	let mut published = 0usize;
	// Records this batch has published a mapping for. `claims` was read before
	// any of them, so a second reverse mapping naming one of these records would
	// otherwise judge, and condition its write on, a value this batch replaced.
	let mut decided = HashSet::new();
	for (((_, doc_id, id), current), claim) in batch.iter().zip(current).zip(claims) {
		let contested_backing = match claim {
			Some(c) if c != *doc_id => Some(behind.next().flatten()),
			_ => None,
		};
		if !backs(current.as_deref(), id) {
			// The record has moved on since the scan; whoever moved it owns
			// the mapping now.
			continue;
		}
		match (claim, contested_backing) {
			// Already published.
			(Some(c), _) if c == *doc_id => continue,
			// Another doc-ID that `!dd` also maps to this record: the one the
			// record resolves to, and so the one its postings reference.
			(Some(_), Some(backing)) if backs(backing.as_deref(), id) => continue,
			_ => {}
		}
		if !decided.insert(RecordIdentity(id.clone())) {
			// Another of the record's doc-IDs, which `!dd` also backs, was
			// published above.
			continue;
		}
		txn.put_compare_key(&forward(id), doc_id, claim.as_ref()).await?;
		published += 1;
	}
	Ok(published)
}

/// Deletes every `!dj` whose doc-ID `!dd` does not map back to its record,
/// returning how many were deleted.
async fn prune_table(
	ds: &Datastore,
	lease: &LeaseHandler,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
) -> Result<usize> {
	let mut pruned = 0usize;
	let mut resume: Option<Vec<u8>> = None;
	loop {
		let Some((batch, last)) = read_forward_batch(ds, ns, db, tb, resume.as_deref()).await?
		else {
			return Ok(pruned);
		};
		resume = Some(last);
		pruned += write_with_retry(ds, ns, db, tb, Batch::Prune(&batch)).await?;
		let _ = lease.try_maintain_lease().await;
	}
}

/// Reads up to [`BATCH`] forward mappings after `resume`, with the raw key of
/// the last one read; `None` once the table has none left. A mapping that does
/// not decode is returned without a record, for [`prune_batch`] to delete.
async fn read_forward_batch(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	resume: Option<&[u8]>,
) -> Result<Option<(Vec<ForwardEntry>, Vec<u8>)>> {
	let txn = ds.transaction(TransactionType::Read).await?;
	let collected = async {
		let range = DocLookupIdentityPrefix::new(ns, db, Cow::Borrowed(tb)).range()?;
		let range = match resume {
			Some(resume) => range.resume_after(resume, Direction::Forward),
			None => range,
		};
		let mut cursor = txn.open_vals_cursor(range, Direction::Forward, 0, None).await?;
		let page = cursor.next_batch(BATCH as u32).await?;
		let Some((last, _)) = page.iter().last() else {
			return Ok(None);
		};
		let last = last.to_vec();
		let out = page
			.iter()
			.map(|(key, value)| {
				let decoded = DocLookupIdentityKey::decode_key(key)
					.and_then(|k| u64::kv_decode_value(value, ()).map(|doc_id| (k.id.0, doc_id)));
				if let Err(e) = &decoded {
					warn!(
						target: TARGET,
						table = %tb,
						error = %e,
						"Deleting a forward doc-ID mapping that does not decode"
					);
				}
				(key.to_vec(), decoded.ok())
			})
			.collect();
		Ok::<_, anyhow::Error>(Some((out, last)))
	}
	.await;
	txn.cancel().await?;
	collected
}

/// One batch of [`prune_table`], given the forward mappings an earlier snapshot
/// read.
///
/// Each mapping is re-read here first, with the reverse mapping behind it, and
/// deleted only if it still names the doc-ID that was read and `!dd` does not
/// back it. A writer that purged the record since, or published a fresh mapping
/// for it, owns the key now. The key deleted is the one the scan returned.
async fn prune_batch(
	txn: &crate::kvs::Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	batch: &[ForwardEntry],
) -> Result<usize> {
	let decoded: Vec<(&[u8], &RecordIdKey, u64)> = batch
		.iter()
		.filter_map(|(key, m)| m.as_ref().map(|(id, doc_id)| (key.as_slice(), id, *doc_id)))
		.collect();
	let current = txn
		.get_many_key(
			decoded
				.iter()
				.map(|(_, id, _)| {
					DocLookupIdentityKey::new(
						ns,
						db,
						Cow::Borrowed(tb),
						RecordIdentity((*id).clone()),
					)
				})
				.collect(),
			None,
		)
		.await?;
	let behind = txn
		.getm_raw(
			decoded
				.iter()
				.map(|(_, _, doc_id)| DocKeyKey::new(ns, db, Cow::Borrowed(tb), *doc_id))
				.collect(),
			None,
		)
		.await?;
	let mut pruned = 0usize;
	for (((key, id, doc_id), current), behind) in decoded.into_iter().zip(current).zip(behind) {
		if current != Some(doc_id) || backs(behind.as_deref(), id) {
			continue;
		}
		txn.del(Key::from(key)).await?;
		pruned += 1;
	}
	for (key, _) in batch.iter().filter(|(_, m)| m.is_none()) {
		txn.del(Key::from(key.as_slice())).await?;
		pruned += 1;
	}
	Ok(pruned)
}

/// Deletes the table's `!di` band, returning how many keys it held.
///
/// In pages, each read and deleted in one transaction: a range delete is capped
/// on some backends, and the band is as large as the table.
async fn erase_table(
	ds: &Datastore,
	lease: &LeaseHandler,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
) -> Result<usize> {
	let mut erased = 0usize;
	loop {
		let page = write_with_retry(ds, ns, db, tb, Batch::Erase).await?;
		if page == 0 {
			return Ok(erased);
		}
		erased += page;
		let _ = lease.try_maintain_lease().await;
	}
}

/// Deletes the first page of the table's `!di` band, returning how many keys
/// it held.
async fn erase_page(
	txn: &crate::kvs::Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
) -> Result<usize> {
	let range = DocLookupPrefix::new(ns, db, Cow::Borrowed(tb)).range()?;
	let keys = txn.keys(range, BATCH as u32, 0, None).await?;
	for key in &keys {
		txn.del(Key::from(key.as_slice())).await?;
	}
	Ok(keys.len())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
	use crate::idx::docids::TableDocIds;
	use crate::key::schema::{DocKeyPrefix, DocLookupKey};
	use crate::val::{Number, Value};

	fn arr(n: Number) -> RecordIdKey {
		RecordIdKey::Array(vec![Value::Number(n)].into())
	}

	struct Space {
		ds: std::sync::Arc<Datastore>,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
	}

	impl Space {
		async fn new() -> Self {
			let ds = Datastore::new("memory").await.unwrap();
			ds.execute(
				"DEFINE NAMESPACE n; USE NS n; DEFINE DATABASE d; USE NS n DB d; DEFINE TABLE t;",
				&Default::default(),
				None,
			)
			.await
			.unwrap();
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let ns = txn.all_ns(None).await.unwrap()[0].namespace_id;
			let db = txn.all_db(ns, None).await.unwrap()[0].database_id;
			txn.cancel().await.unwrap();
			Self {
				ds,
				ns,
				db,
				tb: "t".into(),
			}
		}

		fn forward(&self, id: &RecordIdKey) -> DocLookupIdentityKey<'_> {
			DocLookupIdentityKey::new(
				self.ns,
				self.db,
				Cow::Borrowed(&self.tb),
				RecordIdentity(id.clone()),
			)
		}

		fn reverse(&self, doc_id: u64) -> DocKeyKey<'_> {
			DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id)
		}

		fn erased<'a>(&'a self, id: &'a RecordIdKey) -> DocLookupKey<'a> {
			DocLookupKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), Cow::Borrowed(id))
		}

		/// Writes the given mappings, as a mix of releases would have left them.
		async fn seed(
			&self,
			forward: &[(&RecordIdKey, u64)],
			reverse: &[(u64, &RecordIdKey)],
			erased: &[(&RecordIdKey, u64)],
		) {
			let txn = self.ds.transaction(TransactionType::Write).await.unwrap();
			for (id, doc_id) in forward {
				txn.set_key(&self.forward(id), doc_id).await.unwrap();
			}
			for (doc_id, id) in reverse {
				txn.set_key(&self.reverse(*doc_id), *id).await.unwrap();
			}
			for (id, doc_id) in erased {
				txn.set_key(&self.erased(id), doc_id).await.unwrap();
			}
			txn.commit().await.unwrap();
		}

		async fn forward_of(&self, id: &RecordIdKey) -> Option<u64> {
			let txn = self.ds.transaction(TransactionType::Read).await.unwrap();
			let found = txn.get_key(&self.forward(id), None).await.unwrap();
			txn.cancel().await.unwrap();
			found
		}

		async fn erased_len(&self) -> usize {
			let range =
				DocLookupPrefix::new(self.ns, self.db, Cow::Borrowed(&self.tb)).range().unwrap();
			let txn = self.ds.transaction(TransactionType::Read).await.unwrap();
			let keys = txn.keys(range, u32::MAX, 0, None).await.unwrap();
			txn.cancel().await.unwrap();
			keys.len()
		}

		async fn forward_len(&self) -> usize {
			let range = DocLookupIdentityPrefix::new(self.ns, self.db, Cow::Borrowed(&self.tb))
				.range()
				.unwrap();
			let txn = self.ds.transaction(TransactionType::Read).await.unwrap();
			let keys = txn.keys(range, u32::MAX, 0, None).await.unwrap();
			txn.cancel().await.unwrap();
			keys.len()
		}

		async fn resolve(&self, id: &RecordIdKey) -> Option<u64> {
			let docs = TableDocIds::new(self.ns, self.db, self.tb.clone());
			let txn = self.ds.transaction(TransactionType::Read).await.unwrap();
			let found = docs.get_doc_id(&txn, id).await.unwrap();
			txn.cancel().await.unwrap();
			found
		}
	}

	/// A record mapped only through `!di` and `!dd` gets its `!dj`, and one whose
	/// `!dj` names a doc-ID `!dd` no longer maps to it has that `!dj` replaced.
	#[tokio::test]
	async fn a_record_whose_forward_mapping_is_missing_or_stale_gets_the_one_dd_backs() {
		let s = Space::new().await;
		let (missing, stale) = (arr(Number::Int(1)), arr(Number::Int(2)));
		// `stale`'s `!dj` names doc 3, which `!dd` maps to nothing: a pre-release
		// node purged and re-created it as doc 4 through `!di` and `!dd` alone.
		s.seed(&[(&stale, 3)], &[(1, &missing), (4, &stale)], &[(&missing, 1), (&stale, 4)]).await;

		retire_the_type_erased_forward_doc_ids(&s.ds).await.unwrap();

		assert_eq!(s.forward_of(&missing).await, Some(1));
		assert_eq!(s.forward_of(&stale).await, Some(4), "an unbacked claim is replaced");
		assert_eq!(s.resolve(&missing).await, Some(1));
		assert_eq!(s.resolve(&stale).await, Some(4));
	}

	/// Two doc-IDs `!dd` maps to one record: the one `!dj` already names, and
	/// so the one every release resolves to, is kept.
	#[tokio::test]
	async fn a_forward_mapping_dd_backs_is_kept_over_another_doc_id_of_the_record() {
		let s = Space::new().await;
		let id = arr(Number::Int(1));
		s.seed(&[(&id, 7)], &[(5, &id), (7, &id)], &[]).await;

		retire_the_type_erased_forward_doc_ids(&s.ds).await.unwrap();

		assert_eq!(s.forward_of(&id).await, Some(7));
	}

	/// Two doc-IDs `!dd` maps to one record, read in one batch, with no `!dj`
	/// or one `!dd` does not back: the record is given one of them, and the
	/// batch does not trip over the mapping it just wrote.
	#[tokio::test]
	async fn two_doc_ids_of_one_record_in_one_batch_publish_one_mapping() {
		for stale in [None, Some(3u64)] {
			let s = Space::new().await;
			let id = arr(Number::Int(1));
			let forward: Vec<(&RecordIdKey, u64)> = stale.iter().map(|d| (&id, *d)).collect();
			s.seed(&forward, &[(5, &id), (7, &id)], &[]).await;

			retire_the_type_erased_forward_doc_ids(&s.ds).await.unwrap();

			assert_eq!(s.forward_of(&id).await, Some(5), "with {stale:?} claimed");
		}
	}

	/// A `!dj` whose doc-ID `!dd` maps to no record, or to another, is deleted,
	/// and ids differing only in a number's kind keep a mapping each.
	#[tokio::test]
	async fn a_forward_mapping_dd_does_not_back_is_pruned() {
		let s = Space::new().await;
		let (int, dec, gone) =
			(arr(Number::Int(1)), arr(Number::Decimal(1.into())), arr(Number::Int(9)));
		// `gone` was purged by a pre-release node; `dec` and `int` are a
		// colliding pair, each mapped through its own `!dj`.
		s.seed(&[(&int, 1), (&dec, 2), (&gone, 3)], &[(1, &int), (2, &dec)], &[(&int, 2)]).await;

		retire_the_type_erased_forward_doc_ids(&s.ds).await.unwrap();

		assert_eq!(s.forward_of(&gone).await, None, "a purged record's claim is pruned");
		assert_eq!(s.forward_of(&int).await, Some(1));
		assert_eq!(s.forward_of(&dec).await, Some(2));
		assert_eq!(s.forward_len().await, 2);
	}

	/// The band is deleted whole, and a re-run finds nothing more to do.
	#[tokio::test]
	async fn the_erased_band_is_deleted_and_a_rerun_changes_nothing() {
		let s = Space::new().await;
		let ids: Vec<RecordIdKey> = (0..(BATCH as i64 + 3)).map(|i| arr(Number::Int(i))).collect();
		let erased: Vec<(&RecordIdKey, u64)> =
			ids.iter().enumerate().map(|(i, id)| (id, i as u64)).collect();
		let reverse: Vec<(u64, &RecordIdKey)> =
			ids.iter().enumerate().map(|(i, id)| (i as u64, id)).collect();
		s.seed(&[], &reverse, &erased).await;

		retire_the_type_erased_forward_doc_ids(&s.ds).await.unwrap();
		assert_eq!(s.erased_len().await, 0, "the band spans more than one page and is gone");
		assert_eq!(s.forward_len().await, ids.len());

		retire_the_type_erased_forward_doc_ids(&s.ds).await.unwrap();
		assert_eq!(s.forward_len().await, ids.len());
		for (i, id) in ids.iter().enumerate() {
			assert_eq!(s.forward_of(id).await, Some(i as u64));
		}
	}

	/// A batch that keeps losing its commit fails the migration rather than
	/// leaving a record its readers would resolve wrongly.
	#[tokio::test]
	async fn a_batch_that_keeps_losing_its_commit_fails_the_migration() {
		let s = Space::new().await;
		let id = arr(Number::Int(1));
		s.seed(&[(&id, 3)], &[(4, &id)], &[]).await;

		{
			let _guard = crate::kvs::testing::inject_retryable_conflicts(
				RetryableConflictSite::DocLookupErasedMigration,
				s.ds.id(),
				WRITE_ATTEMPTS,
			);
			let err = retire_the_type_erased_forward_doc_ids(&s.ds)
				.await
				.expect_err("a batch lost to conflicts must fail the migration");
			assert!(is_write_conflict(&err), "{err:#}");
		}
		assert_eq!(s.forward_of(&id).await, Some(3), "nothing was committed");

		retire_the_type_erased_forward_doc_ids(&s.ds).await.unwrap();
		assert_eq!(s.forward_of(&id).await, Some(4), "a later run completes it");
	}

	/// A prune batch read before a concurrent purge or re-publish leaves the
	/// key to whoever moved it, rather than failing on a value that changed.
	///
	/// A 3.3.x node keeps purging and re-creating records while this runs.
	#[tokio::test]
	async fn a_stale_prune_batch_leaves_what_moved_on_under_it() {
		let s = Space::new().await;
		let (purged, republished) = (arr(Number::Int(1)), arr(Number::Int(2)));
		// As the scan saw them: both claims unbacked.
		let raw =
			|id: &RecordIdKey| crate::key::KVKey::encode_key(&s.forward(id)).unwrap().to_vec();
		let stale = vec![
			(raw(&purged), Some((purged.clone(), 3u64))),
			(raw(&republished), Some((republished.clone(), 4u64))),
		];
		// Since then: `purged` is gone in both directions, and `republished`
		// was mapped afresh to doc 9.
		s.seed(&[(&republished, 9)], &[(9, &republished)], &[]).await;

		let txn = s.ds.transaction(TransactionType::Write).await.unwrap();
		let pruned = prune_batch(&txn, s.ns, s.db, &s.tb, &stale).await.unwrap();
		txn.commit().await.unwrap();

		assert_eq!(pruned, 0);
		assert_eq!(s.forward_of(&purged).await, None);
		assert_eq!(s.forward_of(&republished).await, Some(9), "the fresh mapping is kept");
	}

	/// A reverse mapping that does not decode backs nothing, so the forward
	/// mapping naming it is pruned, and the rest of the space is reconciled.
	#[tokio::test]
	async fn a_reverse_mapping_that_does_not_decode_backs_nothing() {
		let s = Space::new().await;
		let (broken, fine) = (arr(Number::Int(1)), arr(Number::Int(2)));
		s.seed(&[(&broken, 5)], &[(6, &fine)], &[]).await;
		{
			let txn = s.ds.transaction(TransactionType::Write).await.unwrap();
			let garbage = crate::key::KVKey::encode_key(&s.reverse(5)).unwrap();
			txn.set(garbage, vec![0xff; 3]).await.unwrap();
			txn.commit().await.unwrap();
		}

		retire_the_type_erased_forward_doc_ids(&s.ds).await.unwrap();

		assert_eq!(s.forward_of(&broken).await, None);
		assert_eq!(s.forward_of(&fine).await, Some(6));
		let txn = s.ds.transaction(TransactionType::Read).await.unwrap();
		let reverse = txn.keys(
			DocKeyPrefix::new(s.ns, s.db, Cow::Borrowed(&s.tb)).range().unwrap(),
			10,
			0,
			None,
		);
		assert_eq!(
			reverse.await.unwrap().len(),
			2,
			"the reverse band is not this migration's to edit"
		);
		txn.cancel().await.unwrap();
	}
}
