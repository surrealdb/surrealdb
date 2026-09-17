//! Rebuilds each table's forward doc-ID mapping under a key that is injective.
//!
//! A table's doc-ID space has two directions: `!dd{doc_id}` → record key, and
//! record key → `!di` → doc-ID. The reverse direction stores the record key as
//! a value and so keeps it exactly. The forward direction spells it into the
//! key under `IndexFormat`, which omits a number's kind, so `[1]` and `[1dec]`
//! — two records with two distinct record keys — land on one `!di` key and
//! therefore one doc-ID. The second record to be resolved adopts the first's,
//! `!dd` names only one of them, and every doc-ID-consuming index (full-text
//! and both ANN families) returns that one and silently drops the other.
//!
//! `!dj` spells the same direction with `RecordIdentity`, which encodes through
//! the lossless codec and so is injective. This migration populates it from
//! `!dd`, which is the only place a record key survives in full.
//!
//! # Why the legacy key stays
//!
//! `!di` is left in place, and this release keeps writing it alongside `!dj`.
//! A migration runs as soon as the first upgraded node starts, while the rest
//! of a cluster may still be on the previous release, which resolves doc-IDs at
//! `!di` and nowhere else. Removing it would strand every record those nodes
//! hold — and a record they then failed to resolve would be handed a *second*
//! doc-ID, which is the one thing the shared space exists to prevent.
//!
//! What the older nodes cannot see is the disambiguation itself: a colliding
//! pair still shares one `!di` key there, so they keep returning one of the two.
//! That is the asymmetry the upgrade window carries, and it belongs in the
//! release notes.
//!
//! A reader on this release treats a `!di` hit as a hint, not an answer: it is
//! adopted only when `!dd` agrees that the doc-ID names the record that was
//! asked for. That is what keeps a pre-existing collision from being inherited
//! by the record that did not own it.
//!
//! TODO(3.5): once no supported upgrade path starts below 3.4, reconcile `!dj`
//! from `!dd` *with overwrite*, then delete the `!di` band and drop the mirrored
//! write in `TableDocIds::resolve_or_assign`.
//!
//! The overwrite is what this migration cannot do. Its puts are create-only, so
//! they treat any existing `!dj` as the fresher answer — which an entry a node
//! on the previous release orphaned, by purging a record through `!di` and
//! `!dd` alone, contradicts. Deleting `!di` before reconciling would strand
//! every record that still resolves only through it, and the next resolve would
//! mint each a second doc-ID while the first still owned its postings.
//!
//! # Re-entrancy
//!
//! Entries are copied in bounded batches, each in its own transaction, and a
//! batch writes the same bytes it would have written on any earlier attempt.
//! Re-running after an interruption converges, and two nodes running this at
//! once do the same work in the same order and reach the same state.

use std::borrow::Cow;
use std::time::Duration;

use anyhow::Result;
use tracing::{debug, trace};

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::schema::{DocKeyKey, DocKeyPrefix, DocLookupIdentityKey};
use crate::key::{KVKeyDecode, KVValue, Resumable};
use crate::kvs::ds::Datastore;
use crate::kvs::{Direction, TransactionType};
use crate::val::{RecordIdKey, RecordIdentity, TableName};

const TARGET: &str = "surrealdb::core::kvs::migration";

/// How many mappings one pass reads and copies. Bounds both the resident read
/// and the write set for a table whose doc-ID space is large, without making a
/// transaction per record.
const BATCH: usize = 500;

/// Populates `!dj` from `!dd` for every table in the datastore.
pub(super) async fn rebuild_forward_doc_ids_under_an_injective_key(ds: &Datastore) -> Result<()> {
	let txn = ds.transaction(TransactionType::Read).await?;
	let tables = async {
		let mut tables = Vec::new();
		for ns in txn.all_ns(None).await?.iter() {
			for db in txn.all_db(ns.namespace_id, None).await?.iter() {
				for tb in txn.all_tb(ns.namespace_id, db.database_id, None).await?.iter() {
					tables.push((ns.namespace_id, db.database_id, tb.name.clone()));
				}
			}
		}
		Ok::<_, anyhow::Error>(tables)
	}
	.await;
	txn.cancel().await?;

	let mut copied = 0usize;
	for (ns, db, tb) in tables? {
		copied += migrate_table(ds, ns, db, &tb).await?;
	}
	if copied > 0 {
		debug!(target: TARGET, count = copied, "Rebuilt forward doc-ID mappings under an injective key");
	}
	Ok(())
}

/// Copies one table's mappings, returning how many were written.
async fn migrate_table(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
) -> Result<usize> {
	let mut copied = 0usize;
	let mut resume: Option<Vec<u8>> = None;
	loop {
		let batch = read_batch(ds, ns, db, tb, resume.as_deref()).await?;
		if batch.is_empty() {
			return Ok(copied);
		}
		resume = Some(batch.last().expect("a non-empty batch has a last entry").0.clone());
		copied += write_batch_with_retry(ds, ns, db, tb, &batch).await?;
	}
}

/// How many times one batch is retried before its conflict is surfaced.
const WRITE_ATTEMPTS: usize = 5;

/// Delay before the first retry; doubled on each attempt, plus jitter.
const RETRY_BACKOFF: Duration = Duration::from_millis(10);

/// [`write_batch`], retried on a commit conflict.
///
/// The batch reads every `!dd` it copies, which arms the write-conflict check
/// against anyone deleting those records — and this migration is the first to
/// read user-band keys rather than catalog ones, so a live workload can lose it
/// that race. It runs during [`Datastore::new`], where a returned error fails
/// the node's start, so a conflict this batch can simply retry must not.
///
/// Both conflict classes are retried. A racing delete surfaces at commit as a
/// retryable transaction conflict, which is the one this exists for; a
/// create-only put losing its race surfaces as a conditional-write failure.
/// Matching only the latter would leave the guard inert for the very case it
/// was added for.
///
/// Retrying is safe because the batch is idempotent: the forward mappings are
/// written with create-only puts, and each is re-checked against `!dd` inside
/// the transaction that writes it.
async fn write_batch_with_retry(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	batch: &[(Vec<u8>, u64, RecordIdKey)],
) -> Result<usize> {
	for attempt in 1..=WRITE_ATTEMPTS {
		match write_batch(ds, ns, db, tb, batch).await {
			Err(e)
				if attempt < WRITE_ATTEMPTS
					&& (surrealdb_datastore::is_retryable_transaction_conflict(&e)
						|| crate::kvs::is_conditional_write_conflict(&e)) =>
			{
				trace!(
					target: "surrealdb::core::kvs::migration",
					"Retrying a doc-ID mapping batch after a write conflict (attempt {attempt})"
				);
				// Back off between attempts. Against a table under steady delete
				// churn, five immediate retries re-read the same batch inside a
				// few milliseconds and exhaust the bound without the writer ever
				// having moved on; the jitter keeps two nodes migrating the same
				// table from retrying in step.
				let base = RETRY_BACKOFF * (1 << (attempt - 1));
				let jitter = Duration::from_millis(rand::random::<u64>() % 20);
				tokio::time::sleep(base + jitter).await;
			}
			other => return other,
		}
	}
	unreachable!("the final attempt returns rather than looping")
}

/// Reads up to [`BATCH`] reverse mappings, starting after `resume`.
///
/// The raw `!dd` key of each entry is carried back so the next batch can resume
/// from it: the doc-ID is recoverable from the key, but resuming on re-encoded
/// bytes would assume an encoding rather than observe one.
async fn read_batch(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	resume: Option<&[u8]>,
) -> Result<Vec<(Vec<u8>, u64, RecordIdKey)>> {
	let txn = ds.transaction(TransactionType::Read).await?;
	let collected = async {
		let range = DocKeyPrefix::new(ns, db, Cow::Borrowed(tb)).range()?;
		let range = match resume {
			Some(resume) => range.resume_after(resume, Direction::Forward),
			None => range,
		};
		// A cursor rather than a whole-range read: a table's doc-ID space is as
		// large as the table, and only one batch of it may be resident.
		let mut cursor = txn.open_vals_cursor(range, Direction::Forward, 0, None).await?;
		let mut out = Vec::new();
		for (key, value) in cursor.next_batch(BATCH as u32).await?.iter() {
			let doc_id = DocKeyKey::decode_key(key)?.doc_id;
			let id = RecordIdKey::kv_decode_value(value, ())?;
			out.push((key.to_vec(), doc_id, id));
		}
		Ok::<_, anyhow::Error>(out)
	}
	.await;
	txn.cancel().await?;
	collected
}

/// Writes one batch's injective mappings in a single transaction, returning how
/// many it wrote.
///
/// Each mapping is re-checked against the reverse mapping *in this* transaction
/// before it is written. The batch was read from an older snapshot, and between
/// the two a record may have been deleted — so it no longer owns the doc-ID —
/// or deleted and re-created, so it owns a newer one. Deriving `!dj` from the
/// stale pair either way would point it at a doc-ID that resolves to nothing,
/// which is the failure this migration exists to remove rather than introduce.
/// Reading `!dd` here also arms the write-conflict check, so a racing writer
/// loses the commit rather than the mapping.
async fn write_batch(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	batch: &[(Vec<u8>, u64, RecordIdKey)],
) -> Result<usize> {
	let txn = ds.transaction(TransactionType::Write).await?;
	let written = async {
		let mut written = 0usize;
		for (_, doc_id, id) in batch {
			let dd = DocKeyKey::new(ns, db, Cow::Borrowed(tb), *doc_id);
			match txn.get_key(&dd, None).await? {
				Some(current) if current.addresses_same_record(id) => {}
				// The record has moved on since the scan; whoever moved it owns
				// the mapping now.
				_ => continue,
			}
			let key =
				DocLookupIdentityKey::new(ns, db, Cow::Borrowed(tb), RecordIdentity(id.clone()));
			// A conditional create rather than a set. A record resolved since the
			// scan has already published its own mapping and that one is fresher
			// than anything derived here, so an existing key is left alone — which
			// is also what makes a re-run converge instead of rewriting the band.
			match txn.put_key(&key, doc_id).await {
				Ok(()) => written += 1,
				Err(e) if super::already_exists(&e) => {}
				Err(e) => return Err(e),
			}
		}
		Ok::<_, anyhow::Error>(written)
	}
	.await;
	match written {
		Ok(written) => {
			txn.commit().await?;
			Ok(written)
		}
		Err(e) => {
			txn.cancel().await?;
			Err(e)
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::idx::docids::TableDocIds;
	use crate::val::{Number, Value};

	fn arr(n: Number) -> RecordIdKey {
		RecordIdKey::Array(vec![Value::Number(n)].into())
	}

	/// A store written before `!dj` existed holds only the erased forward key.
	/// After the migration every record resolves through the injective one.
	#[tokio::test]
	async fn the_migration_rebuilds_the_injective_mapping_from_the_reverse_one() {
		let ds = crate::kvs::Datastore::new("memory").await.unwrap();
		ds.execute("DEFINE NAMESPACE n; USE NS n; DEFINE DATABASE d;", &Default::default(), None)
			.await
			.unwrap();
		let (ns, db) = {
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let ns = txn.all_ns(None).await.unwrap()[0].namespace_id;
			let db = txn.all_db(ns, None).await.unwrap()[0].database_id;
			txn.cancel().await.unwrap();
			(ns, db)
		};
		let tb: TableName = "t".into();
		let docids = TableDocIds::new(ns, db, tb.clone());
		let int = arr(Number::Int(1));

		// Only the reverse mapping and the erased forward one, as an older
		// release would have left them. The table must exist for the migration's
		// catalog walk to reach it.
		ds.execute("USE NS n DB d; DEFINE TABLE t;", &Default::default(), None).await.unwrap();
		{
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			txn.set_key(&DocKeyKey::new(ns, db, Cow::Borrowed(&tb), 7), &int).await.unwrap();
			txn.commit().await.unwrap();
		}
		{
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			assert_eq!(
				txn.get_key(
					&DocLookupIdentityKey::new(
						ns,
						db,
						Cow::Borrowed(&tb),
						RecordIdentity(int.clone())
					),
					None
				)
				.await
				.unwrap(),
				None,
				"the injective mapping is what the migration is there to write"
			);
			txn.cancel().await.unwrap();
		}

		rebuild_forward_doc_ids_under_an_injective_key(&ds).await.unwrap();

		// Asserted on the injective key itself, not through `get_doc_id`: the
		// verified `!di` fallback answers for this record either way, so a read
		// through it would pass whether or not the migration wrote anything.
		let dj = DocLookupIdentityKey::new(ns, db, Cow::Borrowed(&tb), RecordIdentity(int.clone()));
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		assert_eq!(
			txn.get_key(&dj, None).await.unwrap(),
			Some(7),
			"the migration must populate the injective key"
		);
		assert_eq!(docids.get_doc_id(&txn, &int).await.unwrap(), Some(7));
		txn.cancel().await.unwrap();

		// Re-running writes the same bytes rather than a second mapping.
		rebuild_forward_doc_ids_under_an_injective_key(&ds).await.unwrap();
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		assert_eq!(txn.get_key(&dj, None).await.unwrap(), Some(7));
		assert_eq!(docids.get_doc_id(&txn, &int).await.unwrap(), Some(7));
		txn.cancel().await.unwrap();
	}

	/// A batch read before a concurrent delete must not resurrect the mapping it
	/// saw, and one read before a re-create must not overwrite the fresher one.
	///
	/// Both would leave `!dj` naming a doc-ID whose reverse mapping is gone or
	/// belongs to someone else — records unresolvable through every doc-ID
	/// index, which is the failure this migration exists to remove.
	#[tokio::test]
	async fn a_stale_batch_does_not_overwrite_what_moved_on_under_it() {
		let ds = crate::kvs::Datastore::new("memory").await.unwrap();
		ds.execute(
			"DEFINE NAMESPACE n; USE NS n; DEFINE DATABASE d; USE NS n DB d; DEFINE TABLE t;",
			&Default::default(),
			None,
		)
		.await
		.unwrap();
		let (ns, db) = {
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let ns = txn.all_ns(None).await.unwrap()[0].namespace_id;
			let db = txn.all_db(ns, None).await.unwrap()[0].database_id;
			txn.cancel().await.unwrap();
			(ns, db)
		};
		let tb: TableName = "t".into();
		let deleted = arr(Number::Int(1));
		let recreated = arr(Number::Int(2));
		let key = |id: &RecordIdKey| {
			DocLookupIdentityKey::new(ns, db, Cow::Borrowed(&tb), RecordIdentity(id.clone()))
		};

		// A batch as the scan saw it, against a store where one record has since
		// been deleted and the other re-created under a newer doc-ID.
		{
			let txn = ds.transaction(TransactionType::Write).await.unwrap();
			// `deleted`: no reverse mapping at all any more.
			// `recreated`: reverse mapping moved to doc 9, and its own `!dj` with it.
			txn.set_key(&DocKeyKey::new(ns, db, Cow::Borrowed(&tb), 9), &recreated).await.unwrap();
			txn.set_key(&key(&recreated), &9u64).await.unwrap();
			txn.commit().await.unwrap();
		}

		let stale =
			vec![(Vec::new(), 7u64, deleted.clone()), (Vec::new(), 8u64, recreated.clone())];
		assert_eq!(write_batch(&ds, ns, db, &tb, &stale).await.unwrap(), 0);

		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		assert_eq!(
			txn.get_key(&key(&deleted), None).await.unwrap(),
			None,
			"a deleted record's mapping must not be resurrected"
		);
		assert_eq!(
			txn.get_key(&key(&recreated), None).await.unwrap(),
			Some(9),
			"a re-created record keeps the doc-ID it was given, not the stale one"
		);
		txn.cancel().await.unwrap();
	}
}
