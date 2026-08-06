use anyhow::Result;
use surrealdb_datastore::Transaction;
use surrealdb_kvs::Direction;
use surrealdb_kvs::consts::COUNT_BATCH_SIZE;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::env::IndexEnv;
use crate::key::schema::{IndexCountKey, IndexCountPrefix};
use crate::key::{KVKeyDecode, Resumable, TypedRange};
use crate::val::TableName;
use crate::{IndexKeyBase, bump_compaction_generation, read_compaction_generation};

pub struct IndexCountThingIterator {
	range: Option<TypedRange<()>>,
	/// Identity of the index being counted, kept so the read can add the
	/// deltas this transaction has buffered but not yet flushed.
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	ix: IndexId,
}

/// Snapshot gathered by the read phase of count-index compaction.
///
/// It contains the total count at the snapshot, the generation that must
/// still match, and the exact `!iu` keys that may be deleted if the CAS wins.
/// The continuation flag records whether the bounded scan left more entries
/// for another compaction batch.
pub struct IndexCountCompactionPlan {
	generation: Option<u64>,
	count: i64,
	has_delta: bool,
	has_more: bool,
	keys: Vec<Vec<u8>>,
}

impl IndexCountCompactionPlan {
	/// Returns true when the plan contains at least one count delta key.
	pub fn has_work(&self) -> bool {
		self.has_delta
	}

	/// Returns true when the `!iu` range has more entries beyond this batch.
	pub fn has_more(&self) -> bool {
		self.has_more
	}
}

impl IndexCountThingIterator {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &TableName, ix: IndexId) -> Result<Self> {
		Ok(Self {
			range: Some(
				IndexCountPrefix {
					ns,
					db,
					tb: std::borrow::Cow::Borrowed(tb),
					ix,
				}
				.range()?,
			),
			ns,
			db,
			tb: tb.clone(),
			ix,
		})
	}

	pub async fn next_count(
		&mut self,
		env: &dyn IndexEnv,
		txn: &Transaction,
		_limit: u32,
	) -> Result<usize> {
		if let Some(range) = self.range.take() {
			let mut count: i64 = 0;
			let mut loops = 0;
			let mut current_range = Some(range);
			while let Some(range) = current_range {
				let batch = txn.batch_keys(range.clone(), COUNT_BATCH_SIZE, None).await?;
				for key in batch.result.iter() {
					loops += 1;
					env.is_done(Some(loops)).await?;
					let iu = IndexCountKey::decode_key(key)?;
					if iu.pos {
						count += iu.count as i64;
					} else {
						count -= iu.count as i64;
					}
				}
				// A continuation means the page was full, so the next one picks up
				// after the last key this one returned.
				current_range = match batch.result.last() {
					Some(last) if batch.next.is_some() => {
						Some(range.resume_after(last, Direction::Forward))
					}
					_ => None,
				};
				env.is_done(None).await?;
			}
			// Add this transaction's own buffered mutations. Count deltas are
			// aggregated per transaction and flushed at commit, so they are not
			// in the scanned range yet; without this a read would not observe
			// writes made earlier in its own transaction.
			count += txn.pending_count_delta(self.ns, self.db, &self.tb, self.ix);
			Ok(count as usize)
		} else {
			Ok(0)
		}
	}

	/// Read phase for count compaction: capture the generation, sum visible
	/// `!iu` entries, and remember the exact keys seen in this snapshot.
	pub(crate) async fn prepare_compaction(
		&mut self,
		ikb: &IndexKeyBase,
		txn: &Transaction,
	) -> Result<IndexCountCompactionPlan> {
		self.prepare_compaction_with_limit(ikb, txn, COUNT_BATCH_SIZE).await
	}

	async fn prepare_compaction_with_limit(
		&mut self,
		ikb: &IndexKeyBase,
		txn: &Transaction,
		limit: u32,
	) -> Result<IndexCountCompactionPlan> {
		let generation = read_compaction_generation(txn, &ikb.new_iv_key()).await?;
		let Some(range) = self.range.take() else {
			return Ok(IndexCountCompactionPlan {
				generation,
				count: 0,
				has_delta: false,
				has_more: false,
				keys: Vec::new(),
			});
		};
		let mut count: i64 = 0;
		let mut has_delta = false;
		let mut has_more = false;
		let mut keys = Vec::new();
		let mut delta_count = 0;
		let mut loops = 0;
		let mut current_range = Some(range.clone());
		let limit = limit.max(1);
		while let Some(r) = current_range.take() {
			let batch = txn.batch_keys(r.clone(), limit.saturating_add(1), None).await?;
			for key in batch.result.iter() {
				loops += 1;
				if loops % 1000 == 0 {
					yield_now!()
				}
				let iu = IndexCountKey::decode_key(key)?;
				if iu.uid.is_some() && delta_count >= limit {
					has_more = true;
					current_range = None;
					break;
				}
				if iu.pos {
					count += iu.count as i64;
				} else {
					count -= iu.count as i64;
				}
				if iu.uid.is_some() {
					has_delta = true;
					delta_count += 1;
				}
				keys.push(key.clone());
			}
			if has_more {
				break;
			}
			// A continuation means the page was full, so the next one picks up after
			// the last key this one returned.
			current_range = match batch.result.last() {
				Some(last) if batch.next.is_some() => {
					Some(r.resume_after(last, Direction::Forward))
				}
				_ => None,
			};
		}
		has_more |= current_range.is_some();
		Ok(IndexCountCompactionPlan {
			generation,
			count,
			has_delta,
			has_more,
			keys,
		})
	}

	/// Write phase for count compaction: CAS the generation, delete only
	/// snapshot-seen keys, and write the compacted `uid = None` aggregate.
	pub(crate) async fn apply_compaction(
		ikb: &IndexKeyBase,
		txn: &Transaction,
		plan: IndexCountCompactionPlan,
	) -> Result<bool> {
		if !plan.has_work() {
			return Ok(false);
		}
		if !bump_compaction_generation(txn, &ikb.new_iv_key(), plan.generation).await? {
			return Ok(false);
		}
		for key in plan.keys.iter() {
			txn.del(key.into()).await?;
		}
		let count = plan.count;
		let pos = count.is_positive();
		let count = count.unsigned_abs();
		let compact_key = IndexCountKey {
			ns: ikb.ns(),
			db: ikb.db(),
			tb: std::borrow::Cow::Borrowed(ikb.table()),
			ix: ikb.index(),
			uid: None,
			pos,
			count,
		};
		txn.set_key(&compact_key, &()).await?;
		Ok(true)
	}

	#[cfg(test)]
	pub(crate) async fn compaction(&mut self, ikb: &IndexKeyBase, txn: &Transaction) -> Result<()> {
		let plan = self.prepare_compaction(ikb, txn).await?;
		Self::apply_compaction(ikb, txn, plan).await?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_kvs::TransactionType::{Read, Write};
	use uuid::Uuid;

	use super::*;
	use crate::IndexKeyBase;
	use crate::catalog::{DatabaseId, IndexId, NamespaceId};
	use crate::key::schema::IndexCountKey;
	use crate::test_env::TestIndexStore;

	fn count_key<'a>(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		uid: Option<(Uuid, Uuid)>,
		pos: bool,
		count: u64,
	) -> IndexCountKey<'a> {
		IndexCountKey {
			ns,
			db,
			tb: std::borrow::Cow::Borrowed(tb),
			ix,
			uid,
			pos,
			count,
		}
	}

	fn count_range(ns: NamespaceId, db: DatabaseId, tb: &TableName, ix: IndexId) -> TypedRange<()> {
		IndexCountPrefix {
			ns,
			db,
			tb: std::borrow::Cow::Borrowed(tb),
			ix,
		}
		.range()
		.unwrap()
	}

	async fn count_value(ds: &TestIndexStore, ikb: &IndexKeyBase) -> usize {
		let mut count_iter =
			IndexCountThingIterator::new(ikb.ns(), ikb.db(), ikb.table(), ikb.index()).unwrap();
		let env = ds.env(Read).await;
		let tx = env.tx();
		let count = count_iter.next_count(&env, &tx, u32::MAX).await.unwrap();
		tx.cancel().await.unwrap();
		count
	}

	/// Non-regression test: consecutive compactions using new iterator instances
	/// must not fail.
	///
	/// In production, `index_count_compaction` creates a fresh
	/// `IndexCountThingIterator` for every compaction run. The compacted key
	/// (uid = None) written by the first compaction already exists when the
	/// second run executes exact-key deletes + write. If the write used `put`
	/// (which errors on existing keys) instead of `set`, the second compaction
	/// would fail. This test ensures that does not happen.
	#[tokio::test]
	async fn test_consecutive_compactions_do_not_fail() {
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let tb: TableName = "test_tb".into();
		let ix = IndexId(3);
		let ikb = IndexKeyBase::new(ns, db, tb.clone(), ix);

		let ds = TestIndexStore::new().await;

		// Write some positive delta entries
		{
			let tx = ds.transaction(Write).await.unwrap();
			let uid1 = (Uuid::new_v4(), Uuid::new_v4());
			let uid2 = (Uuid::new_v4(), Uuid::new_v4());
			let k1 = count_key(ns, db, &tb, ix, Some(uid1), true, 10);
			let k2 = count_key(ns, db, &tb, ix, Some(uid2), true, 5);
			tx.set_key(&k1, &()).await.unwrap();
			tx.set_key(&k2, &()).await.unwrap();
			tx.commit().await.unwrap();
		}

		// First compaction (new iterator) — compacts (+10, +5) into a single +15 entry
		{
			let tx = ds.transaction(Write).await.unwrap();
			IndexCountThingIterator::new(ns, db, &tb, ix)
				.unwrap()
				.compaction(&ikb, &tx)
				.await
				.unwrap();
			tx.commit().await.unwrap();
		}

		// Verify the compacted count is 15
		{
			let count = count_value(&ds, &ikb).await;
			assert_eq!(count, 15, "first compaction should yield count 15");
		}

		// Write additional delta entries on top of the compacted state
		{
			let tx = ds.transaction(Write).await.unwrap();
			let uid3 = (Uuid::new_v4(), Uuid::new_v4());
			let k3 = count_key(ns, db, &tb, ix, Some(uid3), true, 7);
			tx.set_key(&k3, &()).await.unwrap();
			tx.commit().await.unwrap();
		}

		// Second compaction (new iterator) — must not fail even though the
		// compacted key already exists from the first compaction.
		{
			let tx = ds.transaction(Write).await.unwrap();
			IndexCountThingIterator::new(ns, db, &tb, ix)
				.unwrap()
				.compaction(&ikb, &tx)
				.await
				.unwrap();
			tx.commit().await.unwrap();
		}

		// Verify the compacted count is now 22 (15 + 7)
		{
			let count = count_value(&ds, &ikb).await;
			assert_eq!(count, 22, "second compaction should yield count 22 (15 + 7)");
		}
	}

	#[tokio::test]
	async fn count_compaction_preserves_post_snapshot_deltas() {
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let tb: TableName = "test_tb".into();
		let ix = IndexId(3);
		let ikb = IndexKeyBase::new(ns, db, tb.clone(), ix);
		let ds = TestIndexStore::new().await;

		{
			let tx = ds.transaction(Write).await.unwrap();
			let uid1 = (Uuid::new_v4(), Uuid::new_v4());
			let k1 = count_key(ns, db, &tb, ix, Some(uid1), true, 10);
			tx.set_key(&k1, &()).await.unwrap();
			tx.commit().await.unwrap();
		}

		let plan = {
			let tx = ds.transaction(Read).await.unwrap();
			let plan = IndexCountThingIterator::new(ns, db, &tb, ix)
				.unwrap()
				.prepare_compaction(&ikb, &tx)
				.await
				.unwrap();
			tx.cancel().await.unwrap();
			plan
		};

		{
			let tx = ds.transaction(Write).await.unwrap();
			let uid2 = (Uuid::new_v4(), Uuid::new_v4());
			let k2 = count_key(ns, db, &tb, ix, Some(uid2), true, 7);
			tx.set_key(&k2, &()).await.unwrap();
			tx.commit().await.unwrap();
		}

		{
			let tx = ds.transaction(Write).await.unwrap();
			assert!(IndexCountThingIterator::apply_compaction(&ikb, &tx, plan).await.unwrap());
			tx.commit().await.unwrap();
		}

		let tx = ds.transaction(Read).await.unwrap();
		assert_eq!(tx.get_key(&ikb.new_iv_key(), None).await.unwrap(), Some(1));
		let range = count_range(ns, db, &tb, ix);
		assert_eq!(
			tx.count(range, None).await.unwrap(),
			2,
			"compacted root and post-snapshot delta should remain"
		);
		tx.cancel().await.unwrap();
		assert_eq!(count_value(&ds, &ikb).await, 17);
	}

	#[tokio::test]
	async fn count_compaction_batches_visible_deltas() {
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let tb: TableName = "test_tb".into();
		let ix = IndexId(3);
		let ikb = IndexKeyBase::new(ns, db, tb.clone(), ix);
		let ds = TestIndexStore::new().await;

		{
			let tx = ds.transaction(Write).await.unwrap();
			for count in [10, 5, 7] {
				let uid = (Uuid::new_v4(), Uuid::new_v4());
				let key = count_key(ns, db, &tb, ix, Some(uid), true, count);
				tx.set_key(&key, &()).await.unwrap();
			}
			tx.commit().await.unwrap();
		}

		let plan = {
			let tx = ds.transaction(Read).await.unwrap();
			let plan = IndexCountThingIterator::new(ns, db, &tb, ix)
				.unwrap()
				.prepare_compaction_with_limit(&ikb, &tx, 2)
				.await
				.unwrap();
			tx.cancel().await.unwrap();
			plan
		};
		assert!(plan.has_work());
		assert!(plan.has_more());

		{
			let tx = ds.transaction(Write).await.unwrap();
			assert!(IndexCountThingIterator::apply_compaction(&ikb, &tx, plan).await.unwrap());
			tx.commit().await.unwrap();
		}

		let tx = ds.transaction(Read).await.unwrap();
		assert_eq!(
			tx.count(count_range(ns, db, &tb, ix), None).await.unwrap(),
			2,
			"first batch should leave compacted root plus one residual delta"
		);
		tx.cancel().await.unwrap();
		assert_eq!(count_value(&ds, &ikb).await, 22);

		let plan = {
			let tx = ds.transaction(Read).await.unwrap();
			let plan = IndexCountThingIterator::new(ns, db, &tb, ix)
				.unwrap()
				.prepare_compaction_with_limit(&ikb, &tx, 2)
				.await
				.unwrap();
			tx.cancel().await.unwrap();
			plan
		};
		assert!(plan.has_work());
		assert!(!plan.has_more());

		{
			let tx = ds.transaction(Write).await.unwrap();
			assert!(IndexCountThingIterator::apply_compaction(&ikb, &tx, plan).await.unwrap());
			tx.commit().await.unwrap();
		}

		let tx = ds.transaction(Read).await.unwrap();
		assert_eq!(
			tx.count(count_range(ns, db, &tb, ix), None).await.unwrap(),
			1,
			"second batch should collapse all deltas into one compacted root"
		);
		tx.cancel().await.unwrap();
		assert_eq!(count_value(&ds, &ikb).await, 22);
	}

	#[tokio::test]
	async fn count_compaction_generation_allows_only_one_winner() {
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let tb: TableName = "test_tb".into();
		let ix = IndexId(3);
		let ikb = IndexKeyBase::new(ns, db, tb.clone(), ix);
		let ds = TestIndexStore::new().await;

		{
			let tx = ds.transaction(Write).await.unwrap();
			let uid1 = (Uuid::new_v4(), Uuid::new_v4());
			let k1 = count_key(ns, db, &tb, ix, Some(uid1), true, 10);
			tx.set_key(&k1, &()).await.unwrap();
			tx.commit().await.unwrap();
		}

		let plan1 = {
			let tx = ds.transaction(Read).await.unwrap();
			let plan = IndexCountThingIterator::new(ns, db, &tb, ix)
				.unwrap()
				.prepare_compaction(&ikb, &tx)
				.await
				.unwrap();
			tx.cancel().await.unwrap();
			plan
		};
		let plan2 = {
			let tx = ds.transaction(Read).await.unwrap();
			let plan = IndexCountThingIterator::new(ns, db, &tb, ix)
				.unwrap()
				.prepare_compaction(&ikb, &tx)
				.await
				.unwrap();
			tx.cancel().await.unwrap();
			plan
		};

		{
			let tx = ds.transaction(Write).await.unwrap();
			assert!(IndexCountThingIterator::apply_compaction(&ikb, &tx, plan1).await.unwrap());
			tx.commit().await.unwrap();
		}
		{
			let tx = ds.transaction(Write).await.unwrap();
			assert!(!IndexCountThingIterator::apply_compaction(&ikb, &tx, plan2).await.unwrap());
			tx.cancel().await.unwrap();
		}

		let tx = ds.transaction(Read).await.unwrap();
		assert_eq!(tx.get_key(&ikb.new_iv_key(), None).await.unwrap(), Some(1));
		tx.cancel().await.unwrap();
		assert_eq!(count_value(&ds, &ikb).await, 10);
	}
}
