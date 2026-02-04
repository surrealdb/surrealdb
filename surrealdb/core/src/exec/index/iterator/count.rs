//! Count index iterator.
//!
//! Provides efficient counting without scanning all records.

use std::ops::Range;

use anyhow::Result;

use crate::catalog::{DatabaseId, IndexDefinition, NamespaceId};
use crate::cnf::COUNT_BATCH_SIZE;
use crate::ctx::FrozenContext;
use crate::key::index::iu::IndexCountKey;
use crate::kvs::{Key, Transaction};

/// Iterator for count index aggregation.
///
/// Count indexes store incremental count changes that need to be summed
/// to get the total count.
pub struct CountIterator {
	/// The key range to scan
	range: Option<Range<Key>>,
}

impl CountIterator {
	/// Create a new count iterator.
	pub fn new(ns: NamespaceId, db: DatabaseId, ix: &IndexDefinition) -> Result<Self> {
		let range = IndexCountKey::range(ns, db, &ix.table_name, ix.index_id)?;
		Ok(Self {
			range: Some(range),
		})
	}

	/// Get the total count.
	///
	/// This scans all count entries and aggregates them.
	pub async fn get_count(&mut self, ctx: &FrozenContext, tx: &Transaction) -> Result<usize> {
		let Some(range) = self.range.take() else {
			return Ok(0);
		};

		let mut count: i64 = 0;
		let mut loops = 0;
		let mut current_range = Some(range);

		while let Some(range) = current_range {
			let batch = tx.batch_keys(range, *COUNT_BATCH_SIZE, None).await?;

			for key in batch.result.iter() {
				loops += 1;
				ctx.is_done(Some(loops)).await?;

				let iu = IndexCountKey::decode_key(key)?;
				if iu.pos {
					count += iu.count as i64;
				} else {
					count -= iu.count as i64;
				}
			}

			current_range = batch.next;
			ctx.is_done(None).await?;
		}

		Ok(count.max(0) as usize)
	}
}
