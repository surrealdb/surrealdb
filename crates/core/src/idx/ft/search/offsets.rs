use anyhow::Result;

use crate::idx::IndexKeyBase;
use crate::idx::docids::DocId;
use crate::idx::ft::offset::OffsetRecords;
use crate::idx::ft::search::terms::TermId;
use crate::kvs::Transaction;

pub(super) struct Offsets {
	index_key_base: IndexKeyBase,
}

impl Offsets {
	pub(super) fn new(index_key_base: IndexKeyBase) -> Self {
		Self {
			index_key_base,
		}
	}

	pub(super) async fn set_offsets(
		&self,
		tx: &Transaction,
		doc_id: DocId,
		term_id: TermId,
		offsets: OffsetRecords,
	) -> Result<()> {
		let key = self.index_key_base.new_bo_key(doc_id, term_id);
		tx.set(&key, &offsets, None).await?;
		Ok(())
	}

	pub(super) async fn get_offsets(
		&self,
		tx: &Transaction,
		doc_id: DocId,
		term_id: TermId,
	) -> Result<Option<OffsetRecords>> {
		let key = self.index_key_base.new_bo_key(doc_id, term_id);
		tx.get(&key, None).await
	}

	pub(super) async fn remove_offsets(
		&self,
		tx: &Transaction,
		doc_id: DocId,
		term_id: TermId,
	) -> Result<()> {
		let key = self.index_key_base.new_bo_key(doc_id, term_id);
		tx.del(&key).await
	}
}
