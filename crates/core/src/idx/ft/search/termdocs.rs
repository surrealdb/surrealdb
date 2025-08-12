use anyhow::Result;
use roaring::RoaringTreemap;

use crate::idx::IndexKeyBase;
use crate::idx::docids::DocId;
use crate::idx::ft::DocLength;
use crate::idx::ft::search::terms::TermId;
use crate::kvs::Transaction;

pub(in crate::idx) type SearchTermsDocs = Vec<Option<(TermId, RoaringTreemap)>>;

pub(in crate::idx) struct SearchTermDocs {
	index_key_base: IndexKeyBase,
}

impl SearchTermDocs {
	pub(super) fn new(index_key_base: IndexKeyBase) -> Self {
		Self {
			index_key_base,
		}
	}

	pub(super) async fn set_doc(
		&self,
		tx: &Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<()> {
		let mut docs = self.get_docs(tx, term_id).await?.unwrap_or_else(RoaringTreemap::new);
		if docs.insert(doc_id) {
			let key = self.index_key_base.new_bc_key(term_id);
			tx.set(&key, &docs, None).await?;
		}
		Ok(())
	}

	pub(super) async fn get_docs(
		&self,
		tx: &Transaction,
		term_id: TermId,
	) -> Result<Option<RoaringTreemap>> {
		let key = self.index_key_base.new_bc_key(term_id);
		tx.get(&key, None).await
	}

	pub(super) async fn remove_doc(
		&self,
		tx: &Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<DocLength> {
		if let Some(mut docs) = self.get_docs(tx, term_id).await? {
			if docs.contains(doc_id) {
				docs.remove(doc_id);
				let key = self.index_key_base.new_bc_key(term_id);
				if docs.is_empty() {
					tx.del(&key).await?;
				} else {
					tx.set(&key, &docs, None).await?;
				}
			}
			Ok(docs.len())
		} else {
			Ok(0)
		}
	}
}
