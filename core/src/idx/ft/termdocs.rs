use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::ft::doclength::DocLength;
use crate::idx::ft::terms::TermId;
use crate::idx::IndexKeyBase;
use crate::kvs::Transaction;
use roaring::RoaringTreemap;
use std::sync::Arc;

pub(in crate::idx) type TermsDocs = Arc<Vec<Option<(TermId, RoaringTreemap)>>>;

pub(super) struct TermDocs {
	index_key_base: IndexKeyBase,
}

impl TermDocs {
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
	) -> Result<(), Error> {
		let mut docs = self.get_docs(tx, term_id).await?.unwrap_or_else(RoaringTreemap::new);
		if docs.insert(doc_id) {
			let key = self.index_key_base.new_bc_key(term_id);
			let mut val = Vec::new();
			docs.serialize_into(&mut val)?;
			tx.set(key, val).await?;
		}
		Ok(())
	}

	pub(super) async fn get_docs(
		&self,
		tx: &Transaction,
		term_id: TermId,
	) -> Result<Option<RoaringTreemap>, Error> {
		let key = self.index_key_base.new_bc_key(term_id);
		if let Some(val) = tx.get(key).await? {
			let docs = RoaringTreemap::deserialize_from(&mut val.as_slice())?;
			Ok(Some(docs))
		} else {
			Ok(None)
		}
	}

	pub(super) async fn remove_doc(
		&self,
		tx: &Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<DocLength, Error> {
		if let Some(mut docs) = self.get_docs(tx, term_id).await? {
			if docs.contains(doc_id) {
				docs.remove(doc_id);
				let key = self.index_key_base.new_bc_key(term_id);
				if docs.is_empty() {
					tx.del(key).await?;
				} else {
					let mut val = Vec::new();
					docs.serialize_into(&mut val)?;
					tx.set(key, val).await?;
				}
			}
			Ok(docs.len())
		} else {
			Ok(0)
		}
	}
}
