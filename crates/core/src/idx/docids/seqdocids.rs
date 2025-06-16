use crate::idx::docids::{DocId, Resolved};
use crate::kvs::{Key, Transaction};
use anyhow::Result;

/// Sequence-based DocIds store

pub(crate) struct SeqDocIds {}

impl SeqDocIds {
	pub(in crate::idx) fn new() -> Self {
		Self {}
	}

	pub(in crate::idx) async fn get_doc_id(
		&self,
		tx: &Transaction,
		doc_key: Key,
	) -> Result<Option<DocId>> {
		todo!()
	}

	pub(in crate::idx) async fn resolve_doc_id(
		&self,
		tx: &Transaction,
		doc_key: Key,
	) -> Result<Resolved> {
		todo!()
	}
}
