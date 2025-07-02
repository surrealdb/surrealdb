use crate::idx::IndexKeyBaseRef;
use crate::idx::docids::{DocId, Resolved};
use crate::kvs::{Key, Transaction};
use anyhow::Result;

/// Sequence-based DocIds store
pub(crate) struct SeqDocIds<'a> {
	ikb: IndexKeyBaseRef<'a>,
}

impl<'a> SeqDocIds<'a> {
	pub(in crate::idx) fn new(ikb: IndexKeyBaseRef<'a>) -> Self {
		Self {
			ikb,
		}
	}

	pub(in crate::idx) async fn get_doc_id(
		&self,
		_tx: &Transaction,
		_doc_key: Key,
	) -> Result<Option<DocId>> {
		todo!()
	}

	pub(in crate::idx) async fn resolve_doc_id(
		&self,
		_tx: &Transaction,
		_doc_key: Key,
	) -> Result<Resolved> {
		todo!()
	}

	pub(in crate::idx) async fn get_doc_key(
		&self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<Key>> {
		tx.get(self.ikb.new_bi_key(doc_id)?, None).await
	}

	pub(in crate::idx) async fn remove_doc_id(
		&self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<()> {
		tx.del(self.ikb.new_bi_key(doc_id)?).await
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::IndexKeyBaseRef;
	use crate::idx::docids::seqdocids::SeqDocIds;
	use crate::idx::docids::{DocId, Resolved};
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::{Read, Write};
	use crate::kvs::{Datastore, Key, Transaction, TransactionType};

	async fn new_operation(ds: &Datastore, tt: TransactionType) -> (Transaction, SeqDocIds) {
		let tx = ds.transaction(tt, Optimistic).await.unwrap();
		let d = SeqDocIds::new(IndexKeyBaseRef::new("", "", "", ""));
		(tx, d)
	}

	async fn finish(tx: Transaction) {
		tx.commit().await.unwrap();
	}

	async fn check_get_doc_key_id(tx: &Transaction, d: &SeqDocIds<'_>, doc_id: DocId, key: &str) {
		let key: Key = key.into();
		assert_eq!(d.get_doc_key(tx, doc_id).await.unwrap(), Some(key.clone()));
		assert_eq!(d.get_doc_id(tx, key).await.unwrap(), Some(doc_id));
	}
	#[tokio::test]
	async fn test_resolve_doc_id() {
		let ds = Datastore::new("memory").await.unwrap();

		// Resolve a first doc key
		{
			let (tx, d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&tx, "Foo".into()).await.unwrap();
			assert_eq!(doc_id, Resolved::New(0));
			finish(tx).await;

			let (tx, d) = new_operation(&ds, Read).await;
			check_get_doc_key_id(&tx, &d, 0, "Foo").await;
		}

		// Resolve the same doc key
		{
			let (tx, d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&tx, "Foo".into()).await.unwrap();
			assert_eq!(doc_id, Resolved::Existing(0));
			finish(tx).await;

			let (tx, d) = new_operation(&ds, Read).await;
			check_get_doc_key_id(&tx, &d, 0, "Foo").await;
		}

		// Resolve another single doc key
		{
			let (tx, d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&tx, "Bar".into()).await.unwrap();
			assert_eq!(doc_id, Resolved::New(1));
			finish(tx).await;

			let (tx, d) = new_operation(&ds, Read).await;
			check_get_doc_key_id(&tx, &d, 1, "Bar").await;
		}

		// Resolve another two existing doc keys and two new doc keys (interlaced)
		{
			let (tx, d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, "Foo".into()).await.unwrap(), Resolved::Existing(0));
			assert_eq!(d.resolve_doc_id(&tx, "Hello".into()).await.unwrap(), Resolved::New(2));
			assert_eq!(d.resolve_doc_id(&tx, "Bar".into()).await.unwrap(), Resolved::Existing(1));
			assert_eq!(d.resolve_doc_id(&tx, "World".into()).await.unwrap(), Resolved::New(3));
			finish(tx).await;
			let (tx, d) = new_operation(&ds, Read).await;
			check_get_doc_key_id(&tx, &d, 0, "Foo").await;
			check_get_doc_key_id(&tx, &d, 1, "Bar").await;
			check_get_doc_key_id(&tx, &d, 2, "Hello").await;
			check_get_doc_key_id(&tx, &d, 3, "World").await;
		}

		{
			let (tx, d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, "Foo".into()).await.unwrap(), Resolved::Existing(0));
			assert_eq!(d.resolve_doc_id(&tx, "Bar".into()).await.unwrap(), Resolved::Existing(1));
			assert_eq!(d.resolve_doc_id(&tx, "Hello".into()).await.unwrap(), Resolved::Existing(2));
			assert_eq!(d.resolve_doc_id(&tx, "World".into()).await.unwrap(), Resolved::Existing(3));
			finish(tx).await;
			let (tx, d) = new_operation(&ds, Read).await;
			check_get_doc_key_id(&tx, &d, 0, "Foo").await;
			check_get_doc_key_id(&tx, &d, 1, "Bar").await;
			check_get_doc_key_id(&tx, &d, 2, "Hello").await;
			check_get_doc_key_id(&tx, &d, 3, "World").await;
		}
	}

	#[tokio::test]
	async fn test_remove_doc_id() {
		let ds = Datastore::new("memory").await.unwrap();

		// Create two docs
		{
			let (tx, d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, "Foo".into()).await.unwrap(), Resolved::New(0));
			assert_eq!(d.resolve_doc_id(&tx, "Bar".into()).await.unwrap(), Resolved::New(1));
			finish(tx).await;
		}

		// Remove non-existing doc 2 and doc 0 "Foo"
		{
			let (tx, d) = new_operation(&ds, Write).await;
			d.remove_doc_id(&tx, 2).await.unwrap();
			d.remove_doc_id(&tx, 1).await.unwrap();
			finish(tx).await;
		}

		// Check 'Foo' has been removed
		{
			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.get_doc_id(&tx, "Foo".into()).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&tx, "Bar".into()).await.unwrap(), Some(1));
		}

		// Insert a new doc - should take the next available id 2
		{
			let (tx, d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, "Hello".into()).await.unwrap(), Resolved::New(2));
			finish(tx).await;
		}

		// Check we have "Hello" and "Bar"
		{
			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.get_doc_id(&tx, "Foo".into()).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&tx, "Bar".into()).await.unwrap(), Some(1));
			assert_eq!(d.get_doc_id(&tx, "Hello".into()).await.unwrap(), Some(2));
		}

		// Remove doc 1 "Bar"
		{
			let (tx, d) = new_operation(&ds, Write).await;
			d.remove_doc_id(&tx, 1).await.unwrap();
			finish(tx).await;
		}

		// Check "Bar" has been removed
		{
			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.get_doc_id(&tx, "Foo".into()).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&tx, "Bar".into()).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&tx, "Hello".into()).await.unwrap(), Some(2));
		}

		// Insert a new doc - should take the available id 3
		{
			let (tx, d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, "World".into()).await.unwrap(), Resolved::New(3));
			finish(tx).await;
		}

		// Check "World" has been added
		{
			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.get_doc_id(&tx, "Foo".into()).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&tx, "Bar".into()).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&tx, "Hello".into()).await.unwrap(), Some(2));
			assert_eq!(d.get_doc_id(&tx, "World".into()).await.unwrap(), Some(3));
		}
	}
}
