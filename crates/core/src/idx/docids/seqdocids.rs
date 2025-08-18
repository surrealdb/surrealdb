use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use crate::ctx::Context;
use crate::idx::IndexKeyBase;
use crate::idx::docids::{DocId, Resolved};
use crate::kvs::Transaction;
use crate::kvs::sequences::SequenceDomain;
use crate::val::RecordIdKey;

/// Sequence-based DocIds store for concurrent full-text search
///
/// This module implements a document ID management system for the concurrent
/// full-text search implementation. It uses the distributed sequence mechanism
/// to provide concurrent document ID creation, which is essential for the
/// inverted index.
///
/// The `SeqDocIds` struct maintains bidirectional mappings between document IDs
/// (numeric identifiers used internally by the full-text index) and record IDs
/// (the actual identifiers of the documents being indexed). This allows for
/// efficient lookup in both directions.
///
/// Key features:
/// - Uses distributed sequences for concurrent ID generation
/// - Maintains bidirectional mappings between DocIds and record IDs
/// - Supports efficient ID resolution, retrieval, and removal
/// - Enables concurrent document indexing operations
/// - Allocates IDs in batches for better performance
pub(crate) struct SeqDocIds {
	ikb: IndexKeyBase,
	nid: Uuid,
	domain: Arc<SequenceDomain>,
	batch: u32,
}

impl SeqDocIds {
	/// Creates a new SeqDocIds instance
	///
	/// Initializes a new document ID manager with the specified node ID and
	/// index key base. Sets up the sequence domain for generating unique
	/// document IDs.
	///
	/// # Arguments
	/// * `nid` - The node ID used for distributed sequence generation
	/// * `ikb` - The index key base containing namespace, database, table, and index information
	pub(in crate::idx) fn new(nid: Uuid, ikb: IndexKeyBase) -> Self {
		Self {
			nid,
			domain: Arc::new(SequenceDomain::new_ft_doc_ids(ikb.clone())),
			batch: 1000, // TODO ekeller: Make that configurable?
			ikb,
		}
	}

	/// Retrieves a document ID for a given record ID
	///
	/// Looks up the document ID associated with the specified record ID.
	///
	/// # Arguments
	/// * `tx` - The transaction to use for the lookup
	/// * `id` - The record ID to look up
	///
	/// # Returns
	/// * `Ok(Some(DocId))` - The document ID if found
	/// * `Ok(None)` - If no document ID exists for the record ID
	pub(in crate::idx) async fn get_doc_id(
		&self,
		tx: &Transaction,
		id: &RecordIdKey,
	) -> Result<Option<DocId>> {
		let id_key = self.ikb.new_id_key(id.clone());
		tx.get(&id_key, None).await
	}

	/// Resolves a record ID to a document ID, creating a new one if needed
	///
	/// This is a key method for the concurrent full-text search implementation.
	/// It either retrieves an existing document ID for a record ID or generates
	/// a new one using the distributed sequence mechanism.
	///
	/// # Arguments
	/// * `ctx` - The context containing transaction and sequence information
	/// * `id` - The record ID to resolve
	///
	/// # Returns
	/// * `Ok(Resolved::Existing(DocId))` - If the document ID already exists
	/// * `Ok(Resolved::New(DocId))` - If a new document ID was created
	pub(in crate::idx) async fn resolve_doc_id(
		&self,
		ctx: &Context,
		id: RecordIdKey,
	) -> Result<Resolved> {
		let id_key = self.ikb.new_id_key(id.clone());
		let tx = ctx.tx();
		// Do we already have an ID?
		if let Some(doc_id) = tx.get(&id_key, None).await? {
			return Ok(Resolved::Existing(doc_id));
		}
		// If not, let's get one from the sequence
		let new_doc_id = ctx
			.try_get_sequences()?
			.next_val_fts_idx(ctx, self.nid, self.domain.clone(), self.batch)
			.await? as DocId;
		{
			tx.set(&id_key, &new_doc_id, None).await?;
		}
		{
			let k = self.ikb.new_ii_key(new_doc_id);
			tx.set(&k, &id, None).await?;
		}
		Ok(Resolved::New(new_doc_id))
	}

	/// Retrieves a record ID for a given document ID
	///
	/// Looks up the record ID associated with the specified document ID.
	/// This is the reverse lookup of `get_doc_id`.
	///
	/// # Arguments
	/// * `ikb` - The index key base containing namespace, database, table, and index information
	/// * `tx` - The transaction to use for the lookup
	/// * `doc_id` - The document ID to look up
	///
	/// # Returns
	/// * `Ok(Some(Id))` - The record ID if found
	/// * `Ok(None)` - If no record ID exists for the document ID
	pub(in crate::idx) async fn get_id(
		ikb: &IndexKeyBase,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<RecordIdKey>> {
		tx.get(&ikb.new_ii_key(doc_id), None).await
	}

	/// Removes a document ID and its associated record ID
	///
	/// Deletes both the forward (record ID to document ID) and reverse
	/// (document ID to record ID) mappings for a document.
	///
	/// # Arguments
	/// * `tx` - The transaction to use for the removal
	/// * `doc_id` - The document ID to remove
	pub(in crate::idx) async fn remove_doc_id(
		&self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<()> {
		let k = self.ikb.new_ii_key(doc_id);
		if let Some(id) = tx.get(&k, None).await? {
			tx.del(&self.ikb.new_id_key(id)).await?;
			tx.del(&k).await?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use uuid::Uuid;

	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::ctx::Context;
	use crate::idx::IndexKeyBase;
	use crate::idx::docids::seqdocids::SeqDocIds;
	use crate::idx::docids::{DocId, Resolved};
	use crate::key::index::bi::Bi;
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::{Read, Write};
	use crate::kvs::{Datastore, TransactionType};
	use crate::val::RecordIdKey;

	const TEST_NS_ID: NamespaceId = NamespaceId(1);
	const TEST_DB_ID: DatabaseId = DatabaseId(1);
	const TEST_TB: &str = "test_tb";
	const TEST_IX: &str = "test_ix";

	async fn new_operation(ds: &Datastore, tt: TransactionType) -> (Context, SeqDocIds) {
		let mut ctx = ds.setup_ctx().unwrap();
		let tx = ds.transaction(tt, Optimistic).await.unwrap();
		let ikb = IndexKeyBase::new(TEST_NS_ID, TEST_DB_ID, TEST_TB, TEST_IX);
		ctx.set_transaction(tx.into());
		let d = SeqDocIds::new(Uuid::nil(), ikb);
		(ctx.freeze(), d)
	}

	async fn finish(ctx: Context) {
		ctx.tx().commit().await.unwrap();
	}

	async fn check_get_doc_key_id(ctx: &Context, d: &SeqDocIds, doc_id: DocId, key: &str) {
		let tx = ctx.tx();
		let id = RecordIdKey::String(key.to_owned());
		assert_eq!(SeqDocIds::get_id(&d.ikb, &tx, doc_id).await.unwrap(), Some(id.clone()));
		assert_eq!(d.get_doc_id(&tx, &id).await.unwrap(), Some(doc_id));
	}
	#[tokio::test]
	async fn test_resolve_doc_id() {
		let ds = Datastore::new("memory").await.unwrap();

		// Resolve a first doc key
		{
			let (ctx, d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&ctx, strand!("Foo").to_owned().into()).await.unwrap();
			assert_eq!(doc_id, Resolved::New(0));
			finish(ctx).await;

			let (ctx, d) = new_operation(&ds, Read).await;
			check_get_doc_key_id(&ctx, &d, 0, "Foo").await;
		}

		// Resolve the same doc key
		{
			let (tx, d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&tx, strand!("Foo").to_owned().into()).await.unwrap();
			assert_eq!(doc_id, Resolved::Existing(0));
			finish(tx).await;

			let (tx, d) = new_operation(&ds, Read).await;
			check_get_doc_key_id(&tx, &d, 0, "Foo").await;
		}

		// Resolve another single doc key
		{
			let (tx, d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&tx, strand!("Bar").to_owned().into()).await.unwrap();
			assert_eq!(doc_id, Resolved::New(1));
			finish(tx).await;

			let (tx, d) = new_operation(&ds, Read).await;
			check_get_doc_key_id(&tx, &d, 1, "Bar").await;
		}

		// Resolve another two existing doc keys and two new doc keys (interlaced)
		{
			let (tx, d) = new_operation(&ds, Write).await;
			assert_eq!(
				d.resolve_doc_id(&tx, strand!("Foo").to_owned().into()).await.unwrap(),
				Resolved::Existing(0)
			);
			assert_eq!(
				d.resolve_doc_id(&tx, strand!("Hello").to_owned().into()).await.unwrap(),
				Resolved::New(2)
			);
			assert_eq!(
				d.resolve_doc_id(&tx, strand!("Bar").to_owned().into()).await.unwrap(),
				Resolved::Existing(1)
			);
			assert_eq!(
				d.resolve_doc_id(&tx, strand!("World").to_owned().into()).await.unwrap(),
				Resolved::New(3)
			);
			finish(tx).await;
			let (tx, d) = new_operation(&ds, Read).await;
			check_get_doc_key_id(&tx, &d, 0, "Foo").await;
			check_get_doc_key_id(&tx, &d, 1, "Bar").await;
			check_get_doc_key_id(&tx, &d, 2, "Hello").await;
			check_get_doc_key_id(&tx, &d, 3, "World").await;
		}

		{
			let (tx, d) = new_operation(&ds, Write).await;
			assert_eq!(
				d.resolve_doc_id(&tx, strand!("Foo").to_owned().into()).await.unwrap(),
				Resolved::Existing(0)
			);
			assert_eq!(
				d.resolve_doc_id(&tx, strand!("Bar").to_owned().into()).await.unwrap(),
				Resolved::Existing(1)
			);
			assert_eq!(
				d.resolve_doc_id(&tx, strand!("Hello").to_owned().into()).await.unwrap(),
				Resolved::Existing(2)
			);
			assert_eq!(
				d.resolve_doc_id(&tx, strand!("World").to_owned().into()).await.unwrap(),
				Resolved::Existing(3)
			);
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
			assert_eq!(
				d.resolve_doc_id(&tx, strand!("Foo").to_owned().into()).await.unwrap(),
				Resolved::New(0)
			);
			assert_eq!(
				d.resolve_doc_id(&tx, strand!("Bar").to_owned().into()).await.unwrap(),
				Resolved::New(1)
			);
			finish(tx).await;
		}

		// Remove non-existing doc 2 and doc 0 "Foo"
		{
			let (ctx, d) = new_operation(&ds, Write).await;
			d.remove_doc_id(&ctx.tx(), 2).await.unwrap();
			d.remove_doc_id(&ctx.tx(), 0).await.unwrap();
			finish(ctx).await;
		}

		// Check 'Foo' has been removed
		{
			let (ctx, d) = new_operation(&ds, Read).await;
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &strand!("Foo").to_owned().into()).await.unwrap(),
				None
			);
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &strand!("Bar").to_owned().into()).await.unwrap(),
				Some(1)
			);
		}

		// Insert a new doc - should take the next available id 2
		{
			let (ctx, d) = new_operation(&ds, Write).await;
			assert_eq!(
				d.resolve_doc_id(&ctx, strand!("Hello").to_owned().into()).await.unwrap(),
				Resolved::New(2)
			);
			finish(ctx).await;
		}

		// Check we have "Hello" and "Bar"
		{
			let (ctx, d) = new_operation(&ds, Read).await;
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &strand!("Foo").to_owned().into()).await.unwrap(),
				None
			);
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &strand!("Bar").to_owned().into()).await.unwrap(),
				Some(1)
			);
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &strand!("Hello").to_owned().into()).await.unwrap(),
				Some(2)
			);
		}

		// Remove doc 1 "Bar"
		{
			let (ctx, d) = new_operation(&ds, Write).await;
			d.remove_doc_id(&ctx.tx(), 1).await.unwrap();
			finish(ctx).await;
		}

		// Check "Bar" has been removed
		{
			let (ctx, d) = new_operation(&ds, Read).await;
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &strand!("Foo").to_owned().into()).await.unwrap(),
				None
			);
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &strand!("Bar").to_owned().into()).await.unwrap(),
				None
			);
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &strand!("Hello").to_owned().into()).await.unwrap(),
				Some(2)
			);
		}

		// Insert a new doc - should take the available id 3
		{
			let (ctx, d) = new_operation(&ds, Write).await;
			assert_eq!(
				d.resolve_doc_id(&ctx, strand!("World").to_owned().into()).await.unwrap(),
				Resolved::New(3)
			);
			finish(ctx).await;
		}

		// Check "World" has been added
		{
			let (ctx, d) = new_operation(&ds, Read).await;
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &strand!("Foo").to_owned().into()).await.unwrap(),
				None
			);
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &strand!("Bar").to_owned().into()).await.unwrap(),
				None
			);
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &strand!("Hello").to_owned().into()).await.unwrap(),
				Some(2)
			);
			assert_eq!(
				d.get_doc_id(&ctx.tx(), &strand!("World").to_owned().into()).await.unwrap(),
				Some(3)
			);
		}

		// Remove remaining docs
		{
			let (ctx, d) = new_operation(&ds, Write).await;
			d.remove_doc_id(&ctx.tx(), 1).await.unwrap();
			d.remove_doc_id(&ctx.tx(), 2).await.unwrap();
			d.remove_doc_id(&ctx.tx(), 3).await.unwrap();
			finish(ctx).await;
		}

		// Check there's no ID and BI keys left
		{
			let (ctx, _) = new_operation(&ds, Read).await;
			let tx = ctx.tx();
			for id in ["Foo", "Bar", "Hello", "World"] {
				let id = crate::key::index::id::Id::new(
					TEST_NS_ID,
					TEST_DB_ID,
					TEST_TB,
					TEST_IX,
					RecordIdKey::String(id.to_owned()),
				);
				assert!(!tx.exists(&id, None).await.unwrap());
			}
			for doc_id in 0..=3 {
				let bi = Bi::new(TEST_NS_ID, TEST_DB_ID, TEST_TB, TEST_IX, doc_id);
				assert!(!tx.exists(&bi, None).await.unwrap());
			}
		}
	}
}
