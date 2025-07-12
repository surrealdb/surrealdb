use crate::ctx::Context;
use crate::err::Error;
use crate::expr::Id;
use crate::idx::IndexKeyBase;
use crate::idx::docids::{DocId, Resolved};
use crate::kvs::sequences::SequenceDomain;
use crate::kvs::{Key, KeyEncode, Transaction, Val};
use anyhow::Result;
use std::sync::Arc;
use uuid::Uuid;

/// Sequence-based DocIds store
pub(crate) struct SeqDocIds {
	ikb: IndexKeyBase,
	nid: Uuid,
	domain: Arc<SequenceDomain>,
	batch: u32,
}

impl SeqDocIds {
	pub(in crate::idx) fn new(nid: Uuid, ikb: IndexKeyBase) -> Self {
		Self {
			nid,
			domain: Arc::new(SequenceDomain::new_ft_doc_ids(ikb.clone())),
			batch: 1000, // TODO ekeller: Make that configurable?
			ikb,
		}
	}

	pub(in crate::idx) async fn get_doc_id(
		&self,
		tx: &Transaction,
		id: &Id,
	) -> Result<Option<DocId>> {
		let id_key = self.ikb.new_id_key(id.clone());
		if let Some(v) = tx.get(id_key, None).await? {
			return Ok(Some(Self::val_to_doc_id(v)?));
		}
		Ok(None)
	}

	fn val_to_doc_id(val: Val) -> Result<DocId> {
		// Validate the length and convert to array (zero copy)
		let val: [u8; 8] = val.as_slice().try_into().map_err(|_| {
			Error::Internal(format!(
				"invalid stored DocId length: expected 8 bytes, got {}",
				val.len()
			))
		})?;
		Ok(u64::from_be_bytes(val))
	}

	pub(in crate::idx) async fn resolve_doc_id(&self, ctx: &Context, id: Id) -> Result<Resolved> {
		let id_key = self.ikb.new_id_key(id.clone());
		let tx = ctx.tx();
		// Do we already have an ID?
		if let Some(val) = tx.get(id_key.clone(), None).await? {
			let doc_id = Self::val_to_doc_id(val)?;
			return Ok(Resolved::Existing(doc_id));
		}
		// If not, let's get one from the sequence
		let new_doc_id = ctx
			.try_get_sequences()?
			.next_val_fts_idx(ctx, self.nid, self.domain.clone(), self.batch)
			.await? as DocId;
		{
			let val = new_doc_id.to_be_bytes();
			tx.set(id_key, &val, None).await?;
		}
		{
			let k = self.ikb.new_bi_key(new_doc_id);
			let v = revision::to_vec(&id)?;
			tx.set(k, v, None).await?;
		}
		Ok(Resolved::New(new_doc_id))
	}

	pub(in crate::idx) async fn get_id(
		ikb: &IndexKeyBase,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<Id>> {
		if let Some(v) = tx.get(ikb.new_bi_key(doc_id), None).await? {
			Ok(Some(revision::from_slice(&v)?))
		} else {
			Ok(None)
		}
	}

	pub(in crate::idx) async fn remove_doc_id(
		&self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<()> {
		let k: Key = self.ikb.new_bi_key(doc_id).encode()?;
		if let Some(v) = tx.get(k.clone(), None).await? {
			let id: Id = revision::from_slice(&v)?;
			tx.del(self.ikb.new_id_key(id)).await?;
			tx.del(k).await?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::ctx::Context;
	use crate::expr::Id;
	use crate::idx::IndexKeyBase;
	use crate::idx::docids::seqdocids::SeqDocIds;
	use crate::idx::docids::{DocId, Resolved};
	use crate::key::index::bi::Bi;
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::{Read, Write};
	use crate::kvs::{Datastore, TransactionType};
	use uuid::Uuid;

	const TEST_NS: &str = "test_ns";
	const TEST_DB: &str = "test_db";
	const TEST_TB: &str = "test_tb";
	const TEST_IX: &str = "test_ix";

	async fn new_operation(ds: &Datastore, tt: TransactionType) -> (Context, SeqDocIds) {
		let mut ctx = ds.setup_ctx().unwrap();
		let tx = ds.transaction(tt, Optimistic).await.unwrap();
		let ikb = IndexKeyBase::new(TEST_NS, TEST_DB, TEST_TB, TEST_IX).unwrap();
		ctx.set_transaction(tx.into());
		let d = SeqDocIds::new(Uuid::nil(), ikb);
		(ctx.freeze(), d)
	}

	async fn finish(ctx: Context) {
		ctx.tx().commit().await.unwrap();
	}

	async fn check_get_doc_key_id(ctx: &Context, d: &SeqDocIds, doc_id: DocId, key: &str) {
		let tx = ctx.tx();
		let id: Id = key.into();
		assert_eq!(SeqDocIds::get_id(&d.ikb, &tx, doc_id).await.unwrap(), Some(id));
		assert_eq!(d.get_doc_id(&tx, &key.into()).await.unwrap(), Some(doc_id));
	}
	#[tokio::test]
	async fn test_resolve_doc_id() {
		let ds = Datastore::new("memory").await.unwrap();

		// Resolve a first doc key
		{
			let (ctx, d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&ctx, "Foo".into()).await.unwrap();
			assert_eq!(doc_id, Resolved::New(0));
			finish(ctx).await;

			let (ctx, d) = new_operation(&ds, Read).await;
			check_get_doc_key_id(&ctx, &d, 0, "Foo").await;
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
			let (ctx, d) = new_operation(&ds, Write).await;
			d.remove_doc_id(&ctx.tx(), 2).await.unwrap();
			d.remove_doc_id(&ctx.tx(), 0).await.unwrap();
			finish(ctx).await;
		}

		// Check 'Foo' has been removed
		{
			let (ctx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.get_doc_id(&ctx.tx(), &"Foo".into()).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&ctx.tx(), &"Bar".into()).await.unwrap(), Some(1));
		}

		// Insert a new doc - should take the next available id 2
		{
			let (ctx, d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&ctx, "Hello".into()).await.unwrap(), Resolved::New(2));
			finish(ctx).await;
		}

		// Check we have "Hello" and "Bar"
		{
			let (ctx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.get_doc_id(&ctx.tx(), &"Foo".into()).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&ctx.tx(), &"Bar".into()).await.unwrap(), Some(1));
			assert_eq!(d.get_doc_id(&ctx.tx(), &"Hello".into()).await.unwrap(), Some(2));
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
			assert_eq!(d.get_doc_id(&ctx.tx(), &"Foo".into()).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&ctx.tx(), &"Bar".into()).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&ctx.tx(), &"Hello".into()).await.unwrap(), Some(2));
		}

		// Insert a new doc - should take the available id 3
		{
			let (ctx, d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&ctx, "World".into()).await.unwrap(), Resolved::New(3));
			finish(ctx).await;
		}

		// Check "World" has been added
		{
			let (ctx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.get_doc_id(&ctx.tx(), &"Foo".into()).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&ctx.tx(), &"Bar".into()).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&ctx.tx(), &"Hello".into()).await.unwrap(), Some(2));
			assert_eq!(d.get_doc_id(&ctx.tx(), &"World".into()).await.unwrap(), Some(3));
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
				let id =
					crate::key::index::id::Id::new(TEST_NS, TEST_DB, TEST_TB, TEST_IX, id.into());
				assert!(!tx.exists(id, None).await.unwrap());
			}
			for doc_id in 0..=3 {
				let bi = Bi::new(TEST_NS, TEST_DB, TEST_TB, TEST_IX, doc_id);
				assert!(!tx.exists(bi, None).await.unwrap());
			}
		}
	}
}
