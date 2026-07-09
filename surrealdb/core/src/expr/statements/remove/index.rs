use anyhow::Result;
use reblessive::tree::Stk;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Index, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};
use crate::idx::docids::TableDocIds;
use crate::kvs::index::retire_durable_index;
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveIndexStatement {
	pub name: Expr,
	pub what: Expr,
	pub if_exists: bool,
}

impl Default for RemoveIndexStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl RemoveIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Index, Base::Db)?;
		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "index name").await?;
		// Compute the what
		let table_name =
			TableName::new(expr_to_ident(stk, ctx, opt, doc, &self.what, "what").await?);
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the index definition
		let res = txn.expect_tb_index(ns, db, &table_name, &name).await;
		let ix = match res {
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::IxNotFound { .. })) {
					return Ok(Value::None);
				}
				return Err(e);
			}
			Ok(ix) => ix,
		};
		// Get the table definition
		let tb = txn.expect_tb(ns, db, &table_name).await?;
		// Determine — before the definition is removed — whether this is the last
		// doc-ID-consuming index (full-text / HNSW / DiskAnn) on the table. If so,
		// the shared table-level doc-ID mappings must be reclaimed once it is gone
		// (see the purge below).
		let removed_last_doc_id_index =
			matches!(ix.index, Index::FullText(_) | Index::Hnsw(_) | Index::DiskAnn(_))
				&& !txn.all_tb_indexes(ns, db, &table_name, None).await?.iter().any(|other| {
					other.index_id != ix.index_id
						&& matches!(
							other.index,
							Index::FullText(_) | Index::Hnsw(_) | Index::DiskAnn(_)
						)
				});
		// Clear process-local index wrappers immediately, then retire durable
		// build state in the same transaction that removes the catalog
		// definition. The builder abort is deferred until this transaction
		// commits so rollback/cancel keeps an in-flight build alive.
		ctx.get_index_stores().index_removed(ns, db, &tb, &ix).await?;
		if let Some(index_builder) = ctx.get_index_builder() {
			txn.register_index_builder_abort_after_commit(
				index_builder.clone(),
				ns,
				db,
				table_name.clone(),
				ix.index_id,
			)
			.await;
		}
		retire_durable_index(&txn, ns, db, &table_name, ix.index_id).await?;
		// Delete the catalog definition and enqueue the index data for
		// background reclaim. The catalog entry and id→name lookup are removed
		// now (so the index immediately stops being used and maintained); the
		// `/*{ns}*{db}*{tb}+{ix}` data prefix is destroyed asynchronously by
		// `Datastore::reclaim_tombstones`. The small durable build-state keys
		// retired above live under the table prefix, so they are cleared here
		// rather than by the prefix reclaim.
		txn.del_tb_index_deferred(ns, db, &table_name, &name).await?;
		// Reclaim the shared table-level doc-ID space once its last consumer is
		// gone. While a doc-ID index exists, record deletes drop each mapping via
		// `doc::index`'s central removal; with no consumer left that path is
		// skipped, so the `!di`/`!dd` mappings allocated earlier would leak (and
		// could later hand a re-created record a stale, reused doc-ID). The
		// monotonic sequence counter is preserved.
		if removed_last_doc_id_index {
			TableDocIds::new(ns, db, table_name.clone()).remove_all(&txn).await?;
		}
		// Serialize concurrent last-consumer removals on last-writer-wins backends
		// (TiKV). The `removed_last_doc_id_index` decision above is read from a
		// range scan of the index list, which those backends do not validate for
		// write-conflicts; two `REMOVE INDEX`es dropping the final two doc-ID
		// consumers could each decide the other still remains and both skip the
		// purge, leaking the shared `!di`/`!dd` mappings. Reading the table
		// definition key here arms the write-conflict check against the `put_tb`
		// write just below (which targets the same key): the second committer is
		// rejected, its whole transaction rolls back (nothing removed, no leak),
		// and it re-evaluates the decision on retry as the sole remover. On
		// conflict-serializing backends the `put_tb` write already serializes them.
		let tb_key = crate::key::database::tb::TableKey {
			prefix: crate::key::database::all::DatabaseRoot {
				ns,
				db,
			},
			tb: std::borrow::Cow::Borrowed(&table_name),
		};
		let _ = txn.get_key(&tb_key, None).await?;
		// Refresh the table cache for indexes
		txn.put_tb(
			ns_name,
			db_name,
			&TableDefinition {
				cache_indexes_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

/// Concurrency regression test for the last-doc-ID-consumer purge (race #3),
/// gated to the `kv-tikv` backend (the last-writer-wins model where it manifests)
/// and requiring a running cluster at 127.0.0.1:2379 (the `tikv` CI lane).
#[cfg(test)]
#[cfg(feature = "kv-tikv")]
mod tikv_concurrency {
	use std::sync::Arc;

	use uuid::Uuid;

	use crate::CommunityComposer;
	use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
	use crate::dbs::Session;
	use crate::key::KVRange;
	use crate::key::table::{dd, di};
	use crate::kvs::{Datastore, LockType, TransactionType};
	use crate::val::TableName;

	async fn fresh_tikv_ds() -> Arc<Datastore> {
		let ds = Datastore::builder()
			.with_id(Uuid::new_v4())
			.build_with_factory_path("tikv:127.0.0.1:2379", CommunityComposer())
			.await
			.unwrap();
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
		tx.delr((vec![0u8]..vec![0xffu8]).into()).await.unwrap();
		tx.commit().await.unwrap();
		Arc::new(ds)
	}

	/// Run a `REMOVE INDEX IF EXISTS` to completion, retrying on the transient
	/// write-conflict that the serialization guard produces when two removals
	/// race. `IF EXISTS` makes the retry idempotent (a no-op once the index is
	/// already gone).
	async fn remove_index(ds: Arc<Datastore>, sql: String) {
		let ses = Session::owner().with_ns("test").with_db("test");
		let mut last = String::new();
		for _ in 0..200 {
			match ds.execute(&sql, &ses, None).await {
				Ok(responses) => {
					if responses.into_iter().all(|r| r.result.is_ok()) {
						return;
					}
				}
				Err(e) => last = e.to_string(),
			}
		}
		panic!("`{sql}` did not succeed after retries: {last}");
	}

	/// Race #3: two `REMOVE INDEX`es dropping the final two doc-ID consumers of a
	/// table concurrently must not both skip the shared-mapping purge. On TiKV a
	/// blind `put_tb` does not serialize them, so both could read the pre-delete
	/// index list, each see the other still present, and leave the `!di`/`!dd`
	/// mappings behind. The serialization guard (reading the table-definition key
	/// before `put_tb`) forces one to retry and re-decide as the sole remover, so
	/// the mappings are always purged. Repeated to make the racing window likely.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	#[serial_test::serial]
	async fn concurrent_last_consumer_removal_purges_mappings() {
		const ITERS: usize = 6;
		let ds = fresh_tikv_ds().await;
		let ses = Session::owner().with_ns("test").with_db("test");

		for k in 0..ITERS {
			let tb = format!("t{k}");
			// Two full-text indexes (both doc-ID consumers) plus data, so the
			// shared `!di`/`!dd` mappings are populated for this table.
			let setup = format!(
				"DEFINE ANALYZER a{k} TOKENIZERS blank;
				 DEFINE INDEX i1 ON {tb} FIELDS a1 FULLTEXT ANALYZER a{k} BM25;
				 DEFINE INDEX i2 ON {tb} FIELDS a2 FULLTEXT ANALYZER a{k} BM25;
				 CREATE {tb}:1 SET a1 = 'hello', a2 = 'world';
				 CREATE {tb}:2 SET a1 = 'foo', a2 = 'bar';"
			);
			for r in ds.execute(&setup, &ses, None).await.unwrap() {
				r.result.unwrap();
			}

			// Drop both doc-ID consumers concurrently.
			let a = tokio::spawn(remove_index(
				Arc::clone(&ds),
				format!("REMOVE INDEX IF EXISTS i1 ON {tb}"),
			));
			let b = tokio::spawn(remove_index(
				Arc::clone(&ds),
				format!("REMOVE INDEX IF EXISTS i2 ON {tb}"),
			));
			a.await.unwrap();
			b.await.unwrap();

			// The shared doc-ID mappings for this table must be fully reclaimed.
			let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
			let ns = tx.get_ns_by_name("test", None).await.unwrap().unwrap().namespace_id;
			let db = tx.get_db_by_name("test", "test", None).await.unwrap().unwrap().database_id;
			let tb_name: TableName = tb.as_str().into();
			let forward = tx
				.getr(di::Prefix::new(ns, db, &tb_name).encode_range().unwrap(), None)
				.await
				.unwrap();
			let reverse = tx
				.getr(dd::Prefix::new(ns, db, &tb_name).encode_range().unwrap(), None)
				.await
				.unwrap();
			tx.cancel().await.unwrap();
			assert!(
				forward.is_empty() && reverse.is_empty(),
				"iteration {k}: shared doc-ID mappings leaked after removing the last consumer: \
				 {} forward, {} reverse",
				forward.len(),
				reverse.len(),
			);
		}
	}

	/// Run a statement to completion, retrying on the transient write-conflict
	/// the serialization guard produces when two replacements race. Rerunning a
	/// `DEFINE INDEX OVERWRITE` is idempotent (it redefines the same index).
	async fn execute_retrying(ds: Arc<Datastore>, sql: String) {
		let ses = Session::owner().with_ns("test").with_db("test");
		let mut last = String::new();
		for _ in 0..200 {
			match ds.execute(&sql, &ses, None).await {
				Ok(responses) => {
					if responses.into_iter().all(|r| r.result.is_ok()) {
						return;
					}
				}
				Err(e) => last = e.to_string(),
			}
		}
		panic!("`{sql}` did not succeed after retries: {last}");
	}

	/// Overwrite variant of race #3: two `DEFINE INDEX OVERWRITE`s replacing the
	/// final two doc-ID consumers with plain (non-doc-ID) indexes concurrently
	/// must not both skip the shared-mapping purge. The `purge_table_doc_ids`
	/// decision is read from a range scan of the index list, which
	/// last-writer-wins backends do not validate; the same table-definition-key
	/// read guard used by `REMOVE INDEX` (see `define/index.rs`) forces one
	/// replacement to retry and re-decide as the sole remaining replacer.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	#[serial_test::serial]
	async fn concurrent_last_consumer_overwrite_purges_mappings() {
		const ITERS: usize = 6;
		let ds = fresh_tikv_ds().await;
		let ses = Session::owner().with_ns("test").with_db("test");

		for k in 0..ITERS {
			let tb = format!("o{k}");
			let setup = format!(
				"DEFINE ANALYZER oa{k} TOKENIZERS blank;
				 DEFINE INDEX i1 ON {tb} FIELDS a1 FULLTEXT ANALYZER oa{k} BM25;
				 DEFINE INDEX i2 ON {tb} FIELDS a2 FULLTEXT ANALYZER oa{k} BM25;
				 CREATE {tb}:1 SET a1 = 'hello', a2 = 'world';
				 CREATE {tb}:2 SET a1 = 'foo', a2 = 'bar';"
			);
			for r in ds.execute(&setup, &ses, None).await.unwrap() {
				r.result.unwrap();
			}

			// Replace both doc-ID consumers with plain indexes concurrently.
			let a = tokio::spawn(execute_retrying(
				Arc::clone(&ds),
				format!("DEFINE INDEX OVERWRITE i1 ON {tb} FIELDS a1"),
			));
			let b = tokio::spawn(execute_retrying(
				Arc::clone(&ds),
				format!("DEFINE INDEX OVERWRITE i2 ON {tb} FIELDS a2"),
			));
			a.await.unwrap();
			b.await.unwrap();

			// With no doc-ID consumer left, the shared space must be reclaimed.
			let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
			let ns = tx.get_ns_by_name("test", None).await.unwrap().unwrap().namespace_id;
			let db = tx.get_db_by_name("test", "test", None).await.unwrap().unwrap().database_id;
			let tb_name: TableName = tb.as_str().into();
			let forward = tx
				.getr(di::Prefix::new(ns, db, &tb_name).encode_range().unwrap(), None)
				.await
				.unwrap();
			let reverse = tx
				.getr(dd::Prefix::new(ns, db, &tb_name).encode_range().unwrap(), None)
				.await
				.unwrap();
			tx.cancel().await.unwrap();
			assert!(
				forward.is_empty() && reverse.is_empty(),
				"iteration {k}: shared doc-ID mappings leaked after overwriting the last \
				 consumer: {} forward, {} reverse",
				forward.len(),
				reverse.len(),
			);
		}
	}
}
