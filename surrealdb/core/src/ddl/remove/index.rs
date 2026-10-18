#[cfg(test)]
#[cfg(feature = "kv-tikv")]
mod tikv_concurrency {
	use std::borrow::Cow;
	use std::sync::Arc;

	use surrealdb_strand::TableName;
	use tokio_util::sync::CancellationToken;
	use uuid::Uuid;
	use web_time::Duration;

	use crate::CommunityComposer;
	use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
	use crate::dbs::Session;
	use crate::key::schema::{DocKeyPrefix, DocLookupIdentityPrefix, RootRoot, VersionKey};
	use crate::kvs::{Datastore, TransactionType};

	async fn fresh_tikv_ds() -> Arc<Datastore> {
		let ds = Datastore::builder()
			.with_id(Uuid::new_v4())
			.build_with_factory_path("tikv:127.0.0.1:2379", CommunityComposer())
			.await
			.unwrap();
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		// Both top-level regions: everything under the root, and the storage
		// version key, which sits outside it.
		tx.delr(RootRoot {}.range_subtree().unwrap()).await.unwrap();
		tx.del_key(&VersionKey {}).await.unwrap();
		tx.commit().await.unwrap();
		ds
	}

	/// Drain the deferred reclaim queue, so a test can assert on data the
	/// statement under test enqueued for background reclaim rather than deleted.
	async fn drain_reclaim_queue(ds: &Arc<Datastore>) {
		const MAX_PASSES: usize = 64;
		for _ in 0..MAX_PASSES {
			let (batches, _) = Datastore::reclaim_tombstones(
				Arc::clone(ds),
				Duration::from_secs(1),
				Duration::ZERO,
				CancellationToken::new(),
			)
			.await
			.unwrap();
			if batches == 0 {
				return;
			}
		}
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
	/// index list, each see the other still present, and leave the `!dj`/`!dd`
	/// mappings behind. The serialization guard (`replace_tb`'s locked read of
	/// the table-definition key) forces one to retry and re-decide as the sole
	/// remover, so the mappings are always purged. Repeated to make the racing
	/// window likely.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	#[serial_test::serial]
	async fn concurrent_last_consumer_removal_purges_mappings() {
		const ITERS: usize = 6;
		let ds = fresh_tikv_ds().await;
		let ses = Session::owner().with_ns("test").with_db("test");

		for k in 0..ITERS {
			let tb = format!("t{k}");
			// Two full-text indexes (both doc-ID consumers) plus data, so the
			// shared `!dj`/`!dd` mappings are populated for this table.
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
			drain_reclaim_queue(&ds).await;
			let tx = ds.transaction(TransactionType::Read).await.unwrap();
			let ns = tx.get_ns_by_name("test", None).await.unwrap().unwrap().namespace_id;
			let db = tx.get_db_by_name("test", "test", None).await.unwrap().unwrap().database_id;
			let tb_name: TableName = tb.as_str().into();
			let forward = tx
				.getr(
					DocLookupIdentityPrefix::new(ns, db, Cow::Borrowed(&tb_name)).range().unwrap(),
					None,
				)
				.await
				.unwrap();
			let reverse = tx
				.getr(DocKeyPrefix::new(ns, db, Cow::Borrowed(&tb_name)).range().unwrap(), None)
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
			drain_reclaim_queue(&ds).await;
			let tx = ds.transaction(TransactionType::Read).await.unwrap();
			let ns = tx.get_ns_by_name("test", None).await.unwrap().unwrap().namespace_id;
			let db = tx.get_db_by_name("test", "test", None).await.unwrap().unwrap().database_id;
			let tb_name: TableName = tb.as_str().into();
			let forward = tx
				.getr(
					DocLookupIdentityPrefix::new(ns, db, Cow::Borrowed(&tb_name)).range().unwrap(),
					None,
				)
				.await
				.unwrap();
			let reverse = tx
				.getr(DocKeyPrefix::new(ns, db, Cow::Borrowed(&tb_name)).range().unwrap(), None)
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
