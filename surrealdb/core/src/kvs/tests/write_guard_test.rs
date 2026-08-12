//! Tests for the transaction write-cardinality guard
//! (`transaction_max_write_keys`).
//!
//! A statement's physical write count can vastly exceed its logical row
//! count (cascaded deletes, full-text term maintenance, graph-edge cleanup).
//! With the guard configured, a statement transaction that buffers more than
//! the configured number of individual key writes fails with an actionable
//! error and rolls back atomically; internal maintenance transactions are
//! never guarded.

use std::sync::Arc;

use surrealdb_cnf::ConfigMap;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::Session;
use crate::key::Key;
use crate::kvs::{Datastore, TransactionType};

/// Builds a memory datastore with the guard configured to `limit` (0 = off).
async fn guarded_ds(limit: u64) -> (Arc<Datastore>, Session) {
	let config = ConfigMap::empty().with_key_value("transaction_max_write_keys", limit.to_string());
	let ds = Datastore::builder().with_config(config).build_with_path("memory").await.unwrap();
	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		tx.ensure_ns_db(None, "test", "test").await.unwrap();
		tx.commit().await.unwrap();
	}
	(ds, Session::owner().with_ns("test").with_db("test"))
}

/// Executes SurrealQL and asserts every statement succeeded.
async fn run(ds: &Datastore, ses: &Session, sql: &str) {
	let res = ds.execute(sql, ses, None).await.unwrap();
	for (i, r) in res.into_iter().enumerate() {
		if let Err(e) = r.result {
			panic!("statement {i} failed: {e}");
		}
	}
}

/// Executes SurrealQL and returns the first statement error, if any.
async fn run_capture_err(ds: &Datastore, ses: &Session, sql: &str) -> Option<String> {
	let res = ds.execute(sql, ses, None).await.unwrap();
	res.into_iter().find_map(|r| r.result.err().map(|e| e.to_string()))
}

/// A document with `chunks` full-text indexed chunks reached through
/// `REFERENCE ON DELETE CASCADE`; deleting the document costs roughly
/// `2 × words` writes per chunk.
async fn setup_cascade(ds: &Datastore, ses: &Session, chunks: usize, words: usize) {
	run(ds, ses, "DEFINE ANALYZER az TOKENIZERS blank FILTERS lowercase;").await;
	run(
		ds,
		ses,
		"DEFINE TABLE document;
		 DEFINE TABLE chunk;
		 DEFINE FIELD document ON chunk TYPE record<document> REFERENCE ON DELETE CASCADE;
		 DEFINE INDEX chunk_text ON chunk FIELDS text FULLTEXT ANALYZER az BM25;",
	)
	.await;
	run(ds, ses, "CREATE document:doc1;").await;
	for ci in 0..chunks {
		let text: String = (0..words).map(|w| format!("c{ci}w{w}")).collect::<Vec<_>>().join(" ");
		run(ds, ses, &format!("CREATE chunk:c{ci} SET document = document:doc1, text = '{text}';"))
			.await;
	}
}

/// Asserts the cascade shape is fully intact: the rolled-back DELETE left no
/// partial state and the full-text index still serves queries.
async fn assert_intact(ds: &Datastore, ses: &Session, chunks: usize) {
	let res = ds
		.execute(
			"SELECT count() FROM chunk GROUP ALL;
			 SELECT count() FROM document GROUP ALL;
			 SELECT id FROM chunk WHERE text @@ 'c0w0';",
			ses,
			None,
		)
		.await
		.unwrap();
	let values: Vec<String> = res.into_iter().map(|r| format!("{:?}", r.result.unwrap())).collect();
	assert!(
		values[0].contains(&format!("Int({chunks})")),
		"chunk count changed after rolled-back delete: {}",
		values[0]
	);
	assert!(values[1].contains("Int(1)"), "document missing after rolled-back delete");
	assert!(values[2].contains("c0"), "full-text index lost chunk c0: {}", values[2]);
}

/// An over-limit cascade DELETE fails with the guard error and rolls back
/// atomically: every record and index entry survives.
#[tokio::test(flavor = "multi_thread")]
async fn write_guard_aborts_oversized_cascade_atomically() {
	let (ds, ses) = guarded_ds(200).await;
	// Each chunk CREATE buffers ~110 writes (under the limit); the DELETE
	// cascade across all 10 chunks needs >1000 and must be refused.
	setup_cascade(&ds, &ses, 10, 50).await;
	let err = run_capture_err(&ds, &ses, "DELETE document:doc1;")
		.await
		.expect("over-limit delete should fail");
	assert!(err.contains("maximum number of key writes (200)"), "unexpected error message: {err}");
	assert_intact(&ds, &ses, 10).await;
}

/// The same statement succeeds when the guard allows its fan-out.
#[tokio::test(flavor = "multi_thread")]
async fn write_guard_allows_within_limit() {
	let (ds, ses) = guarded_ds(100_000).await;
	setup_cascade(&ds, &ses, 10, 50).await;
	run(&ds, &ses, "DELETE document:doc1;").await;
	let res = ds.execute("SELECT count() FROM chunk GROUP ALL;", &ses, None).await.unwrap();
	let v = format!("{:?}", res.into_iter().next().unwrap().result.unwrap());
	assert!(v.contains("Int(0)"), "cascade should have removed all chunks: {v}");
}

/// The guard applies inside explicit BEGIN/COMMIT blocks too.
#[tokio::test(flavor = "multi_thread")]
async fn write_guard_applies_in_begin_blocks() {
	let (ds, ses) = guarded_ds(200).await;
	setup_cascade(&ds, &ses, 10, 50).await;
	let err = run_capture_err(&ds, &ses, "BEGIN; DELETE document:doc1; COMMIT;")
		.await
		.expect("over-limit delete inside a block should fail");
	assert!(err.contains("maximum number of key writes"), "unexpected error: {err}");
	assert_intact(&ds, &ses, 10).await;
}

/// Each range delete charges one write against the guard, so a guarded
/// transaction cannot issue unbounded range deletes (their per-key expansion
/// is invisible at this layer, but the operations themselves are counted).
#[tokio::test(flavor = "multi_thread")]
async fn write_guard_charges_range_deletes() {
	// No datastore config: the guard is armed directly on the transaction.
	let ds = Datastore::new("memory").await.unwrap();
	let tx = ds
		.transaction(TransactionType::Write)
		.await
		.unwrap()
		.with_write_keys_limit(std::num::NonZeroU64::new(2));
	let range = |a: &str, b: &str| crate::key::RawRange::of_bytes(a.as_bytes(), b.as_bytes());
	tx.delr(range("za", "zb")).await.unwrap();
	tx.delr(range("zb", "zc")).await.unwrap();
	let err = tx.delr(range("zc", "zd")).await.expect_err("third range delete should trip");
	assert!(
		err.to_string().contains("maximum number of key writes (2)"),
		"unexpected error: {err}"
	);
	tx.cancel().await.unwrap();
}

/// Commit-time changefeed writes are part of the transaction's write set and
/// count against the guard: a statement whose per-record writes fit the
/// limit still fails (and rolls back) when its changefeed entries push the
/// buffered write set past the limit.
#[tokio::test(flavor = "multi_thread")]
async fn write_guard_counts_changefeed_writes() {
	let (ds, ses) = guarded_ds(4).await;
	// Two changefeed tables produce multiple commit-time feed entries, all
	// of which must flow through the sequential guarded path.
	run(&ds, &ses, "DEFINE TABLE t CHANGEFEED 1h; DEFINE TABLE u CHANGEFEED 1h;").await;
	// The guard trips at commit time (store_changes), so inside a block the
	// limit error is reported on the COMMIT slot while earlier statements
	// report "not executed" — collect every error and match any of them.
	let errs: Vec<String> = ds
		.execute("BEGIN; CREATE |t:2| RETURN NONE; CREATE |u:2| RETURN NONE; COMMIT;", &ses, None)
		.await
		.unwrap()
		.into_iter()
		.filter_map(|r| r.result.err().map(|e| e.to_string()))
		.collect();
	assert!(
		errs.iter().any(|e| e.contains("maximum number of key writes (4)")),
		"expected the guard error among the block errors: {errs:?}"
	);
	let res = ds.execute("SELECT count() FROM t GROUP ALL;", &ses, None).await.unwrap();
	let v = format!("{:?}", res.into_iter().next().unwrap().result.unwrap());
	assert!(
		v.contains("Int(0)") || v == "Array(Array([]))",
		"rolled-back create left records: {v}"
	);
}

/// The record-access clause evaluator (owner-defined SIGNUP / SIGNIN /
/// AUTHENTICATE expressions, reachable by unauthenticated callers) is
/// statement execution in everything but name and is guarded like one.
#[tokio::test(flavor = "multi_thread")]
async fn write_guard_applies_to_record_access_clauses() {
	let (ds, ses) = guarded_ds(5).await;
	run(&ds, &ses, "DEFINE TABLE t;").await;
	let clause: crate::expr::Expr =
		crate::syn::expr("{ CREATE |t:50| RETURN NONE; RETURN 1; }").unwrap().into();
	let err =
		ds.evaluate(&clause, &ses, None).await.expect_err("an over-limit access clause must fail");
	assert!(
		err.to_string().contains("maximum number of key writes (5)"),
		"unexpected error: {err}"
	);
	// The failed clause left nothing behind.
	let res = ds.execute("SELECT count() FROM t GROUP ALL;", &ses, None).await.unwrap();
	let v = format!("{:?}", res.into_iter().next().unwrap().result.unwrap());
	assert!(
		v.contains("Int(0)") || v == "Array(Array([]))",
		"rolled-back clause left records: {v}"
	);
}

/// Statements executed on an explicit client-owned transaction (the RPC
/// `begin` path and SDK-managed transactions) are guarded like any other
/// statement: wrapping a statement in an external transaction cannot bypass
/// the configured limit.
#[tokio::test(flavor = "multi_thread")]
async fn write_guard_applies_to_external_transactions() {
	let (ds, ses) = guarded_ds(200).await;
	setup_cascade(&ds, &ses, 10, 50).await;
	let tx = Arc::new(ds.transaction(TransactionType::Write).await.unwrap());
	let results = ds
		.execute_with_transaction("DELETE document:doc1;", &ses, None, Arc::clone(&tx))
		.await
		.unwrap();
	let errs: Vec<String> =
		results.into_iter().filter_map(|r| r.result.err().map(|e| e.to_string())).collect();
	assert!(
		errs.iter().any(|e| e.contains("maximum number of key writes (200)")),
		"expected the guard error on the external transaction: {errs:?}"
	);
	tx.cancel().await.unwrap();
	assert_intact(&ds, &ses, 10).await;
}

/// A tripped guard poisons the transaction: after an over-limit statement
/// on a client-owned transaction, an explicit COMMIT is refused (rolling
/// back instead), so the partial statement can never be persisted.
#[tokio::test(flavor = "multi_thread")]
async fn write_guard_poisons_external_transactions_on_trip() {
	let (ds, ses) = guarded_ds(200).await;
	setup_cascade(&ds, &ses, 10, 50).await;
	let tx = Arc::new(ds.transaction(TransactionType::Write).await.unwrap());
	let results = ds
		.execute_with_transaction("DELETE document:doc1;", &ses, None, Arc::clone(&tx))
		.await
		.unwrap();
	assert!(
		results.into_iter().any(|r| r.result.is_err()),
		"the over-limit statement should have failed"
	);
	// The client ignores the error and commits anyway: the commit must be
	// refused with the guard error and roll the transaction back.
	let err = tx.commit().await.expect_err("commit of a poisoned transaction must fail");
	assert!(
		err.to_string().contains("maximum number of key writes (200)"),
		"unexpected commit error: {err}"
	);
	// Nothing from the partial cascade was persisted.
	assert_intact(&ds, &ses, 10).await;
}

/// Custom API handlers (`DEFINE API`, invoked over HTTP) evaluate
/// owner-defined expressions on behalf of external callers and are guarded
/// like any other statement execution: an over-limit handler fails and
/// leaves nothing behind.
#[tokio::test(flavor = "multi_thread")]
async fn write_guard_applies_to_api_handlers() {
	use crate::api::request::ApiRequest;
	use crate::catalog::ApiMethod;
	let (ds, ses) = guarded_ds(5).await;
	run(
		&ds,
		&ses,
		r#"
		DEFINE TABLE t;
		DEFINE API "/spam" FOR get PERMISSIONS FULL THEN {
			CREATE |t:50| RETURN NONE;
			{ status: 200 };
		};
		"#,
	)
	.await;
	let req = ApiRequest {
		method: ApiMethod::Get,
		request_id: "issue-715-guard".to_string(),
		..Default::default()
	};
	let err = ds
		.invoke_api_handler("test", "test", "spam", &ses, req)
		.await
		.expect_err("an over-limit API handler must fail");
	assert!(
		err.to_string().contains("maximum number of key writes (5)"),
		"unexpected error: {err}"
	);
	// The failed handler left nothing behind.
	let res = ds.execute("SELECT count() FROM t GROUP ALL;", &ses, None).await.unwrap();
	let v = format!("{:?}", res.into_iter().next().unwrap().result.unwrap());
	assert!(
		v.contains("Int(0)") || v == "Array(Array([]))",
		"rolled-back handler left records: {v}"
	);
}

/// Writes issued concurrently on one transaction (as graph-pointer
/// maintenance does with `try_join!`) each reserve a distinct slot, so the
/// limit holds under any interleaving: with limit 2, joining three writes
/// must fail with the guard error rather than admitting all three.
#[tokio::test(flavor = "multi_thread")]
async fn write_guard_holds_under_concurrent_writes() {
	// No datastore config: the guard is armed directly on the transaction.
	let ds = Datastore::new("memory").await.unwrap();
	let tx = ds
		.transaction(TransactionType::Write)
		.await
		.unwrap()
		.with_write_keys_limit(std::num::NonZeroU64::new(2));
	let key = |s: &str| Key::from(s.as_bytes().to_vec());
	let res = futures::try_join!(
		tx.set(key("zz-a"), vec![1u8]),
		tx.set(key("zz-b"), vec![1u8]),
		tx.set(key("zz-c"), vec![1u8]),
	);
	let err = res.expect_err("three concurrent writes must not fit a limit of two");
	assert!(
		err.to_string().contains("maximum number of key writes (2)"),
		"unexpected error: {err}"
	);
	tx.cancel().await.unwrap();
}

/// The guard error surfaces to clients as a query error, not as an internal
/// server error: it is an expected, caller-actionable limit violation.
#[test]
fn write_guard_error_is_a_query_error() {
	let err = crate::kvs::DatastoreError::TransactionWriteKeysExceeded {
		limit: 5,
	};
	let types_err = common::LeafError::to_types_error(err);
	assert_eq!(
		types_err.kind_str(),
		"Query",
		"guard error must classify as a query error, got: {types_err:?}"
	);
}

/// Internal transactions (created directly, not by the executor) are never
/// guarded: maintenance work such as index builds and compaction must not be
/// bounded by the statement limit.
#[tokio::test(flavor = "multi_thread")]
async fn write_guard_ignores_internal_transactions() {
	let (ds, ses) = guarded_ds(5).await;
	// An internal transaction writes far past the limit without error.
	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for i in 0..100u32 {
			let key = Key::from(format!("zz-guard-test-{i}").into_bytes());
			tx.set(key, vec![1u8]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}
	// While a statement transaction on the same datastore is still guarded.
	let err = run_capture_err(&ds, &ses, "CREATE |t:10| RETURN NONE;")
		.await
		.expect("over-limit statement should fail");
	assert!(err.contains("maximum number of key writes (5)"), "unexpected error: {err}");
}

/// `commit_bare` skips the close-time actions and the index-delta flush, which is
/// what the maintenance transactions that use it need. It must not become a way
/// around the poison: a client-owned transaction whose statement tripped the guard
/// holds a partial statement either way.
#[tokio::test(flavor = "multi_thread")]
async fn write_guard_poison_refuses_a_bare_commit() {
	let (ds, ses) = guarded_ds(200).await;
	setup_cascade(&ds, &ses, 10, 50).await;
	let tx = Arc::new(ds.transaction(TransactionType::Write).await.unwrap());
	let results = ds
		.execute_with_transaction("DELETE document:doc1;", &ses, None, Arc::clone(&tx))
		.await
		.unwrap();
	assert!(
		results.into_iter().any(|r| r.result.is_err()),
		"the over-limit statement should have failed"
	);
	let err = tx.commit_bare().await.expect_err("a bare commit must be refused too");
	assert!(
		err.to_string().contains("maximum number of key writes (200)"),
		"unexpected bare-commit error: {err}"
	);
	tx.cancel().await.unwrap();
	assert_intact(&ds, &ses, 10).await;
}

/// Buffered index deltas describe writes the transaction is about to make durable,
/// and `commit_bare` does not store them. Committing the writes without them would
/// leave an index disagreeing with the records for good, so the transactions that
/// commit bare are the ones that buffer nothing, and this refuses the rest rather
/// than dropping what they buffered.
#[tokio::test(flavor = "multi_thread")]
async fn a_bare_commit_refuses_buffered_index_deltas() {
	use surrealdb_datastore::values::fulltext::DocLengthAndCount;

	use crate::catalog::{DatabaseId, IndexId, NamespaceId};

	let ds = Datastore::new("memory").await.unwrap();
	let key = |s: &str| Key::from(s.as_bytes().to_vec());

	// Nothing buffered: the maintenance shape, which must still commit.
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	tx.set(key("zz-bare"), vec![1u8]).await.unwrap();
	tx.commit_bare().await.expect("a transaction buffering nothing commits bare");

	// Every family the buffer holds, one at a time: each on its own is a flush the
	// bare commit would skip, so each on its own has to refuse it.
	let ns = NamespaceId(1);
	let db = DatabaseId(2);
	let tb = "t".into();
	let ix = IndexId(3);
	let nid = uuid::Uuid::nil();
	let stats = DocLengthAndCount {
		total_docs_length: 10,
		doc_count: 1,
	};

	for family in ["counts", "compactions", "term_changes", "doc_stats"] {
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		tx.set(key(&format!("zz-bare-{family}")), vec![1u8]).await.unwrap();
		match family {
			"counts" => tx.buffer_count_delta(ns, db, &tb, ix, 1, nid),
			"compactions" => tx.buffer_compaction_trigger(ns, db, &tb, ix, nid),
			"term_changes" => {
				tx.buffer_term_change(ns, db, &tb, ix, "hello", 7, true, nid).unwrap()
			}
			_ => tx.buffer_doc_stats(ns, db, &tb, ix, stats, nid),
		}
		let err = tx.commit_bare().await.expect_err("a buffered family must refuse a bare commit");
		assert!(
			err.to_string().contains("buffered index deltas"),
			"unexpected bare-commit error for {family}: {err}"
		);
		tx.cancel().await.unwrap();
	}
}
