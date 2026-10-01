//! A record write stopped part-way through its index fan-out never reaches a
//! commit, whoever issues the commit.
//!
//! A b-tree index writes one entry per combination of a record's column
//! elements, and the fan-out stops mid-record once its query is cancelled or
//! times out. The executor discards the transaction of a statement that fails,
//! but a client-owned transaction survives the error and can still COMMIT, and
//! a `DEFINE API` handler turns the error into a response and commits. Either
//! way the committed state must hold the record with every index entry, or
//! neither.
//!
//! The record fans out into far more entries than its statement's `TIMEOUT`
//! lets it write, so the deadline lands inside the fan-out. The assertions
//! are the consistency the commit must preserve rather than where the deadline
//! landed, which a loaded machine can move.

use std::ops::Bound;
use std::sync::Arc;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider, TableProvider};
use crate::dbs::Session;
use crate::kvs::{Datastore, QueryRequest, TransactionType};
use crate::val::{RecordIdKey, TableName};

/// Elements in each of the record's two indexed arrays.
const SIDE: usize = 1000;
/// The entries the record's index write makes when it runs to completion.
const ENTRIES: usize = SIDE * SIDE;

/// Creates the record under a deadline far shorter than its fan-out takes.
const STOPPED_CREATE: &str = "CREATE t:1 SET a = array::range(0, 1000), b = array::range(0, 1000) \
                              RETURN NONE TIMEOUT 100ms;";

async fn fan_out_ds() -> (Arc<Datastore>, Session) {
	let ds = Datastore::new("memory").await.unwrap();
	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		tx.ensure_ns_db(None, "test", "test").await.unwrap();
		tx.commit().await.unwrap();
	}
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = ds
		.execute("DEFINE TABLE t SCHEMALESS; DEFINE INDEX ab ON t FIELDS a, b;", &ses, None)
		.await
		.unwrap();
	for r in res {
		r.result.unwrap();
	}
	(ds, ses)
}

/// The committed records of `t` and the committed entries of its index.
async fn committed(ds: &Datastore) -> (usize, usize) {
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let db = tx.expect_db_by_name("test", "test").await.unwrap();
	let tb = TableName::from("t");
	let ix = tx.expect_tb_index(db.namespace_id, db.database_id, &tb, "ab").await.unwrap();
	let entries = crate::idx::keys::compute_index_range(
		db.namespace_id,
		db.database_id,
		&ix,
		Bound::Unbounded,
		Bound::Unbounded,
	)
	.unwrap();
	let entries = tx.count(entries, None).await.unwrap();
	let record = tx
		.get_record(db.namespace_id, db.database_id, &tb, &RecordIdKey::Number(1), None)
		.await
		.unwrap();
	tx.cancel().await.unwrap();
	(usize::from(!record.data.is_nullish()), entries)
}

fn assert_consistent((records, entries): (usize, usize)) {
	assert!(
		(records, entries) == (0, 0) || (records, entries) == (1, ENTRIES),
		"committed {records} record(s) beside {entries} of its {ENTRIES} index entries"
	);
}

/// The client ignores the failed statement and commits anyway.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_owned_transaction_cannot_commit_a_record_its_fan_out_left_half_indexed() {
	let (ds, ses) = fan_out_ds().await;
	let tx = Arc::new(ds.transaction(TransactionType::Write).await.unwrap());
	let results = ds
		.run(QueryRequest::new(STOPPED_CREATE, &ses).with_transaction(Arc::clone(&tx)))
		.await
		.unwrap();
	let err = results.into_iter().find_map(|r| r.result.err()).expect("the CREATE should time out");
	assert!(err.to_string().contains("timeout"), "expected a timeout, got {err}");
	// Refused when the deadline landed inside the fan-out; allowed when it
	// landed before the record was written, which leaves nothing to commit.
	let _ = tx.commit().await;
	assert_consistent(committed(&ds).await);
}

/// The handler's error becomes a response, after which the handler commits.
#[tokio::test(flavor = "multi_thread")]
async fn an_api_handler_cannot_commit_a_record_its_fan_out_left_half_indexed() {
	use crate::api::request::ApiRequest;
	use crate::catalog::ApiMethod;
	let (ds, ses) = fan_out_ds().await;
	let res = ds
		.execute(
			&format!(
				r#"DEFINE API "/fan" FOR get PERMISSIONS FULL THEN {{
					{STOPPED_CREATE}
					{{ status: 200 }};
				}};"#
			),
			&ses,
			None,
		)
		.await
		.unwrap();
	for r in res {
		r.result.unwrap();
	}
	let req = ApiRequest {
		method: ApiMethod::Get,
		request_id: "interrupted-fan-out".to_string(),
		..Default::default()
	};
	// Refused when the deadline landed inside the fan-out; a 500 response over
	// an empty table when it landed before the record was written.
	let _ = ds.invoke_api_handler("test", "test", "fan", &ses, req).await;
	assert_consistent(committed(&ds).await);
}
