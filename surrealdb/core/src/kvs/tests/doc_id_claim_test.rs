//! A record's doc-ID mapping is claimed with a blind `set` where the backend's
//! commit rejects an overlapping write (`Transactable::blind_write_conflicts`),
//! and with an existence-checked `put` where it does not. Either way two
//! writers that race for the same record, unique index value or doc-ID mapping
//! end as they do on a backend without the capability: the first commit lands,
//! the second fails with the error the check-and-write path gives it.
//!
//! Each race runs on SurrealMX under snapshot and serializable snapshot
//! isolation and on SurrealKV, once with the backend's own answer and once with
//! the capability forced off, and the two outcomes must be identical.

#![cfg(any(feature = "kv-mem", feature = "kv-surrealkv"))]
#![allow(clippy::unwrap_used)]

use std::borrow::Cow;
use std::sync::Arc;

use surrealdb_kvs::Isolation;
use surrealdb_kvs::TransactionType::Write;
use surrealdb_types::ToSql;

use super::latency::{Class, LatencyComposer, LatencyStats};
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::dbs::Session;
use crate::idx::docids::{DocId, TableDocIds};
use crate::key::schema::{DocKeyPrefix, DocLookupIdentityPrefix};
use crate::kvs::{Datastore, QueryRequest, Transaction};
use crate::val::{RecordIdKey, TableName};

#[derive(Clone, Copy, Debug)]
enum Backend {
	#[cfg(feature = "kv-mem")]
	MemorySnapshot,
	#[cfg(feature = "kv-mem")]
	MemorySerializable,
	#[cfg(feature = "kv-surrealkv")]
	SurrealKv,
}

fn backends() -> Vec<Backend> {
	vec![
		#[cfg(feature = "kv-mem")]
		Backend::MemorySnapshot,
		#[cfg(feature = "kv-mem")]
		Backend::MemorySerializable,
		#[cfg(feature = "kv-surrealkv")]
		Backend::SurrealKv,
	]
}

struct Rig {
	ds: Arc<Datastore>,
	ses: Session,
	stats: Arc<LatencyStats>,
	isolation: Isolation,
	/// Keeps a SurrealKV store's directory alive.
	_dir: Option<temp_dir::TempDir>,
}

/// A datastore on `backend`, set up with `setup`, whose transactions answer
/// `blind_write_conflicts` as the backend does, or `false` when `forced_off`.
async fn rig(backend: Backend, forced_off: bool, setup: &str) -> Rig {
	let stats = LatencyStats::new();
	stats.force_blind_write_conflicts_off(forced_off);
	let (path, dir, isolation) = match backend {
		#[cfg(feature = "kv-mem")]
		Backend::MemorySnapshot => ("memory".to_owned(), None, Isolation::Snapshot),
		#[cfg(feature = "kv-mem")]
		Backend::MemorySerializable => ("memory".to_owned(), None, Isolation::Serializable),
		#[cfg(feature = "kv-surrealkv")]
		Backend::SurrealKv => {
			let dir = temp_dir::TempDir::new().unwrap();
			let path = format!("surrealkv:{}", dir.path().to_string_lossy());
			(path, Some(dir), Isolation::Snapshot)
		}
	};
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_factory_path(&path, LatencyComposer(Arc::clone(&stats)))
		.await
		.unwrap();
	let ses = Session::owner().with_ns("t").with_db("t");
	ds.execute("DEFINE NAMESPACE t; DEFINE DATABASE t;", &ses, None).await.unwrap();
	for r in ds.execute(setup, &ses, None).await.unwrap() {
		r.result.unwrap();
	}
	Rig {
		ds,
		ses,
		stats,
		isolation,
		_dir: dir,
	}
}

/// What one writer saw: the answer of each of its statements, then its commit.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Outcome {
	statements: Vec<Result<String, String>>,
	commit: Result<(), String>,
}

impl Outcome {
	fn committed(&self) -> bool {
		self.commit.is_ok()
	}
}

async fn open(rig: &Rig) -> Arc<Transaction> {
	Arc::new(rig.ds.transaction_at(Write, rig.isolation).await.unwrap())
}

async fn statements(rig: &Rig, txn: &Arc<Transaction>, sql: &str) -> Vec<Result<String, String>> {
	let request = QueryRequest::new(sql, &rig.ses).with_transaction(Arc::clone(txn));
	rig.ds
		.run(request)
		.await
		.unwrap()
		.into_iter()
		.map(|r| r.result.map(|v| v.to_sql()).map_err(|e| e.to_string()))
		.collect()
}

async fn finish(txn: &Arc<Transaction>, statements: Vec<Result<String, String>>) -> Outcome {
	let commit = if statements.iter().any(Result::is_err) {
		let _ = txn.cancel().await;
		Err("a statement failed".to_owned())
	} else {
		txn.commit().await.map_err(|e| e.to_string())
	};
	Outcome {
		statements,
		commit,
	}
}

/// Run `first` and `second` in two transactions opened before either commits,
/// then commit `first`, then `second`.
async fn race(rig: &Rig, first: &str, second: &str) -> (Outcome, Outcome) {
	let (a, b) = (open(rig).await, open(rig).await);
	let ran_a = statements(rig, &a, first).await;
	let ran_b = statements(rig, &b, second).await;
	let first = finish(&a, ran_a).await;
	let second = finish(&b, ran_b).await;
	(first, second)
}

/// Run `first` to its commit, then `second` in a transaction opened after it.
async fn one_after_the_other(rig: &Rig, first: &str, second: &str) -> (Outcome, Outcome) {
	let a = open(rig).await;
	let ran_a = statements(rig, &a, first).await;
	let first = finish(&a, ran_a).await;
	let b = open(rig).await;
	let ran_b = statements(rig, &b, second).await;
	let second = finish(&b, ran_b).await;
	(first, second)
}

async fn rows(rig: &Rig, sql: &str) -> String {
	let mut out = String::new();
	for r in rig.ds.execute(sql, &rig.ses, None).await.unwrap() {
		out.push_str(&r.result.unwrap().to_sql());
	}
	out
}

/// Run `scenario` on every backend with the capability as the backend answers
/// it and forced off, and require the two to agree; answers what each
/// returned, keyed by backend.
async fn same_with_and_without_the_capability<F, Fut, T>(setup: &str, scenario: F)
where
	F: Fn(Rig) -> Fut,
	Fut: std::future::Future<Output = T>,
	T: std::fmt::Debug + PartialEq,
{
	for backend in backends() {
		let on = scenario(rig(backend, false, setup).await).await;
		let off = scenario(rig(backend, true, setup).await).await;
		assert_eq!(on, off, "{backend:?}: the outcome differs with the capability forced off");
	}
}

const UNIQUE: &str = "DEFINE TABLE t SCHEMALESS; DEFINE INDEX u ON t FIELDS email UNIQUE; \
					  DEFINE INDEX a ON t FIELDS age;";

#[tokio::test(flavor = "multi_thread")]
async fn two_concurrent_creates_of_one_id_commit_once() {
	same_with_and_without_the_capability(UNIQUE, |rig| async move {
		let (first, second) =
			race(&rig, "CREATE t:1 SET v = 1; RETURN 1;", "CREATE t:1 SET v = 2; RETURN 2;").await;
		assert!(first.committed(), "the first commit lands: {first:?}");
		assert!(!second.committed(), "the second commit fails: {second:?}");
		assert_eq!(rows(&rig, "SELECT VALUE v FROM t").await, "[1]");
		(first, second)
	})
	.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_create_of_a_committed_id_is_refused_as_already_existing() {
	same_with_and_without_the_capability(UNIQUE, |rig| async move {
		let (first, second) =
			one_after_the_other(&rig, "CREATE t:1 SET v = 1;", "CREATE t:1 SET v = 2;").await;
		assert!(first.committed(), "{first:?}");
		let Err(error) = &second.statements[0] else {
			panic!("the second create is refused by the record check: {second:?}");
		};
		assert!(error.contains("already exists"), "{error}");
		(first, second)
	})
	.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn two_concurrent_creates_of_one_unique_value_commit_once() {
	same_with_and_without_the_capability(UNIQUE, |rig| async move {
		let (first, second) = race(
			&rig,
			"CREATE t:1 SET email = 'a@x.io', age = 1;",
			"CREATE t:2 SET email = 'a@x.io', age = 2;",
		)
		.await;
		assert!(first.committed(), "the first commit lands: {first:?}");
		assert!(!second.committed(), "the second commit fails: {second:?}");
		assert_eq!(rows(&rig, "SELECT VALUE id FROM t WHERE email = 'a@x.io'").await, "[t:1]");
		(first, second)
	})
	.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_create_of_a_committed_unique_value_is_refused_by_the_index() {
	same_with_and_without_the_capability(UNIQUE, |rig| async move {
		let (first, second) = one_after_the_other(
			&rig,
			"CREATE t:1 SET email = 'a@x.io', age = 1;",
			"CREATE t:2 SET email = 'a@x.io', age = 2;",
		)
		.await;
		assert!(first.committed(), "{first:?}");
		let Err(error) = &second.statements[0] else {
			panic!("the second create is refused by the unique index: {second:?}");
		};
		assert!(error.contains("already contains"), "{error}");
		(first, second)
	})
	.await;
}

/// A delete and a re-create of one record, each in its own transaction, race
/// for the record's `!dj` mapping: it is deleted by one and claimed again by
/// the other. Whichever commits first stands, the other fails, and the store
/// holds one consistent state, its index entries agreeing with its record.
#[tokio::test(flavor = "multi_thread")]
async fn a_create_and_a_delete_of_one_doc_id_race_to_one_state() {
	const DELETE: &str = "DELETE t:1;";
	const RECREATE: &str = "DELETE t:1; CREATE t:1 SET email = 'b@x.io', age = 2;";
	for (delete_first, label) in [(true, "delete first"), (false, "re-create first")] {
		same_with_and_without_the_capability(UNIQUE, move |rig| async move {
			for r in rig
				.ds
				.execute("CREATE t:1 SET email = 'a@x.io', age = 1;", &rig.ses, None)
				.await
				.unwrap()
			{
				r.result.unwrap();
			}
			let (first, second) = match delete_first {
				true => race(&rig, DELETE, RECREATE).await,
				false => race(&rig, RECREATE, DELETE).await,
			};
			assert!(first.committed(), "{label}: the first commit lands: {first:?}");
			assert!(!second.committed(), "{label}: the second commit fails: {second:?}");
			let table = rows(&rig, "SELECT VALUE email FROM t").await;
			let by_email = rows(&rig, "SELECT VALUE email FROM t WHERE email = 'b@x.io'").await;
			let by_age = rows(&rig, "SELECT VALUE email FROM t WHERE age = 2").await;
			let old = rows(&rig, "SELECT VALUE email FROM t WHERE email = 'a@x.io'").await;
			match delete_first {
				true => {
					assert_eq!(table, "[]", "{label}");
					assert_eq!(
						(by_email.as_str(), by_age.as_str(), old.as_str()),
						("[]", "[]", "[]")
					);
				}
				false => {
					assert_eq!(table, "['b@x.io']", "{label}");
					assert_eq!(by_email, table, "{label}");
					assert_eq!(by_age, table, "{label}");
					assert_eq!(old, "[]", "{label}");
				}
			}
			(first, second, table)
		})
		.await;
	}
}

/// Where the backend's commit rejects an overlapping write, a record's first
/// doc-ID mapping is claimed with a blind `set`; where it does not, with a
/// `put` whose existence check is its own operation.
#[tokio::test(flavor = "multi_thread")]
async fn the_doc_id_claim_is_blind_only_with_the_capability() {
	for backend in backends() {
		for forced_off in [false, true] {
			let rig = rig(backend, forced_off, UNIQUE).await;
			rig.stats.set_enabled(true);
			rig.stats.set_tracing(true);
			rig.ds.execute("CREATE t:1 SET v = 1;", &rig.ses, None).await.unwrap();
			rig.stats.set_enabled(false);
			let claim = rig
				.stats
				.take_trace()
				.into_iter()
				.find(|e| e.key.contains("!dj") && e.class != Class::Get)
				.unwrap_or_else(|| panic!("{backend:?}: no write of a forward mapping"));
			let want = match forced_off {
				true => Class::CheckWrite,
				false => Class::BlindWrite,
			};
			assert_eq!(claim.class, want, "{backend:?} forced_off={forced_off}");
		}
	}
}

/// A record that exists with no doc-ID mapping, in a table with no index to give it one.
const PLAIN: &str = "DEFINE TABLE t SCHEMALESS; CREATE t:1 SET v = 1;";

async fn table_ids(rig: &Rig) -> (NamespaceId, DatabaseId) {
	let txn = rig.ds.transaction(surrealdb_kvs::TransactionType::Read).await.unwrap();
	let db = txn.get_db_by_name("t", "t", None).await.unwrap().unwrap();
	txn.cancel().await.unwrap();
	(db.namespace_id, db.database_id)
}

/// A write context over a transaction opened now, on the rig's isolation level.
async fn write_context(rig: &Rig) -> crate::ctx::FrozenContext {
	let mut ctx = rig.ds.setup_ctx().unwrap();
	ctx.set_transaction(Arc::new(open_owned(rig).await));
	ctx.freeze()
}

async fn open_owned(rig: &Rig) -> Transaction {
	rig.ds.transaction_at(Write, rig.isolation).await.unwrap()
}

/// How many forward (`!dj`) and reverse (`!dd`) mappings table `t` holds.
async fn mapping_counts(rig: &Rig) -> (usize, usize) {
	let (ns, db) = table_ids(rig).await;
	let tb = TableName::from("t");
	let txn = rig.ds.transaction(surrealdb_kvs::TransactionType::Read).await.unwrap();
	let dj = DocLookupIdentityPrefix::new(ns, db, Cow::Borrowed(&tb)).range().unwrap();
	let dd = DocKeyPrefix::new(ns, db, Cow::Borrowed(&tb)).range().unwrap();
	let counts = (
		txn.keys(dj, u32::MAX, 0, None).await.unwrap().len(),
		txn.keys(dd, u32::MAX, 0, None).await.unwrap().len(),
	);
	txn.cancel().await.unwrap();
	counts
}

/// Two transactions that each claim the doc-ID mapping of one existing record, with nothing
/// else of theirs overlapping (no record write, no index entry), are told apart by the
/// forward mapping alone: the first commit lands, the second fails with the error the
/// existence-checked claim gives it, and one forward and one reverse mapping remain.
#[tokio::test(flavor = "multi_thread")]
async fn two_concurrent_claims_of_one_mapping_commit_once() {
	same_with_and_without_the_capability(PLAIN, |rig| async move {
		assert_eq!(mapping_counts(&rig).await, (0, 0), "the record starts with no mapping");
		let (ns, db) = table_ids(&rig).await;
		let docs = TableDocIds::new(ns, db, TableName::from("t"));
		let key = RecordIdKey::Number(1);
		let (a, b) = (write_context(&rig).await, write_context(&rig).await);
		let a_doc = docs.resolve_or_assign(&a, &key).await.unwrap();
		let b_doc = docs.resolve_or_assign(&b, &key).await.unwrap();
		assert_ne!(a_doc, b_doc, "each claim drew its own id");
		let first = a.tx().commit().await.map_err(|e| e.to_string());
		let second = b.tx().commit().await.map_err(|e| e.to_string());
		assert!(first.is_ok(), "the first commit lands: {first:?}");
		assert!(second.is_err(), "the second commit fails: {second:?}");
		assert_eq!(mapping_counts(&rig).await, (1, 1), "one forward and one reverse mapping");
		let txn = rig.ds.transaction(surrealdb_kvs::TransactionType::Read).await.unwrap();
		assert_eq!(docs.get_doc_id(&txn, &key).await.unwrap(), Some(a_doc));
		txn.cancel().await.unwrap();
		(first, second)
	})
	.await;
}

/// A claim made blind publishes the reverse mapping with it: the doc-ID reads back as the
/// record, in the claiming transaction and in a fresh one after it commits.
#[tokio::test(flavor = "multi_thread")]
async fn a_claimed_doc_id_reads_back_as_its_record() {
	for backend in backends() {
		for forced_off in [false, true] {
			let rig = rig(backend, forced_off, PLAIN).await;
			let (ns, db) = table_ids(&rig).await;
			let docs = TableDocIds::new(ns, db, TableName::from("t"));
			let key = RecordIdKey::Number(1);
			let ctx = write_context(&rig).await;
			let doc: DocId = docs.resolve_or_assign(&ctx, &key).await.unwrap();
			let tx = ctx.tx();
			let label = format!("{backend:?} forced_off={forced_off}");
			assert_eq!(
				docs.get_record_id(&tx, doc).await.unwrap(),
				Some(key.clone()),
				"{label}: in the claiming transaction"
			);
			tx.commit().await.unwrap();
			let txn = rig.ds.transaction(surrealdb_kvs::TransactionType::Read).await.unwrap();
			assert_eq!(
				docs.get_record_id(&txn, doc).await.unwrap(),
				Some(key.clone()),
				"{label}: after the commit"
			);
			assert_eq!(docs.get_doc_id(&txn, &key).await.unwrap(), Some(doc), "{label}");
			txn.cancel().await.unwrap();
		}
	}
}

/// A record created and indexed in one transaction is found by a full-text query through
/// the reverse mapping, in that transaction and after it commits.
#[tokio::test(flavor = "multi_thread")]
async fn a_record_created_and_indexed_in_one_transaction_is_found_by_full_text() {
	const SETUP: &str = "DEFINE ANALYZER simple TOKENIZERS blank; DEFINE TABLE t SCHEMALESS; \
						 DEFINE INDEX ft ON t FIELDS a FULLTEXT ANALYZER simple BM25;";
	const SEARCH: &str = "SELECT VALUE id FROM t WHERE a @@ 'alpha';";
	for backend in backends() {
		let rig = rig(backend, false, SETUP).await;
		let txn = open(&rig).await;
		let ran =
			statements(&rig, &txn, &format!("CREATE t:1 SET a = 'alpha beta'; {SEARCH}")).await;
		assert_eq!(ran[1], Ok("[t:1]".to_owned()), "{backend:?}: in the creating transaction");
		txn.commit().await.unwrap();
		assert_eq!(rows(&rig, SEARCH).await, "[t:1]", "{backend:?}: after the commit");
	}
}
