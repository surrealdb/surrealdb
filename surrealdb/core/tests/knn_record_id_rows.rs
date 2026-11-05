#![recursion_limit = "256"]
#![deny(clippy::wildcard_enum_match_arm)]
//! An index-backed KNN whose rows need only their id builds them from the
//! record ids the search returned instead of reading the records. Its answer
//! is the one the record path gives: the same records, in the same order, at
//! the same distances — over compacted and still-pending index updates,
//! deletions, vector changes, and writes earlier in the same transaction.

use std::sync::Arc;
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{ToSql, Value};
use tokio_util::sync::CancellationToken;

const DIMENSION: usize = 8;

/// A deterministic vector with fractional components.
fn vector(seed: u64) -> String {
	let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
	let components: Vec<String> = (0..DIMENSION)
		.map(|_| {
			x ^= x >> 29;
			x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
			format!("{}f", (x >> 40) as f64 / 1024.0)
		})
		.collect();
	format!("[{}]", components.join(", "))
}

async fn execute(ds: &Datastore, session: &Session, sql: &str) -> anyhow::Result<Vec<Value>> {
	let mut values = Vec::new();
	for response in ds.execute(sql, session, None).await? {
		values.push(response.result?);
	}
	Ok(values)
}

/// The rendering of the last statement's result.
async fn last(ds: &Datastore, session: &Session, sql: &str) -> anyhow::Result<String> {
	let values = execute(ds, session, sql).await?;
	Ok(values.last().expect("a statement result").to_sql())
}

/// The answers of the queries `sqls`, run after `setup` in one block
/// statement: one transaction, so every query searches the same snapshot of
/// the index.
async fn answers(
	ds: &Datastore,
	session: &Session,
	setup: &str,
	sqls: &[String],
) -> anyhow::Result<Vec<String>> {
	let queries: Vec<String> = sqls.iter().map(|q| format!("({q})")).collect();
	let block = format!("{{ {setup} RETURN [{}]; }};", queries.join(", "));
	match execute(ds, session, &block).await?.pop() {
		Some(Value::Array(values)) => Ok(values.iter().map(|v| v.to_sql()).collect()),
		other => anyhow::bail!("unexpected block result {other:?}"),
	}
}

/// After `setup`, the id-only query and its record-path twin (which projects
/// the vector, so the records are read) return the same rows at the same
/// distances. Returns the ids.
async fn assert_same_answer(
	ds: &Datastore,
	session: &Session,
	case: &str,
	setup: &str,
	q: &str,
) -> anyhow::Result<String> {
	let knn = format!("FROM pts WHERE point <|10,64|> {q}");
	let got = answers(
		ds,
		session,
		setup,
		&[
			format!("SELECT id {knn}"),
			format!("SELECT id FROM (SELECT id, point {knn})"),
			format!("SELECT id, vector::distance::knn() AS d {knn} ORDER BY d, id"),
			format!(
				"SELECT id, d FROM (SELECT id, point, vector::distance::knn() AS d {knn}) ORDER BY d, id"
			),
		],
	)
	.await?;
	assert_eq!(got[0], got[1], "{case}: ids");
	assert_eq!(got[2], got[3], "{case}: distances");
	assert_ne!(got[0], "[]", "{case}: the query must return rows");
	Ok(got[0].clone())
}

/// Only the id-only query reports its records unread.
async fn assert_plans(
	ds: &Datastore,
	session: &Session,
	case: &str,
	q: &str,
) -> anyhow::Result<()> {
	let knn = format!("FROM pts WHERE point <|10,64|> {q}");
	let plan = last(ds, session, &format!("EXPLAIN ANALYZE SELECT id {knn};")).await?;
	assert!(plan.contains("records: not read"), "{case}: id-only plan\n{plan}");
	let plan = last(ds, session, &format!("EXPLAIN ANALYZE SELECT id, point {knn};")).await?;
	assert!(!plan.contains("records: not read"), "{case}: record plan\n{plan}");
	Ok(())
}

async fn same_answer_as_the_record_path(index: &str) -> anyhow::Result<()> {
	// No maintenance tasks: the explicit compaction below is the only one, so
	// the records after it stay pending for the whole test.
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
	let session = Session::owner().with_ns("test").with_db("test");
	execute(&ds, &Session::owner(), "DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;")
		.await?;
	execute(&ds, &session, &format!("DEFINE INDEX ix ON pts FIELDS point {index};")).await?;
	let insert = |range: std::ops::Range<u64>| {
		let rows: Vec<String> =
			range.map(|i| format!("{{ id: pts:{i}, point: {} }}", vector(i))).collect();
		format!("INSERT INTO pts [{}];", rows.join(", "))
	};
	// Compacted: the first 300 records are in the graph.
	execute(&ds, &session, &insert(0..300)).await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	// Pending: more records, deletions and vector changes not yet compacted.
	execute(&ds, &session, &insert(300..400)).await?;
	let deleted: Vec<String> = (0..400).step_by(20).map(|i| format!("pts:{i}")).collect();
	execute(&ds, &session, &format!("DELETE {};", deleted.join(", "))).await?;
	for i in (5..400).step_by(40) {
		execute(&ds, &session, &format!("UPDATE pts:{i} SET point = {};", vector(i + 10_000)))
			.await?;
	}

	for seed in 0..10u64 {
		let q = vector(50_000 + seed);
		let case = format!("{index} query {seed}");
		assert_same_answer(&ds, &session, &case, "", &q).await?;
		assert_plans(&ds, &session, &case, &q).await?;
		// The nearest neighbours deleted earlier in the same transaction are
		// returned by neither path.
		let nearest =
			last(&ds, &session, &format!("SELECT VALUE id FROM pts WHERE point <|3,64|> {q};"))
				.await?;
		let case = format!("{case}, in a transaction");
		let ids =
			assert_same_answer(&ds, &session, &case, &format!("DELETE {nearest};"), &q).await?;
		for id in nearest.trim_matches(['[', ']']).split(", ") {
			assert!(
				!ids.contains(&format!("{id} ")),
				"{case}: {id} deleted in the transaction came back: {ids}"
			);
		}
	}
	Ok(())
}

/// Fields the rows do not need neither block the id-only path nor change its
/// answer: a `COMPUTED` field that fails whenever it is evaluated, and field
/// `SELECT` permissions that deny (on a field, a nested field and every
/// element of an array), that read another field, or that fail whenever they
/// are evaluated — for an owner and for a record user.
async fn unneeded_fields_keep_the_id_only_path(index: &str) -> anyhow::Result<()> {
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
	let owner = Session::owner().with_ns("test").with_db("test");
	execute(&ds, &Session::owner(), "DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;")
		.await?;
	execute(
		&ds,
		&owner,
		&format!(
			"DEFINE TABLE pts PERMISSIONS FULL;
			DEFINE INDEX ix ON pts FIELDS point {index};
			DEFINE FIELD broken ON pts COMPUTED <int> 'x';
			DEFINE FIELD hidden ON pts PERMISSIONS FOR select NONE;
			DEFINE FIELD owned ON pts PERMISSIONS FOR select WHERE owner = $auth.id;
			DEFINE FIELD failing ON pts PERMISSIONS FOR select WHERE <int> code > 0;
			DEFINE FIELD tags ON pts TYPE array;
			DEFINE FIELD tags[*] ON pts PERMISSIONS FOR select NONE;
			DEFINE FIELD meta ON pts TYPE object;
			DEFINE FIELD meta.secret ON pts PERMISSIONS FOR select NONE;"
		),
	)
	.await?;
	let rows: Vec<String> = (0..100u64)
		.map(|i| {
			format!(
				"{{ id: pts:{i}, point: {}, hidden: {i}, owned: {i}, owner: user:{}, code: 'x', tags: [{i}], meta: {{ secret: {i} }} }}",
				vector(i),
				i % 3
			)
		})
		.collect();
	execute(&ds, &owner, &format!("INSERT INTO pts [{}] RETURN NONE;", rows.join(", "))).await?;
	let user = Session::for_record(
		"test",
		"test",
		"user",
		surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", 1i64)),
	);
	for (case, session) in [("owner", &owner), ("record user", &user)] {
		for seed in 0..5u64 {
			let q = vector(70_000 + seed);
			let case = format!("{index}, {case}, query {seed}");
			assert_same_answer(&ds, session, &case, "", &q).await?;
			assert_plans(&ds, session, &case, &q).await?;
		}
	}
	Ok(())
}

/// A field `SELECT` permission on `id` itself, the one field a row built from
/// a record id holds, keeps the records read, whatever the query reads of the
/// rows (`id` cannot be `COMPUTED`). For a record user the permission
/// withholds `id` from, the id-only query returns what the record path does.
async fn a_permission_on_id_keeps_the_records_read(index: &str) -> anyhow::Result<()> {
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
	let owner = Session::owner().with_ns("test").with_db("test");
	execute(&ds, &Session::owner(), "DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;")
		.await?;
	execute(
		&ds,
		&owner,
		&format!(
			"DEFINE TABLE user;
			DEFINE TABLE pts PERMISSIONS FULL;
			DEFINE INDEX ix ON pts FIELDS point {index};
			DEFINE FIELD id ON pts PERMISSIONS FOR select WHERE owner = $auth.id;"
		),
	)
	.await?;
	let rows: Vec<String> = (0..100u64)
		.map(|i| format!("{{ id: pts:{i}, point: {}, owner: user:{} }}", vector(i), i % 3))
		.collect();
	execute(&ds, &owner, &format!("INSERT INTO pts [{}] RETURN NONE;", rows.join(", "))).await?;
	let user = Session::for_record(
		"test",
		"test",
		"user",
		surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", 1i64)),
	);
	for seed in 0..5u64 {
		let q = vector(90_000 + seed);
		let knn = format!("FROM pts WHERE point <|10,64|> {q}");
		for query in [format!("SELECT id {knn}"), format!("SELECT count() {knn} GROUP ALL")] {
			let plan = last(&ds, &owner, &format!("EXPLAIN ANALYZE {query};")).await?;
			assert!(!plan.contains("records: not read"), "{index}: {query}\n{plan}");
		}
		let got = answers(
			&ds,
			&user,
			"",
			&[format!("SELECT id {knn}"), format!("SELECT id FROM (SELECT id, point {knn})")],
		)
		.await?;
		assert_eq!(got[0], got[1], "{index}, record user, query {seed}");
		assert_ne!(got[0], "[]", "{index}, record user, query {seed}: rows");
	}
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn hnsw_a_permission_on_id_keeps_the_records_read() -> anyhow::Result<()> {
	a_permission_on_id_keeps_the_records_read(&format!("HNSW DIMENSION {DIMENSION}")).await
}

#[cfg(diskann)]
#[tokio::test(flavor = "multi_thread")]
async fn diskann_a_permission_on_id_keeps_the_records_read() -> anyhow::Result<()> {
	a_permission_on_id_keeps_the_records_read(&format!("DISKANN DIMENSION {DIMENSION}")).await
}

#[tokio::test(flavor = "multi_thread")]
async fn hnsw_unneeded_fields_keep_the_id_only_path() -> anyhow::Result<()> {
	unneeded_fields_keep_the_id_only_path(&format!("HNSW DIMENSION {DIMENSION}")).await
}

#[cfg(diskann)]
#[tokio::test(flavor = "multi_thread")]
async fn diskann_unneeded_fields_keep_the_id_only_path() -> anyhow::Result<()> {
	unneeded_fields_keep_the_id_only_path(&format!("DISKANN DIMENSION {DIMENSION}")).await
}

#[tokio::test(flavor = "multi_thread")]
async fn hnsw_id_only_rows_match_the_record_path() -> anyhow::Result<()> {
	same_answer_as_the_record_path(&format!("HNSW DIMENSION {DIMENSION}")).await
}

#[cfg(diskann)]
#[tokio::test(flavor = "multi_thread")]
async fn diskann_id_only_rows_match_the_record_path() -> anyhow::Result<()> {
	same_answer_as_the_record_path(&format!("DISKANN DIMENSION {DIMENSION}")).await
}
