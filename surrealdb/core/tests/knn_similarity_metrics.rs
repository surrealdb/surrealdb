#![recursion_limit = "256"]
#![deny(clippy::wildcard_enum_match_arm)]
//! KNN under the JACCARD and PEARSON metrics ranks by a distance: the vector
//! identical to the query comes first, at distance zero, and an HNSW index
//! returns the same neighbours at the same distances as a brute-force scan.

use std::sync::Arc;
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{ToSql, Value};
use tokio_util::sync::CancellationToken;

async fn run(ds: &Datastore, session: &Session, sql: &str) -> anyhow::Result<Value> {
	let mut results = ds.execute(sql, session, None).await?;
	Ok(results.pop().expect("one statement result").result?)
}

/// `(id, distance)` rows of a KNN query.
async fn neighbours(
	ds: &Datastore,
	session: &Session,
	sql: &str,
) -> anyhow::Result<Vec<(String, f64)>> {
	let rows = run(ds, session, sql).await?;
	let Value::Array(rows) = rows else {
		anyhow::bail!("unexpected result {}", rows.to_sql())
	};
	rows.iter()
		.map(|row| {
			let Value::Object(row) = row else {
				anyhow::bail!("unexpected row {}", row.to_sql())
			};
			let id = row.get("id").map(|v| v.to_sql()).unwrap_or_default();
			let d = match row.get("d") {
				Some(Value::Number(n)) => n.to_f64().unwrap_or(f64::NAN),
				other => anyhow::bail!("unexpected distance {other:?}"),
			};
			Ok((id, d))
		})
		.collect()
}

/// A deterministic 4-dimensional vector with non-negative integer components.
fn vector(seed: u64) -> [u64; 4] {
	let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
	[0; 4].map(|_| {
		x ^= x >> 29;
		x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
		(x >> 33) % 50
	})
}

fn literal(v: [u64; 4]) -> String {
	format!("[{}]", v.map(|c| c.to_string()).join(", "))
}

async fn nearest_first(metric: &str, vector_type: &str) -> anyhow::Result<()> {
	let ds = Arc::new(Datastore::new("memory").await?);
	let session = Session::owner().with_ns("test").with_db("test");
	run(&ds, &Session::owner(), "DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;")
		.await?;
	run(
		&ds,
		&session,
		&format!(
			"DEFINE INDEX ix ON pts FIELDS v HNSW DIMENSION 4 DIST {metric} TYPE {vector_type};"
		),
	)
	.await?;
	let query = vector(1_000);
	let mut inserts: Vec<String> =
		(0..40u64).map(|i| format!("{{ id: pts:{i}, v: {} }}", literal(vector(i)))).collect();
	inserts.push(format!("{{ id: pts:query_copy, v: {} }}", literal(query)));
	run(&ds, &session, &format!("INSERT INTO pts [{}];", inserts.join(", "))).await?;
	Datastore::index_compaction(Arc::clone(&ds), Duration::from_secs(1), CancellationToken::new())
		.await?;
	let q = literal(query);
	let case = format!("{metric} {vector_type}");
	let brute = neighbours(
		&ds,
		&session,
		&format!(
			"SELECT id, vector::distance::knn() AS d FROM pts WHERE v <|5,{metric}|> {q} ORDER BY d"
		),
	)
	.await?;
	let index = neighbours(
		&ds,
		&session,
		&format!(
			"SELECT id, vector::distance::knn() AS d FROM pts WHERE v <|5,100|> {q} ORDER BY d"
		),
	)
	.await?;
	for (path, rows) in [("brute force", &brute), ("hnsw", &index)] {
		assert_eq!(rows.len(), 5, "{case} {path}: {rows:?}");
		assert_eq!(rows[0].0, "pts:query_copy", "{case} {path}: {rows:?}");
		assert!(rows[0].1.abs() < 1e-9, "{case} {path}: {rows:?}");
		assert!(rows.windows(2).all(|w| w[0].1 <= w[1].1), "{case} {path}: {rows:?}");
	}
	// The same neighbourhood at the same distances, up to the index's vector
	// type rounding and ties between equidistant records.
	for ((_, b), (_, i)) in brute.iter().zip(&index) {
		assert!((b - i).abs() < 1e-6, "{case}: brute force {brute:?} vs hnsw {index:?}");
	}
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn jaccard_knn_ranks_the_identical_vector_first() -> anyhow::Result<()> {
	for vector_type in ["F64", "F32", "I16"] {
		nearest_first("JACCARD", vector_type).await?;
	}
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn pearson_knn_ranks_the_identical_vector_first() -> anyhow::Result<()> {
	for vector_type in ["F64", "F32", "I16"] {
		nearest_first("PEARSON", vector_type).await?;
	}
	Ok(())
}
