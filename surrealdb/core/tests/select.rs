mod helpers;

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_types::Value;

use crate::helpers::new_ds;

/// Helper: execute a SELECT and return the result row count.
async fn query_row_count(
	ds: &surrealdb_core::kvs::Datastore,
	sess: &Session,
	sql: &str,
) -> Result<usize> {
	let mut res = ds.execute(sql, sess, None).await?;
	let val = res.remove(0).result?;
	Ok(match &val {
		Value::Array(arr) => arr.len(),
		_ => 0,
	})
}

/// Helper: execute a SELECT count() ... GROUP ALL and return the scalar count.
async fn query_count_value(
	ds: &surrealdb_core::kvs::Datastore,
	sess: &Session,
	sql: &str,
) -> Result<i64> {
	let mut res = ds.execute(sql, sess, None).await?;
	let val = res.remove(0).result?;
	let Value::Array(arr) = val else {
		anyhow::bail!("Expected array result for count query")
	};
	let row = arr.first().ok_or_else(|| anyhow::anyhow!("Expected one row for count query"))?;
	let Value::Object(obj) = row else {
		anyhow::bail!("Expected object row for count query")
	};
	let count_val = obj
		.get("count")
		.or_else(|| obj.get("c"))
		.ok_or_else(|| anyhow::anyhow!("Expected count field in count query result"))?;
	match count_val {
		Value::Number(n) => {
			n.to_int().ok_or_else(|| anyhow::anyhow!("Expected count value convertible to i64"))
		}
		_ => anyhow::bail!("Expected numeric count value"),
	}
}

// ---------------------------------------------------------------------------
// Issue 1 – LIMIT pushdown with expression-valued WHERE bounds
//
// When a WHERE clause contains expressions (e.g. `time::now() - 365d`,
// `math::floor(...)`) rather than literal values, the index analyzer cannot
// push the range bounds into the IndexScan. A residual Filter operator is
// needed above the scan, but LIMIT was incorrectly pushed into the IndexScan
// *before* the Filter, causing too few rows to survive filtering.
//
// This affects **all index types** (datetime, number, string, etc.) whenever
// the WHERE clause uses non-literal expressions as range boundaries.
// ---------------------------------------------------------------------------

/// Regression: ASC range scan with literal bounds on an indexed datetime field.
#[tokio::test(flavor = "multi_thread")]
async fn select_indexed_range_literal_datetime() -> Result<()> {
	let (_, ds) = new_ds("test", "test", true).await?;
	let sess = Session::owner().with_ns("test").with_db("test");

	ds.execute("DEFINE INDEX idx_ts ON test FIELDS ts", &sess, None).await?;

	ds.execute(
		"
		CREATE test:1  SET ts = d'2025-01-01T00:00:00Z';
		CREATE test:2  SET ts = d'2025-02-01T00:00:00Z';
		CREATE test:3  SET ts = d'2025-03-01T00:00:00Z';
		CREATE test:4  SET ts = d'2025-04-01T00:00:00Z';
		CREATE test:5  SET ts = d'2025-05-01T00:00:00Z';
		CREATE test:6  SET ts = d'2025-06-01T00:00:00Z';
		CREATE test:7  SET ts = d'2025-07-01T00:00:00Z';
		CREATE test:8  SET ts = d'2025-08-01T00:00:00Z';
		CREATE test:9  SET ts = d'2025-09-01T00:00:00Z';
		CREATE test:10 SET ts = d'2025-10-01T00:00:00Z';
		",
		&sess,
		None,
	)
	.await?;

	// Both directions should return 5 rows (records 3–7 for ASC, 7–3 for DESC).
	let desc = query_row_count(
		&ds,
		&sess,
		"SELECT id, ts FROM test
		 WHERE ts > d'2025-02-01T00:00:00Z' AND ts < d'2025-09-01T00:00:00Z'
		 ORDER BY ts DESC LIMIT 5",
	)
	.await?;
	let asc = query_row_count(
		&ds,
		&sess,
		"SELECT id, ts FROM test
		 WHERE ts > d'2025-02-01T00:00:00Z' AND ts < d'2025-09-01T00:00:00Z'
		 ORDER BY ts ASC LIMIT 5",
	)
	.await?;

	assert_eq!(desc, 5, "DESC with literal datetime bounds should return 5 rows");
	assert_eq!(asc, 5, "ASC with literal datetime bounds should return 5 rows");
	Ok(())
}

/// Regression: expression-valued bounds on an indexed integer field.
/// Ensures the fix is not specific to datetime indexes.
#[tokio::test(flavor = "multi_thread")]
async fn select_indexed_range_expression_integer() -> Result<()> {
	let (_, ds) = new_ds("test", "test", true).await?;
	let sess = Session::owner().with_ns("test").with_db("test");

	ds.execute("DEFINE INDEX idx_score ON item FIELDS score", &sess, None).await?;

	// Create 100 records with score 1..100
	let mut sql = String::new();
	for i in 1..=100 {
		sql.push_str(&format!("CREATE item:{i} SET score = {i};\n"));
	}
	ds.execute(&sql, &sess, None).await?;

	// Literal bounds – LIMIT can be pushed to IndexScan (range fully consumed).
	let literal_asc = query_row_count(
		&ds,
		&sess,
		"SELECT id, score FROM item
		 WHERE score > 20 AND score < 80
		 ORDER BY score ASC LIMIT 10",
	)
	.await?;

	// Expression bounds – LIMIT must NOT be pushed because residual Filter exists.
	let expr_asc = query_row_count(
		&ds,
		&sess,
		"SELECT id, score FROM item
		 WHERE score > math::floor(20.5) AND score < math::ceil(79.5)
		 ORDER BY score ASC LIMIT 10",
	)
	.await?;

	// Both must return exactly 10 rows.
	assert_eq!(literal_asc, 10, "Literal integer bounds ASC LIMIT 10");
	assert_eq!(expr_asc, 10, "Expression integer bounds ASC LIMIT 10");
	Ok(())
}

/// Regression: expression-valued bounds on an indexed string field.
#[tokio::test(flavor = "multi_thread")]
async fn select_indexed_range_expression_string() -> Result<()> {
	let (_, ds) = new_ds("test", "test", true).await?;
	let sess = Session::owner().with_ns("test").with_db("test");

	ds.execute("DEFINE INDEX idx_name ON item FIELDS name", &sess, None).await?;

	// 26 records: name = "a" .. "z"
	let mut sql = String::new();
	for (i, c) in ('a'..='z').enumerate() {
		sql.push_str(&format!("CREATE item:{i} SET name = '{c}';\n"));
	}
	ds.execute(&sql, &sess, None).await?;

	// Expression bound: string::lowercase('E') evaluates to 'e' at runtime.
	// Range d < name < t → 15 matching rows (e..s inclusive).
	let expr_asc = query_row_count(
		&ds,
		&sess,
		"SELECT id, name FROM item
		 WHERE name > string::lowercase('E') AND name < string::lowercase('T')
		 ORDER BY name ASC LIMIT 5",
	)
	.await?;

	let literal_asc = query_row_count(
		&ds,
		&sess,
		"SELECT id, name FROM item
		 WHERE name > 'e' AND name < 't'
		 ORDER BY name ASC LIMIT 5",
	)
	.await?;

	assert_eq!(literal_asc, 5, "Literal string bounds ASC LIMIT 5");
	assert_eq!(expr_asc, 5, "Expression string bounds ASC LIMIT 5");
	Ok(())
}

/// Regression: LIMIT not pushed when there is a residual filter from a
/// compound-condition WHERE clause where only part is consumed by the index.
#[tokio::test(flavor = "multi_thread")]
async fn select_indexed_partial_filter_limit_not_pushed() -> Result<()> {
	let (_, ds) = new_ds("test", "test", true).await?;
	let sess = Session::owner().with_ns("test").with_db("test");

	// Index on `score` only — the `active` condition is NOT indexed.
	ds.execute("DEFINE INDEX idx_score ON item FIELDS score", &sess, None).await?;

	let mut sql = String::new();
	for i in 1..=100 {
		let active = if i % 2 == 0 {
			"true"
		} else {
			"false"
		};
		sql.push_str(&format!("CREATE item:{i} SET score = {i}, active = {active};\n"));
	}
	ds.execute(&sql, &sess, None).await?;

	// Scores 1–100, active for even scores only → 50 active, 50 inactive.
	// Range score > 10 AND score < 90 = 79 rows, of which 39 are active.
	let rows = query_row_count(
		&ds,
		&sess,
		"SELECT id, score FROM item
		 WHERE score > 10 AND score < 90 AND active = true
		 ORDER BY score ASC LIMIT 15",
	)
	.await?;

	assert_eq!(rows, 15, "Partial index filter + LIMIT should return 15 rows");
	Ok(())
}

// ---------------------------------------------------------------------------
// Issue 2 – WHERE IN + ORDER BY + LIMIT returns too few rows
//
// When the table has a separate index on the ORDER BY column (e.g. `idx_ts`)
// AND a different index on the WHERE column (e.g. `idx_status`), the planner
// may choose the ORDER BY index for scan ordering and incorrectly push LIMIT
// into the IndexScan. The WHERE clause (`status IN [...]`) then becomes a
// residual Filter *above* the scan, but because LIMIT was already applied
// at the scan level, the Filter removes rows producing fewer than LIMIT.
//
// This is the same root cause as Issue 1: LIMIT pushed when FilterAction
// is not FullyConsumed.
// ---------------------------------------------------------------------------

/// Regression: WHERE IN with ORDER BY and LIMIT — single index on status only.
/// With only `idx_status`, the planner must use UnionIndexScan (no ORDER BY
/// index available), so this case works even without the fix.
#[tokio::test(flavor = "multi_thread")]
async fn select_where_in_order_by_limit() -> Result<()> {
	let (_, ds) = new_ds("test", "test", true).await?;
	let sess = Session::owner().with_ns("test").with_db("test");

	ds.execute("DEFINE INDEX idx_status ON item FIELDS status", &sess, None).await?;

	// 30 records: cycling through active/pending/archived (10 each).
	let mut sql = String::new();
	for i in 1..=30 {
		let status = match i % 3 {
			1 => "active",
			2 => "pending",
			_ => "archived",
		};
		sql.push_str(&format!(
			"CREATE item:{i} SET status = '{status}', ts = d'2025-01-{i:02}T00:00:00Z';\n"
		));
	}
	ds.execute(&sql, &sess, None).await?;

	// 20 matching rows (active + pending), LIMIT 10.
	let asc = query_row_count(
		&ds,
		&sess,
		"SELECT id, status, ts FROM item
		 WHERE status IN ['active', 'pending']
		 ORDER BY ts ASC LIMIT 10",
	)
	.await?;
	let desc = query_row_count(
		&ds,
		&sess,
		"SELECT id, status, ts FROM item
		 WHERE status IN ['active', 'pending']
		 ORDER BY ts DESC LIMIT 10",
	)
	.await?;

	assert_eq!(asc, 10, "IN + ASC LIMIT 10 should return 10 rows");
	assert_eq!(desc, 10, "IN + DESC LIMIT 10 should return 10 rows");
	Ok(())
}

/// Regression: WHERE IN + ORDER BY + LIMIT with a **separate ORDER BY index**.
///
/// This is the exact reproduction of Issue 2. With both `idx_status` AND
/// `idx_ts`, the planner is tempted to use `idx_ts` for ORDER BY coverage
/// and push LIMIT into the IndexScan. But the WHERE clause `status IN [...]`
/// is not consumed by `idx_ts`, creating a residual Filter that reduces the
/// result below LIMIT.
#[tokio::test(flavor = "multi_thread")]
async fn select_where_in_separate_order_index_limit() -> Result<()> {
	let (_, ds) = new_ds("test", "test", true).await?;
	let sess = Session::owner().with_ns("test").with_db("test");

	// Two separate indexes — the key to reproducing the bug.
	ds.execute(
		"DEFINE INDEX idx_status ON item FIELDS status;
		 DEFINE INDEX idx_ts ON item FIELDS ts;",
		&sess,
		None,
	)
	.await?;

	// 30 records: cycling through active/pending/archived (10 each).
	let mut sql = String::new();
	for i in 1..=30 {
		let status = match i % 3 {
			1 => "active",
			2 => "pending",
			_ => "archived",
		};
		sql.push_str(&format!(
			"CREATE item:{i} SET status = '{status}', ts = d'2025-01-{i:02}T00:00:00Z';\n"
		));
	}
	ds.execute(&sql, &sess, None).await?;

	// 20 matching rows (active + pending). LIMIT 8 to make failures obvious.
	let asc = query_row_count(
		&ds,
		&sess,
		"SELECT id, status, ts FROM item
		 WHERE status IN ['active', 'pending']
		 ORDER BY ts ASC LIMIT 8",
	)
	.await?;
	let desc = query_row_count(
		&ds,
		&sess,
		"SELECT id, status, ts FROM item
		 WHERE status IN ['active', 'pending']
		 ORDER BY ts DESC LIMIT 8",
	)
	.await?;

	assert_eq!(asc, 8, "IN + separate ORDER index + ASC LIMIT 8 should return 8 rows");
	assert_eq!(desc, 8, "IN + separate ORDER index + DESC LIMIT 8 should return 8 rows");
	Ok(())
}

/// Regression: WHERE IN with compound index, ORDER BY, and LIMIT.
/// This test adds both compound and single-field indexes to mirror
/// real-world schemas where the planner has multiple index choices.
#[tokio::test(flavor = "multi_thread")]
async fn select_where_in_compound_index_order_by_limit() -> Result<()> {
	let (_, ds) = new_ds("test", "test", true).await?;
	let sess = Session::owner().with_ns("test").with_db("test");

	// Multiple indexes: compound + separate ORDER BY index (mirrors benchmark schema).
	ds.execute(
		"DEFINE INDEX idx_status ON item FIELDS status;
		 DEFINE INDEX idx_ts ON item FIELDS ts;
		 DEFINE INDEX idx_status_ts ON item FIELDS status, ts;",
		&sess,
		None,
	)
	.await?;

	let mut sql = String::new();
	for i in 1..=60 {
		let status = match i % 3 {
			1 => "active",
			2 => "pending",
			_ => "archived",
		};
		sql.push_str(&format!(
			"CREATE item:{i} SET status = '{status}', ts = d'2025-01-{:02}T{:02}:00:00Z';\n",
			(i - 1) / 24 + 1,
			i % 24
		));
	}
	ds.execute(&sql, &sess, None).await?;

	// 40 matching rows, LIMIT 15.
	let asc = query_row_count(
		&ds,
		&sess,
		"SELECT id, status, ts FROM item
		 WHERE status IN ['active', 'pending']
		 ORDER BY ts ASC LIMIT 15",
	)
	.await?;
	let desc = query_row_count(
		&ds,
		&sess,
		"SELECT id, status, ts FROM item
		 WHERE status IN ['active', 'pending']
		 ORDER BY ts DESC LIMIT 15",
	)
	.await?;

	assert_eq!(asc, 15, "IN + compound index + ASC LIMIT 15");
	assert_eq!(desc, 15, "IN + compound index + DESC LIMIT 15");
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn select_where_in_order_by_id_limit() -> Result<()> {
	let (_, ds) = new_ds("test", "test", true).await?;
	let sess = Session::owner().with_ns("test").with_db("test");

	ds.execute("DEFINE INDEX idx_status ON item FIELDS status", &sess, None).await?;

	let mut sql = String::new();
	for i in 1..=90 {
		let status = match i % 3 {
			1 => "active",
			2 => "pending",
			_ => "archived",
		};
		sql.push_str(&format!("CREATE item:{i} SET status = '{status}';\n"));
	}
	ds.execute(&sql, &sess, None).await?;

	let asc = query_row_count(
		&ds,
		&sess,
		"SELECT id FROM item
		 WHERE status IN ['active', 'pending']
		 ORDER BY id ASC LIMIT 12",
	)
	.await?;
	let desc = query_row_count(
		&ds,
		&sess,
		"SELECT id FROM item
		 WHERE status IN ['active', 'pending']
		 ORDER BY id DESC LIMIT 12",
	)
	.await?;

	assert_eq!(asc, 12, "WHERE IN + ORDER BY id ASC LIMIT 12 should return 12 rows");
	assert_eq!(desc, 12, "WHERE IN + ORDER BY id DESC LIMIT 12 should return 12 rows");
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn select_count_where_btree_paths() -> Result<()> {
	let (_, ds) = new_ds("test", "test", true).await?;
	let sess = Session::owner().with_ns("test").with_db("test");

	ds.execute(
		"DEFINE INDEX idx_score ON item FIELDS score;
		 DEFINE INDEX idx_unique ON item FIELDS uid UNIQUE;
		 DEFINE INDEX idx_status_score ON item FIELDS status, score;",
		&sess,
		None,
	)
	.await?;

	let mut sql = String::new();
	for i in 1..=120 {
		let status = if i % 2 == 0 {
			"active"
		} else {
			"pending"
		};
		sql.push_str(&format!(
			"CREATE item:{i} SET uid = {i}, status = '{status}', score = {};\n",
			i % 30
		));
	}
	ds.execute(&sql, &sess, None).await?;

	let unique_eq =
		query_count_value(&ds, &sess, "SELECT count() AS c FROM item WHERE uid = 42 GROUP ALL")
			.await?;
	let non_unique_eq =
		query_count_value(&ds, &sess, "SELECT count() AS c FROM item WHERE score = 7 GROUP ALL")
			.await?;
	let non_unique_range = query_count_value(
		&ds,
		&sess,
		"SELECT count() AS c FROM item WHERE score > 10 AND score < 20 GROUP ALL",
	)
	.await?;
	let compound_eq = query_count_value(
		&ds,
		&sess,
		"SELECT count() AS c FROM item WITH INDEX idx_status_score
		 WHERE status = 'active' AND score = 8 GROUP ALL",
	)
	.await?;
	let compound_range = query_count_value(
		&ds,
		&sess,
		"SELECT count() AS c FROM item WITH INDEX idx_status_score
		 WHERE status = 'active' AND score > 5 AND score < 15 GROUP ALL",
	)
	.await?;

	assert_eq!(unique_eq, 1, "Unique equality count should return 1");
	assert_eq!(non_unique_eq, 4, "score=7 appears 4 times in 1..=120 with modulo 30");
	assert_eq!(non_unique_range, 36, "scores 11..19 each appear 4 times");
	assert_eq!(compound_eq, 4, "active+score=8 appears 4 times");
	assert_eq!(compound_range, 20, "active rows for scores 6,8,10,12,14");
	Ok(())
}
