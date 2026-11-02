//! SQL window functions run end to end, from the text of a statement in either front end to the
//! rows the datastore answers.
//!
//! Each answer is checked against what the reference server answers for the same rows: the
//! window computes over every row its select keeps, before the select's `DISTINCT`, `ORDER BY`
//! and `LIMIT`; NULL partition keys form one partition; NULL order keys sort last ascending in
//! PostgreSQL and first ascending in MySQL unless `NULLS FIRST` or `NULLS LAST` says otherwise; and
//! the hidden fields the windows are computed into never reach a row. A window that outgrows its
//! query's memory spills its rows to the temporary store and answers what it answers in memory.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use surrealdb_types::{Object, Value};

use crate::dbs::Session;
use crate::exec::memory::runs::{BreakerKind, read_spec};
use crate::exec::memory::{
	BoxFut, FairGovernor, MemTempStore, MemoryGovernor, RunRead, RunRef, RunWrite, SessionName,
	Sharing, TempStore,
};
use crate::kvs::{Datastore, QueryRequest, QuerySource};

/// The prefix of the hidden fields the planner computes window calls into.
const HIDDEN: &str = "__sql_window_";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dialect {
	Pgsql,
	Mysql,
}

impl Dialect {
	fn source(self, text: &str) -> QuerySource<'_> {
		match self {
			Dialect::Pgsql => QuerySource::pgsql(text),
			Dialect::Mysql => QuerySource::mysql(text),
		}
	}
}

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

const SETUP: &str = "DEFINE NAMESPACE test; DEFINE DATABASE test;
	DEFINE TABLE t SCHEMALESS; DEFINE TABLE e SCHEMALESS; DEFINE TABLE big SCHEMALESS;
	DEFINE TABLE nl SCHEMALESS; DEFINE TABLE nk SCHEMALESS; DEFINE TABLE sp SCHEMALESS;
	DEFINE TABLE clash SCHEMALESS;";

async fn set_up(ds: &Datastore) {
	for result in ds.execute(SETUP, &session(), None).await.expect("the setup runs") {
		result.result.expect("a setup statement");
	}
}

/// A datastore holding the fixture tables, with memory to spare.
async fn datastore(dialect: Dialect) -> Arc<Datastore> {
	let ds = Datastore::builder()
		.with_auth(true)
		.build_with_path("memory")
		.await
		.expect("the datastore opens");
	set_up(&ds).await;
	for insert in [
		"INSERT INTO t (n, s) VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'c'), (5, 'b')",
		"INSERT INTO nl (n, p) VALUES (1, NULL), (2, NULL), (3, 'x'), (4, NULL), (5, 'x')",
		"INSERT INTO nk (n, v) VALUES (1, 1), (2, NULL), (3, 3), (4, NULL), (5, 2)",
	] {
		if let Err(error) = rows(&ds, dialect, insert).await {
			panic!("{dialect:?}: `{insert}` failed: {error}");
		}
	}
	ds
}

/// The rows `text` answers, or the message of its error. A row holding a hidden window field
/// fails the test.
async fn rows(ds: &Datastore, dialect: Dialect, text: &str) -> Result<Vec<Object>, String> {
	let session = session();
	let results = ds
		.run(QueryRequest::new(dialect.source(text), &session))
		.await
		.map_err(|error| error.to_string())?;
	let response = results.into_iter().next().ok_or("the statement answered no result")?;
	let value = response.result.map_err(|error| error.to_string())?;
	let rows = if let Value::Array(rows) = value {
		rows.into_vec()
	} else if matches!(value, Value::None | Value::Null) {
		Vec::new()
	} else {
		vec![value]
	};
	rows.into_iter()
		.map(|row| {
			let Value::Object(object) = row else {
				return Err(format!("a row that is not an object: {row:?}"));
			};
			if let Some(key) = object.keys().find(|key| key.starts_with(HIDDEN)) {
				panic!("{dialect:?}: `{text}` answered a row holding `{key}`: {object:?}");
			}
			Ok(object)
		})
		.collect()
}

/// The `columns` of every row `text` answers, the cells of a row joined by `,` and the rows by
/// `|`, or `ERR:` and the error.
async fn answer(ds: &Datastore, dialect: Dialect, text: &str, columns: &[&str]) -> String {
	match rows(ds, dialect, text).await {
		Ok(rows) => rows
			.iter()
			.map(|row| {
				columns
					.iter()
					.map(|column| row.get(*column).map_or_else(|| "MISSING".to_owned(), render))
					.collect::<Vec<_>>()
					.join(",")
			})
			.collect::<Vec<_>>()
			.join("|"),
		Err(error) => format!("ERR:{error}"),
	}
}

/// [`answer`] with its rows sorted by their first cell, an integer, for a statement whose row order
/// is not defined.
async fn answer_sorted(ds: &Datastore, dialect: Dialect, text: &str, columns: &[&str]) -> String {
	let got = answer(ds, dialect, text, columns).await;
	if got.starts_with("ERR:") || got.is_empty() {
		return got;
	}
	let mut rows: Vec<&str> = got.split('|').collect();
	rows.sort_by_key(|row| row.split(',').next().and_then(|cell| cell.parse::<i64>().ok()));
	rows.join("|")
}

fn render(value: &Value) -> String {
	match value {
		Value::None | Value::Null => "NULL".to_owned(),
		Value::Number(n) => match n.to_f64() {
			Some(f) if f.fract() == 0.0 && f.abs() < 1e15 => format!("{}", f as i64),
			Some(f) => format!("{f}"),
			None => n.to_string(),
		},
		Value::String(s) => s.clone(),
		Value::Bool(_)
		| Value::Datetime(_)
		| Value::Array(_)
		| Value::Object(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Set(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_) => surrealdb_types::ToSql::to_sql(value),
	}
}

/// The fields of the rows `text` answers, but the record id, sorted.
async fn fields(ds: &Datastore, dialect: Dialect, text: &str) -> Vec<String> {
	let rows = rows(ds, dialect, text).await.unwrap_or_else(|e| panic!("`{text}` failed: {e}"));
	let mut fields: Vec<String> =
		rows.iter().flat_map(|row| row.keys().cloned()).filter(|key| key != "id").collect();
	fields.sort();
	fields.dedup();
	fields
}

fn expect(dialect: Dialect, text: &str, got: &str, expected: &str) {
	assert_eq!(got, expected, "{dialect:?}: `{text}`");
}

/// A window beside `*`, beside `t.*`, and with no alias: the rows hold the table's columns and
/// the window's, and no hidden field.
async fn window_beside_star(dialect: Dialect) {
	let ds = datastore(dialect).await;
	let text = "SELECT *, rank() OVER (ORDER BY n) AS r FROM t";
	assert_eq!(fields(&ds, dialect, text).await, ["n", "r", "s"], "{dialect:?}: `{text}`");
	let got = answer_sorted(&ds, dialect, text, &["n", "s", "r"]).await;
	expect(dialect, text, &got, "1,a,1|2,b,2|3,a,3|4,c,4|5,b,5");

	let text = "SELECT t.*, row_number() OVER () AS rn FROM t";
	assert_eq!(fields(&ds, dialect, text).await, ["n", "rn", "s"], "{dialect:?}: `{text}`");
	let mut numbers: Vec<String> =
		answer(&ds, dialect, text, &["rn"]).await.split('|').map(str::to_owned).collect();
	numbers.sort();
	assert_eq!(numbers, ["1", "2", "3", "4", "5"], "{dialect:?}: `{text}`");

	let text = "SELECT n, row_number() OVER (ORDER BY n DESC) FROM t ORDER BY n";
	let names = fields(&ds, dialect, text).await;
	let unaliased: Vec<&String> = names.iter().filter(|name| *name != "n").collect();
	assert_eq!(unaliased, ["row_number"], "{dialect:?}: `{text}`");
	let got = answer(&ds, dialect, text, &["n", unaliased[0]]).await;
	expect(dialect, text, &got, "1,5|2,4|3,3|4,2|5,1");
}

/// A window over the groups of a `GROUP BY`, ordered by an aggregate it selects, by one it does
/// not, and summing an aggregate.
async fn window_over_groups(dialect: Dialect) {
	let ds = datastore(dialect).await;
	let text = "SELECT s, sum(n) AS total, rank() OVER (ORDER BY sum(n) DESC) AS r \
		FROM t GROUP BY s ORDER BY s";
	let got = answer(&ds, dialect, text, &["s", "total", "r"]).await;
	expect(dialect, text, &got, "a,4,2|b,7,1|c,4,2");

	let text = "SELECT s, rank() OVER (ORDER BY count(*) DESC, s DESC) AS r \
		FROM t GROUP BY s ORDER BY s";
	assert_eq!(fields(&ds, dialect, text).await, ["r", "s"], "{dialect:?}: `{text}`");
	let got = answer(&ds, dialect, text, &["s", "r"]).await;
	expect(dialect, text, &got, "a,2|b,1|c,3");

	let text = "SELECT s, sum(sum(n)) OVER () AS grand, \
		sum(count(*)) OVER (ORDER BY s) AS running FROM t GROUP BY s ORDER BY s";
	let got = answer(&ds, dialect, text, &["s", "grand", "running"]).await;
	expect(dialect, text, &got, "a,15,2|b,15,4|c,15,5");

	// A selected column named as a hidden aggregate output keeps its own value.
	let text = "SELECT s AS __sql_having_0, count(*) AS c FROM t GROUP BY s \
		HAVING sum(n) > 4 ORDER BY s";
	let got = answer(&ds, dialect, text, &["__sql_having_0", "c"]).await;
	expect(dialect, text, &got, "b,2");
}

/// Every window over an empty table answers no row and no error.
async fn window_over_nothing(dialect: Dialect) {
	let ds = datastore(dialect).await;
	for text in [
		"SELECT row_number() OVER () AS v FROM e",
		"SELECT n, ntile(3) OVER (ORDER BY n) AS v FROM e",
		"SELECT percent_rank() OVER (ORDER BY n) AS v FROM e",
		"SELECT cume_dist() OVER (PARTITION BY s ORDER BY n) AS v FROM e",
		"SELECT sum(n) OVER () AS v FROM e",
		"SELECT lead(n, 2) OVER (ORDER BY n) AS v FROM e ORDER BY v LIMIT 1",
	] {
		let got = answer(&ds, dialect, text, &["v"]).await;
		expect(dialect, text, &got, "");
	}
}

/// A `LIMIT` above a window cuts the rows the window answered, not the rows it reads.
async fn limit_above_window(dialect: Dialect) {
	let ds = datastore(dialect).await;
	let fill = "FOR $i IN 1..=3000 { CREATE big SET n = $i RETURN NONE; };";
	for result in ds.execute(fill, &session(), None).await.expect("the rows are written") {
		result.result.expect("a row is written");
	}
	let text = "SELECT n, row_number() OVER (ORDER BY n) AS v FROM big ORDER BY n LIMIT 3";
	let got = answer(&ds, dialect, text, &["n", "v"]).await;
	expect(dialect, text, &got, "1,1|2,2|3,3");

	let text = "SELECT n, row_number() OVER (ORDER BY n DESC) AS v, count(*) OVER () AS c \
		FROM big ORDER BY n LIMIT 3";
	let got = answer(&ds, dialect, text, &["n", "v", "c"]).await;
	expect(dialect, text, &got, "1,3000,3000|2,2999,3000|3,2998,3000");

	let text = "SELECT n, sum(n) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND \
		UNBOUNDED FOLLOWING) AS v FROM big ORDER BY n DESC LIMIT 2";
	let got = answer(&ds, dialect, text, &["n", "v"]).await;
	expect(dialect, text, &got, "3000,4501500|2999,4501500");
}

/// NULL partition keys form one partition, and NULL order keys sort where the dialect and
/// `NULLS FIRST` or `NULLS LAST` put them.
async fn nulls_in_keys(dialect: Dialect) {
	let ds = datastore(dialect).await;
	let text = "SELECT n, count(*) OVER (PARTITION BY p) AS c, \
		row_number() OVER (PARTITION BY p ORDER BY n) AS r FROM nl ORDER BY n";
	let got = answer(&ds, dialect, text, &["n", "c", "r"]).await;
	expect(dialect, text, &got, "1,3,1|2,3,2|3,2,1|4,3,3|5,2,2");

	// The rows of `nk` by `n` hold `v` = 1, NULL, 3, NULL, 2.
	let mut cases = vec![
		// MySQL sorts NULL first ascending and last descending; PostgreSQL the other way round.
		(
			"ORDER BY v",
			match dialect {
				Dialect::Pgsql => "1,1|2,4|3,3|4,4|5,2",
				Dialect::Mysql => "1,3|2,1|3,5|4,1|5,4",
			},
		),
		(
			"ORDER BY v DESC",
			match dialect {
				Dialect::Pgsql => "1,5|2,1|3,3|4,1|5,4",
				Dialect::Mysql => "1,3|2,4|3,1|4,4|5,2",
			},
		),
	];
	if dialect == Dialect::Pgsql {
		cases.extend([
			("ORDER BY v ASC NULLS FIRST", "1,3|2,1|3,5|4,1|5,4"),
			("ORDER BY v ASC NULLS LAST", "1,1|2,4|3,3|4,4|5,2"),
			("ORDER BY v DESC NULLS FIRST", "1,5|2,1|3,3|4,1|5,4"),
			("ORDER BY v DESC NULLS LAST", "1,3|2,4|3,1|4,4|5,2"),
		]);
	}
	for (order, expected) in cases {
		let text = format!("SELECT n, rank() OVER ({order}) AS r FROM nk ORDER BY n");
		let got = answer(&ds, dialect, &text, &["n", "r"]).await;
		expect(dialect, &text, &got, expected);
	}
	if dialect == Dialect::Pgsql {
		// The frame of a NULL order key holds its NULL peers.
		let text = "SELECT n, count(*) OVER (ORDER BY v NULLS FIRST) AS c FROM nk ORDER BY n";
		let got = answer(&ds, dialect, text, &["n", "c"]).await;
		expect(dialect, text, &got, "1,3|2,2|3,5|4,2|5,4");
	}
}

/// Two windows of different specifications in one select, a window only in `ORDER BY`, and a
/// `DISTINCT` over a window's column.
async fn several_windows(dialect: Dialect) {
	let ds = datastore(dialect).await;
	let text = "SELECT n, row_number() OVER (ORDER BY n) AS a, \
		row_number() OVER (ORDER BY n DESC) AS b, \
		count(*) OVER (PARTITION BY s) AS c FROM t ORDER BY n";
	let got = answer(&ds, dialect, text, &["n", "a", "b", "c"]).await;
	expect(dialect, text, &got, "1,1,5,2|2,2,4,2|3,3,3,2|4,4,2,1|5,5,1,2");

	let text = "SELECT n FROM t ORDER BY row_number() OVER (ORDER BY n DESC)";
	assert_eq!(fields(&ds, dialect, text).await, ["n"], "{dialect:?}: `{text}`");
	let got = answer(&ds, dialect, text, &["n"]).await;
	expect(dialect, text, &got, "5|4|3|2|1");

	let text = "SELECT n FROM t ORDER BY rank() OVER (PARTITION BY s ORDER BY n), n";
	let got = answer(&ds, dialect, text, &["n"]).await;
	expect(dialect, text, &got, "1|2|4|3|5");

	let text = "SELECT DISTINCT s FROM t ORDER BY s DESC";
	let got = answer(&ds, dialect, text, &["s"]).await;
	expect(dialect, text, &got, "c|b|a");

	let text = "SELECT DISTINCT rank() OVER (ORDER BY s) AS r FROM t";
	let got = answer_sorted(&ds, dialect, text, &["r"]).await;
	expect(dialect, text, &got, "1|3|5");

	let text = "SELECT DISTINCT rank() OVER (ORDER BY s) AS r FROM t ORDER BY r DESC";
	let got = answer(&ds, dialect, text, &["r"]).await;
	expect(dialect, text, &got, "5|3|1");

	let text = "SELECT DISTINCT s, count(*) OVER (PARTITION BY s) AS c FROM t \
		ORDER BY c, s LIMIT 2";
	let got = answer(&ds, dialect, text, &["s", "c"]).await;
	expect(dialect, text, &got, "c,1|a,2");
}

/// A named window referenced by two calls.
async fn named_window(dialect: Dialect) {
	let ds = datastore(dialect).await;
	let text = "SELECT n, row_number() OVER w AS a, sum(n) OVER w AS b FROM t \
		WINDOW w AS (PARTITION BY s ORDER BY n) ORDER BY n";
	let got = answer(&ds, dialect, text, &["n", "a", "b"]).await;
	expect(dialect, text, &got, "1,1,1|2,1,2|3,2,4|4,1,4|5,2,7");

	let text = "SELECT n, first_value(n) OVER w AS f, last_value(n) OVER w AS l FROM t \
		WINDOW w AS (ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ORDER BY n";
	let got = answer(&ds, dialect, text, &["n", "f", "l"]).await;
	expect(dialect, text, &got, "1,1,2|2,1,3|3,2,4|4,3,5|5,4,5");

	// A window that refers to another may add an ORDER BY the other lacks, and changes nothing
	// else it declares.
	let text = "SELECT n, sum(n) OVER (w ORDER BY n) AS v FROM t WINDOW w AS (PARTITION BY s) \
		ORDER BY n";
	let got = answer(&ds, dialect, text, &["n", "v"]).await;
	expect(dialect, text, &got, "1,1|2,2|3,4|4,4|5,7");
	for (text, refusal) in [
		(
			"SELECT n, sum(n) OVER (w PARTITION BY n) AS v FROM t WINDOW w AS (PARTITION BY s)",
			"cannot override PARTITION BY clause of window \"w\"",
		),
		(
			"SELECT n, sum(n) OVER (w ORDER BY s) AS v FROM t WINDOW w AS (ORDER BY n)",
			"cannot override ORDER BY clause of window \"w\"",
		),
		(
			"SELECT n, sum(n) OVER (w ORDER BY n) AS v FROM t \
			 WINDOW w AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
			"cannot copy window \"w\" because it has a frame clause",
		),
		(
			"SELECT n, sum(n) OVER w2 AS v FROM t \
			 WINDOW w AS (PARTITION BY s), w2 AS (w PARTITION BY n)",
			"cannot override PARTITION BY clause of window \"w\"",
		),
	] {
		let got = answer(&ds, dialect, text, &["v"]).await;
		assert!(
			got.starts_with("ERR:") && got.contains(refusal),
			"{dialect:?}: `{text}` answered `{got}`, expected `{refusal}`"
		);
	}
}

/// Window constructs the engine cannot compute are refused when the statement is planned, and
/// a window where none may be is an error.
async fn refusals(dialect: Dialect) {
	let ds = datastore(dialect).await;
	let mut unsupported = vec![
		"SELECT n, sum(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM t",
		"SELECT n, count(DISTINCT n) OVER () AS v FROM t",
	];
	if dialect == Dialect::Pgsql {
		unsupported.extend([
			"SELECT n, sum(n) OVER (ORDER BY n GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v \
			 FROM t",
			"SELECT n, sum(n) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW \
			 EXCLUDE CURRENT ROW) AS v FROM t",
			"SELECT n, lag(n, n - 1) OVER (ORDER BY n) AS v FROM t",
			"SELECT n, lead(n, n) OVER (ORDER BY n) AS v FROM t",
			"SELECT n, nth_value(n, n) OVER (ORDER BY n) AS v FROM t",
			"SELECT n, ntile(n) OVER (ORDER BY n) AS v FROM t",
		]);
	}
	for text in unsupported {
		let got = answer(&ds, dialect, text, &["v"]).await;
		assert!(
			got.starts_with("ERR:") && got.contains("not supported"),
			"{dialect:?}: `{text}` answered `{got}`, expected a refusal"
		);
	}
	// PostgreSQL reads a negative offset the other way; MySQL refuses one.
	let text = "SELECT n, lag(n, -1) OVER (ORDER BY n) AS a, lead(n, -2, 0) OVER (ORDER BY n) AS b \
		FROM t ORDER BY n";
	let got = answer(&ds, dialect, text, &["n", "a", "b"]).await;
	match dialect {
		Dialect::Pgsql => expect(dialect, text, &got, "1,2,0|2,3,0|3,4,1|4,5,2|5,NULL,3"),
		Dialect::Mysql => assert!(
			got.starts_with("ERR:") && got.contains("negative offset"),
			"{dialect:?}: `{text}` answered `{got}`, expected a refusal"
		),
	}
	if dialect == Dialect::Pgsql {
		// A FILTER applies to the rows an aggregate folds; no other function folds rows.
		for text in [
			"SELECT n, lag(n) FILTER (WHERE n > 1) OVER (ORDER BY n) AS v FROM t",
			"SELECT n, first_value(n) FILTER (WHERE n > 1) OVER (ORDER BY n) AS v FROM t",
			"SELECT n, abs(n) FILTER (WHERE n > 1) AS v FROM t",
		] {
			let got = answer(&ds, dialect, text, &["v"]).await;
			assert!(
				got.starts_with("ERR:") && got.contains("FILTER on"),
				"{dialect:?}: `{text}` answered `{got}`, expected a FILTER refusal"
			);
		}
		let text = "SELECT n, sum(n) FILTER (WHERE n > 2) OVER (ORDER BY n) AS v FROM t ORDER BY n";
		let got = answer(&ds, dialect, text, &["n", "v"]).await;
		expect(dialect, text, &got, "1,NULL|2,NULL|3,3|4,7|5,12");
	}
	let text = "SELECT n FROM t WHERE row_number() OVER (ORDER BY n) > 1";
	let got = answer(&ds, dialect, text, &["n"]).await;
	assert!(
		got.starts_with("ERR:") && got.contains("window functions are not allowed in WHERE"),
		"{dialect:?}: `{text}` answered `{got}`, expected a window refusal"
	);
}

/// A window `avg` divides as the `avg` aggregate does, so integers do not divide as integers.
async fn integer_averages(dialect: Dialect) {
	let ds = datastore(dialect).await;
	let text = "SELECT n, avg(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v \
		FROM t ORDER BY n";
	let got = answer(&ds, dialect, text, &["n", "v"]).await;
	expect(dialect, text, &got, "1,1|2,1.5|3,2.5|4,3.5|5,4.5");
	let text = "SELECT n, avg(n) OVER (PARTITION BY s) AS v FROM t ORDER BY n";
	let got = answer(&ds, dialect, text, &["n", "v"]).await;
	expect(dialect, text, &got, "1,2|2,3.5|3,2|4,4|5,3.5");
	// A value that is not a number is skipped, as the aggregates skip it, or refused when the
	// statement is planned; it is never added as the value it is.
	let text = "SELECT n, sum(s) OVER () AS v, avg(s) OVER () AS a FROM t ORDER BY n LIMIT 1";
	let got = answer(&ds, dialect, text, &["n", "v", "a"]).await;
	assert!(
		got == "1,NULL,NULL" || (got.starts_with("ERR:") && !got.contains("Internal error")),
		"{dialect:?}: `{text}` answered `{got}`"
	);
	let window = answer(&ds, dialect, "SELECT avg(n) OVER () AS v FROM t LIMIT 1", &["v"]).await;
	let aggregate = answer(&ds, dialect, "SELECT avg(n) AS v FROM t", &["v"]).await;
	assert_eq!(window, aggregate, "{dialect:?}: a window avg and the avg aggregate differ");
}

/// A `DISTINCT` over groups compares the selected columns, not the group keys, the aggregates
/// only other clauses read, or the fields windows are computed into.
async fn distinct_over_groups(dialect: Dialect) {
	let ds = datastore(dialect).await;
	for (text, expected) in [
		("SELECT DISTINCT rank() OVER (ORDER BY sum(n)) AS v FROM t GROUP BY s", "1|3"),
		("SELECT DISTINCT count(*) AS v FROM t GROUP BY s", "1|2"),
		("SELECT DISTINCT count(*) AS v FROM t GROUP BY s HAVING sum(n) > 0", "1|2"),
		("SELECT DISTINCT sum(n) AS v FROM t GROUP BY s ORDER BY v DESC", "7|4"),
		("SELECT DISTINCT sum(n) AS v FROM t GROUP BY s ORDER BY v LIMIT 1", "4"),
	] {
		let got = if text.contains("ORDER BY v") {
			answer(&ds, dialect, text, &["v"]).await
		} else {
			answer_sorted(&ds, dialect, text, &["v"]).await
		};
		expect(dialect, text, &got, expected);
	}
}

/// A `SELECT DISTINCT` compares only its selected columns in every shape: answering bare values to
/// an array subquery, ordered by an unaliased computed column, and grouped.
async fn distinct_in_every_shape(dialect: Dialect) {
	let ds = datastore(dialect).await;
	let text = "SELECT count(*) AS v FROM (SELECT DISTINCT rank() OVER (ORDER BY s) + 1 FROM t \
		ORDER BY 1) sub";
	let got = answer(&ds, dialect, text, &["v"]).await;
	expect(dialect, text, &got, "3");
	if dialect == Dialect::Pgsql {
		for (text, expected) in [
			(
				"SELECT ARRAY(SELECT DISTINCT rank() OVER (ORDER BY sum(n)) FROM t GROUP BY s \
				 ORDER BY 1) AS v",
				"[1, 3]",
			),
			("SELECT ARRAY(SELECT DISTINCT s FROM t ORDER BY s DESC) AS v", "['c', 'b', 'a']"),
			("SELECT ARRAY(SELECT DISTINCT s FROM t ORDER BY s LIMIT 2) AS v", "['a', 'b']"),
		] {
			let got = answer(&ds, dialect, text, &["v"]).await;
			expect(dialect, text, &got, expected);
		}
	}
	let text = "SELECT DISTINCT s FROM t ORDER BY n";
	let got = answer(&ds, dialect, text, &["s"]).await;
	assert!(
		got.starts_with("ERR:") && got.contains("ORDER BY expressions must appear in select list"),
		"{dialect:?}: `{text}` answered `{got}`"
	);

	// An ORDER BY of a selected column reads the value the row answers, and a selected column
	// named like a hidden field keeps its value.
	let text = "SELECT DISTINCT n AS __sql_distinct_order_0 FROM t ORDER BY n DESC LIMIT 2";
	let got = answer(&ds, dialect, text, &["__sql_distinct_order_0"]).await;
	expect(dialect, text, &got, "5|4");
	let text = "SELECT DISTINCT n % 2 AS __sql_distinct_order_0, n % 3 FROM t \
		ORDER BY n % 3, n % 2";
	let got = rows(&ds, dialect, text).await.expect("the selected columns");
	assert_eq!(got.len(), 5, "{dialect:?}: `{text}`");
	assert!(
		got.iter().all(|row| row.get("__sql_distinct_order_0").is_some()),
		"{dialect:?}: `{text}` lost its selected column: {got:?}"
	);
	let random = match dialect {
		Dialect::Pgsql => "random()",
		Dialect::Mysql => "rand()",
	};
	let text = format!("SELECT DISTINCT n * 0 + {random} AS r FROM t ORDER BY r");
	let text = text.as_str();
	let got = rows(&ds, dialect, text).await.expect("the random column");
	let values: Vec<f64> = got
		.iter()
		.map(|row| {
			let Some(Value::Number(n)) = row.get("r") else {
				panic!("{dialect:?}: `{text}` answered a row without a number: {row:?}");
			};
			n.to_f64().expect("a number")
		})
		.collect();
	assert!(values.windows(2).all(|w| w[0] <= w[1]), "{dialect:?}: `{text}` answered {values:?}");

	// A `*` over a join selects the joined relation's columns, so its rows may be ordered by one.
	let text = "SELECT DISTINCT * FROM t JOIN nl ON nl.n = t.n ORDER BY nl.p DESC, t.n";
	let got = rows(&ds, dialect, text).await.expect("the joined rows");
	assert!(
		got.iter().all(|row| row.keys().all(|key| !key.starts_with("__sql_distinct"))),
		"{dialect:?}: `{text}` answered a hidden field: {got:?}"
	);
	let got = answer(&ds, dialect, text, &["n"]).await;
	expect(
		dialect,
		text,
		&got,
		match dialect {
			Dialect::Pgsql => "1|2|4|3|5",
			Dialect::Mysql => "3|5|1|2|4",
		},
	);
}

/// A window function inside an aggregate, and a window of a grouped select that reads a column the
/// groups do not hold, are errors rather than `NULL`s.
async fn grouping_errors(dialect: Dialect) {
	let ds = datastore(dialect).await;
	for (text, message) in [
		(
			"SELECT sum(row_number() OVER (ORDER BY n)) AS v FROM t",
			"window functions are not allowed in aggregate function calls",
		),
		(
			"SELECT s, rank() OVER (ORDER BY n) AS v FROM t GROUP BY s",
			"must appear in the GROUP BY clause",
		),
		(
			"SELECT s, sum(n) OVER (PARTITION BY s) AS v FROM t GROUP BY s",
			"must appear in the GROUP BY clause",
		),
	] {
		let got = answer(&ds, dialect, text, &["v"]).await;
		assert!(
			got.starts_with("ERR:") && got.contains(message),
			"{dialect:?}: `{text}` answered `{got}`, expected `{message}`"
		);
	}
	let text = "SELECT s, rank() OVER (ORDER BY s) AS v, count(*) OVER () AS c FROM t GROUP BY s \
		ORDER BY s";
	let got = answer(&ds, dialect, text, &["s", "v", "c"]).await;
	expect(dialect, text, &got, "a,1,3|b,2,3|c,3,3");
}

/// A window whose rows could not be computed as written is an error, never a wrong answer: a key
/// that answers differently each time it is evaluated, and a row that holds a field named as a
/// window's hidden field.
async fn inconsistent_rows(dialect: Dialect) {
	let ds = datastore(dialect).await;
	let fill = "FOR $i IN 1..=200 { CREATE big SET n = $i RETURN NONE; };
		CREATE clash:1 SET n = 1, __sql_window_0 = 'mine';";
	for result in ds.execute(fill, &session(), None).await.expect("the rows are written") {
		result.result.expect("a row is written");
	}
	let random = match dialect {
		Dialect::Pgsql => "random()",
		Dialect::Mysql => "rand()",
	};
	let text = format!(
		"SELECT n, row_number() OVER (PARTITION BY {random} < 0.5 ORDER BY n) AS v FROM big"
	);
	let got = answer(&ds, dialect, &text, &["n", "v"]).await;
	assert!(
		got.starts_with("ERR:") && got.contains("answered a different value"),
		"{dialect:?}: `{text}` answered `{got}`, expected an error"
	);
	let text = "SELECT n, row_number() OVER (ORDER BY n) AS v FROM clash";
	let got = answer(&ds, dialect, text, &["n", "v"]).await;
	assert!(
		got.starts_with("ERR:") && got.contains("the name a window function is computed into"),
		"{dialect:?}: `{text}` answered `{got}`, expected an error"
	);
}

/// A [`TempStore`] counting the runs each breaker writes, read from the spec in a run's header.
struct Counting {
	inner: MemTempStore,
	by_kind: Arc<[AtomicUsize; 16]>,
}

impl Counting {
	fn new(part_size: usize) -> Self {
		Self {
			inner: MemTempStore::new(part_size, None),
			by_kind: Arc::new(std::array::from_fn(|_| AtomicUsize::new(0))),
		}
	}

	fn written(&self, kind: BreakerKind) -> usize {
		self.by_kind[kind as usize].load(Ordering::Relaxed)
	}
}

/// The offset of a run's spec in its header: after the magic, the format and the build key.
const SPEC_AT: usize = 8 + 2 + 16;

struct CountingWrite {
	inner: Box<dyn RunWrite>,
	first: bool,
	by_kind: Arc<[AtomicUsize; 16]>,
}

impl RunWrite for CountingWrite {
	fn write_part(&mut self, part: Vec<u8>) -> BoxFut<'_, anyhow::Result<()>> {
		if std::mem::take(&mut self.first) {
			let (spec, _) = read_spec(&part, SPEC_AT).expect("a run's first part holds its spec");
			self.by_kind[spec.kind as usize].fetch_add(1, Ordering::Relaxed);
		}
		self.inner.write_part(part)
	}

	fn finish(self: Box<Self>) -> BoxFut<'static, anyhow::Result<RunRef>> {
		self.inner.finish()
	}
}

impl TempStore for Counting {
	fn part_size(&self) -> usize {
		self.inner.part_size()
	}

	fn quota(&self) -> Option<u64> {
		self.inner.quota()
	}

	fn session(&self) -> &SessionName {
		self.inner.session()
	}

	fn shares(&self) -> bool {
		self.inner.shares()
	}

	fn write_run(&self, sharing: Sharing) -> BoxFut<'_, anyhow::Result<Box<dyn RunWrite>>> {
		Box::pin(async move {
			let inner = self.inner.write_run(sharing).await?;
			Ok(Box::new(CountingWrite {
				inner,
				first: true,
				by_kind: Arc::clone(&self.by_kind),
			}) as Box<dyn RunWrite>)
		})
	}

	fn read_run(&self, run: &RunRef) -> BoxFut<'_, anyhow::Result<Box<dyn RunRead>>> {
		self.inner.read_run(run)
	}

	fn delete_run(&self, run: &RunRef) {
		self.inner.delete_run(run);
	}
}

/// The rows of `sp`, `n` in `0..SPILL_ROWS` with `p = n % 3`.
const SPILL_ROWS: i64 = 20_000;

/// A datastore whose queries share `limit` bytes and spill to `store` in parts of 1 KiB, holding
/// the rows of `sp`.
async fn spilling(limit: usize, store: Arc<Counting>) -> (Arc<Datastore>, Arc<FairGovernor>) {
	let governor = Arc::new(FairGovernor::new(limit));
	let config = surrealdb_cnf::ConfigMap::empty()
		.with_key_value("spill_part_size", "1024")
		.with_key_value("spill_merge_fan_in", "4")
		.with_key_value("query_memory_minimum", "4096")
		.with_key_value("memory_reserve_chunk", "4096");
	let ds = Datastore::builder()
		.with_auth(true)
		.with_config(config)
		.with_memory_governor(Arc::clone(&governor) as Arc<dyn MemoryGovernor>)
		.with_temp_store(store)
		.build_with_path("memory")
		.await
		.expect("the datastore opens");
	set_up(&ds).await;
	let fill =
		format!("FOR $i IN 0..{SPILL_ROWS} {{ CREATE sp SET n = $i, p = $i % 3 RETURN NONE; }};");
	for result in ds.execute(&fill, &session(), None).await.expect("the rows are written") {
		result.result.expect("a row is written");
	}
	(ds, governor)
}

/// What the spill statement answers for the row `n`: its `cume_dist`, its `lead(n, 5)` and the
/// sum of `n` over the rows from 3 before it to 2 after it, within the rows of its `p`.
fn spill_expected() -> Vec<String> {
	let partitions: Vec<Vec<i64>> =
		(0..3).map(|p| (0..SPILL_ROWS).filter(|m| m % 3 == p).collect()).collect();
	let mut out = Vec::new();
	for n in 0..SPILL_ROWS {
		let partition = &partitions[(n % 3) as usize];
		let at = (n / 3) as usize;
		let size = partition.len();
		let cume = (at + 1) as f64 / size as f64;
		let lead = partition.get(at + 5).map_or_else(|| "NULL".to_owned(), i64::to_string);
		let sum: i64 = partition[at.saturating_sub(3)..(at + 3).min(size)].iter().sum();
		out.push(format!("{n},{cume},{lead},{sum}"));
	}
	out
}

const SPILL_SELECT: &str = "SELECT n, \
	cume_dist() OVER (PARTITION BY p ORDER BY n) AS cd, \
	lead(n, 5) OVER (PARTITION BY p ORDER BY n) AS ld, \
	sum(n) OVER (PARTITION BY p ORDER BY n ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING) AS sm \
	FROM sp";

/// The spill statement's rows over `ds`, sorted by `n`, each as `n,cd,ld,sm`.
async fn spill_answer(ds: &Datastore, dialect: Dialect) -> Vec<String> {
	let rows = rows(ds, dialect, SPILL_SELECT)
		.await
		.unwrap_or_else(|e| panic!("{dialect:?}: the spill statement failed: {e}"));
	let mut out: Vec<(i64, String)> = rows
		.iter()
		.map(|row| {
			let cell = |name: &str| row.get(name).map_or_else(|| "MISSING".to_owned(), render);
			let n = cell("n");
			let key = n.parse::<i64>().expect("n is an integer");
			(key, format!("{n},{},{},{}", cell("cd"), cell("ld"), cell("sm")))
		})
		.collect();
	out.sort_by_key(|(n, _)| *n);
	out.into_iter().map(|(_, row)| row).collect()
}

fn assert_spill_rows(dialect: Dialect, what: &str, got: &[String], expected: &[String]) {
	assert_eq!(got.len(), expected.len(), "{dialect:?} {what}: the row count");
	for (got, expected) in got.iter().zip(expected) {
		let agree = got == expected || {
			let (g, e): (Vec<&str>, Vec<&str>) =
				(got.split(',').collect(), expected.split(',').collect());
			g.len() == e.len()
				&& g.iter().zip(&e).all(|(g, e)| {
					g == e
						|| matches!((g.parse::<f64>(), e.parse::<f64>()), (Ok(g), Ok(e)) if (g - e).abs() <= 1e-12)
				})
		};
		assert!(agree, "{dialect:?} {what}: answered `{got}`, expected `{expected}`");
	}
}

/// A window whose partitions outgrow a query's memory spills its rows to the temporary store,
/// answers what it answers with memory to spare, deletes every run it wrote, and gives its
/// query's memory back.
async fn window_spills(dialect: Dialect) {
	let expected = spill_expected();

	let roomy_store = Arc::new(Counting::new(1024));
	let (roomy, _) = spilling(1 << 30, Arc::clone(&roomy_store)).await;
	let in_memory = spill_answer(&roomy, dialect).await;
	assert_spill_rows(dialect, "with memory to spare", &in_memory, &expected);
	assert_eq!(
		roomy_store.written(BreakerKind::WindowBuffer),
		0,
		"{dialect:?}: a window spilled with memory to spare"
	);

	let store = Arc::new(Counting::new(1024));
	let (tight, governor) = spilling(64 << 10, Arc::clone(&store)).await;
	let windows_before = store.written(BreakerKind::WindowBuffer);
	let overcommits_before = governor.overcommits();
	let spilled = spill_answer(&tight, dialect).await;
	assert_spill_rows(dialect, "spilled", &spilled, &expected);
	let windows = store.written(BreakerKind::WindowBuffer) - windows_before;
	assert!(windows > 0, "{dialect:?}: the window wrote no run");
	assert_eq!(store.inner.runs(), 0, "{dialect:?}: a spilled run outlived its query");
	assert_eq!(governor.granted(), 0, "{dialect:?}: the query's lease outlived it");
	let overcommits = governor.overcommits() - overcommits_before;
	assert_eq!(overcommits, 0, "{dialect:?}: the query held memory past its grant");
	eprintln!(
		"{dialect:?}: the window wrote {windows} runs, the sorts {}, in {} parts; read at most {} \
		 runs at once; {} overcommits during the query",
		store.written(BreakerKind::Sort),
		store.inner.parts_written(),
		store.inner.most_read_at_once(),
		overcommits,
	);
}

macro_rules! both_dialects {
	($($check:ident => $pgsql:ident, $mysql:ident;)*) => {$(
		#[cfg(feature = "postgres")]
		#[tokio::test]
		async fn $pgsql() {
			$check(Dialect::Pgsql).await;
		}

		#[cfg(feature = "mysql")]
		#[tokio::test]
		async fn $mysql() {
			$check(Dialect::Mysql).await;
		}
	)*};
}

both_dialects! {
	window_beside_star => pgsql_window_beside_star, mysql_window_beside_star;
	window_over_groups => pgsql_window_over_groups, mysql_window_over_groups;
	window_over_nothing => pgsql_window_over_nothing, mysql_window_over_nothing;
	limit_above_window => pgsql_limit_above_window, mysql_limit_above_window;
	nulls_in_keys => pgsql_nulls_in_keys, mysql_nulls_in_keys;
	several_windows => pgsql_several_windows, mysql_several_windows;
	named_window => pgsql_named_window, mysql_named_window;
	window_spills => pgsql_window_spills, mysql_window_spills;
	refusals => pgsql_refusals, mysql_refusals;
	inconsistent_rows => pgsql_inconsistent_rows, mysql_inconsistent_rows;
	distinct_over_groups => pgsql_distinct_over_groups, mysql_distinct_over_groups;
	integer_averages => pgsql_integer_averages, mysql_integer_averages;
	distinct_in_every_shape => pgsql_distinct_in_every_shape, mysql_distinct_in_every_shape;
	grouping_errors => pgsql_grouping_errors, mysql_grouping_errors;
}
