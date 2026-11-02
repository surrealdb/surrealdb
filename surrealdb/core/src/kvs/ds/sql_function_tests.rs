//! Every function the SQL front ends register runs, and answers what the reference server does.
//!
//! The registry may list a function only if a statement calling it lowers and runs. A function
//! that is registered but has no case here, fails, or answers differently from the case is a
//! defect, which `BROKEN` names until it is fixed.

use std::sync::Arc;

use surrealdb_sqlplan::{FunctionRegistry, SqlDialect};
use surrealdb_types::Value;

use super::sql_function_cases::{Case, Sql};
use crate::dbs::Session;
use crate::kvs::{Datastore, QueryRequest, QuerySource};

/// The cases that do not pass yet, as `(dialect, case)`, where a case is named by [`identity`],
/// with ` (stored)` after it for its run over the columns of a stored row, ` (inserted)` for its
/// run in the values of an `INSERT`, and ` (updated)` for its run in the `SET` of an `UPDATE` over
/// the columns of a written row: a failure in any other case fails the test, even of a function
/// listed here. A case leaves the
/// list when it passes, and the test fails while a case stays on it after it passes or names no
/// case.
const BROKEN: &[(Dialect, &str)] = &[
	// pgsql
	(Dialect::Pgsql, r#"SELECT generate_series(1, 10, 4) AS v"#),
	(Dialect::Pgsql, r#"SELECT generate_series(3, 1, -1) AS v"#),
	// The `LIKE` operator is not lowered in the values of a write.
	(Dialect::Pgsql, r#"version() LIKE 'PostgreSQL%' (inserted)"#),
	// mysql
	(Dialect::Mysql, r#"version() LIKE '8.%' (inserted)"#),
	(Dialect::Pgsql, r#"SELECT pg_try_advisory_lock(1) AS v"#),
	(Dialect::Pgsql, r#"SELECT pg_advisory_lock(1) AS v"#),
	(Dialect::Pgsql, r#"SELECT pg_advisory_unlock(1) AS v"#),
	(Dialect::Pgsql, r#"SELECT pg_advisory_unlock_all() AS v"#),
];

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dialect {
	Pgsql,
	Mysql,
}

impl Dialect {
	fn label(self) -> &'static str {
		match self {
			Dialect::Pgsql => "pgsql",
			Dialect::Mysql => "mysql",
		}
	}

	fn sql_dialect(self) -> SqlDialect {
		match self {
			Dialect::Pgsql => SqlDialect::Postgres,
			Dialect::Mysql => SqlDialect::MySql,
		}
	}

	fn source(self, text: &str) -> QuerySource<'_> {
		match self {
			Dialect::Pgsql => QuerySource::pgsql(text),
			Dialect::Mysql => QuerySource::mysql(text),
		}
	}

	fn cases(self) -> &'static [Case] {
		match self {
			Dialect::Pgsql => super::sql_function_cases::PGSQL,
			Dialect::Mysql => super::sql_function_cases::MYSQL,
		}
	}
}

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

async fn datastore() -> Arc<Datastore> {
	let ds = Datastore::builder()
		.with_auth(true)
		.build_with_path("memory")
		.await
		.expect("the datastore opens");
	let setup = "DEFINE NAMESPACE test; DEFINE DATABASE test;
		DEFINE TABLE t SCHEMALESS; DEFINE TABLE fx SCHEMALESS; DEFINE TABLE wx SCHEMALESS;";
	for result in ds.execute(setup, &session(), None).await.expect("the setup runs") {
		result.result.expect("a setup statement");
	}
	ds
}

/// What a statement answers: the `v` column of its rows joined by `|`, or `ERR:` and the message.
async fn outcome(ds: &Datastore, dialect: Dialect, text: &str) -> String {
	let session = session();
	let results = match ds.run(QueryRequest::new(dialect.source(text), &session)).await {
		Ok(results) => results,
		Err(error) => return format!("ERR:{error}"),
	};
	let Some(response) = results.into_iter().next() else {
		return "ERR:the statement answered no result".to_owned();
	};
	let rows = match response.result {
		Ok(value) => value,
		Err(error) => return format!("ERR:{error}"),
	};
	let Value::Array(rows) = rows else {
		return render(dialect, &rows);
	};
	let cells: Vec<String> = rows
		.iter()
		.map(|row| {
			if let Value::Object(object) = row {
				object.get("v").map_or_else(|| "OK".to_owned(), |v| render(dialect, v))
			} else {
				render(dialect, row)
			}
		})
		.collect();
	cells.join("|")
}

/// An interval as PostgreSQL writes it in its default style: each non-zero year, month and day,
/// then the time, with a `+` on a positive field after a negative one.
fn interval_text(months: i32, days: i32, micros: i64) -> String {
	let mut out = String::new();
	let mut before = false;
	for (value, unit) in [(months / 12, "year"), (months % 12, "mon"), (days, "day")] {
		if value == 0 {
			continue;
		}
		if !out.is_empty() {
			out.push(' ');
		}
		if before && value > 0 {
			out.push('+');
		}
		out.push_str(&format!(
			"{value} {unit}{}",
			if value == 1 {
				""
			} else {
				"s"
			}
		));
		before = value < 0;
	}
	if out.is_empty() || micros != 0 {
		if !out.is_empty() {
			out.push(' ');
		}
		if micros < 0 {
			out.push('-');
		} else if before {
			out.push('+');
		}
		let abs = micros.unsigned_abs();
		let secs = abs / 1_000_000;
		out.push_str(&format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60));
		if !abs.is_multiple_of(1_000_000) {
			out.push_str(format!(".{:06}", abs % 1_000_000).trim_end_matches('0'));
		}
	}
	out
}

fn render(dialect: Dialect, value: &Value) -> String {
	match value {
		Value::None | Value::Null => "NULL".to_owned(),
		Value::Bool(b) => match (dialect, b) {
			(Dialect::Pgsql, true) => "t".to_owned(),
			(Dialect::Pgsql, false) => "f".to_owned(),
			(Dialect::Mysql, true) => "1".to_owned(),
			(Dialect::Mysql, false) => "0".to_owned(),
		},
		Value::Number(n) => match n.to_f64() {
			Some(f) if f.fract() == 0.0 && f.abs() < 1e15 => format!("{}", f as i64),
			Some(f) => format!("{f}"),
			None => n.to_string(),
		},
		Value::String(s) => s.clone(),
		Value::Datetime(d) => d.format("%Y-%m-%d %H:%M:%S").to_string(),
		Value::Date(d) => d.to_string(),
		Value::Time(t) => {
			let shown = t.to_string();
			if shown.contains('.') {
				shown.trim_end_matches('0').to_owned()
			} else {
				shown
			}
		}
		Value::Timestamp(t) => t.to_string().replacen('T', " ", 1),
		Value::Interval(i) => interval_text(i.months(), i.days(), i.micros()),
		Value::Array(items) => {
			let items: Vec<String> = items.iter().map(|item| render(dialect, item)).collect();
			format!("[{}]", items.join(","))
		}
		Value::Object(_) => json(value),
		Value::Bytes(_)
		| Value::Duration(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Set(_) => surrealdb_types::ToSql::to_sql(value),
	}
}

/// The compact JSON text of a value.
fn json(value: &Value) -> String {
	match value {
		Value::None | Value::Null => "null".to_owned(),
		Value::Bool(b) => b.to_string(),
		Value::Number(_) => render(Dialect::Pgsql, value),
		Value::String(s) => format!("{s:?}"),
		Value::Array(items) => {
			format!("[{}]", items.iter().map(json).collect::<Vec<_>>().join(","))
		}
		Value::Object(object) => format!(
			"{{{}}}",
			object.iter().map(|(k, v)| format!("{k:?}:{}", json(v))).collect::<Vec<_>>().join(",")
		),
		Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Set(_) => format!("{:?}", surrealdb_types::ToSql::to_sql(value)),
	}
}

fn is_missing_function(message: &str) -> bool {
	message.contains("not found in function registry") || message.contains("Unknown function")
}

/// Whether the answer is the expected one. Integers agree exactly and other numbers to within a
/// part in a billion, since the reference servers print a numeric of any scale; JSON agrees up to
/// its spacing; an expected error agrees with any error that is not a missing function.
fn agrees(expect: &str, got: &str) -> bool {
	if let Some(_code) = expect.strip_prefix("ERR:") {
		return got.starts_with("ERR:") && !is_missing_function(got);
	}
	if expect == got {
		return true;
	}
	if let (Ok(a), Ok(b)) = (expect.parse::<i64>(), got.parse::<i64>()) {
		return a == b;
	}
	if let (Ok(a), Ok(b)) = (expect.parse::<f64>(), got.parse::<f64>()) {
		return (a - b).abs() <= 1e-9 * a.abs().max(1.0);
	}
	let squash = |s: &str| s.replace(' ', "");
	// A PostgreSQL array prints in braces, and the engine's array renders in brackets.
	let expect = match expect.strip_prefix('{').and_then(|rest| rest.strip_suffix('}')) {
		Some(items) if !items.starts_with('"') => format!("[{items}]"),
		_ => expect.to_owned(),
	};
	matches!(expect.chars().next(), Some('{' | '[')) && squash(&expect) == squash(got)
}

/// The text that names `case`: its expression with the arguments written in, or its statement.
fn identity(case: &Case) -> String {
	match &case.sql {
		Sql::Stmt(sql) => (*sql).to_owned(),
		Sql::Literal {
			template,
			args,
		}
		| Sql::Call {
			template,
			args,
		} => fill(template, args),
	}
}

fn fill(template: &str, args: &[&str]) -> String {
	args.iter()
		.enumerate()
		.fold(template.to_owned(), |sql, (i, arg)| sql.replace(&format!("{{{i}}}"), arg))
}

/// One run of a case: the statements that write what the answer is read from, and the statement
/// that reads it. An error answered by a statement that writes is the answer of the run.
struct Run {
	suffix: &'static str,
	writes: Vec<String>,
	read: String,
}

impl Run {
	fn read(read: String) -> Self {
		Run {
			suffix: "",
			writes: Vec::new(),
			read,
		}
	}
}

/// Populates `t` and `fx` for `case`, and returns the runs of the case.
async fn runs(ds: &Datastore, dialect: Dialect, case: &Case) -> Result<Vec<Run>, String> {
	let (template, args) = match &case.sql {
		Sql::Stmt(sql) => return Ok(vec![Run::read((*sql).to_owned())]),
		Sql::Literal {
			template,
			args,
		} => (template, args),
		Sql::Call {
			template,
			args,
		} => (template, args),
	};
	let call = fill(template, args);
	let mut runs = vec![
		Run::read(format!("SELECT {call} AS v")),
		Run {
			suffix: " (inserted)",
			writes: vec![
				"DELETE FROM wx".to_owned(),
				format!("INSERT INTO wx (id, v) VALUES (1, {call})"),
			],
			read: "SELECT v FROM wx".to_owned(),
		},
	];
	if matches!(case.sql, Sql::Call { .. }) && !args.is_empty() {
		let columns: Vec<String> = (0..args.len()).map(|i| format!("a{i}")).collect();
		let stored = args.iter().enumerate().fold((*template).to_owned(), |sql, (i, _)| {
			sql.replace(&format!("{{{i}}}"), &format!("a{i}"))
		});
		let insert =
			format!("INSERT INTO fx ({}) VALUES ({})", columns.join(", "), args.join(", "));
		ds.execute("DELETE fx", &session(), None).await.expect("the fixture is cleared");
		let outcome = outcome(ds, dialect, &insert).await;
		if outcome.starts_with("ERR:") {
			return Err(format!("`{insert}` answered `{outcome}`"));
		}
		runs.push(Run {
			suffix: " (stored)",
			writes: Vec::new(),
			read: format!("SELECT {stored} AS v FROM fx"),
		});
		runs.push(Run {
			suffix: " (updated)",
			writes: vec![
				"DELETE FROM wx".to_owned(),
				format!(
					"INSERT INTO wx (id, {}) VALUES (1, {})",
					columns.join(", "),
					args.join(", ")
				),
				format!("UPDATE wx SET v = {stored} WHERE id = 1"),
			],
			read: "SELECT v FROM wx".to_owned(),
		});
	}
	Ok(runs)
}

async fn load_fixture(ds: &Datastore, dialect: Dialect) {
	let outcome = outcome(
		ds,
		dialect,
		"INSERT INTO t (n, s, x) VALUES (1, 'a', 1.5), (2, 'b', 2.5), (3, 'c', NULL)",
	)
	.await;
	assert!(!outcome.starts_with("ERR:"), "{}: the fixture loads: {outcome}", dialect.label());
}

/// The problems of every case of every registered function, one line each, with the
/// [`identity`] of the case.
async fn problems(dialect: Dialect) -> Vec<(String, String)> {
	let ds = datastore().await;
	load_fixture(&ds, dialect).await;
	let mut problems = Vec::new();
	for case in dialect.cases() {
		let runs = match runs(&ds, dialect, case).await {
			Ok(runs) => runs,
			Err(error) => {
				problems.push((
					identity(case),
					format!(
						"{} {}: the arguments do not store: {error}",
						dialect.label(),
						case.name
					),
				));
				continue;
			}
		};
		for run in runs {
			let mut text = run.read.clone();
			let mut got = None;
			for write in &run.writes {
				let written = outcome(&ds, dialect, write).await;
				if written.starts_with("ERR:") {
					text = write.clone();
					got = Some(written);
					break;
				}
			}
			let got = match got {
				Some(got) => got,
				None => outcome(&ds, dialect, &text).await,
			};
			if !agrees(case.expect, &got) {
				// A run over a stored or written row is a case of its own, as its lowering is.
				let key = format!("{}{}", identity(case), run.suffix);
				problems.push((
					key.clone(),
					format!(
						"{} {} [{key}]: `{text}` answered `{got}`, expected `{}`",
						dialect.label(),
						case.name,
						case.expect
					),
				));
			}
		}
	}
	problems
}

async fn conformance(dialect: Dialect) {
	let registered: Vec<&str> = FunctionRegistry::defs()
		.iter()
		.filter(|def| def.is_registered_in(dialect.sql_dialect()))
		.map(|def| def.name)
		.collect();
	let mut failures = Vec::new();
	for name in &registered {
		if !dialect.cases().iter().any(|case| case.name == *name) {
			failures.push(format!("{} {name}: registered with no case", dialect.label()));
		}
	}
	for case in dialect.cases() {
		if !registered.contains(&case.name) {
			failures.push(format!(
				"{} {}: a case for a function that is not registered",
				dialect.label(),
				case.name
			));
		}
	}
	let problems = problems(dialect).await;
	for (case, line) in &problems {
		if BROKEN.contains(&(dialect, case.as_str())) {
			eprintln!("still broken: {line}");
		} else {
			failures.push(line.clone());
		}
	}
	for (listed, case) in BROKEN {
		if *listed != dialect {
			continue;
		}
		let named = [" (stored)", " (inserted)", " (updated)"]
			.iter()
			.find_map(|suffix| case.strip_suffix(suffix))
			.unwrap_or(case);
		if !dialect.cases().iter().any(|known| identity(known) == named) {
			failures.push(format!("{} `{case}`: in BROKEN, but names no case", dialect.label()));
		} else if !problems.iter().any(|(failing, _)| failing == case) {
			failures.push(format!("{} `{case}`: passes, so it leaves BROKEN", dialect.label()));
		}
	}
	assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn every_function_the_pgsql_front_end_registers_runs() {
	conformance(Dialect::Pgsql).await;
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn every_function_the_mysql_front_end_registers_runs() {
	conformance(Dialect::Mysql).await;
}
