//! The `*` of a relation (`t.*`, `schema.t.*`) in the select list of a PostgreSQL `SELECT`.

mod common;

use common::projection::ToPgQueryJson;
use surrealdb_pgsql::ast::expr::Expr;
use surrealdb_pgsql::ast::stmt::{ResTarget, SelectStmt, Statement};
use surrealdb_pgsql::{
	ErrorKind, ParserSettings, ToPgSql, TranslationError, parse_statement,
	parse_statement_with_settings, translate_sql,
};

fn select(sql: &str) -> SelectStmt {
	let Statement::Select(select) = parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
	else {
		panic!("{sql}: expected a SELECT");
	};
	*select
}

/// The fields of the column reference that `target` is, and its alias.
fn column(target: &ResTarget) -> (&[String], Option<&str>) {
	let Expr::ColumnRef(column) = &target.expr else {
		panic!("expected a column reference, got {:?}", target.expr);
	};
	(&column.fields, target.alias.as_deref())
}

fn fields(names: &[&str]) -> Vec<String> {
	names.iter().map(|name| name.to_string()).collect()
}

#[test]
fn a_relation_star_is_a_column_reference_ending_in_a_star() {
	for (sql, expected) in [
		("SELECT t.* FROM t", &["t", "*"][..]),
		("SELECT public.t.* FROM public.t", &["public", "t", "*"][..]),
		("SELECT \"T\".* FROM \"T\"", &["T", "*"][..]),
		("SELECT \"my schema\".\"My Table\".* FROM x", &["my schema", "My Table", "*"][..]),
		("SELECT name.* FROM name", &["name", "*"][..]),
		("SELECT t . * FROM t", &["t", "*"][..]),
		("SELECT t.*FROM t", &["t", "*"][..]),
		("SELECT a.b.c.* FROM x", &["a", "b", "c", "*"][..]),
	] {
		let select = select(sql);
		let [target] = select.targets.as_slice() else {
			panic!("{sql}: one target");
		};
		assert_eq!(column(target), (fields(expected).as_slice(), None), "{sql}");
	}
}

#[test]
fn a_relation_star_is_one_item_of_the_list() {
	let select = select("SELECT a, t.*, u.x, u.*, 1 FROM t, u");
	assert_eq!(select.targets.len(), 5);
	let lists: Vec<&[String]> = select.targets[..4].iter().map(|target| column(target).0).collect();
	assert_eq!(lists, [&["a"][..], &["t", "*"], &["u", "x"], &["u", "*"]]);
	assert!(matches!(select.targets[4].expr, Expr::Literal { .. }));
}

#[test]
fn an_alias_after_a_relation_star_is_kept_as_written() {
	for sql in ["SELECT t.* AS x FROM t", "SELECT t.* x FROM t"] {
		let select = select(sql);
		assert_eq!(
			column(&select.targets[0]),
			(fields(&["t", "*"]).as_slice(), Some("x")),
			"{sql}"
		);
	}
}

#[test]
fn a_relation_star_is_read_in_any_select() {
	for sql in [
		"SELECT * FROM (SELECT s.* FROM s) d",
		"SELECT (SELECT s.* FROM s LIMIT 1)",
		"SELECT * FROM t WHERE EXISTS (SELECT u.* FROM u WHERE u.a = t.a)",
		"WITH w AS (SELECT s.* FROM s) SELECT w.* FROM w",
		"SELECT t.* FROM t UNION ALL SELECT u.* FROM u",
		"SELECT t.* FROM t ORDER BY 1 LIMIT 1",
		"CREATE VIEW v AS SELECT t.* FROM t",
		"COPY (SELECT t.* FROM t) TO STDOUT",
	] {
		parse_statement(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
	}
	let Statement::Insert(insert) =
		parse_statement("INSERT INTO t (a, b) SELECT s.* FROM s").expect("insert")
	else {
		panic!("expected an INSERT");
	};
	let query = insert.query.expect("the query of the INSERT");
	assert_eq!(column(&query.targets[0]).0, ["s", "*"]);
}

#[test]
fn a_relation_star_that_is_not_a_whole_item_is_refused() {
	for sql in [
		"SELECT 1 + t.* FROM t",
		"SELECT t.* + 1 FROM t",
		"SELECT t.*+1 FROM t",
		"SELECT count(t.*) FROM t",
		"SELECT ROW(t.*) FROM t",
		"SELECT (t.*) FROM t",
		"SELECT t.*.a FROM t",
		"SELECT t.*.* FROM t",
		"SELECT t.*::text FROM t",
		"SELECT t.* IS NULL FROM t",
		"SELECT a FROM t WHERE t.* IS NOT NULL",
		"SELECT a FROM t ORDER BY t.*",
		"SELECT a FROM t GROUP BY t.*",
		"SELECT * AS x FROM t",
		"SELECT t.*.* AS x FROM t",
		"SELECT t. FROM t",
		"SELECT t.* , FROM t",
	] {
		assert!(parse_statement(sql).is_err(), "{sql} must be refused");
	}
}

#[test]
fn a_relation_star_is_refused_where_a_list_is_not_a_select_list() {
	for sql in [
		"DELETE FROM t RETURNING t.*",
		"UPDATE t SET a = 1 RETURNING t.*",
		"INSERT INTO t (a) VALUES (1) RETURNING t.*",
		"INSERT INTO t (a) SELECT 1 RETURNING t.*",
	] {
		assert!(parse_statement(sql).is_err(), "{sql} must be refused");
	}
	// The `*` of a RETURNING list is still read.
	parse_statement("DELETE FROM t RETURNING *").expect("RETURNING *");
}

#[test]
fn a_relation_star_prints_as_written() {
	for sql in ["SELECT t.* FROM t", "SELECT a, \"T\".*, public.t.* FROM t"] {
		let mut first = parse_statement(sql).expect(sql);
		let printed = first.to_pgsql_string();
		assert_eq!(printed, sql, "{sql}");
		let mut second = parse_statement(&printed).expect(&printed);
		first.normalize_spans();
		second.normalize_spans();
		assert_eq!(first, second, "{sql}");
	}
}

#[test]
fn a_relation_star_has_no_translation_to_surrealql() {
	assert!(translate_sql("SELECT * FROM t").is_ok());
	for sql in
		["SELECT t.* FROM t", "SELECT a, public.t.* FROM public.t", "EXPLAIN SELECT t.* FROM t"]
	{
		let err = translate_sql(sql).expect_err(sql);
		assert!(matches!(err, TranslationError::Unsupported(_)), "{sql}: {err:?}");
	}
}

#[test]
fn a_long_select_list_of_relation_stars_hits_the_work_limit() {
	let items = vec!["t.*"; 60_000].join(", ");
	let err = parse_statement(&format!("SELECT {items} FROM t")).expect_err("work limit");
	assert_eq!(err.kind, ErrorKind::WorkLimitExceeded);
}

#[test]
fn a_long_qualified_name_costs_no_recursion() {
	let long = std::thread::Builder::new()
		.stack_size(2 * 1024 * 1024)
		.spawn(|| {
			let name = vec!["a"; 20_000].join(".");
			let star = parse_statement(&format!("SELECT {name}.* FROM t")).map(|_| ());
			let column = parse_statement(&format!("SELECT {name}.b FROM t")).map(|_| ());
			(star, column)
		})
		.expect("spawn")
		.join()
		.expect("no stack overflow");
	assert_eq!(long, (Ok(()), Ok(())));
}

#[test]
fn the_recursion_limit_still_bounds_nested_selects_with_relation_stars() {
	let settings = ParserSettings {
		max_recursion_depth: 16,
		..ParserSettings::default()
	};
	let nested = |depth: usize| {
		let mut sql = "SELECT t.* FROM t".to_string();
		for _ in 0..depth {
			sql = format!("SELECT s.* FROM ({sql}) s");
		}
		sql
	};
	assert!(parse_statement_with_settings(&nested(2), settings).is_ok());
	let err = parse_statement_with_settings(&nested(40), settings).expect_err("depth limit");
	assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);
}

#[test]
fn a_relation_star_projects_to_the_node_pg_query_gives_it() {
	let select = |sql: &str| {
		parse_statement(sql).expect(sql).to_pg_query_json()["RawStmt"]["stmt"]["SelectStmt"]
			["targetList"][0]["ResTarget"]["val"]
			.clone()
	};
	assert_eq!(
		select("SELECT t.* FROM t"),
		serde_json::json!({"ColumnRef": {"fields": [
			{"String": {"sval": "t"}}, {"A_Star": {}}
		]}})
	);
	assert_eq!(
		select("SELECT public.t.* FROM t"),
		serde_json::json!({"ColumnRef": {"fields": [
			{"String": {"sval": "public"}}, {"String": {"sval": "t"}}, {"A_Star": {}}
		]}})
	);
	assert_eq!(
		select("SELECT * FROM t"),
		serde_json::json!({"ColumnRef": {"fields": [{"A_Star": {}}]}})
	);
}
