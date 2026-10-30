//! An unqualified column of a join is read from the relation that holds it.
//!
//! The binder cannot know which relation of a join holds a column when the relations are schemaless
//! tables, as every table is on the wire, so it binds the column to every relation that may and the
//! lowering reads the first that has a value. A relation whose columns are known and lack the
//! column is ruled out, and a column the catalog describes for one relation leads.

use std::collections::HashSet;

use surrealdb_expr::expr::logical::{
	LogicalAggregate, LogicalField, LogicalFields, LogicalJoin, LogicalOp, LogicalOrdering,
	LogicalSort,
};

use crate::catalog::{EmptyCatalog, MemoryCatalog};
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::ir::{BoundColumnRef, BoundExpr, BoundSelect};
use crate::lower::link_tests::{join_keys, my_select_in, pg_select_in, predicate, projection};
use crate::{SqlType, binder};

fn pg_bound(sql: &str, catalog: &MemoryCatalog) -> Result<BoundSelect, PlanError> {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	binder::pgsql::bind_select(select, catalog, &DialectPolicy::postgres(), None)
}

/// The column the `n`th item of the select list reads.
fn column(bound: &BoundSelect, n: usize) -> &BoundColumnRef {
	let BoundExpr::Column(col) = &bound.targets[n].expr else {
		panic!("expected a column, got {:?}", bound.targets[n].expr);
	};
	col
}

/// The relation and the further relations of `n`th item of the select list of `sql`.
fn relations(sql: &str, catalog: &MemoryCatalog) -> Vec<(usize, Vec<usize>)> {
	let bound = pg_bound(sql, catalog).expect("bind");
	(0..bound.targets.len())
		.map(|n| {
			let col = column(&bound, n);
			(col.relation_index, col.also_in.clone())
		})
		.collect()
}

/// The first join of the pipeline.
fn first_join(op: &LogicalOp) -> Option<&LogicalJoin> {
	if let LogicalOp::Join(join) = op {
		return Some(join);
	}
	op.children().iter().find_map(first_join)
}

/// The filter of the first join of the pipeline.
fn join_filter(op: &LogicalOp) -> Option<String> {
	first_join(op).expect("a join").filter.as_ref().map(ToString::to_string)
}

const JOIN: &str = "FROM ja JOIN jb ON ja.k = jb.k";

#[test]
fn a_column_of_a_join_of_tables_without_a_schema_may_be_in_each_of_them() {
	let catalog = MemoryCatalog::new();
	assert_eq!(
		relations(&format!("SELECT v, w {JOIN}"), &catalog),
		vec![(0, vec![1]), (0, vec![1])]
	);
	assert_eq!(
		relations("SELECT v FROM ja JOIN jb ON ja.k = jb.k JOIN jc ON jb.k = jc.k", &catalog),
		vec![(0, vec![1, 2])]
	);
	assert_eq!(relations("SELECT v FROM ja, jb WHERE ja.k = jb.k", &catalog), vec![(0, vec![1])]);
}

#[test]
fn a_column_is_read_from_the_first_table_that_holds_a_value() {
	let op = pg_select_in(&format!("SELECT v, w {JOIN}"), &EmptyCatalog);
	assert_eq!(projection(&op), ["v ?? _join0.v AS v", "w ?? _join0.w AS w"]);
	let op = pg_select_in(
		"SELECT x FROM ja JOIN jb ON ja.k = jb.k LEFT JOIN jc ON jb.k = jc.k",
		&EmptyCatalog,
	);
	assert_eq!(projection(&op), ["x ?? _join0.x ?? _join1.x AS x"]);
	let op = my_select_in(&format!("SELECT v, w {JOIN}"), &EmptyCatalog);
	assert_eq!(projection(&op), ["v ?? _join0.v AS v", "w ?? _join0.w AS w"]);
}

#[test]
fn a_column_of_a_table_alone_or_a_qualified_column_is_read_from_that_table() {
	let catalog = MemoryCatalog::new();
	assert_eq!(relations("SELECT v FROM ja", &catalog), vec![(0, vec![])]);
	assert_eq!(
		relations("SELECT ja.v, jb.w FROM ja JOIN jb ON ja.k = jb.k", &catalog),
		vec![(0, vec![]), (1, vec![])]
	);
	assert_eq!(
		relations("SELECT a.v, b.w FROM ja a JOIN jb b ON a.k = b.k", &catalog),
		vec![(0, vec![]), (1, vec![])]
	);
	let op = pg_select_in("SELECT jb.w FROM ja JOIN jb ON ja.k = jb.k", &catalog);
	assert_eq!(projection(&op), ["_join0.w AS w"]);
}

#[test]
fn a_column_is_read_from_the_tables_that_have_not_ruled_it_out() {
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "jb", &["k", "w"]);
	// `jb` lists its columns and `v` is not one of them: only `ja` can hold it.
	assert_eq!(relations(&format!("SELECT v, w {JOIN}"), &catalog), vec![(0, vec![]), (1, vec![])]);
	// The order of the tables does not matter.
	assert_eq!(
		relations("SELECT v, w FROM jb JOIN ja ON ja.k = jb.k", &catalog),
		vec![(1, vec![]), (0, vec![])]
	);
}

#[test]
fn a_cte_or_a_derived_table_with_a_column_list_rules_out_the_columns_it_does_not_have() {
	let catalog = MemoryCatalog::new();
	assert_eq!(
		relations(
			"WITH c AS (SELECT k, w FROM jb) SELECT v, w FROM c JOIN ja ON ja.k = c.k",
			&catalog
		),
		vec![(1, vec![]), (0, vec![])]
	);
	assert_eq!(
		relations("SELECT v, w FROM (SELECT k, w FROM jb) d(k, w) JOIN ja ON ja.k = d.k", &catalog),
		vec![(1, vec![]), (0, vec![])]
	);
	assert_eq!(
		relations("SELECT v, w FROM ja JOIN (SELECT k, w FROM jb) d(k, w) ON ja.k = d.k", &catalog),
		vec![(0, vec![]), (1, vec![])]
	);
	// A CTE that does not list its columns may hold any.
	assert_eq!(
		relations("WITH c AS (SELECT * FROM jb) SELECT v FROM c JOIN ja ON ja.k = c.k", &catalog),
		vec![(0, vec![1])]
	);
}

#[test]
fn a_derived_table_without_a_column_list_may_hold_any_column() {
	let catalog = MemoryCatalog::new();
	assert_eq!(
		relations("SELECT v, w FROM ja JOIN (SELECT k, w FROM jb) d ON ja.k = d.k", &catalog),
		vec![(0, vec![1]), (0, vec![1])]
	);
	assert_eq!(
		relations("SELECT v, w FROM (SELECT k, w FROM jb) d JOIN ja ON ja.k = d.k", &catalog),
		vec![(0, vec![1]), (0, vec![1])]
	);
}

#[test]
fn a_column_that_every_table_with_known_columns_lacks_is_read_from_the_first() {
	let catalog = MemoryCatalog::new();
	assert_eq!(
		relations(
			"SELECT x FROM (SELECT k FROM ja) a(k) JOIN (SELECT k FROM jb) b(k) ON a.k = b.k",
			&catalog
		),
		vec![(0, vec![])]
	);
}

#[test]
fn a_column_that_two_tables_with_known_columns_have_is_ambiguous() {
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "ja", &["k", "dup"]);
	catalog.define_table(None, "jb", &["k", "dup"]);
	assert!(matches!(
		pg_bound("SELECT dup FROM ja JOIN jb ON ja.k = jb.k", &catalog),
		Err(PlanError::AmbiguousColumn(column)) if column == "dup"
	));
}

#[test]
fn the_id_of_a_join_is_the_id_of_its_first_table() {
	let catalog = MemoryCatalog::new();
	let bound = pg_bound(&format!("SELECT id {JOIN}"), &catalog).expect("bind");
	let id = column(&bound, 0);
	assert_eq!((id.relation_index, id.also_in.as_slice()), (0, [].as_slice()));
	assert_eq!(id.id_table.as_deref(), Some("ja"));
}

#[test]
fn a_column_the_catalog_describes_for_one_table_leads() {
	let catalog = MemoryCatalog::new().with_link("jb", "owner", "ja");
	let bound = pg_bound(&format!("SELECT owner {JOIN}"), &catalog).expect("bind");
	let owner = column(&bound, 0);
	assert_eq!((owner.relation_index, owner.also_in.as_slice()), (1, [0].as_slice()));
	assert_eq!(owner.link_table.as_deref(), Some("ja"));

	let catalog = MemoryCatalog::new().with_column_type("jb", "w", SqlType::Text);
	let bound = pg_bound(&format!("SELECT w {JOIN}"), &catalog).expect("bind");
	let w = column(&bound, 0);
	assert_eq!((w.relation_index, w.also_in.as_slice()), (1, [0].as_slice()));
	assert_eq!(w.declared_type, SqlType::Text);

	// The other tables stay in the order of the FROM clause, after the one that leads.
	let catalog = MemoryCatalog::new().with_column_type("jc", "w", SqlType::Text);
	let bound =
		pg_bound("SELECT w FROM ja JOIN jb ON ja.k = jb.k JOIN jc ON jb.k = jc.k", &catalog)
			.expect("bind");
	let w = column(&bound, 0);
	assert_eq!((w.relation_index, w.also_in.as_slice()), (2, [0, 1].as_slice()));
}

#[test]
fn a_link_column_of_the_second_table_is_compared_with_the_record_it_names() {
	let catalog = MemoryCatalog::new().with_link("jb", "owner", "ja");
	let op = pg_select_in("SELECT v FROM ja JOIN jb ON ja.k = jb.k WHERE owner = 1", &catalog);
	assert_eq!(
		predicate(&op),
		"IF (_join0.owner ?? owner) = NULL OR (_join0.owner ?? owner) = NONE OR (ja:1) = NULL OR (ja:1) = NONE THEN NULL ELSE (_join0.owner ?? owner) = (ja:1) END"
	);
}

#[test]
fn a_column_in_the_where_clause_of_a_join_is_read_from_whichever_table_holds_it() {
	let op = pg_select_in(&format!("SELECT v {JOIN} WHERE w = 'b2'"), &EmptyCatalog);
	assert_eq!(
		predicate(&op),
		"IF (w ?? _join0.w) = NULL OR (w ?? _join0.w) = NONE OR 'b2' = NULL OR 'b2' = NONE THEN NULL ELSE (w ?? _join0.w) = 'b2' END"
	);
}

#[test]
fn a_key_of_a_join_condition_is_oriented_by_the_side_that_names_its_table() {
	// `v` is in `ja` and `w` in `jb`, which the binder cannot know: the side that names its
	// table says which of the two is which, whichever side the other is written on.
	let keys = [("k".to_string(), "w ?? _join0.w".to_string())];
	for condition in ["ja.k = w", "w = ja.k"] {
		let op = pg_select_in(&format!("SELECT v FROM ja JOIN jb ON {condition}"), &EmptyCatalog);
		assert_eq!(join_keys(&op), keys, "{condition}");
		// The key is no residual: only the guard that the probed key is not NULL remains.
		assert_eq!(join_filter(&op).as_deref(), Some("k != NULL AND k != NONE"), "{condition}");
	}
	let keys = [("v ?? _join0.v".to_string(), "_join0.k".to_string())];
	for condition in ["v = jb.k", "jb.k = v"] {
		let op = pg_select_in(&format!("SELECT v FROM ja JOIN jb ON {condition}"), &EmptyCatalog);
		assert_eq!(join_keys(&op), keys, "{condition}");
	}
}

#[test]
fn a_join_condition_that_names_no_table_on_either_side_is_judged_on_the_joined_row() {
	for condition in ["v = w", "w = v", "v = 'a1'", "'a1' = v", "w = $1"] {
		let op = pg_select_in(&format!("SELECT v FROM ja JOIN jb ON {condition}"), &EmptyCatalog);
		assert_eq!(join_keys(&op), [], "{condition}");
		assert!(join_filter(&op).is_some(), "{condition}");
	}
}

#[test]
fn a_column_of_a_join_in_a_subquery_is_read_from_the_tables_of_the_subquery() {
	let catalog = MemoryCatalog::new();
	let sql = "SELECT 1 FROM jc WHERE EXISTS (SELECT 1 FROM ja JOIN jb ON ja.k = jb.k WHERE w = 1)";
	let op = pg_select_in(sql, &catalog);
	let predicate = predicate(&op);
	assert!(predicate.contains("w ?? _join0.w"), "{predicate}");
	assert!(!predicate.contains("$parent"), "{predicate}");
}

/// `SELECT <column> FROM t0, t1, ...` over `tables` tables.
fn many_tables(column: &str, tables: usize) -> String {
	let from: Vec<String> = (0..tables).map(|n| format!("t{n}")).collect();
	format!("SELECT {column} FROM {}", from.join(", "))
}

#[test]
fn a_column_may_be_in_only_so_many_tables_of_a_join() {
	let catalog = MemoryCatalog::new();
	let bound = pg_bound(&many_tables("v", 64), &catalog).expect("bind");
	assert_eq!(column(&bound, 0).also_in.len(), 63);

	let Err(PlanError::UnsupportedExpression(message)) = pg_bound(&many_tables("v", 65), &catalog)
	else {
		panic!("a column of 65 tables must be refused");
	};
	assert_eq!(
		message,
		"column \"v\" may be in more than 64 tables of the FROM clause, qualify it with its table"
	);
	// The `id` and a qualified column name their table.
	pg_bound(&many_tables("id", 65), &catalog).expect("bind");
	pg_bound(&many_tables("t64.v", 65), &catalog).expect("bind");
}

/// The first aggregation of the pipeline.
fn first_aggregate(op: &LogicalOp) -> Option<&LogicalAggregate> {
	if let LogicalOp::Aggregate(aggregate) = op {
		return Some(aggregate);
	}
	op.children().iter().find_map(first_aggregate)
}

/// The group keys of the first aggregation and the expressions of its fields.
fn aggregation(op: &LogicalOp) -> (Vec<String>, Vec<String>) {
	let aggregate = first_aggregate(op).expect("an aggregation");
	let LogicalFields::Select(fields) = &aggregate.fields else {
		panic!("expected Select fields");
	};
	let fields = fields
		.iter()
		.map(|field| match field {
			LogicalField::All => "*".to_string(),
			LogicalField::Single {
				expr,
				..
			} => expr.to_string(),
		})
		.collect();
	(aggregate.group.iter().map(ToString::to_string).collect(), fields)
}

#[test]
fn a_group_key_is_the_selected_column_it_names_with_or_without_its_table() {
	let groups = [
		("SELECT v, count(*) FROM ja JOIN jb ON ja.k = jb.k GROUP BY v", "v ?? _join0.v"),
		("SELECT ja.v, count(*) FROM ja JOIN jb ON ja.k = jb.k GROUP BY v", "v"),
		("SELECT v, count(*) FROM ja JOIN jb ON ja.k = jb.k GROUP BY ja.v", "v ?? _join0.v"),
		("SELECT jb.w, count(*) FROM ja JOIN jb ON ja.k = jb.k GROUP BY w", "_join0.w"),
		("SELECT w, count(*) FROM ja JOIN jb ON ja.k = jb.k GROUP BY jb.w", "w ?? _join0.w"),
	];
	for (sql, key) in groups {
		let (group, fields) = aggregation(&pg_select_in(sql, &EmptyCatalog));
		assert_eq!(group, [key], "{sql}");
		assert_eq!(fields.first().map(String::as_str), Some(key), "{sql}");
		assert_eq!(fields.len(), 2, "the key and the count, no further key: {sql}");
		let (group, fields) = aggregation(&my_select_in(sql, &EmptyCatalog));
		assert_eq!(group, [key], "{sql}");
		assert_eq!(fields.first().map(String::as_str), Some(key), "{sql}");
		assert_eq!(fields.len(), 2, "the key and the count, no further key: {sql}");
	}
}

#[test]
fn a_column_of_another_table_is_not_the_group_key() {
	let (group, fields) = aggregation(&pg_select_in(
		"SELECT jb.w, count(*) FROM ja JOIN jb ON ja.k = jb.k GROUP BY ja.w",
		&EmptyCatalog,
	));
	assert_eq!(group, ["w"]);
	assert_ne!(fields.first(), group.first());
}

#[test]
fn an_open_column_equals_none_of_the_columns_it_is_compatible_with() {
	let bound =
		pg_bound(&format!("SELECT v, ja.v, jb.v, v {JOIN}"), &MemoryCatalog::new()).expect("bind");
	let (open, of_ja, of_jb, open_again) =
		(column(&bound, 0), column(&bound, 1), column(&bound, 2), column(&bound, 3));

	// The one column that may be in either table can be the column of each.
	for named in [of_ja, of_jb] {
		assert!(open.is_compatible_with(named));
		assert!(named.is_compatible_with(open));
		assert_ne!(open, named);
	}
	// The columns of the two tables are two columns, so compatibility is no equivalence.
	assert!(!of_ja.is_compatible_with(of_jb));
	assert_ne!(of_ja, of_jb);
	// Equality is that of the fields: it is reflexive and a key of a set.
	assert_eq!(open, open_again);
	assert!(open.is_compatible_with(open_again));
	let distinct: HashSet<&BoundColumnRef> = [open, of_ja, of_jb, open_again].into_iter().collect();
	assert_eq!(distinct.len(), 3);
}

/// The expression the `n`th item of the select list of `sql` selects.
fn selected(sql: &str, n: usize) -> BoundExpr {
	pg_bound(sql, &MemoryCatalog::new()).expect("bind").targets[n].expr.clone()
}

#[test]
fn an_expression_is_compatible_with_one_that_differs_only_in_the_column_it_names() {
	let list = "SELECT upper(v), upper(ja.v), upper(jb.v), lower(ja.v), upper(w) \
		FROM ja JOIN jb ON ja.k = jb.k";
	let [open, of_ja, of_jb, other_function, other_column] =
		[0, 1, 2, 3, 4].map(|n| selected(list, n));
	assert!(open.is_compatible_with(&of_ja));
	assert!(of_jb.is_compatible_with(&open));
	assert!(!of_ja.is_compatible_with(&of_jb));
	assert!(!open.is_compatible_with(&other_function));
	assert!(!open.is_compatible_with(&other_column));
	assert_ne!(open, of_ja);

	let nested = "SELECT coalesce(upper(v), 'x') || 'y', coalesce(upper(ja.v), 'x') || 'y', \
		coalesce(upper(ja.v), 'z') || 'y', CASE WHEN v = 1 THEN w END, \
		CASE WHEN ja.v = 1 THEN jb.w END, CASE WHEN ja.v = 1 THEN ja.w END, \
		v IS NULL, ja.v IS NULL, v BETWEEN 1 AND 2, ja.v BETWEEN 1 AND 2, \
		v IN (1, 2), ja.v IN (1, 2), CAST(v AS text), CAST(ja.v AS text) \
		FROM ja JOIN jb ON ja.k = jb.k";
	let compatible = [(0, 1), (3, 4), (6, 7), (8, 9), (10, 11), (12, 13)];
	for (left, right) in compatible {
		let (left, right) = (selected(nested, left), selected(nested, right));
		assert!(left.is_compatible_with(&right), "{left:?} {right:?}");
		assert!(right.is_compatible_with(&left), "{right:?} {left:?}");
		assert_ne!(left, right);
	}
	let different = [(1, 2), (4, 5), (0, 2), (7, 9)];
	for (left, right) in different {
		let (left, right) = (selected(nested, left), selected(nested, right));
		assert!(!left.is_compatible_with(&right), "{left:?} {right:?}");
	}
}

/// The first sort of the pipeline.
fn first_sort(op: &LogicalOp) -> Option<&LogicalSort> {
	if let LogicalOp::Sort(sort) = op {
		return Some(sort);
	}
	op.children().iter().find_map(first_sort)
}

/// The expressions the first sort of the pipeline orders by.
fn sort_keys(op: &LogicalOp) -> Vec<String> {
	let LogicalOrdering::Order(orders) = &first_sort(op).expect("a sort").ordering else {
		panic!("expected an ordering");
	};
	orders.iter().map(|order| order.value.to_string()).collect()
}

#[test]
fn a_selected_expression_is_found_by_the_columns_it_names_with_or_without_the_table() {
	// `upper(v)` is the selected `upper(ja.v)` and `count(w)` the selected `count(jb.w)`, in
	// whichever of the two spellings the select list and the other clauses are written: each is
	// one expression of the aggregation, which the HAVING and the ORDER BY read as the column it
	// outputs.
	let spellings = [
		format!(
			"SELECT upper(ja.v) AS u, count(jb.w) AS n {JOIN} GROUP BY upper(v) ORDER BY upper(v)"
		),
		format!(
			"SELECT upper(v) AS u, count(w) AS n {JOIN} GROUP BY upper(ja.v) ORDER BY upper(ja.v)"
		),
	];
	for sql in spellings {
		let op = pg_select_in(&sql, &EmptyCatalog);
		assert_eq!(sort_keys(&op).last().map(String::as_str), Some("u"), "{sql}");
		assert_eq!(aggregation(&op).1.len(), 2, "{sql}");
	}
	let sql = format!(
		"SELECT ja.v, count(jb.w) AS n {JOIN} GROUP BY ja.v HAVING count(w) > 1 ORDER BY count(w)"
	);
	let op = pg_select_in(&sql, &EmptyCatalog);
	assert_eq!(sort_keys(&op).last().map(String::as_str), Some("n"), "{sql}");
	assert_eq!(aggregation(&op).1.len(), 2, "{sql}");
}

#[test]
fn an_aggregate_named_with_and_without_its_table_is_computed_once() {
	// The key, and the one aggregate that the HAVING and the ORDER BY read.
	let sql = format!(
		"SELECT ja.v {JOIN} GROUP BY ja.v HAVING count(w) > 1 AND count(jb.w) < 9 ORDER BY count(w)"
	);
	assert_eq!(aggregation(&pg_select_in(&sql, &EmptyCatalog)).1.len(), 2, "{sql}");

	// The same aggregate of two tables is two aggregates.
	let sql = format!("SELECT ja.v {JOIN} GROUP BY ja.v HAVING max(jb.k) > 1 AND max(ja.k) < 9");
	assert_eq!(aggregation(&pg_select_in(&sql, &EmptyCatalog)).1.len(), 3, "{sql}");
}
