//! The `Window` operators a select's window function calls lower to, where they sit in the plan,
//! and the window constructs that are refused.

// A catch-all arm in these tests reports the value a lowering produced instead of the one
// expected; naming every other variant would add nothing to that report.
#![allow(clippy::wildcard_enum_match_arm)]

use surrealdb_expr::expr::logical::{
	LogicalExpr, LogicalFields, LogicalOp, LogicalWindow, WindowFrame, WindowFrameBound,
	WindowFrameUnits, WindowFunction,
};
use surrealdb_types::ToSql;

use crate::catalog::EmptyCatalog;
use crate::dialect::DialectPolicy;
use crate::error::PlanError;

fn pg(sql: &str) -> Result<LogicalOp, PlanError> {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::bind_and_lower_pgsql(select, &EmptyCatalog, &DialectPolicy::postgres())
}

fn my(sql: &str) -> Result<LogicalOp, PlanError> {
	let stmts = surrealdb_mysql::parse_statements(sql).expect("parse");
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::bind_and_lower_mysql(select, &EmptyCatalog, &DialectPolicy::mysql())
}

fn plan(sql: &str) -> LogicalOp {
	pg(sql).unwrap_or_else(|err| panic!("{sql}: {err}"))
}

/// The operators of the pipeline from the root down, following the first input of each.
fn spine(op: &LogicalOp) -> Vec<&LogicalOp> {
	let mut out = vec![op];
	let mut node = op;
	while let Some(child) = node.children().first() {
		out.push(child);
		node = child;
	}
	out
}

/// The names of the operators of the pipeline from the root down.
fn names(op: &LogicalOp) -> Vec<&'static str> {
	spine(op).into_iter().map(LogicalOp::name).collect()
}

/// The `Window` operators of the pipeline from the root down.
fn windows(op: &LogicalOp) -> Vec<&LogicalWindow> {
	spine(op)
		.into_iter()
		.filter_map(|node| match node {
			LogicalOp::Window(window) => Some(window),
			_ => None,
		})
		.collect()
}

fn texts(exprs: impl IntoIterator<Item = impl ToString>) -> Vec<String> {
	exprs.into_iter().map(|e| e.to_string()).collect()
}

/// The text of the root projection's fields.
fn projected(op: &LogicalOp) -> String {
	let LogicalOp::Project(project) = op else {
		panic!("expected a Project, got {op}");
	};
	match &project.fields {
		LogicalFields::Select(fields) => texts(fields.iter().map(|f| format!("{f:?}"))).join(", "),
		LogicalFields::Value {
			expr,
			..
		} => expr.to_string(),
	}
}

const RANGE_TO_CURRENT: WindowFrame = WindowFrame {
	units: WindowFrameUnits::Range,
	start: WindowFrameBound::UnboundedPreceding,
	end: WindowFrameBound::CurrentRow,
};

const WHOLE_PARTITION: WindowFrame = WindowFrame {
	units: WindowFrameUnits::Rows,
	start: WindowFrameBound::UnboundedPreceding,
	end: WindowFrameBound::UnboundedFollowing,
};

#[test]
fn a_window_call_sits_above_the_filter_and_below_the_sort_and_projection() {
	let op =
		plan("SELECT n, rank() OVER (PARTITION BY s ORDER BY n) FROM t WHERE n > 1 ORDER BY n");
	assert_eq!(names(&op), ["Project", "Sort", "Window", "Filter", "Source"], "{op}");
	let [window] = windows(&op)[..] else {
		panic!("one window: {op}");
	};
	assert_eq!(texts(&window.partition), ["s"]);
	// ASC NULLS LAST is two engine keys.
	assert_eq!(window.order.len(), 2, "{op}");
	assert_eq!(window.calls.len(), 1);
	let call = &window.calls[0];
	assert_eq!(call.output.as_str(), "__sql_window_0");
	assert_eq!(call.function, WindowFunction::Rank);
	assert!(call.args.is_empty());
	assert_eq!(call.frame, RANGE_TO_CURRENT);
	assert!(projected(&op).contains("__sql_window_0"), "{op}");
}

#[test]
fn a_window_call_is_read_under_the_name_of_its_function() {
	// A subquery's select is not keyed, so its unaliased column keeps the function's name.
	let op = plan("SELECT * FROM (SELECT row_number() OVER () FROM t) AS d");
	let text = op.to_string();
	assert!(text.contains("__sql_window_0 AS row_number"), "{text}");
}

#[test]
fn calls_over_one_window_share_one_operator() {
	let op = plan(
		"SELECT row_number() OVER (PARTITION BY s ORDER BY n), sum(n) OVER (PARTITION BY s ORDER BY n) FROM t",
	);
	let [window] = windows(&op)[..] else {
		panic!("one window: {op}");
	};
	let functions: Vec<_> = window.calls.iter().map(|c| c.function).collect();
	assert_eq!(functions, [WindowFunction::RowNumber, WindowFunction::Sum]);
	assert_eq!(texts(&window.calls[1].args), ["n"]);
	let outputs: Vec<_> = window.calls.iter().map(|c| c.output.as_str()).collect();
	assert_eq!(outputs, ["__sql_window_0", "__sql_window_1"]);
}

#[test]
fn equal_calls_share_one_output() {
	let op = plan("SELECT rank() OVER (ORDER BY n) AS a, rank() OVER (ORDER BY n) AS b FROM t");
	let [window] = windows(&op)[..] else {
		panic!("one window: {op}");
	};
	assert_eq!(window.calls.len(), 1, "{op}");
	let text = projected(&op);
	assert_eq!(text.matches("__sql_window_0").count(), 2, "{text}");
}

#[test]
fn calls_over_two_windows_stack_two_operators_in_order_of_appearance() {
	let op = plan(
		"SELECT row_number() OVER (ORDER BY n), count(*) OVER (PARTITION BY s) FROM t WHERE n > 0",
	);
	assert_eq!(names(&op), ["Project", "Window", "Window", "Filter", "Source"], "{op}");
	let [outer, inner] = windows(&op)[..] else {
		panic!("two windows: {op}");
	};
	assert_eq!(inner.calls[0].function, WindowFunction::RowNumber);
	assert!(inner.partition.is_empty());
	assert_eq!(outer.calls[0].function, WindowFunction::Count);
	assert!(outer.calls[0].args.is_empty(), "count(*) counts rows");
	assert_eq!(texts(&outer.partition), ["s"]);
	assert!(outer.order.is_empty());
	assert_eq!(outer.calls[0].frame, WHOLE_PARTITION);
	assert_eq!(outer.calls[0].output.as_str(), "__sql_window_1");
}

#[test]
fn a_window_call_in_order_by_is_computed_and_sorted_on() {
	let op = plan("SELECT n FROM t ORDER BY row_number() OVER (ORDER BY n DESC) LIMIT 3");
	assert_eq!(names(&op), ["Project", "Limit", "Sort", "Window", "Source"], "{op}");
	let LogicalOp::Sort(sort) = spine(&op)[2] else {
		unreachable!();
	};
	assert!(sort.ordering.to_sql().contains("__sql_window_0"), "{op}");
	assert_eq!(windows(&op)[0].calls[0].function, WindowFunction::RowNumber);
}

#[test]
fn a_window_over_a_grouped_select_reads_the_aggregation() {
	let op = plan(
		"SELECT s, sum(n), rank() OVER (ORDER BY sum(n) DESC) FROM t GROUP BY s HAVING count(*) > 1",
	);
	assert_eq!(names(&op), ["Project", "Window", "Filter", "Aggregate", "Source"], "{op}");
	let [window] = windows(&op)[..] else {
		panic!("one window: {op}");
	};
	// The selected `sum(n)` is read from its output column.
	let order = texts(window.order.iter().map(|o| o.value.clone()));
	assert!(order.iter().all(|key| key.contains("sum")), "{order:?}");
	let LogicalOp::Aggregate(aggregate) = spine(&op)[3] else {
		unreachable!();
	};
	assert!(!aggregate.fields.to_sql().contains("rank"), "{op}");
	assert!(projected(&op).contains("__sql_window_0"), "{op}");
}

#[test]
fn a_window_reads_an_aggregate_it_does_not_select_from_a_hidden_column() {
	let op = plan("SELECT s, lag(max(n)) OVER (ORDER BY s) AS prev FROM t GROUP BY s");
	let LogicalOp::Aggregate(aggregate) = spine(&op)[2] else {
		panic!("expected the aggregation under the window: {op}");
	};
	assert!(aggregate.fields.to_sql().contains("__sql_window_agg_0"), "{op}");
	let [window] = windows(&op)[..] else {
		panic!("one window: {op}");
	};
	assert_eq!(window.calls[0].function, WindowFunction::Lag);
	assert_eq!(texts(&window.calls[0].args), ["__sql_window_agg_0"]);
	let text = projected(&op);
	assert!(text.contains("__sql_window_0"), "{text}");
	assert!(!text.contains("__sql_window_agg_0"), "{text}");
}

#[test]
fn a_hidden_aggregate_column_never_takes_a_selected_name() {
	let op = plan(
		"SELECT s AS __sql_window_agg_0, lag(max(n)) OVER (ORDER BY s) AS prev FROM t GROUP BY s",
	);
	let [window] = windows(&op)[..] else {
		panic!("one window: {op}");
	};
	assert_eq!(texts(&window.calls[0].args), ["__sql_window_agg_0_"]);
}

#[test]
fn a_window_over_an_aggregate_makes_the_select_an_aggregate() {
	let op = plan("SELECT sum(sum(n)) OVER () FROM t");
	assert_eq!(names(&op), ["Project", "Window", "Aggregate", "Source"], "{op}");
}

#[test]
fn a_wildcard_omits_the_window_fields() {
	for sql in
		["SELECT *, rank() OVER (ORDER BY n) FROM t", "SELECT t.*, rank() OVER (ORDER BY n) FROM t"]
	{
		let op = plan(sql);
		let LogicalOp::Project(project) = &op else {
			panic!("expected a Project: {op}");
		};
		assert_eq!(project.omit, [LogicalExpr::Field("__sql_window_0".into())], "{sql}: {op}");
	}
	let op = plan("SELECT n, rank() OVER (ORDER BY n) FROM t");
	let LogicalOp::Project(project) = &op else {
		panic!("expected a Project: {op}");
	};
	assert!(project.omit.is_empty(), "{op}");
}

#[test]
fn a_named_window_is_the_window_it_names() {
	let named =
		plan("SELECT rank() OVER w, sum(n) OVER w FROM t WINDOW w AS (PARTITION BY s ORDER BY n)");
	let inline = plan(
		"SELECT rank() OVER (PARTITION BY s ORDER BY n), sum(n) OVER (PARTITION BY s ORDER BY n) FROM t",
	);
	assert_eq!(windows(&named), windows(&inline));
	let my_named =
		my("SELECT RANK() OVER w FROM t WINDOW w AS (PARTITION BY s ORDER BY n)").expect("lower");
	let [window] = windows(&my_named)[..] else {
		panic!("one window: {my_named}");
	};
	assert_eq!(texts(&window.partition), ["s"]);
}

#[test]
fn an_explicit_frame_is_kept() {
	let op = plan(
		"SELECT sum(n) OVER (ORDER BY n ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING), \
		 first_value(n) OVER (ORDER BY n ROWS 3 PRECEDING), \
		 last_value(n) OVER (ORDER BY n RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
	);
	let [window] = windows(&op)[..] else {
		panic!("one window: {op}");
	};
	let frames: Vec<_> = window.calls.iter().map(|c| c.frame).collect();
	assert_eq!(
		frames,
		[
			WindowFrame {
				units: WindowFrameUnits::Rows,
				start: WindowFrameBound::Preceding(2),
				end: WindowFrameBound::Following(1),
			},
			WindowFrame {
				units: WindowFrameUnits::Rows,
				start: WindowFrameBound::Preceding(3),
				end: WindowFrameBound::CurrentRow,
			},
			WindowFrame {
				units: WindowFrameUnits::Range,
				start: WindowFrameBound::CurrentRow,
				end: WindowFrameBound::UnboundedFollowing,
			},
		]
	);
}

#[test]
fn every_window_function_maps_to_the_engine() {
	let op = plan(
		"SELECT row_number() OVER w, rank() OVER w, dense_rank() OVER w, percent_rank() OVER w, \
		 cume_dist() OVER w, ntile(3) OVER w, lag(n, 2, 0) OVER w, lead(n) OVER w, \
		 first_value(n) OVER w, last_value(n) OVER w, nth_value(n, 2) OVER w, sum(n) OVER w, \
		 avg(n) OVER w, count(n) OVER w, min(n) OVER w, max(n) OVER w \
		 FROM t WINDOW w AS (ORDER BY n)",
	);
	let [window] = windows(&op)[..] else {
		panic!("one window: {op}");
	};
	let functions: Vec<_> = window.calls.iter().map(|c| c.function).collect();
	assert_eq!(
		functions,
		[
			WindowFunction::RowNumber,
			WindowFunction::Rank,
			WindowFunction::DenseRank,
			WindowFunction::PercentRank,
			WindowFunction::CumeDist,
			WindowFunction::Ntile,
			WindowFunction::Lag,
			WindowFunction::Lead,
			WindowFunction::FirstValue,
			WindowFunction::LastValue,
			WindowFunction::NthValue,
			WindowFunction::Sum,
			WindowFunction::Avg,
			WindowFunction::Count,
			WindowFunction::Min,
			WindowFunction::Max,
		]
	);
	assert_eq!(texts(&window.calls[6].args), ["n", "2", "0"]);
	assert_eq!(texts(&window.calls[13].args), ["n"]);
	assert!(window.calls.iter().all(|c| c.frame == RANGE_TO_CURRENT));
}

#[test]
fn unsupported_window_constructs_are_refused() {
	for sql in [
		"SELECT sum(n) OVER (ORDER BY n GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
		"SELECT sum(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM t",
		"SELECT sum(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
		"SELECT sum(n) OVER (ORDER BY n ROWS BETWEEN $1 PRECEDING AND CURRENT ROW) FROM t",
		"SELECT sum(n) OVER (ORDER BY n ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM t",
		"SELECT count(DISTINCT n) OVER () FROM t",
		"SELECT string_agg(s, ',') OVER () FROM t",
		"SELECT n FROM t UNION SELECT n FROM u ORDER BY rank() OVER ()",
		"SELECT lag(n, n - 1) OVER (ORDER BY n) FROM t",
		"SELECT lead(n, n) OVER (ORDER BY n) FROM t",
		"SELECT nth_value(n, abs(n)) OVER (ORDER BY n) FROM t",
		"SELECT ntile(n) OVER (ORDER BY n) FROM t",
		"SELECT sum(random()) OVER (), sum(random()) OVER () FROM t",
		"SELECT row_number() OVER (ORDER BY random()) AS a, row_number() OVER (ORDER BY random()) AS b FROM t",
	] {
		let err = pg(sql).expect_err(sql);
		assert!(matches!(err, PlanError::UnsupportedExpression(_)), "{sql}: {err:?}");
		assert_eq!(err.pg_sqlstate(), "0A000", "{sql}");
	}
}

#[test]
fn equal_calls_over_a_volatile_expression_share_only_with_order_by() {
	let op = plan("SELECT sum(random()) OVER () AS a FROM t ORDER BY sum(random()) OVER ()");
	let outputs: Vec<_> =
		windows(&op).iter().flat_map(|w| w.calls.iter().map(|c| c.output.to_string())).collect();
	assert_eq!(outputs, ["__sql_window_0"], "{op}");
	let op = plan("SELECT sum(abs(n)) OVER () AS a, sum(abs(n)) OVER () AS b FROM t");
	assert_eq!(windows(&op)[0].calls.len(), 1, "{op}");
}

#[test]
fn a_distinct_may_not_order_by_what_it_does_not_select() {
	for sql in [
		"SELECT DISTINCT s FROM t ORDER BY row_number() OVER (ORDER BY n)",
		"SELECT DISTINCT s FROM t ORDER BY n",
		"SELECT DISTINCT sum(n) AS v FROM t GROUP BY s ORDER BY count(*)",
	] {
		let err = pg(sql).expect_err(sql);
		assert_eq!(err, PlanError::DistinctOrderByNotSelected, "{sql}");
		assert_eq!(err.pg_sqlstate(), "42P10", "{sql}");
	}
}

#[test]
fn a_distinct_may_order_by_a_window_it_selects() {
	plan(
		"SELECT DISTINCT s, row_number() OVER (ORDER BY n) AS r FROM t ORDER BY row_number() OVER (ORDER BY n)",
	);
	plan("SELECT DISTINCT s FROM t ORDER BY s");
}

#[test]
fn a_constant_offset_or_position_is_kept() {
	for sql in [
		"SELECT lag(n, 2) OVER (ORDER BY n) FROM t",
		"SELECT lead(n, $1, 0) OVER (ORDER BY n) FROM t",
		"SELECT lag(n, 1 + 1) OVER (ORDER BY n) FROM t",
		"SELECT nth_value(n, CAST($1 AS int)) OVER (ORDER BY n) FROM t",
	] {
		pg(sql).expect(sql);
	}
}

#[test]
fn a_window_call_outside_the_select_list_and_order_by_is_not_allowed() {
	for sql in [
		"SELECT n FROM t WHERE rank() OVER () > 1",
		"SELECT n FROM t GROUP BY rank() OVER ()",
		"SELECT count(*) FROM t HAVING rank() OVER () > 1",
		"SELECT lag(rank() OVER ()) OVER () FROM t",
	] {
		let err = pg(sql).expect_err(sql);
		assert!(matches!(err, PlanError::WindowNotAllowed(_)), "{sql}: {err:?}");
	}
}

/// The rendered plan of the example the module documents.
#[test]
fn the_rendered_plan() {
	let op = plan("SELECT n, rank() OVER (PARTITION BY s ORDER BY n) FROM t ORDER BY n");
	println!("{op}");
}

/// A `DISTINCT` compares the columns a select projects, so it sits above the projection. An
/// `ORDER BY` of a named selected column, by its alias or by its expression, sorts above it on that
/// column, and adds no field for a projection above to drop.
#[test]
fn a_distinct_compares_the_projected_window_columns() {
	for (sql, above) in [
		("SELECT DISTINCT rank() OVER (ORDER BY s) AS r FROM t", vec!["Distinct"]),
		(
			"SELECT DISTINCT rank() OVER (ORDER BY s) AS r FROM t ORDER BY r DESC LIMIT 2",
			vec!["Limit", "Sort", "Distinct"],
		),
		(
			"SELECT DISTINCT rank() OVER (ORDER BY s) AS r FROM t ORDER BY rank() OVER (ORDER BY s)",
			vec!["Sort", "Distinct"],
		),
	] {
		let op = plan(sql);
		let names = names(&op);
		assert_eq!(names[..above.len()], above[..], "{sql}: {names:?}");
		assert_eq!(names[above.len()..], ["Project", "Window", "Source"], "{sql}: {names:?}");
	}
}

/// A `SELECT DISTINCT` ordered by an unnamed selected column orders its rows by the value the
/// column answers, which is computed once.
#[test]
fn a_distinct_orders_by_the_selected_value_it_answers() {
	for (sql, op) in [
		(
			"SELECT DISTINCT n + random() FROM t ORDER BY n + random()",
			pg("SELECT DISTINCT n + random() FROM t ORDER BY n + random()"),
		),
		(
			"SELECT DISTINCT n + rand() FROM t ORDER BY n + rand()",
			my("SELECT DISTINCT n + rand() FROM t ORDER BY n + rand()"),
		),
	] {
		let op = op.unwrap_or_else(|err| panic!("{sql}: {err}"));
		let text = op.to_string();
		let above = text.split("Distinct").next().unwrap_or_default();
		assert!(!above.contains("rand"), "{sql}: {text}");
		assert!(!text.contains("__sql_distinct_order"), "{sql}: {text}");
	}
}

#[test]
fn a_distinct_over_a_wildcard_may_not_order_by_a_window_it_does_not_select() {
	for sql in [
		"SELECT DISTINCT * FROM t ORDER BY row_number() OVER (ORDER BY n) DESC",
		"SELECT DISTINCT t.* FROM t ORDER BY rank() OVER (ORDER BY n)",
	] {
		match pg(sql) {
			Ok(op) => panic!("{sql} planned: {op}"),
			Err(error) => {
				assert!(matches!(error, PlanError::DistinctOrderByNotSelected), "{sql}: {error}")
			}
		}
	}
}

#[test]
fn only_a_row_every_row_holds_is_counted_as_a_row() {
	let star = |relation_index: usize, relation_name: &str| crate::ir::BoundColumnRef {
		relation_index,
		relation_name: relation_name.to_string(),
		column: "*".to_string(),
		correlation_depth: 0,
		id_table: None,
		link_table: None,
		enum_column: None,
		composite: None,
		declared_sql: None,
		aggregate_output: false,
		key_read: crate::ir::KeyRead::None,
		also_in: Vec::new(),
		carried_as: None,
		declared_type: crate::SqlType::Unknown,
	};
	assert!(crate::lower::expr::counts_every_row(&star(0, "")));
	assert!(crate::lower::expr::counts_every_row(&star(0, "t")));
	assert!(!crate::lower::expr::counts_every_row(&star(1, "r")));
	assert_eq!(
		crate::lower::expr::lower_column_raw(&star(1, "r")).to_string(),
		surrealdb_expr::expr::logical::RelationAlias::new(0).field().to_string(),
	);
}

/// A wildcard selects the columns of the relations it covers: a `t.*` does not select a joined
/// relation's column, and a `*` selects it, read from a hidden field since the row the `*`
/// projects does not hold a joined relation's columns under their own names.
#[test]
fn a_distinct_orders_by_a_column_only_of_a_relation_its_wildcard_covers() {
	let mut catalog = crate::catalog::MemoryCatalog::new();
	catalog.define_table(None, "t", &["n", "x"]);
	catalog.define_table(None, "u", &["n", "x"]);
	let known = |sql: &str| {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		crate::bind_and_lower_pgsql(select, &catalog, &DialectPolicy::postgres())
	};
	let sql = "SELECT DISTINCT t.* FROM t JOIN u ON t.n = u.n ORDER BY u.x";
	match known(sql) {
		Ok(op) => panic!("{sql} planned: {op}"),
		Err(error) => assert!(matches!(error, PlanError::DistinctOrderByNotSelected), "{error}"),
	}
	let sql = "SELECT DISTINCT t.* FROM t JOIN u ON t.n = u.n ORDER BY t.x";
	known(sql).unwrap_or_else(|err| panic!("{sql}: {err}"));

	let op = plan("SELECT DISTINCT * FROM t JOIN u ON t.n = u.n ORDER BY u.x");
	assert_eq!(names(&op)[..3], ["Project", "Sort", "Distinct"], "{op}");
	assert!(op.to_string().contains("__sql_distinct_order_0"), "{op}");
	let op = plan("SELECT DISTINCT * FROM t JOIN u ON t.n = u.n ORDER BY t.x");
	assert_eq!(names(&op)[..2], ["Sort", "Distinct"], "{op}");
}
