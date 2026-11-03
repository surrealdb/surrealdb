//! A record link column read through a SQL front end as an operand of an expression.
//!
//! A link column that stands alone (a projected item, a group key, an ordering key, a join key, a
//! null test, or an operand of an equality) is the link as stored, which the wire layer shows as
//! its key. Anywhere else it is its key, so arithmetic, text, casts and the ordering of the key's
//! own type see an integer or a string and not a record.

// A catch-all arm in these tests reports the value a lowering produced instead of the one
// expected; naming every other variant would add nothing to that report.
#![allow(clippy::wildcard_enum_match_arm)]

use surrealdb_expr::expr::logical::{LogicalField, LogicalFields, LogicalOp, LogicalOrdering};
use surrealdb_types::ToSql;

use crate::lower::link_tests::{join_keys, my_select, pg_select, predicate, projection};

/// The key of the link column `dept`, as an operand.
pub(super) const KEY: &str = "(IF type::is_record(dept) THEN record::id(dept) ELSE dept END)";

/// The key of the link column `mentor`, as an operand.
pub(super) const MENTOR_KEY: &str =
	"(IF type::is_record(mentor) THEN record::id(mentor) ELSE mentor END)";

/// The key of the link column `dept` of the first joined table, as an operand.
const JOINED_KEY: &str =
	"(IF type::is_record(_join0.dept) THEN record::id(_join0.dept) ELSE _join0.dept END)";

/// The operator nodes of the pipeline, from the outermost.
fn nodes(op: &LogicalOp) -> Vec<&LogicalOp> {
	let mut all = vec![op];
	for child in op.children() {
		all.extend(nodes(child));
	}
	all
}

/// The ordering keys of the pipeline.
fn sort_keys(op: &LogicalOp) -> Vec<String> {
	nodes(op)
		.into_iter()
		.find_map(|node| match node {
			LogicalOp::Sort(sort) => match &sort.ordering {
				LogicalOrdering::Order(orders) => {
					Some(orders.iter().map(|o| o.value.to_string()).collect())
				}
				LogicalOrdering::Random => None,
			},
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_)
			| LogicalOp::Window(_) => None,
		})
		.expect("a sort")
}

/// The group keys and the output fields of the aggregation of the pipeline.
fn aggregation(op: &LogicalOp) -> (Vec<String>, Vec<String>) {
	nodes(op)
		.into_iter()
		.find_map(|node| match node {
			LogicalOp::Aggregate(aggregate) => {
				let LogicalFields::Select(fields) = &aggregate.fields else {
					panic!("expected a projection list");
				};
				Some((
					aggregate.group.iter().map(ToString::to_string).collect(),
					fields.iter().map(field_text).collect(),
				))
			}
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_)
			| LogicalOp::Window(_) => None,
		})
		.expect("an aggregation")
}

/// The text of an output field: its expression and its alias.
fn field_text(field: &LogicalField) -> String {
	match field {
		LogicalField::All => "*".to_string(),
		LogicalField::Single {
			expr,
			alias,
			..
		} => match alias {
			Some(alias) => format!("{expr} AS {}", alias.to_sql()),
			None => expr.to_string(),
		},
	}
}

fn projected(sql: &str) -> String {
	projection(&pg_select(sql)).join(", ")
}

#[test]
fn a_link_in_arithmetic_is_its_key() {
	let text = projected("SELECT dept + 1 AS n FROM emp");
	assert!(text.contains(&format!("{KEY} + 1")), "{text}");
	let text = projected("SELECT 2 * dept AS n FROM emp");
	assert!(text.contains(&format!("2 * {KEY}")), "{text}");
	let text = projected("SELECT dept % 2 AS n FROM emp");
	assert!(text.contains(&format!("{KEY} % 2")), "{text}");
	let text = projected("SELECT dept / 2 AS n FROM emp");
	assert!(text.contains(&format!("{KEY} / 2")), "{text}");
}

#[test]
fn a_negated_link_is_its_key() {
	let text = projected("SELECT -dept AS n FROM emp");
	assert!(text.contains(&format!("-{KEY}")), "{text}");
}

#[test]
fn a_link_in_a_concatenation_is_its_key() {
	let text = projected("SELECT dept || 'x' AS s FROM emp");
	assert!(
		text.contains(&format!("string::concat(type::string({KEY}), type::string('x'))")),
		"{text}"
	);
}

#[test]
fn a_link_matched_with_like_is_its_key() {
	let op = pg_select("SELECT name FROM emp WHERE dept LIKE '1%'");
	assert!(predicate(&op).contains(&format!("string::matches({KEY}, ")), "{}", predicate(&op));
}

#[test]
fn a_link_passed_to_a_function_is_its_key() {
	let text = projected("SELECT abs(dept) AS n FROM emp");
	assert!(
		text.contains(KEY) && text.contains("math::abs([0])") && text.contains("THEN NULL"),
		"{text}"
	);
	let text = projected("SELECT upper(dept) AS n FROM emp");
	assert!(
		text.contains(KEY) && text.contains("string::uppercase(") && text.contains("THEN NULL"),
		"{text}"
	);
}

#[test]
fn a_link_cast_is_cast_from_its_key() {
	let text = projected("SELECT CAST(dept AS text) AS t FROM emp");
	assert!(text.contains(KEY) && text.contains("<string>"), "{text}");
	let text = projected("SELECT CAST(dept AS int) AS t FROM emp");
	assert!(text.contains(KEY) && text.contains("<int>"), "{text}");
	let text = projected("SELECT dept::text AS t FROM emp");
	assert!(text.contains(KEY) && text.contains("<string>"), "{text}");
	let text = projected("SELECT dept::int AS t FROM emp");
	assert!(text.contains(KEY) && text.contains("<int>"), "{text}");
}

#[test]
fn a_link_in_a_case_is_its_key() {
	let text = projected("SELECT CASE dept WHEN 1 THEN 'a' ELSE 'b' END AS c FROM emp");
	assert!(text.contains(&format!("{KEY} = 1")), "{text}");
	let text = projected("SELECT CASE WHEN dept IS NULL THEN 0 ELSE dept END AS c FROM emp");
	assert!(text.contains(&format!("ELSE {KEY} END")), "{text}");
}

#[test]
fn a_link_in_coalesce_and_nullif_is_its_key() {
	let text = projected("SELECT COALESCE(dept, 0) AS c FROM emp");
	assert!(text.contains(&format!("{KEY} ?? 0")), "{text}");
	let text = projected("SELECT NULLIF(dept, 1) AS c FROM emp");
	assert!(text.contains(&format!("{KEY} = 1")), "{text}");
	assert!(text.contains(&format!("ELSE {KEY} END")), "{text}");
}

#[test]
fn a_link_in_an_aggregate_is_its_key() {
	for function in ["sum", "min", "max", "avg"] {
		let op = pg_select(&format!("SELECT {function}(dept) AS a FROM emp"));
		let (_, fields) = aggregation(&op);
		assert!(fields[0].contains(KEY), "{function}: {fields:?}");
	}
	let op = pg_select("SELECT COUNT(DISTINCT dept) AS a FROM emp");
	let (_, fields) = aggregation(&op);
	assert!(fields[0].contains(KEY), "{fields:?}");
}

#[test]
fn counting_a_link_reads_the_stored_link() {
	let op = pg_select("SELECT COUNT(dept) AS a FROM emp");
	let (_, fields) = aggregation(&op);
	assert!(!fields[0].contains("record::id"), "{fields:?}");
	assert!(fields[0].contains("count::present(dept)"), "{fields:?}");
}

#[test]
fn a_link_in_a_having_clause_expression_is_its_key() {
	let op = pg_select("SELECT dept, count(*) AS n FROM emp GROUP BY dept HAVING dept + 1 > 2");
	assert!(predicate(&op).contains(&format!("{KEY} + 1")), "{}", predicate(&op));
}

#[test]
fn a_link_of_a_joined_table_or_an_outer_row_is_its_key() {
	let op = pg_select("SELECT t.dept + 1 AS n FROM emp e JOIN task t ON e.age = t.age");
	let text = projection(&op).join(", ");
	assert!(text.contains(&format!("{JOINED_KEY} + 1")), "{text}");

	let op = pg_select(
		"SELECT e.name FROM emp e WHERE EXISTS (SELECT 1 FROM task t WHERE e.dept + 1 = t.age)",
	);
	let text = predicate(&op);
	assert!(
		text.contains(
			"(IF type::is_record($parent.dept) THEN record::id($parent.dept) ELSE $parent.dept END) + 1"
		),
		"{text}"
	);
}

#[test]
fn a_link_in_an_expression_of_a_where_clause_is_its_key() {
	let op = pg_select("SELECT name FROM emp WHERE dept + 1 = 3");
	assert!(predicate(&op).contains(&format!("{KEY} + 1")), "{}", predicate(&op));
	let op = pg_select("SELECT name FROM emp WHERE abs(dept) = 3");
	assert!(predicate(&op).contains(KEY), "{}", predicate(&op));
}

#[test]
fn a_link_in_an_ordering_comparison_is_compared_by_its_key() {
	for (sql, ordering) in [
		("dept > 1", format!("ELSE {KEY} > 1 END")),
		("dept >= 1.5", format!("ELSE {KEY} >= 1.5dec END")),
		("dept < 10", format!("ELSE {KEY} < 10 END")),
		("dept <= 'ops'", format!("ELSE {KEY} <= 'ops' END")),
		("1 < dept", format!("ELSE 1 < {KEY} END")),
	] {
		let text = predicate(&pg_select(&format!("SELECT name FROM emp WHERE {sql}")));
		assert!(text.ends_with(&ordering), "{sql}: {text}");
		assert!(!text.replace(KEY, "").contains("dept"), "{sql}: {text}");
	}
	let text = predicate(&pg_select("SELECT name FROM emp WHERE dept > mentor"));
	assert!(text.contains(&format!("ELSE {KEY} > {MENTOR_KEY} END")), "{text}");
}

#[test]
fn a_link_between_bounds_is_compared_by_its_key() {
	for sql in [
		"SELECT name FROM emp WHERE dept BETWEEN 1 AND 3",
		"SELECT name FROM emp WHERE dept NOT BETWEEN 1 AND 3",
		"SELECT name FROM emp WHERE dept BETWEEN 1.5 AND 'x'",
	] {
		let text = predicate(&pg_select(sql));
		assert!(text.contains(KEY), "{text}");
		assert!(text.contains("THEN NULL"), "{text}");
	}
}

#[test]
fn a_link_of_a_joined_table_in_an_ordering_comparison_is_compared_by_its_key() {
	let op = pg_select("SELECT e.name FROM emp e JOIN task t ON e.age = t.age WHERE t.dept > 1");
	assert!(predicate(&op).contains(&format!("ELSE {JOINED_KEY} > 1 END")), "{}", predicate(&op));
}

#[test]
fn mysql_compares_a_link_in_an_ordering_comparison_by_its_key() {
	let op = my_select("SELECT name FROM emp WHERE dept > 1");
	assert!(predicate(&op).ends_with(&format!("ELSE {KEY} > 1 END")), "{}", predicate(&op));
	let op = my_select("SELECT name FROM emp WHERE dept BETWEEN 1 AND 3");
	assert!(
		predicate(&op).contains(KEY) && predicate(&op).contains("THEN NULL"),
		"{}",
		predicate(&op)
	);
}

#[test]
fn a_link_that_stands_alone_is_the_link_as_stored() {
	let op = pg_select("SELECT dept FROM emp");
	assert_eq!(projection(&op), ["dept AS dept"]);
	let op = pg_select("SELECT dept AS d, name FROM emp");
	assert_eq!(projection(&op), ["dept AS d", "name AS name"]);
	// The DISTINCT compares the projected rows.
	let op = pg_select("SELECT DISTINCT dept FROM emp");
	let LogicalOp::Distinct(distinct) = &op else {
		panic!("expected Distinct, got {op:?}");
	};
	assert_eq!(projection(&distinct.input), ["dept AS dept"]);
}

#[test]
fn a_link_that_is_grouped_or_ordered_by_is_the_link_as_stored() {
	let op = pg_select("SELECT dept, count(*) AS n FROM emp GROUP BY dept");
	let (group, fields) = aggregation(&op);
	assert_eq!(group, ["dept"]);
	assert_eq!(fields[0], "dept AS dept");

	let op = pg_select("SELECT name FROM emp ORDER BY dept");
	assert_eq!(sort_keys(&op), ["dept = NULL OR dept = NONE", "dept"]);
	let op = pg_select("SELECT name FROM emp ORDER BY dept DESC NULLS LAST");
	assert_eq!(sort_keys(&op), ["dept"]);
}

#[test]
fn a_link_that_is_tested_for_null_is_the_link_as_stored() {
	let op = pg_select("SELECT name FROM emp WHERE mentor IS NULL");
	assert_eq!(predicate(&op), "mentor = NULL OR mentor = NONE");
	let op = pg_select("SELECT name FROM emp WHERE mentor IS NOT NULL");
	assert_eq!(predicate(&op), "!(mentor = NULL OR mentor = NONE)");
}

#[test]
fn a_link_that_is_a_join_key_is_the_link_as_stored() {
	let op = pg_select("SELECT e.name FROM emp e JOIN task t ON e.dept = t.dept");
	assert_eq!(join_keys(&op), [("dept".to_string(), "_join0.dept".to_string())]);
	let op = pg_select("SELECT e.name FROM emp e JOIN task t USING (dept)");
	assert_eq!(join_keys(&op), [("dept".to_string(), "_join0.dept".to_string())]);
	let op = pg_select("SELECT e.name FROM emp e JOIN task t ON e.dept = 2.5");
	assert_eq!(join_keys(&op), [("dept".to_string(), "2.5dec".to_string())]);
}

#[test]
fn a_scalar_subquery_projects_the_key_of_a_link() {
	let op = pg_select("SELECT name FROM emp WHERE dept IN (SELECT dept FROM task)");
	let text = predicate(&op);
	assert!(text.contains(KEY) && text.contains("INSIDE"), "{text}");
	assert!(text.contains(&format!("Project[VALUE {KEY}]")), "{text}");
}

#[test]
fn a_link_equal_to_a_value_is_compared_with_the_record_it_names() {
	let op = pg_select("SELECT name FROM emp WHERE dept = 1");
	assert_eq!(predicate(&op), "dept = (dept:1)");
	let op = pg_select("SELECT name FROM emp WHERE dept = mentor");
	assert!(!predicate(&op).contains("record::id(dept)"), "{}", predicate(&op));
}

#[test]
fn mysql_reads_a_link_in_an_expression_as_its_key() {
	let text = projection(&my_select("SELECT dept + 1 AS n FROM emp")).join(", ");
	assert!(text.contains(&format!("{KEY} + 1")), "{text}");
	let text = projection(&my_select("SELECT CONCAT(dept, 'x') AS s FROM emp")).join(", ");
	assert!(text.contains(KEY), "{text}");
	let text = projection(&my_select("SELECT CAST(dept AS CHAR) AS s FROM emp")).join(", ");
	assert!(text.contains(KEY) && text.contains("<string>"), "{text}");
	let text = projection(&my_select("SELECT CAST(dept AS SIGNED) AS s FROM emp")).join(", ");
	assert!(text.contains(KEY) && text.contains("<int>"), "{text}");
	let text = projection(&my_select("SELECT dept FROM emp")).join(", ");
	assert_eq!(text, "dept AS dept");
}
