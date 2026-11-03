//! The lowered shape of a record link column read through a SQL front end.

// A catch-all arm in these tests reports the value a lowering produced instead of the one
// expected; naming every other variant would add nothing to that report.
#![allow(clippy::wildcard_enum_match_arm)]

use std::collections::BTreeSet;
use std::sync::Mutex;

use surrealdb_expr::expr::logical::{LogicalField, LogicalFields, LogicalOp};
use surrealdb_types::ToSql;

use crate::catalog::{BinderCatalog, EmptyCatalog, MemoryCatalog, TableSchema};
use crate::dialect::DialectPolicy;

/// The tables and link columns of the tests: `emp.dept` and `task.dept` link to `dept`,
/// `emp.mentor` links to `emp` and `task.owner` links to `person`. None of the tables is described,
/// as with the real catalog, which describes only link columns.
pub(super) fn catalog() -> MemoryCatalog {
	MemoryCatalog::new()
		.with_link("emp", "dept", "dept")
		.with_link("emp", "mentor", "emp")
		.with_link("task", "dept", "dept")
		.with_link("task", "owner", "person")
}

pub(super) fn pg_select(sql: &str) -> LogicalOp {
	pg_select_in(sql, &catalog())
}

pub(super) fn pg_select_in(sql: &str, catalog: &dyn BinderCatalog) -> LogicalOp {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::bind_and_lower_pgsql(select, catalog, &DialectPolicy::postgres()).expect("lower")
}

pub(super) fn my_select(sql: &str) -> LogicalOp {
	my_select_in(sql, &catalog())
}

pub(super) fn my_select_in(sql: &str, catalog: &dyn BinderCatalog) -> LogicalOp {
	let stmts = surrealdb_mysql::parse_statements(sql).expect("parse");
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::bind_and_lower_mysql(select, catalog, &DialectPolicy::mysql()).expect("lower")
}

/// The predicate of the first `Filter` of the pipeline.
pub(super) fn predicate(op: &LogicalOp) -> String {
	match op {
		LogicalOp::Filter(filter) => filter.predicate.to_string(),
		LogicalOp::Project(p) => predicate(&p.input),
		LogicalOp::Sort(s) => predicate(&s.input),
		LogicalOp::Limit(l) => predicate(&l.input),
		LogicalOp::Aggregate(a) => predicate(&a.input),
		other @ LogicalOp::Source(_)
		| other @ LogicalOp::Values(_)
		| other @ LogicalOp::Match(_)
		| other @ LogicalOp::Subquery(_)
		| other @ LogicalOp::Write(_)
		| other @ LogicalOp::Ddl(_)
		| other @ LogicalOp::UnionAll(_)
		| other @ LogicalOp::Join(_)
		| other @ LogicalOp::PermissionFilter(_)
		| other @ LogicalOp::Split(_)
		| other @ LogicalOp::Distinct(_)
		| other @ LogicalOp::Fetch(_)
		| other @ LogicalOp::Timeout(_)
		| other @ LogicalOp::VersionScope(_)
		| other @ LogicalOp::Only(_)
		| other @ LogicalOp::Explain(_)
		| other @ LogicalOp::Eval(_)
		| other @ LogicalOp::Sequence(_)
		| other @ LogicalOp::Let(_)
		| other @ LogicalOp::Return(_)
		| other @ LogicalOp::Foreach(_)
		| other @ LogicalOp::If(_)
		| other @ LogicalOp::Throw(_)
		| other @ LogicalOp::Break(_)
		| other @ LogicalOp::Continue(_)
		| other @ LogicalOp::Info(_)
		| other @ LogicalOp::Sleep(_)
		| other @ LogicalOp::Window(_) => panic!("no filter under {other:?}"),
	}
}

/// The lowered expressions of the projection of the pipeline.
pub(super) fn projection(op: &LogicalOp) -> Vec<String> {
	let LogicalOp::Project(project) = op else {
		panic!("expected Project, got {op:?}");
	};
	let LogicalFields::Select(fields) = &project.fields else {
		panic!("expected Select fields");
	};
	fields
		.iter()
		.map(|field| match field {
			LogicalField::All => "*".to_string(),
			LogicalField::Single {
				expr,
				alias,
				..
			} => match alias {
				Some(alias) => format!("{expr} AS {}", alias.to_sql()),
				None => expr.to_string(),
			},
		})
		.collect()
}

pub(super) fn join_keys(op: &LogicalOp) -> Vec<(String, String)> {
	match op {
		LogicalOp::Join(join) => {
			join.on.iter().map(|(l, r)| (l.to_string(), r.to_string())).collect()
		}
		LogicalOp::Project(p) => join_keys(&p.input),
		LogicalOp::Filter(f) => join_keys(&f.input),
		LogicalOp::Sort(s) => join_keys(&s.input),
		other @ LogicalOp::Source(_)
		| other @ LogicalOp::Values(_)
		| other @ LogicalOp::Match(_)
		| other @ LogicalOp::Subquery(_)
		| other @ LogicalOp::Write(_)
		| other @ LogicalOp::Ddl(_)
		| other @ LogicalOp::UnionAll(_)
		| other @ LogicalOp::PermissionFilter(_)
		| other @ LogicalOp::Split(_)
		| other @ LogicalOp::Aggregate(_)
		| other @ LogicalOp::Distinct(_)
		| other @ LogicalOp::Limit(_)
		| other @ LogicalOp::Fetch(_)
		| other @ LogicalOp::Timeout(_)
		| other @ LogicalOp::VersionScope(_)
		| other @ LogicalOp::Only(_)
		| other @ LogicalOp::Explain(_)
		| other @ LogicalOp::Eval(_)
		| other @ LogicalOp::Sequence(_)
		| other @ LogicalOp::Let(_)
		| other @ LogicalOp::Return(_)
		| other @ LogicalOp::Foreach(_)
		| other @ LogicalOp::If(_)
		| other @ LogicalOp::Throw(_)
		| other @ LogicalOp::Break(_)
		| other @ LogicalOp::Continue(_)
		| other @ LogicalOp::Info(_)
		| other @ LogicalOp::Sleep(_)
		| other @ LogicalOp::Window(_) => panic!("no join under {other:?}"),
	}
}

/// The record of `table` the parameter `$_1` names, as it is lowered for a comparison.
fn param_record(table: &str) -> String {
	format!(
		"IF $_1 = '' OR !(type::is_int($_1) OR type::is_i32($_1) OR type::is_i16($_1) OR type::is_i8($_1) OR type::is_u32($_1) OR type::is_u16($_1) OR type::is_u8($_1) OR type::is_string($_1) OR type::is_uuid($_1) OR type::is_array($_1) OR type::is_object($_1) OR type::is_record($_1)) THEN NULL ELSE type::record('{table}', $_1) END"
	)
}

#[test]
fn describing_a_link_does_not_describe_its_table() {
	let catalog = catalog();
	assert!(catalog.get_table(None, "emp").is_none());
	assert!(!catalog.table_exists(None, "emp"));
	assert!(catalog.all_tables().is_empty());
	assert_eq!(catalog.link_table(None, "emp", "dept").as_deref(), Some("dept"));
	assert_eq!(catalog.link_table(None, "EMP", "Dept"), None);
	assert_eq!(catalog.link_table(Some("public"), "emp", "dept").as_deref(), Some("dept"));
	assert_eq!(catalog.link_table(Some("shop"), "emp", "dept").as_deref(), Some("dept"));
	assert_eq!(catalog.link_table(None, "emp", "name"), None);
	assert_eq!(catalog.link_table(None, "visitor", "dept"), None);
	assert_eq!(crate::catalog::EmptyCatalog.link_table(None, "emp", "dept"), None);
}

#[test]
fn a_link_declared_in_a_schema_is_found_in_that_schema() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_link(Some("hr"), "emp", "dept", "dept");
	assert_eq!(catalog.link_table(Some("hr"), "emp", "dept").as_deref(), Some("dept"));
	assert_eq!(catalog.link_table(Some("finance"), "emp", "dept"), None);
	assert_eq!(catalog.link_table(None, "emp", "dept"), None);
}

#[test]
fn a_link_equal_to_an_int_literal_is_compared_with_the_record() {
	let op = pg_select("SELECT name FROM emp WHERE dept = 1");
	assert_eq!(predicate(&op), "dept = (dept:1)");
}

#[test]
fn a_link_equal_to_a_string_literal_is_compared_with_the_record() {
	let op = pg_select("SELECT name FROM emp WHERE dept = 'eng'");
	assert_eq!(predicate(&op), "dept = (dept:eng)");
}

#[test]
fn a_literal_on_the_left_of_a_link_is_compared_with_the_record() {
	let op = pg_select("SELECT name FROM emp WHERE 1 = dept");
	assert_eq!(predicate(&op), "dept = (dept:1)");
}

#[test]
fn a_link_equal_to_a_parameter_is_compared_with_the_record_it_names() {
	let op = pg_select("SELECT name FROM emp WHERE dept = $1");
	assert_eq!(
		predicate(&op),
		format!("dept = ({}) AND dept != NULL AND dept != NONE", param_record("dept"))
	);
}

#[test]
fn a_link_equal_to_a_column_is_compared_with_the_record_it_names() {
	let op = pg_select("SELECT name FROM emp WHERE dept = age");
	assert_eq!(
		predicate(&op),
		"dept = (IF age = '' OR !(type::is_int(age) OR type::is_i32(age) OR type::is_i16(age) OR type::is_i8(age) OR type::is_u32(age) OR type::is_u16(age) OR type::is_u8(age) OR type::is_string(age) OR type::is_uuid(age) OR type::is_array(age) OR type::is_object(age) OR type::is_record(age)) THEN NULL ELSE type::record('dept', age) END) AND dept != NULL AND dept != NONE"
	);
}

#[test]
fn a_link_in_literals_is_a_membership_of_records() {
	let op = pg_select("SELECT name FROM emp WHERE dept IN (1, 2)");
	assert_eq!(predicate(&op), "dept INSIDE [dept:1, dept:2]");
	let op = pg_select("SELECT name FROM emp WHERE dept IN (7)");
	assert_eq!(predicate(&op), "dept = (dept:7)");
}

#[test]
fn a_link_in_parameters_and_columns_is_a_membership_of_the_records_they_name() {
	let op = pg_select("SELECT name FROM emp WHERE dept IN (1, $1)");
	assert_eq!(
		predicate(&op),
		format!(
			"dept INSIDE [dept:1, ({})] AND dept != NULL AND dept != NONE",
			param_record("dept")
		)
	);
}

#[test]
fn a_link_not_in_literals_is_compared_with_the_records_too() {
	let op = pg_select("SELECT name FROM emp WHERE dept NOT IN (1, 2)");
	assert_eq!(
		predicate(&op),
		"[dept, [dept:1, dept:2]].?(IF [1] = [] THEN true ELSE IF [0] = NULL OR [0] = NONE THEN NULL ELSE IF [0] INSIDE [1] THEN false ELSE IF NULL INSIDE [1] OR NONE INSIDE [1] THEN NULL ELSE true END)"
	);
}

#[test]
fn a_link_in_one_literal_is_an_equality_only_as_a_top_level_conjunct() {
	let op = pg_select("SELECT name FROM emp WHERE dept NOT IN (1)");
	assert_eq!(
		predicate(&op),
		"[dept, [dept:1]].?(IF [1] = [] THEN true ELSE IF [0] = NULL OR [0] = NONE THEN NULL ELSE IF [0] INSIDE [1] THEN false ELSE IF NULL INSIDE [1] OR NONE INSIDE [1] THEN NULL ELSE true END)"
	);
	let op = pg_select("SELECT name FROM emp WHERE NOT (dept IN (1))");
	assert!(predicate(&op).contains("[dept, [dept:1]]"), "{}", predicate(&op));
}

#[test]
fn a_link_not_equal_keeps_the_null_handling_and_compares_with_the_record() {
	let op = pg_select("SELECT name FROM emp WHERE dept <> 1");
	assert_eq!(
		predicate(&op),
		"IF dept = NULL OR dept = NONE OR (dept:1) = NULL OR (dept:1) = NONE THEN NULL ELSE dept != (dept:1) END"
	);
}

#[test]
fn a_comparison_that_is_no_top_level_conjunct_keeps_the_null_handling() {
	let negated = predicate(&pg_select("SELECT name FROM emp WHERE NOT (dept = 1)"));
	assert!(negated.contains("dept != ") || negated.contains("dept = (dept:1)"), "{negated}");
	assert!(negated.starts_with("IF "), "{negated}");
	assert!(!negated.contains("type::record"), "{negated}");

	let either = predicate(&pg_select("SELECT name FROM emp WHERE dept = 1 OR dept = 2"));
	assert!(either.contains("dept = (dept:1)") && either.contains("dept = (dept:2)"), "{either}");
	assert!(either.starts_with("IF "), "{either}");

	let op = pg_select("SELECT dept = 1 AS m FROM emp");
	assert_eq!(
		projection(&op),
		[
			"IF dept = NULL OR dept = NONE OR (dept:1) = NULL OR (dept:1) = NONE THEN NULL ELSE dept = (dept:1) END AS m"
		]
	);
}

#[test]
fn the_lookup_conjunct_is_found_among_the_conjuncts() {
	let op = pg_select("SELECT name FROM emp WHERE name = 'x' AND dept = 1 AND age > 3");
	let predicate = predicate(&op);
	assert!(predicate.contains(" AND dept = (dept:1) AND "), "{predicate}");
	assert!(
		!predicate.starts_with("IF "),
		"the conjuncts are joined by the engine's AND: {predicate}"
	);
}

#[test]
fn distinctness_and_null_safe_equality_compare_with_the_record() {
	let op = pg_select("SELECT name FROM emp WHERE dept IS DISTINCT FROM 1");
	assert_eq!(
		predicate(&op),
		"IF (dept = NULL OR dept = NONE) AND ((dept:1) = NULL OR (dept:1) = NONE) THEN false ELSE IF dept = NULL OR dept = NONE OR (dept:1) = NULL OR (dept:1) = NONE THEN true ELSE dept != (dept:1) END"
	);
	let op = pg_select("SELECT name FROM emp WHERE dept IS NOT DISTINCT FROM 1");
	let predicate_text = predicate(&op);
	assert!(predicate_text.ends_with("ELSE dept = (dept:1) END"), "{predicate_text}");

	let op = my_select("SELECT name FROM emp WHERE dept <=> 1");
	let predicate_text = predicate(&op);
	assert!(predicate_text.ends_with("ELSE dept = (dept:1) END"), "{predicate_text}");
}

#[test]
fn a_literal_that_is_no_key_is_compared_as_it_is() {
	for (sql, tail) in [
		("dept = 1.5", "ELSE dept = 1.5dec END"),
		("dept = ''", "ELSE dept = '' END"),
		("dept = NULL", "ELSE dept = NULL END"),
	] {
		let op = pg_select(&format!("SELECT name FROM emp WHERE {sql}"));
		let predicate = predicate(&op);
		assert!(predicate.starts_with("IF dept = NULL OR dept = NONE OR "), "{sql}: {predicate}");
		assert!(predicate.ends_with(tail), "{sql}: {predicate}");
		assert!(!predicate.contains("type::record") && !predicate.contains("dept:"), "{predicate}");
	}
}

#[test]
fn a_link_is_read_as_it_is_stored() {
	let op = pg_select("SELECT name, dept FROM emp");
	assert_eq!(projection(&op), ["name AS name", "dept AS dept"]);
	let op = pg_select("SELECT * FROM emp");
	assert_eq!(projection(&op), ["*", "record::id(id) AS id"]);
}

#[test]
fn a_column_that_is_no_link_is_unchanged() {
	let op = pg_select("SELECT name FROM emp WHERE age = 1");
	assert_eq!(
		predicate(&op),
		"IF age = NULL OR age = NONE OR 1 = NULL OR 1 = NONE THEN NULL ELSE age = 1 END"
	);
	let op = pg_select("SELECT name FROM emp WHERE name IN ('a', 'b')");
	assert_eq!(
		predicate(&op),
		"[name, ['a', 'b']].?(IF [1] = [] THEN false ELSE IF [0] = NULL OR [0] = NONE THEN NULL ELSE IF [0] INSIDE [1] THEN true ELSE IF NULL INSIDE [1] OR NONE INSIDE [1] THEN NULL ELSE false END)"
	);
}

#[test]
fn a_column_of_a_table_the_catalog_does_not_describe_is_unchanged() {
	let op = pg_select("SELECT name FROM visitor WHERE dept = 1");
	assert_eq!(
		predicate(&op),
		"IF dept = NULL OR dept = NONE OR 1 = NULL OR 1 = NONE THEN NULL ELSE dept = 1 END"
	);
	let op = crate::bind_and_lower_pgsql(
		match &surrealdb_pgsql::parse_statements("SELECT name FROM emp WHERE dept = 1")
			.expect("parse")[0]
		{
			surrealdb_pgsql::ast::stmt::Statement::Select(select) => select,
			surrealdb_pgsql::ast::stmt::Statement::Copy(_)
			| surrealdb_pgsql::ast::stmt::Statement::Insert(_)
			| surrealdb_pgsql::ast::stmt::Statement::Update(_)
			| surrealdb_pgsql::ast::stmt::Statement::Delete(_)
			| surrealdb_pgsql::ast::stmt::Statement::Truncate(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreateTable(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreateIndex(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreateView(_)
			| surrealdb_pgsql::ast::stmt::Statement::AlterTable(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreateSchema(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreateExtension(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreateType(_)
			| surrealdb_pgsql::ast::stmt::Statement::AlterType(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreateDomain(_)
			| surrealdb_pgsql::ast::stmt::Statement::AlterDomain(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreateCompositeType(_)
			| surrealdb_pgsql::ast::stmt::Statement::Drop(_)
			| surrealdb_pgsql::ast::stmt::Statement::Transaction(_)
			| surrealdb_pgsql::ast::stmt::Statement::Set(_)
			| surrealdb_pgsql::ast::stmt::Statement::Show(_)
			| surrealdb_pgsql::ast::stmt::Statement::Reset(_)
			| surrealdb_pgsql::ast::stmt::Statement::Explain(_)
			| surrealdb_pgsql::ast::stmt::Statement::Prepare(_)
			| surrealdb_pgsql::ast::stmt::Statement::Execute(_)
			| surrealdb_pgsql::ast::stmt::Statement::Deallocate(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreatePropertyGraph(_)
			| surrealdb_pgsql::ast::stmt::Statement::DropPropertyGraph(_)
			| surrealdb_pgsql::ast::stmt::Statement::Empty => panic!("expected SELECT"),
		},
		&crate::catalog::EmptyCatalog,
		&DialectPolicy::postgres(),
	)
	.expect("lower");
	assert!(!predicate(&op).contains("dept:"), "{}", predicate(&op));
}

#[test]
fn a_derived_link_retains_its_target() {
	let op = pg_select("SELECT * FROM (SELECT dept FROM emp) t WHERE t.dept = 1");
	let predicate = predicate(&op);
	assert_eq!(predicate, "dept = (dept:1)");
}

#[test]
fn a_cte_link_retains_its_target() {
	let op = pg_select("WITH t AS (SELECT dept FROM emp) SELECT * FROM t WHERE t.dept = 1");
	let predicate = predicate(&op);
	assert_eq!(predicate, "dept = (dept:1)");
}

#[test]
fn a_column_of_a_system_catalog_table_is_never_a_link() {
	let catalog = MemoryCatalog::new().with_link("pg_class", "relname", "name");
	let op = pg_select_in("SELECT * FROM pg_catalog.pg_class WHERE relname = 'x'", &catalog);
	let predicate = predicate(&op);
	assert!(!predicate.contains("name:x") && !predicate.contains("type::record"), "{predicate}");
}

#[test]
fn a_link_is_joined_with_the_id_it_links_to_as_stored() {
	let op = pg_select("SELECT e.name FROM emp e JOIN dept d ON e.dept = d.id");
	assert_eq!(join_keys(&op), [("dept".to_string(), "_join0.id".to_string())]);
	let op = pg_select("SELECT e.name FROM emp e JOIN dept d ON d.id = e.dept");
	assert_eq!(join_keys(&op), [("dept".to_string(), "_join0.id".to_string())]);
	let op = pg_select("SELECT e.name FROM dept d JOIN emp e ON e.dept = d.id");
	assert_eq!(join_keys(&op), [("id".to_string(), "_join0.dept".to_string())]);
	let op = pg_select("SELECT e.name FROM dept d JOIN emp e ON d.id = e.dept");
	assert_eq!(join_keys(&op), [("id".to_string(), "_join0.dept".to_string())]);
	let op = pg_select("SELECT e.name FROM emp e JOIN emp m ON e.mentor = m.id");
	assert_eq!(join_keys(&op), [("mentor".to_string(), "_join0.id".to_string())]);
}

#[test]
fn two_links_to_the_same_table_are_joined_as_stored() {
	let op = pg_select("SELECT e.name FROM emp e JOIN task t ON e.dept = t.dept");
	assert_eq!(join_keys(&op), [("dept".to_string(), "_join0.dept".to_string())]);
	let op = pg_select("SELECT e.name FROM task t JOIN emp e ON e.dept = t.dept");
	assert_eq!(join_keys(&op), [("dept".to_string(), "_join0.dept".to_string())]);
}

#[test]
fn a_link_is_joined_with_a_column_by_the_record_the_column_names() {
	let op = pg_select("SELECT e.name FROM emp e JOIN task t ON e.age = t.dept");
	let keys = join_keys(&op);
	assert_eq!(keys.len(), 1);
	assert!(keys[0].0.ends_with("THEN NULL ELSE type::record('dept', age) END"), "{keys:?}");
	assert_eq!(keys[0].1, "_join0.dept", "the link stays a bare column: {keys:?}");

	let op = pg_select("SELECT e.name FROM emp e JOIN task t ON e.dept = t.age");
	let keys = join_keys(&op);
	assert_eq!(keys[0].0, "dept");
	assert!(keys[0].1.ends_with("THEN NULL ELSE type::record('dept', _join0.age) END"), "{keys:?}");
}

#[test]
fn two_links_to_different_tables_are_joined_by_the_record_of_the_build_side() {
	let op = pg_select("SELECT e.name FROM emp e JOIN task t ON e.dept = t.owner");
	let keys = join_keys(&op);
	assert!(keys[0].0.ends_with("THEN NULL ELSE type::record('person', dept) END"), "{keys:?}");
	assert_eq!(keys[0].1, "_join0.owner");
}

#[test]
fn a_literal_in_a_join_condition_is_the_record_it_names() {
	let op = pg_select("SELECT e.name FROM emp e JOIN task t ON e.dept = t.dept AND e.dept = 1");
	assert_eq!(
		join_keys(&op),
		[
			("dept".to_string(), "_join0.dept".to_string()),
			("dept".to_string(), "dept:1".to_string())
		]
	);
}

#[test]
fn a_link_compared_in_the_where_clause_of_a_join_is_compared_with_the_record() {
	let op = pg_select("SELECT e.name FROM emp e JOIN dept d ON d.id = e.dept WHERE e.dept = 3");
	assert_eq!(predicate(&op), "dept = (dept:3)");
	let op = pg_select("SELECT e.name FROM dept d JOIN emp e ON d.id = e.dept WHERE e.dept = 3");
	let predicate = predicate(&op);
	assert!(predicate.starts_with("IF "), "the link of a joined table keeps its null handling");
	assert!(predicate.contains("_join0.dept = (dept:3)"), "{predicate}");
}

#[test]
fn a_correlated_link_is_compared_as_the_local_one_is() {
	let op = pg_select(
		"SELECT e.name FROM emp e WHERE EXISTS (SELECT 1 FROM task t WHERE t.dept = e.dept)",
	);
	let text1 = predicate(&op);
	assert!(text1.contains("dept = $parent.dept"), "{text1}");
	assert!(!text1.contains("type::record"), "{text1}");

	let op = pg_select(
		"SELECT e.name FROM emp e WHERE EXISTS (SELECT 1 FROM task t WHERE t.age = e.dept)",
	);
	let text2 = predicate(&op);
	assert!(text2.contains("type::record('dept', age)"), "{text2}");
	assert!(text2.contains("= $parent.dept"), "{text2}");
}

#[test]
fn a_link_is_compared_with_the_id_of_another_table_by_its_key() {
	let op =
		pg_select("SELECT e.name FROM emp e JOIN person p ON e.dept = p.id WHERE e.dept = p.id");
	let keys = join_keys(&op);
	assert!(keys[0].0.contains("type::record('person', dept)"), "{keys:?}");
}

#[test]
fn a_link_compared_with_a_subquery_is_compared_by_its_key() {
	let op = pg_select("SELECT name FROM emp WHERE dept IN (SELECT id FROM dept)");
	let text1 = predicate(&op);
	assert!(text1.contains("record::id(dept)") && text1.contains("INSIDE"), "{text1}");
	let op = pg_select("SELECT name FROM emp WHERE dept = (SELECT max(id) FROM dept)");
	let text2 = predicate(&op);
	assert!(
		text2.contains("ELSE (IF type::is_record(dept) THEN record::id(dept) ELSE dept END) = ("),
		"{text2}"
	);
}

#[test]
fn a_link_in_a_list_with_a_subquery_is_compared_by_its_key() {
	let text =
		predicate(&pg_select("SELECT name FROM emp WHERE dept IN (1, (SELECT max(id) FROM dept))"));
	assert!(text.contains("record::id(dept)"), "{text}");
	assert!(text.contains("array::max(record::id(id))"), "{text}");
}

#[test]
fn a_group_key_that_is_a_link_is_compared_with_the_record_in_having() {
	let op = pg_select("SELECT dept, count(*) FROM emp GROUP BY dept HAVING dept = 1");
	let predicate = predicate(&op);
	assert!(predicate.ends_with("ELSE dept = (dept:1) END"), "{predicate}");
}

#[test]
fn a_described_table_expands_its_wildcard_and_keeps_its_links() {
	let mut catalog = catalog();
	catalog.define_table(None, "emp", &["id", "name", "dept"]);
	let op = pg_select_in("SELECT * FROM emp WHERE dept = 1", &catalog);
	assert_eq!(predicate(&op), "dept = (dept:1)");
}

#[test]
fn mysql_compares_a_link_with_the_record_too() {
	let op = my_select("SELECT name FROM emp WHERE dept = 1");
	assert_eq!(predicate(&op), "dept = (dept:1)");
	let op = my_select("SELECT name FROM emp WHERE dept IN (1, 2)");
	assert_eq!(predicate(&op), "dept INSIDE [dept:1, dept:2]");
	let op = my_select("SELECT name FROM emp WHERE dept = ?");
	assert!(predicate(&op).starts_with("dept = (IF "), "{}", predicate(&op));
	let op = my_select("SELECT e.name FROM emp e JOIN dept d ON e.dept = d.id");
	assert_eq!(join_keys(&op), [("dept".to_string(), "_join0.id".to_string())]);
}

/// A catalog that records what the binder asks of it.
pub(super) struct RecordingCatalog {
	inner: MemoryCatalog,
	links: Mutex<Vec<(Option<String>, String, String)>>,
	tables: Mutex<Vec<(Option<String>, String)>>,
}

impl RecordingCatalog {
	pub(super) fn new(inner: MemoryCatalog) -> Self {
		Self {
			inner,
			links: Mutex::new(Vec::new()),
			tables: Mutex::new(Vec::new()),
		}
	}

	/// The `(schema, table, column)` triples `link_table` was asked about, without repeats.
	pub(super) fn links_asked(&self) -> BTreeSet<(Option<String>, String, String)> {
		self.links.lock().expect("lock").iter().cloned().collect()
	}

	/// The `(schema, table)` pairs `get_table` was asked about, without repeats.
	pub(super) fn tables_asked(&self) -> BTreeSet<(Option<String>, String)> {
		self.tables.lock().expect("lock").iter().cloned().collect()
	}
}

impl BinderCatalog for RecordingCatalog {
	fn get_table(&self, schema: Option<&str>, name: &str) -> Option<TableSchema> {
		self.tables.lock().expect("lock").push((schema.map(str::to_string), name.to_string()));
		self.inner.get_table(schema, name)
	}

	fn link_table(&self, schema: Option<&str>, table: &str, column: &str) -> Option<String> {
		self.links.lock().expect("lock").push((
			schema.map(str::to_string),
			table.to_string(),
			column.to_string(),
		));
		self.inner.link_table(schema, table, column)
	}
}

pub(super) fn asked(triples: &[(&str, &str)]) -> BTreeSet<(Option<String>, String, String)> {
	triples.iter().map(|(table, column)| (None, table.to_string(), column.to_string())).collect()
}

#[test]
fn the_catalog_is_asked_about_each_column_of_a_base_table_that_is_used() {
	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_select_in(
		"SELECT e.name FROM emp e JOIN dept d ON e.dept = d.id WHERE d.title = 'x'",
		&recorder,
	);
	assert_eq!(
		recorder.links_asked(),
		asked(&[("emp", "name"), ("emp", "dept"), ("dept", "id"), ("dept", "title")])
	);
	assert!(recorder.tables_asked().contains(&(None, "emp".to_string())));
	assert!(recorder.tables_asked().contains(&(None, "dept".to_string())));
}

#[test]
fn the_catalog_is_asked_about_a_correlated_column_of_the_outer_table() {
	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_select_in(
		"SELECT 1 FROM emp e WHERE EXISTS (SELECT 1 FROM task t WHERE t.dept = e.dept)",
		&recorder,
	);
	assert_eq!(recorder.links_asked(), asked(&[("task", "dept"), ("emp", "dept")]));
}

#[test]
fn the_catalog_is_not_asked_about_a_derived_table_or_a_system_catalog_table() {
	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_select_in("SELECT t.dept FROM (SELECT dept FROM emp) t WHERE t.dept = 1", &recorder);
	assert_eq!(recorder.links_asked(), asked(&[("emp", "dept")]));

	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_select_in("SELECT relname FROM pg_catalog.pg_class WHERE relname = 'x'", &recorder);
	assert!(recorder.links_asked().is_empty(), "{:?}", recorder.links_asked());
}

#[test]
fn a_column_that_is_the_id_or_a_wildcard_is_never_a_link() {
	let catalog = MemoryCatalog::new().with_link("emp", "id", "dept");
	let op = pg_select_in("SELECT name FROM emp WHERE id = 1", &catalog);
	assert_eq!(predicate(&op), "id = (emp:1)");
}

#[test]
fn a_link_column_of_a_qualified_table_is_compared_as_a_link() {
	let bare = predicate(&pg_select("SELECT name FROM emp WHERE dept = 1"));
	assert!(bare.contains("dept:1"), "the bare table converts: {bare}");
	assert_eq!(predicate(&pg_select("SELECT name FROM public.emp WHERE dept = 1")), bare);
	assert_eq!(predicate(&pg_select("SELECT name FROM \"public\".\"emp\" WHERE dept = 1")), bare);
	assert_eq!(predicate(&my_select("SELECT name FROM shop.emp WHERE dept = 1")), bare);
	assert_eq!(predicate(&my_select("SELECT name FROM `shop`.`emp` WHERE dept = 1")), bare);

	let keys = join_keys(&pg_select("SELECT e.name FROM emp e JOIN dept d ON e.dept = d.id"));
	assert_eq!(
		join_keys(&pg_select(
			"SELECT e.name FROM public.emp e JOIN public.dept d ON e.dept = d.id"
		)),
		keys
	);
	assert_eq!(
		join_keys(&my_select("SELECT e.name FROM shop.emp e JOIN shop.dept d ON e.dept = d.id")),
		keys
	);
}

#[test]
fn a_link_is_found_only_under_the_name_its_field_is_declared_by() {
	let catalog = MemoryCatalog::new().with_link("emp", "Dept", "dept");
	assert_eq!(catalog.link_table(None, "emp", "Dept").as_deref(), Some("dept"));
	assert_eq!(catalog.link_table(None, "emp", "dept"), None);
	assert_eq!(catalog.link_table(None, "emp", "DEPT"), None);
	assert_eq!(catalog.link_table(Some("public"), "emp", "dept"), None);

	let catalog = MemoryCatalog::new().with_link("EMP", "dept", "dept");
	assert_eq!(catalog.link_table(None, "EMP", "dept").as_deref(), Some("dept"));
	assert_eq!(catalog.link_table(None, "emp", "dept"), None);
}

#[test]
fn links_that_differ_only_in_case_are_told_apart() {
	let catalog = MemoryCatalog::new()
		.with_link("emp", "Dept", "dept")
		.with_link("emp", "DEPT", "region")
		.with_link("emp", "dept", "office");
	assert_eq!(catalog.link_table(None, "emp", "Dept").as_deref(), Some("dept"));
	assert_eq!(catalog.link_table(None, "emp", "DEPT").as_deref(), Some("region"));
	assert_eq!(catalog.link_table(None, "emp", "dept").as_deref(), Some("office"));
	assert_eq!(catalog.link_table(None, "emp", "dEpT"), None);
}

#[test]
fn a_link_declared_in_the_public_schema_is_found_for_a_bare_table() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_link(Some("public"), "emp", "dept", "dept");
	assert_eq!(catalog.link_table(None, "emp", "dept").as_deref(), Some("dept"));
	assert_eq!(catalog.link_table(Some("public"), "emp", "dept").as_deref(), Some("dept"));
	assert_eq!(catalog.link_table(None, "emp", "Dept"), None);
	assert_eq!(catalog.link_table(None, "EMP", "dept"), None);
}

#[test]
fn a_plain_field_that_differs_from_a_link_only_in_case_is_not_a_link() {
	let catalog = MemoryCatalog::new().with_link("emp", "Dept", "dept");
	for sql in [
		"SELECT name FROM emp WHERE dept = 1",
		"SELECT name FROM emp WHERE DEPT = 1",
		"SELECT name FROM emp WHERE dept IN (1, 2)",
	] {
		let op = pg_select_in(sql, &catalog);
		assert!(!predicate(&op).contains("dept:"), "{sql}: {}", predicate(&op));
		let op = my_select_in(sql, &catalog);
		assert!(!predicate(&op).contains("dept:"), "{sql}: {}", predicate(&op));
	}
	let plain = predicate(&pg_select_in("SELECT name FROM emp WHERE dept = 1", &EmptyCatalog));
	assert_eq!(predicate(&pg_select_in("SELECT name FROM emp WHERE dept = 1", &catalog)), plain);
}

#[test]
fn a_reference_that_differs_from_the_declared_name_in_case_reads_the_declared_link() {
	let expected = predicate(&pg_select("SELECT name FROM emp WHERE dept = 1"));
	assert_eq!(expected, "dept = (dept:1)");
	for sql in [
		"SELECT name FROM emp WHERE Dept = 1",
		"SELECT name FROM emp WHERE DEPT = 1",
		"SELECT name FROM EMP WHERE dept = 1",
		"SELECT name FROM Emp e WHERE E.DEPT = 1",
	] {
		assert_eq!(predicate(&pg_select(sql)), expected, "{sql}");
		assert_eq!(predicate(&my_select(sql)), expected, "{sql}");
	}
}

#[test]
fn the_name_the_catalog_is_asked_about_is_the_name_the_column_is_read_by() {
	let recorder = RecordingCatalog::new(catalog());
	let op = pg_select_in("SELECT name FROM EMP WHERE Dept = 1", &recorder);
	assert_eq!(recorder.links_asked(), asked(&[("emp", "name"), ("emp", "dept")]));
	assert_eq!(predicate(&op), "dept = (dept:1)");

	let recorder = RecordingCatalog::new(catalog());
	let op = my_select_in("SELECT name FROM EMP WHERE Dept = 1", &recorder);
	assert_eq!(recorder.links_asked(), asked(&[("emp", "name"), ("emp", "dept")]));
	assert_eq!(predicate(&op), "dept = (dept:1)");
}
