//! Name resolution against tables whose columns the catalog declares.
//!
//! `emp(id, dept, name)` and `dept(id, label)` declare all their columns. `loose` is not described,
//! as a schemaless table is not, and `open` is described without being closed.

use crate::catalog::{BinderCatalog, ColumnInfo, MemoryCatalog, TableSchema};
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::ir::{BoundExpr, BoundSelect, BoundTarget};

fn columns(names: &[&str]) -> Vec<ColumnInfo> {
	names.iter().enumerate().map(|(i, name)| ColumnInfo::new(*name, i)).collect()
}

fn catalog() -> MemoryCatalog {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(TableSchema::new("emp", columns(&["id", "dept", "name"])).as_closed());
	catalog.add_table(TableSchema::new("dept", columns(&["id", "label"])).as_closed());
	catalog.define_table(None, "open", &["id", "a"]);
	catalog
}

fn pg(sql: &str) -> Result<BoundSelect, PlanError> {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::binder::pgsql::bind_select(select, &catalog(), &DialectPolicy::postgres(), None)
}

fn my(sql: &str) -> Result<BoundSelect, PlanError> {
	let stmts = surrealdb_mysql::parse_statements(sql).expect("parse");
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::binder::mysql::bind_select(select, &catalog(), &DialectPolicy::mysql(), None)
}

/// Both dialects bind `sql` to the same result.
fn both(sql: &str) -> Result<(), PlanError> {
	let pg = pg(sql).map(|_| ());
	let my = my(sql).map(|_| ());
	assert_eq!(pg, my, "{sql}");
	pg
}

fn undefined(name: &str) -> Result<(), PlanError> {
	Err(PlanError::UndefinedColumn(name.to_string()))
}

fn column_of(target: &BoundTarget) -> (usize, &str, usize) {
	let BoundExpr::Column(col) = &target.expr else {
		panic!("expected a column, got {:?}", target.expr);
	};
	(col.relation_index, col.column.as_str(), col.correlation_depth)
}

#[test]
fn a_name_a_closed_table_lacks_is_an_error() {
	assert_eq!(both("SELECT nosuch FROM emp"), undefined("nosuch"));
	assert_eq!(both("SELECT emp.nosuch FROM emp"), undefined("nosuch"));
	assert_eq!(both("SELECT e.nosuch FROM emp e"), undefined("nosuch"));
	assert_eq!(both("SELECT name FROM emp WHERE nosuch = 1"), undefined("nosuch"));
	assert_eq!(both("SELECT name FROM emp ORDER BY nosuch"), undefined("nosuch"));
	assert_eq!(both("SELECT count(nosuch) FROM emp"), undefined("nosuch"));
	assert_eq!(both("SELECT name FROM emp GROUP BY nosuch"), undefined("nosuch"));
}

#[test]
fn a_qualifier_does_not_hide_a_missing_column() {
	assert_eq!(both("SELECT public.emp.nosuch FROM public.emp"), undefined("nosuch"));
	assert_eq!(both("SELECT emp.name FROM public.emp"), Ok(()));
}

#[test]
fn the_declared_columns_bind() {
	assert_eq!(both("SELECT id, dept, name FROM emp WHERE dept = 1 ORDER BY name"), Ok(()));
	assert_eq!(both("SELECT e.id, e.name FROM emp e"), Ok(()));
	assert_eq!(both("SELECT count(*) FROM emp"), Ok(()));
}

#[test]
fn a_table_that_does_not_declare_all_its_columns_reads_any_name() {
	assert_eq!(both("SELECT nosuch FROM loose"), Ok(()));
	assert_eq!(both("SELECT loose.nosuch FROM loose"), Ok(()));
	assert_eq!(both("SELECT nosuch FROM open"), Ok(()));
	assert_eq!(both("SELECT open.nosuch FROM open"), Ok(()));
}

#[test]
fn a_dialect_compares_a_column_as_it_names_identifiers() {
	assert_eq!(my("SELECT NAME FROM emp").map(|_| ()), Ok(()));
	assert_eq!(
		pg("SELECT \"NAME\" FROM emp").map(|_| ()),
		Err(PlanError::UndefinedColumn("NAME".into()))
	);
}

#[test]
fn an_output_alias_is_not_a_missing_column() {
	assert_eq!(both("SELECT name AS n, count(*) AS c FROM emp GROUP BY name HAVING c > 1"), Ok(()));
	assert_eq!(both("SELECT dept AS d FROM emp GROUP BY d ORDER BY d"), Ok(()));
}

#[test]
fn a_relation_name_is_not_a_missing_column() {
	assert_eq!(both("SELECT emp FROM emp"), Ok(()));
	assert_eq!(both("SELECT e FROM emp e"), Ok(()));
}

#[test]
fn a_column_a_subquery_table_lacks_belongs_to_the_enclosing_query() {
	for sql in [
		"SELECT name, (SELECT label FROM dept WHERE dept.id = dept) FROM emp",
		"SELECT name FROM emp WHERE EXISTS (SELECT 1 FROM dept WHERE dept.id = emp.dept)",
		"SELECT name FROM emp WHERE dept IN (SELECT id FROM dept WHERE label = name)",
	] {
		assert_eq!(both(sql), Ok(()), "{sql}");
	}

	let bound =
		pg("SELECT name, (SELECT label FROM dept WHERE dept.id = dept) FROM emp").expect("bind");
	let BoundExpr::ScalarSubquery(sub) = &bound.targets[1].expr else {
		panic!("expected a scalar subquery");
	};
	let Some(BoundExpr::Binary {
		right,
		..
	}) = &sub.where_clause
	else {
		panic!("expected a comparison, got {:?}", sub.where_clause);
	};
	let BoundExpr::Column(col) = right.as_ref() else {
		panic!("expected a column, got {right:?}");
	};
	assert_eq!(
		(col.relation_name.as_str(), col.column.as_str(), col.correlation_depth),
		("emp", "dept", 1)
	);
}

#[test]
fn an_alias_hides_the_name_of_its_table() {
	for sql in [
		"SELECT emp.id FROM emp e",
		"SELECT e.id FROM emp e WHERE emp.dept = 1",
		"SELECT e.id FROM emp e ORDER BY emp.name",
		"SELECT e.id FROM emp e JOIN dept d ON emp.dept = d.id",
		"SELECT e.id FROM public.emp e WHERE public.emp.dept = 1",
	] {
		assert_eq!(
			both(sql).map_err(|err| matches!(err, PlanError::MissingFromClauseEntry(_))),
			Err(true),
			"{sql}"
		);
	}
	assert_eq!(
		both("SELECT e.id FROM emp e JOIN dept d ON e.dept = d.id WHERE e.dept = 1"),
		Ok(())
	);
}

#[test]
fn a_table_name_an_alias_frees_names_the_other_relation_that_has_it() {
	let bound = pg("SELECT emp.name, e.name FROM emp e JOIN emp ON emp.id = e.dept").expect("bind");
	assert_eq!(column_of(&bound.targets[0]), (1, "name", 0));
	assert_eq!(column_of(&bound.targets[1]), (0, "name", 0));
}

/// The depth at which each column `expr` reads is bound, in the order the columns appear in a
/// comparison tree of `AND`s.
fn comparison_columns(expr: &BoundExpr, out: &mut Vec<(usize, String, usize)>) {
	if let BoundExpr::Binary {
		left,
		right,
		..
	} = expr
	{
		comparison_columns(left, out);
		comparison_columns(right, out);
	} else if let BoundExpr::Column(col) = expr {
		out.push((col.relation_index, col.column.clone(), col.correlation_depth));
	}
}

#[test]
fn a_subquery_alias_leaves_the_table_name_to_the_enclosing_query() {
	let bound = pg("SELECT name FROM emp WHERE EXISTS \
		(SELECT 1 FROM emp e2 WHERE emp.name = 'x' AND e2.id = emp.id)")
	.expect("bind");
	let Some(BoundExpr::Exists(sub)) = &bound.where_clause else {
		panic!("expected EXISTS, got {:?}", bound.where_clause);
	};
	let mut columns = Vec::new();
	comparison_columns(sub.where_clause.as_ref().expect("a WHERE clause"), &mut columns);
	assert_eq!(
		columns,
		[(0, "name".to_string(), 1), (0, "id".to_string(), 0), (0, "id".to_string(), 1)]
	);

	let bound =
		pg("SELECT (SELECT count(*) FROM emp e2 WHERE e2.name = emp.name) FROM emp").expect("bind");
	let BoundExpr::ScalarSubquery(sub) = &bound.targets[0].expr else {
		panic!("expected a scalar subquery");
	};
	let mut columns = Vec::new();
	comparison_columns(sub.where_clause.as_ref().expect("a WHERE clause"), &mut columns);
	assert_eq!(columns, [(0, "name".to_string(), 0), (0, "name".to_string(), 1)]);
}

#[test]
fn a_column_that_no_query_has_is_an_error_whatever_the_nesting() {
	assert_eq!(both("SELECT (SELECT nosuch FROM dept) FROM emp"), undefined("nosuch"));
	assert_eq!(
		both("SELECT name FROM emp WHERE EXISTS (SELECT 1 FROM dept WHERE label = nosuch)"),
		undefined("nosuch")
	);
}

#[test]
fn a_column_a_closed_table_lacks_may_belong_to_an_open_enclosing_query() {
	assert_eq!(both("SELECT (SELECT label FROM dept WHERE id = other) FROM loose"), Ok(()));
	let bound = pg("SELECT (SELECT label FROM dept WHERE id = other) FROM loose").expect("bind");
	let BoundExpr::ScalarSubquery(sub) = &bound.targets[0].expr else {
		panic!("expected a scalar subquery");
	};
	let Some(BoundExpr::Binary {
		right,
		..
	}) = &sub.where_clause
	else {
		panic!("expected a comparison");
	};
	let BoundExpr::Column(col) = right.as_ref() else {
		panic!("expected a column");
	};
	assert_eq!(col.correlation_depth, 1);
}

#[test]
fn a_join_reads_each_name_from_the_table_that_declares_it() {
	let bound = pg("SELECT dept, label FROM emp JOIN dept ON dept = dept.id").expect("bind");
	assert_eq!(column_of(&bound.targets[0]), (0, "dept", 0));
	assert_eq!(column_of(&bound.targets[1]), (1, "label", 0));

	assert_eq!(both("SELECT nosuch FROM emp JOIN dept ON dept = dept.id"), undefined("nosuch"));
	assert_eq!(
		both("SELECT id FROM emp JOIN dept ON dept = dept.id"),
		Err(PlanError::AmbiguousColumn("id".into()))
	);
}

#[test]
fn a_column_using_merges_is_not_ambiguous() {
	assert_eq!(both("SELECT id FROM emp JOIN dept USING (id)"), Ok(()));
	assert_eq!(
		both("SELECT id FROM emp JOIN dept ON emp.dept = dept.id"),
		Err(PlanError::AmbiguousColumn("id".into()))
	);
}

#[test]
fn a_mysql_using_column_merges_whatever_its_case() {
	assert_eq!(my("SELECT ID FROM emp JOIN dept USING (Id)").map(|_| ()), Ok(()));
	assert_eq!(my("SELECT id FROM emp RIGHT JOIN dept USING (ID)").map(|_| ()), Ok(()));
	assert_eq!(my("SELECT Id FROM emp LEFT JOIN dept USING (id)").map(|_| ()), Ok(()));
}

#[test]
fn a_name_no_declaring_table_has_belongs_to_the_one_that_does_not() {
	let bound = pg("SELECT extra FROM emp JOIN loose ON emp.id = loose.id").expect("bind");
	assert_eq!(column_of(&bound.targets[0]), (1, "extra", 0));
	let bound = pg("SELECT extra FROM loose JOIN emp ON emp.id = loose.id").expect("bind");
	assert_eq!(column_of(&bound.targets[0]), (0, "extra", 0));
	assert_eq!(
		both("SELECT emp.nosuch FROM emp JOIN loose ON emp.id = loose.id"),
		undefined("nosuch")
	);
}

#[test]
fn star_is_the_declared_columns() {
	for bound in [pg("SELECT * FROM emp"), my("SELECT * FROM emp")] {
		let bound = bound.expect("bind");
		let names: Vec<_> = bound.targets.iter().map(|t| column_of(t).1).collect();
		assert_eq!(names, ["id", "dept", "name"]);
		for target in &bound.targets {
			assert!(!target.is_wildcard && target.table_wildcard.is_none());
			assert!(!target.is_unlisted_wildcard());
			assert_eq!(target.alias, None);
		}
		assert!(!bound.targets.iter().any(BoundTarget::is_unlisted_wildcard));
	}

	let bound = my("SELECT e.* FROM emp e").expect("bind");
	let names: Vec<_> = bound.targets.iter().map(|t| column_of(t).1).collect();
	assert_eq!(names, ["id", "dept", "name"]);
	assert!(bound.targets.iter().all(|t| t.table_wildcard.is_none()));
}

#[test]
fn star_stays_one_wildcard_where_a_table_does_not_declare_its_columns() {
	for sql in ["SELECT * FROM loose", "SELECT * FROM emp JOIN dept ON dept = dept.id"] {
		let bound = pg(sql).expect("bind");
		assert_eq!(bound.targets.len(), 1, "{sql}");
		assert!(bound.targets[0].is_unlisted_wildcard(), "{sql}");
	}
	let bound = pg("SELECT * FROM emp JOIN loose ON emp.id = loose.id").expect("bind");
	assert_eq!(bound.targets.len(), 1);
	assert!(bound.targets[0].is_unlisted_wildcard());
}

#[test]
fn a_closed_table_is_found_by_its_exact_name_under_any_qualifier() {
	let catalog = catalog();
	for schema in [None, Some("public"), Some("test")] {
		let table = catalog.get_table(schema, "emp").expect("emp");
		assert!(table.closed);
		assert_eq!(table.column_names(), ["id", "dept", "name"]);
	}
	assert!(catalog.get_table(None, "EMP").is_none(), "the names the datastore holds are exact");
	assert!(
		catalog.get_table(None, "OPEN").is_some(),
		"a table that is not closed matches any case"
	);
}

fn write_metadata(sql: &str) -> Result<(), PlanError> {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let analysis = crate::descriptor::pgsql::PgWriteAnalysis::new(&stmts[0]).expect("a write");
	analysis.describe(&catalog(), &[]).map(|_| ())
}

#[test]
fn a_write_names_the_columns_the_table_declares() {
	assert_eq!(
		write_metadata("INSERT INTO emp (id, name) VALUES (1, 'x') RETURNING id, name"),
		Ok(())
	);
	assert_eq!(write_metadata("UPDATE emp SET name = 'x' WHERE dept = 1 RETURNING id"), Ok(()));
	assert_eq!(write_metadata("INSERT INTO emp (id, nosuch) VALUES (1, 'x')"), undefined("nosuch"));
	assert_eq!(write_metadata("UPDATE emp SET name = 'x' WHERE nosuch = 1"), undefined("nosuch"));
}

#[test]
fn the_default_keyword_is_not_a_column() {
	for sql in [
		"INSERT INTO emp (id, name) VALUES (DEFAULT, 'x') RETURNING id, name",
		"INSERT INTO emp (id, name) VALUES (1, default)",
		"UPDATE emp SET name = DEFAULT WHERE id = 1",
		"INSERT INTO emp (id, name) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET name = DEFAULT",
	] {
		assert_eq!(write_metadata(sql), Ok(()), "{sql}");
	}
}
