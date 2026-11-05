//! The lowered shape of the `id` column of a base table: read as the key of the stored record id.

// A catch-all arm in these tests reports the value a lowering produced instead of the one
// expected; naming every other variant would add nothing to that report.
#![allow(clippy::wildcard_enum_match_arm)]

use std::collections::BTreeSet;

use surrealdb_expr::expr::logical::{
	LogicalExpr, LogicalField, LogicalFields, LogicalOp, LogicalOrdering,
};
use surrealdb_types::ToSql;

use crate::catalog::EmptyCatalog;
use crate::dialect::DialectPolicy;
use crate::lower::id_key::{KEY_KINDS, NAMING_KINDS, RECORD_KIND};
use crate::lower::link_tests::catalog;
use crate::lower::write::{lower_mysql_update, lower_pgsql_delete, lower_pgsql_update};

fn pg_select(sql: &str) -> LogicalOp {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::bind_and_lower_pgsql(select, &EmptyCatalog, &DialectPolicy::postgres()).expect("lower")
}

fn my_select(sql: &str) -> LogicalOp {
	let stmts = surrealdb_mysql::parse_statements(sql).expect("parse");
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::bind_and_lower_mysql(select, &EmptyCatalog, &DialectPolicy::mysql()).expect("lower")
}

/// The predicate of the first `Filter` of the pipeline.
fn predicate(op: &LogicalOp) -> String {
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
fn projection(op: &LogicalOp) -> Vec<String> {
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

fn join_keys(op: &LogicalOp) -> Vec<(String, String)> {
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

#[test]
fn id_equal_to_an_int_literal_reads_the_record_by_its_id() {
	let op = pg_select("SELECT name FROM person WHERE id = 1");
	assert_eq!(predicate(&op), "id = (person:1)");
}

#[test]
fn id_equal_to_a_string_literal_reads_the_record_by_its_id() {
	let op = pg_select("SELECT label FROM tag WHERE id = 'rust'");
	assert_eq!(predicate(&op), "id = (tag:rust)");
}

#[test]
fn a_literal_on_the_left_of_id_reads_the_record_by_its_id() {
	let op = pg_select("SELECT name FROM person WHERE 1 = id");
	assert_eq!(predicate(&op), "id = (person:1)");
}

#[test]
fn id_equal_to_a_parameter_reads_the_record_by_its_id() {
	let op = pg_select("SELECT name FROM person WHERE id = $1");
	assert_eq!(
		predicate(&op),
		"id = (IF $_1 = '' OR !(type::is_int($_1) OR type::is_i32($_1) OR type::is_i16($_1) OR type::is_i8($_1) OR type::is_u32($_1) OR type::is_u16($_1) OR type::is_u8($_1) OR type::is_string($_1) OR type::is_uuid($_1) OR type::is_array($_1) OR type::is_object($_1) OR type::is_record($_1)) THEN NULL ELSE type::record('person', $_1) END)"
	);
}

#[test]
fn id_in_literals_and_parameters_is_a_membership_of_records() {
	let op = pg_select("SELECT name FROM person WHERE id IN (1, 2, $1)");
	assert_eq!(
		predicate(&op),
		"id INSIDE [person:1, person:2, (IF $_1 = '' OR !(type::is_int($_1) OR type::is_i32($_1) OR type::is_i16($_1) OR type::is_i8($_1) OR type::is_u32($_1) OR type::is_u16($_1) OR type::is_u8($_1) OR type::is_string($_1) OR type::is_uuid($_1) OR type::is_array($_1) OR type::is_object($_1) OR type::is_record($_1)) THEN NULL ELSE type::record('person', $_1) END)]"
	);
}

#[test]
fn a_lookup_anywhere_but_a_top_level_conjunct_compares_keys_with_null_handling() {
	let negated = predicate(&pg_select("SELECT name FROM person WHERE NOT (id = $1)"));
	assert!(!negated.contains("type::record"), "{negated}");
	assert!(negated.contains("record::id(id) = $_1"), "{negated}");

	let either = predicate(&pg_select("SELECT name FROM person WHERE id = 1 OR id = 2"));
	assert!(!either.contains("person:"), "{either}");
	assert!(either.contains("record::id(id) = 1"), "{either}");

	let op =
		pg_select("SELECT id = $1 AS m, CASE WHEN id = 1 THEN 'a' ELSE 'b' END AS c FROM person");
	for field in projection(&op) {
		assert!(!field.contains("type::record") && !field.contains("person:"), "{field}");
		assert!(field.contains("record::id(id) = "), "{field}");
	}
	assert!(projection(&op)[0].starts_with("IF "), "{:?}", projection(&op));
}

#[test]
fn an_outer_join_negation_over_an_id_compares_keys_with_null_handling() {
	let op = pg_select(
		"SELECT p.id FROM person p LEFT JOIN profile f ON f.id = p.id WHERE NOT (f.id = 1)",
	);
	let predicate = predicate(&op);
	assert!(!predicate.contains("profile:1"), "{predicate}");
	assert!(predicate.contains("type::is_record(_join0.id)"), "{predicate}");
	assert!(predicate.contains("IF "), "{predicate}");
}

#[test]
fn id_in_one_literal_or_parameter_is_the_lookup_of_its_record() {
	let op = pg_select("SELECT name FROM person WHERE id IN (7)");
	assert_eq!(predicate(&op), "id = (person:7)");
	let op = pg_select("SELECT name FROM person WHERE id IN ($1)");
	assert_eq!(
		predicate(&op),
		"id = (IF $_1 = '' OR !(type::is_int($_1) OR type::is_i32($_1) OR type::is_i16($_1) OR type::is_i8($_1) OR type::is_u32($_1) OR type::is_u16($_1) OR type::is_u8($_1) OR type::is_string($_1) OR type::is_uuid($_1) OR type::is_array($_1) OR type::is_object($_1) OR type::is_record($_1)) THEN NULL ELSE type::record('person', $_1) END)"
	);
}

#[test]
fn id_not_in_reads_the_key() {
	let op = pg_select("SELECT name FROM person WHERE id NOT IN (1, 2)");
	assert_eq!(
		predicate(&op),
		"[record::id(id), [1, 2]].?(IF [1] = [] THEN true ELSE IF [0] = NULL OR [0] = NONE THEN NULL ELSE IF [0] INSIDE [1] THEN false ELSE IF NULL INSIDE [1] OR NONE INSIDE [1] THEN NULL ELSE true END)"
	);
}

#[test]
fn a_range_over_id_reads_the_key() {
	let op = pg_select("SELECT name FROM person WHERE id > 1");
	assert!(predicate(&op).contains("record::id(id) > 1"), "{}", predicate(&op));
	let op = pg_select("SELECT name FROM person WHERE id BETWEEN 1 AND 3");
	assert!(predicate(&op).contains("[record::id(id), 1, 3]"), "{}", predicate(&op));
}

#[test]
fn id_not_equal_reads_the_key() {
	let op = pg_select("SELECT name FROM person WHERE id <> 1");
	assert!(predicate(&op).contains("record::id(id) != 1"), "{}", predicate(&op));
}

#[test]
fn id_equal_to_null_is_never_a_lookup() {
	let op = pg_select("SELECT name FROM person WHERE id = NULL");
	let predicate = predicate(&op);
	assert!(!predicate.contains("type::record") && !predicate.contains("person:"), "{predicate}");
	assert!(predicate.contains("record::id(id)"), "{predicate}");
}

#[test]
fn id_equal_to_a_float_or_the_empty_string_reads_the_key() {
	for sql in ["SELECT name FROM person WHERE id = 1.5", "SELECT name FROM person WHERE id = ''"] {
		let predicate = predicate(&pg_select(sql));
		assert!(!predicate.contains("person:") && !predicate.contains("type::record"), "{sql}");
		assert!(predicate.contains("record::id(id)"), "{sql}: {predicate}");
	}
}

#[test]
fn the_lookup_conjunct_is_found_among_the_conjuncts() {
	let op = pg_select("SELECT name FROM person WHERE name = 'x' AND id = 1 AND age > 3");
	let predicate = predicate(&op);
	assert!(predicate.contains(" AND id = (person:1) AND "), "{predicate}");
	assert!(
		!predicate.starts_with("IF "),
		"the conjuncts are joined by the engine's AND: {predicate}"
	);
}

#[test]
fn a_predicate_without_a_lookup_keeps_the_three_valued_and() {
	let op = pg_select("SELECT name FROM person WHERE name = 'x' AND age > 3");
	assert!(predicate(&op).starts_with("IF "), "{}", predicate(&op));
}

#[test]
fn projected_id_is_the_key_under_its_own_name() {
	let op = pg_select("SELECT id, name FROM person");
	assert_eq!(projection(&op), vec!["record::id(id) AS id".to_string(), "name AS name".into()]);
}

#[test]
fn projected_id_of_a_qualified_reference_is_the_key() {
	let op = pg_select("SELECT p.id FROM person p");
	assert_eq!(projection(&op)[0], "record::id(id) AS id");
}

#[test]
fn a_wildcard_over_a_base_table_is_followed_by_the_key() {
	let op = pg_select("SELECT * FROM person");
	assert_eq!(projection(&op), vec!["*".to_string(), "record::id(id) AS id".into()]);
}

#[test]
fn a_qualified_wildcard_over_a_base_table_is_followed_by_the_key() {
	let wildcard = projection(&my_select("SELECT * FROM person"));
	let qualified = projection(&my_select("SELECT p.* FROM person p"));
	assert_eq!(wildcard, vec!["*".to_string(), "record::id(id) AS id".into()]);
	assert_eq!(qualified, wildcard);
	// So is one qualified by the database too.
	let database_qualified = projection(&my_select("SELECT db.person.* FROM db.person"));
	assert_eq!(database_qualified, wildcard);
	// A wildcard that is not alone keeps the key after the last one.
	let op = my_select("SELECT p.*, name FROM person p");
	let columns = projection(&op);
	assert_eq!(columns[0], "*", "{columns:?}");
	assert_eq!(columns[1], "record::id(id) AS id", "{columns:?}");
	assert_eq!(columns.len(), 3, "{columns:?}");
}

#[test]
fn a_column_named_star_is_not_a_wildcard_and_gets_no_key() {
	let op = my_select("SELECT `*` FROM person");
	assert_eq!(projection(&op), vec!["`*` AS `?column?1`".to_string()]);
	let op = my_select("SELECT p.`*`, name FROM person p");
	let columns = projection(&op);
	assert!(columns.iter().all(|column| !column.contains("record::id(id)")), "{columns:?}");
}

#[test]
fn a_wildcard_over_a_derived_table_is_not_followed_by_the_key() {
	let op = pg_select("SELECT * FROM (SELECT id, name FROM person) d");
	assert_eq!(projection(&op), vec!["*".to_string()]);
}

#[test]
fn the_id_of_a_derived_table_is_not_extracted_again() {
	let op = pg_select("SELECT d.id FROM (SELECT id, name FROM person) d WHERE d.id = 1");
	assert_eq!(projection(&op), vec!["id AS id".to_string()]);
	let LogicalOp::Project(project) = &op else {
		panic!("expected Project");
	};
	let LogicalOp::Filter(filter) = project.input.as_ref() else {
		panic!("expected Filter");
	};
	assert!(filter.predicate.to_string().contains("id = 1"), "{}", filter.predicate);
	assert!(!filter.predicate.to_string().contains("record::id"), "{}", filter.predicate);
	assert!(!filter.predicate.to_string().contains("type::record"), "{}", filter.predicate);
}

#[test]
fn the_id_of_a_cte_is_not_extracted_again() {
	let op = pg_select(
		"WITH c AS (SELECT id, name FROM person) SELECT c.id FROM c WHERE c.id IN (1, 2)",
	);
	let LogicalOp::Project(project) = &op else {
		panic!("expected Project");
	};
	let LogicalOp::Filter(filter) = project.input.as_ref() else {
		panic!("expected Filter");
	};
	assert!(!filter.predicate.to_string().contains("record::id"), "{}", filter.predicate);
	assert!(!filter.predicate.to_string().contains("type::record"), "{}", filter.predicate);
}

#[test]
fn ids_of_the_same_table_are_compared_as_stored() {
	let op = pg_select("SELECT a.name FROM person a JOIN person b ON a.id = b.id");
	assert_eq!(join_keys(&op), vec![("id".to_string(), "_join0.id".to_string())]);
}

#[test]
fn ids_of_different_tables_are_compared_by_key() {
	let op = pg_select("SELECT u.name FROM users u JOIN profiles p ON p.id = u.id");
	let keys = join_keys(&op);
	assert_eq!(keys.len(), 1);
	assert!(
		keys[0].0.contains("type::record('profiles'") && keys[0].0.contains("record::id("),
		"{keys:?}"
	);
	assert_eq!(keys[0].1, "_join0.id");
}

#[test]
fn a_null_probing_the_id_of_the_joined_table_names_no_record() {
	let op = pg_select("SELECT e.name FROM proj p JOIN emp e ON p.emp_id = e.id AND e.id = NULL");
	let keys = join_keys(&op);
	assert_eq!(keys.len(), 2, "{keys:?}");
	assert_eq!(keys[1], ("NULL".to_string(), "_join0.id".to_string()));
}

#[test]
fn a_foreign_key_probing_the_id_of_the_joined_table_keeps_the_joined_id_as_stored() {
	let op = pg_select("SELECT e.name FROM proj p JOIN emp e ON p.emp_id = e.id");
	let keys = join_keys(&op);
	assert_eq!(keys.len(), 1);
	assert!(keys[0].0.contains("type::record('emp'") && keys[0].0.contains("emp_id"), "{keys:?}");
	assert_eq!(keys[0].1, "_join0.id");
}

#[test]
fn a_foreign_key_joined_to_the_id_of_the_left_table_is_compared_with_the_record_it_names() {
	let op = pg_select("SELECT e.name FROM emp e JOIN proj p ON p.emp_id = e.id");
	let keys = join_keys(&op);
	assert_eq!(
		keys,
		vec![(
			"id".to_string(),
			"IF _join0.emp_id = '' OR !(type::is_int(_join0.emp_id) OR type::is_i32(_join0.emp_id) OR type::is_i16(_join0.emp_id) OR type::is_i8(_join0.emp_id) OR type::is_u32(_join0.emp_id) OR type::is_u16(_join0.emp_id) OR type::is_u8(_join0.emp_id) OR type::is_string(_join0.emp_id) OR type::is_uuid(_join0.emp_id) OR type::is_array(_join0.emp_id) OR type::is_object(_join0.emp_id) OR type::is_record(_join0.emp_id)) THEN NULL ELSE type::record('emp', _join0.emp_id) END"
				.to_string()
		)]
	);
}

#[test]
fn using_id_between_tables_compares_keys() {
	let op = pg_select("SELECT 1 FROM users u JOIN profiles p USING (id)");
	let keys = join_keys(&op);
	assert!(keys[0].0.contains("type::record('profiles'"), "{keys:?}");
	let op = pg_select("SELECT 1 FROM users u JOIN users v USING (id)");
	assert_eq!(join_keys(&op), vec![("id".to_string(), "_join0.id".to_string())]);
}

#[test]
fn the_id_of_the_unmatched_side_of_an_outer_join_is_absent_not_an_error() {
	let op = pg_select("SELECT p.id FROM emp e LEFT JOIN proj p ON p.emp_id = e.id");
	let projection = projection(&op);
	assert!(
		projection[0].starts_with(
			"IF type::is_record(_join0.id) THEN record::id(_join0.id) ELSE _join0.id END"
		),
		"{projection:?}"
	);
}

#[test]
fn grouping_by_id_groups_and_projects_the_stored_id_and_reads_its_key_afterwards() {
	let op = pg_select("SELECT id, count(*) AS c FROM person GROUP BY id");
	assert_eq!(
		projection(&op),
		vec![
			"*".to_string(),
			"IF type::is_record(id) THEN record::id(id) ELSE id END AS id".into()
		]
	);
	let LogicalOp::Project(project) = &op else {
		panic!("expected Project");
	};
	let LogicalOp::Aggregate(aggregate) = project.input.as_ref() else {
		panic!("expected Aggregate, got {:?}", project.input);
	};
	assert_eq!(aggregate.group.len(), 1);
	assert_eq!(aggregate.group[0].to_string(), "id");
	let LogicalFields::Select(fields) = &aggregate.fields else {
		panic!("expected Select fields");
	};
	let LogicalField::Single {
		expr,
		alias,
		..
	} = &fields[0]
	else {
		panic!("expected Single field");
	};
	assert_eq!(expr.to_string(), "id");
	assert_eq!(alias.as_ref().unwrap().to_sql(), "id");
}

#[test]
fn grouping_by_the_id_of_a_joined_table_groups_the_stored_id() {
	let op = pg_select(
		"SELECT p.id AS pid, count(*) AS c FROM emp e JOIN proj p ON p.emp_id = e.id GROUP BY p.id",
	);
	assert_eq!(
		projection(&op),
		vec![
			"*".to_string(),
			"IF type::is_record(pid) THEN record::id(pid) ELSE pid END AS pid".into()
		]
	);
	let LogicalOp::Project(project) = &op else {
		panic!("expected Project");
	};
	let LogicalOp::Aggregate(aggregate) = project.input.as_ref() else {
		panic!("expected Aggregate");
	};
	assert_eq!(aggregate.group[0].to_string(), "_join0.id");
}

#[test]
fn ordering_by_id_sorts_the_stored_ids_which_order_as_their_keys() {
	let op = pg_select("SELECT name FROM person ORDER BY id DESC");
	let LogicalOp::Project(project) = &op else {
		panic!("expected Project");
	};
	let LogicalOp::Sort(sort) = project.input.as_ref() else {
		panic!("expected Sort");
	};
	let rendered = format!("{:?}", sort.ordering);
	assert!(!rendered.contains("record::id"), "{rendered}");
	assert!(rendered.contains("Field(\"id\")"), "{rendered}");
}

#[test]
fn a_correlated_reference_to_the_id_of_an_outer_table_is_compared_with_the_record_the_column_names()
{
	let op = pg_select(
		"SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
	);
	let predicate = predicate(&op);
	assert!(predicate.contains("type::record('users', user_id)"), "{predicate}");
	assert!(predicate.contains("$parent.id"), "{predicate}");
	assert!(
		predicate.contains("= NULL"),
		"a reference to an outer table keeps SQL's null handling: {predicate}"
	);
}

#[test]
fn system_catalog_tables_have_no_id_to_read_as_a_key() {
	let op = pg_select("SELECT * FROM pg_catalog.pg_namespace");
	// Each column of the view is selected by name, and none is read as a record key.
	let fields = projection(&op);
	assert_eq!(fields.len(), 4, "{fields:?}");
	assert!(fields.iter().all(|field| !field.contains("record::id")), "{fields:?}");
}

#[test]
fn mysql_reads_the_id_column_the_same_way() {
	let op = my_select("SELECT id, name FROM person WHERE id = 1");
	assert_eq!(predicate(&op), "id = (person:1)");
	assert_eq!(projection(&op)[0], "record::id(id) AS id");

	let op = my_select("SELECT name FROM person WHERE id IN (1, 2) AND name = 'x'");
	assert!(predicate(&op).contains("id INSIDE [person:1, person:2]"), "{}", predicate(&op));

	let op = my_select("SELECT * FROM person");
	assert_eq!(projection(&op), vec!["*".to_string(), "record::id(id) AS id".into()]);

	let op = my_select("SELECT a.name FROM person a JOIN person b ON a.id = b.id");
	assert_eq!(join_keys(&op), vec![("id".to_string(), "_join0.id".to_string())]);
}

#[test]
fn the_lowered_predicate_is_a_plain_comparison_the_engine_can_read_a_record_by() {
	let op = pg_select("SELECT name FROM person WHERE id = 1");
	let LogicalOp::Project(project) = &op else {
		panic!("expected Project");
	};
	let LogicalOp::Filter(filter) = project.input.as_ref() else {
		panic!("expected Filter");
	};
	assert!(
		matches!(
			&filter.predicate,
			LogicalExpr::Binary {
				op: surrealdb_expr::expr::BinaryOperator::Equal,
				right,
				..
			} if matches!(right.as_ref(), LogicalExpr::RecordId(_))
		),
		"{:?}",
		filter.predicate
	);
}

#[test]
fn an_expression_over_id_in_the_ordering_reads_the_key() {
	let op = pg_select("SELECT id FROM person ORDER BY id + 0 DESC LIMIT 1");
	let LogicalOp::Project(project) = &op else {
		panic!("expected Project");
	};
	let LogicalOp::Limit(limit) = project.input.as_ref() else {
		panic!("expected Limit");
	};
	let LogicalOp::Sort(sort) = limit.input.as_ref() else {
		panic!("expected Sort");
	};
	let rendered = format!("{:?}", sort.ordering);
	assert!(rendered.contains("record::id"), "{rendered}");
}

#[test]
fn clauses_over_an_aggregation_read_the_stored_id_it_projected() {
	let op =
		pg_select("SELECT id, count(*) AS c FROM person GROUP BY id HAVING id = 1 ORDER BY id + 0");
	let LogicalOp::Project(project) = &op else {
		panic!("expected Project");
	};
	let LogicalOp::Sort(sort) = project.input.as_ref() else {
		panic!("expected Sort, got {:?}", project.input);
	};
	let rendered = format!("{:?}", sort.ordering);
	assert!(rendered.contains("record::id"), "{rendered}");
	let LogicalOp::Filter(having) = sort.input.as_ref() else {
		panic!("expected the HAVING Filter");
	};
	assert!(
		having.predicate.to_string().contains(&format!("({GROUPED_KEY}) = 1")),
		"{}",
		having.predicate
	);
}

#[test]
fn ordering_a_set_operation_reads_the_keys_its_operands_projected() {
	let op = pg_select("SELECT id FROM person UNION SELECT id FROM profile ORDER BY id");
	let LogicalOp::Sort(sort) = &op else {
		panic!("expected Sort, got {op:?}");
	};
	let rendered = format!("{:?}", sort.ordering);
	assert!(!rendered.contains("record::id"), "{rendered}");
}

const DEPT_RECORD: &str = "IF mgr = '' OR !(type::is_int(mgr) OR type::is_i32(mgr) OR type::is_i16(mgr) OR type::is_i8(mgr) OR type::is_u32(mgr) OR type::is_u16(mgr) OR type::is_u8(mgr) OR type::is_string(mgr) OR type::is_uuid(mgr) OR type::is_array(mgr) OR type::is_object(mgr) OR type::is_record(mgr)) THEN NULL ELSE type::record('dept', mgr) END";

#[test]
fn id_equal_to_a_column_is_compared_with_the_record_the_column_names() {
	let op = pg_select("SELECT name FROM dept WHERE id = mgr");
	assert_eq!(predicate(&op), format!("id = ({DEPT_RECORD})"));
	let op = pg_select("SELECT name FROM dept WHERE mgr = id");
	assert_eq!(predicate(&op), format!("id = ({DEPT_RECORD})"));
	let op = my_select("SELECT name FROM dept WHERE id = mgr");
	assert_eq!(predicate(&op), format!("id = ({DEPT_RECORD})"));
}

#[test]
fn id_equal_to_an_expression_is_compared_with_the_record_the_expression_names() {
	let op = pg_select("SELECT name FROM dept WHERE id = mgr + 1");
	let predicate = predicate(&op);
	assert!(predicate.starts_with("id = (IF "), "{predicate}");
	assert!(predicate.contains("type::record('dept', "), "{predicate}");
	assert!(!predicate.contains("record::id(id)"), "{predicate}");
}

#[test]
fn id_compared_with_a_column_anywhere_but_a_top_level_conjunct_keeps_null_handling() {
	let op = pg_select("SELECT name FROM dept WHERE NOT (id = mgr)");
	let negated = predicate(&op);
	assert!(negated.contains("type::record('dept', mgr)"), "{negated}");
	assert!(negated.contains("IF "), "{negated}");
	let op = pg_select("SELECT name FROM dept WHERE id <> mgr");
	let unequal = predicate(&op);
	assert!(unequal.contains("type::record('dept', mgr)"), "{unequal}");
	assert!(unequal.contains("id != "), "{unequal}");
	let op = pg_select("SELECT id = mgr AS same FROM dept");
	assert!(projection(&op)[0].contains("type::record('dept', mgr)"), "{:?}", projection(&op));
}

#[test]
fn id_in_columns_and_keys_is_a_membership_of_the_records_they_name() {
	let op = pg_select("SELECT name FROM dept WHERE id IN (mgr, 2, $1)");
	let predicate = predicate(&op);
	assert!(predicate.starts_with("id INSIDE [(IF mgr = '' OR !("), "{predicate}");
	assert!(predicate.contains("dept:2"), "{predicate}");
	assert!(predicate.contains("type::record('dept', $_1)"), "{predicate}");
	let op = pg_select("SELECT name FROM dept WHERE id NOT IN (mgr, boss)");
	let negated = predicate_of(&op);
	assert!(negated.starts_with("[id, [") && negated.contains("THEN false"), "{negated}");
	let op = pg_select("SELECT name FROM dept WHERE id IN (mgr)");
	assert_eq!(predicate_of(&op), format!("id = ({DEPT_RECORD})"));
}

fn predicate_of(op: &LogicalOp) -> String {
	predicate(op)
}

#[test]
fn id_compared_with_a_subquery_or_a_float_member_reads_the_key() {
	let op = pg_select("SELECT name FROM dept WHERE id = (SELECT max(n) FROM other)");
	let predicate = predicate(&op);
	assert!(!predicate.contains("type::record"), "{predicate}");
	assert!(predicate.contains("record::id(id)"), "{predicate}");
	let op = pg_select("SELECT name FROM dept WHERE id IN (mgr, 1.5)");
	let predicate = predicate_of(&op);
	assert!(!predicate.contains("type::record"), "{predicate}");
}

#[test]
fn a_derived_column_compared_with_the_id_of_a_base_table_is_compared_with_the_record_it_names() {
	let op = pg_select(
		"SELECT p.name FROM person p JOIN (SELECT author FROM post) d ON p.id = d.author",
	);
	let keys = join_keys(&op);
	assert_eq!(keys.len(), 1);
	assert_eq!(keys[0].0, "id");
	assert!(keys[0].1.contains("type::record('person', _join0.author)"), "{keys:?}");
}

#[test]
fn a_foreign_key_probing_an_id_names_the_record_whether_it_holds_a_key_or_a_link() {
	let op = pg_select("SELECT e.name FROM proj p JOIN emp e ON p.emp_id = e.id");
	let keys = join_keys(&op);
	assert_eq!(keys.len(), 1);
	assert!(keys[0].0.contains("type::record('emp', emp_id)"), "{keys:?}");
	assert_eq!(keys[0].1, "_join0.id");
	let op = my_select("SELECT e.name FROM proj p JOIN emp e ON p.emp_id = e.id");
	let keys = join_keys(&op);
	assert!(keys[0].0.contains("type::record('emp', emp_id)"), "{keys:?}");
}

#[test]
fn the_id_of_a_joined_table_is_never_the_lookup_form() {
	let op =
		pg_select("SELECT p.id FROM person p LEFT JOIN profile f ON f.id = p.id WHERE f.id = $1");
	let predicate = predicate(&op);
	assert!(
		predicate.contains("= NULL"),
		"an outer-joined id keeps SQL's null handling: {predicate}"
	);
	assert!(!predicate.starts_with("_join0.id = "), "{predicate}");
}

#[test]
fn the_id_of_the_table_read_is_the_lookup_form_even_beside_a_join() {
	let op = pg_select("SELECT p.id FROM person p JOIN o ON o.pid = p.id WHERE p.id = 7");
	assert_eq!(predicate(&op), "id = (person:7)");
}

#[test]
fn a_key_that_names_no_record_is_null_and_not_the_absent_id_of_an_outer_join() {
	let op = pg_select("SELECT name FROM person WHERE id = $1");
	let predicate = predicate(&op);
	assert!(
		predicate.contains("$_1 = '' OR !(type::is_int($_1) OR type::is_i32($_1) OR type::is_i16($_1) OR type::is_i8($_1) OR type::is_u32($_1) OR type::is_u16($_1) OR type::is_u8($_1) OR type::is_string($_1) OR type::is_uuid($_1) OR type::is_array($_1) OR type::is_object($_1) OR type::is_record($_1)) THEN NULL ELSE type::record"),
		"{predicate}"
	);
}

#[test]
fn null_safe_and_distinct_comparisons_of_an_id_with_a_column_name_the_record() {
	for sql in [
		"SELECT name FROM emp WHERE id IS NOT DISTINCT FROM mgr",
		"SELECT name FROM emp WHERE id IS DISTINCT FROM mgr",
	] {
		let op = pg_select(sql);
		let predicate = predicate(&op);
		assert!(predicate.contains("type::record('emp', mgr)"), "{sql}: {predicate}");
		assert!(!predicate.contains("record::id(id)"), "{sql}: {predicate}");
	}
	let op = my_select("SELECT name FROM emp WHERE id <=> mgr");
	let predicate = predicate(&op);
	assert!(predicate.contains("type::record('emp', mgr)"), "{predicate}");
}

#[test]
fn a_key_that_is_not_a_record_key_kind_names_no_record() {
	let op = pg_select("SELECT name FROM person WHERE id = $1");
	let predicate = predicate(&op);
	for kind in ["int", "string", "uuid", "array", "object", "record"] {
		assert!(predicate.contains(&format!("type::is_{kind}($_1)")), "{kind}: {predicate}");
	}
	assert!(predicate.contains("THEN NULL ELSE type::record('person', $_1)"), "{predicate}");
}

/// The key of the grouped `id` output, read through the guard of an `id` that may be absent.
const GROUPED_KEY: &str = "IF type::is_record(id) THEN record::id(id) ELSE id END";

/// The values the sort of the pipeline orders by, one for each ordering it lowers to.
fn ordering(op: &LogicalOp) -> Vec<String> {
	match op {
		LogicalOp::Sort(sort) => {
			let LogicalOrdering::Order(orders) = &sort.ordering else {
				panic!("expected an explicit ordering");
			};
			orders.iter().map(|order| order.value.to_string()).collect()
		}
		LogicalOp::Project(p) => ordering(&p.input),
		LogicalOp::Limit(l) => ordering(&l.input),
		other @ LogicalOp::Source(_)
		| other @ LogicalOp::Values(_)
		| other @ LogicalOp::Match(_)
		| other @ LogicalOp::Subquery(_)
		| other @ LogicalOp::Write(_)
		| other @ LogicalOp::Ddl(_)
		| other @ LogicalOp::UnionAll(_)
		| other @ LogicalOp::Join(_)
		| other @ LogicalOp::Filter(_)
		| other @ LogicalOp::PermissionFilter(_)
		| other @ LogicalOp::Split(_)
		| other @ LogicalOp::Aggregate(_)
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
		| other @ LogicalOp::Window(_) => panic!("no sort under {other:?}"),
	}
}

#[test]
fn having_over_a_grouped_id_of_an_outer_joined_table_reads_a_key_that_may_be_absent() {
	let having = "HAVING e.id IS NULL";
	let from = "FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY e.id";
	let expected = format!("({GROUPED_KEY}) = NULL OR ({GROUPED_KEY}) = NONE");
	let op = pg_select(&format!("SELECT e.id, count(*) AS c {from} {having}"));
	assert_eq!(predicate(&op), expected);
	let op = my_select(&format!("SELECT e.id, count(*) AS c {from} {having}"));
	assert_eq!(predicate(&op), expected);

	let op = pg_select(&format!("SELECT e.id, count(*) AS c {from} HAVING e.id > 1"));
	let having = predicate(&op);
	assert!(having.contains(&format!("({GROUPED_KEY}) > 1")), "{having}");
	assert!(!having.replace(GROUPED_KEY, "").contains("record::id"), "{having}");
}

#[test]
fn an_ordering_expression_over_a_grouped_id_of_an_outer_joined_table_reads_a_key_that_may_be_absent()
 {
	let from = "FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY e.id";
	for op in [
		pg_select(&format!("SELECT e.id, count(*) AS c {from} ORDER BY e.id + 0")),
		my_select(&format!("SELECT e.id, count(*) AS c {from} ORDER BY e.id + 0")),
	] {
		let orders = ordering(&op);
		assert!(!orders.is_empty());
		for order in orders {
			assert!(order.contains(GROUPED_KEY), "{order}");
			assert!(!order.replace(GROUPED_KEY, "").contains("record::id"), "{order}");
		}
	}
}

#[test]
fn ordering_by_a_grouped_id_itself_sorts_the_stored_id() {
	let op = pg_select(
		"SELECT e.id, count(*) AS c FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY e.id ORDER BY e.id",
	);
	assert_eq!(ordering(&op), vec!["id = NULL OR id = NONE".to_string(), "id".to_string()]);
}

#[test]
fn a_grouped_id_of_the_table_read_is_read_through_the_same_guard_as_the_projection() {
	let op = pg_select("SELECT id, count(*) AS c FROM person GROUP BY id HAVING id > 1");
	let having = predicate(&op);
	assert!(having.contains(&format!("({GROUPED_KEY}) > 1")), "{having}");
	assert!(!having.replace(GROUPED_KEY, "").contains("record::id"), "{having}");
}

/// The functions `type::is_*` that the rendering `text` calls.
fn kind_tests(text: &str) -> BTreeSet<String> {
	text.split("type::is_")
		.skip(1)
		.map(|rest| {
			let (name, _) = rest.split_once('(').expect("a call");
			format!("type::is_{name}")
		})
		.collect()
}

fn kinds(names: &[&str]) -> BTreeSet<String> {
	names.iter().map(|name| name.to_string()).collect()
}

fn pg_write(sql: &str) -> String {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Update(update) => {
			lower_pgsql_update(update, &catalog())
		}
		surrealdb_pgsql::ast::stmt::Statement::Delete(delete) => {
			lower_pgsql_delete(delete, &catalog())
		}
		surrealdb_pgsql::ast::stmt::Statement::Copy(_)
		| surrealdb_pgsql::ast::stmt::Statement::Select(_)
		| surrealdb_pgsql::ast::stmt::Statement::Insert(_)
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
		| surrealdb_pgsql::ast::stmt::Statement::Empty => panic!("expected UPDATE or DELETE"),
	}
	.expect("lower")
	.to_sql()
}

fn my_write(sql: &str) -> String {
	let stmts = surrealdb_mysql::parse_statements(sql).expect("parse");
	let surrealdb_mysql::ast::stmt::Statement::Update(update) = &stmts[0] else {
		panic!("expected UPDATE");
	};
	lower_mysql_update(update, &catalog()).expect("lower").to_sql()
}

#[test]
fn a_read_and_a_write_name_a_record_by_the_same_kinds_of_value() {
	let naming = kinds(&NAMING_KINDS);

	// An `id` compared with a parameter, with a column and with a probing column of a join.
	let reads = [
		predicate(&pg_select("SELECT name FROM person WHERE id = $1")),
		predicate(&pg_select("SELECT name FROM dept WHERE id = mgr")),
		predicate(&my_select("SELECT name FROM dept WHERE id = mgr")),
		join_keys(&pg_select("SELECT e.name FROM proj p JOIN emp e ON p.emp_id = e.id"))[0]
			.0
			.clone(),
	];
	// An `id` and a link column compared with a parameter in the `WHERE` clause of a write.
	let writes = [
		pg_write("UPDATE emp SET name = 'x' WHERE id = $1"),
		pg_write("DELETE FROM emp WHERE id = $1"),
		pg_write("UPDATE emp SET name = 'x' WHERE dept = $1"),
		my_write("UPDATE emp SET name = 'x' WHERE id = ?"),
	];
	for text in reads.iter().chain(&writes) {
		assert_eq!(kind_tests(text), naming, "{text}");
	}

	// A value written to a link column that is a record already is left as it is: a record is no
	// key. It is the one difference, and it is a record link.
	let linked = [pg_write("UPDATE emp SET dept = $1"), my_write("UPDATE emp SET dept = ?")];
	for text in &linked {
		assert_eq!(kind_tests(text), kinds(&KEY_KINDS), "{text}");
	}
	assert_eq!(
		naming.difference(&kinds(&KEY_KINDS)).cloned().collect::<Vec<_>>(),
		[RECORD_KIND.to_string()]
	);
}

/// A `SELECT` over tables that have the record link columns of the link catalog.
fn pg_select_linked(sql: &str) -> LogicalOp {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::bind_and_lower_pgsql(select, &catalog(), &DialectPolicy::postgres()).expect("lower")
}

fn my_select_linked(sql: &str) -> LogicalOp {
	let stmts = surrealdb_mysql::parse_statements(sql).expect("parse");
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::bind_and_lower_mysql(select, &catalog(), &DialectPolicy::mysql()).expect("lower")
}

/// Whether the rendering `text` holds an optional step, `value.?(...)`, or the `?? NULL` that reads
/// the absent value of one.
fn has_optional_step(text: &str) -> bool {
	text.contains(".?(") || text.contains("?? NULL")
}

#[test]
fn a_key_that_reads_no_row_is_left_for_the_engine_to_fold() {
	// The engine folds a key that reads no row to the record it names, and reads that record, or
	// the rows an index finds for it, directly. It does not fold an optional step.
	let selects = [
		"SELECT v FROM big WHERE id = abs(5)",
		"SELECT v FROM big WHERE id = coalesce(NULL, 5)",
		"SELECT v FROM big WHERE id = least(5, 6)",
		"SELECT v FROM big WHERE id = length('abcd')",
		"SELECT v FROM big WHERE id IN (abs(5))",
		"SELECT v FROM big WHERE id IN (abs(5), abs(6))",
		"SELECT name FROM emp WHERE dept = abs(5)",
		"SELECT name FROM emp WHERE mentor IN (abs(5), 7)",
	];
	for sql in selects {
		let text = predicate(&pg_select_linked(sql));
		assert!(!has_optional_step(&text), "{sql}: {text}");
		assert!(text.contains("type::record("), "{sql}: {text}");
	}
	let selects = [
		"SELECT v FROM big WHERE id = abs(5)",
		"SELECT v FROM big WHERE id IN (abs(5), abs(6))",
		"SELECT name FROM emp WHERE dept = abs(5)",
		"SELECT name FROM emp WHERE mentor IN (abs(5), 7)",
	];
	for sql in selects {
		let text = predicate(&my_select_linked(sql));
		assert!(!has_optional_step(&text), "{sql}: {text}");
		assert!(text.contains("type::record("), "{sql}: {text}");
	}
}

#[test]
fn a_key_that_reads_a_row_is_not_wrapped_in_an_optional_step() {
	// The engine merges the reads of one pure expression, so no key is bound to be read once.
	for sql in [
		"SELECT id FROM emp WHERE id = lower(code)",
		"SELECT id FROM emp WHERE lower(code) = id",
		"SELECT id FROM emp WHERE id IN (lower(code), 'x')",
		"SELECT id FROM emp WHERE name = 'x' AND id = n + 1",
	] {
		let text = predicate(&pg_select(sql));
		assert!(!has_optional_step(&text), "{sql}: {text}");
	}
	let text = predicate(&my_select("SELECT id FROM emp WHERE id = lower(code)"));
	assert!(!has_optional_step(&text), "{text}");

	for sql in [
		"SELECT a.name FROM emp a JOIN emp b ON lower(a.code) = b.id",
		"SELECT a.name FROM emp a LEFT JOIN emp b ON b.id = lower(a.code)",
		"SELECT 1 FROM users u LEFT JOIN profiles p USING (id)",
	] {
		for (probe, build) in join_keys(&pg_select(sql)) {
			assert!(!has_optional_step(&probe) && !has_optional_step(&build), "{sql}: {probe}");
			assert!(probe.contains("type::record(") || build.contains("type::record("), "{sql}");
		}
	}
}

#[test]
fn a_key_that_is_a_call_names_no_record_unless_it_is_a_key() {
	let text = predicate(&pg_select("SELECT id FROM emp WHERE id = lower(code)"));
	assert!(
		text.contains(
			"THEN NULL ELSE type::record('emp', (IF code = NULL OR code = NONE THEN NULL ELSE string::lowercase(code) END)) END"
		),
		"{text}"
	);
}

#[test]
fn the_guard_of_an_absent_id_tests_for_a_record_link_only() {
	let op = pg_select("SELECT p.id FROM emp e LEFT JOIN proj p ON p.emp_id = e.id");
	assert_eq!(kind_tests(&projection(&op)[0]), kinds(&[RECORD_KIND]));
}

/// The text of the key that probes the `id` of the joined table `emp` when `literal` is compared
/// with it, in the forms of both dialects: the literal on either side of `=`, in an inner and in a
/// left join. Every form keeps the `id` of the joined table as it is stored.
fn join_literal_probes(literal: &str) -> Vec<String> {
	let mut probes = Vec::new();
	for on in [format!("e.id = {literal}"), format!("{literal} = e.id")] {
		for kind in ["JOIN", "LEFT JOIN"] {
			let sql = format!("SELECT a.label, e.name FROM a {kind} emp e ON {on}");
			for op in [pg_select(&sql), my_select(&sql)] {
				let keys = join_keys(&op);
				assert_eq!(keys.len(), 1, "{sql}: {keys:?}");
				assert_eq!(keys[0].1, "_join0.id", "{sql}: {keys:?}");
				probes.push(keys[0].0.clone());
			}
		}
	}
	probes
}

#[test]
fn a_literal_key_probing_the_id_of_the_joined_table_names_its_record() {
	for (literal, key) in [("1", "1"), ("-1", "-1"), ("0", "0"), ("'x'", "'x'"), ("'1.5'", "'1.5'")]
	{
		for probe in join_literal_probes(literal) {
			assert_eq!(probe, format!("type::record('emp', {key})"), "{literal}");
		}
	}
}

#[test]
fn a_number_with_an_integral_value_probing_the_id_of_the_joined_table_names_the_integer_key() {
	for (literal, key) in [
		("3.0", "3"),
		("-3.0", "-3"),
		("+3.0", "3"),
		("0.0", "0"),
		("-0.0", "0"),
		("3e0", "3"),
		("1e3", "1000"),
		("9007199254740992.0", "9007199254740992"),
		("-9223372036854775808.0", "-9223372036854775808"),
	] {
		for probe in join_literal_probes(literal) {
			assert_eq!(probe, format!("type::record('emp', {key})"), "{literal}");
		}
	}
}

#[test]
fn a_literal_that_is_no_key_probing_the_id_of_the_joined_table_names_no_record() {
	for literal in [
		"1.5",
		"-1.5",
		"0.5",
		"true",
		"false",
		"''",
		"NULL",
		"1e30",
		"-1e30",
		"9223372036854775808.0",
		"-9223372036854777856.0",
	] {
		for probe in join_literal_probes(literal) {
			assert_eq!(probe, "NULL", "{literal}");
		}
	}
}

#[test]
fn a_literal_names_a_record_by_the_rule_that_judges_a_key_at_run_time() {
	use surrealdb_expr::expr::logical::LogicalLiteral;
	use surrealdb_expr::val::{Bytes, Datetime, Duration, Uuid};
	use surrealdb_strand::Strand;
	use surrealdb_types::{Decimal, f16};

	use crate::lower::id_key::record_of;

	let probe =
		|literal: LogicalLiteral| record_of("emp", LogicalExpr::Literal(literal)).to_string();
	let uuid = Uuid::default();
	let uuid_text = LogicalExpr::Literal(LogicalLiteral::Uuid(uuid)).to_string();
	let record = |key: &str| format!("type::record('emp', {key})");

	assert_eq!(probe(LogicalLiteral::Integer(7)), record("7"));
	assert_eq!(probe(LogicalLiteral::I8(7)), record("7"));
	assert_eq!(probe(LogicalLiteral::U8(7)), record("7"));
	assert_eq!(probe(LogicalLiteral::I16(7)), record("7"));
	assert_eq!(probe(LogicalLiteral::U16(7)), record("7"));
	assert_eq!(probe(LogicalLiteral::I32(7)), record("7"));
	assert_eq!(probe(LogicalLiteral::U32(7)), record("7"));
	assert_eq!(probe(LogicalLiteral::U64(7)), record("7"));
	assert_eq!(probe(LogicalLiteral::F16(f16::from_f32(3.0))), record("3"));
	assert_eq!(probe(LogicalLiteral::F32(3.0)), record("3"));
	assert_eq!(probe(LogicalLiteral::String(Strand::from("k"))), record("'k'"));
	assert_eq!(probe(LogicalLiteral::Uuid(uuid)), record(&uuid_text));
	assert_eq!(probe(LogicalLiteral::Float(3.0)), record("3"));
	assert_eq!(probe(LogicalLiteral::Decimal(Decimal::new(300, 2))), record("3"));
	assert_eq!(probe(LogicalLiteral::Decimal(Decimal::new(-4, 0))), record("-4"));

	for literal in [
		LogicalLiteral::None,
		LogicalLiteral::Null,
		LogicalLiteral::UnboundedRange,
		LogicalLiteral::Bool(true),
		LogicalLiteral::Float(1.5),
		LogicalLiteral::Float(f64::NAN),
		LogicalLiteral::Float(f64::INFINITY),
		LogicalLiteral::Float(f64::NEG_INFINITY),
		LogicalLiteral::Float(-(i64::MIN as f64)),
		LogicalLiteral::F16(f16::from_f32(1.5)),
		LogicalLiteral::F32(1.5),
		LogicalLiteral::U64(u64::MAX),
		LogicalLiteral::Decimal(Decimal::new(15, 1)),
		LogicalLiteral::Decimal(Decimal::MAX),
		LogicalLiteral::String(Strand::from("")),
		LogicalLiteral::Bytes(Bytes::from(vec![1u8])),
		LogicalLiteral::Duration(Duration::default()),
		LogicalLiteral::Datetime(Datetime::MIN_UTC),
	] {
		assert_eq!(probe(literal.clone()), "NULL", "{literal:?}");
	}
}

#[test]
fn a_number_literal_compared_with_an_id_in_a_predicate_is_compared_with_the_key_by_value() {
	for sql in ["id = 3.0", "3.0 = id", "id = -0.0", "id = 1.5", "id = true", "id = ''"] {
		for op in [
			pg_select(&format!("SELECT name FROM emp WHERE {sql}")),
			my_select(&format!("SELECT name FROM emp WHERE {sql}")),
		] {
			let predicate = predicate(&op);
			assert!(!predicate.contains("type::record"), "{sql}: {predicate}");
			assert!(predicate.contains("record::id(id)"), "{sql}: {predicate}");
		}
	}
	let predicate = predicate(&pg_select("SELECT name FROM emp WHERE id = 3.0"));
	assert!(predicate.contains("record::id(id) = 3dec"), "{predicate}");
}
