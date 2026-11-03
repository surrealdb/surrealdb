//! The result descriptor of a column declared with a user-defined domain or composite type.

use surrealdb_expr::expr::Kind;

use crate::catalog::{CompositeType, DomainType, MemoryCatalog, TableSchema};
use crate::descriptor::{ResultComposite, ResultDescriptor, SqlType};
use crate::dialect::DialectPolicy;

/// The table `people`, with a column `rank` of the domain `posint` over `integer`, and a column
/// `home` of the composite type `address`, which holds a text `street` and an int4 `zip`.
fn catalog() -> MemoryCatalog {
	let people = TableSchema::new(
		"people",
		vec![
			crate::ColumnInfo::new("id", 0),
			crate::ColumnInfo::new("rank", 1).with_type("integer").with_domain("posint"),
			crate::ColumnInfo::new("home", 2).with_type("jsonb").with_composite("address"),
		],
	)
	.with_schema("public");
	let mut catalog = MemoryCatalog::new()
		.with_domains(vec![DomainType {
			name: "posint".to_owned(),
			id: 3,
			base: Kind::I32,
			base_type: "int4".to_owned(),
			not_null: true,
			default: None,
			checks: Vec::new(),
		}])
		.with_composites(vec![CompositeType {
			name: "address".to_owned(),
			id: 2,
			fields: vec![("street".to_owned(), Kind::String), ("zip".to_owned(), Kind::I32)],
			declared: Vec::new(),
		}]);
	catalog.add_table(people);
	catalog
}

fn describe(sql: &str, catalog: &MemoryCatalog) -> ResultDescriptor {
	let statements = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Select(stmt) = &statements[0] else {
		panic!("expected SELECT");
	};
	let policy = DialectPolicy::postgres();
	let bound = crate::binder::pgsql::bind_select(stmt, catalog, &policy, None).expect("bind");
	bound.describe(catalog, &policy)
}

#[test]
fn a_domain_column_is_described_by_its_base_type() {
	let result = describe("SELECT rank FROM people", &catalog());
	assert_eq!(result.columns[0].ty, SqlType::Int4);
	assert_eq!(result.columns[0].composite, None);
}

#[test]
fn a_composite_column_is_described_by_its_fields() {
	let result = describe("SELECT home FROM people", &catalog());
	assert_eq!(
		result.columns[0].composite,
		Some(ResultComposite {
			id: 2,
			fields: vec![("street".to_owned(), SqlType::Text), ("zip".to_owned(), SqlType::Int4),],
		})
	);
}

#[test]
fn a_column_of_an_ordinary_table_has_no_composite_type() {
	let result = describe("SELECT id FROM people", &catalog());
	assert_eq!(result.columns[0].composite, None);
}
