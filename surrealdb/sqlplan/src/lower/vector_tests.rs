//! The pgvector surface: the `vector(n)` type, `CREATE EXTENSION`, `hnsw` indexes, the distance
//! operators and functions, and the nearest-neighbour search an `ORDER BY` distance asks for.

use surrealdb_expr::expr::index::{Distance, VectorType};
use surrealdb_expr::expr::index_kind::Index;
use surrealdb_expr::expr::statements::define::DefineStatement;
use surrealdb_expr::expr::{Expr, Kind};
use surrealdb_types::ToSql;

use super::ddl::{lower_pgsql_ddl_over, map_pgsql_type};
use crate::catalog::MemoryCatalog;
use crate::error::PlanError;
use crate::vector::VectorMetric;
use crate::{DialectPolicy, SqlType};

fn statement(sql: &str) -> surrealdb_pgsql::ast::stmt::Statement {
	surrealdb_pgsql::parse_statement(sql).expect("parse")
}

fn lower_ddl(sql: &str, dimensions: Option<usize>) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_ddl_over(&statement(sql), &|_, _| dimensions)
}

/// The index `sql` defines, over a column of `dimensions` dimensions.
fn hnsw(sql: &str, dimensions: Option<usize>) -> Result<Index, PlanError> {
	let exprs = lower_ddl(sql, dimensions)?;
	let [Expr::Block(block)] = exprs.as_slice() else {
		let [Expr::Define(define)] = exprs.as_slice() else {
			panic!("expected one definition, got {exprs:?}");
		};
		let DefineStatement::Index(index) = &**define else {
			panic!("expected an index, got {define:?}");
		};
		return Ok(index.index.clone());
	};
	let [Expr::Define(define)] = block.0.as_slice() else {
		panic!("expected one definition, got {block:?}");
	};
	let DefineStatement::Index(index) = &**define else {
		panic!("expected an index, got {define:?}");
	};
	Ok(index.index.clone())
}

fn hnsw_params(index: Index) -> surrealdb_expr::expr::index::HnswParams {
	let Index::Hnsw(params) = index else {
		panic!("expected an hnsw index");
	};
	params
}

#[test]
fn a_vector_column_is_an_array_of_floats_of_its_dimensions() {
	let Statement::CreateTable(table) = statement("CREATE TABLE t (a vector(3), b vector(1536))")
	else {
		panic!("expected a table");
	};
	assert_eq!(
		map_pgsql_type(&table.columns[0].data_type),
		Kind::Array(Box::new(Kind::Float), Some(3))
	);
	assert_eq!(
		map_pgsql_type(&table.columns[1].data_type),
		Kind::Array(Box::new(Kind::Float), Some(1536))
	);
}

#[test]
fn a_vector_column_needs_valid_dimensions() {
	for sql in [
		"CREATE TABLE t (a vector)",
		"ALTER TABLE t ADD COLUMN a vector",
		"ALTER TABLE t ALTER COLUMN a TYPE vector",
		"CREATE TABLE t (a vector(3)[])",
	] {
		assert!(matches!(lower_ddl(sql, None), Err(PlanError::UnsupportedStatement(_))), "{sql}");
	}
	for sql in [
		"CREATE TABLE t (a vector(0))",
		"CREATE TABLE t (a vector(16001))",
		"CREATE TABLE t (a vector(1, 2))",
		"ALTER TABLE t ADD COLUMN a vector(0)",
	] {
		assert!(matches!(lower_ddl(sql, None), Err(PlanError::InvalidParameterValue(_))), "{sql}");
	}
	assert!(lower_ddl("CREATE TABLE t (a vector(16000))", None).is_ok());
	assert!(lower_ddl("ALTER TABLE t ADD COLUMN a vector(3)", None).is_ok());
}

#[test]
fn create_extension_vector_defines_nothing() {
	for sql in
		["CREATE EXTENSION vector", "CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public"]
	{
		assert_eq!(lower_ddl(sql, None).unwrap(), vec![Expr::Block(Box::default())], "{sql}");
	}
}

#[test]
fn another_extension_is_not_available() {
	assert_eq!(
		lower_ddl("CREATE EXTENSION postgis", None),
		Err(PlanError::ExtensionNotAvailable("postgis".into()))
	);
	assert_eq!(PlanError::ExtensionNotAvailable("x".into()).pg_sqlstate(), "58P01");
}

#[test]
fn an_hnsw_index_ranks_by_the_distance_of_its_operator_class() {
	for (opclass, distance) in [
		("vector_l2_ops", Distance::Euclidean),
		("vector_cosine_ops", Distance::Cosine),
		("vector_ip_ops", Distance::InnerProduct),
		("vector_l1_ops", Distance::Manhattan),
		("public.vector_l2_ops", Distance::Euclidean),
	] {
		let sql = format!("CREATE INDEX ON items USING hnsw (embedding {opclass})");
		let params = hnsw_params(hnsw(&sql, Some(3)).unwrap());
		assert_eq!(params.distance, distance, "{opclass}");
		assert_eq!(params.dimension, 3);
		assert_eq!(params.vector_type, VectorType::F32);
	}
}

#[test]
fn an_hnsw_index_has_the_defaults_of_pgvector() {
	let params = hnsw_params(
		hnsw("CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)", Some(3)).unwrap(),
	);
	assert_eq!((params.m, params.m0, params.ef_construction), (16, 32, 64));
}

#[test]
fn an_hnsw_index_reads_its_storage_parameters() {
	let params = hnsw_params(
		hnsw(
			"CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (m = 8, ef_construction = 40)",
			Some(3),
		)
		.unwrap(),
	);
	assert_eq!((params.m, params.m0, params.ef_construction), (8, 16, 40));
	// A text value is read as the number it holds, as PostgreSQL reads a storage parameter.
	let params = hnsw_params(
		hnsw("CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (m = '4')", Some(3))
			.unwrap(),
	);
	assert_eq!(params.m, 4);
}

#[test]
fn an_hnsw_index_is_named_as_postgresql_names_it() {
	let exprs =
		lower_ddl("CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)", Some(3)).unwrap();
	assert!(exprs[0].to_sql().contains("items_embedding_idx"), "{exprs:?}");
}

#[test]
fn an_hnsw_index_refuses_what_the_engine_index_cannot_do() {
	for (sql, refused) in [
		("CREATE UNIQUE INDEX ON items USING hnsw (e vector_l2_ops)", "unique indexes"),
		(
			"CREATE INDEX ON items USING hnsw (e vector_l2_ops, f vector_l2_ops)",
			"multicolumn indexes",
		),
		("CREATE INDEX ON items USING hnsw (e vector_l2_ops) WHERE f > 1", "partial indexes"),
		("CREATE INDEX ON items USING hnsw (e vector_l2_ops DESC)", "ASC/DESC options"),
		(
			"CREATE INDEX ON items USING hnsw (e vector_l2_ops NULLS LAST)",
			"NULLS FIRST/LAST options",
		),
		("CREATE INDEX ON items USING hnsw ((e + 1) vector_l2_ops)", "expression indexes"),
	] {
		let error = hnsw(sql, Some(3)).unwrap_err();
		assert!(
			matches!(&error, PlanError::UnsupportedStatement(message) if message.contains(refused)),
			"{sql}: {error:?}"
		);
		assert_eq!(error.pg_sqlstate(), "0A000");
	}
}

#[test]
fn an_hnsw_index_refuses_access_methods_and_classes_that_are_not_supported() {
	assert!(matches!(
		lower_ddl("CREATE INDEX ON items USING ivfflat (e vector_l2_ops)", Some(3)),
		Err(PlanError::UnsupportedStatement(_))
	));
	for opclass in ["halfvec_l2_ops", "sparsevec_cosine_ops", "bit_hamming_ops"] {
		let sql = format!("CREATE INDEX ON items USING hnsw (e {opclass})");
		assert!(
			matches!(hnsw(&sql, Some(3)), Err(PlanError::UnsupportedStatement(_))),
			"{opclass}"
		);
	}
}

#[test]
fn an_hnsw_index_needs_a_known_operator_class() {
	assert_eq!(
		hnsw("CREATE INDEX ON items USING hnsw (e)", Some(3)).unwrap_err(),
		PlanError::NoDefaultOperatorClass {
			type_name: "vector".into(),
			method: "hnsw".into(),
		}
	);
	assert_eq!(
		hnsw("CREATE INDEX ON items USING hnsw (e int4_ops)", Some(3)).unwrap_err(),
		PlanError::UndefinedOperatorClass {
			name: "int4_ops".into(),
			method: "hnsw".into(),
		}
	);
	assert_eq!(
		hnsw("CREATE INDEX ON items USING hnsw (e)", Some(3)).unwrap_err().pg_sqlstate(),
		"42704"
	);
}

#[test]
fn an_hnsw_index_checks_its_storage_parameters() {
	for sql in [
		"WITH (m = 1)",
		"WITH (m = 101)",
		"WITH (ef_construction = 3)",
		"WITH (ef_construction = 1001)",
		"WITH (m = 16, ef_construction = 31)",
		"WITH (fillfactor = 70)",
		"WITH (m)",
		"WITH (m = 'x')",
		"WITH (m = 1.5)",
	] {
		let sql = format!("CREATE INDEX ON items USING hnsw (e vector_l2_ops) {sql}");
		let error = hnsw(&sql, Some(3)).unwrap_err();
		assert!(matches!(error, PlanError::InvalidParameterValue(_)), "{sql}: {error:?}");
		assert_eq!(error.pg_sqlstate(), "22023");
	}
	assert!(
		hnsw(
			"CREATE INDEX ON items USING hnsw (e vector_l2_ops) WITH (m = 16, ef_construction = 32)",
			Some(3)
		)
		.is_ok()
	);
}

#[test]
fn an_hnsw_index_needs_a_column_of_dimensions_it_covers() {
	let sql = "CREATE INDEX ON items USING hnsw (e vector_l2_ops)";
	assert_eq!(
		hnsw(sql, None).unwrap_err(),
		PlanError::InvalidParameterValue("column does not have dimensions".into())
	);
	assert!(hnsw(sql, Some(2000)).is_ok());
	let error = hnsw(sql, Some(2001)).unwrap_err();
	assert!(matches!(error, PlanError::ProgramLimitExceeded(_)), "{error:?}");
	assert_eq!(error.pg_sqlstate(), "54000");
}

#[test]
fn the_dimensions_are_those_of_the_column_the_index_names() {
	let asked = std::cell::RefCell::new(Vec::new());
	let stmt = statement("CREATE INDEX ON public.items USING hnsw (embedding vector_l2_ops)");
	let result = lower_pgsql_ddl_over(&stmt, &|table, column| {
		asked.borrow_mut().push((table.to_string(), column.to_string()));
		Some(8)
	});
	assert!(result.is_ok());
	assert_eq!(asked.into_inner(), [("items".to_string(), "embedding".to_string())]);
}

#[test]
fn an_index_of_another_access_method_takes_no_operator_class_or_parameter() {
	assert!(lower_ddl("CREATE INDEX ON t USING btree (a)", None).is_ok());
	for sql in [
		"CREATE INDEX ON t USING btree (a text_pattern_ops)",
		"CREATE INDEX ON t (a) WITH (fillfactor = 70)",
	] {
		assert!(matches!(lower_ddl(sql, None), Err(PlanError::UnsupportedStatement(_))), "{sql}");
	}
}

fn plan(sql: &str) -> Result<String, PlanError> {
	plan_over(sql, &crate::EmptyCatalog)
}

fn plan_over(sql: &str, catalog: &dyn crate::BinderCatalog) -> Result<String, PlanError> {
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = statement(sql) else {
		panic!("expected a select");
	};
	let policy = DialectPolicy::postgres();
	crate::bind_and_lower_pgsql(&select, catalog, &policy).map(|op| format!("{op:?}"))
}

fn searches(plan: &str) -> bool {
	plan.contains("NearestNeighbor")
}

#[test]
fn ordering_by_a_distance_with_a_literal_limit_asks_for_a_nearest_neighbour_search() {
	for (order, distance) in [
		("e <-> '[1,2,3]'", "Euclidean"),
		("e <=> '[1,2,3]'", "Cosine"),
		("e <#> '[1,2,3]'", "InnerProduct"),
		("e <+> '[1,2,3]'", "Manhattan"),
		("'[1,2,3]' <-> e", "Euclidean"),
		("l2_distance(e, $1)", "Euclidean"),
		("cosine_distance(e, $1)", "Cosine"),
		("l1_distance(e, $1::vector)", "Manhattan"),
		("e <-> $1", "Euclidean"),
	] {
		let plan = plan(&format!("SELECT id FROM items ORDER BY {order} LIMIT 5")).unwrap();
		assert!(plan.contains(&format!("K(5, {distance})")), "{order}: {plan}");
	}
}

#[test]
fn the_search_is_asked_of_an_hnsw_index_that_ranks_by_the_distance_of_the_order() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_hnsw_index("items", "e", VectorMetric::L2);
	catalog.add_hnsw_index("items", "f", VectorMetric::Cosine);
	catalog.add_hnsw_index("items", "g", VectorMetric::L2);
	catalog.add_hnsw_index("items", "g", VectorMetric::Cosine);
	for (order, search) in [
		("e <-> $1", "Approximate(5, 40)"),
		("l2_distance(e, $1)", "Approximate(5, 40)"),
		("e <=> $1", "K(5, Cosine)"),
		("e <#> $1", "K(5, InnerProduct)"),
		("f <=> $1", "Approximate(5, 40)"),
		("f <-> $1", "K(5, Euclidean)"),
		// Two indexes over the column: either could answer, so a scan does.
		("g <-> $1", "K(5, Euclidean)"),
		("g <=> $1", "K(5, Cosine)"),
	] {
		let plan =
			plan_over(&format!("SELECT id FROM items ORDER BY {order} LIMIT 5"), &catalog).unwrap();
		assert!(plan.contains(search), "{order}: {plan}");
	}
	let plan = plan_over("SELECT id FROM items ORDER BY e <-> $1 LIMIT 100", &catalog).unwrap();
	assert!(plan.contains("Approximate(100, 100)"), "{plan}");
	let other = plan_over("SELECT id FROM other ORDER BY e <-> $1 LIMIT 5", &catalog).unwrap();
	assert!(other.contains("K(5, Euclidean)"), "{other}");
}

#[test]
fn the_rows_that_have_no_distance_are_told_from_the_column() {
	let descending = plan("SELECT id FROM items ORDER BY e <-> $1 DESC LIMIT 5").unwrap();
	assert_eq!(descending.matches("vector::distance::euclidean").count(), 1, "{descending}");
	let ascending = plan("SELECT id FROM items ORDER BY e <-> $1 LIMIT 5").unwrap();
	assert_eq!(ascending.matches("vector::distance::euclidean").count(), 1, "{ascending}");
	assert!(ascending.contains("Field(\"e\")"), "{ascending}");
}

#[test]
fn the_search_covers_the_rows_the_offset_skips() {
	let plan = plan("SELECT id FROM items ORDER BY e <-> $1 LIMIT 5 OFFSET 10").unwrap();
	assert!(plan.contains("K(15, Euclidean)"), "{plan}");
}

#[test]
fn the_search_keeps_the_where_clause_and_the_order() {
	let plan = plan("SELECT id FROM items WHERE id > 1 ORDER BY e <-> $1 LIMIT 2").unwrap();
	assert!(plan.contains("K(2, Euclidean)"), "{plan}");
	assert!(plan.contains("Sort"), "{plan}");
	assert!(plan.contains("MoreThan"), "{plan}");
}

#[test]
fn a_select_that_is_not_a_search_of_the_table_is_not_lowered_to_one() {
	for sql in [
		"SELECT id FROM items ORDER BY e <-> $1",
		"SELECT id FROM items ORDER BY e <-> $1 LIMIT $2",
		"SELECT id FROM items ORDER BY e <-> $1 LIMIT 5 OFFSET $2",
		"SELECT id FROM items ORDER BY e <-> $1 DESC LIMIT 5",
		"SELECT id FROM items ORDER BY id, e <-> $1 LIMIT 5",
		"SELECT id FROM items ORDER BY e <-> $1, id LIMIT 5",
		"SELECT id FROM items ORDER BY e <-> $1 NULLS FIRST LIMIT 5",
		"SELECT id FROM items ORDER BY e <-> f LIMIT 5",
		"SELECT id FROM items ORDER BY e <-> (f + 1) LIMIT 5",
		"SELECT id FROM items ORDER BY inner_product(e, $1) LIMIT 5",
		"SELECT id FROM items ORDER BY e <-> $1 LIMIT 0",
		"SELECT DISTINCT id, e <-> $1 FROM items ORDER BY e <-> $1 LIMIT 5",
		"SELECT count(*) FROM items ORDER BY e <-> $1 LIMIT 5",
		"SELECT id FROM items GROUP BY id, e ORDER BY e <-> $1 LIMIT 5",
		"SELECT a.id FROM items a JOIN items b ON a.id = b.id ORDER BY a.e <-> $1 LIMIT 5",
		"SELECT id FROM (SELECT * FROM items) i ORDER BY e <-> $1 LIMIT 5",
		"SELECT id FROM items UNION ALL SELECT id FROM items ORDER BY id LIMIT 5",
	] {
		let plan = plan(sql).unwrap();
		assert!(!searches(&plan), "{sql}: {plan}");
	}
}

#[test]
fn a_distance_orders_rows_where_it_asks_for_no_search() {
	let plan = plan("SELECT id FROM items ORDER BY e <-> $1 DESC LIMIT 5").unwrap();
	assert!(plan.contains("vector::distance::euclidean"), "{plan}");
}

#[test]
fn the_distances_are_computed_by_the_engines_vector_functions() {
	for (expression, function) in [
		("e <-> $1", "vector::distance::euclidean"),
		("e <+> $1", "vector::distance::manhattan"),
		("e <=> $1", "vector::similarity::cosine"),
		("e <#> $1", "vector::dot"),
		("inner_product(e, $1)", "vector::dot"),
	] {
		let plan = plan(&format!("SELECT {expression} FROM items")).unwrap();
		assert!(plan.contains(function), "{expression}: {plan}");
	}
}

#[test]
fn the_distance_operators_are_not_defined_by_mysql() {
	let tokens = surrealdb_mysql::parse_statements("SELECT 1 <-> 2");
	let accepted = tokens.is_ok_and(|statements| {
		let surrealdb_mysql::ast::stmt::Statement::Select(select) = &statements[0] else {
			return false;
		};
		let policy = DialectPolicy::mysql();
		crate::bind_and_lower_mysql(select, &crate::EmptyCatalog, &policy)
			.is_ok_and(|op| format!("{op:?}").contains("vector::"))
	});
	assert!(!accepted);
}

#[test]
fn a_text_literal_is_read_as_a_vector_where_a_vector_is_taken() {
	let plan = plan("SELECT l2_distance(e, '[1,2,3]') FROM items").unwrap();
	assert!(plan.contains("Float(1.0)") && plan.contains("Float(3.0)"), "{plan}");
	assert!(!plan.contains("'[1,2,3]'"), "{plan}");
}

#[test]
fn a_cast_to_a_vector_is_read_when_the_statement_is_lowered() {
	let plan = plan("SELECT '[1,2,3]'::vector(3)").unwrap();
	assert!(plan.contains("Float(2.0)"), "{plan}");
	let error = self::plan("SELECT '[1,2]'::vector(3)").unwrap_err();
	assert_eq!(error, PlanError::DataException("expected 3 dimensions, not 2".into()));
	assert_eq!(error.pg_sqlstate(), "22000");
	let error = self::plan("SELECT '[a]'::vector").unwrap_err();
	assert!(matches!(error, PlanError::InvalidTextRepresentation(_)), "{error:?}");
	assert_eq!(error.pg_sqlstate(), "22P02");
	assert!(matches!(
		self::plan("SELECT '[1]'::vector(0)").unwrap_err(),
		PlanError::InvalidParameterValue(_)
	));
	assert!(matches!(
		self::plan("SELECT '[1]'::vector(1, 2)").unwrap_err(),
		PlanError::InvalidParameterValue(_)
	));
}

#[test]
fn a_schema_qualified_vector_cast_is_a_vector() {
	let plan = plan("SELECT '[1,2,3]'::public.vector").unwrap();
	assert!(plan.contains("Float(2.0)"), "{plan}");
}

#[test]
fn a_distance_over_a_null_is_null() {
	let plan = plan("SELECT l2_distance(e, NULL) FROM items").unwrap();
	assert!(plan.contains("Optional"), "{plan}");
}

#[test]
fn a_distance_function_takes_two_arguments() {
	let error = plan("SELECT l2_distance(e) FROM items").unwrap_err();
	assert!(matches!(error, PlanError::FunctionArgumentCountMismatch { .. }), "{error:?}");
	assert_eq!(error.pg_sqlstate(), "42883");
}

#[test]
fn a_parameter_of_a_vector_function_is_a_vector() {
	let Statement::Select(select) = statement("SELECT id FROM items ORDER BY e <=> $1 LIMIT $2")
	else {
		panic!("expected a select");
	};
	let policy = DialectPolicy::postgres();
	let bound = crate::binder::pgsql::bind_select(&select, &crate::EmptyCatalog, &policy, None)
		.expect("bind");
	let described = bound.describe_parameters(&crate::EmptyCatalog);
	assert_eq!(described.param_types, [SqlType::Vector, SqlType::Int8]);
}

#[test]
fn a_parameter_compared_with_a_vector_column_is_a_vector() {
	let catalog = MemoryCatalog::new().with_column_type("items", "e", SqlType::Vector);
	let Statement::Select(select) = statement("SELECT id FROM items WHERE e = $1") else {
		panic!("expected a select");
	};
	let policy = DialectPolicy::postgres();
	let bound = crate::binder::pgsql::bind_select(&select, &catalog, &policy, None).expect("bind");
	assert_eq!(bound.describe_parameters(&catalog).param_types, [SqlType::Vector]);
}

#[test]
fn a_vector_is_described_as_the_vector_type() {
	assert_eq!(SqlType::from_name("vector"), SqlType::Vector);
	assert_eq!(SqlType::from_name("vector(3)"), SqlType::Vector);
	assert_eq!(SqlType::Vector.to_pg_oid(), crate::descriptor::VECTOR_OID);
	assert_eq!(SqlType::from_pg_oid(crate::descriptor::VECTOR_OID), Some(SqlType::Vector));
	assert_eq!(
		SqlType::array_of(SqlType::Vector).map(|ty| ty.to_pg_oid()),
		Some(crate::descriptor::VECTOR_ARRAY_OID)
	);
	let select = "SELECT '[1,2]'::vector AS v, l2_distance('[1]', '[2]') AS d FROM items";
	let Statement::Select(select) = statement(select) else {
		panic!("expected a select");
	};
	let policy = DialectPolicy::postgres();
	let bound = crate::binder::pgsql::bind_select(&select, &crate::EmptyCatalog, &policy, None)
		.expect("bind");
	let described = bound.describe(&crate::EmptyCatalog, &policy);
	assert_eq!(described.columns[0].ty, SqlType::Vector);
	assert_eq!(described.columns[1].ty, SqlType::Float8);
}

#[test]
fn a_write_refuses_the_distance_operators() {
	for sql in [
		"DELETE FROM items WHERE e <-> '[1,2,3]' < 1",
		"UPDATE items SET n = 1 WHERE e <=> '[1,2,3]' < 1",
	] {
		let stmt = statement(sql);
		let error = if let Statement::Delete(delete) = &stmt {
			super::write::lower_pgsql_delete(delete, &crate::EmptyCatalog).unwrap_err()
		} else if let Statement::Update(update) = &stmt {
			super::write::lower_pgsql_update(update, &crate::EmptyCatalog).unwrap_err()
		} else {
			panic!("expected a write");
		};
		assert!(matches!(error, PlanError::UnsupportedExpression(_)), "{sql}: {error:?}");
		assert_eq!(error.pg_sqlstate(), "0A000");
	}
}

#[test]
fn a_text_written_to_a_vector_column_is_the_vector_it_spells() {
	let catalog = MemoryCatalog::new().with_vector("items", "e", 3);
	let stmt = statement("INSERT INTO items (id, e) VALUES (1, '[1,2,3]')");
	let Statement::Insert(insert) = &stmt else {
		panic!("expected an insert");
	};
	let written = super::write::lower_pgsql_insert(insert, &catalog).expect("lower");
	let text = format!("{written:?}");
	assert!(text.contains("Float(2.0)"), "{text}");
	assert!(!text.contains("[1,2,3]"), "{text}");

	let plain = super::write::lower_pgsql_insert(insert, &crate::EmptyCatalog).expect("lower");
	assert!(format!("{plain:?}").contains("[1,2,3]"));
}

#[test]
fn a_parameter_written_to_a_vector_column_may_be_text() {
	let catalog = MemoryCatalog::new().with_vector("items", "e", 3);
	let stmt = statement("INSERT INTO items (id, e) VALUES (1, $1)");
	let Statement::Insert(insert) = &stmt else {
		panic!("expected an insert");
	};
	let written = super::write::lower_pgsql_insert(insert, &catalog).expect("lower");
	let text = format!("{written:?}");
	assert!(text.contains("string::split"), "{text}");
}

use surrealdb_pgsql::ast::stmt::Statement;

fn column_names(sql: &str) -> Vec<String> {
	let Statement::Select(select) = statement(sql) else {
		panic!("expected a select");
	};
	let policy = DialectPolicy::postgres();
	let bound = crate::binder::pgsql::bind_select(&select, &crate::EmptyCatalog, &policy, None)
		.expect("bind");
	bound.describe(&crate::EmptyCatalog, &policy).columns.into_iter().map(|c| c.name).collect()
}

#[test]
fn an_operator_names_its_column_as_postgresql_does_and_a_function_by_its_name() {
	assert_eq!(column_names("SELECT e <-> '[1,2,3]' FROM items"), ["?column?"]);
	assert_eq!(
		column_names("SELECT e <=> '[1,2,3]', e <#> '[1,2,3]' FROM items"),
		["?column?", "?column?"]
	);
	assert_eq!(column_names("SELECT l2_distance(e, '[1,2,3]') FROM items"), ["l2_distance"]);
	assert_eq!(column_names("SELECT e <-> '[1,2,3]' AS d FROM items"), ["d"]);
}

#[test]
fn text_selected_into_a_vector_column_is_the_vector_it_spells() {
	let catalog = MemoryCatalog::new().with_vector("items", "e", 3);
	let stmt = statement("INSERT INTO items (id, e) SELECT 1, '[1,2,3]'");
	let Statement::Insert(insert) = &stmt else {
		panic!("expected an insert");
	};
	let text = format!("{:?}", super::write::lower_pgsql_insert(insert, &catalog).expect("lower"));
	assert!(text.contains("Float(2.0)"), "{text}");
	let wrong = statement("INSERT INTO items (id, e) SELECT 1, '[1,2]'");
	let Statement::Insert(insert) = &wrong else {
		panic!("expected an insert");
	};
	assert!(matches!(
		super::write::lower_pgsql_insert(insert, &catalog),
		Err(PlanError::DataException(_))
	));
}

#[test]
fn a_computed_text_written_to_a_vector_column_is_read_once_as_a_vector() {
	let catalog = MemoryCatalog::new().with_vector("items", "e", 3);
	let stmt = statement("INSERT INTO items (id, e) VALUES (1, concat('[1,', '2,3]'))");
	let Statement::Insert(insert) = &stmt else {
		panic!("expected an insert");
	};
	let text = format!("{:?}", super::write::lower_pgsql_insert(insert, &catalog).expect("lower"));
	assert!(text.contains("string::split"), "{text}");
}
