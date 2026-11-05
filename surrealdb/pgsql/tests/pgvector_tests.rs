//! Grammar the pgvector extension uses: `CREATE EXTENSION`, operator classes and storage
//! parameters on `CREATE INDEX`, and the distance operators.

use surrealdb_pgsql::ast::expr::Expr;
use surrealdb_pgsql::ast::op::BinaryOperator;
use surrealdb_pgsql::ast::stmt::Statement;
use surrealdb_pgsql::printer::ToPgSql;
use surrealdb_pgsql::{parse_statement, parse_statements};

#[test]
fn create_extension_parses_name_and_options() {
	let Statement::CreateExtension(ext) = parse_statement(
		"CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public VERSION '0.8.0' CASCADE",
	)
	.expect("parse") else {
		panic!("expected CreateExtension");
	};
	assert!(ext.if_not_exists);
	assert_eq!(ext.name, "vector");
	assert_eq!(ext.schema.as_deref(), Some("public"));
	assert_eq!(ext.version.as_deref(), Some("0.8.0"));
	assert!(ext.cascade);
}

#[test]
fn create_extension_round_trips() {
	for sql in [
		"CREATE EXTENSION vector",
		"CREATE EXTENSION IF NOT EXISTS vector SCHEMA public VERSION '0.8.0' CASCADE",
	] {
		let stmt = parse_statement(sql).expect("parse");
		assert_eq!(stmt.to_pgsql_string(), sql);
	}
}

#[test]
fn create_index_parses_opclass_and_options() {
	let sql = "CREATE INDEX items_embedding_idx ON items USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64)";
	let Statement::CreateIndex(idx) = parse_statement(sql).expect("parse") else {
		panic!("expected CreateIndex");
	};
	assert_eq!(idx.method.as_deref(), Some("hnsw"));
	assert_eq!(idx.columns.len(), 1);
	assert_eq!(
		idx.columns[0].opclass.as_ref().map(|name| name.parts.clone()),
		Some(vec!["vector_l2_ops".to_string()])
	);
	let names: Vec<_> = idx.options.iter().map(|option| option.name.as_str()).collect();
	assert_eq!(names, ["m", "ef_construction"]);
	assert!(idx.options.iter().all(|option| option.value.is_some()));
}

#[test]
fn create_index_with_opclass_and_options_round_trips() {
	let sql = "CREATE INDEX ON items USING hnsw (embedding public.vector_cosine_ops) WITH (m = 8)";
	let stmt = parse_statement(sql).expect("parse");
	let printed = stmt.to_pgsql_string();
	assert_eq!(parse_statement(&printed).expect("reparse"), stmt);
}

#[test]
fn create_index_without_opclass_or_options_is_unchanged() {
	let Statement::CreateIndex(idx) =
		parse_statement("CREATE INDEX ON t (a DESC NULLS LAST, b)").expect("parse")
	else {
		panic!("expected CreateIndex");
	};
	assert!(idx.options.is_empty());
	assert!(idx.columns.iter().all(|column| column.opclass.is_none()));
}

#[test]
fn create_index_rejects_a_dangling_with() {
	assert!(parse_statements("CREATE INDEX ON t (a) WITH").is_err());
	assert!(parse_statements("CREATE INDEX ON t (a) WITH (m =)").is_err());
}

#[test]
fn distance_operators_parse_as_one_operator() {
	for op in ["<->", "<=>", "<#>", "<+>"] {
		let sql = format!("SELECT embedding {op} '[1,2,3]' FROM items");
		let Statement::Select(select) = parse_statement(&sql).expect("parse") else {
			panic!("expected Select");
		};
		let Expr::BinaryOp {
			op: BinaryOperator::Custom(found),
			..
		} = &select.targets[0].expr
		else {
			panic!("expected a custom operator, got {:?}", select.targets[0].expr);
		};
		assert_eq!(found, op);
	}
}

#[test]
fn distance_operators_bind_tighter_than_comparison_and_looser_than_addition() {
	let Statement::Select(select) =
		parse_statement("SELECT * FROM items WHERE a <-> b + c < 5").expect("parse")
	else {
		panic!("expected Select");
	};
	let Some(Expr::BinaryOp {
		op: BinaryOperator::Lt,
		left,
		..
	}) = &select.where_clause
	else {
		panic!("expected `<` at the root, got {:?}", select.where_clause);
	};
	let Expr::BinaryOp {
		op: BinaryOperator::Custom(distance),
		right,
		..
	} = left.as_ref()
	else {
		panic!("expected the distance operator under `<`, got {left:?}");
	};
	assert_eq!(distance, "<->");
	assert!(matches!(
		right.as_ref(),
		Expr::BinaryOp {
			op: BinaryOperator::Add,
			..
		}
	));
}

#[test]
fn order_by_distance_operator_parses() {
	let Statement::Select(select) =
		parse_statement("SELECT id FROM items ORDER BY embedding <=> $1 LIMIT 5").expect("parse")
	else {
		panic!("expected Select");
	};
	assert_eq!(select.order_by.len(), 1);
	assert!(select.limit.is_some());
}
