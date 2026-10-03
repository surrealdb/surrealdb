//! Integration tests for SQL:2023 Part 16 (SQL/PGQ) and SQL/JSON parity.

use surrealdb_pgsql::ast::graph::GraphElementPattern;
use surrealdb_pgsql::ast::stmt::{Statement, TableRef};
use surrealdb_pgsql::printer::ToPgSql;
use surrealdb_pgsql::{parse_statement, translate_sql};

fn assert_roundtrip(sql: &str) {
	let mut ast = parse_statement(sql).unwrap_or_else(|e| panic!("Failed to parse '{sql}': {e}"));
	ast.normalize_spans();
	let printed = ast.to_pgsql_string();
	let mut ast2 = parse_statement(&printed)
		.unwrap_or_else(|e| panic!("Failed to reparse printed '{printed}': {e}"));
	ast2.normalize_spans();
	assert_eq!(ast, ast2, "Roundtrip failed for:\nOriginal: {sql}\nPrinted:  {printed}");
}

#[test]
fn test_create_property_graph_simple() {
	let sql = "CREATE PROPERTY GRAPH social_network VERTEX TABLES ( users )";
	let ast = parse_statement(sql).unwrap();
	match ast {
		Statement::CreatePropertyGraph(stmt) => {
			assert_eq!(stmt.name.to_string(), "social_network");
			assert!(!stmt.if_not_exists);
			assert_eq!(stmt.vertex_tables.len(), 1);
			assert_eq!(stmt.vertex_tables[0].table_name.to_string(), "users");
			assert!(stmt.edge_tables.is_empty());
		}
		_ => panic!("Expected CreatePropertyGraph"),
	}
	assert_roundtrip(sql);
}

#[test]
fn test_create_property_graph_full() {
	let sql = "CREATE PROPERTY GRAPH IF NOT EXISTS social_graph VERTEX TABLES ( users KEY ( id ) LABEL person PROPERTIES ( name , email ) , companies KEY ( id ) LABEL org ) EDGE TABLES ( knows KEY ( id ) SOURCE KEY ( user_id ) REFERENCES users ( id ) TARGET KEY ( friend_id ) REFERENCES users ( id ) LABEL friend PROPERTIES ( since ) )";
	let ast = parse_statement(sql).unwrap();
	match ast {
		Statement::CreatePropertyGraph(stmt) => {
			assert_eq!(stmt.name.to_string(), "social_graph");
			assert!(stmt.if_not_exists);
			assert_eq!(stmt.vertex_tables.len(), 2);
			assert_eq!(stmt.vertex_tables[0].table_name.to_string(), "users");
			assert_eq!(stmt.vertex_tables[0].key_columns[0].as_str(), "id");
			assert_eq!(stmt.vertex_tables[0].label.as_deref(), Some("person"));
			assert_eq!(stmt.vertex_tables[0].properties[0].as_str(), "name");
			assert_eq!(stmt.vertex_tables[0].properties[1].as_str(), "email");

			assert_eq!(stmt.edge_tables.len(), 1);
			assert_eq!(stmt.edge_tables[0].table_name.to_string(), "knows");
			assert_eq!(stmt.edge_tables[0].source_table.to_string(), "users");
			assert_eq!(stmt.edge_tables[0].source_key[0].as_str(), "user_id");
			assert_eq!(stmt.edge_tables[0].source_references[0].as_str(), "id");
			assert_eq!(stmt.edge_tables[0].target_table.to_string(), "users");
			assert_eq!(stmt.edge_tables[0].target_key[0].as_str(), "friend_id");
			assert_eq!(stmt.edge_tables[0].target_references[0].as_str(), "id");
			assert_eq!(stmt.edge_tables[0].label.as_deref(), Some("friend"));
			assert_eq!(stmt.edge_tables[0].properties[0].as_str(), "since");
		}
		_ => panic!("Expected CreatePropertyGraph"),
	}
	assert_roundtrip(sql);
}

#[test]
fn test_drop_property_graph() {
	assert_roundtrip("DROP PROPERTY GRAPH social_graph");
	assert_roundtrip("DROP PROPERTY GRAPH IF EXISTS social_graph CASCADE");
}

#[test]
fn test_graph_table_simple() {
	let sql = "SELECT * FROM GRAPH_TABLE ( social_network , MATCH ( a : Person ) -[ e : knows ]-> ( b : Person ) COLUMNS ( a . name AS person1 , b . name AS person2 ) ) AS g";
	let ast = parse_statement(sql).unwrap();
	match ast {
		Statement::Select(select) => match &select.from[0] {
			TableRef::GraphTable {
				graph_name,
				pattern,
				columns,
				alias,
				..
			} => {
				assert_eq!(graph_name.to_string(), "social_network");
				assert_eq!(alias.as_ref().unwrap().name, "g");
				assert_eq!(pattern.paths.len(), 1);
				assert_eq!(pattern.paths[0].elements.len(), 3);
				assert_eq!(columns.len(), 2);
				assert_eq!(columns[0].alias.as_deref(), Some("person1"));
				assert_eq!(columns[1].alias.as_deref(), Some("person2"));
			}
			_ => panic!("Expected GraphTable"),
		},
		_ => panic!("Expected Select"),
	}
	assert_roundtrip(sql);
}

#[test]
fn test_graph_table_edge_directions_and_quantifiers() {
	// Right arrow
	assert_roundtrip("SELECT * FROM GRAPH_TABLE ( g , MATCH ( a ) -> ( b ) COLUMNS ( a . id ) )");
	// Left arrow
	assert_roundtrip("SELECT * FROM GRAPH_TABLE ( g , MATCH ( a ) <- ( b ) COLUMNS ( a . id ) )");
	// Undirected / Any
	assert_roundtrip("SELECT * FROM GRAPH_TABLE ( g , MATCH ( a ) - ( b ) COLUMNS ( a . id ) )");
	// Bidirectional
	assert_roundtrip("SELECT * FROM GRAPH_TABLE ( g , MATCH ( a ) <-> ( b ) COLUMNS ( a . id ) )");
	// Quantifiers: *
	assert_roundtrip(
		"SELECT * FROM GRAPH_TABLE ( g , MATCH ( a ) -[ : knows * ]-> ( b ) COLUMNS ( a . id ) )",
	);
	// Quantifiers: +
	assert_roundtrip(
		"SELECT * FROM GRAPH_TABLE ( g , MATCH ( a ) -[ : knows + ]-> ( b ) COLUMNS ( a . id ) )",
	);
	// Quantifiers: ?
	assert_roundtrip(
		"SELECT * FROM GRAPH_TABLE ( g , MATCH ( a ) -[ : knows ? ]-> ( b ) COLUMNS ( a . id ) )",
	);
	// Quantifiers: exact {3}
	assert_roundtrip(
		"SELECT * FROM GRAPH_TABLE ( g , MATCH ( a ) -[ : knows { 3 } ]-> ( b ) COLUMNS ( a . id ) )",
	);
	// Quantifiers: range {1, 5}
	assert_roundtrip(
		"SELECT * FROM GRAPH_TABLE ( g , MATCH ( a ) -[ : knows { 1 , 5 } ]-> ( b ) COLUMNS ( a . id ) )",
	);
}

#[test]
fn test_graph_table_path_variables_and_filters() {
	let sql = "SELECT * FROM GRAPH_TABLE ( g , MATCH p = ( a : Person WHERE a . age > 21 ) -[ e : knows WHERE e . weight > 0.5 ]-> ( b : Person ) WHERE a . id != b . id COLUMNS ( a . name AS src , b . name AS dst ) )";
	let ast = parse_statement(sql).unwrap();
	match ast {
		Statement::Select(select) => match &select.from[0] {
			TableRef::GraphTable {
				pattern,
				..
			} => {
				assert_eq!(pattern.paths[0].path_variable.as_deref(), Some("p"));
				assert!(pattern.where_clause.is_some());
				match &pattern.paths[0].elements[0] {
					GraphElementPattern::Vertex(v) => {
						assert_eq!(v.variable.as_deref(), Some("a"));
						assert_eq!(v.labels, vec!["person"]);
						assert!(v.where_clause.is_some());
					}
					_ => panic!("Expected vertex"),
				}
				match &pattern.paths[0].elements[1] {
					GraphElementPattern::Edge(e) => {
						assert_eq!(e.variable.as_deref(), Some("e"));
						assert_eq!(e.labels, vec!["knows"]);
						assert!(e.where_clause.is_some());
					}
					_ => panic!("Expected edge"),
				}
			}
			_ => panic!("Expected GraphTable"),
		},
		_ => panic!("Expected Select"),
	}
	assert_roundtrip(sql);
}

#[test]
fn test_json_table() {
	let sql = "SELECT * FROM JSON_TABLE ( doc , '$.items[*]' COLUMNS ( id INT PATH '$.id' , name TEXT PATH '$.name' ) ) AS jt";
	let ast = parse_statement(sql).unwrap();
	match ast {
		Statement::Select(select) => match &select.from[0] {
			TableRef::JsonTable(jt) => {
				assert_eq!(jt.path, "$.items[*]");
				assert_eq!(jt.columns.len(), 2);
				assert_eq!(jt.columns[0].name, "id");
				assert_eq!(jt.columns[0].data_type.name.to_string(), "int");
				assert_eq!(jt.columns[0].path.as_deref(), Some("$.id"));
				assert_eq!(jt.columns[1].name, "name");
				assert_eq!(jt.columns[1].data_type.name.to_string(), "text");
				assert_eq!(jt.columns[1].path.as_deref(), Some("$.name"));
				assert_eq!(jt.alias.as_ref().unwrap().name, "jt");
			}
			_ => panic!("Expected JsonTable"),
		},
		_ => panic!("Expected Select"),
	}
	assert_roundtrip(sql);
}

#[test]
fn test_sql_json_expressions() {
	assert_roundtrip("SELECT JSON_OBJECT ( 'id' : 1 , 'name' : 'Alice' )");
	assert_roundtrip("SELECT JSON_ARRAY ( 1 , 2 , 'three' )");
	assert_roundtrip("SELECT JSON_EXISTS ( doc , '$.address' ) FROM t");
	assert_roundtrip("SELECT JSON_VALUE ( doc , '$.name' ) FROM t");
	assert_roundtrip("SELECT JSON_VALUE ( doc , '$.age' RETURNING INT ) FROM t");
	assert_roundtrip("SELECT JSON_QUERY ( doc , '$.address' ) FROM t");
}

#[test]
fn test_is_bound_predicate() {
	let sql = "SELECT * FROM t WHERE a IS BOUND AND b IS NOT BOUND";
	assert_roundtrip(sql);
}

#[test]
fn test_translate_sql2023_to_surrealql() {
	// CREATE PROPERTY GRAPH
	let translated = translate_sql("CREATE PROPERTY GRAPH g VERTEX TABLES ( users ) EDGE TABLES ( knows SOURCE KEY ( uid ) REFERENCES users ( id ) TARGET KEY ( fid ) REFERENCES users ( id ) )").unwrap();
	assert!(translated.contains("DEFINE TABLE users TYPE NORMAL;"));
	assert!(translated.contains("DEFINE TABLE knows TYPE RELATION IN users OUT users;"));

	// DROP PROPERTY GRAPH
	let translated = translate_sql("DROP PROPERTY GRAPH g").unwrap();
	assert!(translated.contains("-- DROP PROPERTY GRAPH g;"));

	// GRAPH_TABLE
	let translated = translate_sql("SELECT * FROM GRAPH_TABLE ( g , MATCH ( a ) -[ e : knows ]-> ( b ) COLUMNS ( a . name AS src , b . name AS dst ) ) AS res").unwrap();
	assert!(translated.contains("SELECT a.name AS src, b.name AS dst FROM a->knows->b"));

	// JSON_TABLE
	let translated = translate_sql("SELECT * FROM JSON_TABLE ( doc , '$.items' COLUMNS ( id INT PATH '$.id' , name TEXT PATH '$.name' ) ) AS jt").unwrap();
	assert!(translated.contains("SELECT id AS id, name AS name FROM doc"));

	// SQL/JSON expressions
	let translated = translate_sql("SELECT JSON_OBJECT ( 'a' : 1 )").unwrap();
	assert!(translated.contains("{'a': 1}"));

	let translated = translate_sql("SELECT JSON_ARRAY ( 1 , 2 )").unwrap();
	assert!(translated.contains("[1, 2]"));

	let translated = translate_sql("SELECT JSON_EXISTS ( doc , '$.user' )").unwrap();
	assert!(translated.contains("(doc.user != NONE)"));

	let translated = translate_sql("SELECT JSON_VALUE ( doc , '$.name' )").unwrap();
	assert!(translated.contains("doc.name"));

	let translated = translate_sql("SELECT JSON_QUERY ( doc , '$.data' )").unwrap();
	assert!(translated.contains("doc.data"));

	// IS BOUND
	let translated = translate_sql("SELECT * FROM t WHERE x IS BOUND AND y IS NOT BOUND").unwrap();
	assert!(translated.contains("(x IS NOT NONE)"));
	assert!(translated.contains("(y IS NONE)"));
}
