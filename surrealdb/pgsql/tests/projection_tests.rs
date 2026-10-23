//! Integration tests for AST structural projection $\pi$ (Phase 6).

mod common;

use common::projection::ToPgQueryJson;
use surrealdb_pgsql::parse_statement;
use surrealdb_sqlfront_spec::ConformanceRatchet;

#[test]
fn test_projection_simple_select() {
	let stmt = parse_statement("SELECT 1;").expect("parse SELECT 1");

	let json = stmt.to_pg_query_json();
	let raw_stmt = json.get("RawStmt").expect("has RawStmt");
	let stmt_obj = raw_stmt.get("stmt").expect("has stmt");
	let select_stmt = stmt_obj.get("SelectStmt").expect("has SelectStmt");

	let target_list =
		select_stmt.get("targetList").and_then(|v| v.as_array()).expect("targetList array");
	assert_eq!(target_list.len(), 1);

	let res_target = target_list[0].get("ResTarget").expect("has ResTarget");
	let val = res_target.get("val").expect("has val");
	let a_const = val.get("A_Const").expect("has A_Const");
	let ival = a_const
		.get("ival")
		.and_then(|v| v.get("ival"))
		.and_then(|v| v.as_i64())
		.expect("ival integer");
	assert_eq!(ival, 1);
}

#[test]
fn test_projection_select_with_table_where_order() {
	let sql = "SELECT id, name FROM users AS u WHERE active = true ORDER BY id DESC;";
	let stmt = parse_statement(sql).expect("parse complex SELECT");

	let json = stmt.to_pg_query_json();
	let select_stmt = json["RawStmt"]["stmt"]["SelectStmt"].clone();

	// Check targetList (id, name)
	let targets = select_stmt["targetList"].as_array().expect("targets array");
	assert_eq!(targets.len(), 2);
	assert_eq!(targets[0]["ResTarget"]["val"]["ColumnRef"]["fields"][0]["String"]["sval"], "id");
	assert_eq!(targets[1]["ResTarget"]["val"]["ColumnRef"]["fields"][0]["String"]["sval"], "name");

	// Check fromClause (users AS u)
	let froms = select_stmt["fromClause"].as_array().expect("fromClause array");
	assert_eq!(froms.len(), 1);
	let range_var = &froms[0]["RangeVar"];
	assert_eq!(range_var["relname"], "users");
	assert_eq!(range_var["alias"]["Alias"]["aliasname"], "u");

	// Check whereClause (active = true)
	let where_clause = &select_stmt["whereClause"]["A_Expr"];
	assert_eq!(where_clause["kind"], "AEXPR_OP");
	assert_eq!(where_clause["name"][0]["String"]["sval"], "=");
	assert_eq!(where_clause["lexpr"]["ColumnRef"]["fields"][0]["String"]["sval"], "active");
	assert_eq!(where_clause["rexpr"]["A_Const"]["boolval"]["boolval"], true);

	// Check sortClause (id DESC)
	let sorts = select_stmt["sortClause"].as_array().expect("sortClause array");
	assert_eq!(sorts.len(), 1);
	assert_eq!(sorts[0]["SortBy"]["sortby_dir"], "SORTBY_DESC");
	assert_eq!(sorts[0]["SortBy"]["node"]["ColumnRef"]["fields"][0]["String"]["sval"], "id");
}

#[test]
fn test_projection_insert() {
	let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice');";
	let stmt = parse_statement(sql).expect("parse INSERT");

	let json = stmt.to_pg_query_json();
	let insert_stmt = json["RawStmt"]["stmt"]["InsertStmt"].clone();

	assert_eq!(insert_stmt["relation"]["RangeVar"]["relname"], "users");

	let cols = insert_stmt["cols"].as_array().expect("cols");
	assert_eq!(cols.len(), 2);
	assert_eq!(cols[0]["ResTarget"]["name"], "id");
	assert_eq!(cols[1]["ResTarget"]["name"], "name");

	let rows =
		insert_stmt["selectStmt"]["SelectStmt"]["valuesLists"].as_array().expect("valuesLists");
	assert_eq!(rows.len(), 1);
	let row0 = rows[0].as_array().expect("row0");
	assert_eq!(row0[0]["A_Const"]["ival"]["ival"], 1);
	assert_eq!(row0[1]["A_Const"]["sval"]["sval"], "Alice");
}

#[test]
fn test_projection_update() {
	let sql = "UPDATE users SET name = 'Bob' WHERE id = 1;";
	let stmt = parse_statement(sql).expect("parse UPDATE");

	let json = stmt.to_pg_query_json();
	let update_stmt = json["RawStmt"]["stmt"]["UpdateStmt"].clone();

	assert_eq!(update_stmt["relation"]["RangeVar"]["relname"], "users");

	let targets = update_stmt["targetList"].as_array().expect("targetList");
	assert_eq!(targets.len(), 1);
	assert_eq!(targets[0]["ResTarget"]["name"], "name");
	assert_eq!(targets[0]["ResTarget"]["val"]["A_Const"]["sval"]["sval"], "Bob");

	let where_clause = &update_stmt["whereClause"]["A_Expr"];
	assert_eq!(where_clause["name"][0]["String"]["sval"], "=");
	assert_eq!(where_clause["lexpr"]["ColumnRef"]["fields"][0]["String"]["sval"], "id");
	assert_eq!(where_clause["rexpr"]["A_Const"]["ival"]["ival"], 1);
}

#[test]
fn test_projection_delete() {
	let sql = "DELETE FROM users WHERE id = 1;";
	let stmt = parse_statement(sql).expect("parse DELETE");

	let json = stmt.to_pg_query_json();
	let del_stmt = json["RawStmt"]["stmt"]["DeleteStmt"].clone();

	assert_eq!(del_stmt["relation"]["RangeVar"]["relname"], "users");
	assert_eq!(del_stmt["whereClause"]["A_Expr"]["name"][0]["String"]["sval"], "=");
}

#[test]
fn test_projection_create_table() {
	let sql = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL);";
	let stmt = parse_statement(sql).expect("parse CREATE TABLE");

	let json = stmt.to_pg_query_json();
	let create_stmt = json["RawStmt"]["stmt"]["CreateStmt"].clone();

	assert_eq!(create_stmt["relation"]["RangeVar"]["relname"], "users");
	assert_eq!(create_stmt["if_not_exists"], false);

	let elts = create_stmt["tableElts"].as_array().expect("tableElts");
	assert_eq!(elts.len(), 2);

	let col0 = &elts[0]["ColumnDef"];
	assert_eq!(col0["colname"], "id");
	assert_eq!(col0["typeName"]["TypeName"]["names"][0]["String"]["sval"], "int");
	assert_eq!(col0["constraints"][0]["Constraint"]["contype"], "CONSTR_PRIMARY");

	let col1 = &elts[1]["ColumnDef"];
	assert_eq!(col1["colname"], "name");
	assert_eq!(col1["typeName"]["TypeName"]["names"][0]["String"]["sval"], "text");
	assert_eq!(col1["constraints"][0]["Constraint"]["contype"], "CONSTR_NOTNULL");
}

#[test]
fn test_projection_transactions() {
	let tx_sqls = ["BEGIN;", "COMMIT;", "ROLLBACK;", "SAVEPOINT sp1;"];
	let expected_kinds = [
		("TRANS_STMT_BEGIN", None),
		("TRANS_STMT_COMMIT", None),
		("TRANS_STMT_ROLLBACK", None),
		("TRANS_STMT_SAVEPOINT", Some("sp1")),
	];

	for (sql, (exp_kind, exp_sp)) in tx_sqls.iter().zip(expected_kinds.iter()) {
		let stmt = parse_statement(sql).expect("parse tx statement");
		let json = stmt.to_pg_query_json();
		let tx_node = &json["RawStmt"]["stmt"]["TransactionStmt"];
		assert_eq!(tx_node["kind"], *exp_kind);
		if let Some(sp) = exp_sp {
			assert_eq!(tx_node["savepoint_name"], *sp);
		}
	}
}

#[test]
fn test_projection_cte() {
	let sql = "WITH cte AS (SELECT 1 AS val) SELECT val FROM cte;";
	let stmt = parse_statement(sql).expect("parse CTE");

	let json = stmt.to_pg_query_json();
	let select_stmt = &json["RawStmt"]["stmt"]["SelectStmt"];

	let with_clause = &select_stmt["withClause"]["WithClause"];
	assert_eq!(with_clause["recursive"], false);

	let ctes = with_clause["ctes"].as_array().expect("ctes array");
	assert_eq!(ctes.len(), 1);
	assert_eq!(ctes[0]["CommonTableExpr"]["ctename"], "cte");
	assert_eq!(ctes[0]["CommonTableExpr"]["ctematerialized"], "CTEMaterializeDefault");
}

#[test]
fn test_projection_locking_and_window() {
	let sql = "SELECT * FROM users FOR UPDATE NOWAIT;";
	let stmt = parse_statement(sql).expect("parse FOR UPDATE NOWAIT");

	let json = stmt.to_pg_query_json();
	let select_stmt = &json["RawStmt"]["stmt"]["SelectStmt"];

	let locks = select_stmt["lockingClause"].as_array().expect("lockingClause");
	assert_eq!(locks.len(), 1);
	assert_eq!(locks[0]["LockingClause"]["strength"], "LCS_FORUPDATE");
	assert_eq!(locks[0]["LockingClause"]["waitPolicy"], "LockWaitError");
}

#[test]
fn test_projection_ddl_and_prepared() {
	let sqls = [
		"CREATE INDEX idx_users_name ON users (name ASC);",
		"PREPARE get_user (INT) AS SELECT * FROM users WHERE id = $1;",
		"EXECUTE get_user (42);",
		"DEALLOCATE get_user;",
	];

	let stmt0 = parse_statement(sqls[0]).expect("parse CREATE INDEX");
	let json0 = stmt0.to_pg_query_json();
	assert_eq!(json0["RawStmt"]["stmt"]["IndexStmt"]["idxname"], "idx_users_name");

	let stmt1 = parse_statement(sqls[1]).expect("parse PREPARE");
	let json1 = stmt1.to_pg_query_json();
	assert_eq!(json1["RawStmt"]["stmt"]["PrepareStmt"]["name"], "get_user");

	let stmt2 = parse_statement(sqls[2]).expect("parse EXECUTE");
	let json2 = stmt2.to_pg_query_json();
	assert_eq!(json2["RawStmt"]["stmt"]["ExecuteStmt"]["name"], "get_user");

	let stmt3 = parse_statement(sqls[3]).expect("parse DEALLOCATE");
	let json3 = stmt3.to_pg_query_json();
	assert_eq!(json3["RawStmt"]["stmt"]["DeallocateStmt"]["name"], "get_user");
}

#[test]
fn test_conformance_ratchet_evaluation() {
	let mut ratchet = ConformanceRatchet::new();

	// Test a batch of queries with ratchet tracking
	let statements = [
		"SELECT 1;",
		"SELECT 2 + 2;",
		"SELECT id FROM users;",
		"INSERT INTO t VALUES (1);",
		"UPDATE t SET a = 1;",
		"DELETE FROM t;",
		"BEGIN;",
		"COMMIT;",
	];

	for sql in statements {
		match parse_statement(sql) {
			Ok(stmt) => {
				ratchet.record_parse_success();
				let json = stmt.to_pg_query_json();
				if json.get("RawStmt").is_some() {
					ratchet.record_projection_success();
				} else {
					ratchet.record_projection_mismatch();
				}
			}
			Err(_) => {
				ratchet.record_parse_failure();
			}
		}
	}

	assert_eq!(ratchet.total_evaluated, 8);
	assert_eq!(ratchet.parse_success, 8);
	assert_eq!(ratchet.parse_failure, 0);
	assert_eq!(ratchet.projection_success, 8);
	assert_eq!(ratchet.projection_mismatch, 0);
	assert_eq!(ratchet.parse_rate(), 100.0);
	assert_eq!(ratchet.projection_rate(), 100.0);
	assert!(ratchet.assert_floor(8).is_ok());
	assert!(ratchet.assert_floor(9).is_err());
}

#[test]
fn scalar_function_range_projection_matches_pg_query() {
	let statement = parse_statement("SELECT * FROM abs(2) WITH ORDINALITY AS a(n, ord)")
		.expect("function table");
	let projected = statement.to_pg_query_json();
	assert_eq!(
		projected["RawStmt"]["stmt"]["SelectStmt"]["fromClause"][0],
		serde_json::json!({
			"RangeFunction": {
				"ordinality": true,
				"functions": [{"List": {"items": [{"FuncCall": {
					"funcname": [{"String": {"sval": "abs"}}],
					"args": [{"A_Const": {"ival": {"ival": 2}}}],
					"funcformat": "COERCE_EXPLICIT_CALL"
				}}, {}]}}],
				"alias": {"aliasname": "a", "colnames": [{"String": {"sval": "n"}}, {"String": {"sval": "ord"}}]}
			}
		})
	);
}
