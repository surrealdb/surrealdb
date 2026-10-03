//! Unit and integration tests for PostgreSQL statement parser.

use surrealdb_pgsql::ast::stmt::{
	DistinctClause, DropObjectType, JoinCondition, JoinType, NullsOrder, OnConflictAction,
	OrderDirection, Statement, TableConstraint, TableRef, TransactionStmt,
};
use surrealdb_pgsql::{parse_statement, parse_statements};

#[test]
fn test_parse_select_comprehensive() {
	let sql = "SELECT id, name AS user_name, age FROM users WHERE age >= 18 GROUP BY id, name HAVING count(*) > 1 ORDER BY age DESC NULLS LAST LIMIT 10 OFFSET 5";
	let stmt = parse_statement(sql).expect("parse select");

	match stmt {
		Statement::Select(select) => {
			assert_eq!(select.targets.len(), 3);
			assert_eq!(select.targets[1].alias.as_deref(), Some("user_name"));
			assert!(select.from.len() == 1);
			assert!(select.where_clause.is_some());
			assert_eq!(select.group_by.len(), 2);
			assert!(select.having.is_some());
			assert_eq!(select.order_by.len(), 1);
			assert_eq!(select.order_by[0].direction, Some(OrderDirection::Desc));
			assert_eq!(select.order_by[0].nulls, Some(NullsOrder::Last));
			assert!(select.limit.is_some());
			assert!(select.offset.is_some());
		}
		other => panic!("expected Select statement, got {:?}", other),
	}
}

#[test]
fn test_parse_select_distinct_on() {
	let sql = "SELECT DISTINCT ON (dept) dept, salary FROM emp";
	let stmt = parse_statement(sql).expect("parse distinct on");

	match stmt {
		Statement::Select(select) => match select.distinct {
			Some(DistinctClause::On(exprs)) => {
				assert_eq!(exprs.len(), 1);
			}
			other => panic!("expected Distinct ON, got {:?}", other),
		},
		other => panic!("expected Select statement, got {:?}", other),
	}
}

#[test]
fn test_parse_select_joins() {
	let sql = "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id LEFT JOIN t3 USING (dept_id)";
	let stmt = parse_statement(sql).expect("parse joins");

	match stmt {
		Statement::Select(select) => {
			assert_eq!(select.from.len(), 1);
			match &select.from[0] {
				TableRef::Join {
					join_type: JoinType::Left,
					condition: Some(JoinCondition::Using(cols)),
					left,
					..
				} => {
					assert_eq!(cols, &vec!["dept_id"]);
					assert!(matches!(
						**left,
						TableRef::Join {
							join_type: JoinType::Inner,
							..
						}
					));
				}
				other => panic!("expected left join, got {:?}", other),
			}
		}
		other => panic!("expected Select statement, got {:?}", other),
	}
}

#[test]
fn test_parse_select_subquery() {
	let sql = "SELECT * FROM (SELECT id FROM users) AS u";
	let stmt = parse_statement(sql).expect("parse subquery");

	match stmt {
		Statement::Select(select) => {
			assert_eq!(select.from.len(), 1);
			match &select.from[0] {
				TableRef::Subquery {
					alias,
					..
				} => {
					assert_eq!(alias.name, "u");
				}
				other => panic!("expected Subquery in from, got {:?}", other),
			}
		}
		other => panic!("expected Select statement, got {:?}", other),
	}
}

#[test]
fn test_parse_select_union() {
	let sql = "SELECT 1 UNION ALL SELECT 2";
	let stmt = parse_statement(sql).expect("parse union");

	match stmt {
		Statement::Select(select) => {
			assert!(select.set_op.is_some());
			let op = select.set_op.unwrap();
			assert!(op.all);
		}
		other => panic!("expected Select statement, got {:?}", other),
	}
}

#[test]
fn test_parse_insert_comprehensive() {
	let sql = "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob') ON CONFLICT (id) DO UPDATE SET name = 'alice_new' RETURNING id, name";
	let stmt = parse_statement(sql).expect("parse insert");

	match stmt {
		Statement::Insert(insert) => {
			assert_eq!(insert.relation.parts, vec!["users"]);
			assert_eq!(insert.columns, vec!["id", "name"]);
			assert_eq!(insert.values.as_ref().map(|v| v.len()), Some(2));
			assert!(insert.returning.is_some());

			let conflict = insert.on_conflict.expect("expected on conflict");
			assert_eq!(conflict.target, Some(vec!["id".to_string()]));
			match conflict.action {
				OnConflictAction::DoUpdate(assignments) => {
					assert_eq!(assignments.len(), 1);
					assert_eq!(assignments[0].column, "name");
				}
				other => panic!("expected DoUpdate, got {:?}", other),
			}
		}
		other => panic!("expected Insert statement, got {:?}", other),
	}
}

#[test]
fn test_parse_update_comprehensive() {
	let sql = "UPDATE users SET name = 'bob', updated_at = 123 WHERE id = 1 RETURNING *";
	let stmt = parse_statement(sql).expect("parse update");

	match stmt {
		Statement::Update(update) => {
			assert_eq!(update.relation.parts, vec!["users"]);
			assert_eq!(update.assignments.len(), 2);
			assert_eq!(update.assignments[0].column, "name");
			assert_eq!(update.assignments[1].column, "updated_at");
			assert!(update.where_clause.is_some());
			assert!(update.returning.is_some());
		}
		other => panic!("expected Update statement, got {:?}", other),
	}
}

#[test]
fn test_parse_delete_comprehensive() {
	let sql = "DELETE FROM users USING blacklisted WHERE users.email = blacklisted.email RETURNING users.id";
	let stmt = parse_statement(sql).expect("parse delete");

	match stmt {
		Statement::Delete(delete) => {
			assert_eq!(delete.relation.parts, vec!["users"]);
			assert!(delete.using.is_some());
			assert!(delete.where_clause.is_some());
			assert!(delete.returning.is_some());
		}
		other => panic!("expected Delete statement, got {:?}", other),
	}
}

#[test]
fn test_parse_create_table_comprehensive() {
	let sql = "CREATE TEMPORARY TABLE IF NOT EXISTS users ( id INT PRIMARY KEY, email VARCHAR(255) NOT NULL UNIQUE, created_at INT DEFAULT 0, org_id INT REFERENCES orgs(id), CONSTRAINT fk_org FOREIGN KEY (org_id) REFERENCES orgs (id) )";
	let stmt = parse_statement(sql).expect("parse create table");

	match stmt {
		Statement::CreateTable(create) => {
			assert!(create.temporary);
			assert!(create.if_not_exists);
			assert_eq!(create.name.parts, vec!["users"]);
			assert_eq!(create.columns.len(), 4);
			assert_eq!(create.constraints.len(), 1);
			match &create.constraints[0] {
				TableConstraint::ForeignKey {
					columns,
					ref_table,
					ref_columns,
				} => {
					assert_eq!(columns, &vec!["org_id"]);
					assert_eq!(ref_table.parts, vec!["orgs"]);
					assert_eq!(ref_columns, &vec!["id"]);
				}
				other => panic!("expected ForeignKey table constraint, got {:?}", other),
			}
		}
		other => panic!("expected CreateTable statement, got {:?}", other),
	}
}

#[test]
fn test_parse_drop_comprehensive() {
	let sql = "DROP TABLE IF EXISTS users, profiles CASCADE";
	let stmt = parse_statement(sql).expect("parse drop");

	match stmt {
		Statement::Drop(drop) => {
			assert_eq!(drop.object_type, DropObjectType::Table);
			assert!(drop.if_exists);
			assert_eq!(drop.names.len(), 2);
			assert!(drop.cascade);
		}
		other => panic!("expected Drop statement, got {:?}", other),
	}
}

#[test]
fn test_parse_transactions() {
	let begin = parse_statement("BEGIN").expect("parse begin");
	assert!(matches!(begin, Statement::Transaction(TransactionStmt::Begin { .. })));

	let sp = parse_statement("SAVEPOINT sp1").expect("parse savepoint");
	assert!(
		matches!(sp, Statement::Transaction(TransactionStmt::Savepoint { ref name, .. }) if name == "sp1")
	);

	let rollback_to =
		parse_statement("ROLLBACK TO SAVEPOINT sp1").expect("parse rollback to savepoint");
	assert!(
		matches!(rollback_to, Statement::Transaction(TransactionStmt::RollbackToSavepoint { ref name, .. }) if name == "sp1")
	);

	let commit = parse_statement("COMMIT").expect("parse commit");
	assert!(matches!(commit, Statement::Transaction(TransactionStmt::Commit { .. })));
}

#[test]
fn test_parse_misc_statements() {
	let set = parse_statement("SET timezone TO 'UTC'").expect("parse set");
	match set {
		Statement::Set(s) => {
			assert_eq!(s.name, "timezone");
			assert_eq!(s.value, "'UTC'");
		}
		other => panic!("expected Set statement, got {:?}", other),
	}

	let show = parse_statement("SHOW client_encoding").expect("parse show");
	match show {
		Statement::Show(s) => {
			assert_eq!(s.name, "client_encoding");
		}
		other => panic!("expected Show statement, got {:?}", other),
	}

	let reset = parse_statement("RESET ALL").expect("parse reset");
	match reset {
		Statement::Reset(s) => {
			assert_eq!(s.name, "ALL");
		}
		other => panic!("expected Reset statement, got {:?}", other),
	}

	let explain = parse_statement("EXPLAIN (ANALYZE, VERBOSE) SELECT 1").expect("parse explain");
	match explain {
		Statement::Explain(e) => {
			assert_eq!(e.options, vec!["analyze", "verbose"]);
			assert!(matches!(*e.statement, Statement::Select(_)));
		}
		other => panic!("expected Explain statement, got {:?}", other),
	}
}

#[test]
fn test_parse_multiple_statements() {
	let sql = "SELECT 1; INSERT INTO t VALUES (2); DELETE FROM t;";
	let stmts = parse_statements(sql).expect("parse statements");
	assert_eq!(stmts.len(), 3);
	assert!(matches!(stmts[0], Statement::Select(_)));
	assert!(matches!(stmts[1], Statement::Insert(_)));
	assert!(matches!(stmts[2], Statement::Delete(_)));
}
