//! Unit and integration tests for PostgreSQL statement parser.

use surrealdb_pgsql::ast::stmt::{
	AlterTableAction, DistinctClause, DropObjectType, JoinCondition, JoinType, NullsOrder,
	OnConflictAction, OrderDirection, Statement, TableConstraint, TableRef, TransactionStmt,
};
use surrealdb_pgsql::printer::ToPgSql;
use surrealdb_pgsql::{parse_statement, parse_statements};

#[test]
fn test_parse_select_comprehensive() {
	let sql = "SELECT id, name AS user_name, age FROM users WHERE age >= 18 GROUP BY id, name HAVING count(*) > 1 ORDER BY age DESC NULLS LAST LIMIT 10 OFFSET 5";
	let stmt = parse_statement(sql).expect("parse select");

	let Statement::Select(select) = stmt else {
		panic!("expected Select statement, got {:?}", stmt);
	};
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

#[test]
fn test_parse_select_distinct_on() {
	let sql = "SELECT DISTINCT ON (dept) dept, salary FROM emp";
	let stmt = parse_statement(sql).expect("parse distinct on");

	let Statement::Select(select) = stmt else {
		panic!("expected Select statement, got {:?}", stmt);
	};
	let Some(DistinctClause::On(exprs)) = select.distinct else {
		panic!("expected Distinct ON, got {:?}", select.distinct);
	};
	assert_eq!(exprs.len(), 1);
}

#[test]
fn test_parse_select_joins() {
	let sql = "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id LEFT JOIN t3 USING (dept_id)";
	let stmt = parse_statement(sql).expect("parse joins");

	let Statement::Select(select) = stmt else {
		panic!("expected Select statement, got {:?}", stmt);
	};
	assert_eq!(select.from.len(), 1);
	let TableRef::Join {
		join_type: JoinType::Left,
		condition: Some(JoinCondition::Using(cols)),
		left,
		..
	} = &select.from[0]
	else {
		panic!("expected left join, got {:?}", select.from[0]);
	};
	assert_eq!(cols, &vec!["dept_id"]);
	assert!(matches!(
		**left,
		TableRef::Join {
			join_type: JoinType::Inner,
			..
		}
	));
}

#[test]
fn test_parse_select_subquery() {
	let sql = "SELECT * FROM (SELECT id FROM users) AS u";
	let stmt = parse_statement(sql).expect("parse subquery");

	let Statement::Select(select) = stmt else {
		panic!("expected Select statement, got {:?}", stmt);
	};
	assert_eq!(select.from.len(), 1);
	let TableRef::Subquery {
		alias,
		..
	} = &select.from[0]
	else {
		panic!("expected Subquery in from, got {:?}", select.from[0]);
	};
	assert_eq!(alias.name, "u");
}

#[test]
fn test_parse_select_values_in_from() {
	let sql = "SELECT n FROM (VALUES (1, 'a'), (2, 'b')) AS v(n, l)";
	let stmt = parse_statement(sql).expect("parse VALUES in FROM");

	let Statement::Select(select) = stmt else {
		panic!("expected Select statement, got {:?}", stmt);
	};
	let TableRef::Subquery {
		query,
		alias,
		..
	} = &select.from[0]
	else {
		panic!("expected Subquery in from, got {:?}", select.from[0]);
	};
	assert_eq!(alias.name, "v");
	assert_eq!(alias.columns, vec!["n".to_string(), "l".to_string()]);
	assert!(query.set_op.is_some());
}

#[test]
fn test_values_rows_must_have_the_same_number_of_columns() {
	for sql in [
		"SELECT * FROM (VALUES (1), (2, 3)) AS v",
		"SELECT * FROM (VALUES (1, 2), (3)) AS v",
		"SELECT * FROM (VALUES ()) AS v",
		"SELECT * FROM (VALUES (1), ()) AS v",
	] {
		assert!(parse_statement(sql).is_err(), "{sql}");
	}
}

#[test]
fn test_parse_select_union() {
	let sql = "SELECT 1 UNION ALL SELECT 2";
	let stmt = parse_statement(sql).expect("parse union");

	let Statement::Select(select) = stmt else {
		panic!("expected Select statement, got {:?}", stmt);
	};
	assert!(select.set_op.is_some());
	let op = select.set_op.unwrap();
	assert!(op.all);
}

#[test]
fn test_parse_insert_comprehensive() {
	let sql = "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob') ON CONFLICT (id) DO UPDATE SET name = 'alice_new' RETURNING id, name";
	let stmt = parse_statement(sql).expect("parse insert");

	let Statement::Insert(insert) = stmt else {
		panic!("expected Insert statement, got {:?}", stmt);
	};
	assert_eq!(insert.relation.parts, vec!["users"]);
	assert_eq!(insert.columns, vec!["id", "name"]);
	assert_eq!(insert.values.as_ref().map(|v| v.len()), Some(2));
	assert!(insert.returning.is_some());

	let conflict = insert.on_conflict.expect("expected on conflict");
	assert_eq!(conflict.target, Some(vec!["id".to_string()]));
	let OnConflictAction::DoUpdate(assignments) = conflict.action else {
		panic!("expected DoUpdate, got {:?}", conflict.action);
	};
	assert_eq!(assignments.len(), 1);
	assert_eq!(assignments[0].column, "name");
}

#[test]
fn test_parse_update_comprehensive() {
	let sql = "UPDATE users SET name = 'bob', updated_at = 123 WHERE id = 1 RETURNING *";
	let stmt = parse_statement(sql).expect("parse update");

	let Statement::Update(update) = stmt else {
		panic!("expected Update statement, got {:?}", stmt);
	};
	assert_eq!(update.relation.parts, vec!["users"]);
	assert_eq!(update.assignments.len(), 2);
	assert_eq!(update.assignments[0].column, "name");
	assert_eq!(update.assignments[1].column, "updated_at");
	assert!(update.where_clause.is_some());
	assert!(update.returning.is_some());
}

#[test]
fn test_parse_delete_comprehensive() {
	let sql = "DELETE FROM users USING blacklisted WHERE users.email = blacklisted.email RETURNING users.id";
	let stmt = parse_statement(sql).expect("parse delete");

	let Statement::Delete(delete) = stmt else {
		panic!("expected Delete statement, got {:?}", stmt);
	};
	assert_eq!(delete.relation.parts, vec!["users"]);
	assert!(delete.using.is_some());
	assert!(delete.where_clause.is_some());
	assert!(delete.returning.is_some());
}

#[test]
fn test_parse_create_table_comprehensive() {
	let sql = "CREATE TEMPORARY TABLE IF NOT EXISTS users ( id INT PRIMARY KEY, email VARCHAR(255) NOT NULL UNIQUE, created_at INT DEFAULT 0, org_id INT REFERENCES orgs(id), CONSTRAINT fk_org FOREIGN KEY (org_id) REFERENCES orgs (id) )";
	let stmt = parse_statement(sql).expect("parse create table");

	let Statement::CreateTable(create) = stmt else {
		panic!("expected CreateTable statement, got {:?}", stmt);
	};
	assert!(create.temporary);
	assert!(create.if_not_exists);
	assert_eq!(create.name.parts, vec!["users"]);
	assert_eq!(create.columns.len(), 4);
	assert_eq!(create.constraints.len(), 1);
	let TableConstraint::ForeignKey {
		columns,
		ref_table,
		ref_columns,
		..
	} = &create.constraints[0]
	else {
		panic!("expected ForeignKey table constraint, got {:?}", create.constraints[0]);
	};
	assert_eq!(columns, &vec!["org_id"]);
	assert_eq!(ref_table.parts, vec!["orgs"]);
	assert_eq!(ref_columns, &vec!["id"]);
}

#[test]
fn test_parse_drop_comprehensive() {
	let sql = "DROP TABLE IF EXISTS users, profiles CASCADE";
	let stmt = parse_statement(sql).expect("parse drop");

	let Statement::Drop(drop) = stmt else {
		panic!("expected Drop statement, got {:?}", stmt);
	};
	assert_eq!(drop.object_type, DropObjectType::Table);
	assert!(drop.if_exists);
	assert_eq!(drop.names.len(), 2);
	assert!(drop.cascade);
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
	let Statement::Set(s) = set else {
		panic!("expected Set statement, got {:?}", set);
	};
	assert_eq!(s.name, "timezone");
	assert_eq!(s.value, "'UTC'");

	let show = parse_statement("SHOW client_encoding").expect("parse show");
	let Statement::Show(s) = show else {
		panic!("expected Show statement, got {:?}", show);
	};
	assert_eq!(s.name, "client_encoding");

	let reset = parse_statement("RESET ALL").expect("parse reset");
	let Statement::Reset(s) = reset else {
		panic!("expected Reset statement, got {:?}", reset);
	};
	assert_eq!(s.name, "ALL");

	let explain = parse_statement("EXPLAIN (ANALYZE, VERBOSE) SELECT 1").expect("parse explain");
	let Statement::Explain(e) = explain else {
		panic!("expected Explain statement, got {:?}", explain);
	};
	assert_eq!(e.options, vec!["analyze", "verbose"]);
	assert!(matches!(*e.statement, Statement::Select(_)));
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

#[test]
fn test_parse_insert_on_conflict_do_update_where() {
	let sql = "INSERT INTO t AS a (id, age) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET age = $3 WHERE (\"public\".\"t\".\"id\" = $4 AND 1 = 1) RETURNING id";
	let Statement::Insert(insert) = parse_statement(sql).expect("parse insert") else {
		panic!("expected Insert statement");
	};
	assert_eq!(insert.alias.as_deref(), Some("a"));
	let conflict = insert.on_conflict.as_ref().expect("expected on conflict");
	assert_eq!(conflict.target, Some(vec!["id".to_string()]));
	assert!(conflict.target_where.is_none());
	assert!(matches!(&conflict.action, OnConflictAction::DoUpdate(sets) if sets.len() == 1));
	assert!(conflict.where_clause.is_some());
	assert_eq!(insert.returning.as_ref().map(Vec::len), Some(1));

	for sql in [sql, "INSERT INTO t (id) VALUES (1) ON CONFLICT (id) WHERE id > 0 DO NOTHING"] {
		let mut ast = parse_statement(sql).expect("parse insert");
		ast.normalize_spans();
		let printed = ast.to_pgsql_string();
		let mut reparsed = parse_statement(&printed).expect("reparse insert");
		reparsed.normalize_spans();
		assert_eq!(ast, reparsed, "printed as: {printed}");
	}
}

#[test]
fn test_parse_insert_on_conflict_target_where_and_do_nothing() {
	let sql = "INSERT INTO t (id) VALUES (1) ON CONFLICT (id) WHERE id > 0 DO NOTHING";
	let Statement::Insert(insert) = parse_statement(sql).expect("parse insert") else {
		panic!("expected Insert statement");
	};
	let conflict = insert.on_conflict.expect("expected on conflict");
	assert!(conflict.target_where.is_some());
	assert!(conflict.where_clause.is_none());
	assert!(matches!(conflict.action, OnConflictAction::DoNothing));
}

#[test]
fn test_parse_foreign_key_referential_actions() {
	use surrealdb_pgsql::ast::stmt::{ColumnConstraint, ReferentialAction};

	let stmt = parse_statement(
		"CREATE TABLE posts (author_id INT REFERENCES users(id) ON UPDATE CASCADE ON DELETE SET \
		 NULL, org_id INT, FOREIGN KEY (org_id) REFERENCES orgs(id) ON DELETE RESTRICT ON UPDATE \
		 SET DEFAULT)",
	)
	.expect("parse actions");
	let Statement::CreateTable(create) = stmt else {
		panic!("expected CreateTable");
	};
	let ColumnConstraint::References {
		on_delete,
		..
	} = &create.columns[0].constraints[0]
	else {
		panic!("expected a REFERENCES constraint");
	};
	assert_eq!(*on_delete, ReferentialAction::SetNull);
	let TableConstraint::ForeignKey {
		on_delete,
		..
	} = &create.constraints[0]
	else {
		panic!("expected a foreign key");
	};
	assert_eq!(*on_delete, ReferentialAction::Restrict);

	let stmt = parse_statement(
		"ALTER TABLE \"CompatPost\" ADD CONSTRAINT \"fk\" FOREIGN KEY (\"authorId\") REFERENCES \
		 \"public\".\"CompatUser\"(\"id\") ON DELETE no action ON UPDATE no action",
	)
	.expect("parse drizzle's foreign key");
	let Statement::AlterTable(alter) = stmt else {
		panic!("expected AlterTable");
	};
	let AlterTableAction::AddConstraint(TableConstraint::ForeignKey {
		on_delete,
		..
	}) = &alter.actions[0]
	else {
		panic!("expected an added foreign key");
	};
	assert_eq!(*on_delete, ReferentialAction::NoAction);

	// An action prints back, so the statement survives a round trip.
	let sql = "CREATE TABLE t (a INT REFERENCES u(id) ON DELETE CASCADE)";
	let printed = parse_statement(sql).unwrap().to_string();
	assert!(printed.contains("ON DELETE CASCADE"), "{printed}");
	assert_eq!(parse_statement(&printed).unwrap().to_string(), printed);

	assert!(parse_statement("CREATE TABLE t (a INT REFERENCES u(id) ON DELETE").is_err());
	assert!(
		parse_statement("CREATE TABLE t (a INT REFERENCES u(id) ON TRUNCATE CASCADE)").is_err()
	);
	assert!(parse_statement("CREATE TABLE t (a INT REFERENCES u(id) ON DELETE SET)").is_err());
}

#[test]
fn test_parse_lateral_subquery() {
	let stmt =
		parse_statement("SELECT k.a FROM t CROSS JOIN LATERAL (SELECT unnest(t.xs)) AS k(a)")
			.expect("parse LATERAL");
	let Statement::Select(select) = stmt else {
		panic!("expected Select");
	};
	let TableRef::Join {
		right,
		..
	} = &select.from[0]
	else {
		panic!("expected a join");
	};
	assert!(matches!(
		right.as_ref(),
		TableRef::Subquery {
			lateral: true,
			..
		}
	));
	let printed = Statement::Select(select).to_string();
	assert!(printed.contains("LATERAL ("), "{printed}");

	assert!(parse_statement("SELECT 1 FROM t, LATERAL generate_series(1, 2) AS g").is_err());
	assert!(parse_statement("SELECT 1 FROM t, LATERAL (t2 JOIN t3 ON true)").is_err());
}

#[test]
fn test_parse_empty_select_list_and_keyword_aliases() {
	let stmt = parse_statement("SELECT FROM t WHERE true").expect("parse an empty list");
	let Statement::Select(select) = stmt else {
		panic!("expected Select");
	};
	assert!(select.targets.is_empty());
	assert!(parse_statement("SELECT EXISTS (SELECT FROM t)").is_ok());

	let stmt = parse_statement("SELECT a AS to, b AS for, c AS using, d AS with FROM t")
		.expect("parse keyword aliases");
	let Statement::Select(select) = stmt else {
		panic!("expected Select");
	};
	let aliases: Vec<_> = select.targets.iter().map(|t| t.alias.as_deref()).collect();
	assert_eq!(aliases, [Some("to"), Some("for"), Some("using"), Some("with")]);
	// Without `AS`, a reserved keyword is no alias.
	assert!(parse_statement("SELECT a to FROM t").is_err());
}
