//! The sequence behind a serial or auto-increment column: how it is named and when a statement
//! starts it over (`CREATE TABLE`, `TRUNCATE`).

use surrealdb_expr::expr::statements::define::{DefineKind, DefineStatement};
use surrealdb_expr::expr::{Block, Expr, Kind, Literal};
use surrealdb_types::ToSql;

use super::ddl::{lower_mysql_ddl_definitions, lower_pgsql_ddl_definitions, sequence_name};
use super::ddl_truncate::{lower_mysql_truncate, lower_pgsql_truncate};

/// The kind and name of every `DEFINE SEQUENCE` among `exprs`, and the position of the first
/// `DEFINE TABLE`.
fn sequences(exprs: &[Expr]) -> (Option<usize>, Vec<(DefineKind, String)>) {
	let table = exprs
		.iter()
		.position(|e| matches!(e, Expr::Define(d) if matches!(**d, DefineStatement::Table(_))));
	let mut sequences = Vec::new();
	for expr in exprs {
		if let Expr::Define(define) = expr
			&& let DefineStatement::Sequence(seq) = &**define
		{
			let Expr::Literal(Literal::String(name)) = &seq.name else {
				panic!("a sequence is named by a string: {:?}", seq.name);
			};
			sequences.push((seq.kind.clone(), name.as_str().to_string()));
		}
	}
	(table, sequences)
}

fn pgsql(sql: &str) -> (Option<usize>, Vec<(DefineKind, String)>) {
	let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
	sequences(&lower_pgsql_ddl_definitions(&stmt).expect("lower"))
}

fn mysql(sql: &str) -> (Option<usize>, Vec<(DefineKind, String)>) {
	let stmt = surrealdb_mysql::parse_statement(sql).expect("parse");
	sequences(&lower_mysql_ddl_definitions(&stmt).expect("lower"))
}

#[test]
fn a_table_that_is_created_starts_its_sequences_over() {
	let (table, sequences) = pgsql("CREATE TABLE t (id SERIAL PRIMARY KEY, n BIGSERIAL);");
	assert_eq!(table, Some(0), "the table is defined before its sequences");
	assert_eq!(sequences.len(), 2, "{sequences:?}");
	assert!(sequences.iter().all(|(kind, _)| matches!(kind, DefineKind::Overwrite)));

	let (table, sequences) = mysql("CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY);");
	assert_eq!(table, Some(0), "the table is defined before its sequences");
	assert_eq!(sequences.len(), 1, "{sequences:?}");
	assert!(matches!(sequences[0].0, DefineKind::Overwrite));
}

#[test]
fn a_table_that_may_exist_keeps_the_numbering_of_its_sequences() {
	let (_, sequences) = pgsql("CREATE TABLE IF NOT EXISTS t (id SERIAL PRIMARY KEY);");
	assert_eq!(sequences.len(), 1, "{sequences:?}");
	assert!(matches!(sequences[0].0, DefineKind::IfNotExists));

	let (_, sequences) = mysql("CREATE TABLE IF NOT EXISTS t (id INT AUTO_INCREMENT PRIMARY KEY);");
	assert_eq!(sequences.len(), 1, "{sequences:?}");
	assert!(matches!(sequences[0].0, DefineKind::IfNotExists));
}

#[test]
fn two_generated_columns_never_share_a_sequence() {
	let name = |sql: &str| {
		let (_, mut sequences) = pgsql(sql);
		assert_eq!(sequences.len(), 1, "{sequences:?}");
		sequences.remove(0).1
	};
	// Joined by underscores the first two would be named alike (`a_b_c_seq`), and so would the
	// next two once the dot of the schema is read as an underscore.
	let names = [
		name("CREATE TABLE a_b (c SERIAL);"),
		name("CREATE TABLE a (b_c SERIAL);"),
		name("CREATE TABLE s.t (c SERIAL);"),
		name("CREATE TABLE s_t (c SERIAL);"),
		// A quoted column name holding a dot is escaped, so it does not read as the table `s.t`
		// with the column `c`.
		name("CREATE TABLE s (\"t.c\" SERIAL);"),
	];
	assert_eq!(names[2], "s.t.c");
	assert_eq!(names[4], "s.`t.c`");
	for (i, left) in names.iter().enumerate() {
		for right in &names[i + 1..] {
			assert_ne!(left, right);
		}
	}
}

#[test]
fn a_mysql_auto_increment_column_is_not_null_like_a_postgresql_serial_column() {
	let kind = |exprs: &[Expr], column: &str| {
		for expr in exprs {
			if let Expr::Define(define) = expr
				&& let DefineStatement::Field(field) = &**define
				&& matches!(&field.name, Expr::Idiom(i) if i.to_sql() == column)
			{
				return field.field_kind.clone();
			}
		}
		panic!("no field {column}");
	};
	let stmt =
		surrealdb_mysql::parse_statement("CREATE TABLE t (k INT AUTO_INCREMENT UNIQUE, n INT);")
			.expect("parse");
	let exprs = lower_mysql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(kind(&exprs, "k"), Some(Kind::I32));
	assert_ne!(kind(&exprs, "n"), Some(Kind::I32), "a column that is not generated may be NULL");

	let stmt = surrealdb_pgsql::parse_statement("CREATE TABLE t (k SERIAL UNIQUE, n INT);")
		.expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(kind(&exprs, "k"), Some(Kind::I32));
}

fn mysql_truncate(sql: &str) -> Expr {
	let stmt = surrealdb_mysql::parse_statement(sql).expect("parse");
	let surrealdb_mysql::ast::stmt::Statement::Truncate(truncate) = stmt else {
		panic!("not a TRUNCATE: {sql}");
	};
	lower_mysql_truncate(&truncate).expect("lower")
}

fn pgsql_truncate(sql: &str) -> Expr {
	let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Truncate(truncate) = stmt else {
		panic!("not a TRUNCATE: {sql}");
	};
	lower_pgsql_truncate(&truncate).expect("lower")
}

/// The three expressions of a statement that removes the rows and restarts the numbering.
fn block(expr: &Expr) -> &[Expr] {
	let Expr::Block(block) = expr else {
		panic!("a statement that restarts the numbering is one block: {expr:?}");
	};
	let Block(exprs) = &**block;
	exprs
}

#[test]
fn a_mysql_truncate_removes_the_rows_then_restarts_the_numbering_and_answers_empty() {
	for sql in ["TRUNCATE TABLE t", "TRUNCATE t", "TRUNCATE TABLE test.t"] {
		let expr = mysql_truncate(sql);
		let exprs = block(&expr);
		assert_eq!(exprs.len(), 3, "{sql}: {exprs:?}");
		assert!(matches!(exprs[0], Expr::Delete(_)), "{sql}: the rows go first: {:?}", exprs[0]);
		assert!(matches!(exprs[1], Expr::Foreach(_)), "{sql}: then the numbering: {:?}", exprs[1]);
		assert_eq!(exprs[2], Expr::Literal(Literal::Array(Vec::new())), "{sql}");
	}
}

#[test]
fn a_postgresql_truncate_restarts_the_numbering_only_for_restart_identity() {
	for sql in [
		"TRUNCATE t",
		"TRUNCATE TABLE t",
		"TRUNCATE TABLE ONLY t CONTINUE IDENTITY",
		"TRUNCATE t CONTINUE IDENTITY CASCADE",
	] {
		assert!(
			matches!(pgsql_truncate(sql), Expr::Delete(_)),
			"{sql} leaves the sequences running"
		);
	}
	for sql in [
		"TRUNCATE t RESTART IDENTITY",
		"TRUNCATE TABLE ONLY t RESTART IDENTITY CASCADE",
		"TRUNCATE public.t RESTART IDENTITY",
	] {
		let expr = pgsql_truncate(sql);
		let exprs = block(&expr);
		assert_eq!(exprs.len(), 3, "{sql}: {exprs:?}");
		assert!(matches!(exprs[0], Expr::Delete(_)), "{sql}: the rows go first");
		assert!(matches!(exprs[1], Expr::Foreach(_)), "{sql}: then the numbering");
		assert_eq!(exprs[2], Expr::Literal(Literal::Array(Vec::new())), "{sql}");
	}
}

#[test]
fn the_restart_reads_the_table_it_names_and_redefines_the_sequences_named_for_its_columns() {
	let loop_of = |expr: &Expr| block(expr)[1].to_sql();
	assert_eq!(
		loop_of(&mysql_truncate("TRUNCATE TABLE t")),
		"FOR $column IN (INFO FOR TABLE t STRUCTURE).fields { IF $column.default CONTAINS \
		 '::nextval(' AND (INFO FOR DATABASE).sequences['t.' + $column.name] != NONE { DEFINE \
		 SEQUENCE OVERWRITE 't.' + $column.name BATCH 1000 START 1 } }"
	);
	// The table's name is used as it is, so a schema-qualified PostgreSQL table reads and names
	// the table the way its `CREATE TABLE` did.
	let expr = pgsql_truncate("TRUNCATE s.t RESTART IDENTITY");
	let text = loop_of(&expr);
	assert!(text.contains("INFO FOR TABLE `s.t` STRUCTURE"), "{text}");
	assert!(text.contains("['s.t.' + $column.name]"), "{text}");
	assert!(text.contains("OVERWRITE 's.t.' + $column.name BATCH 1000 START 1"), "{text}");
}

#[test]
fn the_sequence_of_a_column_is_named_for_its_table_and_the_column_as_surrealql_spells_it() {
	assert_eq!(sequence_name("t", "id"), "t.id");
	assert_eq!(sequence_name("s.t", "n"), "s.t.n");
	assert_eq!(sequence_name("t", "order id"), "t.`order id`");
	assert_eq!(sequence_name("t", "a.b"), "t.`a.b`");
	assert_eq!(sequence_name("t", "it's"), "t.`it's`");
	assert_eq!(sequence_name("t", "a`b"), "t.`a\\`b`");
	assert_eq!(sequence_name("t", "naïve"), "t.`naïve`");

	// What `CREATE TABLE` defines is named as `sequence_name` says.
	let (_, sequences) = pgsql("CREATE TABLE t (id SERIAL, \"a b\" BIGSERIAL, x TEXT)");
	let names: Vec<_> = sequences.into_iter().map(|(_, name)| name).collect();
	assert_eq!(names, ["t.id", "t.`a b`"]);
}

#[test]
fn a_table_that_may_exist_restarts_its_sequences_only_when_it_is_created() {
	let restarts =
		|exprs: &[Expr]| exprs.iter().take_while(|e| matches!(e, Expr::IfElse(_))).count();

	let stmt = surrealdb_pgsql::parse_statement(
		"CREATE TABLE IF NOT EXISTS t (id SERIAL PRIMARY KEY, n BIGSERIAL, x TEXT);",
	)
	.expect("parse");
	let exprs = lower_pgsql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(restarts(&exprs), 2, "one restart per generated column, before the table");
	assert!(matches!(&exprs[2], Expr::Define(d) if matches!(**d, DefineStatement::Table(_))));

	let stmt = surrealdb_mysql::parse_statement(
		"CREATE TABLE IF NOT EXISTS t (id INT AUTO_INCREMENT PRIMARY KEY);",
	)
	.expect("parse");
	let exprs = lower_mysql_ddl_definitions(&stmt).expect("lower");
	assert_eq!(restarts(&exprs), 1);
	assert!(matches!(&exprs[1], Expr::Define(d) if matches!(**d, DefineStatement::Table(_))));

	// A table that is not conditional is created, so its sequences are overwritten in place.
	let stmt =
		surrealdb_pgsql::parse_statement("CREATE TABLE t (id SERIAL PRIMARY KEY);").expect("parse");
	assert_eq!(restarts(&lower_pgsql_ddl_definitions(&stmt).expect("lower")), 0);
}
