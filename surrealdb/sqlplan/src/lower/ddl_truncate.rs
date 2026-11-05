//! `TRUNCATE`: remove every row of a table and, where the dialect does so, start the numbering of
//! its generated columns over.
//!
//! MySQL restarts the `AUTO_INCREMENT` counter on every `TRUNCATE`. PostgreSQL keeps the sequences
//! of a `SERIAL` or identity column running unless the statement says `RESTART IDENTITY`.

use surrealdb_expr::expr::operator::BinaryOperator;
use surrealdb_expr::expr::statements::define::DefineKind;
use surrealdb_expr::expr::statements::{ForeachStatement, IfelseStatement, InfoStatement};
use surrealdb_expr::expr::{Block, Expr, Idiom, Literal, Param, Part};
use surrealdb_mysql::ast::stmt::TruncateStmt as MyTruncateStmt;
use surrealdb_pgsql::ast::stmt::TruncateStmt as PgTruncateStmt;
use surrealdb_strand::{Strand, TableName};

use super::ddl::define_sequence;
use crate::error::PlanError;
use crate::namespace::{resolve_mysql_table_name, resolve_pgsql_table_name};

/// Lower a MySQL `TRUNCATE` into the one expression that answers it: the removal of every row
/// ([`write::lower_mysql_truncate`](super::write::lower_mysql_truncate)), then the restart of the
/// numbering of every generated column of the table, as one block, so that a failure of either
/// leaves the rows and every counter as they were. The answer is the empty array of a statement
/// that removes rows and returns none.
pub fn lower_mysql_truncate(truncate: &MyTruncateStmt) -> Result<Expr, PlanError> {
	let delete = super::write::lower_mysql_truncate(truncate)?;
	let (_, table) = resolve_mysql_table_name(&truncate.table.parts, None)?;
	Ok(restarting(delete, &table))
}

/// Lower a PostgreSQL `TRUNCATE` into the one expression that answers it: the removal of every
/// row ([`write::lower_pgsql_truncate`](super::write::lower_pgsql_truncate)) and, for `RESTART
/// IDENTITY`, the restart of the numbering of every generated column of each table, as one block as
/// for MySQL. `CONTINUE IDENTITY`, the default, leaves the numbering alone, and the expression is
/// the removal of the rows.
pub fn lower_pgsql_truncate(truncate: &PgTruncateStmt) -> Result<Expr, PlanError> {
	let delete = super::write::lower_pgsql_truncate(truncate)?;
	if !truncate.restart_identity {
		return Ok(delete);
	}
	let mut exprs = vec![delete];
	for table in &truncate.tables {
		let (_, table) = resolve_pgsql_table_name(&table.parts, None)?;
		exprs.push(restart_generated_keys(&table));
	}
	exprs.push(Expr::Literal(Literal::Array(Vec::new())));
	Ok(Expr::Block(Box::new(Block(exprs))))
}

/// The block that removes the rows, restarts the numbering, and answers the empty array.
fn restarting(delete: Expr, table: &str) -> Expr {
	Expr::Block(Box::new(Block(vec![
		delete,
		restart_generated_keys(table),
		Expr::Literal(Literal::Array(Vec::new())),
	])))
}

/// The expression that starts the numbering of every generated column of `table` over: for each
/// field of the table whose `DEFAULT` draws a sequence value (`sequence::nextval`) and for which a
/// sequence named by [`sequence_name`](super::ddl::sequence_name) exists, that sequence is defined
/// again (`OVERWRITE`), so its next value is 1.
///
/// ```text
/// FOR $column IN (INFO FOR TABLE t STRUCTURE).fields {
///     IF $column.default CONTAINS '::nextval('
///         AND (INFO FOR DB).sequences['t.' + $column.name] != NONE {
///         DEFINE SEQUENCE OVERWRITE ('t.' + $column.name) BATCH 1000 START 1
///     }
/// }
/// ```
///
/// The statement does not name the columns, so they are read when the expression runs, from the
/// field definitions the transaction sees: the answer is that of the table as it is at that moment,
/// including a table created earlier in the same request or transaction. A table with no generated
/// column costs one read of its definition. A field's name is the one `INFO` reports, which is what
/// [`sequence_name`](super::ddl::sequence_name) spells.
///
/// The expression must run after the expression that removes the table's rows, in the same
/// block ([`restarting`]), which is all or nothing: a statement that fails to remove the rows, or
/// to restart a sequence, then leaves both the rows and every counter as they were, and a counter
/// is never restarted while rows numbered from it remain. Redefining a sequence needs the right to
/// define one, so a session that may delete rows but not define a sequence is refused the whole
/// statement.
fn restart_generated_keys(table: &str) -> Expr {
	let column = |field: &str| {
		Expr::Idiom(Idiom(vec![
			Part::Start(Expr::Param(Param::from("column".to_string()))),
			Part::Field(Strand::from(field)),
		]))
	};
	// `(INFO ...).field`, or `(INFO ...).field[key]` when a key is given.
	let info = |statement: InfoStatement, field: &str, key: Option<Expr>| {
		let mut parts =
			vec![Part::Start(Expr::Info(Box::new(statement))), Part::Field(Strand::from(field))];
		parts.extend(key.map(Part::Value));
		Expr::Idiom(Idiom(parts))
	};
	let name = Expr::Binary {
		left: Box::new(Expr::Literal(Literal::String(Strand::from(format!("{table}.").as_str())))),
		op: BinaryOperator::Add,
		right: Box::new(column("name")),
	};
	let fields =
		info(InfoStatement::Tb(Expr::Table(TableName::from(table)), true, None), "fields", None);
	let sequence = info(InfoStatement::Db(false, None), "sequences", Some(name.clone()));

	let draws_a_sequence = Expr::Binary {
		left: Box::new(column("default")),
		op: BinaryOperator::Contain,
		right: Box::new(Expr::Literal(Literal::String(Strand::from("::nextval(")))),
	};
	let sequence_exists = Expr::Binary {
		left: Box::new(sequence),
		op: BinaryOperator::NotEqual,
		right: Box::new(Expr::Literal(Literal::None)),
	};
	let restart = Expr::Block(Box::new(Block(vec![define_sequence(DefineKind::Overwrite, name)])));
	let when_generated = IfelseStatement {
		exprs: vec![(
			Expr::Binary {
				left: Box::new(draws_a_sequence),
				op: BinaryOperator::And,
				right: Box::new(sequence_exists),
			},
			restart,
		)],
		close: None,
	};
	Expr::Foreach(Box::new(ForeachStatement {
		param: Param::from("column".to_string()),
		range: fields,
		block: Block(vec![Expr::IfElse(Box::new(when_generated))]),
	}))
}
