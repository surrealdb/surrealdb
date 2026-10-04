//! Statement translation from MySQL AST to SurrealQL.

use std::fmt::Write;

use crate::ast::stmt::{
	CreateTableStmt, DescribeStmt, DropTableStmt, ExplainStmt, JoinType, LimitClause, OrderByExpr,
	QualifiedName, SelectItem, SelectStmt, SetStmt, ShowStmt, Statement, TableRef, TransactionStmt,
	UseStmt, VariableTarget,
};
use crate::translate::context::TranslationContext;
use crate::translate::error::TranslationError;
use crate::translate::expr::translate_expr;

/// Translates a MySQL statement into its SurrealQL string representation.
pub fn translate_statement(
	stmt: &Statement,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	match stmt {
		Statement::Select(s) => translate_select(s, ctx, out),
		Statement::Insert(_)
		| Statement::Replace(_)
		| Statement::Update(_)
		| Statement::Delete(_)
		| Statement::Truncate(_) => Err(TranslationError::Unsupported(
			"DML statements must be lowered via surrealdb_sqlplan typed write lowering".into(),
		)),
		Statement::CreateTable(s) => translate_create_table(s, ctx, out),
		Statement::DropTable(s) => translate_drop_table(s, out),
		Statement::Use(s) => translate_use(s, out),
		Statement::Set(s) => translate_set(s, ctx, out),
		Statement::Show(s) => translate_show(s, out),
		Statement::Describe(s) => translate_describe(s, out),
		Statement::Explain(s) => translate_explain(s, ctx, out),
		Statement::Transaction(s) => translate_transaction(s, out),
	}
}

pub fn translate_select(
	sel: &SelectStmt,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	if sel.from.is_none() {
		out.push_str("RETURN [{ ");
		for (i, p) in sel.projection.iter().enumerate() {
			if i > 0 {
				out.push_str(", ");
			}
			match p {
				SelectItem::Expr {
					expr,
					alias,
					..
				} => {
					let key = alias.clone().unwrap_or_else(|| {
						if sel.projection.len() == 1 {
							"col_1".to_string()
						} else {
							format!("col_{}", i + 1)
						}
					});
					write!(out, "{key:?}: ")?;
					translate_expr(expr, ctx, out)?;
				}
				SelectItem::Wildcard(_) => {
					out.push_str("\"*\": true");
				}
				SelectItem::QualifiedWildcard(tbl, _) => {
					write!(out, "\"{tbl}.*\": true")?;
				}
			}
		}
		out.push_str(" }];");
		return Ok(());
	}

	out.push_str("SELECT ");
	if sel.distinct {
		out.push_str("DISTINCT ");
	}

	for (i, p) in sel.projection.iter().enumerate() {
		if i > 0 {
			out.push_str(", ");
		}
		match p {
			SelectItem::Wildcard(_) => out.push('*'),
			SelectItem::QualifiedWildcard(tbl, _) => {
				out.push_str(tbl);
				out.push_str(".*");
			}
			SelectItem::Expr {
				expr,
				alias,
				..
			} => {
				translate_expr(expr, ctx, out)?;
				if let Some(a) = alias {
					out.push_str(" AS ");
					out.push_str(a);
				}
			}
		}
	}

	if let Some(from) = &sel.from {
		out.push_str(" FROM ");
		translate_table_ref(from, ctx, out)?;
	}

	if let Some(w) = &sel.where_clause {
		out.push_str(" WHERE ");
		translate_expr(w, ctx, out)?;
	}

	if !sel.group_by.is_empty() {
		out.push_str(" GROUP BY ");
		for (i, g) in sel.group_by.iter().enumerate() {
			if i > 0 {
				out.push_str(", ");
			}
			translate_expr(g, ctx, out)?;
		}
	}

	if !sel.order_by.is_empty() {
		out.push_str(" ORDER BY ");
		translate_order_by(&sel.order_by, ctx, out)?;
	}

	if let Some(lim) = &sel.limit {
		translate_limit(lim, ctx, out)?;
	}

	out.push(';');
	Ok(())
}

fn translate_table_ref(
	tbl: &TableRef,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	match tbl {
		TableRef::Table {
			name,
			alias,
			..
		} => {
			translate_qualified_name(name, out)?;
			if let Some(a) = alias {
				out.push_str(" AS ");
				out.push_str(a);
			}
			Ok(())
		}
		TableRef::Subquery {
			subquery,
			alias,
			..
		} => {
			out.push('(');
			translate_select(subquery, ctx, out)?;
			out.push_str(") AS ");
			out.push_str(alias);
			Ok(())
		}
		TableRef::Join {
			left,
			join_type,
			right,
			condition,
			..
		} => {
			translate_table_ref(left, ctx, out)?;
			match join_type {
				JoinType::Inner => out.push_str(" JOIN "),
				JoinType::Left => out.push_str(" LEFT JOIN "),
				JoinType::Right => out.push_str(" RIGHT JOIN "),
				JoinType::Cross => out.push_str(" CROSS JOIN "),
				JoinType::Straight => out.push_str(" JOIN "),
			}
			translate_table_ref(right, ctx, out)?;
			if let Some(cond) = condition {
				match cond {
					crate::ast::stmt::JoinCondition::On(expr) => {
						out.push_str(" ON ");
						translate_expr(expr, ctx, out)?;
					}
					crate::ast::stmt::JoinCondition::Using(cols) => {
						out.push_str(" ON ");
						for (i, col) in cols.iter().enumerate() {
							if i > 0 {
								out.push_str(" AND ");
							}
							write!(out, "{col} == {col}")?;
						}
					}
				}
			}
			Ok(())
		}
	}
}

fn translate_order_by(
	orders: &[OrderByExpr],
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	for (i, o) in orders.iter().enumerate() {
		if i > 0 {
			out.push_str(", ");
		}
		translate_expr(&o.expr, ctx, out)?;
		if o.asc {
			out.push_str(" ASC");
		} else {
			out.push_str(" DESC");
		}
	}
	Ok(())
}

fn translate_limit(
	lim: &LimitClause,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	out.push_str(" LIMIT ");
	translate_expr(&lim.count, ctx, out)?;
	if let Some(off) = &lim.offset {
		out.push_str(" START ");
		translate_expr(off, ctx, out)?;
	}
	Ok(())
}

fn translate_qualified_name(
	name: &QualifiedName,
	out: &mut String,
) -> Result<(), TranslationError> {
	for (i, part) in name.parts.iter().enumerate() {
		if i > 0 {
			out.push(':');
		}
		out.push_str(part);
	}
	Ok(())
}

pub fn translate_create_table(
	crt: &CreateTableStmt,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	out.push_str("DEFINE TABLE ");
	translate_qualified_name(&crt.name, out)?;
	out.push_str(" SCHEMAFULL;\n");
	for col in &crt.columns {
		write!(out, "DEFINE FIELD {} ON TABLE ", col.name)?;
		translate_qualified_name(&crt.name, out)?;
		writeln!(out, " TYPE {};", map_mysql_type_to_surreal(&col.data_type))?;
		if let Some(def) = &col.default {
			write!(out, "DEFINE FIELD {} ON TABLE ", col.name)?;
			translate_qualified_name(&crt.name, out)?;
			out.push_str(" DEFAULT ");
			translate_expr(def, ctx, out)?;
			out.push_str(";\n");
		}
	}
	Ok(())
}

fn map_mysql_type_to_surreal(data_type: &str) -> &'static str {
	let lower = data_type.to_ascii_lowercase();
	if lower.starts_with("int")
		|| lower.starts_with("tinyint")
		|| lower.starts_with("smallint")
		|| lower.starts_with("bigint")
	{
		"int"
	} else if lower.starts_with("float")
		|| lower.starts_with("double")
		|| lower.starts_with("decimal")
	{
		"float"
	} else if lower.starts_with("bool") {
		"bool"
	} else if lower.starts_with("json") {
		"object"
	} else if lower.starts_with("datetime") || lower.starts_with("timestamp") {
		"datetime"
	} else {
		"string"
	}
}

pub fn translate_drop_table(drp: &DropTableStmt, out: &mut String) -> Result<(), TranslationError> {
	for name in &drp.names {
		out.push_str("REMOVE TABLE ");
		translate_qualified_name(name, out)?;
		out.push_str(";\n");
	}
	Ok(())
}

pub fn translate_use(use_stmt: &UseStmt, out: &mut String) -> Result<(), TranslationError> {
	write!(out, "USE DB {};", use_stmt.database)?;
	Ok(())
}

pub fn translate_set(
	set: &SetStmt,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	for (target, val) in &set.assignments {
		match target {
			VariableTarget::User(name) => {
				write!(out, "LET ${name} = ")?;
				translate_expr(val, ctx, out)?;
				out.push_str(";\n");
			}
			VariableTarget::System {
				name,
				..
			} => {
				writeln!(out, "-- SET @@{name};")?;
			}
		}
	}
	Ok(())
}

pub fn translate_show(show: &ShowStmt, out: &mut String) -> Result<(), TranslationError> {
	match show {
		ShowStmt::Tables {
			..
		} => out.push_str("INFO FOR DB;"),
		ShowStmt::Databases {
			..
		} => out.push_str("INFO FOR ROOT;"),
		ShowStmt::Columns {
			table,
			..
		} => {
			out.push_str("INFO FOR TABLE ");
			translate_qualified_name(table, out)?;
			out.push(';');
		}
		ShowStmt::CreateTable {
			table,
			..
		} => {
			out.push_str("INFO FOR TABLE ");
			translate_qualified_name(table, out)?;
			out.push(';');
		}
		ShowStmt::Variables {
			..
		} => out.push_str("SELECT * FROM (SELECT 'character_set_client' AS Variable_name, 'utf8mb4' AS Value);"),
		ShowStmt::Status {
			..
		} => out.push_str("SELECT * FROM (SELECT 'Uptime' AS Variable_name, '3600' AS Value);"),
		ShowStmt::Processlist {
			..
		} => out.push_str("SELECT 1 AS Id, 'root' AS User, 'localhost' AS Host, NULL AS db, 'Query' AS Command, 0 AS Time, 'executing' AS State, 'SHOW PROCESSLIST' AS Info;"),
		ShowStmt::Warnings {
			..
		} => out.push_str("SELECT 'Note' AS Level, 0 AS Code, 'Success' AS Message WHERE false;"),
		ShowStmt::Errors {
			..
		} => out.push_str("SELECT 'Error' AS Level, 0 AS Code, 'None' AS Message WHERE false;"),
	}
	Ok(())
}

pub fn translate_describe(desc: &DescribeStmt, out: &mut String) -> Result<(), TranslationError> {
	out.push_str("INFO FOR TABLE ");
	translate_qualified_name(&desc.table, out)?;
	out.push(';');
	Ok(())
}

pub fn translate_explain(
	exp: &ExplainStmt,
	ctx: &TranslationContext,
	out: &mut String,
) -> Result<(), TranslationError> {
	translate_statement(&exp.statement, ctx, out)?;
	// Strip trailing ';' and add EXPLAIN
	if out.ends_with(';') {
		out.pop();
	}
	out.push_str(" EXPLAIN;");
	Ok(())
}

pub fn translate_transaction(
	tx: &TransactionStmt,
	out: &mut String,
) -> Result<(), TranslationError> {
	match tx {
		TransactionStmt::Begin(_)
		| TransactionStmt::StartTransaction {
			..
		} => {
			out.push_str("BEGIN TRANSACTION;");
		}
		TransactionStmt::Commit(_) => {
			out.push_str("COMMIT TRANSACTION;");
		}
		TransactionStmt::Rollback(_) => {
			out.push_str("CANCEL TRANSACTION;");
		}
	}
	Ok(())
}
