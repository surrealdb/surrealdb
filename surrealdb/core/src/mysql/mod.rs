//! MySQL SQL front-end for SurrealDB.
//!
//! Provides direct AST-to-LogicalOp lowering for MySQL queries,
//! executing read operations on the engine without textual
//! serialization or re-parsing.

pub mod lower;

use anyhow::Result;

use crate::dbs::{Capabilities, TopLevelStatement};
use crate::expr::logical::LogicalOp;
use crate::syn::{self, ParserConfig};

/// Parse MySQL SQL text into LogicalOp plans for all planned statements.
pub fn parse_to_plan(
	input: &str,
	_capabilities: &Capabilities,
	_config: &ParserConfig,
) -> Result<Vec<LogicalOp>> {
	let stmts = surrealdb_mysql::parse_statements(input)
		.map_err(|e| anyhow::anyhow!("MySQL parse error: {e}"))?;

	let mut plans = Vec::new();
	for stmt in &stmts {
		match lower::lower_stmt(stmt) {
			Ok(plan) => plans.push(plan),
			Err(e) => anyhow::bail!("Statement does not support logical planning: {e:?}"),
		}
	}
	Ok(plans)
}

/// Parse MySQL SQL text and produce TopLevelStatements.
///
/// Statements with direct logical forms (like SELECT) are lowered directly
/// to TopLevelStatement::Plan(Box<LogicalOp>), bypassing SurrealQL text translation.
/// Mutations and DDL statements are lowered to SurrealQL statements, which run on the engine.
pub(crate) fn parse_to_statements(
	input: &str,
	capabilities: &Capabilities,
	config: &ParserConfig,
) -> Result<Vec<TopLevelStatement>> {
	let stmts = parse(input)?;
	lower_statements(&stmts, &surrealdb_sqlplan::EmptyCatalog, capabilities, config)
}

/// Parse MySQL SQL text into its statements, without lowering them.
pub(crate) fn parse(input: &str) -> Result<Vec<surrealdb_mysql::ast::stmt::Statement>> {
	surrealdb_mysql::parse_statements(input).map_err(|e| anyhow::anyhow!("MySQL parse error: {e}"))
}

/// Lower parsed MySQL statements to the statements the executor runs.
///
/// `catalog` describes the tables the statements name, which decides how a
/// column that holds a record link is compared and assigned; see
/// [`surrealdb_sqlplan::BinderCatalog::link_table`].
pub(crate) fn lower_statements(
	stmts: &[surrealdb_mysql::ast::stmt::Statement],
	catalog: &dyn surrealdb_sqlplan::BinderCatalog,
	capabilities: &Capabilities,
	config: &ParserConfig,
) -> Result<Vec<TopLevelStatement>> {
	let mut statements = Vec::new();
	for stmt in stmts {
		match stmt {
			surrealdb_mysql::ast::stmt::Statement::Select(select) => {
				let plan = lower::lower_select_with(select, catalog)
					.map_err(|e| anyhow::anyhow!("MySQL SELECT lowering error: {e}"))?;
				statements.push(TopLevelStatement::Plan(Box::new(plan)));
			}
			surrealdb_mysql::ast::stmt::Statement::Insert(insert) => {
				let expr = surrealdb_sqlplan::lower_mysql_insert(insert, catalog)
					.map_err(|e| anyhow::anyhow!("MySQL INSERT lowering error: {e}"))?;
				statements
					.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
			}
			surrealdb_mysql::ast::stmt::Statement::Replace(replace) => {
				let expr = surrealdb_sqlplan::lower_mysql_replace(replace, catalog)
					.map_err(|e| anyhow::anyhow!("MySQL REPLACE lowering error: {e}"))?;
				statements
					.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
			}
			surrealdb_mysql::ast::stmt::Statement::Update(update) => {
				let expr = surrealdb_sqlplan::lower_mysql_update(update, catalog)
					.map_err(|e| anyhow::anyhow!("MySQL UPDATE lowering error: {e}"))?;
				statements
					.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
			}
			surrealdb_mysql::ast::stmt::Statement::Delete(delete) => {
				let expr = surrealdb_sqlplan::lower_mysql_delete(delete, catalog)
					.map_err(|e| anyhow::anyhow!("MySQL DELETE lowering error: {e}"))?;
				statements
					.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
			}
			surrealdb_mysql::ast::stmt::Statement::Truncate(truncate) => {
				surrealdb_sqlplan::resolve_mysql_table_name(&truncate.table.parts, Some(catalog))
					.map_err(|e| anyhow::anyhow!("MySQL TRUNCATE lowering error: {e}"))?;
				let expr = surrealdb_sqlplan::lower_mysql_truncate(truncate)
					.map_err(|e| anyhow::anyhow!("MySQL TRUNCATE lowering error: {e}"))?;
				statements
					.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
			}
			surrealdb_mysql::ast::stmt::Statement::CreateTable(create) => {
				surrealdb_sqlplan::resolve_mysql_table_name(&create.name.parts, Some(catalog))
					.map_err(|e| anyhow::anyhow!("MySQL DDL lowering error: {e}"))?;
				let exprs = surrealdb_sqlplan::lower_mysql_ddl(stmt)
					.map_err(|e| anyhow::anyhow!("MySQL DDL lowering error: {e}"))?;
				for expr in exprs {
					statements
						.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
				}
			}
			surrealdb_mysql::ast::stmt::Statement::DropTable(drop) => {
				for name in &drop.names {
					surrealdb_sqlplan::resolve_mysql_table_name(&name.parts, Some(catalog))
						.map_err(|e| anyhow::anyhow!("MySQL DDL lowering error: {e}"))?;
				}
				let exprs = surrealdb_sqlplan::lower_mysql_ddl(stmt)
					.map_err(|e| anyhow::anyhow!("MySQL DDL lowering error: {e}"))?;
				for expr in exprs {
					statements
						.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
				}
			}
			surrealdb_mysql::ast::stmt::Statement::Use(use_stmt) => {
				let use_expr = surrealdb_sqlplan::lower_mysql_use(use_stmt)
					.map_err(|e| anyhow::anyhow!("MySQL USE lowering error: {e}"))?;
				statements
					.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Use(use_expr)));
			}
			surrealdb_mysql::ast::stmt::Statement::CreateDatabase(_)
			| surrealdb_mysql::ast::stmt::Statement::DropDatabase(_)
			| surrealdb_mysql::ast::stmt::Statement::CreateIndex(_)
			| surrealdb_mysql::ast::stmt::Statement::DropIndex(_)
			| surrealdb_mysql::ast::stmt::Statement::CreateView(_)
			| surrealdb_mysql::ast::stmt::Statement::DropView(_)
			| surrealdb_mysql::ast::stmt::Statement::AlterTable(_)
			| surrealdb_mysql::ast::stmt::Statement::CreateUser(_)
			| surrealdb_mysql::ast::stmt::Statement::DropUser(_)
			| surrealdb_mysql::ast::stmt::Statement::Grant(_)
			| surrealdb_mysql::ast::stmt::Statement::Revoke(_)
			| surrealdb_mysql::ast::stmt::Statement::LockTables(_)
			| surrealdb_mysql::ast::stmt::Statement::UnlockTables(_)
			| surrealdb_mysql::ast::stmt::Statement::Prepare(_)
			| surrealdb_mysql::ast::stmt::Statement::Execute(_)
			| surrealdb_mysql::ast::stmt::Statement::Deallocate(_)
			| surrealdb_mysql::ast::stmt::Statement::Set(_)
			| surrealdb_mysql::ast::stmt::Statement::Show(_)
			| surrealdb_mysql::ast::stmt::Statement::Describe(_)
			| surrealdb_mysql::ast::stmt::Statement::Explain(_)
			| surrealdb_mysql::ast::stmt::Statement::Transaction(_) => {
				if let surrealdb_mysql::ast::stmt::Statement::CreateIndex(idx) = stmt {
					surrealdb_sqlplan::resolve_mysql_table_name(&idx.table.parts, Some(catalog))
						.map_err(|e| anyhow::anyhow!("MySQL DDL lowering error: {e}"))?;
				} else if let surrealdb_mysql::ast::stmt::Statement::DropIndex(drop) = stmt {
					surrealdb_sqlplan::resolve_mysql_table_name(&drop.table.parts, Some(catalog))
						.map_err(|e| anyhow::anyhow!("MySQL DDL lowering error: {e}"))?;
				} else if let surrealdb_mysql::ast::stmt::Statement::CreateView(view) = stmt {
					surrealdb_sqlplan::resolve_mysql_table_name(&view.name.parts, Some(catalog))
						.map_err(|e| anyhow::anyhow!("MySQL DDL lowering error: {e}"))?;
					if let Some(surrealdb_mysql::ast::stmt::TableRef::Table {
						name,
						..
					}) = &view.query.from
					{
						surrealdb_sqlplan::resolve_mysql_table_name(&name.parts, Some(catalog))
							.map_err(|e| anyhow::anyhow!("MySQL DDL lowering error: {e}"))?;
					}
				} else if let surrealdb_mysql::ast::stmt::Statement::DropView(drop) = stmt {
					for name in &drop.names {
						surrealdb_sqlplan::resolve_mysql_table_name(&name.parts, Some(catalog))
							.map_err(|e| anyhow::anyhow!("MySQL DDL lowering error: {e}"))?;
					}
				} else if let surrealdb_mysql::ast::stmt::Statement::AlterTable(alter) = stmt {
					surrealdb_sqlplan::resolve_mysql_table_name(&alter.table.parts, Some(catalog))
						.map_err(|e| anyhow::anyhow!("MySQL DDL lowering error: {e}"))?;
					for action in &alter.actions {
						if let surrealdb_mysql::ast::stmt::AlterTableAction::RenameTable {
							new_name,
						} = action
						{
							surrealdb_sqlplan::resolve_mysql_table_name(
								&new_name.parts,
								Some(catalog),
							)
							.map_err(|e| anyhow::anyhow!("MySQL DDL lowering error: {e}"))?;
						}
					}
				}
				let mut surql_text = String::new();
				surrealdb_mysql::translate::translate_statement(
					stmt,
					&surrealdb_mysql::TranslationContext::default(),
					&mut surql_text,
				)
				.map_err(|e| anyhow::anyhow!("Failed to translate MySQL: {e}"))?;

				let ast = syn::parse_with_capabilities(&surql_text, capabilities, config)
					.map_err(|e| anyhow::anyhow!("Failed to parse translated SurrealQL: {e}"))?;
				for expr in ast.expressions {
					statements.push(surface_statement(expr));
				}
			}
		}
	}
	Ok(statements)
}

fn surface_statement(stmt: impl Into<surrealdb_expr::parser::TopLevelExpr>) -> TopLevelStatement {
	TopLevelStatement::Ast(stmt.into())
}
