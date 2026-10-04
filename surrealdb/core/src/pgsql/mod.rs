//! PostgreSQL SQL front-end for SurrealDB.
//!
//! Provides direct AST-to-LogicalOp lowering for PostgreSQL queries,
//! executing read operations on the streaming engine without textual
//! serialization or re-parsing.

pub mod lower;

use anyhow::Result;

use crate::dbs::{Capabilities, TopLevelStatement};
use crate::expr::logical::LogicalOp;
use crate::syn::{self, ParserConfig};

/// Parse PostgreSQL SQL text into LogicalOp plans for all planned statements.
pub fn parse_to_plan(
	input: &str,
	_capabilities: &Capabilities,
	_config: &ParserConfig,
) -> Result<Vec<LogicalOp>> {
	let stmts = surrealdb_pgsql::parse_statements(input)
		.map_err(|e| anyhow::anyhow!("PostgreSQL parse error: {e}"))?;

	let mut plans = Vec::new();
	for stmt in &stmts {
		match lower::lower_stmt(stmt) {
			Ok(plan) => plans.push(plan),
			Err(e) => anyhow::bail!("Statement does not support logical planning: {e:?}"),
		}
	}
	Ok(plans)
}

/// Parse PostgreSQL SQL text and produce TopLevelStatements.
///
/// Statements with direct logical forms (like SELECT) are lowered directly
/// to TopLevelStatement::Plan(Box<LogicalOp>), bypassing SurrealQL text translation.
/// Mutations and DDL statements fall back to the transactional engine.
pub(crate) fn parse_to_statements(
	input: &str,
	capabilities: &Capabilities,
	config: &ParserConfig,
) -> Result<Vec<TopLevelStatement>> {
	let stmts = surrealdb_pgsql::parse_statements(input)
		.map_err(|e| anyhow::anyhow!("PostgreSQL parse error: {e}"))?;

	let mut statements = Vec::new();
	for stmt in stmts {
		match &stmt {
			surrealdb_pgsql::ast::stmt::Statement::Select(select) => {
				let plan = lower::lower_select(select)
					.map_err(|e| anyhow::anyhow!("PostgreSQL SELECT lowering error: {e}"))?;
				statements.push(TopLevelStatement::Plan(Box::new(plan)));
			}
			surrealdb_pgsql::ast::stmt::Statement::Insert(insert) => {
				let expr = surrealdb_sqlplan::lower_pgsql_insert(insert)
					.map_err(|e| anyhow::anyhow!("PostgreSQL INSERT lowering error: {e}"))?;
				statements.push(TopLevelStatement::Ast(crate::sql::TopLevelExpr::Expr(
					crate::sql::Expr::from(expr),
				)));
			}
			surrealdb_pgsql::ast::stmt::Statement::Update(update) => {
				let expr = surrealdb_sqlplan::lower_pgsql_update(update)
					.map_err(|e| anyhow::anyhow!("PostgreSQL UPDATE lowering error: {e}"))?;
				statements.push(TopLevelStatement::Ast(crate::sql::TopLevelExpr::Expr(
					crate::sql::Expr::from(expr),
				)));
			}
			surrealdb_pgsql::ast::stmt::Statement::Delete(delete) => {
				let expr = surrealdb_sqlplan::lower_pgsql_delete(delete)
					.map_err(|e| anyhow::anyhow!("PostgreSQL DELETE lowering error: {e}"))?;
				statements.push(TopLevelStatement::Ast(crate::sql::TopLevelExpr::Expr(
					crate::sql::Expr::from(expr),
				)));
			}
			surrealdb_pgsql::ast::stmt::Statement::Truncate(truncate) => {
				let expr = surrealdb_sqlplan::lower_pgsql_truncate(truncate)
					.map_err(|e| anyhow::anyhow!("PostgreSQL TRUNCATE lowering error: {e}"))?;
				statements.push(TopLevelStatement::Ast(crate::sql::TopLevelExpr::Expr(
					crate::sql::Expr::from(expr),
				)));
			}
			surrealdb_pgsql::ast::stmt::Statement::CreateTable(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreateIndex(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreateView(_)
			| surrealdb_pgsql::ast::stmt::Statement::AlterTable(_)
			| surrealdb_pgsql::ast::stmt::Statement::Drop(_) => {
				let exprs = surrealdb_sqlplan::lower_pgsql_ddl(&stmt)
					.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
				for expr in exprs {
					statements.push(TopLevelStatement::Ast(crate::sql::TopLevelExpr::Expr(
						crate::sql::Expr::from(expr),
					)));
				}
			}
			_ => {
				let mut surql_text = String::new();
				surrealdb_pgsql::translate::translate_statement(
					&stmt,
					&surrealdb_pgsql::TranslationContext::default(),
					&mut surql_text,
				)
				.map_err(|e| anyhow::anyhow!("Failed to translate PostgreSQL: {e}"))?;

				let ast = syn::parse_with_capabilities(&surql_text, capabilities, config)
					.map_err(|e| anyhow::anyhow!("Failed to parse translated SurrealQL: {e}"))?;
				for expr in ast.expressions {
					statements.push(TopLevelStatement::Ast(expr));
				}
			}
		}
	}
	Ok(statements)
}
