//! PostgreSQL SQL front-end for SurrealDB.
//!
//! Provides direct AST-to-LogicalOp lowering for PostgreSQL queries,
//! executing read operations on the engine without textual
//! serialization or re-parsing.

pub mod lower;

use std::collections::HashMap;

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
/// Mutations and DDL statements are lowered to SurrealQL statements, which run on the engine.
pub(crate) fn parse_to_statements(
	input: &str,
	capabilities: &Capabilities,
	config: &ParserConfig,
) -> Result<Vec<TopLevelStatement>> {
	let stmts = parse(input)?;
	lower_statements(&stmts, &surrealdb_sqlplan::EmptyCatalog, capabilities, config)
}

/// Parse PostgreSQL SQL text into its statements, without lowering them.
pub(crate) fn parse(input: &str) -> Result<Vec<surrealdb_pgsql::ast::stmt::Statement>> {
	surrealdb_pgsql::parse_statements(input)
		.map_err(|e| anyhow::anyhow!("PostgreSQL parse error: {e}"))
}

/// Lower parsed PostgreSQL statements to the statements the executor runs.
///
/// `catalog` describes the tables the statements name, which decides how a
/// column that holds a record link is compared and assigned; see
/// [`surrealdb_sqlplan::BinderCatalog::link_table`].
pub(crate) fn lower_statements(
	stmts: &[surrealdb_pgsql::ast::stmt::Statement],
	catalog: &dyn surrealdb_sqlplan::BinderCatalog,
	capabilities: &Capabilities,
	config: &ParserConfig,
) -> Result<Vec<TopLevelStatement>> {
	let mut statements = Vec::new();
	// The vector columns that earlier statements of the request define, which the catalog, read
	// before any of them runs, does not hold.
	let mut defined = VectorColumns::default();
	for stmt in stmts {
		let batch = BatchCatalog {
			inner: catalog,
			vectors: &defined,
		};
		let catalog: &dyn surrealdb_sqlplan::BinderCatalog = &batch;
		match stmt {
			surrealdb_pgsql::ast::stmt::Statement::Select(select) => {
				let plan = lower::lower_select_with(select, catalog)
					.map_err(|e| anyhow::anyhow!("PostgreSQL SELECT lowering error: {e}"))?;
				statements.push(TopLevelStatement::Plan(Box::new(plan)));
			}
			surrealdb_pgsql::ast::stmt::Statement::Insert(insert) => {
				let expr = surrealdb_sqlplan::lower_pgsql_insert(insert, catalog)
					.map_err(|e| anyhow::anyhow!("PostgreSQL INSERT lowering error: {e}"))?;
				statements
					.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
			}
			surrealdb_pgsql::ast::stmt::Statement::Update(update) => {
				let expr = surrealdb_sqlplan::lower_pgsql_update(update, catalog)
					.map_err(|e| anyhow::anyhow!("PostgreSQL UPDATE lowering error: {e}"))?;
				statements
					.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
			}
			surrealdb_pgsql::ast::stmt::Statement::Delete(delete) => {
				let expr = surrealdb_sqlplan::lower_pgsql_delete(delete, catalog)
					.map_err(|e| anyhow::anyhow!("PostgreSQL DELETE lowering error: {e}"))?;
				statements
					.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
			}
			surrealdb_pgsql::ast::stmt::Statement::Truncate(truncate) => {
				surrealdb_sqlplan::resolve_pgsql_table_name(&truncate.table.parts, Some(catalog))
					.map_err(|e| anyhow::anyhow!("PostgreSQL TRUNCATE lowering error: {e}"))?;
				let expr = surrealdb_sqlplan::lower_pgsql_truncate(truncate)
					.map_err(|e| anyhow::anyhow!("PostgreSQL TRUNCATE lowering error: {e}"))?;
				statements
					.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
			}
			surrealdb_pgsql::ast::stmt::Statement::CreateTable(create) => {
				let (_, table) =
					surrealdb_sqlplan::resolve_pgsql_table_name(&create.name.parts, Some(catalog))
						.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
				let exprs = surrealdb_sqlplan::lower_pgsql_ddl(stmt)
					.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
				// With `IF NOT EXISTS` the table may already exist; the definition is taken to
				// apply, as the usual use of the clause is to create the table.
				defined.add_columns(table.as_str(), create.columns.iter());
				for expr in exprs {
					statements
						.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
				}
			}
			surrealdb_pgsql::ast::stmt::Statement::CreateIndex(idx) => {
				let (_, table) =
					surrealdb_sqlplan::resolve_pgsql_table_name(&idx.table.parts, Some(catalog))
						.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
				let _ = table;
				let dimensions =
					|table: &str, column: &str| catalog.vector_dimension(None, table, column);
				let exprs = surrealdb_sqlplan::lower_pgsql_ddl_over(stmt, &dimensions)
					.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
				for expr in exprs {
					statements
						.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
				}
			}
			surrealdb_pgsql::ast::stmt::Statement::CreateView(view) => {
				surrealdb_sqlplan::resolve_pgsql_table_name(&view.name.parts, Some(catalog))
					.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
				for tr in &view.query.from {
					if let surrealdb_pgsql::ast::stmt::TableRef::Table {
						name,
						..
					} = tr
					{
						surrealdb_sqlplan::resolve_pgsql_table_name(&name.parts, Some(catalog))
							.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
					}
				}
				let exprs = surrealdb_sqlplan::lower_pgsql_ddl(stmt)
					.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
				for expr in exprs {
					statements
						.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
				}
			}
			surrealdb_pgsql::ast::stmt::Statement::AlterTable(alter) => {
				let (_, table) =
					surrealdb_sqlplan::resolve_pgsql_table_name(&alter.name.parts, Some(catalog))
						.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
				let exprs = surrealdb_sqlplan::lower_pgsql_ddl(stmt)
					.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
				for action in &alter.actions {
					defined.apply(table.as_str(), action);
				}
				for expr in exprs {
					statements
						.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
				}
			}
			surrealdb_pgsql::ast::stmt::Statement::Drop(drop) => {
				for name in &drop.names {
					surrealdb_sqlplan::resolve_pgsql_table_name(&name.parts, Some(catalog))
						.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
				}
				let exprs = surrealdb_sqlplan::lower_pgsql_ddl(stmt)
					.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
				for expr in exprs {
					statements
						.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
				}
			}
			surrealdb_pgsql::ast::stmt::Statement::Explain(explain)
				if matches!(
					*explain.statement,
					surrealdb_pgsql::ast::stmt::Statement::Select(_)
				) =>
			{
				// The plan described is the one the select runs as, not a translation of it.
				let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &*explain.statement
				else {
					unreachable!("the guard matched a select");
				};
				let plan = lower::lower_select_with(select, catalog)
					.map_err(|e| anyhow::anyhow!("PostgreSQL SELECT lowering error: {e}"))?;
				let analyze =
					explain.options.iter().any(|option| option.eq_ignore_ascii_case("analyze"));
				statements.push(TopLevelStatement::Plan(Box::new(LogicalOp::Explain(
					crate::expr::logical::LogicalExplain {
						input: Box::new(plan),
						analyze,
						format: crate::expr::ExplainFormat::Text,
					},
				))));
			}
			surrealdb_pgsql::ast::stmt::Statement::CreateExtension(_) => {
				let exprs = surrealdb_sqlplan::lower_pgsql_ddl(stmt)
					.map_err(|e| anyhow::anyhow!("PostgreSQL DDL lowering error: {e}"))?;
				for expr in exprs {
					statements
						.push(surface_statement(surrealdb_expr::parser::TopLevelExpr::Expr(expr)));
				}
			}
			surrealdb_pgsql::ast::stmt::Statement::Copy(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreateSchema(_)
			| surrealdb_pgsql::ast::stmt::Statement::Transaction(_)
			| surrealdb_pgsql::ast::stmt::Statement::Set(_)
			| surrealdb_pgsql::ast::stmt::Statement::Show(_)
			| surrealdb_pgsql::ast::stmt::Statement::Reset(_)
			| surrealdb_pgsql::ast::stmt::Statement::Explain(_)
			| surrealdb_pgsql::ast::stmt::Statement::Prepare(_)
			| surrealdb_pgsql::ast::stmt::Statement::Execute(_)
			| surrealdb_pgsql::ast::stmt::Statement::Deallocate(_)
			| surrealdb_pgsql::ast::stmt::Statement::CreatePropertyGraph(_)
			| surrealdb_pgsql::ast::stmt::Statement::DropPropertyGraph(_)
			| surrealdb_pgsql::ast::stmt::Statement::Empty => {
				let mut surql_text = String::new();
				surrealdb_pgsql::translate::translate_statement(
					stmt,
					&surrealdb_pgsql::TranslationContext::default(),
					&mut surql_text,
				)
				.map_err(|e| anyhow::anyhow!("Failed to translate PostgreSQL: {e}"))?;

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

/// What statements of a request say of the columns they define, by table and column: the
/// dimensions of a vector column, or `None` for a column that is not one, which the catalog may
/// still hold as one.
#[derive(Default)]
struct VectorColumns(HashMap<(String, String), Option<usize>>);

impl VectorColumns {
	/// Note the columns of `columns`, which are added to `table`.
	fn add_columns<'a>(
		&mut self,
		table: &str,
		columns: impl Iterator<Item = &'a surrealdb_pgsql::ast::stmt::ColumnDef>,
	) {
		for column in columns {
			self.set(table, &column.name, &column.data_type);
		}
	}

	/// Note that `column` of `table` has the type `data_type`.
	fn set(&mut self, table: &str, column: &str, data_type: &surrealdb_pgsql::ast::expr::TypeName) {
		let kind = surrealdb_sqlplan::map_pgsql_type(data_type);
		let dimensions = if let crate::expr::Kind::Array(element, Some(count)) = kind
			&& matches!(*element, crate::expr::Kind::Float)
			&& data_type.array_bounds.is_empty()
		{
			usize::try_from(count).ok()
		} else {
			None
		};
		self.0.insert((table.to_owned(), column.to_owned()), dimensions);
	}

	/// Note what the action of an `ALTER TABLE` of `table` does to its columns.
	fn apply(&mut self, table: &str, action: &surrealdb_pgsql::ast::stmt::AlterTableAction) {
		use surrealdb_pgsql::ast::stmt::AlterTableAction;
		if let AlterTableAction::AddColumn {
			column,
			..
		} = action
		{
			self.set(table, &column.name, &column.data_type);
		} else if let AlterTableAction::AlterColumnType {
			name,
			data_type,
		} = action
		{
			self.set(table, name, data_type);
		} else if let AlterTableAction::DropColumn {
			name,
			..
		} = action
		{
			self.forget(table, name);
		}
	}

	/// Note that `column` of `table` is dropped.
	fn forget(&mut self, table: &str, column: &str) {
		self.0.insert((table.to_owned(), column.to_owned()), None);
	}

	/// What the request says of `column` of `table`, if anything.
	fn get(&self, table: &str, column: &str) -> Option<Option<usize>> {
		self.0.get(&(table.to_owned(), column.to_owned())).copied()
	}
}

/// A catalog that describes, besides what `inner` does, the vector columns that earlier
/// statements of the request define.
struct BatchCatalog<'a> {
	inner: &'a dyn surrealdb_sqlplan::BinderCatalog,
	vectors: &'a VectorColumns,
}

impl surrealdb_sqlplan::BinderCatalog for BatchCatalog<'_> {
	fn get_table(
		&self,
		schema: Option<&str>,
		name: &str,
	) -> Option<surrealdb_sqlplan::TableSchema> {
		self.inner.get_table(schema, name)
	}

	fn table_exists(&self, schema: Option<&str>, name: &str) -> bool {
		self.inner.table_exists(schema, name)
	}

	fn current_database(&self) -> Option<&str> {
		self.inner.current_database()
	}

	fn all_tables(&self) -> Vec<surrealdb_sqlplan::TableSchema> {
		self.inner.all_tables()
	}

	fn column_type(
		&self,
		schema: Option<&str>,
		table: &str,
		column: &str,
	) -> surrealdb_sqlplan::SqlType {
		match self.vectors.get(table, column) {
			Some(Some(_)) => surrealdb_sqlplan::SqlType::Vector,
			Some(None) => surrealdb_sqlplan::SqlType::Unknown,
			None => self.inner.column_type(schema, table, column),
		}
	}

	fn link_table(&self, schema: Option<&str>, table: &str, column: &str) -> Option<String> {
		self.inner.link_table(schema, table, column)
	}

	fn hnsw_indexes(
		&self,
		schema: Option<&str>,
		table: &str,
	) -> Vec<(String, surrealdb_sqlplan::vector::VectorMetric)> {
		self.inner.hnsw_indexes(schema, table)
	}

	fn vector_dimension(&self, schema: Option<&str>, table: &str, column: &str) -> Option<usize> {
		match self.vectors.get(table, column) {
			Some(dimensions) => dimensions,
			None => self.inner.vector_dimension(schema, table, column),
		}
	}
}

#[cfg(test)]
mod batch_tests {
	use surrealdb_sqlplan::BinderCatalog;

	use super::*;

	fn catalog_after(sql: &str) -> VectorColumns {
		let mut defined = VectorColumns::default();
		for stmt in surrealdb_pgsql::parse_statements(sql).expect("parse") {
			if let surrealdb_pgsql::ast::stmt::Statement::CreateTable(create) = &stmt {
				defined.add_columns("t", create.columns.iter());
			} else if let surrealdb_pgsql::ast::stmt::Statement::AlterTable(alter) = &stmt {
				for action in &alter.actions {
					defined.apply("t", action);
				}
			}
		}
		defined
	}

	#[test]
	fn a_column_type_change_in_the_batch_replaces_the_vector_it_was() {
		let defined = catalog_after(
			"CREATE TABLE t (v vector(2), w text); ALTER TABLE t ALTER COLUMN v TYPE text; ALTER TABLE t ALTER COLUMN w TYPE vector(3)",
		);
		let catalog = BatchCatalog {
			inner: &surrealdb_sqlplan::EmptyCatalog,
			vectors: &defined,
		};
		assert_eq!(catalog.vector_dimension(None, "t", "v"), None);
		assert_eq!(catalog.vector_dimension(None, "t", "w"), Some(3));
	}

	#[test]
	fn a_dropped_column_is_no_longer_a_vector() {
		let defined = catalog_after("CREATE TABLE t (v vector(2)); ALTER TABLE t DROP COLUMN v");
		let catalog = BatchCatalog {
			inner: &surrealdb_sqlplan::EmptyCatalog,
			vectors: &defined,
		};
		assert_eq!(catalog.vector_dimension(None, "t", "v"), None);
	}
}
