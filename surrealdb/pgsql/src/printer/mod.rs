//! Faithful PostgreSQL AST printer / formatter.
//!
//! Produces standard PostgreSQL SQL matching the input AST structure,
//! guaranteeing round-trip fidelity: `parse(print(ast)) == ast`.

pub mod expr;
pub mod stmt;

use core::fmt;

pub use expr::{is_safe_unquoted_ident, print_ident, print_string_literal};

/// Trait for formatting AST nodes into standard PostgreSQL SQL text.
pub trait ToPgSql {
	/// Format the AST node into the provided formatter.
	fn to_pgsql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;

	/// Convert the AST node directly to a PostgreSQL SQL string.
	fn to_pgsql_string(&self) -> String {
		struct Adapter<'a, T: ?Sized>(&'a T);
		impl<'a, T: ToPgSql + ?Sized> fmt::Display for Adapter<'a, T> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				self.0.to_pgsql(f)
			}
		}
		format!("{}", Adapter(self))
	}
}

// Implement core::fmt::Display for all AST types via ToPgSql
macro_rules! impl_display {
	($($t:ty),* $(,)?) => {
		$(
			impl fmt::Display for $t {
				fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
					$crate::printer::ToPgSql::to_pgsql(self, f)
				}
			}
		)*
	};
}

impl_display!(
	crate::ast::expr::ColumnRef,
	crate::ast::expr::QualifiedName,
	crate::ast::expr::TypeName,
	crate::ast::expr::WhenClause,
	crate::ast::expr::Expr,
	crate::ast::stmt::ResTarget,
	crate::ast::stmt::TableAlias,
	crate::ast::expr::OrderByItem,
	crate::ast::stmt::JoinType,
	crate::ast::stmt::JoinCondition,
	crate::ast::stmt::TableRef,
	crate::ast::stmt::SetOpKind,
	crate::ast::stmt::SetOperation,
	crate::ast::stmt::SelectStmt,
	crate::ast::stmt::SetClause,
	crate::ast::stmt::OnConflictAction,
	crate::ast::stmt::OnConflictClause,
	crate::ast::stmt::InsertStmt,
	crate::ast::stmt::UpdateStmt,
	crate::ast::stmt::DeleteStmt,
	crate::ast::stmt::ColumnConstraint,
	crate::ast::stmt::ColumnDef,
	crate::ast::stmt::TableConstraint,
	crate::ast::stmt::CreateTableStmt,
	crate::ast::stmt::DropObjectType,
	crate::ast::stmt::DropStmt,
	crate::ast::stmt::TransactionStmt,
	crate::ast::stmt::SetStmt,
	crate::ast::stmt::ShowStmt,
	crate::ast::stmt::ResetStmt,
	crate::ast::stmt::ExplainStmt,
	crate::ast::expr::WindowSpec,
	crate::ast::expr::WindowFrame,
	crate::ast::expr::WindowFrameBound,
	crate::ast::stmt::WithClause,
	crate::ast::stmt::CommonTableExpr,
	crate::ast::stmt::WindowDef,
	crate::ast::stmt::LockingClause,
	crate::ast::stmt::CreateIndexStmt,
	crate::ast::stmt::IndexElem,
	crate::ast::stmt::CreateViewStmt,
	crate::ast::stmt::AlterTableStmt,
	crate::ast::stmt::AlterTableAction,
	crate::ast::stmt::CreateSchemaStmt,
	crate::ast::stmt::CreateExtensionStmt,
	crate::ast::stmt::CreateTypeStmt,
	crate::ast::stmt::AlterTypeStmt,
	crate::ast::stmt::CreateDomainStmt,
	crate::ast::stmt::AlterDomainStmt,
	crate::ast::stmt::CreateCompositeTypeStmt,
	crate::ast::stmt::PrepareStmt,
	crate::ast::stmt::ExecuteStmt,
	crate::ast::stmt::DeallocateStmt,
	crate::ast::stmt::Statement,
);
