//! Statement parsing module and dispatcher for MySQL.

pub mod ddl;
pub mod delete;
pub mod insert;
pub mod replace;
pub mod select;
pub mod show;
pub mod tx;
pub mod update;

use crate::ast::stmt::Statement;
use crate::lexer::keyword::Keyword;
use crate::lexer::token::TokenKind;
use crate::parser::Parser;
use crate::parser::error::{ErrorKind, ParseError, ParseResult};

impl<'a> Parser<'a> {
	/// Parse a single SQL statement.
	pub fn parse_statement(&mut self) -> ParseResult<Statement> {
		self.check_recursion_limit()?;
		self.recursion_depth += 1;
		let res = self.parse_statement_inner();
		self.recursion_depth -= 1;
		res
	}

	fn parse_statement_inner(&mut self) -> ParseResult<Statement> {
		if self.peek().kind == TokenKind::LParen {
			return Ok(Statement::Select(Box::new(self.parse_select_stmt()?)));
		}
		match self.peek_keyword() {
			Some(Keyword::Select) | Some(Keyword::With) => {
				Ok(Statement::Select(Box::new(self.parse_select_stmt()?)))
			}
			Some(Keyword::Insert) => Ok(Statement::Insert(Box::new(self.parse_insert_stmt()?))),
			Some(Keyword::Update) => Ok(Statement::Update(Box::new(self.parse_update_stmt()?))),
			Some(Keyword::Delete) => Ok(Statement::Delete(Box::new(self.parse_delete_stmt()?))),
			Some(Keyword::Replace) => Ok(Statement::Replace(Box::new(self.parse_replace_stmt()?))),
			Some(Keyword::Truncate) => {
				let start = self.bump().span;
				if self.peek_keyword() == Some(Keyword::Table) {
					self.bump();
				}
				let table = self.parse_qualified_name()?;
				let span = start.merge(table.span);
				Ok(Statement::Truncate(Box::new(crate::ast::stmt::TruncateStmt {
					table,
					span,
				})))
			}
			Some(Keyword::Create) => match self.peek_nth_keyword(1) {
				Some(Keyword::Database)
				| Some(Keyword::Databases)
				| Some(Keyword::Schema)
				| Some(Keyword::Schemas) => Ok(Statement::CreateDatabase(self.parse_create_database_stmt()?)),
				Some(Keyword::Unique) if self.peek_nth_keyword(2) == Some(Keyword::Index) => {
					Ok(Statement::CreateIndex(Box::new(self.parse_create_index_stmt(true)?)))
				}
				Some(Keyword::Index) => {
					Ok(Statement::CreateIndex(Box::new(self.parse_create_index_stmt(false)?)))
				}
				Some(Keyword::View) => {
					Ok(Statement::CreateView(Box::new(self.parse_create_view_stmt(false)?)))
				}
				Some(Keyword::Or)
					if self.peek_nth_keyword(2) == Some(Keyword::Replace)
						&& self.peek_nth_keyword(3) == Some(Keyword::View) =>
				{
					Ok(Statement::CreateView(Box::new(self.parse_create_view_stmt(true)?)))
				}
				Some(Keyword::User) => Ok(Statement::CreateUser(self.parse_create_user_stmt()?)),
				_ => Ok(Statement::CreateTable(Box::new(self.parse_create_stmt()?))),
			},
			Some(Keyword::Drop) => match self.peek_nth_keyword(1) {
				Some(Keyword::Database)
				| Some(Keyword::Databases)
				| Some(Keyword::Schema)
				| Some(Keyword::Schemas) => Ok(Statement::DropDatabase(self.parse_drop_database_stmt()?)),
				Some(Keyword::Index) => Ok(Statement::DropIndex(self.parse_drop_index_stmt()?)),
				Some(Keyword::View) => Ok(Statement::DropView(self.parse_drop_view_stmt()?)),
				Some(Keyword::User) => Ok(Statement::DropUser(self.parse_drop_user_stmt()?)),
				Some(Keyword::Prepare) => Ok(Statement::Deallocate(self.parse_deallocate_stmt()?)),
				_ => Ok(Statement::DropTable(Box::new(self.parse_drop_stmt()?))),
			},
			Some(Keyword::Alter) => {
				Ok(Statement::AlterTable(Box::new(self.parse_alter_table_stmt()?)))
			}
			Some(Keyword::Lock) => Ok(Statement::LockTables(self.parse_lock_tables_stmt()?)),
			Some(Keyword::Unlock) => Ok(Statement::UnlockTables(self.parse_unlock_tables_stmt()?)),
			Some(Keyword::Prepare) => Ok(Statement::Prepare(self.parse_prepare_stmt()?)),
			Some(Keyword::Execute) => Ok(Statement::Execute(self.parse_execute_stmt()?)),
			Some(Keyword::Deallocate) => Ok(Statement::Deallocate(self.parse_deallocate_stmt()?)),
			Some(Keyword::Grant) => Ok(Statement::Grant(self.parse_grant_stmt()?)),
			Some(Keyword::Revoke) => Ok(Statement::Revoke(self.parse_revoke_stmt()?)),
			Some(Keyword::Use) => Ok(Statement::Use(self.parse_use_stmt()?)),
			Some(Keyword::Set) => Ok(Statement::Set(self.parse_set_stmt()?)),
			Some(Keyword::Show) => Ok(Statement::Show(self.parse_show_stmt()?)),
			Some(Keyword::Describe) | Some(Keyword::Desc) => {
				Ok(Statement::Describe(self.parse_describe_stmt()?))
			}
			// `EXPLAIN <table>` is `DESCRIBE <table>`.
			Some(Keyword::Explain) if matches!(self.peek_nth(1).kind, TokenKind::Ident(_)) => {
				Ok(Statement::Describe(self.parse_describe_stmt()?))
			}
			Some(Keyword::Explain) => Ok(Statement::Explain(Box::new(self.parse_explain_stmt()?))),
			Some(Keyword::Start)
			| Some(Keyword::Begin)
			| Some(Keyword::Commit)
			| Some(Keyword::Rollback)
			| Some(Keyword::Savepoint)
			| Some(Keyword::Release) => Ok(Statement::Transaction(self.parse_transaction_stmt()?)),
			_ => Err(ParseError::new(
				self.peek().span,
				ErrorKind::UnexpectedToken {
					expected: "SQL statement (SELECT, INSERT, UPDATE, DELETE, etc.)",
					found: self.peek().kind.to_string(),
				},
			)),
		}
	}
}
