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
		match self.peek_keyword() {
			Some(Keyword::Select) => Ok(Statement::Select(Box::new(self.parse_select_stmt()?))),
			Some(Keyword::Insert) => Ok(Statement::Insert(Box::new(self.parse_insert_stmt()?))),
			Some(Keyword::Update) => Ok(Statement::Update(Box::new(self.parse_update_stmt()?))),
			Some(Keyword::Delete) => Ok(Statement::Delete(Box::new(self.parse_delete_stmt()?))),
			Some(Keyword::Replace) => Ok(Statement::Replace(Box::new(self.parse_replace_stmt()?))),
			Some(Keyword::Create) => {
				Ok(Statement::CreateTable(Box::new(self.parse_create_stmt()?)))
			}
			Some(Keyword::Drop) => Ok(Statement::DropTable(Box::new(self.parse_drop_stmt()?))),
			Some(Keyword::Use) => Ok(Statement::Use(self.parse_use_stmt()?)),
			Some(Keyword::Set) => Ok(Statement::Set(self.parse_set_stmt()?)),
			Some(Keyword::Show) => Ok(Statement::Show(self.parse_show_stmt()?)),
			Some(Keyword::Describe) | Some(Keyword::Desc) => {
				Ok(Statement::Describe(self.parse_describe_stmt()?))
			}
			Some(Keyword::Explain) => Ok(Statement::Explain(Box::new(self.parse_explain_stmt()?))),
			Some(Keyword::Start)
			| Some(Keyword::Begin)
			| Some(Keyword::Commit)
			| Some(Keyword::Rollback) => Ok(Statement::Transaction(self.parse_transaction_stmt()?)),
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
