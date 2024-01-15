use crate::{
	sql::{
		statements::{
			remove::RemoveAnalyzerStatement, RemoveDatabaseStatement, RemoveEventStatement,
			RemoveFieldStatement, RemoveFunctionStatement, RemoveIndexStatement,
			RemoveNamespaceStatement, RemoveParamStatement, RemoveScopeStatement, RemoveStatement,
			RemoveUserStatement,
		},
		Param,
	},
	syn::v2::{
		parser::{
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::t,
	},
};

impl Parser<'_> {
	pub fn parse_remove_stmt(&mut self) -> ParseResult<RemoveStatement> {
		let res = match self.next().kind {
			t!("NAMESPACE") => {
				let name = self.next_token_value()?;
				RemoveStatement::Namespace(RemoveNamespaceStatement {
					name,
				})
			}
			t!("DATABASE") => {
				let name = self.next_token_value()?;
				RemoveStatement::Database(RemoveDatabaseStatement {
					name,
				})
			}
			t!("FUNCTION") => {
				let name = self.parse_custom_function_name()?;
				let next = self.peek();
				if self.eat(t!("(")) {
					self.expect_closing_delimiter(t!(")"), next.span)?;
				}
				RemoveStatement::Function(RemoveFunctionStatement {
					name,
				})
			}
			t!("TOKEN") => {
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				let base = self.parse_base(true)?;
				RemoveStatement::Token(crate::sql::statements::RemoveTokenStatement {
					name,
					base,
				})
			}
			t!("SCOPE") => {
				let name = self.next_token_value()?;
				RemoveStatement::Scope(RemoveScopeStatement {
					name,
				})
			}
			t!("PARAM") => {
				let name = self.next_token_value::<Param>()?;
				RemoveStatement::Param(RemoveParamStatement {
					name: name.0,
				})
			}
			t!("TABLE") => {
				let name = self.next_token_value()?;
				RemoveStatement::Table(crate::sql::statements::RemoveTableStatement {
					name,
				})
			}
			t!("EVENT") => {
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let table = self.next_token_value()?;
				RemoveStatement::Event(RemoveEventStatement {
					name,
					what: table,
				})
			}
			t!("FIELD") => {
				let idiom = self.parse_local_idiom()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let table = self.next_token_value()?;
				RemoveStatement::Field(RemoveFieldStatement {
					name: idiom,
					what: table,
				})
			}
			t!("INDEX") => {
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let what = self.next_token_value()?;
				RemoveStatement::Index(RemoveIndexStatement {
					name,
					what,
				})
			}
			t!("ANALYZER") => {
				let name = self.next_token_value()?;
				RemoveStatement::Analyzer(RemoveAnalyzerStatement {
					name,
				})
			}
			t!("USER") => {
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				let base = self.parse_base(false)?;
				RemoveStatement::User(RemoveUserStatement {
					name,
					base,
				})
			}
			x => unexpected!(self, x, "a remove statement keyword"),
		};
		Ok(res)
	}
}
