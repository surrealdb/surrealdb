use reblessive::Stk;

use crate::{
	sql::{
		statements::{
			remove::RemoveAnalyzerStatement, RemoveAccessStatement, RemoveDatabaseStatement,
			RemoveEventStatement, RemoveFieldStatement, RemoveFunctionStatement,
			RemoveIndexStatement, RemoveNamespaceStatement, RemoveParamStatement, RemoveStatement,
			RemoveUserStatement,
		},
		Param,
	},
	syn::{
		parser::{
			mac::{expected, unexpected},
			ParseResult, Parser,
		},
		token::t,
	},
};

impl Parser<'_> {
	pub async fn parse_remove_stmt(&mut self, ctx: &mut Stk) -> ParseResult<RemoveStatement> {
		let res = match self.next().kind {
			t!("NAMESPACE") | t!("ns") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;

				RemoveStatement::Namespace(RemoveNamespaceStatement {
					name,
					if_exists,
				})
			}
			t!("DATABASE") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;

				RemoveStatement::Database(RemoveDatabaseStatement {
					name,
					if_exists,
				})
			}
			t!("FUNCTION") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.parse_custom_function_name()?;
				let next = self.peek();
				if self.eat(t!("(")) {
					self.expect_closing_delimiter(t!(")"), next.span)?;
				}

				RemoveStatement::Function(RemoveFunctionStatement {
					name,
					if_exists,
				})
			}
			t!("ACCESS") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				let base = self.parse_base(false)?;

				RemoveStatement::Access(RemoveAccessStatement {
					name,
					base,
					if_exists,
				})
			}
			t!("PARAM") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value::<Param>()?;

				RemoveStatement::Param(RemoveParamStatement {
					name: name.0,
					if_exists,
				})
			}
			t!("TABLE") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;

				RemoveStatement::Table(crate::sql::statements::RemoveTableStatement {
					name,
					if_exists,
				})
			}
			t!("EVENT") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let table = self.next_token_value()?;

				RemoveStatement::Event(RemoveEventStatement {
					name,
					what: table,
					if_exists,
				})
			}
			t!("FIELD") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let idiom = self.parse_local_idiom(ctx).await?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let table = self.next_token_value()?;

				RemoveStatement::Field(RemoveFieldStatement {
					name: idiom,
					what: table,
					if_exists,
				})
			}
			t!("INDEX") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let what = self.next_token_value()?;

				RemoveStatement::Index(RemoveIndexStatement {
					name,
					what,
					if_exists,
				})
			}
			t!("ANALYZER") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;

				RemoveStatement::Analyzer(RemoveAnalyzerStatement {
					name,
					if_exists,
				})
			}
			t!("USER") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				let base = self.parse_base(false)?;

				RemoveStatement::User(RemoveUserStatement {
					name,
					base,
					if_exists,
				})
			}
			x => unexpected!(self, x, "a remove statement keyword"),
		};
		Ok(res)
	}
}
