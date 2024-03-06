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
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;

				RemoveStatement::Namespace(RemoveNamespaceStatement {
					name,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("DATABASE") => {
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;

				RemoveStatement::Database(RemoveDatabaseStatement {
					name,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("FUNCTION") => {
				#[cfg(feature = "sql2")]
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
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("TOKEN") => {
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				let base = self.parse_base(true)?;

				RemoveStatement::Token(crate::sql::statements::RemoveTokenStatement {
					name,
					base,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("SCOPE") => {
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;

				RemoveStatement::Scope(RemoveScopeStatement {
					name,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("PARAM") => {
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value::<Param>()?;

				RemoveStatement::Param(RemoveParamStatement {
					name: name.0,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("TABLE") => {
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;

				RemoveStatement::Table(crate::sql::statements::RemoveTableStatement {
					name,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("EVENT") => {
				#[cfg(feature = "sql2")]
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
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("FIELD") => {
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let idiom = self.parse_local_idiom()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let table = self.next_token_value()?;

				RemoveStatement::Field(RemoveFieldStatement {
					name: idiom,
					what: table,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("INDEX") => {
				#[cfg(feature = "sql2")]
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
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("ANALYZER") => {
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.next_token_value()?;

				RemoveStatement::Analyzer(RemoveAnalyzerStatement {
					name,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("USER") => {
				#[cfg(feature = "sql2")]
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
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			x => unexpected!(self, x, "a remove statement keyword"),
		};
		Ok(res)
	}
}
