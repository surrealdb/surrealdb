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
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				RemoveStatement::Namespace(RemoveNamespaceStatement {
					name,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("DATABASE") => {
				let name = self.next_token_value()?;
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				RemoveStatement::Database(RemoveDatabaseStatement {
					name,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("FUNCTION") => {
				let name = self.parse_custom_function_name()?;
				let next = self.peek();
				if self.eat(t!("(")) {
					self.expect_closing_delimiter(t!(")"), next.span)?;
				}
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				RemoveStatement::Function(RemoveFunctionStatement {
					name,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("TOKEN") => {
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				let base = self.parse_base(true)?;
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				RemoveStatement::Token(crate::sql::statements::RemoveTokenStatement {
					name,
					base,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("SCOPE") => {
				let name = self.next_token_value()?;
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				RemoveStatement::Scope(RemoveScopeStatement {
					name,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("PARAM") => {
				let name = self.next_token_value::<Param>()?;
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				RemoveStatement::Param(RemoveParamStatement {
					name: name.0,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("TABLE") => {
				let name = self.next_token_value()?;
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				RemoveStatement::Table(crate::sql::statements::RemoveTableStatement {
					name,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("EVENT") => {
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let table = self.next_token_value()?;
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				RemoveStatement::Event(RemoveEventStatement {
					name,
					what: table,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("FIELD") => {
				let idiom = self.parse_local_idiom()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let table = self.next_token_value()?;
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				RemoveStatement::Field(RemoveFieldStatement {
					name: idiom,
					what: table,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("INDEX") => {
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let what = self.next_token_value()?;
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				RemoveStatement::Index(RemoveIndexStatement {
					name,
					what,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("ANALYZER") => {
				let name = self.next_token_value()?;
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				RemoveStatement::Analyzer(RemoveAnalyzerStatement {
					name,
					#[cfg(feature = "sql2")]
					if_exists,
				})
			}
			t!("USER") => {
				let name = self.next_token_value()?;
				expected!(self, t!("ON"));
				let base = self.parse_base(false)?;
				#[cfg(feature = "sql2")]
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

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
