use surrealdb_sql::statements::remove::{
	RemoveAnalyzerStatement, RemoveApiStatement, RemoveBucketStatement, RemoveConfigKind,
	RemoveConfigStatement, RemoveModuleStatement, RemoveSequenceStatement,
};
use surrealdb_sql::statements::{
	RemoveAccessStatement, RemoveDatabaseStatement, RemoveEventStatement, RemoveFieldStatement,
	RemoveFunctionStatement, RemoveIndexStatement, RemoveNamespaceStatement, RemoveParamStatement,
	RemoveStatement, RemoveUserStatement,
};
use surrealdb_sql::{ModuleName, Param};

use crate::error::bail;
use crate::parser::mac::{expected, expected_whitespace, unexpected};
use crate::parser::{ParseResult, Parser};
use crate::token::t;

impl Parser<'_> {
	pub fn parse_remove_stmt(&mut self) -> ParseResult<RemoveStatement> {
		let next = self.next();
		let res = match next.kind {
			t!("NAMESPACE") => {
				let expunge = if self.eat(t!("AND")) {
					expected!(self, t!("EXPUNGE"));
					true
				} else {
					false
				};

				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				let name = self.parse_expr_field()?;

				RemoveStatement::Namespace(RemoveNamespaceStatement {
					name,
					if_exists,
					expunge,
				})
			}
			t!("DATABASE") => {
				let expunge = if self.eat(t!("AND")) {
					expected!(self, t!("EXPUNGE"));
					true
				} else {
					false
				};

				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				let name = self.parse_expr_field()?;

				RemoveStatement::Database(RemoveDatabaseStatement {
					name,
					if_exists,
					expunge,
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
			t!("MODULE") => {
				if !self.settings.surrealism_enabled {
					bail!(
						"Surrealism modules are not enabled",
						@self.last_span() => "Use of `REMOVE MODULE` requires Surrealism modules"
					)
				}

				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				let peek = self.peek();
				let name = match peek.kind {
					t!("mod") => {
						self.pop_peek();
						expected_whitespace!(self, t!("::"));
						let name = self.parse_ident()?.into_string();
						ModuleName::Module(name)
					}
					t!("silo") => {
						self.pop_peek();
						expected!(self, t!("::"));
						let organisation = self.parse_ident()?.into_string();
						expected!(self, t!("::"));
						let package = self.parse_ident()?.into_string();
						expected!(self, t!("::"));
						expected_whitespace!(self, t!("<"));
						let major = self.parse_version_digits()?;
						expected_whitespace!(self, t!("."));
						let minor = self.parse_version_digits()?;
						expected_whitespace!(self, t!("."));
						let patch = self.parse_version_digits()?;
						expected!(self, t!(">"));
						ModuleName::Silo(organisation, package, major, minor, patch)
					}
					_ => unexpected!(self, peek, "a module name"),
				};

				let next = self.peek();
				if self.eat(t!("(")) {
					self.expect_closing_delimiter(t!(")"), next.span)?;
				}

				RemoveStatement::Module(RemoveModuleStatement {
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
				let name = self.parse_expr_field()?;
				expected!(self, t!("ON"));
				let base = self.parse_base()?;

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
					name: name.into_strand().into(),
					if_exists,
				})
			}
			t!("TABLE") | t!("TB") => {
				let expunge = if self.eat(t!("AND")) {
					expected!(self, t!("EXPUNGE"));
					true
				} else {
					false
				};

				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};

				let name = self.parse_expr_field()?;

				RemoveStatement::Table(surrealdb_sql::statements::RemoveTableStatement {
					name,
					if_exists,
					expunge,
				})
			}
			t!("EVENT") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.parse_expr_field()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let table = self.parse_expr_field()?;

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
				let name = self.parse_expr_field()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let what = self.parse_expr_field()?;

				RemoveStatement::Field(RemoveFieldStatement {
					name,
					what,
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
				let name = self.parse_expr_field()?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let what = self.parse_expr_field()?;

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
				let name = self.parse_expr_field()?;

				RemoveStatement::Analyzer(RemoveAnalyzerStatement {
					name,
					if_exists,
				})
			}
			t!("SEQUENCE") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.parse_expr_field()?;
				RemoveStatement::Sequence(RemoveSequenceStatement {
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
				let name = self.parse_expr_field()?;
				expected!(self, t!("ON"));
				let base = self.parse_base()?;

				RemoveStatement::User(RemoveUserStatement {
					name,
					base,
					if_exists,
				})
			}
			t!("API") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.parse_expr_field()?;

				RemoveStatement::Api(RemoveApiStatement {
					name,
					if_exists,
				})
			}
			t!("BUCKET") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = self.parse_expr_field()?;

				RemoveStatement::Bucket(RemoveBucketStatement {
					name,
					if_exists,
				})
			}
			t!("CONFIG") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let next = self.next();
				let kind = match next.kind {
					t!("GRAPHQL") => RemoveConfigKind::GraphQL,
					t!("API") => RemoveConfigKind::Api,
					t!("DEFAULT") => RemoveConfigKind::Default,
					_ => unexpected!(self, next, "a type of config"),
				};
				RemoveStatement::Config(RemoveConfigStatement {
					kind,
					if_exists,
				})
			}
			_ => unexpected!(self, next, "a remove statement keyword"),
		};
		Ok(res)
	}
}
