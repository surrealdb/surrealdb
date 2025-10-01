use reblessive::Stk;

use crate::sql::Param;
use crate::sql::statements::remove::{
	RemoveAnalyzerStatement, RemoveApiStatement, RemoveBucketStatement, RemoveSequenceStatement,
};
use crate::sql::statements::{
	RemoveAccessStatement, RemoveDatabaseStatement, RemoveEventStatement, RemoveFieldStatement,
	RemoveFunctionStatement, RemoveIndexStatement, RemoveNamespaceStatement, RemoveParamStatement,
	RemoveStatement, RemoveUserStatement,
};
use crate::syn::parser::mac::{expected, unexpected};
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::t;

impl Parser<'_> {
	pub(crate) async fn parse_remove_stmt(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<RemoveStatement> {
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

				let name = stk.run(|stk| self.parse_expr_field(stk)).await?;

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

				let name = stk.run(|stk| self.parse_expr_field(stk)).await?;

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
			t!("ACCESS") => {
				let if_exists = if self.eat(t!("IF")) {
					expected!(self, t!("EXISTS"));
					true
				} else {
					false
				};
				let name = stk.run(|stk| self.parse_expr_field(stk)).await?;
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
					name: name.into_string(),
					if_exists,
				})
			}
			t!("TABLE") => {
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

				let name = stk.run(|stk| self.parse_expr_field(stk)).await?;

				RemoveStatement::Table(crate::sql::statements::RemoveTableStatement {
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
				let name = stk.run(|stk| self.parse_expr_field(stk)).await?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let table = stk.run(|stk| self.parse_expr_field(stk)).await?;

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
				let name = stk.run(|stk| self.parse_expr_field(stk)).await?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let what = stk.run(|stk| self.parse_expr_field(stk)).await?;

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
				let name = stk.run(|stk| self.parse_expr_field(stk)).await?;
				expected!(self, t!("ON"));
				self.eat(t!("TABLE"));
				let what = stk.run(|stk| self.parse_expr_field(stk)).await?;

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
				let name = stk.run(|stk| self.parse_expr_field(stk)).await?;

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
				let name = stk.run(|stk| self.parse_expr_field(stk)).await?;
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
				let name = stk.run(|stk| self.parse_expr_field(stk)).await?;
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
				let name = stk.run(|stk| self.parse_expr_field(stk)).await?;

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
				let name = stk.run(|stk| self.parse_expr_field(stk)).await?;

				RemoveStatement::Bucket(RemoveBucketStatement {
					name,
					if_exists,
				})
			}
			// TODO(raphaeldarley): add Config here
			_ => unexpected!(self, next, "a remove statement keyword"),
		};
		Ok(res)
	}
}
