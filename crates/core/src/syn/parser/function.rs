use reblessive::Stk;

use super::{ParseResult, Parser};
use crate::sql::{Expr, Function, FunctionCall, Model};
use crate::syn::error::{bail, syntax_error};
use crate::syn::parser::mac::{expected, expected_whitespace, unexpected};
use crate::syn::token::{TokenKind, t};

impl Parser<'_> {
	pub(crate) async fn parse_function_name(&mut self) -> ParseResult<Function> {
		let fnc = match self.peek_kind() {
			t!("fn") => {
				self.pop_peek();
				expected!(self, t!("::"));
				let mut name = self.parse_ident()?;
				while self.eat(t!("::")) {
					name.push_str("::");
					name.push_str(self.parse_ident_str()?);
				}

				Function::Custom(name)
			}
			t!("mod") => {
				self.pop_peek();
				if !self.settings.surrealism_enabled {
					bail!(
						"Experimental capability `surrealism` is not enabled",
						@self.last_span() => "Use of `mod::` is still experimental"
					)
				}

				expected!(self, t!("::"));
				let name = self.parse_ident()?;
				let sub = if self.eat(t!("::")) {
					Some(self.parse_ident()?)
				} else {
					None
				};

				Function::Module(name, sub)
			}
			t!("silo") => {
				self.pop_peek();
				if !self.settings.surrealism_enabled {
					bail!(
						"Experimental capability `surrealism` is not enabled",
						@self.last_span() => "Use of `silo::` is still experimental"
					)
				}

				expected!(self, t!("::"));
				let org = self.parse_ident()?;
				expected!(self, t!("::"));
				let pkg = self.parse_ident()?;
				expected!(self, t!("<"));
				let major = self.parse_version_digits()?;
				expected!(self, t!("."));
				let minor = self.parse_version_digits()?;
				expected!(self, t!("."));
				let patch = self.parse_version_digits()?;
				expected!(self, t!(">"));
				let sub = if self.eat(t!("::")) {
					Some(self.parse_ident()?)
				} else {
					None
				};

				Function::Silo {
					org,
					pkg,
					major,
					minor,
					patch,
					sub,
				}
			}
			t!("ml") => {
				self.pop_peek();
				expected!(self, t!("::"));

				let mut name = self.parse_ident()?;
				while self.eat(t!("::")) {
					name.push_str("::");
					name.push_str(self.parse_ident_str()?);
				}

				let (major, minor, patch) = self.parse_model_version()?;
				let version = format!("{}.{}.{}", major, minor, patch);

				Function::Model(Model {
					name,
					version,
				})
			}
			TokenKind::Identifier => {
				let mut name = self.parse_ident()?;
				while self.eat(t!("::")) {
					name.push_str("::");
					name.push_str(self.parse_ident_str()?)
				}
				Function::Normal(name)
			}
			_ => unexpected!(self, self.peek(), "a function name"),
		};

		Ok(fnc)
	}

	/// Parse a custom function function call
	///
	/// Expects `fn` to already be called.
	pub(super) async fn parse_custom_function(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<FunctionCall> {
		expected!(self, t!("::"));
		let mut name = self.parse_ident()?;
		while self.eat(t!("::")) {
			name.push_str("::");
			name.push_str(self.parse_ident_str()?)
		}

		expected!(self, t!("(")).span;
		let args = self.parse_function_args(stk).await?;
		let name = Function::Custom(name);
		Ok(FunctionCall {
			receiver: name,
			arguments: args,
		})
	}

	/// Parse a module function function call
	///
	/// Expects `mod` to already be called.
	pub(super) async fn parse_module_function(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<FunctionCall> {
		if !self.settings.surrealism_enabled {
			bail!(
				"Experimental capability `surrealism` is not enabled",
				@self.last_span() => "Use of `mod::` is still experimental"
			)
		}

		expected!(self, t!("::"));
		let name = self.parse_ident()?;
		let sub = if self.eat(t!("::")) {
			Some(self.parse_ident()?)
		} else {
			None
		};

		expected!(self, t!("(")).span;
		let args = self.parse_function_args(stk).await?;
		let name = Function::Module(name, sub);
		Ok(FunctionCall {
			receiver: name,
			arguments: args,
		})
	}

	/// Parse a silo function function call
	///
	/// Expects `silo` to already be called.
	pub(super) async fn parse_silo_function(&mut self, stk: &mut Stk) -> ParseResult<FunctionCall> {
		if !self.settings.surrealism_enabled {
			bail!(
				"Experimental capability `surrealism` is not enabled",
				@self.last_span() => "Use of `silo::` is still experimental"
			)
		}

		expected!(self, t!("::"));
		let org = self.parse_ident()?;
		expected!(self, t!("::"));
		let pkg = self.parse_ident()?;
		expected!(self, t!("<"));
		let major = self.parse_version_digits()?;
		expected!(self, t!("."));
		let minor = self.parse_version_digits()?;
		expected!(self, t!("."));
		let patch = self.parse_version_digits()?;
		expected!(self, t!(">"));
		let sub = if self.eat(t!("::")) {
			Some(self.parse_ident()?)
		} else {
			None
		};

		expected!(self, t!("(")).span;
		let args = self.parse_function_args(stk).await?;
		let name = Function::Silo {
			org,
			pkg,
			major,
			minor,
			patch,
			sub,
		};

		Ok(FunctionCall {
			receiver: name,
			arguments: args,
		})
	}

	pub(super) async fn parse_function_args(&mut self, stk: &mut Stk) -> ParseResult<Vec<Expr>> {
		let start = self.last_span();
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let arg = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
			args.push(arg);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), start)?;
				break;
			}
		}
		Ok(args)
	}

	pub fn parse_version_digits(&mut self) -> ParseResult<u32> {
		let token = self.next();
		match token.kind {
			TokenKind::Digits => self
				.lexer
				.span_str(token.span)
				.parse::<u32>()
				.map_err(|e| syntax_error!("Failed to parse model version: {e}", @token.span)),
			_ => unexpected!(self, token, "an integer"),
		}
	}

	pub(super) fn parse_model_version(&mut self) -> ParseResult<(u32, u32, u32)> {
		let start = expected!(self, t!("<")).span;

		let major: u32 = self.parse_version_digits()?;

		expected_whitespace!(self, t!("."));

		let minor: u32 = self.parse_version_digits()?;

		expected_whitespace!(self, t!("."));

		let patch: u32 = self.parse_version_digits()?;

		self.expect_closing_delimiter(t!(">"), start)?;

		Ok((major, minor, patch))
	}

	/// Parse a model invocation
	///
	/// Expects `ml` to already be called.
	pub(super) async fn parse_model(&mut self, stk: &mut Stk) -> ParseResult<FunctionCall> {
		expected!(self, t!("::"));

		let mut name = self.parse_ident()?;
		while self.eat(t!("::")) {
			name.push_str("::");
			name.push_str(self.parse_ident_str()?)
		}

		let (major, minor, patch) = self.parse_model_version()?;

		let start = expected!(self, t!("(")).span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let arg = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
			args.push(arg);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), start)?;
				break;
			}
		}

		let func = Function::Model(Model {
			name,
			version: format!("{}.{}.{}", major, minor, patch),
		});

		Ok(FunctionCall {
			receiver: func,
			arguments: args,
		})
	}
}

#[cfg(test)]
mod test {
	use surrealdb_types::ToSql;

	use super::*;
	use crate::{sql, syn};

	#[test]
	fn function_single() {
		let sql = "count()";
		let out = syn::expr(sql).unwrap();
		assert_eq!("count()", out.to_sql());
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(f.receiver, Function::Normal(String::from("count")));
		assert_eq!(f.arguments, vec![]);
	}

	#[test]
	fn function_single_not() {
		let sql = "not(10)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("not(10)", out.to_sql());
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(f.receiver, Function::Normal(String::from("not")));
		assert_eq!(f.arguments, vec![sql::Expr::Literal(sql::Literal::Integer(10))]);
	}

	#[test]
	fn function_module() {
		let sql = "rand::uuid()";
		let out = syn::expr(sql).unwrap();
		assert_eq!("rand::uuid()", out.to_sql());
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(f.receiver, Function::Normal(String::from("rand::uuid")));
		assert_eq!(f.arguments, vec![]);
	}

	#[test]
	fn function_arguments() {
		let sql = "string::is_numeric(null)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("string::is_numeric(NULL)", out.to_sql());
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(f.receiver, Function::Normal(String::from("string::is_numeric")));
		assert_eq!(f.arguments, vec![sql::Expr::Literal(sql::Literal::Null)]);
	}

	#[test]
	fn function_simple_together() {
		let sql = "function() { return 'test'; }";
		let out = syn::expr(sql).unwrap();
		assert_eq!("function() { return 'test'; }", out.to_sql());
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(f.receiver, Function::Script(sql::Script::from(" return 'test'; ")));
		assert_eq!(f.arguments, vec![]);
	}

	#[test]
	fn function_simple_whitespace() {
		let sql = "function () { return 'test'; }";
		let out = syn::expr(sql).unwrap();
		assert_eq!("function() { return 'test'; }", out.to_sql());
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(f.receiver, Function::Script(sql::Script::from(" return 'test'; ")));
		assert_eq!(f.arguments, vec![]);
	}

	#[test]
	fn function_script_expression() {
		let sql = "function() { return this.tags.filter(t => { return t.length > 3; }); }";
		let out = syn::expr(sql).unwrap();
		assert_eq!(
			"function() { return this.tags.filter(t => { return t.length > 3; }); }",
			out.to_sql()
		);
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(
			f.receiver,
			Function::Script(sql::Script::from(
				" return this.tags.filter(t => { return t.length > 3; }); "
			))
		);
		assert_eq!(f.arguments, vec![]);
	}

	#[test]
	fn ml_model_example() {
		let sql = r#"ml::insurance::prediction<1.0.0>({
 			age: 18,
 			disposable_income: "yes",
 			purchased_before: true
 		})
 		"#;
		let out = syn::expr(sql).unwrap();
		assert_eq!(
			"ml::insurance::prediction<1.0.0>({ age: 18, disposable_income: 'yes', purchased_before: true })",
			out.to_sql()
		);
	}

	#[test]
	fn ml_model_example_in_select() {
		let sql = r"
 			SELECT
 			name,
 			age,
 			ml::insurance::prediction<1.0.0>({
 				age: age,
 				disposable_income: math::round(income),
 				purchased_before: array::len(->purchased->property) > 0,
 			}) AS likely_to_buy FROM person:tobie;
 		";
		let out = syn::parse(sql).unwrap();
		assert_eq!(
			"SELECT name, age, ml::insurance::prediction<1.0.0>({ age: age, disposable_income: math::round(income), purchased_before: array::len(->purchased->property) > 0 }) AS likely_to_buy FROM person:tobie;",
			out.to_sql()
		);
	}

	#[test]
	fn ml_model_with_mutiple_arguments() {
		let sql = "ml::insurance::prediction<1.0.0>(1,2,3,4,)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("ml::insurance::prediction<1.0.0>(1, 2, 3, 4)", out.to_sql());
	}

	#[test]
	fn script_basic() {
		let sql = "function(){return true;}";
		let out = syn::expr(sql).unwrap();
		assert_eq!("function() {return true;}", out.to_sql());
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(f.receiver, Function::Script(sql::Script::from("return true;")));
		assert_eq!(f.arguments, vec![]);
	}

	#[test]
	fn script_object() {
		let sql = "function(){return { test: true, something: { other: true } };}";
		let out = syn::expr(sql).unwrap();
		assert_eq!("function() {return { test: true, something: { other: true } };}", out.to_sql());
		let Expr::FunctionCall(f) = out else {
			panic!()
		};

		assert_eq!(
			f.receiver,
			Function::Script(sql::Script::from(
				"return { test: true, something: { other: true } };"
			))
		);
		assert_eq!(f.arguments, vec![]);
	}

	#[test]
	fn script_closure() {
		let sql = "function(){return this.values.map(v => `This value is ${Number(v * 3)}`);}";
		let out = syn::expr(sql).unwrap();
		assert_eq!(
			"function() {return this.values.map(v => `This value is ${Number(v * 3)}`);}",
			out.to_sql()
		);
		let Expr::FunctionCall(f) = out else {
			panic!()
		};

		assert_eq!(
			f.receiver,
			Function::Script(sql::Script::from(
				"return this.values.map(v => `This value is ${Number(v * 3)}`);"
			))
		);
		assert_eq!(f.arguments, vec![]);
	}

	#[test]
	fn script_complex() {
		let sql = r#"function(){return { test: true, some: { object: "some text with uneven {{{ {} \" brackets", else: false } };}"#;
		let out = syn::expr(sql).unwrap();
		assert_eq!(
			r#"function() {return { test: true, some: { object: "some text with uneven {{{ {} \" brackets", else: false } };}"#,
			out.to_sql()
		);
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(
			f.receiver,
			Function::Script(sql::Script::from(
				r#"return { test: true, some: { object: "some text with uneven {{{ {} \" brackets", else: false } };"#
			))
		);
	}

	#[test]
	fn script_advanced() {
		let body = r#"
 			// {
 			// }
 			// {}
 			// { }
 			/* { */
 			/* } */
 			/* {} */
 			/* { } */
 			/* {{{ $ }} */
 			/* /* /* /* */
 			let x = {};
 			let x = { };
 			let x = '{';
 			let x = "{";
 			let x = '}';
 			let x = "}";
 			let x = '} } { {';
 			let x = 3 / 4 * 2;
 			let x = /* something */ 45 + 2;
 			"#;
		let sql = "function() {".to_owned() + body + "}";
		let out = syn::expr(&sql).unwrap();

		assert_eq!(sql, out.to_sql());
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(f.receiver, Function::Script(sql::Script::from(body)));
	}
}
