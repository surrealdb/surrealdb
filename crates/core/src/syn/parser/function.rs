use reblessive::Stk;

use super::{ParseResult, Parser};
use crate::sql::{Expr, Function, FunctionCall, Ident, Model};
use crate::syn::error::syntax_error;
use crate::syn::parser::mac::{expected, expected_whitespace, unexpected};
use crate::syn::token::{TokenKind, t};

impl Parser<'_> {
	/// Parse a custom function function call
	///
	/// Expects `fn` to already be called.
	pub(super) async fn parse_custom_function(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<FunctionCall> {
		expected!(self, t!("::"));
		let mut name = self.next_token_value::<Ident>()?.into_string();
		while self.eat(t!("::")) {
			name.push_str("::");
			name.push_str(&self.next_token_value::<Ident>()?)
		}
		expected!(self, t!("(")).span;
		let args = self.parse_function_args(stk).await?;
		let name = Function::Custom(name);
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

	/// Parse a model invocation
	///
	/// Expects `ml` to already be called.
	pub(super) async fn parse_model(&mut self, stk: &mut Stk) -> ParseResult<FunctionCall> {
		expected!(self, t!("::"));
		let mut name = self.next_token_value::<Ident>()?.into_string();
		while self.eat(t!("::")) {
			name.push_str("::");
			name.push_str(&self.next_token_value::<Ident>()?)
		}
		let start = expected!(self, t!("<")).span;

		let token = self.next();
		let major: u32 =
			match token.kind {
				TokenKind::Digits => self.lexer.span_str(token.span).parse().map_err(
					|e| syntax_error!("Failed to parse model version: {e}", @token.span),
				)?,
				_ => unexpected!(self, token, "an integer"),
			};

		expected_whitespace!(self, t!("."));

		let token = self.next_whitespace();
		let minor: u32 =
			match token.kind {
				TokenKind::Digits => self.lexer.span_str(token.span).parse().map_err(
					|e| syntax_error!("Failed to parse model version: {e}", @token.span),
				)?,
				_ => unexpected!(self, token, "an integer"),
			};

		expected_whitespace!(self, t!("."));

		let token = self.next_whitespace();
		let patch: u32 =
			match token.kind {
				TokenKind::Digits => self.lexer.span_str(token.span).parse().map_err(
					|e| syntax_error!("Failed to parse model version: {e}", @token.span),
				)?,
				_ => unexpected!(self, token, "an integer"),
			};

		self.expect_closing_delimiter(t!(">"), start)?;

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
	use super::*;
	use crate::{sql, syn};

	#[test]
	fn function_single() {
		let sql = "count()";
		let out = syn::expr(sql).unwrap();
		assert_eq!("count()", format!("{}", out));
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
		assert_eq!("not(10)", format!("{}", out));
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
		assert_eq!("rand::uuid()", format!("{}", out));
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(f.receiver, Function::Normal(String::from("rand::uuid")));
		assert_eq!(f.arguments, vec![]);
	}

	#[test]
	fn function_arguments() {
		let sql = "string::is::numeric(null)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("string::is::numeric(NULL)", format!("{}", out));
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(f.receiver, Function::Normal(String::from("string::is::numeric")));
		assert_eq!(f.arguments, vec![sql::Expr::Literal(sql::Literal::Null)]);
	}

	#[test]
	fn function_simple_together() {
		let sql = "function() { return 'test'; }";
		let out = syn::expr(sql).unwrap();
		assert_eq!("function() { return 'test'; }", format!("{}", out));
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
		assert_eq!("function() { return 'test'; }", format!("{}", out));
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
			format!("{}", out)
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
			out.to_string()
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
			out.to_string()
		);
	}

	#[test]
	fn ml_model_with_mutiple_arguments() {
		let sql = "ml::insurance::prediction<1.0.0>(1,2,3,4,)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("ml::insurance::prediction<1.0.0>(1, 2, 3, 4)", out.to_string());
	}

	#[test]
	fn script_basic() {
		let sql = "function(){return true;}";
		let out = syn::expr(sql).unwrap();
		assert_eq!("function() {return true;}", format!("{}", out));
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
		assert_eq!(
			"function() {return { test: true, something: { other: true } };}",
			format!("{}", out)
		);
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
			format!("{}", out)
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
			format!("{}", out)
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

		assert_eq!(sql, format!("{}", out));
		let Expr::FunctionCall(f) = out else {
			panic!()
		};
		assert_eq!(f.receiver, Function::Script(sql::Script::from(body)));
	}
}
