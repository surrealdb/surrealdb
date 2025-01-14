use reblessive::Stk;

use crate::{
	sql::{Function, Ident, Model, Value},
	syn::{
		error::syntax_error,
		parser::mac::{expected, expected_whitespace, unexpected},
		token::{t, TokenKind},
	},
};

use super::{ParseResult, Parser};

impl Parser<'_> {
	/// Parse a custom function function call
	///
	/// Expects `fn` to already be called.
	pub(super) async fn parse_custom_function(&mut self, ctx: &mut Stk) -> ParseResult<Function> {
		expected!(self, t!("::"));
		let mut name = self.next_token_value::<Ident>()?.0;
		while self.eat(t!("::")) {
			name.push_str("::");
			name.push_str(&self.next_token_value::<Ident>()?.0)
		}
		expected!(self, t!("(")).span;
		let args = self.parse_function_args(ctx).await?;
		Ok(Function::Custom(name, args))
	}

	pub(super) async fn parse_function_args(&mut self, ctx: &mut Stk) -> ParseResult<Vec<Value>> {
		let start = self.last_span();
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let arg = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
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
	pub(super) async fn parse_model(&mut self, ctx: &mut Stk) -> ParseResult<Model> {
		expected!(self, t!("::"));
		let mut name = self.next_token_value::<Ident>()?.0;
		while self.eat(t!("::")) {
			name.push_str("::");
			name.push_str(&self.next_token_value::<Ident>()?.0)
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

			let arg = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
			args.push(arg);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), start)?;
				break;
			}
		}
		Ok(Model {
			name,
			version: format!("{}.{}.{}", major, minor, patch),
			args,
		})
	}
}

#[cfg(test)]
mod test {
	use crate::{
		dbs::Capabilities, sql::{Script, Value}, syn::{self, Parse}
	};

	use super::*;

	#[test]
	fn function_single() {
		let sql = "count()";
		let out = Value::parse(sql);
		assert_eq!("count()", format!("{}", out));
		assert_eq!(out, Value::from(Function::Normal(String::from("count"), vec![])));
	}

	#[test]
	fn function_single_not() {
		let sql = "not(10)";
		let out = Value::parse(sql);
		assert_eq!("not(10)", format!("{}", out));
		assert_eq!(out, Value::from(Function::Normal("not".to_owned(), vec![10.into()])));
	}

	#[test]
	fn function_module() {
		let sql = "rand::uuid()";
		let out = Value::parse(sql);
		assert_eq!("rand::uuid()", format!("{}", out));
		assert_eq!(out, Value::from(Function::Normal(String::from("rand::uuid"), vec![])));
	}

	#[test]
	fn function_arguments() {
		let sql = "string::is::numeric(null)";
		let out = Value::parse(sql);
		assert_eq!("string::is::numeric(NULL)", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Function::Normal(String::from("string::is::numeric"), vec![Value::Null]))
		);
	}

	#[test]
	fn function_simple_together() {
		let sql = "function() { return 'test'; }";
		let out = Value::parse(sql);
		assert_eq!("function() { return 'test'; }", format!("{}", out));
		assert_eq!(out, Value::from(Function::Script(Script::from(" return 'test'; "), vec![])));
	}

	#[test]
	fn function_simple_whitespace() {
		let sql = "function () { return 'test'; }";
		let out = Value::parse(sql);
		assert_eq!("function() { return 'test'; }", format!("{}", out));
		assert_eq!(out, Value::from(Function::Script(Script::from(" return 'test'; "), vec![])));
	}

	#[test]
	fn function_script_expression() {
		let sql = "function() { return this.tags.filter(t => { return t.length > 3; }); }";
		let out = Value::parse(sql);
		assert_eq!(
			"function() { return this.tags.filter(t => { return t.length > 3; }); }",
			format!("{}", out)
		);
		assert_eq!(
			out,
			Value::from(Function::Script(
				Script::from(" return this.tags.filter(t => { return t.length > 3; }); "),
				vec![]
			))
		);
	}

	#[test]
	fn ml_model_example() {
		let sql = r#"ml::insurance::prediction<1.0.0>({
			age: 18,
			disposable_income: "yes",
			purchased_before: true
		})
		"#;
		let out = Value::parse(sql);
		assert_eq!("ml::insurance::prediction<1.0.0>({ age: 18, disposable_income: 'yes', purchased_before: true })",out.to_string());
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
		let out = syn::parse(sql, &Capabilities::all()).unwrap();
		assert_eq!(
			"SELECT name, age, ml::insurance::prediction<1.0.0>({ age: age, disposable_income: math::round(income), purchased_before: array::len(->purchased->property) > 0 }) AS likely_to_buy FROM person:tobie;",
			out.to_string()
		);
	}

	#[test]
	fn ml_model_with_mutiple_arguments() {
		let sql = "ml::insurance::prediction<1.0.0>(1,2,3,4,)";
		let out = Value::parse(sql);
		assert_eq!("ml::insurance::prediction<1.0.0>(1,2,3,4)", out.to_string());
	}

	#[test]
	fn script_basic() {
		let sql = "function(){return true;}";
		let out = Value::parse(sql);
		assert_eq!("function() {return true;}", format!("{}", out));
		assert_eq!(out, Value::from(Function::Script(Script::from("return true;"), Vec::new())));
	}

	#[test]
	fn script_object() {
		let sql = "function(){return { test: true, something: { other: true } };}";
		let out = Value::parse(sql);
		assert_eq!(
			"function() {return { test: true, something: { other: true } };}",
			format!("{}", out)
		);
		assert_eq!(
			out,
			Value::from(Function::Script(
				Script::from("return { test: true, something: { other: true } };"),
				Vec::new()
			))
		);
	}

	#[test]
	fn script_closure() {
		let sql = "function(){return this.values.map(v => `This value is ${Number(v * 3)}`);}";
		let out = Value::parse(sql);
		assert_eq!(
			"function() {return this.values.map(v => `This value is ${Number(v * 3)}`);}",
			format!("{}", out)
		);
		assert_eq!(
			out,
			Value::from(Function::Script(
				Script::from("return this.values.map(v => `This value is ${Number(v * 3)}`);"),
				Vec::new()
			))
		);
	}

	#[test]
	fn script_complex() {
		let sql = r#"function(){return { test: true, some: { object: "some text with uneven {{{ {} \" brackets", else: false } };}"#;
		let out = Value::parse(sql);
		assert_eq!(
			r#"function() {return { test: true, some: { object: "some text with uneven {{{ {} \" brackets", else: false } };}"#,
			format!("{}", out)
		);
		assert_eq!(
			out,
			Value::from(Function::Script(
				Script::from(
					r#"return { test: true, some: { object: "some text with uneven {{{ {} \" brackets", else: false } };"#
				),
				Vec::new()
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
		let out = Value::parse(&sql);
		assert_eq!(sql, format!("{}", out));
		assert_eq!(out, Value::from(Function::Script(Script::from(body), Vec::new())));
	}
}
