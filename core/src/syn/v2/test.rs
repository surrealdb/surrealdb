use reblessive::Stack;

use super::super::Parse;
use super::lexer::Lexer;
use super::parser::Parser;
use crate::sql::{Array, Expression, Ident, Idiom, Param, Script, Thing, Value};
use crate::syn::v2::token::{t, TokenKind};

impl Parse<Self> for Value {
	fn parse(val: &str) -> Self {
		super::value(val).unwrap()
	}
}

impl Parse<Self> for Array {
	fn parse(val: &str) -> Self {
		let mut parser = Parser::new(val.as_bytes());
		let mut stack = Stack::new();
		let start = parser.peek().span;
		assert!(parser.eat(t!("[")));
		stack
			.run(|mut ctx| async move { parser.parse_array(&mut ctx, start).await })
			.finish()
			.unwrap()
	}
}

impl Parse<Self> for Param {
	fn parse(val: &str) -> Self {
		let mut lexer = Lexer::new(val.as_bytes());
		let token = lexer.next_token();
		assert_eq!(token.kind, TokenKind::Parameter);
		Param(Ident(lexer.string.take().unwrap()))
	}
}

impl Parse<Self> for Idiom {
	fn parse(val: &str) -> Self {
		super::idiom(val).unwrap()
	}
}

impl Parse<Self> for Script {
	fn parse(_val: &str) -> Self {
		todo!()
	}
}

impl Parse<Self> for Thing {
	fn parse(val: &str) -> Self {
		super::thing(val).unwrap()
	}
}

impl Parse<Self> for Expression {
	fn parse(val: &str) -> Self {
		let mut parser = Parser::new(val.as_bytes());
		let mut stack = Stack::new();
		let value = stack.run(|ctx| parser.parse_value_field(ctx)).finish().unwrap();
		if let Value::Expression(x) = value {
			return *x;
		}
		panic!("not an expression");
	}
}
