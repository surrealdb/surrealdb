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
		let start = parser.peek().span;
		assert!(parser.eat(t!("[")));
		parser.parse_array(start).unwrap()
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
		let value = parser.parse_value_field().unwrap();
		if let Value::Expression(x) = value {
			return *x;
		}
		panic!("not an expression");
	}
}
