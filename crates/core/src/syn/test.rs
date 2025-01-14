use reblessive::Stack;

use super::lexer::Lexer;
use super::parse;
use super::parser::Parser;
use super::Parse;
use crate::dbs::Capabilities;
use crate::err::Error;
use crate::sql::{Array, Expression, Ident, Idiom, Param, Script, Thing, Value};
use crate::syn::token::{t, TokenKind};

impl Parse<Self> for Value {
	fn parse(val: &str) -> Self {
		let mut parser = Parser::new(val.as_bytes());
		let mut stack = Stack::new();
		stack
			.enter(|stk| parser.parse_value_field(stk))
			.finish()
			.and_then(|e| parser.assert_finished().map(|_| e))
			.map_err(|e| e.render_on(val))
			.map_err(Error::InvalidQuery)
			.inspect_err(|e| eprintln!("{e}"))
			.unwrap()
	}
}

impl Parse<Self> for Array {
	fn parse(val: &str) -> Self {
		let mut parser = Parser::new(val.as_bytes());
		let mut stack = Stack::new();
		let start = parser.peek().span;
		assert!(parser.eat(t!("[")));
		stack
			.enter(|ctx| async move { parser.parse_array(ctx, start).await })
			.finish()
			.map_err(|e| e.render_on(val))
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
		let value = stack
			.enter(|ctx| parser.parse_value_field(ctx))
			.finish()
			.map_err(|e| e.render_on(val))
			.unwrap();
		if let Value::Expression(x) = value {
			return *x;
		}
		panic!("not an expression");
	}
}

#[test]
fn test_error_in_lineterminator() {
	let q = r#"
select * from person
CREATE person CONTENT { foo:'bar'};
"#;
	parse(q, &Capabilities::all()).unwrap_err();
}

#[test]
fn test_excessive_size() {
	let mut q = String::new();
	q.reserve_exact(u32::MAX as usize + 40);
	for _ in 0..u32::MAX {
		q.push(' ');
	}
	q.push_str("RETURN 1;");
	parse(&q, &Capabilities::all()).unwrap_err();
}

#[test]
fn empty_thing() {
	super::thing("").unwrap_err();
}

#[test]
fn empty_block() {
	super::block("").unwrap_err();
}

#[test]
fn empty_range() {
	super::range("").unwrap_err();
}

#[test]
fn empty_duration() {
	super::duration("").unwrap_err();
}

#[test]
fn empty_datetime() {
	super::datetime("").unwrap_err();
}

#[test]
fn empty_idiom() {
	super::idiom("").unwrap_err();
}

#[test]
fn empty_subquery() {
	super::subquery("").unwrap_err();
}

#[test]
fn empty_json() {
	super::json("").unwrap_err();
}
