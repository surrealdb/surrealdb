use crate::err::Error;
use crate::sql::{subquery::Subquery, Idiom, Query, Thing, Value};
use crate::syn::v2::parser::Parser;

pub fn parse(input: &str) -> Result<Query, Error> {
	let mut parser = Parser::new(input.as_bytes());
	match parser.parse_query() {
		Ok(x) => Ok(x),
		_ => todo!(),
	}
}

pub fn json(input: &str) -> Result<Value, Error> {
	todo!()
}

pub fn idiom(input: &str) -> Result<Idiom, Error> {
	let mut parser = Parser::new(input.as_bytes());
	match parser.parse_plain_idiom() {
		Ok(x) => Ok(x),
		_ => todo!(),
	}
}

pub fn thing(input: &str) -> Result<Thing, Error> {
	let mut parser = Parser::new(input.as_bytes());
	match parser.parse_thing() {
		Ok(x) => Ok(x),
		_ => todo!(),
	}
}

pub fn subquery(input: &str) -> Result<Subquery, Error> {
	let mut parser = Parser::new(input.as_bytes());
	match parser.parse_full_subquery() {
		Ok(x) => Ok(x),
		_ => todo!(),
	}
}

pub fn value(input: &str) -> Result<Value, Error> {
	let mut parser = Parser::new(input.as_bytes());
	match parser.parse_value_field() {
		Ok(x) => Ok(x),
		_ => todo!(),
	}
}
