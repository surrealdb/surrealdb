//! Module containing the implementation of the surrealql tokens, lexer, and parser.

pub mod common;
pub mod error;

#[cfg(not(feature = "experimental_parser"))]
pub mod v1;
#[cfg(not(feature = "experimental_parser"))]
pub use v1::{
	datetime_raw, duration, idiom, json, parse as tmp_parse, range, subquery, thing, value,
};

#[cfg(feature = "experimental_parser")]
pub mod v2;
#[cfg(feature = "experimental_parser")]
pub use v2::{
	datetime_raw, duration, idiom, json, parse as tmp_parse, range, subquery, thing, value,
};

#[cfg(test)]
pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

use crate::err::Error;
use crate::sql::Query;

pub fn parse(i: &str) -> Result<Query, Error> {
	println!("INPUT: {}", i);
	let res = tmp_parse(i);
	println!("OUTPUT: {:#?}", res);
	res
}
