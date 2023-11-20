//! Module containing the implementation of the surrealql tokens, lexer, and parser.

pub mod common;
pub mod error;

pub mod v1;
pub use v1::{
	datetime, datetime_raw, duration, idiom, json, parse, path_like, range, subquery, thing,
	thing_raw, value,
};

#[cfg(test)]
pub mod test;
