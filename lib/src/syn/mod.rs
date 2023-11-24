//! Module containing the implementation of the surrealql tokens, lexer, and parser.

pub mod common;
pub mod error;

#[cfg(not(feature = "experimental_parser"))]
pub mod v1;
#[cfg(not(feature = "experimental_parser"))]
pub use v1::{datetime_raw, duration, idiom, json, parse, range, subquery, thing, value};

#[cfg(all(test, not(feature = "experimental_parser")))]
pub use v1::test::builtin_name;

#[cfg(feature = "experimental_parser")]
pub mod v2;
#[cfg(feature = "experimental_parser")]
pub use v2::{datetime_raw, duration, idiom, json, parse, range, subquery, thing, value};

#[cfg(test)]
pub trait Parse<T> {
	fn parse(val: &str) -> T;
}
#[cfg(all(test, feature = "experimental_parser"))]
pub use v2::test::builtin_name;
