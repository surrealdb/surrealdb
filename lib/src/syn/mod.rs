//! Module containing the implementation of the surrealql tokens, lexer, and parser.

pub mod common;
pub mod error;

#[cfg(not(feature = "experimental_parser"))]
pub mod v1;
#[cfg(not(feature = "experimental_parser"))]
pub use v1::{datetime_raw, duration, idiom, json, parse, range, subquery, thing, value};

#[cfg(feature = "experimental_parser")]
mod v2;
#[cfg(feature = "experimental_parser")]
pub use v2::{datetime_raw, duration, idiom, json, parse, range, subquery, thing, value};

#[cfg(test)]
pub mod test;
