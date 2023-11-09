//! Module containing the implementation of the surrealql tokens, lexer, and parser.

pub mod common;
pub mod error;
pub mod parser;
pub mod test;

#[cfg(feature = "experimental_parser")]
pub mod lexer;
#[cfg(feature = "experimental_parser")]
pub mod parser;
#[cfg(feature = "experimental_parser")]
pub mod token;

pub mod v1;
