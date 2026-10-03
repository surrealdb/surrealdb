#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]

//! MySQL language front end for SurrealDB.

pub mod ast;
pub mod lexer;
pub mod parser;
pub mod printer;
pub mod translate;

pub use parser::{parse_expr, parse_statement, parse_statements};
pub use printer::ToMySql;
pub use translate::{
	ParamStyle, ToSurrealQl, TranslationContext, TranslationError, translate_sql,
	translate_sql_with_context,
};
