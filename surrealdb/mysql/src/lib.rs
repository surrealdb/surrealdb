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

pub use parser::error::{ErrorKind, ParseError, ParseResult};
pub use parser::{
	ParserSettings, parse_expr, parse_expr_with_settings, parse_statement,
	parse_statement_with_settings, parse_statements, parse_statements_with_settings,
};
pub use printer::ToMySql;
pub use translate::{
	ParamStyle, ToSurrealQl, TranslationContext, TranslationError, translate_sql,
	translate_sql_with_context,
};
