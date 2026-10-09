//! The SurrealQL front end sized for the edge build: a lexer that never
//! allocates, a parser whose stack use does not grow with expression
//! nesting, a flat syntax tree, and statement-at-a-time streaming.
//!
//! See `doc/parser/RFC_PARSER_NEXT.md` for the design and how this crate is
//! measured. The crate has no dependencies.

pub mod ast;
pub mod decode;
pub mod kw;
pub mod lex;
pub mod parse;
pub mod stream;

pub use ast::{Ast, Node, NodeId, Tag};
pub use parse::{
	Diag, DiagKind, Entry, Extra, Limits, PathKind, Scratch, StatementEnd, parse_entry,
	parse_record_id, parse_record_id_all, parse_script, parse_statement,
};
pub use stream::{Statement, Step, StreamError, StreamParser};

/// Parses `src` with default limits into a fresh tree.
pub fn parse(src: &str) -> (Ast, Vec<Diag>) {
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	parse_script(src, Limits::default(), &mut ast, &mut scratch);
	(ast, scratch.diags)
}
