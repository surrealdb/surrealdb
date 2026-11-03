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

/// The words a kind reads as a built-in type, compared without regard to case. None of them
/// names a type declared by `DEFINE TYPE`: a kind reads each as the built-in type or refuses it,
/// so a declared type is refused one of these names.
pub const BUILTIN_KIND_NAMES: &[&str] = &[
	"any",
	"array",
	"bigdecimal",
	"bool",
	"bytes",
	"collection",
	"date",
	"datetime",
	"decimal",
	"duration",
	"either",
	"enum",
	"f16",
	"f32",
	"f64",
	"false",
	"file",
	"float",
	"function",
	"geometry",
	"i16",
	"i32",
	"i64",
	"i8",
	"int",
	"interval",
	"line",
	"literal",
	"multiline",
	"multipoint",
	"multipolygon",
	"none",
	"null",
	"number",
	"object",
	"option",
	"point",
	"polygon",
	"range",
	"record",
	"regex",
	"set",
	"string",
	"table",
	"tb",
	"time",
	"timestamp",
	"true",
	"u16",
	"u32",
	"u64",
	"u8",
	"uuid",
];

/// Whether `name` is one of [`BUILTIN_KIND_NAMES`].
pub fn is_builtin_kind_name(name: &str) -> bool {
	BUILTIN_KIND_NAMES.iter().any(|builtin| builtin.eq_ignore_ascii_case(name))
}
