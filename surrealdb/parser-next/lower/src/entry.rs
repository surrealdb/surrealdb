//! `syn`'s entry points over parser-next: parse into a flat tree, then lower
//! into the `surrealdb_sql` AST.
//!
//! Each function parses with the given [`Settings`] and returns the same tree
//! `syn` would, or an [`Error`] where `syn` would reject the input. An error
//! carries the first diagnostic only; rendering it against the source is the
//! caller's concern.

use surrealdb_parser_next::{
	Ast, Diag, Entry, Limits, Scratch, StatementEnd, Tag, parse_entry, parse_script,
	parse_statement,
};
use surrealdb_sql::{Ast as SqlAst, Block, Expr, Fields, Idiom, Kind, TopLevelExpr};

use crate::{Lower, Options};

/// The settings `syn`'s `ParserSettings` carry that parser-next reads.
#[derive(Clone, Copy, Debug)]
pub struct Settings {
	pub limits: Limits,
	pub opts: Options,
}

/// Why the input was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
	/// The parser rejected the input.
	Syntax(Diag),
	/// The input parsed, but a check `syn` makes while parsing failed in the
	/// lowering.
	Lower(&'static str),
}

/// Parses a whole query.
pub fn query(src: &str, settings: &Settings) -> Result<SqlAst, Error> {
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	parse_script(src, settings.limits, &mut ast, &mut scratch);
	if let Some(&d) = scratch.diags.first() {
		return Err(Error::Syntax(d));
	}
	let lower = Lower {
		ast: &ast,
		src,
		opts: settings.opts,
	};
	let expressions = ast
		.stmts
		.iter()
		.map(|&s| lower.top(s).map_err(|r| Error::Lower(r.0)))
		.collect::<Result<_, _>>()?;
	Ok(SqlAst {
		expressions,
	})
}

fn entry<T>(
	src: &str,
	settings: &Settings,
	which: Entry,
	require_all: bool,
	f: impl FnOnce(&Lower<'_>, u32) -> crate::R<T>,
) -> Result<T, Error> {
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	let Some(root) = parse_entry(src, which, settings.limits, require_all, &mut ast, &mut scratch)
	else {
		let d = scratch.diags.first().copied().expect("a failed parse records a diagnostic");
		return Err(Error::Syntax(d));
	};
	let lower = Lower {
		ast: &ast,
		src,
		opts: settings.opts,
	};
	f(&lower, root).map_err(|r| Error::Lower(r.0))
}

/// Parses an expression in which a bare identifier is a field.
pub fn expr(src: &str, settings: &Settings, require_all: bool) -> Result<Expr, Error> {
	entry(src, settings, Entry::Expr, require_all, |l, root| l.expr(root))
}

/// Parses a `{ … }` block.
pub fn block(src: &str, settings: &Settings, require_all: bool) -> Result<Block, Error> {
	entry(src, settings, Entry::Block, require_all, |l, root| match l.expr(root)? {
		Expr::Block(b) => Ok(*b),
		_ => unreachable!("a block entry lowers to a block"),
	})
}

/// Parses an idiom without a leading value.
pub fn idiom(src: &str, settings: &Settings, require_all: bool) -> Result<Idiom, Error> {
	entry(src, settings, Entry::Idiom, require_all, |l, root| l.idiom(root))
}

/// Parses a kind without the enclosing `<` `>`.
pub fn kind(src: &str, settings: &Settings, require_all: bool) -> Result<Kind, Error> {
	entry(src, settings, Entry::Kind, require_all, |l, root| l.kind(root))
}

/// Parses a projection list, or `VALUE` and one selector.
pub fn fields(src: &str, settings: &Settings, require_all: bool) -> Result<Fields, Error> {
	entry(src, settings, Entry::Fields, require_all, |l, root| {
		let n = &l.ast.nodes[root as usize];
		debug_assert_eq!(n.tag, Tag::Fields);
		l.fields_node(n)
	})
}

/// Where [`statement`] stopped.
#[derive(Debug)]
pub enum Statement {
	/// Only `;` and trivia; `consumed` bytes of them can be dropped.
	Empty {
		consumed: u32,
	},
	/// The input ends inside a statement and more could change it.
	NeedMore,
	/// One statement, ending after its `;` (or at the end of complete input)
	/// at `consumed`.
	Parsed {
		stmt: Box<TopLevelExpr>,
		consumed: u32,
	},
	/// The next statement is invalid whatever follows.
	Rejected(Error),
}

/// Parses the first statement of `src`, for input that arrives in pieces.
/// With `complete`, no more input follows.
pub fn statement(src: &str, settings: &Settings, complete: bool) -> Statement {
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	match parse_statement(src, settings.limits, &mut ast, &mut scratch) {
		StatementEnd::Empty {
			consumed,
		} => {
			if complete {
				Statement::Empty {
					consumed: src.len() as u32,
				}
			} else {
				Statement::Empty {
					consumed,
				}
			}
		}
		StatementEnd::Statement {
			final_,
			consumed,
		} => {
			if !final_ && !complete {
				return Statement::NeedMore;
			}
			if let Some(&d) = scratch.diags.first() {
				return Statement::Rejected(Error::Syntax(d));
			}
			let lower = Lower {
				ast: &ast,
				src,
				opts: settings.opts,
			};
			match lower.top(ast.stmts[0]) {
				Ok(stmt) => Statement::Parsed {
					stmt: Box::new(stmt),
					consumed,
				},
				Err(r) => Statement::Rejected(Error::Lower(r.0)),
			}
		}
	}
}
