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

use crate::{Lower, Options, Reject};

/// The settings `syn`'s `ParserSettings` carry that parser-next reads.
#[derive(Clone, Copy, Debug)]
pub struct Settings {
	pub limits: Limits,
	pub opts: Options,
}

/// Why the input was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
	/// The parser rejected the input.
	Syntax(Diag),
	/// The input parsed, but a check `syn` makes while parsing failed in the
	/// lowering.
	Lower(Reject),
}

/// Parses a whole query.
pub fn query(src: &str, settings: &Settings) -> Result<SqlAst, Error> {
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	parse_script(src, settings.limits, &mut ast, &mut scratch);
	let lower = Lower {
		ast: &ast,
		src,
		opts: settings.opts,
	};
	// Statement by statement, as `syn` reports the first failure it meets.
	let mut diags = scratch.diags.iter();
	let mut expressions = Vec::with_capacity(ast.stmts.len());
	for (i, &s) in ast.stmts.iter().enumerate() {
		let from = ast.spans[i].0;
		if ast.node(s).tag == Tag::Error {
			let d = *diags.next().expect("one diagnostic per failed statement");
			let e = eager_in_statement(src, settings, from, d.start)
				.map_or(Error::Syntax(d), Error::Lower);
			return Err(first_error(src, from, e, false));
		}
		match lower.top(s) {
			Ok(e) => expressions.push(e),
			Err(r) => return Err(first_error(src, from, Error::Lower(r), false)),
		}
	}
	Ok(SqlAst {
		expressions,
	})
}

fn entry<T>(
	src: &str,
	settings: &Settings,
	which: Entry,
	require_all: bool,
	f: impl Fn(&Lower<'_>, u32) -> crate::R<T>,
) -> Result<T, Error> {
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	let Some(root) = parse_entry(src, which, settings.limits, require_all, &mut ast, &mut scratch)
	else {
		let d = scratch.diags.first().copied().expect("a failed parse records a diagnostic");
		// The same check on the input before the syntax error.
		let eager = src.get(..d.start as usize).and_then(|prefix| {
			let mut ast = Ast::new();
			let mut scratch = Scratch::default();
			let root = parse_entry(prefix, which, settings.limits, false, &mut ast, &mut scratch)?;
			let lower = Lower {
				ast: &ast,
				src: prefix,
				opts: settings.opts,
			};
			eager(f(&lower, root).err()?, d.start)
		});
		let e = eager.map_or(Error::Syntax(d), Error::Lower);
		return Err(first_error(src, 0, e, false));
	};
	let lower = Lower {
		ast: &ast,
		src,
		opts: settings.opts,
	};
	f(&lower, root).map_err(|r| first_error(src, 0, Error::Lower(r), false))
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
				let from = ast.spans.first().map_or(0, |s| s.0);
				let e = eager_in_statement(src, settings, from, d.start)
					.map_or(Error::Syntax(d), Error::Lower);
				return Statement::Rejected(first_error(src, 0, e, false));
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
				Err(r) => Statement::Rejected(first_error(src, 0, Error::Lower(r), false)),
			}
		}
	}
}

/// The checks `syn` makes partway through a statement, once the part they
/// look at is read, by the start of their message.
const EAGER: &[&str] = &[
	"Missing split idiom",
	"Missing group idiom",
	"SPLIT and GROUP are mutually exclusive",
	"FLEXIBLE can only be used",
];

/// `r` when it is a check `syn` makes partway through a statement, failing
/// before byte `at`.
fn eager(r: Reject, at: u32) -> Option<Reject> {
	let early = r.span.is_some_and(|(s, _)| s < at);
	(early && EAGER.iter().any(|m| r.msg.starts_with(m))).then_some(r)
}

/// A check `syn` makes partway through the statement at `from` that fails
/// on its text before the syntax error at `at`, so before `syn` meets that
/// error.
fn eager_in_statement(src: &str, settings: &Settings, from: u32, at: u32) -> Option<Reject> {
	let prefix = src.get(..at as usize)?;
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	parse_script(prefix, settings.limits, &mut ast, &mut scratch);
	let i = ast.spans.iter().position(|&(s, _)| s == from)?;
	let s = ast.stmts[i];
	if ast.node(s).tag == Tag::Error {
		return None;
	}
	let lower = Lower {
		ast: &ast,
		src: prefix,
		opts: settings.opts,
	};
	eager(lower.top(s).err()?, at)
}

/// The error `syn` reports for input parser-next rejected with `e`, reading
/// from `from`: `syn` decodes escapes in strings and escaped identifiers, and
/// the contents of prefixed strings, as it consumes them, so an invalid one
/// before `e` is the error it meets first.
pub(crate) fn first_error(src: &str, from: u32, e: Error, json: bool) -> Error {
	// A path is checked once all of it is read, its escapes decoded.
	const PATH: &str = "Invalid function/constant path";
	let at = match &e {
		Error::Syntax(d) if d.msg.starts_with(PATH) => d.end,
		Error::Syntax(d) => d.start,
		Error::Lower(r) if r.msg.starts_with(PATH) => r.span.map_or(u32::MAX, |(_, e)| e),
		Error::Lower(r) => r.span.map_or(u32::MAX, |(s, _)| s),
	};
	match first_escape_error(src, from, at, json) {
		Some(r) => Error::Lower(r),
		None => e,
	}
}

/// The first invalid escape in a string, escaped-identifier or escaped
/// parameter token that starts in `from..before`, or the first prefixed
/// string there whose contents do not decode, skipping JavaScript function
/// bodies, whose strings `syn` does not decode.
pub(crate) fn first_escape_error(src: &str, from: u32, before: u32, json: bool) -> Option<Reject> {
	use surrealdb_parser_next::decode::escape_error;
	use surrealdb_parser_next::kw::Kw;
	use surrealdb_parser_next::lex::{Lexer, T};
	// Only an escape or a prefixed string can fail here.
	let rest = src.as_bytes().get(from as usize..)?;
	let prefixed = |w: &[u8]| matches!(w, [b'd' | b'u' | b'b' | b'f' | b'r', b'"' | b'\'']);
	if !rest.contains(&b'\\') && !rest.windows(2).any(prefixed) {
		return None;
	}
	let mut l = Lexer::new(src.as_bytes());
	l.pos = from;
	loop {
		let t = l.next_token();
		if t.kind == T::Eof || t.start >= before {
			return None;
		}
		match t.kind {
			T::Str | T::EscIdent | T::Param => {
				let text = src.get(t.start as usize..t.end as usize)?;
				// An escaped parameter's name follows its `$`.
				let skip = (t.kind == T::Param) as u32;
				if let Some((s, e, msg, label)) = escape_error(&text[skip as usize..], json) {
					let (s, e) = (t.start + skip + s, t.start + skip + e);
					return Some(Reject::new(msg).at(s, e).with_label(label));
				}
				if t.kind == T::Str
					&& let Some(r) = crate::lit::prefixed_error(text, t.aux(), t.start)
				{
					return Some(r);
				}
			}
			T::Ident if t.kw == Kw::Function => {
				// `function(args) { body }`: skip to the body and over it.
				let mut depth = 0u32;
				loop {
					let a = l.next_token();
					match a.kind {
						T::Eof => return None,
						T::LParen => depth += 1,
						T::RParen => {
							depth = depth.saturating_sub(1);
							if depth == 0 {
								break;
							}
						}
						_ if depth == 0 => break,
						_ => {}
					}
				}
				let mut peek = l;
				let open = peek.next_token();
				if open.kind == T::LBrace {
					match l.js_body(open.start) {
						Ok(end) => l.pos = end,
						Err(_) => return None,
					}
				}
			}
			_ => {}
		}
	}
}
