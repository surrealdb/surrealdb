//! The parser entry points over parser-next: parse into a flat tree, then lower
//! into the runtime expression tree.
//!
//! Each function parses with the given [`Settings`] and returns a runtime
//! expression tree or an [`Error`] for rejected input. An error
//! carries the first diagnostic only; rendering it against the source is the
//! caller's concern.

use surrealdb_parser_next::lex::decode_duration;
use surrealdb_parser_next::{
	Ast, Entry, Scratch, StatementEnd, Tag, parse_entry, parse_script, parse_statement,
};
/// The parts of `ParserSettings` that parser-next reads.
#[derive(Clone, Copy, Debug)]
pub struct Settings {
	pub limits: surrealdb_parser_next::Limits,
	pub opts: Options,
}

/// Why the input was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
	/// The parser rejected the input.
	Syntax(surrealdb_parser_next::Diag),
	/// The input parsed, but a parse-time check, made in the lowering,
	/// failed.
	Lower(Reject),
}

use surrealdb_strand::TableName;

use super::query::{Query, TopLevelExpr};
use super::{Lower, Options, Reject};
use crate::expr::{Block, Expr, Fields, Function, Idiom, KeyNames, Kind, Literal};

/// The most nodes, and twice that many `extra` slots, an arena keeps between
/// parses on one thread. A parse that grew it further hands the memory back.
const KEEP_NODES: usize = 1 << 16;

std::thread_local! {
	static ARENA: std::cell::RefCell<Option<(Ast, Scratch)>> =
		const { std::cell::RefCell::new(None) };
}

/// Runs `f` on this thread's reusable flat-tree arena. Once the arena has
/// grown to the statements it sees, flat parsing needs no storage growth. A call made
/// while the arena is in use, from inside `f`, gets a fresh one.
fn with_arena<T>(f: impl FnOnce(&mut Ast, &mut Scratch) -> T) -> T {
	let (mut ast, mut scratch) = ARENA.with(|a| a.borrow_mut().take()).unwrap_or_default();
	let out = f(&mut ast, &mut scratch);
	if ast.nodes.capacity() <= KEEP_NODES && ast.extra.capacity() <= 2 * KEEP_NODES {
		ARENA.with(|a| *a.borrow_mut() = Some((ast, scratch)));
	}
	out
}

/// Parses a whole query.
pub fn query(src: &str, settings: &Settings) -> Result<Query, Error> {
	with_arena(|ast, scratch| query_in(src, settings, ast, scratch))
}

/// Parse request statements directly into logical operators. Directives and
/// statements whose credentials or lock admission require a runtime boundary
/// retain their dedicated syntax payload.
pub fn execution_query(
	src: &str,
	settings: super::ParserSettings,
) -> Result<Vec<super::ExecutionStatement>, Error> {
	use surrealdb_parser_next::ast::select;

	use super::execution::{ExecutionStatement, LogicalStatement, StatementKind, StatementText};
	with_arena(|ast, scratch| {
		let next = settings.next();
		parse_script(src, next.limits, ast, scratch);
		let lower = Lower {
			ast,
			src,
			opts: next.opts,
		};
		let mut diags = scratch.diags.iter();
		let mut dedicated = vec![false; ast.stmts.len()];
		for n in &ast.nodes {
			if matches!(n.tag, Tag::Define | Tag::Alter | Tag::Remove | Tag::Rebuild)
				|| n.tag == Tag::Select
					&& ast
						.extra
						.get(n.a as usize..)
						.and_then(|rec| rec.get(select::FLAGS))
						.is_some_and(|flags| flags & 64 != 0)
			{
				let at =
					ast.spans.partition_point(|&(start, _)| start <= n.start).saturating_sub(1);
				if let Some(&(from, end)) = ast.spans.get(at)
					&& n.start >= from
					&& n.end <= end
				{
					dedicated[at] = true;
				}
			}
		}
		let mut statements = Vec::with_capacity(ast.stmts.len());
		let mut source = None;
		for (i, &id) in ast.stmts.iter().enumerate() {
			let (from, end) = ast.spans[i];
			let tag = ast.node(id).tag;
			if tag == Tag::Error {
				let d = *diags.next().expect("one diagnostic per failed statement");
				let e = eager_in_statement(src, &next, from, d.start)
					.map_or(Error::Syntax(d), Error::Lower);
				return Err(first_error(src, from, e, false));
			}
			let dedicated = dedicated[i]
				|| matches!(
					tag,
					Tag::Begin
						| Tag::Cancel | Tag::Commit
						| Tag::Use | Tag::OptionStmt
						| Tag::Kill | Tag::Live
						| Tag::Show | Tag::AccessStmt
				);
			let statement = if dedicated {
				ExecutionStatement::Surface(
					lower.top(id).map_err(|r| first_error(src, from, Error::Lower(r), false))?,
				)
			} else {
				let plan = lower
					.logical_statement(id, false)
					.map_err(|r| first_error(src, from, Error::Lower(r), false))?;
				ExecutionStatement::Logical(Box::new(LogicalStatement {
					plan,
					kind: StatementKind::from_tag(tag),
					text: StatementText {
						source: std::sync::Arc::clone(
							source.get_or_insert_with(|| std::sync::Arc::<str>::from(src)),
						),
						span: from as usize..end as usize,
						settings,
					},
				}))
			};
			statements.push(statement);
		}
		Ok(statements)
	})
}

fn query_in(
	src: &str,
	settings: &Settings,
	ast: &mut Ast,
	scratch: &mut Scratch,
) -> Result<Query, Error> {
	parse_script(src, settings.limits, ast, scratch);
	let ast = &*ast;
	let lower = Lower {
		ast,
		src,
		opts: settings.opts,
	};
	// Statement by statement, reporting the first failure in reading order.
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
	Ok(Query {
		expressions,
	})
}

fn entry<T>(
	src: &str,
	settings: &Settings,
	which: Entry,
	require_all: bool,
	f: impl Fn(&Lower<'_>, u32) -> super::R<T>,
) -> Result<T, Error> {
	with_arena(|ast, scratch| entry_in(src, settings, which, require_all, f, ast, scratch))
}

fn entry_in<T>(
	src: &str,
	settings: &Settings,
	which: Entry,
	require_all: bool,
	f: impl Fn(&Lower<'_>, u32) -> super::R<T>,
	ast: &mut Ast,
	scratch: &mut Scratch,
) -> Result<T, Error> {
	let Some(root) = parse_entry(src, which, settings.limits, require_all, ast, scratch) else {
		let d = scratch.diags.first().copied().expect("a failed parse records a diagnostic");
		// The same check on the input before the syntax error.
		let eager = cuts(src, 0, d.start)
			.into_iter()
			.find_map(|cut| {
				let prefix = src.get(..cut as usize)?;
				let mut ast = Ast::new();
				let mut scratch = Scratch::default();
				let root =
					parse_entry(prefix, which, settings.limits, false, &mut ast, &mut scratch)?;
				let lower = Lower {
					ast: &ast,
					src: prefix,
					opts: settings.opts,
				};
				Some(eager(f(&lower, root).err()?, d.start))
			})
			.flatten();
		let e = eager.map_or(Error::Syntax(d), Error::Lower);
		return Err(first_error(src, 0, e, false));
	};
	let lower = Lower {
		ast,
		src,
		opts: settings.opts,
	};
	f(&lower, root).map_err(|r| first_error(src, 0, Error::Lower(r), false))
}

/// Parses an expression in which a bare identifier is a field.
pub fn expr(src: &str, settings: &Settings, require_all: bool) -> Result<Expr, Error> {
	entry(src, settings, Entry::Expr, require_all, |l, root| l.expr(root))
}

/// Parses a scalar directly into its logical representation.
pub fn logical_expr(
	src: &str,
	settings: &Settings,
	require_all: bool,
	stored: bool,
) -> Result<crate::expr::logical::LogicalExpr, Error> {
	entry(src, settings, Entry::Expr, require_all, |l, root| l.logical_expr(root, stored))
}

/// Parses a stored block into its logical sequence under the stored-text profile.
pub fn logical_block(
	src: &str,
	settings: &Settings,
) -> Result<crate::expr::logical::LogicalOp, Error> {
	entry(src, settings, Entry::Block, true, |l, root| l.logical_statement(root, true))
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

/// Parses the `TYPE` of a field named `id`, without the enclosing `<` `>`: a kind
/// whose outermost tuple may name its components.
///
/// With `stored`, the text is a definition read back from the catalog, and names
/// that do not form a valid list are dropped instead of refused
/// ([`Lower::stored_id_type`]); otherwise they are a syntax error.
pub fn id_kind(
	src: &str,
	settings: &Settings,
	require_all: bool,
	stored: bool,
) -> Result<(Kind, Option<KeyNames>), Error> {
	entry(src, settings, Entry::IdKind, require_all, |l, root| {
		if stored {
			l.stored_id_type(root)
		} else {
			l.field_type(root, true)
		}
	})
}

/// Parses a projection list, or `VALUE` and one selector.
pub fn fields(src: &str, settings: &Settings, require_all: bool) -> Result<Fields, Error> {
	entry(src, settings, Entry::Fields, require_all, |l, root| {
		let n = &l.ast.nodes[root as usize];
		debug_assert_eq!(n.tag, Tag::Fields);
		l.fields_node(n)
	})
}

/// Parses a duration literal and nothing after it.
pub fn duration(src: &str, settings: &Settings) -> Result<std::time::Duration, Error> {
	entry(src, settings, Entry::Duration, true, |l, root| {
		let (s, ns) = decode_duration(l.text(root).as_bytes())
			.ok_or(Reject::new("Invalid duration, value overflowed maximum allowed value"))?;
		Ok(std::time::Duration::new(s, ns))
	})
}

/// Parses an identifier naming a table. Anything after it is ignored.
pub fn table(src: &str, settings: &Settings) -> Result<TableName, Error> {
	entry(src, settings, Entry::Table, false, |l, root| Ok(TableName::new(l.name(root)?)))
}

/// Parses a function name without arguments. Anything after it is ignored.
pub fn function(src: &str, settings: &Settings) -> Result<Function, Error> {
	entry(src, settings, Entry::Function, false, |l, root| l.function_ref(root))
}

/// Parses a datetime written without its `d"…"` delimiters; the whole of
/// `src` is the datetime.
pub fn datetime(src: &str) -> Result<surrealdb_types::Datetime, Error> {
	match super::lit::datetime(src).map_err(Error::Lower)? {
		Literal::Datetime(d) => Ok(d.into()),
		_ => unreachable!("a datetime body decodes to a datetime"),
	}
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
	with_arena(|ast, scratch| statement_in(src, settings, complete, ast, scratch))
}

fn statement_in(
	src: &str,
	settings: &Settings,
	complete: bool,
	ast: &mut Ast,
	scratch: &mut Scratch,
) -> Statement {
	match parse_statement(src, settings.limits, ast, scratch) {
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
				ast,
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

/// The checks made partway through a statement, once the part they
/// look at is read, by the start of their message. Clients have always been
/// told about these before any syntax error later in the statement, so they
/// are reported first.
const EAGER: &[&str] = &[
	"Missing split idiom",
	"Missing group idiom",
	"SPLIT and GROUP are mutually exclusive",
	"FLEXIBLE can only be used",
	"Expected one column, found",
	"Cannot create a count index with fields",
	"Invalid numbers of values to insert",
	"Invalid SCRAM verifier",
];

/// `r` when it is a check made partway through a statement, failing
/// before byte `at`.
fn eager(r: Reject, at: u32) -> Option<Reject> {
	let early = r.span.is_some_and(|(s, _)| s < at);
	(early && EAGER.iter().any(|m| r.msg.starts_with(m))).then_some(r)
}

/// A check made partway through the statement at `from` that fails on its
/// text before the syntax error at `at`, and so is the error reported
/// first. The statement is read up to each of [`cuts`] in turn, until a
/// prefix parses.
fn eager_in_statement(src: &str, settings: &Settings, from: u32, at: u32) -> Option<Reject> {
	let cuts = cuts(src, from, at);
	for cut in cuts {
		let prefix = src.get(..cut as usize)?;
		let mut ast = Ast::new();
		let mut scratch = Scratch::default();
		parse_script(prefix, settings.limits, &mut ast, &mut scratch);
		let i = ast.spans.iter().position(|&(s, _)| s == from)?;
		let s = ast.stmts[i];
		if ast.node(s).tag == Tag::Error {
			continue;
		}
		let lower = Lower {
			ast: &ast,
			src: prefix,
			opts: settings.opts,
		};
		return eager(lower.top(s).err()?, at);
	}
	None
}

/// Where to cut the input reading from `from` to look for a check made
/// before the syntax error at `at`: at `at`, then before each of the
/// few tokens before it, nearest first, that cannot continue what precedes
/// them (an operator, `;` or a closing delimiter), so that a clause already
/// finished and checked counts even when the error follows it directly.
fn cuts(src: &str, from: u32, at: u32) -> Vec<u32> {
	use surrealdb_parser_next::lex::{Lexer, T};
	let mut tokens = Vec::new();
	let mut l = Lexer::new(src.as_bytes());
	l.pos = from;
	loop {
		let t = l.next_token();
		if t.kind == T::Eof || t.start >= at {
			break;
		}
		tokens.push(t);
	}
	let ends =
		|k: T| {
			matches!(
				k,
				T::Semi
					| T::Lt | T::RParen
					| T::RBracket | T::RBrace
					| T::Plus | T::Minus
					| T::Star | T::StarStar
					| T::Slash | T::Percent
					| T::Eq | T::EqEq
					| T::Ne | T::Gt | T::Ge
					| T::Le | T::AndAnd
					| T::OrOr | T::Times
					| T::Divide | T::Contains
					| T::NotContains
					| T::ContainsAll
					| T::ContainsAny
					| T::ContainsNone
					| T::Inside | T::NotInside
					| T::AllInside | T::AnyInside
					| T::NoneInside
			)
		};
	std::iter::once(at)
		.chain(tokens.iter().rev().take(4).filter(|t| ends(t.kind)).map(|t| t.start))
		.collect()
}

/// The error reported for input parser-next rejected with `e`, reading from
/// `from`: escapes in strings and escaped identifiers, and the contents of
/// prefixed strings, are decoded as they are read, so an invalid one before
/// `e` is the error reported first.
pub(crate) fn first_error(src: &str, from: u32, e: Error, json: bool) -> Error {
	// A path is checked once all of it is read, its escapes decoded.
	const PATH: &str = "Invalid function/constant path";
	let at = match &e {
		Error::Syntax(d) if d.msg.starts_with(PATH) => d.end,
		// Detected at the second operator, after both operands are read.
		Error::Syntax(d) if d.msg.starts_with("Chained relational") => d.end,
		// Checked once the whole statement is read.
		Error::Syntax(d) if d.msg.starts_with("Parameter declarations without `let`") => d.end,
		// Reported over the module source, once it is decoded.
		Error::Syntax(d) if d.msg.starts_with("Expected the `UNSIGNED` keyword") => d.end,
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
/// bodies, whose strings are not decoded.
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
					&& let Some(r) = super::lit::prefixed_error(text, t.aux(), t.start)
				{
					return Some(r);
				}
			}
			T::Digits => {
				// A number or duration runs on over letters and quotes the
				// token lexer splits off, so `1d'x'` holds no `d'…'` string.
				// Where it does not lex as one (a record-id key such as
				// `01GE6…`, or `10u'`), its letters still belong to it.
				let mut n = l;
				if n.numeric(t.start).is_ok() {
					l = n;
				} else {
					let b = src.as_bytes();
					let mut end = t.end as usize;
					while b.get(end).is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_') {
						end += 1;
					}
					l.pos = end as u32;
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
