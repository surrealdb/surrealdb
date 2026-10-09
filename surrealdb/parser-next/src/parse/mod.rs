//! The parser.
//!
//! Statements are parsed by plain recursive descent. Expressions — every
//! production an input can nest cheaply: operators, parentheses, arrays,
//! objects, sets, blocks, closures, call arguments, idiom parts, record-id
//! keys, kinds — are parsed by a Pratt parser that keeps its pending work on
//! an explicit heap stack (`Frame`) instead of the native call stack. The
//! native stack grows only for a statement inside an expression
//! (`(SELECT …)`), a lookup with clauses and destructuring, and every such
//! level charges one of the depth budgets ([`Limits`]), which `syn` charges at
//! the same places.
//!
//! Errors are collected, not returned. The first error in a statement is
//! recorded, the statement becomes a [`Tag::Error`] node, and parsing resumes
//! at the next `;` outside brackets, so one parse reports one error per broken
//! statement and still yields every statement that did parse.

use crate::ast::{self, Assign, Ast, Binary, COVERED, DataKind, NONE, Node, NodeId, Prefix, Tag};
use crate::kw::Kw;
use crate::lex::{LexError, Lexer, NumKind, T, Tok};

mod expr;
mod idiom;
mod kind;
mod lit;
mod record;
mod stmt;

use expr::*;
use stmt::is_statement_keyword;

/// Depth budgets: `syn`'s three recursion limits, charged where `syn`
/// charges them, so that the same inputs are rejected for nesting too deeply.
/// Defaults are `syn`'s defaults for queries.
///
/// - `expr_depth` (`expr_recursion_limit`): one level per expression entered, per operator appended
///   to an expression's spine, per idiom part and per destructuring level.
/// - `query_depth` (`query_recursion_limit`): one level per nested expression context `syn` enters
///   through `parse_expr_field`, `parse_expr_table` or `parse_expr_inherit`: statement clauses,
///   parentheses, arguments, index parts, block statements, closure bodies.
/// - `object_depth` (`object_recursion_limit`): one level per array, object or set literal and per
///   nested kind.
///
/// Together they bound the tree's depth, the explicit stack, and the native
/// recursion (statements within expressions, lookups with clauses,
/// destructuring), each of whose levels charges at least one of them.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
	pub expr_depth: u16,
	pub query_depth: u16,
	pub object_depth: u16,
	/// Record-id keys follow identifier rules, so `a:1b` is not one key
	/// (`syn`'s `flexible_record_id` off).
	pub strict_record_ids: bool,
}

impl Default for Limits {
	fn default() -> Self {
		Limits {
			expr_depth: 128,
			query_depth: 20,
			object_depth: 100,
			strict_record_ids: false,
		}
	}
}

impl Limits {
	/// A bound on the native recursion depth. Each native level charges at
	/// least one budget, so the sum is never reached by input the budgets
	/// accept; it is a backstop.
	pub fn backstop(&self) -> u16 {
		self.expr_depth.saturating_add(self.query_depth).saturating_add(self.object_depth)
	}

	/// A bound on the tree's depth, for consumers that recurse over it. No
	/// construct adds more than two tree levels per budget level (a
	/// destructuring level adds an entry and a part), so input the budgets
	/// accept stays below it.
	pub fn tree_depth(&self) -> u16 {
		self.backstop().saturating_mul(2)
	}

	/// The budgets for re-parsing stored catalog text (`syn`'s
	/// `ParserSettings::STORED_TEXT`).
	pub fn stored_text() -> Self {
		Limits {
			expr_depth: 512,
			query_depth: 400,
			object_depth: 400,
			strict_record_ids: false,
		}
	}
}

/// A diagnostic. Messages are static, so recording one formats nothing; a
/// renderer at the API boundary adds the source snippet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Diag {
	pub start: u32,
	pub end: u32,
	pub msg: &'static str,
}

#[derive(Clone, Copy)]
struct Checkpoint {
	lex_pos: u32,
	tok: Tok,
	prev_end: u32,
	nodes: usize,
	extra: usize,
	vals: usize,
	frames: usize,
	diags: usize,
	budgets: u64,
}

/// Marker that a diagnostic was recorded and the statement is abandoned.
#[derive(Debug)]
pub struct Stop;

type R<T> = Result<T, Stop>;

/// Scratch space reused across parses.
#[derive(Default)]
pub struct Scratch {
	frames: Vec<Frame>,
	/// For each frame, the budgets in use when it was pushed, restored when
	/// it is popped.
	saved: Vec<u64>,
	vals: Vec<u32>,
	pub diags: Vec<Diag>,
}

pub struct Parser<'a, 's> {
	src: &'a [u8],
	text: &'a str,
	lex: Lexer<'a>,
	tok: Tok,
	prev_end: u32,
	pub ast: &'s mut Ast,
	frames: &'s mut Vec<Frame>,
	saved: &'s mut Vec<u64>,
	vals: &'s mut Vec<u32>,
	diags: &'s mut Vec<Diag>,
	limits: Limits,
	/// Budgets in use: expression, query and object levels.
	e: u16,
	q: u16,
	o: u16,
	/// Native recursion depth (statements within expressions, lookups with
	/// clauses, destructuring).
	native: u16,
	table_as_field: bool,
	/// Set when the parser has looked at a token within [`END_SLACK`] bytes
	/// of the end of input, so more input could have changed what it saw.
	/// Only a failed statement consults it: a parsed one is confirmed by its
	/// `;`.
	saw_end: core::cell::Cell<bool>,
}

/// How close to the end of input a token must end for more input to
/// possibly change it: the longest lookahead past a token's end that any
/// lexer entry point takes.
const END_SLACK: u32 = 4;

/// Parses a script into `ast`, reporting into `scratch.diags`. Both are
/// cleared first and keep their capacity.
pub fn parse_script(src: &str, limits: Limits, ast: &mut Ast, scratch: &mut Scratch) {
	ast.clear();
	scratch.diags.clear();
	scratch.frames.clear();
	scratch.saved.clear();
	scratch.vals.clear();
	let mut p = Parser::new(src, limits, ast, scratch);
	p.script();
}

/// Where [`parse_statement`] stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementEnd {
	/// Only empty statements and trivia remain. `consumed` bytes of `;`
	/// tokens and the trivia between them can be dropped; trivia after the
	/// last `;` cannot, since more input could turn it into a statement or
	/// extend a comment.
	Empty {
		consumed: u32,
	},
	/// One statement was parsed into the tree (as a [`Tag::Error`] node if it
	/// failed), ending at `consumed`: after its `;`, or at the end of `src`.
	/// When `final_`, no byte appended to `src` can change the result.
	/// Otherwise more input could extend or repair the statement, and
	/// `consumed` is only where it ends if the input ends here.
	Statement {
		final_: bool,
		consumed: u32,
	},
}

/// Parses at most one statement from the start of `src`, for callers that
/// receive input in pieces. The tree holds that one statement; spans are
/// offsets into `src`.
pub fn parse_statement(
	src: &str,
	limits: Limits,
	ast: &mut Ast,
	scratch: &mut Scratch,
) -> StatementEnd {
	ast.clear();
	scratch.diags.clear();
	scratch.frames.clear();
	scratch.saved.clear();
	scratch.vals.clear();
	let mut p = Parser::new(src, limits, ast, scratch);
	match p.next_statement() {
		None => StatementEnd::Empty {
			consumed: p.prev_end,
		},
		Some(final_) => StatementEnd::Statement {
			final_,
			consumed: if p.at(T::Semi) {
				p.tok.end
			} else {
				src.len() as u32
			},
		},
	}
}

/// What [`parse_entry`] parses: the parts of the grammar `syn` exposes as
/// entry points of their own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Entry {
	/// An expression in which a bare identifier is a field (`syn`'s
	/// `parse_expr_field`).
	Expr,
	/// A `{ … }` block.
	Block,
	/// An idiom without a leading value (`syn`'s `parse_plain_idiom`).
	Idiom,
	/// A kind without the enclosing `<` `>` (`syn`'s `parse_inner_kind`).
	Kind,
	/// A projection list, or `VALUE` and one selector, as a [`Tag::Fields`]
	/// node.
	Fields,
}

/// Parses one `entry` from the start of `src`. With `require_all`, anything
/// after it is an error; without, parsing stops where the entry ends, as
/// `syn`'s non-definition entry points do. Returns the root, or `None` with a
/// diagnostic in `scratch.diags`.
pub fn parse_entry(
	src: &str,
	entry: Entry,
	limits: Limits,
	require_all: bool,
	ast: &mut Ast,
	scratch: &mut Scratch,
) -> Option<NodeId> {
	ast.clear();
	scratch.diags.clear();
	scratch.frames.clear();
	scratch.saved.clear();
	scratch.vals.clear();
	let mut p = Parser::new(src, limits, ast, scratch);
	let res = match entry {
		Entry::Expr => p.expr(true),
		Entry::Block => p.block(),
		Entry::Idiom => p.plain_idiom(),
		Entry::Kind => p.kind(),
		Entry::Fields => p.fields_entry(),
	};
	let res = res.and_then(|id| {
		if require_all && !p.at(T::Eof) {
			Err(p.unexpected("expected no more tokens"))
		} else {
			Ok(id)
		}
	});
	res.ok()
}

/// Parses a record id (`table:key`, without a range) from the start of
/// `src`, as the contents of an `r"…"` string are read. Anything after the
/// record id is ignored.
pub fn parse_record_id(src: &str, ast: &mut Ast, scratch: &mut Scratch) -> Option<NodeId> {
	ast.clear();
	scratch.diags.clear();
	scratch.frames.clear();
	scratch.saved.clear();
	scratch.vals.clear();
	let mut p = Parser::new(src, Limits::default(), ast, scratch);
	p.record_id_only().ok()
}

/// [`parse_record_id`], succeeding only when the record id is the whole of
/// `src`.
pub fn parse_record_id_all(src: &str, ast: &mut Ast, scratch: &mut Scratch) -> Option<NodeId> {
	ast.clear();
	scratch.diags.clear();
	scratch.frames.clear();
	scratch.saved.clear();
	scratch.vals.clear();
	let mut p = Parser::new(src, Limits::default(), ast, scratch);
	let id = p.record_id_only().ok()?;
	p.at(T::Eof).then_some(id)
}

impl<'a, 's> Parser<'a, 's> {
	fn new(text: &'a str, limits: Limits, ast: &'s mut Ast, scratch: &'s mut Scratch) -> Self {
		let src = text.as_bytes();
		let mut lex = Lexer::new(src);
		let tok = lex.next_token();
		let p = Parser {
			src,
			text,
			lex,
			tok,
			prev_end: 0,
			ast,
			frames: &mut scratch.frames,
			saved: &mut scratch.saved,
			vals: &mut scratch.vals,
			diags: &mut scratch.diags,
			limits,
			e: 0,
			q: 0,
			o: 0,
			native: 0,
			table_as_field: true,
			saw_end: core::cell::Cell::new(false),
		};
		p.note(p.tok);
		p
	}

	/// Records whether `t` ends close enough to the end of input that more
	/// input could change it.
	#[inline]
	fn note(&self, t: Tok) {
		if t.end + END_SLACK > self.src.len() as u32 {
			self.saw_end.set(true);
		}
	}

	// ---------------------------------------------------------------- tokens

	#[inline]
	fn bump(&mut self) -> Tok {
		let t = self.tok;
		self.prev_end = t.end;
		self.tok = self.lex.next_token();
		self.note(self.tok);
		t
	}

	/// The token after the current one.
	fn peek1(&self) -> Tok {
		let mut l = self.lex;
		let t = l.next_token();
		self.note(t);
		t
	}

	fn peek2(&self) -> Tok {
		let mut l = self.lex;
		l.next_token();
		let t = l.next_token();
		self.note(t);
		t
	}

	/// The token after the current one if no whitespace separates them.
	fn adjacent1(&self) -> Option<Tok> {
		let t = self.peek1();
		(!t.ws()).then_some(t)
	}

	/// Restarts lexing at byte `pos`.
	fn reset_to(&mut self, pos: u32) {
		self.lex.pos = pos;
		self.tok = self.lex.next_token();
		self.note(self.tok);
	}

	#[inline]
	fn at(&self, kind: T) -> bool {
		self.tok.kind == kind
	}

	#[inline]
	fn at_kw(&self, kw: Kw) -> bool {
		self.tok.is_kw(kw)
	}

	fn eat(&mut self, kind: T) -> bool {
		if self.at(kind) {
			self.bump();
			true
		} else {
			false
		}
	}

	fn eat_kw(&mut self, kw: Kw) -> bool {
		if self.at_kw(kw) {
			self.bump();
			true
		} else {
			false
		}
	}

	fn expect(&mut self, kind: T, msg: &'static str) -> R<Tok> {
		if self.at(kind) {
			Ok(self.bump())
		} else {
			Err(self.unexpected(msg))
		}
	}

	fn expect_kw(&mut self, kw: Kw, msg: &'static str) -> R<Tok> {
		if self.at_kw(kw) {
			Ok(self.bump())
		} else {
			Err(self.unexpected(msg))
		}
	}

	// ---------------------------------------------------------------- errors

	fn error(&mut self, start: u32, end: u32, msg: &'static str) -> Stop {
		self.diags.push(Diag {
			start,
			end,
			msg,
		});
		Stop
	}

	fn unexpected(&mut self, msg: &'static str) -> Stop {
		let t = self.tok;
		let msg = if t.kind == T::Invalid {
			"invalid token"
		} else {
			msg
		};
		self.error(t.start, t.end, msg)
	}

	fn lex_error(&mut self, start: u32, e: LexError) -> Stop {
		let msg = match e {
			LexError::InvalidNumber => "invalid number",
			LexError::InvalidDuration => "invalid duration",
			LexError::InvalidRegex => "unterminated regex",
			LexError::IntOverflow => "number does not fit in a 64-bit signed integer",
			LexError::UnterminatedString => "unterminated string",
			LexError::UnterminatedComment => "unterminated comment",
			LexError::UnterminatedIdent => "unterminated identifier",
			LexError::UnexpectedChar => "unexpected character",
			LexError::UnterminatedScript => "invalid JavaScript function, unexpected end of input",
		};
		let end = self.lex.pos.max(start + 1);
		if end + END_SLACK > self.src.len() as u32 {
			self.saw_end.set(true);
		}
		self.error(start, end, msg)
	}

	// ------------------------------------------------------------------ tree

	fn depth_of(&self, id: NodeId) -> u16 {
		if id == NONE {
			0
		} else {
			self.ast.nodes[id as usize].depth & !COVERED
		}
	}

	/// Appends a node whose depth is one more than its deepest child among
	/// `a`, `b` and `list_depth`, failing if that exceeds the budget.
	fn node(
		&mut self,
		tag: Tag,
		op: u8,
		a: u32,
		b: u32,
		start: u32,
		child_depth: u16,
	) -> R<NodeId> {
		// The budgets bound the tree's depth; this is a backstop for
		// consumers that recurse over it.
		let depth = child_depth.saturating_add(1);
		if depth > self.limits.tree_depth() {
			return Err(self.error(start, self.prev_end, "expression nests too deeply"));
		}
		let id = self.ast.nodes.len() as NodeId;
		self.ast.nodes.push(Node {
			tag,
			op,
			depth,
			a,
			b,
			start,
			end: self.prev_end.max(start),
		});
		Ok(id)
	}

	fn leaf(&mut self, tag: Tag, op: u8, t: Tok) -> NodeId {
		let id = self.ast.nodes.len() as NodeId;
		self.ast.nodes.push(Node {
			tag,
			op,
			depth: 1,
			a: 0,
			b: 0,
			start: t.start,
			end: t.end,
		});
		id
	}

	fn leaf_span(&mut self, tag: Tag, op: u8, a: u32, b: u32, start: u32, end: u32) -> NodeId {
		let id = self.ast.nodes.len() as NodeId;
		self.ast.nodes.push(Node {
			tag,
			op,
			depth: 1,
			a,
			b,
			start,
			end,
		});
		id
	}

	/// Moves `vals[mark..]` into `extra` as a list, returning its index and
	/// the depth of its deepest item.
	fn list_from(&mut self, mark: u32) -> (u32, u16) {
		let mark = mark as usize;
		let idx = self.ast.extra.len() as u32;
		let n = self.vals.len() - mark;
		self.ast.extra.push(n as u32);
		let mut depth = 0;
		for i in mark..self.vals.len() {
			let v = self.vals[i];
			depth = depth.max(self.depth_of(v));
			self.ast.extra.push(v);
		}
		self.vals.truncate(mark);
		(idx, depth)
	}

	/// Moves `vals[mark..]` into `extra` as a list that may hold `NONE`
	/// entries, which carry no depth.
	fn list_from_raw(&mut self, mark: u32) -> u32 {
		let mark = mark as usize;
		let idx = self.ast.extra.len() as u32;
		let n = self.vals.len() - mark;
		self.ast.extra.push(n as u32);
		for i in mark..self.vals.len() {
			let v = self.vals[i];
			self.ast.extra.push(v);
		}
		self.vals.truncate(mark);
		idx
	}

	fn budgets(&self) -> u64 {
		self.e as u64 | (self.q as u64) << 16 | (self.o as u64) << 32
	}

	fn set_budgets(&mut self, b: u64) {
		self.e = b as u16;
		self.q = (b >> 16) as u16;
		self.o = (b >> 32) as u16;
	}

	/// Charges one expression level.
	fn enter_e(&mut self) -> R<()> {
		if self.e >= self.limits.expr_depth {
			return Err(self.error(self.tok.start, self.tok.end, "expression nests too deeply"));
		}
		self.e += 1;
		Ok(())
	}

	/// Charges one query level.
	fn enter_q(&mut self) -> R<()> {
		if self.q >= self.limits.query_depth {
			return Err(self.error(self.tok.start, self.tok.end, "statements nest too deeply"));
		}
		self.q += 1;
		Ok(())
	}

	/// Charges one object level.
	fn enter_o(&mut self) -> R<()> {
		if self.o >= self.limits.object_depth {
			return Err(self.error(self.tok.start, self.tok.end, "objects nest too deeply"));
		}
		self.o += 1;
		Ok(())
	}

	/// Pushes a frame, charging the budgets the context it opens costs in
	/// `syn`; popping it restores them.
	fn push_frame(&mut self, f: Frame) -> R<()> {
		self.saved.push(self.budgets());
		self.frames.push(f);
		match f {
			Frame::Base {
				charged,
				..
			} => {
				if charged {
					self.enter_q()?;
				}
				self.enter_e()
			}
			Frame::Infix {
				..
			}
			| Frame::Prefix {
				..
			}
			| Frame::Cast {
				..
			} => self.enter_e(),
			Frame::Paren {
				..
			}
			| Frame::Args {
				..
			}
			| Frame::Bracket {
				..
			}
			| Frame::BlockOrSet {
				..
			}
			| Frame::Block {
				..
			} => {
				self.enter_q()?;
				self.enter_e()
			}
			Frame::Closure {
				returns,
				..
			} if returns == NONE => {
				self.enter_q()?;
				self.enter_e()
			}
			Frame::Array {
				..
			}
			| Frame::Object {
				..
			}
			| Frame::Set {
				..
			} => {
				self.enter_o()?;
				self.enter_e()
			}
			Frame::KindAlts {
				option: true,
				..
			}
			| Frame::KindSized {
				..
			}
			| Frame::KindObject {
				..
			}
			| Frame::KindTuple {
				..
			} => self.enter_o(),
			_ => Ok(()),
		}
	}

	/// Pops the top frame, restoring the budgets in use when it was pushed.
	fn pop_frame(&mut self) -> Option<Frame> {
		let f = self.frames.pop()?;
		if let Some(b) = self.saved.pop() {
			self.set_budgets(b);
		}
		Some(f)
	}

	/// Pops frames down to `len`.
	fn truncate_frames(&mut self, len: usize) {
		if len < self.frames.len() {
			self.set_budgets(self.saved[len]);
			self.frames.truncate(len);
			self.saved.truncate(len);
		}
	}

	// --------------------------------------------------------------- script

	fn script(&mut self) {
		while self.next_statement().is_some() {}
	}

	/// Skips empty statements and parses the next statement into the tree.
	/// Returns `None` at the end of input, or whether more input could not
	/// change the statement. A parsed statement is final when a `;` follows
	/// it. A failed one is final when its recovery stopped at a `;` and the
	/// parser saw nothing near the end of input before failing: recovery
	/// lexes without the parser's context (a regex can hold a `;`), so its
	/// `;` alone does not prove the parser would have failed the same way.
	fn next_statement(&mut self) -> Option<bool> {
		while self.eat(T::Semi) {}
		if self.at(T::Eof) {
			return None;
		}
		let start = self.tok.start;
		self.saw_end.set(false);
		self.note(self.tok);
		let res = self.statement().and_then(|id| {
			if self.at(T::Semi) || self.at(T::Eof) {
				Ok(id)
			} else {
				Err(self.unexpected("expected `;` or the end of the query"))
			}
		});
		match res {
			Ok(id) => {
				self.ast.stmts.push(id);
				self.ast.spans.push((start, self.prev_end));
			}
			Err(Stop) => {
				let saw_end = self.saw_end.get();
				self.frames.clear();
				self.saved.clear();
				self.vals.clear();
				self.set_budgets(0);
				self.native = 0;
				self.table_as_field = true;
				let end = self.recover(start).max(start);
				let id = self.leaf_span(Tag::Error, 0, 0, 0, start, end);
				self.ast.stmts.push(id);
				self.ast.spans.push((start, end));
				// The error is only final if nothing the parser looked at
				// could change with more input.
				return Some(self.at(T::Semi) && !saw_end);
			}
		}
		Some(self.at(T::Semi))
	}

	/// Skips from `start` to the next `;` outside brackets and returns where
	/// the skipped statement ends. Brackets are matched by kind, and a closer
	/// that does not match the innermost opener closes the openers above its
	/// match, so one missing `]` does not swallow the rest of the script.
	fn recover(&mut self, start: u32) -> u32 {
		self.reset_to(start);
		// Open bracket kinds, two bits per level, innermost lowest; levels
		// past what fits are only counted.
		let mut kinds: u128 = 0;
		let mut depth: u32 = 0;
		loop {
			let kind = match self.tok.kind {
				T::Eof => return self.prev_end,
				T::Semi if depth == 0 => return self.prev_end,
				T::LParen => Some((true, 1)),
				T::LBracket => Some((true, 2)),
				T::LBrace => Some((true, 3)),
				T::RParen => Some((false, 1)),
				T::RBracket => Some((false, 2)),
				T::RBrace => Some((false, 3)),
				_ => None,
			};
			match kind {
				Some((true, k)) => {
					kinds = kinds << 2 | k;
					depth += 1;
				}
				Some((false, k)) => {
					let known = depth.min(64);
					if let Some(i) = (0..known).find(|&i| (kinds >> (2 * i)) & 3 == k) {
						kinds >>= 2 * (i + 1);
						depth -= i + 1;
					} else if depth > 64 {
						depth -= 1;
					}
				}
				None => {}
			}
			self.bump();
		}
	}

	/// A top-level statement. Statements other than the top-level-only ones
	/// are expressions, so an operator may follow one, as in `syn`.
	fn statement(&mut self) -> R<NodeId> {
		let t = self.tok;
		if t.kind == T::Ident {
			if let Some(id) = self.top_level_statement(t)? {
				return Ok(id);
			}
			if is_statement_keyword(self, t) {
				self.bump();
				// The statement is the first operand of the top expression,
				// which `syn` charges one expression level.
				self.e += 1;
				let s = self.statement_body(t)?;
				self.e -= 1;
				let e = self.expr_from(s, true)?;
				self.check_letless(e)?;
				return Ok(e);
			}
		}
		let covered = self.at(T::LParen);
		let e = self.expr_from(NONE, true)?;
		if !covered {
			self.check_letless(e)?;
		}
		Ok(e)
	}

	/// Rejects `$a = 1` as a statement: parameter declarations without `LET`
	/// are deprecated.
	fn check_letless(&mut self, e: NodeId) -> R<()> {
		let n = self.ast.nodes[e as usize];
		if n.tag == Tag::Binary
			&& n.op == Binary::Eq as u8
			&& self.ast.nodes[n.a as usize].tag == Tag::Param
		{
			return Err(self.error(
				n.start,
				n.end,
				"parameter declarations without `let` are deprecated",
			));
		}
		Ok(())
	}

	/// A statement inside an expression, at its keyword `t`: native
	/// recursion, charged against the query depth budget.
	fn nested_statement(&mut self, t: Tok) -> R<NodeId> {
		self.enter_native()?;
		let saved = self.table_as_field;
		self.bump();
		let res = self.statement_body(t);
		self.table_as_field = saved;
		self.native -= 1;
		res
	}

	/// Counts one level of native recursion. Each level also charges at least
	/// one depth budget, so the cap is only a backstop.
	fn enter_native(&mut self) -> R<()> {
		if self.native >= self.limits.backstop() {
			return Err(self.error(self.tok.start, self.tok.end, "statements nest too deeply"));
		}
		self.native += 1;
		Ok(())
	}

	/// Depth of the deepest node appended since `first`: the depth of a
	/// statement's deepest clause, since children are appended before their
	/// parents.
	fn max_depth_since(&self, first: usize) -> u16 {
		self.ast.nodes[first..].iter().map(|n| n.depth & !COVERED).max().unwrap_or(0)
	}

	/// `FETCH` targets: parameters and plain idioms, or at `type`, a field
	/// list whose expressions are the targets (as in `syn`).
	fn fetch_list(&mut self) -> R<u32> {
		let mark = self.vals.len() as u32;
		loop {
			if self.at(T::Param) {
				let t = self.bump();
				let p = self.leaf(Tag::Param, 0, t);
				self.vals.push(p);
			} else if self.at_kw(Kw::Type) {
				let fields = self.fields()?;
				let n = self.ast.extra[fields as usize] as usize;
				for i in 0..n {
					let f = self.ast.extra[fields as usize + 1 + i];
					let node = self.ast.nodes[f as usize];
					if node.tag == Tag::Selector {
						self.vals.push(node.a);
					}
				}
			} else {
				let i = self.plain_idiom()?;
				self.vals.push(i);
			}
			if !self.eat(T::Comma) {
				break;
			}
		}
		Ok(self.list_from(mark).0)
	}

	/// The selector after `VALUE`: an expression and an optional identifier
	/// alias.
	fn value_selector(&mut self) -> R<NodeId> {
		let s = self.tok.start;
		let e = self.expr(true)?;
		let alias = if self.eat_kw(Kw::As) {
			let t = self.ident()?;
			self.leaf(Tag::Field, 0, t)
		} else {
			NONE
		};
		let d = self.depth_of(e).max(self.depth_of(alias));
		self.node(Tag::Selector, 0, e, alias, s, d)
	}

	/// A projection list or `VALUE` and one selector, as a [`Tag::Fields`]
	/// node.
	fn fields_entry(&mut self) -> R<NodeId> {
		let s = self.tok.start;
		let (a, b) = if self.eat_kw(Kw::Value) {
			(NONE, self.value_selector()?)
		} else {
			(self.fields()?, NONE)
		};
		self.node(Tag::Fields, 0, a, b, s, 1)
	}

	/// A projection list: `*`, or expressions with optional `AS` aliases.
	fn fields(&mut self) -> R<u32> {
		let mark = self.vals.len() as u32;
		loop {
			if self.at(T::Star) {
				let t = self.bump();
				let n = self.leaf(Tag::FieldAll, 0, t);
				self.vals.push(n);
			} else {
				let s = self.tok.start;
				let e = self.expr(true)?;
				let alias = if self.eat_kw(Kw::As) {
					self.plain_idiom()?
				} else {
					NONE
				};
				let d = self.depth_of(e).max(self.depth_of(alias));
				let n = self.node(Tag::Selector, 0, e, alias, s, d)?;
				self.vals.push(n);
			}
			if !self.eat(T::Comma) {
				break;
			}
		}
		Ok(self.list_from(mark).0)
	}

	fn expr_list(&mut self, table_as_field: bool) -> R<u32> {
		let mark = self.vals.len() as u32;
		loop {
			let e = self.expr(table_as_field)?;
			self.vals.push(e);
			if !self.eat(T::Comma) {
				break;
			}
		}
		Ok(self.list_from(mark).0)
	}

	fn ident(&mut self) -> R<Tok> {
		if self.tok.is_ident() {
			Ok(self.bump())
		} else {
			Err(self.unexpected("expected an identifier"))
		}
	}

	/// A point to return to: the arena only grows, so undoing a speculative
	/// parse is a few truncations.
	fn checkpoint(&self) -> Checkpoint {
		Checkpoint {
			lex_pos: self.lex.pos,
			tok: self.tok,
			prev_end: self.prev_end,
			nodes: self.ast.nodes.len(),
			extra: self.ast.extra.len(),
			vals: self.vals.len(),
			frames: self.frames.len(),
			diags: self.diags.len(),
			budgets: self.budgets(),
		}
	}

	fn restore(&mut self, cp: Checkpoint) {
		self.lex.pos = cp.lex_pos;
		self.tok = cp.tok;
		self.prev_end = cp.prev_end;
		self.ast.nodes.truncate(cp.nodes);
		self.ast.extra.truncate(cp.extra);
		self.vals.truncate(cp.vals);
		self.truncate_frames(cp.frames);
		self.set_budgets(cp.budgets);
		self.diags.truncate(cp.diags);
	}

	/// Appends a statement record to `extra`, returning its index.
	fn record(&mut self, slots: &[u32]) -> u32 {
		let idx = self.ast.extra.len() as u32;
		self.ast.extra.extend_from_slice(slots);
		idx
	}

	/// A statement node over everything appended since node `first`.
	fn stmt_node(
		&mut self,
		tag: Tag,
		op: u8,
		a: u32,
		b: u32,
		start: u32,
		first: usize,
	) -> R<NodeId> {
		let d = self.max_depth_since(first);
		self.node(tag, op, a, b, start, d)
	}

	/// `NAMESPACE`, `DATABASE` or `ROOT`, as a base code.
	fn base(&mut self) -> R<u32> {
		let b = match self.tok.kw {
			_ if self.tok.kind != T::Ident => 0,
			Kw::Namespace => ast::BASE_NS,
			Kw::Database => ast::BASE_DB,
			Kw::Root => ast::BASE_ROOT,
			_ => 0,
		};
		if b == 0 {
			return Err(self.unexpected("expected `NAMESPACE`, `DATABASE` or `ROOT`"));
		}
		self.bump();
		Ok(b)
	}

	/// The source text of a token.
	fn text_of(&self, t: Tok) -> &'a str {
		&self.text[t.start as usize..t.end as usize]
	}

	/// An identifier as a `Name` leaf.
	fn name(&mut self) -> R<NodeId> {
		let t = self.ident()?;
		Ok(self.leaf(Tag::Name, 0, t))
	}

	fn empty_list(&mut self) -> u32 {
		let idx = self.ast.extra.len() as u32;
		self.ast.extra.push(0);
		idx
	}
}

/// Marker node id a primary returns when it pushed a frame instead of
/// producing a value; the engine then wants an operand.
const PUSHED: NodeId = NONE - 1;

/// Whether a token can start a primary value (the current parser's
/// `kind_starts_prime_value`).
fn starts_prime(t: &Tok) -> bool {
	matches!(
		t.kind,
		T::Plus
			| T::Minus
			| T::Str | T::Digits
			| T::NaN | T::LParen
			| T::LBrace
			| T::LBracket
			| T::Slash
			| T::Pipe | T::OrOr
			| T::Lt | T::Arrow
			| T::Param
			| T::DotDot
			| T::Ident
			| T::EscIdent
	)
}

/// Whether `t` can start an expression (`syn`'s `kind_starts_expression`).
pub fn starts_expression(t: &Tok) -> bool {
	starts_prime(t)
}

#[cfg(test)]
mod test {
	use super::*;

	fn run(src: &str) -> (Ast, Vec<Diag>) {
		crate::parse(src)
	}

	#[test]
	fn recovers_at_statement_boundaries() {
		let (ast, diags) =
			run("SELECT * FROM; SELECT a FROM b WHERE (c = [1, 2); CREATE x SET y = 1; UPDATE");
		assert_eq!(ast.stmts.len(), 4);
		let tags: Vec<_> = ast.stmts.iter().map(|&s| ast.node(s).tag).collect();
		assert_eq!(tags, [Tag::Error, Tag::Error, Tag::Create, Tag::Error]);
		assert_eq!(diags.len(), 3);
	}

	fn run_with(src: &str, limits: Limits) -> (Ast, Vec<Diag>) {
		let mut ast = Ast::new();
		let mut scratch = Scratch::default();
		parse_script(src, limits, &mut ast, &mut scratch);
		(ast, scratch.diags)
	}

	#[test]
	fn rejects_deep_nesting_with_bounded_cost() {
		let lifted = Limits {
			expr_depth: u16::MAX,
			query_depth: u16::MAX,
			object_depth: u16::MAX,
			strict_record_ids: false,
		};
		for open in ["(", "[", "-(", "{a:", "math::abs(", "!", "{1;", "a[", "<array<"] {
			// `<array<array<…` nests a kind; the rest nest expressions.
			let nest = |n: usize| match open.strip_prefix('<') {
				Some(kind) => format!("<{}int", kind.repeat(n)),
				None => open.repeat(n) + "1",
			};
			let (ast, diags) = run(&nest(100_000));
			assert_eq!(ast.stmts.len(), 1, "{open}");
			assert_eq!(diags.len(), 1, "{open}");
			assert!(diags[0].msg.ends_with("too deeply"), "{open}: {}", diags[0].msg);
			// With the budgets lifted, the nesting still costs no native
			// stack: a small thread parses it (to the missing closers, if any).
			let src = nest(30_000);
			let (ast, diags) = std::thread::Builder::new()
				.stack_size(64 * 1024)
				.spawn(move || run_with(&src, lifted))
				.expect("spawn")
				.join()
				.expect("no overflow");
			assert_eq!(ast.stmts.len(), 1, "{open}");
			assert!(diags.len() <= 1, "{open}");
		}
	}

	#[test]
	fn bounds_tree_depth_of_flat_chains() {
		let src = "1".to_owned() + &" + 1".repeat(10_000);
		let (_, diags) = run(&src);
		assert_eq!(diags[0].msg, "expression nests too deeply");
		let src = "a".to_owned() + &".a".repeat(500);
		let (_, diags) = run(&src);
		assert_eq!(diags[0].msg, "expression nests too deeply");
	}

	#[test]
	fn bounds_statement_nesting() {
		let src = "(SELECT * FROM ".repeat(50) + "1" + &")".repeat(50);
		let (_, diags) = run(&src);
		assert_eq!(diags[0].msg, "statements nest too deeply");
	}

	#[test]
	fn reuse_stops_allocating() {
		let mut ast = Ast::new();
		let mut scratch = Scratch::default();
		let src = "SELECT a, b.c[0], count() AS n FROM t:1..5 WHERE x IN [1, 2] AND y = { k: 'v' }";
		parse_script(src, Limits::default(), &mut ast, &mut scratch);
		let caps = (ast.nodes.capacity(), ast.extra.capacity(), scratch.frames.capacity());
		parse_script(src, Limits::default(), &mut ast, &mut scratch);
		assert_eq!(caps, (ast.nodes.capacity(), ast.extra.capacity(), scratch.frames.capacity()));
		assert!(scratch.diags.is_empty());
	}
}
