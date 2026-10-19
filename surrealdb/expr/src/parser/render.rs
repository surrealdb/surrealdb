//! Canonical SurrealQL text straight from the parser arena.
//!
//! Each `render_*` method writes the text that `Lower::expr(id)?.to_sql()`
//! (or the matching idiom, part or top-level lowering) produces for the node,
//! without building the expression. Structure, operators and parentheses are
//! derived from the arena; leaves are decoded by the same `Lower` helpers and
//! written through the formatter types the `fmt_sql` arms use.
//!
//! Parentheses are decided from a node's [`Shape`], the part of its lowered
//! `Expr` variant that `fmt_sql` consults, so no expression is built. Only
//! `DEFINE`, `ALTER`, `REMOVE` and the top-level-only statements render by
//! lowering the statement and printing it.
//!
//! The layout is single-line: [`SqlFormat::SingleLine`].

use common::fmt::{
	EscapeIdent, EscapeKwFreeIdent, EscapeKwIdent, EscapeObjectKey, Float, QuoteStr, SqlDuration,
};
use surrealdb_parser_next::ast::{self, Ast, Binary, NONE, Node, NodeId, Prefix, Tag};
use surrealdb_parser_next::decode;
use surrealdb_parser_next::lex::decode_duration;
use surrealdb_types::{SqlFormat, ToSql};

use super::{CHECK_NAMES, FILES_DISABLED, Lower, R, Reject, binary, lit};
use crate::expr::operator::BindingPower;
use crate::expr::{BinaryOperator, Function, Literal};

/// Writes `v` in the single-line layout.
fn put<T: ToSql + ?Sized>(out: &mut String, v: &T) {
	v.fmt_sql(out, SqlFormat::SingleLine);
}

/// What `LitShape` records about a lowered `Expr::Literal`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LitShape {
	None,
	Null,
	Int {
		neg: bool,
	},
	Float {
		neg: bool,
	},
	Decimal {
		neg: bool,
	},
	RecordId,
	UnboundedRange,
	Other,
}

/// The first part of a lowered idiom, as `Idiom::fmt_sql` distinguishes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FirstPart {
	Empty,
	Field,
	Doc,
	/// A lookup; `out` is whether it is the `->` direction.
	Lookup {
		out: bool,
	},
	/// A `Part::Start` holding the expression at this node.
	Start(NodeId),
	Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PostfixKind {
	Range,
	Call,
}

/// The variant of the lowered `Expr` a node stands for, reduced to what
/// parenthesisation reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
	Lit(LitShape),
	Param,
	Table,
	Mock,
	Block,
	Constant,
	Idiom(FirstPart),
	Prefix {
		bp: BindingPower,
		neg: bool,
	},
	Postfix(PostfixKind),
	Binary(BindingPower),
	FunctionCall,
	Closure,
	Break,
	Continue,
	Throw,
	Return {
		fetch: bool,
	},
	/// A statement-shaped expression: `SELECT`, `IF`, `LET`, `DEFINE`, ...
	Stmt,
}

impl Shape {
	/// `BindingPower::for_expr` of the lowered expression.
	fn bp(self) -> BindingPower {
		match self {
			Shape::Prefix {
				bp,
				..
			}
			| Shape::Binary(bp) => bp,
			Shape::Postfix(PostfixKind::Range) => BindingPower::Range,
			Shape::Postfix(PostfixKind::Call) => BindingPower::Call,
			_ => BindingPower::Prime,
		}
	}

	/// `Expr::needs_parentheses`.
	fn needs_parentheses(self) -> bool {
		matches!(
			self,
			Shape::Lit(LitShape::UnboundedRange | LitShape::RecordId)
				| Shape::Closure
				| Shape::Break
				| Shape::Continue
				| Shape::Throw
				| Shape::Return { .. }
				| Shape::Stmt
				| Shape::Postfix(_)
		)
	}

	/// Whether `CoverStmts` parenthesises the expression.
	fn covered(self) -> bool {
		matches!(
			self,
			Shape::Stmt
				| Shape::Return {
					fetch: true
				}
		)
	}
}

/// The operator text and binding power of a prefix operator code, and whether
/// it is a negation.
fn prefix_op(op: u8) -> (&'static str, BindingPower, bool) {
	match op {
		x if x == Prefix::Not as u8 => ("!", BindingPower::Prefix, false),
		x if x == Prefix::Neg as u8 => ("-", BindingPower::Prefix, true),
		x if x == Prefix::Pos as u8 => ("+", BindingPower::Prefix, false),
		x if x == Prefix::Range as u8 => ("..", BindingPower::Range, false),
		_ => ("..=", BindingPower::Range, false),
	}
}

/// An index into `extra` of a record as a slice of `len` slots.
fn slots(ast: &Ast, at: u32, len: usize) -> &[u32] {
	&ast.extra[at as usize..at as usize + len]
}

/// One part of a lowered idiom, before it is written.
#[derive(Clone, Copy)]
enum PartRef {
	Doc,
	/// A `Part::Field` named by an idiom head.
	Name(NodeId),
	/// A `Part::Start` holding the expression at this node.
	Start(NodeId),
	/// A part node of the idiom's part list.
	Listed(NodeId),
}

impl Lower<'_> {
	/// Appends to `out` the canonical single-line SurrealQL of the expression
	/// at `id`: the text `self.expr(id)?.to_sql()` produces, without building
	/// the expression. Fails where lowering the node would fail on a leaf
	/// (escape, duration overflow, unknown builtin, disabled grammar).
	pub fn render_expr(&self, id: NodeId, out: &mut String) -> R<()> {
		let n = self.n(id);
		match n.tag {
			Tag::Array
			| Tag::Object
			| Tag::Prefix
			| Tag::Cast
			| Tag::Binary
			| Tag::Block
			| Tag::Set => self.render_nesting(n, out),
			_ => self.render_other(id, out),
		}
	}

	/// The text `self.idiom(id)?.to_sql()` produces, for a `Field` or `Idiom` node.
	pub fn render_idiom(&self, id: NodeId, out: &mut String) -> R<()> {
		let parts = self.idiom_parts(id)?;
		for (i, part) in parts.iter().enumerate() {
			self.render_part_ref(*part, i == 0, out)?;
		}
		Ok(())
	}

	/// The text of the `i`th part of `self.idiom(id)?`, as `Part::to_sql` writes it.
	pub fn render_part(&self, id: NodeId, i: usize, out: &mut String) -> R<()> {
		let parts = self.idiom_parts(id)?;
		match parts.get(i) {
			Some(part) => self.render_part_ref(*part, false, out),
			None => Err(Reject::new("no such idiom part")),
		}
	}

	/// The text `self.top(id)?.to_sql()` produces for a top-level statement:
	/// the nine top-level-only tags by delegation to `Lower::top`, every other
	/// tag through `render_expr`, with the parentheses that keep a bare
	/// `$p = x` from reading as the deprecated `LET`.
	pub fn render_top(&self, id: NodeId, out: &mut String) -> R<()> {
		match self.n(id).tag {
			Tag::Begin
			| Tag::Cancel
			| Tag::Commit
			| Tag::Use
			| Tag::OptionStmt
			| Tag::Kill
			| Tag::Live
			| Tag::Show
			| Tag::AccessStmt => {
				put(out, &self.top(id)?);
				Ok(())
			}
			_ if self.is_param_equality(id) => {
				out.push('(');
				self.render_expr(id, out)?;
				out.push(')');
				Ok(())
			}
			_ => self.render_expr(id, out),
		}
	}

	/// [`Self::render_expr`] for the constructs that nest.
	#[inline(never)]
	fn render_nesting(&self, n: &Node, out: &mut String) -> R<()> {
		match n.tag {
			Tag::Array => {
				out.push('[');
				self.render_covered_list(self.list(n.a), out)?;
				out.push(']');
			}
			Tag::Object => self.render_object(n, out)?,
			Tag::Prefix => {
				let (text, bp, neg) = prefix_op(n.op);
				out.push_str(text);
				self.render_operand(bp, neg, n.a, out)?;
			}
			Tag::Cast => {
				out.push('<');
				put(out, &self.kind(n.a)?);
				out.push_str("> ");
				self.render_operand(BindingPower::Prefix, false, n.b, out)?;
			}
			Tag::Binary => {
				let op = binary(Binary::from_u8(n.op));
				self.render_binary(&op, n.a, n.b, out)?;
			}
			Tag::Block => self.render_block(self.list(n.a), out)?,
			Tag::Set => self.render_set(self.list(n.a), out)?,
			t => return Err(Reject::new(format!("not a nesting expression: {t:?}"))),
		}
		Ok(())
	}

	/// [`Self::render_expr`] for everything else.
	#[inline(never)]
	fn render_other(&self, id: NodeId, out: &mut String) -> R<()> {
		let n = self.n(id);
		match n.tag {
			Tag::None => out.push_str("NONE"),
			Tag::Null => out.push_str("NULL"),
			Tag::True => out.push_str("true"),
			Tag::False => out.push_str("false"),
			Tag::Int => out.push_str(&Ast::int(n).to_string()),
			Tag::Float => put(out, &Float(Ast::float(n))),
			Tag::NaN => put(out, &Float(f64::NAN)),
			Tag::Infinity => put(out, &Float(f64::INFINITY)),
			Tag::UnboundedRange => out.push_str(".."),
			Tag::Decimal => put(out, &lit::decimal(self.text(id))?),
			Tag::Regex => put(out, &lit::regex_at(self.text(id), n.start, n.end)?),
			Tag::Path => match lit::builtin(&self.path_name(id)?, n.start, n.end)? {
				lit::Builtin::Constant(c) => put(out, &c),
				lit::Builtin::Function(_) => return Err(self.unexpected_after(n.end, "(")),
			},
			Tag::Duration => {
				let (s, ns) = decode_duration(self.text(id).as_bytes())
					.ok_or(Reject::new("duration overflow"))?;
				put(out, &SqlDuration(std::time::Duration::new(s, ns)));
			}
			Tag::Str => self.render_str(id, out)?,
			Tag::Param => {
				out.push('$');
				put(out, &EscapeKwFreeIdent(&self.param_name(id)?));
			}
			Tag::Field | Tag::Table => put(out, &EscapeIdent(self.name(id)?)),
			Tag::RecordId => {
				put(out, &EscapeIdent(self.name(n.a)?));
				out.push(':');
				self.render_key(n.b, out)?;
			}
			Tag::BinaryX => {
				let op = self.binary_extended(n)?;
				let right = self.ast.extra[n.b as usize];
				self.render_binary(&op, n.a, right, out)?;
			}
			Tag::PostfixRange => {
				let s = self.shape(n.a)?;
				self.render_parenthesised(
					s.needs_parentheses() || s.bp() <= BindingPower::Range,
					n.a,
					out,
				)?;
				out.push_str(if n.op == 0 {
					".."
				} else {
					">.."
				});
			}
			Tag::Call => self.render_call(n, out)?,
			Tag::Script => {
				out.push_str("function(");
				self.render_covered_list(self.list(n.b), out)?;
				out.push_str(") {");
				out.push_str(self.text(n.a));
				out.push('}');
			}
			Tag::Point => {
				out.push('(');
				put(out, &Float(self.coordinate(n.a)));
				out.push_str(", ");
				put(out, &Float(self.coordinate(n.b)));
				out.push(')');
			}
			Tag::CallExpr => {
				out.push('(');
				self.render_expr(n.a, out)?;
				out.push_str(")(");
				self.render_covered_list(self.list(n.b), out)?;
				out.push(')');
			}
			Tag::Idiom => self.render_idiom(id, out)?,
			Tag::Select => self.render_select(n, out)?,
			Tag::Create => self.render_create(n, out)?,
			Tag::Update => self.render_update(n, "UPDATE", true, out)?,
			Tag::Upsert => self.render_update(n, "UPSERT", true, out)?,
			Tag::Delete => self.render_update(n, "DELETE", false, out)?,
			Tag::Relate => self.render_relate(n, out)?,
			Tag::Insert => self.render_insert(n, out)?,
			Tag::Return => {
				out.push_str("RETURN ");
				self.render_covered(n.a, out)?;
				if n.b != NONE {
					out.push_str(" FETCH ");
					self.render_list(self.list(n.b), out)?;
				}
			}
			Tag::Let => {
				let rec = slots(self.ast, n.a, 3);
				out.push_str("LET $");
				put(out, &EscapeKwFreeIdent(&self.param_name(rec[0])?));
				if rec[1] != NONE {
					out.push_str(": ");
					put(out, &self.kind(rec[1])?);
				}
				out.push_str(" = ");
				self.render_covered(rec[2], out)?;
			}
			Tag::If => self.render_if(n, out)?,
			Tag::For => {
				let rec = slots(self.ast, n.a, 3);
				if self.n(rec[2]).tag != Tag::Block {
					return Err(Reject::new("not a block"));
				}
				out.push_str("FOR $");
				put(out, &EscapeKwFreeIdent(&self.param_name(rec[0])?));
				out.push_str(" IN ");
				self.render_covered(rec[1], out)?;
				out.push(' ');
				self.render_expr(rec[2], out)?;
			}
			Tag::Explain => {
				out.push_str("EXPLAIN");
				if n.op & 1 != 0 {
					out.push_str(" ANALYZE");
				}
				out.push_str(if n.op & 2 != 0 {
					" FORMAT JSON "
				} else {
					" FORMAT TEXT "
				});
				self.render_expr(n.a, out)?;
			}
			Tag::Alter => put(out, &self.alter(n)?),
			Tag::Remove => put(out, &self.remove(n)?),
			Tag::Define => put(out, &self.define(n)?),
			Tag::Closure => self.render_closure(n, out)?,
			Tag::Mock => self.render_mock(n, out)?,
			Tag::Sleep => {
				let (s, ns) = decode_duration(self.text(n.a).as_bytes())
					.ok_or(Reject::new("duration overflow"))?;
				out.push_str("SLEEP ");
				put(out, &SqlDuration(std::time::Duration::new(s, ns)));
			}
			Tag::Throw => {
				out.push_str("THROW ");
				self.render_covered(n.a, out)?;
			}
			Tag::Break => out.push_str("BREAK"),
			Tag::Continue => out.push_str("CONTINUE"),
			Tag::Info => self.render_info(n, out)?,
			Tag::Rebuild => {
				let rec = slots(self.ast, n.a, 3);
				out.push_str("REBUILD INDEX");
				if rec[2] & 1 != 0 {
					out.push_str(" IF EXISTS");
				}
				out.push(' ');
				put(out, &EscapeKwIdent(&self.name(rec[0])?, &["IF"]));
				out.push_str(" ON ");
				put(out, &EscapeKwFreeIdent(&self.name(rec[1])?));
				if rec[2] & 2 != 0 {
					out.push_str(" CONCURRENTLY");
				}
			}
			t => return Err(Reject::new(format!("not an expression: {t:?}"))),
		}
		Ok(())
	}

	// ----- shapes and predicates --------------------------------------------

	/// The [`Shape`] of the expression at `id`. Reads only the node, its left
	/// spine and an idiom's head.
	fn shape(&self, id: NodeId) -> R<Shape> {
		let n = self.n(id);
		Ok(match n.tag {
			Tag::None => Shape::Lit(LitShape::None),
			Tag::Null => Shape::Lit(LitShape::Null),
			Tag::Int => Shape::Lit(LitShape::Int {
				neg: Ast::int(n) < 0,
			}),
			Tag::Float => Shape::Lit(LitShape::Float {
				neg: Ast::float(n).is_sign_negative(),
			}),
			Tag::NaN | Tag::Infinity => Shape::Lit(LitShape::Float {
				neg: false,
			}),
			Tag::Decimal => Shape::Lit(match lit::decimal(self.text(id))? {
				Literal::Decimal(d) => LitShape::Decimal {
					neg: d.is_sign_negative(),
				},
				_ => LitShape::Other,
			}),
			Tag::Str => Shape::Lit(self.str_shape(id)?),
			Tag::RecordId => Shape::Lit(LitShape::RecordId),
			Tag::UnboundedRange => Shape::Lit(LitShape::UnboundedRange),
			Tag::True
			| Tag::False
			| Tag::Array
			| Tag::Object
			| Tag::Set
			| Tag::Point
			| Tag::Regex
			| Tag::Duration => Shape::Lit(LitShape::Other),
			Tag::Path => Shape::Constant,
			Tag::Call => {
				if self.is_constant_call(n)? {
					Shape::Postfix(PostfixKind::Call)
				} else {
					Shape::FunctionCall
				}
			}
			Tag::Script => Shape::FunctionCall,
			Tag::CallExpr => Shape::Postfix(PostfixKind::Call),
			Tag::PostfixRange => Shape::Postfix(PostfixKind::Range),
			Tag::Cast => Shape::Prefix {
				bp: BindingPower::Prefix,
				neg: false,
			},
			Tag::Prefix => {
				let (_, bp, neg) = prefix_op(n.op);
				Shape::Prefix {
					bp,
					neg,
				}
			}
			Tag::Binary => {
				Shape::Binary(BindingPower::for_binary_operator(&binary(Binary::from_u8(n.op))))
			}
			Tag::BinaryX => Shape::Binary(BindingPower::Relation),
			Tag::Field => Shape::Idiom(FirstPart::Field),
			Tag::Idiom => Shape::Idiom(self.first_part(id)?),
			Tag::Param => Shape::Param,
			Tag::Table => Shape::Table,
			Tag::Mock => Shape::Mock,
			Tag::Block => Shape::Block,
			Tag::Closure => Shape::Closure,
			Tag::Break => Shape::Break,
			Tag::Continue => Shape::Continue,
			Tag::Throw => Shape::Throw,
			Tag::Return => Shape::Return {
				fetch: n.b != NONE,
			},
			Tag::If
			| Tag::Select
			| Tag::Create
			| Tag::Update
			| Tag::Upsert
			| Tag::Delete
			| Tag::Relate
			| Tag::Insert
			| Tag::Define
			| Tag::Remove
			| Tag::Alter
			| Tag::Rebuild
			| Tag::Info
			| Tag::For
			| Tag::Let
			| Tag::Sleep
			| Tag::Explain => Shape::Stmt,
			t => return Err(Reject::new(format!("not an expression: {t:?}"))),
		})
	}

	/// The shape of a string leaf: a record id only when its text is one.
	fn str_shape(&self, id: NodeId) -> R<LitShape> {
		let n = self.n(id);
		Ok(match n.op {
			b'r' => LitShape::RecordId,
			0 | b's' if self.opts.legacy_strands => {
				let mut s = String::new();
				if !decode::unescape(decode::strip_delims(self.text(id)), &mut s) {
					return Err(Reject::new("invalid escape"));
				}
				match lit::legacy_strand(s) {
					Literal::RecordId(_) => LitShape::RecordId,
					_ => LitShape::Other,
				}
			}
			_ => LitShape::Other,
		})
	}

	/// Whether a `Call` node is a constant followed by arguments, which lowers
	/// to a postfix call of the constant's value.
	fn is_constant_call(&self, n: &Node) -> R<bool> {
		let callee = self.n(n.a);
		if callee.op != 0 || !CHECK_NAMES.with(|c| c.get()) {
			return Ok(false);
		}
		let path = self.path_name(n.a)?;
		Ok(matches!(lit::builtin(&path, callee.start, callee.end)?, lit::Builtin::Constant(_)))
	}

	/// The first part of the idiom at `id` after head splicing.
	fn first_part(&self, id: NodeId) -> R<FirstPart> {
		let n = self.n(id);
		let head = self.n(n.a);
		let uncovered = head.depth & ast::COVERED == 0;
		let listed = || match self.list(n.b).first() {
			None => FirstPart::Empty,
			Some(&p) => match self.n(p).tag {
				Tag::PartField => FirstPart::Field,
				Tag::PartGraph => FirstPart::Lookup {
					out: self.n(p).op == 0,
				},
				_ => FirstPart::Other,
			},
		};
		Ok(match head.tag {
			Tag::Field if head.op == 1 => listed(),
			Tag::Doc => FirstPart::Doc,
			Tag::Field | Tag::Table if uncovered => FirstPart::Field,
			Tag::Idiom if uncovered => match self.first_part(n.a)? {
				FirstPart::Empty => listed(),
				first => first,
			},
			_ => FirstPart::Start(n.a),
		})
	}

	/// The left operand of an operator or postfix node, whose own shape
	/// decides the predicates that read the leftmost spot.
	fn left_operand(&self, id: NodeId) -> Option<NodeId> {
		let n = self.n(id);
		match n.tag {
			Tag::Binary | Tag::BinaryX | Tag::PostfixRange | Tag::CallExpr => Some(n.a),
			_ => None,
		}
	}

	/// `Expr::has_left_none_null`.
	fn has_left_none_null(&self, mut id: NodeId) -> R<bool> {
		loop {
			match self.shape(id)? {
				Shape::Lit(LitShape::None | LitShape::Null) => return Ok(true),
				Shape::Binary(_) | Shape::Postfix(_) => match self.left_operand(id) {
					Some(left) => id = left,
					None => return Ok(false),
				},
				Shape::Idiom(FirstPart::Start(head)) => id = head,
				_ => return Ok(false),
			}
		}
	}

	/// `Expr::has_left_minus`.
	fn has_left_minus(&self, mut id: NodeId) -> R<bool> {
		loop {
			match self.shape(id)? {
				Shape::Prefix {
					neg,
					..
				} => return Ok(neg),
				Shape::Lit(
					LitShape::Int {
						neg,
					}
					| LitShape::Float {
						neg,
					}
					| LitShape::Decimal {
						neg,
					},
				) => return Ok(neg),
				Shape::Idiom(FirstPart::Lookup {
					out,
				}) => return Ok(out),
				Shape::Binary(_) | Shape::Postfix(_) => match self.left_operand(id) {
					Some(left) => id = left,
					None => return Ok(false),
				},
				_ => return Ok(false),
			}
		}
	}

	/// `Expr::has_left_idiom`.
	fn has_left_idiom(&self, mut id: NodeId) -> R<bool> {
		loop {
			match self.shape(id)? {
				Shape::Idiom(_) => return Ok(true),
				Shape::Binary(_) | Shape::Postfix(_) => match self.left_operand(id) {
					Some(left) => id = left,
					None => return Ok(false),
				},
				_ => return Ok(false),
			}
		}
	}

	/// Whether the node is `$param = value`, which a statement position must
	/// keep parenthesised.
	fn is_param_equality(&self, id: NodeId) -> bool {
		let n = self.n(id);
		n.tag == Tag::Binary && n.op == Binary::Eq as u8 && self.n(n.a).tag == Tag::Param
	}

	// ----- operands and lists -----------------------------------------------

	/// `CoverStmts`: the expression, in parentheses when it is statement-shaped.
	fn render_covered(&self, id: NodeId, out: &mut String) -> R<()> {
		if self.shape(id)?.covered() {
			out.push('(');
			self.render_expr(id, out)?;
			out.push(')');
			Ok(())
		} else {
			self.render_expr(id, out)
		}
	}

	/// `CoverStmts` over each node, joined by `, `.
	fn render_covered_list(&self, ids: &[NodeId], out: &mut String) -> R<()> {
		for (i, &id) in ids.iter().enumerate() {
			if i > 0 {
				out.push_str(", ");
			}
			self.render_covered(id, out)?;
		}
		Ok(())
	}

	/// Each node as written, joined by `, `.
	fn render_list(&self, ids: &[NodeId], out: &mut String) -> R<()> {
		for (i, &id) in ids.iter().enumerate() {
			if i > 0 {
				out.push_str(", ");
			}
			self.render_expr(id, out)?;
		}
		Ok(())
	}

	/// The operand of a prefix operator of binding power `op_bp`.
	fn render_operand(
		&self,
		op_bp: BindingPower,
		neg: bool,
		id: NodeId,
		out: &mut String,
	) -> R<()> {
		let s = self.shape(id)?;
		let bp = s.bp();
		let paren = s.needs_parentheses()
			|| bp < op_bp
			|| bp == op_bp && op_bp == BindingPower::Range
			|| neg && self.has_left_minus(id)?;
		self.render_parenthesised(paren, id, out)
	}

	fn render_parenthesised(&self, paren: bool, id: NodeId, out: &mut String) -> R<()> {
		if paren {
			out.push('(');
		}
		self.render_expr(id, out)?;
		if paren {
			out.push(')');
		}
		Ok(())
	}

	/// `left op right`, with the parentheses `Expr::fmt_sql` puts around an
	/// operand that would otherwise rebind.
	fn render_binary(
		&self,
		op: &BinaryOperator,
		left: NodeId,
		right: NodeId,
		out: &mut String,
	) -> R<()> {
		let op_bp = BindingPower::for_binary_operator(op);
		let ls = self.shape(left)?;
		let rs = self.shape(right)?;
		let (lbp, rbp) = (ls.bp(), rs.bp());
		let left_paren = ls.needs_parentheses()
			|| lbp < op_bp
			|| lbp == op_bp
				&& matches!(
					lbp,
					BindingPower::Range
						| BindingPower::Relation
						| BindingPower::Equality
						| BindingPower::Power
				);
		self.render_parenthesised(left_paren, left, out)?;
		if matches!(
			op,
			BinaryOperator::Range
				| BinaryOperator::RangeSkip
				| BinaryOperator::RangeInclusive
				| BinaryOperator::RangeSkipInclusive
		) {
			put(out, op);
		} else {
			out.push(' ');
			put(out, op);
			out.push(' ');
		}
		let right_paren = rs.needs_parentheses()
			|| rbp < op_bp
			|| rbp == op_bp
				&& matches!(
					rbp,
					BindingPower::Range
						| BindingPower::Relation
						| BindingPower::Equality
						| BindingPower::AddSub
						| BindingPower::MulDiv
				);
		self.render_parenthesised(right_paren, right, out)
	}

	// ----- literals and values ----------------------------------------------

	fn render_str(&self, id: NodeId, out: &mut String) -> R<()> {
		let n = self.n(id);
		match n.op {
			0 | b's' => {
				let mut s = String::new();
				if !decode::unescape(decode::strip_delims(self.text(id)), &mut s) {
					return Err(Reject::new("invalid escape"));
				}
				if self.opts.legacy_strands {
					put(out, &lit::legacy_strand(s));
				} else {
					put(out, &QuoteStr(&s));
				}
			}
			b'f' => {
				if !self.opts.files {
					return Err(Reject::new(FILES_DISABLED));
				}
				let mut s = String::new();
				if !decode::unescape(decode::strip_delims(self.text(id)), &mut s) {
					return Err(Reject::new("invalid escape"));
				}
				let f = lit::file(&s).map_err(|r| lit::in_token(r, self.text(id), n.start))?;
				put(out, &Literal::File(f.into()));
			}
			_ => put(out, &self.prefixed(id)?),
		}
		Ok(())
	}

	fn render_object(&self, n: &Node, out: &mut String) -> R<()> {
		out.push_str("{ ");
		for (i, kv) in self.list(n.a).chunks(2).enumerate() {
			if i > 0 {
				out.push_str(", ");
			}
			put(out, &EscapeObjectKey(&self.name(kv[0])?));
			out.push_str(": ");
			self.render_covered(kv[1], out)?;
		}
		out.push_str(" }");
		Ok(())
	}

	fn render_set(&self, items: &[NodeId], out: &mut String) -> R<()> {
		out.push('{');
		if items.is_empty() {
			out.push(',');
		}
		for (i, &item) in items.iter().enumerate() {
			if i > 0 {
				out.push_str(", ");
				self.render_covered(item, out)?;
			} else if self.shape(item)? == Shape::Lit(LitShape::RecordId) {
				out.push('(');
				self.render_expr(item, out)?;
				out.push(')');
			} else {
				self.render_covered(item, out)?;
			}
		}
		if items.len() == 1 {
			out.push(',');
		}
		out.push('}');
		Ok(())
	}

	/// A block: `{;}` when empty, otherwise its statements on one line, a
	/// leading record id or a `$p = x` kept parenthesised.
	fn render_block(&self, stmts: &[NodeId], out: &mut String) -> R<()> {
		match stmts.len() {
			0 => out.push_str("{;}"),
			_ => {
				out.push_str("{ ");
				for (i, &s) in stmts.iter().enumerate() {
					if i > 0 {
						out.push(' ');
					}
					let paren = (i == 0 && self.shape(s)? == Shape::Lit(LitShape::RecordId))
						|| self.is_param_equality(s);
					self.render_parenthesised(paren, s, out)?;
					if stmts.len() > 1 {
						out.push(';');
					}
				}
				out.push_str(" }");
			}
		}
		Ok(())
	}

	/// A record-id key at `id`: a scalar key, an array or object, or a range.
	fn render_key(&self, id: NodeId, out: &mut String) -> R<()> {
		let n = self.n(id);
		match n.tag {
			Tag::Array | Tag::Object => self.render_expr(id, out),
			Tag::KeyRange => {
				if n.a != NONE {
					self.render_key(n.a, out)?;
					if n.op & 1 != 0 {
						out.push('>');
					}
				}
				if n.b == NONE {
					out.push_str("..");
				} else {
					out.push_str(if n.op & 2 != 0 {
						"..="
					} else {
						".."
					});
					self.render_key(n.b, out)?;
				}
				Ok(())
			}
			Tag::KeyInt | Tag::KeyStr | Tag::KeyUuid | Tag::KeyGen => {
				put(out, &self.key(id)?);
				Ok(())
			}
			t => Err(Reject::new(format!("not a key: {t:?}"))),
		}
	}

	fn render_call(&self, n: &Node, out: &mut String) -> R<()> {
		let callee = self.n(n.a);
		if callee.op == 0 {
			let path = self.path_name(n.a)?;
			let name = if !CHECK_NAMES.with(|c| c.get()) {
				path.to_lowercase()
			} else {
				match lit::builtin(&path, callee.start, callee.end)? {
					lit::Builtin::Function(f) => f,
					lit::Builtin::Constant(c) => {
						out.push('(');
						put(out, &c);
						out.push_str(")(");
						self.render_covered_list(self.list(n.b), out)?;
						out.push(')');
						return Ok(());
					}
				}
			};
			Self::render_receiver(&Function::Normal(name), out);
		} else {
			Self::render_receiver(&self.function_ref(n.a)?, out);
		}
		out.push('(');
		self.render_covered_list(self.list(n.b), out)?;
		out.push(')');
		Ok(())
	}

	/// The callee of a call, as `FunctionCall::fmt_sql` writes it before the
	/// argument list.
	fn render_receiver(f: &Function, out: &mut String) {
		match f {
			Function::Normal(s) => {
				for (i, seg) in s.split("::").enumerate() {
					if i == 0 {
						put(out, &EscapeIdent(seg));
					} else {
						out.push_str("::");
						out.push_str(seg);
					}
				}
			}
			Function::Custom(s) => {
				out.push_str("fn");
				for seg in s.split("::") {
					out.push_str("::");
					put(out, &EscapeKwFreeIdent(seg));
				}
			}
			Function::Model(m) => put(out, m),
			Function::Module(m, sub, prefix) => {
				out.push_str(prefix.as_str());
				out.push_str("::");
				put(out, &EscapeKwFreeIdent(m));
				if let Some(sub) = sub {
					for seg in sub.split("::") {
						out.push_str("::");
						put(out, &EscapeKwFreeIdent(seg));
					}
				}
			}
			Function::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			} => {
				out.push_str("silo::");
				put(out, &EscapeKwFreeIdent(org));
				out.push_str("::");
				put(out, &EscapeKwFreeIdent(pkg));
				out.push_str(&format!("<{major}.{minor}.{patch}>"));
				if let Some(sub) = sub {
					for seg in sub.split("::") {
						out.push_str("::");
						put(out, &EscapeKwFreeIdent(seg));
					}
				}
			}
			Function::Script(_) => {}
		}
	}

	fn render_closure(&self, n: &Node, out: &mut String) -> R<()> {
		let r = slots(self.ast, n.a, 3);
		out.push('|');
		for (i, pk) in self.list(r[0]).chunks(2).enumerate() {
			if i > 0 {
				out.push_str(", ");
			}
			out.push('$');
			put(out, &EscapeKwFreeIdent(&self.param_name(pk[0])?));
			out.push_str(": ");
			if pk[1] == NONE {
				out.push_str("any");
			} else {
				let kind = self.kind(pk[1])?;
				if matches!(kind, crate::expr::Kind::Either(_)) {
					out.push('<');
					put(out, &kind);
					out.push('>');
				} else {
					put(out, &kind);
				}
			}
		}
		out.push('|');
		if r[1] != NONE {
			out.push_str(" -> ");
			put(out, &self.kind(r[1])?);
		}
		if self.has_left_idiom(r[2])? {
			out.push_str(" (");
			self.render_expr(r[2], out)?;
			out.push(')');
		} else {
			out.push(' ');
			self.render_covered(r[2], out)?;
		}
		Ok(())
	}

	fn render_mock(&self, n: &Node, out: &mut String) -> R<()> {
		let r = slots(self.ast, n.b, 3);
		let int = |id: u32| Ast::int(self.n(id));
		out.push('|');
		put(out, &EscapeKwFreeIdent(&self.name(n.a)?));
		out.push(':');
		if r[2] & 1 == 0 {
			out.push_str(&int(r[0]).to_string());
		} else {
			if r[0] != NONE {
				out.push_str(&int(r[0]).to_string());
				out.push_str(if r[2] & 2 != 0 {
					">.."
				} else {
					".."
				});
			} else {
				out.push_str("..");
			}
			if r[1] != NONE {
				if r[2] & 4 != 0 {
					out.push('=');
				}
				out.push_str(&int(r[1]).to_string());
			}
		}
		out.push('|');
		Ok(())
	}

	// ----- idioms ------------------------------------------------------------

	/// The parts of the idiom at `id`, an uncovered idiom head spliced in as
	/// `Lower::idiom` does.
	fn idiom_parts(&self, id: NodeId) -> R<Vec<PartRef>> {
		let mut parts = Vec::new();
		self.collect_parts(id, &mut parts)?;
		Ok(parts)
	}

	fn collect_parts(&self, id: NodeId, parts: &mut Vec<PartRef>) -> R<()> {
		let n = self.n(id);
		match n.tag {
			Tag::Field => {
				if n.op != 1 {
					parts.push(PartRef::Name(id));
				}
				return Ok(());
			}
			Tag::Idiom => {}
			t => return Err(Reject::new(format!("not an idiom: {t:?}"))),
		}
		let head = self.n(n.a);
		let uncovered = head.depth & ast::COVERED == 0;
		match head.tag {
			Tag::Field if head.op == 1 => {}
			Tag::Doc => parts.push(PartRef::Doc),
			Tag::Field | Tag::Table if uncovered => parts.push(PartRef::Name(n.a)),
			Tag::Idiom if uncovered => self.collect_parts(n.a, parts)?,
			_ => parts.push(PartRef::Start(n.a)),
		}
		parts.extend(self.list(n.b).iter().map(|&p| PartRef::Listed(p)));
		Ok(())
	}

	/// One part. `first` selects the idiom's leading-part form: a field name
	/// without its dot, a start expression covered when it would rebind.
	fn render_part_ref(&self, part: PartRef, first: bool, out: &mut String) -> R<()> {
		match part {
			PartRef::Doc => out.push('@'),
			PartRef::Name(id) => self.render_field_part(id, first, out)?,
			PartRef::Start(id) => {
				if first {
					self.render_start(id, out)?;
				} else {
					self.render_expr(id, out)?;
				}
			}
			PartRef::Listed(id) => self.render_listed_part(id, first, out)?,
		}
		Ok(())
	}

	fn render_field_part(&self, id: NodeId, first: bool, out: &mut String) -> R<()> {
		let name = self.name(id)?;
		if first {
			put(out, &EscapeIdent(&name));
		} else {
			out.push('.');
			put(out, &EscapeKwFreeIdent(&name));
		}
		Ok(())
	}

	/// The expression an idiom starts with, in parentheses when the parts that
	/// follow would otherwise extend it.
	fn render_start(&self, id: NodeId, out: &mut String) -> R<()> {
		let s = self.shape(id)?;
		let paren = s.needs_parentheses()
			|| matches!(
				s,
				Shape::Binary(_) | Shape::Prefix { .. } | Shape::Postfix(_) | Shape::Idiom(_)
			) || matches!(
			s,
			Shape::Lit(
				LitShape::Decimal {
					neg: true
				} | LitShape::Int {
					neg: true
				} | LitShape::Float {
					neg: true
				}
			)
		);
		self.render_parenthesised(paren, id, out)
	}

	fn render_listed_part(&self, id: NodeId, first: bool, out: &mut String) -> R<()> {
		let pn = self.n(id);
		match pn.tag {
			Tag::PartField => self.render_field_part(id, first, out)?,
			Tag::PartAll => out.push_str(".*"),
			Tag::PartLast => out.push_str("[$]"),
			Tag::PartFlatten => out.push('…'),
			Tag::PartOptional => out.push_str(".?"),
			Tag::PartIndex => {
				out.push('[');
				self.render_expr(pn.a, out)?;
				out.push(']');
			}
			Tag::PartWhere => {
				out.push_str("[WHERE ");
				self.render_expr(pn.a, out)?;
				out.push(']');
			}
			Tag::PartMethod => {
				out.push('.');
				put(out, &EscapeKwFreeIdent(&self.name(pn.a)?));
				out.push('(');
				self.render_covered_list(self.list(pn.b), out)?;
				out.push(')');
			}
			Tag::PartGraph => self.render_lookup(pn, out)?,
			Tag::PartRepeat => out.push_str(".@"),
			Tag::PartRecurse => self.render_recurse(pn, out)?,
			Tag::PartDestructure => self.render_destructure(pn, out)?,
			t => return Err(Reject::new(format!("not a part: {t:?}"))),
		}
		Ok(())
	}

	fn render_recurse(&self, pn: &Node, out: &mut String) -> R<()> {
		let r = slots(self.ast, pn.a, 7);
		let num = |id: u32| -> R<Option<u32>> {
			if id == NONE {
				return Ok(None);
			}
			u32::try_from(Ast::int(self.n(id))).map(Some).map_err(|_| Reject::new("u32"))
		};
		out.push_str(".{");
		let (min, max) = (num(r[1])?, num(r[2])?);
		if r[0] == 0 {
			out.push_str(&min.ok_or(Reject::new("fixed recursion has a count"))?.to_string());
		} else {
			if let Some(min) = min {
				out.push_str(&min.to_string());
			}
			out.push_str("..");
			if let Some(max) = max {
				out.push_str(&max.to_string());
			}
		}
		let inclusive = r[5] == 1;
		match r[3] {
			0 => {}
			1 => out.push_str("+path"),
			2 => out.push_str("+collect"),
			_ => {
				out.push_str("+shortest=");
				self.render_expr(r[4], out)?;
			}
		}
		if r[3] != 0 && inclusive {
			out.push_str("+inclusive");
		}
		out.push('}');
		if r[6] != NONE {
			out.push('(');
			for part in self.idiom_parts(r[6])? {
				self.render_part_ref(part, false, out)?;
			}
			out.push(')');
		}
		Ok(())
	}

	/// `.{ a, b: c, d.*, e.{ … }, ... }`.
	fn render_destructure(&self, n: &Node, out: &mut String) -> R<()> {
		out.push_str(".{ ");
		for (i, &e) in self.list(n.a).iter().enumerate() {
			if i > 0 {
				out.push_str(", ");
			}
			let en = self.n(e);
			if en.op == 4 {
				out.push_str("...");
				continue;
			}
			put(out, &EscapeKwFreeIdent(&self.name(en.a)?));
			match en.op {
				0 => {}
				1 => {
					out.push_str(": ");
					// An alias that lowers to an idiom is that idiom; any other
					// expression is the start of a one-part idiom.
					match self.n(en.b).tag {
						Tag::Idiom => self.render_idiom(en.b, out)?,
						Tag::Field => put(out, &EscapeIdent(self.name(en.b)?)),
						_ => self.render_start(en.b, out)?,
					}
				}
				2 => out.push_str(".*"),
				_ => self.render_destructure(self.n(en.b), out)?,
			}
		}
		out.push_str(" }");
		Ok(())
	}

	// ----- lookups -----------------------------------------------------------

	/// A graph or reference lookup part: the short form when it names at most
	/// one plain subject and carries no projection, condition or alias, the
	/// parenthesised `SELECT`-shaped form otherwise.
	fn render_lookup(&self, pn: &Node, out: &mut String) -> R<()> {
		use ast::select as s;
		let rec = slots(self.ast, pn.a, s::LEN);
		let flags = rec[s::FLAGS];
		let has_expr = flags & 128 != 0;
		let subjects = self.list(rec[s::WHAT]);
		out.push_str(match pn.op {
			0 => "->",
			1 => "<-",
			2 => "<->",
			_ => "<~",
		});
		let plain = |&sj: &NodeId| {
			let x = slots(self.ast, self.n(sj).b, 2);
			x[1] == NONE && (x[0] == NONE || self.n(x[0]).b != NONE)
		};
		if subjects.len() <= 1
			&& subjects.iter().all(plain)
			&& rec[s::COND] == NONE
			&& rec[s::FETCH] == NONE
			&& !has_expr
		{
			match subjects.first() {
				None => out.push('?'),
				Some(&sj) => self.render_subject(sj, out)?,
			}
			return Ok(());
		}
		out.push('(');
		if has_expr {
			out.push_str("SELECT ");
			self.render_projection(rec, out)?;
			out.push_str(" FROM ");
			if flags & 1 != 0 {
				out.push_str("ONLY ");
			}
		}
		if subjects.is_empty() {
			out.push('?');
		}
		for (i, &sj) in subjects.iter().enumerate() {
			if i > 0 {
				out.push_str(", ");
			}
			self.render_subject(sj, out)?;
		}
		if rec[s::COND] != NONE {
			out.push_str(" WHERE ");
			self.render_covered(rec[s::COND], out)?;
		}
		if has_expr {
			self.render_split_group(rec, out)?;
		}
		self.render_order_limit_start(rec, out)?;
		if rec[s::FETCH] != NONE {
			out.push_str(" AS ");
			self.render_idiom(rec[s::FETCH], out)?;
		}
		out.push(')');
		Ok(())
	}

	/// A lookup subject: a table or `table:range`, with its referencing field.
	fn render_subject(&self, id: NodeId, out: &mut String) -> R<()> {
		let sn = self.n(id);
		let x = slots(self.ast, sn.b, 2);
		put(out, &EscapeKwFreeIdent(&self.name(sn.a)?));
		if x[0] != NONE {
			if self.n(x[0]).tag != Tag::KeyRange {
				return Err(Reject::new("not a range"));
			}
			out.push(':');
			self.render_key(x[0], out)?;
		}
		if x[1] != NONE {
			out.push_str(" FIELD ");
			put(out, &EscapeKwFreeIdent(&self.name(x[1])?));
		}
		Ok(())
	}

	// ----- statements --------------------------------------------------------

	/// The projection of a select-shaped record: `VALUE selector` or the field list.
	fn render_projection(&self, rec: &[u32], out: &mut String) -> R<()> {
		use ast::select as s;
		if rec[s::VALUE] != NONE {
			out.push_str("VALUE ");
			return self.render_selector(rec[s::VALUE], out);
		}
		for (i, &f) in self.list(rec[s::FIELDS]).iter().enumerate() {
			if i > 0 {
				out.push_str(", ");
			}
			self.render_field(f, out)?;
		}
		Ok(())
	}

	fn render_field(&self, id: NodeId, out: &mut String) -> R<()> {
		if self.n(id).tag == Tag::FieldAll {
			out.push('*');
			Ok(())
		} else {
			self.render_selector(id, out)
		}
	}

	fn render_selector(&self, id: NodeId, out: &mut String) -> R<()> {
		let n = self.n(id);
		self.render_covered(n.a, out)?;
		if n.b != NONE {
			out.push_str(" AS ");
			self.render_idiom(n.b, out)?;
		}
		Ok(())
	}

	fn render_idiom_list(&self, ids: &[NodeId], out: &mut String) -> R<()> {
		for (i, &id) in ids.iter().enumerate() {
			if i > 0 {
				out.push_str(", ");
			}
			self.render_idiom(id, out)?;
		}
		Ok(())
	}

	/// ` SPLIT ON …` and ` GROUP BY …` or ` GROUP ALL`.
	fn render_split_group(&self, rec: &[u32], out: &mut String) -> R<()> {
		use ast::select as s;
		if rec[s::SPLIT] != NONE {
			out.push_str(" SPLIT ON ");
			self.render_idiom_list(self.list(rec[s::SPLIT]), out)?;
		}
		if rec[s::FLAGS] & 2 != 0 {
			out.push_str(" GROUP ALL");
		} else if rec[s::GROUP] != NONE {
			out.push_str(" GROUP BY ");
			self.render_idiom_list(self.list(rec[s::GROUP]), out)?;
		}
		Ok(())
	}

	/// ` ORDER BY …`, ` LIMIT …` and ` START …`.
	fn render_order_limit_start(&self, rec: &[u32], out: &mut String) -> R<()> {
		use ast::select as s;
		if rec[s::ORDER] != NONE {
			let items = self.list(rec[s::ORDER]);
			if items.len() == 1 && self.n(items[0]).tag == Tag::OrderRand {
				out.push_str(" ORDER BY RAND()");
			} else {
				out.push_str(" ORDER BY ");
				for (i, &o) in items.iter().enumerate() {
					if i > 0 {
						out.push_str(", ");
					}
					let on = self.n(o);
					self.render_idiom(on.a, out)?;
					if on.op & 2 != 0 {
						out.push_str(" COLLATE");
					}
					if on.op & 4 != 0 {
						out.push_str(" NUMERIC");
					}
					if on.op & 1 != 0 {
						out.push_str(" DESC");
					}
				}
			}
		}
		if rec[s::LIMIT] != NONE {
			out.push_str(" LIMIT ");
			self.render_covered(rec[s::LIMIT], out)?;
		}
		if rec[s::START] != NONE {
			out.push_str(" START ");
			self.render_covered(rec[s::START], out)?;
		}
		Ok(())
	}

	/// ` WITH …` for the `With` node at `id`, if any.
	fn render_with(&self, id: NodeId, out: &mut String) -> R<()> {
		if let Some(with) = self.with(id)? {
			out.push(' ');
			put(out, &with);
		}
		Ok(())
	}

	/// ` TIMEOUT …` or ` VERSION …`, omitted when absent or written as `NONE`.
	fn render_opt_clause(&self, keyword: &str, id: NodeId, out: &mut String) -> R<()> {
		if id != NONE && self.n(id).tag != Tag::None {
			out.push(' ');
			out.push_str(keyword);
			out.push(' ');
			self.render_covered(id, out)?;
		}
		Ok(())
	}

	fn render_select(&self, n: &Node, out: &mut String) -> R<()> {
		use ast::select as s;
		let rec = slots(self.ast, n.a, s::LEN);
		let flags = rec[s::FLAGS];
		out.push_str("SELECT ");
		self.render_projection(rec, out)?;
		let omit = self.list(rec[s::OMIT]);
		if !omit.is_empty() {
			out.push_str(" OMIT ");
			self.render_covered_list(omit, out)?;
		}
		out.push_str(" FROM");
		if flags & 1 != 0 {
			out.push_str(" ONLY");
		}
		out.push(' ');
		self.render_covered_list(self.list(rec[s::WHAT]), out)?;
		self.render_with(rec[s::WITH], out)?;
		if rec[s::COND] != NONE {
			out.push_str(" WHERE ");
			self.render_covered(rec[s::COND], out)?;
		}
		self.render_split_group(rec, out)?;
		self.render_order_limit_start(rec, out)?;
		if rec[s::FETCH] != NONE {
			out.push_str(" FETCH ");
			self.render_list(self.list(rec[s::FETCH]), out)?;
		}
		self.render_opt_clause("VERSION", rec[s::VERSION], out)?;
		self.render_opt_clause("TIMEOUT", rec[s::TIMEOUT], out)?;
		if flags & 64 != 0 {
			out.push_str(" FOR UPDATE");
		}
		if flags & 8 != 0 {
			out.push_str(" TEMPFILES");
		}
		self.render_explain(flags, out);
		Ok(())
	}

	fn render_explain(&self, flags: u32, out: &mut String) {
		if flags & 16 != 0 {
			out.push_str(" EXPLAIN");
			if flags & 32 != 0 {
				out.push_str(" FULL");
			}
		}
	}

	/// ` SET …`, ` UNSET …`, ` CONTENT …` and the like for a write record.
	fn render_data(&self, rec: &[u32], out: &mut String) -> R<()> {
		use ast::write as w;
		let payload = rec[w::DATA];
		let word = match rec[w::DATA_KIND] {
			0 => return Ok(()),
			1 => {
				out.push_str(" SET ");
				return self.render_assigns(payload, out);
			}
			2 => {
				out.push_str(" UNSET ");
				return self.render_idiom_list(self.list(payload), out);
			}
			3 => " CONTENT ",
			4 => " MERGE ",
			5 => " PATCH ",
			_ => " REPLACE ",
		};
		out.push_str(word);
		self.render_expr(payload, out)
	}

	/// `place op value`, joined by `, `.
	fn render_assigns(&self, list: u32, out: &mut String) -> R<()> {
		for (i, &a) in self.list(list).iter().enumerate() {
			if i > 0 {
				out.push_str(", ");
			}
			let an = self.n(a);
			self.render_idiom(an.a, out)?;
			out.push_str(match an.op {
				0 => " = ",
				1 => " += ",
				2 => " -= ",
				_ => " +?= ",
			});
			self.render_covered(an.b, out)?;
		}
		Ok(())
	}

	/// ` RETURN …` for a write record's output clause.
	fn render_output(&self, rec: &[u32], out: &mut String) -> R<()> {
		use ast::write as w;
		let word = match rec[w::OUTPUT_KIND] {
			0 => return Ok(()),
			1 => " RETURN NONE",
			2 => " RETURN NULL",
			3 => " RETURN DIFF",
			4 => " RETURN AFTER",
			5 => " RETURN BEFORE",
			7 => {
				out.push_str(" RETURN VALUE ");
				return self.render_selector(rec[w::OUTPUT], out);
			}
			_ => {
				out.push_str(" RETURN ");
				let fields = self.list(rec[w::OUTPUT]);
				for (i, &f) in fields.iter().enumerate() {
					if i > 0 {
						out.push_str(", ");
						self.render_field(f, out)?;
						continue;
					}
					let fnode = self.n(f);
					if fnode.tag == Tag::FieldAll {
						out.push('*');
						continue;
					}
					// A leading `NONE` or `NULL` would read as the clause.
					if self.has_left_none_null(fnode.a)? {
						out.push('(');
						self.render_expr(fnode.a, out)?;
						out.push(')');
					} else {
						self.render_covered(fnode.a, out)?;
					}
					if fnode.b != NONE {
						out.push_str(" AS ");
						self.render_idiom(fnode.b, out)?;
					}
				}
				return Ok(());
			}
		};
		out.push_str(word);
		Ok(())
	}

	fn render_create(&self, n: &Node, out: &mut String) -> R<()> {
		use ast::write as w;
		let rec = slots(self.ast, n.a, w::LEN);
		out.push_str("CREATE");
		if rec[w::FLAGS] & 1 != 0 {
			out.push_str(" ONLY");
		}
		out.push(' ');
		self.render_covered_list(self.list(rec[w::WHAT]), out)?;
		self.render_data(rec, out)?;
		self.render_output(rec, out)?;
		self.render_opt_clause("TIMEOUT", rec[w::TIMEOUT], out)
	}

	/// `UPDATE`, `UPSERT` or `DELETE`; only the first two carry data.
	fn render_update(&self, n: &Node, keyword: &str, with_data: bool, out: &mut String) -> R<()> {
		use ast::write as w;
		let rec = slots(self.ast, n.a, w::LEN);
		out.push_str(keyword);
		if rec[w::FLAGS] & 1 != 0 {
			out.push_str(" ONLY");
		}
		out.push(' ');
		self.render_covered_list(self.list(rec[w::WHAT]), out)?;
		self.render_with(rec[w::WITH], out)?;
		if with_data {
			self.render_data(rec, out)?;
		}
		if rec[w::COND] != NONE {
			out.push_str(" WHERE ");
			self.render_covered(rec[w::COND], out)?;
		}
		self.render_output(rec, out)?;
		self.render_opt_clause("TIMEOUT", rec[w::TIMEOUT], out)?;
		self.render_explain(rec[w::FLAGS], out);
		Ok(())
	}

	/// Whether a `RELATE` endpoint is written without parentheses: an array, a
	/// parameter, or a record id that is not a range.
	fn is_bare_endpoint(&self, id: NodeId) -> R<bool> {
		let n = self.n(id);
		Ok(match n.tag {
			Tag::Array | Tag::Param => true,
			Tag::RecordId => self.n(n.b).tag != Tag::KeyRange,
			Tag::Str => matches!(
				self.expr(id)?,
				crate::expr::Expr::Literal(Literal::RecordId(r))
					if !matches!(r.key, crate::expr::RecordIdKeyLit::Range(_))
			),
			_ => false,
		})
	}

	fn render_relate(&self, n: &Node, out: &mut String) -> R<()> {
		use ast::{relate as r, write as w};
		let rec = slots(self.ast, n.a, r::LEN);
		out.push_str("RELATE");
		if rec[w::FLAGS] & 1 != 0 {
			out.push_str(" ONLY");
		}
		if rec[w::FLAGS] & 4 != 0 {
			out.push_str(" OR UPDATE");
		}
		out.push(' ');
		let from = rec[r::FROM];
		self.render_parenthesised(!self.is_bare_endpoint(from)?, from, out)?;
		out.push_str(" -> ");
		let through = rec[r::THROUGH];
		let bare = matches!(self.n(through).tag, Tag::Param | Tag::Table);
		self.render_parenthesised(!bare, through, out)?;
		out.push_str(" -> ");
		let to = rec[r::TO];
		self.render_parenthesised(!self.is_bare_endpoint(to)?, to, out)?;
		self.render_data(rec, out)?;
		self.render_output(rec, out)?;
		self.render_opt_clause("TIMEOUT", rec[w::TIMEOUT], out)
	}

	fn render_insert(&self, n: &Node, out: &mut String) -> R<()> {
		use ast::{insert as i, write as w};
		let rec = slots(self.ast, n.a, i::LEN);
		out.push_str("INSERT");
		if rec[w::FLAGS] & 2 != 0 {
			out.push_str(" RELATION");
		}
		if rec[w::FLAGS] & 4 != 0 {
			out.push_str(" IGNORE");
		}
		if rec[i::INTO] != NONE {
			out.push_str(" INTO ");
			self.render_covered(rec[i::INTO], out)?;
		}
		out.push(' ');
		if rec[w::DATA_KIND] == 1 {
			let v = slots(self.ast, rec[w::DATA], 2);
			let rows = self.list(v[1]);
			out.push('(');
			if !rows.is_empty() {
				self.render_idiom_list(self.list(v[0]), out)?;
			}
			out.push_str(") VALUES ");
			for (i, &row) in rows.iter().enumerate() {
				if i > 0 {
					out.push_str(", ");
				}
				out.push('(');
				self.render_covered_list(self.list(row), out)?;
				out.push(')');
			}
		} else {
			self.render_covered(rec[w::DATA], out)?;
		}
		if rec[i::UPDATE] != NONE {
			out.push_str(" ON DUPLICATE KEY UPDATE ");
			self.render_assigns(rec[i::UPDATE], out)?;
		}
		self.render_output(rec, out)?;
		self.render_opt_clause("TIMEOUT", rec[w::TIMEOUT], out)
	}

	/// An `IF` whose branches are all blocks is written with braces, any
	/// other in the `THEN … END` form.
	fn if_is_bracketed(&self, n: &Node) -> bool {
		self.list(n.a).chunks(2).all(|cb| cb.get(1).is_some_and(|&b| self.n(b).tag == Tag::Block))
			&& (n.b == NONE || self.n(n.b).tag == Tag::Block)
	}

	fn render_if(&self, n: &Node, out: &mut String) -> R<()> {
		let branches = self.list(n.a);
		if self.if_is_bracketed(n) {
			for (i, cb) in branches.chunks(2).enumerate() {
				if i > 0 {
					out.push_str(" ELSE ");
				}
				out.push_str("IF ");
				self.render_covered(cb[0], out)?;
				out.push(' ');
				self.render_if_block(cb[1], out)?;
			}
			if n.b != NONE {
				out.push_str(" ELSE ");
				self.render_if_block(n.b, out)?;
			}
			return Ok(());
		}
		for (i, cb) in branches.chunks(2).enumerate() {
			if i > 0 {
				out.push_str(" ELSE ");
			}
			out.push_str("IF ");
			self.render_covered(cb[0], out)?;
			out.push_str(" THEN ");
			let then = self.n(cb[1]);
			let nested = then.tag == Tag::If && self.if_is_bracketed(then);
			self.render_parenthesised(nested, cb[1], out)?;
		}
		if n.b != NONE {
			out.push_str(" ELSE ");
			self.render_covered(n.b, out)?;
		}
		out.push_str(" END");
		Ok(())
	}

	/// A branch block of a braced `IF`: statements are separated by newlines
	/// and indented when every statement is a plain value.
	fn render_if_block(&self, id: NodeId, out: &mut String) -> R<()> {
		let stmts = self.list(self.n(id).a);
		match stmts.len() {
			0 => out.push_str("{;}"),
			1 => {
				out.push_str("{ ");
				self.render_expr(stmts[0], out)?;
				out.push_str(" }");
			}
			_ => {
				let mut simple = true;
				for &s in stmts {
					simple &=
						matches!(self.shape(s)?, Shape::Lit(_) | Shape::Param | Shape::Idiom(_));
				}
				out.push_str("{\n");
				for (i, &s) in stmts.iter().enumerate() {
					if i > 0 {
						out.push('\n');
					}
					if simple {
						out.push('\t');
					}
					self.render_expr(s, out)?;
					out.push(';');
				}
				out.push_str("\n}");
			}
		}
		Ok(())
	}

	fn render_info(&self, n: &Node, out: &mut String) -> R<()> {
		use ast::{InfoTarget as I, info};
		let rec = slots(self.ast, n.a, info::LEN);
		let structure = rec[info::FLAGS] & 1 != 0;
		let version = |out: &mut String| -> R<()> {
			if rec[info::VERSION] != NONE {
				out.push_str(" VERSION ");
				self.render_covered(rec[info::VERSION], out)?;
			}
			Ok(())
		};
		match n.op {
			x if x == I::Root as u8 => out.push_str("INFO FOR ROOT"),
			x if x == I::Ns as u8 => out.push_str("INFO FOR NAMESPACE"),
			x if x == I::Db as u8 => out.push_str("INFO FOR DATABASE"),
			x if x == I::Table as u8 => {
				out.push_str("INFO FOR TABLE ");
				self.render_covered(rec[info::WHAT], out)?;
			}
			x if x == I::User as u8 => {
				out.push_str("INFO FOR USER ");
				self.render_covered(rec[info::WHAT], out)?;
				let base = rec[info::FLAGS] >> 1;
				if base != 0 {
					out.push_str(" ON ");
					put(out, &Self::base(base));
				}
			}
			_ => {
				out.push_str("INFO FOR INDEX ");
				self.render_covered(rec[info::WHAT], out)?;
				out.push_str(" ON ");
				self.render_covered(rec[info::TABLE], out)?;
			}
		}
		if n.op <= I::Table as u8 {
			version(out)?;
		}
		if structure {
			out.push_str(" STRUCTURE");
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_parser_next::{Limits, Scratch, parse_script};

	use super::*;
	use crate::parser::Options;

	/// The tags `Lower::expr` handles.
	const EXPR_TAGS: &[Tag] = &[
		Tag::Array,
		Tag::Object,
		Tag::Prefix,
		Tag::Cast,
		Tag::Binary,
		Tag::Block,
		Tag::Set,
		Tag::None,
		Tag::Null,
		Tag::True,
		Tag::False,
		Tag::Int,
		Tag::Float,
		Tag::NaN,
		Tag::Infinity,
		Tag::UnboundedRange,
		Tag::Decimal,
		Tag::Regex,
		Tag::Path,
		Tag::Duration,
		Tag::Str,
		Tag::Param,
		Tag::Field,
		Tag::Table,
		Tag::RecordId,
		Tag::BinaryX,
		Tag::PostfixRange,
		Tag::Call,
		Tag::Script,
		Tag::Point,
		Tag::CallExpr,
		Tag::Idiom,
		Tag::Select,
		Tag::Create,
		Tag::Update,
		Tag::Upsert,
		Tag::Delete,
		Tag::Return,
		Tag::Let,
		Tag::If,
		Tag::For,
		Tag::Relate,
		Tag::Insert,
		Tag::Explain,
		Tag::Alter,
		Tag::Remove,
		Tag::Define,
		Tag::Closure,
		Tag::Mock,
		Tag::Sleep,
		Tag::Throw,
		Tag::Break,
		Tag::Continue,
		Tag::Info,
		Tag::Rebuild,
	];

	/// Replaces each quoted run of 128 alphanumeric characters, a generated key.
	fn mask(sql: &str) -> String {
		let mut out = String::new();
		let mut rest = sql;
		while let Some(i) = rest.find(['\'', '"']) {
			let quote = rest.as_bytes()[i];
			let after = &rest[i + 1..];
			let key = after.len() > 128
				&& after.as_bytes()[128] == quote
				&& after.as_bytes()[..128].iter().all(|b| b.is_ascii_alphanumeric());
			out.push_str(&rest[..=i]);
			if key {
				out.push_str("[KEY]");
				out.push(quote as char);
				rest = &after[129..];
			} else {
				rest = after;
			}
		}
		out.push_str(rest);
		out
	}

	fn rendered(f: impl FnOnce(&mut String) -> R<()>) -> Result<String, String> {
		let mut out = String::new();
		f(&mut out).map(|()| out).map_err(|e| e.msg.to_string())
	}

	/// Renders every node of `src` and compares it with the lowered text.
	fn check(src: &str) -> Result<(), String> {
		let mut ast = Ast::new();
		let mut scratch = Scratch::default();
		parse_script(src, Limits::default(), &mut ast, &mut scratch);
		if !scratch.diags.is_empty() || ast.stmts.is_empty() {
			return Err(format!("fixture does not parse: {src}"));
		}
		let sets = [
			Options::ALL,
			Options {
				legacy_strands: true,
				..Options::ALL
			},
			Options {
				files: false,
				surrealism: false,
				legacy_strands: false,
			},
		];
		// The `SELECT` of a table view is read by the definition's own lowering.
		let views: Vec<NodeId> = ast
			.nodes
			.iter()
			.filter(|n| n.tag == Tag::Clause && n.op == ast::Cl::View as u8)
			.map(|n| n.a)
			.collect();
		for (set, opts) in sets.into_iter().enumerate() {
			let lower = Lower {
				ast: &ast,
				src,
				opts,
			};
			let mut compared = 0;
			let mut failures = Vec::new();
			let mut same = |what: &str, id: NodeId, want: String, got: Result<String, String>| {
				compared += 1;
				match got {
					Ok(got) if mask(&got) == mask(&want) => {}
					got => failures.push(format!(
						"{what} differs under option set {set}\n  fixture: {src}\n  node:    {:?} {}\n  want:    {want}\n  got:     {got:?}",
						ast.node(id).tag,
						lower.text(id)
					)),
				}
			};
			for &s in &ast.stmts {
				if let Ok(top) = lower.top(s) {
					same("statement", s, top.to_sql(), rendered(|o| lower.render_top(s, o)));
				}
			}
			for id in 0..ast.nodes.len() as NodeId {
				let tag = ast.node(id).tag;
				if matches!(tag, Tag::Field | Tag::Idiom)
					&& let Ok(idiom) = lower.idiom(id)
				{
					same("idiom", id, idiom.to_sql(), rendered(|o| lower.render_idiom(id, o)));
					for (i, part) in idiom.0.iter().enumerate() {
						same("part", id, part.to_sql(), rendered(|o| lower.render_part(id, i, o)));
					}
				}
				if EXPR_TAGS.contains(&tag)
					&& !views.contains(&id)
					&& let Ok(expr) = lower.expr(id)
				{
					same("expression", id, expr.to_sql(), rendered(|o| lower.render_expr(id, o)));
				}
			}
			if set == 0 && compared == 0 {
				return Err(format!("fixture compared nothing: {src}"));
			}
			if let Some(first) = failures.into_iter().next() {
				return Err(first);
			}
		}
		Ok(())
	}

	#[test]
	fn fixtures_render_as_lowered() {
		let failures: Vec<String> = FIXTURES.iter().filter_map(|src| check(src).err()).collect();
		assert!(
			failures.is_empty(),
			"{} fixtures failed:\n{}",
			failures.len(),
			failures.join("\n")
		);
	}

	/// Every tag `Lower::expr` handles and every top-level-only statement has
	/// at least three fixture nodes that lower.
	#[test]
	fn fixtures_cover_every_tag() {
		const TOP_ONLY: &[Tag] = &[
			Tag::Begin,
			Tag::Cancel,
			Tag::Commit,
			Tag::Use,
			Tag::OptionStmt,
			Tag::Kill,
			Tag::Live,
			Tag::Show,
			Tag::AccessStmt,
		];
		let mut counts = std::collections::BTreeMap::<String, usize>::new();
		let mut ast = Ast::new();
		let mut scratch = Scratch::default();
		for src in FIXTURES {
			parse_script(src, Limits::default(), &mut ast, &mut scratch);
			let lower = Lower::new(&ast, src);
			for &s in &ast.stmts {
				if TOP_ONLY.contains(&ast.node(s).tag) && lower.top(s).is_ok() {
					*counts.entry(format!("{:?}", ast.node(s).tag)).or_default() += 1;
				}
			}
			let views: Vec<NodeId> = ast
				.nodes
				.iter()
				.filter(|n| n.tag == Tag::Clause && n.op == ast::Cl::View as u8)
				.map(|n| n.a)
				.collect();
			for id in 0..ast.nodes.len() as NodeId {
				let tag = ast.node(id).tag;
				if EXPR_TAGS.contains(&tag) && !views.contains(&id) && lower.expr(id).is_ok() {
					*counts.entry(format!("{tag:?}")).or_default() += 1;
				}
			}
		}
		let thin: Vec<String> = EXPR_TAGS
			.iter()
			.chain(TOP_ONLY)
			.map(|tag| (tag, counts.get(&format!("{tag:?}")).copied().unwrap_or(0)))
			.filter(|&(_, n)| n < 3)
			.map(|(tag, n)| format!("{tag:?} has {n}"))
			.collect();
		assert!(thin.is_empty(), "fewer than 3 fixture nodes: {}", thin.join(", "));
	}

	const FIXTURES: &[&str] = &[
		// Leaves.
		"NONE",
		"NULL",
		"true",
		"false",
		"[false, true, false]",
		"!false",
		"1",
		"-1",
		"- 1",
		"1.5f",
		"-1.5f",
		"1.5",
		"-0.0f",
		"NaN",
		"[NaN, NaN]",
		"Infinity",
		"-Infinity",
		"..",
		"1.5dec",
		"-2.50dec",
		"1e3dec",
		"-2e-3dec",
		"1_000dec",
		"9223372036854775807",
		"-9223372036854775808",
		r#"/a\/b/"#,
		"math::pi",
		"math::PI",
		"time::epoch",
		"90s",
		"1y2w3d",
		"1h30m",
		"1ms",
		"1ns",
		r#"'a'"#,
		r#""a""#,
		r#"'it\'s'"#,
		r#""quo\"te""#,
		r#"'a"b'"#,
		r#"'tab\t nl\n nul\0 bs\b'"#,
		"s'x'",
		"d'2020-01-01T00:00:00Z'",
		r#"d"2020-01-01T01:00:00+01:00""#,
		"d'2020-01-01T00:00:00.123456789Z'",
		"u'018a6180-0000-7000-8000-000000000000'",
		"b'ff00ab'",
		"b''",
		"r'person:tobie'",
		"r'person:[1, 2]'",
		"r'person:1..5'",
		"f'bucket:/some/key'",
		"$a",
		"$select",
		"$`a b`",
		"a",
		"`select`",
		"`value`",
		"`a b`",
		"⟨a b⟩",
		r#"`a\`b`"#,
		"(1, 2)",
		"(1.5, -2.5)",
		"(NaN, Infinity)",
		"(1, -0.0)",
		// String, key and identifier escaping.
		r#"{ 'a"b': 1 }"#,
		r#"{ "a'b": 1 }"#,
		"{ a: 1, b: 2 }",
		r#"{ 'a\nb': 1, '': 2, '0a': 3, 'NaN': 4, 'Infinity': 5, `select`: 6 }"#,
		"/^[a-z]+$/",
		"[/a/, /b\\/c/]",
		"{ `x y`: 1 }",
		r#"a.`select`"#,
		r#"a.`b c`"#,
		r#"`value`:1"#,
		r#"`a b`:`c d`"#,
		r#"person:`1a`"#,
		r#"person:⟨a b⟩"#,
		r#"$`\``"#,
		// Record ids and keys.
		"person:1",
		"person:tobie",
		"person:u'018a6180-0000-7000-8000-000000000000'",
		"person:rand()",
		"person:ulid()",
		"person:uuid()",
		"person:[1, 2]",
		"person:[]",
		"person:{ a: 1 }",
		"person:{}",
		"person:[1, [2, (SELECT * FROM x)]]",
		"person:1..5",
		"person:1>..5",
		"person:1..=5",
		"person:1>..=5",
		"person:..5",
		"person:..=5",
		"person:1..",
		"person:1>..",
		"person:..",
		"person:[1]..[2]",
		"person:{ a: 1 }..{ a: 2 }",
		"person:-9223372036854775809",
		"person:123abc",
		"person:1_0",
		// Arrays, objects and sets.
		"[]",
		"[1, 2, 3]",
		"[(SELECT * FROM x), (RETURN 1 FETCH a), (IF a { b })]",
		"[[1], { a: [2] }]",
		"{ a: (SELECT * FROM x), b: (RETURN 1 FETCH a), c: (LET $a = 1) }",
		"{,}",
		"{1,}",
		"{1, 2}",
		"{a:1,}",
		"{(a:1), b:2}",
		"{ (SELECT * FROM x), 2 }",
		"{ 1, (SELECT * FROM y) }",
		"{{ a: 1 },}",
		// Prefix and cast.
		"!a",
		"-a",
		"+a",
		"..a",
		"..=a",
		"!!a",
		"- -a",
		"-(-a)",
		"-(-1)",
		"- -1",
		"+(-1)",
		"-(+1)",
		"!(a && b)",
		"!a && b",
		"-(1 + 2)",
		"-a + b",
		"-(a + b)",
		"..(1..2)",
		"..(..2)",
		"..=(1..2)",
		"-(1..2)",
		"-(..)",
		"-(a:1)",
		"!(a:1)",
		"-(1..)",
		"..(1..)",
		"-(math::pi(1))",
		"-(->a)",
		"-(<-a)",
		"-(<->a)",
		"-(->a + 1)",
		"-(->a.b)",
		"-(<-a + 1)",
		"-(-1 + 2)",
		"-(-1.5 + 2)",
		"-(-1.5dec + 2)",
		"-(-a + 2)",
		"-(a - -1)",
		"-(a..)",
		"<int> a",
		"<int> -1",
		"<int> (a + b)",
		"<int> a + b",
		"<array<int>> [1]",
		"<array<int, 3>> [1]",
		"<set<string>> {1,}",
		"<option<int>> 1",
		"<int | string> 1",
		"<record<a | b>> a:1",
		"<geometry<point>> (1, 2)",
		"<int><float> 1",
		"<int> (SELECT * FROM x)",
		"<{ a: int }> { a: 1 }",
		"<[int, string]> [1, 'a']",
		"<'a' | 1 | true> 'a'",
		"<function> a",
		"<file<b>> a",
		// Binary operators and parentheses.
		"a + b",
		"a - b",
		"a * b",
		"a / b",
		"a % b",
		"a ** b",
		"a = b",
		"a == b",
		"a != b",
		"a *= b",
		"a ?= b",
		"a || b",
		"a OR b",
		"a && b",
		"a AND b",
		"a ?? b",
		"a ?: b",
		"a < b",
		"a <= b",
		"a > b",
		"a >= b",
		"a ∋ b",
		"a ∌ b",
		"a ⊇ b",
		"a ⊃ b",
		"a ⊅ b",
		"a ∈ b",
		"a ∉ b",
		"a ⊆ b",
		"a ⊂ b",
		"a ⊄ b",
		"a CONTAINS b",
		"a CONTAINSNOT b",
		"a CONTAINSALL b",
		"a CONTAINSANY b",
		"a CONTAINSNONE b",
		"a INSIDE b",
		"a NOTINSIDE b",
		"a ALLINSIDE b",
		"a ANYINSIDE b",
		"a NONEINSIDE b",
		"a OUTSIDE b",
		"a INTERSECTS b",
		"1..2",
		"1..=2",
		"1>..2",
		"1>..=2",
		"a @@ b",
		"a @1@ b",
		"a @AND@ b",
		"a @OR@ b",
		"a @1,OR@ b",
		"a <|3|> b",
		"a <|3,COSINE|> b",
		"a <|3,EUCLIDEAN|> b",
		"a <|3,MANHATTAN|> b",
		"a <|3,40|> b",
		"(a @@ b) @@ c",
		"a @@ (b @@ c)",
		"(a <|3|> b) <|3|> c",
		"a + b + c",
		"(a + b) + c",
		"a + (b + c)",
		"a - b + c",
		"a - (b + c)",
		"(a - b) - c",
		"a - (b - c)",
		"(a + b) * c",
		"a * (b + c)",
		"a * b + c",
		"a / b * c",
		"a / (b * c)",
		"(a * b) / c",
		"a * (b / c)",
		"a ** b ** c",
		"(a ** b) ** c",
		"a ** (b ** c)",
		"(a ** b) * c",
		"(a = b) = c",
		"a = (b = c)",
		"a == b == c",
		"(a == b) == c",
		"a == (b == c)",
		"(a < b) < c",
		"a < (b < c)",
		"(a < b) == c",
		"a < b == c",
		"a == (b < c)",
		"a || b && c",
		"(a || b) && c",
		"a || (b && c)",
		"a && (b || c)",
		"(a ?? b) ?: c",
		"a ?? (b ?: c)",
		"a ?? b || c",
		"(a ?? b) || c",
		"(1..2)..3",
		"1..(2..3)",
		"(1..2) + 1",
		"1 + (2..3)",
		"1..2 + 3",
		"(1 + 2)..3",
		"a..b",
		"(a:1) + 1",
		"1 + (a:1)",
		"(a:1) = (b:2)",
		"(..) + 1",
		"1 + (..)",
		"(..) = (..)",
		"(1 + 2) > 3",
		"1 + (2 > 3)",
		"1 > 2 + 3",
		"-1 + 2",
		"1 + -2",
		"1 - -2",
		"1 - (-2)",
		"1 - - 2",
		"-1 ** 2",
		"-(1 ** 2)",
		"(-1) ** 2",
		"(-a) ** 2",
		"!a = b",
		"!(a = b)",
		"(!a) = b",
		"1 + (SELECT * FROM x)",
		"(SELECT * FROM x) + 1",
		"(CREATE a) + (UPDATE b)",
		"1 + (RETURN 1)",
		"1 + (RETURN 1 FETCH a)",
		"1 + (IF a { b })",
		"1 + (LET $a = 1)",
		"(THROW 1) + 1",
		"1 + (THROW 1)",
		"1 + (|| 1)",
		"(|| 1) + 1",
		"(INFO FOR ROOT) + 1",
		"(SLEEP 1s) + 1",
		"(a:1..2) + 1",
		"1 + (a:1..2)",
		"1 + ($a = 1)",
		// Postfix ranges and calls.
		"a..",
		"a>..",
		"1..",
		"1>..",
		"(a..)..",
		"(a>..)>..",
		"(a + b)..",
		"a + b..",
		"(a..) + b",
		"a + (b..)",
		"(-1)..",
		"-1..",
		"(..2)..",
		"(a:1)..",
		"(a.b)..",
		"a.b..",
		"(a..).b",
		"(a)(1)",
		"(a + b)(1)",
		"(1 + 2)(3, 4)",
		"(a)(1)(2)",
		"(a.b)(1)",
		"(f(1))(2)",
		"(a..)(1)",
		"math::pi(1)",
		"math::pi(1) + 1",
		"1 + math::pi(1)",
		"math::pi(1)(2)",
		"(math::pi(1))..",
		"math::pi",
		"(|$a| $a)(1)",
		"($a)(1)",
		"(a)((SELECT * FROM x), (RETURN 1 FETCH a))",
		// Function calls.
		"math::abs(1)",
		"math::ABS(1)",
		"string::len('a')",
		"type::is::array(a)",
		"count()",
		"COUNT()",
		"time::now()",
		"math::abs((SELECT * FROM x))",
		"math::abs(RETURN 1)",
		"math::abs((RETURN 1 FETCH a))",
		"math::abs(-1)",
		"math::abs(1 + 2)",
		"string::concat('a', 'b', 'c')",
		"fn::x(1, 2)",
		"fn::x::y()",
		"fn::`a b`()",
		"fn::`select`()",
		"ext::foo(1)",
		"ext::foo::bar(1)",
		"ext::`a b`::`c d`(1)",
		"mod::foo(1)",
		"silo::org::pkg<1.2.3>(1)",
		"silo::org::pkg<1.2.3>::sub::x(1)",
		"ml::model<1.0.0>(1)",
		"ml::a::b<1.0.0>(1, 2)",
		"function(1, 2) { return 1; }",
		"function() {}",
		"function() { return `x`; }",
		"(function() { return 1; }).a",
		// Idioms and parts.
		"a.b",
		"a.b.c",
		"a.`select`.c",
		"a[0]",
		"a[1 + 2]",
		"a[$]",
		"a.*",
		"a[*]",
		"a[WHERE x > 1]",
		"a[WHERE (SELECT * FROM x)]",
		"a.b()",
		"a.b(1, 2)",
		"a.b.c(1).d",
		"a.b((SELECT * FROM x))",
		"a.`select`(1)",
		"a.?",
		"a.b.?.c",
		"a...",
		"a.@",
		"a.@.b",
		"a.b[0].c[1]",
		"@.a",
		"@.a.b",
		"$a.b",
		"$a[0]",
		"$a.b.c",
		"$a.*",
		"(a.b).c",
		"(a).b",
		"(a.b)[0]",
		"(a.b)[0].c",
		"((a.b).c).d",
		"(a)[0]",
		"([1, 2])[0]",
		"[1, 2][0]",
		"{ a: 1 }.a",
		"({ a: 1 }).a",
		"({ a: [1] }.a)[0]",
		"(1 + 2).a",
		"(a + b).c",
		"(-1).a",
		"(-1.5).a",
		"(-1.5dec).a",
		"(-0.0).a",
		"(1).a",
		"(NONE).a",
		"(NULL).a",
		"(a:1).b",
		"a:1.b",
		"a:1.b.c",
		"(!a).b",
		"(<int> a).b",
		"(a..).b",
		"(a..2).b",
		"(a(1)).b",
		"f(1).b",
		"f(1).b[0]",
		"f(1)[0]",
		"math::abs(1).b",
		"math::pi.a",
		"(math::pi).a",
		"(math::pi(1)).a",
		"(SELECT * FROM x).a",
		"(SELECT * FROM x)[0].a",
		"(RETURN 1).a",
		"(IF a { b }).c",
		"(|| 1).a",
		"(|| 1)[0]",
		"(1, 2).c",
		"|a:3|.b",
		"('x').a",
		"(r'a:1').b",
		"(u'018a6180-0000-7000-8000-000000000000').a",
		"(..).a",
		"($a = 1).b",
		// Lookups and graph-edge links.
		"->a",
		"<-a",
		"<->a",
		"<~a",
		"->?",
		"<-?",
		"<->?",
		"<~?",
		"->a->b",
		"->a->b.c",
		"<-a<-b",
		"->a<-b",
		"<->a<->b",
		"a->b",
		"a:1->b",
		"a:1->b->c.d",
		"a:1->b[WHERE x]->c.name",
		"a:1<-b<-c",
		"a:1<~b",
		"$a->b->c",
		"$a<-b",
		"@->a",
		"->(a, b)",
		"<-(a, b)",
		"<->(a, b, c)",
		"->(a, b)->c",
		"->`select`",
		"->`a b`",
		"->(`select`:1..2)",
		"<~(`select` FIELD `value`)",
		"->(`select`, `value`)",
		"->(a WHERE x > 1)",
		"->(a WHERE x AS y)",
		"->(a AS y)",
		"->(? WHERE x)",
		"->(?)",
		"->(a LIMIT 2)",
		"->(a WHERE x LIMIT 3 START 1)",
		"->(SELECT x FROM a ORDER BY y DESC, z COLLATE NUMERIC LIMIT 3 START 1)",
		"->(SELECT x FROM a ORDER BY RAND())",
		"|| (->a)",
		"|| (->?)",
		"|| (<-a.b)",
		"a.{2}()",
		"a.{ b: (->x) }.c",
		"->(a START 2)",
		"->(a, b WHERE x)",
		"->(SELECT x FROM a)",
		"->(SELECT x, y AS z FROM a)",
		"->(SELECT * FROM a)",
		"->(SELECT VALUE x FROM a)",
		"->(SELECT VALUE x AS y FROM a)",
		"->(SELECT x FROM ONLY a)",
		"->(SELECT x FROM a, b)",
		"->(SELECT x FROM a WHERE y)",
		"->(SELECT x FROM a WHERE y ORDER BY z LIMIT 1 START 2 AS w)",
		"->(SELECT x FROM a SPLIT x)",
		"->(SELECT x, count() FROM a GROUP BY x)",
		"->(SELECT count() FROM a GROUP ALL)",
		"->(SELECT x FROM ? WHERE y)",
		"->(SELECT x FROM a:1..5)",
		"->a:1..5",
		"->(a:1..5)",
		"->(a:1..)",
		"->(a:..5)",
		"->(a:..=5)",
		"->(a:1>..=5)",
		"->(a:1>..)",
		"->(a:..)",
		"->(a:1..3, b)",
		"->(a:[1]..[2])",
		"->(a:1..5 WHERE x)",
		"->(a:1..5 AS y)",
		"<~(a FIELD f)",
		"<~(a FIELD f, b)",
		"<~(a FIELD f WHERE x)",
		"<~a.b",
		"<~(a).b",
		"<-(a:1..).b",
		"<~(a WHERE x)",
		"<~(SELECT x FROM a)",
		"->(a WHERE (SELECT * FROM x))",
		"->(SELECT (SELECT * FROM x) FROM a)",
		"->(a FIELD f)",
		"-(->a)",
		"(->a).b",
		"(->a)->b",
		"(->a)[0]",
		"->a.{ b, c }",
		"(->a)(1)",
		"1 + ->a",
		"->a + 1",
		"1 - ->a",
		// Recursion, destructuring and repeat.
		"a.{3}",
		"a.{1..5}",
		"a.{..5}",
		"a.{1..}",
		"a.{..}",
		"a.{3}.b",
		"a.{1..3+path}",
		"a.{1..3+collect}",
		"a.{..+path+inclusive}",
		"a.{..+collect+inclusive}",
		"a.{3+path}",
		"a.{3+shortest=b:1}",
		"a.{..+shortest=b:1+inclusive}",
		"a.{2}(->x)",
		"a.{2}(->x->y)",
		"a.{1..3}(.b.c)",
		"a.{2}(.b)",
		"a.{2}(->x.name)",
		"a.{2}(->x).@",
		"a.{ b }",
		"a.{ b, c }",
		"a.{ b: c }",
		"a.{ b: c.d }",
		"a.{ b: (c) }",
		"a.{ b: ->x }",
		"a.{ b: (->x) }",
		"a.{ b: 1 }",
		"a.{ b: -1 }",
		"a.{ b: 1 + 2 }",
		"a.{ b: (1 + 2) }",
		"a.{ b: f(1) }",
		"a.{ b: (SELECT * FROM x) }",
		"a.{ b: 'x' }",
		"a.{ b: 'a:1' }",
		"a.{ b: $c }",
		"a.{ b: $c.d }",
		"a.{ b: [1] }",
		"a.{ b: c:1 }",
		"a.{ b: NONE }",
		"a.{ b.* }",
		"a.{ b.{ c } }",
		"a.{ b.{ c, d: e }, f }",
		"a.{ ... }",
		"a.{ b, ... }",
		"a.{ `select`, `a b`: c }",
		"a.{}",
		"a.{ b }.c",
		"a.{ b }[0]",
		"$a.{ b, c }",
		// Blocks.
		"{;}",
		"{ 1 }",
		"{ 1; 2 }",
		"{ 1; 2; 3 }",
		"{ a }",
		"{ a; b }",
		"{ (a:1) }",
		"{ (a:1); 2 }",
		"{ 1; (a:1) }",
		"{ (a:1); (b:2) }",
		"{ ($a = 1) }",
		"{ ($a = 1); 2 }",
		"{ 1; ($a = 1) }",
		"{ ($a = 1); ($b = 2) }",
		"{ LET $a = 1; $a }",
		"{ RETURN 1 }",
		"{ RETURN 1 FETCH a }",
		"{ SELECT * FROM x }",
		"{ { 1 } }",
		"{ { 1 }; { 2 } }",
		"{ BREAK }",
		"{ THROW 'x' }",
		"{ (SELECT * FROM x); (SELECT * FROM y) }",
		"{ 1; }",
		"{ a:1 }.a",
		"($a = 1)",
		"(a:1)",
		"(a:1); 1",
		// Closures.
		"|| 1",
		"|$a| $a",
		"|$a: int| $a",
		"|$a: array<int>| $a",
		"|$a, $b| $a + $b",
		"|$a: int, $b: string| $a",
		"|$a: int| -> int { $a }",
		"|| -> int { 1 }",
		"|$a| -> int | string { $a }",
		"|$a: <int | string>| $a",
		"|$a: <option<int>>| $a",
		"|| a.b",
		"|| a + 1",
		"|| a",
		"|| (a + 1)",
		"|| (SELECT * FROM x)",
		"|| (RETURN 1 FETCH a)",
		"|| RETURN 1",
		"|| (a:1)",
		"|| { 1 }",
		"|| |$a| $a",
		"|| 1 + ->a",
		"|| a..",
		"|| (1..)",
		"|$`a b`| $`a b`",
		"|$select| $select",
		"|| ((a)..)",
		"|| (f(1))",
		"|| f(1)",
		"|| (THROW 1)",
		"|| (IF a { b })",
		// Mocks.
		"|a:3|",
		"|a:1..3|",
		"|a:1>..3|",
		"|a:1..=3|",
		"|a:1>..=3|",
		"|a:..3|",
		"|a:..=3|",
		"|a:1..|",
		"|a:1>..|",
		"|a:..|",
		"|`a b`:3|",
		"|`select`:3|",
		"|a:-3|",
		"|a:0|",
		// LET, IF, FOR, RETURN, THROW, SLEEP, BREAK, CONTINUE.
		"LET $a = 1",
		"LET $a: int = 1",
		"LET $a: int | string = 1",
		"LET $a = (SELECT * FROM x)",
		"LET $a = (RETURN 1 FETCH b)",
		"LET $`a b` = 1",
		"LET $select = a:1",
		"LET $a = (LET $b = 1)",
		"LET $a = $b = 1",
		"IF a { b }",
		"IF a { b } ELSE { c }",
		"IF a { b } ELSE IF c { d }",
		"IF a { b } ELSE IF c { d } ELSE { e }",
		"IF a THEN b END",
		"IF a THEN b ELSE c END",
		"IF a THEN b ELSE IF c THEN d END",
		"IF a THEN b ELSE IF c THEN d ELSE e END",
		"IF a THEN (IF b { c }) END",
		"IF a THEN (IF b { c } ELSE { d }) ELSE e END",
		"IF a THEN (IF b THEN c END) END",
		"IF a THEN b ELSE (SELECT * FROM x) END",
		"IF (SELECT * FROM x) THEN (SELECT * FROM y) ELSE (SELECT * FROM z) END",
		"IF (SELECT * FROM x) { 2 }",
		"IF a { b; c }",
		"IF a { b; c; d }",
		"IF a { 1; 2 }",
		"IF a { 'x'; $y }",
		"IF a { 1; 2 } ELSE { 3; 4 }",
		"IF a { SELECT * FROM x; 2 }",
		"IF a { SELECT * FROM x }",
		"IF a { LET $b = 1; $b }",
		"IF a {;}",
		"IF a {;} ELSE {;}",
		"IF a { (b:1) }",
		"IF a { ($b = 1) }",
		"IF a { b } ELSE IF c { 1; 2 }",
		"IF a { (b:1); c }",
		"IF a THEN { b } END",
		"IF a THEN { b } ELSE { c } END",
		"IF a { IF b { c } }",
		"IF a { b } ELSE { IF c { d } ELSE { e } }",
		"FOR $a IN [1, 2] { a }",
		"FOR $a IN 1..3 { 1; 2 }",
		"FOR $a IN (SELECT * FROM x) {;}",
		"FOR $a IN b { BREAK; CONTINUE }",
		"FOR $a IN b { IF a { CONTINUE } }",
		"FOR $`a b` IN b { $`a b` }",
		"FOR $a IN (RETURN [1] FETCH b) { $a }",
		"RETURN 1",
		"RETURN (SELECT * FROM x)",
		"RETURN a FETCH b, c",
		"RETURN a FETCH b.c",
		"RETURN (RETURN 1 FETCH a)",
		"RETURN (a:1)",
		"RETURN $a = 1",
		"RETURN IF a { b }",
		"RETURN { a: 1 }",
		"THROW 'x'",
		"THROW (SELECT * FROM x)",
		"THROW (THROW 1)",
		"THROW { a: 1 }",
		"THROW (RETURN 1 FETCH a)",
		"SLEEP 1s",
		"SLEEP 90s",
		"SLEEP 1h30m",
		"BREAK",
		"CONTINUE",
		// SELECT.
		"SELECT * FROM a",
		"SELECT a, b AS c FROM a, b",
		"SELECT a.b AS c.d FROM a",
		"SELECT *, a FROM a",
		"SELECT VALUE a FROM a",
		"SELECT VALUE a AS b FROM a",
		"SELECT VALUE (SELECT * FROM x) FROM a",
		"SELECT (SELECT * FROM x) AS x FROM a",
		"SELECT * OMIT a, b FROM a",
		"SELECT * OMIT (a) FROM a",
		"SELECT * FROM ONLY a",
		"SELECT * FROM ONLY a:1",
		"SELECT * FROM a:1, b:2",
		"SELECT * FROM a:1..5",
		"SELECT * FROM (SELECT * FROM x)",
		"SELECT * FROM (a:1)",
		"SELECT * FROM $a",
		"SELECT * FROM [1, 2]",
		"SELECT * FROM a WITH NOINDEX",
		"SELECT * FROM a WITH INDEX i, `j k`",
		"SELECT * FROM a WHERE b",
		"SELECT * FROM a WHERE (SELECT * FROM x)",
		"SELECT * FROM a WHERE NONE",
		"SELECT * FROM a WHERE b = 1 AND c > 2",
		"SELECT a, b FROM c SPLIT a",
		"SELECT a, b FROM c SPLIT a, b",
		"SELECT a, b FROM c SPLIT ON a",
		"SELECT a, count() FROM c GROUP BY a",
		"SELECT a, b, count() FROM c GROUP BY a, b",
		"SELECT count() FROM a GROUP ALL",
		"SELECT * FROM a ORDER BY b",
		"SELECT * FROM a ORDER BY b DESC",
		"SELECT * FROM a ORDER BY b ASC, c DESC",
		"SELECT * FROM a ORDER BY b COLLATE",
		"SELECT * FROM a ORDER BY b NUMERIC",
		"SELECT * FROM a ORDER BY b COLLATE NUMERIC DESC",
		"SELECT * FROM a ORDER BY RAND()",
		"SELECT * FROM a ORDER BY b.c[0]",
		"SELECT * FROM a LIMIT 1",
		"SELECT * FROM a LIMIT (SELECT * FROM x)",
		"SELECT * FROM a LIMIT 1 START 2",
		"SELECT * FROM a START 2",
		"SELECT * FROM a FETCH b",
		"SELECT * FROM a FETCH b, c.d",
		"SELECT * FROM a VERSION d'2020-01-01T00:00:00Z'",
		"SELECT * FROM a VERSION (SELECT * FROM x)",
		"SELECT * FROM a VERSION NONE",
		"SELECT * FROM a VERSION (NONE)",
		"SELECT * FROM a TIMEOUT 1s",
		"SELECT * FROM a TIMEOUT NONE",
		"SELECT * FROM a TIMEOUT (NONE)",
		"SELECT * FROM a TIMEOUT $t",
		"SELECT * FROM a FOR UPDATE",
		"SELECT * FROM a TEMPFILES",
		"SELECT * FROM a EXPLAIN",
		"SELECT * FROM a EXPLAIN FULL",
		"SELECT a, b AS c FROM d:1..2 WITH INDEX i WHERE e SPLIT a ORDER BY b LIMIT 1 START 2 FETCH f",
		"SELECT a FROM d FETCH f VERSION d'2020-01-01T00:00:00Z' TIMEOUT 1s EXPLAIN FULL",
		"SELECT a FROM d TIMEOUT 1s FOR UPDATE TEMPFILES EXPLAIN",
		"SELECT * FROM x",
		"SELECT 1 + 2 AS x FROM a",
		"SELECT -1 AS x FROM a",
		"SELECT NONE FROM a",
		"SELECT (NONE) FROM a",
		"EXPLAIN SELECT * FROM x",
		"EXPLAIN ANALYZE SELECT * FROM a",
		"EXPLAIN FORMAT JSON SELECT * FROM a",
		"EXPLAIN ANALYZE FORMAT JSON SELECT * FROM a",
		"EXPLAIN FORMAT TEXT SELECT * FROM a",
		"EXPLAIN CREATE a",
		"(EXPLAIN SELECT * FROM x)",
		// CREATE, UPDATE, UPSERT, DELETE, RELATE, INSERT.
		"CREATE a",
		"CREATE ONLY a:1",
		"CREATE a, b",
		"CREATE a:1, a:2",
		"CREATE (a:1)",
		"CREATE $a",
		"CREATE a SET b = 1",
		"CREATE a SET b = 1, c += 2, d -= 3, e +?= 4",
		"CREATE a SET b.c = (SELECT * FROM x), d[0] = 2",
		"CREATE a SET b = (RETURN 1 FETCH c)",
		"CREATE a UNSET b, c.d",
		"CREATE a CONTENT { b: 1 }",
		"CREATE a CONTENT $b",
		"CREATE a CONTENT (SELECT * FROM x)",
		"CREATE a MERGE { b: 1 }",
		"CREATE a RETURN NONE",
		"CREATE a RETURN NULL",
		"CREATE a RETURN DIFF",
		"CREATE a RETURN AFTER",
		"CREATE a RETURN BEFORE",
		"CREATE a RETURN b, c",
		"CREATE a RETURN b AS c, d",
		"CREATE a RETURN *",
		"CREATE a RETURN *, b",
		"CREATE a RETURN b, *",
		"CREATE a RETURN VALUE b",
		"CREATE a RETURN VALUE b AS c",
		"CREATE a RETURN VALUE (NONE)",
		"CREATE a RETURN (NONE)",
		"CREATE a RETURN (NULL)",
		"CREATE a RETURN (NONE + 1)",
		"CREATE a RETURN (NONE..)",
		"CREATE a RETURN (NONE) AS b",
		"CREATE a RETURN (NONE), (NONE)",
		"CREATE a RETURN (NONE).b",
		"CREATE a RETURN (NONE)(1)",
		"CREATE a RETURN (NONE = 1) AS b, c",
		"CREATE a RETURN (-1)",
		"CREATE a RETURN (SELECT * FROM x)",
		"CREATE a RETURN 1 + 2",
		"CREATE a RETURN b..",
		"CREATE a TIMEOUT 1s",
		"CREATE a TIMEOUT NONE",
		"CREATE a SET b = 1 RETURN AFTER TIMEOUT 1s",
		"UPDATE a",
		"UPDATE ONLY a:1",
		"UPDATE a SET b = 1 WHERE c",
		"UPDATE a SET b = 1 WHERE c RETURN AFTER",
		"UPDATE a WITH NOINDEX SET b = 1",
		"UPDATE a WITH INDEX i SET b = 1",
		"UPDATE a CONTENT { b: 1 }",
		"UPDATE a MERGE { b: 1 }",
		"UPDATE a REPLACE { b: 1 }",
		"UPDATE a PATCH [{ op: 'add', path: '/b', value: 1 }]",
		"UPDATE a UNSET b",
		"UPDATE a SET b = 1 TIMEOUT 1s",
		"UPDATE a SET b = 1 EXPLAIN",
		"UPDATE a SET b = 1 EXPLAIN FULL",
		"UPDATE a WHERE (SELECT * FROM x)",
		"UPDATE a, b WHERE c",
		"UPSERT a",
		"UPSERT ONLY a:1 SET b = 1",
		"UPSERT a SET b = 1 WHERE c RETURN BEFORE",
		"UPSERT a WITH NOINDEX CONTENT { b: 1 } EXPLAIN",
		"UPSERT a MERGE $b TIMEOUT 1s",
		"DELETE a",
		"DELETE ONLY a:1",
		"DELETE a WHERE b",
		"DELETE a WITH NOINDEX WHERE b RETURN BEFORE",
		"DELETE a WHERE b TIMEOUT 1s EXPLAIN",
		"DELETE a:1..3",
		"DELETE (SELECT * FROM x)",
		"RELATE a:1->b->c:2",
		"RELATE ONLY a:1->b->c:2",
		"RELATE a:1->b->c:2 SET x = 1",
		"RELATE a:1->b->c:2 CONTENT { x: 1 }",
		"RELATE a:1->b->c:2 RETURN NONE",
		"RELATE a:1->b->c:2 TIMEOUT 1s",
		"RELATE [a:1]->b->[c:2]",
		"RELATE $a->$b->$c",
		"RELATE a:1->$b->c:2",
		"RELATE (a:1..2)->b->c:3",
		"RELATE a:1->b->(c:2..3)",
		"RELATE a:1->(1 + 1)->c:2",
		"RELATE (SELECT * FROM x)->b->(SELECT * FROM y)",
		"RELATE a:rand()->b->c:ulid()",
		"RELATE a:[1]->b->c:{ x: 1 }",
		"RELATE a:u'018a6180-0000-7000-8000-000000000000'->b->c:2",
		"RELATE (b)->c->(d)",
		"RELATE (r'a:1')->b->(r'c:2')",
		"RELATE (r'a:1..2')->b->(r'c:2..3')",
		"RELATE ('a:1')->b->('c:2')",
		"RELATE a:1->`b c`->c:2",
		"RELATE a:1->b:1->c:2",
		"INSERT INTO a { b: 1 }",
		"INSERT INTO a [{ b: 1 }, { b: 2 }]",
		"INSERT INTO a (b, c) VALUES (1, 2), (3, 4)",
		"INSERT INTO a (b) VALUES ((SELECT * FROM x))",
		"INSERT IGNORE INTO a { b: 1 }",
		"INSERT RELATION INTO a { b: 1 }",
		"INSERT RELATION IGNORE INTO a { b: 1 }",
		"INSERT INTO $a { b: 1 }",
		"INSERT INTO a (b) VALUES (1) ON DUPLICATE KEY UPDATE b += 1, c = 2",
		"INSERT INTO a { b: 1 } ON DUPLICATE KEY UPDATE b = (SELECT * FROM x) RETURN AFTER TIMEOUT 1s",
		"INSERT { id: 1 }",
		"INSERT (b) VALUES (1)",
		"INSERT $a",
		"INSERT INTO a $b",
		"INSERT INTO a { b: 1 } RETURN NONE",
		// INFO, REBUILD.
		"INFO FOR ROOT",
		"INFO FOR NS",
		"INFO FOR DB",
		"INFO FOR TABLE a",
		"INFO FOR TABLE `a b`",
		"INFO FOR TABLE (a)",
		"INFO FOR USER a",
		"INFO FOR USER a ON ROOT",
		"INFO FOR USER a ON NS",
		"INFO FOR USER a ON DB",
		"INFO FOR INDEX i ON a",
		"INFO FOR ROOT STRUCTURE",
		"INFO FOR NS STRUCTURE",
		"INFO FOR DB STRUCTURE",
		"INFO FOR TABLE a STRUCTURE",
		"INFO FOR USER a ON ROOT STRUCTURE",
		"INFO FOR USER a STRUCTURE",
		"INFO FOR INDEX i ON a STRUCTURE",
		"INFO FOR ROOT VERSION d'2020-01-01T00:00:00Z'",
		"INFO FOR NS VERSION d'2020-01-01T00:00:00Z' STRUCTURE",
		"INFO FOR DB VERSION d'2020-01-01T00:00:00Z'",
		"INFO FOR TABLE a VERSION d'2020-01-01T00:00:00Z' STRUCTURE",
		"INFO FOR DB VERSION (SELECT * FROM x)",
		"REBUILD INDEX i ON a",
		"REBUILD INDEX IF EXISTS i ON a",
		"REBUILD INDEX i ON a CONCURRENTLY",
		"REBUILD INDEX IF EXISTS `if` ON `a b` CONCURRENTLY",
		"REBUILD INDEX `select` ON `select`",
		// Top-level-only statements.
		"BEGIN",
		"BEGIN TRANSACTION",
		"BEGIN ISOLATION LEVEL SNAPSHOT",
		"BEGIN ISOLATION LEVEL SERIALIZABLE",
		"BEGIN READ ONLY",
		"BEGIN READ ONLY MAX STALENESS 5s",
		"BEGIN READ ONLY MAX STALENESS 5s STRICT",
		"BEGIN READ ONLY MAX STALENESS $x",
		"CANCEL",
		"COMMIT",
		"CANCEL TRANSACTION",
		"COMMIT TRANSACTION",
		"BEGIN; CANCEL; BEGIN; COMMIT",
		"USE NS a",
		"USE DB b",
		"USE NS a DB b",
		"USE DEFAULT",
		"USE NS $a DB (SELECT * FROM x)",
		"OPTION IMPORT",
		"OPTION IMPORT = false",
		"OPTION IMPORT = true",
		"KILL u'018a6180-0000-7000-8000-000000000000'",
		"KILL $x",
		"KILL u'018a6180-0000-7000-8000-000000000001'",
		"LIVE SELECT * FROM a",
		"LIVE SELECT DIFF FROM a",
		"LIVE SELECT VALUE x FROM a WHERE y FETCH z",
		"LIVE SELECT a, b FROM $c WHERE d",
		"SHOW CHANGES FOR TABLE a SINCE 1 LIMIT 10",
		"SHOW CHANGES FOR DATABASE SINCE d'2020-01-01T00:00:00Z'",
		"SHOW CHANGES FOR TABLE `a b` SINCE 0",
		"ACCESS a GRANT FOR USER b",
		"ACCESS a GRANT FOR RECORD c:1",
		"ACCESS a ON DATABASE SHOW ALL",
		"ACCESS a ON NAMESPACE SHOW GRANT x",
		"ACCESS a SHOW WHERE b = 1",
		"ACCESS a REVOKE ALL",
		"ACCESS a REVOKE GRANT x",
		"ACCESS a REVOKE WHERE b",
		"ACCESS a PURGE EXPIRED FOR 1d",
		"ACCESS a PURGE REVOKED",
		"ACCESS a PURGE EXPIRED, REVOKED FOR 1h",
		// DEFINE, ALTER, REMOVE.
		"DEFINE NAMESPACE a",
		"DEFINE DATABASE a",
		"DEFINE TABLE a",
		"DEFINE TABLE IF NOT EXISTS a SCHEMAFULL",
		"DEFINE TABLE OVERWRITE a TYPE RELATION IN b OUT c",
		"DEFINE TABLE a AS SELECT x, count() FROM b GROUP BY x",
		"DEFINE FIELD a ON b TYPE int",
		"DEFINE FIELD a.* ON b TYPE option<string> DEFAULT 'x' ASSERT $value != NONE",
		"DEFINE INDEX i ON a FIELDS b UNIQUE",
		"DEFINE INDEX i ON a FIELDS b, c HNSW DIMENSION 3 DIST COSINE",
		"DEFINE FUNCTION fn::f($a: int) -> int { RETURN $a; }",
		"DEFINE PARAM $p VALUE 1",
		"DEFINE EVENT e ON a WHEN true THEN (RETURN 1)",
		"DEFINE USER u ON ROOT PASSWORD 'x' ROLES OWNER",
		"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret'",
		"DEFINE ACCESS a ON DATABASE TYPE BEARER FOR USER",
		"DEFINE ACCESS a ON DATABASE TYPE JWT URL 'https://example.com/jwks'",
		"DEFINE ANALYZER a TOKENIZERS blank FILTERS lowercase",
		"DEFINE SEQUENCE a BATCH 10 START 1",
		"ALTER TABLE a SCHEMAFULL",
		"ALTER TABLE a DROP COMMENT",
		"ALTER FIELD a ON b TYPE string",
		"REMOVE TABLE a",
		"REMOVE FIELD a ON b",
		"REMOVE INDEX i ON a",
		"REMOVE FUNCTION fn::f",
		// Legacy strings and mixed statements.
		"'018a6180-0000-7000-8000-000000000000'",
		"'2020-01-01T00:00:00Z'",
		"'person:tobie'",
		"'a:1' + 1",
		"1 + 'a:1'",
		"'a:1'..2",
		"{ 'a:1' }",
		"{ 'a:1'; 2 }",
		"{ 'a:1', 2 }",
		"{ 2, 'a:1' }",
		"'a:1'.b",
		"-'a:1'",
		"[ 'a:1', '2020-01-01T00:00:00Z' ]",
		"CREATE 'a:1'",
		"SELECT * FROM 'a:1'",
		"'a:1..2'",
		"'a:rand()'",
		"'a:[1]'",
		"'a b:1'",
		"s'a:1' + 1",
		"1; 2; SELECT * FROM z",
	];
}
