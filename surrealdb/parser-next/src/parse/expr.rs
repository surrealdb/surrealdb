use super::*;

// Binding powers, lowest first.
/// The token count [`Parser::infix_op`] returns for an operator whose
/// tokens carry a payload, parsed by [`Parser::operator_payload`].
pub(crate) const OPERATOR_PAYLOAD: u8 = u8::MAX;

pub(crate) const BP_BASE: u8 = 0;
pub(crate) const BP_NULLISH: u8 = 1;
pub(crate) const BP_OR: u8 = 2;
pub(crate) const BP_AND: u8 = 3;
pub(crate) const BP_EQ: u8 = 4;
pub(crate) const BP_REL: u8 = 5;
pub(crate) const BP_ADD: u8 = 6;
pub(crate) const BP_MUL: u8 = 7;
pub(crate) const BP_POW: u8 = 8;
pub(crate) const BP_PREFIX: u8 = 9;
pub(crate) const BP_RANGE: u8 = 10;
pub(crate) const BP_CALL: u8 = 11;
pub(crate) const BP_PRIME: u8 = 12;

/// Binding power of each binary operator, indexed by `Binary as usize`.
pub(crate) const BINARY_BP: [u8; Binary::COUNT] = {
	let mut t = [0u8; Binary::COUNT];
	t[Binary::Or as usize] = BP_OR;
	t[Binary::And as usize] = BP_AND;
	t[Binary::Tco as usize] = BP_NULLISH;
	t[Binary::Nco as usize] = BP_NULLISH;
	let eq =
		[Binary::Eq, Binary::ExactEq, Binary::Ne, Binary::AllEq, Binary::AnyEq, Binary::Matches];
	let mut i = 0;
	while i < eq.len() {
		t[eq[i] as usize] = BP_EQ;
		i += 1;
	}
	let rel = [
		Binary::Lt,
		Binary::Le,
		Binary::Gt,
		Binary::Ge,
		Binary::Contain,
		Binary::NotContain,
		Binary::ContainAll,
		Binary::ContainAny,
		Binary::ContainNone,
		Binary::Inside,
		Binary::NotInside,
		Binary::AllInside,
		Binary::AnyInside,
		Binary::NoneInside,
		Binary::Outside,
		Binary::Intersects,
		Binary::Knn,
	];
	let mut i = 0;
	while i < rel.len() {
		t[rel[i] as usize] = BP_REL;
		i += 1;
	}
	t[Binary::Add as usize] = BP_ADD;
	t[Binary::Sub as usize] = BP_ADD;
	t[Binary::Mul as usize] = BP_MUL;
	t[Binary::Div as usize] = BP_MUL;
	t[Binary::Rem as usize] = BP_MUL;
	t[Binary::Pow as usize] = BP_POW;
	t[Binary::Range as usize] = BP_RANGE;
	t[Binary::RangeInclusive as usize] = BP_RANGE;
	t[Binary::RangeSkip as usize] = BP_RANGE;
	t[Binary::RangeSkipInclusive as usize] = BP_RANGE;
	t
};

/// The binding power the chained-operator check compares, which differs
/// from the parsing binding power for `@@`: it parses at equality but is
/// checked as a relation, as earlier releases check it.
pub(crate) fn check_bp(op: Binary) -> u8 {
	if op == Binary::Matches {
		BP_REL
	} else {
		BINARY_BP[op as usize]
	}
}

/// Whether chaining the operator without parentheses is defined.
pub(crate) fn associative(op: Binary) -> bool {
	!(op == Binary::Eq
		|| op == Binary::Ne
		|| op == Binary::AllEq
		|| op == Binary::AnyEq
		|| op == Binary::Lt
		|| op == Binary::Le
		|| op == Binary::Gt
		|| op == Binary::Ge
		|| op == Binary::Matches
		|| op == Binary::Contain
		|| op == Binary::NotContain
		|| op == Binary::ContainAll
		|| op == Binary::ContainAny
		|| op == Binary::ContainNone
		|| op == Binary::Inside
		|| op == Binary::NotInside
		|| op == Binary::AllInside
		|| op == Binary::AnyInside
		|| op == Binary::NoneInside
		|| op == Binary::Outside
		|| op == Binary::Intersects
		|| op == Binary::Knn)
}

/// Pending work of the expression parser.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Frame {
	/// Bottom of one `expr` call; restores `table_as_field` when popped.
	/// `charged` when the call costs a query level.
	Base {
		table_as_field: bool,
		charged: bool,
	},
	/// Bottom of an [`Parser::atom`] call: ends the parse as soon as one
	/// primary is complete, before any idiom part or operator.
	Atom {
		table_as_field: bool,
	},
	/// Bottom of a [`Parser::plain_idiom`] call: ends the parse when the
	/// idiom has no more parts, before any operator.
	Path {
		table_as_field: bool,
	},
	Infix {
		op: Binary,
		start: u32,
		/// The operator token, and whether its left operand already chains
		/// a non-associative operator of the same power: an error reported
		/// once the right operand is parsed, as it always has been.
		op_start: u32,
		chain: bool,
		/// For `@…@` and `<|…|>`, the index into `extra` of the operator's
		/// payload; otherwise `NONE`.
		payload: u32,
	},
	Prefix {
		op: Prefix,
		start: u32,
	},
	Cast {
		kind: NodeId,
		start: u32,
	},
	Paren {
		start: u32,
	},
	/// Elements are on the value stack from `mark`.
	Array {
		start: u32,
		mark: u32,
		wrap: Wrap,
	},
	/// Alternating keys and values are on the value stack from `mark`.
	Object {
		start: u32,
		mark: u32,
		wrap: Wrap,
	},
	/// Arguments are on the value stack from `mark`; `callee` is a path
	/// `Name`, an expression, or a method `Name`.
	Args {
		start: u32,
		/// The `(`.
		open: u32,
		mark: u32,
		callee: NodeId,
		tag: Tag,
	},
	/// Idiom parts are on the value stack from `mark`.
	Idiom {
		start: u32,
		mark: u32,
		head: NodeId,
	},
	/// A bracketed idiom part: `[expr]` or `[WHERE expr]`.
	Bracket {
		start: u32,
		tag: Tag,
		table_as_field: bool,
	},
	/// The first element after `{` that was not an object key: a block or a
	/// set, decided by the token after it.
	BlockOrSet {
		start: u32,
		mark: u32,
		covered: bool,
	},
	/// Statements of a block, on the value stack from `mark`. `covered` when
	/// the current statement started with `(`.
	Block {
		start: u32,
		mark: u32,
		covered: bool,
	},
	/// Elements of a set literal.
	Set {
		start: u32,
		mark: u32,
	},
	/// Alternatives of a kind separated by `|`, on the value stack from
	/// `mark`. `option` when inside `option<…>`.
	KindAlts {
		start: u32,
		/// For `option<…>`, its `<`.
		open: u32,
		mark: u32,
		option: bool,
		/// A single kind: `|` ends it rather than adding an alternative.
		single: bool,
	},
	/// A closure body: `|args| body` or `|args| -> kind { … }`.
	Closure {
		start: u32,
		args: u32,
		returns: NodeId,
	},
	/// `array<…>` or `set<…>` awaiting an optional size and `>`.
	KindSized {
		start: u32,
		/// The `<`.
		open: u32,
		code: u8,
	},
	/// A literal object kind; keys and kinds alternate on the value stack.
	KindObject {
		start: u32,
		mark: u32,
	},
	/// A literal array kind. When `named`, it is the opening tuple of an `id`
	/// field's `TYPE`: names and kinds alternate on the value stack.
	KindTuple {
		start: u32,
		mark: u32,
		named: bool,
	},
}

impl Frame {
	pub(crate) const fn is_idiom(&self) -> bool {
		match self {
			Self::Idiom {
				..
			} => true,
			Self::Base {
				..
			}
			| Self::Atom {
				..
			}
			| Self::Path {
				..
			}
			| Self::Infix {
				..
			}
			| Self::Prefix {
				..
			}
			| Self::Cast {
				..
			}
			| Self::Paren {
				..
			}
			| Self::Array {
				..
			}
			| Self::Object {
				..
			}
			| Self::Args {
				..
			}
			| Self::Bracket {
				..
			}
			| Self::BlockOrSet {
				..
			}
			| Self::Block {
				..
			}
			| Self::Set {
				..
			}
			| Self::KindAlts {
				..
			}
			| Self::Closure {
				..
			}
			| Self::KindSized {
				..
			}
			| Self::KindObject {
				..
			}
			| Self::KindTuple {
				..
			} => false,
		}
	}
}

/// What to build around a finished array or object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Wrap {
	None,
	/// The record-id key of table `table`, which may still become a range.
	KeyStart {
		table: NodeId,
		start: u32,
		/// Whether a range may follow the key.
		ranges: bool,
	},
	/// The end key of a record-id range.
	KeyEnd {
		table: NodeId,
		start: u32,
		lo: NodeId,
		flags: u8,
	},
}

impl<'a, 's> Parser<'a, 's> {
	/// Parses one expression. `table_as_field` decides whether a bare
	/// identifier is a field of the current document or a table name.
	pub(crate) fn expr(&mut self, table_as_field: bool) -> R<NodeId> {
		self.expr_in(NONE, table_as_field, true)
	}

	/// One primary value with nothing after it: no idiom parts, postfix or
	/// infix operators.
	pub(crate) fn atom(&mut self, table_as_field: bool) -> R<NodeId> {
		let base = self.frames.len();
		self.push_frame(Frame::Atom {
			table_as_field: self.table_as_field,
		})?;
		self.table_as_field = table_as_field;
		let res = self.run(base, NONE);
		if res.is_err() {
			self.truncate_frames(base);
		}
		res
	}

	/// A record id at an identifier followed by `:`, with nothing after it
	/// and no range key.
	pub(crate) fn record_id_atom(&mut self) -> R<NodeId> {
		let base = self.frames.len();
		self.push_frame(Frame::Atom {
			table_as_field: self.table_as_field,
		})?;
		let t = self.tok;
		let res = self.record_id(t, false).and_then(|v| self.run(base, v));
		if res.is_err() {
			self.truncate_frames(base);
		}
		res
	}

	/// A top-level expression whose first operand, when not `NONE`, is
	/// `first`, already parsed: the rest of a statement that starts with a
	/// statement keyword. Charges no query level, as earlier releases
	/// charge none here.
	pub(crate) fn expr_from(&mut self, first: NodeId, table_as_field: bool) -> R<NodeId> {
		self.expr_in(first, table_as_field, false)
	}

	fn expr_in(&mut self, first: NodeId, table_as_field: bool, charged: bool) -> R<NodeId> {
		let base = self.frames.len();
		self.push_frame(Frame::Base {
			table_as_field: self.table_as_field,
			charged,
		})?;
		self.table_as_field = table_as_field;
		let res = self.run(base, first);
		if res.is_err() {
			self.truncate_frames(base);
		}
		res
	}

	/// The engine. Alternates between wanting an operand and having one,
	/// until the `Base` frame at `base` is reduced.
	pub(crate) fn run(&mut self, base: usize, mut first: NodeId) -> R<NodeId> {
		'operand: loop {
			// ---- want an operand: prefix operators push frames, a primary
			// pushes a value.
			let mut from_operand = false;
			let mut v = if first != NONE {
				let f = core::mem::replace(&mut first, NONE);
				if f == PUSHED {
					// The caller pushed a frame and wants its operand.
					continue 'operand;
				}
				f
			} else {
				from_operand = true;
				loop {
					match self.operand()? {
						Some(v) => break v,
						None => continue,
					}
				}
			};
			// ---- have an operand: postfix, idiom parts and infix operators,
			// reducing frames whose binding power is not lower. Idiom parts
			// continue only a prime value, never a postfix call or range
			// (they are checked for only right after a prime).
			// `..` alone is parsed as a prefix operator, not a prime.
			let mut prime = !(from_operand
				&& self.ast.nodes.get(v as usize).is_some_and(|n| n.tag == Tag::UnboundedRange));
			loop {
				if let Some(&Frame::Atom {
					table_as_field,
				}) = self.frames.last()
				{
					self.pop_frame();
					self.table_as_field = table_as_field;
					debug_assert_eq!(self.frames.len(), base);
					return Ok(v);
				}
				let more_parts = prime && self.continues_idiom();
				if let Some(&Frame::Path {
					table_as_field,
				}) = self.frames.last()
					&& !more_parts
				{
					self.pop_frame();
					self.table_as_field = table_as_field;
					debug_assert_eq!(self.frames.len(), base);
					return Ok(v);
				}
				let on_idiom = self.frames.last().is_some_and(Frame::is_idiom);
				if more_parts && !on_idiom {
					let start = self.ast.nodes[v as usize].start;
					self.push_frame(Frame::Idiom {
						start,
						mark: self.vals.len() as u32,
						head: v,
					})?;
				}
				if self.frames.last().is_some_and(Frame::is_idiom) {
					// Each part costs an expression level until the idiom ends.
					if self.tok.kind == T::DotDotDot
						|| self.tok.kind == T::Dot
						|| self.tok.kind == T::LBracket
						|| self.tok.kind == T::Arrow
						|| self.tok.kind == T::Lt
					{
						self.enter_e()?;
					}
					match self.idiom_part()? {
						PartStep::Done => {}
						PartStep::Pushed => continue 'operand,
						PartStep::Finished => {
							let Some(Frame::Idiom {
								start,
								mark,
								head,
							}) = self.pop_frame()
							else {
								unreachable!()
							};
							// A path lowers to one node per part, so each part
							// is charged a level.
							let n = (self.vals.len() as u32 - mark) as u16;
							let (parts, d) = self.list_from(mark);
							let d = d.max(self.depth_of(head)) + n;
							v = self.node(Tag::Idiom, 0, head, parts, start, d)?;
							// The idiom ended where parts are no longer looked for.
							prime = false;
						}
					}
					continue;
				}
				let min = self.frame_bp();
				// Postfix operators.
				if let Some(op) = self.postfix_op() {
					if BP_RANGE <= min {
						// Falls through to reduce.
					} else {
						let start = self.ast.nodes[v as usize].start;
						self.check_postfix_chain(v)?;
						// A postfix operator deepens the expression's spine.
						self.enter_e()?;
						self.bump();
						if op == 1 {
							self.bump();
						}
						let d = self.depth_of(v);
						v = self.node(Tag::PostfixRange, op, v, 0, start, d)?;
						prime = false;
						continue;
					}
				} else if self.at(T::LParen) && BP_CALL > min {
					self.enter_e()?;
					self.bump();
					let start = self.ast.nodes[v as usize].start;
					if self.eat(T::RParen) {
						let idx = self.empty_list();
						let d = self.depth_of(v);
						v = self.node(Tag::CallExpr, 0, v, idx, start, d)?;
						prime = false;
						continue;
					}
					self.push_frame(Frame::Args {
						start,
						open: self.prev_end - 1,
						mark: self.vals.len() as u32,
						callee: v,
						tag: Tag::CallExpr,
					})?;
					continue 'operand;
				}
				if self.tok.kind == T::PlusEq
					|| self.tok.kind == T::MinusEq
					|| self.tok.kind == T::PlusQEq
				{
					return Err(self.diag(Diag {
						start: self.tok.start,
						end: self.tok.end,
						kind: DiagKind::Unexpected,
						msg: "an operator",
						label: "assignment operators are only allowed in SET and DUPLICATE KEY UPDATE clauses",
						extra: None,
					}));
				}
				if let Some((op, extra)) = self.infix_op()? {
					let bp = BINARY_BP[op as usize];
					if bp > min {
						let start = self.ast.nodes[v as usize].start;
						// An infix operator deepens the expression's spine.
						self.enter_e()?;
						let chain = !associative(op) && self.node_bp(v) == check_bp(op);
						let op_start = self.tok.start;
						let payload = if extra == OPERATOR_PAYLOAD {
							self.operator_payload(op)?
						} else {
							self.bump();
							for _ in 0..extra {
								self.bump();
							}
							NONE
						};
						self.vals.push(v);
						self.push_frame(Frame::Infix {
							op,
							start,
							op_start,
							chain,
							payload,
						})?;
						continue 'operand;
					}
				}
				// Reduce the top frame with `v`.
				let f = self.pop_frame().expect("the base frame is never popped early");
				// Closing a collection, a parenthesis or a closure yields a prime.
				if let Frame::Paren {
					..
				}
				| Frame::Array {
					..
				}
				| Frame::Object {
					..
				}
				| Frame::BlockOrSet {
					..
				}
				| Frame::Block {
					..
				}
				| Frame::Set {
					..
				}
				| Frame::Closure {
					..
				} = f
				{
					prime = true;
				}
				match f {
					Frame::Base {
						table_as_field,
						..
					} => {
						self.table_as_field = table_as_field;
						debug_assert_eq!(self.frames.len(), base);
						return Ok(v);
					}
					Frame::Infix {
						op,
						start,
						op_start,
						chain,
						payload,
					} => {
						let lhs = self.vals.pop().expect("infix lhs");
						if chain {
							return Err(self.chain_error(op, op_start, true));
						}
						if !associative(op)
							&& self.node_bp(v) == check_bp(op)
							&& self.ast.nodes[v as usize].depth & COVERED == 0
						{
							return Err(self.chain_error(op, op_start, false));
						}
						let d = self.depth_of(lhs).max(self.depth_of(v));
						v = if payload == NONE {
							self.node(Tag::Binary, op as u8, lhs, v, start, d)?
						} else {
							// The payload record's first slot is the right operand.
							self.ast.extra[payload as usize] = v;
							self.node(Tag::BinaryX, op as u8, lhs, payload, start, d)?
						};
					}
					Frame::Prefix {
						op,
						start,
					} => {
						let d = self.depth_of(v);
						v = self.node(Tag::Prefix, op as u8, v, 0, start, d)?;
					}
					Frame::Cast {
						kind,
						start,
					} => {
						let d = self.depth_of(v);
						v = self.node(Tag::Cast, 0, kind, v, start, d)?;
					}
					Frame::Paren {
						start,
					} => {
						self.close(T::RParen, start)?;
						self.ast.nodes[v as usize].depth |= COVERED;
					}
					Frame::Array {
						start,
						mark,
						wrap,
					} => {
						self.vals.push(v);
						if self.eat(T::Comma) && !self.at(T::RBracket) {
							self.push_frame(f)?;
							continue 'operand;
						}
						self.close(T::RBracket, start)?;
						let (list, d) = self.list_from(mark);
						v = self.node(Tag::Array, 0, list, 0, start, d)?;
						match self.wrap(v, wrap)? {
							Some(w) => v = w,
							None => continue 'operand,
						}
					}
					Frame::Object {
						start,
						mark,
						wrap,
					} => {
						self.vals.push(v);
						if self.eat(T::Comma) && !self.at(T::RBrace) {
							self.push_frame(f)?;
							self.object_key()?;
							continue 'operand;
						}
						self.close(T::RBrace, start)?;
						let (list, d) = self.list_from(mark);
						v = self.node(Tag::Object, 0, list, 0, start, d)?;
						match self.wrap(v, wrap)? {
							Some(w) => v = w,
							None => continue 'operand,
						}
					}
					Frame::Closure {
						start,
						args,
						returns,
					} => {
						let d = self.depth_of(v).max(self.depth_of(returns));
						let rec = self.record(&[args, returns, v]);
						v = self.node(Tag::Closure, 0, rec, 0, start, d)?;
					}
					Frame::BlockOrSet {
						start,
						mark,
						covered,
					} => {
						if self.eat(T::Comma) {
							self.vals.push(v);
							if self.eat(T::RBrace) {
								let (list, d) = self.list_from(mark);
								v = self.node(Tag::Set, 0, list, 0, start, d)?;
							} else {
								self.push_frame(Frame::Set {
									start,
									mark,
								})?;
								continue 'operand;
							}
						} else {
							if !covered {
								self.check_letless(v)?;
							}
							self.vals.push(v);
							// The first statement needs no `;` before the next.
							while self.eat(T::Semi) {}
							if self.eat(T::RBrace) {
								let (list, d) = self.list_from(mark);
								v = self.node(Tag::Block, 0, list, 0, start, d)?;
							} else {
								self.push_frame(Frame::Block {
									start,
									mark,
									covered: self.at(T::LParen),
								})?;
								continue 'operand;
							}
						}
					}
					Frame::Block {
						start,
						mark,
						covered,
					} => {
						if !covered {
							self.check_letless(v)?;
						}
						self.vals.push(v);
						if self.eat(T::Semi) {
							while self.eat(T::Semi) {}
							if !self.at(T::RBrace) {
								self.push_frame(Frame::Block {
									start,
									mark,
									covered: self.at(T::LParen),
								})?;
								continue 'operand;
							}
						}
						self.close(T::RBrace, start)?;
						let (list, d) = self.list_from(mark);
						v = self.node(Tag::Block, 0, list, 0, start, d)?;
					}
					Frame::Set {
						start,
						mark,
					} => {
						self.vals.push(v);
						if self.eat(T::Comma) && !self.at(T::RBrace) {
							self.push_frame(f)?;
							continue 'operand;
						}
						self.close(T::RBrace, start)?;
						let (list, d) = self.list_from(mark);
						v = self.node(Tag::Set, 0, list, 0, start, d)?;
					}
					Frame::Args {
						start,
						open,
						mark,
						callee,
						tag,
					} => {
						self.vals.push(v);
						if self.eat(T::Comma) && !self.at(T::RParen) {
							self.push_frame(f)?;
							continue 'operand;
						}
						self.close(T::RParen, open)?;
						let (list, d) = self.list_from(mark);
						if tag == Tag::Script {
							v = self.script_body(start, list, d)?;
							prime = true;
							continue;
						}
						let d = d.max(self.depth_of(callee));
						let n = self.node(tag, 0, callee, list, start, d)?;
						if tag == Tag::PartMethod {
							// A method is an idiom part: back to the part loop.
							self.vals.push(n);
							v = NONE;
							continue;
						}
						v = n;
						prime = tag != Tag::CallExpr;
					}
					Frame::Bracket {
						start,
						tag,
						table_as_field,
					} => {
						self.close(T::RBracket, start)?;
						self.table_as_field = table_as_field;
						let d = self.depth_of(v);
						let n = self.node(tag, 0, v, 0, start, d)?;
						self.vals.push(n);
						v = NONE;
						continue;
					}
					Frame::Idiom {
						..
					} => unreachable!("idiom frames are handled above"),
					Frame::Atom {
						..
					}
					| Frame::Path {
						..
					}
					| Frame::KindAlts {
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
					} => unreachable!("kind frames are reduced by the kind parser"),
				}
			}
		}
	}

	/// Binding power below which the top frame does not let operators bind.
	pub(crate) fn frame_bp(&self) -> u8 {
		if let Some(Frame::Infix {
			op,
			..
		}) = self.frames.last()
		{
			BINARY_BP[*op as usize]
		} else if let Some(Frame::Prefix {
			op,
			..
		}) = self.frames.last()
		{
			match op {
				Prefix::Range | Prefix::RangeInclusive => BP_RANGE,
				Prefix::Not | Prefix::Neg | Prefix::Pos => BP_PREFIX,
			}
		} else if let Some(Frame::Cast {
			..
		}) = self.frames.last()
		{
			BP_PREFIX
		} else {
			BP_BASE
		}
	}

	/// The binding power a finished node presents to the associativity check.
	pub(crate) fn node_bp(&self, id: NodeId) -> u8 {
		let n = &self.ast.nodes[id as usize];
		if n.depth & COVERED != 0 {
			return BP_PRIME;
		}
		if n.tag == Tag::Binary || n.tag == Tag::BinaryX {
			check_bp(Binary::from_u8(n.op))
		} else if n.tag == Tag::Prefix
			&& (n.op == Prefix::Range as u8 || n.op == Prefix::RangeInclusive as u8)
		{
			BP_RANGE
		} else if n.tag == Tag::Prefix || n.tag == Tag::Cast {
			BP_PREFIX
		} else if n.tag == Tag::PostfixRange {
			BP_RANGE
		} else {
			BP_PRIME
		}
	}

	/// A non-associative operator chained without parentheses, over the
	/// operator at `op_start` through the token after its right operand.
	/// `lhs` when the left operand is the chained one; the established
	/// wording differs slightly between the two cases.
	pub(crate) fn chain_error(&mut self, op: Binary, op_start: u32, lhs: bool) -> Stop {
		let range = BINARY_BP[op as usize] == BP_RANGE;
		let (msg, label) = match (range, lhs) {
			(true, true) => (
				"Chained range operators has no specified associativity",
				"use parens, '()', to specify which operator must be evaluated first",
			),
			(true, false) => (
				"Chained range operators have no defined associativity.",
				"Use parens, '()', to specify which operator must be evaluated first",
			),
			(false, _) => (
				"Chained relational operators have no defined associativity.",
				"Use parens, '()', to specify which operator must be evaluated first",
			),
		};
		let end = self.tok.end;
		self.note(self.tok);
		self.diag(Diag::message(op_start, end, msg).with_label(label))
	}

	pub(crate) fn check_postfix_chain(&mut self, v: NodeId) -> R<()> {
		if self.node_bp(v) == BP_RANGE {
			let t = self.tok;
			return Err(self.diag(
				Diag::message(
					t.start,
					t.end,
					"Chaining range operators has no specified associativity",
				)
				.with_label("use parens, '()', to specify which operator must be evaluated first"),
			));
		}
		Ok(())
	}

	/// Finishes an array or object that was a record-id key. Returns the value
	/// to continue with, or `None` when a range end key was pushed as a frame.
	pub(crate) fn wrap(&mut self, v: NodeId, wrap: Wrap) -> R<Option<NodeId>> {
		match wrap {
			Wrap::None => Ok(Some(v)),
			Wrap::KeyStart {
				table,
				start,
				ranges,
			} => self.record_after_key(table, start, v, ranges),
			Wrap::KeyEnd {
				table,
				start,
				lo,
				flags,
			} => {
				let range = self.node(Tag::KeyRange, flags, lo, v, start, 2)?;
				let id = self.node(Tag::RecordId, 0, table, range, start, 3)?;
				Ok(Some(id))
			}
		}
	}

	/// Consumes a `@…@` or `<|…|>` operator, returning the index into
	/// `extra` of its payload record, whose first slot is left for the right
	/// operand.
	fn operator_payload(&mut self, op: Binary) -> R<u32> {
		let open = self.bump();
		if op == Binary::Matches {
			// `[rhs, reference Int or NONE, 0 | 1 AND | 2 OR]`
			let t = self.tok;
			let (rf, bool_op) = if t.kind == T::Digits {
				let rf = self.uint_lit()?;
				let b = if self.eat(T::Comma) {
					let o = self.tok;
					let b = if o.kind == T::Ident && o.kw == Kw::And {
						1
					} else if o.kind == T::Ident && o.kw == Kw::Or {
						2
					} else {
						return Err(self.unexpected("either `AND` or `OR`"));
					};
					self.bump();
					b
				} else {
					0
				};
				self.expect(T::At, "@")?;
				(rf, b)
			} else if t.kind == T::Ident && (t.kw == Kw::And || t.kw == Kw::Or) {
				self.bump();
				self.expect(T::At, "@")?;
				(
					NONE,
					if t.kw == Kw::And {
						1
					} else {
						2
					},
				)
			} else if t.kind == T::At {
				self.bump();
				(NONE, 0)
			} else {
				return Err(self.unexpected("a match reference, operator or `@`"));
			};
			return Ok(self.record(&[NONE, rf, bool_op]));
		}
		// `[rhs, 0 k-tree | 1 distance | 2 approximate, k, distance Name or ef
		// Int, Minkowski order]`
		let k = self.uint_lit()?;
		let mut rec = [NONE, 0, k, NONE, NONE];
		if self.eat(T::Comma) {
			let t = self.tok;
			if t.kind == T::Ident && crate::parse::stmt::is_distance(self.text_of(t)) {
				self.bump();
				rec[1] = 1;
				rec[3] = self.leaf(Tag::Name, 0, t);
				if self.text_of(t).eq_ignore_ascii_case("MINKOWSKI") {
					rec[4] = self.number_lit()?;
				}
			} else if t.kind == T::Digits {
				rec[1] = 2;
				rec[3] = self.uint_lit()?;
			} else {
				let end = self.syn_token_end(t);
				return Err(self.diag(
					Diag::message(
						t.start,
						end,
						"Unexpected token {found} expected a distance of an integer",
					)
					.with_label(
						"The NN operator accepts either a distance or an EF value (integer)",
					),
				));
			}
		}
		if !self.at(T::Pipe) || self.adjacent1().is_none_or(|t| t.kind != T::Gt) {
			// The error names the token after a consumed `|`.
			if self.at(T::Pipe) {
				self.bump();
			}
			let t = self.tok;
			let end = self.syn_token_end(t);
			return Err(self.diag(
				Diag::message(t.start, end, "Unexpected token `{found}` expected delimiter `|>`")
					.with_extra(open.start, open.end, "expected this delimiter to close"),
			));
		}
		self.bump();
		self.bump();
		Ok(self.record(&rec))
	}

	/// Returns a postfix range operator at the cursor: 0 for `..`, 1 for
	/// `>..`.
	pub(crate) fn postfix_op(&self) -> Option<u8> {
		if self.tok.kind == T::DotDot {
			if let Some(t) = self.adjacent1()
				&& (t.kind == T::Eq || starts_expression(&t))
			{
				None
			} else {
				Some(0)
			}
		} else if self.tok.kind == T::Gt {
			let t1 = self.adjacent1()?;
			if t1.kind != T::DotDot {
				return None;
			}
			let t2 = self.peek2();
			if !t2.ws() && (t2.kind == T::Eq || starts_expression(&t2)) {
				None
			} else {
				Some(1)
			}
		} else {
			None
		}
	}

	/// Returns the infix operator at the cursor and how many tokens after the
	/// first it spans.
	pub(crate) fn infix_op(&mut self) -> R<Option<(Binary, u8)>> {
		let t = self.tok;
		let op = if t.kind == T::OrOr {
			Binary::Or
		} else if t.kind == T::AndAnd {
			Binary::And
		} else if t.kind == T::QColon {
			Binary::Tco
		} else if t.kind == T::QQ {
			Binary::Nco
		} else if t.kind == T::Question {
			// A lone `?` binds as the start of `??`, so an expression never
			// ends at one.
			return Err(self.unexpected("`??`"));
		} else if t.kind == T::EqEq {
			Binary::ExactEq
		} else if t.kind == T::Ne {
			Binary::Ne
		} else if t.kind == T::AllEq {
			Binary::AllEq
		} else if t.kind == T::AnyEq {
			Binary::AnyEq
		} else if t.kind == T::Eq {
			Binary::Eq
		} else if t.kind == T::Le {
			Binary::Le
		} else if t.kind == T::Ge {
			Binary::Ge
		} else if t.kind == T::StarStar {
			Binary::Pow
		} else if t.kind == T::Plus {
			Binary::Add
		} else if t.kind == T::Minus {
			Binary::Sub
		} else if t.kind == T::Star || t.kind == T::Times {
			Binary::Mul
		} else if t.kind == T::Slash || t.kind == T::Divide {
			Binary::Div
		} else if t.kind == T::Percent {
			Binary::Rem
		} else if t.kind == T::Contains {
			Binary::Contain
		} else if t.kind == T::NotContains {
			Binary::NotContain
		} else if t.kind == T::Inside {
			Binary::Inside
		} else if t.kind == T::NotInside {
			Binary::NotInside
		} else if t.kind == T::ContainsAll {
			Binary::ContainAll
		} else if t.kind == T::ContainsAny {
			Binary::ContainAny
		} else if t.kind == T::ContainsNone {
			Binary::ContainNone
		} else if t.kind == T::AllInside {
			Binary::AllInside
		} else if t.kind == T::AnyInside {
			Binary::AnyInside
		} else if t.kind == T::NoneInside {
			Binary::NoneInside
		} else if t.kind == T::At {
			return Ok(Some((Binary::Matches, OPERATOR_PAYLOAD)));
		} else if t.kind == T::KnnOpen {
			return Ok(Some((Binary::Knn, OPERATOR_PAYLOAD)));
		} else if t.kind == T::Lt {
			if let Some(n) = self.adjacent1()
				&& (n.kind == T::Minus
					|| n.kind == T::Tilde
					|| n.kind == T::Arrow
					|| n.kind == T::DotDot)
			{
				return Ok(None);
			}
			Binary::Lt
		} else if t.kind == T::Gt {
			if self.adjacent1().is_some_and(|t| t.kind == T::DotDot) {
				if self.peek2().kind == T::Eq && !self.peek2().ws() {
					return Ok(Some((Binary::RangeSkipInclusive, 2)));
				}
				return Ok(Some((Binary::RangeSkip, 1)));
			}
			Binary::Gt
		} else if t.kind == T::DotDot {
			if self.adjacent1().is_some_and(|t| t.kind == T::Eq) {
				return Ok(Some((Binary::RangeInclusive, 1)));
			}
			Binary::Range
		} else if t.kind == T::Ident {
			if t.kw == Kw::Or {
				Binary::Or
			} else if t.kw == Kw::And {
				Binary::And
			} else if t.kw == Kw::Is {
				if self.peek1().is_kw(Kw::Not) {
					return Ok(Some((Binary::Ne, 1)));
				}
				Binary::Eq
			} else if t.kw == Kw::Contains {
				Binary::Contain
			} else if t.kw == Kw::ContainsNot {
				Binary::NotContain
			} else if t.kw == Kw::ContainsAll {
				Binary::ContainAll
			} else if t.kw == Kw::ContainsAny {
				Binary::ContainAny
			} else if t.kw == Kw::ContainsNone {
				Binary::ContainNone
			} else if t.kw == Kw::Inside || t.kw == Kw::In {
				Binary::Inside
			} else if t.kw == Kw::NotInside {
				Binary::NotInside
			} else if t.kw == Kw::AllInside {
				Binary::AllInside
			} else if t.kw == Kw::AnyInside {
				Binary::AnyInside
			} else if t.kw == Kw::NoneInside {
				Binary::NoneInside
			} else if t.kw == Kw::Outside {
				Binary::Outside
			} else if t.kw == Kw::Intersects {
				Binary::Intersects
			} else if t.kw == Kw::Not {
				if self.peek1().is_kw(Kw::In) {
					return Ok(Some((Binary::NotInside, 1)));
				}
				// `NOT` must be followed by `IN`.
				self.bump();
				return Err(self.unexpected("IN"));
			} else {
				return Ok(None);
			}
		} else {
			return Ok(None);
		};
		Ok(Some((op, 0)))
	}

	/// Whether an idiom continues at the cursor. For `<` the next token may
	/// follow whitespace, though a lookup needs it adjacent: `1 < -2` makes
	/// `1` an idiom with no parts, as earlier releases read it.
	pub(crate) fn continues_idiom(&self) -> bool {
		if self.tok.kind == T::Arrow
			|| self.tok.kind == T::LBracket
			|| self.tok.kind == T::Dot
			|| self.tok.kind == T::DotDotDot
		{
			true
		} else if self.tok.kind == T::Lt {
			let k = self.peek1().kind;
			k == T::Minus || k == T::Tilde || k == T::Arrow
		} else {
			false
		}
	}

	/// Whether a lookup arrow `<-`, `<->` or `<~` starts at the cursor.
	pub(crate) fn at_lookup_arrow(&self) -> bool {
		self.tok.kind == T::Lt
			&& self
				.adjacent1()
				.is_some_and(|t| t.kind == T::Minus || t.kind == T::Tilde || t.kind == T::Arrow)
	}

	/// Wants an operand. Pushes prefix frames and returns `None`, or returns
	/// a finished primary.
	pub(crate) fn operand(&mut self) -> R<Option<NodeId>> {
		let t = self.tok;
		if t.kind == T::Bang {
			self.bump();
			self.push_frame(Frame::Prefix {
				op: Prefix::Not,
				start: t.start,
			})?;
			Ok(None)
		} else if t.kind == T::Minus || t.kind == T::Plus {
			if let Some(n) = self.adjacent1()
				&& n.kind == T::Digits
			{
				self.bump();
				// The digits are read alone and the sign applied to them.
				return self.number_in(t.start, true, false, Compound::Numeric, n.start).map(Some);
			}
			self.bump();
			let op = if t.kind == T::Minus {
				Prefix::Neg
			} else {
				Prefix::Pos
			};
			self.push_frame(Frame::Prefix {
				op,
				start: t.start,
			})?;
			Ok(None)
		} else if t.kind == T::DotDot {
			self.bump();
			let n = self.tok;
			if n.kind == T::Eq && !n.ws() {
				self.bump();
				self.push_frame(Frame::Prefix {
					op: Prefix::RangeInclusive,
					start: t.start,
				})?;
				return Ok(None);
			}
			if n.ws() || !starts_prime(&n) {
				return Ok(Some(self.leaf(Tag::UnboundedRange, 0, t)));
			}
			self.push_frame(Frame::Prefix {
				op: Prefix::Range,
				start: t.start,
			})?;
			Ok(None)
		} else if t.kind == T::Lt {
			// `<-` starts a lookup, except `<-` and digits: a cast to a
			// negative literal kind, as earlier releases decide it.
			if let Some(n) = self.adjacent1()
				&& (n.kind == T::Minus || n.kind == T::Arrow || n.kind == T::Tilde)
				&& !(n.kind == T::Minus && self.peek2().kind == T::Digits)
			{
				return self.graph_start().map(Some);
			}
			self.bump();
			let kind = self.cast_kind()?;
			self.push_frame(Frame::Cast {
				kind,
				start: t.start,
			})?;
			Ok(None)
		} else {
			let v = self.primary()?;
			Ok((v != PUSHED).then_some(v))
		}
	}

	/// An idiom that starts with a graph arrow.
	pub(crate) fn graph_start(&mut self) -> R<NodeId> {
		let t = self.tok;
		let field = self.leaf_span(Tag::Field, 1, 0, 0, t.start, t.start);
		// The idiom loop picks up the arrow; an empty `Field` head with `op`
		// 1 marks an idiom without a start.
		Ok(field)
	}

	pub(crate) fn primary(&mut self) -> R<NodeId> {
		let t = self.tok;
		if t.kind == T::Digits {
			self.number(t.start, false)
		} else if t.kind == T::Str {
			self.bump();
			Ok(self.leaf(Tag::Str, t.aux(), t))
		} else if t.kind == T::Param {
			self.bump();
			Ok(self.leaf(Tag::Param, 0, t))
		} else if t.kind == T::NaN {
			self.bump();
			Ok(self.leaf(Tag::NaN, 0, t))
		} else if t.kind == T::Infinity {
			self.bump();
			Ok(self.leaf(Tag::Infinity, 0, t))
		} else if t.kind == T::Slash {
			let mut l = self.lex;
			if let Err(e) = l.regex(t.start) {
				self.lex.pos = l.pos;
				return Err(self.lex_error(t.start, e));
			}
			let end = l.pos;
			if let Some(compiles) = self.limits.regex
				&& !compiles(self.text, t.start, end)
			{
				return Err(self.error(t.start, end, "{regex_error}"));
			}
			self.reset_to(end);
			self.prev_start = t.start;
			self.prev_end = end;
			Ok(self.leaf_span(Tag::Regex, 0, 0, 0, t.start, end))
		} else if t.kind == T::LBracket {
			self.bump();
			if self.at(T::RBracket) {
				// An empty array costs the level a full one does.
				let b = self.budgets();
				self.enter_o()?;
				self.enter_e()?;
				self.set_budgets(b);
				self.bump();
				let idx = self.empty_list();
				return self.node(Tag::Array, 0, idx, 0, t.start, 0);
			}
			self.push_frame(Frame::Array {
				start: t.start,
				mark: self.vals.len() as u32,
				wrap: Wrap::None,
			})?;
			self.operand_continue()
		} else if t.kind == T::LBrace {
			self.object_open(t.start, Wrap::None)
		} else if t.kind == T::LParen {
			self.bump();
			let n = self.tok;
			if (n.kind == T::Plus
				|| n.kind == T::Minus
				|| n.kind == T::NaN
				|| n.kind == T::Infinity
				|| n.kind == T::Digits)
				&& let Some(p) = self.point(t.start)?
			{
				return Ok(p);
			}
			self.push_frame(Frame::Paren {
				start: t.start,
			})?;
			self.operand_continue()
		} else if t.kind == T::Arrow {
			self.graph_start()
		} else if t.kind == T::Ident {
			self.ident_primary(t)
		} else if t.kind == T::EscIdent {
			let pk = self.peek1().kind;
			if pk == T::Colon {
				self.record_id(t, true)
			} else if pk == T::PathSep || pk == T::LParen {
				// As for a plain identifier, `::` or `(` makes a builtin path.
				self.path_call(t.start, false)
			} else {
				self.bump();
				Ok(self.bare_ident(t))
			}
		} else if t.kind == T::Pipe {
			self.bump();
			if self.at(T::Param) {
				self.closure(t.start)
			} else {
				self.mock(t.start)
			}
		} else if t.kind == T::OrOr {
			self.bump();
			let args = self.empty_list();
			self.closure_body(t.start, args)
		} else if t.kind == T::At {
			self.bump();
			let d = self.leaf(Tag::Doc, 0, t);
			if self.continues_idiom() {
				return Ok(d);
			}
			// `@field`: the first part follows without a `.`.
			self.push_frame(Frame::Idiom {
				start: t.start,
				mark: self.vals.len() as u32,
				head: d,
			})?;
			match self.dot_part()? {
				PartStep::Pushed => Ok(PUSHED),
				PartStep::Done | PartStep::Finished => Ok(d),
			}
		} else {
			Err(self.unexpected("an expression"))
		}
	}

	/// `|$a: kind, …| …` after the first `|`.
	fn closure(&mut self, start: u32) -> R<NodeId> {
		let mark = self.vals.len() as u32;
		loop {
			if self.eat(T::Pipe) {
				break;
			}
			let p = self.expect(T::Param, "a parameter")?;
			let p = self.leaf(Tag::Param, 0, p);
			self.vals.push(p);
			let k = if self.eat(T::Colon) {
				if self.eat(T::Lt) {
					self.cast_kind()?
				} else {
					self.single_kind()?
				}
			} else {
				NONE
			};
			self.vals.push(k);
			if !self.eat(T::Comma) {
				self.close(T::Pipe, start)?;
				break;
			}
		}
		let args = self.list_from_raw(mark);
		self.closure_body(start, args)
	}

	/// A closure's body after its arguments: `-> kind { … }` or an
	/// expression. Pushes the closure frame and wants the body.
	fn closure_body(&mut self, start: u32, args: u32) -> R<NodeId> {
		if self.eat(T::Arrow) {
			let returns = self.kind()?;
			let open = self.expect(T::LBrace, "{")?;
			self.push_frame(Frame::Closure {
				start,
				args,
				returns,
			})?;
			return match self.block_open(open.start)? {
				PUSHED => Ok(PUSHED),
				empty => {
					// An empty block: the closure is complete.
					let Some(Frame::Closure {
						..
					}) = self.pop_frame()
					else {
						unreachable!("the closure frame was just pushed")
					};
					let d = self.depth_of(empty).max(self.depth_of(returns));
					let rec = self.record(&[args, returns, empty]);
					self.node(Tag::Closure, 0, rec, 0, start, d)
				}
			};
		}
		self.push_frame(Frame::Closure {
			start,
			args,
			returns: NONE,
		})?;
		Ok(PUSHED)
	}

	/// `|table:count|` or `|table:range|` after the first `|`.
	fn mock(&mut self, start: u32) -> R<NodeId> {
		let table = self.name()?;
		self.expect(T::Colon, ":")?;
		// `[from, to, flags]`: flags bit 0 a range, bit 1 start excluded, bit 2
		// end included.
		let mut rec = [NONE, NONE, 0];
		if self.eat(T::DotDot) {
			rec[2] = 1;
		} else {
			rec[0] = self.int_lit()?;
			if self.eat(T::DotDot) {
				rec[2] = 1;
			} else if self.eat(T::Gt) {
				self.expect(T::DotDot, "..")?;
				rec[2] = 3;
			} else {
				self.close(T::Pipe, start)?;
				let idx = self.record(&rec);
				return self.node(Tag::Mock, 0, table, idx, start, 1);
			}
		}
		if !self.eat(T::Pipe) {
			if self.eat(T::Eq) {
				rec[2] |= 4;
			}
			rec[1] = self.int_lit()?;
			self.close(T::Pipe, start)?;
		}
		let idx = self.record(&rec);
		self.node(Tag::Mock, 0, table, idx, start, 1)
	}

	/// `(x, y)` after `(`: a geometry point, when two numbers separated by a
	/// comma follow. Anything else is restored for a parenthesised expression.
	fn point(&mut self, start: u32) -> R<Option<NodeId>> {
		let cp = self.checkpoint();
		let Ok(x) = self.float_lit() else {
			self.restore(cp);
			return Ok(None);
		};
		if !self.eat(T::Comma) {
			self.restore(cp);
			return Ok(None);
		}
		let y = self.float_lit()?;
		self.close(T::RParen, start)?;
		self.node(Tag::Point, 0, x, y, start, 1).map(Some)
	}

	/// `ext::…(…)` (or its alias `mod::…(…)`), `silo::org::pkg::<1.2.3>[::…](…)`
	/// or `ml::name<1.2.3>(…)`
	/// at its first keyword: a call whose callee is a `Name` with `op` 2, 3 or
	/// 4 and `a` an index into `extra` of its parts.
	fn special_call(&mut self, t: Tok) -> R<NodeId> {
		let callee = self.special_callee(t)?;
		let op = self.ast.nodes[callee as usize].op;
		self.expect(T::LParen, "(")?;
		if self.eat(T::RParen) {
			let idx = self.empty_list();
			return self.node(Tag::Call, op, callee, idx, t.start, 1);
		}
		self.push_frame(Frame::Args {
			start: t.start,
			open: self.prev_end - 1,
			mark: self.vals.len() as u32,
			callee,
			tag: Tag::Call,
		})?;
		Ok(PUSHED)
	}

	/// The callee of [`Self::special_call`]: a `Name` with `op` 2 (`ext` or
	/// `mod`), 3 (`silo`) or 4 (`ml`) whose `a` indexes its parts in `extra`
	/// and whose `b` holds the [`ext`](crate::ast::ext) bits of a deprecated
	/// spelling.
	fn special_callee(&mut self, t: Tok) -> R<NodeId> {
		self.bump();
		if self.limits.no_surrealism {
			if t.kw == Kw::Ext {
				return Err(self.surrealism_off(t, "Use of `ext::` requires Surrealism extensions"));
			} else if t.kw == Kw::Mod {
				return Err(self.surrealism_off(t, "Use of `mod::` requires Surrealism extensions"));
			} else if t.kw == Kw::Silo {
				return Err(
					self.surrealism_off(t, "Use of `silo::` requires Surrealism extensions")
				);
			}
		}
		self.expect(T::PathSep, "::")?;
		let path = |p: &mut Self| -> R<NodeId> {
			let first = p.ident()?;
			let mut end = first.end;
			while p.eat(T::PathSep) {
				end = p.ident()?.end;
			}
			Ok(p.leaf_span(Tag::Name, 0, 0, 0, first.start, end))
		};
		let mark = self.vals.len() as u32;
		let mut deprecated = 0;
		let op = if t.kw == Kw::Ext || t.kw == Kw::Mod {
			if t.kw == Kw::Mod {
				deprecated = crate::ast::ext::MOD_PREFIX;
			}
			let name = self.name()?;
			self.vals.push(name);
			let sub = if self.eat(T::PathSep) {
				path(self)?
			} else {
				NONE
			};
			self.vals.push(sub);
			2
		} else if t.kw == Kw::Silo {
			let org = self.name()?;
			self.vals.push(org);
			self.expect(T::PathSep, "::")?;
			let pkg = self.name()?;
			self.vals.push(pkg);
			self.silo_version()?;
			let sub = if self.eat(T::PathSep) {
				path(self)?
			} else {
				NONE
			};
			self.vals.push(sub);
			deprecated = crate::ast::ext::SILO_NAME;
			3
		} else {
			let name = path(self)?;
			self.vals.push(name);
			self.model_version()?;
			4
		};
		let parts = self.list_from_raw(mark);
		Ok(self.leaf_span(Tag::Name, op, parts, deprecated, t.start, self.prev_end))
	}

	/// A function name without arguments, read as the function-name entry
	/// point reads it: a `Name` with `op` 0 (a builtin path, not checked against
	/// the registry), 1 (`fn::…`) or that of [`Self::special_callee`].
	pub(crate) fn function_name(&mut self) -> R<NodeId> {
		let t = self.tok;
		let custom = if t.kw == Kw::Fn {
			self.bump();
			self.expect(T::PathSep, "::")?;
			true
		} else if (t.kw == Kw::Ext || t.kw == Kw::Mod || t.kw == Kw::Silo || t.kw == Kw::Ml)
			&& t.is_ident()
		{
			return self.special_callee(t);
		} else if t.is_ident() {
			false
		} else {
			return Err(self.unexpected("a function name"));
		};
		let first = self.ident()?;
		let mut end = first.end;
		while self.eat(T::PathSep) {
			end = self.ident()?.end;
		}
		Ok(self.leaf_span(Tag::Name, custom as u8, 0, 0, first.start, end))
	}

	/// `function(args) { javascript }` at `function`.
	fn js_function(&mut self, t: Tok) -> R<NodeId> {
		self.bump();
		self.expect(T::LParen, "(")?;
		if self.eat(T::RParen) {
			let args = self.empty_list();
			return self.script_body(t.start, args, 0);
		}
		self.push_frame(Frame::Args {
			start: t.start,
			open: self.prev_end - 1,
			mark: self.vals.len() as u32,
			callee: NONE,
			tag: Tag::Script,
		})?;
		Ok(PUSHED)
	}

	/// The JavaScript body after a script's arguments, as a [`Tag::Script`]
	/// node whose `a` is a `Name` spanning the body inside its braces.
	pub(crate) fn script_body(&mut self, start: u32, args: u32, depth: u16) -> R<NodeId> {
		let open = self.tok;
		if open.kind != T::LBrace {
			return Err(self.unexpected("{"));
		}
		let mut l = self.lex;
		let end = match l.js_body(open.start) {
			Ok(end) => end,
			Err(e) => {
				self.lex.pos = l.pos;
				return Err(self.lex_error(open.start, e));
			}
		};
		self.reset_to(end);
		self.prev_start = open.start;
		self.prev_end = end;
		let body = self.leaf_span(Tag::Name, 0, 0, 0, open.start + 1, end - 1);
		self.node(Tag::Script, 0, body, args, start, depth)
	}

	/// Signals the engine that a frame was pushed and an operand is wanted.
	/// The engine re-enters `operand`, so this returns the marker the caller
	/// propagates.
	pub(crate) fn operand_continue(&mut self) -> R<NodeId> {
		Ok(PUSHED)
	}

	pub(crate) fn bare_ident(&mut self, t: Tok) -> NodeId {
		let tag = if self.table_as_field {
			Tag::Field
		} else {
			Tag::Table
		};
		self.leaf(tag, 0, t)
	}

	pub(crate) fn ident_primary(&mut self, t: Tok) -> R<NodeId> {
		if t.kw == Kw::None_ {
			self.bump();
			return Ok(self.leaf(Tag::None, 0, t));
		} else if t.kw == Kw::Null {
			self.bump();
			return Ok(self.leaf(Tag::Null, 0, t));
		} else if t.kw == Kw::True {
			self.bump();
			return Ok(self.leaf(Tag::True, 0, t));
		} else if t.kw == Kw::False {
			self.bump();
			return Ok(self.leaf(Tag::False, 0, t));
		} else if is_statement_keyword(self, t) {
			return self.nested_statement(t);
		} else if t.kw == Kw::Fn && self.peek1().kind == T::PathSep {
			self.bump();
			return self.path_call(t.start, true);
		} else if (t.kw == Kw::Ext || t.kw == Kw::Mod || t.kw == Kw::Silo || t.kw == Kw::Ml)
			&& self.peek1().kind == T::PathSep
		{
			return self.special_call(t);
		} else if t.kw == Kw::Function {
			return self.js_function(t);
		}
		let n = self.peek1();
		if n.kind == T::PathSep || n.kind == T::LParen {
			self.path_call(t.start, false)
		} else if n.kind == T::Colon {
			self.record_id(t, true)
		} else {
			self.bump();
			Ok(self.bare_ident(t))
		}
	}

	/// `a::b::c(args)` or the constant `a::b::c`, at its first segment.
	/// `custom` is set after `fn` was consumed and `::` is next.
	pub(crate) fn path_call(&mut self, start: u32, custom: bool) -> R<NodeId> {
		let any_path = core::mem::take(&mut self.any_path);
		if custom {
			self.bump();
		}
		let first = self.ident()?;
		let mut end = first.end;
		while self.at(T::PathSep) {
			self.bump();
			end = self.ident()?.end;
		}
		// Without the hook, the consumer checks the path.
		let kind = if let Some(kind) = self.limits.builtin
			&& !custom
			&& !any_path
		{
			Some(kind(self.text, first.start, end))
		} else {
			None
		};
		if kind == Some(PathKind::Unknown) {
			return Err(self.error(first.start, end, "Invalid function/constant path{suggest}"));
		}
		let name = self.leaf_span(Tag::Name, custom as u8, 0, 0, first.start, end);
		if !self.at(T::LParen) {
			if custom || kind == Some(PathKind::Function) {
				return Err(self.unexpected("("));
			}
			return Ok(self.leaf_span(Tag::Path, 0, 0, 0, start, end));
		}
		self.bump();
		if self.eat(T::RParen) {
			let idx = self.empty_list();
			return self.node(Tag::Call, custom as u8, name, idx, start, 1);
		}
		self.push_frame(Frame::Args {
			start,
			open: self.prev_end - 1,
			mark: self.vals.len() as u32,
			callee: name,
			tag: Tag::Call,
		})?;
		self.operand_continue()
	}

	/// After `{`: an object, or a block this parser does not model.
	pub(crate) fn object_open(&mut self, start: u32, wrap: Wrap) -> R<NodeId> {
		self.bump();
		if self.eat(T::RBrace) {
			let idx = self.empty_list();
			let v = self.node(Tag::Object, 0, idx, 0, start, 0)?;
			return match self.wrap(v, wrap)? {
				Some(v) => Ok(v),
				None => Ok(PUSHED),
			};
		}
		// Where only an object can follow, its first key is read as one,
		// so a key without its `:` fails at the `:`, as it always has.
		if self.at_object_key() || wrap != Wrap::None {
			self.push_frame(Frame::Object {
				start,
				mark: self.vals.len() as u32,
				wrap,
			})?;
			self.object_key()?;
			return Ok(PUSHED);
		}
		if self.eat(T::Comma) {
			// `{,}`: the empty set.
			self.close(T::RBrace, start)?;
			let idx = self.empty_list();
			return self.node(Tag::Set, 0, idx, 0, start, 0);
		}
		if self.at(T::Semi) {
			return self.block_open(start);
		}
		self.push_frame(Frame::BlockOrSet {
			start,
			mark: self.vals.len() as u32,
			covered: self.at(T::LParen),
		})?;
		Ok(PUSHED)
	}

	/// The statements of a block after its `{`. Returns the empty block, or
	/// pushes a `Block` frame and returns [`PUSHED`].
	pub(crate) fn block_open(&mut self, start: u32) -> R<NodeId> {
		while self.eat(T::Semi) {}
		if self.eat(T::RBrace) {
			let idx = self.empty_list();
			return self.node(Tag::Block, 0, idx, 0, start, 0);
		}
		self.push_frame(Frame::Block {
			start,
			mark: self.vals.len() as u32,
			covered: self.at(T::LParen),
		})?;
		Ok(PUSHED)
	}

	/// A block, at its `{`, and nothing after it: for statements whose
	/// grammar requires one.
	pub(crate) fn block(&mut self) -> R<NodeId> {
		let t = self.expect(T::LBrace, "{")?;
		let base = self.frames.len();
		self.push_frame(Frame::Atom {
			table_as_field: self.table_as_field,
		})?;
		let res = match self.block_open(t.start) {
			Ok(PUSHED) => self.run(base, NONE),
			Ok(v) => {
				self.pop_frame();
				Ok(v)
			}
			Err(e) => Err(e),
		};
		if res.is_err() {
			self.truncate_frames(base);
		}
		res
	}

	/// Whether an object entry starts here: an object key and then `:`.
	fn at_object_key(&self) -> bool {
		let k = self.tok;
		if k.kind == T::Ident || k.kind == T::EscIdent || (k.kind == T::Str && k.aux() == 0) {
			self.peek1().kind == T::Colon
		} else if k.kind == T::Digits {
			let mut l = self.lex;
			match l.numeric(k.start) {
				Ok(NumKind::Duration) | Err(_) => false,
				Ok(_) => {
					let next = l.next_token();
					self.note(next);
					next.kind == T::Colon
				}
			}
		} else {
			false
		}
	}

	/// An object key and its `:`; pushes the key `Name`.
	pub(crate) fn object_key(&mut self) -> R<()> {
		let k = self.tok;
		if !(k.kind == T::Ident || k.kind == T::EscIdent || k.kind == T::Str || k.kind == T::Digits)
			|| k.aux() != 0
		{
			return Err(self.unexpected("an object key"));
		}
		let n = if k.kind == T::Digits {
			// A number key keeps its source text; it must be a valid number.
			let num = self.number_in(k.start, false, false, Compound::Number, k.start)?;
			let nn = self.ast.nodes[num as usize];
			if nn.tag == Tag::Duration {
				// The key is read as a number, which a duration is not.
				return Err(self.number_error(
					k.start,
					LexError::InvalidNumber,
					Compound::Number,
					k.start,
				));
			}
			self.ast.nodes.pop();
			self.leaf_span(Tag::Name, 0, 0, 0, nn.start, nn.end)
		} else {
			self.bump();
			let op = if k.kind == T::EscIdent {
				1
			} else if k.kind == T::Str {
				2
			} else {
				0
			};
			self.leaf(Tag::Name, op, k)
		};
		self.vals.push(n);
		self.expect(T::Colon, ":")?;
		Ok(())
	}

	/// A malformed numeric literal at `start`: the compound-literal error
	/// when there is one, else the lexer's own.
	pub(crate) fn number_error(
		&mut self,
		start: u32,
		e: LexError,
		mode: Compound,
		origin: u32,
	) -> Stop {
		match crate::lex::compound_error(self.src, origin, mode) {
			Some(d) => {
				if d.end + 4 > self.src.len() as u32 {
					self.saw_end.set(true);
				}
				self.diag(d)
			}
			None => self.lex_error(start, e),
		}
	}

	/// A numeric literal at `start` (a sign, when `signed`, then digits).
	pub(crate) fn number(&mut self, start: u32, signed: bool) -> R<NodeId> {
		let mode = if signed {
			Compound::Number
		} else {
			Compound::Numeric
		};
		self.number_in(start, signed, false, mode, start)
	}

	/// [`Self::number`], also accepting a positive integer up to `u64::MAX`
	/// when `unsigned`: such an integer is an `Int` node with `op` 1, read with
	/// [`crate::ast::Ast::uint`].
	pub(crate) fn number_with(&mut self, start: u32, signed: bool, unsigned: bool) -> R<NodeId> {
		self.number_in(start, signed, unsigned, Compound::Integer, start)
	}

	/// [`Self::number_with`], reporting a malformed literal with the
	/// established error for reading it with `mode` from `origin`.
	pub(crate) fn number_in(
		&mut self,
		start: u32,
		signed: bool,
		unsigned: bool,
		mode: Compound,
		origin: u32,
	) -> R<NodeId> {
		let mut l = self.lex;
		let kind = match l.numeric(start) {
			Ok(k) => k,
			Err(e) => {
				self.lex.pos = l.pos;
				return Err(self.number_error(start, e, mode, origin));
			}
		};
		let end = l.pos;
		self.reset_to(end);
		self.prev_start = start;
		self.prev_end = end;
		let text = &self.src[start as usize..end as usize];
		let neg = text[0] == b'-';
		let body = if signed {
			&text[1..]
		} else {
			text
		};
		match kind {
			NumKind::Int => {
				let Some(u) = crate::lex::decode_uint(body) else {
					return Err(self.number_error(start, LexError::IntOverflow, mode, origin));
				};
				let v = if neg {
					if u > i64::MAX as u64 + 1 {
						return Err(self.number_error(start, LexError::IntOverflow, mode, origin));
					}
					(u as i64).wrapping_neg()
				} else {
					if u > i64::MAX as u64 {
						if unsigned {
							return Ok(self.leaf_span(
								Tag::Int,
								1,
								u as u32,
								(u >> 32) as u32,
								start,
								end,
							));
						}
						return Err(self.number_error(start, LexError::IntOverflow, mode, origin));
					}
					u as i64
				};
				let v = v as u64;
				Ok(self.leaf_span(Tag::Int, 0, v as u32, (v >> 32) as u32, start, end))
			}
			NumKind::I8
			| NumKind::U8
			| NumKind::I16
			| NumKind::U16
			| NumKind::I32
			| NumKind::U32
			| NumKind::I64
			| NumKind::U64
			| NumKind::Float
			| NumKind::F16
			| NumKind::F32
			| NumKind::F64 => {
				use crate::lex::DecodedNumber;
				let Some(dec) = crate::lex::decode_number(kind, text) else {
					let err = match kind {
						NumKind::Float | NumKind::F16 | NumKind::F32 | NumKind::F64 => {
							LexError::InvalidNumber
						}
						NumKind::Int
						| NumKind::I8
						| NumKind::U8
						| NumKind::I16
						| NumKind::U16
						| NumKind::I32
						| NumKind::U32
						| NumKind::I64
						| NumKind::U64
						| NumKind::Decimal
						| NumKind::BigDecimal
						| NumKind::Duration => LexError::IntOverflow,
					};
					return Err(self.number_error(start, err, mode, origin));
				};
				Ok(match dec {
					DecodedNumber::I8(v) => {
						self.leaf_span(Tag::I8, 0, v as u8 as u32, 0, start, end)
					}
					DecodedNumber::U8(v) => self.leaf_span(Tag::U8, 0, v as u32, 0, start, end),
					DecodedNumber::I16(v) => {
						self.leaf_span(Tag::I16, 0, v as u16 as u32, 0, start, end)
					}
					DecodedNumber::U16(v) => self.leaf_span(Tag::U16, 0, v as u32, 0, start, end),
					DecodedNumber::I32(v) => self.leaf_span(Tag::I32, 0, v as u32, 0, start, end),
					DecodedNumber::U32(v) => self.leaf_span(Tag::U32, 0, v, 0, start, end),
					DecodedNumber::Int(v) => {
						let v = v as u64;
						self.leaf_span(Tag::Int, 0, v as u32, (v >> 32) as u32, start, end)
					}
					DecodedNumber::U64(u) => {
						self.leaf_span(Tag::U64, 0, u as u32, (u >> 32) as u32, start, end)
					}
					DecodedNumber::Float(f) => {
						let bits = f.to_bits();
						self.leaf_span(Tag::Float, 0, bits as u32, (bits >> 32) as u32, start, end)
					}
					DecodedNumber::F16(h) => {
						self.leaf_span(Tag::F16, 0, h.to_bits() as u32, 0, start, end)
					}
					DecodedNumber::F32(f) => {
						self.leaf_span(Tag::F32, 0, f.to_bits(), 0, start, end)
					}
				})
			}
			NumKind::Decimal => Ok(self.leaf_span(Tag::Decimal, 0, 0, 0, start, end)),
			NumKind::BigDecimal => {
				Ok(self.leaf_span(Tag::Decimal, ast::DECIMAL_BIG, 0, 0, start, end))
			}
			NumKind::Duration => {
				if crate::lex::decode_duration(body).is_none() {
					return Err(self.number_error(start, LexError::InvalidDuration, mode, origin));
				}
				let lit = self.leaf_span(Tag::Duration, 0, 0, 0, start + signed as u32, end);
				if signed {
					let op = if neg {
						Prefix::Neg
					} else {
						Prefix::Pos
					};
					return self.node(Tag::Prefix, op as u8, lit, 0, start, 1);
				}
				Ok(lit)
			}
		}
	}
}

pub(crate) enum PartStep {
	/// A part was appended.
	Done,
	/// A frame was pushed; an operand is wanted.
	Pushed,
	/// No more parts.
	Finished,
}
