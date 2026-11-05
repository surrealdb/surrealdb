use super::*;

impl<'a, 's> Parser<'a, 's> {
	/// An idiom that starts with a field name or a lookup: `a.b[0]`,
	/// `->edge->node`. Takes every idiom part, but no operator after it.
	pub(crate) fn plain_idiom(&mut self) -> R<NodeId> {
		let head = if self.tok.kind == T::Arrow {
			self.graph_start()?
		} else if self.tok.kind == T::Lt {
			if !self
				.adjacent1()
				.is_some_and(|n| n.kind == T::Minus || n.kind == T::Arrow || n.kind == T::Tilde)
			{
				return Err(self.unexpected("either `<-` `<->` or `->`"));
			}
			self.graph_start()?
		} else {
			let t = self.ident()?;
			self.leaf(Tag::Field, 0, t)
		};
		self.path_from(head)
	}

	/// The idiom parts after `head`, and nothing else.
	pub(crate) fn path_from(&mut self, head: NodeId) -> R<NodeId> {
		let base = self.frames.len();
		self.push_frame(Frame::Path {
			table_as_field: self.table_as_field,
		})?;
		let res = self.run(base, head);
		if res.is_err() {
			self.truncate_frames(base);
		}
		res
	}

	/// The `GROUP BY` / `ORDER BY` idiom: fields, `.*`, `[n]`, `[$]`, `[*]`.
	pub(crate) fn basic_idiom(&mut self) -> R<NodeId> {
		let t = self.ident()?;
		let head = self.leaf(Tag::Field, 0, t);
		self.idiom_tail(head, t.start)
	}

	/// The parts of a basic idiom: `.field`, `.*`, `[*]`, `[$]` and `[n]`.
	pub(crate) fn idiom_tail(&mut self, head: NodeId, start: u32) -> R<NodeId> {
		let mark = self.vals.len() as u32;
		let e = self.e;
		loop {
			if self.tok.kind == T::Dot || self.tok.kind == T::LBracket {
				self.enter_e()?;
			}
			if self.tok.kind == T::Dot {
				self.bump();
				let p = if self.at(T::Star) {
					let t = self.bump();
					self.leaf(Tag::PartAll, 0, t)
				} else if self.tok.is_ident() {
					let t = self.bump();
					self.leaf(Tag::PartField, 0, t)
				} else {
					return Err(self.unexpected("an identifier"));
				};
				self.vals.push(p);
			} else if self.tok.kind == T::LBracket {
				let open = self.bump();
				let p = if self.tok.kind == T::Star {
					self.bump();
					self.leaf_span(Tag::PartAll, 0, 0, 0, open.start, self.tok.end)
				} else if self.tok.kind == T::Dollar {
					self.bump();
					self.leaf_span(Tag::PartLast, 0, 0, 0, open.start, self.tok.end)
				} else if self.tok.kind == T::Digits || self.tok.kind == T::Plus {
					let v = self.number_lit()?;
					if self.ast.nodes[v as usize].tag == Tag::NaN {
						return Err(self.unexpected("$, * or a number"));
					}
					self.node(Tag::PartIndex, 0, v, 0, open.start, 1)?
				} else if self.tok.kind == T::Minus {
					let t = self.tok;
					if let Some(d) = self.adjacent1()
						&& d.kind == T::Digits
					{
						return Err(self.diag(
							Diag::message(
								t.start,
								d.end,
								"Unexpected token `-` expected $, *, or a number",
							)
							.with_label("an index in a basic idiom can't be negative"),
						));
					}
					return Err(self.unexpected("$, * or a number"));
				} else {
					return Err(self.unexpected("$, * or a number"));
				};
				self.close(T::RBracket, open.start)?;
				self.vals.push(p);
			} else {
				break;
			}
		}
		self.e = e;
		if self.vals.len() as u32 == mark {
			return Ok(head);
		}
		let n = self.vals.len() as u16 - mark as u16;
		let (parts, d) = self.list_from(mark);
		self.node(Tag::Idiom, 0, head, parts, start, d.max(self.depth_of(head)) + n)
	}

	/// One step of the idiom part loop, with an `Idiom` frame on top.
	pub(crate) fn idiom_part(&mut self) -> R<PartStep> {
		let t = self.tok;
		if t.kind == T::DotDotDot {
			self.bump();
			let p = self.leaf(Tag::PartFlatten, 0, t);
			self.vals.push(p);
		} else if t.kind == T::Dot {
			self.bump();
			return self.dot_part();
		} else if t.kind == T::LBracket {
			self.bump();
			let n = self.tok;
			if n.kind == T::Star || n.kind == T::Dollar {
				self.bump();
				self.close(T::RBracket, t.start)?;
				let tag = if n.kind == T::Star {
					Tag::PartAll
				} else {
					Tag::PartLast
				};
				let p = self.leaf_span(tag, 0, 0, 0, t.start, self.prev_end);
				self.vals.push(p);
			} else {
				let tag = if n.kind == T::Question || n.is_kw(Kw::Where) {
					self.bump();
					Tag::PartWhere
				} else {
					Tag::PartIndex
				};
				self.push_frame(Frame::Bracket {
					start: t.start,
					tag,
					table_as_field: self.table_as_field,
				})?;
				if tag == Tag::PartWhere {
					self.table_as_field = true;
				}
				return Ok(PartStep::Pushed);
			}
		} else if t.kind == T::Arrow {
			self.bump();
			let p = self.graph(0, t.start)?;
			self.vals.push(p);
		} else if t.kind == T::Lt && self.at_lookup_arrow() {
			self.bump();
			let dir = if self.tok.kind == T::Minus {
				1
			} else if self.tok.kind == T::Arrow {
				2
			} else {
				3
			};
			self.bump();
			let p = self.graph(dir, t.start)?;
			self.vals.push(p);
		} else {
			return Ok(PartStep::Finished);
		}
		Ok(PartStep::Done)
	}

	/// The part after a `.` (consumed), with an `Idiom` frame on top.
	pub(crate) fn dot_part(&mut self) -> R<PartStep> {
		let n = self.tok;
		if n.kind == T::Star {
			self.bump();
			let p = self.leaf(Tag::PartAll, 0, n);
			self.vals.push(p);
		} else if n.kind == T::Question {
			self.bump();
			let p = self.leaf(Tag::PartOptional, 0, n);
			self.vals.push(p);
		} else if n.kind == T::QQ {
			// Here `??` is two tokens, as it has always read: the first
			// `?` is the part and the second is the next token.
			let at = n.start + 1;
			let p = self.leaf_span(Tag::PartOptional, 0, 0, 0, n.start, at);
			self.reset_to(at);
			self.prev_start = n.start;
			self.prev_end = at;
			self.vals.push(p);
		} else if n.kind == T::Ident || n.kind == T::EscIdent {
			self.bump();
			if self.at(T::LParen) {
				self.bump();
				let name = self.leaf(Tag::Name, 0, n);
				if self.eat(T::RParen) {
					let idx = self.empty_list();
					let p = self.node(Tag::PartMethod, 0, name, idx, n.start, 1)?;
					self.vals.push(p);
				} else {
					self.push_frame(Frame::Args {
						start: n.start,
						open: self.prev_end - 1,
						mark: self.vals.len() as u32,
						callee: name,
						tag: Tag::PartMethod,
					})?;
					return Ok(PartStep::Pushed);
				}
			} else {
				let p = self.leaf(Tag::PartField, 0, n);
				self.vals.push(p);
			}
		} else if n.kind == T::At {
			self.bump();
			let p = self.leaf(Tag::PartRepeat, 0, n);
			self.vals.push(p);
		} else if n.kind == T::LBrace {
			self.bump();
			let p = if self.tok.kind == T::Star
				|| self.tok.kind == T::DotDot
				|| self.tok.kind == T::Digits
			{
				self.recurse_part(n.start)?
			} else {
				self.nested(|p| p.destructure(n.start))?
			};
			self.vals.push(p);
		} else {
			return Err(self.unexpected("an identifier"));
		}
		Ok(PartStep::Done)
	}

	/// `.{min..max +instruction}(nested idiom)` after `.{`.
	fn recurse_part(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		// `[kind 0 fixed | 1 range, min, max, instruction 0 none | 1 path | 2
		// collect | 3 shortest, expects, inclusive, nest]`
		let mut rec = [0, NONE, NONE, 0, NONE, 0, NONE];
		if self.at(T::Digits) {
			rec[1] = self.uint_lit()?;
		}
		if self.at(T::DotDot) && !self.tok.ws() {
			self.bump();
			rec[0] = 1;
			if self.at(T::Digits) && !self.tok.ws() {
				rec[2] = self.uint_lit()?;
			}
		} else if rec[1] == NONE {
			let t = self.tok;
			let end = self.syn_token_end(t);
			return Err(self.error(
				t.start,
				end,
				"Unexpected token `{found}` expected an integer or ..",
			));
		}
		if self.eat(T::Plus) {
			let k = self.ident()?;
			let word = |w: &str| self.ident_is(k, w, true);
			rec[3] = if word("path") {
				1
			} else if word("collect") {
				2
			} else if word("shortest") {
				3
			} else {
				return Err(self.error(
					k.start,
					k.end,
					"Unexpected instruction `{text}` expected `path`, `collect`, or `shortest`",
				));
			};
			if rec[3] == 3 {
				self.expect(T::Eq, "=")?;
				let t = self.tok;
				rec[4] = if t.kind == T::Param {
					self.bump();
					self.leaf(Tag::Param, 0, t)
				} else if t.is_ident() {
					self.record_id_only()?
				} else {
					return Err(self.unexpected("a param or record-id"));
				};
			}
			while self.eat(T::Plus) {
				let o = self.ident()?;
				if !self.ident_is(o, "inclusive", true) {
					return Err(self.error(
						o.start,
						o.end,
						"Unexpected option `{text}` expected `inclusive`",
					));
				}
				rec[5] = 1;
			}
		}
		self.close(T::RBrace, start)?;
		if self.at(T::LParen) {
			rec[6] = self.nested(|p| {
				let open = p.bump().start;
				let head = p.leaf_span(Tag::Field, 1, 0, 0, p.tok.start, p.tok.start);
				let i = p.path_from(head)?;
				p.close(T::RParen, open)?;
				Ok(i)
			})?;
		}
		let idx = self.record(&rec);
		self.stmt_node(Tag::PartRecurse, 0, idx, 0, start, first)
	}

	/// `.{ field, field: expr, field.*, field.{…}, … }` after `.{`, as a
	/// nested statement (each level is native recursion).
	fn destructure(&mut self, start: u32) -> R<NodeId> {
		self.enter_e()?;
		let first = self.ast.nodes.len();
		let mark = self.vals.len() as u32;
		loop {
			if self.eat(T::RBrace) {
				break;
			}
			let t = self.tok;
			if self.eat(T::DotDotDot) {
				let e = self.leaf(Tag::DestructEntry, 4, t);
				self.vals.push(e);
				self.close(T::RBrace, start)?;
				break;
			}
			let f = self.ast.nodes.len();
			let name = self.name()?;
			let (op, b) = if self.tok.kind == T::Colon {
				self.bump();
				(1, self.expr(true)?)
			} else if self.tok.kind == T::Dot {
				self.bump();
				if self.tok.kind == T::Star {
					self.bump();
					(2, NONE)
				} else if self.tok.kind == T::LBrace
					&& (self.peek1().kind == T::Star
						|| self.peek1().kind == T::DotDot
						|| self.peek1().kind == T::Digits)
				{
					// A recursion is read here, then rejected.
					let s = self.bump().start;
					self.recurse_part(s)?;
					return Err(self.error(
						self.prev_start,
						self.prev_end,
						"Unexpected token `{` expected a `*` or a destructuring",
					));
				} else if self.tok.kind == T::LBrace {
					let s = self.bump().start;
					(3, self.nested(|p| p.destructure(s))?)
				} else if self.tok.kind == T::Invalid {
					// Any idiom part is read here and all but `*` and a
					// destructuring rejected once read; what starts no
					// part fails as the part would.
					return Err(self.invalid_token(self.tok));
				} else if self.tok.kind == T::Question
					|| self.tok.kind == T::At
					|| self.tok.kind == T::Ident
					|| self.tok.kind == T::EscIdent
				{
					let t = self.tok;
					let end = self.syn_token_end(t);
					return Err(self.diag(Diag::message(
						t.start,
						end,
						"Unexpected token `{found}` expected a `*` or a destructuring",
					)));
				} else {
					return Err(self.unexpected("an identifier"));
				}
			} else {
				(0, NONE)
			};
			let e = self.stmt_node(Tag::DestructEntry, op, name, b, t.start, f)?;
			self.vals.push(e);
			if !self.eat(T::Comma) {
				self.close(T::RBrace, start)?;
				break;
			}
		}
		let list = self.list_from(mark).0;
		self.e -= 1;
		self.stmt_node(Tag::PartDestructure, 0, list, 0, start, first)
	}

	/// A graph or reference lookup after its arrow, as a [`Tag::PartGraph`]
	/// node whose `op` is the direction (0 `->`, 1 `<-`, 2 `<->`, 3 `<~`).
	/// A lookup in parentheses takes clauses, so it is parsed as a nested
	/// statement.
	pub(crate) fn graph(&mut self, dir: u8, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let mut rec = [NONE; crate::ast::select::LEN];
		let mut flags = 0;
		let t = self.tok;
		if t.kind == T::Question {
			self.bump();
			rec[crate::ast::select::WHAT] = self.empty_list();
		} else if t.kind == T::QQ {
			// The first `?` of `??` is the subject; the second is the next
			// token, as it has always read.
			let at = t.start + 1;
			self.reset_to(at);
			self.prev_start = t.start;
			self.prev_end = at;
			rec[crate::ast::select::WHAT] = self.empty_list();
		} else if t.kind == T::LParen {
			self.nested(|p| p.lookup_clauses(&mut rec, &mut flags))?;
		} else if t.is_ident() {
			let s = self.subject(false)?;
			let mark = self.vals.len() as u32;
			self.vals.push(s);
			rec[crate::ast::select::WHAT] = self.list_from(mark).0;
		} else {
			return Err(self.unexpected("`?`, `(` or an identifier"));
		}
		rec[crate::ast::select::FLAGS] = flags;
		let idx = self.record(&rec);
		self.stmt_node(Tag::PartGraph, dir, idx, 0, start, first)
	}

	/// `( [SELECT fields FROM [ONLY]] subjects [WHERE …] … [AS alias] )`.
	fn lookup_clauses(&mut self, rec: &mut [u32], flags: &mut u32) -> R<()> {
		let open = self.bump().start;
		let select = self.eat_kw(Kw::Select);
		if select {
			*flags |= 128;
			if self.eat_kw(Kw::Value) {
				rec[crate::ast::select::VALUE] = self.value_selector()?;
			} else {
				rec[crate::ast::select::FIELDS] = self.fields()?;
			}
			self.expect_kw(Kw::From, "FROM")?;
			if self.eat_kw(Kw::Only) {
				*flags |= 1;
			}
		}
		let mark = self.vals.len() as u32;
		if !self.eat(T::Question) {
			if !self.tok.is_ident() {
				return Err(self.unexpected("`?`, an identifier or a range"));
			}
			loop {
				let s = self.subject(true)?;
				self.vals.push(s);
				if !self.eat(T::Comma) {
					break;
				}
			}
		}
		rec[crate::ast::select::WHAT] = self.list_from(mark).0;
		if self.eat_kw(Kw::Where) {
			rec[crate::ast::select::COND] = self.expr(true)?;
		}
		if select {
			self.split_clause(rec)?;
			self.group_clause(rec, flags)?;
			self.order_clause(rec)?;
		}
		self.limit_start(rec)?;
		if self.eat_kw(Kw::As) {
			rec[crate::ast::select::FETCH] = self.plain_idiom()?;
		}
		self.close(T::RParen, open)?;
		Ok(())
	}

	/// A lookup subject: `table`, `table:range`, with `FIELD name` when
	/// `field` is allowed.
	fn subject(&mut self, field: bool) -> R<NodeId> {
		let start = self.tok.start;
		let first = self.ast.nodes.len();
		let table = self.name()?;
		let range = if self.at(T::Colon) && !self.tok.ws() {
			self.bump();
			self.id_range()?
		} else {
			NONE
		};
		let f = if field && self.eat_kw(Kw::Field) {
			self.name()?
		} else {
			NONE
		};
		let idx = self.record(&[range, f]);
		self.stmt_node(Tag::Subject, 0, table, idx, start, first)
	}

	/// A record-id range after `table:`: `[key [>]] .. [[=]key]`, as a
	/// `KeyRange` node.
	fn id_range(&mut self) -> R<NodeId> {
		let start = self.tok.start;
		let first = self.ast.nodes.len();
		let mut flags = 0u8;
		let lo = if !self.tok.ws() && starts_record_key(self.tok) {
			let k = self.record_key()?;
			if self.at(T::Gt) && !self.tok.ws() {
				self.bump();
				flags |= 1;
			}
			k
		} else {
			NONE
		};
		self.expect(T::DotDot, "..")?;
		let hi = if self.at(T::Eq) && !self.tok.ws() {
			self.bump();
			flags |= 2;
			self.record_key()?
		} else if !self.tok.ws() && starts_record_key(self.tok) {
			self.record_key()?
		} else {
			NONE
		};
		let d = self.max_depth_since(first);
		self.node(Tag::KeyRange, flags, lo, hi, start, d)
	}

	/// One record-id key, with no whitespace before it.
	pub(crate) fn record_key(&mut self) -> R<NodeId> {
		if self.tok.ws() {
			return Err(self.key_after_whitespace());
		}
		if self.tok.kind == T::LBracket || self.tok.kind == T::LBrace {
			let open = self.tok.kind;
			if open == T::LBrace {
				// Only an object can follow, so what is neither a key
				// nor its end is reported as an object reads it.
				let n = self.peek1();
				if !(n.kind == T::RBrace
					|| n.kind == T::Ident
					|| n.kind == T::EscIdent
					|| n.kind == T::Str
					|| n.kind == T::Digits)
				{
					self.bump();
					return Err(self.unexpected("an object key"));
				}
				if (n.kind == T::Ident || n.kind == T::EscIdent || n.kind == T::Str)
					&& self.peek2().kind != T::Colon
				{
					self.bump();
					self.bump();
					return Err(self.unexpected(":"));
				}
			}
			let k = self.atom(self.table_as_field)?;
			let tag = self.ast.nodes[k as usize].tag;
			let ok = if open == T::LBracket {
				tag == Tag::Array
			} else {
				tag == Tag::Object
			};
			if !ok {
				let n = self.ast.nodes[k as usize];
				return Err(self.error(n.start, n.end, "expected an object"));
			}
			Ok(k)
		} else {
			self.simple_key()
		}
	}

	/// Runs `f` as a nested statement: native recursion, charged against the
	/// query depth budget.
	pub(crate) fn nested<T>(&mut self, f: impl FnOnce(&mut Self) -> R<T>) -> R<T> {
		self.enter_native()?;
		let saved = self.table_as_field;
		let res = f(self);
		self.table_as_field = saved;
		self.native -= 1;
		res
	}
}

/// Whether `t` can start a record-id key.
fn starts_record_key(t: Tok) -> bool {
	t.is_ident()
		|| (t.kind == T::Digits
			|| t.kind == T::LBrace
			|| t.kind == T::LBracket
			|| t.kind == T::Plus
			|| t.kind == T::Minus)
		|| (t.kind == T::Str && (t.aux() == 0 || t.aux() == b'u'))
}
