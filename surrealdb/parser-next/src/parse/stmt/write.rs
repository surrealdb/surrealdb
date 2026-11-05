use crate::parse::*;

impl<'a, 's> Parser<'a, 's> {
	pub(crate) fn write(&mut self, tag: Tag, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let mut rec = [NONE; crate::ast::write::LEN];
		let mut flags = 0u32;
		if self.eat_kw(Kw::Only) {
			flags |= 1;
		}
		rec[crate::ast::write::WHAT] = self.expr_list(false)?;
		if tag != Tag::Create {
			rec[crate::ast::write::WITH] = self.with_clause()?;
		}
		rec[crate::ast::write::DATA_KIND] = DataKind::None as u32;
		if tag != Tag::Delete {
			self.data_clause(&mut rec)?;
		}
		if tag != Tag::Create && self.eat_kw(Kw::Where) {
			rec[crate::ast::write::COND] = self.expr(true)?;
		}
		self.output_clause(&mut rec)?;
		if tag == Tag::Create && self.eat_kw(Kw::Version) {
			// Accepted and ignored, for compatibility with existing queries.
			self.expr(true)?;
		}
		if self.eat_kw(Kw::Timeout) {
			rec[crate::ast::write::TIMEOUT] = self.expr(true)?;
		}
		if tag != Tag::Create {
			flags |= self.explain_clause();
		}
		rec[crate::ast::write::FLAGS] = flags;
		let idx = self.ast.extra.len() as u32;
		self.ast.extra.extend_from_slice(&rec);
		let d = self.max_depth_since(first);
		self.node(tag, 0, idx, 0, start, d)
	}

	/// `SET`, `UNSET`, `CONTENT`, `MERGE`, `PATCH` or `REPLACE`, if present.
	pub(crate) fn data_clause(&mut self, rec: &mut [u32]) -> R<()> {
		let t = self.tok;
		let kind = if t.kind != T::Ident {
			DataKind::None
		} else if t.kw == Kw::Set {
			DataKind::Set
		} else if t.kw == Kw::Unset {
			DataKind::Unset
		} else if t.kw == Kw::Content {
			DataKind::Content
		} else if t.kw == Kw::Merge {
			DataKind::Merge
		} else if t.kw == Kw::Patch {
			DataKind::Patch
		} else if t.kw == Kw::Replace {
			DataKind::Replace
		} else {
			DataKind::None
		};
		rec[crate::ast::write::DATA_KIND] = kind as u32;
		match kind {
			DataKind::None => {}
			DataKind::Set => {
				self.bump();
				rec[crate::ast::write::DATA] = self.assignments()?;
			}
			DataKind::Unset => {
				self.bump();
				let mark = self.vals.len() as u32;
				loop {
					let i = self.plain_idiom()?;
					self.vals.push(i);
					if !self.eat(T::Comma) {
						break;
					}
				}
				rec[crate::ast::write::DATA] = self.list_from(mark).0;
			}
			DataKind::Content | DataKind::Merge | DataKind::Patch | DataKind::Replace => {
				self.bump();
				rec[crate::ast::write::DATA] = self.expr(true)?;
			}
		}
		Ok(())
	}

	/// `place op value, …`.
	pub(crate) fn assignments(&mut self) -> R<u32> {
		let mark = self.vals.len() as u32;
		loop {
			let s = self.tok.start;
			let place = self.plain_idiom()?;
			let op = if self.tok.kind == T::Eq {
				Assign::Set
			} else if self.tok.kind == T::PlusEq {
				Assign::Add
			} else if self.tok.kind == T::MinusEq {
				Assign::Sub
			} else if self.tok.kind == T::PlusQEq {
				Assign::Ext
			} else {
				return Err(self.unexpected("an assign operator"));
			};
			self.bump();
			let v = self.expr(true)?;
			let d = self.depth_of(place).max(self.depth_of(v));
			let n = self.node(Tag::Assign, op as u8, place, v, s, d)?;
			self.vals.push(n);
			if !self.eat(T::Comma) {
				break;
			}
		}
		Ok(self.list_from(mark).0)
	}

	/// `RETURN …`, if present.
	pub(crate) fn output_clause(&mut self, rec: &mut [u32]) -> R<()> {
		rec[crate::ast::write::OUTPUT_KIND] = 0;
		if self.eat_kw(Kw::Return) {
			let t = self.tok;
			let k = if t.kind != T::Ident {
				6
			} else if t.kw == Kw::None_ {
				1
			} else if t.kw == Kw::Null {
				2
			} else if t.kw == Kw::Diff {
				3
			} else if t.kw == Kw::After {
				4
			} else if t.kw == Kw::Before {
				5
			} else {
				6
			};
			if k == 6 && self.at_kw(Kw::Value) {
				self.bump();
				rec[crate::ast::write::OUTPUT] = self.value_selector()?;
				rec[crate::ast::write::OUTPUT_KIND] = 7;
			} else if k == 6 {
				rec[crate::ast::write::OUTPUT] = self.fields()?;
			} else {
				self.bump();
			}
			if rec[crate::ast::write::OUTPUT_KIND] == 0 {
				rec[crate::ast::write::OUTPUT_KIND] = k;
			}
		}
		Ok(())
	}

	/// `RETURN expr [FETCH …]`, after `RETURN`.
	pub(crate) fn return_stmt(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let what = self.expr(self.table_as_field)?;
		let fetch = if self.eat_kw(Kw::Fetch) {
			self.fetch_list()?
		} else {
			NONE
		};
		let d = self.max_depth_since(first);
		self.node(Tag::Return, 0, what, fetch, start, d)
	}

	/// `LET $name [: kind] = expr`, after `LET`.
	pub(crate) fn let_stmt(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let name = self.expect(T::Param, "a parameter")?;
		let name = self.leaf(Tag::Param, 0, name);
		let kind = if self.eat(T::Colon) {
			self.kind()?
		} else {
			NONE
		};
		self.expect(T::Eq, "=")?;
		let what = self.expr(self.table_as_field)?;
		let idx = self.ast.extra.len() as u32;
		self.ast.extra.extend_from_slice(&[name, kind, what]);
		let d = self.max_depth_since(first);
		self.node(Tag::Let, 0, idx, 0, start, d)
	}
}

impl<'a, 's> Parser<'a, 's> {
	/// `RELATE [ONLY] [OR UPDATE] from -> through -> to …`, after `RELATE`.
	pub(crate) fn relate(&mut self, start: u32) -> R<NodeId> {
		use crate::ast::{relate as r, write as w};
		let first = self.ast.nodes.len();
		let mut rec = [NONE; r::LEN];
		let mut flags = 0u32;
		if self.eat_kw(Kw::Only) {
			flags |= 1;
		}
		if self.eat_kw(Kw::Or) {
			self.expect_kw(Kw::Update, "UPDATE")?;
			flags |= 4;
		}
		let a = self.relate_end()?;
		let out = if self.tok.kind == T::Arrow {
			true
		} else if self.tok.kind == T::Lt {
			self.relate_in_arrow()?;
			false
		} else {
			return Err(self.unexpected("a relation arrow"));
		};
		if out {
			self.bump();
		}
		rec[r::THROUGH] = self.relate_through()?;
		if out {
			self.expect(T::Arrow, "->")?;
		} else {
			if !self.at(T::Lt) {
				return Err(self.unexpected("<"));
			}
			self.relate_in_arrow()?;
		}
		let b = self.relate_end()?;
		(rec[r::FROM], rec[r::TO]) = if out {
			(a, b)
		} else {
			(b, a)
		};
		self.eat_kw(Kw::Unique);
		self.data_clause(&mut rec)?;
		self.output_clause(&mut rec)?;
		if self.eat_kw(Kw::Timeout) {
			rec[w::TIMEOUT] = self.expr(true)?;
		}
		rec[w::FLAGS] = flags;
		let idx = self.record(&rec);
		self.stmt_node(Tag::Relate, 0, idx, 0, start, first)
	}

	/// `<-`, with no whitespace between `<` and `-`.
	fn relate_in_arrow(&mut self) -> R<()> {
		self.bump();
		if !self.at(T::Minus) || self.tok.ws() {
			return Err(self.unexpected("-"));
		}
		self.bump();
		Ok(())
	}

	/// The `from` or `to` of a relation.
	fn relate_end(&mut self) -> R<NodeId> {
		let t = self.tok;
		if t.kind == T::LBracket {
			self.atom(true)
		} else if t.kind == T::Param {
			self.bump();
			Ok(self.leaf(Tag::Param, 0, t))
		} else if t.kind == T::LParen {
			let open = self.bump();
			let e = self.expr(true)?;
			self.close(T::RParen, open.start)?;
			self.ast.nodes[e as usize].depth |= COVERED;
			Ok(e)
		} else if t.kind == T::Ident
			&& (t.kw == Kw::Return
				|| t.kw == Kw::Select
				|| t.kw == Kw::Create
				|| t.kw == Kw::Upsert
				|| t.kw == Kw::Update
				|| t.kw == Kw::Delete
				|| t.kw == Kw::Relate
				|| t.kw == Kw::Define
				|| t.kw == Kw::Alter
				|| t.kw == Kw::Remove
				|| t.kw == Kw::Rebuild
				|| t.kw == Kw::Info
				|| t.kw == Kw::If)
		{
			self.expr(true)
		} else {
			self.record_id_only()
		}
	}

	/// A record-id literal without a range key, and nothing after it.
	pub(crate) fn record_id_only(&mut self) -> R<NodeId> {
		if !self.tok.is_ident() {
			return Err(self.unexpected("an identifier"));
		}
		if self.peek1().kind != T::Colon {
			self.bump();
			return Err(self.unexpected(":"));
		}
		let id = self.record_id_atom()?;
		let n = self.ast.nodes[id as usize];
		if n.tag != Tag::RecordId {
			return Err(self.error(n.start, n.end, "expected a record id"));
		}
		Ok(id)
	}

	/// The edge table of a relation.
	fn relate_through(&mut self) -> R<NodeId> {
		let t = self.tok;
		if t.kind == T::Param {
			self.bump();
			Ok(self.leaf(Tag::Param, 0, t))
		} else if t.kind == T::LParen {
			let open = self.bump();
			let e = self.expr(self.table_as_field)?;
			self.close(T::RParen, open.start)?;
			self.ast.nodes[e as usize].depth |= COVERED;
			Ok(e)
		} else {
			if self.adjacent1().is_some_and(|n| n.kind == T::Colon) {
				return self.record_id_only();
			}
			let n = self.ident()?;
			Ok(self.leaf(Tag::Table, 0, n))
		}
	}

	/// `INSERT [RELATION] [IGNORE] [INTO target] values …`, after `INSERT`.
	pub(crate) fn insert(&mut self, start: u32) -> R<NodeId> {
		use crate::ast::{insert as i, write as w};
		let first = self.ast.nodes.len();
		let mut rec = [NONE; i::LEN];
		let mut flags = 0u32;
		if self.eat_kw(Kw::Relation) {
			flags |= 2;
		}
		if self.eat_kw(Kw::Ignore) {
			flags |= 4;
		}
		if self.eat_kw(Kw::Into) {
			let t = self.tok;
			rec[i::INTO] = if t.kind == T::Param {
				self.bump();
				self.leaf(Tag::Param, 0, t)
			} else {
				let n = self.ident()?;
				self.leaf(Tag::Table, 0, n)
			};
		}
		if self.at(T::LParen) {
			let cp = self.checkpoint();
			match self.insert_values()? {
				Some(v) => {
					rec[w::DATA_KIND] = 1;
					rec[w::DATA] = v;
				}
				None => {
					self.restore(cp);
					rec[w::DATA] = self.expr(true)?;
				}
			}
		} else {
			rec[w::DATA] = self.expr(true)?;
		}
		if self.eat_kw(Kw::On) {
			self.expect_kw(Kw::Duplicate, "DUPLICATE")?;
			self.expect_kw(Kw::Key, "KEY")?;
			self.expect_kw(Kw::Update, "UPDATE")?;
			rec[i::UPDATE] = self.assignments()?;
		}
		self.output_clause(&mut rec)?;
		if self.eat_kw(Kw::Version) {
			// Accepted and ignored, for compatibility with existing queries.
			self.expr(true)?;
		}
		if self.eat_kw(Kw::Timeout) {
			rec[w::TIMEOUT] = self.expr(true)?;
		}
		rec[w::FLAGS] = flags;
		let idx = self.record(&rec);
		self.stmt_node(Tag::Insert, 0, idx, 0, start, first)
	}

	/// `(fields) VALUES (…), …`, as an index into `extra` of `[idiom list,
	/// row list]`; `None` when the input is not that form and must be read as
	/// an expression instead (the caller restores the checkpoint).
	fn insert_values(&mut self) -> R<Option<u32>> {
		self.bump();
		if is_statement_start(self.tok) {
			return Ok(None);
		}
		let mark = self.vals.len() as u32;
		let Ok(first) = self.plain_idiom() else {
			return Ok(None);
		};
		self.vals.push(first);
		let mut comma = false;
		loop {
			if !self.eat(T::Comma) {
				if comma {
					self.expect(T::RParen, ")")?;
				} else if !self.eat(T::RParen) {
					return Ok(None);
				}
				break;
			}
			comma = true;
			if self.eat(T::RParen) {
				break;
			}
			let i = self.plain_idiom()?;
			self.vals.push(i);
		}
		if comma {
			self.expect_kw(Kw::Values, "VALUES")?;
		} else if !self.eat_kw(Kw::Values) {
			return Ok(None);
		}
		let idioms = self.list_from(mark).0;
		let rows_mark = self.vals.len() as u32;
		loop {
			self.expect(T::LParen, "(")?;
			let row_mark = self.vals.len() as u32;
			loop {
				let e = self.expr(true)?;
				self.vals.push(e);
				if !self.eat(T::Comma) {
					self.expect(T::RParen, ")")?;
					break;
				}
				if self.eat(T::RParen) {
					break;
				}
			}
			let row = self.list_from(row_mark).0;
			// Rows are list indices, not nodes: keep them out of depth checks.
			self.vals.push(row);
			if !self.eat(T::Comma) {
				break;
			}
		}
		let n = self.vals.len() - rows_mark as usize;
		let rows_idx = self.ast.extra.len() as u32;
		self.ast.extra.push(n as u32);
		for k in rows_mark as usize..self.vals.len() {
			let v = self.vals[k];
			self.ast.extra.push(v);
		}
		self.vals.truncate(rows_mark as usize);
		Ok(Some(self.record(&[idioms, rows_idx])))
	}
}

/// Whether `t` starts a statement.
fn is_statement_start(t: Tok) -> bool {
	t.kind == T::Ident
		&& (t.kw == Kw::Access
			|| t.kw == Kw::Alter
			|| t.kw == Kw::Begin
			|| t.kw == Kw::Break
			|| t.kw == Kw::Cancel
			|| t.kw == Kw::Commit
			|| t.kw == Kw::Continue
			|| t.kw == Kw::Create
			|| t.kw == Kw::Define
			|| t.kw == Kw::Delete
			|| t.kw == Kw::For
			|| t.kw == Kw::If
			|| t.kw == Kw::Info
			|| t.kw == Kw::Insert
			|| t.kw == Kw::Kill
			|| t.kw == Kw::Live
			|| t.kw == Kw::Option
			|| t.kw == Kw::Rebuild
			|| t.kw == Kw::Return
			|| t.kw == Kw::Relate
			|| t.kw == Kw::Remove
			|| t.kw == Kw::Select
			|| t.kw == Kw::Let
			|| t.kw == Kw::Show
			|| t.kw == Kw::Sleep
			|| t.kw == Kw::Throw
			|| t.kw == Kw::Update
			|| t.kw == Kw::Upsert
			|| t.kw == Kw::Use)
}
