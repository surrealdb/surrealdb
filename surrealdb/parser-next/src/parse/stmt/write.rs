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
			// Accepted and ignored, as by `syn`.
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
		let kind = match t.kw {
			_ if t.kind != T::Ident => DataKind::None,
			Kw::Set => DataKind::Set,
			Kw::Unset => DataKind::Unset,
			Kw::Content => DataKind::Content,
			Kw::Merge => DataKind::Merge,
			Kw::Patch => DataKind::Patch,
			Kw::Replace => DataKind::Replace,
			_ => DataKind::None,
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
			_ => {
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
			let op = match self.tok.kind {
				T::Eq => Assign::Set,
				T::PlusEq => Assign::Add,
				T::MinusEq => Assign::Sub,
				T::PlusQEq => Assign::Ext,
				_ => return Err(self.unexpected("expected an assignment operator")),
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
			let k = match t.kw {
				_ if t.kind != T::Ident => 6,
				Kw::None_ => 1,
				Kw::Null => 2,
				Kw::Diff => 3,
				Kw::After => 4,
				Kw::Before => 5,
				_ => 6,
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
		let name = self.expect(T::Param, "expected a parameter")?;
		let name = self.leaf(Tag::Param, 0, name);
		let kind = if self.eat(T::Colon) {
			self.kind()?
		} else {
			NONE
		};
		self.expect(T::Eq, "expected `=`")?;
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
			self.expect_kw(Kw::Update, "expected `UPDATE`")?;
			flags |= 4;
		}
		let a = self.relate_end()?;
		let out = match self.tok.kind {
			T::Arrow => true,
			T::Lt => {
				self.relate_in_arrow()?;
				false
			}
			_ => return Err(self.unexpected("expected a relation arrow")),
		};
		if out {
			self.bump();
		}
		rec[r::THROUGH] = self.relate_through()?;
		if out {
			self.expect(T::Arrow, "expected `->`")?;
		} else {
			if !self.at(T::Lt) {
				return Err(self.unexpected("expected `<-`"));
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
			return Err(self.unexpected("expected `-`"));
		}
		self.bump();
		Ok(())
	}

	/// The `from` or `to` of a relation.
	fn relate_end(&mut self) -> R<NodeId> {
		let t = self.tok;
		match t.kind {
			T::LBracket => self.atom(true),
			T::Param => {
				self.bump();
				Ok(self.leaf(Tag::Param, 0, t))
			}
			T::LParen => {
				self.bump();
				let e = self.expr(true)?;
				self.expect(T::RParen, "expected `)`")?;
				self.ast.nodes[e as usize].depth |= COVERED;
				Ok(e)
			}
			T::Ident
				if matches!(
					t.kw,
					Kw::Return
						| Kw::Select | Kw::Create
						| Kw::Upsert | Kw::Update
						| Kw::Delete | Kw::Relate
						| Kw::Define | Kw::Alter
						| Kw::Remove | Kw::Rebuild
						| Kw::Info | Kw::If
				) =>
			{
				self.expr(true)
			}
			_ => self.record_id_only(),
		}
	}

	/// A record-id literal without a range key, and nothing after it.
	pub(crate) fn record_id_only(&mut self) -> R<NodeId> {
		if !self.tok.is_ident() {
			return Err(self.unexpected("expected an identifier"));
		}
		if self.peek1().kind != T::Colon {
			self.bump();
			return Err(self.unexpected("expected `:`"));
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
		match t.kind {
			T::Param => {
				self.bump();
				Ok(self.leaf(Tag::Param, 0, t))
			}
			T::LParen => {
				self.bump();
				let e = self.expr(self.table_as_field)?;
				self.expect(T::RParen, "expected `)`")?;
				self.ast.nodes[e as usize].depth |= COVERED;
				Ok(e)
			}
			_ => {
				if self.adjacent1().is_some_and(|n| n.kind == T::Colon) {
					return self.record_id_only();
				}
				let n = self.ident()?;
				Ok(self.leaf(Tag::Table, 0, n))
			}
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
			self.expect_kw(Kw::Duplicate, "expected `DUPLICATE`")?;
			self.expect_kw(Kw::Key, "expected `KEY`")?;
			self.expect_kw(Kw::Update, "expected `UPDATE`")?;
			rec[i::UPDATE] = self.assignments()?;
		}
		self.output_clause(&mut rec)?;
		if self.eat_kw(Kw::Version) {
			// Accepted and ignored, as by `syn`.
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
					self.expect(T::RParen, "expected `)`")?;
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
			self.expect_kw(Kw::Values, "expected `VALUES`")?;
		} else if !self.eat_kw(Kw::Values) {
			return Ok(None);
		}
		let idioms = self.list_from(mark).0;
		let rows_mark = self.vals.len() as u32;
		loop {
			self.expect(T::LParen, "expected `(`")?;
			let row_mark = self.vals.len() as u32;
			loop {
				let e = self.expr(true)?;
				self.vals.push(e);
				if !self.eat(T::Comma) {
					self.expect(T::RParen, "expected `)`")?;
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

/// Whether `t` starts a statement (`syn`'s `kind_starts_statement`).
fn is_statement_start(t: Tok) -> bool {
	t.kind == T::Ident
		&& matches!(
			t.kw,
			Kw::Access
				| Kw::Alter | Kw::Begin
				| Kw::Break | Kw::Cancel
				| Kw::Commit | Kw::Continue
				| Kw::Create | Kw::Define
				| Kw::Delete | Kw::For
				| Kw::If | Kw::Info
				| Kw::Insert | Kw::Kill
				| Kw::Live | Kw::Option
				| Kw::Rebuild
				| Kw::Return | Kw::Relate
				| Kw::Remove | Kw::Select
				| Kw::Let | Kw::Show
				| Kw::Sleep | Kw::Throw
				| Kw::Update | Kw::Upsert
				| Kw::Use
		)
}
