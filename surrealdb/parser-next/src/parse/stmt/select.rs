use crate::parse::*;

impl<'a, 's> Parser<'a, 's> {
	// ------------------------------------------------------------ statements

	pub(crate) fn select(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let mut rec = [NONE; crate::ast::select::LEN];
		let mut flags = 0u32;
		if self.eat_kw(Kw::Value) {
			rec[crate::ast::select::VALUE] = self.value_selector()?;
		} else {
			rec[crate::ast::select::FIELDS] = self.fields()?;
		}
		if self.eat_kw(Kw::Omit) {
			let mark = self.vals.len() as u32;
			loop {
				let e = self.expr(true)?;
				self.vals.push(e);
				if !self.eat(T::Comma) {
					break;
				}
			}
			rec[crate::ast::select::OMIT] = self.list_from(mark).0;
		}
		self.expect_kw(Kw::From, "expected `FROM`")?;
		if self.eat_kw(Kw::Only) {
			flags |= 1;
		}
		rec[crate::ast::select::WHAT] = self.expr_list(false)?;
		rec[crate::ast::select::WITH] = self.with_clause()?;
		if self.eat_kw(Kw::Where) {
			rec[crate::ast::select::COND] = self.expr(true)?;
		}
		self.split_clause(&mut rec)?;
		self.group_clause(&mut rec, &mut flags)?;
		self.order_clause(&mut rec)?;
		self.limit_start(&mut rec)?;
		if self.eat_kw(Kw::Fetch) {
			rec[crate::ast::select::FETCH] = self.fetch_list()?;
		}
		if self.eat_kw(Kw::Version) {
			rec[crate::ast::select::VERSION] = self.expr(true)?;
		}
		if self.eat_kw(Kw::Timeout) {
			rec[crate::ast::select::TIMEOUT] = self.expr(true)?;
		}
		if self.eat_kw(Kw::For) {
			self.expect_kw(Kw::Update, "expected `UPDATE`")?;
			flags |= 64;
		}
		if self.eat_kw(Kw::Tempfiles) {
			flags |= 8;
		}
		flags |= self.explain_clause();
		rec[crate::ast::select::FLAGS] = flags;
		let idx = self.ast.extra.len() as u32;
		self.ast.extra.extend_from_slice(&rec);
		let d = self.max_depth_since(first);
		self.node(Tag::Select, 0, idx, 0, start, d)
	}

	/// `WITH NOINDEX`, `WITH NO INDEX` or `WITH INDEX a, …`, if present.
	pub(crate) fn with_clause(&mut self) -> R<NodeId> {
		if !self.at_kw(Kw::With) {
			return Ok(NONE);
		}
		let t = self.bump();
		let n = self.tok;
		match n.kw {
			Kw::NoIndex if n.kind == T::Ident => {
				self.bump();
			}
			Kw::No if n.kind == T::Ident => {
				self.bump();
				self.expect_kw(Kw::Index, "expected `INDEX`")?;
			}
			Kw::Index if n.kind == T::Ident => {
				self.bump();
				let first = self.ast.nodes.len();
				let mark = self.vals.len() as u32;
				loop {
					let i = self.name()?;
					self.vals.push(i);
					if !self.eat(T::Comma) {
						break;
					}
				}
				let list = self.list_from(mark).0;
				return self.stmt_node(Tag::With, 1, list, 0, t.start, first);
			}
			_ => return Err(self.unexpected("expected `NO`, `NOINDEX` or `INDEX`")),
		}
		Ok(self.leaf_span(Tag::With, 0, 0, 0, t.start, self.prev_end))
	}

	/// `EXPLAIN [FULL]`, if present, as flag bits 16 and 32.
	pub(crate) fn explain_clause(&mut self) -> u32 {
		if !self.eat_kw(Kw::Explain) {
			return 0;
		}
		if self.eat_kw(Kw::Full) {
			48
		} else {
			16
		}
	}

	/// `SPLIT [ON] idiom, …`, if present.
	pub(crate) fn split_clause(&mut self, rec: &mut [u32]) -> R<()> {
		if self.eat_kw(Kw::Split) {
			self.eat_kw(Kw::On);
			let mark = self.vals.len() as u32;
			loop {
				let i = self.basic_idiom()?;
				self.vals.push(i);
				if !self.eat(T::Comma) {
					break;
				}
			}
			rec[crate::ast::select::SPLIT] = self.list_from(mark).0;
		}
		Ok(())
	}

	/// `ORDER [BY] …`, if present.
	pub(crate) fn order_clause(&mut self, rec: &mut [u32]) -> R<()> {
		if self.eat_kw(Kw::Order) {
			self.eat_kw(Kw::By);
			let mark = self.vals.len() as u32;
			if self.at_kw(Kw::Rand) {
				let t = self.bump();
				self.expect(T::LParen, "expected `(`")?;
				self.expect(T::RParen, "expected `)`")?;
				let n = self.leaf_span(Tag::OrderRand, 0, 0, 0, t.start, self.prev_end);
				self.vals.push(n);
			} else {
				loop {
					let s = self.tok.start;
					let i = self.basic_idiom()?;
					let mut op = 0u8;
					if self.eat_kw(Kw::Collate) {
						op |= 2;
					}
					if self.eat_kw(Kw::Numeric) {
						op |= 4;
					}
					if self.eat_kw(Kw::Descending) {
						op |= 1;
					} else {
						self.eat_kw(Kw::Ascending);
					}
					let d = self.depth_of(i);
					let n = self.node(Tag::OrderItem, op, i, 0, s, d)?;
					self.vals.push(n);
					if !self.eat(T::Comma) {
						break;
					}
				}
			}
			rec[crate::ast::select::ORDER] = self.list_from(mark).0;
		}
		Ok(())
	}

	/// `LIMIT` and `START` in either order, if present.
	pub(crate) fn limit_start(&mut self, rec: &mut [u32]) -> R<()> {
		if self.at_kw(Kw::Start) {
			self.start_clause(rec)?;
			self.limit_clause(rec)
		} else {
			self.limit_clause(rec)?;
			self.start_clause(rec)
		}
	}

	/// `GROUP ALL` or `GROUP [BY] idiom, …`, if present.
	pub(crate) fn group_clause(&mut self, rec: &mut [u32], flags: &mut u32) -> R<()> {
		if self.eat_kw(Kw::Group) {
			if self.eat_kw(Kw::All) {
				*flags |= 2;
			} else {
				self.eat_kw(Kw::By);
				let mark = self.vals.len() as u32;
				loop {
					let i = self.basic_idiom()?;
					self.vals.push(i);
					if !self.eat(T::Comma) {
						break;
					}
				}
				rec[crate::ast::select::GROUP] = self.list_from(mark).0;
			}
		}
		Ok(())
	}

	pub(crate) fn limit_clause(&mut self, rec: &mut [u32]) -> R<()> {
		if self.eat_kw(Kw::Limit) {
			self.eat_kw(Kw::By);
			rec[crate::ast::select::LIMIT] = self.expr(true)?;
		}
		Ok(())
	}

	pub(crate) fn start_clause(&mut self, rec: &mut [u32]) -> R<()> {
		if self.eat_kw(Kw::Start) {
			self.eat_kw(Kw::At);
			rec[crate::ast::select::START] = self.expr(true)?;
		}
		Ok(())
	}
}
