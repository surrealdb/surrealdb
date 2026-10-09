//! Short statements: transactions, `USE`, `OPTION`, `KILL`, `LIVE`, `SHOW`,
//! `SLEEP`, `THROW`, `BREAK`, `CONTINUE`, `INFO` and `REBUILD`.

use crate::ast::{InfoTarget, info};
use crate::parse::*;

impl<'a, 's> Parser<'a, 's> {
	/// `BEGIN`, `CANCEL` or `COMMIT`, after the keyword.
	pub(crate) fn transaction(&mut self, tag: Tag, t: Tok) -> R<NodeId> {
		self.eat_kw(Kw::Transaction);
		Ok(self.leaf_span(tag, 0, 0, 0, t.start, self.prev_end))
	}

	pub(crate) fn use_stmt(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let (op, a, b) = match self.tok.kw {
			_ if self.tok.kind != T::Ident => {
				return Err(self.unexpected("either DATABASE or NAMESPACE"));
			}
			Kw::Namespace => {
				self.bump();
				let ns = self.expr(true)?;
				if self.eat_kw(Kw::Database) {
					let db = self.expr(true)?;
					(2, ns, db)
				} else {
					(0, ns, NONE)
				}
			}
			Kw::Database => {
				self.bump();
				(1, self.expr(true)?, NONE)
			}
			Kw::Default => {
				self.bump();
				(3, NONE, NONE)
			}
			_ => return Err(self.unexpected("either DATABASE or NAMESPACE")),
		};
		self.stmt_node(Tag::Use, op, a, b, start, first)
	}

	pub(crate) fn option_stmt(&mut self, start: u32) -> R<NodeId> {
		let name = self.name()?;
		let mut value = true;
		if self.eat(T::Eq) {
			value = match self.tok.kw {
				Kw::True if self.tok.kind == T::Ident => true,
				Kw::False if self.tok.kind == T::Ident => false,
				_ => return Err(self.unexpected("`true` or `false`")),
			};
			self.bump();
		}
		self.node(Tag::OptionStmt, value as u8, name, 0, start, 1)
	}

	pub(crate) fn kill_stmt(&mut self, start: u32) -> R<NodeId> {
		let t = self.tok;
		let id = match t.kind {
			T::Str if t.aux() == b'u' => {
				self.bump();
				self.leaf(Tag::Str, b'u', t)
			}
			T::Param => {
				self.bump();
				self.leaf(Tag::Param, 0, t)
			}
			_ => return Err(self.unexpected("a UUID or a parameter")),
		};
		self.node(Tag::Kill, 0, id, 0, start, 1)
	}

	pub(crate) fn live_stmt(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		self.expect_kw(Kw::Select, "SELECT")?;
		let (fields, flags) = if self.eat_kw(Kw::Diff) {
			(NONE, 1)
		} else if self.eat_kw(Kw::Value) {
			(self.value_selector()?, 2)
		} else {
			(self.fields()?, 0)
		};
		self.expect_kw(Kw::From, "FROM")?;
		let what = self.expr(false)?;
		let cond = if self.eat_kw(Kw::Where) {
			self.expr(true)?
		} else {
			NONE
		};
		let fetch = if self.eat_kw(Kw::Fetch) {
			self.fetch_list()?
		} else {
			NONE
		};
		let rec = self.record(&[fields, what, cond, fetch, flags]);
		self.stmt_node(Tag::Live, 0, rec, 0, start, first)
	}

	pub(crate) fn show_stmt(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		self.expect_kw(Kw::Changes, "CHANGES")?;
		self.expect_kw(Kw::For, "FOR")?;
		let table = match self.tok.kw {
			Kw::Table | Kw::Tb if self.tok.kind == T::Ident => {
				self.bump();
				self.name()?
			}
			Kw::Database if self.tok.kind == T::Ident => {
				self.bump();
				NONE
			}
			_ => return Err(self.unexpected("`TABLE` or `DATABASE`")),
		};
		self.expect_kw(Kw::Since, "SINCE")?;
		let t = self.tok;
		let since = match t.kind {
			T::Digits => self.uint_lit()?,
			T::Str if t.aux() == b'd' => {
				self.bump();
				self.leaf(Tag::Str, b'd', t)
			}
			_ => return Err(self.unexpected("a version stamp or a datetime")),
		};
		let limit = if self.eat_kw(Kw::Limit) {
			self.uint_lit()?
		} else {
			NONE
		};
		let rec = self.record(&[table, since, limit]);
		self.stmt_node(Tag::Show, 0, rec, 0, start, first)
	}

	pub(crate) fn sleep_stmt(&mut self, start: u32) -> R<NodeId> {
		let d = self.duration_lit()?;
		self.node(Tag::Sleep, 0, d, 0, start, 1)
	}

	pub(crate) fn throw_stmt(&mut self, start: u32) -> R<NodeId> {
		let v = self.expr(self.table_as_field)?;
		let d = self.depth_of(v);
		self.node(Tag::Throw, 0, v, 0, start, d)
	}

	pub(crate) fn info_stmt(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		self.expect_kw(Kw::For, "FOR")?;
		let mut rec = [NONE; info::LEN];
		let mut flags = 0;
		let t = self.tok;
		let target = match t.kw {
			_ if t.kind != T::Ident => return Err(self.unexpected("an info target")),
			Kw::Root => InfoTarget::Root,
			Kw::Namespace => InfoTarget::Ns,
			Kw::Database => InfoTarget::Db,
			Kw::Table | Kw::Tb => InfoTarget::Table,
			Kw::User => InfoTarget::User,
			Kw::Index => InfoTarget::Index,
			_ => return Err(self.unexpected("an info target")),
		};
		self.bump();
		match target {
			InfoTarget::Table => rec[info::WHAT] = self.expr(false)?,
			InfoTarget::User => {
				rec[info::WHAT] = self.expr(self.table_as_field)?;
				if self.eat_kw(Kw::On) {
					flags |= self.base()? << 1;
				}
			}
			InfoTarget::Index => {
				rec[info::WHAT] = self.expr(true)?;
				self.expect_kw(Kw::On, "ON")?;
				self.eat_kw(Kw::Table);
				rec[info::TABLE] = self.expr(false)?;
			}
			_ => {}
		}
		if !matches!(target, InfoTarget::User | InfoTarget::Index) && self.eat_kw(Kw::Version) {
			rec[info::VERSION] = self.expr(self.table_as_field)?;
		}
		if self.eat_kw(Kw::Structure) {
			flags |= 1;
		}
		rec[info::FLAGS] = flags;
		let idx = self.record(&rec);
		self.stmt_node(Tag::Info, target as u8, idx, 0, start, first)
	}

	/// `EXPLAIN [ANALYZE] [FORMAT TEXT|JSON] stmt`.
	pub(crate) fn explain_stmt(&mut self, start: u32) -> R<NodeId> {
		let mut op = 0;
		// `ANALYZE`, `FORMAT`, `TEXT` and `JSON` are plain identifiers.
		let word =
			|p: &Self, w: &str| p.tok.kind == T::Ident && p.text_of(p.tok).eq_ignore_ascii_case(w);
		if word(self, "ANALYZE") && self.tok.kw == Kw::None {
			self.bump();
			op |= 1;
		}
		if word(self, "FORMAT") && self.tok.kw == Kw::None {
			self.bump();
			if word(self, "JSON") {
				op |= 2;
			} else if !word(self, "TEXT") {
				return Err(self.unexpected("`TEXT` or `JSON`"));
			}
			self.bump();
		}
		let s = self.expr(true)?;
		let d = self.depth_of(s);
		self.node(Tag::Explain, op, s, 0, start, d)
	}

	pub(crate) fn rebuild_stmt(&mut self, start: u32) -> R<NodeId> {
		self.expect_kw(Kw::Index, "INDEX")?;
		let mut flags = 0;
		if self.eat_kw(Kw::If) {
			self.expect_kw(Kw::Exists, "EXISTS")?;
			flags |= 1;
		}
		let name = self.name()?;
		self.expect_kw(Kw::On, "ON")?;
		self.eat_kw(Kw::Table);
		let table = self.name()?;
		if self.eat_kw(Kw::Concurrently) {
			flags |= 2;
		}
		let rec = self.record(&[name, table, flags]);
		self.node(Tag::Rebuild, 0, rec, 0, start, 1)
	}
}

impl<'a, 's> Parser<'a, 's> {
	/// `IF cond THEN a [ELSE IF …] [ELSE b] END` or `IF cond { … } [ELSE IF
	/// cond { … }] [ELSE { … }]`, after `IF`.
	pub(crate) fn if_stmt(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let mark = self.vals.len() as u32;
		let inherit = self.table_as_field;
		let cond = self.expr(inherit)?;
		self.vals.push(cond);
		let mut close = NONE;
		if self.eat_kw(Kw::Then) {
			let body = self.expr(inherit)?;
			self.vals.push(body);
			self.eat(T::Semi);
			loop {
				if self.eat_kw(Kw::End) {
					break;
				}
				if !self.eat_kw(Kw::Else) {
					return Err(self.unexpected("if to end"));
				}
				if self.eat_kw(Kw::If) {
					let c = self.expr(inherit)?;
					self.vals.push(c);
					self.expect_kw(Kw::Then, "THEN")?;
					let b = self.expr(inherit)?;
					self.vals.push(b);
					self.eat(T::Semi);
				} else {
					close = self.expr(inherit)?;
					self.eat(T::Semi);
					self.expect_kw(Kw::End, "END")?;
					break;
				}
			}
		} else if self.at(T::LBrace) {
			let body = self.block()?;
			self.vals.push(body);
			while self.eat_kw(Kw::Else) {
				if self.eat_kw(Kw::If) {
					let c = self.expr(inherit)?;
					self.vals.push(c);
					let b = self.block()?;
					self.vals.push(b);
				} else {
					close = self.block()?;
					break;
				}
			}
		} else {
			return Err(self.unexpected("THEN or '{'"));
		}
		let list = self.list_from(mark).0;
		self.stmt_node(Tag::If, 0, list, close, start, first)
	}

	/// `FOR $param IN expr { … }`, after `FOR`.
	pub(crate) fn for_stmt(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let p = self.expect(T::Param, "a parameter")?;
		let param = self.leaf(Tag::Param, 0, p);
		self.expect_kw(Kw::In, "IN")?;
		let range = self.expr(self.table_as_field)?;
		let block = self.block()?;
		let rec = self.record(&[param, range, block]);
		self.stmt_node(Tag::For, 0, rec, 0, start, first)
	}
}

impl<'a, 's> Parser<'a, 's> {
	/// `ACCESS name [ON base] GRANT | SHOW | REVOKE | PURGE …`, after
	/// `ACCESS`.
	pub(crate) fn access_stmt(&mut self, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		let ac = self.name()?;
		let base = if self.eat_kw(Kw::On) {
			self.base()?
		} else {
			0
		};
		// `syn` reports a bad word after the keyword at the keyword.
		let t = self.tok;
		let (op, x, y) = match t.kw {
			_ if t.kind != T::Ident => {
				return Err(self.unexpected("one of GRANT, SHOW, REVOKE or PURGE"));
			}
			Kw::Grant => {
				self.bump();
				self.expect_kw(Kw::For, "FOR")?;
				match self.tok.kw {
					Kw::User if self.tok.kind == T::Ident => {
						self.bump();
						(0, self.name()?, 0)
					}
					Kw::Record if self.tok.kind == T::Ident => {
						self.bump();
						(0, self.record_id_only()?, 1)
					}
					_ => return Err(self.unexpected_at(t, "either USER or RECORD")),
				}
			}
			Kw::Show | Kw::Revoke => {
				self.bump();
				let op = if t.kw == Kw::Show {
					1
				} else {
					2
				};
				match self.tok.kw {
					Kw::All if self.tok.kind == T::Ident => {
						self.bump();
						(op, NONE, 0)
					}
					Kw::Grant if self.tok.kind == T::Ident => {
						self.bump();
						(op, self.name()?, 1)
					}
					Kw::Where if self.tok.kind == T::Ident => {
						self.bump();
						(op, self.expr(true)?, 2)
					}
					_ => return Err(self.unexpected_at(t, "one of ALL, GRANT or WHERE")),
				}
			}
			Kw::Purge => {
				self.bump();
				let mut kinds = 0u32;
				loop {
					let bit = match self.tok.kw {
						Kw::Expired if self.tok.kind == T::Ident => 1,
						Kw::Revoked if self.tok.kind == T::Ident => 2,
						_ => {
							if kinds == 0 {
								return Err(self.unexpected_at(t, "EXPIRED, or REVOKED"));
							}
							break;
						}
					};
					if kinds & bit != 0 || kinds == 3 {
						return Err(self.unexpected_at(t, "ACCESS PURGE statement to end"));
					}
					self.bump();
					kinds |= bit;
					self.eat(T::Comma);
				}
				let grace = if self.eat_kw(Kw::For) {
					self.duration_lit()?
				} else {
					NONE
				};
				(3, grace, kinds)
			}
			_ => return Err(self.unexpected("one of GRANT, SHOW, REVOKE or PURGE")),
		};
		let rec = self.record(&[ac, base, x, y]);
		self.stmt_node(Tag::AccessStmt, op, rec, 0, start, first)
	}
}
