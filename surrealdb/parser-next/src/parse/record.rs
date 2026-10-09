use super::*;

impl<'a, 's> Parser<'a, 's> {
	// -------------------------------------------------------------- record ids

	/// `table:key`, with `table` the current token. With `ranges`, as an
	/// expression reads it: no whitespace before the `:`, and the key may be a
	/// range. Without, as a record id outside an expression reads it.
	pub(crate) fn record_id(&mut self, table_tok: Tok, ranges: bool) -> R<NodeId> {
		self.bump();
		let colon = self.bump();
		if colon.ws() && ranges {
			return Err(self.diag(
				Diag::message(table_tok.start, table_tok.end, "Unexpected whitespace")
					.with_label("No whitespace allowed after this token"),
			));
		}
		let table = self.leaf(Tag::Name, (table_tok.kind == T::EscIdent) as u8, table_tok);
		let start = table_tok.start;
		if ranges && self.tok.kind == T::DotDot && !self.tok.ws() {
			return self.record_range(table, start, NONE, 0).map(|v| v.unwrap_or(PUSHED));
		}
		if self.tok.ws() {
			if !ranges {
				return Err(self.key_after_whitespace());
			}
			return Err(self.unexpected("a record-id key"));
		}
		match self.tok.kind {
			T::LBracket => {
				let open = self.bump();
				if self.eat(T::RBracket) {
					let idx = self.empty_list();
					let v = self.node(Tag::Array, 0, idx, 0, open.start, 0)?;
					return self
						.record_after_key(table, start, v, ranges)
						.map(|v| v.unwrap_or(PUSHED));
				}
				self.push_frame(Frame::Array {
					start: open.start,
					mark: self.vals.len() as u32,
					wrap: Wrap::KeyStart {
						table,
						start,
						ranges,
					},
				})?;
				Ok(PUSHED)
			}
			T::LBrace => {
				let s = self.tok.start;
				self.object_open(
					s,
					Wrap::KeyStart {
						table,
						start,
						ranges,
					},
				)
			}
			// In an expression what starts a key is checked before reading one.
			_ if ranges && !self.starts_key() && self.tok.kind != T::Param => {
				Err(self.unexpected("a record-id key"))
			}
			// Outside an expression a key is read with no `$param` hint.
			T::Param if !ranges => Err(self.unexpected("an identifier")),
			_ => {
				let key = self.simple_key()?;
				self.record_after_key(table, start, key, ranges).map(|v| v.unwrap_or(PUSHED))
			}
		}
	}

	/// Whether the current token starts a record-id key: a plain or uuid
	/// string does too, though only the uuid string makes a valid one.
	fn starts_key(&self) -> bool {
		matches!(
			self.tok.kind,
			T::Ident | T::EscIdent | T::Digits | T::LBracket | T::LBrace | T::Plus | T::Minus
		) || (self.tok.kind == T::Str && matches!(self.tok.aux(), 0 | b's' | b'u'))
	}

	/// After a record-id key: a range, or the finished record id.
	pub(crate) fn record_after_key(
		&mut self,
		table: NodeId,
		start: u32,
		key: NodeId,
		ranges: bool,
	) -> R<Option<NodeId>> {
		if !ranges {
			let d = self.depth_of(key);
			return self.node(Tag::RecordId, 0, table, key, start, d).map(Some);
		}
		let mut flags = 0;
		// A `>` directly after the key makes it an excluded range start,
		// which a `..` must then follow.
		if self.tok.kind == T::Gt && !self.tok.ws() {
			self.bump();
			flags |= 1;
			if self.tok.kind != T::DotDot || self.tok.ws() {
				return Err(self.unexpected("the range operator `..`"));
			}
		}
		if self.tok.kind == T::DotDot && !self.tok.ws() {
			return self.record_range(table, start, key, flags);
		}
		let d = self.depth_of(key);
		self.node(Tag::RecordId, 0, table, key, start, d).map(Some)
	}

	/// At the `..` of a record-id range with lower key `lo`.
	pub(crate) fn record_range(
		&mut self,
		table: NodeId,
		start: u32,
		lo: NodeId,
		mut flags: u8,
	) -> R<Option<NodeId>> {
		self.bump();
		if self.tok.kind == T::Eq && !self.tok.ws() {
			self.bump();
			flags |= 2;
		}
		if self.tok.ws() || !self.starts_key() {
			if flags & 2 != 0 {
				// Any token after `..=` is read as a key, and the established
				// error names an identifier as what the fallback expects.
				if self.tok.ws() {
					return Err(self.key_after_whitespace());
				}
				return Err(self.unexpected("an identifier"));
			}
			let range = self.node(Tag::KeyRange, flags, lo, NONE, start, 2)?;
			let d = self.depth_of(range);
			return self.node(Tag::RecordId, 0, table, range, start, d).map(Some);
		}
		match self.tok.kind {
			T::LBracket => {
				let open = self.bump();
				let wrap = Wrap::KeyEnd {
					table,
					start,
					lo,
					flags,
				};
				if self.eat(T::RBracket) {
					let idx = self.empty_list();
					let v = self.node(Tag::Array, 0, idx, 0, open.start, 0)?;
					return self.wrap(v, wrap);
				}
				self.push_frame(Frame::Array {
					start: open.start,
					mark: self.vals.len() as u32,
					wrap,
				})?;
				Ok(None)
			}
			T::LBrace => {
				let open = self.tok.start;
				let v = self.object_open(
					open,
					Wrap::KeyEnd {
						table,
						start,
						lo,
						flags,
					},
				)?;
				Ok((v != PUSHED).then_some(v))
			}
			_ => {
				let hi = self.simple_key()?;
				let range = self.node(Tag::KeyRange, flags, lo, hi, start, 2)?;
				self.node(Tag::RecordId, 0, table, range, start, 3).map(Some)
			}
		}
	}

	/// Whether `n`, an identifier joined to digits, is only `_` and digits:
	/// they have always lexed as part of the digits token, so even strict
	/// keys take them.
	fn digits_continue(&self, n: Tok) -> bool {
		n.kind == T::Ident
			&& self.src[n.start as usize..n.end as usize]
				.iter()
				.all(|&b| b == b'_' || b.is_ascii_digit())
	}

	/// A record-id key that is not an array or object.
	pub(crate) fn simple_key(&mut self) -> R<NodeId> {
		let t = self.tok;
		match t.kind {
			T::Digits => {
				// Digits joined to an identifier (`1abc`, ``1`x` ``) form one
				// string key, spelled as written.
				if let Some(n) = self.adjacent1()
					&& matches!(n.kind, T::Ident | T::EscIdent | T::NaN | T::Infinity)
					&& (!self.limits.strict_record_ids || self.digits_continue(n))
				{
					self.bump();
					self.bump();
					return Ok(self.leaf_span(Tag::KeyStr, 3, 0, 0, t.start, n.end));
				}
				// A strict key ends where the digits token does, after any
				// `_` and digits that open the joined identifier; with an `_`
				// it is a string key.
				if let Some(n) = self.adjacent1()
					&& n.kind == T::Ident
					&& self.src[n.start as usize] == b'_'
				{
					let mut dend = n.start as usize;
					while self.src.get(dend).is_some_and(|&b| b.is_ascii_digit() || b == b'_') {
						dend += 1;
					}
					let dend = dend as u32;
					self.reset_to(dend);
					self.prev_start = t.start;
					self.prev_end = dend;
					return Ok(self.leaf_span(Tag::KeyStr, 0, 0, 0, t.start, dend));
				}
				self.bump();
				let text = &self.src[t.start as usize..t.end as usize];
				match crate::lex::decode_uint(text) {
					Some(u) if u <= i64::MAX as u64 => Ok(self.leaf_span(
						Tag::KeyInt,
						0,
						u as u32,
						(u >> 32) as u32,
						t.start,
						t.end,
					)),
					_ => Ok(self.leaf_span(Tag::KeyStr, 0, 0, 0, t.start, t.end)),
				}
			}
			T::Plus | T::Minus => {
				self.bump();
				let neg = t.kind == T::Minus;
				let d = self.tok;
				// After `+` the digits must follow directly; after `-` they may not.
				if neg && d.kind != T::Digits {
					return Err(self.unexpected("a number"));
				}
				if !neg && d.ws() {
					return Err(self.unexpected_at(t, "a record-id key"));
				}
				if d.kind != T::Digits {
					return Err(self.unexpected("an integer"));
				}
				// The digits token runs on over `_`, as it always has.
				let mut dend = d.end as usize;
				while self.src.get(dend).is_some_and(|&b| b.is_ascii_digit() || b == b'_') {
					dend += 1;
				}
				let dend = dend as u32;
				if !neg {
					// The digits after `+` are read without consuming them, so
					// the key is their number and they remain the next token.
					let text = &self.src[d.start as usize..dend as usize];
					// Read as written, so `_` makes the key a string.
					let number = if text.contains(&b'_') {
						None
					} else {
						crate::lex::decode_uint(text)
					};
					return Ok(match number {
						Some(u) if u <= i64::MAX as u64 => self.leaf_span(
							Tag::KeyInt,
							0,
							u as u32,
							(u >> 32) as u32,
							t.start,
							dend,
						),
						_ => self.leaf_span(Tag::KeyStr, 3, 0, 0, d.start, dend),
					});
				}
				self.reset_to(dend);
				self.prev_start = d.start;
				self.prev_end = dend;
				let text = &self.src[d.start as usize..dend as usize];
				if self.src.get(dend as usize).is_some_and(|b| b.is_ascii_alphabetic()) {
					// A letter after the digits ends a failed integer and is
					// dropped, as earlier releases read it: the key is the
					// string `-digits`.
					self.reset_to(dend + 1);
					self.prev_start = d.start;
					self.prev_end = dend + 1;
					return Ok(self.leaf_span(Tag::KeyStr, 2, 0, 0, d.start, dend));
				}
				let u = crate::lex::decode_uint(text);
				let v = match u {
					Some(u) if !neg && u <= i64::MAX as u64 => u,
					Some(u) if neg && u <= i64::MAX as u64 + 1 => (u as i64).wrapping_neg() as u64,
					// Out of range: the digits become a string key, with the
					// `-` kept (`op` 2) and the `+` dropped.
					_ => {
						return Ok(self.leaf_span(Tag::KeyStr, 2 * neg as u8, 0, 0, d.start, dend));
					}
				};
				Ok(self.leaf_span(Tag::KeyInt, 0, v as u32, (v >> 32) as u32, t.start, dend))
			}
			T::Ident => {
				self.bump();
				let gen_kind = match t.kw {
					Kw::Ulid => Some(0),
					Kw::Uuid => Some(1),
					Kw::Rand => Some(2),
					_ => None,
				};
				if let Some(g) = gen_kind
					&& self.at(T::LParen)
				{
					self.bump();
					self.expect(T::RParen, ")")?;
					return Ok(self.leaf_span(Tag::KeyGen, g, 0, 0, t.start, self.prev_end));
				}
				Ok(self.leaf(Tag::KeyStr, 0, t))
			}
			T::EscIdent => {
				self.bump();
				Ok(self.leaf(Tag::KeyStr, 1, t))
			}
			T::Str if t.aux() == b'u' => {
				self.bump();
				Ok(self.leaf(Tag::KeyUuid, 0, t))
			}
			T::Param => Err(self.diag(
				Diag::message(t.start, t.end, "Unexpected token `$param` expected a record-id key")
					.with_label(
						"Record-id's can be create from a param with `type::record(\"{table}\",{text})`",
					),
			)),
			_ => Err(self.unexpected("an identifier")),
		}
	}
}
