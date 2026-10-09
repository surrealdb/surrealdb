//! Literal tokens in positions that take one kind of literal only: integers,
//! durations, strings. These follow `syn`'s `next_token_value` rules.

use super::*;

impl<'a, 's> Parser<'a, 's> {
	/// An unsigned integer: digits, optionally after an adjacent `+`.
	pub(crate) fn uint_lit(&mut self) -> R<NodeId> {
		let t = self.tok;
		let signed = match t.kind {
			T::Digits => false,
			T::Plus => {
				self.bump();
				if self.tok.kind != T::Digits || self.tok.ws() {
					return Err(self.unexpected("an unsigned integer"));
				}
				true
			}
			T::Minus => return Err(self.unexpected("only positive integers are allowed here")),
			_ => return Err(self.unexpected("an unsigned integer")),
		};
		let n = self.number_with(t.start, signed, true)?;
		self.only_int(n)
	}

	/// A signed integer: digits, optionally after an adjacent sign.
	pub(crate) fn int_lit(&mut self) -> R<NodeId> {
		let t = self.tok;
		let signed = match t.kind {
			T::Digits => false,
			T::Plus | T::Minus => {
				self.bump();
				if self.tok.kind != T::Digits || self.tok.ws() {
					return Err(self.unexpected("an integer"));
				}
				true
			}
			_ => return Err(self.unexpected("an signed integer")),
		};
		let n = self.number_in(t.start, signed, false, Compound::Integer, t.start)?;
		self.only_int(n)
	}

	fn only_int(&mut self, n: NodeId) -> R<NodeId> {
		let node = self.ast.nodes[n as usize];
		if node.tag != Tag::Int {
			return Err(self.error(node.start, node.end, "only integers are allowed here"));
		}
		Ok(n)
	}

	/// A floating-point number (`syn`'s `f32`/`f64` token value): digits with
	/// an optional fraction and exponent, an adjacent sign, `NaN` or
	/// `Infinity`.
	pub(crate) fn float_lit(&mut self) -> R<NodeId> {
		self.numeric_lit(false)
	}

	/// A number (`syn`'s `Number` token value): an integer, float or decimal.
	pub(crate) fn number_lit(&mut self) -> R<NodeId> {
		self.numeric_lit(true)
	}

	fn numeric_lit(&mut self, decimal: bool) -> R<NodeId> {
		let t = self.tok;
		let signed = match t.kind {
			T::NaN | T::Infinity => {
				self.bump();
				let tag = if t.kind == T::NaN {
					Tag::NaN
				} else {
					Tag::Infinity
				};
				return Ok(self.leaf(tag, 0, t));
			}
			T::Digits => false,
			T::Plus | T::Minus => {
				self.bump();
				if self.tok.kind != T::Digits || self.tok.ws() {
					let mode = if decimal {
						Compound::Number
					} else {
						Compound::Float
					};
					return Err(self.number_error(t.start, LexError::InvalidNumber, mode, t.start));
				}
				true
			}
			_ if decimal => return Err(self.unexpected("a number")),
			_ => return Err(self.unexpected("a floating point number")),
		};
		let mode = if decimal {
			Compound::Number
		} else {
			Compound::Float
		};
		let n = self.number_in(t.start, signed, false, mode, t.start)?;
		let node = self.ast.nodes[n as usize];
		let ok = match node.tag {
			Tag::Int | Tag::Float => true,
			Tag::Decimal => decimal,
			_ => false,
		};
		if !ok {
			return Err(self.error(node.start, node.end, "expected a number"));
		}
		Ok(n)
	}

	/// A duration literal.
	pub(crate) fn duration_lit(&mut self) -> R<NodeId> {
		if self.tok.kind != T::Digits {
			return Err(self.unexpected("a duration"));
		}
		let at = self.tok.start;
		let n = self.number_in(at, false, false, Compound::Duration, at)?;
		let node = self.ast.nodes[n as usize];
		if node.tag != Tag::Duration {
			return Err(self.error(node.start, node.end, "expected a duration"));
		}
		Ok(n)
	}

	/// A plain string literal, without a prefix.
	pub(crate) fn string_lit(&mut self) -> R<NodeId> {
		let t = self.tok;
		if t.kind != T::Str || t.aux() != 0 {
			return Err(self.unexpected("a strand"));
		}
		self.bump();
		Ok(self.leaf(Tag::Str, 0, t))
	}
}
