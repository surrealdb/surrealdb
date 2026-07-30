//! Lexing of numeric literals.
//!
//! From `unsignedNumericLiteral` (GQL.g4:2977-3002) and the numeric lexer
//! rules (GQL.g4:3192-3272):
//!
//! - Decimal integers allow single underscore digit separators between digits: `1_000_000`.
//! - `0x`/`0o`/`0b` introduce hex/octal/binary integers; the prefix letters are case-sensitive
//!   (lowercase only), so `0X1` lexes as the number `0` followed by the identifier `X1`, exactly as
//!   the grammar splits it. Each digit may be preceded by a single underscore (`0xdead_beef`,
//!   `0x_1`).
//! - Common notation floats: `123.`, `123.456`, `.456`.
//! - Scientific notation: mantissa, case-insensitive `E`, signed integer exponent. The `E` is only
//!   consumed when an exponent actually follows, so `1e` lexes as `1` followed by the identifier
//!   `e`.
//! - Case-insensitive suffixes: `M` (exact), `F` (float), `D` (double).
//! - No sign: `-3` is the unary minus operator applied to `3`.
//!
//! Maximal munch is followed where the grammar splits adjacent tokens
//! (`123.` is a float even in `123..456`; `0b12` is `0b1` then `2`; `123fish`
//! is `123f` then `ish`), with one documented deviation: a misplaced
//! underscore separator (`1__2`, `1_`, `0x1_`) is a lexical error rather than
//! a token split, for better error messages.

use crate::gql::lexer::Lexer;
use crate::gql::token::{NumberKind, NumberSuffix, Token, TokenKind};
use crate::syn::error::{SyntaxError, syntax_error};
use crate::syn::token::Span;

impl Lexer<'_> {
	/// Lex a numeric literal. The starting digit must already be consumed.
	pub(super) fn lex_number(&mut self, start: u8) -> Token {
		if start == b'0' {
			let prefixed = match self.reader.peek() {
				Some(b'x') => self.lex_prefixed_integer(NumberKind::Hex, |x| x.is_ascii_hexdigit()),
				Some(b'o') => {
					self.lex_prefixed_integer(NumberKind::Octal, |x| matches!(x, b'0'..=b'7'))
				}
				Some(b'b') => {
					self.lex_prefixed_integer(NumberKind::Binary, |x| matches!(x, b'0' | b'1'))
				}
				_ => None,
			};
			if let Some(token) = prefixed {
				return token;
			}
		}
		if let Err(e) = self.eat_separated_digits(|x| x.is_ascii_digit()) {
			return self.invalid_token(e);
		}
		let mut kind = NumberKind::Integer;
		// Common notation: maximal munch means the period is always part of
		// the number, even in `123..456` (`123.` then `.456`) and `123.foo`
		// (`123.` then `foo`).
		if self.eat(b'.') {
			kind = NumberKind::Float;
			if self.eat_when(|x| x.is_ascii_digit())
				&& let Err(e) = self.eat_separated_digits(|x| x.is_ascii_digit())
			{
				return self.invalid_token(e);
			}
		}
		self.lex_number_tail(kind)
	}

	/// Lex a numeric literal in common notation starting with a period, like
	/// `.456`. The period must already be consumed and the next byte must be
	/// a digit.
	pub(super) fn lex_number_starting_period(&mut self) -> Token {
		if let Err(e) = self.eat_separated_digits(|x| x.is_ascii_digit()) {
			return self.invalid_token(e);
		}
		self.lex_number_tail(NumberKind::Float)
	}

	/// Lex the optional exponent and suffix of a decimal numeric literal.
	fn lex_number_tail(&mut self, mut kind: NumberKind) -> Token {
		if matches!(self.reader.peek(), Some(b'e' | b'E')) {
			let backup = self.reader.offset();
			self.reader.next();
			self.eat_when(|x| matches!(x, b'+' | b'-'));
			if self.eat_when(|x| x.is_ascii_digit()) {
				kind = NumberKind::Scientific;
				if let Err(e) = self.eat_separated_digits(|x| x.is_ascii_digit()) {
					return self.invalid_token(e);
				}
			} else {
				// Not an exponent: `1e` lexes as `1` followed by the
				// identifier `e`, like the grammar splits it.
				self.reader.backup(backup);
			}
		}
		let suffix = match self.reader.peek() {
			Some(b'm' | b'M') => Some(NumberSuffix::Exact),
			Some(b'f' | b'F') => Some(NumberSuffix::Float),
			Some(b'd' | b'D') => Some(NumberSuffix::Double),
			_ => None,
		};
		if suffix.is_some() {
			self.reader.next();
		}
		self.finish_token(TokenKind::Number {
			kind,
			suffix,
		})
	}

	/// Lex a `0x`/`0o`/`0b` prefixed integer. The `0` must already be
	/// consumed and the prefix letter must be the next byte.
	///
	/// Returns `None`, without consuming anything, when no digit follows the
	/// prefix: per the grammar `0xg` lexes as the number `0` followed by the
	/// identifier `xg`.
	fn lex_prefixed_integer(
		&mut self,
		kind: NumberKind,
		is_digit: impl Fn(u8) -> bool + Copy,
	) -> Option<Token> {
		let backup = self.reader.offset();
		self.reader.next();
		// `('_'? HEX_DIGIT)+`: the first digit may be preceded by a single
		// underscore (`0x_1` is valid).
		let underscore = self.eat(b'_');
		if !self.eat_when(is_digit) {
			if underscore {
				// `0x_` with no digit would otherwise lex as `0` and the
				// identifier `x_`; report it as a misplaced separator like
				// the other underscore errors.
				let error = self.separator_error();
				return Some(self.invalid_token(error));
			}
			self.reader.backup(backup);
			return None;
		}
		if let Err(e) = self.eat_separated_digits(is_digit) {
			return Some(self.invalid_token(e));
		}
		Some(self.finish_token(TokenKind::Number {
			kind,
			suffix: None,
		}))
	}

	/// Eat a run of digits with optional single underscore separators
	/// between them, per `DIGIT (UNDERSCORE? DIGIT)*`. The first digit must
	/// already be consumed. An underscore not followed by a digit is a
	/// lexical error.
	fn eat_separated_digits(
		&mut self,
		is_digit: impl Fn(u8) -> bool + Copy,
	) -> Result<(), SyntaxError> {
		loop {
			match self.reader.peek() {
				Some(x) if is_digit(x) => {
					self.reader.next();
				}
				Some(b'_') => {
					self.reader.next();
					if !self.eat_when(is_digit) {
						return Err(self.separator_error());
					}
				}
				_ => return Ok(()),
			}
		}
	}

	/// The error for an underscore digit separator which is not followed by
	/// a digit. The reader must be positioned directly after the underscore.
	fn separator_error(&self) -> SyntaxError {
		let span = Span {
			offset: self.reader.offset() - 1,
			len: 1,
		};
		syntax_error!(
			"Invalid numeric literal, underscore digit separators must be followed by a digit",
			@span
		)
	}

	/// Checks if the closure returns true when given the next byte, if it
	/// does it consumes the byte and returns true. Otherwise returns false.
	fn eat_when(&mut self, f: impl FnOnce(u8) -> bool) -> bool {
		let Some(x) = self.reader.peek() else {
			return false;
		};
		if f(x) {
			self.reader.next();
			true
		} else {
			false
		}
	}
}
