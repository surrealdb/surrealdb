//! Implements token gluing logic.

use crate::{
	sql::duration::{
		SECONDS_PER_DAY, SECONDS_PER_HOUR, SECONDS_PER_MINUTE, SECONDS_PER_WEEK, SECONDS_PER_YEAR,
	},
	syn::{
		parser::{mac::unexpected, ParseError, ParseErrorKind, ParseResult, Parser},
		token::{t, DurationSuffix, NumberKind, NumberSuffix, Token, TokenKind},
	},
};

use std::time::Duration as StdDuration;

impl Parser<'_> {
	/// Returns if a token kind can start an identifier.
	pub fn tokenkind_can_start_ident(t: TokenKind) -> bool {
		matches!(
			t,
			TokenKind::Keyword(_)
				| TokenKind::Language(_)
				| TokenKind::Algorithm(_)
				| TokenKind::Distance(_)
				| TokenKind::VectorType(_)
				| TokenKind::Identifier
				| TokenKind::Exponent
				| TokenKind::DatetimeChars(_)
				| TokenKind::NumberSuffix(_)
				| TokenKind::DurationSuffix(
					// All except Micro unicode
					DurationSuffix::Nano
						| DurationSuffix::Micro | DurationSuffix::Milli
						| DurationSuffix::Second | DurationSuffix::Minute
						| DurationSuffix::Hour | DurationSuffix::Day
						| DurationSuffix::Week | DurationSuffix::Year
				)
		)
	}

	/// Returns if a token kind can start continue an identifier.
	pub fn tokenkind_continues_ident(t: TokenKind) -> bool {
		matches!(
			t,
			TokenKind::Keyword(_)
				| TokenKind::Language(_)
				| TokenKind::Algorithm(_)
				| TokenKind::Distance(_)
				| TokenKind::VectorType(_)
				| TokenKind::Identifier
				| TokenKind::DatetimeChars(_)
				| TokenKind::Exponent
				| TokenKind::NumberSuffix(_)
				| TokenKind::NaN | TokenKind::DurationSuffix(
				// All except Micro unicode
				DurationSuffix::Nano
					| DurationSuffix::Micro
					| DurationSuffix::Milli
					| DurationSuffix::Second
					| DurationSuffix::Minute
					| DurationSuffix::Hour
					| DurationSuffix::Day
					| DurationSuffix::Week
			)
		)
	}

	/// Returns if the peeked token can be a identifier.
	pub fn peek_can_start_ident(&mut self) -> bool {
		Self::tokenkind_can_start_ident(self.peek_kind())
	}

	/// Returns if the peeked token can be a identifier.
	pub fn peek_continues_ident(&mut self) -> bool {
		Self::tokenkind_can_start_ident(self.peek_kind())
	}

	/// Glue an token and immediately consume it.
	pub fn glue_next(&mut self) -> ParseResult<Token> {
		self.glue()?;
		Ok(self.next())
	}

	/// Glues the next token together, returning its value, doesnt consume the token.
	pub fn glue(&mut self) -> ParseResult<Token> {
		let token = self.peek();
		match token.kind {
			TokenKind::Exponent
			| TokenKind::NumberSuffix(_)
			| TokenKind::DurationSuffix(_)
			| TokenKind::DatetimeChars(_) => self.glue_ident(false),
			TokenKind::Digits => self.glue_numeric(),
			t!("\"") | t!("'") => {
				self.pop_peek();
				let t = self.lexer.relex_strand(token);
				let TokenKind::Strand = t.kind else {
					unexpected!(self, t.kind, "a strand")
				};
				self.prepend_token(t);
				Ok(t)
			}
			t!("+") | t!("-") => {
				if let TokenKind::Digits = self.peek_whitespace_token_at(1).kind {
					self.glue_number()
				} else {
					Ok(token)
				}
			}
			_ => Ok(token),
		}
	}

	/// Glues all next tokens follow eachother, which can make up an ident into a single string.
	pub fn glue_ident(&mut self, flexible: bool) -> ParseResult<Token> {
		let start = self.peek();

		let mut token_buffer = match start.kind {
			TokenKind::Exponent | TokenKind::NumberSuffix(_) => {
				self.pop_peek();

				self.span_str(start.span).to_owned()
			}
			TokenKind::Digits if flexible => {
				self.pop_peek();
				self.span_str(start.span).to_owned()
			}
			TokenKind::DurationSuffix(x) if x.can_be_ident() => {
				self.pop_peek();

				self.span_str(start.span).to_owned()
			}
			TokenKind::DatetimeChars(_) => {
				self.pop_peek();

				self.span_str(start.span).to_owned()
			}
			_ => return Ok(start),
		};

		debug_assert!(
			start.is_followed_by(&self.peek_whitespace()),
			"a whitespace token was eaten where eating it would disturb parsing\n {:?}@{:?} => {:?}@{:?}",
			start.kind,
			start.span,
			self.peek_whitespace().kind,
			self.peek_whitespace().span
		);

		let mut prev = start;
		loop {
			let p = self.peek_whitespace();
			match p.kind {
				// These token_kinds always complete an ident, no more identifier parts can happen
				// after this.
				TokenKind::Identifier => {
					self.pop_peek();
					let buffer = self.lexer.string.take().unwrap();
					token_buffer.push_str(&buffer);
					prev = p;
					break;
				}
				TokenKind::Keyword(_)
				| TokenKind::Language(_)
				| TokenKind::Algorithm(_)
				| TokenKind::Distance(_)
				| TokenKind::VectorType(_)
				| TokenKind::NumberSuffix(_) => {
					self.pop_peek();
					let str = self.span_str(p.span);
					token_buffer.push_str(str);

					prev = p;

					break;
				}
				// These tokens might have some more parts following them
				TokenKind::Exponent | TokenKind::DatetimeChars(_) | TokenKind::Digits => {
					self.pop_peek();
					let str = self.span_str(p.span);
					token_buffer.push_str(str);

					prev = p;
				}
				TokenKind::DurationSuffix(suffix) => {
					self.pop_peek();
					if !suffix.can_be_ident() {
						return Err(ParseError::new(ParseErrorKind::InvalidIdent, p.span));
					}
					token_buffer.push_str(suffix.as_str());
					prev = p;
				}
				_ => break,
			}
		}

		let token = Token {
			kind: TokenKind::Identifier,
			span: start.span.covers(prev.span),
		};

		self.lexer.string = Some(token_buffer);
		self.prepend_token(token);

		Ok(token)
	}

	pub fn glue_numeric(&mut self) -> ParseResult<Token> {
		let peek = self.peek();
		match peek.kind {
			TokenKind::Digits => {
				if matches!(self.peek_whitespace_token_at(1).kind, TokenKind::DurationSuffix(_)) {
					return self.glue_duration();
				}
				self.glue_number()
			}
			t!("+") | t!("-") => self.glue_number(),
			_ => Ok(peek),
		}
	}

	pub fn glue_number(&mut self) -> ParseResult<Token> {
		let start = self.peek();

		match start.kind {
			t!("+") | t!("-") => {
				self.pop_peek();

				debug_assert!(
					start.is_followed_by(&self.peek_whitespace()),
					"a whitespace token was eaten where eating it would disturb parsing\n {:?}@{:?} => {:?}@{:?}",
					start.kind,
					start.span,
					self.peek_whitespace().kind,
					self.peek_whitespace().span
				);

				let n = self.peek_whitespace();

				if n.kind != TokenKind::Digits {
					unexpected!(self, start.kind, "a number")
				}

				self.pop_peek();
			}
			TokenKind::Digits => {
				self.pop_peek();
				debug_assert!(
					start.is_followed_by(&self.peek_whitespace()),
					"a whitespace token was eaten where eating it would disturb parsing\n {:?}@{:?} => {:?}@{:?}",
					start.kind,
					start.span,
					self.peek_whitespace().kind,
					self.peek_whitespace().span
				);
			}
			_ => return Ok(start),
		};

		let mut kind = NumberKind::Integer;

		// Check for mantissa
		if let t!(".") = self.peek_whitespace().kind {
			self.pop_peek();
			let next = self.peek_whitespace();
			if next.kind != TokenKind::Digits {
				unexpected!(self, next.kind, "digits after the dot");
			}
			self.pop_peek();
			kind = NumberKind::Float;
		}

		// Check for exponent
		if let TokenKind::Exponent = self.peek_whitespace().kind {
			self.pop_peek();
			let exponent_token = self.peek_whitespace();
			match exponent_token.kind {
				t!("+") | t!("-") => {
					self.pop_peek();
					let exponent_token = self.peek_whitespace();
					if exponent_token.kind != TokenKind::Digits {
						unexpected!(self, exponent_token.kind, "digits after the exponent")
					}
				}
				TokenKind::Digits => {}
				x => unexpected!(self, x, "digits after the exponent"),
			}
			self.pop_peek();
			kind = NumberKind::Float;
		}

		// Check for number suffix
		let suffix_token = self.peek_whitespace();
		if let TokenKind::NumberSuffix(suffix) = suffix_token.kind {
			self.pop_peek();
			match suffix {
				NumberSuffix::Float => {
					kind = NumberKind::Float;
				}
				NumberSuffix::Decimal => {
					kind = NumberKind::Decimal;
				}
			}
		}

		// Check that no ident-like identifiers follow
		let next = self.peek_whitespace();
		if Self::tokenkind_continues_ident(next.kind) {
			unexpected!(self, next.kind, "number to end")
		}

		let token = Token {
			kind: TokenKind::Number(kind),
			span: start.span.covers(self.last_span()),
		};

		self.prepend_token(token);

		Ok(token)
	}

	pub fn glue_duration(&mut self) -> ParseResult<Token> {
		let mut duration = StdDuration::ZERO;

		let start = self.peek();
		match start.kind {
			TokenKind::Digits => {
				self.pop_peek();
			}
			_ => return Ok(start),
		};

		debug_assert!(
			start.is_followed_by(&self.peek_whitespace()),
			"a whitespace token was eaten where eating it would disturb parsing"
		);

		let mut cur = start;
		loop {
			let p = self.peek_whitespace();

			let suffix = match p.kind {
				TokenKind::DurationSuffix(x) => x,
				x => unexpected!(self, x, "a duration suffix"),
			};

			self.pop_peek();

			let digits_str = self.span_str(cur.span);
			let digits_value: u64 = digits_str
				.parse()
				.map_err(ParseErrorKind::InvalidInteger)
				.map_err(|e| ParseError::new(e, p.span))?;

			let addition = match suffix {
				DurationSuffix::Nano => StdDuration::from_nanos(digits_value),
				DurationSuffix::Micro | DurationSuffix::MicroUnicode => {
					StdDuration::from_micros(digits_value)
				}
				DurationSuffix::Milli => StdDuration::from_millis(digits_value),
				DurationSuffix::Second => StdDuration::from_secs(digits_value),
				DurationSuffix::Minute => {
					let minutes =
						digits_value.checked_mul(SECONDS_PER_MINUTE).ok_or_else(|| {
							let span = start.span.covers(p.span);
							ParseError::new(ParseErrorKind::DurationOverflow, span)
						})?;
					StdDuration::from_secs(minutes)
				}
				DurationSuffix::Hour => {
					let hours = digits_value.checked_mul(SECONDS_PER_HOUR).ok_or_else(|| {
						let span = start.span.covers(p.span);
						ParseError::new(ParseErrorKind::DurationOverflow, span)
					})?;
					StdDuration::from_secs(hours)
				}
				DurationSuffix::Day => {
					let days = digits_value.checked_mul(SECONDS_PER_DAY).ok_or_else(|| {
						let span = start.span.covers(p.span);
						ParseError::new(ParseErrorKind::DurationOverflow, span)
					})?;
					StdDuration::from_secs(days)
				}
				DurationSuffix::Week => {
					let weeks = digits_value.checked_mul(SECONDS_PER_WEEK).ok_or_else(|| {
						let span = start.span.covers(p.span);
						ParseError::new(ParseErrorKind::DurationOverflow, span)
					})?;
					StdDuration::from_secs(weeks)
				}
				DurationSuffix::Year => {
					let years = digits_value.checked_mul(SECONDS_PER_YEAR).ok_or_else(|| {
						let span = start.span.covers(p.span);
						ParseError::new(ParseErrorKind::DurationOverflow, span)
					})?;
					StdDuration::from_secs(years)
				}
			};

			duration = duration.checked_add(addition).ok_or_else(|| {
				let span = start.span.covers(p.span);
				ParseError::new(ParseErrorKind::DurationOverflow, span)
			})?;

			match self.peek_whitespace().kind {
				TokenKind::Digits => {
					cur = self.pop_peek();
				}
				x if Parser::tokenkind_continues_ident(x) => {
					let span = start.span.covers(p.span);
					unexpected!(@span, self, x, "a duration")
				}
				_ => break,
			}
		}

		let span = start.span.covers(cur.span);
		let token = Token {
			kind: TokenKind::Duration,
			span,
		};

		self.lexer.duration = Some(duration);
		self.prepend_token(token);

		Ok(token)
	}

	/// Glues the next tokens which would make up a float together into a single buffer.
	/// Return err if the tokens would return a invalid float.
	pub fn glue_float(&mut self) -> ParseResult<Token> {
		let start = self.peek();

		match start.kind {
			t!("+") | t!("-") => {
				self.pop_peek();

				debug_assert!(
					start.is_followed_by(&self.peek_whitespace()),
					"a whitespace token was eaten where eating it would disturb parsing"
				);

				let digits_token = self.peek_whitespace();
				if TokenKind::Digits != digits_token.kind {
					let span = start.span.covers(digits_token.span);
					unexpected!(@span, self,digits_token.kind, "a floating point number")
				}
				self.pop_peek();
			}
			TokenKind::Digits => {
				self.pop_peek();

				debug_assert!(
					start.is_followed_by(&self.peek_whitespace()),
					"a whitespace token was eaten where eating it would disturb parsing"
				);
			}
			TokenKind::NumberSuffix(NumberSuffix::Float) => {
				return Ok(start);
			}
			_ => return Ok(start),
		}

		// check for mantissa
		if let t!(".") = self.peek_whitespace().kind {
			self.pop_peek();
			let digits_token = self.peek_whitespace();
			if TokenKind::Digits != digits_token.kind {
				unexpected!(self, digits_token.kind, "a floating point number")
			}
			self.pop_peek();
		};

		// check for exponent
		if let TokenKind::Exponent = self.peek_whitespace().kind {
			self.pop_peek();
			let mut digits_token = self.peek_whitespace();

			if let t!("+") | t!("-") = digits_token.kind {
				self.pop_peek();
				digits_token = self.peek_whitespace();
			}

			if TokenKind::Digits != digits_token.kind {
				unexpected!(self, digits_token.kind, "a floating point number")
			}
			self.pop_peek();
		}

		// check for exponent
		if let TokenKind::NumberSuffix(suffix) = self.peek_whitespace().kind {
			match suffix {
				NumberSuffix::Float => {
					self.pop_peek();
				}
				NumberSuffix::Decimal => {
					unexpected!(self, t!("dec"), "a floating point number")
				}
			}
		}

		let t = self.peek_whitespace();
		if Self::tokenkind_continues_ident(t.kind) {
			unexpected!(self, t.kind, "a floating point number to end")
		}

		let span = start.span.covers(self.last_span());
		let token = Token {
			kind: TokenKind::Number(NumberKind::Float),
			span,
		};

		self.prepend_token(token);

		Ok(token)
	}

	pub fn glue_plain_strand(&mut self) -> ParseResult<Token> {
		let start = self.peek();
		match start.kind {
			t!("\"") | t!("'") => {}
			_ => return Ok(start),
		};

		let token = self.lexer.relex_strand(start);
		self.prepend_token(token);
		Ok(token)
	}
}
