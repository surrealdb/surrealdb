use crate::{
	sql::Uuid,
	syn::{
		parser::{
			mac::{expected_whitespace, unexpected},
			ParseError, ParseErrorKind, ParseResult, Parser,
		},
		token::{t, TokenKind},
	},
};

impl Parser<'_> {
	/// Parses a uuid strand.
	pub fn parse_uuid(&mut self) -> ParseResult<Uuid> {
		let quote_token = self.peek_whitespace();

		let double = match quote_token.kind {
			t!("u\"") => true,
			t!("u'") => false,
			x => unexpected!(self, x, "a uuid"),
		};

		self.pop_peek();

		// number of bytes is 4-2-2-2-6

		let mut uuid_buffer = [0u8; 16];

		self.eat_uuid_hex(&mut uuid_buffer[0..4])?;

		expected_whitespace!(self, t!("-"));

		self.eat_uuid_hex(&mut uuid_buffer[4..6])?;

		expected_whitespace!(self, t!("-"));

		self.eat_uuid_hex(&mut uuid_buffer[6..8])?;

		expected_whitespace!(self, t!("-"));

		self.eat_uuid_hex(&mut uuid_buffer[8..10])?;

		expected_whitespace!(self, t!("-"));

		self.eat_uuid_hex(&mut uuid_buffer[10..16])?;

		if double {
			expected_whitespace!(self, t!("\""));
		} else {
			expected_whitespace!(self, t!("'"));
		}

		Ok(Uuid(uuid::Uuid::from_bytes(uuid_buffer)))
	}

	/// Eats a uuid hex section, enough to fill the given buffer with bytes.
	fn eat_uuid_hex(&mut self, buffer: &mut [u8]) -> ParseResult<()> {
		// A function to covert a hex digit to its number representation.
		fn ascii_to_hex(b: u8) -> Option<u8> {
			if (b'0'..=b'9').contains(&b) {
				return Some(b - b'0');
			}

			if (b'a'..=b'f').contains(&b) {
				return Some(b - (b'a' - 10));
			}

			if (b'A'..=b'F').contains(&b) {
				return Some(b - (b'A' - 10));
			}

			None
		}
		// the amounts of character required is twice the buffer len.
		// since every character is half a byte.
		let required_len = buffer.len() * 2;

		// The next token should be digits or an identifier
		// If it is digits an identifier might be after it.
		let start_token = self.peek_whitespace();
		let mut cur = start_token;
		loop {
			match cur.kind {
				TokenKind::Identifier => {
					self.pop_peek();
					break;
				}
				TokenKind::Exponent | TokenKind::Digits => {
					self.pop_peek();
					cur = self.peek_whitespace();
				}
				_ => unexpected!(self, TokenKind::Identifier, "UUID hex digits"),
			}
		}

		// Get the span that covered all eaten tokens.
		let digits_span = start_token.span.covers(cur.span);
		let digits_bytes = self.span_bytes(digits_span);

		// for error handling, the incorrect hex character should be returned first, before
		// returning the not correct length for segment error even if both are valid.
		if !digits_bytes.into_iter().all(|x| matches!(x,b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')) {
			return Err(ParseError::new(
				ParseErrorKind::Unexpected {
					found: TokenKind::Strand,
					expected: "UUID hex digits",
				},
				digits_span,
			));
		}

		if digits_bytes.len() != required_len {
			return Err(ParseError::new(
				ParseErrorKind::InvalidUuidPart {
					len: required_len,
				},
				digits_span,
			));
		}

		// write into the buffer
		for (i, b) in buffer.iter_mut().enumerate() {
			*b = ascii_to_hex(digits_bytes[i * 2]).unwrap() << 4
				| ascii_to_hex(digits_bytes[i * 2 + 1]).unwrap();
		}

		Ok(())
	}
}
