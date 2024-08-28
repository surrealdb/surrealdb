use crate::{
	sql::Uuid,
	syn::{
		parser::{
			mac::{expected_whitespace, unexpected},
			ParseError, ParseErrorKind, ParseResult, Parser,
		},
		token::{t, DurationSuffix, NumberSuffix, TokenKind, VectorTypeKind},
	},
};

impl Parser<'_> {
	/// Parses a uuid strand.
	pub fn parse_uuid(&mut self) -> ParseResult<Uuid> {
		let quote_token = self.peek();

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
			if b.is_ascii_digit() {
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
			let next = self.peek_whitespace();
			match next.kind {
				TokenKind::Identifier => {
					cur = self.pop_peek();
					break;
				}
				TokenKind::Exponent
				| TokenKind::Digits
				| TokenKind::DurationSuffix(DurationSuffix::Day)
				| TokenKind::NumberSuffix(NumberSuffix::Float) => {
					cur = self.pop_peek();
				}
				TokenKind::Language(_)
				| TokenKind::Keyword(_)
				| TokenKind::VectorType(VectorTypeKind::F64 | VectorTypeKind::F32) => {
					// there are some keywords and languages keywords which could be part of the
					// hex section.
					if !self.span_bytes(next.span).iter().all(|x| x.is_ascii_hexdigit()) {
						unexpected!(self, TokenKind::Identifier, "UUID hex digits");
					}
					cur = self.pop_peek();
					break;
				}
				t!("-") | t!("\"") | t!("'") => break,
				_ => unexpected!(self, TokenKind::Identifier, "UUID hex digits"),
			}
		}

		// Get the span that covered all eaten tokens.
		let digits_span = start_token.span.covers(cur.span);
		let digits_bytes = self.span_str(digits_span).as_bytes();

		// for error handling, the incorrect hex character should be returned first, before
		// returning the not correct length for segment error even if both are valid.
		if !digits_bytes.iter().all(|x| x.is_ascii_hexdigit()) {
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

#[cfg(test)]
mod test {
	use crate::syn::parser::Parser;

	#[test]
	fn uuid_parsing() {
		fn assert_uuid_parses(s: &str) {
			let uuid_str = format!("u'{s}'");
			let mut parser = Parser::new(uuid_str.as_bytes());
			let uuid = parser.parse_uuid().unwrap();
			assert_eq!(uuid::Uuid::parse_str(s).unwrap(), *uuid);
		}

		assert_uuid_parses("0531956f-20ec-4575-bb68-3e6b49d813fa");
		assert_uuid_parses("0531956d-20ec-4575-bb68-3e6b49d813fa");
		assert_uuid_parses("0531956e-20ec-4575-bb68-3e6b49d813fa");
		assert_uuid_parses("0531956a-20ec-4575-bb68-3e6b49d813fa");
		assert_uuid_parses("053195f1-20ec-4575-bb68-3e6b49d813fa");
		assert_uuid_parses("053195d1-20ec-4575-bb68-3e6b49d813fa");
		assert_uuid_parses("053195e1-20ec-4575-bb68-3e6b49d813fa");
		assert_uuid_parses("053195a1-20ec-4575-bb68-3e6b49d813fa");
		assert_uuid_parses("f0531951-20ec-4575-bb68-3e6b49d813fa");
		assert_uuid_parses("d0531951-20ec-4575-bb68-3e6b49d813fa");
		assert_uuid_parses("e0531951-20ec-4575-bb68-3e6b49d813fa");
		assert_uuid_parses("a0531951-20ec-4575-bb68-3e6b49d813fa");
		assert_uuid_parses("b98839b9-0471-4dbb-aae0-14780e848f32");
	}

	#[test]
	fn test_uuid_characters() {
		let hex_characters =
			[b'0', b'a', b'b', b'c', b'd', b'e', b'f', b'A', b'B', b'C', b'D', b'E', b'F'];

		let mut uuid_string: Vec<u8> = "u'0531956f-20ec-4575-bb68-3e6b49d813fa'".to_string().into();

		fn assert_uuid_parses(s: &[u8]) {
			let mut parser = Parser::new(s);
			parser.parse_uuid().unwrap();
		}

		for i in hex_characters.iter() {
			for j in hex_characters.iter() {
				for k in hex_characters.iter() {
					uuid_string[3] = *i;
					uuid_string[4] = *j;
					uuid_string[5] = *k;

					assert_uuid_parses(&uuid_string)
				}
			}
		}
	}
}
