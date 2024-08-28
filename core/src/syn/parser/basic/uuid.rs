use crate::{
	sql::Uuid,
	syn::{
		parser::{
			mac::{expected_whitespace, unexpected},
			ParseError, ParseErrorKind, ParseResult, Parser,
		},
		token::t,
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

		// ensure there are no tokens in the buffer
		self.backup_after(quote_token.span);

		let digits_bytes = self.lexer.reader.by_ref().take(36).collect::<Vec<_>>();

		if double {
			expected_whitespace!(self, t!("\""));
		} else {
			expected_whitespace!(self, t!("'"));
		}

		let span = self.lexer.current_span();

		// mark last_offset as the end of the uuid
		self.lexer.backup_after(span);

		uuid::Uuid::try_parse_ascii(digits_bytes.as_slice())
			.map(|t| Uuid(t))
			.map_err(|e| ParseError::new(ParseErrorKind::InvalidUUID(e), span))
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
		assert_uuid_parses("5a7297d9-c07d-4444-b936-2d984533987d");
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
