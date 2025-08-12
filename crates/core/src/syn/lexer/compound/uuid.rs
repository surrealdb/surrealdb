use uuid::Uuid;

use crate::syn::error::{SyntaxError, bail};
use crate::syn::lexer::Lexer;
use crate::syn::token::{Token, t};

pub fn uuid(lexer: &mut Lexer, start: Token) -> Result<Uuid, SyntaxError> {
	let double = match start.kind {
		t!("u\"") => true,
		t!("u'") => false,
		x => panic!("Invalid start token of uuid compound: {x}"),
	};

	let mut uuid_buffer = [0u8; 16];
	// number of bytes is 4-2-2-2-6

	eat_uuid_hex(lexer, &mut uuid_buffer[0..4])?;

	lexer.expect('-')?;

	eat_uuid_hex(lexer, &mut uuid_buffer[4..6])?;

	lexer.expect('-')?;

	eat_uuid_hex(lexer, &mut uuid_buffer[6..8])?;

	lexer.expect('-')?;

	eat_uuid_hex(lexer, &mut uuid_buffer[8..10])?;

	lexer.expect('-')?;

	eat_uuid_hex(lexer, &mut uuid_buffer[10..16])?;

	if double {
		lexer.expect('"')?;
	} else {
		lexer.expect('\'')?;
	}

	Ok(Uuid::from_bytes(uuid_buffer))
}

fn eat_uuid_hex(lexer: &mut Lexer, buffer: &mut [u8]) -> Result<(), SyntaxError> {
	// the amounts of character required is twice the buffer len.
	// since every character is half a byte.
	for x in buffer {
		let a = eat_hex_character(lexer)?;
		let b = eat_hex_character(lexer)?;
		*x = (a << 4) | b;
	}

	Ok(())
}

fn eat_hex_character(lexer: &mut Lexer) -> Result<u8, SyntaxError> {
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

	let Some(peek) = lexer.reader.peek() else {
		bail!("Unexpected end of file, expected UUID token to finish",@lexer.current_span());
	};
	let Some(res) = ascii_to_hex(peek) else {
		lexer.advance_span();
		let char = lexer.reader.next().unwrap();
		let char = lexer.reader.convert_to_char(char)?;
		bail!("Unexpected character `{char}` expected hexidecimal digit",@lexer.current_span());
	};
	lexer.reader.next();
	Ok(res)
}
