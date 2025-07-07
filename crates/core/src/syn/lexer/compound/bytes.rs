use crate::syn::error::{SyntaxError, bail};
use crate::syn::lexer::Lexer;
use crate::syn::token::{Token, t};
use crate::val::Bytes;

pub fn bytes(lexer: &mut Lexer, start: Token) -> Result<Bytes, SyntaxError> {
	let close_char = match start.kind {
		t!("b\"") => b'"',
		t!("b'") => b'\'',
		x => panic!("Invalid start token of bytes compound: {x}"),
	};

	let mut bytes: Vec<u8> = Vec::new();

	loop {
		if lexer.eat(close_char) {
			return Ok(Bytes(bytes));
		} else {
			bytes.push(eat_hex_byte(lexer)?);
		}
	}
}

fn eat_hex_byte(lexer: &mut Lexer) -> Result<u8, SyntaxError> {
	// Get the first hex digit
	let high_nibble = eat_hex_character(lexer)?;
	// Get the second hex digit
	let low_nibble = eat_hex_character(lexer)?;

	// Combine both nibbles into a single byte (high << 4 | low)
	Ok((high_nibble << 4) | low_nibble)
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
		bail!("Unexpected end of file, expected byte to finish",@lexer.current_span());
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
