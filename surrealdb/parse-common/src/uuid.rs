use logos::{Lexer, Logos};
use uuid::Uuid;

use crate::Error;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
enum UuidToken {
	#[regex("[0-9a-fA-F]+")]
	Digits,
	#[token("-")]
	Dash,
}

fn parse_hex_byte(byte: u8) -> u8 {
	match byte {
		b'0'..=b'9' => byte - b'0',
		b'a'..=b'f' => byte - b'a' + 10,
		b'A'..=b'F' => byte - b'A' + 10,
		// Enforced by the tokenizer.
		_ => unreachable!(),
	}
}

fn eat_digits(lexer: &mut Lexer<UuidToken>, bytes: &mut [u8]) -> Result<(), Error> {
	match lexer.next() {
		Some(Ok(UuidToken::Digits)) => {
			let span = lexer.span();
			let span_len = span.len();
			if span_len != bytes.len() * 2 {
				return Err(Error {
					span,
					message: format!(
						"Invalid uuid token, invalid number of hex digits, expected {}, found {}",
						bytes.len() * 2,
						span_len
					),
				});
			}

			let slice = lexer.slice().as_bytes();
			for (idx, b) in bytes.iter_mut().enumerate() {
				let u = parse_hex_byte(slice[idx * 2]);
				let l = parse_hex_byte(slice[idx * 2 + 1]);

				*b = (u << 4) | l
			}
		}
		_ => {
			let span = lexer.span();

			return Err(Error {
				span,
				message: "Invalid uuid token, unexpected character, expected hex digit".to_owned(),
			});
		}
	}
	Ok(())
}

fn eat_dash(lexer: &mut Lexer<UuidToken>) -> Result<(), Error> {
	match lexer.next() {
		Some(Ok(UuidToken::Dash)) => {}
		_ => {
			let span = lexer.span();
			return Err(Error {
				span,
				message: "Invalid uuid token, unexpected character, expected `-`".to_owned(),
			});
		}
	}
	Ok(())
}

/// Parse a uuid in its canonical textual format: five groups of hex digits, `8-4-4-4-12`,
/// in any mixture of lower and upper case.
pub fn uuid(text: &str) -> Result<Uuid, Error> {
	let mut lexer = UuidToken::lexer(text);
	let mut buffer = [0u8; 16];

	eat_digits(&mut lexer, &mut buffer[0..4])?;
	eat_dash(&mut lexer)?;
	eat_digits(&mut lexer, &mut buffer[4..6])?;
	eat_dash(&mut lexer)?;
	eat_digits(&mut lexer, &mut buffer[6..8])?;
	eat_dash(&mut lexer)?;
	eat_digits(&mut lexer, &mut buffer[8..10])?;
	eat_dash(&mut lexer)?;
	eat_digits(&mut lexer, &mut buffer[10..16])?;

	Ok(Uuid::from_bytes(buffer))
}

#[cfg(test)]
mod test {
	use super::uuid;

	#[test]
	fn happy_path() {
		let parsed = uuid("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap();
		assert_eq!(parsed.to_string(), "67e55044-10b1-426f-9247-bb680e5fe0c8");

		// Mixed case.
		let parsed = uuid("67E55044-10B1-426f-9247-BB680e5fe0c8").unwrap();
		assert_eq!(parsed.to_string(), "67e55044-10b1-426f-9247-bb680e5fe0c8");
	}

	#[test]
	fn wrong_group_length() {
		let err = uuid("67e5504-10b1-426f-9247-bb680e5fe0c8").unwrap_err();
		assert_eq!(
			err.message,
			"Invalid uuid token, invalid number of hex digits, expected 8, found 7"
		);
		assert_eq!(err.span, 0..7);
	}

	#[test]
	fn non_hex_character() {
		let err = uuid("g7e55044-10b1-426f-9247-bb680e5fe0c8").unwrap_err();
		assert_eq!(err.message, "Invalid uuid token, unexpected character, expected hex digit");
		assert_eq!(err.span, 0..1);
	}

	#[test]
	fn missing_dash() {
		let err = uuid("67e5504410b1426f9247bb680e5fe0c8").unwrap_err();
		assert_eq!(
			err.message,
			"Invalid uuid token, invalid number of hex digits, expected 8, found 32"
		);
	}

	#[test]
	fn truncated() {
		let err = uuid("67e55044-10b1").unwrap_err();
		assert_eq!(err.message, "Invalid uuid token, unexpected character, expected `-`");
	}
}
