use crate::Error;

/// Parse a bytes string: pairs of hex digits, each pair encoding a single byte.
///
/// Error spans are byte offsets into `source`; since every valid character is ASCII an offset
/// always points at the offending character.
pub fn bytes(source: &str) -> Result<Vec<u8>, Error> {
	let mut res = Vec::new();
	let mut chars = source.chars();

	let mut offset = 0;
	while let Some(a) = chars.next() {
		let first = match a {
			'a'..='f' => (a as u8) - b'a' + 10,
			'A'..='F' => (a as u8) - b'A' + 10,
			'0'..='9' => (a as u8) - b'0',
			_ => {
				return Err(Error {
					span: offset..(offset + 1),
					message: format!(
						"Invalid bytes string token `{a}`, expected a hexidecimal digit"
					),
				});
			}
		};

		offset += 1;

		let Some(b) = chars.next() else {
			return Err(Error {
				span: offset..(offset + 1),
				message: "Unexpected bytes string end, expected second hexidecimal digit of a pair"
					.to_string(),
			});
		};

		let second = match b {
			'a'..='f' => (b as u8) - b'a' + 10,
			'A'..='F' => (b as u8) - b'A' + 10,
			'0'..='9' => (b as u8) - b'0',
			_ => {
				return Err(Error {
					span: offset..(offset + 1),
					message: format!(
						"Invalid bytes string token `{b}`, expected a hexidecimal digit"
					),
				});
			}
		};

		offset += 1;

		res.push(first << 4 | second);
	}

	Ok(res)
}

#[cfg(test)]
mod test {
	use super::bytes;

	#[test]
	fn happy_path() {
		assert_eq!(bytes("").unwrap(), Vec::<u8>::new());
		assert_eq!(bytes("00ff10Ab").unwrap(), vec![0x00, 0xFF, 0x10, 0xAB]);
	}

	#[test]
	fn invalid_digit() {
		let err = bytes("0g").unwrap_err();
		assert_eq!(err.message, "Invalid bytes string token `g`, expected a hexidecimal digit");
		assert_eq!(err.span, 1..2);
	}

	#[test]
	fn odd_length() {
		let err = bytes("0f1").unwrap_err();
		assert_eq!(
			err.message,
			"Unexpected bytes string end, expected second hexidecimal digit of a pair"
		);
		assert_eq!(err.span, 3..4);
	}
}
