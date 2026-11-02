//! The MySQL `YEAR` type on the wire. The binary protocol sends the year as a little-endian
//! `uint16`. The text protocol sends four decimal digits, zero-padded, so the zero year is
//! `0000`.

/// The binary form of a year.
pub fn year_binary(year: u16) -> [u8; 2] {
	year.to_le_bytes()
}

/// The year of a binary `YEAR` value. `None` when the value is not two bytes.
pub fn year_from_binary(bytes: &[u8]) -> Option<u16> {
	let array: [u8; 2] = bytes.try_into().ok()?;
	Some(u16::from_le_bytes(array))
}

/// The text form of a year: exactly four digits.
pub fn year_text(year: u16) -> String {
	format!("{year:04}")
}

/// Read the text form of a year. Only the four-digit form is accepted; `None` for any other
/// length or a non-digit.
pub fn parse_year_text(text: &str) -> Option<u16> {
	if text.len() != 4 || !text.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	}
	text.parse().ok()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn binary_is_little_endian() {
		assert_eq!(year_binary(2024), [0xE8, 0x07]);
		assert_eq!(year_from_binary(&[0xE8, 0x07]), Some(2024));
		assert_eq!(year_from_binary(&year_binary(0)), Some(0));
		assert_eq!(year_from_binary(&[0]), None);
		assert_eq!(year_from_binary(&[0, 0, 0]), None);
	}

	#[test]
	fn text_is_four_padded_digits() {
		assert_eq!(year_text(2024), "2024");
		assert_eq!(year_text(0), "0000");
		assert_eq!(year_text(1901), "1901");
		assert_eq!(parse_year_text("0000"), Some(0));
		assert_eq!(parse_year_text("2155"), Some(2155));
		assert_eq!(parse_year_text("24"), None);
		assert_eq!(parse_year_text("20240"), None);
		assert_eq!(parse_year_text("20x4"), None);
		assert_eq!(parse_year_text("+024"), None);
	}
}
