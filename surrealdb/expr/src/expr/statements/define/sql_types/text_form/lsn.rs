//! The text form of the PostgreSQL `pg_lsn` type: two hexadecimal halves separated by `/`, the
//! high 32 bits and the low 32 bits, each in upper case without leading zeros (`0/16B3748`).

/// The text form of a log sequence number, as PostgreSQL prints it.
pub fn lsn_text(lsn: u64) -> String {
	format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// Read the text form of a `pg_lsn` value. Each half is one to eight hexadecimal digits in
/// either case. `None` for any other shape.
pub fn parse_lsn_text(text: &str) -> Option<u64> {
	let (high, low) = text.trim().split_once('/')?;
	let high = parse_half(high)?;
	let low = parse_half(low)?;
	Some((high << 32) | low)
}

fn parse_half(text: &str) -> Option<u64> {
	if text.is_empty() || text.len() > 8 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
		return None;
	}
	u64::from_str_radix(text, 16).ok()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn text_prints_high_and_low_halves_in_upper_case() {
		assert_eq!(lsn_text(0), "0/0");
		assert_eq!(lsn_text(0x16B_3748), "0/16B3748");
		assert_eq!(lsn_text(1 << 32), "1/0");
		assert_eq!(lsn_text(u64::MAX), "FFFFFFFF/FFFFFFFF");
	}

	#[test]
	fn text_reads_what_it_prints_and_either_case() {
		for lsn in [0, 0x16B_3748, 1 << 32, 0xABCD_EF01_2345_6789, u64::MAX] {
			assert_eq!(parse_lsn_text(&lsn_text(lsn)), Some(lsn), "{lsn}");
		}
		assert_eq!(parse_lsn_text("0/16b3748"), Some(0x16B_3748));
		assert_eq!(parse_lsn_text(" 1/0 "), Some(1 << 32));
	}

	#[test]
	fn text_refuses_other_shapes() {
		assert_eq!(parse_lsn_text(""), None);
		assert_eq!(parse_lsn_text("12"), None);
		assert_eq!(parse_lsn_text("/5"), None);
		assert_eq!(parse_lsn_text("1/"), None);
		assert_eq!(parse_lsn_text("1/2/3"), None);
		assert_eq!(parse_lsn_text("1/123456789"), None);
		assert_eq!(parse_lsn_text("G/0"), None);
		assert_eq!(parse_lsn_text("-1/0"), None);
		assert_eq!(parse_lsn_text("0x1/0"), None);
	}
}
