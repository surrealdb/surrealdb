/// Returns wether the status code is an null body status
pub fn is_null_body_status(status: u16) -> bool {
	matches!(status, 101 | 103 | 204 | 205 | 304)
}

/// Returns wether the status code is an ok status
pub fn is_ok_status(status: u16) -> bool {
	(200..=299).contains(&status)
}

/// Returns wether the status code is an redirect status
pub fn is_redirect_status(status: u16) -> bool {
	[301, 302, 303, 307, 308].contains(&status)
}

/// Test whether a string matches the reason phrase http spec production.
pub fn is_reason_phrase(text: &str) -> bool {
	// Cannot be empty
	!text.is_empty()
		// all characters match VCHAR (0x21..=0x7E), obs-text (0x80..=0xFF), HTAB, or SP
		&& text.as_bytes().iter().all(|b| matches!(b,0x21..=0x7E | 0x80..=0xFF | b'\t' | b' '))
}

/// Returns whether to string are equal when all ascii characters are lowercased.
pub fn ascii_equal_ignore_case(a: &[u8], b: &[u8]) -> bool {
	if a.len() != b.len() {
		return false;
	}
	a.iter().zip(b).all(|(a, b)| a.to_ascii_lowercase() == b.to_ascii_lowercase())
}
