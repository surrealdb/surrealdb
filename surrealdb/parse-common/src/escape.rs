use std::borrow::Cow;

use logos::Logos;

use crate::Error;

/// Token type used to unescape all stringly formatted surrealql structures.
#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
enum EscapeTokenKind {
	#[token("\\n")]
	EscNewline,
	#[token("\\r")]
	EscCarriageReturn,
	#[token("\\t")]
	EscTab,
	#[token("\\0")]
	EscZeroByte,
	#[token("\\\\")]
	EscBackSlash,
	#[token("\\b")]
	EscBackSpace,
	#[token("\\f")]
	EscFormFeed,
	#[token("\\'")]
	EscQuote,
	#[token("\\\"")]
	EscDoubleQuote,
	#[token("\\`")]
	EscBackTick,
	#[token("\\⟩")]
	EscBracketClose,
	#[regex(r#"\\u[0-9a-fA-F]{4}"#)]
	EscUnicodeFixed,
	#[regex(r#"\\u\{[0-9a-fA-F]{1,6}}"#)]
	EscUnicodeBracket,
	#[regex(r#"[^\\]+"#, priority = 0)]
	Chars,
}

/// Decode the hex payload of a unicode escape sequence.
///
/// The lexer already verified that `bytes` contains only hex digits.
fn decode_unicode_hex(bytes: &[u8]) -> u32 {
	let mut char = 0u32;
	for b in bytes.iter().copied() {
		char <<= 4;
		match b {
			c @ b'0'..=b'9' => char += (c - b'0') as u32,
			c @ b'a'..=b'f' => char += (c - b'a' + 10) as u32,
			c @ b'A'..=b'F' => char += (c - b'A' + 10) as u32,
			_ => unreachable!(),
		}
	}
	char
}

/// Push the unescaped value of an escape sequence token onto the buffer.
///
/// Returns false if the escape sequence encodes an invalid code point.
fn handle_escape(buffer: &mut String, slice: &str, token: EscapeTokenKind) -> bool {
	match token {
		EscapeTokenKind::EscNewline => {
			buffer.push('\n');
			true
		}
		EscapeTokenKind::EscCarriageReturn => {
			buffer.push('\r');
			true
		}
		EscapeTokenKind::EscTab => {
			buffer.push('\t');
			true
		}
		EscapeTokenKind::EscZeroByte => {
			buffer.push('\0');
			true
		}
		EscapeTokenKind::EscBackSlash => {
			buffer.push('\\');
			true
		}
		EscapeTokenKind::EscBackSpace => {
			buffer.push('\x08');
			true
		}
		EscapeTokenKind::EscFormFeed => {
			buffer.push('\x0C');
			true
		}
		EscapeTokenKind::EscQuote => {
			buffer.push('\'');
			true
		}
		EscapeTokenKind::EscDoubleQuote => {
			buffer.push('\"');
			true
		}
		EscapeTokenKind::EscBackTick => {
			buffer.push('`');
			true
		}
		EscapeTokenKind::EscBracketClose => {
			buffer.push('⟩');
			true
		}
		EscapeTokenKind::EscUnicodeFixed => {
			let char = decode_unicode_hex(&slice.as_bytes()["\\u".len()..]);
			if let Some(x) = char::from_u32(char) {
				buffer.push(x);
				true
			} else {
				false
			}
		}
		EscapeTokenKind::EscUnicodeBracket => {
			let char = decode_unicode_hex(&slice.as_bytes()["\\u{".len()..slice.len() - 1]);
			if let Some(x) = char::from_u32(char) {
				buffer.push(x);
				true
			} else {
				false
			}
		}
		// Caller should only call this on escape sequence tokens.
		EscapeTokenKind::Chars => unreachable!(),
	}
}

/// Unescape `source` into `buffer`.
///
/// The buffer is cleared first. If `source` contains no escape sequences the returned string
/// borrows from `source` directly and the buffer stays empty, otherwise it borrows from
/// `buffer`.
///
/// Error spans are relative to `source`, i.e. the escaped text.
pub fn unescape<'a>(source: &'a str, buffer: &'a mut String) -> Result<&'a str, Error> {
	buffer.clear();

	let mut lexer = EscapeTokenKind::lexer(source);
	let mut pending_span = 0..0;
	loop {
		let Some(next) = lexer.next() else {
			// Fast path for if there are no escape sequences.
			if pending_span.len() == source.len() {
				return Ok(source);
			}
			buffer.push_str(&source[pending_span]);
			break;
		};

		let span = lexer.span();

		let next = match next {
			Ok(x) => x,
			Err(()) => {
				return Err(Error {
					span,
					message: "Invalid escape sequence".to_string(),
				});
			}
		};

		match next {
			EscapeTokenKind::Chars => {
				pending_span.end = span.end;
			}
			x => {
				buffer.push_str(&source[pending_span]);
				pending_span = span.end..span.end;
				if !handle_escape(buffer, &source[span.clone()], x) {
					return Err(Error {
						span,
						message: "Invalid escape sequence".to_string(),
					});
				}
			}
		}
	}

	Ok(buffer.as_str())
}

/// Like [`unescape`] but without a caller-provided buffer, only allocating when `source`
/// contains escape sequences.
pub fn unescape_cow(source: &str) -> Result<Cow<'_, str>, Error> {
	let mut buffer = String::new();
	let unescaped = unescape(source, &mut buffer)?;
	if std::ptr::eq(unescaped, source) {
		Ok(Cow::Borrowed(source))
	} else {
		Ok(Cow::Owned(buffer))
	}
}

/// Returns the offset in `source` corresponding to `offset` in the unescaped version of
/// `source`.
///
/// For example `unescaped_to_escaped_offset("\\u{21}a", 1)` will return `6` because `\u{21}`
/// unescapes to a single character.
///
/// Offsets which fall inside the characters produced by an escape sequence map to the end of
/// that escape sequence.
///
/// # Panics
/// This function can panic if the escaped string has invalid escape sequences inside and
/// therefore should only be called on strings which are already verified to have correct
/// escape sequences.
pub fn unescaped_to_escaped_offset(source: &str, offset: usize) -> usize {
	let mut lexer = EscapeTokenKind::lexer(source);

	let mut offset_idx = 0;
	loop {
		if offset_idx >= offset {
			return lexer.span().end;
		}

		let Some(t) = lexer.next() else {
			break;
		};

		let t = t.expect("string should have already been checked to be correct");
		match t {
			EscapeTokenKind::EscNewline
			| EscapeTokenKind::EscCarriageReturn
			| EscapeTokenKind::EscTab
			| EscapeTokenKind::EscZeroByte
			| EscapeTokenKind::EscBackSlash
			| EscapeTokenKind::EscBackSpace
			| EscapeTokenKind::EscFormFeed
			| EscapeTokenKind::EscQuote
			| EscapeTokenKind::EscDoubleQuote
			| EscapeTokenKind::EscBackTick => {
				offset_idx += 1;
			}
			EscapeTokenKind::EscBracketClose => {
				offset_idx += const { '⟩'.len_utf8() };
			}
			EscapeTokenKind::EscUnicodeFixed => {
				let char = decode_unicode_hex(&lexer.slice().as_bytes()["\\u".len()..]);
				offset_idx +=
					char::from_u32(char).expect("escape string should be valid").len_utf8();
			}
			EscapeTokenKind::EscUnicodeBracket => {
				let slice = lexer.slice().as_bytes();
				let char = decode_unicode_hex(&slice["\\u{".len()..slice.len() - 1]);
				offset_idx +=
					char::from_u32(char).expect("escape string should be valid").len_utf8();
			}
			EscapeTokenKind::Chars => {
				let slice = lexer.span();
				if offset_idx + slice.len() >= offset {
					return slice.start + (offset - offset_idx);
				}
				offset_idx += slice.len();
			}
		}
	}

	lexer.span().end
}

#[cfg(test)]
mod test {
	use std::borrow::Cow;

	use super::{unescape, unescape_cow, unescaped_to_escaped_offset};

	#[test]
	fn no_escapes_borrows_source() {
		let source = "hello world";
		let mut buffer = String::new();
		let res = unescape(source, &mut buffer).unwrap();
		assert!(std::ptr::eq(res, source));
		assert!(buffer.is_empty());

		let cow = unescape_cow(source).unwrap();
		assert!(matches!(cow, Cow::Borrowed(_)));
	}

	#[test]
	fn simple_escapes() {
		let mut buffer = String::new();
		let res = unescape(r#"a\nb\rc\td\0e\\f\bg\fh\'i\"j\`k"#, &mut buffer).unwrap();
		assert_eq!(res, "a\nb\rc\td\0e\\f\x08g\x0Ch'i\"j`k");
	}

	#[test]
	fn bracket_escape() {
		let mut buffer = String::new();
		let res = unescape(r"a\⟩b", &mut buffer).unwrap();
		assert_eq!(res, "a⟩b");
	}

	#[test]
	fn unicode_escapes() {
		let mut buffer = String::new();
		assert_eq!(unescape(r"\u0021", &mut buffer).unwrap(), "!");
		assert_eq!(unescape(r"\u{21}", &mut buffer).unwrap(), "!");
		assert_eq!(unescape(r"\u{1F600}", &mut buffer).unwrap(), "😀");
		assert_eq!(unescape(r"a\u{78}b", &mut buffer).unwrap(), "axb");
	}

	#[test]
	fn invalid_codepoint() {
		let mut buffer = String::new();
		let err = unescape(r"ab\u{d800}cd", &mut buffer).unwrap_err();
		assert_eq!(err.message, "Invalid escape sequence");
		assert_eq!(err.span, 2..10);
	}

	#[test]
	fn invalid_escape() {
		let mut buffer = String::new();
		let err = unescape(r"ab\xcd", &mut buffer).unwrap_err();
		assert_eq!(err.message, "Invalid escape sequence");
		assert_eq!(err.span.start, 2);
	}

	#[test]
	fn cow_owned_on_escape() {
		let cow = unescape_cow(r"a\nb").unwrap();
		assert!(matches!(cow, Cow::Owned(_)));
		assert_eq!(cow, "a\nb");
	}

	#[test]
	fn offset_mapping() {
		// Doc example: `\u{21}` unescapes to one character.
		assert_eq!(unescaped_to_escaped_offset(r"\u{21}a", 1), 6);
		assert_eq!(unescaped_to_escaped_offset(r"\u{21}a", 2), 7);
		// Offset zero maps to the start.
		assert_eq!(unescaped_to_escaped_offset(r"\u{21}a", 0), 0);
		// Inside an unescaped run the mapping is exact.
		assert_eq!(unescaped_to_escaped_offset(r"ab\ncd", 1), 1);
		assert_eq!(unescaped_to_escaped_offset(r"ab\ncd", 3), 4);
		assert_eq!(unescaped_to_escaped_offset(r"ab\ncd", 5), 6);
		// Multi-byte escape results: `\⟩` unescapes to a 3 byte character.
		assert_eq!(unescaped_to_escaped_offset(r"\⟩a", 3), 4);
		// Multi-byte character in a plain run.
		assert_eq!(unescaped_to_escaped_offset("µa\\n", 2), 2);
		// Offset at the end of input.
		assert_eq!(unescaped_to_escaped_offset(r"ab\ncd", 6), 6);
	}
}
