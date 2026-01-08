mod datetime;

use super::BytesReader;
use super::unicode::byte;
use crate::syn::error::{SyntaxError, bail, syntax_error};
use crate::syn::lexer::Lexer;
use crate::syn::token::Span;
use crate::types::{PublicBytes, PublicFile, PublicUuid};

impl Lexer<'_> {
	/// Unescapes a string slice.
	/// Expects a string and the span of that string within the source code where the string
	/// contains the full string token, including possible prefix digit and qoutes.
	///
	/// Note that the string token can contain invalid escape sequences which will be properly
	/// reported as errors by this function.
	///
	/// Returns the actual unescaped value of the string.
	///
	/// Will panic if it is not a string slice.
	pub fn unescape_string_span<'a>(
		str: &str,
		span: Span,
		buffer: &'a mut Vec<u8>,
	) -> Result<&'a str, SyntaxError> {
		buffer.clear();

		let mut reader = BytesReader::new(str.as_bytes());

		let mut double = false;
		match reader.next() {
			Some(b's' | b'r' | b'u' | b'f' | b'd' | b'b') => {
				double = reader.next() == Some(b'"');
			}
			Some(b'"') => double = true,
			Some(b'\'') => {}
			_ => panic!("string given to unescape_string_span was not a valid string token"),
		};

		loop {
			let before = reader.offset();
			// There must be a next byte as the string token must end in a `"`.
			let byte = reader.next().expect("Invalid string token");
			match byte {
				b'\\' => {
					Self::lex_common_escape_sequence(&mut reader, span, before, buffer)?;
				}
				b'"' if double => break,
				b'\'' if !double => break,
				x => buffer.push(x),
			}
		}

		// Safety: The string this was created from was a valid utf-8 string and the code ensures
		// that only valid sequences are pushed into the buffer meaning that the final buffer is
		// also a valid utf-8.
		Ok(unsafe { std::str::from_utf8_unchecked(buffer) })
	}

	/// Unescapes a regex slice.
	/// Expects a string and the span of that string within the source code where the string
	/// contains the full string token, including possible prefix digit and qoutes.
	///
	/// Note that the string token can contain invalid escape sequences which will be properly
	/// reported as errors by this function.
	///
	/// Returns the actual unescaped value of the regex.
	///
	/// Will panic if it is not a regex slice.
	pub fn unescape_regex_span<'a>(
		str: &str,
		span: Span,
		buffer: &'a mut Vec<u8>,
	) -> Result<&'a str, SyntaxError> {
		buffer.clear();

		let mut reader = BytesReader::new(str.as_bytes());

		let Some(b'/') = reader.next() else {
			panic!("string given to unescape_string_span was not a valid string token")
		};

		loop {
			let before = reader.offset();
			// There must be a next byte as the string token must end in a `"`.
			let byte = reader.next().expect("Invalid string token");
			match byte {
				b'\\' => {
					if let Some(b'/') = reader.peek() {
						buffer.push(b'/');
					} else {
						Self::lex_common_escape_sequence(&mut reader, span, before, buffer)?;
					}
				}
				b'/' => break,
				x => buffer.push(x),
			}
		}

		// Safety: The string this was created from was a valid utf-8 string and the code ensures
		// that only valid sequences are pushed into the buffer meaning that the final buffer is
		// also a valid utf-8.
		Ok(unsafe { std::str::from_utf8_unchecked(buffer) })
	}

	pub(super) fn lex_common_escape_sequence(
		reader: &mut BytesReader,
		span: Span,
		before: u32,
		buffer: &mut Vec<u8>,
	) -> Result<(), SyntaxError> {
		let Some(c) = reader.next() else {
			let span = reader.span_since(before).as_within(span);
			bail!("Invalid escape sequence", @span => "missing escape character")
		};
		match c {
			b'n' => {
				buffer.push(b'\n');
			}
			b'r' => {
				buffer.push(b'\r');
			}
			b't' => {
				buffer.push(b'\t');
			}
			b'0' => {
				buffer.push(b'\0');
			}
			b'\\' => {
				buffer.push(b'\\');
			}
			b'b' => {
				buffer.push(byte::BS);
			}
			b'f' => {
				buffer.push(byte::FF);
			}
			b'\'' => {
				buffer.push(b'\'');
			}
			b'"' => {
				buffer.push(b'"');
			}
			b'`' => {
				buffer.push(b'`');
			}
			b'u' => {
				let char = Self::lex_unicode_escape(reader, before, span)?;
				let mut char_buffer = [0u8; 4];
				buffer.extend_from_slice(char.encode_utf8(&mut char_buffer).as_bytes())
			}
			_ => {
				let span = reader.span_since(before).as_within(span);
				bail!("Invalid escape sequence", @span => "not a valid escape character")
			}
		}
		Ok(())
	}

	fn lex_unicode_escape(
		reader: &mut BytesReader,
		before: u32,
		span: Span,
	) -> Result<char, SyntaxError> {
		if reader.eat(b'{') {
			let mut accum = 0;

			for _ in 0..6 {
				match reader.peek() {
					Some(c @ b'a'..=b'f') => {
						reader.next();
						accum <<= 4;
						accum += (c - b'a') as u32 + 10;
					}
					Some(c @ b'A'..=b'F') => {
						reader.next();
						accum <<= 4;
						accum += (c - b'A') as u32 + 10;
					}
					Some(c @ b'0'..=b'9') => {
						reader.next();
						accum <<= 4;
						accum += (c - b'0') as u32;
					}
					Some(b'}') => {
						break;
					}
					_ => {
						let offset = reader.offset();
						reader.next();
						let span = reader.span_since(offset).as_within(span);
						bail!("Invalid escape sequence, expected `}}` or hexadecimal character.", @span => "Unexpected character")
					}
				}
			}

			if !reader.eat(b'}') {
				let offset = reader.offset();
				let n = reader.next();
				let span = reader.span_since(offset).as_within(span);
				if n.map(|x| x.is_ascii_hexdigit()).unwrap_or(false) {
					bail!("Invalid escape sequence, expected `}}` character.", @span => "Too many hex-digits")
				} else {
					bail!("Invalid escape sequence, expected `}}` character.", @span => "Unexpected character")
				}
			}

			char::from_u32(accum).ok_or_else(||{
				let span = reader.span_since(before).as_within(span);
				syntax_error!("Invalid escape sequence, unicode escape character is not a valid unicode character.", @span => "Not a valid character code")
			})
		} else {
			let mut accum = 0;
			for _ in 0..4 {
				match reader.next() {
					Some(c @ b'a'..=b'f') => {
						accum <<= 4;
						accum += (c - b'a') as u32 + 10;
					}
					Some(c @ b'A'..=b'F') => {
						accum <<= 4;
						accum += (c - b'A') as u32 + 10;
					}
					Some(c @ b'0'..=b'9') => {
						accum <<= 4;
						accum += (c - b'0') as u32;
					}
					_ => {
						let span = reader.span_since(reader.offset() - 1).as_within(span);
						bail!("String contains invalid escape sequence, expected a hexadecimal character.", @span => "Unexpected character")
					}
				}
			}
			char::from_u32(accum).ok_or_else(||{
				let span = reader.span_since(before).as_within(span);
				syntax_error!("String contains invalid escape sequence, unicode escape character is not a valid unicode character.", @span => "Not a valid character code")
			})
		}
	}

	/// Returns the offset within a string from the offset within the escaped string.
	/// For instance given the string `a\rb` and the offset 2 this function will return 3 as
	/// the 2 index in the resulting string as from index 3 in the source string.
	///
	/// # Panic
	/// Assumes the escaped string is valid string including valid escape squences.
	/// if the string is not valid it will panic.
	pub fn escaped_string_offset(escaped_str: &str, offset: u32) -> u32 {
		let mut reader = BytesReader::new(escaped_str.as_bytes());

		// skip over the starting `s"` or `"` or `b"`, etc.
		if !reader.eat(b'"') && !reader.eat(b'\'') {
			reader.next();
			reader.next();
		}

		let mut offset_idx = 0;
		let mut bytes = [0u8; 4];

		loop {
			if offset_idx >= offset {
				return reader.offset();
			}
			let Some(b) = reader.next() else {
				break;
			};
			match b {
				b'\\' => match reader.next().expect("lexer validated input") {
					b'u' => {
						if reader.eat(b'{') {
							let mut accum = 0;
							let mut at_end = false;
							for _ in 0..6 {
								match reader.next().expect("lexer validated input") {
									c @ b'a'..=b'f' => {
										accum <<= 4;
										accum += (c - b'a') as u32 + 10;
									}
									c @ b'A'..=b'F' => {
										accum <<= 4;
										accum += (c - b'A') as u32 + 10;
									}
									c @ b'0'..=b'9' => {
										accum <<= 4;
										accum += (c - b'0') as u32;
									}
									b'}' => {
										at_end = true;
										break;
									}
									_ => panic!("invalid escape sequence"),
								}
							}

							if !at_end {
								reader.next();
							}

							offset_idx += char::from_u32(accum)
								.expect("valid unicode codepoint")
								.encode_utf8(&mut bytes)
								.len() as u32;
						} else {
							let mut accum = 0;
							for _ in 0..4 {
								match reader.next().expect("lexer validated input") {
									c @ b'a'..=b'f' => {
										accum <<= 4;
										accum += (c - b'a') as u32 + 10;
									}
									c @ b'A'..=b'F' => {
										accum <<= 4;
										accum += (c - b'A') as u32 + 10;
									}
									c @ b'0'..=b'9' => {
										accum <<= 4;
										accum += (c - b'0') as u32;
									}
									_ => panic!("invalid escape sequence"),
								}
							}

							offset_idx += char::from_u32(accum)
								.expect("valid unicode codepoint")
								.encode_utf8(&mut bytes)
								.len() as u32;
						}
					}
					_ => {
						offset_idx += 1;
					}
				},
				_ => {
					offset_idx += 1;
				}
			}
		}

		reader.offset()
	}

	pub fn lex_uuid(str: &str) -> Result<PublicUuid, SyntaxError> {
		let mut uuid_buffer = [0u8; 16];

		let mut reader = BytesReader::new(str.as_bytes());

		fn eat_uuid_hex(
			reader: &mut BytesReader<'_>,
			buffer: &mut [u8],
		) -> Result<(), SyntaxError> {
			// the amounts of character required is twice the buffer len.
			// since every character is half a byte.
			for x in buffer {
				let a = eat_hex_character(reader)?;
				let b = eat_hex_character(reader)?;
				*x = (a << 4) | b;
			}

			Ok(())
		}

		fn eat_hex_character(reader: &mut BytesReader<'_>) -> Result<u8, SyntaxError> {
			fn ascii_to_hex(b: u8) -> Option<u8> {
				if b.is_ascii_digit() {
					return Some(b - b'0');
				}

				if (b'a'..=b'f').contains(&b) {
					return Some(b - b'a' + 10);
				}

				if (b'A'..=b'F').contains(&b) {
					return Some(b - b'A' + 10);
				}

				None
			}

			let Some(peek) = reader.peek() else {
				let offset = reader.offset();
				let span = reader.span_since(offset);
				bail!("Unexpected end of string, expected UUID token to finish",@span);
			};
			let Some(res) = ascii_to_hex(peek) else {
				let offset = reader.offset();
				let char = reader.next().expect("lexer validated input");
				// Source is a string, so there can't be invalid characters.
				let char = reader.convert_to_char(char).expect("lexer validated input");
				let span = reader.span_since(offset);
				bail!("Unexpected character `{char}` expected hexidecimal digit",@span);
			};
			reader.next();
			Ok(res)
		}

		fn expect_seperator(reader: &mut BytesReader<'_>) -> Result<(), SyntaxError> {
			let before = reader.offset();
			match reader.next() {
				Some(b'-') => Ok(()),
				Some(x) => {
					// This function operates on a valid string so this function can never error.
					let span = reader.span_since(before);
					let c = reader.convert_to_char(x).expect("lexer validated input");
					bail!("Unexpected character `{c}`, expected byte seperator `-`", @span);
				}
				None => {
					let span = reader.span_since(before);
					bail!("Unexpected end of string, expected UUID token to finish", @span);
				}
			}
		}

		eat_uuid_hex(&mut reader, &mut uuid_buffer[0..4])?;

		expect_seperator(&mut reader)?;

		eat_uuid_hex(&mut reader, &mut uuid_buffer[4..6])?;

		expect_seperator(&mut reader)?;

		eat_uuid_hex(&mut reader, &mut uuid_buffer[6..8])?;

		expect_seperator(&mut reader)?;

		eat_uuid_hex(&mut reader, &mut uuid_buffer[8..10])?;

		expect_seperator(&mut reader)?;

		eat_uuid_hex(&mut reader, &mut uuid_buffer[10..16])?;

		Ok(PublicUuid::from(uuid::Uuid::from_bytes(uuid_buffer)))
	}

	/// Lex a bytes string.
	pub fn lex_bytes(str: &str) -> Result<PublicBytes, SyntaxError> {
		let mut res = Vec::with_capacity(str.len() / 2);
		let mut reader = BytesReader::new(str.as_bytes());
		while let Some(x) = reader.next() {
			let byte1 = match x {
				b'0'..=b'9' => x - b'0',
				b'A'..=b'F' => x - b'A' + 10,
				b'a'..=b'f' => x - b'a' + 10,
				x => {
					let before = reader.offset() - 1;
					// Source is a string, so there can't be invalid characters.
					let c = reader.convert_to_char(x).expect("lexer validated input");
					let span = reader.span_since(before);
					bail!("Unexpected character `{c}`, expected a hexidecimal digit", @span);
				}
			};
			let Some(x) = reader.next() else {
				let span = reader.span_since(reader.offset());
				bail!("Unexpected end of byte-string, expected a hexidecimal digit", @span);
			};
			let byte2 = match x {
				b'0'..=b'9' => x - b'0',
				b'A'..=b'F' => x - b'A' + 10,
				b'a'..=b'f' => x - b'a' + 10,
				x => {
					let before = reader.offset() - 1;
					// Source is a string, so there can't be invalid characters.
					let c = reader.convert_to_char(x).expect("lexer validated input");
					let span = reader.span_since(before);
					bail!("Unexpected character `{c}`, expected a hexidecimal digit", @span);
				}
			};
			res.push(byte1 << 4 | byte2);
		}

		Ok(PublicBytes::from(res))
	}

	pub fn lex_file(str: &str) -> Result<PublicFile, SyntaxError> {
		let mut reader = BytesReader::new(str.as_bytes());
		let mut bucket = String::new();
		loop {
			let before = reader.offset();
			let Some(x) = reader.next() else {
				let span = reader.span_since(reader.offset());
				bail!("Unexpected end of file string, missing bucket seperator `:/`", @span);
			};

			match x {
				b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'-' | b'.' => {
					bucket.push(x as char);
				}
				b':' => break,
				x => {
					let span = reader.span_since(before);
					// Reader operates on a valid string so unwrap shouldn't trigger.
					let c = reader.convert_to_char(x).expect("lexer validated input");
					bail!("Unexpected character `{c}`, file strings buckets only allow alpha numeric characters and `_`, `-`, and `.`", @span);
				}
			}
		}

		let before = reader.offset();
		match reader.next() {
			Some(b'/') => {}
			Some(x) => {
				let span = reader.span_since(before);
				// Reader operates on a valid string so unwrap shouldn't trigger.
				let c = reader.convert_to_char(x).expect("lexer validated input");
				bail!("Unexpected character `{c}`, expected `/`", @span);
			}
			None => {
				let span = reader.span_since(reader.offset());
				bail!("Unexpected end of file string, missing file string key.", @span);
			}
		}

		let mut key = String::with_capacity(reader.remaining().len() + 1);
		key.push('/');

		while let Some(x) = reader.next() {
			match x {
				b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'/' => {
					key.push(x as char);
				}
				b':' => break,
				x => {
					let before = reader.offset() - 1;
					let span = reader.span_since(before);
					let c = reader.convert_to_char(x).expect("lexer validated input");
					bail!("Unexpected character `{c}`, file strings key's only allow alpha numeric characters and `_`, `-`, `.`, and `/`", @span);
				}
			}
		}

		Ok(PublicFile::new(bucket, key))
	}
}
