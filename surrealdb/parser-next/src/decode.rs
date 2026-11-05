//! Decoding leaf spans into values, for the lowering pass.

/// Unescapes the body of a string or escaped identifier (without its
/// delimiters) into `out`. Returns `false` for an invalid escape.
pub fn unescape(body: &str, out: &mut String) -> bool {
	unescape_with(body, out, false)
}

/// [`unescape`], where with `json` a `\uXXXX` high surrogate must be followed
/// by a `\uXXXX` low surrogate and the pair is one character, as JSON
/// encodes characters outside the basic plane.
pub fn unescape_with(body: &str, out: &mut String, json: bool) -> bool {
	let mut chars = body.chars();
	while let Some(c) = chars.next() {
		if c != '\\' {
			out.push(c);
			continue;
		}
		let Some(e) = chars.next() else {
			return false;
		};
		let ch = match e {
			'n' => '\n',
			'r' => '\r',
			't' => '\t',
			'0' => '\0',
			'b' => '\u{8}',
			'f' => '\u{c}',
			'\\' | '\'' | '"' | '`' => e,
			'u' => {
				let rest = chars.as_str();
				let (code, used) = if let Some(r) = rest.strip_prefix('{') {
					let Some(close) = r.find('}') else {
						return false;
					};
					if close > 6 || !r[..close].bytes().all(|b| b.is_ascii_hexdigit()) {
						return false;
					}
					// `\u{}` is U+0000, as earlier releases read it.
					match u32::from_str_radix(&r[..close], 16) {
						Ok(v) => (v, close + 2),
						Err(_) if close == 0 => (0, 2),
						Err(_) => return false,
					}
				} else {
					let Some(hex) = rest.get(..4) else {
						return false;
					};
					if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
						return false;
					}
					let v = u32::from_str_radix(hex, 16).expect("four hex digits");
					if json && (0xD800..=0xDBFF).contains(&v) {
						let low = rest[4..]
							.strip_prefix("\\u")
							.and_then(|r| r.get(..4))
							.filter(|h| h.bytes().all(|b| b.is_ascii_hexdigit()))
							.map(|h| u32::from_str_radix(h, 16).expect("four hex digits"));
						match low {
							Some(low @ 0xDC00..=0xDFFF) => {
								((v - 0xD800) * 0x400 + (low - 0xDC00) + 0x10000, 10)
							}
							_ => return false,
						}
					} else {
						(v, 4)
					}
				};
				chars = rest[used..].chars();
				match char::from_u32(code) {
					Some(c) => c,
					None => return false,
				}
			}
			_ => return false,
		};
		out.push(ch);
	}
	true
}

/// The body of a quoted token: strips a string prefix and the quotes, a
/// pair of backticks, or `⟨` `⟩`.
pub fn strip_delims(text: &str) -> &str {
	let t = text.trim_start_matches(|c: char| c.is_ascii_lowercase());
	if let Some(inner) = t.strip_prefix('⟨') {
		return inner.strip_suffix('⟩').unwrap_or(inner);
	}
	if t.len() >= 2 {
		&t[1..t.len() - 1]
	} else {
		t
	}
}

/// The established error for the first invalid escape in a string or escaped
/// identifier token, as `(start, end, message, label)` with offsets into
/// `token`; `None` when every escape is valid. With `json`, a `\uXXXX` high
/// surrogate must be followed by a low one.
pub fn escape_error(token: &str, json: bool) -> Option<(u32, u32, &'static str, &'static str)> {
	let b = token.as_bytes();
	// Skip a string prefix and the opening delimiter.
	let mut i = match b.first() {
		Some(b'`') | Some(b'"') | Some(b'\'') => 1,
		Some(b'a'..=b'z') => crate::lex::string_prefix_len(token) + 1,
		// `⟨`
		Some(0xE2) => 3,
		_ => return None,
	};
	let at = |i: usize| b.get(i).copied();
	let hex = |c: u8| (c as char).to_digit(16);
	while i < b.len() {
		if b[i] != b'\\' {
			i += 1;
			continue;
		}
		let before = i as u32;
		i += 1;
		let Some(c) = at(i) else {
			return Some((before, i as u32, "Invalid escape sequence", "missing escape character"));
		};
		i += 1;
		match c {
			b'n' | b'r' | b't' | b'0' | b'\\' | b'b' | b'f' | b'\'' | b'"' | b'`' => {}
			b'u' => {
				if at(i) == Some(b'{') {
					i += 1;
					let mut accum: u32 = 0;
					for _ in 0..6 {
						match at(i) {
							Some(b'}') => break,
							Some(c) if hex(c).is_some() => {
								accum = accum * 16 + hex(c).unwrap_or(0);
								i += 1;
							}
							_ => {
								return Some((
									i as u32,
									i as u32 + 1,
									"Invalid escape sequence, expected `}` or hexadecimal character.",
									"Unexpected character",
								));
							}
						}
					}
					if at(i) != Some(b'}') {
						let label = if at(i).is_some_and(|c| c.is_ascii_hexdigit()) {
							"Too many hex-digits"
						} else {
							"Unexpected character"
						};
						return Some((
							i as u32,
							i as u32 + 1,
							"Invalid escape sequence, expected `}` character.",
							label,
						));
					}
					i += 1;
					if char::from_u32(accum).is_none() {
						return Some((
							before,
							i as u32,
							"Invalid escape sequence, unicode escape character is not a valid unicode character.",
							"Not a valid character code",
						));
					}
				} else {
					let mut accum: u32 = 0;
					for _ in 0..4 {
						match at(i).and_then(hex) {
							Some(d) => {
								accum = accum * 16 + d;
								i += 1;
							}
							None => {
								return Some((
									i as u32,
									i as u32 + 1,
									"String contains invalid escape sequence, expected a hexadecimal character.",
									"Unexpected character",
								));
							}
						}
					}
					if json && (0xD800..=0xDBFF).contains(&accum) {
						if at(i) == Some(b'\\') && at(i + 1) == Some(b'u') {
							i += 2;
							let mut low: u32 = 0;
							for _ in 0..4 {
								match at(i).and_then(hex) {
									Some(d) => {
										low = low * 16 + d;
										i += 1;
									}
									None => {
										return Some((
											i as u32,
											i as u32 + 1,
											"String contains invalid escape sequence, expected a hexadecimal character.",
											"Unexpected character",
										));
									}
								}
							}
							if (0xDC00..=0xDFFF).contains(&low) {
								continue;
							}
						} else if at(i) == Some(b'\\') {
							i += 1;
						}
						return Some((
							before,
							i as u32,
							"String contains invalid escape sequence, high surrogate must be followed by low surrogate (\\uDC00-\\uDFFF).",
							"Incomplete surrogate pair",
						));
					}
					if char::from_u32(accum).is_none() {
						return Some((
							before,
							i as u32,
							"String contains invalid escape sequence, unicode escape character is not a valid unicode character.",
							"Not a valid character code",
						));
					}
				}
			}
			_ => {
				return Some((
					before,
					i as u32,
					"Invalid escape sequence",
					"not a valid escape character",
				));
			}
		}
	}
	None
}
