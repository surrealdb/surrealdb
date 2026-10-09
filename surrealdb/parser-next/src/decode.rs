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
					// `\u{}` is U+0000, as in `syn`.
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
	let t = text.trim_start_matches(['r', 'u', 'd', 'b', 'f', 's']);
	if let Some(inner) = t.strip_prefix('⟨') {
		return inner.strip_suffix('⟩').unwrap_or(inner);
	}
	if t.len() >= 2 {
		&t[1..t.len() - 1]
	} else {
		t
	}
}
