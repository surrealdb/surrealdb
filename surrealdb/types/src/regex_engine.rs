//! The regular expression engine behind SurrealQL regex values and the
//! builtins that match against fixed patterns.
//!
//! With the `regex-unicode` feature (on by default) the engine is the `regex`
//! crate, with full Unicode support. Without it the engine is `regex-lite`,
//! which leaves out the Unicode tables. The two engines accept the same syntax
//! and match the same strings for every pattern `compile` accepts, except that
//! case-insensitive matching under `regex-lite` does not equate `k` with
//! U+212A KELVIN SIGN or `s` with U+017F LATIN SMALL LETTER LONG S.
//!
//! A pattern whose matches would otherwise differ is rejected by `compile`
//! under `regex-lite` with an error naming the `regex-unicode` feature, so a
//! stored pattern never silently matches differently. Rejected while Unicode
//! mode is on (the default, turned off with `(?-u)`):
//! - Unicode classes `\p{..}` and `\P{..}`;
//! - the Perl classes `\d`, `\D`, `\s`, `\S`, `\w` and `\W`, which are ASCII-only under
//!   `regex-lite`;
//! - the word boundaries `\b`, `\B`, `\<` and `\>`, which depend on `\w`;
//! - a cased non-ASCII character, literal or escaped, in a case-insensitive pattern.

use std::fmt;

pub use engine::{Captures, Match, Regex, Replacer};
#[cfg(feature = "regex-unicode")]
use regex as engine;
#[cfg(not(feature = "regex-unicode"))]
use regex_lite as engine;

/// An error compiling a regular expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegexError(String);

impl RegexError {
	/// The error message.
	pub fn message(&self) -> &str {
		&self.0
	}
}

impl fmt::Display for RegexError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.0)
	}
}

impl std::error::Error for RegexError {}

/// Compiles `pattern`, failing if the compiled program would exceed
/// `size_limit` bytes (the engine's default limit when `None`).
pub fn compile(pattern: &str, size_limit: Option<usize>) -> Result<Regex, RegexError> {
	#[cfg(not(feature = "regex-unicode"))]
	check_portable(pattern)?;
	let mut builder = engine::RegexBuilder::new(pattern);
	if let Some(limit) = size_limit {
		builder.size_limit(limit);
	}
	builder.build().map_err(|e| RegexError(e.to_string()))
}

/// Whether this build matches with full Unicode support.
pub const UNICODE: bool = cfg!(feature = "regex-unicode");

#[cfg_attr(feature = "regex-unicode", allow(dead_code))]
fn unsupported(what: &str) -> RegexError {
	RegexError(format!(
		"{what} requires full Unicode regex support, which this build was compiled without (the `regex-unicode` feature)"
	))
}

/// Rejects a pattern whose matches under `regex-lite` could differ from those
/// under the `regex` crate. Syntax errors are left to the engine.
#[cfg_attr(feature = "regex-unicode", allow(dead_code))]
fn check_portable(pattern: &str) -> Result<(), RegexError> {
	#[derive(Clone, Copy)]
	struct Flags {
		unicode: bool,
		case_insensitive: bool,
		ignore_whitespace: bool,
	}

	// A character that a case-insensitive Unicode match equates with another.
	fn is_cased(c: char) -> bool {
		!c.is_ascii()
			&& (c.is_lowercase()
				|| c.is_uppercase()
				|| c.to_lowercase().ne(std::iter::once(c))
				|| c.to_uppercase().ne(std::iter::once(c)))
	}

	fn check_literal(c: char, flags: Flags) -> Result<(), RegexError> {
		if flags.unicode && flags.case_insensitive && is_cased(c) {
			return Err(unsupported(&format!(
				"case-insensitive matching of the non-ASCII character {c:?}"
			)));
		}
		Ok(())
	}

	let chars: Vec<char> = pattern.chars().collect();
	let mut flags = Flags {
		unicode: true,
		case_insensitive: false,
		ignore_whitespace: false,
	};
	let mut groups: Vec<Flags> = Vec::new();
	// Nesting depth of character classes; 0 outside any class.
	let mut class_depth = 0usize;
	let mut i = 0;
	while i < chars.len() {
		let c = chars[i];
		i += 1;
		match c {
			'\\' => {
				let Some(&e) = chars.get(i) else {
					break;
				};
				i += 1;
				match e {
					'p' | 'P' => return Err(unsupported(&format!("the Unicode class `\\{e}`"))),
					'd' | 'D' | 's' | 'S' | 'w' | 'W' | 'b' | 'B' | '<' | '>' if flags.unicode => {
						return Err(unsupported(&format!(
							"`\\{e}`, which is Unicode-aware unless written under `(?-u)`,"
						)));
					}
					'x' | 'u' | 'U' => {
						// `\x7F`, `\u007F`, `\U0000007F` or `\x{7F}`.
						let digits: String = if chars.get(i) == Some(&'{') {
							let end = chars[i..].iter().position(|&c| c == '}').map(|p| i + p);
							let Some(end) = end else {
								break;
							};
							let s = chars[i + 1..end].iter().collect();
							i = end + 1;
							s
						} else {
							let n = match e {
								'x' => 2,
								'u' => 4,
								_ => 8,
							};
							let end = (i + n).min(chars.len());
							let s = chars[i..end].iter().collect();
							i = end;
							s
						};
						if let Some(c) =
							u32::from_str_radix(&digits, 16).ok().and_then(char::from_u32)
						{
							check_literal(c, flags)?;
						}
					}
					_ => {}
				}
			}
			'[' if class_depth > 0 && chars.get(i) == Some(&':') => {
				// A POSIX class such as `[:alpha:]`, which is ASCII-only in both engines.
				match chars[i..].windows(2).position(|w| w == [':', ']']) {
					Some(p) => i += p + 2,
					None => class_depth += 1,
				}
			}
			'[' => {
				class_depth += 1;
				if chars.get(i) == Some(&'^') {
					i += 1;
				}
				// A `]` first in a class is a literal.
				if chars.get(i) == Some(&']') {
					i += 1;
				}
			}
			']' if class_depth > 0 => class_depth -= 1,
			_ if class_depth > 0 => check_literal(c, flags)?,
			'(' => {
				if chars.get(i) != Some(&'?') {
					groups.push(flags);
					continue;
				}
				// `(?flags)`, `(?flags:...)`, or a named or non-capturing group.
				let start = i + 1;
				let mut j = start;
				let mut negate = false;
				let mut next = flags;
				while let Some(&f) = chars.get(j) {
					match f {
						'-' => negate = true,
						'u' => next.unicode = !negate,
						'i' => next.case_insensitive = !negate,
						'x' => next.ignore_whitespace = !negate,
						'm' | 's' | 'U' | 'R' => {}
						_ => break,
					}
					j += 1;
				}
				match chars.get(j) {
					Some(')') if j > start => {
						flags = next;
						i = j + 1;
					}
					Some(':') => {
						groups.push(flags);
						flags = next;
						i = j + 1;
					}
					_ => groups.push(flags),
				}
			}
			')' => {
				if let Some(outer) = groups.pop() {
					flags = outer;
				}
			}
			'#' if flags.ignore_whitespace => {
				while i < chars.len() && chars[i] != '\n' {
					i += 1;
				}
			}
			_ => check_literal(c, flags)?,
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	const PORTABLE: &[&str] = &[
		"^[a-z0-9_]+$",
		"(?i)hello",
		"(?i)HeLLo [a-z]+",
		"[[:alpha:]]+",
		"(?-u:\\w+)",
		"(?-u)\\bfoo\\b",
		"caf\u{e9}",
		"\\x41\\.\\*",
		"(?i)[0-9]+\u{2192}",
		"(?x) a # \\d in a comment\n b",
		"(?P<year>[0-9]{4})-(?:[0-9]{2})",
		"[^\u{3b2}]",
	];

	const UNICODE_ONLY: &[&str] = &[
		"\\d+",
		"\\w",
		"[\\s,]",
		"\\bword\\b",
		"\\p{Greek}",
		"\\PL",
		"(?i)stra\u{df}e",
		"(?i:\\x{e9})",
		"(?i)[\u{e0}-\u{ff}]",
		"(?-u:a)\\d",
		"(?-u)(?u)\\d",
	];

	const HAYSTACKS: &[&str] = &[
		"",
		"hello world",
		"HELLO",
		"caf\u{e9} CAF\u{c9}",
		"1999-12 \u{663}\u{664}",
		"foo_bar foo\u{e9}bar",
		"\u{3b1}\u{3b2}\u{3b3} \u{2192} 42",
		"a b\nab",
	];

	#[test]
	fn check_accepts_portable_patterns() {
		for p in PORTABLE {
			check_portable(p).unwrap_or_else(|e| panic!("{p}: {e}"));
			let re = compile(p, None).unwrap_or_else(|e| panic!("{p}: {e}"));
			assert_eq!(re.as_str(), *p);
		}
	}

	#[test]
	fn check_rejects_unicode_patterns() {
		for p in UNICODE_ONLY {
			let e = check_portable(p).expect_err(p);
			assert!(e.message().contains("regex-unicode"), "{p}: {e}");
			// Rejected exactly when the build lacks Unicode support.
			assert_eq!(compile(p, None).is_ok(), UNICODE, "{p}");
		}
	}

	/// Every pattern the check accepts matches the same spans in both engines.
	#[test]
	fn portable_patterns_match_alike() {
		for p in PORTABLE {
			let full = regex::Regex::new(p).unwrap();
			let lite = regex_lite::Regex::new(p).unwrap();
			for h in HAYSTACKS {
				let a: Vec<_> = full.find_iter(h).map(|m| m.range()).collect();
				let b: Vec<_> = lite.find_iter(h).map(|m| m.range()).collect();
				assert_eq!(a, b, "{p} on {h:?}");
			}
		}
	}

	/// A stored pattern this build cannot match alike fails to decode.
	#[test]
	fn decoding_follows_the_build() {
		let r = serde_json::from_str::<crate::Regex>(r#""\\d+""#);
		assert_eq!(r.is_ok(), UNICODE);
		match r {
			Ok(re) => assert!(re.is_match("\u{663}")),
			Err(e) => assert!(e.to_string().contains("regex-unicode"), "{e}"),
		}
	}

	#[test]
	fn reports_syntax_errors() {
		assert!(compile("(unclosed", None).is_err());
		assert!(compile("^(a|b|c){1000000}", Some(10 << 20)).is_err());
	}
}
