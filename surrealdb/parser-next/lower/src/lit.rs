//! Decoding of literal leaves at lowering: decimals, regexes, datetimes,
//! uuids, byte strings, record-id strings and builtin paths, following the
//! rules `syn` applies at parse time.
//!
//! The values are built through the engine's own types. Where the harness
//! cannot name a third-party type (`rust_decimal::Decimal`, `regex::Regex`,
//! `uuid::Uuid`, `bytes::Bytes`), it is inferred from the `Literal` variant
//! that holds it, so no dependency is added.

use std::str::FromStr;

use common::decimal::DecimalExt;
use surrealdb_parser_next::ast::{Ast, NodeId};
use surrealdb_parser_next::decode;
use surrealdb_sql::builtin_paths::{self, PATHS, PathKind};
use surrealdb_sql::{Expr, Literal};
use surrealdb_strand::Strand;

use super::{Lower, Options, R, Reject};

/// Parses `s` as the type `variant` wraps.
fn parse_as<T: FromStr, U>(_variant: fn(T) -> U, s: &str) -> Option<T> {
	s.parse().ok()
}

/// [`parse_as`], keeping the parse error's message.
fn parse_as_msg<T: FromStr, U>(_variant: fn(T) -> U, s: &str) -> Result<T, String>
where
	T::Err: std::fmt::Display,
{
	s.parse().map_err(|e: T::Err| e.to_string())
}

fn normalized_as<T: DecimalExt, U>(_variant: fn(T) -> U, s: &str) -> Option<T> {
	T::from_str_normalized(s).ok()
}

fn from_vec_as<T: From<Vec<u8>>, U>(_variant: fn(T) -> U, v: Vec<u8>) -> T {
	T::from(v)
}

/// The unescaped body of a string token.
pub fn string_body(text: &str) -> R<String> {
	let mut s = String::new();
	if !decode::unescape(decode::strip_delims(text), &mut s) {
		return Err(Reject::new("invalid escape"));
	}
	Ok(s)
}

/// A decimal literal span (`1.5dec`, `-2e3dec`).
pub fn decimal(text: &str) -> R<Literal> {
	let s: String = text.trim_end_matches("dec").chars().filter(|c| *c != '_').collect();
	let d = if let Some(i) = s.find(['e', 'E']) {
		// Scientific notation: the exponent shifts the decimal point.
		let (mantissa, exp) = (&s[..i], &s[i + 1..]);
		let exp: i64 = exp.parse().map_err(|_| Reject::new("invalid decimal exponent"))?;
		let neg = mantissa.starts_with('-');
		let mantissa = mantissa.trim_start_matches(['-', '+']);
		let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
		let digits = format!("{int}{frac}");
		let scale = frac.len() as i64 - exp;
		let plain = if scale <= 0 {
			format!("{digits}{}", "0".repeat((-scale) as usize))
		} else {
			let scale = scale as usize;
			let padded = format!("{}{digits}", "0".repeat(scale.saturating_sub(digits.len()) + 1));
			let at = padded.len() - scale;
			format!("{}.{}", &padded[..at], &padded[at..])
		};
		let plain = if neg {
			format!("-{plain}")
		} else {
			plain
		};
		parse_as(Literal::Decimal, &plain)
	} else {
		normalized_as(Literal::Decimal, &s)
	};
	d.map(Literal::Decimal).ok_or(Reject::new("invalid decimal"))
}

/// Whether the regex literal at `start..end` of `src`, with its
/// delimiters, compiles ([`surrealdb_parser_next::Limits::regex`]).
pub fn is_regex(src: &str, start: u32, end: u32) -> bool {
	src.get(start as usize..end as usize).is_some_and(|text| regex(text).is_ok())
}

/// The message for the regex literal `text` that does not compile.
pub fn regex_error(text: &str) -> String {
	regex(text).err().map(|r| r.msg.into_owned()).unwrap_or_default()
}

/// A regex literal; a pattern that does not compile is reported over the
/// literal's span, `start..end`, with the regex engine's message.
pub fn regex_at(text: &str, start: u32, end: u32) -> R<Literal> {
	regex(text).map_err(|r| r.at(start, end))
}

pub fn regex(text: &str) -> R<Literal> {
	let body = &text[1..text.len() - 1];
	let mut out = String::with_capacity(body.len());
	let mut chars = body.chars();
	while let Some(c) = chars.next() {
		if c != '\\' {
			out.push(c);
			continue;
		}
		match chars.next() {
			Some('0') => out.push('\0'),
			Some('/') => out.push('/'),
			Some(x) => {
				out.push('\\');
				out.push(x);
			}
			None => return Err(Reject::new("invalid escape sequence")),
		}
	}
	parse_as_msg(Literal::Regex, &out)
		.map(Literal::Regex)
		.map_err(|e| Reject::new(format!("Invalid regex syntax {e}")))
}

/// A failure over bytes `start..end` of a string body.
fn body_err(msg: impl Into<std::borrow::Cow<'static, str>>, start: usize, end: usize) -> Reject {
	Reject::new(msg).at(start as u32, end as u32)
}

/// `syn`'s error for the unexpected character at byte `at` of `s`: `msg`
/// with `{c}` replaced by it, over the whole character.
fn char_err(s: &str, at: usize, msg: &str) -> Reject {
	let c = s.get(at..).and_then(|r| r.chars().next()).unwrap_or('\0');
	body_err(msg.replace("{c}", c.encode_utf8(&mut [0; 4])), at, at + c.len_utf8())
}

/// Reads `min` digits and up to `max - min + 1` more, as `syn` does, and
/// checks the value against `range`.
fn digits(
	s: &str,
	at: &mut usize,
	min: usize,
	max: usize,
	range: std::ops::RangeInclusive<usize>,
) -> R<usize> {
	let b = s.as_bytes();
	let start = *at;
	let mut v = 0usize;
	for _ in 0..min {
		match b.get(*at) {
			Some(d) if d.is_ascii_digit() => {
				v = v * 10 + (d - b'0') as usize;
				*at += 1;
			}
			Some(_) => {
				return Err(char_err(
					s,
					*at,
					"Invalid datetime, expected digit character found `{c}`",
				));
			}
			None => {
				return Err(body_err(
					"Expected end of datetime, expected datetime digit character",
					*at,
					*at,
				));
			}
		}
	}
	for _ in min..=max {
		match b.get(*at) {
			Some(d) if d.is_ascii_digit() => {
				v = v.saturating_mul(10).saturating_add((d - b'0') as usize);
				*at += 1;
			}
			_ => break,
		}
	}
	if !range.contains(&v) {
		return Err(body_err(
			"Invalid datetime digit section, section not within allowed range",
			start,
			*at,
		)
		.with_label(format!("This section must be within {}..={}", range.start(), range.end())));
	}
	Ok(v)
}

fn sep(s: &str, at: &mut usize, c: u8) -> R<()> {
	match s.as_bytes().get(*at) {
		Some(&x) if x == c => {
			*at += 1;
			Ok(())
		}
		Some(_) => Err(char_err(
			s,
			*at,
			&format!(
				"Unexpected character `{{c}}`, expected datetime separator characters `{}`",
				c as char
			),
		)),
		None => Err(body_err(
			format!(
				"Expected end of string, expected datetime separator character `{}`",
				c as char
			),
			*at,
			*at,
		)),
	}
}

/// Days from 1970-01-01 to the proleptic Gregorian date `y-m-d`.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
	let y = if m <= 2 {
		y - 1
	} else {
		y
	};
	let era = y.div_euclid(400);
	let yoe = y - era * 400;
	let mp = (m + 9) % 12;
	let doy = (153 * mp + 2) / 5 + d - 1;
	let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
	era * 146_097 + doe - 719_468
}

fn days_in_month(y: i64, m: i64) -> i64 {
	match m {
		1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
		4 | 6 | 9 | 11 => 30,
		_ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
		_ => 28,
	}
}

/// A datetime string body, as `syn`'s `lex_datetime` reads it. Error spans
/// are byte offsets into `s`.
pub fn datetime(s: &str) -> R<Literal> {
	let b = s.as_bytes();
	let mut at = 0;
	let neg = b.first() == Some(&b'-');
	if neg || b.first() == Some(&b'+') {
		at += 1;
	}
	let year = digits(s, &mut at, 4, 6, 0..=usize::MAX)? as i64;
	sep(s, &mut at, b'-')?;
	let month = digits(s, &mut at, 2, 2, 1..=12)? as i64;
	sep(s, &mut at, b'-')?;
	let day = digits(s, &mut at, 2, 2, 1..=31)? as i64;
	let year = if neg {
		-year
	} else {
		year
	};
	// chrono's `NaiveDate` range.
	if !(-262_144..=262_143).contains(&year) || day > days_in_month(year, month) {
		return Err(body_err("Invalid Datetime date: date outside of valid range", 0, at));
	}
	let days = days_from_civil(year, month, day);
	let before = at;
	let (secs, nanos) = match b.get(at) {
		None => (days * 86_400, 0u32),
		Some(b't' | b'T' | b' ') => {
			at += 1;
			let time_start = at;
			let hour = digits(s, &mut at, 2, 2, 0..=24)? as i64;
			sep(s, &mut at, b':')?;
			let minute = digits(s, &mut at, 2, 2, 0..=59)? as i64;
			sep(s, &mut at, b':')?;
			let second = digits(s, &mut at, 2, 2, 0..=60)? as i64;
			let mut nanos = 0u32;
			if b.get(at) == Some(&b'.') {
				at += 1;
				let nanos_start = at;
				let mut count = 0;
				while let Some(d) = b.get(at).filter(|d| d.is_ascii_digit()) {
					at += 1;
					// Past nine digits each digit of five or more rounds up,
					// as in `syn`.
					if count == 9 {
						if d - b'0' >= 5 {
							nanos += 1;
						}
						continue;
					}
					nanos = nanos * 10 + (d - b'0') as u32;
					count += 1;
				}
				if count == 0 {
					return Err(body_err(
						"Invalid datetime nanoseconds, expected at least a single digit",
						nanos_start,
						at,
					));
				}
				for _ in count..9 {
					nanos *= 10;
				}
			}
			// chrono's `NaiveTime::from_hms_nano_opt`: a leap second only at
			// second 59.
			if hour > 23
				|| second > 59
				|| nanos >= 2_000_000_000
				|| (nanos >= 1_000_000_000 && second != 59)
			{
				return Err(body_err(
					"Invalid Datetime time: time outside of valid range",
					time_start,
					at,
				));
			}
			let timezone_start = at;
			let offset = match b.get(at) {
				Some(&x @ (b'-' | b'+')) => {
					at += 1;
					let h = digits(s, &mut at, 2, 2, 0..=23)? as i64;
					sep(s, &mut at, b':')?;
					let m = digits(s, &mut at, 2, 2, 0..=59)? as i64;
					let o = h * 3600 + m * 60;
					if x == b'-' {
						-o
					} else {
						o
					}
				}
				Some(b'Z' | b'z') => {
					at += 1;
					0
				}
				Some(_) => {
					// `syn` spans from the time separator.
					let mut r = char_err(
						s,
						at,
						"Unexpected character `{c}`, expected `Z` or a timezone offset.",
					);
					r.span = r.span.map(|(_, e)| (before as u32, e));
					return Err(r);
				}
				None => {
					return Err(body_err(
						"Invalid end of datetime, expected datetime timezone",
						timezone_start,
						at,
					));
				}
			};
			let secs = days * 86_400 + hour * 3600 + minute * 60 + second - offset;
			if surrealdb_types::Datetime::from_timestamp(secs, nanos).is_none() {
				return Err(body_err(
					"Invalid Datetime, timezone was outside of the range of valid datetimes",
					timezone_start,
					at,
				));
			}
			(secs, nanos)
		}
		Some(_) => {
			return Err(char_err(s, at, "Unexpected character `{c}`, expected time separator `T`"));
		}
	};
	let dt = surrealdb_types::Datetime::from_timestamp(secs, nanos).ok_or(body_err(
		"Invalid Datetime date: date outside of valid range",
		0,
		at,
	))?;
	Ok(Literal::Datetime(dt.into_inner()))
}

/// A uuid string body: 8-4-4-4-12 hexadecimal digits, anything after them
/// ignored, as `syn`'s `lex_uuid` reads it. Error spans are byte offsets
/// into `s`.
pub fn uuid(s: &str) -> R<Literal> {
	const END: &str = "Unexpected end of string, expected UUID token to finish";
	let b = s.as_bytes();
	let mut hex = String::with_capacity(36);
	let mut at = 0;
	for (i, n) in [8, 4, 4, 4, 12].into_iter().enumerate() {
		if i > 0 {
			match b.get(at) {
				Some(b'-') => {}
				Some(_) => {
					return Err(char_err(
						s,
						at,
						"Unexpected character `{c}`, expected byte separator `-`",
					));
				}
				None => return Err(body_err(END, at, at)),
			}
			hex.push('-');
			at += 1;
		}
		for _ in 0..n {
			match b.get(at) {
				Some(c) if c.is_ascii_hexdigit() => hex.push(*c as char),
				Some(_) => {
					return Err(char_err(
						s,
						at,
						"Unexpected character `{c}` expected hexidecimal digit",
					));
				}
				None => return Err(body_err(END, at, at)),
			}
			at += 1;
		}
	}
	parse_as(Literal::Uuid, &hex).map(Literal::Uuid).ok_or(Reject::new("invalid uuid"))
}

/// A byte-string body: pairs of hexadecimal digits, as `syn`'s `lex_bytes`
/// reads it. Error spans are byte offsets into `s`.
pub fn bytes(s: &str) -> R<Literal> {
	const NOT_HEX: &str = "Unexpected character `{c}`, expected a hexidecimal digit";
	let b = s.as_bytes();
	let hex = |at: usize| {
		(b[at] as char).to_digit(16).map(|d| d as u8).ok_or_else(|| char_err(s, at, NOT_HEX))
	};
	let mut v = Vec::with_capacity(b.len() / 2);
	let mut at = 0;
	while at < b.len() {
		let hi = hex(at)?;
		at += 1;
		if at == b.len() {
			return Err(body_err(
				"Unexpected end of byte-string, expected a hexidecimal digit",
				at,
				at,
			));
		}
		v.push(hi << 4 | hex(at)?);
		at += 1;
	}
	Ok(Literal::Bytes(from_vec_as(Literal::Bytes, v)))
}

/// A file string body, `bucket:/key`, as `syn`'s `lex_file` reads it: the
/// key ends at a `:`. Error spans are byte offsets into `s`.
pub fn file(s: &str) -> R<surrealdb_types::File> {
	let b = s.as_bytes();
	let ok = |c: u8| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.');
	let mut at = 0;
	loop {
		match b.get(at) {
			None => {
				return Err(body_err(
					"Unexpected end of file string, missing bucket separator `:/`",
					at,
					at,
				));
			}
			Some(b':') => break,
			Some(&c) if ok(c) => at += 1,
			Some(_) => {
				return Err(char_err(
					s,
					at,
					"Unexpected character `{c}`, file strings buckets only allow alpha numeric characters and `_`, `-`, and `.`",
				));
			}
		}
	}
	let bucket = &s[..at];
	at += 1;
	match b.get(at) {
		Some(b'/') => {}
		Some(_) => return Err(char_err(s, at, "Unexpected character `{c}`, expected `/`")),
		None => {
			return Err(body_err(
				"Unexpected end of file string, missing file string key.",
				at,
				at,
			));
		}
	}
	let start = at;
	while let Some(&c) = b.get(at) {
		match c {
			b':' => break,
			c if ok(c) || c == b'/' => at += 1,
			_ => {
				return Err(char_err(
					s,
					at,
					"Unexpected character `{c}`, file strings key's only allow alpha numeric characters and `_`, `-`, `.`, and `/`",
				));
			}
		}
	}
	Ok(surrealdb_types::File::new(bucket.to_owned(), s[start..at].to_owned()))
}

/// `r`, a failure in the decoded body of the string token `token` at byte
/// `start`, with its spans moved onto the token as `syn` moves them.
pub fn in_token(mut r: Reject, token: &str, start: u32) -> Reject {
	let at = |o: u32| start + escaped_offset(token, o);
	r.span = r.span.map(|(a, b)| (at(a), at(b)));
	r.extra = r.extra.map(|(a, b, l)| (at(a), at(b), l));
	r
}

/// Where byte `offset` of the decoded body of the string token `token` lies
/// in the token (`syn`'s `escaped_string_offset`).
fn escaped_offset(token: &str, offset: u32) -> u32 {
	let b = token.as_bytes();
	let mut i = if matches!(b.first(), Some(b'"' | b'\'')) {
		1
	} else {
		2
	};
	let mut idx = 0u32;
	let hex = |c: u8| (c as char).to_digit(16);
	while idx < offset {
		let Some(&c) = b.get(i) else {
			break;
		};
		i += 1;
		if c != b'\\' {
			idx += 1;
			continue;
		}
		let e = b.get(i).copied();
		i += 1;
		if e != Some(b'u') {
			idx += 1;
			continue;
		}
		let mut v = 0u32;
		if b.get(i) == Some(&b'{') {
			i += 1;
			let mut closed = false;
			for _ in 0..6 {
				let d = b.get(i).copied().unwrap_or(b'}');
				i += 1;
				if d == b'}' {
					closed = true;
					break;
				}
				v = v << 4 | hex(d).unwrap_or(0);
			}
			if !closed {
				i += 1;
			}
		} else {
			for _ in 0..4 {
				v = v << 4 | b.get(i).copied().and_then(hex).unwrap_or(0);
				i += 1;
			}
		}
		idx += char::from_u32(v).map_or(3, |c| c.len_utf8() as u32);
	}
	i as u32
}

/// What a builtin path names.
pub enum Builtin {
	Function(String),
	Constant(surrealdb_sql::Constant),
}

/// Looks `path` up in the builtin registry.
pub fn builtin(path: &str, start: u32, end: u32) -> R<Builtin> {
	let lower = path.to_lowercase();
	if let Some(kind) = builtin_paths::lookup(&lower) {
		return Ok(match kind {
			PathKind::Function => Builtin::Function(lower),
			PathKind::Constant(c) => Builtin::Constant(c.clone()),
		});
	}
	let msg = match find_suggestion(&lower) {
		Some(s) => format!("Invalid function/constant path, did you maybe mean `{s}`").into(),
		None => std::borrow::Cow::Borrowed("Invalid function/constant path"),
	};
	Err(Reject::new(msg).at(start, end))
}

/// A path as written (segments separated by `::`, possibly escaped),
/// decoded and lower-cased as `syn` looks it up.
pub fn decode_path(text: &str) -> Option<String> {
	let mut out = String::new();
	for (i, seg) in text.split("::").enumerate() {
		if i > 0 {
			out.push_str("::");
		}
		let seg = seg.trim();
		if seg.starts_with('`') || seg.starts_with('⟨') {
			let mut s = String::new();
			if !decode::unescape(decode::strip_delims(seg), &mut s) {
				return None;
			}
			out.push_str(&s);
		} else {
			out.push_str(seg);
		}
	}
	Some(out.to_lowercase())
}

/// Whether the builtin path written at `start..end` of `src` exists
/// ([`surrealdb_parser_next::Limits::builtin`]).
pub fn is_builtin(src: &str, start: u32, end: u32) -> bool {
	src.get(start as usize..end as usize)
		.and_then(decode_path)
		.is_some_and(|p| builtin_paths::lookup(&p).is_some())
}

/// `, did you maybe mean `…`` for an unknown builtin path written as
/// `text`, or nothing.
pub fn suggestion(text: &str) -> String {
	match decode_path(text).as_deref().and_then(find_suggestion) {
		Some(s) => format!(", did you maybe mean `{s}`"),
		None => String::new(),
	}
}

const MAX_LEVENSHTEIN_CUT_OFF: u8 = 4;

/// Levenshtein distance between `a` and `b`, ignoring ASCII case, or
/// `cut_off + 1` once it must exceed `cut_off`.
fn levenshtein(a: &[u8], b: &[u8], cut_off: u8) -> u8 {
	if a.len().abs_diff(b.len()) > cut_off as usize {
		return cut_off + 1;
	}
	let mut rows = [vec![0u8; a.len() + 1], vec![0u8; a.len() + 1]];
	for (i, item) in rows[0].iter_mut().enumerate().skip(1) {
		*item = i.min(255) as u8;
	}
	for i in 1..=b.len() {
		let cur = i & 1;
		let prev = cur ^ 1;
		rows[cur][0] = i.min(255) as u8;
		let mut lowest = rows[cur][0];
		for j in 1..=a.len() {
			let cost = (!a[j - 1].eq_ignore_ascii_case(&b[i - 1])) as u8;
			let res = (rows[prev][j].saturating_add(1))
				.min(rows[cur][j - 1].saturating_add(1))
				.min(rows[prev][j - 1].saturating_add(cost));
			rows[cur][j] = res;
			lowest = lowest.min(res);
		}
		if lowest > cut_off {
			return cut_off + 1;
		}
	}
	rows[b.len() & 1][a.len()]
}

/// The path `syn` suggests for an unknown one: the current name of a
/// deprecated path, or else the nearest path within edit distance 3.
fn find_suggestion(got: &str) -> Option<&'static str> {
	if let Some(p) = PATHS.entries().find_map(|(path, (_, old))| match old {
		Some(s) if s.into_inner() == got => Some(path.into_inner()),
		_ => None,
	}) {
		return Some(p);
	}
	let mut cut_off = MAX_LEVENSHTEIN_CUT_OFF;
	let best = PATHS
		.keys()
		.copied()
		.min_by_key(|x| {
			let d = levenshtein(got.as_bytes(), x.as_bytes(), cut_off);
			cut_off = d.min(cut_off);
			d
		})
		.map(|x| x.into_inner());
	if cut_off >= MAX_LEVENSHTEIN_CUT_OFF {
		return None;
	}
	best
}

/// A plain string under `legacy_strands`: a uuid, datetime or record id
/// when the whole of its contents reads as one, otherwise the string.
pub fn legacy_strand(s: String) -> Literal {
	if let Ok(l) = uuid(&s) {
		return l;
	}
	if let Ok(l) = datetime(&s) {
		return l;
	}
	let mut ast = Ast::new();
	let mut scratch = surrealdb_parser_next::Scratch::default();
	if let Some(root) = surrealdb_parser_next::parse_record_id_all(&s, &mut ast, &mut scratch) {
		// The contents are read with `syn`'s default settings, which leave
		// the optional grammars off.
		let inner = Lower {
			ast: &ast,
			src: &s,
			opts: Options {
				legacy_strands: false,
				files: false,
				surrealism: false,
			},
		};
		if let Ok(r) = inner.record_id(root) {
			return Literal::RecordId(r);
		}
	}
	Literal::String(Strand::new(s))
}

impl Lower<'_> {
	/// A prefixed string leaf: datetime, uuid, bytes or record id.
	pub fn prefixed(&self, id: NodeId) -> R<Expr> {
		let n = self.n(id);
		let text = self.text(id);
		let body = string_body(text)?;
		let at = |r| in_token(r, text, n.start);
		Ok(Expr::Literal(match n.op {
			b'd' => datetime(&body).map_err(at)?,
			b'u' => uuid(&body).map_err(at)?,
			b'b' => bytes(&body).map_err(at)?,
			_ => {
				// A record id parsed from the string's contents.
				let mut ast = Ast::new();
				let mut scratch = surrealdb_parser_next::Scratch::default();
				let Some(root) =
					surrealdb_parser_next::parse_record_id(&body, &mut ast, &mut scratch)
				else {
					let d = scratch.diags.first().expect("a failed parse records a diagnostic");
					return Err(at(super::render::reject(&body, d)));
				};
				let inner = Lower {
					ast: &ast,
					src: &body,
					opts: self.opts,
				};
				Literal::RecordId(inner.record_id(root).map_err(at)?)
			}
		}))
	}
}

/// `syn`'s error for the string token `text` at `start` with prefix
/// `prefix` whose contents do not decode, if they do not.
pub fn prefixed_error(text: &str, prefix: u8, start: u32) -> Option<Reject> {
	if !matches!(prefix, b'd' | b'u' | b'b' | b'f' | b'r') {
		return None;
	}
	let body = string_body(text).ok()?;
	let r = match prefix {
		b'd' => datetime(&body).err(),
		b'u' => uuid(&body).err(),
		b'b' => bytes(&body).err(),
		b'f' => file(&body).err(),
		_ => {
			let mut ast = Ast::new();
			let mut scratch = surrealdb_parser_next::Scratch::default();
			match surrealdb_parser_next::parse_record_id(&body, &mut ast, &mut scratch) {
				None => scratch.diags.first().map(|d| super::render::reject(&body, d)),
				Some(root) => Lower {
					ast: &ast,
					src: &body,
					opts: Options::ALL,
				}
				.record_id(root)
				.err(),
			}
		}
	}?;
	Some(in_token(r, text, start))
}

/// Decodes standard padded base64.
fn base64(s: &str) -> Option<Vec<u8>> {
	let b = s.as_bytes();
	if !b.len().is_multiple_of(4) {
		return None;
	}
	let val = |c: u8| -> Option<u32> {
		Some(match c {
			b'A'..=b'Z' => c - b'A',
			b'a'..=b'z' => c - b'a' + 26,
			b'0'..=b'9' => c - b'0' + 52,
			b'+' => 62,
			b'/' => 63,
			_ => return None,
		} as u32)
	};
	let mut out = Vec::with_capacity(b.len() / 4 * 3);
	for (i, chunk) in b.chunks(4).enumerate() {
		let last = i == b.len() / 4 - 1;
		let pad = chunk.iter().rev().take_while(|c| **c == b'=').count();
		if pad > 2 || (pad > 0 && !last) {
			return None;
		}
		let mut n = 0u32;
		for &c in &chunk[..4 - pad] {
			n = n << 6 | val(c)?;
		}
		n <<= 6 * pad as u32;
		let bytes = [(n >> 16) as u8, (n >> 8) as u8, n as u8];
		// Padding bits must be zero.
		let keep = 3 - pad;
		if pad > 0 && bytes[keep..].iter().any(|b| *b != 0) {
			return None;
		}
		out.extend_from_slice(&bytes[..keep]);
	}
	Some(out)
}

/// Whether `s` is a valid `SCRAM-SHA-256$<iterations>:<salt>$<stored>:<server>`
/// verifier, as the engine's credential parser requires.
pub fn scram_verifier(s: &str) -> bool {
	let check = || -> Option<()> {
		let (mechanism, rest) = s.split_once('$')?;
		if mechanism != "SCRAM-SHA-256" {
			return None;
		}
		let (iter_salt, keys) = rest.split_once('$')?;
		let (iterations, salt) = iter_salt.split_once(':')?;
		let (stored, server) = keys.split_once(':')?;
		let iterations: u32 = iterations.parse().ok()?;
		if !(4096..=1 << 24).contains(&iterations) {
			return None;
		}
		let (salt, stored, server) = (base64(salt)?, base64(stored)?, base64(server)?);
		(!salt.is_empty() && stored.len() == 32 && server.len() == 32).then_some(())
	};
	check().is_some()
}
