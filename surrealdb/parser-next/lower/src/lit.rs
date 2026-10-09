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
use surrealdb_sql::builtin_paths::{PATHS, PathKind};
use surrealdb_sql::{Expr, Literal};
use surrealdb_strand::Strand;

use super::{Lower, Options, R, Reject};

/// Parses `s` as the type `variant` wraps.
fn parse_as<T: FromStr, U>(_variant: fn(T) -> U, s: &str) -> Option<T> {
	s.parse().ok()
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
		return Err(Reject("invalid escape"));
	}
	Ok(s)
}

/// A decimal literal span (`1.5dec`, `-2e3dec`).
pub fn decimal(text: &str) -> R<Literal> {
	let s: String = text.trim_end_matches("dec").chars().filter(|c| *c != '_').collect();
	let d = if let Some(i) = s.find(['e', 'E']) {
		// Scientific notation: the exponent shifts the decimal point.
		let (mantissa, exp) = (&s[..i], &s[i + 1..]);
		let exp: i64 = exp.parse().map_err(|_| Reject("invalid decimal exponent"))?;
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
	d.map(Literal::Decimal).ok_or(Reject("invalid decimal"))
}

/// A regex literal span (`/…/`).
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
			None => return Err(Reject("invalid escape sequence")),
		}
	}
	parse_as(Literal::Regex, &out).map(Literal::Regex).ok_or(Reject("invalid regex syntax"))
}

/// Reads `min` digits and up to `max - min + 1` more, as `syn` does, and
/// checks the value against `range`.
fn digits(
	b: &[u8],
	at: &mut usize,
	min: usize,
	max: usize,
	range: std::ops::RangeInclusive<usize>,
) -> R<usize> {
	let mut v = 0usize;
	for _ in 0..min {
		match b.get(*at) {
			Some(d) if d.is_ascii_digit() => {
				v = v * 10 + (d - b'0') as usize;
				*at += 1;
			}
			_ => return Err(Reject("invalid datetime digit")),
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
		return Err(Reject("datetime section out of range"));
	}
	Ok(v)
}

fn sep(b: &[u8], at: &mut usize, c: u8) -> R<()> {
	if b.get(*at) == Some(&c) {
		*at += 1;
		Ok(())
	} else {
		Err(Reject("expected a datetime separator"))
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

/// A datetime string body, as `syn`'s `lex_datetime` reads it.
pub fn datetime(s: &str) -> R<Literal> {
	let b = s.as_bytes();
	let mut at = 0;
	let neg = b.first() == Some(&b'-');
	if neg || b.first() == Some(&b'+') {
		at += 1;
	}
	let year = digits(b, &mut at, 4, 6, 0..=usize::MAX)? as i64;
	sep(b, &mut at, b'-')?;
	let month = digits(b, &mut at, 2, 2, 1..=12)? as i64;
	sep(b, &mut at, b'-')?;
	let day = digits(b, &mut at, 2, 2, 1..=31)? as i64;
	let year = if neg {
		-year
	} else {
		year
	};
	// chrono's `NaiveDate` range.
	if !(-262_144..=262_143).contains(&year) || day > days_in_month(year, month) {
		return Err(Reject("date outside of valid range"));
	}
	let days = days_from_civil(year, month, day);
	let (secs, nanos) = match b.get(at) {
		None => (days * 86_400, 0u32),
		Some(b't' | b'T' | b' ') => {
			at += 1;
			let hour = digits(b, &mut at, 2, 2, 0..=24)? as i64;
			sep(b, &mut at, b':')?;
			let minute = digits(b, &mut at, 2, 2, 0..=59)? as i64;
			sep(b, &mut at, b':')?;
			let second = digits(b, &mut at, 2, 2, 0..=60)? as i64;
			let mut nanos = 0u32;
			if b.get(at) == Some(&b'.') {
				at += 1;
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
					return Err(Reject("expected fractional digits"));
				}
				for _ in count..9 {
					nanos *= 10;
				}
			}
			if hour > 23 || second > 59 || nanos >= 1_000_000_000 {
				return Err(Reject("time outside of valid range"));
			}
			let offset = match b.get(at) {
				Some(&x @ (b'-' | b'+')) => {
					at += 1;
					let h = digits(b, &mut at, 2, 2, 0..=23)? as i64;
					sep(b, &mut at, b':')?;
					let m = digits(b, &mut at, 2, 2, 0..=59)? as i64;
					let o = h * 3600 + m * 60;
					if x == b'-' {
						-o
					} else {
						o
					}
				}
				Some(b'Z' | b'z') => 0,
				_ => return Err(Reject("expected a timezone")),
			};
			(days * 86_400 + hour * 3600 + minute * 60 + second - offset, nanos)
		}
		Some(_) => return Err(Reject("expected time separator `T`")),
	};
	let dt = surrealdb_types::Datetime::from_timestamp(secs, nanos)
		.ok_or(Reject("datetime out of range"))?;
	Ok(Literal::Datetime(dt.into_inner()))
}

/// A uuid string body: 8-4-4-4-12 hexadecimal digits (anything after is
/// ignored, as by `syn`).
pub fn uuid(s: &str) -> R<Literal> {
	let b = s.as_bytes();
	let mut hex = String::with_capacity(36);
	let mut at = 0;
	for (i, n) in [8, 4, 4, 4, 12].into_iter().enumerate() {
		if i > 0 {
			if b.get(at) != Some(&b'-') {
				return Err(Reject("expected `-`"));
			}
			hex.push('-');
			at += 1;
		}
		for _ in 0..n {
			match b.get(at) {
				Some(c) if c.is_ascii_hexdigit() => hex.push(*c as char),
				_ => return Err(Reject("expected a hexadecimal digit")),
			}
			at += 1;
		}
	}
	parse_as(Literal::Uuid, &hex).map(Literal::Uuid).ok_or(Reject("invalid uuid"))
}

/// A byte-string body: pairs of hexadecimal digits.
pub fn bytes(s: &str) -> R<Literal> {
	let b = s.as_bytes();
	if !b.len().is_multiple_of(2) {
		return Err(Reject("expected a hexadecimal digit"));
	}
	let hex = |c: u8| (c as char).to_digit(16).map(|d| d as u8).ok_or(Reject("not hexadecimal"));
	let mut v = Vec::with_capacity(b.len() / 2);
	for p in b.chunks(2) {
		v.push(hex(p[0])? << 4 | hex(p[1])?);
	}
	Ok(Literal::Bytes(from_vec_as(Literal::Bytes, v)))
}

/// What a builtin path names.
pub enum Builtin {
	Function(String),
	Constant(surrealdb_sql::Constant),
}

/// Looks `path` up in the builtin registry.
pub fn builtin(path: &str) -> R<Builtin> {
	let lower = path.to_lowercase();
	for (k, (kind, _)) in PATHS.entries() {
		if k.eq_ignore_ascii_case(&lower) {
			return Ok(match kind {
				PathKind::Function => Builtin::Function(lower),
				PathKind::Constant(c) => Builtin::Constant(c.clone()),
			});
		}
	}
	Err(Reject("invalid function/constant path"))
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
		let body = string_body(self.text(id))?;
		Ok(Expr::Literal(match n.op {
			b'd' => datetime(&body)?,
			b'u' => uuid(&body)?,
			b'b' => bytes(&body)?,
			_ => {
				// A record id parsed from the string's contents.
				let mut ast = Ast::new();
				let mut scratch = surrealdb_parser_next::Scratch::default();
				let root = surrealdb_parser_next::parse_record_id(&body, &mut ast, &mut scratch)
					.ok_or(Reject("invalid record id"))?;
				let inner = Lower {
					ast: &ast,
					src: &body,
					opts: self.opts,
				};
				Literal::RecordId(inner.record_id(root)?)
			}
		}))
	}
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
