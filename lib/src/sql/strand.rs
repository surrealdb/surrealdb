use crate::sql::error::Error::Parser;
use crate::sql::error::IResult;
use crate::sql::escape::quote_str;
use nom::branch::alt;
use nom::bytes::complete::{escaped_transform, is_not, tag, take, take_while_m_n};
use nom::character::complete::char;
use nom::combinator::value;
use nom::sequence::preceded;
use nom::Err::Failure;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::ops::{self, RangeInclusive};
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Strand";

const SINGLE: char = '\'';
const SINGLE_ESC_NUL: &str = "'\\\0";

const DOUBLE: char = '"';
const DOUBLE_ESC_NUL: &str = "\"\\\0";

const LEADING_SURROGATES: RangeInclusive<u16> = 0xD800..=0xDBFF;
const TRAILING_SURROGATES: RangeInclusive<u16> = 0xDC00..=0xDFFF;

/// A string that doesn't contain NUL bytes.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Strand")]
pub struct Strand(#[serde(with = "no_nul_bytes")] pub String);

impl From<String> for Strand {
	fn from(s: String) -> Self {
		debug_assert!(!s.contains('\0'));
		Strand(s)
	}
}

impl From<&str> for Strand {
	fn from(s: &str) -> Self {
		debug_assert!(!s.contains('\0'));
		Self::from(String::from(s))
	}
}

impl Deref for Strand {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<Strand> for String {
	fn from(s: Strand) -> Self {
		s.0
	}
}

impl Strand {
	/// Get the underlying String slice
	pub fn as_str(&self) -> &str {
		self.0.as_str()
	}
	/// Returns the underlying String
	pub fn as_string(self) -> String {
		self.0
	}
	/// Convert the Strand to a raw String
	pub fn to_raw(self) -> String {
		self.0
	}
}

impl Display for Strand {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&quote_str(&self.0), f)
	}
}

impl ops::Add for Strand {
	type Output = Self;
	fn add(mut self, other: Self) -> Self {
		self.0.push_str(other.as_str());
		self
	}
}

pub fn strand(i: &str) -> IResult<&str, Strand> {
	let (i, v) = strand_raw(i)?;
	Ok((i, Strand(v)))
}

pub fn strand_raw(i: &str) -> IResult<&str, String> {
	alt((strand_blank, strand_single, strand_double))(i)
}

fn strand_blank(i: &str) -> IResult<&str, String> {
	alt((
		|i| {
			let (i, _) = char(SINGLE)(i)?;
			let (i, _) = char(SINGLE)(i)?;
			Ok((i, String::new()))
		},
		|i| {
			let (i, _) = char(DOUBLE)(i)?;
			let (i, _) = char(DOUBLE)(i)?;
			Ok((i, String::new()))
		},
	))(i)
}

fn strand_single(i: &str) -> IResult<&str, String> {
	let (i, _) = char(SINGLE)(i)?;
	let (i, v) = escaped_transform(
		is_not(SINGLE_ESC_NUL),
		'\\',
		alt((
			char_unicode,
			value('\u{5c}', char('\\')),
			value('\u{27}', char('\'')),
			value('\u{2f}', char('/')),
			value('\u{08}', char('b')),
			value('\u{0c}', char('f')),
			value('\u{0a}', char('n')),
			value('\u{0d}', char('r')),
			value('\u{09}', char('t')),
		)),
	)(i)?;
	let (i, _) = char(SINGLE)(i)?;
	Ok((i, v))
}

fn strand_double(i: &str) -> IResult<&str, String> {
	let (i, _) = char(DOUBLE)(i)?;
	let (i, v) = escaped_transform(
		is_not(DOUBLE_ESC_NUL),
		'\\',
		alt((
			char_unicode,
			value('\u{5c}', char('\\')),
			value('\u{22}', char('\"')),
			value('\u{2f}', char('/')),
			value('\u{08}', char('b')),
			value('\u{0c}', char('f')),
			value('\u{0a}', char('n')),
			value('\u{0d}', char('r')),
			value('\u{09}', char('t')),
		)),
	)(i)?;
	let (i, _) = char(DOUBLE)(i)?;
	Ok((i, v))
}

fn char_unicode(i: &str) -> IResult<&str, char> {
	preceded(char('u'), alt((char_unicode_bracketed, char_unicode_bare)))(i)
}

// \uABCD or \uDBFF\uDFFF (surrogate pair)
fn char_unicode_bare(i: &str) -> IResult<&str, char> {
	// Take exactly 4 bytes
	let (i, v) = take(4usize)(i)?;
	// Parse them as hex, where an error indicates invalid hex digits
	let v: u16 = u16::from_str_radix(v, 16).map_err(|_| Failure(Parser(i)))?;

	if LEADING_SURROGATES.contains(&v) {
		let leading = v;

		// Read the next \u.
		let (i, _) = tag("\\u")(i)?;
		// Take exactly 4 more bytes
		let (i, v) = take(4usize)(i)?;
		// Parse them as hex, where an error indicates invalid hex digits
		let trailing = u16::from_str_radix(v, 16).map_err(|_| Failure(Parser(i)))?;
		if !TRAILING_SURROGATES.contains(&trailing) {
			return Err(Failure(Parser(i)));
		}
		// Compute the codepoint.
		// https://datacadamia.com/data/type/text/surrogate#from_surrogate_to_character_code
		let codepoint = 0x10000
			+ ((leading as u32 - *LEADING_SURROGATES.start() as u32) << 10)
			+ trailing as u32
			- *TRAILING_SURROGATES.start() as u32;
		// Convert to char
		let v = char::from_u32(codepoint).ok_or(Failure(Parser(i)))?;
		// Return the char
		Ok((i, v))
	} else {
		// We can convert this to char or error in the case of invalid Unicode character
		let v = char::from_u32(v as u32).filter(|c| *c != 0 as char).ok_or(Failure(Parser(i)))?;
		// Return the char
		Ok((i, v))
	}
}

// \u{10ffff}
fn char_unicode_bracketed(i: &str) -> IResult<&str, char> {
	// Read the { character
	let (i, _) = char('{')(i)?;
	// Let's up to 6 ascii hexadecimal characters
	let (i, v) = take_while_m_n(1, 6, |c: char| c.is_ascii_hexdigit())(i)?;
	// We can convert this to u32 as the max is 0xffffff
	let v = u32::from_str_radix(v, 16).unwrap();
	// We can convert this to char or error in the case of invalid Unicode character
	let v = char::from_u32(v).filter(|c| *c != 0 as char).ok_or(Failure(Parser(i)))?;
	// Read the } character
	let (i, _) = char('}')(i)?;
	// Return the char
	Ok((i, v))
}

// serde(with = no_nul_bytes) will (de)serialize with no NUL bytes.
pub(crate) mod no_nul_bytes {
	use serde::{
		de::{self, Visitor},
		Deserializer, Serializer,
	};
	use std::fmt;

	pub(crate) fn serialize<S>(s: &str, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		debug_assert!(!s.contains('\0'));
		serializer.serialize_str(s)
	}

	pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct NoNulBytesVisitor;

		impl<'de> Visitor<'de> for NoNulBytesVisitor {
			type Value = String;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("a string without any NUL bytes")
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				if value.contains('\0') {
					Err(de::Error::custom("contained NUL byte"))
				} else {
					Ok(value.to_owned())
				}
			}

			fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				if value.contains('\0') {
					Err(de::Error::custom("contained NUL byte"))
				} else {
					Ok(value)
				}
			}
		}

		deserializer.deserialize_string(NoNulBytesVisitor)
	}
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn strand_empty() {
		let sql = r#""""#;
		let res = strand(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#"''"#, format!("{}", out));
		assert_eq!(out, Strand::from(""));
	}

	#[test]
	fn strand_single() {
		let sql = r#"'test'"#;
		let res = strand(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#"'test'"#, format!("{}", out));
		assert_eq!(out, Strand::from("test"));
	}

	#[test]
	fn strand_double() {
		let sql = r#""test""#;
		let res = strand(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#"'test'"#, format!("{}", out));
		assert_eq!(out, Strand::from("test"));
	}

	#[test]
	fn strand_quoted_single() {
		let sql = r#"'te\'st'"#;
		let res = strand(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#""te'st""#, format!("{}", out));
		assert_eq!(out, Strand::from(r#"te'st"#));
	}

	#[test]
	fn strand_quoted_double() {
		let sql = r#""te\"st""#;
		let res = strand(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#"'te"st'"#, format!("{}", out));
		assert_eq!(out, Strand::from(r#"te"st"#));
	}

	#[test]
	fn strand_quoted_escaped() {
		let sql = r#""te\"st\n\tand\bsome\u05d9""#;
		let res = strand(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("'te\"st\n\tand\u{08}some\u{05d9}'", format!("{}", out));
		assert_eq!(out, Strand::from("te\"st\n\tand\u{08}some\u{05d9}"));
	}

	#[test]
	fn strand_nul_byte() {
		assert!(strand("'a\0b'").is_err());
		assert!(strand("'a\\u0000b'").is_err());
		assert!(strand("'a\\u{0}b'").is_err());
	}

	#[test]
	fn strand_fuzz_escape() {
		for n in (1..=char::MAX as u32).step_by(101) {
			if let Some(c) = char::from_u32(n) {
				let expected = format!("a{c}b");

				let utf32 = format!("\"a\\u{{{n:x}}}b\"");
				let (rest, s) = strand(&utf32).unwrap();
				assert_eq!(rest, "");
				assert_eq!(s.as_str(), &expected);

				let mut utf16 = String::with_capacity(16);
				utf16 += "\"a";
				let mut buf = [0; 2];
				for &mut n in c.encode_utf16(&mut buf) {
					utf16 += &format!("\\u{n:04x}");
				}
				utf16 += "b\"";
				let (rest, s) = strand(&utf16).unwrap();
				assert_eq!(rest, "");
				assert_eq!(s.as_str(), &expected);
			}
		}

		// Unpaired surrogate.
		assert!(strand("\"\\u{DBFF}\"").is_err());
	}
}
