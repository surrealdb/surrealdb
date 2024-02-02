use super::super::{error::expected, IResult, ParseError};
use crate::sql::Strand;
use nom::{
	branch::alt,
	bytes::complete::{escaped_transform, is_not, tag, take, take_while_m_n},
	character::complete::char,
	combinator::{opt, value},
	sequence::preceded,
	Err,
};
use std::ops::RangeInclusive;

const LEADING_SURROGATES: RangeInclusive<u16> = 0xD800..=0xDBFF;
const TRAILING_SURROGATES: RangeInclusive<u16> = 0xDC00..=0xDFFF;

pub fn strand(i: &str) -> IResult<&str, Strand> {
	let (i, v) = strand_raw(i)?;
	Ok((i, Strand(v)))
}

pub fn strand_raw(i: &str) -> IResult<&str, String> {
	expected("a strand", alt((strand_blank, strand_single, strand_double)))(i)
}

fn strand_blank(i: &str) -> IResult<&str, String> {
	alt((
		|i| {
			let (i, _) = opt(char('s'))(i)?;
			let (i, _) = char('\'')(i)?;
			let (i, _) = char('\'')(i)?;
			Ok((i, String::new()))
		},
		|i| {
			let (i, _) = opt(char('s'))(i)?;
			let (i, _) = char('\"')(i)?;
			let (i, _) = char('\"')(i)?;
			Ok((i, String::new()))
		},
	))(i)
}

fn strand_single(i: &str) -> IResult<&str, String> {
	let (i, _) = opt(char('s'))(i)?;
	let (i, _) = char('\'')(i)?;
	let (i, v) = escaped_transform(
		is_not("\'\\\0"),
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
	let (i, _) = char('\'')(i)?;
	Ok((i, v))
}

fn strand_double(i: &str) -> IResult<&str, String> {
	let (i, _) = opt(char('s'))(i)?;
	let (i, _) = char('\"')(i)?;
	let (i, v) = escaped_transform(
		is_not("\"\\\0"),
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
	let (i, _) = char('\"')(i)?;
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
	let v: u16 = u16::from_str_radix(v, 16).map_err(|_| {
		Err::Failure(ParseError::InvalidUnicode {
			tried: i,
		})
	})?;

	if LEADING_SURROGATES.contains(&v) {
		let leading = v;

		// Read the next \u.
		let (i, _) = tag("\\u")(i)?;
		// Take exactly 4 more bytes
		let (i, v) = take(4usize)(i)?;
		// Parse them as hex, where an error indicates invalid hex digits
		let trailing = u16::from_str_radix(v, 16).map_err(|_| {
			Err::Failure(ParseError::InvalidUnicode {
				tried: i,
			})
		})?;
		if !TRAILING_SURROGATES.contains(&trailing) {
			return Err(Err::Failure(ParseError::InvalidUnicode {
				tried: i,
			}));
		}
		// Compute the codepoint.
		// https://datacadamia.com/data/type/text/surrogate#from_surrogate_to_character_code
		let codepoint = 0x10000
			+ ((leading as u32 - *LEADING_SURROGATES.start() as u32) << 10)
			+ trailing as u32
			- *TRAILING_SURROGATES.start() as u32;
		// Convert to char
		let v = char::from_u32(codepoint).ok_or(Err::Failure(ParseError::InvalidUnicode {
			tried: i,
		}))?;
		// Return the char
		Ok((i, v))
	} else {
		// We can convert this to char or error in the case of invalid Unicode character
		let v = char::from_u32(v as u32).filter(|c| *c != 0 as char).ok_or(Err::Failure(
			ParseError::InvalidUnicode {
				tried: i,
			},
		))?;
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
	let v = char::from_u32(v).filter(|c| *c != 0 as char).ok_or(Err::Failure(
		ParseError::InvalidUnicode {
			tried: i,
		},
	))?;
	// Read the } character
	let (i, _) = char('}')(i)?;
	// Return the char
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use crate::{sql::Value, syn::Parse};

	use super::*;

	#[test]
	fn strand_empty() {
		let sql = r#""""#;
		let res = strand(sql);
		let out = res.unwrap().1;
		assert_eq!(r#"''"#, format!("{}", out));
		assert_eq!(out, Strand::from(""));
	}

	#[test]
	fn strand_single() {
		let sql = r#"'test'"#;
		let res = strand(sql);
		let out = res.unwrap().1;
		assert_eq!(r#"'test'"#, format!("{}", out));
		assert_eq!(out, Strand::from("test"));
	}

	#[test]
	fn strand_double() {
		let sql = r#""test""#;
		let res = strand(sql);
		let out = res.unwrap().1;
		assert_eq!(r#"'test'"#, format!("{}", out));
		assert_eq!(out, Strand::from("test"));
	}

	#[test]
	fn strand_quoted_single() {
		let sql = r"'te\'st'";
		let res = strand(sql);
		let out = res.unwrap().1;
		assert_eq!(r#""te'st""#, format!("{}", out));
		assert_eq!(out, Strand::from(r#"te'st"#));
	}

	#[test]
	fn strand_quoted_double() {
		let sql = r#""te\"st""#;
		let res = strand(sql);
		let out = res.unwrap().1;
		assert_eq!(r#"'te"st'"#, format!("{}", out));
		assert_eq!(out, Strand::from(r#"te"st"#));
	}

	#[test]
	fn strand_quoted_escaped() {
		let sql = r#""te\"st\n\tand\bsome\u05d9""#;
		let res = strand(sql);
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

	#[test]
	fn strand_prefix() {
		// ensure that strands which match other string like types are actually parsed as strand
		// when prefixed.

		let v = Value::parse("s'2012-04-23T18:25:43.000051100Z'");
		if let Value::Strand(x) = v {
			assert_eq!(x.as_str(), "2012-04-23T18:25:43.000051100Z");
		} else {
			panic!("not a strand");
		}

		let v = Value::parse("s'a:b'");
		if let Value::Strand(x) = v {
			assert_eq!(x.as_str(), "a:b");
		} else {
			panic!("not a strand");
		}

		let v = Value::parse("s'e72bee20-f49b-11ec-b939-0242ac120002'");
		if let Value::Strand(x) = v {
			assert_eq!(x.as_str(), "e72bee20-f49b-11ec-b939-0242ac120002");
		} else {
			panic!("not a strand");
		}
	}
}
