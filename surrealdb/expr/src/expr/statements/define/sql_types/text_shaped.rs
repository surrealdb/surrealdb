//! The PostgreSQL types whose values are text: `inet`, `cidr`, `macaddr`, `macaddr8`, `bit`,
//! `bit varying`, `varbit`, `xml`, `tsvector`, `tsquery`, `jsonpath`, and `tid`.
//!
//! Each is stored as a string. This family checks the text against its type and writes it in
//! PostgreSQL's canonical form: `inet` drops a prefix that covers the whole address, `macaddr`
//! and `macaddr8` write lower-case colon-separated octets, `tid` writes `(block,offset)`, and
//! `tsvector` writes its sorted lexemes. `xml`, `tsquery`, and `jsonpath` are checked and kept as
//! written. A MySQL declaration, or a PostgreSQL one this family does not own, is declined.

pub mod network;

mod jsonpath;
mod text_search;
mod xml;

use super::Validated;
use crate::expr::statements::define::SqlTypeDialect;
use crate::expr::statements::define::declared_type::{DeclaredSqlType, TypeViolation};
use crate::val::{Array, Value};

/// SQLSTATE `22P02`, invalid text representation.
const INVALID_TEXT: &str = "22P02";
/// SQLSTATE `22026`, string data length mismatch.
const BIT_LENGTH_MISMATCH: &str = "22026";
/// SQLSTATE `22001`, string data right truncation.
const STRING_TRUNCATION: &str = "22001";
/// SQLSTATE `42601`, syntax error.
const SYNTAX_ERROR: &str = "42601";
/// SQLSTATE `2200N`, invalid XML content.
const INVALID_XML: &str = "2200N";

/// The family's `validate`: the value to store for a text-shaped PostgreSQL type, or the
/// violation that refuses it. Arrays are checked element by element, and a value that is not a
/// string passes unchanged.
pub fn validate(declared: &DeclaredSqlType, value: Value, _column: &str) -> Validated {
	let Some(shape) = Shape::of(declared) else {
		return Validated::Declined(value);
	};
	let bit_length = declared.modifiers().first().and_then(|m| m.parse::<u64>().ok());
	Validated::Decided(apply(shape, bit_length, declared, value))
}

/// The text-shaped PostgreSQL types the family owns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
	Inet,
	Cidr,
	MacAddr,
	MacAddr8,
	Bit {
		varying: bool,
	},
	Xml,
	TsVector,
	TsQuery,
	JsonPath,
	Tid,
}

impl Shape {
	fn of(declared: &DeclaredSqlType) -> Option<Self> {
		if declared.dialect() != SqlTypeDialect::PostgreSql {
			return None;
		}
		Some(match declared.base() {
			"inet" => Shape::Inet,
			"cidr" => Shape::Cidr,
			"macaddr" => Shape::MacAddr,
			"macaddr8" => Shape::MacAddr8,
			"bit" => Shape::Bit {
				varying: false,
			},
			"bit varying" | "varbit" => Shape::Bit {
				varying: true,
			},
			"xml" => Shape::Xml,
			"tsvector" => Shape::TsVector,
			"tsquery" => Shape::TsQuery,
			"jsonpath" => Shape::JsonPath,
			"tid" => Shape::Tid,
			_ => return None,
		})
	}
}

fn apply(
	shape: Shape,
	bit_length: Option<u64>,
	declared: &DeclaredSqlType,
	value: Value,
) -> Result<Value, TypeViolation> {
	if let Value::String(text) = &value {
		let canonical = canonical(shape, bit_length, declared, text.as_str())?;
		return Ok(Value::from(canonical));
	}
	let Value::Array(array) = value else {
		// A value that is no text is left to the field's kind.
		return Ok(value);
	};
	let items = array
		.0
		.into_iter()
		.map(|item| apply(shape, bit_length, declared, item))
		.collect::<Result<Vec<Value>, TypeViolation>>()?;
	Ok(Value::Array(Array(items)))
}

/// The canonical text of `text` as a value of `shape`, or the violation it breaks.
fn canonical(
	shape: Shape,
	bit_length: Option<u64>,
	declared: &DeclaredSqlType,
	text: &str,
) -> Result<String, TypeViolation> {
	match shape {
		Shape::Inet => network::parse(text)
			.map(|net| net.inet_text())
			.map_err(|err| invalid_input(declared, text, &err.reason)),
		Shape::Cidr => network::parse_cidr(text)
			.map(|net| net.cidr_text())
			.map_err(|err| invalid_input(declared, text, &err.reason)),
		Shape::MacAddr => parse_mac(text, 6)
			.map(|octets| format_mac(&octets))
			.ok_or_else(|| invalid_input(declared, text, "")),
		Shape::MacAddr8 => parse_mac8(text)
			.map(|octets| format_mac(&octets))
			.ok_or_else(|| invalid_input(declared, text, "")),
		Shape::Bit {
			varying,
		} => check_bits(varying, bit_length, text).map(|()| text.to_string()),
		Shape::Xml => {
			xml::check_content(text).map(|()| text.to_string()).map_err(|err| TypeViolation {
				code: INVALID_XML,
				message: format!("invalid XML content: {}", err.reason),
			})
		}
		Shape::TsVector => {
			text_search::canonical_tsvector(text).map_err(|err| syntax_violation(err.reason))
		}
		Shape::TsQuery => text_search::check_tsquery(text)
			.map(|()| text.trim().to_string())
			.map_err(|err| syntax_violation(err.reason)),
		Shape::JsonPath => jsonpath::check(text)
			.map(|()| text.trim().to_string())
			.map_err(|err| syntax_violation(err.reason)),
		Shape::Tid => parse_tid(text)
			.map(|(block, offset)| format!("({block},{offset})"))
			.ok_or_else(|| invalid_input(declared, text, "")),
	}
}

/// A text that does not read as the declared type. `detail` is added after the type when it is
/// not empty.
fn invalid_input(declared: &DeclaredSqlType, text: &str, detail: &str) -> TypeViolation {
	let base = declared.base();
	let message = if detail.is_empty() {
		format!("invalid input syntax for type {base}: \"{text}\"")
	} else {
		format!("invalid input syntax for type {base}: \"{text}\" ({detail})")
	};
	TypeViolation {
		code: INVALID_TEXT,
		message,
	}
}

/// A text that breaks the syntax of its type.
fn syntax_violation(reason: String) -> TypeViolation {
	TypeViolation {
		code: SYNTAX_ERROR,
		message: reason,
	}
}

/// The bits of a `bit` or `bit varying` text, checked against the declared length. A `bit(n)`
/// takes exactly `n` bits, and a `bit` with no length is `bit(1)`; a `bit varying(n)` takes at
/// most `n`, and one with no length is unbounded. The length is judged before the digits, as
/// PostgreSQL's `bit_in` and `varbit_in` judge it, so a text of the wrong length is refused for
/// its length even when it also holds a digit that is not binary.
fn check_bits(varying: bool, length: Option<u64>, text: &str) -> Result<(), TypeViolation> {
	// The digits are read before the length is judged, as PostgreSQL's `bit_in` does.
	if let Some(bad) = text.chars().find(|c| *c != '0' && *c != '1') {
		return Err(TypeViolation {
			code: INVALID_TEXT,
			message: format!("\"{bad}\" is not a valid binary digit"),
		});
	}
	let written = u64::try_from(text.len()).unwrap_or(u64::MAX);
	if !varying {
		let limit = length.unwrap_or(1);
		if written != limit {
			return Err(TypeViolation {
				code: BIT_LENGTH_MISMATCH,
				message: format!("bit string length {written} does not match type bit({limit})"),
			});
		}
	} else if let Some(limit) = length
		&& written > limit
	{
		return Err(TypeViolation {
			code: STRING_TRUNCATION,
			message: format!("bit string too long for type bit varying({limit})"),
		});
	}
	Ok(())
}

/// The octets of a MAC address of `octets` bytes.
///
/// Six octets (`macaddr`) are read from one of PostgreSQL's forms: pairs separated by `:` or `-`
/// (`08:00:2b:01:02:03`), quads separated by `.` or `-` (`0800.2b01.0203`), sextets separated by
/// `:` or `-` (`08002b:010203`), or no separator (`08002b010203`). Any other count (`macaddr8`) is
/// read as PostgreSQL reads a `macaddr8`: hex digit pairs with a `:`, `-` or `.` allowed between
/// any two octets (`08002b.0102.0304.0506`). Every separator in a text is the same character.
pub fn parse_mac(text: &str, octets: usize) -> Option<Vec<u8>> {
	let text = text.trim();
	if octets != 6 {
		return parse_mac_octets(text, octets);
	}
	let digits = octets * 2;
	let separators: Vec<char> = text.chars().filter(|c| matches!(c, ':' | '-' | '.')).collect();
	let all_digits: String = if separators.is_empty() {
		text.to_string()
	} else {
		let separator = separators[0];
		if separators.iter().any(|c| *c != separator) {
			return None;
		}
		let groups: Vec<&str> = text.split(separator).collect();
		let group_len = groups[0].len();
		let separator_allowed = match group_len {
			2 => matches!(separator, ':' | '-'),
			4 => matches!(separator, '.' | '-'),
			6 => matches!(separator, ':' | '-'),
			_ => false,
		};
		if !separator_allowed || groups.iter().any(|group| group.len() != group_len) {
			return None;
		}
		groups.concat()
	};
	if all_digits.len() != digits || !all_digits.bytes().all(|b| b.is_ascii_hexdigit()) {
		return None;
	}
	(0..octets)
		.map(|index| u8::from_str_radix(&all_digits[index * 2..index * 2 + 2], 16).ok())
		.collect()
}

/// The eight octets of a `macaddr8` text, read as PostgreSQL's `macaddr8_in` reads it: hex digit
/// pairs, each optionally followed by one separator (`:`, `-` or `.`), the same separator
/// throughout, with whitespace allowed before the first pair and after the last. Six pairs are a
/// six-octet address, expanded with `ff:fe` between the vendor and device parts; any count other
/// than six or eight is refused.
fn parse_mac8(text: &str) -> Option<Vec<u8>> {
	let bytes = text.trim().as_bytes();
	let mut octets = Vec::with_capacity(8);
	let mut separator = None;
	let mut at = 0;
	while at < bytes.len() {
		let pair = bytes.get(at..at + 2)?;
		let octet =
			std::str::from_utf8(pair).ok().filter(|p| p.bytes().all(|b| b.is_ascii_hexdigit()))?;
		if octets.len() == 8 {
			return None;
		}
		octets.push(u8::from_str_radix(octet, 16).ok()?);
		at += 2;
		if let Some(&next) = bytes.get(at)
			&& matches!(next, b':' | b'-' | b'.')
		{
			if *separator.get_or_insert(next) != next {
				return None;
			}
			at += 1;
			// A separator stands between two pairs, never at the end.
			if at == bytes.len() {
				return None;
			}
		}
	}
	match octets.len() {
		8 => Some(octets),
		6 => {
			Some(vec![octets[0], octets[1], octets[2], 0xff, 0xfe, octets[3], octets[4], octets[5]])
		}
		_ => None,
	}
}

/// `octets` hex digit pairs with one separator character allowed between any two of them.
fn parse_mac_octets(text: &str, octets: usize) -> Option<Vec<u8>> {
	let mut separator = None;
	let mut read = Vec::with_capacity(octets);
	let mut rest = text;
	while !rest.is_empty() {
		if !read.is_empty()
			&& let Some(c) = rest.chars().next().filter(|c| matches!(c, ':' | '-' | '.'))
		{
			if *separator.get_or_insert(c) != c {
				return None;
			}
			rest = &rest[1..];
		}
		let pair = rest.get(..2)?;
		if !pair.bytes().all(|b| b.is_ascii_hexdigit()) {
			return None;
		}
		read.push(u8::from_str_radix(pair, 16).ok()?);
		rest = &rest[2..];
	}
	(read.len() == octets).then_some(read)
}

/// The canonical text of a MAC address: lower-case octets separated by colons.
pub fn format_mac(octets: &[u8]) -> String {
	octets.iter().map(|octet| format!("{octet:02x}")).collect::<Vec<_>>().join(":")
}

/// The block and offset of a `tid`, written `(block,offset)` with optional spaces.
fn parse_tid(text: &str) -> Option<(u32, u16)> {
	let inner = text.trim().strip_prefix('(')?.strip_suffix(')')?;
	let (block, offset) = inner.split_once(',')?;
	let (block, offset) = (block.trim(), offset.trim());
	let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
	if !digits(block) || !digits(offset) {
		return None;
	}
	Some((block.parse::<u32>().ok()?, offset.parse::<u16>().ok()?))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn pg(text: &str) -> DeclaredSqlType {
		DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, text).expect("a PostgreSQL type")
	}

	/// The stored text of `value` written to a column declared `declared`.
	fn stored(declared: &str, value: &str) -> Result<String, TypeViolation> {
		match validate(&pg(declared), Value::from(value), "c") {
			Validated::Decided(Ok(Value::String(text))) => Ok(text.into_string()),
			Validated::Decided(Err(violation)) => Err(violation),
			Validated::Decided(Ok(other)) => panic!("a string was expected, got {other:?}"),
			Validated::Declined(_) => panic!("{declared} should be owned by the family"),
		}
	}

	fn accepts(declared: &str, value: &str, canonical: &str) {
		assert_eq!(stored(declared, value).expect("accepted"), canonical, "{declared} {value:?}");
	}

	fn refuses(declared: &str, value: &str, code: &str) {
		let violation = stored(declared, value).expect_err("refused");
		assert_eq!(violation.code, code, "{declared} {value:?}: {}", violation.message);
	}

	#[test]
	fn inet_and_cidr_are_canonicalised_through_the_family() {
		accepts("inet", "192.168.0.1/32", "192.168.0.1");
		accepts("inet", "192.168.0.1/24", "192.168.0.1/24");
		accepts("inet", "2001:0db8::0001", "2001:db8::1");
		refuses("inet", "192.168.0", INVALID_TEXT);
		accepts("cidr", "10.1.2.0/24", "10.1.2.0/24");
		accepts("cidr", "10.1.2.3", "10.1.2.3/32");
		refuses("cidr", "10.1.2.3/24", INVALID_TEXT);
	}

	#[test]
	fn an_array_of_addresses_is_checked_element_by_element() {
		let declared = pg("inet[]");
		let value = Value::Array(Array(vec![Value::from("10.0.0.1/32"), Value::Null]));
		match validate(&declared, value, "c") {
			Validated::Decided(Ok(Value::Array(Array(items)))) => {
				assert_eq!(items[0], Value::from("10.0.0.1"));
				assert_eq!(items[1], Value::Null);
			}
			other @ (Validated::Decided(_) | Validated::Declined(_)) => {
				panic!("unexpected {other:?}")
			}
		}
		let bad = Value::Array(Array(vec![Value::from("nope")]));
		assert!(matches!(validate(&declared, bad, "c"), Validated::Decided(Err(_))));
	}

	#[test]
	fn macaddr_accepts_every_postgresql_form_and_writes_lower_case_colons() {
		for text in [
			"08:00:2b:01:02:03",
			"08-00-2b-01-02-03",
			"08002b:010203",
			"08002b-010203",
			"0800.2b01.0203",
			"0800-2b01-0203",
			"08002b010203",
			"08:00:2B:01:02:03",
			"  08:00:2b:01:02:03  ",
		] {
			accepts("macaddr", text, "08:00:2b:01:02:03");
		}
		accepts("macaddr", "FF:FF:FF:FF:FF:FF", "ff:ff:ff:ff:ff:ff");
	}

	#[test]
	fn macaddr_refuses_wrong_lengths_separators_and_digits() {
		for text in [
			"08:00:2b:01:02",
			"08:00:2b:01:02:03:04",
			"08:00-2b:01:02:03",
			"08.00.2b.01.02.03",
			"0800:2b01:0203",
			"08:00:2b:01:02:0g",
			"08002b01020",
			"",
			"08:00:2b:01:02:",
		] {
			refuses("macaddr", text, INVALID_TEXT);
		}
	}

	#[test]
	fn macaddr8_takes_eight_octets_or_expands_six_with_ff_fe() {
		accepts("macaddr8", "08:00:2b:01:02:03:04:05", "08:00:2b:01:02:03:04:05");
		accepts("macaddr8", "08-00-2b-01-02-03-04-05", "08:00:2b:01:02:03:04:05");
		accepts("macaddr8", "08002b:0102030405", "08:00:2b:01:02:03:04:05");
		accepts("macaddr8", "0800.2b01.0203.0405", "08:00:2b:01:02:03:04:05");
		accepts("macaddr8", "08002b01:02030405", "08:00:2b:01:02:03:04:05");
		accepts("macaddr8", "08002b0102030405", "08:00:2b:01:02:03:04:05");
		// Nine octets, in groups no PostgreSQL form takes.
		refuses("macaddr8", "08002b.0102.0304.0506", INVALID_TEXT);
		refuses("macaddr8", "08:00-2b:01:02:03:04:05", INVALID_TEXT);
		accepts("macaddr8", "08-00-2b-01-02-03", "08:00:2b:ff:fe:01:02:03");
		accepts("macaddr8", "08002b010203", "08:00:2b:ff:fe:01:02:03");
		refuses("macaddr8", "08:00:2b:01:02:03:04", INVALID_TEXT);
		refuses("macaddr8", "08:00:2b:01:02:03:04:05:06", INVALID_TEXT);
		refuses("macaddr8", "08:00-2b:01:02:03:04:05", INVALID_TEXT);
		refuses("macaddr8", ":08:00:2b:01:02:03:04:05", INVALID_TEXT);
		refuses("macaddr8", "08:00:2b:01:02:03:04:05:", INVALID_TEXT);
		refuses("macaddr8", "08:0", INVALID_TEXT);
	}

	#[test]
	fn bit_checks_its_digits_and_its_exact_length() {
		accepts("bit(3)", "101", "101");
		accepts("bit", "1", "1");
		refuses("bit", "10", BIT_LENGTH_MISMATCH);
		refuses("bit(3)", "10", BIT_LENGTH_MISMATCH);
		// The digits are checked before the length, as PostgreSQL does.
		refuses("bit(3)", "1012", INVALID_TEXT);
		refuses("bit(3)", "1011", BIT_LENGTH_MISMATCH);
		refuses("bit(3)", "1a1", INVALID_TEXT);
		refuses("bit(3)", "1 1", INVALID_TEXT);
	}

	#[test]
	fn varbit_checks_its_digits_and_its_maximum_length() {
		accepts("varbit(3)", "10", "10");
		accepts("varbit(3)", "", "");
		accepts("varbit", "1010101010", "1010101010");
		accepts("bit varying(4)", "1111", "1111");
		refuses("varbit(3)", "1010", STRING_TRUNCATION);
		refuses("bit varying(3)", "2", INVALID_TEXT);
	}

	#[test]
	fn the_violation_messages_follow_postgresql() {
		let err = stored("bit(3)", "10").unwrap_err();
		assert_eq!(err.message, "bit string length 2 does not match type bit(3)");
		let err = stored("varbit(2)", "101").unwrap_err();
		assert_eq!(err.message, "bit string too long for type bit varying(2)");
		let err = stored("inet", "nope").unwrap_err();
		assert!(err.message.starts_with("invalid input syntax for type inet: \"nope\""), "{err:?}");
	}

	#[test]
	fn xml_is_checked_for_content_and_kept_as_written() {
		accepts("xml", "<a>  x  </a>", "<a>  x  </a>");
		accepts("xml", "text <b/>", "text <b/>");
		refuses("xml", "<a>", INVALID_XML);
		let err = stored("xml", "<a>").unwrap_err();
		assert!(err.message.starts_with("invalid XML content: "), "{err:?}");
	}

	#[test]
	fn tsvector_is_written_in_canonical_form() {
		accepts("tsvector", "fat:4 cat:3 fat:2", "'cat':3 'fat':2,4");
		refuses("tsvector", "a:0", SYNTAX_ERROR);
	}

	#[test]
	fn tsquery_and_jsonpath_are_checked_and_kept_trimmed() {
		accepts("tsquery", "  fat & rat  ", "fat & rat");
		refuses("tsquery", "fat &", SYNTAX_ERROR);
		accepts("jsonpath", " lax $.a[*] ", "lax $.a[*]");
		refuses("jsonpath", "$.", SYNTAX_ERROR);
	}

	#[test]
	fn tid_is_written_as_block_and_offset() {
		accepts("tid", "(0,1)", "(0,1)");
		accepts("tid", " ( 42 , 7 ) ", "(42,7)");
		accepts("tid", "(4294967295,65535)", "(4294967295,65535)");
		refuses("tid", "(4294967296,1)", INVALID_TEXT);
		refuses("tid", "(0,65536)", INVALID_TEXT);
		refuses("tid", "(1,-1)", INVALID_TEXT);
		refuses("tid", "1,2", INVALID_TEXT);
		refuses("tid", "(1)", INVALID_TEXT);
	}

	#[test]
	fn a_type_outside_the_family_is_declined_and_a_mysql_type_is_too() {
		for declared in ["text", "int", "varchar(10)", "decimal(10,2)"] {
			assert!(matches!(
				validate(&pg(declared), Value::from("x"), "c"),
				Validated::Declined(_)
			));
		}
		let mysql_bit = DeclaredSqlType::parse(SqlTypeDialect::MySql, "bit(8)").expect("mysql");
		assert!(matches!(validate(&mysql_bit, Value::from("1"), "c"), Validated::Declined(_)));
	}

	#[test]
	fn a_value_that_is_not_a_string_passes_unchanged() {
		match validate(&pg("inet"), Value::Null, "c") {
			Validated::Decided(Ok(Value::Null)) => {}
			other @ (Validated::Decided(_) | Validated::Declined(_)) => {
				panic!("unexpected {other:?}")
			}
		}
		match validate(&pg("inet"), Value::from(7), "c") {
			Validated::Decided(Ok(value)) => assert_eq!(value, Value::from(7)),
			other @ (Validated::Decided(_) | Validated::Declined(_)) => {
				panic!("unexpected {other:?}")
			}
		}
	}
}
