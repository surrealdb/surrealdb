use crate::idx::ft::MatchRef;
use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::character::complete::u32 as uint32;
use nom::character::complete::u8 as uint8;
use nom::combinator::cut;
use nom::combinator::opt;
use nom::combinator::value;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Write;

/// Binary operators.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub enum Operator {
	//
	Neg, // -
	Not, // !
	//
	Or,  // ||
	And, // &&
	Tco, // ?: Ternary conditional operator
	Nco, // ?? Null coalescing operator
	//
	Add, // +
	Sub, // -
	Mul, // *
	Div, // /
	Pow, // **
	Inc, // +=
	Dec, // -=
	Ext, // +?=
	//
	Equal,    // =
	Exact,    // ==
	NotEqual, // !=
	AllEqual, // *=
	AnyEqual, // ?=
	//
	Like,                      // ~
	NotLike,                   // !~
	AllLike,                   // *~
	AnyLike,                   // ?~
	Matches(Option<MatchRef>), // @{ref}@
	//
	LessThan,        // <
	LessThanOrEqual, // <=
	MoreThan,        // >
	MoreThanOrEqual, // >=
	//
	Contain,     // ∋
	NotContain,  // ∌
	ContainAll,  // ⊇
	ContainAny,  // ⊃
	ContainNone, // ⊅
	Inside,      // ∈
	NotInside,   // ∉
	AllInside,   // ⊆
	AnyInside,   // ⊂
	NoneInside,  // ⊄
	//
	Outside,
	Intersects,
	//
	Knn(u32), // <{k}>
}

impl Default for Operator {
	fn default() -> Self {
		Self::Equal
	}
}

impl Operator {
	#[inline]
	pub fn precedence(&self) -> u8 {
		match self {
			Self::Or => 1,
			Self::And => 2,
			Self::Tco => 3,
			Self::Nco => 4,
			Self::Sub => 6,
			Self::Add => 7,
			Self::Mul => 8,
			Self::Div => 9,
			_ => 5,
		}
	}
}

impl fmt::Display for Operator {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Neg => f.write_str("-"),
			Self::Not => f.write_str("!"),
			Self::Or => f.write_str("OR"),
			Self::And => f.write_str("AND"),
			Self::Tco => f.write_str("?:"),
			Self::Nco => f.write_str("??"),
			Self::Add => f.write_str("+"),
			Self::Sub => f.write_char('-'),
			Self::Mul => f.write_char('*'),
			Self::Div => f.write_char('/'),
			Self::Pow => f.write_str("**"),
			Self::Inc => f.write_str("+="),
			Self::Dec => f.write_str("-="),
			Self::Ext => f.write_str("+?="),
			Self::Equal => f.write_char('='),
			Self::Exact => f.write_str("=="),
			Self::NotEqual => f.write_str("!="),
			Self::AllEqual => f.write_str("*="),
			Self::AnyEqual => f.write_str("?="),
			Self::Like => f.write_char('~'),
			Self::NotLike => f.write_str("!~"),
			Self::AllLike => f.write_str("*~"),
			Self::AnyLike => f.write_str("?~"),
			Self::LessThan => f.write_char('<'),
			Self::LessThanOrEqual => f.write_str("<="),
			Self::MoreThan => f.write_char('>'),
			Self::MoreThanOrEqual => f.write_str(">="),
			Self::Contain => f.write_str("CONTAINS"),
			Self::NotContain => f.write_str("CONTAINSNOT"),
			Self::ContainAll => f.write_str("CONTAINSALL"),
			Self::ContainAny => f.write_str("CONTAINSANY"),
			Self::ContainNone => f.write_str("CONTAINSNONE"),
			Self::Inside => f.write_str("INSIDE"),
			Self::NotInside => f.write_str("NOTINSIDE"),
			Self::AllInside => f.write_str("ALLINSIDE"),
			Self::AnyInside => f.write_str("ANYINSIDE"),
			Self::NoneInside => f.write_str("NONEINSIDE"),
			Self::Outside => f.write_str("OUTSIDE"),
			Self::Intersects => f.write_str("INTERSECTS"),
			Self::Matches(reference) => {
				if let Some(r) = reference {
					write!(f, "@{}@", r)
				} else {
					f.write_str("@@")
				}
			}
			Self::Knn(k) => write!(f, "<{}>", k),
		}
	}
}

pub fn assigner(i: &str) -> IResult<&str, Operator> {
	alt((
		value(Operator::Equal, char('=')),
		value(Operator::Inc, tag("+=")),
		value(Operator::Dec, tag("-=")),
		value(Operator::Ext, tag("+?=")),
	))(i)
}

pub fn unary(i: &str) -> IResult<&str, Operator> {
	unary_symbols(i)
}

pub fn unary_symbols(i: &str) -> IResult<&str, Operator> {
	let (i, _) = mightbespace(i)?;
	let (i, v) = alt((value(Operator::Neg, tag("-")), value(Operator::Not, tag("!"))))(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

pub fn binary(i: &str) -> IResult<&str, Operator> {
	alt((binary_symbols, binary_phrases))(i)
}

pub fn binary_symbols(i: &str) -> IResult<&str, Operator> {
	let (i, _) = mightbespace(i)?;
	let (i, v) = alt((
		alt((
			value(Operator::Or, tag("||")),
			value(Operator::And, tag("&&")),
			value(Operator::Tco, tag("?:")),
			value(Operator::Nco, tag("??")),
		)),
		alt((
			value(Operator::Exact, tag("==")),
			value(Operator::NotEqual, tag("!=")),
			value(Operator::AllEqual, tag("*=")),
			value(Operator::AnyEqual, tag("?=")),
			value(Operator::Equal, char('=')),
		)),
		alt((
			value(Operator::NotLike, tag("!~")),
			value(Operator::AllLike, tag("*~")),
			value(Operator::AnyLike, tag("?~")),
			value(Operator::Like, char('~')),
			matches,
			knn,
		)),
		alt((
			value(Operator::LessThanOrEqual, tag("<=")),
			value(Operator::LessThan, char('<')),
			value(Operator::MoreThanOrEqual, tag(">=")),
			value(Operator::MoreThan, char('>')),
			knn,
		)),
		alt((
			value(Operator::Pow, tag("**")),
			value(Operator::Add, char('+')),
			value(Operator::Sub, char('-')),
			value(Operator::Mul, char('*')),
			value(Operator::Mul, char('×')),
			value(Operator::Mul, char('∙')),
			value(Operator::Div, char('/')),
			value(Operator::Div, char('÷')),
		)),
		alt((
			value(Operator::Contain, char('∋')),
			value(Operator::NotContain, char('∌')),
			value(Operator::Inside, char('∈')),
			value(Operator::NotInside, char('∉')),
			value(Operator::ContainAll, char('⊇')),
			value(Operator::ContainAny, char('⊃')),
			value(Operator::ContainNone, char('⊅')),
			value(Operator::AllInside, char('⊆')),
			value(Operator::AnyInside, char('⊂')),
			value(Operator::NoneInside, char('⊄')),
		)),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

pub fn binary_phrases(i: &str) -> IResult<&str, Operator> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = alt((
		alt((
			value(Operator::Or, tag_no_case("OR")),
			value(Operator::And, tag_no_case("AND")),
			value(Operator::NotEqual, tag_no_case("IS NOT")),
			value(Operator::Equal, tag_no_case("IS")),
		)),
		alt((
			value(Operator::ContainAll, tag_no_case("CONTAINSALL")),
			value(Operator::ContainAny, tag_no_case("CONTAINSANY")),
			value(Operator::ContainNone, tag_no_case("CONTAINSNONE")),
			value(Operator::NotContain, tag_no_case("CONTAINSNOT")),
			value(Operator::Contain, tag_no_case("CONTAINS")),
			value(Operator::AllInside, tag_no_case("ALLINSIDE")),
			value(Operator::AnyInside, tag_no_case("ANYINSIDE")),
			value(Operator::NoneInside, tag_no_case("NONEINSIDE")),
			value(Operator::NotInside, tag_no_case("NOTINSIDE")),
			value(Operator::Inside, tag_no_case("INSIDE")),
			value(Operator::Outside, tag_no_case("OUTSIDE")),
			value(Operator::Intersects, tag_no_case("INTERSECTS")),
			value(Operator::NotInside, tag_no_case("NOT IN")),
			value(Operator::Inside, tag_no_case("IN")),
		)),
	))(i)?;
	let (i, _) = shouldbespace(i)?;
	Ok((i, v))
}

pub fn matches(i: &str) -> IResult<&str, Operator> {
	let (i, _) = char('@')(i)?;
	cut(|i| {
		let (i, reference) = opt(uint8)(i)?;
		let (i, _) = char('@')(i)?;
		Ok((i, Operator::Matches(reference)))
	})(i)
}

pub fn knn(i: &str) -> IResult<&str, Operator> {
	let (i, _) = char('<')(i)?;
	let (i, k) = uint32(i)?;
	let (i, _) = char('>')(i)?;
	Ok((i, Operator::Knn(k)))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn matches_without_reference() {
		let res = matches("@@");
		let out = res.unwrap().1;
		assert_eq!("@@", format!("{}", out));
		assert_eq!(out, Operator::Matches(None));
	}

	#[test]
	fn matches_with_reference() {
		let res = matches("@12@");
		let out = res.unwrap().1;
		assert_eq!("@12@", format!("{}", out));
		assert_eq!(out, Operator::Matches(Some(12u8)));
	}

	#[test]
	fn matches_with_invalid_reference() {
		let res = matches("@256@");
		res.unwrap_err();
	}

	#[test]
	fn test_knn() {
		let res = knn("<5>");
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<5>", format!("{}", out));
		assert_eq!(out, Operator::Knn(5));
	}
}
