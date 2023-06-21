use crate::idx::ft::MatchRef;
use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::character::complete::u8 as uint8;
use nom::combinator::{map, opt};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Write;

/// Binary operators.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
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
		}
	}
}

pub fn assigner(i: &str) -> IResult<&str, Operator> {
	alt((
		map(char('='), |_| Operator::Equal),
		map(tag("+="), |_| Operator::Inc),
		map(tag("-="), |_| Operator::Dec),
		map(tag("+?="), |_| Operator::Ext),
	))(i)
}

pub fn unary(i: &str) -> IResult<&str, Operator> {
	unary_symbols(i)
}

pub fn unary_symbols(i: &str) -> IResult<&str, Operator> {
	let (i, _) = mightbespace(i)?;
	let (i, v) =
		alt((alt((map(tag("-"), |_| Operator::Neg), map(tag("!"), |_| Operator::Not))),))(i)?;
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
			map(tag("||"), |_| Operator::Or),
			map(tag("&&"), |_| Operator::And),
			map(tag("?:"), |_| Operator::Tco),
			map(tag("??"), |_| Operator::Nco),
		)),
		alt((
			map(tag("=="), |_| Operator::Exact),
			map(tag("!="), |_| Operator::NotEqual),
			map(tag("*="), |_| Operator::AllEqual),
			map(tag("?="), |_| Operator::AnyEqual),
			map(char('='), |_| Operator::Equal),
		)),
		alt((
			map(tag("!~"), |_| Operator::NotLike),
			map(tag("*~"), |_| Operator::AllLike),
			map(tag("?~"), |_| Operator::AnyLike),
			map(char('~'), |_| Operator::Like),
			matches,
		)),
		alt((
			map(tag("<="), |_| Operator::LessThanOrEqual),
			map(char('<'), |_| Operator::LessThan),
			map(tag(">="), |_| Operator::MoreThanOrEqual),
			map(char('>'), |_| Operator::MoreThan),
		)),
		alt((
			map(tag("**"), |_| Operator::Pow),
			map(char('+'), |_| Operator::Add),
			map(char('-'), |_| Operator::Sub),
			map(char('*'), |_| Operator::Mul),
			map(char('×'), |_| Operator::Mul),
			map(char('∙'), |_| Operator::Mul),
			map(char('/'), |_| Operator::Div),
			map(char('÷'), |_| Operator::Div),
		)),
		alt((
			map(char('∋'), |_| Operator::Contain),
			map(char('∌'), |_| Operator::NotContain),
			map(char('∈'), |_| Operator::Inside),
			map(char('∉'), |_| Operator::NotInside),
			map(char('⊇'), |_| Operator::ContainAll),
			map(char('⊃'), |_| Operator::ContainAny),
			map(char('⊅'), |_| Operator::ContainNone),
			map(char('⊆'), |_| Operator::AllInside),
			map(char('⊂'), |_| Operator::AnyInside),
			map(char('⊄'), |_| Operator::NoneInside),
		)),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

pub fn binary_phrases(i: &str) -> IResult<&str, Operator> {
	let (i, _) = shouldbespace(i)?;
	let (i, v) = alt((
		alt((
			map(tag_no_case("OR"), |_| Operator::Or),
			map(tag_no_case("AND"), |_| Operator::And),
			map(tag_no_case("IS NOT"), |_| Operator::NotEqual),
			map(tag_no_case("IS"), |_| Operator::Equal),
		)),
		alt((
			map(tag_no_case("CONTAINSALL"), |_| Operator::ContainAll),
			map(tag_no_case("CONTAINSANY"), |_| Operator::ContainAny),
			map(tag_no_case("CONTAINSNONE"), |_| Operator::ContainNone),
			map(tag_no_case("CONTAINSNOT"), |_| Operator::NotContain),
			map(tag_no_case("CONTAINS"), |_| Operator::Contain),
			map(tag_no_case("ALLINSIDE"), |_| Operator::AllInside),
			map(tag_no_case("ANYINSIDE"), |_| Operator::AnyInside),
			map(tag_no_case("NONEINSIDE"), |_| Operator::NoneInside),
			map(tag_no_case("NOTINSIDE"), |_| Operator::NotInside),
			map(tag_no_case("INSIDE"), |_| Operator::Inside),
			map(tag_no_case("OUTSIDE"), |_| Operator::Outside),
			map(tag_no_case("INTERSECTS"), |_| Operator::Intersects),
			map(tag_no_case("NOT IN"), |_| Operator::NotInside),
			map(tag_no_case("IN"), |_| Operator::Inside),
		)),
	))(i)?;
	let (i, _) = shouldbespace(i)?;
	Ok((i, v))
}

pub fn matches(i: &str) -> IResult<&str, Operator> {
	let (i, _) = char('@')(i)?;
	// let (i, reference) = opt(|i| uint8(i))(i)?;
	let (i, reference) = opt(uint8)(i)?;
	let (i, _) = char('@')(i)?;
	Ok((i, Operator::Matches(reference)))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn matches_without_reference() {
		let res = matches("@@");
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("@@", format!("{}", out));
		assert_eq!(out, Operator::Matches(None));
	}

	#[test]
	fn matches_with_reference() {
		let res = matches("@12@");
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("@12@", format!("{}", out));
		assert_eq!(out, Operator::Matches(Some(12u8)));
	}

	#[test]
	fn matches_with_invalid_reference() {
		let res = matches("@256@");
		assert!(res.is_err());
	}
}
