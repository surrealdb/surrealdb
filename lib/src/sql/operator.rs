use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub enum Operator {
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
	//
	Equal,    // =
	Exact,    // ==
	NotEqual, // !=
	AllEqual, // *=
	AnyEqual, // ?=
	//
	Like,    // ~
	NotLike, // !~
	AllLike, // *~
	AnyLike, // ?~
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
			Operator::Or => 1,
			Operator::And => 2,
			Operator::Tco => 3,
			Operator::Nco => 4,
			Operator::Sub => 6,
			Operator::Add => 7,
			Operator::Mul => 8,
			Operator::Div => 9,
			_ => 5,
		}
	}
}

impl fmt::Display for Operator {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::Or => "OR",
			Self::And => "AND",
			Self::Tco => "?:",
			Self::Nco => "??",
			Self::Add => "+",
			Self::Sub => "-",
			Self::Mul => "*",
			Self::Div => "/",
			Self::Pow => "**",
			Self::Inc => "+=",
			Self::Dec => "-=",
			Self::Equal => "=",
			Self::Exact => "==",
			Self::NotEqual => "!=",
			Self::AllEqual => "*=",
			Self::AnyEqual => "?=",
			Self::Like => "~",
			Self::NotLike => "!~",
			Self::AllLike => "*~",
			Self::AnyLike => "?~",
			Self::LessThan => "<",
			Self::LessThanOrEqual => "<=",
			Self::MoreThan => ">",
			Self::MoreThanOrEqual => ">=",
			Self::Contain => "CONTAINS",
			Self::NotContain => "CONTAINSNOT",
			Self::ContainAll => "CONTAINSALL",
			Self::ContainAny => "CONTAINSANY",
			Self::ContainNone => "CONTAINSNONE",
			Self::Inside => "INSIDE",
			Self::NotInside => "NOTINSIDE",
			Self::AllInside => "ALLINSIDE",
			Self::AnyInside => "ANYINSIDE",
			Self::NoneInside => "NONEINSIDE",
			Self::Outside => "OUTSIDE",
			Self::Intersects => "INTERSECTS",
		})
	}
}

pub fn assigner(i: &str) -> IResult<&str, Operator> {
	alt((
		map(char('='), |_| Operator::Equal),
		map(tag("+="), |_| Operator::Inc),
		map(tag("-="), |_| Operator::Dec),
	))(i)
}

pub fn operator(i: &str) -> IResult<&str, Operator> {
	alt((symbols, phrases))(i)
}

pub fn symbols(i: &str) -> IResult<&str, Operator> {
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

pub fn phrases(i: &str) -> IResult<&str, Operator> {
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
