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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Operator {
	Or,  // ||
	And, // &&
	//
	Add, // +
	Sub, // -
	Mul, // *
	Div, // /
	Inc, // +=
	Dec, // -=
	//
	Exact, // ==
	//
	Equal,    // =
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
	Outside,     // ∈
	Intersects,  // ∩
}

impl Default for Operator {
	fn default() -> Operator {
		Operator::Equal
	}
}

impl Operator {
	#[inline]
	pub fn precedence(&self) -> u8 {
		match self {
			Operator::Or => 1,
			Operator::And => 2,
			Operator::Sub => 4,
			Operator::Add => 5,
			Operator::Mul => 6,
			Operator::Div => 7,
			_ => 3,
		}
	}
}

impl fmt::Display for Operator {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Operator::Or => "OR",
			Operator::And => "AND",
			Operator::Add => "+",
			Operator::Sub => "-",
			Operator::Mul => "*",
			Operator::Div => "/",
			Operator::Inc => "+=",
			Operator::Dec => "-=",
			Operator::Exact => "==",
			Operator::Equal => "=",
			Operator::NotEqual => "!=",
			Operator::AllEqual => "*=",
			Operator::AnyEqual => "?=",
			Operator::Like => "~",
			Operator::NotLike => "!~",
			Operator::AllLike => "*~",
			Operator::AnyLike => "?~",
			Operator::LessThan => "<",
			Operator::LessThanOrEqual => "<=",
			Operator::MoreThan => ">",
			Operator::MoreThanOrEqual => ">=",
			Operator::Contain => "CONTAINS",
			Operator::NotContain => "CONTAINSNOT",
			Operator::ContainAll => "CONTAINSALL",
			Operator::ContainAny => "CONTAINSANY",
			Operator::ContainNone => "CONTAINSNONE",
			Operator::Inside => "INSIDE",
			Operator::NotInside => "NOTINSIDE",
			Operator::AllInside => "ALLINSIDE",
			Operator::AnyInside => "ANYINSIDE",
			Operator::NoneInside => "NONEINSIDE",
			Operator::Outside => "OUTSIDE",
			Operator::Intersects => "INTERSECTS",
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
			map(tag_no_case("&&"), |_| Operator::And),
			map(tag_no_case("AND"), |_| Operator::And),
			map(tag_no_case("||"), |_| Operator::Or),
			map(tag_no_case("OR"), |_| Operator::Or),
		)),
		alt((
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
		)),
	))(i)?;
	let (i, _) = shouldbespace(i)?;
	Ok((i, v))
}
