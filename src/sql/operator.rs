use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::IResult;
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
	ContainSome, // ⊃
	ContainNone, // ⊅
	Inside,      // ∈
	NotInside,   // ∉
	AllInside,   // ⊆
	SomeInside,  // ⊂
	NoneInside,  // ⊄
	Intersects,  // ∩
}

impl Default for Operator {
	fn default() -> Operator {
		Operator::Equal
	}
}

impl fmt::Display for Operator {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Operator::Or => write!(f, "OR"),
			Operator::And => write!(f, "AND"),
			Operator::Add => write!(f, "+"),
			Operator::Sub => write!(f, "-"),
			Operator::Mul => write!(f, "*"),
			Operator::Div => write!(f, "/"),
			Operator::Inc => write!(f, "+="),
			Operator::Dec => write!(f, "-="),
			Operator::Equal => write!(f, "="),
			Operator::Exact => write!(f, "=="),
			Operator::NotEqual => write!(f, "!="),
			Operator::AllEqual => write!(f, "*="),
			Operator::AnyEqual => write!(f, "?="),
			Operator::Like => write!(f, "~"),
			Operator::NotLike => write!(f, "!~"),
			Operator::AllLike => write!(f, "*~"),
			Operator::AnyLike => write!(f, "?~"),
			Operator::LessThan => write!(f, "<"),
			Operator::LessThanOrEqual => write!(f, "<="),
			Operator::MoreThan => write!(f, ">"),
			Operator::MoreThanOrEqual => write!(f, ">="),
			Operator::Contain => write!(f, "CONTAINS"),
			Operator::NotContain => write!(f, "∌"),
			Operator::ContainAll => write!(f, "⊇"),
			Operator::ContainSome => write!(f, "⊃"),
			Operator::ContainNone => write!(f, "⊅"),
			Operator::Inside => write!(f, "INSIDE"),
			Operator::NotInside => write!(f, "∉"),
			Operator::AllInside => write!(f, "⊆"),
			Operator::SomeInside => write!(f, "⊂"),
			Operator::NoneInside => write!(f, "⊄"),
			Operator::Intersects => write!(f, "INTERSECTS"),
		}
	}
}

pub fn assigner(i: &str) -> IResult<&str, Operator> {
	alt((
		map(tag("="), |_| Operator::Equal),
		map(tag("+="), |_| Operator::Inc),
		map(tag("-="), |_| Operator::Dec),
	))(i)
}

pub fn operator(i: &str) -> IResult<&str, Operator> {
	alt((
		alt((
			map(tag("+"), |_| Operator::Add),
			map(tag("-"), |_| Operator::Sub),
			map(tag("*"), |_| Operator::Mul),
			map(tag("×"), |_| Operator::Mul),
			map(tag("∙"), |_| Operator::Mul),
			map(tag("/"), |_| Operator::Div),
			map(tag("÷"), |_| Operator::Div),
		)),
		alt((
			map(tag("="), |_| Operator::Equal),
			map(tag("=="), |_| Operator::Exact),
			map(tag("!="), |_| Operator::NotEqual),
			map(tag("*="), |_| Operator::AllEqual),
			map(tag("?="), |_| Operator::AnyEqual),
		)),
		alt((
			map(tag("~"), |_| Operator::Like),
			map(tag("!~"), |_| Operator::NotLike),
			map(tag("*~"), |_| Operator::AllLike),
			map(tag("?~"), |_| Operator::AnyLike),
		)),
		alt((
			map(tag("<="), |_| Operator::LessThanOrEqual),
			map(tag("<"), |_| Operator::LessThan),
			map(tag(">="), |_| Operator::MoreThanOrEqual),
			map(tag(">"), |_| Operator::MoreThan),
		)),
		alt((
			map(tag("∋"), |_| Operator::Contain),
			map(tag("∌"), |_| Operator::NotContain),
			map(tag("∈"), |_| Operator::Inside),
			map(tag("∉"), |_| Operator::NotInside),
			map(tag("⊇"), |_| Operator::ContainAll),
			map(tag("⊃"), |_| Operator::ContainSome),
			map(tag("⊅"), |_| Operator::ContainNone),
			map(tag("⊆"), |_| Operator::AllInside),
			map(tag("⊂"), |_| Operator::SomeInside),
			map(tag("⊄"), |_| Operator::NoneInside),
			map(tag("<•>"), |_| Operator::Inside),
			map(tag("<|>"), |_| Operator::Intersects),
		)),
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
			map(tag_no_case("CONTAINS ALL"), |_| Operator::ContainAll),
			map(tag_no_case("CONTAINS NONE"), |_| Operator::ContainNone),
			map(tag_no_case("CONTAINS SOME"), |_| Operator::ContainSome),
			map(tag_no_case("CONTAINS NOT"), |_| Operator::NotContain),
			map(tag_no_case("CONTAINS"), |_| Operator::Contain),
			map(tag_no_case("ALL INSIDE"), |_| Operator::AllInside),
			map(tag_no_case("NONE INSIDE"), |_| Operator::NoneInside),
			map(tag_no_case("SOME INSIDE"), |_| Operator::SomeInside),
			map(tag_no_case("NOT INSIDE"), |_| Operator::NotInside),
			map(tag_no_case("INSIDE"), |_| Operator::Inside),
			map(tag_no_case("OUTSIDE"), |_| Operator::NotInside),
			map(tag_no_case("INTERSECTS"), |_| Operator::Intersects),
		)),
	))(i)
}
