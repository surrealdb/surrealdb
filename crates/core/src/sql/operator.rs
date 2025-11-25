use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::Fmt;
use crate::sql::index::Distance;
use crate::sql::{Expr, Kind};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum PrefixOperator {
	/// `!`
	Not,
	/// `+`
	Positive,
	/// `-`
	Negate,
	/// `..`
	Range,
	/// `..=`
	RangeInclusive,
	Cast(Kind),
}

impl From<PrefixOperator> for crate::expr::PrefixOperator {
	fn from(value: PrefixOperator) -> Self {
		match value {
			PrefixOperator::Not => crate::expr::PrefixOperator::Not,
			PrefixOperator::Positive => crate::expr::PrefixOperator::Positive,
			PrefixOperator::Negate => crate::expr::PrefixOperator::Negate,
			PrefixOperator::Range => crate::expr::PrefixOperator::Range,
			PrefixOperator::RangeInclusive => crate::expr::PrefixOperator::RangeInclusive,
			PrefixOperator::Cast(k) => crate::expr::PrefixOperator::Cast(k.into()),
		}
	}
}

impl From<crate::expr::PrefixOperator> for PrefixOperator {
	fn from(value: crate::expr::PrefixOperator) -> Self {
		match value {
			crate::expr::PrefixOperator::Not => PrefixOperator::Not,
			crate::expr::PrefixOperator::Positive => PrefixOperator::Positive,
			crate::expr::PrefixOperator::Negate => PrefixOperator::Negate,
			crate::expr::PrefixOperator::Range => PrefixOperator::Range,
			crate::expr::PrefixOperator::RangeInclusive => PrefixOperator::RangeInclusive,
			crate::expr::PrefixOperator::Cast(k) => PrefixOperator::Cast(k.into()),
		}
	}
}

impl ToSql for PrefixOperator {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Not => write_sql!(f, fmt, "!"),
			Self::Positive => write_sql!(f, fmt, "+"),
			Self::Negate => write_sql!(f, fmt, "-"),
			Self::Range => write_sql!(f, fmt, ".."),
			Self::RangeInclusive => write_sql!(f, fmt, "..="),
			Self::Cast(kind) => write_sql!(f, fmt, "<{kind}> "),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum PostfixOperator {
	Range,
	RangeSkip,
	MethodCall(String, Vec<Expr>),
	Call(Vec<Expr>),
}

impl From<PostfixOperator> for crate::expr::PostfixOperator {
	fn from(value: PostfixOperator) -> Self {
		match value {
			PostfixOperator::Range => crate::expr::PostfixOperator::Range,
			PostfixOperator::RangeSkip => crate::expr::PostfixOperator::RangeSkip,
			PostfixOperator::MethodCall(name, x) => crate::expr::PostfixOperator::MethodCall(
				name,
				x.into_iter().map(From::from).collect(),
			),
			PostfixOperator::Call(x) => {
				crate::expr::PostfixOperator::Call(x.into_iter().map(From::from).collect())
			}
		}
	}
}

impl From<crate::expr::PostfixOperator> for PostfixOperator {
	fn from(value: crate::expr::PostfixOperator) -> Self {
		match value {
			crate::expr::PostfixOperator::Range => PostfixOperator::Range,
			crate::expr::PostfixOperator::RangeSkip => PostfixOperator::RangeSkip,
			crate::expr::PostfixOperator::MethodCall(name, args) => {
				PostfixOperator::MethodCall(name, args.into_iter().map(From::from).collect())
			}
			crate::expr::PostfixOperator::Call(args) => {
				PostfixOperator::Call(args.into_iter().map(From::from).collect())
			}
		}
	}
}

impl ToSql for PostfixOperator {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::Range => write_sql!(f, sql_fmt, ".."),
			Self::RangeSkip => write_sql!(f, sql_fmt, ">.."),
			Self::MethodCall(name, x) => {
				write_sql!(f, sql_fmt, "{name}({})", Fmt::comma_separated(x))
			}
			Self::Call(args) => write_sql!(f, sql_fmt, "({})", Fmt::comma_separated(args.iter())),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum BinaryOperator {
	/// `-`
	Subtract,
	/// `+`
	Add,
	/// `*`, `×`
	Multiply,
	/// `/`
	Divide,
	/// `%`
	Remainder,
	/// `**`
	Power,
	/// `=`
	Equal,
	/// `==`
	ExactEqual,
	/// `!=`
	NotEqual,
	/// `*=`
	AllEqual,
	/// `?=`
	AnyEqual,

	/// `||`, `OR`
	Or,
	/// `&&`, `AND`
	And,
	/// `??`
	NullCoalescing,
	// Might be usefull to remove.
	TenaryCondition,

	/// `<`
	LessThan,
	/// `<=`
	LessThanEqual,
	/// `>`
	MoreThan,
	/// `>=`
	MoreThanEqual,

	/// `∋`
	Contain,
	/// `∌`
	NotContain,
	/// `⊇`
	ContainAll,
	/// `⊃`
	ContainAny,
	/// `⊅`
	ContainNone,
	/// `∈`
	Inside,
	/// `∉`
	NotInside,
	/// `⊆`
	AllInside,
	/// `⊂`
	AnyInside,
	/// `⊄`
	NoneInside,

	/// `OUTSIDE`
	Outside,
	/// `INTERSECTS`
	Intersects,

	/// `..`
	Range,
	/// `..=`
	RangeInclusive,
	/// `>..`
	RangeSkip,
	/// `>..=`
	RangeSkipInclusive,

	// `@@`
	Matches(MatchesOperator),
	// `<|k,..|>`
	NearestNeighbor(Box<NearestNeighbor>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct MatchesOperator {
	pub rf: Option<u8>,
	pub operator: Option<BooleanOperator>,
}

impl ToSql for MatchesOperator {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		if let Some(r) = self.rf {
			if let Some(ref o) = self.operator {
				write_sql!(f, sql_fmt, "@{r},{o}@")
			} else {
				write_sql!(f, sql_fmt, "@{r}@")
			}
		} else if let Some(ref o) = self.operator {
			write_sql!(f, sql_fmt, "@{o}@")
		} else {
			write_sql!(f, sql_fmt, "@@")
		}
	}
}

impl From<MatchesOperator> for crate::expr::operator::MatchesOperator {
	fn from(value: MatchesOperator) -> Self {
		crate::expr::operator::MatchesOperator {
			rf: value.rf,
			operator: value
				.operator
				.map(From::from)
				.unwrap_or(crate::expr::operator::BooleanOperator::And),
		}
	}
}

impl From<crate::expr::operator::MatchesOperator> for MatchesOperator {
	fn from(value: crate::expr::operator::MatchesOperator) -> Self {
		MatchesOperator {
			rf: value.rf,
			operator: Some(value.operator.into()),
		}
	}
}

/// Boolean operation executed by the full-text index
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum BooleanOperator {
	And,
	Or,
}

impl From<BooleanOperator> for crate::expr::operator::BooleanOperator {
	fn from(value: BooleanOperator) -> Self {
		match value {
			BooleanOperator::And => crate::expr::operator::BooleanOperator::And,
			BooleanOperator::Or => crate::expr::operator::BooleanOperator::Or,
		}
	}
}

impl From<crate::expr::operator::BooleanOperator> for BooleanOperator {
	fn from(value: crate::expr::operator::BooleanOperator) -> Self {
		match value {
			crate::expr::operator::BooleanOperator::And => BooleanOperator::And,
			crate::expr::operator::BooleanOperator::Or => BooleanOperator::Or,
		}
	}
}

impl ToSql for BooleanOperator {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::And => write_sql!(f, sql_fmt, "AND"),
			Self::Or => write_sql!(f, sql_fmt, "OR"),
		}
	}
}

impl From<BinaryOperator> for crate::expr::BinaryOperator {
	fn from(value: BinaryOperator) -> Self {
		match value {
			BinaryOperator::Subtract => crate::expr::BinaryOperator::Subtract,
			BinaryOperator::Add => crate::expr::BinaryOperator::Add,
			BinaryOperator::Multiply => crate::expr::BinaryOperator::Multiply,
			BinaryOperator::Divide => crate::expr::BinaryOperator::Divide,
			BinaryOperator::Remainder => crate::expr::BinaryOperator::Remainder,
			BinaryOperator::Power => crate::expr::BinaryOperator::Power,
			BinaryOperator::Equal => crate::expr::BinaryOperator::Equal,
			BinaryOperator::ExactEqual => crate::expr::BinaryOperator::ExactEqual,
			BinaryOperator::NotEqual => crate::expr::BinaryOperator::NotEqual,
			BinaryOperator::AllEqual => crate::expr::BinaryOperator::AllEqual,
			BinaryOperator::AnyEqual => crate::expr::BinaryOperator::AnyEqual,
			BinaryOperator::Or => crate::expr::BinaryOperator::Or,
			BinaryOperator::And => crate::expr::BinaryOperator::And,
			BinaryOperator::NullCoalescing => crate::expr::BinaryOperator::NullCoalescing,
			BinaryOperator::TenaryCondition => crate::expr::BinaryOperator::TenaryCondition,
			BinaryOperator::LessThan => crate::expr::BinaryOperator::LessThan,
			BinaryOperator::LessThanEqual => crate::expr::BinaryOperator::LessThanEqual,
			BinaryOperator::MoreThan => crate::expr::BinaryOperator::MoreThan,
			BinaryOperator::MoreThanEqual => crate::expr::BinaryOperator::MoreThanEqual,
			BinaryOperator::Contain => crate::expr::BinaryOperator::Contain,
			BinaryOperator::NotContain => crate::expr::BinaryOperator::NotContain,
			BinaryOperator::ContainAll => crate::expr::BinaryOperator::ContainAll,
			BinaryOperator::ContainAny => crate::expr::BinaryOperator::ContainAny,
			BinaryOperator::ContainNone => crate::expr::BinaryOperator::ContainNone,
			BinaryOperator::Inside => crate::expr::BinaryOperator::Inside,
			BinaryOperator::NotInside => crate::expr::BinaryOperator::NotInside,
			BinaryOperator::AllInside => crate::expr::BinaryOperator::AllInside,
			BinaryOperator::AnyInside => crate::expr::BinaryOperator::AnyInside,
			BinaryOperator::NoneInside => crate::expr::BinaryOperator::NoneInside,
			BinaryOperator::Outside => crate::expr::BinaryOperator::Outside,
			BinaryOperator::Intersects => crate::expr::BinaryOperator::Intersects,
			BinaryOperator::Range => crate::expr::BinaryOperator::Range,
			BinaryOperator::RangeInclusive => crate::expr::BinaryOperator::RangeInclusive,
			BinaryOperator::RangeSkip => crate::expr::BinaryOperator::RangeSkip,
			BinaryOperator::RangeSkipInclusive => crate::expr::BinaryOperator::RangeSkipInclusive,
			BinaryOperator::Matches(m) => crate::expr::BinaryOperator::Matches(m.into()),
			BinaryOperator::NearestNeighbor(n) => {
				crate::expr::BinaryOperator::NearestNeighbor(Box::new((*n).into()))
			}
		}
	}
}

impl From<crate::expr::BinaryOperator> for BinaryOperator {
	fn from(value: crate::expr::BinaryOperator) -> Self {
		match value {
			crate::expr::BinaryOperator::Subtract => BinaryOperator::Subtract,
			crate::expr::BinaryOperator::Add => BinaryOperator::Add,
			crate::expr::BinaryOperator::Multiply => BinaryOperator::Multiply,
			crate::expr::BinaryOperator::Divide => BinaryOperator::Divide,
			crate::expr::BinaryOperator::Remainder => BinaryOperator::Remainder,
			crate::expr::BinaryOperator::Power => BinaryOperator::Power,
			crate::expr::BinaryOperator::Equal => BinaryOperator::Equal,
			crate::expr::BinaryOperator::ExactEqual => BinaryOperator::ExactEqual,
			crate::expr::BinaryOperator::NotEqual => BinaryOperator::NotEqual,
			crate::expr::BinaryOperator::AllEqual => BinaryOperator::AllEqual,
			crate::expr::BinaryOperator::AnyEqual => BinaryOperator::AnyEqual,
			crate::expr::BinaryOperator::Or => BinaryOperator::Or,
			crate::expr::BinaryOperator::And => BinaryOperator::And,
			crate::expr::BinaryOperator::NullCoalescing => BinaryOperator::NullCoalescing,
			crate::expr::BinaryOperator::TenaryCondition => BinaryOperator::TenaryCondition,
			crate::expr::BinaryOperator::LessThan => BinaryOperator::LessThan,
			crate::expr::BinaryOperator::LessThanEqual => BinaryOperator::LessThanEqual,
			crate::expr::BinaryOperator::MoreThan => BinaryOperator::MoreThan,
			crate::expr::BinaryOperator::MoreThanEqual => BinaryOperator::MoreThanEqual,
			crate::expr::BinaryOperator::Contain => BinaryOperator::Contain,
			crate::expr::BinaryOperator::NotContain => BinaryOperator::NotContain,
			crate::expr::BinaryOperator::ContainAll => BinaryOperator::ContainAll,
			crate::expr::BinaryOperator::ContainAny => BinaryOperator::ContainAny,
			crate::expr::BinaryOperator::ContainNone => BinaryOperator::ContainNone,
			crate::expr::BinaryOperator::Inside => BinaryOperator::Inside,
			crate::expr::BinaryOperator::NotInside => BinaryOperator::NotInside,
			crate::expr::BinaryOperator::AllInside => BinaryOperator::AllInside,
			crate::expr::BinaryOperator::AnyInside => BinaryOperator::AnyInside,
			crate::expr::BinaryOperator::NoneInside => BinaryOperator::NoneInside,
			crate::expr::BinaryOperator::Outside => BinaryOperator::Outside,
			crate::expr::BinaryOperator::Intersects => BinaryOperator::Intersects,
			crate::expr::BinaryOperator::Range => BinaryOperator::Range,
			crate::expr::BinaryOperator::RangeInclusive => BinaryOperator::RangeInclusive,
			crate::expr::BinaryOperator::RangeSkip => BinaryOperator::RangeSkip,
			crate::expr::BinaryOperator::RangeSkipInclusive => BinaryOperator::RangeSkipInclusive,
			crate::expr::BinaryOperator::Matches(m) => BinaryOperator::Matches(m.into()),
			crate::expr::BinaryOperator::NearestNeighbor(n) => {
				BinaryOperator::NearestNeighbor(Box::new((*n).into()))
			}
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum NearestNeighbor {
	/// `<|k, dist|>`
	K(u32, Distance),
	/// `<|k|>`
	KTree(u32),
	/// `<|k, ef|>`
	Approximate(u32, u32),
}

impl ToSql for NearestNeighbor {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::K(k, d) => write_sql!(f, fmt, "<|{k},{d}|>"),
			Self::KTree(k) => write_sql!(f, fmt, "<|{k}|>"),
			Self::Approximate(k, ef) => write_sql!(f, fmt, "<|{k},{ef}|>"),
		}
	}
}
impl From<NearestNeighbor> for crate::expr::operator::NearestNeighbor {
	fn from(value: NearestNeighbor) -> Self {
		match value {
			NearestNeighbor::K(k, d) => crate::expr::operator::NearestNeighbor::K(k, d.into()),
			NearestNeighbor::KTree(k) => crate::expr::operator::NearestNeighbor::KTree(k),
			NearestNeighbor::Approximate(k, ef) => {
				crate::expr::operator::NearestNeighbor::Approximate(k, ef)
			}
		}
	}
}

impl From<crate::expr::operator::NearestNeighbor> for NearestNeighbor {
	fn from(value: crate::expr::operator::NearestNeighbor) -> Self {
		match value {
			crate::expr::operator::NearestNeighbor::K(k, d) => NearestNeighbor::K(k, d.into()),
			crate::expr::operator::NearestNeighbor::KTree(k) => NearestNeighbor::KTree(k),
			crate::expr::operator::NearestNeighbor::Approximate(k, ef) => {
				NearestNeighbor::Approximate(k, ef)
			}
		}
	}
}

impl ToSql for BinaryOperator {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Or => write_sql!(f, fmt, "OR"),
			Self::And => write_sql!(f, fmt, "AND"),
			Self::NullCoalescing => write_sql!(f, fmt, "??"),
			Self::TenaryCondition => write_sql!(f, fmt, "?:"),
			Self::Add => write_sql!(f, fmt, "+"),
			Self::Subtract => write_sql!(f, fmt, "-"),
			Self::Multiply => write_sql!(f, fmt, "*"),
			Self::Divide => write_sql!(f, fmt, "/"),
			Self::Remainder => write_sql!(f, fmt, "%"),
			Self::Power => write_sql!(f, fmt, "**"),
			Self::Equal => write_sql!(f, fmt, "="),
			Self::ExactEqual => write_sql!(f, fmt, "=="),
			Self::NotEqual => write_sql!(f, fmt, "!="),
			Self::AllEqual => write_sql!(f, fmt, "*="),
			Self::AnyEqual => write_sql!(f, fmt, "?="),
			Self::LessThan => write_sql!(f, fmt, "<"),
			Self::LessThanEqual => write_sql!(f, fmt, "<="),
			Self::MoreThan => write_sql!(f, fmt, ">"),
			Self::MoreThanEqual => write_sql!(f, fmt, ">="),
			Self::Contain => write_sql!(f, fmt, "CONTAINS"),
			Self::NotContain => write_sql!(f, fmt, "CONTAINSNOT"),
			Self::ContainAll => write_sql!(f, fmt, "CONTAINSALL"),
			Self::ContainAny => write_sql!(f, fmt, "CONTAINSANY"),
			Self::ContainNone => write_sql!(f, fmt, "CONTAINSNONE"),
			Self::Inside => write_sql!(f, fmt, "INSIDE"),
			Self::NotInside => write_sql!(f, fmt, "NOTINSIDE"),
			Self::AllInside => write_sql!(f, fmt, "ALLINSIDE"),
			Self::AnyInside => write_sql!(f, fmt, "ANYINSIDE"),
			Self::NoneInside => write_sql!(f, fmt, "NONEINSIDE"),
			Self::Outside => write_sql!(f, fmt, "OUTSIDE"),
			Self::Intersects => write_sql!(f, fmt, "INTERSECTS"),
			Self::Matches(m) => m.fmt_sql(f, fmt),
			Self::Range => write_sql!(f, fmt, ".."),
			Self::RangeInclusive => write_sql!(f, fmt, "..="),
			Self::RangeSkip => write_sql!(f, fmt, ">.."),
			Self::RangeSkipInclusive => write_sql!(f, fmt, ">..="),
			Self::NearestNeighbor(n) => n.fmt_sql(f, fmt),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AssignOperator {
	Assign,
	Add,
	Subtract,
	Extend,
}

impl From<AssignOperator> for crate::expr::AssignOperator {
	fn from(value: AssignOperator) -> Self {
		match value {
			AssignOperator::Assign => crate::expr::AssignOperator::Assign,
			AssignOperator::Add => crate::expr::AssignOperator::Add,
			AssignOperator::Extend => crate::expr::AssignOperator::Extend,
			AssignOperator::Subtract => crate::expr::AssignOperator::Subtract,
		}
	}
}
impl From<crate::expr::AssignOperator> for AssignOperator {
	fn from(value: crate::expr::AssignOperator) -> Self {
		match value {
			crate::expr::AssignOperator::Assign => AssignOperator::Assign,
			crate::expr::AssignOperator::Add => AssignOperator::Add,
			crate::expr::AssignOperator::Extend => AssignOperator::Extend,
			crate::expr::AssignOperator::Subtract => AssignOperator::Subtract,
		}
	}
}

impl ToSql for AssignOperator {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Assign => write_sql!(f, fmt, "="),
			Self::Add => write_sql!(f, fmt, "+="),
			Self::Subtract => write_sql!(f, fmt, "-="),
			Self::Extend => write_sql!(f, fmt, "+?="),
		}
	}
}

/// An enum which defines how strong a operator binds it's operands.
///
/// If a binding power is higher the operator is more likely to directly operate
/// on it's neighbours.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum BindingPower {
	Base,
	Or,
	And,
	Equality,
	Relation,
	AddSub,
	MulDiv,
	Power,
	Nullish,
	Prefix,
	Range,
	Call,
	Prime,
}

impl BindingPower {
	/// Returns the binding power of this operator.
	///
	/// Note that there are some variants here which can have multiple meanings.
	/// `Operator::Equal` can be assignment but can also be equality.
	/// `Operator::Add` can be the add operator but also the plus prefix
	/// operator which have different binding powers.
	///
	/// This function returns the binding power for if the operator is used in
	/// the infix position.
	pub fn for_binary_operator(op: &BinaryOperator) -> Self {
		match op {
			BinaryOperator::Or => BindingPower::Or,
			BinaryOperator::And => BindingPower::And,

			BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual => BindingPower::Equality,

			BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::Matches(_)
			| BinaryOperator::Contain
			| BinaryOperator::NotContain
			| BinaryOperator::ContainAll
			| BinaryOperator::ContainAny
			| BinaryOperator::ContainNone
			| BinaryOperator::Inside
			| BinaryOperator::NotInside
			| BinaryOperator::AllInside
			| BinaryOperator::AnyInside
			| BinaryOperator::NoneInside
			| BinaryOperator::Outside
			| BinaryOperator::Intersects
			| BinaryOperator::NearestNeighbor(_) => BindingPower::Relation,

			BinaryOperator::Add | BinaryOperator::Subtract => BindingPower::AddSub,

			BinaryOperator::Multiply | BinaryOperator::Divide | BinaryOperator::Remainder => {
				BindingPower::MulDiv
			}

			BinaryOperator::Power => BindingPower::Power,

			BinaryOperator::NullCoalescing | BinaryOperator::TenaryCondition => {
				BindingPower::Nullish
			}

			BinaryOperator::Range
			| BinaryOperator::RangeInclusive
			| BinaryOperator::RangeSkip
			| BinaryOperator::RangeSkipInclusive => BindingPower::Range,
		}
	}

	pub fn for_postfix_operator(op: &PostfixOperator) -> Self {
		match op {
			PostfixOperator::Range | PostfixOperator::RangeSkip => BindingPower::Range,
			PostfixOperator::MethodCall(..) | PostfixOperator::Call(..) => BindingPower::Call,
		}
	}

	pub fn for_prefix_operator(op: &PrefixOperator) -> Self {
		match op {
			PrefixOperator::Range | PrefixOperator::RangeInclusive => BindingPower::Range,
			PrefixOperator::Not
			| PrefixOperator::Positive
			| PrefixOperator::Negate
			| PrefixOperator::Cast(_) => BindingPower::Prefix,
		}
	}

	/// Returns the binding power for this expression. This is generally
	/// `BindingPower::Prime` as most value variants are prime expressions,
	/// however some like Value::Expression and Value::Range have a different
	/// binding power.
	pub fn for_expr(expr: &Expr) -> BindingPower {
		match expr {
			Expr::Prefix {
				..
			} => BindingPower::Prefix,
			Expr::Binary {
				op,
				..
			} => BindingPower::for_binary_operator(op),
			_ => BindingPower::Prime,
		}
	}
}
