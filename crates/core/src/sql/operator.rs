use crate::sql::fmt::Fmt;
use crate::sql::index::Distance;
use crate::sql::{Expr, Kind};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
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
			PrefixOperator::Negate => crate::expr::PrefixOperator::Negative,
			PrefixOperator::Range => crate::expr::PrefixOperator::Range,
			PrefixOperator::RangeInclusive => crate::expr::PrefixOperator::RangeInclusive,
			PrefixOperator::Cast(k) => crate::expr::PrefixOperator::Cast(k.into()),
		}
	}
}

impl From<crate::expr::PrefixOperator> for PrefixOperator {
	fn from(value: PrefixOperator) -> Self {
		match value {
			crate::expr::PrefixOperator::Not => PrefixOperator::Not,
			crate::expr::PrefixOperator::Positive => PrefixOperator::Positive,
			crate::expr::PrefixOperator::Negative => PrefixOperator::Negate,
			crate::expr::PrefixOperator::Range => PrefixOperator::Range,
			crate::expr::PrefixOperator::RangeInclusive => PrefixOperator::RangeInclusive,
			crate::expr::PrefixOperator::Cast(k) => PrefixOperator::Cast(k.into()),
		}
	}
}

impl fmt::Display for PrefixOperator {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Not => write!(f, "!"),
			Self::Positive => write!(f, "+"),
			Self::Negate => write!(f, "-"),
			Self::Range => write!(f, ".."),
			Self::RangeInclusive => write!(f, "..="),
			Self::Cast(kind) => write!(f, "<{kind}>"),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum PostfixOperator {
	Range,
	RangeSkip,
	Call(Vec<Expr>),
}

impl From<PostfixOperator> for crate::expr::PostfixOperator {
	fn from(value: PrefixOperator) -> Self {
		match value {
			PostfixOperator::Range => crate::expr::PostfixOperator::Range,
			PostfixOperator::RangeSkip => crate::expr::PostfixOperator::RangeSkip,
			PostfixOperator::Call(x) => {
				crate::expr::PostfixOperator::Call(x.into_iter().map(|x| x.into()))
			}
		}
	}
}

impl From<crate::expr::PostfixOperator> for PostfixOperator {
	fn from(value: PostfixOperator) -> Self {
		match value {
			crate::expr::PostfixOperator::Range => PostfixOperator::Range,
			crate::expr::PostfixOperator::RangeSkip => PostfixOperator::RangeSkip,
			crate::expr::PostfixOperator::Call(x) => {
				PostfixOperator::Call(x.into_iter().map(|x| x.into()))
			}
		}
	}
}

impl fmt::Display for PostfixOperator {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Range => write!(f, ".."),
			Self::RangeSkip => write!(f, ">.."),
			Self::Call(x) => write!(f, "({})", Fmt::comma_separated(x)),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum BinaryOperator {
	/// `-`
	Subtract,
	/// `+`
	Add,
	/// `*`, `×`
	Multiply,
	/// `/`
	Divide,
	/// `%`
	Modulo,
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
	Matches(Option<u8>),
	// `<|k,..|>`
	NearestNeighbor(Box<NearestNeighbor>),
}

impl From<BinaryOperator> for crate::expr::BinaryOperator {
	fn from(value: BinaryOperator) -> Self {
		match value {
			BinaryOperator::Subtract => crate::expr::BinaryOperator::Subtract,
			BinaryOperator::Add => crate::expr::BinaryOperator::Add,
			BinaryOperator::Multiply => crate::expr::BinaryOperator::Multiply,
			BinaryOperator::Divide => crate::expr::BinaryOperator::Divide,
			BinaryOperator::Modulo => crate::expr::BinaryOperator::Remainder,
			BinaryOperator::Power => crate::expr::BinaryOperator::Power,
			BinaryOperator::Equal => crate::expr::BinaryOperator::Equal,
			BinaryOperator::ExactEqual => crate::expr::BinaryOperator::ExactEqual,
			BinaryOperator::NotEqual => crate::expr::BinaryOperator::NotEqual,
			BinaryOperator::AllEqual => crate::expr::BinaryOperator::AllEqual,
			BinaryOperator::AnyEqual => crate::expr::BinaryOperator::AnyEqual,
			BinaryOperator::Or => crate::expr::BinaryOperator::Or,
			BinaryOperator::And => crate::expr::BinaryOperator::And,
			BinaryOperator::NullCoalescing => crate::expr::BinaryOperator::NullCoalescing,
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
				crate::expr::BinaryOperator::NearestNeighbor(n.into())
			}
		}
	}
}

impl From<crate::expr::BinaryOperator> for BinaryOperator {
	fn from(value: BinaryOperator) -> Self {
		match value {
			crate::expr::BinaryOperator::Subtract => BinaryOperator::Subtract,
			crate::expr::BinaryOperator::Add => BinaryOperator::Add,
			crate::expr::BinaryOperator::Multiply => BinaryOperator::Multiply,
			crate::expr::BinaryOperator::Divide => BinaryOperator::Divide,
			crate::expr::BinaryOperator::Remainder => BinaryOperator::Modulo,
			crate::expr::BinaryOperator::Power => BinaryOperator::Power,
			crate::expr::BinaryOperator::Equal => BinaryOperator::Equal,
			crate::expr::BinaryOperator::ExactEqual => BinaryOperator::ExactEqual,
			crate::expr::BinaryOperator::NotEqual => BinaryOperator::NotEqual,
			crate::expr::BinaryOperator::AllEqual => BinaryOperator::AllEqual,
			crate::expr::BinaryOperator::AnyEqual => BinaryOperator::AnyEqual,
			crate::expr::BinaryOperator::Or => BinaryOperator::Or,
			crate::expr::BinaryOperator::And => BinaryOperator::And,
			crate::expr::BinaryOperator::NullCoalescing => BinaryOperator::NullCoalescing,
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
				BinaryOperator::NearestNeighbor(n.into())
			}
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum NearestNeighbor {
	/// `<|k, dist|>`
	K(u32, Distance),
	/// `<|k|>`
	KTree(u32),
	/// `<|k, ef|>`
	Approximate(u32, u32),
}

impl From<NearestNeighbor> for crate::expr::operator::NearestNeighbor {
	fn from(value: NearestNeighbor) -> Self {
		match value {
			NearestNeighbor::K(k, d) => crate::expr::operator::NearestNeighbor::K(k, d),
			NearestNeighbor::KTree(k) => crate::expr::operator::NearestNeighbor::KTree(k),
			NearestNeighbor::Approximate(k, ef) => {
				crate::expr::operator::NearestNeighbor::Approximate(k, ef)
			}
		}
	}
}

impl fmt::Display for BinaryOperator {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Or => write!(f, "OR"),
			Self::And => write!(f, "AND"),
			Self::NullCoalescing => write!(f, "??"),
			Self::Add => write!(f, "+"),
			Self::Subtract => write!(f, "-"),
			Self::Multiply => write!(f, "*"),
			Self::Divide => write!(f, "/"),
			Self::Modulo => write!(f, "%"),
			Self::Power => write!(f, "**"),
			Self::Equal => write!(f, "="),
			Self::ExactEqual => write!(f, "=="),
			Self::NotEqual => write!(f, "!="),
			Self::AllEqual => write!(f, "*="),
			Self::AnyEqual => write!(f, "?="),
			Self::LessThan => write!(f, "<"),
			Self::LessThanEqual => write!(f, "<="),
			Self::MoreThan => write!(f, ">"),
			Self::MoreThanEqual => write!(f, ">="),
			Self::Contain => write!(f, "CONTAINS"),
			Self::NotContain => write!(f, "CONTAINSNOT"),
			Self::ContainAll => write!(f, "CONTAINSALL"),
			Self::ContainAny => write!(f, "CONTAINSANY"),
			Self::ContainNone => write!(f, "CONTAINSNONE"),
			Self::Inside => write!(f, "INSIDE"),
			Self::NotInside => write!(f, "NOTINSIDE"),
			Self::AllInside => write!(f, "ALLINSIDE"),
			Self::AnyInside => write!(f, "ANYINSIDE"),
			Self::NoneInside => write!(f, "NONEINSIDE"),
			Self::Outside => write!(f, "OUTSIDE"),
			Self::Intersects => write!(f, "INTERSECTS"),
			Self::Matches(reference) => {
				if let Some(r) = reference {
					write!(f, "@{r}@")
				} else {
					f.write_str("@@")
				}
			}
			Self::Range => write!(f, ".."),
			Self::RangeInclusive => write!(f, "..="),
			Self::RangeSkip => write!(f, ">.."),
			Self::RangeSkipInclusive => write!(f, ">..="),
			Self::NearestNeighbor(n) => match &**n {
				NearestNeighbor::KTree(k) => {
					write!(f, "<|{k}|>")
				}
				NearestNeighbor::K(k, distance) => {
					write!(f, "<|{k},{distance}|>")
				}
				NearestNeighbor::Approximate(k, ef) => {
					write!(f, "<|{k},{ef}|>")
				}
			},
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
	fn from(value: AssignOperator) -> Self {
		match value {
			crate::expr::AssignOperator::Assign => AssignOperator::Assign,
			crate::expr::AssignOperator::Add => AssignOperator::Add,
			crate::expr::AssignOperator::Extend => AssignOperator::Extend,
			crate::expr::AssignOperator::Subtract => AssignOperator::Subtract,
		}
	}
}

impl fmt::Display for AssignOperator {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Assign => write!(f, "="),
			Self::Add => write!(f, "+="),
			Self::Subtract => write!(f, "-="),
			Self::Extend => write!(f, "+?="),
		}
	}
}

/// An enum which defines how strong a operator binds it's operands.
///
/// If a binding power is higher the operator is more likely to directly operate on it's
/// neighbours.
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
	Range,
	Nullish,
	Prefix,
	Call,
	Prime,
}

impl BindingPower {
	/// Returns the binding power of this operator.
	///
	/// Note that there are some variants here which can have multiple meanings.
	/// `Operator::Equal` can be assignment but can also be equality.
	/// `Operator::Add` can be the add operator but also the plus prefix operator which have different binding
	/// powers.
	///
	/// This function returns the binding power for if the operator is used in the infix position.
	pub fn for_binary_opator(op: &BinaryOperator) -> Self {
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

			BinaryOperator::Multiply | BinaryOperator::Divide | BinaryOperator::Modulo => {
				BindingPower::MulDiv
			}

			BinaryOperator::Power => BindingPower::Power,

			BinaryOperator::NullCoalescing => BindingPower::Nullish,

			BinaryOperator::Range
			| BinaryOperator::RangeInclusive
			| BinaryOperator::RangeSkip
			| BinaryOperator::RangeSkipInclusive => BindingPower::Range,
		}
	}

	/// Returns the binding power for this expression. This is generally `BindingPower::Prime` as
	/// most value variants are prime expressions, however some like Value::Expression and
	/// Value::Range have a different binding power.
	pub fn for_expr(expr: &Expr) -> BindingPower {
		match expr {
			Expr::Prefix {
				..
			} => BindingPower::Prefix,
			Expr::Binary {
				op,
				..
			} => BindingPower::for_binary_opator(op),
			_ => BindingPower::Prime,
		}
	}
}
