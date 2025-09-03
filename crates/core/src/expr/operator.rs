use std::fmt;

use crate::catalog::Distance;
use crate::expr::fmt::Fmt;
use crate::expr::{Expr, Ident, Kind};
use crate::idx::ft::MatchRef;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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

impl fmt::Display for PrefixOperator {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Not => write!(f, "!"),
			Self::Positive => write!(f, "+"),
			Self::Negate => write!(f, "-"),
			Self::Range => write!(f, ".."),
			Self::RangeInclusive => write!(f, "..="),
			Self::Cast(kind) => write!(f, "<{kind}> "),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum PostfixOperator {
	Range,
	RangeSkip,
	/// Not used as of yet but will be once the idiom is properly restructured.
	MethodCall(Ident, Vec<Expr>),
	Call(Vec<Expr>),
}

impl fmt::Display for PostfixOperator {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Range => write!(f, ".."),
			Self::RangeSkip => write!(f, ">.."),
			Self::MethodCall(name, expr) => {
				write!(f, ".{name}({})", Fmt::comma_separated(expr.iter()))
			}
			Self::Call(expr) => {
				write!(f, "({})", Fmt::comma_separated(expr.iter()))
			}
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
	/// `?:`
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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MatchesOperator {
	pub rf: Option<MatchRef>,
	pub operator: BooleanOperator,
}

impl fmt::Display for MatchesOperator {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if let Some(r) = self.rf {
			if self.operator != BooleanOperator::And {
				write!(f, "@{r},{}@", self.operator)
			} else {
				write!(f, "@{r}@")
			}
		} else if self.operator != BooleanOperator::And {
			write!(f, "@{}@", self.operator)
		} else {
			f.write_str("@@")
		}
	}
}

/// Boolean operation executed by the full-text index

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum BooleanOperator {
	And,
	Or,
}

impl fmt::Display for BooleanOperator {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::And => f.write_str("AND"),
			Self::Or => f.write_str("OR"),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum NearestNeighbor {
	/// `<|k, dist|>`
	K(u32, Distance),
	/// `<|k|>`
	KTree(u32),
	/// `<|k, ef|>`
	Approximate(u32, u32),
}

impl fmt::Display for NearestNeighbor {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			NearestNeighbor::KTree(k) => {
				write!(f, "<|{k}|>")
			}
			NearestNeighbor::K(k, distance) => {
				write!(f, "<|{k},{distance}|>")
			}
			NearestNeighbor::Approximate(k, ef) => {
				write!(f, "<|{k},{ef}|>")
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
			Self::TenaryCondition => write!(f, "?:"),
			Self::Add => write!(f, "+"),
			Self::Subtract => write!(f, "-"),
			Self::Multiply => write!(f, "*"),
			Self::Divide => write!(f, "/"),
			Self::Remainder => write!(f, "%"),
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
			Self::Matches(x) => x.fmt(f),
			Self::Range => write!(f, ".."),
			Self::RangeInclusive => write!(f, "..="),
			Self::RangeSkip => write!(f, ">.."),
			Self::RangeSkipInclusive => write!(f, ">..="),
			Self::NearestNeighbor(n) => n.fmt(f),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum AssignOperator {
	Assign,
	Add,
	Subtract,
	Extend,
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
/// If a binding power is higher the operator is more likely to directly operate
/// on it's neighbours.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum BindingPower {
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
	Postfix,
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
			PostfixOperator::MethodCall(..) | PostfixOperator::Call(..) => BindingPower::Postfix,
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
			Expr::Postfix {
				op,
				..
			} => BindingPower::for_postfix_operator(op),
			Expr::Binary {
				op,
				..
			} => BindingPower::for_binary_operator(op),
			_ => BindingPower::Prime,
		}
	}
}
