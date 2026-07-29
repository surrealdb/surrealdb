use common::fmt::{EscapeKwFreeIdent, Fmt};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::index::Distance;
use crate::{CoverStmts, Expr, Kind};

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

impl ToSql for PrefixOperator {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Not => f.push('!'),
			Self::Positive => f.push('+'),
			Self::Negate => f.push('-'),
			Self::Range => f.push_str(".."),
			Self::RangeInclusive => f.push_str("..="),
			Self::Cast(kind) => write_sql!(f, fmt, "<{kind}> "),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum PostfixOperator {
	Range,
	RangeSkip,
	#[cfg_attr(feature = "arbitrary", arbitrary(skip))]
	MethodCall(String, Vec<Expr>),
	Call(Vec<Expr>),
}

impl ToSql for PostfixOperator {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Range => f.push_str(".."),
			Self::RangeSkip => f.push_str(">.."),
			Self::MethodCall(name, x) => {
				write_sql!(
					f,
					fmt,
					".{}({})",
					EscapeKwFreeIdent(name),
					Fmt::comma_separated(x.iter().map(CoverStmts))
				);
			}
			Self::Call(args) => {
				write_sql!(f, fmt, "({})", Fmt::comma_separated(args.iter().map(CoverStmts)))
			}
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
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
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if let Some(r) = self.rf {
			if let Some(ref o) = self.operator {
				// Don't show AND operator since it's the default
				if !matches!(o, BooleanOperator::And) {
					write_sql!(f, fmt, "@{r},{o}@");
				} else {
					write_sql!(f, fmt, "@{r}@");
				}
			} else {
				write_sql!(f, fmt, "@{r}@");
			}
		} else if let Some(ref o) = self.operator {
			// Don't show AND operator since it's the default
			if !matches!(o, BooleanOperator::And) {
				write_sql!(f, fmt, "@{o}@");
			} else {
				f.push_str("@@");
			}
		} else {
			f.push_str("@@");
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

impl ToSql for BooleanOperator {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			Self::And => f.push_str("AND"),
			Self::Or => f.push_str("OR"),
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

impl ToSql for NearestNeighbor {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::K(k, d) => write_sql!(f, fmt, "<|{k},{d}|>"),
			Self::KTree(k) => write_sql!(f, fmt, "<|{k}|>"),
			Self::Approximate(k, ef) => write_sql!(f, fmt, "<|{k},{ef}|>"),
		}
	}
}

impl ToSql for BinaryOperator {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Or => f.push_str("OR"),
			Self::And => f.push_str("AND"),
			Self::NullCoalescing => f.push_str("??"),
			Self::TenaryCondition => f.push_str("?:"),
			Self::Add => f.push('+'),
			Self::Subtract => f.push('-'),
			Self::Multiply => f.push('*'),
			Self::Divide => f.push('/'),
			Self::Remainder => f.push('%'),
			Self::Power => f.push_str("**"),
			Self::Equal => f.push('='),
			Self::ExactEqual => f.push_str("=="),
			Self::NotEqual => f.push_str("!="),
			Self::AllEqual => f.push_str("*="),
			Self::AnyEqual => f.push_str("?="),
			Self::LessThan => f.push('<'),
			Self::LessThanEqual => f.push_str("<="),
			Self::MoreThan => f.push('>'),
			Self::MoreThanEqual => f.push_str(">="),
			Self::Contain => f.push_str("CONTAINS"),
			Self::NotContain => f.push_str("CONTAINSNOT"),
			Self::ContainAll => f.push_str("CONTAINSALL"),
			Self::ContainAny => f.push_str("CONTAINSANY"),
			Self::ContainNone => f.push_str("CONTAINSNONE"),
			Self::Inside => f.push_str("INSIDE"),
			Self::NotInside => f.push_str("NOTINSIDE"),
			Self::AllInside => f.push_str("ALLINSIDE"),
			Self::AnyInside => f.push_str("ANYINSIDE"),
			Self::NoneInside => f.push_str("NONEINSIDE"),
			Self::Outside => f.push_str("OUTSIDE"),
			Self::Intersects => f.push_str("INTERSECTS"),
			Self::Matches(m) => m.fmt_sql(f, fmt),
			Self::Range => f.push_str(".."),
			Self::RangeInclusive => f.push_str("..="),
			Self::RangeSkip => f.push_str(">.."),
			Self::RangeSkipInclusive => f.push_str(">..="),
			Self::NearestNeighbor(n) => match &**n {
				NearestNeighbor::KTree(k) => {
					write_sql!(f, fmt, "<|{k}|>");
				}
				NearestNeighbor::K(k, distance) => {
					write_sql!(f, fmt, "<|{k},{distance}|>");
				}
				NearestNeighbor::Approximate(k, ef) => {
					write_sql!(f, fmt, "<|{k},{ef}|>");
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

impl ToSql for AssignOperator {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			Self::Assign => f.push('='),
			Self::Add => f.push_str("+="),
			Self::Subtract => f.push_str("-="),
			Self::Extend => f.push_str("+?="),
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
	Nullish,
	Or,
	And,
	Equality,
	Relation,
	AddSub,
	MulDiv,
	Power,
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
				op,
				..
			} => {
				if let PrefixOperator::Range | PrefixOperator::RangeInclusive = *op {
					BindingPower::Range
				} else {
					BindingPower::Prefix
				}
			}
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
