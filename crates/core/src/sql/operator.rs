use crate::idx::ft::search::MatchRef;
use crate::sql::index::Distance;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Write;

use super::SqlValue;

/// Binary operators.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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
	Knn(u32, Option<Distance>), // <|{k}[,{dist}]|>
	#[revision(start = 2)]
	Ann(u32, u32), // <|{k},{ef}|>
	//
	Rem, // %
}

impl Default for Operator {
	fn default() -> Self {
		Self::Equal
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
			Self::Rem => f.write_char('%'),
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
					write!(f, "@{r}@")
				} else {
					f.write_str("@@")
				}
			}
			Self::Knn(k, dist) => {
				if let Some(d) = dist {
					write!(f, "<|{k},{d}|>")
				} else {
					write!(f, "<|{k}|>")
				}
			}
			Self::Ann(k, ef) => {
				write!(f, "<|{k},{ef}|>")
			}
		}
	}
}

impl From<Operator> for crate::expr::Operator {
	fn from(v: Operator) -> Self {
		match v {
			Operator::Neg => crate::expr::Operator::Neg,
			Operator::Not => crate::expr::Operator::Not,
			Operator::Or => crate::expr::Operator::Or,
			Operator::And => crate::expr::Operator::And,
			Operator::Tco => crate::expr::Operator::Tco,
			Operator::Nco => crate::expr::Operator::Nco,
			Operator::Add => crate::expr::Operator::Add,
			Operator::Sub => crate::expr::Operator::Sub,
			Operator::Mul => crate::expr::Operator::Mul,
			Operator::Div => crate::expr::Operator::Div,
			Operator::Rem => crate::expr::Operator::Rem,
			Operator::Pow => crate::expr::Operator::Pow,
			Operator::Inc => crate::expr::Operator::Inc,
			Operator::Dec => crate::expr::Operator::Dec,
			Operator::Ext => crate::expr::Operator::Ext,
			Operator::Equal => crate::expr::Operator::Equal,
			Operator::Exact => crate::expr::Operator::Exact,
			Operator::NotEqual => crate::expr::Operator::NotEqual,
			Operator::AllEqual => crate::expr::Operator::AllEqual,
			Operator::AnyEqual => crate::expr::Operator::AnyEqual,
			Operator::Like => crate::expr::Operator::Like,
			Operator::NotLike => crate::expr::Operator::NotLike,
			Operator::AllLike => crate::expr::Operator::AllLike,
			Operator::AnyLike => crate::expr::Operator::AnyLike,
			Operator::Matches(r) => crate::expr::Operator::Matches(r),
			Operator::LessThan => crate::expr::Operator::LessThan,
			Operator::LessThanOrEqual => crate::expr::Operator::LessThanOrEqual,
			Operator::MoreThan => crate::expr::Operator::MoreThan,
			Operator::MoreThanOrEqual => crate::expr::Operator::MoreThanOrEqual,
			Operator::Contain => crate::expr::Operator::Contain,
			Operator::NotContain => crate::expr::Operator::NotContain,
			Operator::ContainAll => crate::expr::Operator::ContainAll,
			Operator::ContainAny => crate::expr::Operator::ContainAny,
			Operator::ContainNone => crate::expr::Operator::ContainNone,
			Operator::Inside => crate::expr::Operator::Inside,
			Operator::NotInside => crate::expr::Operator::NotInside,
			Operator::AllInside => crate::expr::Operator::AllInside,
			Operator::AnyInside => crate::expr::Operator::AnyInside,
			Operator::NoneInside => crate::expr::Operator::NoneInside,
			Operator::Outside => crate::expr::Operator::Outside,
			Operator::Intersects => crate::expr::Operator::Intersects,
			Operator::Knn(k, d) => crate::expr::Operator::Knn(k, d.map(Into::into)),
			Operator::Ann(k, ef) => crate::expr::Operator::Ann(k, ef),
		}
	}
}

impl From<crate::expr::Operator> for Operator {
	fn from(v: crate::expr::Operator) -> Self {
		match v {
			crate::expr::Operator::Neg => Self::Neg,
			crate::expr::Operator::Not => Self::Not,
			crate::expr::Operator::Or => Self::Or,
			crate::expr::Operator::And => Self::And,
			crate::expr::Operator::Tco => Self::Tco,
			crate::expr::Operator::Nco => Self::Nco,
			crate::expr::Operator::Add => Self::Add,
			crate::expr::Operator::Sub => Self::Sub,
			crate::expr::Operator::Mul => Self::Mul,
			crate::expr::Operator::Div => Self::Div,
			crate::expr::Operator::Rem => Self::Rem,
			crate::expr::Operator::Pow => Self::Pow,
			crate::expr::Operator::Inc => Self::Inc,
			crate::expr::Operator::Dec => Self::Dec,
			crate::expr::Operator::Ext => Self::Ext,
			crate::expr::Operator::Equal => Self::Equal,
			crate::expr::Operator::Exact => Self::Exact,
			crate::expr::Operator::NotEqual => Self::NotEqual,
			crate::expr::Operator::AllEqual => Self::AllEqual,
			crate::expr::Operator::AnyEqual => Self::AnyEqual,
			crate::expr::Operator::Like => Self::Like,
			crate::expr::Operator::NotLike => Self::NotLike,
			crate::expr::Operator::AllLike => Self::AllLike,
			crate::expr::Operator::AnyLike => Self::AnyLike,
			crate::expr::Operator::Matches(r) => Self::Matches(r),
			crate::expr::Operator::LessThan => Self::LessThan,
			crate::expr::Operator::LessThanOrEqual => Self::LessThanOrEqual,
			crate::expr::Operator::MoreThan => Self::MoreThan,
			crate::expr::Operator::MoreThanOrEqual => Self::MoreThanOrEqual,
			crate::expr::Operator::Contain => Self::Contain,
			crate::expr::Operator::NotContain => Self::NotContain,
			crate::expr::Operator::ContainAll => Self::ContainAll,
			crate::expr::Operator::ContainAny => Self::ContainAny,
			crate::expr::Operator::ContainNone => Self::ContainNone,
			crate::expr::Operator::Inside => Self::Inside,
			crate::expr::Operator::NotInside => Self::NotInside,
			crate::expr::Operator::AllInside => Self::AllInside,
			crate::expr::Operator::AnyInside => Self::AnyInside,
			crate::expr::Operator::NoneInside => Self::NoneInside,
			crate::expr::Operator::Outside => Self::Outside,
			crate::expr::Operator::Intersects => Self::Intersects,
			crate::expr::Operator::Knn(k, d) => Self::Knn(k, d.map(Into::into)),
			crate::expr::Operator::Ann(k, ef) => Self::Ann(k, ef),
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
	Cast,
	Range,
	Nullish,
	Unary,
	Postfix,
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
	pub fn for_operator(op: &Operator) -> Self {
		match op {
			Operator::Or => BindingPower::Or,
			Operator::And => BindingPower::And,

			Operator::Equal
			| Operator::Exact
			| Operator::NotEqual
			| Operator::AllEqual
			| Operator::AnyEqual
			| Operator::Like
			| Operator::NotLike
			| Operator::AllLike
			| Operator::AnyLike => BindingPower::Equality,

			Operator::LessThan
			| Operator::LessThanOrEqual
			| Operator::MoreThan
			| Operator::MoreThanOrEqual
			| Operator::Matches(_)
			| Operator::Contain
			| Operator::NotContain
			| Operator::ContainAll
			| Operator::ContainAny
			| Operator::ContainNone
			| Operator::Inside
			| Operator::NotInside
			| Operator::AllInside
			| Operator::AnyInside
			| Operator::NoneInside
			| Operator::Outside
			| Operator::Intersects
			| Operator::Knn(_, _)
			| Operator::Ann(_, _) => BindingPower::Relation,

			Operator::Add | Operator::Sub => BindingPower::AddSub,

			Operator::Mul | Operator::Div | Operator::Rem => BindingPower::MulDiv,

			Operator::Pow => BindingPower::Power,

			Operator::Tco | Operator::Nco => BindingPower::Nullish,

			Operator::Neg | Operator::Not => BindingPower::Unary,

			Operator::Inc | Operator::Dec | Operator::Ext => BindingPower::Base,
		}
	}

	/// Returns the binding power for this expression. This is generally `BindingPower::Prime` as
	/// most value variants are prime expressions, however some like Value::Expression and
	/// Value::Range have a different binding power.
	pub fn for_value(value: &SqlValue) -> BindingPower {
		match value {
			SqlValue::Expression(expr) => match **expr {
				// All prefix expressions have the same binding power, regardless of the actual
				// operator.
				super::Expression::Unary {
					..
				} => BindingPower::Unary,
				super::Expression::Binary {
					ref o,
					..
				} => BindingPower::for_operator(o),
			},
			SqlValue::Range(..) => BindingPower::Range,
			_ => BindingPower::Prime,
		}
	}
}
