use crate::idx::ft::MatchRef;
use crate::sql::index::Distance;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Write;

use super::Value;

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
	pub fn for_value(value: &Value) -> BindingPower {
		match value {
			Value::Expression(expr) => match **expr {
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
			Value::Range(..) => BindingPower::Range,
			_ => BindingPower::Prime,
		}
	}
}
