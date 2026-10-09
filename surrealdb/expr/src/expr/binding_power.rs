//! Operator precedence used by canonical expression printing.

use super::BinaryOperator;

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
}
