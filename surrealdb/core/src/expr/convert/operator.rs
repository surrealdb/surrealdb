//! `sql` -> `expr` conversions for [`crate::sql::operator`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::operator::*;

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
