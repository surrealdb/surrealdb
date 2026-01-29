use priority_lfu::DeepSizeOf;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::Distance;
use crate::expr::{Expr, Kind};
use crate::idx::ft::MatchRef;

#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
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
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let prefix_operator: crate::sql::PrefixOperator = self.clone().into();
		prefix_operator.fmt_sql(f, sql_fmt);
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
pub(crate) enum PostfixOperator {
	Range,
	RangeSkip,
	/// Not used as of yet but will be once the idiom is properly restructured.
	MethodCall(String, Vec<Expr>),
	Call(Vec<Expr>),
}

impl ToSql for PostfixOperator {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let postfix_operator: crate::sql::PostfixOperator = self.clone().into();
		postfix_operator.fmt_sql(f, sql_fmt);
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
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

#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
pub(crate) struct MatchesOperator {
	pub rf: Option<MatchRef>,
	pub operator: BooleanOperator,
}

impl ToSql for MatchesOperator {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let matches_operator: crate::sql::operator::MatchesOperator = self.clone().into();
		matches_operator.fmt_sql(f, sql_fmt);
	}
}

/// Boolean operation executed by the full-text index

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
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

#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
pub(crate) enum NearestNeighbor {
	/// `<|k, dist|>`
	K(u32, Distance),
	/// `<|k|>`
	KTree(u32),
	/// `<|k, ef|>`
	Approximate(u32, u32),
}

impl ToSql for NearestNeighbor {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let nn: crate::sql::operator::NearestNeighbor = self.clone().into();
		nn.fmt_sql(f, sql_fmt);
	}
}

impl ToSql for BinaryOperator {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let binary_operator: crate::sql::BinaryOperator = self.clone().into();
		binary_operator.fmt_sql(f, sql_fmt);
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, priority_lfu::DeepSizeOf)]
pub enum AssignOperator {
	Assign,
	Add,
	Subtract,
	Extend,
}

impl ToSql for AssignOperator {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let op: crate::sql::AssignOperator = self.clone().into();
		op.fmt_sql(f, sql_fmt);
	}
}

/// An enum which defines how strong a operator binds it's operands.
///
/// If a binding power is higher the operator is more likely to directly operate
/// on it's neighbours.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
#[allow(dead_code)]
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

#[allow(dead_code)]
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
				op,
				..
			} => Self::for_prefix_operator(op),
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
