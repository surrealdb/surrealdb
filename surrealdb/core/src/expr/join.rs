use super::cond::Cond;
use super::expression::Expr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum JoinKind {
	Inner,
	Left,
	Right,
	Cross,
	/// Returns left rows that have at least one match on the right (EXISTS / IN subquery).
	Semi,
	/// Returns left rows that have no matches on the right (NOT EXISTS / NOT IN subquery).
	Anti,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct JoinExpr {
	pub kind: JoinKind,
	pub left: Expr,
	pub right: Expr,
	pub cond: Option<Cond>,
	pub left_alias: Option<String>,
	pub right_alias: Option<String>,
}
