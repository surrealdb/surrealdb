use super::cond::Cond;
use super::expression::Expr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum JoinKind {
	Inner,
	Left,
	Right,
	Cross,
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
