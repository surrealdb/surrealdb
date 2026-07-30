use super::DefineKind;
use crate::expr::Expr;
use crate::sql::EventKind;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineEventStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub target_table: Expr,
	pub when: Expr,
	pub then: Vec<Expr>,
	pub comment: Expr,
	pub event_kind: EventKind,
}
