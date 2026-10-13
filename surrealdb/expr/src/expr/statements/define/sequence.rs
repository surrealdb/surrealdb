use super::DefineKind;
use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineSequenceStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub batch: Expr,
	pub start: Expr,
	pub timeout: Expr,
}

impl Default for DefineSequenceStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			batch: Expr::Literal(Literal::Integer(0)),
			start: Expr::Literal(Literal::Integer(0)),
			timeout: Expr::Literal(Literal::None),
		}
	}
}
