use super::DefineKind;
use crate::expr::index_kind::Index;
use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineIndexStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub what: Expr,
	pub cols: Vec<Expr>,
	pub index: Index,
	pub comment: Expr,
	pub concurrently: bool,
}

impl Default for DefineIndexStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			cols: Vec::new(),
			index: Index::Idx,
			comment: Expr::Literal(Literal::None),
			concurrently: false,
		}
	}
}
