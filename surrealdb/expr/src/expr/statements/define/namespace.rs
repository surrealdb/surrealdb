use surrealdb_strand::Strand;

use super::DefineKind;
use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineNamespaceStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub comment: Expr,
}

impl Default for DefineNamespaceStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::String(Strand::default())),
			comment: Expr::Literal(Literal::None),
		}
	}
}
