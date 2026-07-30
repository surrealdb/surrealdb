use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveTableStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub expunge: bool,
}

impl Default for RemoveTableStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			expunge: false,
		}
	}
}
