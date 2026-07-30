use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveApiStatement {
	pub name: Expr,
	pub if_exists: bool,
}

impl Default for RemoveApiStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}
