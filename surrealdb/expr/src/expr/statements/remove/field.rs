use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveFieldStatement {
	pub name: Expr,
	pub table_name: Expr,
	pub if_exists: bool,
}

impl Default for RemoveFieldStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			table_name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}
