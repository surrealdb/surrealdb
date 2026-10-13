use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefaultConfig {
	pub namespace: Expr,
	pub database: Expr,
}

impl Default for DefaultConfig {
	fn default() -> Self {
		Self {
			namespace: Expr::Literal(Literal::None),
			database: Expr::Literal(Literal::None),
		}
	}
}
