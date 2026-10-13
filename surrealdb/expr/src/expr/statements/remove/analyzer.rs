use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveAnalyzerStatement {
	pub name: Expr,
	pub if_exists: bool,
}

impl Default for RemoveAnalyzerStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}
