use crate::expr::{Base, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveAccessStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
}

impl Default for RemoveAccessStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::default(),
			if_exists: false,
		}
	}
}
