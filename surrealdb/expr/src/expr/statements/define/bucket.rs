use super::DefineKind;
use crate::expr::permission::Permission;
use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineBucketStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub backend: Option<Expr>,
	pub permissions: Permission,
	pub readonly: bool,
	pub comment: Expr,
}

impl Default for DefineBucketStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			backend: None,
			permissions: Permission::default(),
			readonly: false,
			comment: Expr::Literal(Literal::None),
		}
	}
}
