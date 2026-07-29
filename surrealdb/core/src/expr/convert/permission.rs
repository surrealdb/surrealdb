//! `sql` -> `expr` conversions for [`crate::sql::permission`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::permission::*;

impl From<crate::catalog::Permission> for Permission {
	fn from(v: crate::catalog::Permission) -> Self {
		match v {
			crate::catalog::Permission::None => Self::None,
			crate::catalog::Permission::Full => Self::Full,
			crate::catalog::Permission::Specific(expr) => Self::Specific(expr.into()),
		}
	}
}

/// Plain lowering, with no rendering step: a guard in a statement is an
/// expression, and only becomes text when the definition it belongs to is
/// written. Statements hold this form so that an expr-layer pass walks the
/// guard directly instead of parsing text and rendering it back.
impl From<Permission> for crate::catalog::Permission {
	fn from(v: Permission) -> Self {
		match v {
			Permission::None => Self::None,
			Permission::Full => Self::Full,
			Permission::Specific(v) => Self::Specific(crate::expr::Expr::from(v)),
		}
	}
}

impl From<Permissions> for crate::catalog::Permissions {
	fn from(v: Permissions) -> Self {
		Self {
			select: v.select.into(),
			create: v.create.into(),
			update: v.update.into(),
			delete: v.delete.into(),
		}
	}
}

impl From<crate::catalog::Permissions> for Permissions {
	fn from(v: crate::catalog::Permissions) -> Self {
		Self {
			select: v.select.into(),
			create: v.create.into(),
			update: v.update.into(),
			delete: v.delete.into(),
		}
	}
}
