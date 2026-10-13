//! `sql` -> `expr` conversions for [`crate::sql::field`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::field::*;

impl From<Fields> for crate::expr::field::Fields {
	fn from(v: Fields) -> Self {
		match v {
			Fields::Value(x) => crate::expr::field::Fields::Value(Box::new((*x).into())),
			Fields::Select(x) => {
				crate::expr::field::Fields::Select(x.into_iter().map(From::from).collect())
			}
		}
	}
}

impl From<crate::expr::field::Fields> for Fields {
	fn from(v: crate::expr::field::Fields) -> Self {
		match v {
			crate::expr::field::Fields::Value(x) => Fields::Value(Box::new((*x).into())),
			crate::expr::field::Fields::Select(x) => {
				Fields::Select(x.into_iter().map(From::from).collect())
			}
		}
	}
}

impl From<Field> for crate::expr::field::Field {
	fn from(v: Field) -> Self {
		match v {
			Field::All => Self::All,
			Field::Single(s) => crate::expr::field::Field::Single(s.into()),
		}
	}
}

impl From<crate::expr::field::Field> for Field {
	fn from(v: crate::expr::field::Field) -> Self {
		match v {
			crate::expr::field::Field::All => Self::All,
			crate::expr::field::Field::Single(s) => Self::Single(s.into()),
		}
	}
}

impl From<Selector> for crate::expr::field::Selector {
	fn from(v: Selector) -> Self {
		crate::expr::field::Selector {
			expr: v.expr.into(),
			alias: v.alias.map(Into::into),
		}
	}
}

impl From<crate::expr::field::Selector> for Selector {
	fn from(v: crate::expr::field::Selector) -> Self {
		Selector {
			expr: v.expr.into(),
			alias: v.alias.map(Into::into),
		}
	}
}
