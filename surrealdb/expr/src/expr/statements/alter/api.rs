use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::expr::statements::define::ApiAction;
use crate::expr::statements::define::config::api::ApiConfig;
use crate::expr::{Expr, Literal};
use crate::sql::ApiMethod;

/// A single `FOR` clause within an `ALTER API` statement.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum AlterApiClause {
	/// `FOR any [config] [THEN expr | DROP THEN]`
	ForAny {
		config: Option<ApiConfig>,
		fallback: AlterKind<Expr>,
	},
	/// `FOR method1, method2 [config] THEN expr`
	SetAction(ApiAction),
	/// `FOR method1, method2 DROP THEN`
	DropAction {
		methods: Vec<ApiMethod>,
	},
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterApiStatement {
	pub path: Expr,
	pub if_exists: bool,
	pub clauses: Vec<AlterApiClause>,
	pub comment: AlterKind<String>,
}

impl Default for AlterApiStatement {
	fn default() -> Self {
		Self {
			path: Expr::Literal(Literal::None),
			if_exists: false,
			clauses: Vec::new(),
			comment: AlterKind::None,
		}
	}
}

impl ToSql for AlterApiStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterApiStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
