use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use super::config::api::ApiConfig;
use crate::expr::Expr;
use crate::sql::ApiMethod;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineApiStatement {
	pub kind: DefineKind,
	pub path: Expr,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Expr>,
	pub config: ApiConfig,
	pub comment: Expr,
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ApiAction {
	pub methods: Vec<ApiMethod>,
	pub action: Expr,
	pub config: ApiConfig,
}

impl ToSql for ApiAction {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let stmt: crate::sql::statements::define::ApiAction = self.clone().into();
		stmt.fmt_sql(f, sql_fmt);
	}
}
