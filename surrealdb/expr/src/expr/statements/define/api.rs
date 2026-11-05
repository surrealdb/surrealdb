use common::fmt::Fmt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use super::config::api::ApiConfig;
use crate::expr::{ApiMethod, ApiSecurity, CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineApiStatement {
	pub kind: DefineKind,
	pub path: Expr,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Expr>,
	pub config: ApiConfig,
	pub security: ApiSecurity,
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
		f.push_str("FOR ");
		f.push_str(&Fmt::comma_separated(self.methods.iter()).to_sql());
		if sql_fmt.is_pretty() {
			f.push('\n');
			let inner_fmt = sql_fmt.increment();
			inner_fmt.write_indent(f);
		}
		write_sql!(f, sql_fmt, "{} THEN {}", self.config, self.action);
	}
}

impl ToSql for DefineApiStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "DEFINE API");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, sql_fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, sql_fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, sql_fmt, " {}", CoverStmts(&self.path));
		let sql_fmt = sql_fmt.increment();

		write_sql!(f, sql_fmt, " FOR any");
		{
			let sql_fmt = sql_fmt.increment();

			write_sql!(f, sql_fmt, "{}", self.config);

			if let Some(fallback) = &self.fallback {
				write_sql!(f, sql_fmt, " THEN {}", CoverStmts(fallback));
			}
		}

		for action in &self.actions {
			write_sql!(f, sql_fmt, " {}", action);
		}

		// The default is left unwritten.
		if self.security != ApiSecurity::Definer {
			write_sql!(f, sql_fmt, " SECURITY {}", self.security);
		}

		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
	}
}
