use common::fmt::Fmt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use super::config::api::ApiConfig;
use crate::{ApiMethod, CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineApiStatement {
	pub kind: DefineKind,
	pub path: Expr,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Expr>,
	pub config: ApiConfig,
	pub comment: Expr,
}

impl Default for DefineApiStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			path: Expr::Literal(Literal::None),
			actions: Vec::new(),
			fallback: None,
			config: ApiConfig::default(),
			comment: Expr::Literal(Literal::None),
		}
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

		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ApiAction {
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary::atleast_one))]
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
