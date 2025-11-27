use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use super::config::api::ApiConfig;
use crate::catalog::ApiMethod;
use crate::fmt::Fmt;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineApiStatement {
	pub kind: DefineKind,
	pub path: Expr,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Expr>,
	pub config: ApiConfig,
	pub comment: Option<Expr>,
}

impl Default for DefineApiStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			path: Expr::Literal(Literal::None),
			actions: Vec::new(),
			fallback: None,
			config: ApiConfig::default(),
			comment: None,
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
		write_sql!(f, sql_fmt, " {}", self.path);
		let sql_fmt = sql_fmt.increment();

		write_sql!(f, sql_fmt, " FOR any");
		{
			let sql_fmt = sql_fmt.increment();

			write_sql!(f, sql_fmt, "{}", self.config);

			if let Some(fallback) = &self.fallback {
				write_sql!(f, sql_fmt, " THEN {}", fallback);
			}
		}

		for action in &self.actions {
			write_sql!(f, sql_fmt, " {}", action);
		}

		if let Some(ref comment) = self.comment {
			write_sql!(f, sql_fmt, " COMMENT {}", comment);
		}
	}
}

impl From<DefineApiStatement> for crate::expr::statements::DefineApiStatement {
	fn from(v: DefineApiStatement) -> Self {
		crate::expr::statements::DefineApiStatement {
			kind: v.kind.into(),
			path: v.path.into(),
			actions: v.actions.into_iter().map(Into::into).collect(),
			fallback: v.fallback.map(Into::into),
			config: v.config.into(),
			comment: v.comment.map(|x| x.into()),
		}
	}
}

impl From<crate::expr::statements::DefineApiStatement> for DefineApiStatement {
	fn from(v: crate::expr::statements::DefineApiStatement) -> Self {
		DefineApiStatement {
			kind: v.kind.into(),
			path: v.path.into(),
			actions: v.actions.into_iter().map(Into::into).collect(),
			fallback: v.fallback.map(Into::into),
			config: v.config.into(),
			comment: v.comment.map(|x| x.into()),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct ApiAction {
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
		} else {
			f.push(' ');
		}
		write_sql!(f, sql_fmt, "{} THEN {}", self.config, self.action);
	}
}

impl From<ApiAction> for crate::expr::statements::define::ApiAction {
	fn from(v: ApiAction) -> Self {
		crate::expr::statements::define::ApiAction {
			methods: v.methods,
			action: v.action.into(),
			config: v.config.into(),
		}
	}
}

impl From<crate::expr::statements::define::ApiAction> for ApiAction {
	fn from(v: crate::expr::statements::define::ApiAction) -> Self {
		ApiAction {
			methods: v.methods,
			action: v.action.into(),
			config: v.config.into(),
		}
	}
}
