use std::fmt::{self, Display};

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use super::config::api::ApiConfig;
use crate::catalog::ApiMethod;
use crate::fmt::{Fmt, pretty_indent};
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
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, "DEFINE API");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, " IF NOT EXISTS"),
		}
		write_sql!(f, " {}", self.path);
		let indent = pretty_indent();

		write_sql!(f, " FOR any");
		{
			let indent = pretty_indent();

			write_sql!(f, "{}", self.config);

			if let Some(fallback) = &self.fallback {
				write_sql!(f, " THEN {}", fallback);
			}

			drop(indent);
		}

		for action in &self.actions {
			write_sql!(f, " {}", action);
		}

		if let Some(ref comment) = self.comment {
			write_sql!(f, " COMMENT {}", comment);
		}

		drop(indent);
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
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, "FOR {}", Fmt::comma_separated(self.methods.iter()));
		if fmt.is_pretty() {
			f.push('\n');
			let inner_fmt = fmt.increment();
			inner_fmt.write_indent(f);
		} else {
			f.push(' ');
		}
		self.config.fmt_sql(f, fmt);
		f.push_str(" THEN ");
		self.action.fmt_sql(f, fmt);
	}
}

impl Display for ApiAction {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		use surrealdb_types::ToSql;
		write!(f, "{}", self.to_sql())
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
