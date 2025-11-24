use std::fmt::{self, Display};

use super::DefineKind;
use super::config::api::ApiConfig;
use crate::catalog::ApiMethod;
use crate::fmt::{CoverStmts, Fmt, pretty_indent};
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

impl Display for DefineApiStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE API")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", CoverStmts(&self.path))?;
		let indent = pretty_indent();

		write!(f, " FOR any")?;
		{
			let indent = pretty_indent();

			write!(f, "{}", self.config)?;

			if let Some(fallback) = &self.fallback {
				write!(f, " THEN {}", CoverStmts(fallback))?;
			}

			drop(indent);
		}

		for action in &self.actions {
			write!(f, " {}", action)?;
		}

		if let Some(ref comment) = self.comment {
			write!(f, " COMMENT {}", CoverStmts(comment))?;
		}

		drop(indent);
		Ok(())
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

impl Display for ApiAction {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FOR {}", Fmt::comma_separated(self.methods.iter()))?;
		let _indent = pretty_indent();
		write!(f, "{}", self.config)?;
		write!(f, " THEN {}", self.action)?;
		Ok(())
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
