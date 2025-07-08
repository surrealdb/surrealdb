use crate::api::method::Method;
use crate::api::path::Path;
use crate::sql::Expr;
use crate::sql::fmt::{Fmt, pretty_indent};
use crate::val::Strand;
use std::fmt::{self, Display};

use super::DefineKind;
use super::config::api::ApiConfig;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineApiStatement {
	pub kind: DefineKind,
	pub path: Expr,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Expr>,
	pub config: Option<ApiConfig>,
	pub comment: Option<Strand>,
}

impl Display for DefineApiStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE API")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", self.path)?;
		let indent = pretty_indent();

		if self.config.is_some() || self.fallback.is_some() {
			write!(f, "FOR any")?;
			let indent = pretty_indent();

			if let Some(config) = &self.config {
				write!(f, "{}", config)?;
			}

			if let Some(fallback) = &self.fallback {
				write!(f, "THEN {}", fallback)?;
			}

			drop(indent);
		}

		for action in &self.actions {
			write!(f, "{}", action)?;
		}

		if let Some(ref comment) = self.comment {
			write!(f, " COMMENT {}", comment)?;
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
			config: v.config.map(Into::into),
			comment: v.comment.map(Into::into),
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
			config: v.config.map(Into::into),
			comment: v.comment.map(Into::into),
		}
	}
}

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ApiDefinition {
	pub id: Option<u32>,
	pub path: Path,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Expr>,
	pub config: Option<ApiConfig>,
	pub comment: Option<Strand>,
}

impl Display for ApiDefinition {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let da: DefineApiStatement = self.clone().into();
		da.fmt(f)
	}
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ApiAction {
	pub methods: Vec<Method>,
	pub action: Expr,
	pub config: Option<ApiConfig>,
}

impl Display for ApiAction {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FOR {}", Fmt::comma_separated(self.methods.iter()))?;
		let indent = pretty_indent();
		if let Some(config) = &self.config {
			write!(f, "{}", config)?;
		}
		write!(f, "THEN {}", self.action)?;
		Ok(())
	}
}

impl From<ApiAction> for crate::expr::statements::define::ApiAction {
	fn from(v: ApiAction) -> Self {
		crate::expr::statements::define::ApiAction {
			methods: v.methods,
			action: v.action.into(),
			config: v.config.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::define::ApiAction> for ApiAction {
	fn from(v: crate::expr::statements::define::ApiAction) -> Self {
		ApiAction {
			methods: v.methods,
			action: v.action.into(),
			config: v.config.map(Into::into),
		}
	}
}
