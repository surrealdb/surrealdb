use crate::api::method::Method;
use crate::api::path::Path;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::fmt::{Fmt, pretty_indent};
use crate::sql::{Base, FlowResultExt as _, Object, Strand, SqlValue};
use crate::iam::{Action, ResourceKind};
use crate::{ctx::Context};
use anyhow::{Result, bail};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

use super::CursorDoc;
use super::config::api::ApiConfig;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineApiStatement {
	pub if_not_exists: bool,
	pub overwrite: bool,
	pub path: SqlValue,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<SqlValue>,
	pub config: Option<ApiConfig>,
	#[revision(start = 2)]
	pub comment: Option<Strand>,
}

impl Display for DefineApiStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE API")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
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
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
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
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			path: v.path.into(),
			actions: v.actions.into_iter().map(Into::into).collect(),
			fallback: v.fallback.map(Into::into),
			config: v.config.map(Into::into),
			comment: v.comment.map(Into::into),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[non_exhaustive]
pub struct ApiDefinition {
	pub id: Option<u32>,
	pub path: Path,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<SqlValue>,
	pub config: Option<ApiConfig>,
	pub comment: Option<Strand>,
}

impl From<ApiDefinition> for DefineApiStatement {
	fn from(value: ApiDefinition) -> Self {
		DefineApiStatement {
			if_not_exists: false,
			overwrite: false,
			path: value.path.to_string().into(),
			actions: value.actions,
			fallback: value.fallback,
			config: value.config,
			comment: value.comment,
		}
	}
}



impl Display for ApiDefinition {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let da: DefineApiStatement = self.clone().into();
		da.fmt(f)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ApiAction {
	pub methods: Vec<Method>,
	pub action: SqlValue,
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
		drop(indent);
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
