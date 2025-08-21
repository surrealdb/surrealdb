use std::fmt;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::config::api::{ApiConfig, ApiConfigStore};
use super::{CursorDoc, DefineKind};
use crate::api::method::Method;
use crate::api::path::Path;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::fmt::{Fmt, pretty_indent};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Base, Expr, FlowResultExt as _, Value};
use crate::iam::{Action, ResourceKind};
use crate::kvs::impl_kv_value_revisioned;
use crate::val::{Object, Strand};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct DefineApiStatement {
	pub kind: DefineKind,
	pub path: Expr,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Expr>,
	pub config: ApiConfig,
	pub comment: Option<Strand>,
}

impl DefineApiStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Api, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		// Check if the definition exists
		if txn.get_db_api(ns, db, &self.path.to_string()).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::ApAlreadyExists {
							value: self.path.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}
		}

		let path = stk.run(|stk| self.path.compute(stk, ctx, opt, doc)).await.catch_return()?;
		// Process the statement
		let path: Path = path.coerce_to::<String>()?.parse()?;
		let name = path.to_string();

		let config = self.config.compute(stk, ctx, opt, doc).await?;

		let key = crate::key::database::ap::new(ns, db, &name);
		let mut actions = Vec::new();
		for action in self.actions.iter() {
			actions.push(ApiActionStore {
				methods: action.methods.clone(),
				action: action.action.clone(),
				config: action.config.compute(stk, ctx, opt, doc).await?,
			});
		}

		let ap = ApiDefinition {
			// Don't persist the `IF NOT EXISTS` clause to schema
			path,
			actions,
			fallback: self.fallback.clone(),
			config,
			comment: self.comment.clone(),
		};
		txn.set(&key, &ap, None).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl fmt::Display for DefineApiStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE API")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", self.path)?;
		let indent = pretty_indent();

		write!(f, "FOR any")?;
		{
			let indent = pretty_indent();

			write!(f, "{}", self.config)?;

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

impl InfoStructure for DefineApiStatement {
	fn structure(self) -> Value {
		Value::from(Object(map! {
			// TODO: Null byte validity
			"path".to_string() => Strand::new(self.path.to_string()).unwrap().into(),
			"config".to_string() => self.config.structure(),
			"fallback".to_string(), if let Some(fallback) = self.fallback => fallback.structure(),
			"actions".to_string() => Value::from(self.actions.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
			"comment".to_string(), if let Some(comment) = self.comment => comment.into(),
		}))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ApiDefinition {
	pub path: Path,
	pub actions: Vec<ApiActionStore>,
	pub fallback: Option<Expr>,
	pub config: ApiConfigStore,
	pub comment: Option<Strand>,
}

impl_kv_value_revisioned!(ApiDefinition);

impl ApiDefinition {
	/// Finds the api definition which most closely matches the segments of the
	/// path.
	pub fn find_definition<'a>(
		definitions: &'a [ApiDefinition],
		segments: Vec<&str>,
		method: Method,
	) -> Option<(&'a ApiDefinition, Object)> {
		let mut specificity = 0;
		let mut res = None;
		for api in definitions.iter() {
			if let Some(params) = api.path.fit(segments.as_slice()) {
				if api.fallback.is_some() || api.actions.iter().any(|x| x.methods.contains(&method))
				{
					let s = api.path.specificity();
					if s > specificity {
						specificity = s;
						res = Some((api, params));
					}
				}
			}
		}

		res
	}
}

impl InfoStructure for ApiDefinition {
	fn structure(self) -> Value {
		Value::from(Object(map! {
			// TODO: Null byte validity
			"path".to_string() => Strand::new(self.path.to_string()).unwrap().into(),
			"config".to_string() => self.config.structure(),
			"fallback".to_string(), if let Some(fallback) = self.fallback => fallback.structure(),
			"actions".to_string() => Value::from(self.actions.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
			"comment".to_string(), if let Some(comment) = self.comment => comment.into(),
		}))
	}
}

impl fmt::Display for ApiDefinition {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE API")?;
		write!(f, " {}", self.path)?;
		let indent = pretty_indent();

		write!(f, "FOR any")?;
		{
			let indent = pretty_indent();

			write!(f, "{}", self.config)?;

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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ApiAction {
	pub methods: Vec<Method>,
	pub action: Expr,
	pub config: ApiConfig,
}

impl fmt::Display for ApiAction {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FOR {}", Fmt::comma_separated(self.methods.iter()))?;
		let indent = pretty_indent();
		write!(f, "{}", &self.config)?;
		write!(f, "THEN {}", self.action)?;
		drop(indent);
		Ok(())
	}
}

impl InfoStructure for ApiAction {
	fn structure(self) -> Value {
		Value::from(map!(
			"methods" => Value::from(self.methods.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
			"action" => Value::from(self.action.to_string()),
			"config" => self.config.structure(),
		))
	}
}

/// The ApiAction as it is stored in the KV.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ApiActionStore {
	pub methods: Vec<Method>,
	pub action: Expr,
	pub config: ApiConfigStore,
}

impl fmt::Display for ApiActionStore {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FOR {}", Fmt::comma_separated(self.methods.iter()))?;
		let indent = pretty_indent();
		write!(f, "{}", &self.config)?;
		write!(f, "THEN {}", self.action)?;
		drop(indent);
		Ok(())
	}
}

impl InfoStructure for ApiActionStore {
	fn structure(self) -> Value {
		Value::from(map!(
			"methods" => Value::from(self.methods.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
			"action" => Value::from(self.action.to_string()),
			"config" => self.config.structure(),
		))
	}
}
