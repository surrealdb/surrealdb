use crate::api::method::Method;
use crate::api::path::Path;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{pretty_indent, Fmt};
use crate::sql::{Base, Ident, Object, Permission, Value};
use crate::{ctx::Context, sql::statements::info::InfoStructure};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

use super::config::api::ApiConfig;
use super::CursorDoc;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineBucketStatement {
	pub id: Option<u32>,
	pub if_not_exists: bool,
	pub overwrite: bool,
	pub name: Ident,
	pub permissions: Permission,
	pub metadata: Value,
}

impl DefineBucketStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Api, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = (opt.ns()?, opt.db()?);
		// Check if the definition exists
		if txn.get_db_api(ns, db, &self.path.to_string()).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::ApAlreadyExists {
					value: self.path.to_string(),
				});
			}
		}
		// Process the statement
		let name = self.name.to_string();
		let key = crate::key::database::ap::new(ns, db, &name);
		txn.get_or_add_ns(ns, opt.strict).await?;
		txn.get_or_add_db(ns, db, opt.strict).await?;
		let ap = DefineBucketStatement {
			// Don't persist the `IF NOT EXISTS` clause to schema
			if_not_exists: false,
			overwrite: false,
			..Default::default()
		};
		txn.set(key, revision::to_vec(&ap)?, None).await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
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
		if let Some(config) = &self.config {
			write!(f, "{}", config)?;
		}

		if let Some(fallback) = &self.fallback {
			write!(f, "FOR any {}", fallback)?;
		}

		for action in &self.actions {
			write!(f, "{}", action)?;
		}

		drop(indent);
		Ok(())
	}
}

impl InfoStructure for DefineApiStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"path".to_string() => Value::from(self.path.to_string()),
			"config".to_string(), if let Some(config) = self.config => config.structure(),
			"fallback".to_string(), if let Some(fallback) = self.fallback => fallback.structure(),
			"actions".to_string() => Value::from(self.actions.into_iter().map(InfoStructure::structure).collect::<Vec<Value>>()),
		})
	}
}
