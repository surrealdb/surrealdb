pub mod graphql;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ConfigKind, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Base, Value};
use derive::Store;
use graphql::GraphQLConfig;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineConfigStatement {
	pub inner: ConfigInner,
	pub if_not_exists: bool,
	pub overwrite: bool,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum ConfigInner {
	GraphQL(GraphQLConfig),
}

impl DefineConfigStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Config(ConfigKind::GraphQL), &Base::Db)?;
		// get transaction
		let txn = ctx.tx();

		// check if already defined
		if txn.get_db_config(opt.ns()?, opt.db()?, "graphql").await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::CgAlreadyExists {
					value: "graphql".to_string(),
				});
			}
		}

		let key = crate::key::database::cg::new(opt.ns()?, opt.db()?, "graphql");
		txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
		txn.get_or_add_db(opt.ns()?, opt.db()?, opt.strict).await?;
		txn.set(key, self.clone(), None).await?;

		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl ConfigInner {
	pub fn name(&self) -> String {
		ConfigKind::from(self).to_string()
	}

	pub fn try_into_graphql(self) -> Result<GraphQLConfig, Error> {
		match self {
			ConfigInner::GraphQL(g) => Ok(g),
			c => Err(fail!("found {c} when a graphql config was expected")),
		}
	}
}

impl From<ConfigInner> for ConfigKind {
	fn from(value: ConfigInner) -> Self {
		value.into()
	}
}

impl From<&ConfigInner> for ConfigKind {
	fn from(value: &ConfigInner) -> Self {
		match value {
			ConfigInner::GraphQL(_) => ConfigKind::GraphQL,
		}
	}
}

impl InfoStructure for DefineConfigStatement {
	fn structure(self) -> Value {
		match self.inner {
			ConfigInner::GraphQL(v) => Value::from(map!(
				"graphql" => v.structure()
			)),
		}
	}
}

impl Display for DefineConfigStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE CONFIG")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}

		Ok(())
	}
}

impl Display for ConfigInner {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match &self {
			ConfigInner::GraphQL(v) => Display::fmt(v, f),
		}
	}
}
