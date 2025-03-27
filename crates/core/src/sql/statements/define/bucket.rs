use crate::buc;
use crate::dbs::capabilities::ExperimentalTarget;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Permissions, Strand, Value};
use crate::{ctx::Context, sql::statements::info::InfoStructure};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

use super::CursorDoc;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineBucketStatement {
	pub if_not_exists: bool,
	pub overwrite: bool,
	pub name: Ident,
	pub backend: Option<Value>,
	pub permissions: Permissions,
	pub readonly: bool,
	pub comment: Option<Strand>,
}

impl DefineBucketStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		if !ctx.get_capabilities().allows_experimental(&ExperimentalTarget::Files) {
			return Err(Error::Unreachable("Experimental files capability is not enabled".into()));
		}

		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Api, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = (opt.ns()?, opt.db()?);
		// Check if the definition exists
		if txn.get_db_bucket(ns, db, &self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::BuAlreadyExists {
					value: self.name.to_string(),
				});
			}
		}
		// Process the backend input
		let backend = if let Some(ref url) = self.backend {
			Some(url.compute(stk, ctx, opt, doc).await?.coerce_to_string()?)
		} else {
			None
		};

		// Validate the store
		let name = self.name.to_string();
		let store = if let Some(ref backend) = backend {
			buc::connect(backend, false, self.readonly)?
		} else {
			buc::connect_global(ns, db, &name)?
		};

		// Persist the store to cache
		if let Some(buckets) = ctx.get_buckets() {
			buckets.insert((ns.to_string(), db.to_string(), name.clone()), store);
		}

		// Process the statement
		let key = crate::key::database::bu::new(ns, db, &name);
		txn.get_or_add_ns(ns, opt.strict).await?;
		txn.get_or_add_db(ns, db, opt.strict).await?;
		let ap = BucketDefinition {
			name: self.name.clone(),
			backend,
			permissions: self.permissions.clone(),
			readonly: self.readonly,
			comment: self.comment.clone(),
			..Default::default()
		};
		txn.set(key, revision::to_vec(&ap)?, None).await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineBucketStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE BUCKET")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {}", self.name)?;

		if self.readonly {
			write!(f, " READONLY")?;
		}

		if let Some(ref backend) = self.backend {
			write!(f, " BACKEND {}", backend)?;
		}

		write!(f, " PERMISSIONS {}", self.permissions)?;

		if let Some(ref comment) = self.comment {
			write!(f, " COMMENT {}", comment)?;
		}

		Ok(())
	}
}

impl InfoStructure for DefineBucketStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"backend".to_string(), if let Some(backend) = self.backend => backend,
			"readonly".to_string() => self.readonly.into(),
			"comment".to_string(), if let Some(comment) = self.comment => comment.into(),
		})
	}
}

// Computed bucket definition struct

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[non_exhaustive]
pub struct BucketDefinition {
	pub id: Option<u32>,
	pub name: Ident,
	pub backend: Option<String>,
	pub permissions: Permissions,
	pub readonly: bool,
	pub comment: Option<Strand>,
}

impl From<BucketDefinition> for DefineBucketStatement {
	fn from(value: BucketDefinition) -> Self {
		DefineBucketStatement {
			if_not_exists: false,
			overwrite: false,
			name: value.name,
			backend: value.backend.map(|v| v.into()),
			permissions: value.permissions,
			readonly: value.readonly,
			comment: value.comment,
		}
	}
}

impl InfoStructure for BucketDefinition {
	fn structure(self) -> Value {
		let db: DefineBucketStatement = self.into();
		db.structure()
	}
}

impl Display for BucketDefinition {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let db: DefineBucketStatement = self.clone().into();
		db.fmt(f)
	}
}
