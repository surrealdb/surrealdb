use crate::buc::backend::BucketBackend;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{pretty_indent, pretty_sequence_item};
use crate::sql::{Base, Ident, Permissions, Value};
use crate::{ctx::Context, sql::statements::info::InfoStructure};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use url::Url;

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
	pub metadata: Option<Value>,
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
		let backend = if let Some(ref v) = self.backend {
			let v = v.compute(stk, ctx, opt, doc).await?.coerce_to_string()?;
			let v = Url::parse(&v).map_err(|_| Error::Unreachable("Invalid backend URL".into()))?;

			if !matches!(v.scheme(), "memory" | "file")
				&& !crate::ent::file::backend_allowed(v.scheme(), false)
			{
				return Err(Error::Unreachable("bla".into()));
			}

			Some(v)
		} else {
			None
		};

		// Process the statement
		let name = self.name.to_string();
		let key = crate::key::database::bu::new(ns, db, &name);
		txn.get_or_add_ns(ns, opt.strict).await?;
		txn.get_or_add_db(ns, db, opt.strict).await?;
		let ap = BucketDefinition {
			// Don't persist the `IF NOT EXISTS` clause to schema
			name: self.name.clone(),
			backend,
			permissions: self.permissions.clone(),
			metadata: self.metadata.clone(),
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

		let indent = pretty_indent();

		write!(f, " PERMISSIONS {}", self.permissions)?;

		if let Some(ref backend) = self.backend {
			pretty_sequence_item();
			write!(f, " BACKEND {}", Value::from(backend.to_string()))?;
		}

		if let Some(ref metadata) = self.metadata {
			pretty_sequence_item();
			write!(f, " METADATA {}", metadata)?;
		}

		drop(indent);
		Ok(())
	}
}

impl InfoStructure for DefineBucketStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"backend".to_string(), if let Some(backend) = self.backend => backend.structure(),
			"metadata".to_string(), if let Some(metadata) = self.metadata => metadata.structure(),
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
	pub backend: Option<Url>,
	pub permissions: Permissions,
	pub metadata: Option<Value>,
}

impl From<BucketDefinition> for DefineBucketStatement {
	fn from(value: BucketDefinition) -> Self {
		DefineBucketStatement {
			if_not_exists: false,
			overwrite: false,
			name: value.name,
			backend: value.backend.map(|v| v.to_string().into()),
			permissions: value.permissions,
			metadata: value.metadata,
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
