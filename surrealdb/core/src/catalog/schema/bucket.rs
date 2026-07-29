use anyhow::Context as _;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{FromStored, Permission, StoredPermission};
use crate::expr::statements::info::InfoStructure;
use crate::key::impl_kv_value_revisioned;
use crate::sql;
use crate::sql::statements::define::{DefineBucketStatement, DefineKind};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct BucketId(pub u32);

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct StoredBucketDefinition {
	pub(crate) id: Option<BucketId>,
	pub(crate) name: Strand,
	pub(crate) backend: Option<Strand>,
	pub(crate) permissions: StoredPermission,
	pub(crate) readonly: bool,
	pub(crate) comment: Option<String>,
}
impl_kv_value_revisioned!(StoredBucketDefinition);

impl BucketDefinition {
	fn to_sql_definition(&self) -> DefineBucketStatement {
		DefineBucketStatement {
			kind: DefineKind::Default,
			name: sql::Expr::Idiom(sql::Idiom::field(self.name.clone())),
			backend: self.backend.clone().map(|v| sql::Expr::Literal(sql::Literal::String(v))),
			permissions: self.permissions.to_sql_permission(),
			readonly: self.readonly,
			comment: self
				.comment
				.clone()
				.map(|v| sql::Expr::Literal(sql::Literal::String(v.into())))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
		}
	}
}

impl InfoStructure for BucketDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => self.name.into(),
			"permissions" => self.permissions.structure(),
			"backend", if let Some(backend) = self.backend => backend.into(),
			"readonly" => self.readonly.into(),
			"comment", if let Some(comment) = self.comment => comment.into(),
		})
	}
}

impl ToSql for BucketDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

/// Runtime form of [`StoredBucketDefinition`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BucketDefinition {
	pub id: Option<BucketId>,
	pub readonly: bool,
	pub name: Strand,
	pub backend: Option<Strand>,
	pub permissions: Permission,
	pub comment: Option<String>,
}

impl FromStored for BucketDefinition {
	type Stored = StoredBucketDefinition;

	fn from_stored(stored: &StoredBucketDefinition) -> anyhow::Result<BucketDefinition> {
		fn build(stored: &StoredBucketDefinition) -> anyhow::Result<BucketDefinition> {
			Ok(BucketDefinition {
				id: stored.id,
				readonly: stored.readonly,
				name: stored.name.clone(),
				backend: stored.backend.clone(),
				permissions: Permission::from_stored(&stored.permissions)?,
				comment: stored.comment.clone(),
			})
		}
		build(stored).with_context(|| {
			format!("the stored definition of bucket `{}` no longer compiles", stored.name)
		})
	}
}

impl BucketDefinition {
	pub(crate) fn to_stored(&self) -> StoredBucketDefinition {
		StoredBucketDefinition {
			id: self.id,
			name: self.name.clone(),
			backend: self.backend.clone(),
			permissions: self.permissions.to_stored(),
			readonly: self.readonly,
			comment: self.comment.clone(),
		}
	}
}
