use anyhow::Context as _;
use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{FromStored, Permission, StoredPermission};
use crate::expr::statements::info::InfoStructure;
use crate::key::impl_kv_value_revisioned;
use crate::sql;
use crate::sql::statements::define::DefineKind;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct StoredMlModelDefinition {
	pub hash: Strand,
	pub name: Strand,
	pub version: Strand,
	pub comment: Option<String>,
	pub(crate) permissions: StoredPermission,
}

impl_kv_value_revisioned!(StoredMlModelDefinition);

impl MlModelDefinition {
	fn to_sql_definition(&self) -> sql::DefineModelStatement {
		sql::DefineModelStatement {
			kind: DefineKind::Default,
			hash: self.hash.clone(),
			name: self.name.clone().into(),
			version: self.version.clone(),
			permissions: self.permissions.to_sql_permission(),
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x.into())))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
		}
	}
}

impl InfoStructure for MlModelDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => self.name.into(),
			"version" => self.version.into(),
			"permissions" => self.permissions.structure(),
			"comment", if let Some(v) = self.comment => v.into(),
		})
	}
}

impl ToSql for MlModelDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

/// Runtime form of [`StoredMlModelDefinition`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MlModelDefinition {
	pub name: Strand,
	pub hash: Strand,
	pub version: Strand,
	pub permissions: Permission,
	pub comment: Option<String>,
}

impl FromStored for MlModelDefinition {
	type Stored = StoredMlModelDefinition;

	fn from_stored(stored: &StoredMlModelDefinition) -> anyhow::Result<MlModelDefinition> {
		fn build(stored: &StoredMlModelDefinition) -> anyhow::Result<MlModelDefinition> {
			Ok(MlModelDefinition {
				name: stored.name.clone(),
				hash: stored.hash.clone(),
				version: stored.version.clone(),
				permissions: Permission::from_stored(&stored.permissions)?,
				comment: stored.comment.clone(),
			})
		}
		build(stored).with_context(|| {
			format!(
				"the stored definition of model `ml::{}<{}>` no longer compiles",
				stored.name, stored.version
			)
		})
	}
}

impl MlModelDefinition {
	pub(crate) fn to_stored(&self) -> StoredMlModelDefinition {
		StoredMlModelDefinition {
			hash: self.hash.clone(),
			name: self.name.clone(),
			version: self.version.clone(),
			comment: self.comment.clone(),
			permissions: self.permissions.to_stored(),
		}
	}
}
