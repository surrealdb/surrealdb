use anyhow::Context as _;
use revision::revisioned;
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{FromStored, LogicalPermissionBody, Permission, StoredPermission};
use crate::expr::logical::LogicalPermission;
use crate::expr::statements::info::InfoStructure;
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
	pub permissions: StoredPermission,
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
///
/// `permissions` and `permissions_body` name the same clause; a constructor
/// sets both from one source ([`LogicalPermissionBody::of`] for a `DEFINE`,
/// the stored text in `from_stored`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MlModelDefinition {
	pub name: Strand,
	pub hash: Strand,
	pub version: Strand,
	pub permissions: Permission,
	/// The `PERMISSIONS` clause in logical form; see
	/// [`Self::permissions_logical`].
	pub permissions_body: LogicalPermissionBody,
	pub comment: Option<String>,
}

impl MlModelDefinition {
	/// The `PERMISSIONS` clause in logical form, compiling its guard on first
	/// call.
	pub fn permissions_logical(&self) -> anyhow::Result<LogicalPermission> {
		self.permissions_body.logical()
	}
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
				permissions_body: LogicalPermissionBody::from_stored(&stored.permissions),
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
	pub fn to_stored(&self) -> StoredMlModelDefinition {
		StoredMlModelDefinition {
			hash: self.hash.clone(),
			name: self.name.clone(),
			version: self.version.clone(),
			comment: self.comment.clone(),
			permissions: crate::catalog::StoredPermission::from_runtime(&self.permissions),
		}
	}
}
