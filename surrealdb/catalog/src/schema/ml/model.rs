use anyhow::Context as _;
use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{FromStored, LogicalPermissionBody, Permission, StoredPermission};
use crate::expr::logical::LogicalPermission;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::info::InfoStructure;
use crate::stored::Stored;
use crate::val::Value;
use crate::{expr as runtime, impl_kv_value_catalog};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredMlModelDefinition {
	#[stored(n = 2)]
	pub hash: Strand,
	#[stored(n = 4)]
	pub name: Strand,
	#[stored(n = 6)]
	pub version: Strand,
	#[stored(
		n = 1,
		ignorable,
		independent = "a comment is text for people and no other field reads it"
	)]
	pub comment: Option<String>,
	#[stored(n = 8)]
	pub permissions: StoredPermission,
}

impl_kv_value_catalog!(StoredMlModelDefinition);

impl MlModelDefinition {
	fn to_runtime_definition(&self) -> runtime::statements::DefineModelStatement {
		runtime::statements::DefineModelStatement {
			kind: DefineKind::Default,
			hash: self.hash.clone(),
			name: self.name.clone(),
			version: self.version.clone(),
			permissions: self.permissions.clone(),
			comment: self
				.comment
				.clone()
				.map(|x| runtime::Expr::Literal(runtime::Literal::String(x.into())))
				.unwrap_or(runtime::Expr::Literal(runtime::Literal::None)),
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
		self.to_runtime_definition().fmt_sql(f, fmt)
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
