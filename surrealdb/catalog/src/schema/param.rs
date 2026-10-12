use anyhow::Context as _;
use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{FromStored, LogicalPermissionBody, Permission, StoredPermission};
use crate::expr::logical::LogicalPermission;
use crate::expr::statements::define::{DefineKind, DefineParamStatement};
use crate::expr::statements::info::InfoStructure;
use crate::stored::Stored;
use crate::val::Value;
use crate::{expr as runtime, impl_kv_value_catalog};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredParamDefinition {
	#[stored(n = 2)]
	pub name: Strand,
	#[stored(n = 4)]
	pub value: Value,
	#[stored(
		n = 1,
		ignorable,
		independent = "a comment is text for people and no other field reads it"
	)]
	pub comment: Option<String>,
	#[stored(n = 6)]
	pub permissions: StoredPermission,
}
impl_kv_value_catalog!(StoredParamDefinition);

impl ParamDefinition {
	fn to_runtime_definition(&self) -> DefineParamStatement {
		DefineParamStatement {
			kind: DefineKind::Default,
			name: self.name.clone(),
			value: {
				let public_val: crate::types::PublicValue =
					self.value.clone().try_into().expect("value conversion should succeed");
				runtime::Expr::from_surface_value(public_val)
			},
			comment: self
				.comment
				.clone()
				.map(|x| runtime::Expr::Literal(runtime::Literal::String(x.into())))
				.unwrap_or(runtime::Expr::Literal(runtime::Literal::None)),

			permissions: self.permissions.clone(),
		}
	}
}

impl ToSql for ParamDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition().fmt_sql(f, fmt)
	}
}

impl InfoStructure for ParamDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => self.name.into(),
			"value" => self.value.structure(),
			"permissions" => self.permissions.structure(),
			"comment", if let Some(v) = self.comment => v.into(),
		})
	}
}

/// Runtime form of [`crate::catalog::StoredParamDefinition`].
///
/// `permissions` and `permissions_body` name the same clause, compiled from
/// the same stored text for the legacy and the streaming engine respectively;
/// a constructor sets both from one source ([`LogicalPermissionBody::of`] for
/// the permission a `DEFINE` statement produced).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParamDefinition {
	pub name: Strand,
	pub value: Value,
	pub permissions: Permission,
	/// The `PERMISSIONS` clause in logical form; see
	/// [`Self::permissions_logical`].
	pub permissions_body: LogicalPermissionBody,
	pub comment: Option<String>,
}

impl ParamDefinition {
	/// The `PERMISSIONS` clause in logical form, compiling its guard on first
	/// call.
	pub fn permissions_logical(&self) -> anyhow::Result<LogicalPermission> {
		self.permissions_body.logical()
	}
}

impl FromStored for ParamDefinition {
	type Stored = StoredParamDefinition;

	fn from_stored(stored: &StoredParamDefinition) -> anyhow::Result<ParamDefinition> {
		fn build(stored: &StoredParamDefinition) -> anyhow::Result<ParamDefinition> {
			Ok(ParamDefinition {
				name: stored.name.clone(),
				value: stored.value.clone(),
				permissions: Permission::from_stored(&stored.permissions)?,
				permissions_body: LogicalPermissionBody::from_stored(&stored.permissions),
				comment: stored.comment.clone(),
			})
		}
		build(stored).with_context(|| {
			format!("the stored definition of parameter `${}` no longer compiles", stored.name)
		})
	}
}

impl ParamDefinition {
	pub fn to_stored(&self) -> StoredParamDefinition {
		StoredParamDefinition {
			name: self.name.clone(),
			value: self.value.clone(),
			comment: self.comment.clone(),
			permissions: crate::catalog::StoredPermission::from_runtime(&self.permissions),
		}
	}
}
