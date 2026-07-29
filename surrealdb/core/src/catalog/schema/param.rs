use anyhow::Context as _;
use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{FromStored, Permission, StoredPermission};
use crate::expr::statements::info::InfoStructure;
use crate::key::impl_kv_value_revisioned;
use crate::sql;
use crate::sql::statements::define::{DefineKind, DefineParamStatement};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct StoredParamDefinition {
	pub(crate) name: Strand,
	pub(crate) value: Value,
	pub(crate) comment: Option<String>,
	pub(crate) permissions: StoredPermission,
}
impl_kv_value_revisioned!(StoredParamDefinition);

impl ParamDefinition {
	fn to_sql_definition(&self) -> DefineParamStatement {
		DefineParamStatement {
			kind: DefineKind::Default,
			name: self.name.clone().into(),
			value: {
				let public_val: crate::types::PublicValue =
					self.value.clone().try_into().expect("value conversion should succeed");
				sql::Expr::from_public_value(public_val)
			},
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x.into())))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),

			permissions: self.permissions.to_sql_permission(),
		}
	}
}

impl ToSql for ParamDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParamDefinition {
	pub name: Strand,
	pub value: Value,
	pub permissions: Permission,
	pub comment: Option<String>,
}

impl FromStored for ParamDefinition {
	type Stored = StoredParamDefinition;

	fn from_stored(stored: &StoredParamDefinition) -> anyhow::Result<ParamDefinition> {
		fn build(stored: &StoredParamDefinition) -> anyhow::Result<ParamDefinition> {
			Ok(ParamDefinition {
				name: stored.name.clone(),
				value: stored.value.clone(),
				permissions: Permission::from_stored(&stored.permissions)?,
				comment: stored.comment.clone(),
			})
		}
		build(stored).with_context(|| {
			format!("the stored definition of parameter `${}` no longer compiles", stored.name)
		})
	}
}

impl ParamDefinition {
	pub(crate) fn to_stored(&self) -> StoredParamDefinition {
		StoredParamDefinition {
			name: self.name.clone(),
			value: self.value.clone(),
			comment: self.comment.clone(),
			permissions: self.permissions.to_stored(),
		}
	}
}
