use anyhow::{Context as _, bail};
use revision::revisioned;
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{FromStored, ModuleExecutable, Permission, StoredPermission};
use crate::expr::statements::info::InfoStructure;
use crate::sql::statements::define::DefineKind;
use crate::sql::{self, DefineModuleStatement};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StoredModuleDefinition {
	pub name: Option<String>,
	pub comment: Option<String>,
	pub permissions: StoredPermission,
	pub executable: ModuleExecutable,
}

impl_kv_value_revisioned!(StoredModuleDefinition);

impl StoredModuleDefinition {
	/// This function is used to get the storage name of a module.
	pub fn get_storage_name(&self) -> anyhow::Result<String> {
		if let Some(name) = &self.name {
			Ok(format!("mod::{}", name))
		} else if let ModuleExecutable::Silo(silo) = &self.executable {
			Ok(format!(
				"silo::{}::{}::<{}.{}.{}>",
				silo.organisation, silo.package, silo.major, silo.minor, silo.patch
			))
		} else {
			bail!("A module without a name cannot be stored")
		}
	}
}

impl ModuleDefinition {
	fn to_sql_definition(&self) -> DefineModuleStatement {
		DefineModuleStatement {
			kind: DefineKind::Default,
			name: self.name.clone().map(Into::into),
			executable: self.executable.clone().into(),
			permissions: self.permissions.to_sql_permission(),
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x.into())))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
		}
	}

	/// See [`StoredModuleDefinition::get_storage_name`]; same derivation on the
	/// compiled form.
	pub fn get_storage_name(&self) -> anyhow::Result<String> {
		if let Some(name) = &self.name {
			Ok(format!("mod::{}", name))
		} else if let ModuleExecutable::Silo(silo) = &self.executable {
			Ok(format!(
				"silo::{}::{}::<{}.{}.{}>",
				silo.organisation, silo.package, silo.major, silo.minor, silo.patch
			))
		} else {
			bail!("A module without a name cannot be stored")
		}
	}
}

impl InfoStructure for ModuleDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name", if let Some(name) = self.name => name.into(),
			"executable" => self.executable.structure(),
			"permissions" => self.permissions.structure(),
			"comment", if let Some(v) = self.comment => v.to_sql().into(),
		})
	}
}

impl ToSql for ModuleDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

/// Runtime form of [`StoredModuleDefinition`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModuleDefinition {
	pub name: Option<String>,
	pub executable: ModuleExecutable,
	pub permissions: Permission,
	pub comment: Option<String>,
}

impl FromStored for ModuleDefinition {
	type Stored = StoredModuleDefinition;

	fn from_stored(stored: &StoredModuleDefinition) -> anyhow::Result<ModuleDefinition> {
		fn build(stored: &StoredModuleDefinition) -> anyhow::Result<ModuleDefinition> {
			Ok(ModuleDefinition {
				name: stored.name.clone(),
				executable: stored.executable.clone(),
				permissions: Permission::from_stored(&stored.permissions)?,
				comment: stored.comment.clone(),
			})
		}
		build(stored).with_context(|| {
			format!(
				"the stored definition of module `{}` no longer compiles",
				stored.name.as_deref().unwrap_or("<unnamed>")
			)
		})
	}
}

impl ModuleDefinition {
	pub fn to_stored(&self) -> StoredModuleDefinition {
		StoredModuleDefinition {
			name: self.name.clone(),
			comment: self.comment.clone(),
			permissions: crate::catalog::StoredPermission::from_runtime(&self.permissions),
			executable: self.executable.clone(),
		}
	}
}
