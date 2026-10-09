use anyhow::{Context as _, bail};
use revision::revisioned;
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{
	FromStored, LogicalPermissionBody, ModuleExecutable, Permission, StoredPermission,
};
use crate::expr as runtime;
use crate::expr::logical::LogicalPermission;
use crate::expr::statements::DefineModuleStatement;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StoredModuleDefinition {
	pub name: Option<String>,
	pub comment: Option<String>,
	pub permissions: StoredPermission,
	pub executable: ModuleExecutable,
	/// Whether this module is loaded without verifying a signature.
	///
	/// Definitions written before this field existed predate signing
	/// entirely, so they decode as unsigned and keep loading unchanged.
	#[revision(start = 2, default_fn = "default_unsigned")]
	pub unsigned: bool,
}

impl StoredModuleDefinition {
	fn default_unsigned(_revision: u16) -> Result<bool, revision::Error> {
		Ok(true)
	}
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
	fn to_runtime_definition(&self) -> DefineModuleStatement {
		DefineModuleStatement {
			kind: DefineKind::Default,
			name: self.name.clone(),
			executable: self.executable.clone().into(),
			unsigned: self.unsigned,
			permissions: self.permissions.clone(),
			comment: self
				.comment
				.clone()
				.map(|x| runtime::Expr::Literal(runtime::Literal::String(x.into())))
				.unwrap_or(runtime::Expr::Literal(runtime::Literal::None)),
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
			"unsigned" => self.unsigned.into(),
			"permissions" => self.permissions.structure(),
			"comment", if let Some(v) = self.comment => v.to_sql().into(),
		})
	}
}

impl ToSql for ModuleDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition().fmt_sql(f, fmt)
	}
}

/// Runtime form of [`StoredModuleDefinition`].
///
/// `permissions` and `permissions_body` name the same clause; a constructor
/// sets both from one source ([`LogicalPermissionBody::of`] for a `DEFINE`,
/// the stored text in `from_stored`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModuleDefinition {
	pub name: Option<String>,
	pub executable: ModuleExecutable,
	/// See [`StoredModuleDefinition::unsigned`].
	pub unsigned: bool,
	pub permissions: Permission,
	/// The `PERMISSIONS` clause in logical form; see
	/// [`Self::permissions_logical`].
	pub permissions_body: LogicalPermissionBody,
	pub comment: Option<String>,
}

impl ModuleDefinition {
	/// The `PERMISSIONS` clause in logical form, compiling its guard on first
	/// call.
	pub fn permissions_logical(&self) -> anyhow::Result<LogicalPermission> {
		self.permissions_body.logical()
	}
}

impl FromStored for ModuleDefinition {
	type Stored = StoredModuleDefinition;

	fn from_stored(stored: &StoredModuleDefinition) -> anyhow::Result<ModuleDefinition> {
		fn build(stored: &StoredModuleDefinition) -> anyhow::Result<ModuleDefinition> {
			Ok(ModuleDefinition {
				name: stored.name.clone(),
				executable: stored.executable.clone(),
				unsigned: stored.unsigned,
				permissions: Permission::from_stored(&stored.permissions)?,
				permissions_body: LogicalPermissionBody::from_stored(&stored.permissions),
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
			unsigned: self.unsigned,
		}
	}
}
