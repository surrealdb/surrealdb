use anyhow::bail;
use revision::revisioned;
use surrealdb_types::{ToSql, write_sql};

use crate::catalog::{ModuleExecutable, Permission};
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::DefineKind;
use crate::sql::{self, DefineModuleStatement};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ModuleDefinition {
	pub(crate) name: Option<String>,
	pub(crate) comment: Option<String>,
	pub(crate) permissions: Permission,
	pub(crate) executable: ModuleExecutable,
}

impl_kv_value_revisioned!(ModuleDefinition);

impl ModuleDefinition {
	fn to_sql_definition(&self) -> DefineModuleStatement {
		DefineModuleStatement {
			kind: DefineKind::Default,
			name: self.name.clone(),
			executable: self.executable.clone().into(),
			permissions: self.permissions.clone().into(),
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x)))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
		}
	}

	/// This function is used to get the storage name of a module.
	pub(crate) fn get_storage_name(&self) -> anyhow::Result<String> {
		if let Some(name) = &self.name {
			Ok(format!("mod::{}", name))
		} else if let ModuleExecutable::Silo(silo) = &self.executable {
			Ok(format!(
				"silo::{}::{}<{}.{}.{}>",
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
			"name".to_string(), if let Some(name) = self.name => name.into(),
			"executable".to_string() => self.executable.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.to_sql().into(),
		})
	}
}

impl ToSql for &ModuleDefinition {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self.to_sql_definition())
	}
}
