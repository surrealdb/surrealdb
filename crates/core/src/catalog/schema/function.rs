use revision::revisioned;
use surrealdb_types::{ToSql, write_sql};

use crate::catalog::{Executable, Permission};
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::DefineFunctionStatement;
use crate::sql::statements::define::DefineKind;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FunctionDefinition {
	pub(crate) name: String,
	pub(crate) comment: Option<String>,
	pub(crate) permissions: Permission,
	pub(crate) executable: Executable,
}

impl_kv_value_revisioned!(FunctionDefinition);

impl FunctionDefinition {
	fn to_sql_definition(&self) -> DefineFunctionStatement {
		DefineFunctionStatement {
			kind: DefineKind::Default,
			name: self.name.clone(),
			executable: self.executable.clone().into(),
			permissions: self.permissions.clone().into(),
			comment: self
				.comment
				.clone()
				.map(|x| crate::sql::Expr::Literal(crate::sql::Literal::String(x))),
		}
	}
}

impl InfoStructure for FunctionDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"executable".to_string() => self.executable.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.to_sql().into(),
		})
	}
}

impl ToSql for &FunctionDefinition {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self.to_sql_definition())
	}
}
