use revision::revisioned;

use crate::catalog::Permission;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::ToSql;
use crate::sql::statements::define::DefineKind;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct MlModelDefinition {
	pub hash: String,
	pub name: String,
	pub version: String,
	pub comment: Option<String>,
	pub permissions: Permission,
}

impl_kv_value_revisioned!(MlModelDefinition);

impl MlModelDefinition {
	fn to_sql_definition(&self) -> crate::sql::DefineModelStatement {
		crate::sql::DefineModelStatement {
			kind: DefineKind::Default,
			hash: self.hash.clone(),
			name: unsafe { crate::sql::Ident::new_unchecked(self.name.clone()) },
			version: self.version.clone(),
			permissions: self.permissions.clone().into(),
			comment: self.comment.clone().map(Into::into),
		}
	}
}

impl InfoStructure for MlModelDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"version".to_string() => self.version.into(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}

impl ToSql for MlModelDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}
