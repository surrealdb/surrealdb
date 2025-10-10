use revision::revisioned;
use surrealdb_types::{ToSql, write_sql};

use crate::catalog::Permission;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::{DefineKind, DefineParamStatement};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct ParamDefinition {
	pub(crate) name: String,
	pub(crate) value: Value,
	pub(crate) comment: Option<String>,
	pub(crate) permissions: Permission,
}
impl_kv_value_revisioned!(ParamDefinition);

impl ParamDefinition {
	fn to_sql_definition(&self) -> DefineParamStatement {
		DefineParamStatement {
			kind: DefineKind::Default,
			name: self.name.clone(),
			value: {
				let public_val: crate::types::PublicValue = self.value.clone().try_into().unwrap();
				crate::sql::Expr::from_public_value(public_val)
			},
			comment: self
				.comment
				.clone()
				.map(|x| crate::sql::Expr::Literal(crate::sql::Literal::String(x))),
			permissions: self.permissions.clone().into(),
		}
	}
}

impl ToSql for &ParamDefinition {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self.to_sql_definition())
	}
}

impl InfoStructure for ParamDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"value".to_string() => self.value.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
