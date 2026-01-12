use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::Permission;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql;
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
				let public_val: crate::types::PublicValue =
					self.value.clone().try_into().expect("value conversion should succeed");
				sql::Expr::from_public_value(public_val)
			},
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x)))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),

			permissions: self.permissions.clone().into(),
		}
	}
}

impl ToSql for &ParamDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
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
