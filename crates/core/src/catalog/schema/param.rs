use revision::revisioned;

use crate::catalog::Permission;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::DefineParamStatement;
use crate::sql::ToSql;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct ParamDefinition {
	pub name: String,
	pub value: Value,
	pub comment: Option<String>,
	pub permissions: Permission,
}
impl_kv_value_revisioned!(ParamDefinition);

impl ParamDefinition {
	pub fn to_sql_definition(&self) -> DefineParamStatement {
		todo!("STU");
		// DefineParamStatement {
		// 	kind: DefineKind::Default,
		// 	name: unsafe { Ident::new_unchecked(self.name.clone()) },
		// 	value: Expr::Literal(Literal::Value(self.value.into())),
		// 	comment: self.comment.clone().map(|c| c.into()),
		// 	permissions: self.permissions.into(),
		// }
	}
}

impl ToSql for &ParamDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
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
