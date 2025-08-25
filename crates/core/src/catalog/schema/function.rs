use revision::revisioned;

use crate::catalog::Permission;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Block, Kind};
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::DefineKind;
use crate::sql::{DefineFunctionStatement, ToSql};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FunctionDefinition {
	pub name: String,
	pub args: Vec<(String, Kind)>,
	pub block: Block,
	pub comment: Option<String>,
	pub permissions: Permission,
	pub returns: Option<Kind>,
}

impl_kv_value_revisioned!(FunctionDefinition);

impl FunctionDefinition {
	fn to_sql_definition(&self) -> DefineFunctionStatement {
		DefineFunctionStatement {
			kind: DefineKind::Default,
			name: unsafe { crate::sql::Ident::new_unchecked(self.name.clone()) },
			args: self
				.args
				.clone()
				.into_iter()
				.map(|(n, k)| {
					(unsafe { crate::sql::Ident::new_unchecked(n) }, crate::sql::Kind::from(k))
				})
				.collect(),
			block: self.block.clone().into(),
			permissions: self.permissions.clone().into(),
			returns: self.returns.clone().map(|k| k.into()),
			comment: self.comment.clone().map(Into::into),
		}
	}
}

impl InfoStructure for FunctionDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"args".to_string() => self.args
				.into_iter()
				.map(|(n, k)| vec![n.into(), k.structure()].into())
				.collect::<Vec<Value>>()
				.into(),
			"block".to_string() => self.block.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
			"returns".to_string(), if let Some(v) = self.returns => v.structure(),
		})
	}
}

impl ToSql for &FunctionDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}
