use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::Permission;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Block, Kind};
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::DefineKind;
use crate::sql::{self, DefineFunctionStatement};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FunctionDefinition {
	pub(crate) name: String,
	pub(crate) args: Vec<(String, Kind)>,
	pub(crate) block: Block,
	pub(crate) comment: Option<String>,
	pub(crate) permissions: Permission,
	pub(crate) returns: Option<Kind>,
}

impl_kv_value_revisioned!(FunctionDefinition);

impl FunctionDefinition {
	fn to_sql_definition(&self) -> DefineFunctionStatement {
		DefineFunctionStatement {
			kind: DefineKind::Default,
			name: self.name.clone(),
			args: self.args.clone().into_iter().map(|(n, k)| (n, sql::Kind::from(k))).collect(),
			block: self.block.clone().into(),
			permissions: self.permissions.clone().into(),
			returns: self.returns.clone().map(|k| k.into()),
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x)))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
		}
	}
}

impl InfoStructure for FunctionDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"args".to_string() => self.args
				.into_iter()
				.map(|(n, k)| vec![n.into(), k.to_sql().into()].into())
				.collect::<Vec<Value>>()
				.into(),
			"block".to_string() => self.block.to_sql().into(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.to_sql().into(),
			"returns".to_string(), if let Some(v) = self.returns => v.to_sql().into(),
		})
	}
}

impl ToSql for &FunctionDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}
