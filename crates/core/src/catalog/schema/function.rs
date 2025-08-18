use anyhow::Result;
use reblessive::Stack;
use revision::revisioned;

use crate::catalog::Permission;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Kind};
use crate::kvs::{KVValue, impl_kv_value_revisioned};
use crate::sql::{DefineFunctionStatement, ToSql};
use crate::syn::parser::Parser;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct FunctionDefinitionStore {
	pub name: String,
	pub args: Vec<(String, Kind)>,
	pub block: String,
	pub comment: Option<String>,
	pub permissions: Permission,
	pub returns: Option<Kind>,
}

impl_kv_value_revisioned!(FunctionDefinitionStore);

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FunctionDefinition {
	pub name: String,
	pub args: Vec<(String, Kind)>,
	pub block: Expr,
	pub comment: Option<String>,
	pub permissions: Permission,
	pub returns: Option<Kind>,
}

impl FunctionDefinition {
	fn to_store(&self) -> FunctionDefinitionStore {
		FunctionDefinitionStore {
			name: self.name.clone(),
			args: self.args.clone(),
			block: self.block.to_string(),
			comment: self.comment.clone(),
			permissions: self.permissions.clone(),
			returns: self.returns.clone(),
		}
	}

	fn from_store(store: FunctionDefinitionStore) -> Result<Self> {
		let mut stack = Stack::new();
		let mut parser = Parser::new(&store.block.as_bytes());
		let block = stack
			.enter(|stk| parser.parse_expr(stk))
			.finish()
			.map_err(|err| anyhow::anyhow!("Failed to parse function block: {err:?}"))?;

		Ok(FunctionDefinition {
			name: store.name,
			args: store.args,
			block: block.into(),
			comment: store.comment,
			permissions: store.permissions,
			returns: store.returns,
		})
	}

	fn to_sql_definition(&self) -> DefineFunctionStatement {
		todo!("STU")
	}
}

impl KVValue for FunctionDefinition {
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let store = self.to_store();
		Ok(store.kv_encode_value()?)
	}

	fn kv_decode_value(bytes: Vec<u8>) -> Result<Self> {
		let store = FunctionDefinitionStore::kv_decode_value(bytes)?;
		FunctionDefinition::from_store(store)
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
