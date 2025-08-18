use anyhow::Result;
use revision::revisioned;
use uuid::Uuid;

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Expr, Fetchs, Fields};
use crate::iam::Auth;
use crate::kvs::{KVValue, impl_kv_value_revisioned};
use crate::sql::ToSql;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SubscriptionDefinitionStore {
	pub id: Uuid,
	pub node: Uuid,
	pub fields: Fields,
	pub what: Expr,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,
}

impl_kv_value_revisioned!(SubscriptionDefinitionStore);

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SubscriptionDefinition {
	pub id: Uuid,
	pub node: Uuid,
	pub fields: Fields,
	pub what: Expr,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can check it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	pub(crate) auth: Option<Auth>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can check it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	pub(crate) session: Option<Value>,
}

impl SubscriptionDefinition {
	fn to_store(&self) -> SubscriptionDefinitionStore {
		todo!("STU")
	}

	fn from_store(store: SubscriptionDefinitionStore) -> Result<Self> {
		todo!("STU")
	}

	pub(crate) fn to_expr_definition(&self) -> crate::expr::LiveStatement {
		todo!("STU")
	}

	fn to_sql_definition(&self) -> crate::sql::LiveStatement {
		todo!("STU")
	}
}

impl KVValue for SubscriptionDefinition {
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let store = self.to_store();
		Ok(store.kv_encode_value()?)
	}

	fn kv_decode_value(bytes: Vec<u8>) -> Result<Self> {
		let store = SubscriptionDefinitionStore::kv_decode_value(bytes)?;
		SubscriptionDefinition::from_store(store)
	}
}

impl InfoStructure for SubscriptionDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"id".to_string() => crate::val::Uuid(self.id).into(),
			"node".to_string() => crate::val::Uuid(self.node).into(),
			"expr".to_string() => self.fields.structure(),
			"what".to_string() => self.what.structure(),
			"cond".to_string(), if let Some(v) = self.cond => v.0.structure(),
			"fetch".to_string(), if let Some(v) = self.fetch => v.structure(),
		})
	}
}

impl ToSql for &SubscriptionDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}
