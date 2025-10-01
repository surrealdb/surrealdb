use revision::revisioned;
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Fetchs, Fields};
use crate::iam::Auth;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::ToSql;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SubscriptionDefinition {
	pub id: Uuid,
	pub node: Uuid,
	pub fields: Fields,
	pub what: Expr,
	pub cond: Option<Expr>,
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

impl_kv_value_revisioned!(SubscriptionDefinition);

impl SubscriptionDefinition {
	fn to_sql_definition(&self) -> crate::sql::LiveStatement {
		crate::sql::LiveStatement {
			fields: self.fields.clone().into(),
			what: self.what.clone().into(),
			cond: self.cond.clone().map(|c| crate::sql::Cond(c.into())),
			fetch: self.fetch.clone().map(|f| f.into()),
		}
	}
}

impl InfoStructure for SubscriptionDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"id".to_string() => crate::val::Uuid(self.id).into(),
			"node".to_string() => crate::val::Uuid(self.node).into(),
			"expr".to_string() => self.fields.structure(),
			"what".to_string() => self.what.structure(),
			"cond".to_string(), if let Some(v) = self.cond => v.structure(),
			"fetch".to_string(), if let Some(v) = self.fetch => v.structure(),
		})
	}
}

impl ToSql for &SubscriptionDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct NodeLiveQuery {
	pub(crate) ns: NamespaceId,
	pub(crate) db: DatabaseId,
	pub(crate) tb: String,
}
impl_kv_value_revisioned!(NodeLiveQuery);
