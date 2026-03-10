use std::collections::BTreeMap;

use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Fetchs, Fields};
use crate::iam::Auth;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::live::LiveFields;
use crate::val::{TableName, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum SubscriptionFields {
	Diff,
	Select(Fields),
}

impl InfoStructure for SubscriptionFields {
	fn structure(self) -> Value {
		match self {
			SubscriptionFields::Diff => "diff".to_string().into(),
			SubscriptionFields::Select(x) => x.to_sql().into(),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SubscriptionDefinition {
	pub(crate) id: Uuid,
	pub(crate) node: Uuid,
	pub(crate) fields: SubscriptionFields,
	pub(crate) what: Expr,
	pub(crate) cond: Option<Expr>,
	pub(crate) fetch: Option<Fetchs>,
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
	// When a live query is created, we analyze the query
	// and store the variables that are used in the query.
	pub(crate) vars: BTreeMap<String, Value>,
}

impl_kv_value_revisioned!(SubscriptionDefinition);

impl SubscriptionDefinition {
	fn to_sql_definition(&self) -> crate::sql::LiveStatement {
		let fields = match &self.fields {
			SubscriptionFields::Diff => LiveFields::Diff,
			SubscriptionFields::Select(x) => LiveFields::Select(x.clone().into()),
		};

		crate::sql::LiveStatement {
			fields,
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
			"fields".to_string() => self.fields.structure(),
			"what".to_string() => self.what.structure(),
			"cond".to_string(), if let Some(v) = self.cond => v.structure(),
			"fetch".to_string(), if let Some(v) = self.fetch => v.structure(),
		})
	}
}

impl ToSql for &SubscriptionDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct NodeLiveQuery {
	pub(crate) ns: NamespaceId,
	pub(crate) db: DatabaseId,
	pub(crate) tb: TableName,
}
impl_kv_value_revisioned!(NodeLiveQuery);
