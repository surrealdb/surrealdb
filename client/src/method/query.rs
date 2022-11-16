use crate::method::Method;
use crate::param;
use crate::param::from_json;
use crate::param::Param;
use crate::Connection;
use crate::Result;
use crate::Router;
use futures::future::BoxFuture;
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::future::IntoFuture;
use surrealdb::sql;
use surrealdb::sql::Statement;
use surrealdb::sql::Statements;
use surrealdb::sql::Value;

/// A query future
#[derive(Debug)]
pub struct Query<'r, C: Connection> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) query: Vec<Result<Vec<Statement>>>,
	pub(super) bindings: BTreeMap<String, Value>,
}

impl<'r, Client> IntoFuture for Query<'r, Client>
where
	Client: Connection,
{
	type Output = Result<Vec<Result<Vec<Value>>>>;
	type IntoFuture = BoxFuture<'r, Result<Vec<Result<Vec<Value>>>>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut statements = Vec::with_capacity(self.query.len());
			for query in self.query {
				statements.extend(query?);
			}
			let mut param = vec![sql::Query(Statements(statements)).to_string().into()];
			if !self.bindings.is_empty() {
				param.push(self.bindings.into());
			}
			let mut conn = Client::new(Method::Query);
			conn.execute_query(self.router?, Param::new(param)).await
		})
	}
}

impl<'r, C> Query<'r, C>
where
	C: Connection,
{
	/// Chains a query onto an existing query
	pub fn query(mut self, query: impl param::Query) -> Self {
		self.query.push(query.try_into_query());
		self
	}

	/// Binds a parameter to a query
	pub fn bind<D>(mut self, key: impl Into<String>, value: D) -> Self
	where
		D: Serialize + Send,
	{
		self.bindings.insert(key.into(), from_json(json!(value)));
		self
	}
}
