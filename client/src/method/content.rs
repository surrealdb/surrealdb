use crate::method::Method;
use crate::param::from_json;
use crate::param::DbResource;
use crate::param::Param;
use crate::param::Range;
use crate::Connection;
use crate::Result;
use crate::Router;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb::sql::Id;

/// A content future
///
/// Content inserts or replaces the contents of a record entirely
#[derive(Debug)]
pub struct Content<'r, C: Connection, D, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) method: Method,
	pub(super) resource: Result<DbResource>,
	pub(super) range: Option<Range<Id>>,
	pub(super) content: D,
	pub(super) response_type: PhantomData<R>,
}

impl<'r, Client, D, R> IntoFuture for Content<'r, Client, D, R>
where
	Client: Connection,
	D: Serialize + Send + 'r,
	R: DeserializeOwned + Send,
{
	type Output = Result<R>;
	type IntoFuture = BoxFuture<'r, Result<R>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let resource = self.resource?;
			let param = match self.range {
				Some(range) => resource.with_range(range)?,
				None => resource.into(),
			};
			let content = json!(self.content);
			let mut conn = Client::new(self.method);
			conn.execute(self.router?, Param::new(vec![param, from_json(content)])).await
		})
	}
}
