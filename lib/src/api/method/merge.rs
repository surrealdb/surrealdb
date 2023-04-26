use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::conn::Router;
use crate::api::opt::Range;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use crate::sql::to_value;
use crate::sql::Id;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;

/// A merge future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Merge<'r, C: Connection, D, R> {
	pub(super) router: Result<&'r Router<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) range: Option<Range<Id>>,
	pub(super) content: D,
	pub(super) response_type: PhantomData<R>,
}

impl<'r, Client, D, R> IntoFuture for Merge<'r, Client, D, R>
where
	Client: Connection,
	D: Serialize,
	R: DeserializeOwned,
{
	type Output = Result<R>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		let Merge {
			router,
			resource,
			range,
			content,
			..
		} = self;
		let content = to_value(content);
		Box::pin(async move {
			let param = match range {
				Some(range) => resource?.with_range(range)?,
				None => resource?.into(),
			};
			let mut conn = Client::new(Method::Merge);
			conn.execute(router?, Param::new(vec![param, content?])).await
		})
	}
}
