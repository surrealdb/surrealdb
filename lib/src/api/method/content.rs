use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::opt::Range;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::sql::to_value;
use crate::sql::Id;
use crate::sql::Value;
use crate::Surreal;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

/// A content future
///
/// Content inserts or replaces the contents of a record entirely
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Content<'r, C: Connection, D, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) method: Method,
	pub(super) resource: Result<Resource>,
	pub(super) range: Option<Range<Id>>,
	pub(super) content: D,
	pub(super) response_type: PhantomData<R>,
}

impl<C, D, R> Content<'_, C, D, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Content<'static, C, D, R> {
		Content {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Content {
				client,
				method,
				resource,
				range,
				content,
				..
			} = self;
			let content = to_value(content);
			Box::pin(async move {
				let param = match range {
					Some(range) => resource?.with_range(range)?.into(),
					None => resource?.into(),
				};
				let params = match content? {
					Value::None | Value::Null => vec![param],
					content => vec![param, content],
				};
				let router = client.router.extract()?;
				router.$method(method, Param::new(params)).await
			})
		}
	};
}

impl<'r, Client, D> IntoFuture for Content<'r, Client, D, Value>
where
	Client: Connection,
	D: Serialize,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, D, R> IntoFuture for Content<'r, Client, D, Option<R>>
where
	Client: Connection,
	D: Serialize,
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, D, R> IntoFuture for Content<'r, Client, D, Vec<R>>
where
	Client: Connection,
	D: Serialize,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}
