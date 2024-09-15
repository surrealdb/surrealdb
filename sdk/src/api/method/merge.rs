use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::value::Value;
use crate::Surreal;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::sql::{to_value as to_core_value, Value as CoreValue};

/// A merge future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Merge<'r, C: Connection, D, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) content: D,
	pub(super) response_type: PhantomData<R>,
}

impl<C, D, R> Merge<'_, C, D, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Merge<'static, C, D, R> {
		Merge {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Merge {
				client,
				resource,
				content,
				..
			} = self;
			let content = to_core_value(content);
			Box::pin(async move {
				let content = match content? {
					CoreValue::None | CoreValue::Null => None,
					x => Some(x),
				};

				let router = client.router.extract()?;
				let cmd = Command::Merge {
					what: resource?,
					data: content,
				};
				router.$method(cmd).await
			})
		}
	};
}

impl<'r, Client, D> IntoFuture for Merge<'r, Client, D, Value>
where
	Client: Connection,
	D: Serialize + 'static,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, D, R> IntoFuture for Merge<'r, Client, D, Option<R>>
where
	Client: Connection,
	D: Serialize + 'static,
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, D, R> IntoFuture for Merge<'r, Client, D, Vec<R>>
where
	Client: Connection,
	D: Serialize + 'static,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}
