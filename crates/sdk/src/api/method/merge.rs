use super::validate_data;
use crate::Surreal;
use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;
use crate::method::OnceLockExt;
use crate::value::Value;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::{Value as CoreValue, to_value as to_core_value};
use uuid::Uuid;

/// A merge future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Merge<'r, C: Connection, D, R> {
	pub(super) txn: Option<Uuid>,
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) content: D,
	pub(super) upsert: bool,
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
	() => {
		fn into_future(self) -> Self::IntoFuture {
			let Merge {
				txn,
				client,
				resource,
				content,
				upsert,
				..
			} = self;
			let content = to_core_value(content);
			Box::pin(async move {
				let content = match content? {
					CoreValue::None | CoreValue::Null => None,
					data => {
						validate_data(
							&data,
							"Tried to merge non-object-like data, only structs and objects are supported",
						)?;
						Some(data)
					}
				};

				let router = client.inner.router.extract()?;
				let cmd = Command::Merge {
					upsert,
					txn,
					what: resource?,
					data: content,
				};
				router.execute_query(cmd).await?.take(0)
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

	into_future! {}
}

impl<'r, Client, D, R> IntoFuture for Merge<'r, Client, D, Option<R>>
where
	Client: Connection,
	D: Serialize + 'static,
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}

impl<'r, Client, D, R> IntoFuture for Merge<'r, Client, D, Vec<R>>
where
	Client: Connection,
	D: Serialize + 'static,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}
