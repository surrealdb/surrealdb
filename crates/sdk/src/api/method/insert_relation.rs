use super::BoxFuture;
use crate::Surreal;
use crate::Value;
use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::method::OnceLockExt;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use uuid::Uuid;

/// An Insert Relation future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct InsertRelation<'r, C: Connection, R> {
	pub(super) txn: Option<Uuid>,
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) command: Result<Command>,
	pub(super) response_type: PhantomData<R>,
}

impl<'r, C, R> InsertRelation<'r, C, R>
where
	C: Connection,
{
	pub(crate) fn from_closure<F>(client: Cow<'r, Surreal<C>>, txn: Option<Uuid>, f: F) -> Self
	where
		F: FnOnce() -> Result<Command>,
	{
		InsertRelation {
			txn,
			client,
			command: f(),
			response_type: PhantomData,
		}
	}

	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> InsertRelation<'static, C, R> {
		InsertRelation {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let InsertRelation {
				client,
				command,
				..
			} = self;
			Box::pin(async move {
				let router = client.inner.router.extract()?;
				router.$method(command?).await
			})
		}
	};
}

impl<'r, Client> IntoFuture for InsertRelation<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R> IntoFuture for InsertRelation<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for InsertRelation<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}
