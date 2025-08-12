use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::{Connection, Result};
use crate::method::OnceLockExt;
use crate::{Surreal, Value};

/// A content future
///
/// Content inserts or replaces the contents of a record entirely
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Content<'r, C: Connection, R> {
	#[allow(dead_code)]
	pub(super) txn: Option<Uuid>,
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) command: Result<Command>,
	pub(super) response_type: PhantomData<R>,
}

impl<'r, C, R> Content<'r, C, R>
where
	C: Connection,
{
	pub(crate) fn from_closure<F>(client: Cow<'r, Surreal<C>>, txn: Option<Uuid>, f: F) -> Self
	where
		F: FnOnce() -> Result<Command>,
	{
		Content {
			txn,
			client,
			command: f(),
			response_type: PhantomData,
		}
	}

	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Content<'static, C, R> {
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

impl<'r, Client> IntoFuture for Content<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R> IntoFuture for Content<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Content<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}
