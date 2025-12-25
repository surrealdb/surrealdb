use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use uuid::Uuid;

use super::validate_data;
use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt};
use crate::opt::Resource;
use crate::types::{SurrealValue, Value, Variables};
use crate::{Connection, Result, Surreal};

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
	/// Converts to an owned type which can easily be moved to a different
	/// thread
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
				txn,
				client,
				resource,
				content,
				upsert,
				..
			} = self;
			let content = content.into_value();
			Box::pin(async move {
				let content = match content {
					crate::types::Value::None | crate::types::Value::Null => None,
					data => {
						validate_data(
							&data,
							"Tried to merge non-object-like data, only structs and objects are supported",
						)?;
						Some(data)
					}
				};

				let router = client.inner.router.extract()?;

				let what = resource?;

				let mut variables = Variables::new();
				let what = what.for_sql_query(&mut variables)?;

				let operation = if upsert {
					"UPSERT"
				} else {
					"UPDATE"
				};

				let query = match content {
					None => Cow::Owned(format!("{operation} {what}")),
					Some(data) => {
						variables.insert("_data".to_string(), data);
						Cow::Owned(format!("{operation} {what} MERGE $_data"))
					}
				};

				let cmd = Command::Query {
					txn,
					query,
					variables,
				};

				router.$method(client.session_id, cmd).await
			})
		}
	};
}

impl<'r, Client, D> IntoFuture for Merge<'r, Client, D, Value>
where
	Client: Connection,
	D: SurrealValue,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, D, R> IntoFuture for Merge<'r, Client, D, Option<R>>
where
	Client: Connection,
	D: SurrealValue,
	R: SurrealValue,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, D, R> IntoFuture for Merge<'r, Client, D, Vec<R>>
where
	Client: Connection,
	D: SurrealValue,
	R: SurrealValue,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}
