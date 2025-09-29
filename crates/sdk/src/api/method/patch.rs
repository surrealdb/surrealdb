use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use surrealdb_types::{SurrealValue, Value, Variables};
use uuid::Uuid;

use crate::Surreal;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::{PatchOp, Resource};
use crate::api::{Connection, Result};
use crate::method::OnceLockExt;

/// A patch future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Patch<'r, C: Connection, R> {
	pub(super) txn: Option<Uuid>,
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) patches: Vec<Value>,
	pub(super) upsert: bool,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> Patch<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Patch<'static, C, R> {
		Patch {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Patch {
				txn,
				client,
				resource,
				patches,
				upsert,
				..
			} = self;
			Box::pin(async move {
				let mut vec = Vec::with_capacity(patches.len());
				for patch in patches {
					vec.push(patch);
				}
				let patches = surrealdb_types::Value::Array(surrealdb_types::Array::from(vec));
				let router = client.inner.router.extract()?;

				let what = resource?.into_value();
				let operation = if upsert {
					"UPSERT"
				} else {
					"UPDATE"
				};
				let query = format!("{operation} {what} PATCH {patches}");

				let cmd = Command::RawQuery {
					txn,
					query: Cow::Owned(query),
					variables: Variables::new(),
				};

				router.$method(cmd).await
			})
		}
	};
}

impl<'r, Client> IntoFuture for Patch<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R> IntoFuture for Patch<'r, Client, Option<R>>
where
	Client: Connection,
	R: SurrealValue,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Patch<'r, Client, Vec<R>>
where
	Client: Connection,
	R: SurrealValue,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

impl<'r, C, R> Patch<'r, C, R>
where
	C: Connection,
{
	/// Applies JSON Patch changes to all records, or a specific record, in the
	/// database.
	pub fn patch(mut self, patch: impl Into<PatchOp>) -> Patch<'r, C, R> {
		let PatchOp(patch) = patch.into();
		match patch {
			Value::Array(values) => {
				for value in values {
					self.patches.push(value);
				}
			}
			value => self.patches.push(value),
		}
		self
	}
}
