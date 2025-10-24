use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use surrealdb_types::{SurrealValue, Value, Variables};
use uuid::Uuid;

use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt};
use crate::opt::{PatchOp, PatchOps, Resource};
use crate::{Connection, Result, Surreal};

/// A patch future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Patch<'r, C: Connection, R> {
	pub(super) txn: Option<Uuid>,
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) patches: PatchOps,
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
				for patch in patches.into_iter() {
					vec.push(surrealdb_types::Value::from(patch));
				}
				let patches = surrealdb_types::Value::Array(surrealdb_types::Array::from(vec));
				let router = client.inner.router.extract()?;

				let what = resource?;

				let mut variables = Variables::new();
				let what = what.for_sql_query(&mut variables)?;

				let operation = if upsert {
					"UPSERT"
				} else {
					"UPDATE"
				};

				variables.insert("_patches".to_string(), patches);

				let cmd = Command::Query {
					txn,
					query: Cow::Owned(format!("{operation} {what} PATCH $_patches RETURN AFTER")),
					variables,
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
	pub fn patch(self, patch: impl Into<PatchOp>) -> Self {
		let Patch {
			txn,
			client,
			resource,
			patches,
			upsert,
			response_type,
		} = self;
		Patch {
			txn,
			client,
			resource,
			patches: patches.push(patch.into()),
			upsert,
			response_type,
		}
	}
}
