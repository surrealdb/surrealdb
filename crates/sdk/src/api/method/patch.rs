use crate::opt::PatchOps;
use crate::Surreal;
use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::PatchOp;
use crate::api::opt::Resource;
use crate::method::OnceLockExt;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::{Value, Array, Data};
use surrealdb_core::expr::TryFromValue;

/// A patch future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Patch<'r, C: Connection, R> {
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
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Patch<'static, C, R> {
		Patch {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	() => {
		fn into_future(self) -> Self::IntoFuture {
			let Patch {
				client,
				resource,
				patches,
				upsert,
				..
			} = self;
			Box::pin(async move {
				let mut vec = Vec::with_capacity(patches.len());
				for patch in patches.into_iter() {
					let value = Value::try_from(patch)?;
					vec.push(value);
				}
				let router = client.inner.router.extract()?;
				let cmd = Command::Upsert {
					what: resource?,
					data: Some(Data::PatchExpression(Value::Array(Array(vec)))),
				};

				let query_results = router.execute_query(cmd).await?;

				query_results.take(0_usize)
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

	into_future! {}
}

impl<'r, Client, R> IntoFuture for Patch<'r, Client, Option<R>>
where
	Client: Connection,
	R: TryFromValue,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}

impl<'r, Client, R> IntoFuture for Patch<'r, Client, Vec<R>>
where
	Client: Connection,
	R: TryFromValue,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}

impl<'r, C, R> Patch<'r, C, R>
where
	C: Connection,
{
	/// Applies JSON Patch changes to all records, or a specific record, in the database.
	pub fn patch(mut self, patch: impl Into<PatchOp>) -> Patch<'r, C, R> {
		self.patches.push(patch.into());
		self
	}
}
