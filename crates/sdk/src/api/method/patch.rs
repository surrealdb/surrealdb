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
pub struct Patch<'r, C: Connection, R: Resource, RT> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: R,
	pub(super) patches: PatchOps,
	pub(super) upsert: bool,
	pub(super) response_type: PhantomData<RT>,
}

impl<C, R, RT> Patch<'_, C, R, RT>
where
	C: Connection,
	R: Resource,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Patch<'static, C, R, RT> {
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
					what: resource.into_values(),
					data: Some(Data::PatchExpression(Value::Array(Array(vec)))),
				};

				let query_results = router.execute_query(cmd).await?;

				query_results.take(0_usize)
			})
		}
	};
}

impl<'r, Client, R> IntoFuture for Patch<'r, Client, R, Value>
where
	Client: Connection,
	R: Resource,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}

impl<'r, Client, R, RT> IntoFuture for Patch<'r, Client, R, Option<RT>>
where
	Client: Connection,
	R: Resource,
	RT: TryFromValue,
{
	type Output = Result<Option<RT>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}

impl<'r, Client, R, RT> IntoFuture for Patch<'r, Client, R, Vec<RT>>
where
	Client: Connection,
	R: Resource,
	RT: TryFromValue,
{
	type Output = Result<Vec<RT>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}

impl<'r, C, R, RT> Patch<'r, C, R, RT>
where
	C: Connection,
	R: Resource,
{
	/// Applies JSON Patch changes to all records, or a specific record, in the database.
	pub fn patch(mut self, patch: impl Into<PatchOp>) -> Patch<'r, C, R, RT> {
		self.patches.push(patch.into());
		self
	}
}
