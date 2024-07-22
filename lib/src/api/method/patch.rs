use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::PatchOp;
use crate::api::opt::Range;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::sql::Id;
use crate::sql::Value;
use crate::Surreal;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::result::Result as StdResult;

/// A patch future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Patch<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) range: Option<Range<Id>>,
	pub(super) patches: Vec<StdResult<Value, crate::err::Error>>,
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
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Patch {
				client,
				resource,
				range,
				patches,
				..
			} = self;
			Box::pin(async move {
				let param: Value = match range {
					Some(range) => resource?.with_range(range)?.into(),
					None => resource?.into(),
				};
				let mut vec = Vec::with_capacity(patches.len());
				for result in patches {
					vec.push(result?);
				}
				let patches = Value::from(vec);
				let router = client.router.extract()?;
				let cmd = Command::Patch {
					what: param,
					data: Some(patches),
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
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Patch<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

impl<'r, C, R> Patch<'r, C, R>
where
	C: Connection,
{
	/// Applies JSON Patch changes to all records, or a specific record, in the database.
	pub fn patch(mut self, PatchOp(patch): PatchOp) -> Patch<'r, C, R> {
		self.patches.push(patch);
		self
	}
}
