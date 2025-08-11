use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde_content::Value as Content;
use uuid::Uuid;

use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::{PatchOp, Resource};
use crate::api::{Connection, Result};
use crate::method::OnceLockExt;
use crate::{Surreal, Value};

/// A patch future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Patch<'r, C: Connection, R> {
	pub(super) txn: Option<Uuid>,
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) patches: Vec<serde_content::Result<Content<'static>>>,
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
	() => {
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
				for result in patches {
					let content =
						result.map_err(|x| crate::error::Api::DeSerializeValue(x.to_string()))?;
					let value = crate::api::value::to_core_value(content)?;
					vec.push(value);
				}
				let patches = crate::core::val::Value::from(vec);
				let router = client.inner.router.extract()?;
				let cmd = Command::Patch {
					txn,
					upsert,
					what: resource?,
					data: Some(patches),
				};

				router.execute_query(cmd).await?.take(0)
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
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}

impl<'r, Client, R> IntoFuture for Patch<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
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
			Ok(Content::Seq(values)) => {
				for value in values {
					self.patches.push(Ok(value));
				}
			}
			Ok(value) => self.patches.push(Ok(value)),
			Err(error) => self.patches.push(Err(error)),
		}
		self
	}
}
