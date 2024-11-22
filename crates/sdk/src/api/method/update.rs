use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::method::Content;
use crate::api::method::Merge;
use crate::api::method::Patch;
use crate::api::opt::PatchOp;
use crate::api::opt::Resource;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::opt::KeyRange;
use crate::Surreal;
use crate::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::sql::{to_value as to_core_value, Value as CoreValue};

/// An update future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Update<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> Update<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Update<'static, C, R> {
		Update {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Update {
				client,
				resource,
				..
			} = self;
			Box::pin(async move {
				let router = client.router.extract()?;
				router
					.$method(Command::Update {
						what: resource?,
						data: None,
					})
					.await
			})
		}
	};
}

impl<'r, Client> IntoFuture for Update<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R> IntoFuture for Update<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Update<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

impl<C> Update<'_, C, Value>
where
	C: Connection,
{
	/// Restricts the records to update to those in the specified range
	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
		self.resource = self.resource.and_then(|x| x.with_range(range.into()));
		self
	}
}

impl<C, R> Update<'_, C, Vec<R>>
where
	C: Connection,
{
	/// Restricts the records to update to those in the specified range
	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
		self.resource = self.resource.and_then(|x| x.with_range(range.into()));
		self
	}
}

impl<'r, C, R> Update<'r, C, R>
where
	C: Connection,
	R: DeserializeOwned,
{
	/// Replaces the current document / record data with the specified data
	pub fn content<D>(self, data: D) -> Content<'r, C, R>
	where
		D: Serialize + 'static,
	{
		Content::from_closure(self.client, || {
			let data = to_core_value(data)?;

			let what = self.resource?;

			let data = match data {
				CoreValue::None => None,
				content => Some(content),
			};

			Ok(Command::Update {
				what,
				data,
			})
		})
	}

	/// Merges the current document / record data with the specified data
	pub fn merge<D>(self, data: D) -> Merge<'r, C, D, R>
	where
		D: Serialize,
	{
		Merge {
			client: self.client,
			resource: self.resource,
			content: data,
			response_type: PhantomData,
		}
	}

	/// Patches the current document / record data with the specified JSON Patch data
	pub fn patch(self, PatchOp(patch): PatchOp) -> Patch<'r, C, R> {
		Patch {
			client: self.client,
			resource: self.resource,
			patches: vec![patch],
			response_type: PhantomData,
		}
	}
}
