use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::method::Content;
use crate::api::method::Merge;
use crate::api::method::Patch;
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
use serde::Serialize;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::sql::to_value;

/// An upsert future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Upsert<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
	pub(super) range: Option<Range<Id>>,
	pub(super) response_type: PhantomData<R>,
}

impl<C, R> Upsert<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Upsert<'static, C, R> {
		Upsert {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Upsert {
				client,
				resource,
				range,
				..
			} = self;
			Box::pin(async move {
				let param: Value = match range {
					Some(range) => resource?.with_range(range)?.into(),
					None => resource?.into(),
				};
				let router = client.router.extract()?;
				router
					.$method(Command::Upsert {
						what: param,
						data: None,
					})
					.await
			})
		}
	};
}

impl<'r, Client> IntoFuture for Upsert<'r, Client, Value>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R> IntoFuture for Upsert<'r, Client, Option<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Upsert<'r, Client, Vec<R>>
where
	Client: Connection,
	R: DeserializeOwned,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

impl<C> Upsert<'_, C, Value>
where
	C: Connection,
{
	/// Restricts the records to upsert to those in the specified range
	pub fn range(mut self, bounds: impl Into<Range<Id>>) -> Self {
		self.range = Some(bounds.into());
		self
	}
}

impl<C, R> Upsert<'_, C, Vec<R>>
where
	C: Connection,
{
	/// Restricts the records to upsert to those in the specified range
	pub fn range(mut self, bounds: impl Into<Range<Id>>) -> Self {
		self.range = Some(bounds.into());
		self
	}
}

impl<'r, C, R> Upsert<'r, C, R>
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
			let data = to_value(data)?;

			let what: Value = match self.range {
				Some(range) => self.resource?.with_range(range)?.into(),
				None => self.resource?.into(),
			};

			let data = match data {
				Value::None | Value::Null => None,
				content => Some(content),
			};

			Ok(Command::Upsert {
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
			range: self.range,
			content: data,
			response_type: PhantomData,
		}
	}

	/// Patches the current document / record data with the specified JSON Patch data
	pub fn patch(self, PatchOp(patch): PatchOp) -> Patch<'r, C, R> {
		Patch {
			client: self.client,
			resource: self.resource,
			range: self.range,
			patches: vec![patch],
			response_type: PhantomData,
		}
	}
}
