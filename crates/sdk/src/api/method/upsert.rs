use crate::method::merge::Merge;
use crate::method::patch::Patch;
use crate::opt::PatchOp;
use crate::opt::PatchOps;
use crate::Surreal;

use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::method::Content;
use crate::api::opt::Resource;
use crate::method::OnceLockExt;
use crate::opt::KeyRange;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::Data;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::{Value as Value, to_value as to_core_value};

use super::ensure_values_are_objects;

/// An upsert future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Upsert<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: Result<Resource>,
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
				..
			} = self;
			Box::pin(async move {
				let router = client.inner.router.extract()?;
				router
					.$method(Command::Upsert {
						what: resource?,
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
	R: TryFromValue,
{
	type Output = Result<Option<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R> IntoFuture for Upsert<'r, Client, Vec<R>>
where
	Client: Connection,
	R: TryFromValue,
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
	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
		self.resource = self.resource.and_then(|x| x.with_range(range.into()));
		self
	}
}

impl<C, R> Upsert<'_, C, Vec<R>>
where
	C: Connection,
{
	/// Restricts the records to upsert to those in the specified range
	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
		self.resource = self.resource.and_then(|x| x.with_range(range.into()));
		self
	}
}

impl<'r, C, R> Upsert<'r, C, R>
where
	C: Connection,
	R: TryFromValue,
{
	/// Replaces the current document / record data with the specified data
	pub fn content(self, data: impl Into<Value>) -> Result<Content<'r, C, R>> {
		let data = data.into();

		ensure_values_are_objects(
			&data,
		)?;

		let command = Command::Upsert {
			what: self.resource?,
			data: match data {
				Value::None => None,
				content => Some(content),
			}
		};

		Ok(Content::new(self.client, command))
	}


	/// Merges the current document / record data with the specified data
	pub fn merge(self, data: Value) -> Merge<'r, C, R>
	{
		Merge {
			client: self.client,
			resource: self.resource,
			content: Data::MergeExpression(data),
			upsert: true,
			response_type: PhantomData,
		}
	}

	/// Patches the current document / record data with the specified JSON Patch data
	pub fn patch(self, patch: impl Into<PatchOp>) -> Patch<'r, C, R> {
		Patch {
			patches: PatchOps(vec![patch.into()]),
			client: self.client,
			resource: self.resource,
			upsert: true,
			response_type: PhantomData,
		}
	}
}
