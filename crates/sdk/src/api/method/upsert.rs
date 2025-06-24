use crate::Surreal;
use crate::method::merge::Merge;
use crate::method::patch::Patch;
use crate::opt::PatchOp;
use crate::opt::PatchOps;
use crate::opt::RangeableResource;

use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::method::Content;
use crate::api::opt::Resource;
use crate::method::OnceLockExt;
use crate::opt::KeyRange;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::Data;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::{Value, to_value as to_core_value};

/// An upsert future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Upsert<'r, C: Connection, R: Resource, RT> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: R,
	pub(super) response_type: PhantomData<RT>,
}

impl<C, R, RT> Upsert<'_, C, R, RT>
where
	C: Connection,
	R: Resource,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Upsert<'static, C, R, RT> {
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
						what: resource.into_values(),
						data: None,
					})
					.await
			})
		}
	};
}

impl<'r, Client, R> IntoFuture for Upsert<'r, Client, R, Value>
where
	Client: Connection,
	R: Resource,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R, RT> IntoFuture for Upsert<'r, Client, R, Option<RT>>
where
	Client: Connection,
	R: Resource,
	RT: TryFromValue,
{
	type Output = Result<Option<RT>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R, RT> IntoFuture for Upsert<'r, Client, R, Vec<RT>>
where
	Client: Connection,
	R: Resource,
	RT: TryFromValue,
{
	type Output = Result<Vec<RT>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

// impl<C, R> Upsert<'_, C, R, Value>
// where
// 	C: Connection,
// 	R: RangeableResource,
// {
// 	/// Restricts the records to upsert to those in the specified range
// 	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
// 		self.resource = self.resource.with_range(range.into());
// 		self
// 	}
// }

// impl<C, R, RT> Upsert<'_, C, R, Vec<RT>>
// where
// 	C: Connection,
// 	R: RangeableResource,
// {
// 	/// Restricts the records to upsert to those in the specified range
// 	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
// 		self.resource = self.resource.with_range(range.into());
// 		self
// 	}
// }

impl<'r, C, R, RT> Upsert<'r, C, R, RT>
where
	C: Connection,
	R: Resource,
	RT: TryFromValue,
{
	/// Replaces the current document / record data with the specified data
	pub fn content(self, data: impl Into<Value>) -> Content<'r, C, RT> {
		let data = data.into();

		let command = Command::Upsert {
			what: self.resource.into_values(),
			data: match data {
				Value::None => None,
				content => Some(Data::ContentExpression(content)),
			},
		};

		Content::new(self.client, command)
	}

	/// Merges the current document / record data with the specified data
	pub fn merge(self, data: Value) -> Merge<'r, C, R, RT> {
		Merge {
			client: self.client,
			resource: self.resource,
			content: Data::MergeExpression(data),
			upsert: true,
			response_type: PhantomData,
		}
	}

	/// Patches the current document / record data with the specified JSON Patch data
	pub fn patch(self, patch: impl Into<PatchOp>) -> Patch<'r, C, R, RT> {
		Patch {
			patches: PatchOps(vec![patch.into()]),
			client: self.client,
			resource: self.resource,
			upsert: true,
			response_type: PhantomData,
		}
	}
}
