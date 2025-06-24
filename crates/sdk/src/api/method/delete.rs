use crate::Surreal;
use crate::opt::RangeableResource;

use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;
use crate::method::OnceLockExt;
use crate::opt::KeyRange;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::Value;

/// A record delete future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Delete<'r, C: Connection, R, RT> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: R,
	pub(super) response_type: PhantomData<RT>,
}

impl<C, R, RT> Delete<'_, C, R, RT>
where
	C: Connection,
	R: Resource,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Delete<'static, C, R, RT> {
		Delete {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Delete {
				client,
				resource,
				..
			} = self;
			let what = resource.into_values();
			Box::pin(async move {
				let router = client.inner.router.extract()?;
				router
					.$method(Command::Delete {
						what,
					})
					.await
			})
		}
	};
}

impl<'r, Client, R> IntoFuture for Delete<'r, Client, R, Value>
where
	Client: Connection,
	R: Resource,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R, RT> IntoFuture for Delete<'r, Client, R, Option<RT>>
where
	Client: Connection,
	R: Resource,
	RT: TryFromValue,
{
	type Output = Result<Option<RT>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, Client, R, RT> IntoFuture for Delete<'r, Client, R, Vec<RT>>
where
	Client: Connection,
	R: Resource,
	RT: TryFromValue,
{
	type Output = Result<Vec<RT>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_vec}
}

// impl<C, R, NewResource> Delete<'_, C, R, Value>
// where
// 	C: Connection,
// 	R: RangeableResource,
// {
// 	/// Restricts a range of records to delete
// 	pub fn range(self, range: impl Into<KeyRange>) -> Self {
// 		Self {
// 			resource: self.resource.with_range(range.into()),
// 			..self
// 		}
// 	}
// }

// impl<C, R, RT, NewResource> Delete<'_, C, R, Vec<RT>>
// where
// 	C: Connection,
// 	R: RangeableResource,
// 	NewResource: RangeableResource,
// {
// 	/// Restricts a range of records to delete
// 	pub fn range<'a>(self, range: impl Into<KeyRange>) -> Delete<'a, C, NewResource, Vec<RT>> {
// 		Delete {
// 			resource: self.resource.with_range(range.into()),
// 			client: Cow::Borrowed(self.client.as_ref()),
// 			response_type: PhantomData,
// 		}
// 	}
// }
