use crate::Surreal;
use crate::api::Connection;
use crate::api::Result;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::Data;
use surrealdb_core::expr::Value;
use surrealdb_core::expr::TryFromValue;

/// A merge future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Merge<'r, C: Connection, R: Resource, RT> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) resource: R,
	pub(super) content: Data,
	pub(super) upsert: bool,
	pub(super) response_type: PhantomData<RT>,
}

impl<C, R, RT> Merge<'_, C, R, RT>
where
	C: Connection,
	R: Resource,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Merge<'static, C, R, RT> {
		Merge {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	() => {
		fn into_future(self) -> Self::IntoFuture {
			let Merge {
				client,
				resource,
				content,
				upsert,
				..
			} = self;
			Box::pin(async move {
				todo!("STU: Implement Merge with content handling");
				// let content = match content {
				// 	Value::None | Value::Null => None,
				// 	data => {
				// 		ensure_values_are_objects(
				// 			&data,
				// 			"Tried to merge non-object-like data, only structs and objects are supported",
				// 		)?;
				// 		Some(data)
				// 	}
				// };

				// let router = client.inner.router.extract()?;
				// let cmd = if upsert {
				// 	Command::Upsert {
				// 		what: resource?,
				// 		data: Some(content),
				// 	}
				// } else {
				// 	Command::Update {
				// 		what: resource?,
				// 		data: Some(content),
				// 	}
				// };
				// router.execute_query(cmd).await?.take(0)
			})
		}
	};
}

impl<'r, Client, R> IntoFuture for Merge<'r, Client, R, Value>
where
	Client: Connection,
	R: Resource,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}

impl<'r, Client, R, RT> IntoFuture for Merge<'r, Client, R, Option<RT>>
where
	Client: Connection,
	R: Resource,
	RT: TryFromValue,
{
	type Output = Result<Option<RT>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}

impl<'r, Client, R, RT> IntoFuture for Merge<'r, Client, R, Vec<RT>>
where
	Client: Connection,
	R: Resource,
	RT: TryFromValue,
{
	type Output = Result<Vec<R>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {}
}
