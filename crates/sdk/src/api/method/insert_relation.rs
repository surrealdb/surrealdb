use crate::Surreal;


use crate::api::Result;
use crate::api::conn::Command;

use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::Value;

use super::BoxFuture;

/// An Insert Relation future
///
///
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct InsertRelation<R> {
	pub(super) client: Surreal,
	pub(super) command: Result<Command>,
	pub(super) response_type: PhantomData<R>,
}

impl<R> InsertRelation<R>
{
	pub(crate) fn from_closure<F>(client: Surreal, f: F) -> Self
	where
		F: FnOnce() -> Result<Command>,
	{
		InsertRelation {
			client,
			command: f(),
			response_type: PhantomData,
		}
	}
}

impl<RT> IntoFuture for InsertRelation<RT>
where
	RT: TryFromValue,
{
	type Output = Result<RT>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let InsertRelation {
			client,
			command,
			..
		} = self;
		Box::pin(async move {
			todo!("STU: Implement InsertRelation::into_future");
			// let router = client.inner.router.extract()?;
			// router.$method(command?).await
		})
	}
}
