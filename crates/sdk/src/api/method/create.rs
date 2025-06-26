use super::transaction::WithTransaction;
use crate::Surreal;
use crate::opt::CreatableResource;

use crate::api::Connection;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;
use crate::method::OnceLockExt;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::{Value, to_value as to_core_value};
use uuid::Uuid;

use super::Content;

/// A record create future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Create<'r, C: Connection, R: CreatableResource, RT> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) txn: Option<Uuid>,
	pub(super) resource: R,
	pub(super) response_type: PhantomData<RT>,
}

impl<C, R, RT> WithTransaction for Create<'_, C, R, RT>
where
	C: Connection,
	R: CreatableResource,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<C, R, RT> Create<'_, C, R, RT>
where
	C: Connection,
	R: CreatableResource,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Create<'static, C, R, RT> {
		Create {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Create {
				txn,
				client,
				resource,
				..
			} = self;

			let what = resource.into_values();

			Box::pin(async move {
				let router = client.inner.router.extract()?;

				let cmd = Command::Create {
					txn,
					what,
					data: None,
				};
				router.$method(cmd).await
			})
		}
	};
}

impl<'r, Client, R> IntoFuture for Create<'r, Client, R, Value>
where
	Client: Connection,
	R: CreatableResource,
{
	type Output = Result<Value>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_value}
}

impl<'r, Client, R, RT> IntoFuture for Create<'r, Client, R, Option<RT>>
where
	Client: Connection,
	R: CreatableResource,
	RT: TryFromValue,
{
	type Output = Result<Option<RT>>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	into_future! {execute_opt}
}

impl<'r, C, R> Create<'r, C, R, Value>
where
	C: Connection,
	R: CreatableResource,
{
	/// Sets content of a record
	pub fn content(self, data: impl Into<Value>) -> Content<'r, C, Value> {
		let content = data.into();

		let data = match content {
			Value::None | Value::Null => None,
			content => Some(content),
		};

		let command = Command::Create {
			txn: self.txn,
			what: self.resource.into_values(),
			data,
		};

		Content::new(self.client, command)
	}
}

impl<'r, C, R, RT> Create<'r, C, R, Option<RT>>
where
	C: Connection,
	R: CreatableResource,
{
	/// Sets content of a record
	pub fn content(self, data: impl Into<Value>) -> Content<'r, C, Option<RT>> {
		let content = data.into();

		let data = match content {
			Value::None | Value::Null => None,
			content => Some(content),
		};

		let command = Command::Create {
			txn: self.txn,
			what: self.resource.into_values(),
			data,
		};

		Content::new(self.client, command)
	}
}
