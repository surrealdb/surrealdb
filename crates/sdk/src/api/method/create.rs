use super::transaction::WithTransaction;
use crate::Surreal;
use crate::opt::CreatableResource;

use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;

use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::{Data, TryFromValue};
use surrealdb_core::expr::{Value, to_value as to_core_value};
use surrealdb_protocol::proto::rpc::v1::CreateRequest;
use uuid::Uuid;

/// A record create future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Create<R: CreatableResource, RT> {
	pub(super) client: Surreal,
	pub(super) txn: Option<Uuid>,
	pub(super) what: R,
	pub(super) data: Data,
	pub(super) response_type: PhantomData<RT>,
}

impl<R, RT> WithTransaction for Create<R, RT>
where
	R: CreatableResource,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<R, RT> Create<R, RT> where R: CreatableResource {}

macro_rules! into_future {
	($method:ident) => {
		fn into_future(self) -> Self::IntoFuture {
			let Create {
				txn,
				client,
				what,
				data,
				..
			} = self;

			let what = what.into_values();

			Box::pin(async move {
				let client = client.client;

				let response = client
					.create(CreateRequest {
						txn: txn.map(|id| id.to_string()),
						what,
						data,
					})
					.await?;

				Ok(response.into())
			})
		}
	};
}

impl<R, RT> IntoFuture for Create<R, RT>
where
	R: CreatableResource,
	RT: TryFromValue,
{
	type Output = Result<RT>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Create {
			txn,
			client,
			what,
			data,
			..
		} = self;

		let what = what.into_values();

		Box::pin(async move {
			let client = client.client;

			todo!("STU: Implement CreateResponse");
			// let response = client.create(CreateRequest {
			// 	txn: txn.map(|id| id.try_into()).transpose()?,
			// 	what: what.try_into()?,
			// 	data: data.map(|data| data.try_into()).transpose()?,
			// 	..Default::default()
			// }).await?;

			// Ok(response.into())
		})
	}
}

impl<R, RT> Create<R, RT>
where
	R: CreatableResource,
	RT: TryFromValue,
{
	/// Sets content of a record
	pub fn content(self, data: impl Into<Value>) -> Create<R, RT> {
		let content = data.into();

		let data = match content {
			Value::None | Value::Null => Data::EmptyExpression,
			content => Data::ContentExpression(content),
		};

		Self {
			txn: self.txn,
			client: self.client,
			what: self.what,
			data,
			response_type: PhantomData,
		}
	}
}
