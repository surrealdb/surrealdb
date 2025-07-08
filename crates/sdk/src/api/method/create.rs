use super::transaction::WithTransaction;
use crate::Surreal;
use crate::opt::CreatableResource;

use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;
use anyhow::anyhow;
use futures::StreamExt;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::{Data, TryFromValue};
use surrealdb_core::expr::{Value, to_value as to_core_value};
use surrealdb_core::sql::statements::CreateStatement;
use surrealdb_protocol::proto::rpc::v1::QueryRequest;
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

				let mut create_statement = CreateStatement {
					what,
					data,
				};

				let response = client
					.query(QueryRequest {
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
			let mut client = client.client;

			let mut create_statement = CreateStatement::default();
			create_statement.what = what.into();
			create_statement.data = Some(data.into());

			let query = create_statement.to_string();

			let response = client
				.query(QueryRequest {
					txn_id: txn.map(|id| id.try_into()).transpose()?,
					query,
					variables: None,
				})
				.await?;

			let mut response = response.into_inner();

			while let Some(result) = response.next().await {
				let mut query_response = result?;

				if let Some(err) = query_response.error {
					return Err(anyhow!("{}", err.message));
				}

				if query_response.values.is_empty() {
					return Err(anyhow!("No values returned"));
				}

				let value = query_response.values.remove(0);

				let value = Value::try_from(value)?;

				return RT::try_from_value(value);
			}

			Err(anyhow!("Failed to get response"))
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
