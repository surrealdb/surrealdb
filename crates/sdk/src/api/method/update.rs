use super::transaction::WithTransaction;
use crate::Surreal;
use crate::opt::PatchOp;

use crate::api::Result;
use crate::api::method::BoxFuture;
use crate::api::opt::Resource;

use anyhow::Context;
use futures::StreamExt;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::Data;
use surrealdb_core::expr::Value;
use surrealdb_core::protocol::TryFromValue;
use surrealdb_core::sql::statements::UpdateStatement;
use surrealdb_protocol::proto::rpc::v1::QueryRequest;
use surrealdb_protocol::{QueryResponseValueStream, TryIntoValue};
use uuid::Uuid;

use crate::opt::{KeyRange, RangeableResource};
use surrealdb_core::expr::Thing as RecordId;

/// An update future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Update<R: Resource, RT> {
	pub(super) client: Surreal,
	pub(super) txn: Option<Uuid>,
	pub(super) what: R,
	pub(super) data: Data,
	pub(super) response_type: PhantomData<RT>,
}

impl<R, RT> WithTransaction for Update<R, RT>
where
	R: Resource,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<R, RT> IntoFuture for Update<R, RT>
where
	R: Resource,
	RT: TryFromValue,
{
	type Output = Result<RT>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Update {
			txn,
			client,
			what,
			data,
			..
		} = self;

		let what = what.into_values();

		Box::pin(async move {
			let mut client = client.client.clone();

			let mut stmt = UpdateStatement::default();
			stmt.what = what.into();
			stmt.data = Some(data.try_into()?);

			let response = client
				.query(QueryRequest {
					txn_id: txn.map(|id| id.into()),
					query: stmt.to_string(),
					variables: None,
				})
				.await?;

			let mut stream = QueryResponseValueStream::new(response.into_inner());

			let value = stream.next().await.context("No response from server")??;

			Ok(RT::try_from_value(value)?)
		})
	}
}

// impl<C, R> Update<'_, C, R, Value>
// where
// 	C
// 	R: RangeableResource,
// {
// 	/// Restricts the records to update to those in the specified range
// 	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
// 		self.resource = self.resource.with_range(range.into());
// 		self
// 	}
// }

impl<R, RT> Update<R, RT>
where
	R: RangeableResource,
{
	/// Restricts the records to update to those in the specified range
	pub fn range(self, range: impl Into<KeyRange>) -> Update<RecordId, RT> {
		Update {
			what: self.what.with_range(range.into()),
			client: self.client,
			txn: self.txn,
			data: self.data,
			response_type: PhantomData,
		}
	}
}

impl<R, RT> Update<R, RT>
where
	R: Resource,
	RT: TryFromValue,
{
	/// Replaces the current document / record data with the specified data
	pub fn content<V>(self, value: V) -> Update<R, RT>
	where
		V: TryIntoValue,
	{
		let value = value.try_into_value().unwrap();

		let data = if value.is_none() {
			Data::EmptyExpression
		} else {
			Data::ContentExpression(value.try_into().unwrap())
		};

		Self {
			txn: self.txn,
			client: self.client,
			what: self.what,
			data,
			response_type: PhantomData,
		}
	}

	/// Merges the current document / record data with the specified data
	pub fn merge<V>(self, value: V) -> Update<R, RT>
	where
		V: TryIntoValue,
	{
		let value = value.try_into_value().unwrap();

		Self {
			txn: self.txn,
			client: self.client,
			what: self.what,
			data: Data::MergeExpression(value.try_into().unwrap()),
			response_type: PhantomData,
		}
	}

	/// Patches the current document / record data with the specified JSON Patch data
	pub fn patch(self, patch: impl Into<PatchOp>) -> Update<R, RT> {
		let patch = patch.into().into();
		Self {
			txn: self.txn,
			client: self.client,
			what: self.what,
			data: Data::PatchExpression(patch),
			response_type: PhantomData,
		}
	}
}
