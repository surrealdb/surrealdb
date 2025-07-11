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
use surrealdb_core::sql::statements::UpsertStatement;
use surrealdb_protocol::QueryResponseValueStream;
use surrealdb_protocol::proto::rpc::v1::QueryRequest;
use uuid::Uuid;

/// An upsert future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Upsert<R: Resource, RT> {
	pub(super) client: Surreal,
	pub(super) txn: Option<Uuid>,
	pub(super) what: R,
	pub(super) data: Data,
	pub(super) response_type: PhantomData<RT>,
}

impl<R, RT> WithTransaction for Upsert<R, RT>
where
	R: Resource,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<R, RT> Upsert<R, RT> where R: Resource {}

impl<R, RT> IntoFuture for Upsert<R, RT>
where
	R: Resource,
	RT: TryFromValue,
{
	type Output = Result<RT>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Upsert {
			txn,
			client,
			what,
			data,
			..
		} = self;

		let what = what.into_values();

		Box::pin(async move {
			let mut client = client.client.clone();

			let mut stmt = UpsertStatement::default();
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

// impl<C, R> Upsert<'_, C, R, Value>
// where
// 	C
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
// 	C
// 	R: RangeableResource,
// {
// 	/// Restricts the records to upsert to those in the specified range
// 	pub fn range(mut self, range: impl Into<KeyRange>) -> Self {
// 		self.resource = self.resource.with_range(range.into());
// 		self
// 	}
// }

impl<R, RT> Upsert<R, RT>
where
	R: Resource,
	RT: TryFromValue,
{
	/// Replaces the current document / record data with the specified data
	pub fn content(self, data: impl Into<Value>) -> Upsert<R, RT> {
		let data = data.into();

		Self {
			txn: self.txn,
			client: self.client,
			what: self.what,
			data: Data::ContentExpression(data),
			response_type: PhantomData,
		}
	}

	/// Merges the current document / record data with the specified data
	pub fn merge(self, data: Value) -> Upsert<R, RT> {
		Self {
			txn: self.txn,
			client: self.client,
			what: self.what,
			data: Data::MergeExpression(data),
			response_type: PhantomData,
		}
	}

	/// Patches the current document / record data with the specified JSON Patch data
	pub fn patch(self, patch: impl Into<PatchOp>) -> Upsert<R, RT> {
		let patch = patch.into();
		Self {
			txn: self.txn,
			client: self.client,
			what: self.what,
			data: Data::PatchExpression(patch.into()),
			response_type: PhantomData,
		}
	}
}
