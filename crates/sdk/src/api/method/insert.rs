use super::transaction::WithTransaction;
use crate::Surreal;
use crate::opt::InsertableResource;

use crate::api::Result;
use crate::api::method::BoxFuture;

use anyhow::Context;
use futures::StreamExt;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::Data;
use surrealdb_core::expr::Value;
use surrealdb_core::protocol::TryFromValue;
use surrealdb_core::sql::Output;
use surrealdb_core::sql::statements::InsertStatement;
use surrealdb_protocol::QueryResponseValueStream;
use surrealdb_protocol::TryIntoValue;
use surrealdb_protocol::proto::rpc::v1::QueryRequest;
use uuid::Uuid;

use super::insert_relation::InsertRelation;

/// An insert future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Insert<R: InsertableResource, RT> {
	pub(super) client: Surreal,
	pub(super) txn: Option<Uuid>,
	pub(super) what: R,
	pub(super) data: Data,
	pub(super) response_type: PhantomData<RT>,
}

impl<R, RT> WithTransaction for Insert<R, RT>
where
	R: InsertableResource,
{
	fn with_transaction(mut self, id: Uuid) -> Self {
		self.txn = Some(id);
		self
	}
}

impl<R, RT> IntoFuture for Insert<R, RT>
where
	R: InsertableResource,
	RT: TryFromValue,
{
	type Output = Result<RT>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Insert {
			txn,
			client,
			what,
			data,
			..
		} = self;

		let table_name = what.table_name().to_string();

		Box::pin(async move {
			let mut client = client.client.clone();

			let mut stmt = InsertStatement::default();
			stmt.into = Some(surrealdb_core::sql::Table::from(table_name).into());
			stmt.data = data.into();
			stmt.output = Some(Output::After);

			let resp = client
				.query(QueryRequest {
					txn_id: txn.map(|id| id.into()),
					query: stmt.to_string(),
					variables: Default::default(),
				})
				.await?;

			let mut stream = QueryResponseValueStream::new(resp.into_inner());

			let first = stream.next().await.context("Failed to get first response")??;

			let value = RT::try_from_value(first)?;

			Ok(value)
		})
	}
}

impl<R, RT> Insert<R, RT>
where
	R: InsertableResource,
	RT: TryFromValue,
{
	/// Specifies the data to insert into the table
	pub fn content<V>(self, value: V) -> Insert<R, RT>
	where
		V: TryIntoValue,
	{
		let value = value.try_into_value().unwrap();

		Self {
			txn: self.txn,
			client: self.client,
			what: self.what,
			data: Data::ContentExpression(value.try_into().unwrap()),
			response_type: PhantomData,
		}
	}
}

impl<R, RT> Insert<R, RT>
where
	R: InsertableResource,
	RT: TryFromValue,
{
	/// Specifies the data to insert into the table
	pub fn relation(self, data: Value) -> InsertRelation<R, RT> {
		InsertRelation {
			client: self.client,
			txn_id: self.txn,
			what: self.what,
			data,
			response_type: PhantomData,
		}
	}
}
