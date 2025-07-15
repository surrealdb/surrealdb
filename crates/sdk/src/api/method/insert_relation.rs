use crate::Surreal;
use crate::opt::InsertableResource;

use crate::api::Result;

use anyhow::Context;
use futures::StreamExt;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::Value;
use surrealdb_core::protocol::TryFromValue;
use surrealdb_core::sql::Data;
use surrealdb_core::sql::Output;
use surrealdb_core::sql::statements::InsertStatement;
use surrealdb_protocol::QueryResponseValueStream;
use surrealdb_protocol::proto::rpc::v1::QueryRequest;

use super::BoxFuture;

/// An Insert Relation future
///
///
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct InsertRelation<R, RT> {
	pub(super) client: Surreal,
	pub(super) txn_id: Option<uuid::Uuid>,
	pub(super) what: R,
	pub(super) data: Value,
	pub(super) response_type: PhantomData<RT>,
}

impl<R, RT> IntoFuture for InsertRelation<R, RT>
where
	R: InsertableResource,
	RT: TryFromValue,
{
	type Output = Result<RT>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let InsertRelation {
			client,
			what,
			mut data,
			txn_id,
			..
		} = self;

		let table_name = what.table_name().to_string();
		what.augment_data(&mut data);

		Box::pin(async move {
			let mut stmt = InsertStatement::default();
			stmt.into = Some(surrealdb_core::sql::Table::from(table_name).into());
			stmt.data = Data::SingleExpression(data.into());
			stmt.output = Some(Output::After);
			stmt.relation = true;

			let mut client = client.client.clone();

			let resp = client
				.query(QueryRequest {
					txn_id: txn_id.map(|id| id.into()),
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
