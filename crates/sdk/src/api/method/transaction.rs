use crate::{
	api::{conn::Command, OnceLockExt, Result},
	opt::{CreateResource, Resource, Table},
	Connection, Response, Surreal, Value,
};
use serde::Serialize;
use std::{borrow::Cow, marker::PhantomData};
use surrealdb_core::sql::{
	statements::{self, BeginStatement, CommitStatement},
	Object as CoreObject, Values as CoreValues,
};
use surrealdb_core::sql::{
	statements::{CreateStatement, DeleteStatement},
	to_value, Data, Output, Query, Statement,
};

pub struct TransactionBuilder<'r, C: Connection> {
	client: Cow<'r, Surreal<C>>,
	statements: Vec<Statement>,
}

pub struct TransactionBuilderCreate<'r, C: Connection, R> {
	builder: TransactionBuilder<'r, C>,
	statement: CreateStatement,
	response_type: PhantomData<R>,
}

impl<'r, C, R> TransactionBuilderCreate<'r, C, R>
where
	C: Connection,
{
	pub fn content<D>(mut self, obj: D) -> Result<TransactionBuilder<'r, C>>
	where
		D: 'static + Serialize,
	{
		self.statement.data = Some(Data::ContentExpression(to_value(obj)?));
		self.builder.statements.push(Statement::Create(self.statement));
		Ok(self.builder)
	}
}

// TODO: Copied from engine/mod.rs (where to put this file to share the resource_to_values?)
#[allow(dead_code)]
fn resource_to_values(r: Resource) -> CoreValues {
	let mut res = CoreValues::default();
	match r {
		Resource::Table(x) => {
			res.0 = vec![Table(x).into_core().into()];
		}
		Resource::RecordId(x) => res.0 = vec![x.into_inner().into()],
		Resource::Object(x) => res.0 = vec![x.into_inner().into()],
		Resource::Array(x) => res.0 = Value::array_to_core(x),
		Resource::Edge(x) => res.0 = vec![x.into_inner().into()],
		Resource::Range(x) => res.0 = vec![x.into_inner().into()],
		Resource::Unspecified => {}
	}
	res
}

#[allow(dead_code)]
impl<'r, C> TransactionBuilder<'r, C>
where
	C: Connection,
{
	pub fn new(client: Cow<'r, Surreal<C>>) -> Self {
		Self {
			client,
			statements: vec![],
		}
	}

	pub fn query(mut self, query: Query) -> Self {
		self.statements.extend(query.into_iter());
		self
	}

	pub fn create<R>(
		self,
		resource: impl CreateResource<Option<R>>,
	) -> Result<TransactionBuilderCreate<'r, C, R>> {
		let statement = {
			let mut stmt = CreateStatement::default();
			stmt.what = resource_to_values(resource.into_resource()?);
			stmt.output = Some(Output::After);
			stmt
		};
		Ok(TransactionBuilderCreate {
			statement,
			builder: self,
			response_type: PhantomData,
		})
	}

	pub fn delete<R>(mut self, resource: impl CreateResource<Option<R>>) -> Result<Self> {
		let statement = {
			let mut stmt = DeleteStatement::default();
			stmt.what = resource_to_values(resource.into_resource()?);
			stmt.output = Some(Output::Before);
			stmt
		};
		self.statements.push(Statement::Delete(statement));
		Ok(self)
	}

	pub async fn end_transaction(mut self) -> Result<Response> {
		let mut final_query = Query::default();
		final_query.0 .0.push(Statement::Begin(BeginStatement::default()));
		final_query.0 .0.append(&mut self.statements);
		final_query.0 .0.push(Statement::Commit(CommitStatement::default()));
		let router = self.client.router.extract()?;
		Ok(router
			.execute_query(Command::Query {
				query: final_query,
				variables: CoreObject::default(),
			})
			.await?)
	}
}
