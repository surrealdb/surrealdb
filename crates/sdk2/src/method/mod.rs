use anyhow::Result;
use futures::Stream;
use surrealdb_types::{QueryChunk, SurrealValue, Value, Variables};
use std::{
	future::{Future, IntoFuture},
	pin::Pin,
};
use uuid::Uuid;

mod query;
pub use query::*;

mod select;
pub use select::*;

mod create;
pub use create::*;

mod delete;
pub use delete::*;

mod insert;
pub use insert::*;

mod update;
pub use update::*;

mod upsert;
pub use upsert::*;

mod relate;
pub use relate::*;

mod r#use;
pub use r#use::*;

use crate::{api::SurrealContext, controller::Controller, utils::{QueryResult, ValueStream}};

/// Generic request builder that holds context and the request data.
///
/// This combines the execution context (controller, session, transaction)
/// with the request itself.
#[derive(Clone)]
pub struct Request<R> {
	pub(crate) controller: Controller,
	pub(crate) session_id: Option<Uuid>,
	pub(crate) tx_id: Option<Uuid>,
	pub(crate) inner: R,
}

impl<R> SurrealContext for Request<R> {
	fn controller(&self) -> Controller {
		self.controller.clone()
	}

	fn session_id(&self) -> Option<Uuid> {
		self.session_id
	}

	fn tx_id(&self) -> Option<Uuid> {
		self.tx_id
	}
}

impl<R> Request<R> {
	pub(crate) fn new(ctx: &(impl SurrealContext + ?Sized), inner: R) -> Self {
		Self {
			controller: ctx.controller().clone(),
			session_id: ctx.session_id(),
			tx_id: ctx.tx_id(),
			inner,
		}
	}

	/// Extract the inner value and return both the inner and a new Request builder
	/// that can create a Request with a different inner type using the same context.
	pub(crate) fn split(self) -> (R, RequestContext) {
		(
			self.inner,
			RequestContext {
				controller: self.controller,
				session_id: self.session_id,
				tx_id: self.tx_id,
			},
		)
	}
}

/// Context extracted from a Request, used to create new Requests with the same context.
pub(crate) struct RequestContext {
	pub(crate) controller: Controller,
	pub(crate) session_id: Option<Uuid>,
	pub(crate) tx_id: Option<Uuid>,
}

impl RequestContext {
	pub(crate) fn into_request<R>(self, inner: R) -> Request<R> {
		Request {
			controller: self.controller,
			session_id: self.session_id,
			tx_id: self.tx_id,
			inner,
		}
	}
}

impl SurrealContext for RequestContext {
	fn controller(&self) -> Controller {
		self.controller.clone()
	}

	fn session_id(&self) -> Option<Uuid> {
		self.session_id
	}

	fn tx_id(&self) -> Option<Uuid> {
		self.tx_id
	}
}

/// Trait for request types that can be executed.
///
/// The request type is pure data describing what to execute.
/// The `execute` method receives the full `Request<Self>` with all context.
pub trait Executable: Clone + Send + 'static {
	/// The output type after execution
	type Output: Clone + Send + Sync + 'static;

	/// Execute this request with its context.
	fn execute(req: Request<Self>) -> impl Future<Output = Result<Self::Output>> + Send;
}

impl<R: Executable> IntoFuture for Request<R> {
	type Output = Result<R::Output>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(R::execute(self))
	}
}

pub trait QueryExecutable: Clone + Send + 'static {
	fn query(self) -> (String, Variables);
}

impl<R: QueryExecutable> Request<R> {
	pub async fn collect<T: SurrealValue>(self) -> Result<T> {
		let (inner, ctx) = self.split();
		let (sql, vars) = inner.query();
		let results = ctx.into_request(Query::new(sql)).bind(vars).collect().await?;
		let result = results
			.into_iter()
			.next()
			.ok_or_else(|| anyhow::anyhow!("No result returned from query"))?;
		result.into_t()
	}

	pub async fn stream<T: SurrealValue>(self) -> Result<ValueStream<Pin<Box<dyn Stream<Item = QueryChunk> + Send>>, T>> {
		let (inner, ctx) = self.split();
		let (sql, vars) = inner.query();
		let stream = ctx.into_request(Query::new(sql)).bind(vars).stream().await?;
		Ok(stream.into_value_stream::<T>(0))
	}

	pub fn with_stats(self) -> Request<WithStats<R>> {
		let (inner, ctx) = self.split();
		Request::new(&ctx, WithStats { inner })
	}
}

impl<R: QueryExecutable> Executable for R {
	type Output = Value;

	fn execute(req: Request<Self>) -> impl Future<Output = Result<Self::Output>> + Send {
		req.collect()
	}
}

#[derive(Clone)]
pub struct WithStats<R: QueryExecutable> {
	pub(crate) inner: R,
}

impl<R: QueryExecutable> Request<WithStats<R>> {
	pub async fn collect<T: SurrealValue>(self) -> Result<QueryResult<T>> {
		let (inner, ctx) = self.split();
		let (sql, vars) = inner.inner.query();
		let results = ctx.into_request(Query::new(sql)).bind(vars).collect().await?;
		let result = results
			.into_iter()
			.next()
			.ok_or_else(|| anyhow::anyhow!("No result returned from query"))?;
		result.into_typed()
	}

	pub async fn stream<T: SurrealValue>(self) -> Result<ValueStream<Pin<Box<dyn Stream<Item = QueryChunk> + Send>>, T>> {
		let (inner, ctx) = self.split();
		let (sql, vars) = inner.inner.query();
		let stream = ctx.into_request(Query::new(sql)).bind(vars).stream().await?;
		Ok(stream.into_value_stream::<T>(0))
	}
}

impl<R: QueryExecutable> Executable for WithStats<R> {
	type Output = QueryResult<Value>;

	fn execute(req: Request<Self>) -> impl Future<Output = Result<Self::Output>> + Send {
		req.collect()
	}
}