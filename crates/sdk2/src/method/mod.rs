use anyhow::Result;
use std::{
	future::{Future, IntoFuture},
	pin::Pin,
};
use uuid::Uuid;

mod query;
pub use query::*;

mod select;
pub use select::*;

mod r#use;
pub use r#use::*;

use crate::{api::SurrealContext, controller::Controller};

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
	pub fn new(ctx: &(impl SurrealContext + ?Sized), inner: R) -> Self {
		Self {
			controller: ctx.controller().clone(),
			session_id: ctx.session_id(),
			tx_id: ctx.tx_id(),
			inner,
		}
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
