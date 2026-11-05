//! A request compiled as one program.
//!
//! Every statement of a request that arrives whole is planned and lowered
//! when the request arrives, into one program (`Program::request`). The
//! program holds the request's steps as data, and the program of each
//! statement, each with the units it needs. The driver runs the steps in
//! order. A transaction boundary, `USE`, `OPTION` and `LET` are their own
//! actions. A statement is a run of its program, through the same path as a
//! statement compiled on its own, so its rows stream and its value keeps its
//! usual shape.
//!
//! A plain `EXPLAIN`, whose operator renders the plan of another statement,
//! is planned on its own path. The driver keeps that operator beside the
//! program as [`super::RequestEntry::Operator`].

use std::sync::Arc;

use super::program::Program;
use super::speculate;
use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	ValueBatchStream, monitor_stream,
};

#[cfg(test)]
thread_local! {
	/// The shape of the last request program this thread built: its steps,
	/// its statement programs, and the units they share.
	pub(crate) static LAST: std::cell::RefCell<Option<(usize, usize, usize)>> =
		const { std::cell::RefCell::new(None) };
}

/// A request's program, with the catalog ids its statements' planning held,
/// kept for as long as the request is.
#[derive(Clone)]
pub(crate) struct CompiledRequest {
	pub(super) program: Arc<Program>,
	/// Each statement's late-bound block statements, as written.
	pub(super) served: Vec<Option<super::ServedStatements>>,
	pub(super) held: Vec<Arc<speculate::HeldIds>>,
}

impl CompiledRequest {
	/// The operator that runs statement `index` of the request.
	pub(crate) fn statement(&self, index: u32) -> Option<Arc<dyn ExecOperator>> {
		let statement = Arc::clone(self.program.request.as_ref()?.statements.get(index as usize)?);
		Some(Arc::new(RequestStatement {
			request: Arc::clone(&self.program),
			served: self.served.get(index as usize).cloned().flatten(),
			statement,
			held: self.held.clone(),
			metrics: Arc::new(OperatorMetrics::new()),
		}))
	}
}

/// One statement of a request's program, as the operator the driver runs:
/// the statement's program, run over its own units, or over the request's
/// units when it holds none.
#[derive(Debug)]
struct RequestStatement {
	request: Arc<Program>,
	/// The statement's late-bound block statements, as written.
	served: Option<super::ServedStatements>,
	statement: Arc<Program>,
	/// Kept for as long as a run of the statement may take the ids.
	held: Vec<Arc<speculate::HeldIds>>,
	metrics: Arc<OperatorMetrics>,
}

impl ExecOperator for RequestStatement {
	fn required_context(&self) -> ContextLevel {
		self.statement.required_context
	}

	fn access_mode(&self) -> AccessMode {
		self.statement.access_mode
	}

	fn output_shape(&self) -> crate::exec::OutputShape {
		super::host::output_shape(&self.statement)
	}

	fn execute_encoded(
		&self,
		ctx: &ExecutionContext,
		format: surrealdb_rpc::RowFormat,
	) -> FlowResult<ValueBatchStream> {
		self.run(ctx, Some(format))
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		self.run(ctx, None)
	}
}

impl RequestStatement {
	/// Run the statement, its own rows encoded in `format` when given.
	fn run(
		&self,
		ctx: &ExecutionContext,
		format: Option<surrealdb_rpc::RowFormat>,
	) -> FlowResult<ValueBatchStream> {
		// A statement that holds no units of its own indexes the request's,
		// which every program beneath it shares, closure bodies among them.
		let ctx = match self.statement.units.is_empty() {
			true => ctx.with_vm_units(Arc::clone(&self.request.units)),
			false => ctx.clone(),
		};
		let ctx = match &self.served {
			Some(served) => ctx.with_vm_served(Arc::clone(served)),
			None => ctx,
		};
		let ctx = super::closure::with_units(&self.request, &ctx).unwrap_or(ctx);
		let stream = super::host::execute_program_in(&self.statement, &ctx, None, format)?;
		let stream: ValueBatchStream = match self.held.is_empty() {
			true => stream,
			false => Box::pin(Holding {
				stream,
				_held: self.held.clone(),
			}),
		};
		Ok(monitor_stream(stream, super::host::VmOperator::NAME, &self.metrics))
	}
}

/// A run's stream keeping the catalog ids its draws take until it is dropped.
struct Holding {
	stream: ValueBatchStream,
	_held: Vec<Arc<speculate::HeldIds>>,
}

impl futures::Stream for Holding {
	type Item = <ValueBatchStream as futures::Stream>::Item;

	fn poll_next(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Self::Item>> {
		self.stream.as_mut().poll_next(cx)
	}
}
