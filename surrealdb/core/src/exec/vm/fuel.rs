//! Pre-emption: the fuel a running program spends, and the one place it
//! yields.
//!
//! Every unit of work a program does draws on one meter: a row a pipeline
//! takes, an iteration of a loop (a backward jump of a body), a step a body
//! hands the host, the elements a bulk instruction copies, the elements a
//! builtin walks, the entries a storage read returns, and the elements of a
//! buffer a host service walks — a sort run, a merge, a group finished, a join
//! built, a graph walked. When the meter is spent, the interpreter answers
//! [`Step::Yield`](super::interp::Step), a host service reaches
//! [`yield_point`], and a builtin's walk asks its driver ([`metered`]) to
//! reach it. The yield point checks the deadline and the cancellation flag,
//! refills the meter with the quantum
//! ([`ExecConfig::vm_quantum`](crate::exec::config::ExecConfig::vm_quantum))
//! and gives up the thread through the host's scheduler
//! ([`crate::exec::scheduler`]). Nothing else in the VM yields for the sake of
//! fairness, and nothing else reads the clock to check a deadline.
//!
//! # The bound
//!
//! Between two yields a task does at most one quantum of units, plus the
//! overshoot of the one charge that spent the meter. A walk charges a stretch
//! at a time, no longer than the fuel left, and stops for a yield mid-walk: a
//! builtin over its input ([`surrealdb_runtime::fuel`]), a bulk instruction's
//! copy of a collection, a sort's runs and merge, a key count. A storage read
//! asks for no more entries than the fuel left ([`batch`]) and is charged when
//! it returns, so it overshoots by that read. A key count examines no more
//! keys a call than the fuel left; on a backend that counts off the calling
//! thread, the rest of a large range is counted in parts, a quantum of keys a
//! call with several in flight, each charged as it returns.
//!
//! What is charged whole, overshooting by its own size, is a single operation
//! on one in-memory value that is not a walk: an operator over two collections
//! (`CONTAINS`, `=`), a path step that flattens or slices, a builtin that is
//! not a walk (a regular expression, a digest, a vector kernel), and the read
//! and conversion of a builtin's arguments, each charged by the value's
//! [`weight`]. A sort run, a top-k prune and a top-k merge's run sort no more
//! than a quantum of rows at once, and a merge places no more than a quantum
//! of rows at a time.
//!
//! # One meter per thread
//!
//! The units are the thread's, not a state's. A statement nests work on
//! states of its own — a closure body, a dispatched unit, a subquery, a host
//! pipeline — each made and dropped within the poll of the statement's task.
//! A poll runs on one thread, so the thread's meter is the poll's: everything
//! the poll does draws on it, however it nests, and the bound holds for the
//! poll whatever the statement. A task that another task's yield left with
//! less than a quantum yields sooner; the meter is refilled only at a yield
//! point, just before the task gives up the thread, so no poll starts with
//! more than one quantum.

use std::future::Future;
use std::pin::pin;
use std::task::Poll;

// The meter is the thread's, held one crate down so that the builtins there
// draw on it too.
pub(crate) use surrealdb_runtime::fuel::{cap, charge, left, measure, refill, spent, units};

use crate::err::EngineError;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;

/// The units a value handed whole to or from a builtin weighs: an element of
/// an array, a set or an object, or [`BYTES_PER_UNIT`] bytes of a string or a
/// byte buffer. What the value nests is not walked to weigh it.
///
/// [`BYTES_PER_UNIT`]: surrealdb_runtime::fuel::BYTES_PER_UNIT
#[inline]
pub(crate) fn weight(value: &crate::val::Value) -> u32 {
	use surrealdb_runtime::fuel::BYTES_PER_UNIT;

	use crate::val::Value;
	match value {
		Value::Array(array) => units(array.len()),
		Value::Object(object) => units(object.len()),
		Value::Set(set) => units(set.len()),
		Value::String(string) => units(string.len() / BYTES_PER_UNIT),
		Value::Bytes(bytes) => units(bytes.len() / BYTES_PER_UNIT),
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => 0,
	}
}

/// The entries a storage call may take in one go: no more than `most`, nor
/// than the fuel left, so that the units a read charges once it returns never
/// overshoot the meter by more than the entries of one read.
#[inline]
pub(crate) fn batch(most: u32) -> u32 {
	most.min(left().max(1))
}

/// The quantum `ctx` runs under.
#[inline]
pub(crate) fn quantum(ctx: &ExecutionContext) -> u32 {
	ctx.root().ctx.config.exec.vm_quantum.max(1)
}

/// Raise the statement's cancellation, or its deadline once past it, or the
/// process's memory threshold once beyond it.
pub(crate) fn interrupted(ctx: &ExecutionContext) -> FlowResult<()> {
	#[cfg(test)]
	INTERRUPT_CHECKS.with(|checks| checks.set(checks.get() + 1));
	crate::exec::operators::check_cancelled(ctx)?;
	match ctx.ctx().done(true).map_err(ControlFlow::Err)? {
		None => Ok(()),
		Some(crate::ctx::reason::Reason::Canceled) => {
			Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)))
		}
		Some(crate::ctx::reason::Reason::Timedout(duration)) => {
			Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryTimedout(duration.0))))
		}
	}
}

#[cfg(test)]
thread_local! {
	/// How many times this thread checked a statement's deadline and
	/// cancellation, for tests that bound how often a loop reads the clock.
	pub(super) static INTERRUPT_CHECKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The one place a running program yields: raise an interruption
/// ([`interrupted`]), refill the thread's fuel with `ctx`'s quantum, then give
/// up the thread through the host's scheduler.
///
/// The meter is refilled before the thread is given up, so a task polled
/// next on this thread spends what this one leaves rather than finding the
/// meter spent and yielding without doing any work.
pub(crate) async fn yield_point(ctx: &ExecutionContext) -> FlowResult<()> {
	interrupted(ctx)?;
	refill(quantum(ctx));
	crate::exec::scheduler::note_yield();
	match ctx.ctx().scheduler() {
		Some(scheduler) => scheduler.yield_now().await,
		None => crate::exec::scheduler::yield_now().await,
	}
	Ok(())
}

/// Spend `units` of the thread's fuel, reaching a yield point once it is
/// spent: the charge of a host service's loop, between units of its work.
#[inline]
pub(crate) async fn tick(ctx: &ExecutionContext, units: u32) -> FlowResult<()> {
	if charge(units) {
		Box::pin(yield_point(ctx)).await?;
	}
	Ok(())
}

/// Run `walk` — a function or a host service walking a collection a stretch
/// at a time ([`surrealdb_runtime::fuel`]) — reaching a yield point each time
/// it spends the fuel, and polling it on from there.
///
/// A walk that waits on anything else is pending as that wait is: its waker
/// is the task's, so the task is polled again when the wait ends.
pub(crate) async fn metered<F: Future>(ctx: &ExecutionContext, walk: F) -> FlowResult<F::Output> {
	let mut walk = pin!(walk);
	loop {
		let polled = std::future::poll_fn(|cx| {
			surrealdb_runtime::fuel::take_asked();
			match walk.as_mut().poll(cx) {
				Poll::Ready(out) => Poll::Ready(Some(out)),
				Poll::Pending if surrealdb_runtime::fuel::take_asked() => Poll::Ready(None),
				Poll::Pending => Poll::Pending,
			}
		})
		.await;
		match polled {
			Some(out) => return Ok(out),
			None => Box::pin(yield_point(ctx)).await?,
		}
	}
}
