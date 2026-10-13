//! The rows of a value the VM already holds, an array, a set, the rows of a nested statement or an
//! integer range, run as the pipeline's decision says.
//!
//! A held source has no producer that can fail and no transaction to read, so its rows are handed
//! to [`Rows`] as they are: where the decision is `Morsels` with
//! [`MorselInput::Batches`](crate::exec::planner::placement::MorselInput::Batches) the coordinator
//! gathers them into batches and widens as it does over a scan's decoded rows, and everywhere else
//! each row runs on the statement's own state, one at a time, in the order the value holds them.
//! [`Rows::of`] is the one judge of which, through [`effective_mode`](super::effective_mode).
//!
//! An element is moved into its row and never cloned; a range is generated one position at a time
//! and never materialised.
//!
//! # Per-worker closure invocation
//!
//! A worker runs a body that calls a closure through the program's closure units, which
//! `RunShare` shares those units between the workers of a run, as every morsel body does. A loop
//! over a closure (`arr.map(f)`) runs as morsels only through a pipeline that its lowering emits.
//! The lowering of a closure loop emits no pipeline, so such a loop never runs as morsels. A
//! pipeline that does reach [`run_held_rows`] runs its rows the same way as any other held source.

use std::sync::Arc;

use super::super::host::Out;
use super::super::interp::State;
use super::super::program::{Pipeline, Program};
use super::Rows;
use crate::exec::{ExecutionContext, FlowResult};
use crate::val::Value;

/// The rows a held value yields, in order.
#[allow(dead_code, reason = "request L16-4 hands `run_source`'s held rows to `run_held_rows`")]
pub(in crate::exec::vm) enum HeldRows {
	/// The elements of an array or a set, moved into their rows.
	Values(Vec<Value>),
	/// The positions of an integer range, each a `Value::Number`.
	Range(common::range::IntegerRangeIter),
}

/// Run `pipeline`, one of `program`'s, over `rows`: as `Batches` morsels when its decision is
/// `Morsels` with `Batches` input, one row at a time otherwise. `true` when the pipeline takes no
/// more rows.
///
/// The rows are held already, so the source cannot fail midway and [`Rows::fail`] is never
/// called. A range with no end yields until the pipeline stops taking rows or the statement is
/// interrupted: each row spends the fuel that yields.
#[allow(dead_code, reason = "request L16-4 calls it from `run_source` for held rows")]
pub(in crate::exec::vm) async fn run_held_rows(
	program: &Arc<Program>,
	pipeline: &Pipeline,
	rows: HeldRows,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<bool> {
	// A held row has no sort keys, which a body that reads them finds empty.
	state.sorted_keys.clear();
	let mut sink = Rows::of(program, pipeline, ctx);
	match rows {
		HeldRows::Values(values) => {
			for value in values {
				if sink.row(program, pipeline, value, state, ctx, yielder, flush_at).await? {
					return Ok(true);
				}
			}
		}
		HeldRows::Range(range) => {
			for position in range {
				let value = Value::from(position);
				if sink.row(program, pipeline, value, state, ctx, yielder, flush_at).await? {
					return Ok(true);
				}
			}
		}
	}
	sink.finish(state, ctx, yielder, flush_at).await
}
