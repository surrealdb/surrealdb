//! A morsel's sorted run.
//!
//! A pipeline whose sink is a sort that keeps every row (`Combine::StableMerge`) fills each
//! morsel's sort with the entries of the morsel's rows, in arrival order. When the morsel ends it
//! sorts them, stably, on the thread that ran it: the entries it hands the merge are a run, ordered
//! by the sort's keys, ties in the order the morsel's rows arrived.
//!
//! The merge takes the runs in the order the serial run reads the morsels, each after the entries
//! the statement already holds, so equal keys stay in arrival order in the statement's buffer.
//! The statement's sort is stable. It merges the entries it holds a quantum at a time with a loser
//! tree, and on a tie the earlier run wins (`surrealdb_runtime::fuel::sort_by`). A quantum that
//! lies inside one morsel's run is already in order, so the statement's sort does the merging. The
//! rows come out in the order that sorting every row at once gives, with ties in arrival order,
//! which is the serial order.
//!
//! A morsel whose sort outgrew its share of the query's lease spills its own runs, as the serial
//! sort does. Its sorted buffer and runs reach the statement as a
//! [`Partial`](super::super::partial::Partial). They merge with the statement's runs in order,
//! using the same loser tree at the query's fan-in. Merge passes beyond the fan-in are done by the
//! spill.
//!
//! A sort that keeps a bounded number of rows (`Combine::TopK`) prunes each morsel's entries as the
//! serial sort prunes the whole, and every sort of the morsels of one scan publishes the threshold
//! it holds into the scan's one cell, which only tightens
//! ([`TopKThresholdCell::tighten`](crate::exec::topk_pushdown::TopKThresholdCell::tighten)).

use super::super::interp::State;
use super::super::program::Program;
use super::super::sort::sorted;
use crate::exec::{ExecutionContext, FlowResult};

/// Whether sort `sort` of `program` takes its entries from morsels as sorted runs: it keeps every
/// row, in the order its keys give, and no row of it is anything but an entry of its own.
pub(super) fn takes_runs(program: &Program, sort: usize) -> bool {
	let spec = &program.sorts[sort];
	spec.keep.is_none() && !spec.random && spec.knn_slot.is_none()
}

/// Sort the entries a morsel's `state` holds for `sort` into the morsel's run, where the sort takes
/// runs ([`takes_runs`]) and holds any. The entries keep their own keys and rows; only their order
/// changes, stably.
pub(super) async fn build_run(
	state: &mut State,
	program: &Program,
	sort: usize,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	if !takes_runs(program, sort) || state.sorts[sort].len() < 2 {
		return Ok(());
	}
	let entries = std::mem::take(&mut state.sorts[sort]);
	state.sorts[sort] = sorted(entries, &program.sorts[sort].keys, usize::MAX, ctx).await?;
	#[cfg(test)]
	super::test_hooks::note_run(ctx);
	Ok(())
}
