//! Merging morsels' results into the statement's state, in the order the caller hands them over.
//!
//! The caller hands results over front morsel first, which is the order of the serial run, except
//! for a pipeline whose plan merges freely
//! ([`MergeOrder::Free`](crate::exec::planner::placement::MergeOrder::Free)): its rows are the
//! statement's unordered output, so the caller hands each morsel over as it finishes. Nothing here
//! reorders or waits.

use std::sync::Arc;

use super::super::host::{seal_groups, seal_sort, spill_breakers};
use super::super::interp::{Groups, SortRow, State};
use super::super::program::{Instr, Program};
use super::super::sort::Sorted;
use super::run::{End, MorselResult, RowEnd};
use crate::exec::memory::Reservation;
use crate::exec::memory::runs::BreakerKind;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;

/// The sorts and aggregates a body fills, each once in first-use order, and its paging clause.
pub(in crate::exec::vm) struct BodyBreakers {
	pub(in crate::exec::vm) sorts: Vec<u16>,
	pub(in crate::exec::vm) aggregates: Vec<u16>,
	pub(in crate::exec::vm) paged: Option<u16>,
	/// The distinct set the body's rows pass through, when there is one.
	pub(in crate::exec::vm) distinct: Option<u16>,
}

/// The breakers the body `code` fills.
pub(in crate::exec::vm) fn body_breakers(code: &[Instr]) -> BodyBreakers {
	let mut sorts = Vec::new();
	let mut aggregates = Vec::new();
	let mut paged = None;
	let mut distinct = None;
	for instr in code {
		match instr {
			Instr::Distinct {
				distinct: set,
				..
			} => distinct = Some(*set),
			Instr::Skip {
				limit,
				..
			}
			| Instr::Take {
				limit,
			} => paged = Some(*limit),
			Instr::SortPush {
				sort,
				..
			} if !sorts.contains(sort) => sorts.push(*sort),
			Instr::GroupKey {
				aggregate,
				..
			}
			| Instr::GroupSelect {
				aggregate,
			}
			| Instr::GroupFirstNeeded {
				aggregate,
				..
			}
			| Instr::GroupFirstSet {
				aggregate,
				..
			}
			| Instr::AccUpdate {
				aggregate,
				..
			} if !aggregates.contains(aggregate) => aggregates.push(*aggregate),
			Instr::CoerceField {
				..
			}
			| Instr::BeginComputing {
				..
			}
			| Instr::LandRow {
				..
			}
			| Instr::BindRow {
				..
			}
			| Instr::JumpIfPermissionsBypassed {
				..
			}
			| Instr::CompareFieldInt {
				..
			}
			| Instr::JumpIfFieldInt {
				..
			}
			| Instr::JumpIfCompare {
				..
			}
			| Instr::Compare {
				..
			}
			| Instr::Arith {
				..
			}
			| Instr::Unary {
				..
			}
			| Instr::NewArray {
				..
			}
			| Instr::ArrayPush {
				..
			}
			| Instr::LoadParam {
				..
			}
			| Instr::LoadSlot {
				..
			}
			| Instr::SortKey {
				..
			}
			| Instr::Fetch {
				..
			}
			| Instr::Access {
				..
			}
			| Instr::Each {
				..
			}
			| Instr::Throw {
				..
			}
			| Instr::FilterBegin {
				..
			}
			| Instr::MapBegin {
				..
			}
			| Instr::CollectPush {
				..
			}
			| Instr::Nav {
				..
			}
			| Instr::CallAllowed {
				..
			}
			| Instr::Call {
				..
			}
			| Instr::FunctionEnter {
				..
			}
			| Instr::FunctionBind {
				..
			}
			| Instr::FunctionReturn {
				..
			}
			| Instr::FunctionCall {
				..
			}
			| Instr::ArrayArg {
				..
			}
			| Instr::ElemNext {
				..
			}
			| Instr::ClosureBind {
				..
			}
			| Instr::ClosureReturn {
				..
			}
			| Instr::Dive {
				..
			}
			| Instr::Subquery {
				..
			}
			| Instr::MakeClosure {
				..
			}
			| Instr::ClosureField {
				..
			}
			| Instr::RequireClosure {
				..
			}
			| Instr::Invoke {
				..
			}
			| Instr::Lookup {
				..
			}
			| Instr::Signal {
				..
			}
			| Instr::Extract {
				..
			}
			| Instr::KnnRank {
				..
			}
			| Instr::Measure {
				..
			}
			| Instr::Mock {
				..
			}
			| Instr::Recurse {
				..
			}
			| Instr::Destructure {
				..
			}
			| Instr::RecordId {
				..
			}
			| Instr::MethodResolve {
				..
			}
			| Instr::Method {
				..
			}
			| Instr::MethodClosure {
				..
			}
			| Instr::JumpIfNullish {
				..
			}
			| Instr::JumpIfNone {
				..
			}
			| Instr::JumpIf {
				..
			}
			| Instr::JumpIfLimitReached {
				..
			}
			| Instr::NewObject {
				..
			}
			| Instr::SeedObject {
				..
			}
			| Instr::BuildObject {
				..
			}
			| Instr::LoadPath {
				..
			}
			| Instr::LoadRow {
				..
			}
			| Instr::FetchRow {
				..
			}
			| Instr::RemoveField {
				..
			}
			| Instr::RemovePath {
				..
			}
			| Instr::Omit {
				..
			}
			| Instr::SetField {
				..
			}
			| Instr::WriteProjection {
				..
			}
			| Instr::Load {
				..
			}
			| Instr::Emit {
				..
			}
			| Instr::EmitEnd {
				..
			}
			| Instr::BuildObjectEmit {
				..
			}
			| Instr::FilterFieldInt {
				..
			}
			| Instr::FilterCompare {
				..
			}
			| Instr::Filter {
				..
			}
			| Instr::SplitPush {
				..
			}
			| Instr::SortPush {
				..
			}
			| Instr::JoinProbe {
				..
			}
			| Instr::VerdictJump {
				..
			}
			| Instr::VerdictSet {
				..
			}
			| Instr::JoinPair {
				..
			}
			| Instr::GroupKey {
				..
			}
			| Instr::GroupSelect {
				..
			}
			| Instr::AccUpdate {
				..
			}
			| Instr::Jump {
				..
			}
			| Instr::Next {
				..
			}
			| Instr::Write(_)
			| Instr::Store(_)
			| Instr::Block(_)
			| Instr::EndRow
			| Instr::StopPipeline
			| Instr::TryNone {
				..
			}
			| Instr::TryEnd
			| Instr::MethodRegion {
				..
			}
			| Instr::PermissionRegion {
				..
			}
			| Instr::GroupFirstNeeded {
				..
			}
			| Instr::GroupFirstSet {
				..
			}
			| Instr::CallIndexFn {
				..
			}
			| Instr::Matches {
				..
			}
			| Instr::ExternalEnter {
				..
			}
			| Instr::ExternalCall {
				..
			}
			| Instr::NewSet {
				..
			}
			| Instr::Degree {
				..
			}
			| Instr::RepeatRecurse {
				..
			}
			| Instr::JumpIfNotObject {
				..
			}
			| Instr::JumpIfOpaque {
				..
			}
			| Instr::InvalidControlFlow
			| Instr::JumpIfNotRecordId {
				..
			}
			| Instr::DocumentRoot {
				..
			}
			| Instr::JudgeCopy {
				..
			}
			| Instr::Ir(_) => {}
		}
	}
	BodyBreakers {
		sorts,
		aggregates,
		paged,
		distinct,
	}
}

/// How a paged body's rows have been counted so far, as the merge replays its paging.
#[derive(Default)]
pub(super) struct Replay {
	/// The rows of a paged body the merge has taken, and how many of them reached the paging.
	pub(super) merged: usize,
	pub(super) reached: usize,
	/// Whether the paging's limit is spent: the pipeline takes no more rows.
	pub(super) stopped: bool,
}

impl Replay {
	/// Merge one morsel of a paged body under paging clause `limit`: its rows counted in order as
	/// the serial run counts them.
	pub(super) fn page(
		&mut self,
		limit: u16,
		result: MorselResult,
		state: &mut State,
	) -> FlowResult<()> {
		if !result.spilled.is_empty() {
			return Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
				"a paged morsel spilled".to_owned(),
			))));
		}
		let mut output = result.output.into_iter();
		for end in result.ends {
			// A run of rows that left nothing and did not reach the paging still counts as merged.
			// `Policy::window` scales the paging's need by that count.
			let RowEnd {
				reached,
				skipped,
				emitted,
				failure,
				..
			} = match end {
				End::Passed(rows) => {
					self.merged += rows;
					continue;
				}
				End::Row(row) => row,
			};
			self.merged += 1;
			if !reached {
				if let Some(failure) = failure {
					return Err(failure);
				}
				output.by_ref().take(emitted).for_each(drop);
				continue;
			}
			self.reached += 1;
			let paging = &mut state.paging[limit as usize];
			if skipped || paging.skip > 0 {
				paging.skip = paging.skip.checked_sub(1).ok_or_else(|| {
					ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
						"a morsel skipped a row its paging takes".to_owned(),
					)))
				})?;
				output.by_ref().take(emitted).for_each(drop);
				continue;
			}
			if let Some(remaining) = &mut paging.remaining {
				*remaining = remaining.saturating_sub(1);
			}
			let spent = paging.remaining == Some(0);
			if let Some(failure) = failure {
				return Err(failure);
			}
			state.output.extend(output.by_ref().take(emitted));
			if spent {
				self.stopped = true;
				return Ok(());
			}
		}
		Ok(())
	}
}

/// Merge one morsel of a body whose rows pass through distinct set `distinct`, in the order of the
/// morsels: a row the morsel's own set took is taken by the statement's set only if no row before
/// it equals it, as the serial run takes it. A row the statement's set does not take is dropped
/// with what it emitted and any failure it raised after the set, which the serial run never
/// evaluates; one that is taken hands on its rows or raises its failure. A set that outgrows its
/// budget fails the row that outgrows it, before anything the row does after the set.
pub(super) fn dedupe(distinct: u16, result: MorselResult, state: &mut State) -> FlowResult<()> {
	let mut output = result.output.into_iter();
	for end in result.ends {
		// A distinct morsel keeps a row only when its set took it or it failed. It never
		// produces a run of rows that left nothing.
		let End::Row(RowEnd {
			emitted,
			failure,
			key,
			..
		}) = end
		else {
			continue;
		};
		let taken = match &key {
			Some(key) => state.seen[distinct as usize].insert(key, state.build_budget)?,
			None => true,
		};
		if !taken {
			output.by_ref().take(emitted).for_each(drop);
			continue;
		}
		if let Some(failure) = failure {
			return Err(failure);
		}
		state.output.extend(output.by_ref().take(emitted));
	}
	Ok(())
}

/// Merge one morsel's results into the statement's state: its output after the output the
/// statement holds, its sorts' entries after the sorts' own, its groups folded into the
/// statement's.
pub(super) async fn absorb(
	program: &Arc<Program>,
	breakers: &BodyBreakers,
	result: MorselResult,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	let MorselResult {
		output,
		mut sorts,
		slabs,
		arrivals,
		mut groups,
		mut spilled,
		mut sort_accounts,
		mut group_accounts,
		..
	} = result;
	state.output.extend(output);
	for &sort in &breakers.sorts {
		let at = sort as usize;
		let partial = spilled
			.iter()
			.position(|p| p.spec.kind == BreakerKind::Sort && p.spec.index == sort)
			.map(|found| spilled.swap_remove(found));
		let Some(partial) = partial else {
			adopt_reservation(&mut state.sort_accounts[at], sort_accounts.get_mut(at))?;
			let mut entries = std::mem::take(&mut sorts[at]);
			// Spans into the morsel's slab are shifted by the statement's slab length, because the
			// morsel's slab is appended after the statement's.
			if let Some(slab) = slabs.get(at).filter(|slab| !slab.is_empty()) {
				let base = state.sort_slabs[at].len();
				state.sort_slabs[at].extend_from_slice(slab);
				for (_, row) in &mut entries {
					if let SortRow::Slab {
						start,
						split,
						end,
					} = row
					{
						*start += base;
						*split += base;
						*end += base;
					}
				}
			}
			let arrived = arrivals.get(at).map_or(0, |a| a.arrived);
			state.keep_absorbed(program, sort, entries, arrived);
			continue;
		};
		seal_sort(state, at, ctx).await?;
		adopt_reservation(&mut state.sort_accounts[at], sort_accounts.get_mut(at))?;
		state.sort_runs[at].get_or_insert_with(Default::default).append(partial.runs)?;
		let rows: Vec<Sorted> =
			partial.held.into_iter().map(|(keys, row)| (keys, SortRow::Value(row))).collect();
		let arrived = rows.len() as u64;
		state.keep_absorbed(program, sort, rows, arrived);
	}
	for &aggregate in &breakers.aggregates {
		let at = aggregate as usize;
		let partial = spilled
			.iter()
			.position(|p| p.spec.kind == BreakerKind::Groups && p.spec.index == aggregate)
			.map(|found| spilled.swap_remove(found));
		let Some(partial) = partial else {
			adopt_reservation(&mut state.group_accounts[at], group_accounts.get_mut(at))?;
			let part = groups[at].take();
			merge_groups(&mut state.groups[at], part)?;
			continue;
		};
		seal_groups(state, at, ctx).await?;
		adopt_reservation(&mut state.group_accounts[at], group_accounts.get_mut(at))?;
		state.group_runs[at].get_or_insert_with(Default::default).append(partial.runs)?;
		let functions = state.aggregates.functions[at].clone();
		let into = state.groups[at].as_mut().ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!("a morsel merged groups of another kind"))
		})?;
		for (key, row) in partial.held {
			into.load_saved(key, row, &functions).map_err(ControlFlow::Err)?;
		}
	}
	// Rows the statement took from the morsel are already charged to the reservation adopted
	// from it, so the lease counts them once. If the statement holds more than that reservation
	// covers, the excess is spilled.
	if state.memory.is_some() {
		spill_breakers(state, ctx).await?;
	}
	drop((sort_accounts, group_accounts));
	#[cfg(test)]
	if let Some(memory) = state.memory.as_ref()
		&& memory.lease().held() < held_by_breakers(state)
	{
		super::test_hooks::note_lease_fault(state, super::test_hooks::LeaseFault::Uncounted);
	}
	Ok(())
}

/// Whether the results of a morsel hold nothing but groups that fold into the groups of the morsel
/// after it: every aggregate of the body has its groups in hand, none spilled, and no rows,
/// sorted entries or row ends came with them.
pub(super) fn combinable(result: &MorselResult, breakers: &BodyBreakers) -> bool {
	breakers.paged.is_none()
		&& breakers.distinct.is_none()
		&& breakers.sorts.is_empty()
		&& !breakers.aggregates.is_empty()
		&& result.spilled.is_empty()
		&& result.output.is_empty()
		&& result.ends.is_empty()
		&& breakers.aggregates.iter().all(|&aggregate| result.groups[aggregate as usize].is_some())
}

/// Fold the groups of `later`, the results of the morsels after those of `earlier`, into the
/// groups of `earlier`, an aggregate at a time, and what `later` holds of the query's lease with
/// them: `earlier` then holds what both did, as the statement's merge of the two in turn would.
pub(super) fn fold_into(
	earlier: &mut MorselResult,
	later: &mut MorselResult,
	breakers: &BodyBreakers,
) -> FlowResult<()> {
	for &aggregate in &breakers.aggregates {
		let at = aggregate as usize;
		if let Some(mine) = earlier.group_accounts.get_mut(at) {
			adopt_reservation(mine, later.group_accounts.get_mut(at))?;
		}
		merge_groups(&mut earlier.groups[at], later.groups[at].take())?;
	}
	earlier.rows += later.rows;
	earlier.elapsed += later.elapsed;
	earlier.bytes += later.bytes;
	earlier.chunks += later.chunks;
	earlier.held = earlier.held.max(later.held);
	Ok(())
}

/// Hand a morsel's reservation for a breaker, where it has one, to the statement's account for the
/// same breaker.
fn adopt_reservation(
	statement: &mut Reservation,
	morsel: Option<&mut Reservation>,
) -> FlowResult<()> {
	match morsel {
		Some(morsel) => statement.adopt(morsel),
		None => Ok(()),
	}
}

/// What the statement's breakers hold in memory, as the lease must cover it: the entries of each
/// sort that charges them one by one, and the open groups of each aggregate that can spill.
#[cfg(test)]
fn held_by_breakers(state: &State) -> usize {
	let sorts: usize = state
		.sorts
		.iter()
		.zip(&state.sort_spill)
		.filter(|(_, spill)| spill.as_ref().is_some_and(|spill| spill.keep.is_none()))
		.flat_map(|(entries, _)| entries)
		.map(super::super::sort::sorted_bytes)
		.sum();
	let groups: usize =
		state.groups.iter().flatten().filter(|groups| groups.spillable()).map(Groups::bytes).sum();
	sorts + groups
}

/// What the records `partial` holds in memory weigh, as a breaker's account counts them.
pub(super) fn held_bytes(partial: &super::super::partial::Partial) -> usize {
	partial
		.held
		.iter()
		.map(|(keys, row)| {
			crate::exec::memory::size::values_bytes(keys)
				+ crate::exec::memory::size::value_bytes(row)
		})
		.sum()
}

/// Fold a morsel's partial groups into the groups the statement has.
pub(super) fn merge_groups(main: &mut Option<Groups>, part: Option<Groups>) -> FlowResult<()> {
	match (main, part) {
		(Some(main), Some(part)) => main.absorb(part).map_err(ControlFlow::Err),
		(_, None) => Ok(()),
		(None, Some(_)) => {
			Err(ControlFlow::Err(anyhow::anyhow!("a morsel merged groups of another kind")))
		}
	}
}
