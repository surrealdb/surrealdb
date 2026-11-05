//! The groups of an aggregate's morsels, merged by partitions.
//!
//! A morsel of a pipeline that folds rows into the groups of a keyed aggregate hands the merge the
//! groups its rows opened, each with the states its rows folded. The serial fold is the morsels'
//! groups folded into one another in the order the morsels are read: an accumulator that appends
//! keeps the serial order, and a column that is not aggregated keeps the value the earliest morsel
//! evaluated. Merging them one after the other on the statement's task is the same fold with no
//! parallelism, so the groups of the morsels that have ended wait until a window of them has
//! gathered ([`Policy::partials_ready`]) and are merged together:
//!
//! 1. The key space is divided into ranges by splitters drawn from the keys the morsels hold
//!    ([`Splitters`]). A range, not a hash, because two keys that compare equal under `Value: Ord`
//!    are one group while their hashes may differ, and the ranges keep every key that compares
//!    equal to another in one partition.
//! 2. Each morsel's groups are divided by the splitters, a group moved whole into the partition its
//!    key routes to ([`Groups::split`]).
//! 3. Each partition folds the shares of the morsels in the order of the morsels, on a worker or on
//!    the statement's task.
//! 4. The partitions' groups follow one another in key order, so they fold into the statement's
//!    groups in partition order, which is the key order the serial fold answers.
//!
//! The width of the merge is the statement's own task and the places it can take at that moment
//! ([`Policy::partitions`]); with no place free, or too few groups to repay a division
//! ([`Policy::worth_partitioning`]), the groups are folded in order on the task, as a merge of one
//! partition does. A morsel whose groups spilled ([`MorselResult::spilled`]) is merged
//! at once, after the groups that wait: its runs and the statement's merge by key
//! ([`super::super::host`]'s spilled groups).

use std::sync::Arc;

use super::super::groups::Groups;
use super::super::interp::State;
use super::super::partition::Splitters;
use super::super::program::Program;
use super::merge::{BodyBreakers, absorb, merge_groups};
use super::places::{Asking, Place};
use super::run::MorselResult;
use crate::exec::planner::placement::Policy;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;

/// The sample of each morsel's keys a splitter is drawn from: the keys of about this many groups
/// per partition.
const SAMPLE_PER_PARTITION: usize = 64;

/// The results of morsels whose groups wait to be merged together, in the order of the morsels.
#[derive(Default)]
pub(super) struct Pending {
	results: Vec<MorselResult>,
	/// The groups the results' aggregates hold between them.
	groups: usize,
	/// The run's registration, once a merge of these results has asked it for places.
	asking: Asking,
}

impl Pending {
	/// Whether `result`'s groups may wait. They may when the pipeline only groups by keys, every
	/// aggregate's groups are in hand, and the result does not change what comes after it.
	fn defers(
		program: &Program,
		breakers: &BodyBreakers,
		policy: Policy,
		result: &MorselResult,
	) -> bool {
		policy.defers_partials()
			&& breakers.paged.is_none()
			&& breakers.sorts.is_empty()
			&& !breakers.aggregates.is_empty()
			&& breakers.aggregates.iter().all(|&aggregate| {
				let at = aggregate as usize;
				program.aggregates[at].keys > 0 && result.groups[at].is_some()
			})
	}

	/// Take `result`: its groups wait for the merge when they may, and the merge runs once enough
	/// have gathered; otherwise the groups that wait are merged and then `result`, as a morsel of
	/// any other pipeline is.
	pub(super) async fn take(
		&mut self,
		program: &Arc<Program>,
		breakers: &BodyBreakers,
		policy: Policy,
		result: MorselResult,
		state: &mut State,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		if Self::defers(program, breakers, policy, &result) {
			self.groups += breakers
				.aggregates
				.iter()
				.filter_map(|&aggregate| result.groups[aggregate as usize].as_ref())
				.map(Groups::len)
				.sum::<usize>();
			self.results.push(result);
			if policy.partials_ready(self.results.len(), self.groups) {
				self.flush(program, breakers, policy, state, ctx).await?;
			}
			return Ok(());
		}
		self.flush(program, breakers, policy, state, ctx).await?;
		absorb(program, breakers, result, state, ctx).await
	}

	/// Merge the groups that wait into the statement's, then take the rest of each result as the
	/// merge of one morsel does. A statement that runs rows itself calls this first, so what it
	/// folds follows the groups of the morsels before it.
	pub(super) async fn flush(
		&mut self,
		program: &Arc<Program>,
		breakers: &BodyBreakers,
		policy: Policy,
		state: &mut State,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		if self.results.is_empty() {
			return Ok(());
		}
		let mut results = std::mem::take(&mut self.results);
		self.groups = 0;
		for &aggregate in &breakers.aggregates {
			let at = aggregate as usize;
			let parts: Vec<Groups> =
				results.iter_mut().filter_map(|result| result.groups[at].take()).collect();
			let folded = merged(parts, policy, state, ctx).await;
			self.asking.note(state);
			let folded = folded?;
			for result in &mut results {
				if let Some(account) = result.group_accounts.get_mut(at) {
					state.group_accounts[at].adopt(account)?;
					account.clear();
				}
			}
			for part in folded {
				merge_groups(&mut state.groups[at], Some(part))?;
			}
		}
		for result in results {
			absorb(program, breakers, result, state, ctx).await?;
		}
		Ok(())
	}
}

/// `parts`, the groups of morsels in the order of the morsels, merged: the groups of every
/// partition in turn, in key order, each partition's folded from the morsels' shares.
async fn merged(
	parts: Vec<Groups>,
	policy: Policy,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Vec<Groups>> {
	let total: usize = parts.iter().map(Groups::len).sum();
	if parts.len() < 2 || !policy.worth_partitioning(total) {
		return fold(parts).map(|folded| folded.into_iter().collect()).map_err(ControlFlow::Err);
	}
	let mut places = Vec::new();
	#[cfg(not(target_family = "wasm"))]
	if policy.may_use_workers() {
		let statement = super::places::of(state);
		while places.len() + 1 < policy.partitions(parts.len()) {
			let Some(place) = statement.acquire(policy) else {
				break;
			};
			places.push(place);
		}
	}
	#[cfg(target_family = "wasm")]
	let _ = state;
	let width = policy.partitions(1 + places.len());
	let splitters = (width > 1).then(|| {
		let stride = (total / (SAMPLE_PER_PARTITION * width)).max(1);
		Splitters::from_samples(parts.iter().map(|part| part.sample(stride)).collect(), width)
	});
	let Some(splitters) = splitters.filter(|splitters| splitters.partitions() > 1) else {
		return fold(parts).map(|folded| folded.into_iter().collect()).map_err(ControlFlow::Err);
	};
	#[cfg(test)]
	super::test_hooks::note_partitioned(ctx, splitters.partitions());
	#[cfg(not(test))]
	let _ = ctx;
	let splitters = Arc::new(splitters);
	// Each morsel's groups, divided by the splitters.
	let jobs: Vec<Job<Vec<Groups>>> = parts
		.into_iter()
		.map(|part| {
			let splitters = Arc::clone(&splitters);
			Box::new(move || part.split(&splitters)) as Job<Vec<Groups>>
		})
		.collect();
	let shares = run(&mut places, jobs).await?;
	// Each partition's shares, in the order of the morsels.
	let mut partitions: Vec<Vec<Groups>> =
		(0..splitters.partitions()).map(|_| Vec::with_capacity(shares.len())).collect();
	for share in shares {
		for (partition, part) in partitions.iter_mut().zip(share) {
			partition.push(part);
		}
	}
	let jobs: Vec<Job<anyhow::Result<Option<Groups>>>> = partitions
		.into_iter()
		.map(|shares| Box::new(move || fold(shares)) as Job<anyhow::Result<Option<Groups>>>)
		.collect();
	let mut folded = Vec::new();
	for partition in run(&mut places, jobs).await? {
		folded.extend(partition.map_err(ControlFlow::Err)?);
	}
	Ok(folded)
}

/// `parts` folded one into the one before it, in order; `None` of none.
fn fold(parts: Vec<Groups>) -> anyhow::Result<Option<Groups>> {
	let mut parts = parts.into_iter().filter(|part| !part.is_empty());
	let Some(mut folded) = parts.next() else {
		return Ok(None);
	};
	for part in parts {
		folded.absorb(part)?;
	}
	Ok(Some(folded))
}

/// A piece of the merge that holds no reference to the statement.
type Job<T> = Box<dyn FnOnce() -> T + Send>;

/// Run `jobs`, each on a place's own thread while places last and on this task otherwise, and
/// answer what each answered, in the order of the jobs. A place goes back to the pool as its job
/// ends.
#[cfg_attr(
	target_family = "wasm",
	expect(unused_variables, clippy::ptr_arg, reason = "a target without threads takes no place")
)]
async fn run<T: Send + 'static>(places: &mut Vec<Place>, jobs: Vec<Job<T>>) -> FlowResult<Vec<T>> {
	let mut answers: Vec<Option<T>> = (0..jobs.len()).map(|_| None).collect();
	let mut here = Vec::new();
	#[cfg(not(target_family = "wasm"))]
	let mut away = Vec::new();
	for (at, job) in jobs.into_iter().enumerate() {
		#[cfg(not(target_family = "wasm"))]
		if let Some(place) = places.pop() {
			away.push((
				at,
				tokio::task::spawn_blocking(move || {
					let answer = job();
					drop(place);
					answer
				}),
			));
			continue;
		}
		here.push((at, job));
	}
	for (at, job) in here {
		answers[at] = Some(job());
	}
	#[cfg(not(target_family = "wasm"))]
	for (at, handle) in away {
		answers[at] = Some(handle.await.map_err(|e| {
			ControlFlow::Err(anyhow::anyhow!("a task merging the groups of morsels failed: {e}"))
		})?);
	}
	answers
		.into_iter()
		.map(|answer| {
			answer.ok_or_else(|| {
				ControlFlow::Err(anyhow::anyhow!(
					"a task merging the groups of morsels answered nothing"
				))
			})
		})
		.collect()
}
