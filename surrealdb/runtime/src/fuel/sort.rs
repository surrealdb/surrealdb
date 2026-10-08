//! A stable sort taken a quantum at a time.
//!
//! No single `sort_by` covers more than one quantum of elements: the input is
//! cut into runs of at most a quantum each, in arrival order, and each run is
//! sorted on its own, a unit an element, with a charge before each. The runs
//! are then merged, a unit an element taken, the earlier run winning a tie, so
//! the whole is the stable order one sort would answer.
//!
//! Input that arrived sorted, or strictly in reverse, is recognised by a walk
//! before any run is sorted, and handed on as it lies or reversed. Otherwise
//! the merge is a loser tree: an element taken costs `log k` comparisons over
//! `k` runs. Runs that do not overlap and lie in order, or in reverse, are
//! recognised before the merge and handed on as they lie, with no comparison
//! an element.

use std::cmp::Ordering;

use super::{next_stretch, tick, units};

/// `items` in the stable order `cmp` names, the first `keep` of them, sorted
/// in runs of at most `run` elements.
pub async fn sort_by<T, F>(mut items: Vec<T>, mut cmp: F, keep: usize, run: u32) -> Vec<T>
where
	F: FnMut(&T, &T) -> Ordering,
{
	let run = run.max(1) as usize;
	let len = items.len();
	if len <= run {
		tick(units(len)).await;
		items.sort_by(&mut cmp);
		items.truncate(keep);
		return items;
	}
	// Input that arrived in order is the order, and input that arrived in
	// strict reverse is reversed: no run is sorted.
	match presorted(&items, &mut cmp).await {
		Presorted::InOrder => {
			items.truncate(keep);
			return items;
		}
		Presorted::Reversed => {
			let half = len / 2;
			let mut at = 0;
			while at < half {
				let end = (at + next_stretch().await).min(half);
				for i in at..end {
					items.swap(i, len - 1 - i);
				}
				tick(units(end - at)).await;
				at = end;
			}
			items.truncate(keep);
			return items;
		}
		Presorted::Neither => {}
	}
	// Each run is sorted where it lies, a unit an element.
	let bounds: Vec<(usize, usize)> =
		(0..len).step_by(run).map(|start| (start, (start + run).min(len))).collect();
	for &(start, end) in &bounds {
		tick(units(end - start)).await;
		items[start..end].sort_by(&mut cmp);
	}
	let want = len.min(keep);
	match chained(&items, &bounds, &mut cmp) {
		// The runs arrived in order: they are the order.
		Some(order) if order.iter().enumerate().all(|(at, &run)| at == run) => {
			items.truncate(want);
			return items;
		}
		// The runs arrived in reverse: each is taken off the end in turn.
		Some(order) if order.iter().rev().enumerate().all(|(at, &run)| at == run) => {
			let mut out = Vec::with_capacity(want);
			for &(start, end) in bounds.iter().rev() {
				if out.len() >= want {
					break;
				}
				let taken = (end - start).min(want - out.len());
				out.extend(items.drain(start..).take(taken));
				tick(units(taken)).await;
			}
			return out;
		}
		_ => {}
	}
	// Otherwise the runs are split apart, each cut to what the sort keeps,
	// and merged.
	let mut runs = Vec::with_capacity(bounds.len());
	for &(start, end) in bounds.iter().rev() {
		let mut part = items.split_off(start);
		part.truncate(keep);
		tick(units(end - start)).await;
		runs.push(part.into_iter());
	}
	runs.reverse();
	let mut out = Vec::with_capacity(want);
	let mut tree = LoserTree::new(runs.len(), |a, b| beats(&runs, a, b, &mut cmp));
	while out.len() < want {
		let taken = next_stretch().await.min(want - out.len());
		for _ in 0..taken {
			let winner = tree.winner();
			let Some(item) = runs[winner].next() else {
				unreachable!("the merge takes no more than its runs hold")
			};
			out.push(item);
			tree.replay(winner, runs.len(), |a, b| beats(&runs, a, b, &mut cmp));
		}
		tick(units(taken)).await;
	}
	out
}

/// How input lies before it is sorted.
enum Presorted {
	/// Each element orders no earlier than the one before it.
	InOrder,
	/// Each element orders strictly before the one before it, so reversing
	/// the input sorts it stably.
	Reversed,
	Neither,
}

/// How `items` lie, found a stretch at a time, a unit a comparison, and
/// settled at the first pair that rules both orders out: within a few pairs
/// for input in no order.
async fn presorted<T, F>(items: &[T], cmp: &mut F) -> Presorted
where
	F: FnMut(&T, &T) -> Ordering,
{
	let (mut in_order, mut reversed) = (true, true);
	let mut at = 1;
	while at < items.len() && (in_order || reversed) {
		let end = (at + next_stretch().await).min(items.len());
		let mut compared = 0;
		for i in at..end {
			compared += 1;
			match cmp(&items[i - 1], &items[i]) {
				Ordering::Greater => in_order = false,
				Ordering::Less | Ordering::Equal => reversed = false,
			}
			if !in_order && !reversed {
				break;
			}
		}
		tick(units(compared)).await;
		at = end;
	}
	match (in_order, reversed) {
		(true, _) => Presorted::InOrder,
		(false, true) => Presorted::Reversed,
		(false, false) => Presorted::Neither,
	}
}

/// The order to hand the sorted runs `bounds` of `items` on in when no two
/// of them overlap, or `None` when two do: the runs by their first elements,
/// each ending no later than the next begins, and the earlier run first
/// where one ends on the element the next begins with.
fn chained<T, F>(items: &[T], bounds: &[(usize, usize)], cmp: &mut F) -> Option<Vec<usize>>
where
	F: FnMut(&T, &T) -> Ordering,
{
	let first = |run: usize| &items[bounds[run].0];
	let last = |run: usize| &items[bounds[run].1 - 1];
	// Most input that chains arrived in order or in reverse: those orders
	// are tried before the runs are sorted by their first elements.
	let follows = |cmp: &mut F, a: usize, b: usize| match cmp(last(a), first(b)) {
		Ordering::Less => true,
		Ordering::Equal => a < b,
		Ordering::Greater => false,
	};
	let k = bounds.len();
	if (1..k).all(|run| follows(cmp, run - 1, run)) {
		return Some((0..k).collect());
	}
	if (1..k).all(|run| follows(cmp, run, run - 1)) {
		return Some((0..k).rev().collect());
	}
	let mut order: Vec<usize> = (0..k).collect();
	order.sort_by(|&a, &b| cmp(first(a), first(b)).then(a.cmp(&b)));
	order.windows(2).all(|pair| follows(cmp, pair[0], pair[1])).then_some(order)
}

/// A tournament over `k` ordered sources' heads: each inner node holds the
/// source that lost the match there, and the root the source whose head
/// orders first.
///
/// The tree holds no heads itself. A caller answers `beats(a, b)` — whether
/// source `a`'s head orders before source `b`'s — from the heads it keeps, and
/// must make a spent source lose every match and the earlier source win a tie
/// for the merge to be stable. Taking the winner's head and replaying its path
/// costs `log k` comparisons.
pub struct LoserTree {
	/// `nodes[0]` is the winner; `nodes[1..k]` hold the losers, as a binary
	/// heap over the `k` sources, whose leaves sit at `k..2k`.
	nodes: Vec<usize>,
}

impl LoserTree {
	/// The tournament over `k` sources as their heads stand.
	pub fn new(k: usize, mut beats: impl FnMut(usize, usize) -> bool) -> Self {
		let mut nodes = vec![0; k.max(1)];
		let mut winners = vec![0; 2 * k];
		for (at, winner) in winners[k..].iter_mut().enumerate() {
			*winner = at;
		}
		for node in (1..k).rev() {
			let (a, b) = (winners[2 * node], winners[2 * node + 1]);
			if beats(a, b) {
				winners[node] = a;
				nodes[node] = b;
			} else {
				winners[node] = b;
				nodes[node] = a;
			}
		}
		if k > 1 {
			nodes[0] = winners[1];
		}
		Self {
			nodes,
		}
	}

	/// The source whose head orders first.
	#[inline]
	pub fn winner(&self) -> usize {
		self.nodes[0]
	}

	/// Replay the matches on the path of source `run` of `k`, whose head has
	/// just changed (taken, or the source spent).
	#[inline]
	pub fn replay(&mut self, run: usize, k: usize, mut beats: impl FnMut(usize, usize) -> bool) {
		let mut winner = run;
		let mut node = (k + run) / 2;
		while node > 0 {
			let held = self.nodes[node];
			if beats(held, winner) {
				self.nodes[node] = winner;
				winner = held;
			}
			node /= 2;
		}
		self.nodes[0] = winner;
	}
}

/// Whether run `a`'s head orders before run `b`'s: a spent run orders after
/// every other, and the earlier run wins a tie.
#[inline]
fn beats<T, F>(runs: &[std::vec::IntoIter<T>], a: usize, b: usize, cmp: &mut F) -> bool
where
	F: FnMut(&T, &T) -> Ordering,
{
	match (runs[a].as_slice().first(), runs[b].as_slice().first()) {
		(Some(x), Some(y)) => match cmp(x, y) {
			Ordering::Less => true,
			Ordering::Greater => false,
			Ordering::Equal => a < b,
		},
		(Some(_), None) => true,
		(None, _) => false,
	}
}

#[cfg(test)]
mod tests {
	use super::super::{now, refill};
	use super::*;

	fn check(input: &[(u32, u32)], quantum: u32) {
		let mut expected = input.to_vec();
		expected.sort_by_key(|&(key, _)| key);
		for keep in [0, 1, 6, 7, 8, 50, input.len().saturating_sub(1), input.len(), usize::MAX] {
			refill(quantum);
			let got = now(sort_by(input.to_vec(), |a, b| a.0.cmp(&b.0), keep, quantum));
			let want: Vec<_> = expected.iter().take(keep).copied().collect();
			assert_eq!(got, want, "keep {keep}, quantum {quantum}, {} items", input.len());
		}
	}

	/// Runs merged answer the stable order one sort answers, under every
	/// `keep`, whether the runs overlap or arrived in order or in reverse.
	#[test]
	fn runs_merged_answer_the_stable_order_of_one_sort() {
		for quantum in [1, 2, 3, 7, 64, 4096] {
			// Many ties, in arrival order by tag.
			check(&(0..200).map(|i| ((i * 37) % 11, i)).collect::<Vec<_>>(), quantum);
			check(&(0..200).map(|i| (i, i)).collect::<Vec<_>>(), quantum);
			check(&(0..200).map(|i| (200 - i, i)).collect::<Vec<_>>(), quantum);
			check(&(0..200).map(|i| (i / 10, i)).collect::<Vec<_>>(), quantum);
			check(&(0..200).map(|i| (5, i)).collect::<Vec<_>>(), quantum);
			check(&(0..200).map(|i| ((200 - i) / 3, i)).collect::<Vec<_>>(), quantum);
			check(
				&(0..200)
					.map(|i| {
						(
							if i < 100 {
								i
							} else {
								300 - i
							},
							i,
						)
					})
					.collect::<Vec<_>>(),
				quantum,
			);
			check(&(0..201).map(|i| ((i * 7919) % 101, i)).collect::<Vec<_>>(), quantum);
			check(&[], quantum);
		}
		refill(super::super::INITIAL);
	}
}
