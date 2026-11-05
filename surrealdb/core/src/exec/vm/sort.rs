//! The order a sort hands its rows on in, taken a quantum at a time.
//!
//! No single sort walks a buffer larger than one quantum ([`super::fuel`]):
//! the rows are cut into runs of at most a quantum each, sorted one at a time,
//! and merged by a loser tree, a unit of fuel per row
//! ([`surrealdb_runtime::fuel::sort_by`]). The earlier run wins a tie, so the
//! whole is the stable order a single sort would answer.

use std::cmp::Ordering;

use super::interp::{SortRow, compare_keys};
use super::program::KeyOrder;
use crate::exec::{ExecutionContext, FlowResult};
use crate::val::Value;

/// A row a sort holds, with its keys.
pub(super) type Sorted = (Vec<Value>, SortRow);

/// How a sort that may spill writes its runs.
#[derive(Debug, Clone)]
pub(super) struct SortSpill {
	/// The rows it holds before it spills whatever its query's memory: a
	/// sort under `TEMPFILES`.
	pub(super) rows: Option<usize>,
	/// The order its runs are written and merged in.
	pub(super) keys: Vec<KeyOrder>,
	/// Whether the order is random: each row takes a random key as it is
	/// spilled or merged ([`with_random_keys`]), so the merge of the runs is
	/// a uniform shuffle of every row.
	pub(super) random: bool,
	/// The rows the sort keeps (`SortSpec::keep`): it spills only once what it
	/// holds passes one chunk of its lease, writes no more than this many rows
	/// a run, and merges with this limit.
	pub(super) keep: Option<usize>,
	/// The bytes and rows the sort has taken, for the mean row a keep sort
	/// charges its held rows at.
	pub(super) taken_bytes: u64,
	pub(super) taken_rows: u64,
	/// The keys (without an arrival stamp) of the last record of a spilled run
	/// that holds exactly `keep` records, the least such: a later row whose
	/// keys do not order strictly before them cannot be among the first
	/// `keep`, because those `keep` rows all arrived earlier and win every tie
	/// by source order.
	pub(super) floor: Option<Vec<Value>>,
}

/// The order of the random key [`with_random_keys`] gives each row.
pub(super) const RANDOM_ORDER: KeyOrder = KeyOrder {
	ascending: true,
	collate: false,
	numeric: false,
};

/// Give each of `rows` a key drawn uniformly at random, in place of the keys
/// it holds: sorted by those keys, the rows lie in a uniformly random order,
/// and runs so sorted merge into one.
pub(super) fn with_random_keys(rows: &mut [Sorted]) {
	use rand::Rng as _;
	let mut rng = rand::rng();
	for (keys, _) in rows {
		keys.clear();
		keys.push(Value::from(rng.random::<f64>()));
	}
}

/// The bytes a row a sort holds weighs, for its account.
#[inline]
pub(super) fn sorted_bytes((keys, row): &Sorted) -> usize {
	let row = match row {
		SortRow::Value(value) => crate::exec::memory::size::value_bytes(value),
		SortRow::Stored {
			bytes,
			..
		} => std::mem::size_of::<SortRow>() + bytes.len(),
		// The row's bytes lie in its sort's slab, which grows by them.
		SortRow::Slab {
			start,
			end,
			..
		} => std::mem::size_of::<SortRow>() + (end - start),
	};
	row + crate::exec::memory::size::values_bytes(keys) + std::mem::size_of::<Sorted>()
}

/// The order of two rows' keys under `keys`, then — for a sort that keeps a
/// bounded number of rows, whose rows carry their arrival after their keys
/// ([`super::interp::State::keep_sorted`]) — the earlier arrival first.
#[inline]
pub(super) fn compare_rows(a: &[Value], b: &[Value], keys: &[KeyOrder]) -> Ordering {
	compare_keys(a, b, keys).then_with(|| match (a.get(keys.len()), b.get(keys.len())) {
		(Some(a), Some(b)) => a.cmp(b),
		_ => Ordering::Equal,
	})
}

/// `rows` shuffled, the first `keep` of them: a Fisher-Yates shuffle taken a
/// quantum of swaps at a time.
pub(super) async fn shuffled(
	mut rows: Vec<Sorted>,
	keep: usize,
	ctx: &ExecutionContext,
) -> FlowResult<Vec<Sorted>> {
	use rand::Rng as _;
	let quantum = super::fuel::quantum(ctx) as usize;
	let mut at = rows.len();
	while at > 1 {
		let end = at.saturating_sub(quantum).max(1);
		{
			let mut rng = rand::rng();
			for i in (end..at).rev() {
				rows.swap(i, rng.random_range(0..=i));
			}
		}
		super::fuel::tick(ctx, super::fuel::units(at - end)).await?;
		at = end;
	}
	rows.truncate(keep);
	Ok(rows)
}

/// `rows` in the stable order `keys` names, the first `keep` of them.
pub(super) async fn sorted(
	rows: Vec<Sorted>,
	keys: &[KeyOrder],
	keep: usize,
	ctx: &ExecutionContext,
) -> FlowResult<Vec<Sorted>> {
	let run = super::fuel::quantum(ctx);
	super::fuel::metered(
		ctx,
		surrealdb_runtime::fuel::sort_by(
			rows,
			|(a, _), (b, _)| compare_rows(a, b, keys),
			keep,
			run,
		),
	)
	.await
}

#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests {
	use super::*;

	fn row(key: i64, tag: i64) -> Sorted {
		(vec![Value::from(key)], SortRow::Value(Value::from(tag)))
	}

	fn tags(rows: Vec<Sorted>) -> Vec<(Value, Value)> {
		rows.into_iter()
			.map(|(mut keys, row)| (keys.remove(0), row.into_value().expect("a value row")))
			.collect()
	}

	/// Runs merged answer the stable order one sort answers, under every
	/// `keep`, for a quantum far smaller than the rows.
	#[tokio::test]
	async fn runs_merged_answer_the_stable_order_of_one_sort() {
		let db = crate::exec::operators::test_util::TestDb::new_with_config(
			"",
			surrealdb_cnf::ConfigMap::empty().with_key_value("vm_quantum", "7"),
		)
		.await;
		let ctx = db.exec_ctx().await;
		let keys = [KeyOrder {
			ascending: true,
			collate: false,
			numeric: false,
		}];
		// Many ties, in arrival order by tag.
		let input = || (0..200).map(|i| row((i * 37) % 11, i)).collect::<Vec<_>>();
		let mut expected = input();
		expected.sort_by(|(a, _), (b, _)| compare_keys(a, b, &keys));
		let expected = tags(expected);
		for keep in [0, 1, 6, 7, 8, 50, 199, 200, usize::MAX] {
			let got = sorted(input(), &keys, keep, &ctx).await.expect("sorts");
			let want: Vec<_> = expected.iter().take(keep).cloned().collect();
			assert_eq!(tags(got), want, "keep {keep}");
		}
	}
}
