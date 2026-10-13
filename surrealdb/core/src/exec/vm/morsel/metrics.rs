//! What a scan's morsels did, for tests and for `EXPLAIN ANALYZE`.
//!
//! The counters are kept for every scan: a handful of integers touched when a morsel is queued,
//! claimed, parked or merged, never per entry. They are rendered only when
//! [`morsel_explain_analyze`](crate::exec::config::ExecConfig::morsel_explain_analyze) is set.
//! Morsels are named by their ordinal, the position in which the merge took them, never by the keys
//! they covered.

use std::fmt::Write;

use crate::val::{Object, Value};

/// The most morsels whose individual figures are kept; the counters still count every one.
const LISTED: usize = 4096;

/// The most morsels the rendering lists.
const RENDERED: usize = 16;

/// What one merged morsel read and took.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MorselStat {
	/// The entries the morsel read.
	pub(crate) rows: u64,
	/// The bytes of the keys and values it read.
	pub(crate) bytes: u64,
	/// The time it ran, in nanoseconds.
	pub(crate) elapsed_ns: u64,
	/// The chunks it read its range in.
	pub(crate) chunks: u64,
}

/// The rows and time of the morsels that ran in one place.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Share {
	pub(crate) rows: u64,
	pub(crate) elapsed_ns: u64,
}

/// The counters of the morsels of one or more scans.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MorselMetrics {
	/// Scans that began as one morsel over their whole range.
	pub(crate) initial: u64,
	/// Morsels cut off a range by the policy's first split.
	pub(crate) split: u64,
	/// Morsels donated to an idle worker by a running morsel, or by every one when each donates.
	pub(crate) stolen: u64,
	/// Morsels a running morsel cut off itself because it would outlast its siblings.
	pub(crate) resplit: u64,
	/// Morsels that stopped at a chunk boundary with part of their range unread.
	pub(crate) parked: u64,
	/// Morsels that began where a parked one stopped.
	pub(crate) resumed: u64,
	/// Morsels the statement no longer needed.
	pub(crate) cancelled: u64,
	/// Steals asked of a running morsel, and the asks it answered by declining for want of a
	/// boundary in its remainder.
	pub(crate) steal_asks: u64,
	pub(crate) declined: u64,
	/// Places returned to a waiting statement before the morsel ended.
	pub(crate) yielded: u64,
	/// The most places the statement held at once, and the sum and count of the samples taken.
	pub(crate) places_max: u64,
	places_sum: u64,
	places_samples: u64,
	/// The entries read, and the time spent, by the coordinator and by workers.
	pub(crate) coordinator: Share,
	pub(crate) workers: Share,
	/// The time the coordinator waited for the morsel at the front of the merge, in nanoseconds.
	pub(crate) front_wait_ns: u64,
	/// The most output bytes one morsel held.
	pub(crate) window_bytes_max: u64,
	/// Morsels merged.
	pub(crate) merged: u64,
	/// The figures of the first morsels merged, in merge order.
	listed: Vec<MorselStat>,
}

impl MorselMetrics {
	/// A merged morsel that ran on a worker or on the coordinator. Its figures are kept for the
	/// listing only when `listed`, so a scan nobody renders allocates nothing.
	pub(crate) fn merged_morsel(&mut self, stat: MorselStat, worker: bool, listed: bool) {
		self.merged += 1;
		let share = if worker {
			&mut self.workers
		} else {
			&mut self.coordinator
		};
		share.rows += stat.rows;
		share.elapsed_ns += stat.elapsed_ns;
		if listed && self.listed.len() < LISTED {
			self.listed.push(stat);
		}
	}

	/// The places the statement holds, sampled.
	pub(crate) fn sampled_places(&mut self, held: usize) {
		let held = held as u64;
		self.places_max = self.places_max.max(held);
		self.places_sum += held;
		self.places_samples += 1;
	}

	/// The most output bytes a morsel held.
	pub(crate) fn held_window(&mut self, bytes: usize) {
		self.window_bytes_max = self.window_bytes_max.max(bytes as u64);
	}

	/// The mean of the places sampled, or zero when none was.
	pub(crate) fn places_mean(&self) -> f64 {
		if self.places_samples == 0 {
			0.0
		} else {
			self.places_sum as f64 / self.places_samples as f64
		}
	}

	/// Add the counters of another scan.
	pub(crate) fn absorb(&mut self, other: &Self) {
		self.initial += other.initial;
		self.split += other.split;
		self.stolen += other.stolen;
		self.resplit += other.resplit;
		self.parked += other.parked;
		self.resumed += other.resumed;
		self.cancelled += other.cancelled;
		self.steal_asks += other.steal_asks;
		self.declined += other.declined;
		self.yielded += other.yielded;
		self.places_max = self.places_max.max(other.places_max);
		self.places_sum += other.places_sum;
		self.places_samples += other.places_samples;
		for (mine, theirs) in
			[(&mut self.coordinator, other.coordinator), (&mut self.workers, other.workers)]
		{
			mine.rows += theirs.rows;
			mine.elapsed_ns += theirs.elapsed_ns;
		}
		self.front_wait_ns += other.front_wait_ns;
		self.window_bytes_max = self.window_bytes_max.max(other.window_bytes_max);
		self.merged += other.merged;
		let room = LISTED.saturating_sub(self.listed.len());
		self.listed.extend(other.listed.iter().take(room).copied());
	}

	/// The figures of one measure over the morsels listed: smallest, median, 95th percentile and
	/// largest.
	fn spread(&self, of: impl Fn(&MorselStat) -> u64) -> [u64; 4] {
		let mut all: Vec<u64> = self.listed.iter().map(of).collect();
		if all.is_empty() {
			return [0; 4];
		}
		all.sort_unstable();
		let at = |share: usize| all[((all.len() - 1) * share).div_ceil(100)];
		[all[0], at(50), at(95), all[all.len() - 1]]
	}

	/// The text `EXPLAIN ANALYZE` renders under the plan: counters, then the spread of each
	/// measure over the morsels, then the first morsels by ordinal.
	pub(crate) fn render_text(&self) -> String {
		let mut out = String::from("Morsels:");
		let _ = write!(
			out,
			" initial: {}, split: {}, stolen: {}, resplit: {}, parked: {}, resumed: {}, cancelled: {}, merged: {}",
			self.initial,
			self.split,
			self.stolen,
			self.resplit,
			self.parked,
			self.resumed,
			self.cancelled,
			self.merged
		);
		let _ = write!(
			out,
			"\n  steals: asked: {}, declined: {}, places yielded: {}",
			self.steal_asks, self.declined, self.yielded
		);
		let _ = write!(out, "\n  places: max: {}, avg: {:.2}", self.places_max, self.places_mean());
		let _ = write!(
			out,
			"\n  coordinator: rows: {}, elapsed: {}ns; workers: rows: {}, elapsed: {}ns",
			self.coordinator.rows,
			self.coordinator.elapsed_ns,
			self.workers.rows,
			self.workers.elapsed_ns
		);
		for (name, of) in MEASURES {
			let [min, p50, p95, max] = self.spread(of);
			let _ = write!(out, "\n  {name}: min: {min}, p50: {p50}, p95: {p95}, max: {max}");
		}
		let _ = write!(
			out,
			"\n  merge: front wait: {}ns, window bytes max: {}",
			self.front_wait_ns, self.window_bytes_max
		);
		for (ordinal, stat) in self.listed.iter().take(RENDERED).enumerate() {
			let _ = write!(
				out,
				"\n  #{ordinal}: rows: {}, bytes: {}, chunks: {}, elapsed: {}ns",
				stat.rows, stat.bytes, stat.chunks, stat.elapsed_ns
			);
		}
		if self.listed.len() > RENDERED {
			let _ = write!(out, "\n  ... {} more", self.merged - RENDERED as u64);
		}
		out
	}

	/// The object `EXPLAIN ANALYZE FORMAT JSON` renders under the plan.
	pub(crate) fn render_object(&self) -> Object {
		let number = |n: u64| Value::from(i64::try_from(n).unwrap_or(i64::MAX));
		let mut out = Object::default();
		for (key, value) in [
			("initial", self.initial),
			("split", self.split),
			("stolen", self.stolen),
			("resplit", self.resplit),
			("parked", self.parked),
			("resumed", self.resumed),
			("cancelled", self.cancelled),
			("merged", self.merged),
			("steals_asked", self.steal_asks),
			("steals_declined", self.declined),
			("places_yielded", self.yielded),
			("places_max", self.places_max),
			("coordinator_rows", self.coordinator.rows),
			("coordinator_elapsed_ns", self.coordinator.elapsed_ns),
			("worker_rows", self.workers.rows),
			("worker_elapsed_ns", self.workers.elapsed_ns),
			("front_wait_ns", self.front_wait_ns),
			("window_bytes_max", self.window_bytes_max),
		] {
			out.insert(key, number(value));
		}
		for (name, of) in MEASURES {
			let [min, p50, p95, max] = self.spread(of);
			let mut spread = Object::default();
			for (key, value) in [("min", min), ("p50", p50), ("p95", p95), ("max", max)] {
				spread.insert(key, number(value));
			}
			out.insert(name, Value::Object(spread));
		}
		let listed = self
			.listed
			.iter()
			.take(RENDERED)
			.enumerate()
			.map(|(ordinal, stat)| {
				let mut one = Object::default();
				one.insert("ordinal", number(ordinal as u64));
				one.insert("rows", number(stat.rows));
				one.insert("bytes", number(stat.bytes));
				one.insert("chunks", number(stat.chunks));
				one.insert("elapsed_ns", number(stat.elapsed_ns));
				Value::Object(one)
			})
			.collect::<Vec<_>>();
		out.insert("morsels", Value::Array(listed.into()));
		out
	}
}

/// One measure of a morsel's figures.
type Measure = fn(&MorselStat) -> u64;

/// The measures a morsel's figures are spread over, by name.
const MEASURES: [(&str, Measure); 4] = [
	("rows", |stat| stat.rows),
	("bytes", |stat| stat.bytes),
	("elapsed_ns", |stat| stat.elapsed_ns),
	("chunks", |stat| stat.chunks),
];

#[cfg(test)]
mod tests {
	use super::*;

	/// The spread is read off the morsels merged, smallest to largest.
	#[test]
	fn the_spread_orders_the_figures() {
		let mut metrics = MorselMetrics::default();
		for rows in [5, 1, 9, 3, 7] {
			metrics.merged_morsel(
				MorselStat {
					rows,
					..MorselStat::default()
				},
				true,
				true,
			);
		}
		assert_eq!(metrics.spread(|stat| stat.rows), [1, 5, 9, 9]);
		assert_eq!(metrics.workers.rows, 25);
	}

	/// Two scans' counters add, and the list of morsels stays in merge order.
	#[test]
	fn absorbing_adds_counters() {
		let mut one = MorselMetrics {
			stolen: 2,
			..Default::default()
		};
		one.sampled_places(2);
		let mut two = MorselMetrics {
			stolen: 3,
			..Default::default()
		};
		two.sampled_places(4);
		one.absorb(&two);
		assert_eq!((one.stolen, one.places_max), (5, 4));
		assert!((one.places_mean() - 3.0).abs() < f64::EPSILON);
	}

	/// What is rendered names morsels by ordinal.
	#[test]
	fn the_text_names_ordinals() {
		let mut metrics = MorselMetrics::default();
		metrics.merged_morsel(MorselStat::default(), false, true);
		let text = metrics.render_text();
		assert!(text.starts_with("Morsels:"), "{text}");
		assert!(text.contains("#0:"), "{text}");
	}
}
