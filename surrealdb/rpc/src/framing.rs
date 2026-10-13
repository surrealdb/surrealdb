//! Turning a streaming execution's items into the frames a transport sends.
//!
//! [`QueryStreamItem`]s come off the executor at whatever size it produced them
//! — one item routinely carries a whole statement's result — and a transport
//! needs them as [`QueryStreamFrame`]s of a size it can put on a wire. That
//! conversion is the same wherever the frames are going, so it lives here
//! rather than in any one transport: the WebSocket protocol and the embedded
//! JavaScript engines both drive this, and a client cannot tell which produced
//! the frames it is reading.
//!
//! The driving itself is not here. How a transport paces sends, what it does
//! when one fails, and how it settles the live queries an execution registered
//! all depend on what it is sending through, and are the caller's.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use surrealdb_types::{Error as TypesError, Value};
use uuid::Uuid;

use crate::{QueryStreamFrame, QueryStreamItem, QueryType};

/// The first rows frame a statement sends, in records.
///
/// The ramp exists for time-to-first-row: a client waiting on a large `SELECT`
/// sees something after this many records rather than after a full frame. Each
/// subsequent frame doubles up to [`QUERY_BATCH_RECORDS`], so the small frames
/// are confined to the start and a long result still costs one frame per
/// [`QUERY_BATCH_RECORDS`] rows.
const QUERY_FIRST_BATCH_RECORDS: usize = 16;

/// The most records one rows frame carries.
///
/// Bounds the size of a single message so a long result interleaves with
/// whatever else shares the transport — concurrent responses, live-query
/// notifications, pings — instead of occupying it with one enormous frame.
pub const QUERY_BATCH_RECORDS: usize = 256;

/// The error a terminal `End` carries when the stream was stopped rather than
/// answered: cancelled by its consumer, torn down with the connection, or
/// abandoned because its frames could no longer be delivered.
///
/// Its presence is what retracts the statements that never finished, so a
/// truncated answer is never mistaken for a whole one.
pub fn stream_stopped() -> TypesError {
	TypesError::internal("The streaming query was stopped before it completed".to_string())
}

/// The error a terminal `End` carries when live queries the consumer was
/// already told about could not be kept: their session went away mid-query, so
/// the ids it holds will never fire.
///
/// Those statements finished successfully, so an errored `End` does not retract
/// them — retraction covers only statements without a `Finished` frame. The ids
/// are therefore named here, which is what lets a consumer with more than one
/// `LIVE SELECT` in the stream tell which of them is dead.
pub fn live_queries_disowned(ids: &[Uuid]) -> TypesError {
	let ids = ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ");
	TypesError::internal(format!(
		"These live queries were discarded because their session ended, and will never \
		 deliver notifications: {ids}"
	))
}

/// Turns a streaming execution's items into wire frames.
///
/// Holds one statement's rows back only until they fill a frame, so the wire
/// sees them long before the statement — or the query — has finished. Separate
/// from the driver because none of these decisions depend on how the items
/// arrive: a statement's rows are framed the same whether they were produced
/// one batch at a time or all at once.
///
/// Frames are produced one at a time, on demand, and never accumulated: a
/// single item routinely carries a whole statement's result — every sort and
/// aggregate operator emits one batch, as does any statement answered by the
/// legacy evaluator — and pre-framing all of it would hold thousands of row
/// vectors at once and defeat the backpressure the bounded channels provide.
/// Because a statement's rows are taken from the front of a queue rather than
/// drained out of a vector, framing an item costs time linear in its size.
pub struct StreamFrames {
	/// Everything each statement still has to emit.
	pending: HashMap<usize, PendingStatement>,
	/// The statements with work outstanding, in the order they were first seen,
	/// which is the order their frames go out in.
	order: VecDeque<usize>,
	/// Statements already terminated, so a second attempt is ignored.
	///
	/// Two things can end a statement: its own `Finished` item, and a value the
	/// transport could not encode. Whichever happens first wins and the other is
	/// dropped — sending both would put a frame after a terminal one, which the
	/// protocol forbids.
	terminated: HashSet<usize>,
	/// Statements whose terminal frame has actually reached the client. A
	/// statement in here can no longer be retracted: its outcome is something
	/// the client has already been told. It is also what the terminal `End`
	/// counts, since that is the number a client can reconcile against the
	/// `Finished` frames it received.
	delivered: HashSet<usize>,
	/// The live queries this execution produced, each with the statement that
	/// produced it, so settlement can tell an id the client learned from one it
	/// never did.
	live_queries: Vec<(usize, Uuid)>,
}

/// One statement's frames on their way to the wire.
struct PendingStatement {
	/// Rows awaiting framing, taken from the front so that framing an item
	/// costs time linear in its size rather than quadratic.
	///
	/// The deque is what makes that true, and is pinned below: taking a frame
	/// off the front of a `VecDeque` leaves the rows behind it where they are,
	/// where draining the front of a `Vec` shifts every one of them down and
	/// makes framing a whole result quadratic in its size.
	values: VecDeque<Value>,
	/// A single value awaiting its own frame. Emitted before anything else the
	/// statement produces.
	single: Option<Value>,
	/// Records to accumulate before emitting a frame, doubling per frame up to
	/// [`QUERY_BATCH_RECORDS`]. Never zero, which is what lets the emission
	/// loop make progress.
	target: usize,
	/// Set when the statement's value is a single value rather than a list.
	///
	/// This decides the `Finished` frame's `single` flag, and it cannot be
	/// inferred from the count: a `SELECT` returning one row is still a
	/// one-element array, where `SELECT ONLY` returning one row is that row.
	is_single: bool,
	/// The uuid a single-value statement produced, kept until the statement
	/// finishes so a `LIVE SELECT`'s id can be read back there — the point at
	/// which the statement's kind is known.
	live_query: Option<surrealdb_types::Uuid>,
	/// The statement's outcome, once known. Its `Finished` frame goes out after
	/// everything else the statement owes.
	closing: Option<(Duration, QueryType, Option<TypesError>)>,
}

impl Default for PendingStatement {
	fn default() -> Self {
		Self {
			values: VecDeque::new(),
			single: None,
			target: QUERY_FIRST_BATCH_RECORDS,
			is_single: false,
			live_query: None,
			closing: None,
		}
	}
}

const _: () = {
	// Pins the container behind `PendingStatement::values`. A `Vec` there would
	// compile everywhere else and produce byte-identical frames, so nothing
	// observable about the output can catch the swap, while each frame would
	// shift the rest of the statement's rows down to make room.
	fn _rows_wait_in_a_deque(pending: &PendingStatement) {
		let _: &VecDeque<Value> = &pending.values;
	}
};

impl Default for StreamFrames {
	fn default() -> Self {
		Self::new()
	}
}

impl StreamFrames {
	pub fn new() -> Self {
		Self {
			pending: HashMap::new(),
			order: VecDeque::new(),
			terminated: HashSet::new(),
			delivered: HashSet::new(),
			live_queries: Vec::new(),
		}
	}

	/// How many statements the consumer has actually been told the outcome of.
	///
	/// This is what a terminal `End` reports, because it is the number the
	/// consumer can reconcile against the `Finished` frames it received — a
	/// statement whose terminal frame was built but never sent is one it cannot
	/// see.
	pub fn delivered_count(&self) -> usize {
		self.delivered.len()
	}

	/// Whether the consumer holds this statement's outcome.
	pub fn was_delivered(&self, index: usize) -> bool {
		self.delivered.contains(&index)
	}

	/// The live queries this execution produced, each with the statement that
	/// produced it.
	///
	/// A caller settles these itself: whether an id is registered, deleted or
	/// reported as lost depends on whether the consumer was told about it, which
	/// only the caller knows.
	pub fn live_queries(&self) -> &[(usize, Uuid)] {
		&self.live_queries
	}

	/// The entry for `index`, registering it in emission order the first time.
	fn entry(&mut self, index: usize) -> &mut PendingStatement {
		if !self.pending.contains_key(&index) {
			self.order.push_back(index);
		}
		self.pending.entry(index).or_default()
	}

	/// The next frame, or `None` when nothing is ready.
	///
	/// At most one frame is built per call, so a statement's rows reach the wire
	/// a frame at a time and the caller's send is what paces production.
	pub fn pop(&mut self) -> Option<QueryStreamFrame> {
		for position in 0..self.order.len() {
			let index = self.order[position];
			let Some(entry) = self.pending.get_mut(&index) else {
				continue;
			};
			// A single value is not a list, so it is not batched: one frame
			// carries it whole.
			if let Some(value) = entry.single.take() {
				return Some(QueryStreamFrame::Value {
					index,
					value,
				});
			}
			// A full frame's worth while the statement is open; whatever is
			// left once its outcome is known.
			let closing = entry.closing.is_some();
			if entry.values.len() >= entry.target || (closing && !entry.values.is_empty()) {
				let take = entry.target.min(entry.values.len());
				let values: Vec<Value> = entry.values.drain(..take).collect();
				// Ramp toward the maximum, so the small frames that make the
				// first row arrive early do not become a per-frame cost on a
				// long result.
				entry.target = (entry.target * 2).min(QUERY_BATCH_RECORDS);
				return Some(QueryStreamFrame::Rows {
					index,
					values,
				});
			}
			if let Some((time, query_type, error)) = entry.closing.take() {
				let single = error.is_none() && entry.is_single;
				self.pending.remove(&index);
				self.order.remove(position);
				return Some(QueryStreamFrame::Finished {
					index,
					time,
					query_type,
					single,
					error,
				});
			}
		}
		None
	}

	/// Fold one item into what its statement owes the wire.
	pub fn absorb(&mut self, item: QueryStreamItem) {
		// A statement this server already terminated — because a value of its
		// own could not be encoded — takes nothing further. Its rows would
		// otherwise be framed after its terminal frame, which the protocol
		// forbids and a client reading rows as they arrive would act on.
		if self.terminated.contains(&item.index()) {
			return;
		}
		match item {
			QueryStreamItem::Rows {
				index,
				values,
			} => self.entry(index).values.extend(values),
			QueryStreamItem::Value {
				index,
				value,
			} => {
				let entry = self.entry(index);
				entry.is_single = true;
				// Only a `LIVE SELECT`'s id is ever read back from here, so
				// only a uuid is worth keeping: retaining every single
				// statement's value would double the peak memory of the path
				// built to bound it.
				if let Value::Uuid(id) = &value {
					entry.live_query = Some(*id);
				}
				entry.single = Some(value);
			}
			QueryStreamItem::Finished {
				index,
				time,
				query_type,
				error,
			} => {
				// A `LIVE SELECT`'s id rides the stream as this statement's
				// single value, and this is where that statement is known to
				// have produced one.
				if matches!(query_type, QueryType::Live)
					&& error.is_none()
					&& let Some(id) = self.pending.get(&index).and_then(|p| p.live_query)
				{
					self.live_queries.push((index, id.into_inner()));
				}
				self.finished(index, time, query_type, error);
			}
		}
	}

	/// Record a statement's outcome. Its `Finished` frame goes out once the
	/// statement has emitted everything else it owes.
	fn finished(
		&mut self,
		index: usize,
		time: Duration,
		query_type: QueryType,
		error: Option<TypesError>,
	) {
		if !self.terminated.insert(index) {
			// Already terminated; see `terminated`.
			return;
		}
		let failed = error.is_some();
		let entry = self.entry(index);
		// A failed statement's rows are retracted by its error, so nothing
		// residual goes out for it.
		if failed {
			entry.values.clear();
			entry.single = None;
		}
		entry.closing = Some((time, query_type, error));
	}

	/// Fail a statement that has not yet been answered, retracting whatever it
	/// had produced. Returns whether the retraction could be made: a statement
	/// whose terminal frame the client already holds cannot be taken back, and
	/// the caller has to fail the whole stream instead.
	pub fn retract(&mut self, index: usize, error: TypesError) -> bool {
		if self.delivered.contains(&index) {
			return false;
		}
		// The statement may have been closed while its `Finished` frame was
		// still waiting to go out; that outcome has not been seen, so the
		// retraction takes its place.
		self.terminated.remove(&index);
		self.finished(index, Duration::ZERO, QueryType::Other, Some(error));
		true
	}

	/// Record that a statement's terminal frame reached the client.
	pub fn mark_delivered(&mut self, index: usize) {
		self.delivered.insert(index);
	}
}
#[cfg(test)]
mod tests {
	use surrealdb_types::Number;

	use super::*;

	fn int(n: i64) -> Value {
		Value::Number(Number::Int(n))
	}

	fn rows(index: usize, n: usize) -> QueryStreamItem {
		QueryStreamItem::Rows {
			index,
			values: (0..n).map(|i| int(i as i64)).collect(),
		}
	}

	fn finished(index: usize) -> QueryStreamItem {
		QueryStreamItem::Finished {
			index,
			time: Duration::from_millis(1),
			query_type: QueryType::Other,
			error: None,
		}
	}

	fn drain(frames: &mut StreamFrames) -> Vec<QueryStreamFrame> {
		std::iter::from_fn(|| frames.pop()).collect()
	}

	/// How many rows are still waiting to be framed, across every statement.
	fn queued(frames: &StreamFrames) -> usize {
		frames.pending.values().map(|p| p.values.len()).sum()
	}

	/// The first frames are small so the first row arrives early, and they
	/// double toward the cap so a long result is not a per-frame tax.
	#[test]
	fn rows_ramp_from_small_frames_to_the_cap() {
		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, 2048));
		frames.absorb(finished(0));
		let sizes: Vec<usize> = drain(&mut frames)
			.into_iter()
			.filter_map(|f| match f {
				QueryStreamFrame::Rows {
					values,
					..
				} => Some(values.len()),
				_ => None,
			})
			.collect();
		assert_eq!(sizes[..5], [16, 32, 64, 128, 256], "the ramp doubles to the cap");
		assert!(sizes[5..].iter().all(|s| *s <= QUERY_BATCH_RECORDS), "the cap holds");
		assert_eq!(sizes.iter().sum::<usize>(), 2048, "every row goes out exactly once");
	}

	/// A statement's terminal frame is terminal: nothing follows it, and a
	/// second attempt to finish it is dropped.
	#[test]
	fn nothing_follows_a_statements_terminal_frame() {
		let mut frames = StreamFrames::new();
		assert!(frames.retract(0, TypesError::internal("cannot encode".to_string())));
		frames.absorb(rows(0, 100));
		frames.absorb(finished(0));
		let produced = drain(&mut frames);
		assert_eq!(produced.len(), 1, "only the retraction: {produced:?}");
		assert!(matches!(
			produced[0],
			QueryStreamFrame::Finished {
				index: 0,
				error: Some(_),
				..
			}
		));
		assert_eq!(
			frames.terminated.len(),
			1,
			"a statement terminates exactly once in the result count",
		);
	}

	/// A retraction takes back what the client has not seen: rows still
	/// queued for that statement are dropped, and a success finish that was
	/// queued but never delivered is replaced by the failure.
	#[test]
	fn a_retraction_drops_that_statements_queued_frames() {
		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, 16));
		frames.absorb(rows(1, 16));
		frames.absorb(finished(0));
		// Nothing has been sent yet: statement 0's rows and its success finish
		// are both still queued, and the retraction must replace both.
		assert!(frames.retract(0, TypesError::internal("cannot encode".to_string())));
		let produced = drain(&mut frames);
		let for_zero: Vec<&QueryStreamFrame> = produced
			.iter()
			.filter(|f| {
				matches!(
					f,
					QueryStreamFrame::Rows {
						index: 0,
						..
					} | QueryStreamFrame::Finished {
						index: 0,
						..
					}
				)
			})
			.collect();
		assert_eq!(for_zero.len(), 1, "only the retraction survives: {for_zero:?}");
		assert!(matches!(
			for_zero[0],
			QueryStreamFrame::Finished {
				error: Some(_),
				..
			}
		));
		// Statement 1 is untouched.
		assert!(produced.iter().any(|f| matches!(
			f,
			QueryStreamFrame::Rows {
				index: 1,
				..
			}
		)));
	}

	/// An answer the client already holds cannot be taken back: a retraction
	/// against a delivered statement is refused so the caller fails the whole
	/// stream instead of reporting a truncated statement as complete.
	#[test]
	fn a_delivered_statement_cannot_be_retracted() {
		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, 4));
		frames.absorb(finished(0));
		drain(&mut frames);
		frames.mark_delivered(0);
		assert!(
			!frames.retract(0, TypesError::internal("too late".to_string())),
			"a delivered terminal frame cannot be unsaid",
		);
		assert!(drain(&mut frames).is_empty(), "nothing is queued after a refused retraction");
	}

	/// A failed statement's residual rows are retracted, not flushed.
	#[test]
	fn a_failed_statement_retracts_its_residue() {
		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, 10));
		frames.absorb(QueryStreamItem::Finished {
			index: 0,
			time: Duration::ZERO,
			query_type: QueryType::Other,
			error: Some(TypesError::internal("boom".to_string())),
		});
		let produced = drain(&mut frames);
		assert_eq!(produced.len(), 1, "no rows frame precedes the failure: {produced:?}");
		assert!(matches!(
			produced[0],
			QueryStreamFrame::Finished {
				error: Some(_),
				single: false,
				..
			}
		));
	}

	/// A single value goes out whole and its statement finishes `single`, so
	/// the client does not rebuild an array around it; residue below the ramp
	/// threshold goes out with the statement's finish.
	#[test]
	fn single_values_and_small_residues_complete_their_statement() {
		let mut frames = StreamFrames::new();
		frames.absorb(QueryStreamItem::Value {
			index: 0,
			value: int(7),
		});
		frames.absorb(finished(0));
		frames.absorb(rows(1, 3));
		frames.absorb(finished(1));
		let produced = drain(&mut frames);
		assert!(matches!(
			produced[0],
			QueryStreamFrame::Value {
				index: 0,
				..
			}
		));
		assert!(matches!(
			produced[1],
			QueryStreamFrame::Finished {
				index: 0,
				single: true,
				error: None,
				..
			}
		));
		assert!(
			matches!(
				&produced[2],
				QueryStreamFrame::Rows {
					index: 1,
					values,
				} if values.len() == 3
			),
			"a residue below the first ramp target flushes on finish"
		);
		assert!(matches!(
			produced[3],
			QueryStreamFrame::Finished {
				index: 1,
				single: false,
				..
			}
		));
		assert_eq!(frames.terminated.len(), 2, "both statements count toward the result count");
	}

	/// Framing an item costs time proportional to its size, and holds one frame
	/// at a time rather than pre-framing the whole result.
	///
	/// A single item routinely carries an entire statement's result — every
	/// sort and aggregate operator emits exactly one batch — so an item of
	/// hundreds of thousands of rows is an ordinary `ORDER BY`, not an
	/// adversarial input. Framing has no await point, so a cost that grew
	/// faster than the item would wedge the worker thread doing it.
	///
	/// Both halves of that cost are asserted here as exact counts. `absorb`
	/// queues an item's rows without framing any of them, and each `pop` then
	/// takes one frame's worth off the front and leaves the rest alone: every
	/// row is moved out of the queue exactly once, and never more than one
	/// frame's worth at a time. The remaining ingredient — that taking from the
	/// front does not move the rows behind it — belongs to the container rather
	/// than to the frames, which are identical either way, and is pinned at
	/// [`PendingStatement::values`].
	#[test]
	fn framing_a_whole_result_holds_one_frame_at_a_time() {
		const ROWS: usize = 600_000;

		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, ROWS));
		assert_eq!(queued(&frames), ROWS, "absorbing an item queues its rows without framing them");
		frames.absorb(finished(0));

		let mut delivered = 0;
		let mut produced = 0;
		let mut widest = 0;
		while let Some(frame) = frames.pop() {
			produced += 1;
			if let QueryStreamFrame::Rows {
				values,
				..
			} = &frame
			{
				delivered += values.len();
				widest = widest.max(values.len());
				// A frame carries rows off the queue and nothing else touches
				// them, so the two account for the whole result between them at
				// every point in the walk.
				assert_eq!(
					queued(&frames) + delivered,
					ROWS,
					"a frame takes only the rows it carries"
				);
			}
		}

		assert_eq!(delivered, ROWS, "every row goes out exactly once");
		assert_eq!(widest, QUERY_BATCH_RECORDS, "a long result fills its frames to the cap");
		// The ramp doubles from the first frame's size to the cap, so it spends
		// `log2(cap / first)` frames carrying `cap - first` rows between them
		// before every later frame is a full one, and one `Finished` closes the
		// statement.
		let ramp_frames = (QUERY_BATCH_RECORDS / QUERY_FIRST_BATCH_RECORDS).ilog2() as usize;
		let ramp_rows = QUERY_BATCH_RECORDS - QUERY_FIRST_BATCH_RECORDS;
		let expected = ramp_frames + (ROWS - ramp_rows).div_ceil(QUERY_BATCH_RECORDS) + 1;
		assert_eq!(
			produced, expected,
			"one frame per {QUERY_BATCH_RECORDS} rows once the ramp reaches the cap"
		);
	}

	/// A statement's rows keep their order across the frames they are split
	/// into, which is what lets a client concatenate them.
	#[test]
	fn rows_keep_their_order_across_frames() {
		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, 1000));
		frames.absorb(finished(0));
		let mut seen = Vec::new();
		while let Some(frame) = frames.pop() {
			if let QueryStreamFrame::Rows {
				values,
				..
			} = frame
			{
				seen.extend(values);
			}
		}
		let expected: Vec<Value> = (0..1000).map(|i| int(i as i64)).collect();
		assert_eq!(seen, expected, "rows arrive in the order the statement produced them");
	}

	/// Statements are framed independently: each keeps its own ramp, its rows
	/// precede its own terminal frame, and nothing follows it. The order
	/// statements take relative to each other is deliberately not pinned — the
	/// protocol leaves it free, and only each statement's own sequence matters.
	#[test]
	fn statements_are_framed_independently() {
		let mut frames = StreamFrames::new();
		frames.absorb(rows(0, 16));
		frames.absorb(rows(1, 16));
		frames.absorb(finished(1));
		frames.absorb(finished(0));
		let produced = drain(&mut frames);
		for index in [0, 1] {
			let sequence: Vec<&QueryStreamFrame> = produced
				.iter()
				.filter(|f| {
					matches!(f,
						QueryStreamFrame::Rows { index: i, .. }
						| QueryStreamFrame::Finished { index: i, .. } if *i == index)
				})
				.collect();
			assert!(
				matches!(sequence.last(), Some(QueryStreamFrame::Finished { .. })),
				"statement {index} ends with its terminal frame: {sequence:?}",
			);
			assert_eq!(
				sequence.iter().filter(|f| matches!(f, QueryStreamFrame::Finished { .. })).count(),
				1,
				"statement {index} terminates exactly once",
			);
			let rows: usize = sequence
				.iter()
				.filter_map(|f| match f {
					QueryStreamFrame::Rows {
						values,
						..
					} => Some(values.len()),
					_ => None,
				})
				.sum();
			assert_eq!(rows, 16, "statement {index} delivers every row exactly once");
		}
		assert_eq!(frames.terminated.len(), 2, "each statement terminates once");
	}
}
