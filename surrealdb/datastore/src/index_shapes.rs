//! The value shapes a B-tree index has stored, per column.
//!
//! The index writer lists a record holding an array once per element of it,
//! and stores every other value whole, at the one place it sorts. Whether a
//! column has stored either shape decides what a scan of the index may claim:
//! an index whose column has stored no array holds each record at one entry,
//! at the place its whole value sorts, so a scan over it orders the column,
//! lists each record once and answers a range exactly. One whose element
//! column has stored nothing whole holds every record under its elements, so
//! an element seek answers containment.
//!
//! # Contract
//!
//! - **Recorded with the entries.** Every path that writes an entry records the shapes of the
//!   values it writes in the transaction that writes the entries
//!   ([`Transaction::record_index_shapes`]): a document write merges them into the index's `!ia`
//!   key, a build's scan and its replay of queued writes into its `!ir` key. Each merge reads its
//!   key before it writes it, so two recorders of one kind adding shapes concurrently conflict
//!   rather than lose one, while a build never writes the key a statement writes — a statement that
//!   defines an index and writes to its table in one transaction does not conflict with the build.
//! - **Tracked.** The transaction that begins a build clears the index's data, both keys with it,
//!   writes the index's `!it` marker, and records in `!ir` the shape each column is expected to
//!   store. From then on the two keys together hold every shape the index stores: an entry a writer
//!   adds is recorded with it, and an entry the clearing removed is written again, with its shape,
//!   by the build's scan. A writer storing the shapes its columns are expected to records nothing,
//!   so a table's concurrent first writers do not conflict over the index's keys and catalog
//!   version.
//! - **Only added.** A shape is recorded once and stays until the next build clears the index. A
//!   delete never clears one: an index has no cheap way to prove no value of the shape is left.
//! - **Read at the snapshot.** A writer reads the shapes as its transaction's snapshot holds them,
//!   so a shape it skips recording is one its snapshot holds recorded.
//! - **Versioned.** A document write that records a shape stamps the table's catalog version, so no
//!   plan or compiled program made before the shape was stored is reused after it. A build's own
//!   recording stamps nothing: the index serves no read until the build publishes it, and every
//!   shape the build records commits before it does.
//! - **Shared for planning.** A planner reads the shapes of the indexes whose build had finished
//!   through the node's cache, under the table's catalog version
//!   ([`Transaction::planning_index_shapes`]); every other index's are read at the snapshot.
//! - **Untracked.** An index with no `!it` marker was built by a release that recorded no shapes.
//!   It may hold either shape in any column, whatever its keys hold.
//!
//! [`Transaction::record_index_shapes`]: crate::Transaction::record_index_shapes
//! [`Transaction::planning_index_shapes`]: crate::Transaction::planning_index_shapes

use std::sync::Arc;

use crate::catalog::IndexId;

/// One shape of value an index column stores.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueShape {
	/// An array, listed once per element: an empty one under `NULL`.
	Array,
	/// A value other than `NONE` or `NULL`, stored whole: a scalar, a set, an
	/// object, or an array in a column the writer does not expand.
	Whole,
}

impl ValueShape {
	const fn bit(self) -> u8 {
		match self {
			ValueShape::Array => 1,
			ValueShape::Whole => 2,
		}
	}
}

/// Who records a shape, and so which key it is merged into.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShapeRecorder {
	/// A document write, merging into `!ia`.
	Write,
	/// An index build's scan or replay, merging into `!ir`.
	Build,
}

/// The shapes one index has stored, per column, as one transaction sees them.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexShapes {
	/// Whether the index's `!it` marker is present.
	tracked: bool,
	/// Per column, the bits of the shapes recorded under `!ia`; a column past
	/// the end has recorded none.
	written: Vec<u8>,
	/// Per column, the bits of the shapes recorded under `!ir`.
	built: Vec<u8>,
}

impl IndexShapes {
	/// The shapes of an index whose `!it` marker is present or not, whose
	/// `!ia` key holds `written` and whose `!ir` key holds `built`.
	pub fn new(tracked: bool, written: Vec<u8>, built: Vec<u8>) -> Self {
		Self {
			tracked,
			written,
			built,
		}
	}

	/// Whether the index records every shape it stores.
	pub fn is_tracked(&self) -> bool {
		self.tracked
	}

	/// Whether column `col` may hold a value of `shape`: it has stored one, or
	/// the index is untracked.
	pub fn may_hold(&self, col: usize, shape: ValueShape) -> bool {
		!self.tracked || self.has(col, shape)
	}

	/// Whether either key records `shape` for column `col`.
	fn has(&self, col: usize, shape: ValueShape) -> bool {
		[&self.written, &self.built]
			.into_iter()
			.any(|bits| bits.get(col).is_some_and(|bits| bits & shape.bit() != 0))
	}

	/// Whether every one of `shapes` is recorded, so a write of them records
	/// nothing.
	pub fn records_all(&self, shapes: impl IntoIterator<Item = (u16, ValueShape)>) -> bool {
		shapes.into_iter().all(|(col, shape)| self.has(usize::from(col), shape))
	}

	/// The value of `by`'s key with `shapes` recorded beside those already
	/// there, or `None` when every one is recorded.
	pub(crate) fn with(&self, shapes: &[(u16, ValueShape)], by: ShapeRecorder) -> Option<Vec<u8>> {
		if self.records_all(shapes.iter().copied()) {
			return None;
		}
		let mut stored = self.of(by).to_vec();
		for &(col, shape) in shapes {
			let col = usize::from(col);
			if stored.len() <= col {
				stored.resize(col + 1, 0);
			}
			stored[col] |= shape.bit();
		}
		Some(stored)
	}

	/// The bits `by`'s key holds.
	pub(crate) fn of(&self, by: ShapeRecorder) -> &[u8] {
		match by {
			ShapeRecorder::Write => &self.written,
			ShapeRecorder::Build => &self.built,
		}
	}

	/// These shapes, with `by`'s key holding `stored`.
	pub(crate) fn replacing(&self, by: ShapeRecorder, stored: Vec<u8>) -> Self {
		let mut next = self.clone();
		match by {
			ShapeRecorder::Write => next.written = stored,
			ShapeRecorder::Build => next.built = stored,
		}
		next
	}
}

/// The shapes the B-tree indexes of one table have stored, as one transaction
/// sees them ([`Transaction::table_index_shapes`]).
///
/// [`Transaction::table_index_shapes`]: crate::Transaction::table_index_shapes
#[derive(Clone, Debug, Default)]
pub struct TableIndexShapes(pub(crate) Arc<[(IndexId, Arc<IndexShapes>)]>);

impl TableIndexShapes {
	/// The shapes of index `ix`, or `None` for an index these were not read
	/// for, which may hold any.
	pub fn of(&self, ix: IndexId) -> Option<&Arc<IndexShapes>> {
		self.0.iter().find(|(id, _)| *id == ix).map(|(_, shapes)| shapes)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn an_untracked_index_may_hold_every_shape_and_still_records() {
		let shapes = IndexShapes::new(false, Vec::new(), Vec::new());
		assert!(shapes.may_hold(0, ValueShape::Array));
		assert!(shapes.may_hold(3, ValueShape::Whole));
		assert!(!shapes.records_all([(0, ValueShape::Array)]));
		assert_eq!(shapes.with(&[(0, ValueShape::Array)], ShapeRecorder::Write), Some(vec![1]));
	}

	#[test]
	fn a_tracked_index_holds_what_either_recorder_recorded() {
		let shapes = IndexShapes::new(true, Vec::new(), Vec::new());
		assert!(!shapes.may_hold(0, ValueShape::Array));
		let written = shapes
			.with(&[(1, ValueShape::Whole)], ShapeRecorder::Write)
			.expect("a shape to record");
		assert_eq!(written, vec![0, 2]);
		let shapes = shapes.replacing(ShapeRecorder::Write, written);
		assert!(shapes.may_hold(1, ValueShape::Whole));
		assert!(!shapes.may_hold(1, ValueShape::Array));
		assert_eq!(shapes.with(&[(1, ValueShape::Whole)], ShapeRecorder::Build), None);
		let built = shapes
			.with(&[(0, ValueShape::Array)], ShapeRecorder::Build)
			.expect("a shape to record");
		assert_eq!(built, vec![1], "a build merges into its own key");
		let shapes = shapes.replacing(ShapeRecorder::Build, built);
		assert!(shapes.may_hold(0, ValueShape::Array));
		assert!(shapes.records_all([(0, ValueShape::Array), (1, ValueShape::Whole)]));
	}
}
