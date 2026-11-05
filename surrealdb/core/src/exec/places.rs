//! The places a host runs a morsel pipeline's morsels at, beside the node's own.
//!
//! A morsel pipeline's morsels run on the node's places by default: the statement's task and the
//! workers of the node's pool (`exec::vm::morsel::places`), which read their key ranges through the
//! transaction's read view, give part of a long range to an idle worker and park when their output
//! outgrows the merge's window. A host that can run morsels elsewhere — the leaves of a fan-out
//! tree — supplies [`MorselPlaces`] through the datastore
//! ([`Datastore::with_morsel_places`](crate::kvs::Datastore::with_morsel_places)), and the
//! morsels the policy cuts from a scan are handed to it as well as to the node's workers.
//!
//! What a host place runs is decided where the statement is planned, never by the place: the
//! logical plan states the source split, the chain's legality, the sink's combine and the arrival
//! order; the physical plan decides admission, the merge order, the write cut and the pruning;
//! lowering copies those decisions into the program. A host place runs one morsel's half of the
//! pipeline over the morsel's key range and answers its partials. The statement's task merges
//! those exactly as it merges the partials of a morsel its own workers ran, so the rows, their
//! order, the first failure and the order of effects are the serial run's whichever place ran a
//! morsel.
//!
//! The contract a host keeps:
//!
//! - A morsel is run against the transaction's read view the [`MorselPlaces::run`] call names, as
//!   of the morsel's version, and nothing else: a leaf reads the morsel's key range from a reader
//!   that answers what that view answers, and reads no other key.
//! - The frames of one morsel are answered in the order the leaf produced them, and the last frame
//!   is the morsel's end (the last `Datastore::run_morsel` answers). The frames are opaque bytes to
//!   the host.
//! - A place may run a morsel more than once (a hedged copy), and answers the frames of exactly one
//!   run. A run of a read pipeline reads and writes nothing else, so which copy answers is not
//!   observable.
//! - A stream that ends with an error, or before the end frame, is a failure of the place, not of
//!   the morsel: where no frame of it was taken the statement runs the morsel itself, and otherwise
//!   the statement fails with the error.
//!
//! The frames, the unit and the morsel are versioned: a leaf refuses a unit of a format or an
//! engine build it does not run, and the statement then runs the morsel itself.

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use surrealdb_kvs::Snapshot;

pub use crate::exec::vm::morsel::wire::{LeafStats, frame_stats, unit_reads_row};

/// The frames a place answers for one morsel, in the order the leaf produced them.
pub type MorselFrames = Pin<Box<dyn Stream<Item = anyhow::Result<Vec<u8>>> + Send + 'static>>;

/// A compiled morsel unit: the half of a pipeline a morsel runs, with every value it reads,
/// encoded for a leaf running the same engine build. One unit serves every morsel of a scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MorselUnit {
	bytes: Arc<[u8]>,
}

impl MorselUnit {
	pub(crate) fn new(bytes: Vec<u8>) -> Self {
		Self {
			bytes: bytes.into(),
		}
	}

	/// The encoded unit, which a leaf hands to `Datastore::run_morsel`.
	pub fn bytes(&self) -> &[u8] {
		&self.bytes
	}
}

/// One morsel: a key range of the scan, encoded for the leaf. The bounds are those of the read
/// view's keys; a leaf reads `[start, end)` in the unit's direction.
#[derive(Clone, Debug)]
pub struct Morsel {
	/// The first key of the range.
	pub start: Vec<u8>,
	/// The key past the range's last.
	pub end: Vec<u8>,
	/// The encoded morsel the leaf runs: its range, its paging and what the statement shares with
	/// it as it stands.
	pub bytes: Vec<u8>,
}

/// The places a host runs morsels at.
pub trait MorselPlaces: Send + Sync + 'static {
	/// The most morsels of one scan the host runs at once: the scan hands it no more than this many
	/// at a time. Zero hands it none.
	fn width(&self) -> usize;

	/// Run `morsel` of `unit` at a place, reading as of `view`, the transaction's read view, and
	/// answer the frames the leaf's run produced.
	fn run(&self, unit: &MorselUnit, morsel: Morsel, view: &Arc<dyn Snapshot>) -> MorselFrames;
}

/// The loopback places of width `width` (`morsel_loopback_places`), or `None` for a width of zero
/// or a build without the in-memory store a loopback leaf runs on.
pub(crate) fn loopback(width: usize) -> Option<Arc<dyn MorselPlaces>> {
	#[cfg(feature = "kv-mem")]
	if width > 0 {
		return Some(Arc::new(LoopbackPlaces::new(width)));
	}
	let _ = width;
	None
}

/// Places that run every morsel in a second engine of this process, as a host's leaf runs it: the
/// unit, the morsel and every frame cross as bytes, the leaf's engine is a leaf's
/// ([`Builder::for_leaf`](crate::kvs::Builder::for_leaf)), and the leaf reads the morsel's range
/// through the read view it is handed. What a statement answers through them is what it answers
/// through any host's places, so a conformance run through them checks the host-place contract.
#[cfg(feature = "kv-mem")]
pub struct LoopbackPlaces {
	width: usize,
	/// The leaf's engine, opened when the first morsel runs.
	leaf: Arc<tokio::sync::OnceCell<Arc<crate::kvs::Datastore>>>,
	/// The morsels run, and the frames answered.
	runs: Arc<std::sync::atomic::AtomicUsize>,
	frames: Arc<std::sync::atomic::AtomicUsize>,
	/// What the leaf's end frames stated, summed.
	stats: Arc<std::sync::Mutex<LeafStats>>,
}

#[cfg(feature = "kv-mem")]
impl LoopbackPlaces {
	/// Loopback places that run `width` morsels of a scan at once.
	pub fn new(width: usize) -> Self {
		Self {
			width,
			leaf: Arc::default(),
			runs: Arc::default(),
			frames: Arc::default(),
			stats: Arc::default(),
		}
	}

	/// The morsels these places have run.
	pub fn runs(&self) -> usize {
		self.runs.load(std::sync::atomic::Ordering::Acquire)
	}

	/// The frames these places have answered.
	pub fn frames(&self) -> usize {
		self.frames.load(std::sync::atomic::Ordering::Acquire)
	}

	/// The figures of every morsel these places ran to its end, summed; `reads_row` holds when it
	/// held for any of them.
	pub fn stats(&self) -> LeafStats {
		*self.stats.lock().unwrap_or_else(|e| e.into_inner())
	}
}

#[cfg(feature = "kv-mem")]
impl MorselPlaces for LoopbackPlaces {
	fn width(&self) -> usize {
		self.width
	}

	fn run(&self, unit: &MorselUnit, morsel: Morsel, view: &Arc<dyn Snapshot>) -> MorselFrames {
		use futures::StreamExt;
		self.runs.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
		let unit = unit.bytes().to_vec();
		let view = Arc::clone(view);
		let leaf = Arc::clone(&self.leaf);
		let frames = Arc::clone(&self.frames);
		let stats = Arc::clone(&self.stats);
		let run = async move {
			let leaf = leaf
				.get_or_try_init(|| crate::kvs::Builder::for_leaf().build_with_path("memory"))
				.await
				.map_err(|e| e.context("Failed to open the loopback leaf's engine"));
			match leaf {
				Ok(leaf) => leaf.run_morsel(&unit, &morsel, view).await,
				Err(e) => Box::pin(futures::stream::once(async move { Err(e) })) as MorselFrames,
			}
		};
		Box::pin(futures::stream::once(run).flatten().inspect(move |frame| {
			frames.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
			if let Some(ended) = frame.as_ref().ok().and_then(|frame| frame_stats(frame)) {
				let mut sum = stats.lock().unwrap_or_else(|e| e.into_inner());
				sum.entries += ended.entries;
				sum.key_bytes += ended.key_bytes;
				sum.value_bytes += ended.value_bytes;
				sum.chunks += ended.chunks;
				sum.elapsed_ns += ended.elapsed_ns;
				sum.rows_judged += ended.rows_judged;
				sum.rows_emitted += ended.rows_emitted;
				sum.column_reads += ended.column_reads;
				sum.decoded += ended.decoded;
				sum.reads_row |= ended.reads_row;
			}
		}))
	}
}
