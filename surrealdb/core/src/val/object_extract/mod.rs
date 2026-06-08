//! Walker-based field extraction for revision-encoded
//! [`Record`](crate::catalog::Record) bytes.
//!
//! Table rows are encoded as revisioned [`crate::catalog::Record`] values
//! whose payload is typically a [`Value::Object`] with sorted [`Strand`] keys
//! backed by [`surrealdb_collections::VecMap`]. The pre-decode filter walks
//! into nested fields without materialising the entire `Value` tree by using
//! the per-type walkers emitted by `revision`'s `#[revisioned(...)]` derive.

use revision::optimised::IndexedMapWalker;
use revision::{
	BorrowedReader, DeserializeRevisioned, Error as RevisionError, SerializeRevisioned,
	WalkRevisioned,
};
use surrealdb_strand::Strand;
use wire_skip::{rev2_optimised_payload_unchecked, skip_value_wire};

use crate::catalog::Record;
use crate::val::Value;

// Note: there is intentionally no bail on `metadata`. `metadata` flags the
// record type (plain vs. edge) and carries `aggregation_stats` for
// materialised-view rows, but in **all** cases the `data` field stores the
// user-visible value at the time of last write:
//
//   - plain table rows: `data` is the row's value;
//   - edge rows: `data` is the row's value (with `in` / `out` graph references plus user fields);
//   - materialised-view rows: `data` is populated by evaluating the view's `SELECT` expressions
//     against the aggregated internal document (see `doc::table::compute` setting `record.data =
//     data`), so it is exactly the value the engine returns to the user.
//
// The pre-decode filter therefore reads `data` for every record kind.

mod tests;
pub(crate) mod wire_skip;

/// A path segment paired with its pre-encoded `Strand` wire form
/// (`<usize varint || utf8>`).
///
/// Path segments are fixed at plan time. Storing the wire form alongside
/// the UTF-8 string eliminates a per-row `Vec<u8>` allocation in
/// [`lookup_value_bytes_in_map`]'s indexed binary-search comparator.
///
/// [`Strand`] is 24 bytes with an inline capacity of 23 (see
/// [`surrealdb_strand::Strand`]), so field names like `name`, `email`, or
/// `metadata` carry no heap allocation. `wire` is `Box<[u8]>` so the
/// pre-encoded bytes own exactly their needed length with no extra
/// capacity slot.
#[derive(Debug, Clone)]
pub(crate) struct PathSegment {
	utf8: Strand,
	wire: Box<[u8]>,
}

impl PathSegment {
	/// Construct from a UTF-8 string, pre-serialising the [`Strand`] wire
	/// form once.
	pub(crate) fn new(utf8: impl Into<Strand>) -> Self {
		let utf8: Strand = utf8.into();
		let mut wire = Vec::with_capacity(utf8.len() + 4);
		<Strand as SerializeRevisioned>::serialize_revisioned(&utf8, &mut wire)
			.expect("Vec writer never errors");
		Self {
			utf8,
			wire: wire.into_boxed_slice(),
		}
	}

	/// UTF-8 view of the segment name. Borrows from the inline [`Strand`]
	/// (no allocation, no copy).
	#[inline]
	pub(crate) fn as_str(&self) -> &str {
		self.utf8.as_str()
	}

	/// UTF-8 bytes of the segment name (no length prefix).
	#[inline]
	pub(crate) fn as_bytes(&self) -> &[u8] {
		self.utf8.as_bytes()
	}

	/// The owning [`Strand`] for the segment name. Lets callers reuse the
	/// stored small-string without re-allocating.
	#[inline]
	pub(crate) fn as_strand(&self) -> &Strand {
		&self.utf8
	}

	/// Pre-encoded [`Strand`] wire bytes: `<usize varint || utf8>`. Used
	/// as the needle for [`revision::optimised::IndexedMapWalker::find_value_bytes`].
	#[inline]
	pub(crate) fn wire(&self) -> &[u8] {
		&self.wire
	}
}

impl From<&str> for PathSegment {
	fn from(s: &str) -> Self {
		Self::new(s)
	}
}

impl From<String> for PathSegment {
	fn from(s: String) -> Self {
		Self::new(s)
	}
}

impl From<Strand> for PathSegment {
	fn from(s: Strand) -> Self {
		Self::new(s)
	}
}

/// Equality / ordering compare the segment by its UTF-8 form only — the
/// wire bytes are derived from the UTF-8 string, so two `PathSegment`s
/// with equal `utf8` always have equal `wire`. Comparing on `utf8`
/// avoids the per-comparison double-cost of touching the wire slice.
impl PartialEq for PathSegment {
	fn eq(&self, other: &Self) -> bool {
		self.utf8 == other.utf8
	}
}

impl Eq for PathSegment {}

impl PartialOrd for PathSegment {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for PathSegment {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.utf8.cmp(&other.utf8)
	}
}

/// Missing path segment while walking to a leaf.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum WalkLeafErr {
	Missing,
	Bail,
}

/// Result of extracting a field from a revisioned record.
#[derive(Debug)]
pub(crate) enum Extracted {
	/// The field was reachable and decoded into a `Value`.
	Found(Value),
	/// At least one path segment was missing in the record's object tree;
	/// callers treat this as semantic [`Value::None`].
	Missing,
	/// The path could not be reached because an intermediate value was not
	/// an object, or a wire-level error occurred while decoding. Callers
	/// should fall back to a full decode.
	Bail,
}

/// Walk the revisioned `record_bytes` along `path` and decode the leaf as a
/// [`Value`].
///
/// `depth_limit` bounds the number of path segments we descend through. The
/// pre-decode filter threads `ctx.config.idiom_recursion_limit` (default 256)
/// from the planner; paths longer than the limit return [`Extracted::Bail`],
/// causing the caller to fall back to full-record decode + post-decode
/// evaluation.
///
/// Returns:
/// - [`Extracted::Found`] when every segment is present in the record's nested object map and the
///   leaf decodes successfully.
/// - [`Extracted::Missing`] when an intermediate or leaf object key is absent at any level.
/// - [`Extracted::Bail`] for edge / view records, non-object intermediates, paths longer than
///   `depth_limit`, or any wire-level error.
pub(crate) fn extract_field_from_record_bytes(
	record_bytes: &[u8],
	path: &[PathSegment],
	depth_limit: u32,
) -> Extracted {
	extract_field_from_record_bytes_parts(record_bytes, &[], path, depth_limit)
}

/// Like [`extract_field_from_record_bytes`] but walks `prefix` segments
/// first, then `path`, without concatenating into a new [`Vec`].
///
/// Lets per-record evaluators in `pre_decode_filter` avoid allocating a
/// `Vec<PathSegment>` of `prefix ++ path` in their hot loops. `prefix` is a
/// slice of [`PathSegment`] references because the upstream
/// `PreDecodeFilter::eval_node` accumulates [`NavigatePrefix`] segments by
/// pointer, not by clone — see that file's [`PathSegment::clone`] cost note.
pub(crate) fn extract_field_from_record_bytes_parts(
	record_bytes: &[u8],
	prefix: &[&PathSegment],
	path: &[PathSegment],
	depth_limit: u32,
) -> Extracted {
	if prefix.is_empty() && path.is_empty() {
		return Extracted::Bail;
	}
	// Open the record walker, get the `data` field's wire bytes via the
	// macro-emitted accessor. For rev-2 `indexed_struct` records this is
	// O(1) (read `(data_off, data_off+1)` from the prologue, slice
	// straight to the field); for rev-1 it's a sequential `metadata` skip.
	// `into_data_bytes()` returns `Cow<'_, [u8]>` which derefs to
	// `&[u8]` for the inner `Value::walk_revisioned` follow-up.
	let mut record_reader: &[u8] = record_bytes;
	let data_bytes =
		match Record::walk_revisioned(&mut record_reader).and_then(|w| w.into_data_bytes()) {
			Ok(b) => b,
			Err(_) => return Extracted::Bail,
		};
	let mut reader: &[u8] = &data_bytes;
	let value_walker = match Value::walk_revisioned(&mut reader) {
		Ok(w) => w,
		Err(_) => return Extracted::Bail,
	};
	descend_value_path_parts(value_walker, prefix, path, depth_limit)
}

/// Construct an [`IndexedMapWalker`] directly from an [`Object`]'s rev-2
/// wire bytes, **bypassing** the macro-generated
/// `Object::walk_revisioned` → `into_walk_field_0` → `walker()` chain.
///
/// Why: the macro path parses the indexed-map prologue twice per descent.
/// `into_walk_field_0` runs `skip_indexed_map` (O(1) on indexed bodies
/// since revision 0.26.0, but it still reads the offset table and dense-
/// region lengths to derive the field's exact bytes for the
/// `IndexedMapView`). Then [`IndexedMapView::walker`] calls
/// [`IndexedMapWalker::from_payload`] on those same bytes — re-reading and
/// validating the prologue a second time. Constructing the walker
/// directly from the envelope payload parses the prologue once.
///
/// `Object` is `#[revisioned(revision(1), revision(2, optimised))]` with a
/// single `#[revision(indexed_map)]` field, so its rev-2 wire layout is
/// fully known:
///
/// ```text
///   <varint u16 rev=2>  ||  <u32_le payload_length>  ||  <indexed_map body>
/// ```
///
/// Since the indexed_map is the **only** field, the payload IS the
/// indexed_map body — no offset prologue, no field separator. We read the
/// rev prefix, validate it's 2, skip the `u32_le` envelope length, and
/// feed the remainder straight to
/// [`IndexedMapWalker::from_payload_unvalidated`].
///
/// Uses `from_payload_unvalidated` rather than the validating
/// `from_payload`. The validating constructor eagerly walks the whole
/// offset table and key region (`validate_map_prologue` +
/// `validate_key_region_ascending`) on **every** record — O(fields), which
/// a no-index scan profile showed costing ~6.8 % of total CPU, roughly five
/// times the O(log fields) binary-search lookup it guards. That validation
/// existed only because the walker's region slices were unchecked: under
/// the workspace release profile's `panic = 'abort'`, a corrupt offset
/// (disk bit-rot, FS corruption, an off-path serialiser) would slice out of
/// bounds and abort the whole `surrealdb` process.
///
/// As of `revision` 0.28 those slices are bounds-checked at the point of
/// use: `find_value_bytes` returns
/// [`Error::OptimisedOffsetOutOfRange`] and `entries` clamps to an empty
/// slice instead of panicking. So the unvalidated walker preserves the
/// graceful-recovery contract (corruption → `Err` → caller falls through to
/// full decode) without the per-record O(fields) tax. RocksDB block
/// checksums already detect storage corruption upstream, so the only
/// residual difference from the validating path — a corrupt *non-ascending*
/// key region making a lookup report a present key as absent — is both
/// already-guarded-against and, on the rare miss, recovered via the
/// full-decode fallback.
///
/// Returns the rev prefix-validated walker on success, or an error on
/// non-2 rev (callers fall back to the macro-emitted path) or a truncated
/// envelope.
///
/// [`Error::OptimisedOffsetOutOfRange`]: revision::Error::OptimisedOffsetOutOfRange
fn indexed_map_walker_from_object_bytes(
	object_wire: &[u8],
) -> Result<IndexedMapWalker<'_, Strand, Value>, RevisionError> {
	// Caller is descending from a rev-2 `Value::Object(_)`; the parent
	// walker already validated the outer `Value` rev, and by macro
	// construction the inner `Object` shares the rev. Skip the inner
	// rev re-read.
	let payload = rev2_optimised_payload_unchecked(object_wire)?;
	IndexedMapWalker::<'_, Strand, Value>::from_payload_unvalidated(payload)
}

/// Serialise pre-validated UTF-8 bytes to their on-wire `Strand`
/// representation (`<usize varint len || utf8>`) for sites that don't
/// have a [`PathSegment`] handy (test helpers, scan needle preparation).
fn strand_wire_bytes_from_utf8(utf8: &[u8]) -> Vec<u8> {
	let mut out = Vec::with_capacity(utf8.len() + 4);
	<usize as SerializeRevisioned>::serialize_revisioned(&utf8.len(), &mut out)
		.expect("Vec writer never errors");
	out.extend_from_slice(utf8);
	out
}

/// Look up a `needle` key in an indexed-map walker and return the matching value's
/// wire bytes. Handles both the indexed path (O(log n) binary search) and the
/// legacy sub-threshold path (`flags = 0`, no offset table — fall back to a
/// linear scan over the dense `(Strand, Value)*` body without allocating a `Strand`).
///
/// Wire-form note: `IndexedMapWalker::find_value_bytes`'s predicate compares
/// against the **full wire bytes** of each key (`<usize varint len || utf8>`),
/// and the keys are sorted by wire bytes (which means by `(length, utf8 bytes)`,
/// not by UTF-8 codepoint order). The caller passes a [`PathSegment`] whose
/// `wire()` is the pre-encoded needle (one allocation at plan time, none on
/// the hot path); on the legacy / sub-threshold path we strip each entry's
/// length prefix and compare against the segment's UTF-8 bytes directly. The
/// legacy scan does not short-circuit on a "greater" key because wire order
/// is not UTF-8 order.
fn lookup_value_bytes_in_map<'p>(
	map_walker: &IndexedMapWalker<'p, Strand, Value>,
	needle: &PathSegment,
) -> Result<Option<&'p [u8]>, RevisionError> {
	let needle_utf8 = needle.as_bytes();
	if map_walker.is_indexed() {
		let needle_wire = needle.wire();
		return map_walker.find_value_bytes(|kb: &[u8]| kb.cmp(needle_wire));
	}
	// Legacy sub-threshold body: `(Strand wire || Value wire)*` with `len`
	// known up front. Walk all entries, comparing UTF-8 bytes against the
	// needle — wire order may differ from UTF-8 order when entries have
	// varying lengths, so we can't short-circuit.
	let Some(body) = map_walker.legacy_body() else {
		return Ok(None);
	};
	let len = map_walker.len();
	let mut reader: &[u8] = body;
	for _ in 0..len {
		let key_len = <usize as DeserializeRevisioned>::deserialize_revisioned(&mut reader)?;
		if reader.len() < key_len {
			return Err(RevisionError::Deserialize(
				"legacy indexed-map body: key length exceeds remaining bytes".into(),
			));
		}
		let key_bytes = &reader[..key_len];
		reader = &reader[key_len..];
		if key_bytes == needle_utf8 {
			let v_start = body.len() - reader.len();
			let mut probe: &[u8] = reader;
			skip_value_wire(&mut probe)?;
			let v_end = body.len() - probe.len();
			return Ok(Some(&body[v_start..v_end]));
		}
		skip_value_wire(&mut reader)?;
	}
	Ok(None)
}

/// Borrowed cursor at any nested object inside a revision-encoded `Value`:
/// walks `prefix` segments first, then `path`, without concatenating into a
/// new [`Vec`]. The decoded leaf is the value at the end of `path` (or, when
/// `path` is empty, at the end of `prefix`).
///
/// `depth_limit` is the hard upper bound on path segment count; paths longer
/// than the limit return [`Extracted::Bail`]. Sourced from
/// `ctx.config.idiom_recursion_limit` at the planner-side construction of
/// [`crate::exec::pre_decode_filter::PreDecodeFilter`].
///
/// **Iterative form** — earlier revisions used recursion because each level
/// holds a `VariantView<Object>` and `IndexedMapView` from which the next
/// level's value bytes borrow; the recursive frame kept those views alive
/// for the duration of the descent. The current form copies each
/// intermediate object's wire bytes into a small owned `Vec<u8>` per level,
/// which breaks the borrow chain cleanly without sacrificing soundness on
/// the cross-revision `convert_fn` path (where the underlying `Cow` is
/// already `Owned`). For the leaf and the root we never copy; the per-level
/// allocation only fires for intermediate segments. With `depth_limit` of
/// 256, the worst case is 254 small `Vec<u8>` allocations per descent, all
/// freed at function return.
fn descend_value_path_parts<'r, R: BorrowedReader>(
	value_walker: <Value as WalkRevisioned>::Walker<'r, R>,
	prefix: &[&PathSegment],
	path: &[PathSegment],
	depth_limit: u32,
) -> Extracted {
	let total = prefix.len() + path.len();
	if total == 0 || total > depth_limit as usize {
		return Extracted::Bail;
	}
	// Recursive descent: each frame holds its own `OwnedIndexedMapView`
	// (the `view` / `object_walker` / `map_view` / `map_walker` chain)
	// alive on the stack, and the recursive call borrows the inner
	// value's wire bytes from that frame. No intermediate `Vec<u8>`
	// copy per level — the previous iterative form copied each level's
	// body into a fresh `Vec` only to break the borrow-checker chain.
	descend_value_recursive(value_walker, prefix, path)
}

/// Inner recursion for [`descend_value_path_parts`]. Pops one segment
/// per call from `prefix` first, then `path`; the terminal step (no
/// segments left) is unreachable because each call short-circuits on
/// `segments.len() == 1`. Returns [`Extracted::Bail`] for non-object
/// intermediates, [`Extracted::Missing`] for absent map keys, and a
/// decoded leaf [`Value`] for success.
fn descend_value_recursive<'r, R: BorrowedReader>(
	walker: <Value as WalkRevisioned>::Walker<'r, R>,
	prefix: &[&PathSegment],
	path: &[PathSegment],
) -> Extracted {
	let needle: &PathSegment = if let Some(&s) = prefix.first() {
		s
	} else if let Some(s) = path.first() {
		s
	} else {
		// Caller guards against empty (prefix ++ path), so this only
		// fires if the recursive trampoline is reached with no work
		// left — treat as bail.
		return Extracted::Bail;
	};
	if !walker.is_object() {
		return Extracted::Bail;
	}
	let view = match walker.object_view() {
		Ok(v) => v,
		Err(_) => return Extracted::Bail,
	};
	// Bypass the macro-emitted `walk_revisioned → into_walk_field_0 →
	// walker()` chain — that path parses the indexed-map prologue twice
	// (once via `skip_indexed_map` to derive the field's bytes, then again
	// in `IndexedMapWalker::from_payload`). See
	// `indexed_map_walker_from_object_bytes` for the manual rev-2 envelope
	// decode.
	let map_walker = match indexed_map_walker_from_object_bytes(view.as_bytes()) {
		Ok(w) => w,
		Err(_) => return Extracted::Bail,
	};
	let value_bytes = match lookup_value_bytes_in_map(&map_walker, needle) {
		Ok(Some(b)) => b,
		Ok(None) => return Extracted::Missing,
		Err(_) => return Extracted::Bail,
	};
	// One segment consumed — slice the remaining `(prefix, path)`. We pop
	// from `prefix` first (the `NavigatePrefix` chain accumulated by the
	// upstream pre-decode filter) and only descend into `path` once
	// `prefix` is empty.
	let (next_prefix, next_path): (&[&PathSegment], &[PathSegment]) = if prefix.is_empty() {
		(&[], &path[1..])
	} else {
		(&prefix[1..], path)
	};
	if next_prefix.is_empty() && next_path.is_empty() {
		// Leaf: decode the value.
		let mut leaf_reader: &[u8] = value_bytes;
		return match <Value as DeserializeRevisioned>::deserialize_revisioned(&mut leaf_reader) {
			Ok(v) => Extracted::Found(v),
			Err(_) => Extracted::Bail,
		};
	}
	// Intermediate: descend one more level. The new walker borrows from
	// `value_bytes`, which borrows from `view` (alive on this frame).
	let mut value_reader: &[u8] = value_bytes;
	let inner_walker = match <Value as WalkRevisioned>::walk_revisioned(&mut value_reader) {
		Ok(w) => w,
		Err(_) => return Extracted::Bail,
	};
	descend_value_recursive(inner_walker, next_prefix, next_path)
}

/// Outcome of [`descend_to_value_walker`].
pub(crate) enum DescendResult<T> {
	/// Successfully consumed every path segment; the callback's return value
	/// is forwarded to the caller.
	Found(T),
	/// A path segment was missing during descent.
	Missing,
	/// Wire-level error or non-object intermediate.
	Bail,
}

/// Walk `path` segments inside the value tree and invoke `consume` with the
/// navigated [`Value`]'s wire bytes.
///
/// `depth_limit` bounds the descent length — see
/// [`descend_value_path_parts`] for the rationale. The callback shape lets
/// the most-recent owned bytes stay alive while the borrowed bytes are in
/// use — handing the bytes out by reference would let them outlive their
/// owning buffer. Callers open a fresh `Value::walk_revisioned` (or
/// similar) inside the closure.
pub(crate) fn descend_to_value_walker<T, F>(
	value_walker: <Value as WalkRevisioned>::Walker<'_, &[u8]>,
	path: &[&PathSegment],
	depth_limit: u32,
	consume: F,
) -> DescendResult<T>
where
	F: FnOnce(&[u8]) -> T,
{
	descend_to_value_walker_parts(value_walker, path, &[], depth_limit, consume)
}

/// Like [`descend_to_value_walker`] but walks `prefix` segments first, then
/// `path`, without concatenating into a new [`Vec`].
///
/// Recursive descent: each frame holds its own `view` / `object_walker` /
/// `map_view` / `map_walker` alive on the stack and the recursive call
/// borrows the inner value's wire bytes from that frame. The terminal call
/// hands those borrowed bytes to `consume` while the source frame is still
/// alive — no intermediate `Vec<u8>` copy per level.
pub(crate) fn descend_to_value_walker_parts<T, F>(
	walker: <Value as WalkRevisioned>::Walker<'_, &[u8]>,
	prefix: &[&PathSegment],
	path: &[PathSegment],
	depth_limit: u32,
	consume: F,
) -> DescendResult<T>
where
	F: FnOnce(&[u8]) -> T,
{
	let total = prefix.len() + path.len();
	if total == 0 || total > depth_limit as usize {
		return DescendResult::Bail;
	}
	descend_walker_recursive(walker, prefix, path, consume)
}

/// Inner recursion for [`descend_to_value_walker_parts`]. Each call pops
/// one segment (prefix first, then path), opens the navigated value's
/// walker on the bytes borrowed from this frame's `view`, and either
/// invokes `consume` on the leaf bytes (terminal) or recurses one level
/// deeper. The closure is `FnOnce` so it threads through unchanged until
/// the terminal step consumes it.
fn descend_walker_recursive<'r, R, T, F>(
	walker: <Value as WalkRevisioned>::Walker<'r, R>,
	prefix: &[&PathSegment],
	path: &[PathSegment],
	consume: F,
) -> DescendResult<T>
where
	R: BorrowedReader,
	F: FnOnce(&[u8]) -> T,
{
	let needle: &PathSegment = if let Some(&s) = prefix.first() {
		s
	} else if let Some(s) = path.first() {
		s
	} else {
		return DescendResult::Bail;
	};
	if !walker.is_object() {
		return DescendResult::Bail;
	}
	let view = match walker.object_view() {
		Ok(v) => v,
		Err(_) => return DescendResult::Bail,
	};
	// Bypass the macro-emitted walker chain — see
	// `indexed_map_walker_from_object_bytes` for rationale.
	let map_walker = match indexed_map_walker_from_object_bytes(view.as_bytes()) {
		Ok(w) => w,
		Err(_) => return DescendResult::Bail,
	};
	let value_bytes = match lookup_value_bytes_in_map(&map_walker, needle) {
		Ok(Some(b)) => b,
		Ok(None) => return DescendResult::Missing,
		Err(_) => return DescendResult::Bail,
	};
	let (next_prefix, next_path): (&[&PathSegment], &[PathSegment]) = if prefix.is_empty() {
		(&[], &path[1..])
	} else {
		(&prefix[1..], path)
	};
	if next_prefix.is_empty() && next_path.is_empty() {
		// Terminal: hand the leaf bytes to `consume`. `view` is still
		// alive on this frame, so `value_bytes` is valid for the call.
		return DescendResult::Found(consume(value_bytes));
	}
	// Recurse into the inner value. The new walker borrows from
	// `value_bytes`, which borrows from `view` (alive on this frame
	// until the recursive call returns).
	let mut value_reader: &[u8] = value_bytes;
	let inner_walker = match <Value as WalkRevisioned>::walk_revisioned(&mut value_reader) {
		Ok(w) => w,
		Err(_) => return DescendResult::Bail,
	};
	descend_walker_recursive(inner_walker, next_prefix, next_path, consume)
}

/// Outcome of [`scan_record_object_at_path_with_slots`].
#[derive(Debug)]
pub(crate) enum SlotScanResult<T> {
	/// Descent succeeded; `on_slots` produced `T`.
	Found(T),
	/// A path segment was missing during descent.
	Missing,
	/// Wire-level error, non-object intermediate, or non-object inner.
	Bail,
}

/// Walk `path` segments inside the record, then scan the navigated object for
/// each needle in `needles_sorted` (in input order), and invoke `on_slots`
/// with one borrowed wire slice per needle (or `None` when the needle is
/// absent from the object).
///
/// Zero-copy alternative to [`scan_record_object_at_path_for_keys_sorted`]:
/// matched values stay as borrowed wire bytes for typed byte-compare downstream
/// instead of being deserialised into [`Value`] per row.
///
/// `needles_sorted` is assumed strictly ascending by UTF-8 bytes (the same
/// invariant the legacy function expects).
///
/// Lifetime notes: the closure receives a slice of `Option<&[u8]>` whose
/// borrows are valid only inside the call; evaluation must complete before
/// return. `T` may not borrow from those slices (it is moved out of the
/// closure).
pub(crate) fn scan_record_object_at_path_with_slots<K, F, T>(
	record_bytes: &[u8],
	path: &[&PathSegment],
	needles_sorted: &[K],
	depth_limit: u32,
	on_slots: F,
) -> SlotScanResult<T>
where
	K: AsRef<[u8]>,
	F: FnOnce(&[Option<&[u8]>]) -> T,
{
	if needles_sorted.is_empty() {
		return SlotScanResult::Found(on_slots(&[]));
	}
	debug_assert!(
		needles_sorted.windows(2).all(|w| w[0].as_ref() < w[1].as_ref()),
		"needles_sorted must be strictly increasing in UTF-8 byte order",
	);
	// Open the record walker, take the `data` field's wire bytes via the
	// macro-emitted accessor. O(1) on rev-2 `indexed_struct` records (read
	// `data_off` from the prologue, slice); sequential `metadata` skip on
	// rev-1.
	let mut record_reader: &[u8] = record_bytes;
	let data_bytes =
		match Record::walk_revisioned(&mut record_reader).and_then(|w| w.into_data_bytes()) {
			Ok(b) => b,
			Err(_) => return SlotScanResult::Bail,
		};
	let mut reader: &[u8] = &data_bytes;
	let value_walker = match Value::walk_revisioned(&mut reader) {
		Ok(w) => w,
		Err(_) => return SlotScanResult::Bail,
	};
	if path.is_empty() {
		match scan_value_object_with_slots(value_walker, needles_sorted, on_slots) {
			Some(t) => SlotScanResult::Found(t),
			None => SlotScanResult::Bail,
		}
	} else {
		let result = descend_to_value_walker(value_walker, path, depth_limit, |value_bytes| {
			let mut reader: &[u8] = value_bytes;
			let walker = <Value as WalkRevisioned>::walk_revisioned(&mut reader).ok()?;
			scan_value_object_with_slots(walker, needles_sorted, on_slots)
		});
		match result {
			DescendResult::Found(Some(t)) => SlotScanResult::Found(t),
			DescendResult::Found(None) => SlotScanResult::Bail,
			DescendResult::Missing => SlotScanResult::Missing,
			DescendResult::Bail => SlotScanResult::Bail,
		}
	}
}

/// Walker-positioned variant of [`scan_record_object_at_path_with_slots`]:
/// scans the object reached by `value_walker` for `needles_sorted` and invokes
/// `on_slots` with one borrowed wire slice per needle (or `None` for absent
/// needles). Returns `None` on a wire-level error so callers can map to bail.
fn scan_value_object_with_slots<'r, R, K, F, T>(
	value_walker: <Value as WalkRevisioned>::Walker<'r, R>,
	needles_sorted: &[K],
	on_slots: F,
) -> Option<T>
where
	R: BorrowedReader,
	K: AsRef<[u8]>,
	F: FnOnce(&[Option<&[u8]>]) -> T,
{
	if !value_walker.is_object() {
		return None;
	}
	let object_view = value_walker.object_view().ok()?;
	// Bypass the macro-emitted walker chain — see
	// `indexed_map_walker_from_object_bytes` for rationale.
	let map_walker = indexed_map_walker_from_object_bytes(object_view.as_bytes()).ok()?;

	let n = needles_sorted.len();
	// Slot buffer. Most multi-needle scans touch 2-5 fields per object
	// level (one fused conjunct per referenced field), so the stack array
	// covers the typical case with zero allocation; only fan-out beyond
	// 8 falls back to heap. Mirrors the stack/heap split in
	// `ArrayOverlapsLiteralSet::evaluate`'s `All` bitmask.
	const STACK_SLOT_CAP: usize = 8;
	let mut stack_slots: [Option<&[u8]>; STACK_SLOT_CAP] = [None; STACK_SLOT_CAP];
	let mut heap_slots: Vec<Option<&[u8]>> = Vec::new();
	let slots: &mut [Option<&[u8]>] = if n <= STACK_SLOT_CAP {
		&mut stack_slots[..n]
	} else {
		heap_slots.resize(n, None);
		heap_slots.as_mut_slice()
	};
	let mut remaining = n;

	if map_walker.is_indexed() {
		// Two strategies on the indexed path:
		//
		// * **Iterate entries (N · log M):** walk every map entry, binary search the (sorted)
		//   needle slice. Best when needles approach the map size — the per-entry cost is one
		//   varint decode + one `binary_search_by`.
		//
		// * **Iterate needles (M · log N):** for each needle, ask the indexed prologue's
		//   binary-search (`find_value_bytes`) to resolve it. Best when needles are sparse against
		//   a wide row.
		//
		// Empirical threshold: if `4 * M < N` switch to needle-driven.
		// The `4` weights `find_value_bytes`'s constant (full Strand-wire
		// compare per probe + offset-table indexing) against `binary_search`'s
		// (UTF-8 byte compare against a sorted in-memory slice).
		let m = needles_sorted.len();
		let n = map_walker.len();
		if m > 0 && 4 * m < n {
			for (i, needle) in needles_sorted.iter().enumerate() {
				let needle_utf8 = needle.as_ref();
				let needle_wire = strand_wire_bytes_from_utf8(needle_utf8);
				match map_walker.find_value_bytes(|kb: &[u8]| kb.cmp(needle_wire.as_slice())) {
					Ok(Some(vb)) => slots[i] = Some(vb),
					Ok(None) => {}
					Err(_) => return None,
				}
			}
		} else {
			for (kb_wire, vb) in map_walker.entries()? {
				if remaining == 0 {
					break;
				}
				// Indexed key wire is `<usize varint len> || utf8`;
				// strip the length prefix before comparing against
				// the UTF-8 needle.
				let mut kr: &[u8] = kb_wire;
				let key_len =
					<usize as DeserializeRevisioned>::deserialize_revisioned(&mut kr).ok()?;
				if kr.len() != key_len {
					return None;
				}
				if let Ok(idx) = needles_sorted.binary_search_by(|n| n.as_ref().cmp(kr))
					&& slots[idx].is_none()
				{
					slots[idx] = Some(vb);
					remaining -= 1;
				}
			}
		}
	} else {
		let body = map_walker.legacy_body()?;
		let len = map_walker.len();
		let mut reader: &[u8] = body;
		for _ in 0..len {
			if remaining == 0 {
				break;
			}
			let key_len =
				<usize as DeserializeRevisioned>::deserialize_revisioned(&mut reader).ok()?;
			if reader.len() < key_len {
				return None;
			}
			let kb_utf8 = &reader[..key_len];
			reader = &reader[key_len..];
			if let Ok(idx) = needles_sorted.binary_search_by(|n| n.as_ref().cmp(kb_utf8)) {
				let v_start = body.len() - reader.len();
				let mut probe: &[u8] = reader;
				skip_value_wire(&mut probe).ok()?;
				let v_end = body.len() - probe.len();
				if slots[idx].is_none() {
					slots[idx] = Some(&body[v_start..v_end]);
					remaining -= 1;
				}
				reader = probe;
			} else {
				skip_value_wire(&mut reader).ok()?;
			}
		}
	}
	Some(on_slots(slots))
}

/// Depth limit used by test helpers and by test sites that construct a
/// `PreDecodeFilter` without going through the planner. Matches the default
/// `idiom_recursion_limit` (256) so test behaviour reflects production.
#[cfg(test)]
pub(crate) const TEST_DEPTH_LIMIT: u32 = 256;

#[cfg(test)]
pub(crate) fn descend_record_value_path(record_bytes: &[u8], path: &[PathSegment]) -> Extracted {
	extract_field_from_record_bytes(record_bytes, path, TEST_DEPTH_LIMIT)
}

#[cfg(test)]
pub(crate) fn descend_value_slice_path(value_wire: &[u8], path: &[PathSegment]) -> Extracted {
	if path.is_empty() {
		return Extracted::Bail;
	}
	let mut reader = value_wire;
	let walker = match Value::walk_revisioned(&mut reader) {
		Ok(w) => w,
		Err(_) => return Extracted::Bail,
	};
	descend_value_path_parts(walker, &[], path, TEST_DEPTH_LIMIT)
}
