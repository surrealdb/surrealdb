//! Zero-copy and streaming leaf evaluators for the pre-decode filter.
//!
//! [`SubstringMatch`] peeks UTF-8 bytes via [`revision::LeafWalker::with_bytes`] and uses
//! [`memchr::memmem::Finder`] — no `Strand` allocation on the hot path.
//!
//! [`ArrayElementContains`] and [`ArrayOverlapsLiteralSet`] walk array
//! elements as **borrowed wire bytes** and dispatch via
//! [`super::wire_cmp`] — no `Value` decode per element when the needle /
//! set member is a Strand or Number. Compound needles fall back to a
//! per-element decode (rare in scan benchmarks).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use memchr::memmem;
use revision::optimised::IndexedSeqWalker;
use revision::{DeserializeRevisioned, Error as RevisionError, WalkRevisioned};

use super::wire_literal::{LiteralSet, LiteralWire};
use super::{Evidence, wire_cmp};
use crate::val::Value;
use crate::val::object_extract::wire_skip::{rev2_optimised_payload_unchecked, skip_value_wire};

/// Construct an [`IndexedSeqWalker`] directly from an [`Array`]'s rev-2
/// wire bytes, **bypassing** the macro-generated
/// `Array::walk_revisioned` → `into_walk_field_0` → `walker()` chain.
///
/// `Array` is `#[revisioned(revision(1), revision(2, optimised))]` with a
/// single `#[revision(indexed_seq)]` field, so the rev-2 envelope payload
/// IS the indexed-seq body — see
/// [`rev2_optimised_payload`](crate::val::object_extract::wire_skip::rev2_optimised_payload)
/// for the layout and rationale (same trick as
/// `indexed_map_walker_from_object_bytes` in [`object_extract`]).
///
/// Uses the **validating** `from_payload` (not `from_payload_unvalidated`)
/// for the same reason as `indexed_map_walker_from_object_bytes`: the
/// workspace's `panic = 'abort'` release profile turns a corrupted offset
/// table into a process abort instead of the clean
/// `OptimisedOffsetsNonMonotonic` error + graceful fall-through to full
/// decode that the validating walker contract guarantees.
///
/// [`object_extract`]: crate::val::object_extract
#[inline]
fn indexed_seq_walker_from_array_bytes(
	array_wire: &[u8],
) -> Result<IndexedSeqWalker<'_, Value>, RevisionError> {
	// Same invariant as `indexed_map_walker_from_object_bytes`: the
	// parent `Value` walker already validated the outer rev, so the
	// inner `Array` rev re-read is redundant.
	let payload = rev2_optimised_payload_unchecked(array_wire)?;
	IndexedSeqWalker::<'_, Value>::from_payload(payload)
}

/// Iterate array elements as **borrowed wire bytes**, applying a per-element
/// closure. Handles both the indexed path (O(1) random access via
/// `element_bytes`) and the legacy sub-threshold path (linear walk of the
/// dense `Value*` body using [`skip_value_wire`] for envelope-only stepping).
///
/// The closure returns `Ok(ControlFlow::Break(v))` to short-circuit with `v`;
/// `Ok(ControlFlow::Continue(()))` to keep scanning; or `Err(())` to signal
/// "wire-level error, treat as unknown". Returns `Some(v)` when the predicate
/// short-circuits, `None` when the scan completes without a break, or the
/// `Err(())` signal bubbles up as `None` as well (callers map both to
/// `Evidence::Unknown`).
fn for_each_array_element_bytes<F, T>(
	walker: &IndexedSeqWalker<'_, Value>,
	mut f: F,
) -> Result<Option<T>, ()>
where
	F: FnMut(&[u8]) -> Result<std::ops::ControlFlow<T>, ()>,
{
	let len = walker.len();
	if walker.is_indexed() {
		for i in 0..len {
			let bytes = walker.element_bytes(i).map_err(|_| ())?;
			if let std::ops::ControlFlow::Break(out) = f(bytes)? {
				return Ok(Some(out));
			}
		}
	} else {
		let body = walker.body();
		let mut cursor: &[u8] = body;
		for _ in 0..len {
			let consumed_before = body.len() - cursor.len();
			let mut probe: &[u8] = cursor;
			skip_value_wire(&mut probe).map_err(|_| ())?;
			let consumed_after = body.len() - probe.len();
			let element_bytes = &body[consumed_before..consumed_after];
			cursor = probe;
			if let std::ops::ControlFlow::Break(out) = f(element_bytes)? {
				return Ok(Some(out));
			}
		}
	}
	Ok(None)
}

/// Walker for a [`Value`] backed by a byte slice (KV record bytes).
pub(crate) type ValueWalker<'r> = <Value as WalkRevisioned>::Walker<'r, &'r [u8]>;

/// Streaming evaluation of a leaf predicate without building the full [`Value`].
pub(crate) trait StreamingLeafEvaluator: std::fmt::Debug + Send + Sync {
	/// Inspect `leaf` and return partial truth for this row.
	fn evaluate(&self, leaf: ValueWalker<'_>) -> Evidence;

	/// Evidence to emit when the field referenced by this evaluator is
	/// **absent** from the row's object map (the descent returned
	/// `DescendResult::Missing`).
	///
	/// Default: the caller's [`LeafFallback`](super::LeafFallback) applied
	/// against [`Value::None`]. Correct for evaluators whose underlying op
	/// is a total boolean — `CONTAINS`, `CONTAINSANY`, equality, etc. all
	/// evaluate to a definitive `false` against `None`, so the row is
	/// provably rejected and the pre-decode filter wins.
	///
	/// Evaluators wrapping a SurrealQL function whose argument-type
	/// contract **errors** on `NONE` (today: `array::len(NONE)`) must
	/// override this to return [`Evidence::Unknown`], letting the
	/// authoritative post-decode evaluator produce the runtime error
	/// rather than silently rejecting the row.
	fn evaluate_missing(&self, fallback: &super::LeafFallback) -> Evidence {
		super::evidence_from_binary_cmp(
			&fallback.op,
			&fallback.literal,
			fallback.reversed,
			&Value::None,
		)
	}
}

/// [`BinaryOperator::Contain`] / [`BinaryOperator::NotContain`] on a string leaf with a literal
/// substring, using substring search over wire UTF-8 bytes only.
///
/// **Allocations:** none on the hot path; the [`memmem::Finder`] is built once at compile time.
#[derive(Debug)]
pub(crate) struct SubstringMatch {
	/// Precomputed substring searcher for the needle (owns needle bytes).
	pub(crate) finder: memmem::Finder<'static>,
	pub(crate) negated: bool,
}

impl StreamingLeafEvaluator for SubstringMatch {
	fn evaluate(&self, leaf: ValueWalker<'_>) -> Evidence {
		if !leaf.is_string() {
			return Evidence::Unknown;
		}
		// Rev-2 optimised Value walker: `string_view()` borrows the variant
		// body. Strand's wire form is `usize len || utf8`, so we skip past
		// the length prefix and run `memmem` directly on the UTF-8 bytes.
		let Ok(view) = leaf.string_view() else {
			return Evidence::Unknown;
		};
		let bytes = view.as_bytes();
		let mut reader: &[u8] = bytes;
		let Ok(len) = <usize as DeserializeRevisioned>::deserialize_revisioned(&mut reader) else {
			return Evidence::Unknown;
		};
		if reader.len() < len {
			return Evidence::Unknown;
		}
		let utf8 = &reader[..len];
		let hit = self.finder.find(utf8).is_some();
		let truth = hit ^ self.negated;
		if truth {
			Evidence::ProvablyTrue
		} else {
			Evidence::ProvablyFalse
		}
	}
}

/// [`BinaryOperator::Contain`] / [`BinaryOperator::NotContain`] on an array leaf: walk
/// elements as borrowed wire bytes and compare against `needle` via
/// [`wire_cmp::eq_wire`]. Falls back to a per-element decode + [`Value::eq`]
/// only when the needle is a compound (Object / RecordId / etc.) where wire
/// equality is not applicable.
///
/// **Allocations:** none on the Strand / Number hot path (the needle is
/// pre-encoded as [`LiteralWire`] at plan time); at most one decoded element
/// at a time on the compound-needle fallback path.
#[derive(Debug)]
pub(crate) struct ArrayElementContains {
	/// Original needle, kept for the compound-needle fallback and for
	/// diagnostics / error paths.
	pub(crate) needle: Arc<Value>,
	/// Plan-time pre-encoded needle for byte-level element comparison.
	pub(crate) needle_wire: Arc<LiteralWire>,
	pub(crate) negated: bool,
}

impl StreamingLeafEvaluator for ArrayElementContains {
	fn evaluate(&self, leaf: ValueWalker<'_>) -> Evidence {
		if !leaf.is_array() {
			return Evidence::Unknown;
		}
		// Rev-2 optimised Value walker: `array_view()` borrows the variant
		// body; build an `IndexedSeqWalker` directly from the envelope to
		// skip the macro-emitted `walk_revisioned → into_walk_field_0 →
		// walker()` chain, which parses the seq prologue twice (once via
		// `skip_indexed_seq` to derive the field bytes, then again in
		// `IndexedSeqWalker::from_payload`).
		let Ok(array_view) = leaf.array_view() else {
			return Evidence::Unknown;
		};
		let Ok(seq_walker) = indexed_seq_walker_from_array_bytes(array_view.as_bytes()) else {
			return Evidence::Unknown;
		};
		let needle = self.needle.as_ref();
		let needle_wire = self.needle_wire.as_ref();
		let hit = for_each_array_element_bytes(&seq_walker, |elem_bytes| {
			// Wire-fast path first: byte-eq against the pre-encoded needle
			// for Strand / Number needles.
			match wire_cmp::eq_wire(elem_bytes, needle_wire) {
				Some(true) => return Ok(std::ops::ControlFlow::Break(())),
				Some(false) => return Ok(std::ops::ControlFlow::Continue(())),
				None => {}
			}
			// Compound needle: decode this element and use `Value::eq`.
			let mut r: &[u8] = elem_bytes;
			let v =
				<Value as DeserializeRevisioned>::deserialize_revisioned(&mut r).map_err(|_| ())?;
			if v == *needle {
				Ok(std::ops::ControlFlow::Break(()))
			} else {
				Ok(std::ops::ControlFlow::Continue(()))
			}
		});
		let hit = match hit {
			Ok(Some(())) => true,
			Ok(None) => false,
			Err(()) => return Evidence::Unknown,
		};
		let truth = hit ^ self.negated;
		if truth {
			Evidence::ProvablyTrue
		} else {
			Evidence::ProvablyFalse
		}
	}
}

/// How [`ArrayOverlapsLiteralSet`] combines membership hits across streamed elements.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum OverlapMode {
	/// [`BinaryOperator::ContainAny`]
	Any,
	/// [`BinaryOperator::ContainNone`]
	None,
	/// [`BinaryOperator::ContainAll`]
	All,
}

/// Cap on literals covered by a stack-allocated bitmask (`64 * STACK_MASK_WORDS` bits).
const STACK_MASK_WORDS: usize = 8;

/// [`BinaryOperator::ContainAny`] / [`ContainNone`](BinaryOperator::ContainNone) /
/// [`ContainAll`](BinaryOperator::ContainAll) with a literal set, streaming array elements
/// as borrowed wire bytes.
///
/// **Allocations:** none on the Strand / Number hot path (set literals are
/// pre-encoded as [`LiteralSet`] / `wire_to_idx` at plan time); compound
/// elements decode one at a time. `All` uses a small on-stack bitmask when
/// the literal count is ≤ `64 * STACK_MASK_WORDS`, otherwise one `Vec<u64>`
/// per evaluation.
#[derive(Debug)]
pub(crate) struct ArrayOverlapsLiteralSet {
	/// Literal value → stable bit index. Source of truth for the `All`
	/// bitmask; also the lookup table for the compound-element fallback.
	pub(crate) literal_to_idx: Arc<HashMap<Value, usize>>,
	/// Wire bytes (full rev-2 [`Value`] wire) → stable bit index. Indices
	/// match `literal_to_idx`. Covers Strand and Number literals so the
	/// `All` mode hot path can resolve a bit without decoding the element.
	pub(crate) wire_to_idx: Arc<HashMap<Vec<u8>, usize>>,
	/// Partitioned wire set used by `Any` / `None` modes for the wire-fast
	/// contains check; identical content to `literal_to_idx` but indexed by
	/// wire bytes for byte-eq probes.
	pub(crate) literal_set: Arc<LiteralSet>,
	pub(crate) mode: OverlapMode,
}

impl ArrayOverlapsLiteralSet {
	fn all_bits_set(mask: &[u64], bit_count: usize) -> bool {
		let full = bit_count / 64;
		for i in 0..full {
			if mask.get(i).copied().unwrap_or(0) != u64::MAX {
				return false;
			}
		}
		let rem = bit_count % 64;
		if rem == 0 {
			return true;
		}
		let last = mask.get(full).copied().unwrap_or(0);
		last == (1u64 << rem) - 1
	}
}

impl StreamingLeafEvaluator for ArrayOverlapsLiteralSet {
	fn evaluate(&self, leaf: ValueWalker<'_>) -> Evidence {
		let n_lit = self.literal_to_idx.len();
		if n_lit == 0 {
			return match self.mode {
				OverlapMode::Any => Evidence::ProvablyFalse,
				OverlapMode::None | OverlapMode::All => Evidence::ProvablyTrue,
			};
		}
		if !leaf.is_array() {
			return Evidence::Unknown;
		}
		// Rev-2 optimised Value walker: `array_view()` borrows the variant
		// body; build an `IndexedSeqWalker` directly from the envelope
		// (see `ArrayElementContains` for rationale).
		let Ok(array_view) = leaf.array_view() else {
			return Evidence::Unknown;
		};
		let Ok(seq_walker) = indexed_seq_walker_from_array_bytes(array_view.as_bytes()) else {
			return Evidence::Unknown;
		};
		let n_words = n_lit.div_ceil(64);
		let literal_set = self.literal_set.as_ref();
		let literal_to_idx = self.literal_to_idx.as_ref();
		let wire_to_idx = self.wire_to_idx.as_ref();
		match self.mode {
			OverlapMode::Any => {
				let result = for_each_array_element_bytes(&seq_walker, |elem_bytes| {
					if Self::element_in_set(elem_bytes, literal_set, literal_to_idx)? {
						Ok(std::ops::ControlFlow::Break(()))
					} else {
						Ok(std::ops::ControlFlow::Continue(()))
					}
				});
				match result {
					Ok(Some(())) => Evidence::ProvablyTrue,
					Ok(None) => Evidence::ProvablyFalse,
					Err(()) => Evidence::Unknown,
				}
			}
			OverlapMode::None => {
				let result = for_each_array_element_bytes(&seq_walker, |elem_bytes| {
					if Self::element_in_set(elem_bytes, literal_set, literal_to_idx)? {
						Ok(std::ops::ControlFlow::Break(()))
					} else {
						Ok(std::ops::ControlFlow::Continue(()))
					}
				});
				match result {
					Ok(Some(())) => Evidence::ProvablyFalse,
					Ok(None) => Evidence::ProvablyTrue,
					Err(()) => Evidence::Unknown,
				}
			}
			OverlapMode::All => {
				let mut stack_mask = [0u64; STACK_MASK_WORDS];
				let mut heap_mask: Vec<u64> = Vec::new();
				let mask: &mut [u64] = if n_words <= STACK_MASK_WORDS {
					&mut stack_mask[..n_words]
				} else {
					heap_mask.resize(n_words, 0);
					heap_mask.as_mut_slice()
				};
				let result = for_each_array_element_bytes(&seq_walker, |elem_bytes| {
					if let Some(idx) =
						Self::element_set_index(elem_bytes, wire_to_idx, literal_to_idx)?
					{
						mask[idx / 64] |= 1u64 << (idx % 64);
						if Self::all_bits_set(mask, n_lit) {
							return Ok(std::ops::ControlFlow::Break(()));
						}
					}
					Ok(std::ops::ControlFlow::Continue(()))
				});
				match result {
					Ok(Some(())) => Evidence::ProvablyTrue,
					Ok(None) => {
						if Self::all_bits_set(mask, n_lit) {
							Evidence::ProvablyTrue
						} else {
							Evidence::ProvablyFalse
						}
					}
					Err(()) => Evidence::Unknown,
				}
			}
		}
	}
}

impl ArrayOverlapsLiteralSet {
	/// Decide whether an array element's wire bytes match any literal in
	/// `set`. Wire-fast path probes the Strand / Number partitions; on a
	/// compound element tag, decodes the element and probes `literal_to_idx`.
	#[inline]
	fn element_in_set(
		elem_bytes: &[u8],
		set: &LiteralSet,
		literal_to_idx: &HashMap<Value, usize>,
	) -> Result<bool, ()> {
		match wire_cmp::wire_value_in_set(elem_bytes, set) {
			Some(b) => Ok(b),
			None => {
				let mut r: &[u8] = elem_bytes;
				let v = <Value as DeserializeRevisioned>::deserialize_revisioned(&mut r)
					.map_err(|_| ())?;
				Ok(literal_to_idx.contains_key(&v))
			}
		}
	}

	/// Look up an array element's bit index in the literal set (for `All`
	/// mode). Wire-fast path uses the precomputed `wire_to_idx` table; on a
	/// compound tag, decodes the element and probes `literal_to_idx`.
	#[inline]
	fn element_set_index(
		elem_bytes: &[u8],
		wire_to_idx: &HashMap<Vec<u8>, usize>,
		literal_to_idx: &HashMap<Value, usize>,
	) -> Result<Option<usize>, ()> {
		// Peek the tag to decide whether the wire-byte lookup applies. For
		// Strand and Number elements the full Value wire bytes (input
		// `elem_bytes`) are the key in `wire_to_idx`. For everything else
		// fall back to a full element decode.
		if let Some(&idx) = wire_to_idx.get(elem_bytes) {
			return Ok(Some(idx));
		}
		let mut r: &[u8] = elem_bytes;
		let v = <Value as DeserializeRevisioned>::deserialize_revisioned(&mut r).map_err(|_| ())?;
		Ok(literal_to_idx.get(&v).copied())
	}
}

/// `WHERE array::len(field) <op> N` against an array leaf.
///
/// The rev-2 `Array` wire prologue carries the element count as a varint
/// right after the `flags` byte (see `IndexedSeqWalker`); we read just that
/// prefix and compare against `expected_len`. No element decode, no array
/// iteration, no Strand / Value materialisation.
#[derive(Debug)]
pub(crate) struct ArrayLenCompare {
	/// Right-hand side of the predicate: the numeric literal `array::len`
	/// is being compared against.
	pub(crate) expected_len: i64,
	/// The binary operator. Restricted at compile time to the comparison
	/// ops below; anything else returns `Evidence::Unknown`.
	pub(crate) op: crate::expr::operator::BinaryOperator,
}

impl StreamingLeafEvaluator for ArrayLenCompare {
	fn evaluate(&self, leaf: ValueWalker<'_>) -> Evidence {
		use crate::expr::operator::BinaryOperator;

		if !leaf.is_array() {
			// `array::len` on a non-array is a runtime error at
			// post-decode time ("Argument 1 was the wrong type. Expected
			// `array` but found `<T>`"); bailing to Unknown lets the
			// authoritative evaluator surface that error instead of
			// silently rejecting the row.
			return Evidence::Unknown;
		}
		let Ok(array_view) = leaf.array_view() else {
			return Evidence::Unknown;
		};
		// Bypass the macro-emitted walker chain — see
		// `indexed_seq_walker_from_array_bytes` for rationale.
		let Ok(seq_walker) = indexed_seq_walker_from_array_bytes(array_view.as_bytes()) else {
			return Evidence::Unknown;
		};
		let actual: i64 = seq_walker.len() as i64;
		let truth = match self.op {
			BinaryOperator::Equal | BinaryOperator::ExactEqual => actual == self.expected_len,
			BinaryOperator::NotEqual => actual != self.expected_len,
			BinaryOperator::LessThan => actual < self.expected_len,
			BinaryOperator::LessThanEqual => actual <= self.expected_len,
			BinaryOperator::MoreThan => actual > self.expected_len,
			BinaryOperator::MoreThanEqual => actual >= self.expected_len,
			_ => return Evidence::Unknown,
		};
		if truth {
			Evidence::ProvablyTrue
		} else {
			Evidence::ProvablyFalse
		}
	}

	/// Field absent → `array::len(NONE)` errors at post-decode time
	/// (`Incorrect arguments for function array::len(). Argument 1 was the
	/// wrong type. Expected array but found NONE`). Bail to `Unknown` so
	/// the authoritative evaluator surfaces the error rather than the
	/// pre-decode filter silently rejecting the row via
	/// `evidence_from_binary_cmp` against `Value::None`.
	fn evaluate_missing(&self, _fallback: &super::LeafFallback) -> Evidence {
		Evidence::Unknown
	}
}

#[cfg(test)]
mod array_len_missing_tests {
	use super::*;
	use crate::exec::pre_decode_filter::LeafFallback;
	use crate::expr::operator::BinaryOperator;
	use crate::val::Number;

	/// Regression for `reproductions/3545_where_clause_relations.surql`:
	/// `array::len(absent_field) >= 1` must NOT be wire-rejected by the
	/// pre-decode filter — the post-decode evaluator needs to error with
	/// "Expected array but found NONE". Confirms `evaluate_missing`
	/// returns `Unknown` regardless of the fallback's literal/op.
	#[test]
	fn array_len_compare_evaluate_missing_returns_unknown() {
		let evaluator = ArrayLenCompare {
			expected_len: 1,
			op: BinaryOperator::MoreThanEqual,
		};
		let fallback = LeafFallback {
			op: BinaryOperator::MoreThanEqual,
			literal: Value::Number(Number::Int(1)),
			reversed: false,
		};
		assert_eq!(evaluator.evaluate_missing(&fallback), Evidence::Unknown);
	}

	/// SubstringMatch's default `evaluate_missing` should still fall
	/// through the LeafFallback path — `Value::None CONTAINS 'x'` is a
	/// well-defined `false`, so the row gets provably rejected. (Default
	/// trait method exercise; locks down that we didn't change the
	/// behaviour for non-`array::len` evaluators.)
	#[test]
	fn substring_match_evaluate_missing_uses_fallback() {
		let evaluator = SubstringMatch {
			finder: memmem::Finder::new(b"x").into_owned(),
			negated: false,
		};
		let fallback = LeafFallback {
			op: BinaryOperator::Contain,
			literal: Value::String("x".into()),
			reversed: false,
		};
		// `Value::None CONTAINS "x"` → false → ProvablyFalse.
		assert_eq!(evaluator.evaluate_missing(&fallback), Evidence::ProvablyFalse);
	}
}

/// Plan-time lookup tables for [`ArrayOverlapsLiteralSet`].
///
/// All three tables share the same bit indices: a literal at position `i`
/// in iteration order keys to `i` in both [`literal_to_idx`] (decoded form,
/// used for compound-element fallback) and [`wire_to_idx`] (wire-byte form,
/// used for the Strand / Number fast path). [`literal_set`] carries the
/// same elements partitioned for membership probes by `Any` / `None` modes.
///
/// Field names mirror [`ArrayOverlapsLiteralSet`]'s fields so plan-time
/// construction is a flat move from this struct into the evaluator.
///
/// [`literal_to_idx`]: Self::literal_to_idx
/// [`wire_to_idx`]: Self::wire_to_idx
/// [`literal_set`]: Self::literal_set
#[derive(Debug, Clone)]
pub(crate) struct OverlapLookupTables {
	/// Decoded literal → stable bit index. Source of truth for the `All`
	/// bitmask; also the lookup table for compound-element decode fallback.
	pub(crate) literal_to_idx: Arc<HashMap<Value, usize>>,
	/// Full rev-2 [`Value`] wire bytes → stable bit index (Strand / Number
	/// literals only). Indices match [`Self::literal_to_idx`].
	pub(crate) wire_to_idx: Arc<HashMap<Vec<u8>, usize>>,
	/// Partitioned literal set used by `Any` / `None` modes' fast paths.
	pub(crate) literal_set: Arc<LiteralSet>,
}

/// Plan-time build for [`ArrayOverlapsLiteralSet`]: produce the decoded
/// `value → bit_index` map, the wire-bytes `→ bit_index` map (covers Strand
/// and Number literals), and the [`LiteralSet`] partitions used by the
/// `Any` / `None` modes' fast paths. Indices are shared across all three.
pub(crate) fn overlap_streaming_from_set(set: &HashSet<Value>) -> OverlapLookupTables {
	use revision::SerializeRevisioned;

	let mut value_map = HashMap::with_capacity(set.len());
	let mut wire_map: HashMap<Vec<u8>, usize> = HashMap::new();
	for (i, v) in set.iter().enumerate() {
		if matches!(v, Value::String(_) | Value::Number(_)) {
			let mut full = Vec::new();
			v.serialize_revisioned(&mut full).expect("serialize into Vec");
			wire_map.insert(full, i);
		}
		value_map.insert(v.clone(), i);
	}
	let set = LiteralSet::from_set(set);
	OverlapLookupTables {
		literal_to_idx: Arc::new(value_map),
		wire_to_idx: Arc::new(wire_map),
		literal_set: Arc::new(set),
	}
}
