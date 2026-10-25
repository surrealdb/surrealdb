//! Pre-decode filter ([`PreDecodeFilter`]) — **reject-only** optimisation during KV scans.
//!
//! Before paying full [`crate::catalog::Record`] deserialization, the scan may prove from
//! **the stored KV bytes alone** that the pushed-down WHERE cannot hold for this row. Only
//! [`PreDecodeFilterOutcome::Reject`] drops the row without decode; every other outcome falls
//! through to the normal path.
//!
//! A record in the stored-value encoding ([`crate::catalog::StoredRecord`]) is read in place:
//! each leaf the predicate names is decoded alone and judged by the comparison the
//! authoritative evaluator makes. A record in the `revision` encoding is walked on the wire, and
//! its leaves are compared as wire bytes where [`wire_cmp`] covers the comparison. A record in a
//! packed layout is read through its generation's descriptor, which a byte-level filter does not
//! hold, so it is judged unknown and decoded.
//!
//! ## Timing (vs `scan_predicate`)
//!
//! This runs synchronously inside [`crate::exec::operators::scan::pipeline::kv_scan_stream`]
//! **before** [`crate::exec::operators::scan::pipeline::decode_record`]. The scan's pushed-down
//! [`crate::exec::Clause`] predicate (`scan_predicate` on table / dynamic / record-id range
//! scans) still runs **after** decode in
//! [`crate::exec::operators::scan::pipeline::filter_and_process_batch`], together with table-level
//! permission filtering, computed fields, and field-level SELECT permissions — that evaluation is
//! **authoritative** for inclusion.
//!
//! | | Pre-decode filter | `scan_predicate` pushdown |
//! |--|-------------------|---------------------------|
//! | Input | Raw KV bytes | Decoded [`crate::val::Value`] |
//! | Semantics | Conservative; reject-only | Exact (must match `Filter`) |
//! | Async / params | Literal RHS baked at compile; sync only | Full [`Clause`] |
//!
//! ## Eligibility
//!
//! Predicate shape is compiled by [`compile::compile_predicate_shape`] (AND/OR/NOT, comparison and
//! collection containment ops with literal RHS where the field path is static, excluding geometry
//! and list-equality shorthands). Referenced root fields must not be computed
//! (`DEFINE FIELD … VALUE …`), and field-level SELECT permissions must be `Allow` when permission
//! checks apply — otherwise raw-byte reads would diverge from engine-visible values or bypass
//! authorisation.
//!
//! ## Relation to `ExpressionRegistry`
//!
//! ORDER BY / SELECT expression deduplication uses
//! [`crate::exec::expression_registry::ExpressionRegistry`] and runs **post-WHERE** on decoded
//! values. This module does not participate in that path.

mod compile;
mod packed;
pub(crate) use packed::extract_from_packed;
#[cfg(all(test, feature = "kv-mem"))]
mod packed_tests;
mod streaming;
mod wire_cmp;
pub(crate) mod wire_literal;

use std::collections::HashSet;
use std::sync::Arc;

pub(crate) use compile::PreDecodeShape;
use revision::WalkRevisioned;
pub(crate) use streaming::StreamingLeafEvaluator;
pub(crate) use wire_cmp::{int_on_wire, strand_payload_str};
use wire_literal::{LiteralSet, LiteralWire, hash_miss_is_decisive};

use crate::exec::object_extract::{
	DescendResult, Extracted, NeedleKey, PathSegment, SlotScanResult, WalkLeafErr, descend_stored,
	descend_to_value_walker_parts, extract_field_from_record_bytes, record_data_bytes,
	scan_record_object_at_path_with_slots, stored_record_object_in,
};
use crate::exec::packed_view::AdmittedRow;
use crate::expr::operator::BinaryOperator;
use crate::fnc::operate;
use crate::val::Value;
use crate::val::stored::OrderedObjectRef;

/// When a streaming leaf walk bails or misses, re-apply this op against a decoded leaf or
/// [`Value::None`] to preserve parity with the ordinary [`PredNode::Leaf`] path.
#[derive(Debug, Clone)]
pub(crate) struct LeafFallback {
	pub(crate) op: BinaryOperator,
	pub(crate) literal: Value,
	pub(crate) reversed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreDecodeFilterOutcome {
	Reject,
	NeedFullDecode,
}

/// One `(op, literal)` predicate to run against a single navigated value.
#[derive(Debug, Clone)]
pub(crate) struct FlatClauseOp {
	pub(crate) op: BinaryOperator,
	pub(crate) literal: Value,
	/// Plan-time pre-encoded literal for byte-level comparison; built once
	/// from [`Self::literal`] so the hot loop never serialises it again.
	/// Wrapped in `Arc` so cloned predicate trees share the underlying
	/// wire bytes / compiled regex.
	pub(crate) literal_wire: Arc<LiteralWire>,
	pub(crate) reversed: bool,
}

/// One field within a [`FusedFlatMapAnd`] node: a key plus **one or more**
/// `(op, literal)` clauses to apply to the field's navigated value.
///
/// Multi-clause entries (`ops.len() > 1`) carry shapes like
/// `field > 3 AND field < 7` — both clauses share a single descent and a
/// single map lookup; the evaluator just runs all `ops` against the same
/// `value_bytes` and `combine_and`s the results.
#[derive(Debug, Clone)]
pub(crate) struct FusedFlatClause {
	pub(crate) key_utf8: Vec<u8>,
	/// Plan-time pre-encoded `Strand` wire (`<usize varint len || utf8>`) of
	/// `key_utf8`, built once at construction. The fused object scan's
	/// needle-driven branch uses it as the `find_value_bytes` needle so the
	/// per-row hot loop never re-serialises the key. Mirrors
	/// [`PathSegment`](crate::exec::object_extract::PathSegment)'s pre-encoded
	/// `wire`.
	pub(crate) key_wire: Box<[u8]>,
	pub(crate) ops: Vec<FlatClauseOp>,
}

impl FusedFlatClause {
	/// Build a clause from its key and ops, pre-encoding the `key_wire`
	/// once. Used by both the production compile path
	/// (`flat_clauses_from_specs`) and the test single-clause constructor.
	pub(crate) fn new(key_utf8: Vec<u8>, ops: Vec<FlatClauseOp>) -> Self {
		let key_wire =
			crate::exec::object_extract::strand_wire_bytes_from_utf8(&key_utf8).into_boxed_slice();
		Self {
			key_utf8,
			key_wire,
			ops,
		}
	}

	/// Single-clause convenience constructor — the common case for
	/// non-range predicates that touch a key exactly once. Used only by
	/// test fixtures today; production compile paths build the
	/// `BTreeMap<key, Vec<FlatClauseOp>>` directly in
	/// `flat_clauses_from_specs` so multi-clause same-field shapes work
	/// uniformly.
	#[cfg(test)]
	pub(crate) fn single(
		key_utf8: Vec<u8>,
		op: BinaryOperator,
		literal: Value,
		literal_wire: Arc<LiteralWire>,
		reversed: bool,
	) -> Self {
		Self::new(
			key_utf8,
			vec![FlatClauseOp {
				op,
				literal,
				literal_wire,
				reversed,
			}],
		)
	}
}

/// Needle-key view for the fused object scan
/// ([`scan_record_object_at_path_with_slots`]): the clause's `key_utf8`
/// plus its plan-time pre-encoded `key_wire`, so evaluation can pass
/// `&[FusedFlatClause]` directly with no per-row projection or re-encoding.
///
/// [`scan_record_object_at_path_with_slots`]: crate::exec::object_extract::scan_record_object_at_path_with_slots
impl NeedleKey for FusedFlatClause {
	#[inline]
	fn key_utf8(&self) -> &[u8] {
		&self.key_utf8
	}

	#[inline]
	fn key_wire(&self) -> Option<&[u8]> {
		Some(&self.key_wire)
	}
}

/// A sorted, deduplicated set of [`FusedFlatClause`] for one
/// [`PredNode::FusedFlatMapAnd`].
///
/// Wraps a private [`Vec<FusedFlatClause>`] guaranteed to be **strictly ascending by
/// `key_utf8` with unique keys**. Fused-map evaluation pairs this slice with the result
/// of [`scan_record_root_object_for_keys_sorted`] by parallel iteration; if the invariant
/// is broken the pairing is silently misaligned and rows return wrong evidence. Construct
/// only via [`FusedFlatClauses::try_new`] (returns `None` on duplicate or out-of-order
/// keys), which makes the misaligned shape unrepresentable.
#[derive(Debug, Clone)]
pub(crate) struct FusedFlatClauses(Vec<FusedFlatClause>);

impl FusedFlatClauses {
	/// Construct from a vector already strictly ascending by `key_utf8` with unique keys.
	/// Returns `None` if the invariant is violated.
	pub(crate) fn try_new(inner: Vec<FusedFlatClause>) -> Option<Self> {
		if inner.windows(2).any(|w| w[0].key_utf8 >= w[1].key_utf8) {
			return None;
		}
		Some(Self(inner))
	}

	pub(crate) fn as_slice(&self) -> &[FusedFlatClause] {
		&self.0
	}
}

impl std::ops::Deref for FusedFlatClauses {
	type Target = [FusedFlatClause];

	fn deref(&self) -> &[FusedFlatClause] {
		&self.0
	}
}

#[derive(Debug, Clone)]
pub(crate) enum PredNode {
	And(Vec<PredNode>),
	Or(Vec<PredNode>),
	Not(Box<PredNode>),
	Leaf {
		path: Vec<PathSegment>,
		op: BinaryOperator,
		literal: Value,
		/// Plan-time pre-encoded literal for byte-level comparison; built
		/// once from [`Self::literal`] so the hot loop never serialises it
		/// again. Wrapped in `Arc` so cloned predicate trees share the
		/// underlying wire bytes / compiled regex.
		literal_wire: Arc<LiteralWire>,
		reversed: bool,
	},
	/// `field IN [..]` / `field NOT IN [..]` with a hashset-safe literal array or set.
	///
	/// `op` is [`BinaryOperator::Inside`] or [`BinaryOperator::NotInside`] only.
	LeafSetMembership {
		path: Vec<PathSegment>,
		op: BinaryOperator,
		set: Arc<HashSet<Value>>,
		/// Plan-time wire-partitioned mirror of [`Self::set`]. Strand and
		/// Number elements get their full rev-2 wire bytes parked in dedicated
		/// `HashSet<Vec<u8>>` partitions for byte-level membership probes;
		/// everything else falls into [`LiteralSet::fallback`] for the decode
		/// path.
		literal_set: Arc<LiteralSet>,
		/// Carried for symmetry with [`PredNode::Leaf`]; v1 hashset emission uses `false` only.
		#[allow(dead_code)]
		reversed: bool,
	},
	/// Leaf predicate evaluated via [`StreamingLeafEvaluator`] without decoding the leaf when
	/// possible; see [`LeafFallback`] for wire / shape escapes.
	LeafStreaming {
		path: Vec<PathSegment>,
		evaluator: Arc<dyn StreamingLeafEvaluator>,
		fallback: LeafFallback,
	},
	/// One map scan over a [`Value::Object`] VecMap at the current anchor: the table row's root
	/// object, or the nested object a [`PredNode::NavigatePrefix`] above it descended into.
	///
	/// `clauses` is a [`FusedFlatClauses`] which enforces the strictly-ascending,
	/// deduplicated-by-`key_utf8` invariant at the type level — fused-map evaluation
	/// pairs the slice with [`scan_record_root_object_for_keys_sorted`] output by position.
	FusedFlatMapAnd {
		clauses: FusedFlatClauses,
	},
	NavigatePrefix {
		segment: PathSegment,
		child: Box<PredNode>,
	},
}

/// Partial truth status of the predicate from cheap structural inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Evidence {
	/// Predicate is definitely false for this row → safe to skip full decode.
	ProvablyFalse,
	/// Predicate is definitely true (for the sub-expression) from decoded field bytes.
	ProvablyTrue,
	/// Cannot conclude — must full-decode and evaluate as today.
	Unknown,
}

/// A predicate never names the record's `id`: the id is the record's key, which the
/// stored bytes do not hold. [`PreDecodeShape`] refuses any predicate whose path starts
/// at `id`, and every reader of the tree relies on that refusal.
#[derive(Debug)]
pub(crate) struct PreDecodeFilter {
	pub(crate) root: PredNode,
	/// Hard cap on path-segment count for any pre-decode descent. Sourced
	/// from `ctx.config.exec.idiom_recursion_limit` at the planner; bounds the
	/// stack and intermediate allocations done by
	/// [`crate::exec::object_extract`]'s walker descent. Paths longer than
	/// the limit fall back to full-record decode + post-decode evaluation.
	pub(crate) depth_limit: u32,
	/// Whether a scan checks each packed row it reads for this filter to judge it from its
	/// columns. Where it does not, a packed row is read as bytes, which the filter does not judge,
	/// unless another reader of the scan has checked it.
	judges_packed: bool,
}

impl PreDecodeFilter {
	/// A filter of `root` that judges packed rows.
	pub(crate) fn new(root: PredNode, depth_limit: u32) -> Self {
		Self {
			root,
			depth_limit,
			judges_packed: true,
		}
	}

	/// This filter, judging packed rows or leaving them unjudged as `judges` says.
	pub(crate) fn judging_packed_rows(mut self, judges: bool) -> Self {
		self.judges_packed = judges;
		self
	}

	/// Whether a scan checks each packed row it reads for this filter to judge it from its
	/// columns.
	#[inline]
	pub(crate) fn judges_packed_rows(&self) -> bool {
		self.judges_packed
	}

	/// [`Self::apply_in`] with no layouts, which walks every document it judges.
	#[cfg(test)]
	pub(crate) fn apply(&self, row: &AdmittedRow<'_>) -> PreDecodeFilterOutcome {
		self.apply_in(row, None)
	}

	/// Judges the stored record `row` from its bytes alone.
	///
	/// The caller has admitted the record's generation against its table's
	/// layouts ([`crate::kvs::RowGate`]) before this reads any field of it: a
	/// row of a generation the table does not list is refused, never judged
	/// here. `layouts` are the layouts that admitted it. A document is judged
	/// only as one the whole decode accepts: unwalked when `layouts` vouch for
	/// its generation ([`crate::catalog::StoredRecord::admit_document`]), and
	/// walked first otherwise. A record this reads in neither encoding is
	/// judged unknown, which keeps it: a packed row among the bytes, which has
	/// not been checked, and one the check refused, whose whole decode raises
	/// the error. A checked packed row is judged from its columns, and is
	/// rejected only where the predicate is false of the record its whole
	/// decode holds.
	pub(crate) fn apply_in(
		&self,
		row: &AdmittedRow<'_>,
		layouts: Option<&crate::catalog::TableLayouts>,
	) -> PreDecodeFilterOutcome {
		let ev = match row {
			AdmittedRow::Bytes(record_bytes) => {
				match stored_record_object_in(record_bytes, layouts) {
					Some(Some(data)) => self.eval_stored(data, &[], &self.root),
					Some(None) => Evidence::Unknown,
					None => self.eval_node(record_bytes, &[], &self.root),
				}
			}
			AdmittedRow::Packed {
				row,
				..
			} => {
				let evidence = self.eval_packed(*row, &[], &self.root);
				#[cfg(test)]
				if evidence == Evidence::ProvablyFalse {
					crate::exec::vm::row_packed::observed::note_filtered();
				}
				evidence
			}
			AdmittedRow::Refused => Evidence::Unknown,
		};
		match ev {
			Evidence::ProvablyFalse => PreDecodeFilterOutcome::Reject,
			Evidence::ProvablyTrue | Evidence::Unknown => PreDecodeFilterOutcome::NeedFullDecode,
		}
	}

	/// Evaluate `node` against `record_bytes` with `prefix` as a path
	/// prepended to every leaf access.
	///
	/// `NavigatePrefix` chains are folded into `prefix` rather than carrying a
	/// per-anchor state through the tree; the walker-based field extractor
	/// handles missing intermediates itself.
	fn eval_node(&self, record_bytes: &[u8], prefix: &[&PathSegment], node: &PredNode) -> Evidence {
		match node {
			PredNode::And(xs) => {
				combine_and(xs.iter().map(|x| self.eval_node(record_bytes, prefix, x)))
			}
			PredNode::Or(xs) => {
				combine_or(xs.iter().map(|x| self.eval_node(record_bytes, prefix, x)))
			}
			PredNode::Not(inner) => match self.eval_node(record_bytes, prefix, inner) {
				Evidence::ProvablyFalse => Evidence::ProvablyTrue,
				Evidence::ProvablyTrue => Evidence::ProvablyFalse,
				Evidence::Unknown => Evidence::Unknown,
			},
			PredNode::Leaf {
				path,
				op,
				literal,
				literal_wire,
				reversed,
			} => self.eval_leaf(
				record_bytes,
				prefix,
				path,
				op,
				literal,
				literal_wire.as_ref(),
				*reversed,
			),
			PredNode::LeafSetMembership {
				path,
				op,
				set,
				literal_set,
				reversed: _,
			} => self.eval_set_membership(
				record_bytes,
				prefix,
				path,
				op,
				set.as_ref(),
				literal_set.as_ref(),
			),
			PredNode::LeafStreaming {
				path,
				evaluator,
				fallback,
			} => self.eval_leaf_streaming(record_bytes, prefix, path, evaluator.as_ref(), fallback),
			PredNode::FusedFlatMapAnd {
				clauses,
				..
			} => self.eval_fused_flat(record_bytes, prefix, clauses),
			PredNode::NavigatePrefix {
				segment,
				child,
			} => {
				// Fold the navigate segment into the path prefix and keep
				// descending. The leaf / fused / scan node responsible for the
				// actual extraction will resolve the full path against the
				// row's encoded bytes.
				//
				// `prefix` is a slice of `&PathSegment` so the recursive call
				// only grows a `Vec` of pointers — no per-record clone of the
				// segment, which would otherwise allocate a fresh wire-bytes
				// `Box<[u8]>` for every row that flows through a navigated
				// leaf (the hot path on `WHERE address.city = ...` shapes).
				let mut next: Vec<&PathSegment> = Vec::with_capacity(prefix.len() + 1);
				next.extend_from_slice(prefix);
				next.push(segment);
				self.eval_node(record_bytes, &next, child.as_ref())
			}
		}
	}

	fn eval_leaf_streaming(
		&self,
		record_bytes: &[u8],
		prefix: &[&PathSegment],
		path: &[PathSegment],
		evaluator: &dyn StreamingLeafEvaluator,
		fallback: &LeafFallback,
	) -> Evidence {
		let stream_ev: Result<Evidence, WalkLeafErr> = (|| {
			if prefix.is_empty() && path.is_empty() {
				return Err(WalkLeafErr::Bail);
			}
			// Open the record's `data` field wire bytes: slice-direct on
			// rev-2 `indexed_struct` records, walker-chain fallback otherwise.
			let data_bytes = record_data_bytes(record_bytes).map_err(|_| WalkLeafErr::Bail)?;
			let mut reader: &[u8] = &data_bytes;
			let value_walker =
				Value::walk_revisioned(&mut reader).map_err(|_| WalkLeafErr::Bail)?;
			// Closure form: the descent holds the navigated value's owning
			// `OwnedIndexedMapView` alive on its stack while we open a fresh
			// `Value` walker from the borrowed bytes and hand it to the
			// streaming evaluator. The `Evidence` return type doesn't borrow
			// from the walker, so we can hand it back across the closure.
			let result: DescendResult<Result<Evidence, WalkLeafErr>> =
				descend_to_value_walker_parts(
					value_walker,
					prefix,
					path,
					self.depth_limit,
					|value_bytes| {
						let mut reader: &[u8] = value_bytes;
						let w = <Value as revision::WalkRevisioned>::walk_revisioned(&mut reader)
							.map_err(|_| WalkLeafErr::Bail)?;
						Ok(evaluator.evaluate(w))
					},
				);
			match result {
				DescendResult::Found(inner) => inner,
				DescendResult::Missing => Err(WalkLeafErr::Missing),
				DescendResult::Bail => Err(WalkLeafErr::Bail),
			}
		})();
		match stream_ev {
			Ok(ev) => ev,
			// Delegate to the evaluator so it can choose between
			// "treat absent field as Value::None" (default) and
			// "bail to post-decode" (e.g. `array::len` which errors
			// on NONE).
			Err(WalkLeafErr::Missing) => evaluator.evaluate_missing(fallback),
			Err(WalkLeafErr::Bail) => {
				// Cold fallback: the wire-fast descent bailed (e.g. unsupported
				// variant, corrupted offsets). Materialise `prefix ++ path` as
				// an owned `Vec<PathSegment>` for `extract_field_from_record_bytes`,
				// which deserialises the leaf into a `Value`. The clones are paid
				// only here, not on the hot path.
				let full: Vec<PathSegment> =
					prefix.iter().map(|&s| s.clone()).chain(path.iter().cloned()).collect();
				self.fallback_leaf_streaming(record_bytes, &full, evaluator, fallback)
			}
		}
	}

	fn fallback_leaf_streaming(
		&self,
		record_bytes: &[u8],
		full: &[PathSegment],
		evaluator: &dyn StreamingLeafEvaluator,
		fallback: &LeafFallback,
	) -> Evidence {
		match extract_field_from_record_bytes(record_bytes, full, self.depth_limit) {
			Extracted::Found(v) => evaluator.evaluate_decoded(&v, fallback),
			Extracted::Missing => evaluator.evaluate_missing(fallback),
			Extracted::Bail => Evidence::Unknown,
		}
	}

	/// Evaluate `field IN set` / `field NOT IN set` using a compile-time
	/// [`HashSet`]. Hits the wire-fast partitions in `literal_set` first; any
	/// element whose tag doesn't map to a wire partition (and any compound
	/// element in the set) falls back to a full decode + `set.contains`.
	fn eval_set_membership(
		&self,
		record_bytes: &[u8],
		prefix: &[&PathSegment],
		path: &[PathSegment],
		op: &BinaryOperator,
		set: &HashSet<Value>,
		literal_set: &LiteralSet,
	) -> Evidence {
		// Single descent: try the wire-fast partition probe on the
		// borrowed value bytes; on any wire-bail (compound tag, mixed
		// Strand-vs-Regex set, etc.) decode just this one value and probe
		// the original `set`.
		//
		// `wire_value_in_set` is sound on Strand/Number tags only when the
		// set has no asymmetric Regex peers — see
		// [`LiteralSet::has_strand_asymmetric_match`].
		let descent = self.descend_with_leaf_bytes(record_bytes, prefix, path, |value_bytes| {
			if let Some(inside) = wire_cmp::wire_value_in_set(value_bytes, literal_set) {
				return Some(Self::set_evidence_for(op, inside));
			}
			let mut r: &[u8] = value_bytes;
			match <Value as revision::DeserializeRevisioned>::deserialize_revisioned(&mut r) {
				Ok(v) => Some(hash_set_evidence_for(op, set.contains(&v), &v, literal_set)),
				Err(_) => None,
			}
		});
		match descent {
			DescendResult::Found(Some(ev)) => ev,
			DescendResult::Found(None) => Evidence::Unknown,
			// Field absent → not in set.
			DescendResult::Missing => Self::set_evidence_for(op, false),
			DescendResult::Bail => Evidence::Unknown,
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn eval_leaf(
		&self,
		record_bytes: &[u8],
		prefix: &[&PathSegment],
		path: &[PathSegment],
		op: &BinaryOperator,
		literal: &Value,
		literal_wire: &LiteralWire,
		reversed: bool,
	) -> Evidence {
		// Single descent: try the wire-fast path on the borrowed value
		// bytes; if the wire path doesn't cover the (op, type) combination,
		// decode just this one value from the same slot bytes so we never
		// pay for a second descent through the record.
		let descent = self.descend_with_leaf_bytes(record_bytes, prefix, path, |value_bytes| {
			if let Some(ev) =
				wire_cmp::evaluate_leaf_on_wire(value_bytes, op, literal_wire, reversed)
			{
				return Some(ev);
			}
			let mut r: &[u8] = value_bytes;
			match <Value as revision::DeserializeRevisioned>::deserialize_revisioned(&mut r) {
				Ok(v) => Some(evidence_from_binary_cmp(op, literal, reversed, &v)),
				Err(_) => None,
			}
		});
		match descent {
			DescendResult::Found(Some(ev)) => ev,
			DescendResult::Found(None) => Evidence::Unknown,
			DescendResult::Missing => evidence_from_binary_cmp(op, literal, reversed, &Value::None),
			DescendResult::Bail => Evidence::Unknown,
		}
	}

	/// [`Self::eval_node`] over `data`, the data of a record in the
	/// stored-value encoding, read in place.
	///
	/// Each leaf is decoded alone and judged by the comparison the
	/// authoritative evaluator makes of it; a leaf whose bytes do not decode is
	/// unknown.
	fn eval_stored(
		&self,
		data: OrderedObjectRef<'_>,
		prefix: &[&PathSegment],
		node: &PredNode,
	) -> Evidence {
		match node {
			PredNode::And(xs) => combine_and(xs.iter().map(|x| self.eval_stored(data, prefix, x))),
			PredNode::Or(xs) => combine_or(xs.iter().map(|x| self.eval_stored(data, prefix, x))),
			PredNode::Not(inner) => match self.eval_stored(data, prefix, inner) {
				Evidence::ProvablyFalse => Evidence::ProvablyTrue,
				Evidence::ProvablyTrue => Evidence::ProvablyFalse,
				Evidence::Unknown => Evidence::Unknown,
			},
			PredNode::Leaf {
				path,
				op,
				literal,
				reversed,
				..
			} => match self.stored_leaf(data, prefix, path) {
				DescendResult::Found(Some(v)) => {
					evidence_from_binary_cmp(op, literal, *reversed, &v)
				}
				DescendResult::Missing => {
					evidence_from_binary_cmp(op, literal, *reversed, &Value::None)
				}
				DescendResult::Found(None) | DescendResult::Bail => Evidence::Unknown,
			},
			PredNode::LeafSetMembership {
				path,
				op,
				set,
				literal_set,
				..
			} => match self.stored_leaf(data, prefix, path) {
				DescendResult::Found(Some(v)) => {
					hash_set_evidence_for(op, set.contains(&v), &v, literal_set)
				}
				// Field absent → not in set.
				DescendResult::Missing => Self::set_evidence_for(op, false),
				DescendResult::Found(None) | DescendResult::Bail => Evidence::Unknown,
			},
			PredNode::LeafStreaming {
				path,
				evaluator,
				fallback,
			} => match self.stored_leaf(data, prefix, path) {
				DescendResult::Found(Some(v)) => evaluator.evaluate_decoded(&v, fallback),
				DescendResult::Missing => evaluator.evaluate_missing(fallback),
				DescendResult::Found(None) | DescendResult::Bail => Evidence::Unknown,
			},
			PredNode::FusedFlatMapAnd {
				clauses,
				..
			} => self.eval_stored_fused(data, prefix, clauses),
			PredNode::NavigatePrefix {
				segment,
				child,
			} => {
				let mut next: Vec<&PathSegment> = Vec::with_capacity(prefix.len() + 1);
				next.extend_from_slice(prefix);
				next.push(segment);
				self.eval_stored(data, &next, child.as_ref())
			}
		}
	}

	/// The leaf `prefix ++ path` names in a stored-value record's `data`,
	/// decoded: `Found(None)` when its bytes do not decode.
	fn stored_leaf(
		&self,
		data: OrderedObjectRef<'_>,
		prefix: &[&PathSegment],
		path: &[PathSegment],
	) -> DescendResult<Option<Value>> {
		match descend_stored(data, prefix, path, self.depth_limit) {
			DescendResult::Found(leaf) => DescendResult::Found(leaf.decode().ok()),
			DescendResult::Missing => DescendResult::Missing,
			DescendResult::Bail => DescendResult::Bail,
		}
	}

	/// [`Self::eval_fused_flat`] over a stored-value record's `data`: each
	/// clause's field of the object `prefix` names, decoded once for all of
	/// the clause's ops.
	fn eval_stored_fused(
		&self,
		data: OrderedObjectRef<'_>,
		prefix: &[&PathSegment],
		clauses: &FusedFlatClauses,
	) -> Evidence {
		if clauses.is_empty() {
			return Evidence::Unknown;
		}
		let none = || {
			combine_and(clauses.iter().flat_map(|clause| {
				clause.ops.iter().map(|opc| {
					evidence_from_binary_cmp(&opc.op, &opc.literal, opc.reversed, &Value::None)
				})
			}))
		};
		let object = if prefix.is_empty() {
			data
		} else {
			match descend_stored(data, prefix, &[], self.depth_limit) {
				// An object whose keys do not ascend is refused by a whole decode.
				DescendResult::Found(value) => match value.as_ordered_object() {
					Ok(Some(object)) => object,
					_ => return Evidence::Unknown,
				},
				DescendResult::Missing => return none(),
				DescendResult::Bail => return Evidence::Unknown,
			}
		};
		combine_and(clauses.iter().flat_map(|clause| {
			// An absent field is NONE; one whose bytes do not read is unknown.
			let value = match std::str::from_utf8(&clause.key_utf8).map(|k| object.get(k)) {
				Ok(Ok(Some(value))) => value.decode().ok(),
				Ok(Ok(None)) => Some(Value::None),
				Ok(Err(_)) | Err(_) => None,
			};
			clause.ops.iter().map(move |opc| match &value {
				Some(v) => evidence_from_binary_cmp(&opc.op, &opc.literal, opc.reversed, v),
				None => Evidence::Unknown,
			})
		}))
	}

	fn eval_fused_flat(
		&self,
		record_bytes: &[u8],
		prefix: &[&PathSegment],
		clauses: &FusedFlatClauses,
	) -> Evidence {
		if clauses.is_empty() {
			return Evidence::Unknown;
		}
		debug_assert!(
			clauses.windows(2).all(|w| w[0].key_utf8 < w[1].key_utf8),
			"FusedFlatClauses invariant violated",
		);
		// Scan the navigated object's entries, handing each clause-key its
		// matched value's wire bytes (or `None` when the key is absent).
		// Inside the closure, each clause runs **all of its `ops`** against
		// the same `value_bytes` — multi-op clauses like
		// `a > 3 AND a < 7` get one descent + one map lookup, not two.
		// Each op tries the wire-fast comparator first; on miss we decode
		// the value once and reuse it for the rest of the same clause's ops.
		let result = scan_record_object_at_path_with_slots(
			record_bytes,
			prefix,
			clauses.as_slice(),
			self.depth_limit,
			|slots: &[Option<&[u8]>]| {
				debug_assert_eq!(slots.len(), clauses.len());
				combine_and(clauses.iter().zip(slots.iter()).flat_map(|(clause, slot)| {
					// One decode is amortised across this clause's ops.
					let mut decoded: Option<Value> = None;
					clause.ops.iter().map(move |opc| match slot {
						None => evidence_from_binary_cmp(
							&opc.op,
							&opc.literal,
							opc.reversed,
							&Value::None,
						),
						Some(value_bytes) => {
							if let Some(ev) = wire_cmp::evaluate_leaf_on_wire(
								value_bytes,
								&opc.op,
								opc.literal_wire.as_ref(),
								opc.reversed,
							) {
								return ev;
							}
							if decoded.is_none() {
								let mut r: &[u8] = value_bytes;
								decoded = <Value as revision::DeserializeRevisioned>::deserialize_revisioned(
									&mut r,
								)
								.ok();
							}
							match &decoded {
								Some(v) => {
									evidence_from_binary_cmp(&opc.op, &opc.literal, opc.reversed, v)
								}
								None => Evidence::Unknown,
							}
						}
					})
				}))
			},
		);
		match result {
			SlotScanResult::Found(ev) => ev,
			SlotScanResult::Missing => combine_and(clauses.iter().flat_map(|clause| {
				clause.ops.iter().map(|opc| {
					evidence_from_binary_cmp(&opc.op, &opc.literal, opc.reversed, &Value::None)
				})
			})),
			SlotScanResult::Bail => Evidence::Unknown,
		}
	}

	/// Open a [`Value`] walker positioned at the record's `data` field,
	/// descend `prefix ++ path`, and hand the navigated leaf's wire bytes
	/// to `inner`. A wire-level error opening the record envelope is
	/// surfaced as [`DescendResult::Bail`] so callers only need to match a
	/// single layer of enum, not a nested `Option<DescendResult<_>>`.
	///
	/// Shared scaffolding for the leaf and set-membership wire-fast paths:
	/// the walker borrows from the local `reader`, so we can't return it
	/// from a helper; instead the helper runs the descent + leaf closure
	/// inside its own scope.
	fn descend_with_leaf_bytes<F, T>(
		&self,
		record_bytes: &[u8],
		prefix: &[&PathSegment],
		path: &[PathSegment],
		inner: F,
	) -> DescendResult<T>
	where
		F: FnOnce(&[u8]) -> T,
	{
		// Open the record's `data` field wire bytes: slice-direct on rev-2
		// `indexed_struct` records, walker-chain fallback otherwise.
		let Ok(data_bytes) = record_data_bytes(record_bytes) else {
			return DescendResult::Bail;
		};
		let mut reader: &[u8] = &data_bytes;
		let Ok(value_walker) = Value::walk_revisioned(&mut reader) else {
			return DescendResult::Bail;
		};
		descend_to_value_walker_parts(value_walker, prefix, path, self.depth_limit, inner)
	}

	/// Convert a definite `inside` answer into [`Evidence`] for the
	/// `Inside`/`NotInside` operator; anything else maps to [`Evidence::Unknown`].
	#[inline]
	fn set_evidence_for(op: &BinaryOperator, inside: bool) -> Evidence {
		if matches!(op, BinaryOperator::Inside) {
			if inside {
				Evidence::ProvablyTrue
			} else {
				Evidence::ProvablyFalse
			}
		} else if matches!(op, BinaryOperator::NotInside) {
			if inside {
				Evidence::ProvablyFalse
			} else {
				Evidence::ProvablyTrue
			}
		} else {
			Evidence::Unknown
		}
	}
}

/// [`Evidence`] for a set-membership probe that went through a `HashSet<Value>`.
///
/// A hit is always a hit. A **miss** only proves absence when
/// [`hash_miss_is_decisive`] says so: a value whose hash may disagree with its
/// equality ([`Value::hash_agrees_with_eq`]) can miss a set holding a value it
/// equals, and a stored regex misses a set holding a string it matches. An
/// inconclusive miss sends the row to the authoritative predicate.
#[inline]
fn hash_set_evidence_for(
	op: &BinaryOperator,
	inside: bool,
	value: &Value,
	literal_set: &LiteralSet,
) -> Evidence {
	if !inside && !hash_miss_is_decisive(value, literal_set) {
		return Evidence::Unknown;
	}
	PreDecodeFilter::set_evidence_for(op, inside)
}

/// Three-state AND of [`Evidence`] values.
///
/// Short-circuits on the first [`Evidence::ProvablyFalse`]. An empty input or any
/// [`Evidence::Unknown`] in the absence of a definite false yields [`Evidence::Unknown`];
/// otherwise [`Evidence::ProvablyTrue`].
fn combine_and<I: IntoIterator<Item = Evidence>>(iter: I) -> Evidence {
	let mut any = false;
	let mut unknown = false;
	for ev in iter {
		any = true;
		match ev {
			Evidence::ProvablyFalse => return Evidence::ProvablyFalse,
			Evidence::Unknown => unknown = true,
			Evidence::ProvablyTrue => {}
		}
	}
	if !any || unknown {
		Evidence::Unknown
	} else {
		Evidence::ProvablyTrue
	}
}

/// Three-state OR of [`Evidence`] values.
///
/// Short-circuits on the first [`Evidence::ProvablyTrue`]. An empty input or any
/// [`Evidence::Unknown`] in the absence of a definite true yields [`Evidence::Unknown`];
/// otherwise [`Evidence::ProvablyFalse`].
fn combine_or<I: IntoIterator<Item = Evidence>>(iter: I) -> Evidence {
	let mut any = false;
	let mut unknown = false;
	for ev in iter {
		any = true;
		match ev {
			Evidence::ProvablyTrue => return Evidence::ProvablyTrue,
			Evidence::Unknown => unknown = true,
			Evidence::ProvablyFalse => {}
		}
	}
	if !any || unknown {
		Evidence::Unknown
	} else {
		Evidence::ProvablyFalse
	}
}

fn evidence_from_binary_cmp(
	op: &BinaryOperator,
	literal: &Value,
	reversed: bool,
	v: &Value,
) -> Evidence {
	let (left, right) = if reversed {
		(literal, v)
	} else {
		(v, literal)
	};
	match apply_binary(op, left, right) {
		Some(Value::Bool(false)) => Evidence::ProvablyFalse,
		Some(Value::Bool(true)) => Evidence::ProvablyTrue,
		Some(_) | None => Evidence::Unknown,
	}
}

fn apply_binary(op: &BinaryOperator, left: &Value, right: &Value) -> Option<Value> {
	use BinaryOperator::*;
	match op {
		Equal => operate::equal(left, right).ok(),
		ExactEqual => operate::exact(left, right).ok(),
		NotEqual => operate::not_equal(left, right).ok(),
		LessThan => operate::less_than(left, right).ok(),
		LessThanEqual => operate::less_than_or_equal(left, right).ok(),
		MoreThan => operate::more_than(left, right).ok(),
		MoreThanEqual => operate::more_than_or_equal(left, right).ok(),
		Contain => operate::contain(left, right).ok(),
		NotContain => operate::not_contain(left, right).ok(),
		ContainAll => operate::contain_all(left, right).ok(),
		ContainAny => operate::contain_any(left, right).ok(),
		ContainNone => operate::contain_none(left, right).ok(),
		Inside => operate::inside(left, right).ok(),
		NotInside => operate::not_inside(left, right).ok(),
		AllInside => operate::inside_all(left, right).ok(),
		AnyInside => operate::inside_any(left, right).ok(),
		NoneInside => operate::inside_none(left, right).ok(),
		Subtract | Add | Multiply | Divide | Remainder | Power | AllEqual | AnyEqual | Or | And
		| NullCoalescing | TenaryCondition | Outside | Intersects | Range | RangeInclusive
		| RangeSkip | RangeSkipInclusive | Matches(_) | NearestNeighbor(_) => None,
	}
}

#[cfg(test)]
mod tests {
	use std::collections::hash_map::DefaultHasher;
	use std::collections::{BTreeMap, HashSet};
	use std::hash::{Hash, Hasher};
	use std::sync::Arc;

	use memchr::memmem;
	use revision::SerializeRevisioned;
	use rust_decimal::Decimal;
	use surrealdb_strand::Strand;

	use super::streaming::{
		ArrayElementContains, ArrayOverlapsLiteralSet, OverlapMode, SubstringMatch,
		overlap_streaming_from_set,
	};
	use super::wire_literal::{LiteralSet, LiteralWire};
	use super::{
		AdmittedRow, FusedFlatClause, FusedFlatClauses, LeafFallback, PathSegment, PreDecodeFilter,
		PreDecodeFilterOutcome, PreDecodeShape, PredNode, apply_binary,
	};
	use crate::catalog::Record;
	use crate::expr::operator::BinaryOperator;
	use crate::val::{Number, Object, RecordId, Value};

	/// The format the generation-0 samples are written in.
	pub(super) const DOCUMENT: crate::catalog::RecordFormat =
		crate::catalog::RecordFormat::Document {
			generation: None,
		};

	/// Depth limit used in test fixtures. Matches the default
	/// `ctx.config.exec.idiom_recursion_limit` (256) so test behaviour reflects
	/// the production planner's wiring.
	pub(super) const TEST_DEPTH_LIMIT: u32 = 256;

	/// Plans a test builds when its verdict must not depend on the hasher's
	/// seed. Each plan draws fresh `RandomState` seeds for its sets and maps. A
	/// probe meets an equal entry that hashes apart for about one seed in a
	/// hundred, so a verdict that followed such a probe shows within this many
	/// plans in all but about one run in 10^14.
	const SEED_DRAWS: usize = 4096;

	// ---------- test-only constructors ----------
	//
	// Mirror what `compile.rs` does in production: pre-encode the literal
	// wire / set partitions so every test predicate has the same shape as a
	// real one. Lets the tests stay readable (no `LiteralWire::from_value`
	// noise per construction site).

	pub(super) fn leaf(
		path: Vec<PathSegment>,
		op: BinaryOperator,
		literal: Value,
		reversed: bool,
	) -> PredNode {
		let literal_wire = Arc::new(LiteralWire::from_value(&literal));
		PredNode::Leaf {
			path,
			op,
			literal,
			literal_wire,
			reversed,
		}
	}

	pub(super) fn set_membership(
		path: Vec<PathSegment>,
		op: BinaryOperator,
		set: Arc<HashSet<Value>>,
		reversed: bool,
	) -> PredNode {
		let literal_set = Arc::new(LiteralSet::from_set(set.as_ref()));
		PredNode::LeafSetMembership {
			path,
			op,
			set,
			literal_set,
			reversed,
		}
	}

	pub(super) fn fused_clause(
		key_utf8: Vec<u8>,
		op: BinaryOperator,
		literal: Value,
		reversed: bool,
	) -> FusedFlatClause {
		let literal_wire = Arc::new(LiteralWire::from_value(&literal));
		FusedFlatClause::single(key_utf8, op, literal, literal_wire, reversed)
	}

	fn wire_record_plain_object(obj: Object) -> Vec<u8> {
		// Route through the macro-generated `Record::serialize_revisioned`
		// so the bytes always match the latest revision's wire layout (rev-2
		// optimised + indexed_struct). Hand-rolling the envelope would break
		// the moment the revision bumps.
		let rec = Record {
			metadata: None,
			data: Value::Object(obj),
		};
		let mut out = Vec::new();
		rec.serialize_revisioned(&mut out).unwrap();
		out
	}

	#[test]
	fn empty_and_never_rejects() {
		let pf = PreDecodeFilter::new(PredNode::And(Vec::new()), TEST_DEPTH_LIMIT);
		let rec = wire_record_plain_object(Object::default());
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::NeedFullDecode);
	}

	#[test]
	fn leaf_eq_false_rejects_row() {
		let root =
			leaf(vec!["a".into()], BinaryOperator::Equal, Value::Number(Number::Int(1)), false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(2)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	/// `==` is `Value`'s derived `PartialEq`, under which a regex equals only a
	/// regex with the same pattern. `=` and `!=` are `Value::equal`, which also
	/// matches a regex against every string it matches. A comparison with a
	/// regex literal judged on the wire — as a leaf of its own, or fused with
	/// another comparison into one scan of the row object — must reject a
	/// record only where the predicate, evaluated by `apply_binary` on the
	/// decoded fields, is false.
	#[test]
	fn equality_with_a_regex_literal_rejects_only_where_the_predicate_is_false() {
		use super::{PreDecodeShape, apply_binary};

		/// What `shape` evaluates to on the decoded `row`.
		fn holds(shape: &PreDecodeShape, row: &Object) -> bool {
			match shape {
				PreDecodeShape::And(parts) => parts.iter().all(|part| holds(part, row)),
				PreDecodeShape::Or(parts) => parts.iter().any(|part| holds(part, row)),
				PreDecodeShape::Not(inner) => !holds(inner, row),
				PreDecodeShape::Selective(inner) => holds(inner, row),
				PreDecodeShape::Leaf {
					path,
					op,
					literal,
					reversed,
				} => {
					assert_eq!(path.len(), 1, "a root field");
					let field = row.get(path[0].as_str()).cloned().unwrap_or(Value::None);
					let (left, right) = if *reversed {
						(literal, &field)
					} else {
						(&field, literal)
					};
					match apply_binary(op, left, right) {
						Some(Value::Bool(answer)) => answer,
						other => panic!("{op:?} answered {other:?}"),
					}
				}
			}
		}

		let regex = |pattern: &str| Value::Regex(pattern.parse().expect("regex"));
		let field_leaf =
			|name: &str, op: BinaryOperator, literal: Value, reversed: bool| PreDecodeShape::Leaf {
				path: vec![PathSegment::from(name)].into(),
				op,
				literal,
				reversed,
			};
		// True on every row below, so a conjunction with it is true exactly
		// where the regex comparison is.
		let n_is_one = field_leaf("n", BinaryOperator::Equal, Value::Number(Number::Int(1)), false);
		let stored_v = [
			Some(Value::from("abc")),
			Some(Value::from("xyz")),
			Some(regex("a.*")),
			Some(regex("x")),
			Some(Value::Number(Number::Int(1))),
			None,
		];

		let mut wrong = Vec::new();
		for op in [BinaryOperator::ExactEqual, BinaryOperator::Equal, BinaryOperator::NotEqual] {
			for reversed in [false, true] {
				let leaf = field_leaf("v", op.clone(), regex("a.*"), reversed);
				let conjunction = PreDecodeShape::And(vec![leaf.clone(), n_is_one.clone()]);
				// Each shape with whether its comparisons fuse into one scan.
				let shapes = [
					(leaf.clone(), false),
					(PreDecodeShape::Not(Box::new(leaf)), false),
					(conjunction.clone(), true),
					(PreDecodeShape::Not(Box::new(conjunction)), true),
				];
				for (shape, fused) in &shapes {
					let pf = shape.filter(TEST_DEPTH_LIMIT);
					let comparison = if let PredNode::Not(inner) = &pf.root {
						inner.as_ref()
					} else {
						&pf.root
					};
					if *fused {
						assert!(
							matches!(comparison, PredNode::FusedFlatMapAnd { .. }),
							"{shape:?} built {:?}",
							pf.root
						);
					} else {
						assert!(
							matches!(comparison, PredNode::Leaf { .. }),
							"{shape:?} built {:?}",
							pf.root
						);
					}
					for v in &stored_v {
						let mut fields =
							BTreeMap::from([(Strand::from("n"), Value::Number(Number::Int(1)))]);
						if let Some(v) = v {
							fields.insert(Strand::from("v"), v.clone());
						}
						let row = Object::from(fields);
						let rec = wire_record_plain_object(row.clone());
						if pf.apply(&AdmittedRow::Bytes(&rec)) == PreDecodeFilterOutcome::Reject
							&& holds(shape, &row)
						{
							wrong.push(format!("{shape:?} on v = {v:?}"));
						}
					}
				}
			}
		}
		assert!(wrong.is_empty(), "rejected records the predicate holds for: {wrong:#?}");
	}

	#[test]
	fn eval_set_membership_hits_returns_need_full_decode_for_inside() {
		let set = Arc::new(HashSet::from([Value::Number(Number::Int(1))]));
		let root = set_membership(vec!["a".into()], BinaryOperator::Inside, set, false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(1)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::NeedFullDecode);
	}

	#[test]
	fn eval_set_membership_misses_returns_reject_for_inside() {
		let set = Arc::new(HashSet::from([Value::Number(Number::Int(1))]));
		let root = set_membership(vec!["a".into()], BinaryOperator::Inside, set, false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(99)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	/// A float and a decimal are one value only when the decimal spells the
	/// float's exact expansion, and hash alike exactly then: `0.5f` hits a set
	/// holding `0.5dec`, and `0.1f` misses one holding `0.1dec` decisively.
	#[test]
	fn eval_set_membership_float_and_decimal_hit_only_their_exact_value() {
		let hash_of = |v: &Value| {
			let mut hasher = DefaultHasher::new();
			v.hash(&mut hasher);
			hasher.finish()
		};
		for (f, d, outcome) in [
			(0.5, "0.5", PreDecodeFilterOutcome::NeedFullDecode),
			(0.1, "0.1", PreDecodeFilterOutcome::Reject),
		] {
			let float = Value::Number(Number::Float(f));
			let decimal = Value::Number(Number::Decimal(d.parse().unwrap()));
			let equal = outcome == PreDecodeFilterOutcome::NeedFullDecode;
			assert_eq!(float == decimal, equal);
			assert_eq!(hash_of(&float) == hash_of(&decimal), equal);
			for (stored, field) in [(&decimal, &float), (&float, &decimal)] {
				let set = Arc::new(HashSet::from([stored.clone()]));
				let root = set_membership(vec!["a".into()], BinaryOperator::Inside, set, false);
				let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
				let obj = Object::from(BTreeMap::from([(Strand::from("a"), field.clone())]));
				let rec = wire_record_plain_object(obj);
				assert_eq!(
					pf.apply(&AdmittedRow::Bytes(&rec)),
					outcome,
					"stored {stored:?} field {field:?}"
				);
			}
		}
	}

	/// The same one level down, where both sides take the decode path because
	/// an object element has no wire partition.
	#[test]
	fn eval_set_membership_nested_float_and_decimal_hit_only_their_exact_value() {
		let nested =
			|v: Value| Value::Object(Object::from(BTreeMap::from([(Strand::from("v"), v)])));
		for (f, d, outcome) in [
			(0.5, "0.5", PreDecodeFilterOutcome::NeedFullDecode),
			(0.1, "0.1", PreDecodeFilterOutcome::Reject),
		] {
			let set = Arc::new(HashSet::from([nested(Value::Number(Number::Decimal(
				d.parse().unwrap(),
			)))]));
			let root = set_membership(vec!["a".into()], BinaryOperator::Inside, set, false);
			let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
			let obj = Object::from(BTreeMap::from([(
				Strand::from("a"),
				nested(Value::Number(Number::Float(f))),
			)]));
			let rec = wire_record_plain_object(obj);
			assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), outcome, "{f}");
		}
	}

	/// A number-free value keeps its rejection even against a set full of
	/// numbers: equality needs matching variants, so the miss is decisive.
	#[test]
	fn eval_set_membership_number_free_value_still_rejects() {
		let set = Arc::new(HashSet::from([
			Value::Number(Number::Decimal("0.1".parse().unwrap())),
			Value::Object(Object::from(BTreeMap::from([(
				Strand::from("v"),
				Value::Number(Number::Float(2.5)),
			)]))),
		]));
		let root = set_membership(vec!["a".into()], BinaryOperator::Inside, set, false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj = Object::from(BTreeMap::from([(
			Strand::from("a"),
			Value::Object(Object::from(BTreeMap::from([(
				Strand::from("v"),
				Value::String("z".into()),
			)]))),
		)]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	/// A decimal's stored bytes carry its scale and the sign of a zero, so one
	/// value has many encodings: `2.0dec` equals `2dec`, and `-0dec` equals
	/// `0dec`. Every leaf the plan builds over a decimal literal judges the
	/// value, as the operator does on the decoded field: a predicate the filter
	/// rejects is false, and so is a negation it rejects. The stored bytes
	/// settle every leaf but `CONTAINSALL`, whose misses are decoded.
	#[test]
	fn decimal_leaves_judge_the_value_not_its_encoding() {
		let d = |s: &str| -> Decimal { s.parse().unwrap() };
		let num = |d: Decimal| Value::Number(Number::Decimal(d));
		let wire = |v: &Value| {
			let mut out = Vec::new();
			v.serialize_revisioned(&mut out).unwrap();
			out
		};
		let list = |v: &Value| Value::from(vec![v.clone()]);
		// One value spelled two ways. Parsing keeps the scale it reads, and
		// negating a zero sets its sign.
		let spellings = [
			(d("2.0"), d("2")),
			(d("2"), d("2.0")),
			(-d("0"), d("0")),
			(d("0"), -d("0")),
			(d("0.00"), -d("0.0")),
		];
		for (a, b) in spellings {
			assert_eq!(num(a), num(b));
			assert_ne!(wire(&num(a)), wire(&num(b)), "{a:?} and {b:?} share an encoding");
		}
		// Two values, neither spelled in its shortest form.
		let distinct = [(d("2.50"), d("3.0"))];

		let mut wrong = Vec::new();
		for (stored, literal) in spellings.into_iter().chain(distinct) {
			let (stored, literal) = (num(stored), num(literal));
			let cases = [
				(stored.clone(), BinaryOperator::Equal, literal.clone()),
				(stored.clone(), BinaryOperator::ExactEqual, literal.clone()),
				(stored.clone(), BinaryOperator::NotEqual, literal.clone()),
				(
					stored.clone(),
					BinaryOperator::Inside,
					Value::from(vec![literal.clone(), num(d("7"))]),
				),
				(stored.clone(), BinaryOperator::NotInside, list(&literal)),
				(list(&stored), BinaryOperator::Contain, literal.clone()),
				(list(&stored), BinaryOperator::NotContain, literal.clone()),
				(list(&stored), BinaryOperator::ContainAny, list(&literal)),
				(list(&stored), BinaryOperator::ContainNone, list(&literal)),
				(list(&stored), BinaryOperator::ContainAll, list(&literal)),
			];
			for (field, op, operand) in cases {
				let holds = match apply_binary(&op, &field, &operand) {
					Some(Value::Bool(b)) => b,
					other => panic!("{field:?} {op:?} {operand:?} answered {other:?}"),
				};
				let rec = wire_record_plain_object(Object::from(BTreeMap::from([
					(Strand::from("a"), field.clone()),
					(Strand::from("b"), Value::Number(Number::Int(1))),
				])));
				let leaf = PreDecodeShape::Leaf {
					path: Arc::from([PathSegment::from("a")]),
					op: op.clone(),
					literal: operand.clone(),
					reversed: false,
				};
				// Beside a second comparison at the root, a comparison of `a`
				// is judged in one scan of the record's object.
				let fused = PreDecodeShape::And(vec![
					leaf.clone(),
					PreDecodeShape::Leaf {
						path: Arc::from([PathSegment::from("b")]),
						op: BinaryOperator::Equal,
						literal: Value::Number(Number::Int(1)),
						reversed: false,
					},
				]);
				for (form, shape) in [("alone", leaf), ("with b = 1", fused)] {
					let rejects = |shape: PreDecodeShape| {
						shape.filter(TEST_DEPTH_LIMIT).apply(&AdmittedRow::Bytes(&rec))
							== PreDecodeFilterOutcome::Reject
					};
					let negated = PreDecodeShape::Not(Box::new(shape.clone()));
					let (rejected, negation_rejected) = (rejects(shape), rejects(negated));
					let case = format!("a = {field:?}, a {op:?} {operand:?}, {form}");
					if rejected && holds {
						wrong.push(format!("rejected though it holds: {case}"));
					}
					if negation_rejected && !holds {
						wrong.push(format!("negation rejected though it fails: {case}"));
					}
					if !rejected && !negation_rejected && !matches!(op, BinaryOperator::ContainAll)
					{
						wrong.push(format!("left to the decode: {case}"));
					}
				}
			}
		}
		assert!(wrong.is_empty(), "{}", wrong.join("\n"));
	}

	#[test]
	fn eval_set_membership_inverts_for_not_inside() {
		let set = Arc::new(HashSet::from([Value::Number(Number::Int(1))]));
		let root = set_membership(vec!["a".into()], BinaryOperator::NotInside, set, false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(1)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn eval_set_membership_missing_field_treated_as_value_none() {
		let set = Arc::new(HashSet::from([Value::Number(Number::Int(1))]));
		let root = set_membership(vec!["missing".into()], BinaryOperator::Inside, set, false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(2)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn eval_set_membership_empty_set_rejects_every_row_for_inside() {
		let set = Arc::new(HashSet::<Value>::new());
		let root = set_membership(vec!["a".into()], BinaryOperator::Inside, set, false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(5)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn eval_contains_substring_provably_false_via_apply_binary() {
		let root = leaf(
			vec!["msg".into()],
			BinaryOperator::Contain,
			Value::String("needle".into()),
			false,
		);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("msg"), Value::String("hello".into()))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn eval_contains_any_negative_rejects() {
		let root = leaf(
			vec!["tags".into()],
			BinaryOperator::ContainAny,
			Value::from(vec![Value::String("x".into()), Value::String("y".into())]),
			false,
		);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj = Object::from(BTreeMap::from([(
			Strand::from("tags"),
			Value::from(vec![Value::String("a".into()), Value::String("b".into())]),
		)]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn eval_not_inside_via_prednode_not_inverts() {
		let set = Arc::new(HashSet::from([Value::Number(Number::Int(1))]));
		let inner = set_membership(vec!["a".into()], BinaryOperator::Inside, set, false);
		let root = PredNode::Not(Box::new(inner));
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(1)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn eval_anyinside_via_apply_binary() {
		let root = leaf(
			vec!["arr".into()],
			BinaryOperator::AnyInside,
			Value::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))]),
			false,
		);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj = Object::from(BTreeMap::from([(
			Strand::from("arr"),
			Value::from(vec![Value::Number(Number::Int(3))]),
		)]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	/// Navigate through a missing intermediate field: descendant leaf reads as `Value::None`,
	/// so `outer.x = 1` is provably false on a row where `outer` itself is absent.
	#[test]
	fn scoped_missing_intermediate_makes_descendant_leaf_provably_false() {
		let root = PredNode::NavigatePrefix {
			segment: "outer".into(),
			child: Box::new(leaf(
				vec!["x".into()],
				BinaryOperator::Equal,
				Value::Number(Number::Int(1)),
				false,
			)),
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		// Record has no `outer` key.
		let obj =
			Object::from(BTreeMap::from([(Strand::from("other"), Value::Number(Number::Int(2)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	/// `FusedFlatClauses::try_new` rejects out-of-order keys.
	#[test]
	fn fused_flat_clauses_try_new_rejects_unsorted() {
		let bad = vec![
			fused_clause(b"b".to_vec(), BinaryOperator::Equal, Value::None, false),
			fused_clause(b"a".to_vec(), BinaryOperator::Equal, Value::None, false),
		];
		assert!(FusedFlatClauses::try_new(bad).is_none());
	}

	/// `FusedFlatClauses::try_new` rejects duplicate keys.
	#[test]
	fn fused_flat_clauses_try_new_rejects_duplicates() {
		let dup = vec![
			fused_clause(b"a".to_vec(), BinaryOperator::Equal, Value::None, false),
			fused_clause(b"a".to_vec(), BinaryOperator::Equal, Value::None, false),
		];
		assert!(FusedFlatClauses::try_new(dup).is_none());
	}

	/// The plan-time pre-encoded `key_wire` must be byte-for-byte identical
	/// to the on-demand `strand_wire_bytes_from_utf8` encoding and to
	/// `PathSegment::wire()` — the fused scan's needle-driven branch relies
	/// on it as the `find_value_bytes` needle, so any divergence would
	/// mis-resolve keys. This is the whole correctness basis for pre-encoding:
	/// only the *source* of the bytes changes, never the bytes.
	#[test]
	fn fused_clause_key_wire_matches_on_demand_encoding() {
		use crate::exec::object_extract::{NeedleKey, strand_wire_bytes_from_utf8};
		for key in [b"a".as_slice(), b"number", b"geography", b"a_longer_field_name_here"] {
			let clause = fused_clause(key.to_vec(), BinaryOperator::Equal, Value::None, false);
			assert_eq!(
				&*clause.key_wire,
				strand_wire_bytes_from_utf8(key).as_slice(),
				"key_wire must equal the on-demand Strand wire encoding",
			);
			// And equal to the PathSegment needle used on the leaf path, so
			// both descent entry points feed `find_value_bytes` identical bytes.
			let seg = PathSegment::from(std::str::from_utf8(key).unwrap());
			assert_eq!(&*clause.key_wire, seg.wire(), "key_wire must equal PathSegment::wire()");
			// NeedleKey exposes both views consistently.
			assert_eq!(clause.key_utf8(), key);
			assert_eq!(clause.key_wire(), Some(&*clause.key_wire));
		}
	}

	#[test]
	fn streaming_substring_rejects_when_needle_absent() {
		let lit = Value::String("needle".into());
		let eval: Arc<dyn super::StreamingLeafEvaluator> = Arc::new(SubstringMatch {
			finder: memmem::Finder::new(b"needle").into_owned(),
			negated: false,
		});
		let root = PredNode::LeafStreaming {
			path: vec!["msg".into()],
			evaluator: eval,
			fallback: LeafFallback {
				op: BinaryOperator::Contain,
				literal: lit,
				reversed: false,
			},
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("msg"), Value::String("hello".into()))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn streaming_substring_unknown_on_non_string_leaf_needs_decode() {
		let lit = Value::String("x".into());
		let eval: Arc<dyn super::StreamingLeafEvaluator> = Arc::new(SubstringMatch {
			finder: memmem::Finder::new(b"x").into_owned(),
			negated: false,
		});
		let root = PredNode::LeafStreaming {
			path: vec!["n".into()],
			evaluator: eval,
			fallback: LeafFallback {
				op: BinaryOperator::Contain,
				literal: lit,
				reversed: false,
			},
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("n"), Value::Number(Number::Int(1)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::NeedFullDecode);
	}

	#[test]
	fn streaming_array_contains_rejects_on_miss() {
		let needle = Value::Number(Number::Int(99));
		let needle_wire = Arc::new(LiteralWire::from_value(&needle));
		let eval: Arc<dyn super::StreamingLeafEvaluator> = Arc::new(ArrayElementContains {
			needle: Arc::new(needle.clone()),
			needle_wire,
			negated: false,
		});
		let root = PredNode::LeafStreaming {
			path: vec!["tags".into()],
			evaluator: eval,
			fallback: LeafFallback {
				op: BinaryOperator::Contain,
				literal: needle,
				reversed: false,
			},
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj = Object::from(BTreeMap::from([(
			Strand::from("tags"),
			Value::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))]),
		)]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn streaming_array_containsany_rejects_when_no_overlap() {
		let lit = Value::from(vec![Value::String("x".into()), Value::String("y".into())]);
		let set = Arc::new(HashSet::from([Value::String("x".into()), Value::String("y".into())]));
		let tables = overlap_streaming_from_set(set.as_ref());
		let eval: Arc<dyn super::StreamingLeafEvaluator> = Arc::new(ArrayOverlapsLiteralSet {
			literal_to_idx: tables.literal_to_idx,
			wire_to_idx: tables.wire_to_idx,
			literal_set: tables.literal_set,
			literal_count: tables.literal_count,
			mode: OverlapMode::Any,
		});
		let root = PredNode::LeafStreaming {
			path: vec!["tags".into()],
			evaluator: eval,
			fallback: LeafFallback {
				op: BinaryOperator::ContainAny,
				literal: lit,
				reversed: false,
			},
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj = Object::from(BTreeMap::from([(
			Strand::from("tags"),
			Value::from(vec![Value::String("a".into()), Value::String("b".into())]),
		)]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn streaming_array_containsall_rejects_when_mask_incomplete() {
		let lit = Value::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))]);
		let set =
			Arc::new(HashSet::from([Value::Number(Number::Int(1)), Value::Number(Number::Int(2))]));
		let tables = overlap_streaming_from_set(set.as_ref());
		let eval: Arc<dyn super::StreamingLeafEvaluator> = Arc::new(ArrayOverlapsLiteralSet {
			literal_to_idx: tables.literal_to_idx,
			wire_to_idx: tables.wire_to_idx,
			literal_set: tables.literal_set,
			literal_count: tables.literal_count,
			mode: OverlapMode::All,
		});
		let root = PredNode::LeafStreaming {
			path: vec!["a".into()],
			evaluator: eval,
			fallback: LeafFallback {
				op: BinaryOperator::ContainAll,
				literal: lit,
				reversed: false,
			},
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj = Object::from(BTreeMap::from([(
			Strand::from("a"),
			Value::from(vec![Value::Number(Number::Int(1))]),
		)]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	/// A bit index identifies a literal value, not a spelling of one. A set
	/// that spells one value in several kinds holds it once, as equal numbers
	/// hash alike, so an array holding any spelling satisfies `ContainAll`.
	#[test]
	fn streaming_array_overlap_all_collapses_equal_literal_spellings() {
		let spellings = [
			Value::Number(Number::Int(1)),
			Value::Number(Number::Float(1.0)),
			Value::Number(Number::Decimal("1.0".parse().unwrap())),
		];
		let set: HashSet<Value> = spellings.iter().cloned().collect();
		assert_eq!(set.len(), 1);

		let tables = overlap_streaming_from_set(&set);
		assert_eq!(tables.literal_count, 1, "the spellings share one bit");
		for spelling in &spellings {
			assert_eq!(
				tables.literal_to_idx.get(spelling),
				Some(&0),
				"{spelling:?} stays addressable"
			);
		}

		for element in &spellings {
			let eval: Arc<dyn super::StreamingLeafEvaluator> = Arc::new(ArrayOverlapsLiteralSet {
				literal_to_idx: Arc::clone(&tables.literal_to_idx),
				wire_to_idx: Arc::clone(&tables.wire_to_idx),
				literal_set: Arc::clone(&tables.literal_set),
				literal_count: tables.literal_count,
				mode: OverlapMode::All,
			});
			let root = PredNode::LeafStreaming {
				path: vec!["tags".into()],
				evaluator: eval,
				fallback: LeafFallback {
					op: BinaryOperator::ContainAll,
					literal: Value::from(spellings.to_vec()),
					reversed: false,
				},
			};
			let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
			let obj = Object::from(BTreeMap::from([(
				Strand::from("tags"),
				Value::from(vec![element.clone()]),
			)]));
			let rec = wire_record_plain_object(obj);
			assert_eq!(
				pf.apply(&AdmittedRow::Bytes(&rec)),
				PreDecodeFilterOutcome::NeedFullDecode,
				"element {element:?} satisfies the whole set"
			);
		}
	}

	/// Two genuinely distinct literals still need both present, so an array
	/// holding one of them is provably rejected.
	#[test]
	fn streaming_array_overlap_all_still_rejects_a_missing_literal() {
		let set = Arc::new(HashSet::from([
			Value::Number(Number::Decimal("0.1".parse().unwrap())),
			Value::Number(Number::Decimal("0.2".parse().unwrap())),
		]));
		let tables = overlap_streaming_from_set(set.as_ref());
		assert_eq!(tables.literal_count, 2);
		let eval: Arc<dyn super::StreamingLeafEvaluator> = Arc::new(ArrayOverlapsLiteralSet {
			literal_to_idx: tables.literal_to_idx,
			wire_to_idx: tables.wire_to_idx,
			literal_set: tables.literal_set,
			literal_count: tables.literal_count,
			mode: OverlapMode::All,
		});
		let root = PredNode::LeafStreaming {
			path: vec!["tags".into()],
			evaluator: eval,
			fallback: LeafFallback {
				op: BinaryOperator::ContainAll,
				literal: Value::from(vec![
					Value::Number(Number::Decimal("0.1".parse().unwrap())),
					Value::Number(Number::Decimal("0.2".parse().unwrap())),
				]),
				reversed: false,
			},
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj = Object::from(BTreeMap::from([(
			Strand::from("tags"),
			Value::from(vec![Value::Number(Number::Decimal("0.1".parse().unwrap()))]),
		)]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	/// The array-overlap evaluators resolve compound elements through the same
	/// `HashMap<Value, _>`, which answers a nested float against a nested
	/// decimal exactly: a hit where the decimal spells the float, a decisive
	/// miss otherwise. The verdict is the same under every hasher seed.
	#[test]
	fn streaming_array_overlap_nested_float_and_decimal_resolve_exactly() {
		// Wrapped in an object so the element takes the decode path rather than
		// the Number wire partition.
		let wrap = |v: Value| Value::Object(Object::from(BTreeMap::from([(Strand::from("v"), v)])));
		let (hit, miss) = (PreDecodeFilterOutcome::NeedFullDecode, PreDecodeFilterOutcome::Reject);
		for (f, d, equal) in [(0.5, "0.5", true), (0.1, "0.1", false)] {
			let stored = wrap(Value::Number(Number::Decimal(d.parse().unwrap())));
			let field = wrap(Value::Number(Number::Float(f)));
			let rec = wire_record_plain_object(Object::from(BTreeMap::from([(
				Strand::from("tags"),
				Value::from(vec![field]),
			)])));
			for (mode, op, outcome) in [
				(
					OverlapMode::Any,
					BinaryOperator::ContainAny,
					if equal {
						hit
					} else {
						miss
					},
				),
				(
					OverlapMode::All,
					BinaryOperator::ContainAll,
					if equal {
						hit
					} else {
						miss
					},
				),
				(
					OverlapMode::None,
					BinaryOperator::ContainNone,
					if equal {
						miss
					} else {
						hit
					},
				),
			] {
				for _ in 0..SEED_DRAWS {
					let set = Arc::new(HashSet::from([stored.clone()]));
					let tables = overlap_streaming_from_set(set.as_ref());
					let eval: Arc<dyn super::StreamingLeafEvaluator> =
						Arc::new(ArrayOverlapsLiteralSet {
							literal_to_idx: tables.literal_to_idx,
							wire_to_idx: tables.wire_to_idx,
							literal_set: tables.literal_set,
							literal_count: tables.literal_count,
							mode,
						});
					let root = PredNode::LeafStreaming {
						path: vec!["tags".into()],
						evaluator: eval,
						fallback: LeafFallback {
							op: op.clone(),
							literal: Value::from(vec![stored.clone()]),
							reversed: false,
						},
					};
					let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
					assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), outcome, "{f} {mode:?}");
				}
			}
		}
	}

	/// One float is equal to at most one of two decimals that differ from each
	/// other, so `ContainAll` over the two is never satisfied by an array
	/// holding the float alone, and the filter says so under every seed, for the
	/// numbers bare or nested.
	#[test]
	fn streaming_array_overlap_all_float_equals_at_most_one_of_two_decimals() {
		let float = Value::Number(Number::Float(0.8800000005551132));
		let lo = Value::Number(Number::Decimal("0.8800000005551131776471763448".parse().unwrap()));
		let hi = Value::Number(Number::Decimal("0.8800000005551131776471763449".parse().unwrap()));
		assert_ne!(lo, hi);
		assert!((float == lo) as u8 + (float == hi) as u8 <= 1, "equality is transitive");

		let wrap = |v: &Value| {
			Value::Object(Object::from(BTreeMap::from([(Strand::from("v"), v.clone())])))
		};
		for (literals, element) in [
			(vec![lo.clone(), hi.clone()], float.clone()),
			(vec![wrap(&lo), wrap(&hi)], wrap(&float)),
		] {
			let rec = wire_record_plain_object(Object::from(BTreeMap::from([(
				Strand::from("tags"),
				Value::from(vec![element.clone()]),
			)])));
			let holds = apply_binary(
				&BinaryOperator::ContainAll,
				&Value::from(vec![element.clone()]),
				&Value::from(literals.clone()),
			);
			assert_eq!(holds, Some(Value::Bool(false)), "{element:?}");
			for _ in 0..SEED_DRAWS {
				let set: HashSet<Value> = literals.iter().cloned().collect();
				let tables = overlap_streaming_from_set(&set);
				assert_eq!(tables.literal_count, 2, "the decimals are two values");
				let eval: Arc<dyn super::StreamingLeafEvaluator> =
					Arc::new(ArrayOverlapsLiteralSet {
						literal_to_idx: tables.literal_to_idx,
						wire_to_idx: tables.wire_to_idx,
						literal_set: tables.literal_set,
						literal_count: tables.literal_count,
						mode: OverlapMode::All,
					});
				let root = PredNode::LeafStreaming {
					path: vec!["tags".into()],
					evaluator: eval,
					fallback: LeafFallback {
						op: BinaryOperator::ContainAll,
						literal: Value::from(literals.clone()),
						reversed: false,
					},
				};
				let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
				assert_eq!(
					pf.apply(&AdmittedRow::Bytes(&rec)),
					PreDecodeFilterOutcome::Reject,
					"{element:?}"
				);
			}
		}
	}

	/// A plan over literals that spell one number as floats and decimals judges
	/// rows exactly as the operator does, under every seed: a row the predicate
	/// holds for is never rejected, and one it fails for is.
	/// A float and a decimal are one value only where the decimal spells the
	/// float's exact value, so literals mixing the two form equivalence classes
	/// as any others do: `ContainAll` over them is planned as the streaming
	/// array-overlap evaluator, never as a decode of the field.
	#[test]
	fn contains_all_over_floats_and_decimals_plans_a_streaming_leaf() {
		let dec = |s: &str| Value::Number(Number::Decimal(s.parse().unwrap()));
		let float = |f: f64| Value::Number(Number::Float(f));
		for literal in [
			Value::from(vec![float(0.1), dec("0.1")]),
			Value::from(vec![float(0.5), dec("0.5"), dec("0.50")]),
		] {
			let shape = PreDecodeShape::Leaf {
				path: vec![PathSegment::from("tags")].into(),
				op: BinaryOperator::ContainAll,
				literal: literal.clone(),
				reversed: false,
			};
			let pf = shape.filter(TEST_DEPTH_LIMIT);
			assert!(
				matches!(pf.root, PredNode::LeafStreaming { .. }),
				"{literal:?} planned as {:?}",
				pf.root
			);
		}
	}

	#[test]
	fn contains_all_over_floats_and_decimals_judges_as_the_operator() {
		let float = Value::Number(Number::Float(0.8800000005551132));
		let lo = Value::Number(Number::Decimal("0.8800000005551131776471763448".parse().unwrap()));
		let hi = Value::Number(Number::Decimal("0.8800000005551131776471763449".parse().unwrap()));
		let literal = Value::from(vec![float.clone(), lo.clone(), hi.clone()]);
		let contains_all = PreDecodeShape::Leaf {
			path: vec![PathSegment::from("tags")].into(),
			op: BinaryOperator::ContainAll,
			literal: literal.clone(),
			reversed: false,
		};
		let not_contains_all = PreDecodeShape::Not(Box::new(contains_all.clone()));
		let tags = |v: &[&Value]| {
			let field = Value::from(v.iter().map(|v| (*v).clone()).collect::<Vec<_>>());
			(
				wire_record_plain_object(Object::from(BTreeMap::from([(
					Strand::from("tags"),
					field.clone(),
				)]))),
				apply_binary(&BinaryOperator::ContainAll, &field, &literal)
					== Some(Value::Bool(true)),
			)
		};
		let (all, all_holds) = tags(&[&float, &lo, &hi]);
		let (one, one_holds) = tags(&[&float]);
		assert!(all_holds && !one_holds);
		// `ContainAll` holds for the array holding every literal, and its
		// negation for the array holding the float alone.
		let cases = [
			(&contains_all, &all, PreDecodeFilterOutcome::NeedFullDecode),
			(&not_contains_all, &one, PreDecodeFilterOutcome::NeedFullDecode),
			(&contains_all, &one, PreDecodeFilterOutcome::Reject),
		];
		for _ in 0..SEED_DRAWS {
			for (shape, rec, outcome) in &cases {
				let pf = shape.filter(TEST_DEPTH_LIMIT);
				assert_eq!(pf.apply(&AdmittedRow::Bytes(rec)), *outcome, "{shape:?}");
			}
		}
	}

	#[test]
	fn streaming_array_containsnone_rejects_on_first_hit() {
		let lit = Value::from(vec![Value::String("a".into()), Value::String("b".into())]);
		let set = Arc::new(HashSet::from([Value::String("a".into()), Value::String("b".into())]));
		let tables = overlap_streaming_from_set(set.as_ref());
		let eval: Arc<dyn super::StreamingLeafEvaluator> = Arc::new(ArrayOverlapsLiteralSet {
			literal_to_idx: tables.literal_to_idx,
			wire_to_idx: tables.wire_to_idx,
			literal_set: tables.literal_set,
			literal_count: tables.literal_count,
			mode: OverlapMode::None,
		});
		let root = PredNode::LeafStreaming {
			path: vec!["tags".into()],
			evaluator: eval,
			fallback: LeafFallback {
				op: BinaryOperator::ContainNone,
				literal: lit,
				reversed: false,
			},
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj = Object::from(BTreeMap::from([(
			Strand::from("tags"),
			Value::from(vec![Value::String("a".into()), Value::String("z".into())]),
		)]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn streaming_missing_field_substring_contain_is_false_on_none() {
		let lit = Value::String("hi".into());
		let eval: Arc<dyn super::StreamingLeafEvaluator> = Arc::new(SubstringMatch {
			finder: memmem::Finder::new(b"hi").into_owned(),
			negated: false,
		});
		let root = PredNode::LeafStreaming {
			path: vec!["missing".into()],
			evaluator: eval,
			fallback: LeafFallback {
				op: BinaryOperator::Contain,
				literal: lit,
				reversed: false,
			},
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(2)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&AdmittedRow::Bytes(&rec)), PreDecodeFilterOutcome::Reject);
	}

	/// The records [`stored_value_records_are_judged_as_revision_records_are`]
	/// judges: scalars of each comparable kind, nested objects, arrays, a
	/// field that is not an object where a path descends through it, numbers
	/// of differing variants, an empty record and one wide enough to index.
	pub(super) fn differential_records() -> Vec<Object> {
		let object = |entries: Vec<(&str, Value)>| {
			Object::from(
				entries.into_iter().map(|(k, v)| (Strand::from(k), v)).collect::<BTreeMap<_, _>>(),
			)
		};
		let int = |i: i64| Value::Number(Number::Int(i));
		let text = |s: &str| Value::String(s.into());
		let mut wide = Object::default();
		for i in 0..12 {
			wide.0.insert(Strand::from(format!("k{i:02}")), int(i));
		}
		vec![
			object(vec![
				("a", int(1)),
				("b", text("x")),
				("c", Value::from(vec![int(1), int(2), int(3)])),
				("d", Value::Object(object(vec![("e", int(5)), ("f", text("hello world"))]))),
				("g", Value::Bool(true)),
				("h", Value::Number(Number::Float(1.5))),
			]),
			object(vec![
				("a", int(2)),
				("b", text("y")),
				("c", Value::from(Vec::<Value>::new())),
				("d", Value::Object(object(vec![("e", text("str"))]))),
				("tags", Value::from(vec![text("red"), text("blue")])),
			]),
			object(vec![("a", text("1")), ("b", Value::Null), ("d", int(5))]),
			object(vec![
				("a", Value::Number(Number::Decimal("1".parse().unwrap()))),
				("b", Value::Number(Number::Float(1.0))),
				("c", Value::from(vec![text("a"), int(2)])),
			]),
			Object::default(),
			wide,
		]
	}

	/// The predicates [`stored_value_records_are_judged_as_revision_records_are`]
	/// judges: every node kind the compiler emits, over present, absent, nested
	/// and mistyped fields.
	pub(super) fn differential_predicates() -> Vec<PredNode> {
		use BinaryOperator::*;

		let int = |i: i64| Value::Number(Number::Int(i));
		let text = |s: &str| Value::String(s.into());
		let path = |p: &[&str]| p.iter().copied().map(PathSegment::from).collect::<Vec<_>>();
		let cmp = |p: &[&str], op: BinaryOperator, lit: Value| leaf(path(p), op, lit, false);
		let streaming = |p: &[&str], evaluator: Arc<dyn super::StreamingLeafEvaluator>, op, lit| {
			PredNode::LeafStreaming {
				path: path(p),
				evaluator,
				fallback: LeafFallback {
					op,
					literal: lit,
					reversed: false,
				},
			}
		};
		let substring = |p: &[&str], needle: &str, negated: bool| {
			let evaluator = Arc::new(SubstringMatch {
				finder: memmem::Finder::new(needle.as_bytes()).into_owned(),
				negated,
			});
			streaming(
				p,
				evaluator,
				if negated {
					NotContain
				} else {
					Contain
				},
				text(needle),
			)
		};
		let contains = |p: &[&str], needle: Value, negated: bool| {
			let evaluator = Arc::new(ArrayElementContains {
				needle: Arc::new(needle.clone()),
				needle_wire: Arc::new(LiteralWire::from_value(&needle)),
				negated,
			});
			streaming(
				p,
				evaluator,
				if negated {
					NotContain
				} else {
					Contain
				},
				needle,
			)
		};
		let overlaps = |p: &[&str], members: Vec<Value>, mode: OverlapMode| {
			let set: HashSet<Value> = members.iter().cloned().collect();
			let tables = overlap_streaming_from_set(&set);
			let evaluator = Arc::new(ArrayOverlapsLiteralSet {
				literal_to_idx: tables.literal_to_idx,
				wire_to_idx: tables.wire_to_idx,
				literal_set: tables.literal_set,
				literal_count: tables.literal_count,
				mode,
			});
			let op = match mode {
				OverlapMode::Any => ContainAny,
				OverlapMode::None => ContainNone,
				OverlapMode::All => ContainAll,
			};
			streaming(p, evaluator, op, Value::from(members))
		};
		let members = |values: Vec<Value>| Arc::new(values.into_iter().collect::<HashSet<_>>());
		let fused = |clauses: Vec<(&str, BinaryOperator, Value)>| PredNode::FusedFlatMapAnd {
			clauses: FusedFlatClauses::try_new(
				clauses
					.into_iter()
					.map(|(k, op, lit)| fused_clause(k.as_bytes().to_vec(), op, lit, false))
					.collect(),
			)
			.expect("sorted unique"),
		};
		let navigate = |segment: &str, child: PredNode| PredNode::NavigatePrefix {
			segment: segment.into(),
			child: Box::new(child),
		};
		vec![
			cmp(&["a"], Equal, int(1)),
			cmp(&["a"], NotEqual, int(1)),
			cmp(&["a"], MoreThan, int(1)),
			cmp(&["a"], LessThanEqual, int(1)),
			cmp(&["a"], ExactEqual, int(1)),
			leaf(path(&["a"]), LessThan, int(1), true),
			cmp(&["b"], Equal, text("x")),
			cmp(&["b"], LessThan, text("y")),
			cmp(&["h"], MoreThanEqual, Value::Number(Number::Float(1.5))),
			cmp(&["zz"], Equal, Value::None),
			cmp(&["zz"], NotEqual, int(1)),
			cmp(&["d", "e"], Equal, int(5)),
			cmp(&["d", "f"], Contain, text("world")),
			cmp(&["d", "e", "x"], Equal, int(5)),
			cmp(&["k07"], MoreThan, int(6)),
			navigate("d", cmp(&["e"], Equal, int(5))),
			set_membership(path(&["a"]), Inside, members(vec![int(1), int(2)]), false),
			set_membership(path(&["b"]), NotInside, members(vec![text("x")]), false),
			set_membership(
				path(&["a"]),
				Inside,
				members(vec![Value::Number(Number::Decimal("1".parse().unwrap()))]),
				false,
			),
			substring(&["b"], "x", false),
			substring(&["d", "f"], "world", true),
			contains(&["c"], int(2), false),
			contains(&["tags"], text("red"), true),
			overlaps(&["tags"], vec![text("red"), text("green")], OverlapMode::Any),
			overlaps(&["c"], vec![int(1), int(9)], OverlapMode::All),
			overlaps(&["c"], vec![int(7)], OverlapMode::None),
			fused(vec![("a", MoreThan, int(0)), ("b", Equal, text("x"))]),
			fused(vec![("k03", Equal, int(3)), ("k11", LessThan, int(5))]),
			navigate(
				"d",
				PredNode::FusedFlatMapAnd {
					clauses: FusedFlatClauses::try_new(vec![
						fused_clause(b"e".to_vec(), Equal, int(5), false),
						fused_clause(b"f".to_vec(), Equal, text("hello world"), false),
					])
					.expect("sorted unique"),
				},
			),
			PredNode::And(vec![cmp(&["a"], Equal, int(1)), cmp(&["b"], Equal, text("y"))]),
			PredNode::Or(vec![cmp(&["a"], Equal, int(9)), cmp(&["g"], Equal, Value::Bool(true))]),
			PredNode::Not(Box::new(cmp(&["a"], Equal, int(1)))),
		]
	}

	/// What the predicate is over the decoded record, where its leaves can say:
	/// `None` when a path descends through a field that is not an object, or
	/// an operator gives no boolean.
	pub(super) fn truth(node: &PredNode, data: &Value, prefix: &[&str]) -> Option<bool> {
		let pick = |p: &[PathSegment]| -> Option<Value> {
			let mut v = data;
			for segment in prefix.iter().copied().chain(p.iter().map(PathSegment::as_str)) {
				v = if let Value::Object(o) = v {
					match o.get(segment) {
						Some(inner) => inner,
						None => return Some(Value::None),
					}
				} else {
					return None;
				};
			}
			Some(v.clone())
		};
		let judge = |op: &BinaryOperator, left: &Value, right: &Value| match super::apply_binary(
			op, left, right,
		) {
			Some(Value::Bool(b)) => Some(b),
			_ => None,
		};
		let compare = |p: &[PathSegment], op: &BinaryOperator, lit: &Value, reversed: bool| {
			let v = pick(p)?;
			if reversed {
				judge(op, lit, &v)
			} else {
				judge(op, &v, lit)
			}
		};
		// Three-valued: a definite part decides a conjunction or disjunction
		// whatever the others are.
		let all = |parts: Vec<Option<bool>>| match parts.contains(&Some(false)) {
			true => Some(false),
			false => parts.into_iter().collect::<Option<Vec<_>>>().map(|_| true),
		};
		match node {
			PredNode::And(xs) => all(xs.iter().map(|x| truth(x, data, prefix)).collect()),
			PredNode::Or(xs) => {
				let parts: Vec<_> = xs.iter().map(|x| truth(x, data, prefix)).collect();
				match parts.contains(&Some(true)) {
					true => Some(true),
					false => parts.into_iter().collect::<Option<Vec<_>>>().map(|_| false),
				}
			}
			PredNode::Not(inner) => truth(inner, data, prefix).map(|b| !b),
			PredNode::Leaf {
				path,
				op,
				literal,
				reversed,
				..
			} => compare(path, op, literal, *reversed),
			PredNode::LeafSetMembership {
				path,
				op,
				set,
				..
			} => compare(path, op, &Value::from(set.iter().cloned().collect::<Vec<_>>()), false),
			PredNode::LeafStreaming {
				path,
				fallback,
				..
			} => judge(&fallback.op, &pick(path)?, &fallback.literal),
			PredNode::FusedFlatMapAnd {
				clauses,
				..
			} => {
				let mut parts = Vec::new();
				for clause in clauses.iter() {
					let key = PathSegment::from(std::str::from_utf8(&clause.key_utf8).unwrap());
					for opc in &clause.ops {
						parts.push(compare(
							std::slice::from_ref(&key),
							&opc.op,
							&opc.literal,
							opc.reversed,
						));
					}
				}
				all(parts)
			}
			PredNode::NavigatePrefix {
				segment,
				child,
			} => {
				let mut next = prefix.to_vec();
				next.push(segment.as_str());
				truth(child, data, &next)
			}
		}
	}

	/// A stored-value record is judged as its `revision` encoding is: it is
	/// rejected only when the predicate is false of the decoded record, and
	/// rejected whenever the `revision` encoding of it is. Covers a record
	/// whose `id` its key replaces and one that stores none.
	#[test]
	fn stored_value_records_are_judged_as_revision_records_are() {
		let rid = RecordId::new("t".into(), crate::val::RecordIdKey::Number(1));
		let mut rejected = 0;
		for object in differential_records() {
			let mut with_id = object.clone();
			with_id.0.insert(Strand::from("id"), Value::RecordId(rid.clone()));
			let revision = wire_record_plain_object(with_id.clone());
			let data = Value::Object(with_id);
			let keyed =
				crate::catalog::encode_record(DOCUMENT, None, &data, Some(&rid)).expect("encodes");
			let bare =
				crate::catalog::encode_record(DOCUMENT, None, &Value::Object(object.clone()), None)
					.expect("encodes");
			for predicate in differential_predicates() {
				let pf = PreDecodeFilter::new(predicate.clone(), TEST_DEPTH_LIMIT);
				let expected = pf.apply(&AdmittedRow::Bytes(&revision));
				let truth = truth(&predicate, &data, &[]);
				for (encoding, bytes) in [("keyed", &keyed), ("bare", &bare)] {
					let outcome = pf.apply(&AdmittedRow::Bytes(bytes));
					if outcome == PreDecodeFilterOutcome::Reject {
						rejected += 1;
						assert_eq!(
							truth,
							Some(false),
							"{encoding} record rejected though {predicate:?} is not false of {data:?}"
						);
					}
					if expected == PreDecodeFilterOutcome::Reject {
						assert_eq!(
							outcome, expected,
							"{encoding} record not rejected by {predicate:?} over {data:?}"
						);
					}
				}
			}
		}
		assert!(rejected > 0, "the corpus rejects no stored-value record");
	}

	// ---------- a regex against strings: `Value::equal` is not `==` ----------

	fn regex(pattern: &str) -> Value {
		Value::Regex(pattern.parse().expect("regex"))
	}

	fn strings(items: &[&str]) -> Value {
		Value::from(items.iter().map(|s| Value::from(*s)).collect::<Vec<_>>())
	}

	/// A record whose one field `v` holds `value`.
	fn record_with_v(value: Value) -> Vec<u8> {
		wire_record_plain_object(Object::from(BTreeMap::from([(Strand::from("v"), value)])))
	}

	/// Whether `field <op> literal` holds, as the operator judges the decoded
	/// field.
	fn operator_holds(op: &BinaryOperator, field: &Value, literal: &Value) -> bool {
		match super::apply_binary(op, field, literal) {
			Some(Value::Bool(holds)) => holds,
			other => panic!("{op:?} answered {other:?}"),
		}
	}

	/// The shape of `v <op> literal`, whose `filter` builds the tree a scan
	/// judges records by.
	fn v_shape(op: BinaryOperator, literal: Value) -> PreDecodeShape {
		PreDecodeShape::Leaf {
			path: vec![PathSegment::from("v")].into(),
			op,
			literal,
			reversed: false,
		}
	}

	/// `path CONTAINSANY` / `CONTAINSALL` / `CONTAINSNONE` over `set` exactly as
	/// given, including elements the planner keeps out of a literal set.
	fn overlap(path: Vec<PathSegment>, mode: OverlapMode, set: &HashSet<Value>) -> PredNode {
		let op = match mode {
			OverlapMode::Any => BinaryOperator::ContainAny,
			OverlapMode::All => BinaryOperator::ContainAll,
			OverlapMode::None => BinaryOperator::ContainNone,
		};
		let tables = overlap_streaming_from_set(set);
		PredNode::LeafStreaming {
			path,
			evaluator: Arc::new(ArrayOverlapsLiteralSet {
				literal_to_idx: tables.literal_to_idx,
				wire_to_idx: tables.wire_to_idx,
				literal_set: tables.literal_set,
				literal_count: tables.literal_count,
				mode,
			}),
			fallback: LeafFallback {
				op,
				literal: Value::from(set.iter().cloned().collect::<Vec<_>>()),
				reversed: false,
			},
		}
	}

	/// `IN` and `CONTAINS*` judge membership by `Value::equal`, which matches a
	/// string against a regex, while the decoded probe of a literal set compares
	/// with `==`. A stored regex misses every string it matches, so its miss must
	/// not reject the row, however number-free the set.
	#[test]
	fn stored_regex_matching_a_string_literal_needs_full_decode() {
		use BinaryOperator::{ContainAll, ContainAny, Inside};
		let cases = [
			(Inside, regex("a.*"), strings(&["abc"])),
			(ContainAny, Value::from(vec![regex("a.*")]), strings(&["abc"])),
			(ContainAll, Value::from(vec![regex("a.*")]), strings(&["abc"])),
			// One regex answers for both literals.
			(ContainAll, Value::from(vec![regex("a.*")]), strings(&["abc", "axe"])),
			// The string hits on the wire; the regex still answers for `xyz`.
			(
				ContainAll,
				Value::from(vec![Value::from("abc"), regex("x")]),
				strings(&["abc", "xyz"]),
			),
		];
		let mut rejected = Vec::new();
		for (op, field, literal) in cases {
			assert!(operator_holds(&op, &field, &literal), "{field:?} {op:?} {literal:?}");
			let pf = v_shape(op.clone(), literal.clone()).filter(TEST_DEPTH_LIMIT);
			if pf.apply(&AdmittedRow::Bytes(&record_with_v(field.clone())))
				== PreDecodeFilterOutcome::Reject
			{
				rejected.push(format!("{field:?} {op:?} {literal:?}"));
			}
		}
		assert!(rejected.is_empty(), "rejected rows the operator holds for: {rejected:#?}");
	}

	/// `v = 'abc' OR v = 'xyz'` is fused into one set probe. The single
	/// comparison `v = 'abc'` decodes a regex peer, and the fused set must not
	/// reject it either.
	#[test]
	fn fused_equality_chain_on_a_stored_regex_needs_full_decode() {
		let field = regex("a.*");
		assert!(operator_holds(&BinaryOperator::Equal, &field, &Value::from("abc")));
		let shape = PreDecodeShape::Or(vec![
			v_shape(BinaryOperator::Equal, Value::from("abc")),
			v_shape(BinaryOperator::Equal, Value::from("xyz")),
		]);
		let pf = shape.filter(TEST_DEPTH_LIMIT);
		assert!(matches!(pf.root, PredNode::LeafSetMembership { .. }), "{:?}", pf.root);
		assert_eq!(
			pf.apply(&AdmittedRow::Bytes(&record_with_v(field))),
			PreDecodeFilterOutcome::NeedFullDecode
		);
	}

	/// `NOT IN` and `CONTAINSNONE` read the same miss as proof that a stored
	/// regex matches no literal. Under `NOT` that becomes a rejection of a row
	/// the predicate holds for.
	#[test]
	fn negated_exclusion_of_a_stored_regex_matching_a_string_needs_full_decode() {
		use BinaryOperator::{ContainNone, NotInside};
		let literal = strings(&["abc"]);
		let mut rejected = Vec::new();
		for (op, field) in
			[(NotInside, regex("a.*")), (ContainNone, Value::from(vec![regex("a.*")]))]
		{
			// The exclusion is false, so its negation holds.
			assert!(!operator_holds(&op, &field, &literal), "{field:?} {op:?}");
			let shape = PreDecodeShape::Not(Box::new(v_shape(op.clone(), literal.clone())));
			let pf = shape.filter(TEST_DEPTH_LIMIT);
			if pf.apply(&AdmittedRow::Bytes(&record_with_v(field.clone())))
				== PreDecodeFilterOutcome::Reject
			{
				rejected.push(format!("NOT ({field:?} {op:?})"));
			}
		}
		assert!(rejected.is_empty(), "rejected rows the predicate holds for: {rejected:#?}");
	}

	/// The same pairing the other way round: a stored string misses a set
	/// holding a regex that matches it. The planner keeps regexes out of literal
	/// sets, so only a hand-built set reaches this.
	#[test]
	fn stored_string_matching_a_regex_in_the_set_needs_full_decode() {
		let set = HashSet::from([regex("a.*")]);
		let literal = Value::from(vec![regex("a.*")]);
		let field = Value::from("abc");
		let array = Value::from(vec![field.clone()]);
		assert!(operator_holds(&BinaryOperator::Inside, &field, &literal));
		assert!(operator_holds(&BinaryOperator::ContainAny, &array, &literal));
		let inside =
			set_membership(vec!["v".into()], BinaryOperator::Inside, Arc::new(set.clone()), false);
		let any = overlap(vec!["v".into()], OverlapMode::Any, &set);
		let mut rejected = Vec::new();
		for (name, root, stored) in [("IN", inside, field), ("CONTAINSANY", any, array)] {
			let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
			if pf.apply(&AdmittedRow::Bytes(&record_with_v(stored)))
				== PreDecodeFilterOutcome::Reject
			{
				rejected.push(name);
			}
		}
		assert!(rejected.is_empty(), "rejected rows the operator holds for: {rejected:?}");
	}

	/// A wrong *hit* cannot reject a row. A probe hits only an element `==` to
	/// the value, and `==` implies `Value::equal` for every pair of values, so
	/// where `NOT IN` and `CONTAINSNONE` reject on a hit the operator is false
	/// too — for a string, a compound value and, under `NOT IN`, a regex (in a
	/// hand-built set, since the planner keeps regexes out of literal sets).
	/// `CONTAINSNONE` makes no probe for a regex against a set holding one,
	/// since a miss there would not be decisive (see `hash_miss_is_decisive`),
	/// so it decodes that row instead of rejecting it.
	#[test]
	fn exclusion_rejects_on_a_hit_only_where_the_operator_is_false() {
		let object =
			Value::Object(Object::from(BTreeMap::from([(Strand::from("k"), Value::from("abc"))])));
		for (field, overlap_probes) in
			[(Value::from("abc"), true), (object, true), (regex("a.*"), false)]
		{
			let set = HashSet::from([field.clone()]);
			let literal = Value::from(vec![field.clone()]);

			assert!(!operator_holds(&BinaryOperator::NotInside, &field, &literal), "{field:?}");
			let root = set_membership(
				vec!["v".into()],
				BinaryOperator::NotInside,
				Arc::new(set.clone()),
				false,
			);
			let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
			assert_eq!(
				pf.apply(&AdmittedRow::Bytes(&record_with_v(field.clone()))),
				PreDecodeFilterOutcome::Reject,
				"NOT IN {field:?}"
			);

			if !overlap_probes {
				continue;
			}
			let array = Value::from(vec![field.clone()]);
			assert!(!operator_holds(&BinaryOperator::ContainNone, &array, &literal), "{field:?}");
			let pf = PreDecodeFilter::new(
				overlap(vec!["v".into()], OverlapMode::None, &set),
				TEST_DEPTH_LIMIT,
			);
			assert_eq!(
				pf.apply(&AdmittedRow::Bytes(&record_with_v(array))),
				PreDecodeFilterOutcome::Reject,
				"CONTAINSNONE {field:?}"
			);
		}
	}
}
