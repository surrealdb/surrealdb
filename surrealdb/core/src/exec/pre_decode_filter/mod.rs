//! Pre-decode filter ([`PreDecodeFilter`]) — **reject-only** optimisation during KV scans.
//!
//! Before paying full [`crate::catalog::Record`] deserialization, the scan may prove from
//! **revision-encoded KV bytes alone** that the pushed-down WHERE cannot hold for this row. Only
//! [`PreDecodeFilterOutcome::Reject`] drops the row without decode; every other outcome falls
//! through to the normal path.
//!
//! ## Timing (vs `scan_predicate`)
//!
//! This runs synchronously inside [`crate::exec::operators::scan::pipeline::kv_scan_stream`]
//! **before** [`crate::exec::operators::scan::pipeline::decode_record`]. The scan's pushed-down
//! [`crate::exec::PhysicalExpr`] predicate (`scan_predicate` on table / dynamic / record-id range
//! scans) still runs **after** decode in
//! [`crate::exec::operators::scan::pipeline::filter_and_process_batch`], together with table-level
//! permission filtering, computed fields, and field-level SELECT permissions — that evaluation is
//! **authoritative** for inclusion.
//!
//! | | Pre-decode filter | `scan_predicate` pushdown |
//! |--|-------------------|---------------------------|
//! | Input | Raw KV bytes | Decoded [`crate::val::Value`] |
//! | Semantics | Conservative; reject-only | Exact (must match `Filter`) |
//! | Async / params | Literal RHS baked at compile; sync only | Full [`PhysicalExpr`] |
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
mod streaming;
mod wire_cmp;
pub(crate) mod wire_literal;

use std::collections::HashSet;
use std::sync::Arc;

pub(crate) use compile::{pre_decode_filter_for_execute, pre_decode_filter_status_at_plan_time};
use revision::WalkRevisioned;
pub(crate) use streaming::StreamingLeafEvaluator;
use wire_literal::{LiteralSet, LiteralWire};

use crate::catalog::Record;
use crate::expr::operator::BinaryOperator;
use crate::fnc::operate;
use crate::key::record::RecordKey;
use crate::val::object_extract::{
	DescendResult, Extracted, PathSegment, SlotScanResult, WalkLeafErr,
	descend_to_value_walker_parts, extract_field_from_record_bytes,
	scan_record_object_at_path_with_slots,
};
use crate::val::{RecordId, Value};

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
	pub(crate) ops: Vec<FlatClauseOp>,
}

impl FusedFlatClause {
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
		Self {
			key_utf8,
			ops: vec![FlatClauseOp {
				op,
				literal,
				literal_wire,
				reversed,
			}],
		}
	}
}

/// Lookup-key view used by
/// [`scan_record_root_object_for_keys_sorted`]: returns the clause's
/// `key_utf8` bytes. Lets fused-map evaluation pass `&[FusedFlatClause]`
/// directly without projecting into a `Vec<&[u8]>` per row.
impl AsRef<[u8]> for FusedFlatClause {
	fn as_ref(&self) -> &[u8] {
		&self.key_utf8
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
	/// One map scan over a [`Value::Object`] VecMap at the current anchor (`at_record_root`:
	/// anchored at the table row's root object; otherwise at a nested object).
	///
	/// `clauses` is a [`FusedFlatClauses`] which enforces the strictly-ascending,
	/// deduplicated-by-`key_utf8` invariant at the type level — fused-map evaluation
	/// pairs the slice with [`scan_record_root_object_for_keys_sorted`] output by position.
	FusedFlatMapAnd {
		at_record_root: bool,
		clauses: FusedFlatClauses,
	},
	NavigatePrefix {
		segment: PathSegment,
		child: Box<PredNode>,
	},
}

/// Why a pre-decode filter cannot be built for a scan.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum PreDecodeFilterReason {
	/// The predicate's physical shape is not supported by [`compile::compile_predicate_shape`]
	/// (e.g. `Outside` / `Intersects` / `AllEqual` / `AnyEqual`, `MATCHES`, or non-static field
	/// paths).
	UnsupportedPredicate,
	/// One of the referenced root field names is computed (`DEFINE FIELD … VALUE …`); the raw
	/// KV bytes do not necessarily match the value the engine will materialise.
	ComputedFields,
	/// One of the referenced root field names has a non-`Allow` SELECT permission, so reading
	/// it from raw KV bytes would bypass per-field authorisation.
	FieldPermissions,
}

/// Plan / execute-time status of the pre-decode filter for a KV scan
/// ([`TableScan`](crate::exec::operators::scan::table::TableScan),
/// [`DynamicScan`](crate::exec::operators::scan::dynamic::DynamicScan),
/// [`RecordIdScan`](crate::exec::operators::scan::record_id::RecordIdScan)).
#[derive(Debug, Clone)]
pub(crate) enum PreDecodeFilterStatus {
	/// No WHERE predicate pushed into the scan — omit `pre_decode_filter` in EXPLAIN.
	NotApplicable,
	/// Predicate compiled and field checks passed at plan time.
	Active(Arc<PreDecodeFilter>),
	/// Predicate shape is known; field state or permission checks need runtime context.
	Deferred(Arc<PredNode>),
	/// Predicate shape unsupported or field checks failed.
	Ineligible(PreDecodeFilterReason),
}

impl PreDecodeFilterStatus {
	/// Human-readable value for the `pre_decode_filter` attribute in EXPLAIN output.
	///
	/// Returns [`None`] when the scan has no WHERE predicate pushed into it, in which case the
	/// `pre_decode_filter` attribute should be omitted from `attrs()` entirely.
	///
	/// Returns `&'static str` rather than `String` — every variant maps to a
	/// compile-time literal, so callers borrow a static buffer instead of
	/// paying for a `String::from` per EXPLAIN render.
	pub(crate) fn explain_text(&self) -> Option<&'static str> {
		match self {
			Self::NotApplicable => None,
			Self::Active(_) => Some("yes"),
			Self::Deferred(_) => Some("deferred (runtime field state)"),
			Self::Ineligible(PreDecodeFilterReason::UnsupportedPredicate) => {
				Some("no (unsupported predicate)")
			}
			Self::Ineligible(PreDecodeFilterReason::ComputedFields) => Some("no (computed fields)"),
			Self::Ineligible(PreDecodeFilterReason::FieldPermissions) => {
				Some("no (field permissions)")
			}
		}
	}
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

#[derive(Debug)]
pub(crate) struct PreDecodeFilter {
	pub(crate) root: PredNode,
	/// Hard cap on path-segment count for any pre-decode descent. Sourced
	/// from `ctx.config.idiom_recursion_limit` at the planner; bounds the
	/// stack and intermediate allocations done by
	/// [`crate::val::object_extract`]'s walker descent. Paths longer than
	/// the limit fall back to full-record decode + post-decode evaluation.
	pub(crate) depth_limit: u32,
}

impl PreDecodeFilter {
	pub(crate) fn new(root: PredNode, depth_limit: u32) -> Self {
		Self {
			root,
			depth_limit,
		}
	}

	pub(crate) fn apply(&self, key: &[u8], record_bytes: &[u8]) -> PreDecodeFilterOutcome {
		let ev = self.eval_node(key, record_bytes, &[], &self.root);
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
	fn eval_node(
		&self,
		key: &[u8],
		record_bytes: &[u8],
		prefix: &[&PathSegment],
		node: &PredNode,
	) -> Evidence {
		match node {
			PredNode::And(xs) => {
				combine_and(xs.iter().map(|x| self.eval_node(key, record_bytes, prefix, x)))
			}
			PredNode::Or(xs) => {
				combine_or(xs.iter().map(|x| self.eval_node(key, record_bytes, prefix, x)))
			}
			PredNode::Not(inner) => match self.eval_node(key, record_bytes, prefix, inner) {
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
				key,
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
				child: _,
			} if segment.as_str() == "id" => Evidence::Unknown,
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
				self.eval_node(key, record_bytes, &next, child.as_ref())
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
			// Open the record walker, take the `data` field's wire bytes via
			// the macro-emitted accessor (O(1) on rev-2 `indexed_struct`
			// records; sequential `metadata` skip on rev-1).
			let mut record_reader: &[u8] = record_bytes;
			let data_bytes = Record::walk_revisioned(&mut record_reader)
				.and_then(|w| w.into_data_bytes())
				.map_err(|_| WalkLeafErr::Bail)?;
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
				self.fallback_leaf_streaming(record_bytes, &full, fallback)
			}
		}
	}

	fn fallback_leaf_streaming(
		&self,
		record_bytes: &[u8],
		full: &[PathSegment],
		fallback: &LeafFallback,
	) -> Evidence {
		match extract_field_from_record_bytes(record_bytes, full, self.depth_limit) {
			Extracted::Found(v) => {
				evidence_from_binary_cmp(&fallback.op, &fallback.literal, fallback.reversed, &v)
			}
			Extracted::Missing => evidence_from_binary_cmp(
				&fallback.op,
				&fallback.literal,
				fallback.reversed,
				&Value::None,
			),
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
		// the original `set` so `fallback` membership is exact.
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
				Ok(v) => Some(Self::set_evidence_for(op, set.contains(&v))),
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
		key: &[u8],
		record_bytes: &[u8],
		prefix: &[&PathSegment],
		path: &[PathSegment],
		op: &BinaryOperator,
		literal: &Value,
		literal_wire: &LiteralWire,
		reversed: bool,
	) -> Evidence {
		// Fast-path: equality on the synthetic `id` field at the row root —
		// derive the record id straight from the KV key, no decode.
		if prefix.is_empty() && path.len() == 1 && path[0].as_str() == "id" {
			let Ok(k) = RecordKey::decode_key(key) else {
				return Evidence::Unknown;
			};
			let v = Value::RecordId(RecordId {
				table: k.tb.into_owned(),
				key: k.id,
			});
			return evidence_from_binary_cmp(op, literal, reversed, &v);
		}
		// `id.<sub>` at the row root cannot be reliably proved from the
		// encoded value alone — table-level / projection nuances apply.
		if prefix.is_empty() && path.first().is_some_and(|s| s.as_str() == "id") && path.len() > 1 {
			return Evidence::Unknown;
		}
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
		// Open the record walker, take the `data` field's wire bytes via the
		// macro-emitted accessor (O(1) on rev-2 `indexed_struct` records;
		// sequential `metadata` skip on rev-1).
		let mut record_reader: &[u8] = record_bytes;
		let Ok(data_bytes) =
			Record::walk_revisioned(&mut record_reader).and_then(|w| w.into_data_bytes())
		else {
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
		match op {
			BinaryOperator::Inside => {
				if inside {
					Evidence::ProvablyTrue
				} else {
					Evidence::ProvablyFalse
				}
			}
			BinaryOperator::NotInside => {
				if inside {
					Evidence::ProvablyFalse
				} else {
					Evidence::ProvablyTrue
				}
			}
			_ => Evidence::Unknown,
		}
	}
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
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use std::collections::{BTreeMap, HashSet};
	use std::sync::Arc;

	use memchr::memmem;
	use revision::SerializeRevisioned;
	use surrealdb_strand::Strand;

	use super::streaming::{
		ArrayElementContains, ArrayOverlapsLiteralSet, OverlapMode, SubstringMatch,
		overlap_streaming_from_set,
	};
	use super::wire_literal::{LiteralSet, LiteralWire};
	use super::{
		FusedFlatClause, FusedFlatClauses, LeafFallback, PathSegment, PreDecodeFilter,
		PreDecodeFilterOutcome, PredNode,
	};
	use crate::catalog::Record;
	use crate::expr::operator::BinaryOperator;
	use crate::val::{Number, Object, Value};

	/// Depth limit used in test fixtures. Matches the default
	/// `ctx.config.idiom_recursion_limit` (256) so test behaviour reflects
	/// the production planner's wiring.
	const TEST_DEPTH_LIMIT: u32 = 256;

	// ---------- test-only constructors ----------
	//
	// Mirror what `compile.rs` does in production: pre-encode the literal
	// wire / set partitions so every test predicate has the same shape as a
	// real one. Lets the tests stay readable (no `LiteralWire::from_value`
	// noise per construction site).

	fn leaf(
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

	fn set_membership(
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

	fn fused_clause(
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::NeedFullDecode);
	}

	#[test]
	fn leaf_eq_false_rejects_row() {
		let root =
			leaf(vec!["a".into()], BinaryOperator::Equal, Value::Number(Number::Int(1)), false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(2)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn eval_set_membership_hits_returns_need_full_decode_for_inside() {
		let set = Arc::new(HashSet::from([Value::Number(Number::Int(1))]));
		let root = set_membership(vec!["a".into()], BinaryOperator::Inside, set, false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(1)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::NeedFullDecode);
	}

	#[test]
	fn eval_set_membership_misses_returns_reject_for_inside() {
		let set = Arc::new(HashSet::from([Value::Number(Number::Int(1))]));
		let root = set_membership(vec!["a".into()], BinaryOperator::Inside, set, false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(99)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn eval_set_membership_inverts_for_not_inside() {
		let set = Arc::new(HashSet::from([Value::Number(Number::Int(1))]));
		let root = set_membership(vec!["a".into()], BinaryOperator::NotInside, set, false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(1)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn eval_set_membership_missing_field_treated_as_value_none() {
		let set = Arc::new(HashSet::from([Value::Number(Number::Int(1))]));
		let root = set_membership(vec!["missing".into()], BinaryOperator::Inside, set, false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(2)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn eval_set_membership_empty_set_rejects_every_row_for_inside() {
		let set = Arc::new(HashSet::<Value>::new());
		let root = set_membership(vec!["a".into()], BinaryOperator::Inside, set, false);
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(5)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
	}

	#[test]
	fn navigate_id_segment_forces_full_decode() {
		let root = PredNode::NavigatePrefix {
			segment: "id".into(),
			child: Box::new(leaf(
				vec!["x".into()],
				BinaryOperator::Equal,
				Value::Number(Number::Int(1)),
				false,
			)),
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let obj =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(2)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::NeedFullDecode);
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
	}

	/// `FusedFlatMapAnd` over a missing scope: each clause reads `Value::None`. With at least
	/// one clause that compares unequal to `None`, the whole AND is provably false.
	#[test]
	fn scoped_missing_intermediate_makes_fused_clauses_provably_false() {
		let inner = FusedFlatClauses::try_new(vec![
			fused_clause(
				b"a".to_vec(),
				BinaryOperator::Equal,
				Value::Number(Number::Int(1)),
				false,
			),
			fused_clause(
				b"b".to_vec(),
				BinaryOperator::Equal,
				Value::Number(Number::Int(2)),
				false,
			),
		])
		.expect("sorted unique");
		let root = PredNode::NavigatePrefix {
			segment: "outer".into(),
			child: Box::new(PredNode::FusedFlatMapAnd {
				at_record_root: false,
				clauses: inner,
			}),
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		// Record has no `outer` key.
		let obj =
			Object::from(BTreeMap::from([(Strand::from("z"), Value::Number(Number::Int(99)))]));
		let rec = wire_record_plain_object(obj);
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
	}

	/// Two-level nested fusion (`outer.inner.{a,b}`): walk through both prefixes, then evaluate
	/// the fused map at the deepest object. A row whose deepest leaf disagrees with one clause
	/// is rejected.
	#[test]
	fn scoped_two_level_nested_fused_rejects_mismatched_inner() {
		let inner_clauses = FusedFlatClauses::try_new(vec![
			fused_clause(
				b"a".to_vec(),
				BinaryOperator::Equal,
				Value::Number(Number::Int(1)),
				false,
			),
			fused_clause(
				b"b".to_vec(),
				BinaryOperator::Equal,
				Value::Number(Number::Int(2)),
				false,
			),
		])
		.expect("sorted unique");
		let root = PredNode::NavigatePrefix {
			segment: "outer".into(),
			child: Box::new(PredNode::NavigatePrefix {
				segment: "inner".into(),
				child: Box::new(PredNode::FusedFlatMapAnd {
					at_record_root: false,
					clauses: inner_clauses,
				}),
			}),
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		// {outer: {inner: {a: 1, b: 99}}} — clause `b == 2` is provably false.
		let inner = Object::from(BTreeMap::from([
			(Strand::from("a"), Value::Number(Number::Int(1))),
			(Strand::from("b"), Value::Number(Number::Int(99))),
		]));
		let middle = Object::from(BTreeMap::from([(Strand::from("inner"), Value::Object(inner))]));
		let outer = Object::from(BTreeMap::from([(Strand::from("outer"), Value::Object(middle))]));
		let rec = wire_record_plain_object(outer);
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
	}

	/// Same shape as above but the row matches both clauses — pre-decode filter cannot prove
	/// false, so the row must be passed through to full decode.
	#[test]
	fn scoped_two_level_nested_fused_passes_matching_inner() {
		let inner_clauses = FusedFlatClauses::try_new(vec![
			fused_clause(
				b"a".to_vec(),
				BinaryOperator::Equal,
				Value::Number(Number::Int(1)),
				false,
			),
			fused_clause(
				b"b".to_vec(),
				BinaryOperator::Equal,
				Value::Number(Number::Int(2)),
				false,
			),
		])
		.expect("sorted unique");
		let root = PredNode::NavigatePrefix {
			segment: "outer".into(),
			child: Box::new(PredNode::NavigatePrefix {
				segment: "inner".into(),
				child: Box::new(PredNode::FusedFlatMapAnd {
					at_record_root: false,
					clauses: inner_clauses,
				}),
			}),
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		let inner = Object::from(BTreeMap::from([
			(Strand::from("a"), Value::Number(Number::Int(1))),
			(Strand::from("b"), Value::Number(Number::Int(2))),
		]));
		let middle = Object::from(BTreeMap::from([(Strand::from("inner"), Value::Object(inner))]));
		let outer = Object::from(BTreeMap::from([(Strand::from("outer"), Value::Object(middle))]));
		let rec = wire_record_plain_object(outer);
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::NeedFullDecode);
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::NeedFullDecode);
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
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
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
	}

	/// Regression: an `outer` object that exists but is missing some of the
	/// fused needles must still be evaluated as if the missing keys read
	/// `Value::None`, so a row like `{outer: {a: 1}}` is rejected by the
	/// predicate `outer.a = 1 AND outer.b = 2` rather than falling through
	/// to full decode.
	#[test]
	fn scoped_partial_object_with_missing_inner_keys_rejects() {
		let clauses = FusedFlatClauses::try_new(vec![
			fused_clause(
				b"a".to_vec(),
				BinaryOperator::Equal,
				Value::Number(Number::Int(1)),
				false,
			),
			fused_clause(
				b"b".to_vec(),
				BinaryOperator::Equal,
				Value::Number(Number::Int(2)),
				false,
			),
		])
		.expect("sorted unique");
		let root = PredNode::NavigatePrefix {
			segment: "outer".into(),
			child: Box::new(PredNode::FusedFlatMapAnd {
				at_record_root: false,
				clauses,
			}),
		};
		let pf = PreDecodeFilter::new(root, TEST_DEPTH_LIMIT);
		// `outer` is present and is an object, but only carries `a`; `b` is
		// absent. The fused evaluator should treat `b` as `Value::None`,
		// reduce the AND to ProvablyFalse, and reject the row.
		let inner =
			Object::from(BTreeMap::from([(Strand::from("a"), Value::Number(Number::Int(1)))]));
		let outer = Object::from(BTreeMap::from([(Strand::from("outer"), Value::Object(inner))]));
		let rec = wire_record_plain_object(outer);
		assert_eq!(pf.apply(&[], &rec), PreDecodeFilterOutcome::Reject);
	}

	/// Exhaustiveness check for [`PreDecodeFilterStatus::explain_text`]: every
	/// `Ineligible` reason variant must produce an `EXPLAIN` string. Adding a
	/// new variant breaks the inner `match`, which forces the implementer to
	/// also extend `explain_text` and the corresponding language tests.
	#[test]
	fn explain_text_covers_every_ineligible_reason() {
		use super::{PreDecodeFilterReason, PreDecodeFilterStatus};

		fn all_reasons() -> impl IntoIterator<Item = PreDecodeFilterReason> {
			// Adding a new variant fails to compile here, prompting a fix.
			let probe: [PreDecodeFilterReason; 3] = [
				PreDecodeFilterReason::UnsupportedPredicate,
				PreDecodeFilterReason::ComputedFields,
				PreDecodeFilterReason::FieldPermissions,
			];
			for r in probe.iter() {
				match r {
					PreDecodeFilterReason::UnsupportedPredicate
					| PreDecodeFilterReason::ComputedFields
					| PreDecodeFilterReason::FieldPermissions => {}
				}
			}
			probe
		}

		for reason in all_reasons() {
			let status = PreDecodeFilterStatus::Ineligible(reason);
			assert!(
				status.explain_text().is_some(),
				"PreDecodeFilterStatus::Ineligible({:?}).explain_text() returned None",
				reason
			);
		}
		assert!(PreDecodeFilterStatus::NotApplicable.explain_text().is_none());
	}
}
