//! Build [`PreDecodeFilter`](super::PreDecodeFilter) trees from synchronous physical predicates.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use memchr::memmem;
use surrealdb_strand::Strand;

use super::streaming::{
	ArrayElementContains, ArrayOverlapsLiteralSet, OverlapMode, StreamingLeafEvaluator,
	SubstringMatch, overlap_streaming_from_set,
};
use super::wire_literal::{LiteralSet, LiteralWire};
use super::{
	FusedFlatClause, FusedFlatClauses, LeafFallback, PreDecodeFilter, PreDecodeFilterReason,
	PreDecodeFilterStatus, PredNode,
};
use crate::exec::operators::scan::pipeline::FieldState;
use crate::exec::permission::PhysicalPermission;
use crate::exec::physical_expr::record_id::PhysicalRecordIdKey;
use crate::exec::physical_expr::{
	ArrayLiteral, BinaryOp, IdiomExpr, ObjectLiteral, PhysicalExpr, RecordIdExpr, SetLiteral,
	SimpleBinaryOp, UnaryOp,
};
use crate::exec::planner::is_simple_binary_eligible;
use crate::expr::operator::{BinaryOperator, PrefixOperator};
use crate::val::object_extract::PathSegment;
use crate::val::{RecordId, RecordIdKey, Value};

/// Compile the physical WHERE predicate into a fused predicate tree for KV-byte inspection.
///
/// Pure shape compile — does not consult [`FieldState`]. Returns
/// [`PreDecodeFilterReason::UnsupportedPredicate`] when the predicate cannot be translated
/// (e.g. uses `Outside` / `Intersects` / `AllEqual` / `AnyEqual`, `MATCHES`, or non-static field
/// paths).
pub(crate) fn compile_predicate_shape(
	pred: &Arc<dyn PhysicalExpr>,
) -> Result<PredNode, PreDecodeFilterReason> {
	let root = compile_expr(pred).ok_or(PreDecodeFilterReason::UnsupportedPredicate)?;
	Ok(fuse_tree(root))
}

/// Walk a [`PredNode`] tree and collect the set of root field names it references.
///
/// Used by [`finalize_pre_decode_filter`] to apply field-state checks only against fields the
/// predicate actually inspects, rather than rejecting the pre-decode filter for the whole table
/// when an unrelated field is computed or has restricted permissions.
fn collect_referenced_root_segments(node: &PredNode, out: &mut BTreeSet<Strand>) {
	match node {
		PredNode::And(xs) | PredNode::Or(xs) => {
			for c in xs {
				collect_referenced_root_segments(c, out);
			}
		}
		PredNode::Not(inner) => collect_referenced_root_segments(inner, out),
		PredNode::Leaf {
			path,
			..
		} => {
			if let Some(seg) = path.first() {
				out.insert(seg.as_strand().clone());
			}
		}
		PredNode::LeafSetMembership {
			path,
			..
		} => {
			if let Some(seg) = path.first() {
				out.insert(seg.as_strand().clone());
			}
		}
		PredNode::LeafStreaming {
			path,
			..
		} => {
			if let Some(seg) = path.first() {
				out.insert(seg.as_strand().clone());
			}
		}
		PredNode::FusedFlatMapAnd {
			at_record_root,
			clauses,
		} => {
			if *at_record_root {
				for c in clauses.iter() {
					if let Ok(s) = std::str::from_utf8(&c.key_utf8) {
						out.insert(Strand::from(s));
					}
				}
			}
			// When `at_record_root == false`, this fused node is nested under a
			// `NavigatePrefix` whose top-level segment was already added to
			// `refs` by the arm below. Adding the full dotted leaves
			// (`outer.a`, `outer.b`, …) here would be redundant: the
			// disqualification check (`field_permission_covers`) keys on the
			// top-level segment, and any `DEFINE FIELD outer.* PERMISSIONS …`
			// permission's idiom starts with `Part::Field("outer")` — so the
			// outer `NavigatePrefix(outer)` already triggers the bail-out.
			// (`filter_fields_by_permission` now enforces nested-path SELECT
			// permissions correctly at decode time — see issue #83 — but the
			// pre-decode filter operates on raw bytes and can't do a
			// per-subpath cut, so disqualifying conservatively at the
			// top-level segment is the right shape.)
		}
		PredNode::NavigatePrefix {
			segment,
			child: _,
		} => {
			out.insert(segment.as_strand().clone());
		}
	}
}

/// Apply field-state rules to a compiled predicate tree and wrap as [`PreDecodeFilter`].
///
/// # Arguments
/// * `root` — predicate tree from [`compile_predicate_shape`].
/// * `field_state` — schema-derived field state (computed fields, per-field SELECT permissions) for
///   the projected fields of the scan's table.
/// * `check_perms` — when `true`, reject if any referenced field has a non-`Allow` SELECT
///   permission. Set to `false` to skip that check (e.g. when the caller is the schema owner or
///   permission enforcement is performed elsewhere).
///
/// # Returns
/// `Ok(pre_decode_filter)` when every referenced root field name is safe to read from raw KV bytes;
/// otherwise [`PreDecodeFilterReason::ComputedFields`] or
/// [`PreDecodeFilterReason::FieldPermissions`].
pub(crate) fn finalize_pre_decode_filter(
	root: &PredNode,
	field_state: &FieldState,
	check_perms: bool,
	depth_limit: u32,
) -> Result<Arc<PreDecodeFilter>, PreDecodeFilterReason> {
	let mut refs: BTreeSet<Strand> = BTreeSet::new();
	collect_referenced_root_segments(root, &mut refs);
	for name in &refs {
		if field_state.computed_fields.iter().any(|c| c.field_name() == name.as_str()) {
			return Err(PreDecodeFilterReason::ComputedFields);
		}
		if check_perms && field_permission_covers(field_state, name.as_str()) {
			return Err(PreDecodeFilterReason::FieldPermissions);
		}
	}
	Ok(Arc::new(PreDecodeFilter::new(root.clone(), depth_limit)))
}

/// Returns `true` if any non-`Allow` field permission could affect a value
/// reached via top-level field `name`.
///
/// Conservative for nested-path permissions (`DEFINE FIELD outer.a …
/// PERMISSIONS …` — see issue #83): the pre-decode filter operates on
/// raw bytes and can't enforce a per-subpath cut, so any reference to a
/// top-level segment that has nested permissions defined under it
/// disqualifies pre-decode filtering.
fn field_permission_covers(field_state: &FieldState, name: &str) -> bool {
	use crate::expr::part::Part;
	field_state.field_permissions.iter().any(|(idiom, perm)| {
		if matches!(perm, PhysicalPermission::Allow) {
			return false;
		}
		// Today the parser produces `Part::Field` as the first part of every
		// `DEFINE FIELD` idiom, so the only interesting comparison is the
		// name match. Any other shape (wildcard / index root, or an empty
		// idiom) is treated as covering by default so a future parser
		// change can't silently let a non-`Allow` permission slip past
		// pre-decode filtering — the post-decode filter still catches it,
		// but we'd rather not pretend the field is safe to read raw.
		match idiom.0.first() {
			Some(Part::Field(f)) => f.as_str() == name,
			_ => true,
		}
	})
}

/// Resolve pre-decode filter status at plan time, given an optional plan-time [`FieldState`].
///
/// * No predicate → [`PreDecodeFilterStatus::NotApplicable`].
/// * Predicate shape uncompilable → [`PreDecodeFilterStatus::Ineligible`].
/// * Field state available and full check (with permissions) passes →
///   [`PreDecodeFilterStatus::Active`].
/// * Field state available, only the permission check fails, but the field-state check without
///   permissions would pass → [`PreDecodeFilterStatus::Deferred`] (final permission check happens
///   at execute time, where the active session permission level is known).
/// * Field state unavailable at plan time → [`PreDecodeFilterStatus::Deferred`] (full check runs at
///   execute time, e.g. for [`DynamicScan`](crate::exec::operators::scan::dynamic::DynamicScan)).
pub(crate) fn pre_decode_filter_status_at_plan_time(
	predicate: Option<&Arc<dyn PhysicalExpr>>,
	field_state: Option<&FieldState>,
	depth_limit: u32,
) -> PreDecodeFilterStatus {
	let Some(pred) = predicate else {
		return PreDecodeFilterStatus::NotApplicable;
	};
	let fused = match compile_predicate_shape(pred) {
		Ok(n) => n,
		Err(r) => return PreDecodeFilterStatus::Ineligible(r),
	};
	match field_state {
		Some(fs) => match finalize_pre_decode_filter(&fused, fs, true, depth_limit) {
			Ok(p) => PreDecodeFilterStatus::Active(p),
			Err(PreDecodeFilterReason::FieldPermissions) => {
				match finalize_pre_decode_filter(&fused, fs, false, depth_limit) {
					Ok(_) => PreDecodeFilterStatus::Deferred(Arc::new(fused)),
					Err(e) => PreDecodeFilterStatus::Ineligible(e),
				}
			}
			Err(e) => PreDecodeFilterStatus::Ineligible(e),
		},
		None => PreDecodeFilterStatus::Deferred(Arc::new(fused)),
	}
}

/// Resolve the optional pre-decode filter for a KV scan at execution time.
///
/// Returns the plan-time [`PreDecodeFilter`] for [`PreDecodeFilterStatus::Active`], finalises a
/// [`PreDecodeFilterStatus::Deferred`] tree against the runtime [`FieldState`], and yields
/// [`None`] for [`PreDecodeFilterStatus::NotApplicable`] / [`PreDecodeFilterStatus::Ineligible`].
pub(crate) fn pre_decode_filter_for_execute(
	status: &PreDecodeFilterStatus,
	field_state: &FieldState,
	check_perms: bool,
	depth_limit: u32,
) -> Option<Arc<PreDecodeFilter>> {
	match status {
		PreDecodeFilterStatus::NotApplicable | PreDecodeFilterStatus::Ineligible(_) => None,
		PreDecodeFilterStatus::Active(p) => Some(Arc::clone(p)),
		PreDecodeFilterStatus::Deferred(node) => {
			finalize_pre_decode_filter(node.as_ref(), field_state, check_perms, depth_limit).ok()
		}
	}
}

/// Operators the structural KV pre-decode filter cannot compile (geometry, list-equality
/// shorthands).
fn unsupported_containment_like(op: &BinaryOperator) -> bool {
	matches!(
		op,
		BinaryOperator::Outside
			| BinaryOperator::Intersects
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual
	)
}

/// Elements excluded from a compile-time IN hashset — [`Value::equal`] may diverge from
/// `Hash`/`Eq`, or semantics are not a flat membership check.
fn literal_hashset_element_safe(v: &Value) -> bool {
	!matches!(
		v,
		Value::None | Value::Regex(_) | Value::Range(_) | Value::Geometry(_) | Value::Closure(_)
	)
}

/// Build a [`HashSet`] from a literal `Array` or `Set` RHS for `INSIDE` / `NOTINSIDE`, or `None` if
/// the shape is not a collection or any element fails [`literal_hashset_element_safe`].
fn try_build_inside_literal_hashset(literal: &Value) -> Option<Arc<HashSet<Value>>> {
	let mut set = HashSet::new();
	match literal {
		Value::Array(a) => {
			for v in a.iter() {
				if !literal_hashset_element_safe(v) {
					return None;
				}
				set.insert(v.clone());
			}
		}
		Value::Set(s) => {
			for v in s.iter() {
				if !literal_hashset_element_safe(v) {
					return None;
				}
				set.insert(v.clone());
			}
		}
		_ => return None,
	}
	Some(Arc::new(set))
}

/// Try to compile a containment / overlap predicate into [`PredNode::LeafStreaming`].
fn try_compile_leaf_streaming(
	path: Vec<PathSegment>,
	op: &BinaryOperator,
	literal: &Value,
	reversed: bool,
) -> Option<PredNode> {
	if reversed {
		return None;
	}
	let fallback = LeafFallback {
		op: op.clone(),
		literal: literal.clone(),
		reversed: false,
	};
	match op {
		BinaryOperator::Contain | BinaryOperator::NotContain => {
			if let Value::String(s) = literal {
				let finder = memmem::Finder::new(s.as_str().as_bytes()).into_owned();
				let negated = matches!(op, BinaryOperator::NotContain);
				let evaluator: Arc<dyn StreamingLeafEvaluator> = Arc::new(SubstringMatch {
					finder,
					negated,
				});
				Some(PredNode::LeafStreaming {
					path,
					evaluator,
					fallback,
				})
			} else {
				let negated = matches!(op, BinaryOperator::NotContain);
				let needle_wire = Arc::new(LiteralWire::from_value(literal));
				let evaluator: Arc<dyn StreamingLeafEvaluator> = Arc::new(ArrayElementContains {
					needle: Arc::new(literal.clone()),
					needle_wire,
					negated,
				});
				Some(PredNode::LeafStreaming {
					path,
					evaluator,
					fallback,
				})
			}
		}
		BinaryOperator::ContainAny | BinaryOperator::ContainNone | BinaryOperator::ContainAll => {
			let set = try_build_inside_literal_hashset(literal)?;
			let tables = overlap_streaming_from_set(set.as_ref());
			let mode = match op {
				BinaryOperator::ContainAny => OverlapMode::Any,
				BinaryOperator::ContainNone => OverlapMode::None,
				BinaryOperator::ContainAll => OverlapMode::All,
				_ => return None,
			};
			let evaluator: Arc<dyn StreamingLeafEvaluator> = Arc::new(ArrayOverlapsLiteralSet {
				literal_to_idx: tables.literal_to_idx,
				wire_to_idx: tables.wire_to_idx,
				literal_set: tables.literal_set,
				mode,
			});
			Some(PredNode::LeafStreaming {
				path,
				evaluator,
				fallback,
			})
		}
		_ => None,
	}
}

/// Decode a plan-time [`PhysicalRecordIdKey`] when it uses only static literals, collections, and
/// nested [`RecordIdExpr`] keys (no `rand()` / ulid / uuid generators or range bounds).
fn try_static_record_id_key(key: &PhysicalRecordIdKey) -> Option<RecordIdKey> {
	match key {
		PhysicalRecordIdKey::Number(n) => Some(RecordIdKey::Number(*n)),
		PhysicalRecordIdKey::String(s) => Some(RecordIdKey::String(s.clone())),
		PhysicalRecordIdKey::Uuid(u) => Some(RecordIdKey::Uuid(*u)),
		PhysicalRecordIdKey::Generate(_) => None,
		PhysicalRecordIdKey::Array(elements) => {
			let mut values = Vec::with_capacity(elements.len());
			for elem in elements {
				values.push(try_static_literal_value(elem)?);
			}
			Some(RecordIdKey::Array(crate::val::Array::from(values)))
		}
		PhysicalRecordIdKey::Object(entries) => {
			let mut obj = crate::val::Object::default();
			for (k, e) in entries {
				obj.insert(k.clone(), try_static_literal_value(e)?);
			}
			Some(RecordIdKey::Object(obj))
		}
		PhysicalRecordIdKey::Range {
			..
		} => None,
	}
}

/// Fold planner [`ArrayLiteral`] / [`SetLiteral`] / [`ObjectLiteral`] trees into a [`Value`] when
/// every leaf is a static [`Literal`](crate::exec::physical_expr::Literal) (or nested literal
/// collection). `field INSIDE [1, 2]` lowers to an [`ArrayLiteral`] RHS, so
/// [`PhysicalExpr::try_literal`] alone is insufficient for the pre-decode compile path.
fn try_static_literal_value(expr: &Arc<dyn PhysicalExpr>) -> Option<Value> {
	if let Some(v) = expr.try_literal() {
		return Some(v.clone());
	}
	let e = expr.as_ref();
	if let Some(arr) = e.downcast_ref::<ArrayLiteral>() {
		let mut out = Vec::with_capacity(arr.elements.len());
		for elem in &arr.elements {
			out.push(try_static_literal_value(elem)?);
		}
		return Some(Value::Array(crate::val::Array::from(out)));
	}
	if let Some(set) = e.downcast_ref::<SetLiteral>() {
		let mut acc = crate::val::Set::new();
		for elem in &set.elements {
			acc.insert(try_static_literal_value(elem)?);
		}
		return Some(Value::Set(acc));
	}
	if let Some(obj) = e.downcast_ref::<ObjectLiteral>() {
		let mut map = BTreeMap::new();
		for (k, v_expr) in &obj.entries {
			map.insert(k.clone(), try_static_literal_value(v_expr)?);
		}
		return Some(Value::Object(crate::val::Object::from(map)));
	}
	if let Some(rid) = e.downcast_ref::<RecordIdExpr>() {
		let key = try_static_record_id_key(&rid.key)?;
		return Some(Value::RecordId(RecordId {
			table: rid.table.clone(),
			key,
		}));
	}
	None
}

fn compile_expr(expr: &Arc<dyn PhysicalExpr>) -> Option<PredNode> {
	let expr: &dyn PhysicalExpr = expr.as_ref();
	if let Some(sb) = expr.downcast_ref::<SimpleBinaryOp>() {
		return compile_simple_binary(sb);
	}
	if let Some(b) = expr.downcast_ref::<BinaryOp>() {
		return compile_binary(b);
	}
	if let Some(u) = expr.downcast_ref::<UnaryOp>() {
		return compile_unary(u);
	}
	None
}

fn compile_simple_binary(sb: &SimpleBinaryOp) -> Option<PredNode> {
	if !is_simple_binary_eligible(&sb.op) || unsupported_containment_like(&sb.op) {
		return None;
	}
	let path: Vec<PathSegment> = vec![PathSegment::from(sb.field_name.as_str())];
	if !sb.reversed
		&& matches!(sb.op, BinaryOperator::Inside | BinaryOperator::NotInside)
		&& let Some(set) = try_build_inside_literal_hashset(&sb.literal)
	{
		let literal_set = Arc::new(LiteralSet::from_set(set.as_ref()));
		return Some(PredNode::LeafSetMembership {
			path,
			op: sb.op.clone(),
			set,
			literal_set,
			reversed: false,
		});
	}
	if let Some(n) = try_compile_leaf_streaming(path.clone(), &sb.op, &sb.literal, sb.reversed) {
		return Some(n);
	}
	let literal_wire = Arc::new(LiteralWire::from_value(&sb.literal));
	Some(PredNode::Leaf {
		path,
		op: sb.op.clone(),
		literal: sb.literal.clone(),
		literal_wire,
		reversed: sb.reversed,
	})
}

fn compile_binary(b: &BinaryOp) -> Option<PredNode> {
	match &b.op {
		BinaryOperator::And => {
			Some(PredNode::And(vec![compile_expr(&b.left)?, compile_expr(&b.right)?]))
		}
		BinaryOperator::Or => {
			Some(PredNode::Or(vec![compile_expr(&b.left)?, compile_expr(&b.right)?]))
		}
		_ => {
			if !is_simple_binary_eligible(&b.op) || unsupported_containment_like(&b.op) {
				return None;
			}
			// Try the dedicated `array::len(field) <op> N` shape first —
			// it bypasses every field-value decode by reading just the
			// array's prologue length.
			if let Some(n) = try_compile_array_len_pair(&b.left, &b.right, false, &b.op) {
				return Some(n);
			}
			if let Some(n) = try_compile_array_len_pair(&b.right, &b.left, true, &b.op) {
				return Some(n);
			}
			compile_field_lit_pair(&b.left, &b.right, false, &b.op)
				.or_else(|| compile_field_lit_pair(&b.right, &b.left, true, &b.op))
		}
	}
}

/// Recognise `array::len(field) <op> N` and emit a `LeafStreaming` over
/// [`ArrayLenCompare`] (skips every Strand / Value decode — reads the
/// array's varint length prologue and compares).
///
/// Returns `None` for any other shape; the caller falls back to
/// [`compile_field_lit_pair`].
fn try_compile_array_len_pair(
	function_side: &Arc<dyn PhysicalExpr>,
	literal_side: &Arc<dyn PhysicalExpr>,
	reversed: bool,
	op: &BinaryOperator,
) -> Option<PredNode> {
	use super::streaming::ArrayLenCompare;
	use crate::exec::physical_expr::function::BuiltinFunctionExec;
	use crate::val::Number;

	let func = function_side.as_ref().downcast_ref::<BuiltinFunctionExec>()?;
	if func.name != "array::len" || func.arguments.len() != 1 {
		return None;
	}
	let path = resolve_static_path(&func.arguments[0])?;
	let literal = try_static_literal_value(literal_side)?;
	let Value::Number(n) = literal.clone() else {
		return None;
	};
	let expected_len: i64 = match n {
		Number::Int(i) => i,
		Number::Float(f)
			if f.fract() == 0.0 && (i64::MIN as f64..=i64::MAX as f64).contains(&f) =>
		{
			f as i64
		}
		_ => return None,
	};
	// `array::len(x) > N` and `N < array::len(x)` are the same predicate;
	// when `reversed`, flip the operator so the evaluator's comparison
	// stays "actual <op> expected_len".
	let effective_op = if reversed {
		match op {
			BinaryOperator::LessThan => BinaryOperator::MoreThan,
			BinaryOperator::LessThanEqual => BinaryOperator::MoreThanEqual,
			BinaryOperator::MoreThan => BinaryOperator::LessThan,
			BinaryOperator::MoreThanEqual => BinaryOperator::LessThanEqual,
			// Equal / NotEqual / ExactEqual are commutative.
			BinaryOperator::Equal | BinaryOperator::NotEqual | BinaryOperator::ExactEqual => {
				op.clone()
			}
			_ => return None,
		}
	} else {
		match op {
			BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual => op.clone(),
			_ => return None,
		}
	};
	let fallback = LeafFallback {
		op: op.clone(),
		literal,
		reversed,
	};
	let evaluator: Arc<dyn StreamingLeafEvaluator> = Arc::new(ArrayLenCompare {
		expected_len,
		op: effective_op,
	});
	Some(PredNode::LeafStreaming {
		path,
		evaluator,
		fallback,
	})
}

fn compile_field_lit_pair(
	field_side: &Arc<dyn PhysicalExpr>,
	literal_side: &Arc<dyn PhysicalExpr>,
	reversed: bool,
	op: &BinaryOperator,
) -> Option<PredNode> {
	let path = resolve_static_path(field_side)?;
	let literal = try_static_literal_value(literal_side)?;
	if !reversed
		&& matches!(op, BinaryOperator::Inside | BinaryOperator::NotInside)
		&& let Some(set) = try_build_inside_literal_hashset(&literal)
	{
		let literal_set = Arc::new(LiteralSet::from_set(set.as_ref()));
		return Some(PredNode::LeafSetMembership {
			path,
			op: op.clone(),
			set,
			literal_set,
			reversed: false,
		});
	}
	if let Some(n) = try_compile_leaf_streaming(path.clone(), op, &literal, reversed) {
		return Some(n);
	}
	let literal_wire = Arc::new(LiteralWire::from_value(&literal));
	Some(PredNode::Leaf {
		path,
		op: op.clone(),
		literal,
		literal_wire,
		reversed,
	})
}

fn resolve_static_path(e: &Arc<dyn PhysicalExpr>) -> Option<Vec<PathSegment>> {
	let e: &dyn PhysicalExpr = e.as_ref();
	if let Some(id) = e.downcast_ref::<IdiomExpr>() {
		id.try_static_object_field_path()
			.map(|parts| parts.into_iter().map(PathSegment::from).collect())
	} else {
		let s = e.try_simple_field()?;
		Some(vec![PathSegment::from(s)])
	}
}

fn compile_unary(u: &UnaryOp) -> Option<PredNode> {
	match u.op {
		PrefixOperator::Not => {
			let inner = compile_expr(&u.expr)?;
			Some(PredNode::Not(Box::new(inner)))
		}
		_ => None,
	}
}

// --- fusion pass ---

fn fuse_tree(node: PredNode) -> PredNode {
	match node {
		PredNode::And(children) => {
			let parts: Vec<PredNode> = children.into_iter().map(fuse_tree).collect();
			fuse_and_combine(parts)
		}
		PredNode::Or(children) => {
			let parts: Vec<PredNode> = children.into_iter().map(fuse_tree).collect();
			fuse_or_combine(parts)
		}
		PredNode::Not(inner) => PredNode::Not(Box::new(fuse_tree(*inner))),
		other => other,
	}
}

fn flatten_and_into(acc: &mut Vec<PredNode>, node: PredNode) {
	match node {
		PredNode::And(xs) => {
			for x in xs {
				flatten_and_into(acc, x);
			}
		}
		x => acc.push(x),
	}
}

fn fuse_and_combine(children: Vec<PredNode>) -> PredNode {
	let mut bucket = Vec::new();
	for c in children {
		flatten_and_into(&mut bucket, c);
	}
	partition_and_fuse(bucket)
}

/// Flatten nested [`PredNode::Or`] into a single flat list of disjunction arms.
///
/// Left-associative parsing nests `a OR b OR c` as `Or(Or(a, b), c)`, and bottom-up
/// fusion may have already collapsed an inner `Or` into a [`PredNode::LeafSetMembership`].
/// Flattening lets [`fuse_or_combine`] see every arm at once so same-field equalities
/// across nesting levels fuse into one set.
fn flatten_or_into(acc: &mut Vec<PredNode>, node: PredNode) {
	match node {
		PredNode::Or(xs) => {
			for x in xs {
				flatten_or_into(acc, x);
			}
		}
		x => acc.push(x),
	}
}

/// Whether `path` targets the synthetic record-root `id` field (`["id"]`).
///
/// `id` is not stored in the encoded record body — it is derived from the KV key,
/// which only [`PreDecodeFilter::eval_leaf`] knows how to do (its id-key fast path).
/// [`PreDecodeFilter::eval_set_membership`] reads from the body, so it would see
/// `id` as missing and wrongly reject every row. Equality arms on `id` must
/// therefore stay as [`PredNode::Leaf`] rather than fuse into a set — mirroring the
/// `id`-leaf carve-out in [`partition_and_fuse`].
fn is_synthetic_id_path(path: &[PathSegment]) -> bool {
	path.len() == 1 && path[0].as_str() == "id"
}

/// Fuse a disjunction's arms, rewriting same-field equality chains into a single
/// wire-fast [`PredNode::LeafSetMembership`].
///
/// `a = X OR a = Y` is semantically identical to `a IN [X, Y]` under SurrealQL's
/// loose equality (`=` / [`operate::equal`]), which matches the `set.contains` probe
/// used by `eval_set_membership`. Collapsing the chain pays one field descent + one
/// hash probe per row instead of one descent + comparison per arm.
///
/// Conservative — an arm only joins a fused group when it is:
/// - a [`PredNode::Leaf`] with `op == Equal`, `reversed == false`, a hashset-safe literal (see
///   [`literal_hashset_element_safe`]), and a non-`id` path (see [`is_synthetic_id_path`]); or
/// - an existing [`PredNode::LeafSetMembership`] with `op == Inside`, `reversed == false`, and a
///   non-`id` path (absorbing an `a IN [..]` arm into the same-field group).
///
/// Everything else — `ExactEqual` (type-strict, diverges from the loose set path),
/// `NotEqual`, `NotInside`, reversed arms, `id` paths, different fields, and non-leaf
/// nodes — passes through untouched.
fn fuse_or_combine(children: Vec<PredNode>) -> PredNode {
	let mut bucket = Vec::new();
	for c in children {
		flatten_or_into(&mut bucket, c);
	}

	// A fuseable group of same-field arms. `arms` keeps each contributing node so a
	// group that ends up with a single arm can be emitted verbatim (preserving e.g.
	// an original `a IN [..]` arm rather than degrading it to a one-element `Leaf`).
	struct Group {
		path: Vec<PathSegment>,
		set: HashSet<Value>,
		arms: Vec<PredNode>,
	}
	// Slots preserve the original arm order in the output disjunction.
	enum Slot {
		Group(usize),
		Passthrough(PredNode),
	}

	let mut groups: Vec<Group> = Vec::new();
	let mut slots: Vec<Slot> = Vec::new();

	'arm: for arm in bucket {
		// Read the path + literals this arm would contribute, without moving it: a
		// non-fuseable arm must pass through unchanged.
		let contribution: Option<(Vec<PathSegment>, Vec<Value>)> = match &arm {
			PredNode::Leaf {
				path,
				op: BinaryOperator::Equal,
				literal,
				reversed: false,
				..
			} if literal_hashset_element_safe(literal) && !is_synthetic_id_path(path) => {
				Some((path.clone(), vec![literal.clone()]))
			}
			PredNode::LeafSetMembership {
				path,
				op: BinaryOperator::Inside,
				set,
				reversed: false,
				..
			} if !is_synthetic_id_path(path) => Some((path.clone(), set.iter().cloned().collect())),
			_ => None,
		};

		let Some((path, literals)) = contribution else {
			slots.push(Slot::Passthrough(arm));
			continue;
		};

		for g in groups.iter_mut() {
			if g.path == path {
				for v in literals {
					g.set.insert(v);
				}
				g.arms.push(arm);
				continue 'arm;
			}
		}

		let idx = groups.len();
		let mut set = HashSet::new();
		for v in literals {
			set.insert(v);
		}
		groups.push(Group {
			path,
			set,
			arms: vec![arm],
		});
		slots.push(Slot::Group(idx));
	}

	// Reassemble in stable order. A group with 2+ contributing arms collapses into
	// one `LeafSetMembership`; a lone-arm group is emitted exactly as it came in.
	let mut out: Vec<PredNode> = Vec::with_capacity(slots.len());
	for slot in slots {
		match slot {
			Slot::Passthrough(n) => out.push(n),
			Slot::Group(idx) => {
				let g = &mut groups[idx];
				if g.arms.len() >= 2 {
					let set = std::mem::take(&mut g.set);
					let literal_set = Arc::new(LiteralSet::from_set(&set));
					out.push(PredNode::LeafSetMembership {
						path: std::mem::take(&mut g.path),
						op: BinaryOperator::Inside,
						set: Arc::new(set),
						literal_set,
						reversed: false,
					});
				} else {
					out.push(g.arms.pop().expect("group has >=1 arm"));
				}
			}
		}
	}

	if out.len() == 1 {
		out.pop().expect("len checked")
	} else {
		PredNode::Or(out)
	}
}

fn partition_and_fuse(bucket: Vec<PredNode>) -> PredNode {
	let mut id_leaves = Vec::new();
	let mut root_fusible = Vec::new();
	let mut multi_leaves = Vec::new();
	let mut rest = Vec::new();

	for n in bucket {
		match n {
			PredNode::LeafSetMembership {
				..
			} => rest.push(n),
			PredNode::LeafStreaming {
				..
			} => rest.push(n),
			PredNode::Leaf {
				ref path,
				..
			} if path.len() == 1 && path[0].as_str() == "id" => id_leaves.push(n),
			PredNode::Leaf {
				ref path,
				..
			} if path.len() == 1 => root_fusible.push(n),
			PredNode::Leaf {
				ref path,
				..
			} if path.len() > 1 => multi_leaves.push(n),
			other => rest.push(other),
		}
	}

	let mut out: Vec<PredNode> = Vec::new();
	out.append(&mut id_leaves);

	if root_fusible.len() >= 2 {
		if let Some(fused) = build_root_fused_flat_map(&root_fusible) {
			out.push(fused);
		} else {
			out.append(&mut root_fusible);
		}
	} else {
		out.append(&mut root_fusible);
	}

	if !multi_leaves.is_empty() {
		out.push(fuse_multi_segment_leaves(multi_leaves));
	}

	out.append(&mut rest);

	normalize_and(out)
}

fn leaf_to_spec(
	path: Vec<PathSegment>,
	op: BinaryOperator,
	literal: Value,
	reversed: bool,
) -> Result<FuseLeaf, ()> {
	if path.is_empty() {
		return Err(());
	}
	Ok(FuseLeaf {
		path,
		op,
		literal,
		reversed,
	})
}

#[derive(Clone)]
struct FuseLeaf {
	path: Vec<PathSegment>,
	op: BinaryOperator,
	literal: Value,
	reversed: bool,
}

fn build_root_fused_flat_map(leaves: &[PredNode]) -> Option<PredNode> {
	let mut specs = Vec::new();
	for n in leaves {
		if let PredNode::Leaf {
			path,
			op,
			literal,
			literal_wire: _,
			reversed,
		} = n
		{
			specs.push(leaf_to_spec(path.clone(), op.clone(), literal.clone(), *reversed).ok()?);
		} else {
			return None;
		}
	}
	let clauses = flat_clauses_from_specs(&specs)?;
	Some(PredNode::FusedFlatMapAnd {
		at_record_root: true,
		clauses,
	})
}

/// Returns a [`FusedFlatClauses`] (strictly ascending by UTF‑8 `key_utf8`,
/// unique keys) from the supplied root-leaf specs. Multiple specs with the
/// same `key_utf8` are folded into a single [`FusedFlatClause`] holding all
/// of their `(op, literal, reversed)` triples in `ops` — input order is
/// preserved per key. Returns `None` if any spec has a non-single-segment
/// path; the `BTreeMap` guarantees the sortedness/dedup invariant for the
/// success case.
fn flat_clauses_from_specs(specs: &[FuseLeaf]) -> Option<FusedFlatClauses> {
	use super::FlatClauseOp;

	let mut map: BTreeMap<Vec<u8>, Vec<FlatClauseOp>> = BTreeMap::new();
	for s in specs {
		if s.path.len() != 1 {
			return None;
		}
		let literal_wire = Arc::new(LiteralWire::from_value(&s.literal));
		map.entry(s.path[0].as_bytes().to_vec()).or_default().push(FlatClauseOp {
			op: s.op.clone(),
			literal: s.literal.clone(),
			literal_wire,
			reversed: s.reversed,
		});
	}
	let inner: Vec<FusedFlatClause> = map
		.into_iter()
		.map(|(key_utf8, ops)| FusedFlatClause {
			key_utf8,
			ops,
		})
		.collect();
	FusedFlatClauses::try_new(inner)
}

fn fuse_multi_segment_leaves(leaves: Vec<PredNode>) -> PredNode {
	let mut specs: Vec<FuseLeaf> = Vec::new();
	for n in leaves {
		match n {
			PredNode::Leaf {
				path,
				op,
				literal,
				literal_wire: _,
				reversed,
			} => {
				if let Ok(fl) = leaf_to_spec(path, op, literal, reversed) {
					specs.push(fl);
				}
			}
			other => {
				unreachable!(
					"fuse_multi_segment_leaves expects only multi-segment leaves, got {:?}",
					other
				);
			}
		}
	}
	cluster_multi_specs(specs)
}

fn cluster_multi_specs(mut specs: Vec<FuseLeaf>) -> PredNode {
	if specs.is_empty() {
		return PredNode::And(Vec::new());
	}
	if specs.len() == 1 {
		let s = specs.remove(0);
		let literal_wire = Arc::new(LiteralWire::from_value(&s.literal));
		return PredNode::Leaf {
			path: s.path,
			op: s.op,
			literal: s.literal,
			literal_wire,
			reversed: s.reversed,
		};
	}
	specs.sort_by(|a, b| a.path.cmp(&b.path));
	let seg0 = specs[0].path[0].clone();
	let mut run_end = 1usize;
	while run_end < specs.len() && specs[run_end].path[0] == seg0 {
		run_end += 1;
	}

	let run_specs = specs[..run_end].to_vec();
	let rest_specs = if run_end < specs.len() {
		specs[run_end..].to_vec()
	} else {
		Vec::new()
	};

	let tails: Vec<FuseLeaf> = run_specs
		.into_iter()
		.map(|mut l| {
			l.path.remove(0);
			l
		})
		.collect();

	let inner = fuse_tails_under_segment(&tails);

	let run_node = PredNode::NavigatePrefix {
		segment: seg0,
		child: Box::new(inner),
	};

	if rest_specs.is_empty() {
		run_node
	} else {
		normalize_and(vec![run_node, cluster_multi_specs(rest_specs)])
	}
}

/// `tails` are paths relative to the navigated segment; empty [`FuseLeaf::path`] means the leaf
/// compares at the current anchor (one conjunct ends exactly here while another continues deeper).
fn fuse_tails_under_segment(tails: &[FuseLeaf]) -> PredNode {
	if tails.is_empty() {
		return PredNode::And(Vec::new());
	}
	if tails.len() == 1 {
		let t = &tails[0];
		let literal_wire = Arc::new(LiteralWire::from_value(&t.literal));
		return PredNode::Leaf {
			path: t.path.clone(),
			op: t.op.clone(),
			literal: t.literal.clone(),
			literal_wire,
			reversed: t.reversed,
		};
	}

	let non_empty: Vec<FuseLeaf> = tails.iter().filter(|t| !t.path.is_empty()).cloned().collect();
	let has_empty = tails.iter().any(|t| t.path.is_empty());

	// One leaf ends at this anchor (`path == []`) while another continues — cannot recurse into
	// [`cluster_multi_specs`] with empty first segments (would index `path[0]` out of bounds).
	if has_empty && !non_empty.is_empty() {
		let mut parts: Vec<PredNode> = Vec::new();
		for t in tails.iter().filter(|t| t.path.is_empty()) {
			let literal_wire = Arc::new(LiteralWire::from_value(&t.literal));
			parts.push(PredNode::Leaf {
				path: Vec::new(),
				op: t.op.clone(),
				literal: t.literal.clone(),
				literal_wire,
				reversed: t.reversed,
			});
		}
		parts.push(fuse_tails_under_segment(&non_empty));
		return normalize_and(parts);
	}

	if tails.iter().all(|t| t.path.is_empty()) {
		let parts: Vec<PredNode> = tails
			.iter()
			.map(|t| {
				let literal_wire = Arc::new(LiteralWire::from_value(&t.literal));
				PredNode::Leaf {
					path: Vec::new(),
					op: t.op.clone(),
					literal: t.literal.clone(),
					literal_wire,
					reversed: t.reversed,
				}
			})
			.collect();
		return normalize_and(parts);
	}

	debug_assert!(
		tails.iter().all(|t| !t.path.is_empty()),
		"fuse: empty path suffix after stripping shared prefix",
	);

	if tails.len() >= 2
		&& tails.iter().all(|t| t.path.len() == 1)
		&& let Some(clauses) = flat_clauses_from_specs(tails)
	{
		return PredNode::FusedFlatMapAnd {
			at_record_root: false,
			clauses,
		};
	}

	cluster_multi_specs(tails.to_vec())
}

fn normalize_and(mut xs: Vec<PredNode>) -> PredNode {
	xs.retain(|n| !matches!(n, PredNode::And(v) if v.is_empty()));
	if xs.is_empty() {
		PredNode::And(Vec::new())
	} else if xs.len() == 1 {
		xs.swap_remove(0)
	} else {
		PredNode::And(xs)
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;
	use std::sync::Arc;

	use super::{PathSegment, PredNode, compile_expr, compile_predicate_shape};
	use crate::exec::parts::field::FieldPart;
	use crate::exec::physical_expr::{
		ArrayLiteral, BinaryOp, IdiomExpr, Literal, PhysicalExpr, SimpleBinaryOp, UnaryOp,
	};
	use crate::expr::operator::{BinaryOperator, PrefixOperator};
	use crate::val::{Number, Object, Value};

	fn sb(field: &str, op: BinaryOperator, literal: Value) -> Arc<dyn crate::exec::PhysicalExpr> {
		Arc::new(SimpleBinaryOp {
			field_name: field.into(),
			op,
			literal,
			reversed: false,
		})
	}

	#[test]
	fn compile_simple_binary_flat_key_keeps_dots() {
		let p = sb("a.b", BinaryOperator::Equal, Value::Number(Number::Int(5)));
		let n = compile_predicate_shape(&p).expect("compile");
		match n {
			PredNode::Leaf {
				path,
				..
			} => assert_eq!(path, vec![PathSegment::from("a.b")]),
			other => panic!("expected Leaf, got {other:?}"),
		}
	}

	#[test]
	fn compile_simple_binary_reversed() {
		let p = Arc::new(SimpleBinaryOp {
			field_name: "x".into(),
			op: BinaryOperator::Equal,
			literal: Value::Number(Number::Int(1)),
			reversed: true,
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		match n {
			PredNode::Leaf {
				reversed,
				..
			} => assert!(reversed),
			other => panic!("expected Leaf, got {other:?}"),
		}
	}

	#[test]
	fn compile_outside_intersects_all_equal_any_equal_still_unsupported() {
		for op in [
			BinaryOperator::Outside,
			BinaryOperator::Intersects,
			BinaryOperator::AllEqual,
			BinaryOperator::AnyEqual,
		] {
			let p = Arc::new(SimpleBinaryOp {
				field_name: "a".into(),
				op: op.clone(),
				literal: Value::Number(Number::Int(1)),
				reversed: false,
			}) as Arc<dyn crate::exec::PhysicalExpr>;
			assert!(compile_predicate_shape(&p).is_err(), "expected unsupported for {op:?}");
		}
	}

	#[test]
	fn compile_contain_field_literal_pair_via_binary_op() {
		let left = Arc::new(IdiomExpr::new(
			"a".into(),
			None,
			vec![Arc::new(FieldPart {
				name: "a".into(),
			}) as Arc<dyn PhysicalExpr>],
		)) as Arc<dyn PhysicalExpr>;
		let p = Arc::new(BinaryOp {
			left,
			op: BinaryOperator::Contain,
			right: Arc::new(Literal(Value::Number(Number::Int(1)))),
		}) as Arc<dyn PhysicalExpr>;
		match compile_predicate_shape(&p).expect("compile") {
			PredNode::LeafStreaming {
				..
			} => {}
			other => panic!("expected LeafStreaming, got {other:?}"),
		}
	}

	#[test]
	fn compile_in_literal_array_builds_set_membership() {
		let arr = Value::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))]);
		let p = sb("v", BinaryOperator::Inside, arr);
		let n = compile_predicate_shape(&p).expect("compile");
		let PredNode::LeafSetMembership {
			op,
			set,
			reversed,
			path,
			..
		} = n
		else {
			panic!("expected LeafSetMembership, got {n:?}");
		};
		assert_eq!(path, vec![PathSegment::from("v")]);
		assert_eq!(op, BinaryOperator::Inside);
		assert!(!reversed);
		assert!(set.contains(&Value::Number(Number::Int(1))));
		assert_eq!(set.len(), 2);
	}

	#[test]
	fn compile_in_planner_array_literal_rhs_builds_set_membership() {
		let left = Arc::new(IdiomExpr::new(
			"v".into(),
			None,
			vec![Arc::new(FieldPart {
				name: "v".into(),
			}) as Arc<dyn PhysicalExpr>],
		)) as Arc<dyn PhysicalExpr>;
		let rhs = Arc::new(ArrayLiteral {
			elements: vec![
				Arc::new(Literal(Value::Number(Number::Int(1)))),
				Arc::new(Literal(Value::Number(Number::Int(2)))),
			],
		}) as Arc<dyn PhysicalExpr>;
		let p = Arc::new(BinaryOp {
			left,
			op: BinaryOperator::Inside,
			right: rhs,
		}) as Arc<dyn PhysicalExpr>;
		assert!(
			matches!(
				compile_predicate_shape(&p).expect("compile"),
				PredNode::LeafSetMembership { .. }
			),
			"ArrayLiteral RHS should fold to Value::Array and use hashset path"
		);
	}

	#[test]
	fn compile_in_literal_with_regex_falls_back_to_leaf() {
		let re = crate::val::Regex::from_str("/x/").expect("regex");
		let arr = Value::from(vec![Value::Number(Number::Int(1)), Value::Regex(re)]);
		let p = sb("v", BinaryOperator::Inside, arr);
		match compile_predicate_shape(&p).expect("compile") {
			PredNode::Leaf {
				op,
				..
			} => assert_eq!(op, BinaryOperator::Inside),
			other => panic!("expected Leaf fallback, got {other:?}"),
		}
	}

	#[test]
	fn compile_in_literal_with_range_rhs_falls_back_to_leaf() {
		use std::ops::Bound;

		use crate::val::Range;
		let range_lit = Value::Range(Box::new(Range {
			start: Bound::Included(Value::Number(Number::Int(1))),
			end: Bound::Included(Value::Number(Number::Int(10))),
		}));
		let p = sb("v", BinaryOperator::Inside, range_lit.clone());
		match compile_predicate_shape(&p).expect("compile") {
			PredNode::Leaf {
				literal,
				..
			} => assert_eq!(literal, range_lit),
			other => panic!("expected Leaf, got {other:?}"),
		}
	}

	#[test]
	fn compile_in_literal_with_nested_object_keeps_set_membership() {
		use std::collections::BTreeMap;

		use surrealdb_strand::Strand;
		let inner = Value::Object(Object::from(BTreeMap::from([(
			Strand::from("k"),
			Value::Number(Number::Int(1)),
		)])));
		let arr = Value::from(vec![inner]);
		let p = sb("v", BinaryOperator::Inside, arr);
		assert!(
			matches!(
				compile_predicate_shape(&p).expect("compile"),
				PredNode::LeafSetMembership { .. }
			),
			"nested object element should be hash eligible"
		);
	}

	#[test]
	fn compile_in_with_mixed_number_variants_collapses_in_set() {
		use rust_decimal::Decimal;
		let arr = Value::from(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Float(1.0)),
			Value::Number(Number::Decimal(Decimal::from(1))),
		]);
		let p = sb("v", BinaryOperator::Inside, arr);
		let n = compile_predicate_shape(&p).expect("compile");
		let PredNode::LeafSetMembership {
			set,
			..
		} = n
		else {
			panic!("expected LeafSetMembership");
		};
		assert_eq!(set.len(), 1);
	}

	#[test]
	fn compile_in_reversed_literal_left_falls_back_to_leaf() {
		let arr = Value::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))]);
		let p = Arc::new(SimpleBinaryOp {
			field_name: "x".into(),
			op: BinaryOperator::Inside,
			literal: arr,
			reversed: true,
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		assert!(
			matches!(compile_predicate_shape(&p).expect("compile"), PredNode::Leaf { .. }),
			"reversed IN should use Leaf"
		);
	}

	#[test]
	fn compile_contains_compiles_to_leaf_streaming() {
		let p = sb("s", BinaryOperator::Contain, Value::String("needle".into()));
		match compile_predicate_shape(&p).expect("compile") {
			PredNode::LeafStreaming {
				..
			} => {}
			other => panic!("expected LeafStreaming, got {other:?}"),
		}
	}

	#[test]
	fn fused_map_does_not_swallow_leaf_streaming() {
		let contain_clause = sb("s", BinaryOperator::Contain, Value::String("needle".into()));
		let and1 = Arc::new(BinaryOp {
			left: contain_clause,
			op: BinaryOperator::And,
			right: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
		}) as Arc<dyn PhysicalExpr>;
		let full = Arc::new(BinaryOp {
			left: and1,
			op: BinaryOperator::And,
			right: sb("b", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		}) as Arc<dyn PhysicalExpr>;
		let n = compile_predicate_shape(&full).expect("compile");
		let PredNode::And(parts) = n else {
			panic!("expected And, got {n:?}");
		};
		assert_eq!(parts.len(), 2);
		let has_fused = parts.iter().any(|p| matches!(p, PredNode::FusedFlatMapAnd { .. }));
		let has_stream = parts.iter().any(|p| matches!(p, PredNode::LeafStreaming { .. }));
		assert!(has_fused && has_stream, "got {parts:?}");
	}

	#[test]
	fn fused_map_does_not_swallow_set_membership() {
		let arr = Value::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))]);
		let in_clause = sb("v", BinaryOperator::Inside, arr);
		let and1 = Arc::new(BinaryOp {
			left: in_clause,
			op: BinaryOperator::And,
			right: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let full = Arc::new(BinaryOp {
			left: and1,
			op: BinaryOperator::And,
			right: sb("b", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&full).expect("compile");
		let PredNode::And(parts) = n else {
			panic!("expected And, got {n:?}");
		};
		assert_eq!(parts.len(), 2);
		let has_fused = parts.iter().any(|p| matches!(p, PredNode::FusedFlatMapAnd { .. }));
		let has_set = parts.iter().any(|p| matches!(p, PredNode::LeafSetMembership { .. }));
		assert!(has_fused && has_set, "got {parts:?}");
	}

	#[test]
	fn compile_and_fuses_two_root_leaves() {
		let p = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			op: BinaryOperator::And,
			right: sb("b", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		assert!(
			matches!(
				n,
				PredNode::FusedFlatMapAnd {
					at_record_root: true,
					..
				}
			),
			"expected fused root map, got {n:?}"
		);
	}

	#[test]
	fn compile_or_keeps_disjunction_shape() {
		let p = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			op: BinaryOperator::Or,
			right: sb("b", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		match n {
			PredNode::Or(ch) => assert_eq!(ch.len(), 2),
			other => panic!("expected Or, got {other:?}"),
		}
	}

	#[test]
	fn compile_or_eq_same_field_fuses_to_set_membership() {
		// `a = 1 OR a = 2` -> bare LeafSetMembership { a, {1, 2} }
		let p = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			op: BinaryOperator::Or,
			right: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		let PredNode::LeafSetMembership {
			path,
			op,
			set,
			reversed,
			..
		} = n
		else {
			panic!("expected LeafSetMembership, got {n:?}");
		};
		assert_eq!(path, vec![PathSegment::from("a")]);
		assert_eq!(op, BinaryOperator::Inside);
		assert!(!reversed);
		assert_eq!(set.len(), 2);
		assert!(set.contains(&Value::Number(Number::Int(1))));
		assert!(set.contains(&Value::Number(Number::Int(2))));
	}

	#[test]
	fn compile_or_eq_three_arms_fuses() {
		// `a = 1 OR a = 2 OR a = 3` (left-assoc) -> LeafSetMembership { a, {1, 2, 3} }
		let inner = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			op: BinaryOperator::Or,
			right: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let p = Arc::new(BinaryOp {
			left: inner,
			op: BinaryOperator::Or,
			right: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(3))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		let PredNode::LeafSetMembership {
			set,
			..
		} = n
		else {
			panic!("expected LeafSetMembership, got {n:?}");
		};
		assert_eq!(set.len(), 3);
		for i in 1..=3 {
			assert!(set.contains(&Value::Number(Number::Int(i))));
		}
	}

	#[test]
	fn compile_or_eq_different_fields_stays_or() {
		// `a = 1 OR b = 2` -> disjunction preserved (no cross-field fusion).
		let p = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			op: BinaryOperator::Or,
			right: sb("b", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		match n {
			PredNode::Or(ch) => {
				assert_eq!(ch.len(), 2);
				assert!(ch.iter().all(|c| matches!(c, PredNode::Leaf { .. })));
			}
			other => panic!("expected Or, got {other:?}"),
		}
	}

	#[test]
	fn compile_array_len_eq_n_emits_leaf_streaming() {
		use crate::exec::ContextLevel;
		use crate::exec::parts::field::FieldPart;
		use crate::exec::physical_expr::function::BuiltinFunctionExec;
		use crate::exec::physical_expr::{IdiomExpr, Literal};

		let tags_idiom = Arc::new(IdiomExpr::new(
			"tags".into(),
			None,
			vec![Arc::new(FieldPart {
				name: "tags".into(),
			}) as Arc<dyn crate::exec::PhysicalExpr>],
		)) as Arc<dyn crate::exec::PhysicalExpr>;
		let array_len = Arc::new(BuiltinFunctionExec {
			name: "array::len".into(),
			arguments: vec![tags_idiom],
			func_required_context: ContextLevel::Root,
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let p = Arc::new(BinaryOp {
			left: array_len,
			op: BinaryOperator::Equal,
			right: Arc::new(Literal(Value::Number(Number::Int(3)))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		match compile_predicate_shape(&p).expect("compile") {
			PredNode::LeafStreaming {
				path,
				..
			} => {
				assert_eq!(path, vec![PathSegment::from("tags")]);
			}
			other => panic!("expected LeafStreaming, got {other:?}"),
		}
	}

	#[test]
	fn compile_or_mixed_field_partial_fuse() {
		// `a = 1 OR a = 2 OR b = 3` -> Or([ LeafSetMembership{a,{1,2}}, Leaf{b=3} ]).
		let inner = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			op: BinaryOperator::Or,
			right: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let p = Arc::new(BinaryOp {
			left: inner,
			op: BinaryOperator::Or,
			right: sb("b", BinaryOperator::Equal, Value::Number(Number::Int(3))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		let PredNode::Or(ch) = n else {
			panic!("expected Or, got {n:?}");
		};
		assert_eq!(ch.len(), 2);
		let has_set = ch.iter().any(|c| {
			matches!(c, PredNode::LeafSetMembership { path, set, .. }
				if path == &vec![PathSegment::from("a")] && set.len() == 2)
		});
		let has_leaf = ch.iter().any(
			|c| matches!(c, PredNode::Leaf { path, .. } if path == &vec![PathSegment::from("b")]),
		);
		assert!(has_set && has_leaf, "got {ch:?}");
	}

	#[test]
	fn compile_or_exact_equal_not_fused() {
		// `a == 1 OR a == 2` (ExactEqual) must NOT fuse: the set path is loose-eq.
		let p = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::ExactEqual, Value::Number(Number::Int(1))),
			op: BinaryOperator::Or,
			right: sb("a", BinaryOperator::ExactEqual, Value::Number(Number::Int(2))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		match n {
			PredNode::Or(ch) => {
				assert_eq!(ch.len(), 2);
				assert!(ch.iter().all(|c| matches!(c, PredNode::Leaf { .. })));
			}
			other => panic!("expected Or, got {other:?}"),
		}
	}

	#[test]
	fn compile_or_absorbs_in_arm() {
		// `a IN [1, 2] OR a = 3` -> LeafSetMembership { a, {1, 2, 3} }.
		let arr = Value::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))]);
		let p = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::Inside, arr),
			op: BinaryOperator::Or,
			right: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(3))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		let PredNode::LeafSetMembership {
			path,
			op,
			set,
			..
		} = n
		else {
			panic!("expected LeafSetMembership, got {n:?}");
		};
		assert_eq!(path, vec![PathSegment::from("a")]);
		assert_eq!(op, BinaryOperator::Inside);
		assert_eq!(set.len(), 3);
		for i in 1..=3 {
			assert!(set.contains(&Value::Number(Number::Int(i))));
		}
	}

	#[test]
	fn compile_or_lone_in_arm_preserved() {
		// `a IN [1, 2] OR b = 3` -> the lone IN arm stays a LeafSetMembership,
		// it must not degrade into a single-element Leaf.
		let arr = Value::from(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))]);
		let p = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::Inside, arr),
			op: BinaryOperator::Or,
			right: sb("b", BinaryOperator::Equal, Value::Number(Number::Int(3))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		let PredNode::Or(ch) = n else {
			panic!("expected Or, got {n:?}");
		};
		assert_eq!(ch.len(), 2);
		let has_set = ch.iter().any(|c| {
			matches!(c, PredNode::LeafSetMembership { path, set, .. }
				if path == &vec![PathSegment::from("a")] && set.len() == 2)
		});
		assert!(has_set, "lone IN arm should be preserved, got {ch:?}");
	}

	#[test]
	fn compile_or_not_equal_not_fused() {
		// `a != 1 OR a != 2` must NOT fuse (NotEqual is out of scope).
		let p = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::NotEqual, Value::Number(Number::Int(1))),
			op: BinaryOperator::Or,
			right: sb("a", BinaryOperator::NotEqual, Value::Number(Number::Int(2))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		match n {
			PredNode::Or(ch) => {
				assert_eq!(ch.len(), 2);
				assert!(ch.iter().all(|c| matches!(c, PredNode::Leaf { .. })));
			}
			other => panic!("expected Or, got {other:?}"),
		}
	}

	#[test]
	fn compile_or_eq_id_field_not_fused() {
		// `id = 1 OR id = 2`: the synthetic `id` field is derived from the KV key,
		// which only `eval_leaf` can resolve. Fusing into a `LeafSetMembership` would
		// route it through `eval_set_membership` (body lookup) and wrongly reject every
		// row — so id equalities must stay an Or of Leaf nodes. (Regression test for the
		// GraphQL `id: { in: [...] }` filter, which lowers to `id = a OR id = b`.)
		let p = Arc::new(BinaryOp {
			left: sb("id", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			op: BinaryOperator::Or,
			right: sb("id", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		match n {
			PredNode::Or(ch) => {
				assert_eq!(ch.len(), 2);
				assert!(ch.iter().all(|c| matches!(c, PredNode::Leaf { .. })));
			}
			other => panic!("expected Or, got {other:?}"),
		}
	}

	#[test]
	fn compile_array_len_reversed_swaps_operator() {
		// `5 < array::len(tags)` reversed becomes `array::len(tags) > 5`.
		use crate::exec::ContextLevel;
		use crate::exec::parts::field::FieldPart;
		use crate::exec::physical_expr::function::BuiltinFunctionExec;
		use crate::exec::physical_expr::{IdiomExpr, Literal};

		let tags_idiom = Arc::new(IdiomExpr::new(
			"tags".into(),
			None,
			vec![Arc::new(FieldPart {
				name: "tags".into(),
			}) as Arc<dyn crate::exec::PhysicalExpr>],
		)) as Arc<dyn crate::exec::PhysicalExpr>;
		let array_len = Arc::new(BuiltinFunctionExec {
			name: "array::len".into(),
			arguments: vec![tags_idiom],
			func_required_context: ContextLevel::Root,
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		// Literal on the left, function on the right → reversed compile.
		let p = Arc::new(BinaryOp {
			left: Arc::new(Literal(Value::Number(Number::Int(5)))),
			op: BinaryOperator::LessThan,
			right: array_len,
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		match compile_predicate_shape(&p).expect("compile") {
			PredNode::LeafStreaming {
				path,
				..
			} => {
				assert_eq!(path, vec![PathSegment::from("tags")]);
			}
			other => panic!("expected LeafStreaming, got {other:?}"),
		}
	}

	#[test]
	fn compile_not_inverts_supported_inner() {
		let p = Arc::new(UnaryOp {
			op: PrefixOperator::Not,
			expr: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		assert!(matches!(n, PredNode::Not(_)), "expected Not, got {n:?}");
	}

	#[test]
	fn repeated_root_key_keeps_both_clauses_in_fusion() {
		// Range / multi-clause same-field: `a = 1 AND a = 2` (and shapes
		// like `a > 3 AND a < 7`) used to collapse to last-clause-wins in
		// `flat_clauses_from_specs`. With multi-clause `FusedFlatClause`,
		// both clauses are preserved under a single key and evaluated
		// against the same `value_bytes` after one map lookup.
		let p = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			op: BinaryOperator::And,
			right: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		let PredNode::FusedFlatMapAnd {
			clauses,
			..
		} = n
		else {
			panic!("expected FusedFlatMapAnd, got {n:?}");
		};
		assert_eq!(clauses.len(), 1);
		assert_eq!(clauses[0].ops.len(), 2);
		assert_eq!(clauses[0].ops[0].literal, Value::Number(Number::Int(1)));
		assert_eq!(clauses[0].ops[1].literal, Value::Number(Number::Int(2)));
	}

	#[test]
	fn range_same_field_keeps_both_ops() {
		// `a > 3 AND a < 7` — the bread-and-butter range query — must land
		// on a single `FusedFlatClause` with both `>` and `<` ops.
		let p = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::MoreThan, Value::Number(Number::Int(3))),
			op: BinaryOperator::And,
			right: sb("a", BinaryOperator::LessThan, Value::Number(Number::Int(7))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		let PredNode::FusedFlatMapAnd {
			clauses,
			..
		} = n
		else {
			panic!("expected FusedFlatMapAnd, got {n:?}");
		};
		assert_eq!(clauses.len(), 1);
		assert_eq!(clauses[0].ops.len(), 2);
		assert_eq!(clauses[0].ops[0].op, BinaryOperator::MoreThan);
		assert_eq!(clauses[0].ops[1].op, BinaryOperator::LessThan);
	}

	#[test]
	fn compile_nested_shared_prefix_becomes_navigate_and_fused_inner() {
		let left_idiom = Arc::new(IdiomExpr::new(
			"outer.x".into(),
			None,
			vec![
				Arc::new(FieldPart {
					name: "outer".into(),
				}) as Arc<dyn crate::exec::PhysicalExpr>,
				Arc::new(FieldPart {
					name: "x".into(),
				}) as Arc<dyn crate::exec::PhysicalExpr>,
			],
		));
		let right_idiom = Arc::new(IdiomExpr::new(
			"outer.y".into(),
			None,
			vec![
				Arc::new(FieldPart {
					name: "outer".into(),
				}) as Arc<dyn crate::exec::PhysicalExpr>,
				Arc::new(FieldPart {
					name: "y".into(),
				}) as Arc<dyn crate::exec::PhysicalExpr>,
			],
		));
		let and = Arc::new(BinaryOp {
			left: Arc::new(BinaryOp {
				left: left_idiom,
				op: BinaryOperator::Equal,
				right: Arc::new(Literal(Value::Number(Number::Int(10)))),
			}) as Arc<dyn crate::exec::PhysicalExpr>,
			op: BinaryOperator::And,
			right: Arc::new(BinaryOp {
				left: right_idiom,
				op: BinaryOperator::Equal,
				right: Arc::new(Literal(Value::Number(Number::Int(20)))),
			}) as Arc<dyn crate::exec::PhysicalExpr>,
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&and).expect("compile");
		let PredNode::NavigatePrefix {
			segment,
			child,
		} = n
		else {
			panic!("expected NavigatePrefix, got {n:?}");
		};
		assert_eq!(segment.as_str(), "outer");
		assert!(matches!(
			*child,
			PredNode::FusedFlatMapAnd {
				at_record_root: false,
				..
			}
		));
	}

	#[test]
	fn compile_expr_skips_unsupported_leaf_in_and() {
		let bad = Arc::new(BinaryOp {
			left: Arc::new(Literal(Value::Number(Number::Int(1)))),
			op: BinaryOperator::Add,
			right: Arc::new(Literal(Value::Number(Number::Int(2)))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let p = Arc::new(BinaryOp {
			left: sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			op: BinaryOperator::And,
			right: bad,
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		assert!(compile_expr(&p).is_none());
	}

	/// Three-segment shared prefix (`a.b.c = 1 AND a.b.d = 2`) must compile to two nested
	/// [`PredNode::NavigatePrefix`] nodes followed by a [`PredNode::FusedFlatMapAnd`] over the
	/// deepest two leaves.
	#[test]
	fn compile_two_level_shared_prefix_nests_navigate_then_fuse() {
		let mk_idiom = |a: &str, b: &str, c: &str| {
			Arc::new(IdiomExpr::new(
				format!("{a}.{b}.{c}"),
				None,
				vec![
					Arc::new(FieldPart {
						name: a.into(),
					}) as Arc<dyn crate::exec::PhysicalExpr>,
					Arc::new(FieldPart {
						name: b.into(),
					}) as Arc<dyn crate::exec::PhysicalExpr>,
					Arc::new(FieldPart {
						name: c.into(),
					}) as Arc<dyn crate::exec::PhysicalExpr>,
				],
			))
		};
		let left = Arc::new(BinaryOp {
			left: mk_idiom("a", "b", "c"),
			op: BinaryOperator::Equal,
			right: Arc::new(Literal(Value::Number(Number::Int(1)))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let right = Arc::new(BinaryOp {
			left: mk_idiom("a", "b", "d"),
			op: BinaryOperator::Equal,
			right: Arc::new(Literal(Value::Number(Number::Int(2)))),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let and = Arc::new(BinaryOp {
			left,
			op: BinaryOperator::And,
			right,
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&and).expect("compile");
		let PredNode::NavigatePrefix {
			segment: outer_seg,
			child: outer_child,
		} = n
		else {
			panic!("expected outer NavigatePrefix, got {n:?}");
		};
		assert_eq!(outer_seg.as_str(), "a");
		let PredNode::NavigatePrefix {
			segment: inner_seg,
			child: inner_child,
		} = *outer_child
		else {
			panic!("expected inner NavigatePrefix, got {outer_child:?}");
		};
		assert_eq!(inner_seg.as_str(), "b");
		let PredNode::FusedFlatMapAnd {
			at_record_root,
			clauses,
		} = *inner_child
		else {
			panic!("expected FusedFlatMapAnd, got {inner_child:?}");
		};
		assert!(!at_record_root);
		assert_eq!(clauses.len(), 2);
		assert_eq!(clauses[0].key_utf8, b"c".to_vec());
		assert_eq!(clauses[1].key_utf8, b"d".to_vec());
	}

	fn mk_path_eq(parts: &[&str], lit: i64) -> Arc<dyn crate::exec::PhysicalExpr> {
		let path_parts: Vec<Arc<dyn crate::exec::PhysicalExpr>> = parts
			.iter()
			.map(|n| {
				Arc::new(FieldPart {
					name: (*n).to_string(),
				}) as Arc<dyn crate::exec::PhysicalExpr>
			})
			.collect();
		let joined = parts.join(".");
		Arc::new(BinaryOp {
			left: Arc::new(IdiomExpr::new(joined, None, path_parts)),
			op: BinaryOperator::Equal,
			right: Arc::new(Literal(Value::Number(Number::Int(lit)))),
		}) as Arc<dyn crate::exec::PhysicalExpr>
	}

	/// `WHERE a.b = … AND a.b.c = …` shares a prefix but one conjunct ends strictly earlier —
	/// fusion must not strip into empty path segments (would panic in [`cluster_multi_specs`]).
	#[test]
	fn compile_strict_prefix_paths_in_and_does_not_panic() {
		let p = Arc::new(BinaryOp {
			left: mk_path_eq(&["a", "b"], 1),
			op: BinaryOperator::And,
			right: mk_path_eq(&["a", "b", "c"], 2),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		let PredNode::NavigatePrefix {
			segment,
			..
		} = n
		else {
			panic!("expected NavigatePrefix, got {n:?}");
		};
		assert_eq!(segment.as_str(), "a");
	}

	#[test]
	fn compile_three_level_strict_prefix_paths_in_and_does_not_panic() {
		let p = Arc::new(BinaryOp {
			left: mk_path_eq(&["a", "b", "c"], 1),
			op: BinaryOperator::And,
			right: mk_path_eq(&["a", "b", "c", "d"], 2),
		}) as Arc<dyn crate::exec::PhysicalExpr>;
		let n = compile_predicate_shape(&p).expect("compile");
		let PredNode::NavigatePrefix {
			segment,
			..
		} = n
		else {
			panic!("expected NavigatePrefix, got {n:?}");
		};
		assert_eq!(segment.as_str(), "a");
	}
}
