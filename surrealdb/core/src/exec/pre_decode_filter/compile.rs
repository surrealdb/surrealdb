//! Build [`PreDecodeFilter`](super::PreDecodeFilter) trees from logical predicates.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use memchr::memmem;

use super::streaming::{
	ArrayElementContains, ArrayOverlapsLiteralSet, OverlapMode, StreamingLeafEvaluator,
	SubstringMatch, overlap_streaming_from_set,
};
use super::wire_literal::{LiteralSet, LiteralWire};
use super::{FusedFlatClause, FusedFlatClauses, LeafFallback, PreDecodeFilter, PredNode};
use crate::exec::object_extract::PathSegment;
use crate::exec::planner::is_simple_binary_eligible;
use crate::expr::logical::LogicalExpr;
use crate::expr::operator::{BinaryOperator, PrefixOperator};
use crate::val::Value;

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
				literal_count: tables.literal_count,
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

/// The leaf judging `path <op> literal` (`literal <op> path` when `reversed`):
/// a set membership, a streaming evaluator or a plain comparison, whichever
/// the operator and literal admit.
fn leaf_node(
	path: Vec<PathSegment>,
	op: &BinaryOperator,
	literal: Value,
	reversed: bool,
) -> PredNode {
	if !reversed
		&& matches!(op, BinaryOperator::Inside | BinaryOperator::NotInside)
		&& let Some(set) = try_build_inside_literal_hashset(&literal)
	{
		let literal_set = Arc::new(LiteralSet::from_set(set.as_ref()));
		return PredNode::LeafSetMembership {
			path,
			op: op.clone(),
			set,
			literal_set,
			reversed: false,
		};
	}
	if let Some(n) = try_compile_leaf_streaming(path.clone(), op, &literal, reversed) {
		return n;
	}
	let literal_wire = Arc::new(LiteralWire::from_value(&literal));
	PredNode::Leaf {
		path,
		op: op.clone(),
		literal,
		literal_wire,
		reversed,
	}
}

/// The predicate a table scan judges of a record's stored bytes before it
/// decodes the record, as plain data: the comparisons of a field with a
/// literal, joined by `AND`, `OR` and `NOT`.
///
/// A record the shape judges false is never decoded; one it cannot judge is
/// decoded and judged by the predicate itself, so the shape only ever spares
/// a record the predicate would have refused.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PreDecodeShape {
	And(Vec<PreDecodeShape>),
	Or(Vec<PreDecodeShape>),
	Not(Box<PreDecodeShape>),
	/// `path <op> literal`, or `literal <op> path` when `reversed`.
	Leaf {
		path: Arc<[PathSegment]>,
		op: BinaryOperator,
		literal: Value,
		reversed: bool,
	},
}

impl PreDecodeShape {
	/// The shape of `predicate`, when every part of it is one the stored bytes
	/// judge: a comparison of a static field path with a literal, or a
	/// conjunction, disjunction or negation of such.
	pub(crate) fn of(predicate: &LogicalExpr) -> Option<Self> {
		match predicate {
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::And,
				right,
			} => Some(Self::And(vec![Self::of(left)?, Self::of(right)?])),
			LogicalExpr::Binary {
				left,
				op: BinaryOperator::Or,
				right,
			} => Some(Self::Or(vec![Self::of(left)?, Self::of(right)?])),
			LogicalExpr::Prefix {
				op: PrefixOperator::Not,
				expr,
			} => Some(Self::Not(Box::new(Self::of(expr)?))),
			LogicalExpr::Binary {
				left,
				op,
				right,
			} => {
				if !is_simple_binary_eligible(op) || unsupported_containment_like(op) {
					return None;
				}
				Self::leaf(left, right, false, op).or_else(|| Self::leaf(right, left, true, op))
			}
			_ => None,
		}
	}

	fn leaf(
		field: &LogicalExpr,
		literal: &LogicalExpr,
		reversed: bool,
		op: &BinaryOperator,
	) -> Option<Self> {
		use crate::expr::logical::FieldSegment;
		let field_path = field.field_path()?;
		let path = field_path
			.segments()
			.iter()
			.map(|segment| match segment {
				FieldSegment::Field(name) => Some(PathSegment::new(name.as_str())),
				FieldSegment::All => None,
			})
			.collect::<Option<Vec<_>>>()?;
		// The record's id is its key, which its stored bytes do not hold.
		if path.first()?.as_str() == "id" {
			return None;
		}
		let literal = crate::exec::planner::util::try_literal_to_value(literal)?;
		Some(Self::Leaf {
			path: path.into(),
			op: op.clone(),
			literal,
			reversed,
		})
	}

	/// The filter that judges stored bytes by this shape, descending no deeper
	/// than `depth_limit` path segments.
	pub(crate) fn filter(&self, depth_limit: u32) -> Arc<PreDecodeFilter> {
		Arc::new(PreDecodeFilter::new(fuse_tree(self.node()), depth_limit))
	}

	fn node(&self) -> PredNode {
		match self {
			Self::And(parts) => PredNode::And(parts.iter().map(Self::node).collect()),
			Self::Or(parts) => PredNode::Or(parts.iter().map(Self::node).collect()),
			Self::Not(inner) => PredNode::Not(Box::new(inner.node())),
			Self::Leaf {
				path,
				op,
				literal,
				reversed,
			} => leaf_node(path.to_vec(), op, literal.clone(), *reversed),
		}
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
/// loose equality (`=` / [`operate::equal`]). Collapsing the chain pays one field
/// descent + one hash probe per row instead of one descent + comparison per arm;
/// `eval_set_membership` keeps the two relations apart where hashing cannot
/// decide loose equality (numbers, and a regex against strings — see
/// [`hash_miss_is_decisive`](super::wire_literal::hash_miss_is_decisive)).
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
	let inner: Vec<FusedFlatClause> =
		map.into_iter().map(|(key_utf8, ops)| FusedFlatClause::new(key_utf8, ops)).collect();
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

	use super::{PathSegment, PreDecodeShape, PredNode, fuse_tree};
	use crate::expr::logical::LogicalExpr;
	use crate::expr::operator::{BinaryOperator, PrefixOperator};
	use crate::val::{Number, Object, Value};

	fn literal(value: &Value) -> LogicalExpr {
		crate::exec::planner::util::try_value_to_logical_expr(value.clone())
			.expect("a test literal has a logical form")
	}

	/// `field op literal`, or `literal op field` when `reversed`.
	fn field_cmp(field: &str, op: BinaryOperator, value: &Value, reversed: bool) -> LogicalExpr {
		let field = LogicalExpr::Field(surrealdb_strand::Strand::from(field));
		let (left, right) = if reversed {
			(literal(value), field)
		} else {
			(field, literal(value))
		};
		bin(left, op, right)
	}

	fn bin(left: LogicalExpr, op: BinaryOperator, right: LogicalExpr) -> LogicalExpr {
		LogicalExpr::Binary {
			left: Box::new(left),
			op,
			right: Box::new(right),
		}
	}

	fn un(op: PrefixOperator, expr: LogicalExpr) -> LogicalExpr {
		LogicalExpr::Prefix {
			op,
			expr: Box::new(expr),
		}
	}

	/// A read of the field at `parts`, one segment each, compared for equality with `lit`.
	fn mk_path_eq(parts: &[&str], lit: i64) -> LogicalExpr {
		let mut parts = parts.iter();
		let first = parts.next().expect("a path has a segment");
		let mut path = LogicalExpr::Field(surrealdb_strand::Strand::from(*first));
		for part in parts {
			path = LogicalExpr::Access {
				base: Box::new(path),
				field: surrealdb_strand::Strand::from(*part),
			};
		}
		bin(path, BinaryOperator::Equal, literal(&Value::Number(Number::Int(lit))))
	}

	/// The fused predicate tree of `pred`, or `Err` when its shape is not one the stored bytes
	/// judge.
	fn compile_predicate_shape(pred: &LogicalExpr) -> Result<PredNode, ()> {
		PreDecodeShape::of(pred).map(|shape| fuse_tree(shape.node())).ok_or(())
	}

	fn sb(field: &str, op: BinaryOperator, value: impl std::borrow::Borrow<Value>) -> LogicalExpr {
		field_cmp(field, op, value.borrow(), false)
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
		let p = field_cmp("x", BinaryOperator::Equal, &Value::Number(Number::Int(1)), true);
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
			let p = field_cmp("a", op.clone(), &Value::Number(Number::Int(1)), false);
			assert!(compile_predicate_shape(&p).is_err(), "expected unsupported for {op:?}");
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
		let p = field_cmp("x", BinaryOperator::Inside, &arr, true);
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
		let and1 = bin(
			contain_clause,
			BinaryOperator::And,
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
		);
		let full = bin(
			and1,
			BinaryOperator::And,
			sb("b", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		);
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
		let and1 = bin(
			in_clause,
			BinaryOperator::And,
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
		);
		let full = bin(
			and1,
			BinaryOperator::And,
			sb("b", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		);
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
	fn compile_or_keeps_disjunction_shape() {
		let p = bin(
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			BinaryOperator::Or,
			sb("b", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		);
		let n = compile_predicate_shape(&p).expect("compile");
		match n {
			PredNode::Or(ch) => assert_eq!(ch.len(), 2),
			other => panic!("expected Or, got {other:?}"),
		}
	}

	#[test]
	fn compile_or_eq_same_field_fuses_to_set_membership() {
		// `a = 1 OR a = 2` -> bare LeafSetMembership { a, {1, 2} }
		let p = bin(
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			BinaryOperator::Or,
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		);
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
		let inner = bin(
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			BinaryOperator::Or,
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		);
		let p = bin(
			inner,
			BinaryOperator::Or,
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(3))),
		);
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
		let p = bin(
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			BinaryOperator::Or,
			sb("b", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		);
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
	fn compile_or_mixed_field_partial_fuse() {
		// `a = 1 OR a = 2 OR b = 3` -> Or([ LeafSetMembership{a,{1,2}}, Leaf{b=3} ]).
		let inner = bin(
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			BinaryOperator::Or,
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		);
		let p = bin(
			inner,
			BinaryOperator::Or,
			sb("b", BinaryOperator::Equal, Value::Number(Number::Int(3))),
		);
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
		let p = bin(
			sb("a", BinaryOperator::ExactEqual, Value::Number(Number::Int(1))),
			BinaryOperator::Or,
			sb("a", BinaryOperator::ExactEqual, Value::Number(Number::Int(2))),
		);
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
		let p = bin(
			sb("a", BinaryOperator::Inside, arr),
			BinaryOperator::Or,
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(3))),
		);
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
		let p = bin(
			sb("a", BinaryOperator::Inside, arr),
			BinaryOperator::Or,
			sb("b", BinaryOperator::Equal, Value::Number(Number::Int(3))),
		);
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
		let p = bin(
			sb("a", BinaryOperator::NotEqual, Value::Number(Number::Int(1))),
			BinaryOperator::Or,
			sb("a", BinaryOperator::NotEqual, Value::Number(Number::Int(2))),
		);
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
	fn an_id_equality_has_no_shape() {
		// `id = 1 OR id = 2`: the synthetic `id` field is derived from the KV key,
		// which the stored bytes do not hold, so a predicate over it has no
		// pre-decode shape at all (and is never fused into a set membership that
		// would reject every row). Regression test for the GraphQL
		// `id: { in: [...] }` filter, which lowers to `id = a OR id = b`.
		let p = bin(
			sb("id", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			BinaryOperator::Or,
			sb("id", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		);
		assert!(compile_predicate_shape(&p).is_err(), "an id predicate must not compile");
	}

	#[test]
	fn compile_not_inverts_supported_inner() {
		let p =
			un(PrefixOperator::Not, sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))));
		let n = compile_predicate_shape(&p).expect("compile");
		assert!(matches!(n, PredNode::Not(_)), "expected Not, got {n:?}");
	}

	#[test]
	fn repeated_root_key_keeps_both_clauses_in_fusion() {
		// Range / multi-clause same-field: `a = 1 AND a = 2` (and shapes
		// like `a > 3 AND a < 7`). With multi-clause `FusedFlatClause`,
		// both clauses are preserved under a single key and evaluated
		// against the same `value_bytes` after one map lookup.
		let p = bin(
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(1))),
			BinaryOperator::And,
			sb("a", BinaryOperator::Equal, Value::Number(Number::Int(2))),
		);
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
		let p = bin(
			sb("a", BinaryOperator::MoreThan, Value::Number(Number::Int(3))),
			BinaryOperator::And,
			sb("a", BinaryOperator::LessThan, Value::Number(Number::Int(7))),
		);
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

	/// `WHERE a.b = … AND a.b.c = …` shares a prefix but one conjunct ends strictly earlier —
	/// fusion must not strip into empty path segments (would panic in [`cluster_multi_specs`]).
	#[test]
	fn compile_strict_prefix_paths_in_and_does_not_panic() {
		let p =
			bin(mk_path_eq(&["a", "b"], 1), BinaryOperator::And, mk_path_eq(&["a", "b", "c"], 2));
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
		let p = bin(
			mk_path_eq(&["a", "b", "c"], 1),
			BinaryOperator::And,
			mk_path_eq(&["a", "b", "c", "d"], 2),
		);
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
