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
use crate::expr::operator::BinaryOperator;
use crate::val::Value;

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
	if let Value::Array(a) = literal {
		for v in a.iter() {
			if !literal_hashset_element_safe(v) {
				return None;
			}
			set.insert(v.clone());
		}
	} else if let Value::Set(s) = literal {
		for v in s.iter() {
			if !literal_hashset_element_safe(v) {
				return None;
			}
			set.insert(v.clone());
		}
	} else {
		return None;
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
	if matches!(op, BinaryOperator::Contain | BinaryOperator::NotContain) {
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
	} else if matches!(
		op,
		BinaryOperator::ContainAny | BinaryOperator::ContainNone | BinaryOperator::ContainAll
	) {
		let set = try_build_inside_literal_hashset(literal)?;
		let tables = overlap_streaming_from_set(set.as_ref());
		let mode = if matches!(op, BinaryOperator::ContainAny) {
			OverlapMode::Any
		} else if matches!(op, BinaryOperator::ContainNone) {
			OverlapMode::None
		} else {
			OverlapMode::All
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
	} else {
		None
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
	/// The shape inside, stated by the physical layer as one that refuses enough of the rows it
	/// judges for a packed row to be worth checking for it.
	///
	/// A packed row is judged from its columns only once it has been checked whole, which costs a
	/// walk of the row. A scan whose shape is not `Selective` checks no packed row for its filter,
	/// so it decodes each one, unless another reader of the scan has checked it; it pays no check
	/// for a predicate that keeps most rows, where a row kept would be walked twice.
	Selective(Box<PreDecodeShape>),
	/// `path <op> literal`, or `literal <op> path` when `reversed`.
	Leaf {
		path: Arc<[PathSegment]>,
		op: BinaryOperator,
		literal: Value,
		reversed: bool,
	},
}

impl PreDecodeShape {
	/// Whether a leaf of this shape reads a path that opens with the name of a
	/// component of record ids with structure `key`.
	///
	/// A named component is derived from the record id and never stored in
	/// the record body, so its stored bytes would read as missing, as `id`'s
	/// would: a scan of such a table takes no shape that reads one.
	pub(crate) fn reads_key_component(&self, key: &crate::catalog::KeyStructure) -> bool {
		match self {
			Self::And(parts) | Self::Or(parts) => {
				parts.iter().any(|part| part.reads_key_component(key))
			}
			Self::Not(inner) | Self::Selective(inner) => inner.reads_key_component(key),
			Self::Leaf {
				path,
				..
			} => crate::exec::topk_pushdown::reads_key_component(path, key),
		}
	}

	/// Whether a scan checks each packed row it reads for the filter of this shape to judge it
	/// from its columns: the shape is [`Self::Selective`].
	pub(crate) fn judges_packed_rows(&self) -> bool {
		matches!(self, Self::Selective(_))
	}

	/// The filter that judges stored bytes by this shape, descending no deeper
	/// than `depth_limit` path segments.
	pub(crate) fn filter(&self, depth_limit: u32) -> Arc<PreDecodeFilter> {
		let filter = PreDecodeFilter::new(fuse_tree(self.node()), depth_limit);
		Arc::new(filter.judging_packed_rows(self.judges_packed_rows()))
	}

	fn node(&self) -> PredNode {
		match self {
			Self::And(parts) => PredNode::And(parts.iter().map(Self::node).collect()),
			Self::Or(parts) => PredNode::Or(parts.iter().map(Self::node).collect()),
			Self::Not(inner) => PredNode::Not(Box::new(inner.node())),
			Self::Selective(inner) => inner.node(),
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
		other @ (PredNode::Leaf {
			..
		}
		| PredNode::LeafSetMembership {
			..
		}
		| PredNode::LeafStreaming {
			..
		}
		| PredNode::FusedFlatMapAnd {
			..
		}
		| PredNode::NavigatePrefix {
			..
		}) => other,
	}
}

fn flatten_and_into(acc: &mut Vec<PredNode>, node: PredNode) {
	if let PredNode::And(xs) = node {
		for x in xs {
			flatten_and_into(acc, x);
		}
	} else {
		acc.push(node);
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
	if let PredNode::Or(xs) = node {
		for x in xs {
			flatten_or_into(acc, x);
		}
	} else {
		acc.push(node);
	}
}

/// Fuse a disjunction's arms, rewriting same-field equality chains into a single
/// wire-fast [`PredNode::LeafSetMembership`].
///
/// `a = X OR a = Y` is semantically identical to `a IN [X, Y]` under SurrealQL's
/// loose equality (`=` / [`operate::equal`]). Collapsing the chain pays one field
/// descent + one hash probe per row instead of one descent + comparison per arm;
/// `eval_set_membership` keeps the two relations apart where hashing cannot
/// decide loose equality (a regex against strings, and a value whose hash may
/// disagree with its equality — see
/// [`hash_miss_is_decisive`](super::wire_literal::hash_miss_is_decisive)).
///
/// Conservative — an arm only joins a fused group when it is:
/// - a [`PredNode::Leaf`] with `op == Equal`, `reversed == false`, and a hashset-safe literal (see
///   [`literal_hashset_element_safe`]); or
/// - an existing [`PredNode::LeafSetMembership`] with `op == Inside` and `reversed == false`
///   (absorbing an `a IN [..]` arm into the same-field group).
///
/// Everything else — `ExactEqual` (type-strict, diverges from the loose set path),
/// `NotEqual`, `NotInside`, reversed arms, different fields, and non-leaf nodes — passes
/// through untouched.
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
		let contribution: Option<(Vec<PathSegment>, Vec<Value>)> = if let PredNode::Leaf {
			path,
			op: BinaryOperator::Equal,
			literal,
			reversed: false,
			..
		} = &arm
			&& literal_hashset_element_safe(literal)
		{
			Some((path.clone(), vec![literal.clone()]))
		} else if let PredNode::LeafSetMembership {
			path,
			op: BinaryOperator::Inside,
			set,
			reversed: false,
			..
		} = &arm
		{
			Some((path.clone(), set.iter().cloned().collect()))
		} else {
			None
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
	let mut root_fusible = Vec::new();
	let mut multi_leaves = Vec::new();
	let mut rest = Vec::new();

	for n in bucket {
		if let PredNode::Leaf {
			ref path,
			..
		} = n
		{
			if path.len() == 1 {
				root_fusible.push(n);
			} else if path.len() > 1 {
				multi_leaves.push(n);
			} else {
				rest.push(n);
			}
		} else {
			rest.push(n);
		}
	}

	let mut out: Vec<PredNode> = Vec::new();

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
		let PredNode::Leaf {
			path,
			op,
			literal,
			literal_wire: _,
			reversed,
		} = n
		else {
			unreachable!(
				"fuse_multi_segment_leaves expects only multi-segment leaves, got {:?}",
				n
			);
		};
		if let Ok(fl) = leaf_to_spec(path, op, literal, reversed) {
			specs.push(fl);
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
