//! An expression DAG over one evaluation region: which pure subexpressions
//! are evaluated once and read again.
//!
//! [`build`] interns the scalars of one *evaluation region* into a
//! hash-consed graph: equal subtrees evaluated against the same row are one
//! [`DagNode`], and each node records where in the region it is written.
//! [`ExprDag::shared`] then names the values a lowering may evaluate once and
//! read from a register afterwards. The DAG decides nothing about evaluation:
//! a consumer lowers the first occurrence of a [`SharedValue`] as it would
//! lower any expression, keeps the result, and reads it at each
//! [`SharedValue::reuses`] occurrence it reaches. An occurrence it does not
//! reach, or reaches in a different order, is lowered as written.
//!
//! # The key is (region, frame, expression)
//!
//! A DAG holds one *region* of one statement: a straight-line stretch of one
//! pipeline's code over one register file, evaluated once per row (or once,
//! for a block region), whose roots are given in execution order. Sharing
//! never crosses a region: a pipeline, a set-aside or restored register file,
//! a host scalar, a permission region, an aggregate argument or key, a return
//! catch, a statement boundary and a `LET` read by name each start another.
//! A `$name` read is a leaf, and the `LET`'s expression is never substituted
//! for it; an opaque call is never pure, so it is never merged.
//!
//! Within a region a node that reads the row ([`Frame`]) is keyed by the
//! frame it reads. The same text above and beneath a layer denotes two values:
//! a read of a column above a layer sees what the layer set, and the same read
//! beneath it does not (see [`super::schema`]). Every other node is keyed by
//! its shape and its children alone, so its frame flows up through them. A new
//! frame starts at each root whose row differs from the previous root's, and
//! at every child reached as a body, so a body node never merges with a row
//! node.
//!
//! # What is shared
//!
//! **Candidates.** A shared expression is not a leaf (it has a child in
//! [`LogicalExpr::for_each_child`]) and its [`Effects`] are
//! [`Effects::pure`]: deterministic, effect-free and not an index operator.
//! Pure already excludes a field read that may dereference a record, a call
//! that is not deterministic, a method, a closure, a subquery and control
//! flow.
//!
//! **Bodies do not count.** An occurrence inside a body is neither counted
//! nor a first occurrence nor a reuse, since the body is evaluated against a
//! value other than the region's row, or not at all, or more than once. The
//! bodies are:
//!
//! - the body of a broadcast, an optional step, a filter predicate, a recursion and its repeat
//!   marker;
//! - a closure, the clauses of a lookup, the aliases of a destructure and the arguments of a
//!   method;
//! - the index of an index step, which the VM evaluates against the document root;
//! - anything under a subquery.
//!
//! **Conditional occurrences.** An occurrence is *conditional* when the code
//! that evaluates it is reached only on some paths through its parent: the
//! right operand of `AND`, `OR`, `??` or `?:`, which is evaluated only when
//! the left does not decide; the value of a `SetField`, which is evaluated
//! only when its base is an object; and every child of an `IF` but the first
//! condition. Any other counted occurrence is *unconditional*. [`reach`]
//! states the class of each child and must match the order in which the VM
//! lowers it (`value_node` in `exec/vm/lower.rs`).
//!
//! **A value is shared** when all of these hold:
//!
//! - its *first* occurrence, the earliest unconditional one, exists, and the root it is written in
//!   holds no subquery anywhere: a `RETURN` a subquery signals leaves the root, so it could skip
//!   the first occurrence and leave the later reads with nothing to read;
//! - it is a non-leaf, [`Effects::pure`] node;
//! - at least one counted occurrence follows the first.
//!
//! Evaluation order is the roots in the order given, each walked pre-order in
//! the order of [`LogicalExpr::for_each_child`]. A reuse may be conditional:
//! by the time it is reached the unconditional first one has run. A
//! conditional occurrence before the first is neither first nor reuse and is
//! lowered as written, so a branch that comes first does not block sharing
//! between later unconditional occurrences.
//!
//! # Why a fallible expression may be shared
//!
//! The DAG merges an expression that may raise without a proof that it cannot.
//! A kind proof licenses only a *move* or a *drop* of a fallible expression;
//! a merge under dominance is neither, and is unobservable. An occurrence may
//! read the value of an earlier occurrence of the same node when:
//!
//! - (a) the earlier one is the node's first unconditional occurrence in the region;
//! - (b) the earlier one is unconditional: every edge from its root down to it is
//!   [`Reach::Always`];
//! - (c) both are in the same region and frame, which the key guarantees;
//! - (d) the earlier one's root holds no subquery;
//! - (e) the earlier one is not lowered inside a sub-region that catches or converts its error:
//!   such a sub-region hides the scope, so a first occurrence lowered there sets no register and
//!   every reuse is lowered as written;
//! - (f) the node is [`Effects::pure`].
//!
//! Every run that reaches the later occurrence has then already evaluated the
//! first against the same inputs. If the first raised, the statement stopped
//! there, exactly as it does unshared. If it did not, evaluating again would
//! give the same value and raise nothing. The results, the first error and
//! its message are unchanged.
//!
//! # Kinds
//!
//! Each node carries the facts a parent reads of it: its effects, the kind it
//! is proven to evaluate to against the open schema
//! ([`LogicalExpr::checked_kind`]), and whether it may dereference a record.
//! They are folded once per node from its children's facts, and a lowering
//! reads a kind through [`ExprDag::kind_at`]. A kind here is a proof against
//! [`Schema::open`], never a declared kind; the logical layer never reads a
//! schema kind.
//!
//! # Cost
//!
//! [`build`] does work proportional to the nodes it visits times the width of
//! each node. A node is interned once, on its shallow shape
//! ([`LogicalExpr::shallow_eq`]), the ids of its children, and its frame when
//! it reads the row; no subtree is hashed or compared as a whole.
//!
//! # Dead nodes
//!
//! [`build`] interns only what the roots reach, so a branch folded away
//! before the build leaves no node behind. An occurrence inside the reuse of
//! a shared value is not recorded for the nodes beneath it, since a consumer
//! reading the value never evaluates them.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use super::effects::{
	ChildFacts, EffectOracle, Effects, node_effects, node_may_dereference, node_reads_row,
};
use super::expr::LogicalExpr;
use super::schema::Schema;
use crate::expr::{BinaryOperator, Kind};

/// The position of a node in [`ExprDag::nodes`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(u32);

impl NodeId {
	fn index(self) -> usize {
		self.0 as usize
	}
}

/// The row a node that reads the row reads. Two roots, or a root and a body,
/// share a frame only when they are evaluated against the same row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Frame(u32);

impl Frame {
	pub const fn new(index: u32) -> Self {
		Frame(index)
	}

	/// The frame of the row after a step that replaces or rebinds it.
	pub const fn next(self) -> Self {
		Frame(self.0 + 1)
	}
}

/// One expression of a region and the frame it is evaluated against.
#[derive(Clone, Copy, Debug)]
pub struct Root<'a> {
	pub expr: &'a LogicalExpr,
	pub frame: Frame,
}

/// One distinct expression of a region.
#[derive(Debug)]
pub struct DagNode<'a> {
	/// The first written position of the node.
	pub expr: &'a LogicalExpr,
	/// The node of each child of `expr`, in the order of
	/// [`LogicalExpr::for_each_child`].
	pub children: Vec<NodeId>,
	/// What evaluating `expr` does, a subquery not entered.
	pub effects: Effects,
	/// The kind `expr` is proven to evaluate to against the open schema
	/// ([`LogicalExpr::checked_kind`]), folded once for the node.
	pub checked_kind: Option<Kind>,
	/// Whether the value of `expr` may be, or hold, a record id
	/// ([`super::may_dereference`]).
	pub may_dereference: bool,
	/// Where `expr` is written in the region, outside every body, in
	/// evaluation order.
	pub occurrences: Vec<Occurrence<'a>>,
	/// The index in `occurrences` of the first unconditional one.
	first: Option<usize>,
}

/// One written position of a node.
#[derive(Debug)]
pub struct Occurrence<'a> {
	/// The address of the written expression. It names the position and is
	/// compared, never dereferenced.
	pub at: *const LogicalExpr,
	/// The index in the region's roots of the root it is written in.
	pub root: usize,
	/// The position of the occurrence in evaluation order, across the region.
	pub order: usize,
	/// Whether the occurrence is evaluated only on some paths through its
	/// parent (see [`Reach::Conditional`]).
	pub conditional: bool,
	_region: std::marker::PhantomData<&'a LogicalExpr>,
}

/// What the intern table keys a node on: its shallow shape, its children and,
/// when the node itself reads the row, the frame.
struct NodeKey<'a> {
	expr: &'a LogicalExpr,
	children: Vec<NodeId>,
	frame: Option<Frame>,
}

impl Hash for NodeKey<'_> {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.expr.shallow_hash(state);
		self.children.hash(state);
		self.frame.hash(state);
	}
}

impl PartialEq for NodeKey<'_> {
	fn eq(&self, other: &Self) -> bool {
		#[cfg(test)]
		SHALLOW_EQ_CALLS.with(|calls| calls.set(calls.get() + 1));
		self.frame == other.frame
			&& self.children == other.children
			&& self.expr.shallow_eq(other.expr)
	}
}

impl Eq for NodeKey<'_> {}

#[cfg(test)]
thread_local! {
	/// The number of shallow comparisons the intern table has made.
	static SHALLOW_EQ_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The distinct expressions of one evaluation region.
#[derive(Debug)]
pub struct ExprDag<'a> {
	pub nodes: Vec<DagNode<'a>>,
	pub roots: Vec<NodeId>,
	/// The intern table: one node per shallow shape, children and frame.
	table: HashMap<NodeKey<'a>, NodeId>,
	/// Every position visited, bodies included, and its node.
	positions: HashMap<*const LogicalExpr, NodeId>,
	/// For each root, whether a subquery is written anywhere in it.
	holds_subquery: Vec<bool>,
}

impl std::fmt::Debug for NodeKey<'_> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("NodeKey").field("children", &self.children).finish_non_exhaustive()
	}
}

/// A value to evaluate once, and the occurrences that read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedValue {
	/// The first unconditional occurrence in evaluation order.
	pub first: *const LogicalExpr,
	/// Every later counted occurrence, in evaluation order.
	pub reuses: Vec<*const LogicalExpr>,
}

// The addresses name positions of an expression tree and are compared, never
// dereferenced, so a value moves between threads as freely as the integers it
// stands for.
unsafe impl Send for SharedValue {}
unsafe impl Sync for SharedValue {}

/// How a child is reached from its parent.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reach {
	/// Evaluated whenever the parent is.
	Always,
	/// Evaluated only on some paths through the parent's code.
	Conditional,
	/// Inside a body.
	Body,
}

/// How the child at `index` of `parent`, in the order of
/// [`LogicalExpr::for_each_child`], is reached.
///
/// `Always` only when the VM lowers the child on every path through the
/// parent's code; a child the VM lowers after a conditional jump is
/// `Conditional`, and one it lowers against a rebound current value, element or
/// document is `Body`. The VM's order is `value_node` in `exec/vm/lower.rs`,
/// which this must match.
#[doc(hidden)]
pub fn reach(parent: &LogicalExpr, index: usize) -> Reach {
	// Every variant is named, so that a new one states where its children are
	// evaluated.
	match parent {
		LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Index {
			..
		} => match index {
			0 => Reach::Always,
			_ => Reach::Body,
		},
		LogicalExpr::SetField {
			..
		}
		| LogicalExpr::If(_) => match index {
			0 => Reach::Always,
			_ => Reach::Conditional,
		},
		LogicalExpr::RepeatRecurse(_) | LogicalExpr::Closure(_) => Reach::Body,
		LogicalExpr::Binary {
			op,
			..
		} => match (index, op) {
			(
				1,
				BinaryOperator::And
				| BinaryOperator::Or
				| BinaryOperator::NullCoalescing
				| BinaryOperator::TenaryCondition,
			) => Reach::Conditional,
			_ => Reach::Always,
		},
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => Reach::Always,
	}
}

struct Builder<'a, 'o> {
	dag: ExprDag<'a>,
	oracle: &'o dyn EffectOracle,
	/// The next frame to hand a body.
	next_frame: u32,
	root: usize,
	order: usize,
}

impl<'a> Builder<'a, '_> {
	/// Pass 1: intern `expr` and everything beneath it, children first, and
	/// remember the node of every position. Answers the node of `expr`.
	fn intern(&mut self, expr: &'a LogicalExpr, frame: Frame) -> NodeId {
		let mut children = Vec::new();
		expr.for_each_child(&mut |child| children.push(child));
		let children = children
			.into_iter()
			.enumerate()
			.map(|(index, child)| {
				let frame = match reach(expr, index) {
					Reach::Body => self.body_frame(),
					Reach::Always | Reach::Conditional => frame,
				};
				self.intern(child, frame)
			})
			.collect::<Vec<_>>();
		let key = NodeKey {
			expr,
			children,
			frame: node_reads_row(expr, self.oracle).then_some(frame),
		};
		let id = match self.dag.table.get(&key) {
			Some(&id) => id,
			None => {
				let id = self.push(&key);
				self.dag.table.insert(key, id);
				id
			}
		};
		self.dag.positions.insert(expr as *const LogicalExpr, id);
		id
	}

	/// A frame no root and no other body holds.
	fn body_frame(&mut self) -> Frame {
		let frame = Frame(self.next_frame);
		self.next_frame += 1;
		frame
	}

	/// The node of a key not yet interned, its facts folded from its
	/// children's.
	fn push(&mut self, key: &NodeKey<'a>) -> NodeId {
		let facts = key
			.children
			.iter()
			.map(|child| {
				let node = &self.dag.nodes[child.index()];
				ChildFacts {
					effects: node.effects,
					checked_kind: node.checked_kind.clone(),
					may_dereference: node.may_dereference,
				}
			})
			.collect::<Vec<_>>();
		let kinds = facts.iter().map(|facts| facts.checked_kind.clone()).collect::<Vec<_>>();
		let id = NodeId(self.dag.nodes.len() as u32);
		self.dag.nodes.push(DagNode {
			expr: key.expr,
			children: key.children.clone(),
			effects: node_effects(key.expr, &facts, self.oracle),
			checked_kind: key.expr.node_checked_kind(&kinds, &Schema::open()),
			may_dereference: node_may_dereference(key.expr, &facts),
			occurrences: Vec::new(),
			first: None,
		});
		id
	}

	/// Pass 2: record where `expr` is written, in evaluation order.
	fn visit(&mut self, expr: &'a LogicalExpr, conditional: bool) {
		let id = self.dag.positions[&(expr as *const LogicalExpr)];
		// The reuse of a value is read, not evaluated: the nodes beneath it are
		// written nowhere a consumer evaluates.
		let reused = self.dag.nodes[id.index()].first.is_some() && self.dag.first_is_reusable(id);
		self.record(id, expr, conditional);
		if reused {
			return;
		}
		let mut children = Vec::new();
		expr.for_each_child(&mut |child| children.push(child));
		for (index, child) in children.into_iter().enumerate() {
			match reach(expr, index) {
				Reach::Always => self.visit(child, conditional),
				Reach::Conditional => self.visit(child, true),
				// Nothing in a body is recorded.
				Reach::Body => {}
			}
		}
	}

	fn record(&mut self, id: NodeId, at: &'a LogicalExpr, conditional: bool) {
		let order = self.order;
		self.order += 1;
		let node = &mut self.dag.nodes[id.index()];
		if !conditional && node.first.is_none() {
			node.first = Some(node.occurrences.len());
		}
		node.occurrences.push(Occurrence {
			at: at as *const LogicalExpr,
			root: self.root,
			order,
			conditional,
			_region: std::marker::PhantomData,
		});
	}
}

/// The DAG of one evaluation region whose roots are `roots`, in execution
/// order. Each root runs only after every earlier root has finished, and a root
/// that rejects the row ends the row before later roots run. The caller
/// assigns each root its frame; bodies are given frames above the largest.
/// `oracle` answers the effects of the builtins the roots call.
pub fn build<'a>(roots: &[Root<'a>], oracle: &dyn EffectOracle) -> ExprDag<'a> {
	let mut builder = Builder {
		dag: ExprDag {
			nodes: Vec::new(),
			roots: Vec::with_capacity(roots.len()),
			table: HashMap::new(),
			positions: HashMap::new(),
			holds_subquery: roots
				.iter()
				.map(|root| root.expr.any(|node| matches!(node, LogicalExpr::Subquery(_))))
				.collect(),
		},
		oracle,
		next_frame: roots.iter().map(|root| root.frame.0 + 1).max().unwrap_or(0),
		root: 0,
		order: 0,
	};
	for root in roots {
		let id = builder.intern(root.expr, root.frame);
		builder.dag.roots.push(id);
	}
	for (index, root) in roots.iter().enumerate() {
		builder.root = index;
		builder.visit(root.expr, false);
	}
	builder.dag
}

impl<'a> ExprDag<'a> {
	/// The number of distinct nodes.
	pub fn len(&self) -> usize {
		self.nodes.len()
	}

	pub fn is_empty(&self) -> bool {
		self.nodes.is_empty()
	}

	/// The kind node `id` is proven to evaluate to against the open schema, or
	/// `None` when nothing proves one.
	pub fn checked_kind(&self, id: NodeId) -> Option<&Kind> {
		self.nodes[id.index()].checked_kind.as_ref()
	}

	/// The node written at `at`, a position of one of the roots, bodies
	/// included. The address is compared, never dereferenced.
	pub fn node_at(&self, at: *const LogicalExpr) -> Option<NodeId> {
		self.positions.get(&at).copied()
	}

	/// The kind the expression written at `at` is proven to evaluate to
	/// against the open schema, or `None` when nothing proves one or `at` is
	/// not a position of the region.
	pub fn kind_at(&self, at: &LogicalExpr) -> Option<&Kind> {
		self.checked_kind(self.node_at(at)?)
	}

	/// The node an expression equal to `probe`, evaluated against `frame`,
	/// would intern to, without interning anything. Every node of the probe is
	/// taken to be evaluated against `frame`, so a probe holding a body is
	/// answered only when its body nodes read nothing of the row.
	#[cfg(test)]
	pub fn find(&self, probe: &LogicalExpr, frame: Frame) -> Option<NodeId> {
		let mut children = Vec::new();
		probe.for_each_child(&mut |child| children.push(self.find(child, frame)));
		let children = children.into_iter().collect::<Option<Vec<_>>>()?;
		// Whether a node reads the row is a function of its shape, so at most
		// one of the two keys is interned.
		[None, Some(frame)].into_iter().find_map(|frame| {
			let key = NodeKey {
				expr: probe,
				children: children.clone(),
				frame,
			};
			self.table.get(&key).copied()
		})
	}

	/// Whether the value of node `id` is read by every later occurrence once
	/// its first unconditional occurrence has run: that occurrence exists, is
	/// in a root without a subquery, and the node is a pure, non-leaf
	/// expression.
	fn first_is_reusable(&self, id: NodeId) -> bool {
		let node = &self.nodes[id.index()];
		node.first.is_some_and(|first| {
			!self.holds_subquery[node.occurrences[first].root]
				&& !node.children.is_empty()
				&& node.effects.pure()
		})
	}

	/// The values worth evaluating once, by first occurrence in evaluation
	/// order.
	pub fn shared(&self) -> Vec<SharedValue> {
		let mut shared = self
			.nodes
			.iter()
			.enumerate()
			.filter(|(index, node)| {
				self.first_is_reusable(NodeId(*index as u32))
					&& node.first.is_some_and(|first| first + 1 < node.occurrences.len())
			})
			.map(|(_, node)| node)
			.collect::<Vec<_>>();
		shared.sort_by_key(|node| node.occurrences[node.first.unwrap_or(0)].order);
		shared
			.into_iter()
			.map(|node| {
				let first = node.first.unwrap_or(0);
				SharedValue {
					first: node.occurrences[first].at,
					reuses: node.occurrences[first + 1..]
						.iter()
						.map(|occurrence| occurrence.at)
						.collect(),
				}
			})
			.collect()
	}
}

#[cfg(test)]
mod tests {
	use super::{Frame, Reach, Root, SHALLOW_EQ_CALLS, SharedValue, build, reach};
	use crate::expr::logical::{
		EffectOracle, EffectSet, LogicalExpr, LogicalLiteral, Schema, effects_of, lower_expr,
		may_dereference,
	};
	use crate::expr::{BinaryOperator, Kind, Param};

	/// Every builtin is pure but `rand::*`; nothing runs a closure, and no
	/// call is evaluated against the row.
	struct Declared;

	impl EffectOracle for Declared {
		fn builtin(&self, name: &str) -> Option<EffectSet> {
			Some(if name.starts_with("rand::") {
				EffectSet::ENTROPY
			} else {
				EffectSet::PURE
			})
		}

		fn closure_args(&self, _name: &str) -> Option<&'static [usize]> {
			Some(&[])
		}

		fn method_always_dispatches(&self, _name: &str) -> bool {
			false
		}

		fn evaluates_against_row(&self, _name: &str) -> bool {
			false
		}

		fn permission_checked_param(&self, _name: &str) -> bool {
			false
		}
	}

	fn expr(text: &str) -> LogicalExpr {
		lower_expr(crate::syn::expr(text).expect("expression parses")).expect("expression lowers")
	}

	fn roots_in<'a>(exprs: &'a [LogicalExpr], frames: &[u32]) -> Vec<Root<'a>> {
		exprs
			.iter()
			.zip(frames)
			.map(|(expr, frame)| Root {
				expr,
				frame: Frame::new(*frame),
			})
			.collect()
	}

	fn shared_of(texts: &[&str]) -> (Vec<LogicalExpr>, usize, Vec<SharedValue>) {
		let exprs = texts.iter().map(|text| expr(text)).collect::<Vec<_>>();
		let (len, shared) = {
			let roots = roots_in(&exprs, &vec![0; exprs.len()]);
			let dag = build(&roots, &Declared);
			(dag.len(), dag.shared())
		};
		(exprs, len, shared)
	}

	fn at(expr: &LogicalExpr) -> *const LogicalExpr {
		expr as *const LogicalExpr
	}

	#[test]
	fn gvn_merges_identical_subtrees() {
		let (roots, len, shared) = shared_of(&["a * 2", "a * 2 + 1"]);
		// a, 2, a * 2, 1, a * 2 + 1
		assert_eq!(len, 5);
		let [value] = shared.as_slice() else {
			panic!("one shared value, got {shared:?}");
		};
		assert_eq!(value.first, at(&roots[0]));
		assert_eq!(value.reuses.len(), 1);
		let LogicalExpr::Binary {
			left,
			..
		} = &roots[1]
		else {
			panic!("a sum");
		};
		assert_eq!(value.reuses[0], at(left));
	}

	#[test]
	fn a_conditional_first_occurrence_is_not_shared() {
		let (_, _, shared) = shared_of(&["b AND a * 2 > 1", "a * 2"]);
		assert!(shared.is_empty(), "{shared:?}");
	}

	#[test]
	fn a_dominated_conditional_reuse_is_shared() {
		let (roots, _, shared) = shared_of(&["a * 2 > 1 AND a * 2 < 9"]);
		let [value] = shared.as_slice() else {
			panic!("one shared value, got {shared:?}");
		};
		let LogicalExpr::Binary {
			left,
			right,
			..
		} = &roots[0]
		else {
			panic!("a conjunction");
		};
		let operand = |side: &LogicalExpr| {
			let LogicalExpr::Binary {
				left,
				..
			} = side
			else {
				panic!("a comparison, got {side:?}");
			};
			at(left)
		};
		assert_eq!(value.first, operand(left));
		assert_eq!(value.reuses, vec![operand(right)]);
	}

	#[test]
	fn body_occurrences_do_not_count() {
		let (_, _, shared) = shared_of(&["arr[WHERE x * 2 > 1]", "x * 2"]);
		assert!(shared.is_empty(), "{shared:?}");
	}

	#[test]
	fn impure_is_never_shared() {
		let (_, _, shared) = shared_of(&["rand::int(1, 9) + 1", "rand::int(1, 9) + 1"]);
		assert!(shared.is_empty(), "{shared:?}");
	}

	#[test]
	fn a_dereferencing_read_is_never_shared() {
		let (_, _, shared) = shared_of(&["a.b + 1", "a.b + 1"]);
		assert!(shared.is_empty(), "{shared:?}");
	}

	#[test]
	fn dce_counts_only_reachable_nodes() {
		let unfolded = [expr("IF true THEN a * 2 ELSE b * 3 END")];
		let folded = [expr("a * 2")];
		let dead = expr("b * 3");
		let dag = build(&roots_in(&unfolded, &[0]), &Declared);
		assert!(dag.find(&dead, Frame::new(0)).is_some());
		let dag = build(&roots_in(&folded, &[0]), &Declared);
		assert!(dag.find(&dead, Frame::new(0)).is_none());
		// a, 2, a * 2
		assert_eq!(dag.len(), 3);
	}

	#[test]
	fn a_subquery_root_cannot_hold_a_first_occurrence() {
		let (_, _, shared) = shared_of(&["(SELECT * FROM t) + a * 2", "a * 2"]);
		assert!(shared.is_empty(), "{shared:?}");
	}

	#[test]
	fn the_nodes_of_a_reused_value_are_not_counted_again() {
		// `a * 2` is read inside the second sum, which a consumer never
		// evaluates, so only the sum is shared.
		let (_, _, shared) = shared_of(&["a * 2 + 1", "a * 2 + 1"]);
		assert_eq!(shared.len(), 1, "{shared:?}");
	}

	#[test]
	fn dag_records_each_nodes_proof() {
		let root = [expr("<int> a + (a > 1)")];
		let dag = build(&roots_in(&root, &[0]), &Declared);
		let schema = Schema::open();
		let mut compared = 0;
		for (index, node) in dag.nodes.iter().enumerate() {
			let id = super::NodeId(index as u32);
			assert_eq!(dag.checked_kind(id), node.expr.checked_kind(&schema).as_ref());
			compared += 1;
		}
		assert_eq!(compared, dag.len());
		let of = |text: &str| {
			let probe = expr(text);
			let id = dag.find(&probe, Frame::new(0)).expect("the probe is in the DAG");
			dag.checked_kind(id).cloned()
		};
		assert_eq!(of("a > 1"), Some(Kind::Bool));
		assert_eq!(of("a"), None);
		// A position answers through the address it is written at.
		let LogicalExpr::Binary {
			right,
			..
		} = &root[0]
		else {
			panic!("a sum");
		};
		assert_eq!(dag.kind_at(right), Some(&Kind::Bool));
		assert_eq!(dag.kind_at(&root[0]), None);
	}

	/// The expected [`Reach`] of each child of `sample`, written from the VM's
	/// lowering rather than from [`reach`]. A new variant does not compile
	/// until it is classified here.
	fn expected(sample: &LogicalExpr) -> Vec<Reach> {
		let count = sample.children().len();
		let all = |reach: Reach| vec![reach; count];
		// The first child is evaluated, the rest as `rest` says.
		let first_then = |rest: Reach| {
			(0..count)
				.map(|index| {
					if index == 0 {
						Reach::Always
					} else {
						rest
					}
				})
				.collect()
		};
		match sample {
			// `value_node` lowers each element in order, unconditionally.
			LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_) => all(Reach::Always),
			// The base is lowered, then one step applies; nothing is jumped over.
			LogicalExpr::Access {
				..
			}
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
			| LogicalExpr::Each(_)
			| LogicalExpr::Flatten {
				..
			}
			| LogicalExpr::Prefix {
				..
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Throw(_) => all(Reach::Always),
			// The index is lowered by `step_arg`, which rebinds the element to
			// the document root, so it reads another `Current` than its base.
			LogicalExpr::Index {
				..
			} => first_then(Reach::Body),
			// The predicate, the clauses, the aliases and the arguments run
			// against another value, or not at all, or more than once.
			LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Optional {
				..
			}
			| LogicalExpr::Lookup {
				..
			}
			| LogicalExpr::Recurse(_)
			| LogicalExpr::Broadcast {
				..
			} => first_then(Reach::Body),
			LogicalExpr::RepeatRecurse(_) | LogicalExpr::Closure(_) => all(Reach::Body),
			// `JumpIfNotObject` follows the base, and the value is lowered
			// after it.
			LogicalExpr::SetField {
				..
			} => first_then(Reach::Conditional),
			// Each condition and value sits behind a `JumpIf`, except the first
			// condition.
			LogicalExpr::If(_) => first_then(Reach::Conditional),
			// The right operand of a short-circuit operator follows a jump.
			LogicalExpr::Binary {
				op,
				..
			} => {
				if matches!(
					op,
					BinaryOperator::And
						| BinaryOperator::Or
						| BinaryOperator::NullCoalescing
						| BinaryOperator::TenaryCondition
				) {
					first_then(Reach::Conditional)
				} else {
					all(Reach::Always)
				}
			}
			// The arguments are lowered in order before the call.
			LogicalExpr::Call(_)
			| LogicalExpr::Invoke {
				..
			} => all(Reach::Always),
			LogicalExpr::Literal(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => Vec::new(),
		}
	}

	fn marker(index: usize) -> LogicalExpr {
		LogicalExpr::Param(Param::from(format!("c{index}")))
	}

	fn boxed(index: usize) -> Box<LogicalExpr> {
		Box::new(marker(index))
	}

	#[test]
	fn reach_is_pinned_per_variant() {
		let binary = |op| LogicalExpr::Binary {
			left: boxed(0),
			op,
			right: boxed(1),
		};
		let samples = vec![
			LogicalExpr::Array(vec![marker(0), marker(1)]),
			LogicalExpr::Set(vec![marker(0), marker(1)]),
			LogicalExpr::Object(vec![("a".into(), marker(0)), ("b".into(), marker(1))]),
			expr("t:[$c0, $c1]"),
			LogicalExpr::Access {
				base: boxed(0),
				field: "f".into(),
			},
			LogicalExpr::Index {
				base: boxed(0),
				index: boxed(1),
			},
			LogicalExpr::First(boxed(0)),
			LogicalExpr::Last(boxed(0)),
			LogicalExpr::Each(boxed(0)),
			LogicalExpr::Flatten {
				base: boxed(0),
				explicit: true,
			},
			LogicalExpr::Filter {
				base: boxed(0),
				predicate: boxed(1),
			},
			LogicalExpr::Method {
				base: boxed(0),
				name: "m".into(),
				args: vec![marker(1), marker(2)],
			},
			expr("$c0.{ a: $c1, b: $c2 }"),
			LogicalExpr::Optional {
				base: boxed(0),
				then: boxed(1),
			},
			expr("$c0->(likes WHERE x = $c1)"),
			expr("$c0.{2}(.a[$c1])"),
			LogicalExpr::RepeatRecurse(boxed(0)),
			LogicalExpr::Broadcast {
				base: boxed(0),
				body: boxed(1),
				flatten: false,
			},
			LogicalExpr::SetField {
				base: boxed(0),
				field: "k".into(),
				value: boxed(1),
			},
			LogicalExpr::Prefix {
				op: crate::expr::PrefixOperator::Negate,
				expr: boxed(0),
			},
			LogicalExpr::Postfix {
				expr: boxed(0),
				op: crate::expr::logical::LogicalPostfixOperator::Range,
			},
			LogicalExpr::Invoke {
				target: boxed(0),
				args: vec![marker(1)],
			},
			binary(BinaryOperator::Add),
			binary(BinaryOperator::And),
			binary(BinaryOperator::Or),
			binary(BinaryOperator::NullCoalescing),
			binary(BinaryOperator::TenaryCondition),
			expr("math::abs($c0)"),
			expr("|$x: int| $x + $c0"),
			expr("IF $c0 THEN $c1 ELSE IF $c2 THEN $c3 ELSE $c4 END"),
			LogicalExpr::Throw(boxed(0)),
		];
		for sample in &samples {
			let expected = expected(sample);
			assert_eq!(sample.children().len(), expected.len(), "{sample:?}");
			assert!(!expected.is_empty(), "{sample:?} has children");
			for (index, want) in expected.into_iter().enumerate() {
				assert_eq!(reach(sample, index), want, "child {index} of {sample:?}");
			}
		}
	}

	#[test]
	fn a_set_field_value_does_not_hold_a_first_occurrence() {
		let roots = [
			LogicalExpr::SetField {
				base: Box::new(expr("o")),
				field: "k".into(),
				value: Box::new(expr("a * 2")),
			},
			expr("a * 2"),
		];
		let dag = build(&roots_in(&roots, &[0, 0]), &Declared);
		assert!(dag.shared().is_empty(), "{:?}", dag.shared());
	}

	#[test]
	fn a_set_field_base_dominates_its_value() {
		let roots = [LogicalExpr::SetField {
			base: Box::new(expr("a * 2")),
			field: "k".into(),
			value: Box::new(expr("a * 2")),
		}];
		let dag = build(&roots_in(&roots, &[0]), &Declared);
		let LogicalExpr::SetField {
			base,
			value,
			..
		} = &roots[0]
		else {
			panic!("a set-field");
		};
		assert_eq!(
			dag.shared(),
			vec![SharedValue {
				first: at(base),
				reuses: vec![at(value)],
			}]
		);
	}

	#[test]
	fn an_if_condition_dominates_its_branches() {
		let roots = [expr("IF a * 2 > 1 THEN a * 2 ELSE 0 END")];
		let dag = build(&roots_in(&roots, &[0]), &Declared);
		let shared = dag.shared();
		let [value] = shared.as_slice() else {
			panic!("one shared value, got {shared:?}");
		};
		assert_eq!(value.reuses.len(), 1);
		let LogicalExpr::If(branches) = &roots[0] else {
			panic!("an IF");
		};
		let LogicalExpr::Binary {
			left,
			..
		} = &branches.branches[0].0
		else {
			panic!("a comparison");
		};
		assert_eq!(value.first, at(left));
		assert_eq!(value.reuses[0], at(&branches.branches[0].1));

		let (_, _, shared) = shared_of(&["IF b THEN 1 ELSE a * 2 END", "a * 2"]);
		assert!(shared.is_empty(), "{shared:?}");
	}

	#[test]
	fn a_conditional_occurrence_before_the_first_is_lowered_as_written() {
		let (roots, _, shared) = shared_of(&["b AND a * 2 > 1", "a * 2", "a * 2 + 1"]);
		let LogicalExpr::Binary {
			left,
			..
		} = &roots[2]
		else {
			panic!("a sum");
		};
		assert_eq!(
			shared,
			vec![SharedValue {
				first: at(&roots[1]),
				reuses: vec![at(left)],
			}]
		);
	}

	#[test]
	fn an_index_argument_is_a_body() {
		let (_, _, shared) = shared_of(&["arr[a * 2]", "a * 2"]);
		assert!(shared.is_empty(), "{shared:?}");
	}

	#[test]
	fn frames_separate_row_reads() {
		let roots = [expr("a * 2"), expr("a * 2")];
		let dag = build(&roots_in(&roots, &[0, 1]), &Declared);
		let probe = expr("a * 2");
		let (first, second) =
			(dag.find(&probe, Frame::new(0)).unwrap(), dag.find(&probe, Frame::new(1)).unwrap());
		assert_ne!(first, second);
		assert!(dag.shared().is_empty());

		let roots = [expr("$p * 2"), expr("$p * 2")];
		let dag = build(&roots_in(&roots, &[0, 1]), &Declared);
		let probe = expr("$p * 2");
		assert_eq!(dag.find(&probe, Frame::new(0)), dag.find(&probe, Frame::new(1)));
		assert_eq!(dag.shared().len(), 1);
	}

	#[test]
	fn a_body_read_never_merges_with_a_row_read() {
		let roots = [expr("arr[WHERE x * 2 > 1]"), expr("x * 2")];
		let dag = build(&roots_in(&roots, &[0, 0]), &Declared);
		let LogicalExpr::Filter {
			predicate,
			..
		} = &roots[0]
		else {
			panic!("a filter, got {:?}", roots[0]);
		};
		let LogicalExpr::Binary {
			left: in_body,
			..
		} = &**predicate
		else {
			panic!("a comparison");
		};
		let in_body = dag.node_at(at(in_body)).unwrap();
		let on_row = dag.node_at(at(&roots[1])).unwrap();
		assert_ne!(in_body, on_row);
		let field_of = |id: super::NodeId| dag.nodes[id.index()].children[0];
		assert_ne!(field_of(in_body), field_of(on_row));
	}

	#[test]
	fn facts_fold_from_children() {
		let corpus = [
			"a * 2",
			"a * 2 + 1",
			"b AND a * 2 > 1",
			"a * 2 > 1 AND a * 2 < 9",
			"arr[WHERE x * 2 > 1]",
			"rand::int(1, 9) + 1",
			"a.b + 1",
			"<int> a + (a > 1)",
			"<int> 1",
			"<int> (1 > 0)",
			"<string> (<int> v)",
			"(1 > 0) AND (2 > 1)",
			"(1 > 0) OR a",
			"IF true THEN a * 2 ELSE b * 3 END",
			"(SELECT * FROM t) + a * 2",
			"-v",
			"math::sqrt(v)",
			"o[1]",
			"[1, 2, a]",
			"[1, 2, t:1]",
			"{ a: 1, b: { c: t:1 } }",
			"{ x: a }.x",
			"{ x: 1 }.x.y",
			"(<object> $v).f",
			"(<string> b).x",
			"a.{ x, y: z }",
			"{ a: 1 }.{ a }",
			"t:1.f",
			"t:1.*",
			"a.*",
			"a[0]",
			"a[$]",
			"a.?.b",
			"->likes->person",
			"$this.f",
			"$parent.f + 1",
			"a ?? b",
			"a ?: b",
			"|$x: int| $x + 1",
			"$f(1)",
			"a.slice(1, 2)",
			"t:[1, 2]",
			"t:rand()",
			"1..5",
			"a..",
			"$p * 2",
			"fn::f(a) + 0",
		];
		let exprs = corpus.iter().map(|text| expr(text)).collect::<Vec<_>>();
		let dag = build(&roots_in(&exprs, &vec![0; exprs.len()]), &Declared);
		let schema = Schema::open();
		assert!(!dag.is_empty());
		for node in &dag.nodes {
			assert_eq!(node.effects, effects_of(node.expr, &Declared), "{:?}", node.expr);
			assert_eq!(node.checked_kind, node.expr.checked_kind(&schema), "{:?}", node.expr);
			assert_eq!(node.may_dereference, may_dereference(node.expr), "{:?}", node.expr);
		}
	}

	fn field(name: &str) -> LogicalExpr {
		LogicalExpr::Field(name.into())
	}

	fn binary(left: LogicalExpr, op: BinaryOperator, right: LogicalExpr) -> LogicalExpr {
		LogicalExpr::Binary {
			left: Box::new(left),
			op,
			right: Box::new(right),
		}
	}

	fn int(value: i64) -> LogicalExpr {
		LogicalExpr::Literal(LogicalLiteral::Integer(value))
	}

	/// `a{n} * 2 + 1`, three times over: depth seven.
	fn sum(n: usize) -> LogicalExpr {
		let mut node = field(&format!("a{n}"));
		for _ in 0..3 {
			node =
				binary(binary(node, BinaryOperator::Multiply, int(2)), BinaryOperator::Add, int(1));
		}
		node
	}

	#[test]
	fn build_is_linear() {
		const DISTINCT: usize = 5_000;
		let mut exprs = (0..DISTINCT).map(sum).collect::<Vec<_>>();
		exprs.extend((0..DISTINCT).map(sum));
		SHALLOW_EQ_CALLS.with(|calls| calls.set(0));
		let dag = build(&roots_in(&exprs, &vec![0; exprs.len()]), &Declared);
		let calls = SHALLOW_EQ_CALLS.with(|calls| calls.get());
		let visited: usize = exprs.iter().map(count).sum();
		assert!(calls <= 4 * visited, "{calls} comparisons over {visited} positions");
		// A field and six operators per sum, and the literals `2` and `1`.
		assert_eq!(dag.len(), DISTINCT * 7 + 2);
		assert_eq!(dag.shared().len(), DISTINCT);
	}

	fn count(expr: &LogicalExpr) -> usize {
		let mut positions = 0;
		let _ = expr.visit(&mut |_| {
			positions += 1;
			std::ops::ControlFlow::<()>::Continue(())
		});
		positions
	}
}
