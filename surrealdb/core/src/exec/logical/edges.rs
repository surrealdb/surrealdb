//! Which of two sibling statements are ordered, and why.
//!
//! Sibling statements, the statements of one
//! [`LogicalSequence`](crate::expr::logical::LogicalSequence) (a block, a
//! `FOR` body, an `IF` arm), are ordered by the text. An [`Edges`] records the
//! part of that order the program can observe; a pair with no edge may be
//! reordered or run in parallel.
//!
//! Edges are a pure function of the footprints and effects the caller supplies
//! and read no catalog. A caller deciding a rewrite passes each footprint as
//! [`expand`](super::footprint::expand) leaves it, as `plan_catalog::footprint` and
//! `speculate::footprint` produce it, and passes an arm's as
//! [`of_arms`](super::footprint::of_arms) gives it, expanded. A footprint whose reads are
//! `[Unknown]`, the writes-only form those helpers produce when the reads are
//! not wanted, is a valid and conservative input. [`of`](super::footprint::of) alone omits
//! what a table's events, views and reference repairs run, so it is sound only
//! where the caller knows the catalog runs none.
//!
//! The last statement of a sequence is its value: [`Dependence`] marks every
//! edge into it with [`Edges::TAIL`], so that no statement is moved after it.
//!
//! Statements of different transactions are always ordered, and a
//! `LogicalSequence` never spans two transactions, so a pair here is always of
//! one transaction.
//!
//! Edges are computed on demand for a pair, never stored for every pair: a
//! request may hold thousands of statements.

use super::footprint::{CatalogKind, Footprint, Resource};
use super::{effects_of_plan, plan_may_raise};
use crate::exec::function::FunctionRegistry;
use crate::expr::logical::{LogicalEval, LogicalIfOp, LogicalOp};

/// The reasons two sibling statements are ordered, as a set of flags.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Edges(u8);

impl Edges {
	/// The statements are not ordered.
	pub(crate) const NONE: Self = Self(0);
	/// The earlier statement writes what the later one reads.
	pub(crate) const FLOW: Self = Self(1 << 0);
	/// The earlier statement reads what the later one writes.
	pub(crate) const ANTI: Self = Self(1 << 1);
	/// Both write a common resource.
	pub(crate) const OUTPUT: Self = Self(1 << 2);
	/// One binds by `LET` a name the other binds or reads.
	pub(crate) const NAME: Self = Self(1 << 3);
	/// The pair must keep its order because one may fail, or draws a
	/// non-deterministic value, and the other is such a statement too or
	/// writes.
	pub(crate) const ORDER: Self = Self(1 << 4);
	/// The later statement is the sequence's last, whose value is the
	/// sequence's: no statement is moved after it. It does not order the
	/// two statements' runs, only their places.
	pub(crate) const TAIL: Self = Self(1 << 5);

	/// The set holding every flag of either operand.
	pub(crate) const fn union(self, other: Self) -> Self {
		Self(self.0 | other.0)
	}

	/// Whether every flag of `other` is in this set.
	pub(crate) const fn contains(self, other: Self) -> bool {
		self.0 & other.0 == other.0
	}

	/// Whether any flag of `other` is in this set.
	pub(crate) const fn intersects(self, other: Self) -> bool {
		self.0 & other.0 != 0
	}

	/// Whether the set holds no flag.
	pub(crate) const fn is_empty(self) -> bool {
		self.0 == 0
	}
}

/// The data edges from `earlier` to `later`: [`Edges::FLOW`], [`Edges::ANTI`]
/// and [`Edges::OUTPUT`], from the resources the footprints share.
///
/// `FLOW` is `later` reading what `earlier` writes, which is
/// [`Footprint::writes_over`] of each of its reads; `FLOW` or `OUTPUT` is
/// [`Footprint::conflicts`] of `earlier` with `later`.
pub(crate) fn data(earlier: &Footprint, later: &Footprint) -> Edges {
	let mut out = Edges::NONE;
	if earlier.writes.iter().any(|w| later.reads.iter().any(|r| w.overlaps(r))) {
		out = out.union(Edges::FLOW);
	}
	if earlier.reads.iter().any(|r| later.writes.iter().any(|w| r.overlaps(w))) {
		out = out.union(Edges::ANTI);
	}
	if earlier.writes.iter().any(|w| later.writes.iter().any(|v| w.overlaps(v))) {
		out = out.union(Edges::OUTPUT);
	}
	out
}

/// A sibling statement, summarised for [`between`].
pub(crate) struct Node {
	/// What the statement reads and writes.
	pub(crate) footprint: Footprint,
	/// The name the statement binds for its siblings: set only for a direct
	/// `LET`, since a nested sequence scopes its own.
	pub(crate) binds: Option<String>,
	/// Whether the statement must keep its place against every other pinned
	/// statement and every writer: it may fail, leave the block, or draw a
	/// value that depends on when it runs.
	pub(crate) pinned: bool,
	/// Whether the statement writes: a resource in its footprint, or a call
	/// that changes state outliving it.
	pub(crate) writes: bool,
}

impl Node {
	/// The summary of the statement `op`, whose footprint is `footprint`.
	pub(crate) fn of(op: &LogicalOp, footprint: Footprint, registry: &FunctionRegistry) -> Node {
		let binds = if let LogicalOp::Let(binding) = op {
			Some(binding.name.clone())
		} else {
			None
		};
		let (pinned, writes) = flags(op, &footprint, registry);
		Node {
			footprint,
			binds,
			pinned,
			writes,
		}
	}

	/// The summary of each arm of the statement `IF` `ifs`, in order, the
	/// `ELSE` arm last. `footprints` holds the footprint of each arm as
	/// [`of_arms`](super::footprint::of_arms) gives it, expanded. An arm's footprint, `pinned`
	/// and `writes` cover the arm and the conditions evaluated to reach it. No
	/// arm binds a name for the siblings of the `IF`.
	///
	/// # Panics
	///
	/// When `footprints` and the arms of `ifs` differ in length.
	#[cfg_attr(
		not(test),
		allow(
			dead_code,
			reason = "the per-arm summary RFC §3.4 defines; no rewrite in this build works inside one arm"
		)
	)]
	pub(crate) fn of_arms(
		ifs: &LogicalIfOp,
		footprints: Vec<Footprint>,
		registry: &FunctionRegistry,
	) -> Vec<Node> {
		assert_eq!(footprints.len(), ifs.arms.len(), "one footprint per arm");
		let mut cond_pinned = false;
		let mut cond_writes = false;
		footprints
			.into_iter()
			.zip(&ifs.arms)
			.enumerate()
			.map(|(at, (footprint, arm))| {
				if let Some(condition) = ifs.conditions.get(at) {
					let eval = LogicalOp::Eval(LogicalEval {
						expr: condition.clone(),
					});
					let effects = effects_of_plan(&eval, registry);
					cond_pinned |= may_fail(&eval, registry) || !effects.set.is_deterministic();
					cond_writes |= effects.set.mutates_state();
				}
				let (pinned, writes) = flags(arm, &footprint, registry);
				Node {
					footprint,
					binds: None,
					pinned: pinned || cond_pinned,
					writes: writes || cond_writes,
				}
			})
			.collect()
	}
}

/// Whether `op` is pinned and whether it writes, given its `footprint`.
fn flags(op: &LogicalOp, footprint: &Footprint, registry: &FunctionRegistry) -> (bool, bool) {
	let effects = effects_of_plan(op, registry);
	let writes = !footprint.writes.is_empty() || effects.set.mutates_state();
	let pinned = may_fail(op, registry) || !effects.set.is_deterministic();
	(pinned, writes)
}

/// Whether running `op` may raise an error or leave the block, which no
/// statement is moved across.
fn may_fail(op: &LogicalOp, registry: &FunctionRegistry) -> bool {
	// Only these kinds are known to fail never: everything else, a read of
	// storage, a write, a schema statement, a `THROW`, a wait, may. A `LET` with
	// a declared kind coerces its value to it and may fail.
	let kinds = op
		.visit(&mut |node| {
			if matches!(
				node,
				LogicalOp::Eval(_)
					| LogicalOp::Sequence(_)
					| LogicalOp::If(_)
					| LogicalOp::Values(_)
			) || matches!(node, LogicalOp::Let(binding) if binding.kind.is_none())
			{
				std::ops::ControlFlow::Continue(())
			} else {
				std::ops::ControlFlow::Break(())
			}
		})
		.is_break();
	if kinds {
		return true;
	}
	// Control leaving a statement is never moved across.
	let exits = op.exits();
	if !exits.falls_through || exits.returns || exits.breaks || exits.continues || exits.throws {
		return true;
	}
	plan_may_raise(op) || effects_of_plan(op, registry).impure
}

/// The edges from `earlier` to `later`: the union of their data edges, a name
/// clash, and the order a statement that may fail or draw a value keeps
/// against the other statements that do or that write.
///
/// A pair has no position, so the result never holds [`Edges::TAIL`]; see
/// [`Dependence::edges`].
pub(crate) fn between(earlier: &Node, later: &Node) -> Edges {
	let mut out = data(&earlier.footprint, &later.footprint);
	if names_clash(earlier, later) || names_clash(later, earlier) {
		out = out.union(Edges::NAME);
	}
	if (earlier.pinned && (later.pinned || later.writes)) || (later.pinned && earlier.writes) {
		out = out.union(Edges::ORDER);
	}
	out
}

/// Whether the name `binder` binds is bound or read by `other`. A name a
/// footprint cannot name, an unknown resource, counts as read.
fn names_clash(binder: &Node, other: &Node) -> bool {
	let Some(name) = &binder.binds else {
		return false;
	};
	other.binds.as_ref() == Some(name) || touches(&other.footprint, name)
}

/// Whether `fp` may read or write the parameter `name`.
fn touches(fp: &Footprint, name: &str) -> bool {
	let param = Resource::Catalog {
		kind: CatalogKind::Param,
		table: None,
		name: Some(name.to_owned()),
	};
	fp.reads.iter().chain(&fp.writes).any(|r| r.overlaps(&param))
}

/// The statements of one sequence, from which the edges between any two are
/// answered. Every edge into the last statement holds [`Edges::TAIL`], since
/// its value is the sequence's; this covers a `FOR` body too, whose value is
/// discarded, which is conservative.
#[cfg_attr(
	not(test),
	allow(
		dead_code,
		reason = "the sibling-statement dependence RFC §3.4 defines; no rewrite in this build reorders sibling statements"
	)
)]
pub(crate) struct Dependence {
	nodes: Vec<Node>,
}

#[cfg_attr(
	not(test),
	allow(
		dead_code,
		reason = "the sibling-statement dependence RFC §3.4 defines; no rewrite in this build reorders sibling statements"
	)
)]
impl Dependence {
	/// The summary of each of `stmts`, the statements of one sequence, with
	/// `footprints` as their footprints, one each.
	///
	/// # Panics
	///
	/// When `footprints` and `stmts` differ in length.
	pub(crate) fn of_sequence(
		stmts: &[LogicalOp],
		footprints: Vec<Footprint>,
		registry: &FunctionRegistry,
	) -> Self {
		assert_eq!(footprints.len(), stmts.len(), "one footprint per statement");
		Self {
			nodes: stmts
				.iter()
				.zip(footprints)
				.map(|(stmt, footprint)| Node::of(stmt, footprint, registry))
				.collect(),
		}
	}

	/// The edges from the statement at `earlier` to the one at `later`, with
	/// [`Edges::TAIL`] when `later` is the last statement.
	///
	/// # Panics
	///
	/// When `earlier < later < self.len()` does not hold.
	pub(crate) fn edges(&self, earlier: usize, later: usize) -> Edges {
		assert!(
			earlier < later && later < self.nodes.len(),
			"edges({earlier}, {later}) of {} statements",
			self.nodes.len()
		);
		let edges = between(&self.nodes[earlier], &self.nodes[later]);
		if later + 1 == self.nodes.len() {
			edges.union(Edges::TAIL)
		} else {
			edges
		}
	}

	/// Whether the statement at `later` is ordered after the one at `earlier`.
	pub(crate) fn depends(&self, earlier: usize, later: usize) -> bool {
		!self.edges(earlier, later).is_empty()
	}

	/// The number of statements.
	pub(crate) fn len(&self) -> usize {
		self.nodes.len()
	}
}

#[cfg(test)]
mod tests {
	use super::super::footprint;
	use super::*;

	const DATA: Edges = Edges::FLOW.union(Edges::ANTI).union(Edges::OUTPUT);

	fn registry() -> FunctionRegistry {
		FunctionRegistry::with_builtins()
	}

	fn node(sql: &str) -> Node {
		let registry = registry();
		let op = super::super::lower_test(sql);
		Node::of(&op, footprint::of(&op, &registry), &registry)
	}

	fn dependence(sql: &str) -> Dependence {
		let registry = registry();
		let stmts = block(sql);
		let footprints = stmts.iter().map(|stmt| footprint::of(stmt, &registry)).collect();
		Dependence::of_sequence(&stmts, footprints, &registry)
	}

	fn block(sql: &str) -> Vec<LogicalOp> {
		let LogicalOp::Sequence(sequence) = super::super::lower_test(sql) else {
			panic!("a block");
		};
		sequence.stmts
	}

	/// The data edges between two statements.
	fn pair(earlier: &str, later: &str) -> Edges {
		data(&node(earlier).footprint, &node(later).footprint)
	}

	fn none(edges: Edges) -> bool {
		!edges.intersects(DATA)
	}

	#[test]
	fn data_same_table_and_other_table() {
		assert!(pair("CREATE a:1", "SELECT * FROM a").contains(Edges::FLOW));
		assert!(none(pair("CREATE a:1", "SELECT * FROM b")));
		assert!(pair("SELECT * FROM a", "CREATE a:1").contains(Edges::ANTI));
		assert!(pair("CREATE a:1", "UPDATE a:2 SET x = 1").contains(Edges::OUTPUT));
	}

	#[test]
	fn any_data() {
		assert!(pair("DELETE a:1", "SELECT * FROM b").contains(Edges::FLOW));
	}

	#[test]
	fn catalog_table_covers_its_definitions() {
		for later in [
			"DEFINE FIELD f ON t",
			"DEFINE INDEX i ON t FIELDS f",
			"DEFINE EVENT e ON t WHEN true THEN {}",
			"DEFINE TABLE v AS SELECT f FROM t",
		] {
			assert!(
				pair("DEFINE TABLE t", later).intersects(Edges::FLOW.union(Edges::OUTPUT)),
				"{later}"
			);
		}
		assert!(none(pair("DEFINE TABLE u", "DEFINE FIELD f ON t")));
	}

	#[test]
	fn catalog_function_by_name() {
		assert!(none(pair("DEFINE FUNCTION fn::a() { 1 }", "DEFINE FUNCTION fn::b() { 2 }")));
		assert!(
			pair("DEFINE FUNCTION fn::a() { 1 }", "REMOVE FUNCTION fn::a").contains(Edges::OUTPUT)
		);
	}

	#[test]
	fn catalog_param_by_name() {
		assert!(pair("DEFINE PARAM $p VALUE 1", "RETURN $p").contains(Edges::FLOW));
		assert!(none(pair("DEFINE PARAM $p VALUE 1", "DEFINE PARAM $q VALUE 2")));
	}

	#[test]
	fn catalog_access() {
		let access = "DEFINE ACCESS a ON DATABASE TYPE RECORD";
		let user = "DEFINE USER u ON DATABASE PASSWORD 'x' ROLES VIEWER";
		assert!(pair(access, user).contains(Edges::OUTPUT));
		for earlier in [access, user] {
			assert!(none(pair(earlier, "DEFINE PARAM $q VALUE 2")), "{earlier}");
		}
	}

	#[test]
	fn catalog_other() {
		let analyzer = "DEFINE ANALYZER x TOKENIZERS blank";
		assert!(pair(analyzer, "DEFINE SEQUENCE s").contains(Edges::OUTPUT));
		assert!(pair(analyzer, "INFO FOR DB").contains(Edges::FLOW));
		assert!(none(pair(analyzer, "DEFINE PARAM $q VALUE 2")));
	}

	#[test]
	fn unknown_overlaps_everything() {
		assert!(pair("DEFINE DATABASE d", "SELECT * FROM a").contains(Edges::FLOW));
		assert!(pair("RETURN fn::f()", "SELECT * FROM a").contains(Edges::FLOW));
	}

	#[test]
	fn let_binding() {
		let dependence = dependence("{ LET $x = 1; LET $y = 2; RETURN $x; LET $x = 3; }");
		assert_eq!(dependence.len(), 4);
		assert_eq!(dependence.edges(0, 1), Edges::NONE);
		assert!(dependence.edges(0, 2).contains(Edges::NAME));
		assert!(!dependence.edges(1, 2).contains(Edges::NAME));
		assert!(dependence.edges(0, 3).contains(Edges::NAME), "a rebinding");
		assert!(dependence.edges(2, 3).contains(Edges::NAME), "a read before a rebinding");
		assert!(dependence.depends(0, 2));
		assert!(!dependence.depends(0, 1));
	}

	#[test]
	fn if_arm_versus_union() {
		let registry = registry();
		let LogicalOp::If(ifs) = super::super::lower_test(
			"IF (SELECT * FROM c) { UPDATE a SET x = 1 } ELSE { SELECT * FROM b }",
		) else {
			panic!("a statement IF");
		};
		let later = node("SELECT * FROM a");
		let whole_op = LogicalOp::If(ifs.clone());
		let whole = Node::of(&whole_op, footprint::of(&whole_op, &registry), &registry);
		assert!(data(&whole.footprint, &later.footprint).contains(Edges::FLOW));
		let arms = Node::of_arms(&ifs, footprint::of_arms(&ifs, &registry), &registry);
		assert_eq!(arms.len(), 2);
		assert!(data(&arms[0].footprint, &later.footprint).contains(Edges::FLOW));
		assert!(!data(&arms[1].footprint, &later.footprint).contains(Edges::FLOW));
		assert!(arms.iter().all(|arm| arm.binds.is_none()));
	}

	#[test]
	fn failing_statements_keep_their_order() {
		assert!(between(&node("THROW 'a'"), &node("THROW 'b'")).contains(Edges::ORDER));
	}

	#[test]
	fn a_failing_statement_keeps_its_place_before_a_write() {
		assert!(between(&node("THROW 'a'"), &node("CREATE z:1")).contains(Edges::ORDER));
	}

	#[test]
	fn non_deterministic_keeps_its_place() {
		let dependence =
			dependence("{ LET $a = rand(); LET $b = rand(); LET $c = 1; CREATE z:1; 1; }");
		assert!(dependence.edges(0, 1).contains(Edges::ORDER));
		assert_eq!(dependence.edges(0, 2), Edges::NONE);
		assert!(dependence.edges(0, 3).contains(Edges::ORDER));
		assert_eq!(dependence.edges(2, 3), Edges::NONE, "a pure LET neither fails nor writes");
	}

	#[test]
	fn pure_statements_are_independent() {
		let dependence = dependence("{ LET $a = 1; LET $b = 2; 1 }");
		assert_eq!(dependence.edges(0, 1), Edges::NONE);
	}

	#[test]
	fn a_typed_let_may_fail() {
		assert!(between(&node("CREATE a:1"), &node("LET $x: int = 'a'")).contains(Edges::ORDER));
		assert!(!between(&node("CREATE a:1"), &node("LET $x = 'a'")).contains(Edges::ORDER));
		assert!(between(&node("LET $x: int = 'a'"), &node("THROW 'b'")).contains(Edges::ORDER));
		assert!(
			between(&node("CREATE a:1"), &node("IF true { LET $x: int = 'a' }"))
				.contains(Edges::ORDER)
		);
	}

	#[test]
	fn writes_to_disjoint_tables_are_ordered() {
		let edges = between(&node("CREATE s:1 SET n = -1"), &node("CREATE t:2 SET n = 1"));
		assert!(none(edges));
		assert!(edges.contains(Edges::ORDER));
	}

	#[test]
	fn a_let_wrapping_a_write_is_ordered() {
		let registry = registry();
		for sql in ["LET $x = CREATE c:1", "LET $x = (CREATE c:1)"] {
			assert!(between(&node(sql), &node("THROW 'b'")).contains(Edges::ORDER), "{sql}");
			assert!(between(&node("SELECT * FROM c"), &node(sql)).contains(Edges::ANTI), "{sql}");
			let op = super::super::lower_test(sql);
			assert!(Node::of(&op, footprint::of(&op, &registry), &registry).writes, "{sql}");
		}
	}

	#[test]
	fn an_arm_carries_its_conditions() {
		let registry = registry();
		let LogicalOp::If(ifs) = super::super::lower_test("IF <int> $x { 1 } ELSE { 2 }") else {
			panic!("a statement IF");
		};
		let arms = Node::of_arms(&ifs, footprint::of_arms(&ifs, &registry), &registry);
		assert_eq!(arms.len(), 2);
		assert!(arms.iter().all(|arm| arm.pinned));
		let LogicalOp::If(ifs) = super::super::lower_test("IF (CREATE a:1) { 1 } ELSE { 2 }")
		else {
			panic!("a statement IF");
		};
		let arms = Node::of_arms(&ifs, footprint::of_arms(&ifs, &registry), &registry);
		assert_eq!(arms.len(), 2);
		assert!(arms.iter().all(|arm| arm.writes));
		assert!(between(&arms[0], &node("THROW 'b'")).contains(Edges::ORDER));
	}

	#[test]
	fn the_tail_stays_last() {
		assert_eq!(dependence("{ 1; 2 }").edges(0, 1), Edges::TAIL);
		let lets = dependence("{ LET $a = 1; LET $b = 2; $a }");
		assert_eq!(lets.edges(1, 2), Edges::TAIL);
		assert!(lets.edges(0, 2).contains(Edges::TAIL));
		assert!(lets.edges(0, 2).contains(Edges::NAME));
		assert_eq!(lets.edges(0, 1), Edges::NONE);
		assert_eq!(between(&node("1"), &node("2")), Edges::NONE);
	}

	#[test]
	#[should_panic]
	fn reversed_indices_panic() {
		dependence("{ 1; 2 }").edges(1, 0);
	}

	#[test]
	fn an_expanded_write_footprint_makes_a_writer() {
		let registry = registry();
		let op = super::super::lower_test("1");
		let footprint = Footprint {
			reads: Vec::new(),
			writes: vec![Resource::Unknown],
		};
		let writer = Node::of(&op, footprint, &registry);
		assert!(writer.writes);
		assert!(between(&writer, &node("THROW 'b'")).contains(Edges::ORDER));
	}

	#[test]
	fn two_reads_of_storage_are_ordered() {
		let edges = between(&node("SELECT * FROM a"), &node("SELECT * FROM b"));
		assert!(edges.contains(Edges::ORDER));
		assert!(none(edges));
	}

	#[test]
	fn identities_used_by_callers() {
		let registry = registry();
		let footprints: Vec<Footprint> = [
			"CREATE a:1",
			"SELECT * FROM a",
			"DEFINE INDEX u ON t FIELDS n UNIQUE",
			"CREATE t:1 SET n = 1",
			"SELECT author.name FROM post",
		]
		.into_iter()
		.map(|sql| footprint::of(&super::super::lower_test(sql), &registry))
		.collect();
		for x in &footprints {
			for y in &footprints {
				let edges = data(x, y);
				assert_eq!(edges.intersects(Edges::FLOW.union(Edges::OUTPUT)), x.conflicts(y));
				assert_eq!(edges.contains(Edges::FLOW), y.reads.iter().any(|r| x.writes_over(r)));
			}
		}
	}
}
