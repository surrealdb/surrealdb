//! The declarative IR for GQL v2 `MATCH` queries.
//!
//! `MatchPlan` is the language-neutral binding-table plan node produced by the
//! GQL lowering and embedded into the logical plan as [`Expr::Match`]. The
//! streaming execution planner (`exec/planner/match_plan.rs`) compiles it into a
//! tree of physical operators; it never runs under the compute-only planner.
//!
//! See `doc/gql/V2_DESIGN.md` §2 for the normative contract.

use common::fmt::EscapeIdent;
use surrealdb_strand::TableName;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Expr, Idiom};

/// Index into [`MatchPlan::bindings`]; identifies a binding by position.
///
/// A raw `u32` index (not a newtype) by design: it is crate-private, never
/// crosses an API boundary, and the lowering guarantees every emitted id is a
/// valid `bindings` position (see the `MatchPlan` invariants below), so the
/// extra wrapping would buy no safety here.
pub type BindingId = u32;

/// The declarative plan for a single GQL `MATCH` query.
///
/// Invariants the lowering guarantees (the planner relies on these and never
/// re-derives them):
/// - every [`Expr`] is BINDING-ROW scoped (`a.x` → `Idiom[Field("a"),Field("x")]`), with 3VL guards
///   already inserted;
/// - [`MatchPredicate::deps`] is the exact set of bindings the expr reads;
/// - conjuncts are NNF-split and live on the clause whose pattern scope owns them (critical for
///   OPTIONAL);
/// - column names are final (naming rules applied; duplicates rejected);
/// - ORDER BY aliases are resolved (non-DISTINCT → source exprs; DISTINCT → columns);
/// - repeated pattern variables are rewritten to hidden bindings + equality conjuncts; anonymous
///   edges needing DIFFERENT-EDGES tracking have hidden bindings;
/// - every pattern is anchorable (rule in V2_DESIGN §0).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MatchPlan {
	/// All bindings; index equals the [`BindingId`].
	pub bindings: Vec<BindingDef>,
	/// The query's steps in textual order: read clauses and write stages,
	/// interleaved as written. May be empty only for a bare `RETURN` (rejected
	/// in lowering).
	pub stages: Vec<MatchStage>,
	/// The `RETURN` projection, or `None` for a mutation-only query (no
	/// trailing `RETURN`).
	pub output: Option<MatchOutput>,
}

impl MatchPlan {
	/// Whether this plan carries any write stage. Drives the transaction type
	/// (a mutation-bearing plan is not read-only, so the executor opens a write
	/// transaction) — see `Expr::read_only`.
	pub fn has_mutations(&self) -> bool {
		self.stages.iter().any(|s| matches!(s, MatchStage::Mutate(_)))
	}
}

/// One step of a lowered query, in textual order: a read clause that extends the
/// binding table, or a write stage that mutates it. A read clause after a write
/// re-reads the live (post-write) state in the same transaction.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MatchStage {
	/// A `MATCH`/`OPTIONAL` clause.
	Read(MatchClausePlan),
	/// A write stage applied to the binding table.
	Mutate(MutationStage),
}

/// One write stage applied to the binding table, in textual order.
///
/// A stage consumes the binding rows produced by earlier steps (or a single
/// empty row, for a leading `INSERT`), performs its write through the native
/// document pipeline, and passes the rows on so a trailing `RETURN` can
/// project the post-mutation state.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MutationStage {
	/// `SET a.p = v` / `SET a = {…}` / `REMOVE a.p`: mutate the record bound at
	/// `target`.
	Update {
		target: BindingId,
		data: UpdateData,
	},
	/// `[DETACH|NODETACH] DELETE a`: delete the record bound at `target`.
	Delete {
		target: BindingId,
		detach: DetachMode,
	},
	/// `INSERT …`: create the new nodes and relate the new edges.
	Insert(InsertStage),
}

/// The data a [`MutationStage::Update`] applies. Every [`Expr`] is binding-row
/// scoped (the same `a.x → Idiom[Field("a"),Field("x")]` rule the read side
/// uses) and is evaluated against the current binding row before the write.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum UpdateData {
	/// `SET a.p = v` (one assignment per item): set each field path.
	Set(Vec<(Idiom, Expr)>),
	/// `REMOVE a.p` (one path per item): unset each field.
	Unset(Vec<Idiom>),
	/// `SET a = {…}`: replace all user properties with the object expression
	/// (the record's `id`, and an edge's `in`/`out`, are preserved by the
	/// native `CONTENT` path).
	Content(Expr),
}

/// The detach mode of a [`MutationStage::Delete`]. `NoDetach` (the ISO default)
/// errors if the node still has connected edges; `Detach` cascades them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DetachMode {
	Detach,
	NoDetach,
}

/// An `INSERT` write stage: the new nodes to create (in creation order) and the
/// edges to relate once their endpoints exist.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InsertStage {
	pub nodes: Vec<InsertNodePlan>,
	pub edges: Vec<InsertEdgePlan>,
}

/// A new node created by an `INSERT` stage.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InsertNodePlan {
	/// The binding the created record is bound under.
	pub binding: BindingId,
	/// The target table (= label).
	pub label: TableName,
	/// The property object expression (binding-row scoped), evaluated per row.
	pub props: Expr,
}

/// A new edge related by an `INSERT` stage between two node bindings (each of
/// which is either a node created by this stage or one already bound by the
/// read body).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InsertEdgePlan {
	/// The binding the created edge record is bound under.
	pub binding: BindingId,
	/// The edge table (= label).
	pub label: TableName,
	/// The source endpoint node binding.
	pub from: BindingId,
	/// The target endpoint node binding.
	pub to: BindingId,
	/// The property object expression (binding-row scoped), evaluated per row.
	pub props: Expr,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BindingDef {
	pub name: String,
	pub kind: BindingKind,
	/// `true` if the user wrote this binding; `false` for hidden `__e<n>`/`__v<n>`.
	pub user_named: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BindingKind {
	Node,
	Edge,
	EdgeGroup,
	Path,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MatchClausePlan {
	/// The all-or-nothing OPTIONAL block this clause belongs to (R3), or `None`
	/// for a mandatory clause. Whether a clause is optional is *exactly*
	/// `optional_group.is_some()` (see [`MatchClausePlan::is_optional`]) — the
	/// single source of truth, never a separate flag the two could drift apart.
	///
	/// Every clause lowered from one `OPTIONAL { MATCH …; MATCH … }` (or one plain
	/// `OPTIONAL MATCH …`, which is a block of one) shares the same id. The planner
	/// left-joins all clauses of one group as a SINGLE unit (it must NOT join them
	/// one inner clause at a time), so a block matches all-or-nothing; distinct ids
	/// on adjacent optional clauses mean they chain left-to-right as independent
	/// left-joins. The id is an opaque, per-query dense counter (group order ==
	/// textual order); only equality is meaningful.
	pub optional_group: Option<u32>,
	pub patterns: Vec<PatternPlan>,
	/// Clause-owned NNF conjuncts.
	pub predicates: Vec<MatchPredicate>,
}

impl MatchClausePlan {
	/// Whether this clause is the body of an `OPTIONAL` operand (a left-outer join
	/// against the accumulated binding table, R3) — exactly when it carries an
	/// [`MatchClausePlan::optional_group`] id.
	pub fn is_optional(&self) -> bool {
		self.optional_group.is_some()
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PatternPlan {
	/// Binding for the whole path value (kind == [`BindingKind::Path`]).
	pub path_var: Option<BindingId>,
	/// The lowered path-search / path-mode prefix, or `None` when the pattern
	/// carried no prefix (the default: every path, edge-unique `WALK`). A search
	/// other than `All`/`None` is only ever set on a pattern with exactly one
	/// quantified segment, anchored forward on its start (the lowering enforces
	/// both), so the executing operator's source is the pattern's start node.
	pub search: Option<PathPrefixPlan>,
	pub start: NodeStep,
	/// Multi-hop chain: each step is an edge followed by the node it reaches.
	pub steps: Vec<(EdgeStep, NodeStep)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeStep {
	pub binding: BindingId,
	pub label: Option<TableName>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EdgeStep {
	/// Edge binding, or an [`BindingKind::EdgeGroup`] binding when quantified.
	pub binding: BindingId,
	pub label: Option<TableName>,
	pub direction: ExpandDirection,
	pub quantifier: Option<EdgeQuantifier>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExpandDirection {
	Out,
	In,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EdgeQuantifier {
	pub min: u32,
	pub max: Option<u32>,
}

/// The lowered path-search prefix of a [`PatternPlan`] (`doc/gql/V2_DESIGN.md`).
/// `None` on the pattern means no prefix was written (the default: every path,
/// edge-unique `WALK`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PathPrefixPlan {
	pub search: PathSearch,
	pub mode: PathMode,
}

/// The lowered path-search selector. The AST's optional path/group counts are
/// resolved here (omitted ⇒ 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PathSearch {
	/// Every path (explicit `ALL`, or a bare path-mode prefix).
	All,
	/// Any `count` paths (`ANY [k]`).
	Any {
		count: u32,
	},
	/// Every minimum-length path (`ALL SHORTEST`).
	AllShortest,
	/// One minimum-length path (`ANY SHORTEST`).
	AnyShortest,
	/// The `count` shortest paths (`SHORTEST k`).
	ShortestCounted {
		count: u32,
	},
	/// Every path in the `count` smallest length groups (`SHORTEST [k] GROUP(S)`).
	ShortestGroups {
		count: u32,
	},
}

/// The lowered path mode. `Walk` and `Trail` are equivalent under SurrealDB's
/// fixed DIFFERENT EDGES match mode (R2 forbids an edge binding twice within a
/// path), so the planner maps both to edge-unique traversal; `Simple`/`Acyclic`
/// additionally forbid repeated nodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PathMode {
	Walk,
	Trail,
	Simple,
	Acyclic,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MatchPredicate {
	pub expr: Expr,
	/// Sorted and deduped set of bindings the expression reads.
	pub deps: Vec<BindingId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MatchOutput {
	/// Explicit output columns; `RETURN *` is pre-expanded by the lowering.
	pub columns: Vec<MatchColumn>,
	pub distinct: bool,
	/// Aggregation, if any. `None` means no aggregation (the columns project
	/// straight from the binding rows). `Some(keys)` means the binding table is
	/// folded by the (binding-row scoped) group-key expressions before
	/// projection — an empty `keys` is `GROUP ALL` (a single group over all
	/// rows, e.g. a bare `RETURN count(*)`). Each non-key column carries an
	/// aggregate; the planner inserts an `Aggregate` operator in place of the
	/// projection.
	pub group_by: Option<Vec<Expr>>,
	pub order: Vec<MatchOrder>,
	pub skip: Option<Expr>,
	pub limit: Option<Expr>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MatchColumn {
	pub name: String,
	pub expr: Expr,
	/// A sort-only column materialised by an aggregating query so a non-projected
	/// ORDER BY key (a grouping key, functionally-dependent value, or aggregate)
	/// can be sorted on; the planner emits it from the `Aggregate` operator and
	/// drops it with a final projection before the rows are returned. Always
	/// `false` for user-projected columns.
	pub hidden: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MatchOrder {
	pub expr: Expr,
	pub ascending: bool,
}

impl MatchPlan {
	/// Look up a binding by id.
	///
	/// # Panics
	/// Panics if `id` is not a valid index into [`MatchPlan::bindings`]. Callers
	/// (the streaming planner) only ever pass ids that the lowering placed into
	/// this same plan's `bindings`/patterns, so the index is always in range; an
	/// out-of-range id is an internal-consistency bug, not a user-reachable path.
	/// For the never-panicking rendering path use [`MatchPlan::binding_name`].
	pub fn binding(&self, id: BindingId) -> &BindingDef {
		&self.bindings[id as usize]
	}

	/// The name of a binding by id, or `"?"` for an out-of-range id.
	///
	/// Used by the never-panicking [`ToSql`] rendering, so it tolerates a
	/// malformed plan rather than indexing.
	fn binding_name(&self, id: BindingId) -> &str {
		self.bindings.get(id as usize).map(|b| b.name.as_str()).unwrap_or("?")
	}
}

impl EdgeQuantifier {
	/// `true` if this quantifier is exactly `{n}` (a fixed repeat count).
	pub fn is_exact(&self) -> bool {
		self.max == Some(self.min)
	}
}

impl ExpandDirection {
	/// The reverse direction (used when anchoring mid-pattern).
	pub fn reverse(self) -> Self {
		match self {
			ExpandDirection::Out => ExpandDirection::In,
			ExpandDirection::In => ExpandDirection::Out,
		}
	}
}

/// Deterministic GQL-ish rendering of a [`MatchPlan`].
///
/// Used by EXPLAIN, `Debug`-adjacent tooling, and logs: it must be stable and
/// must never panic on any plan, well-formed or not. Predicate, column, order,
/// and skip/limit slots delegate to [`Expr`]'s `ToSql` so 3VL guard shapes stay
/// textually diffable. The rendering is single-line regardless of `fmt`.
impl ToSql for MatchPlan {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		// Steps render in textual order, space-separated (reads and mutations
		// interleaved as written).
		for (i, stage) in self.stages.iter().enumerate() {
			if i > 0 {
				f.push(' ');
			}
			match stage {
				MatchStage::Read(clause) => self.render_clause(f, clause),
				MatchStage::Mutate(mutation) => self.render_mutation(f, mutation),
			}
		}

		let Some(output) = self.output.as_ref() else {
			return;
		};

		if !self.stages.is_empty() {
			f.push(' ');
		}
		f.push_str("RETURN");
		if output.distinct {
			f.push_str(" DISTINCT");
		}
		for (i, column) in output.columns.iter().enumerate() {
			if i > 0 {
				f.push(',');
			}
			f.push(' ');
			column.expr.fmt_sql(f, SqlFormat::SingleLine);
			f.push_str(" AS ");
			f.push_str(&column.name);
		}

		// `Some([])` is `GROUP ALL` and renders nothing extra (the aggregates in
		// the columns already imply it); `Some(keys)` renders `GROUP BY …`.
		if let Some(keys) = output.group_by.as_ref()
			&& !keys.is_empty()
		{
			f.push_str(" GROUP BY");
			for (i, key) in keys.iter().enumerate() {
				if i > 0 {
					f.push(',');
				}
				f.push(' ');
				key.fmt_sql(f, SqlFormat::SingleLine);
			}
		}

		if !output.order.is_empty() {
			f.push_str(" ORDER BY");
			for (i, order) in output.order.iter().enumerate() {
				if i > 0 {
					f.push(',');
				}
				f.push(' ');
				order.expr.fmt_sql(f, SqlFormat::SingleLine);
				f.push_str(if order.ascending {
					" ASC"
				} else {
					" DESC"
				});
			}
		}

		if let Some(skip) = output.skip.as_ref() {
			f.push_str(" SKIP ");
			skip.fmt_sql(f, SqlFormat::SingleLine);
		}
		if let Some(limit) = output.limit.as_ref() {
			f.push_str(" LIMIT ");
			limit.fmt_sql(f, SqlFormat::SingleLine);
		}
	}
}

impl MatchPlan {
	/// Render one read clause as `[OPTIONAL ] MATCH <patterns> [WHERE <preds>]`
	/// (no trailing space; the step loop joins with single spaces).
	fn render_clause(&self, f: &mut String, clause: &MatchClausePlan) {
		if clause.is_optional() {
			f.push_str("OPTIONAL ");
		}
		f.push_str("MATCH ");
		for (i, pattern) in clause.patterns.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			self.render_pattern(f, pattern);
		}
		if !clause.predicates.is_empty() {
			f.push_str(" WHERE ");
			for (i, predicate) in clause.predicates.iter().enumerate() {
				if i > 0 {
					f.push_str(" AND ");
				}
				predicate.expr.fmt_sql(f, SqlFormat::SingleLine);
			}
		}
	}

	/// Render one mutation stage in a deterministic GQL-ish form (EXPLAIN /
	/// logs). Never panics: out-of-range bindings render as `?` via
	/// [`MatchPlan::binding_name`].
	fn render_mutation(&self, f: &mut String, stage: &MutationStage) {
		match stage {
			MutationStage::Update {
				target,
				data,
			} => match data {
				UpdateData::Set(assignments) => {
					f.push_str("SET");
					for (i, (place, value)) in assignments.iter().enumerate() {
						if i > 0 {
							f.push(',');
						}
						f.push(' ');
						place.fmt_sql(f, SqlFormat::SingleLine);
						f.push_str(" = ");
						value.fmt_sql(f, SqlFormat::SingleLine);
					}
				}
				UpdateData::Unset(fields) => {
					f.push_str("REMOVE");
					for (i, place) in fields.iter().enumerate() {
						if i > 0 {
							f.push(',');
						}
						f.push(' ');
						place.fmt_sql(f, SqlFormat::SingleLine);
					}
				}
				UpdateData::Content(expr) => {
					f.push_str("SET ");
					f.push_str(self.binding_name(*target));
					f.push_str(" = ");
					expr.fmt_sql(f, SqlFormat::SingleLine);
				}
			},
			MutationStage::Delete {
				target,
				detach,
			} => {
				if matches!(detach, DetachMode::Detach) {
					f.push_str("DETACH ");
				}
				f.push_str("DELETE ");
				f.push_str(self.binding_name(*target));
			}
			MutationStage::Insert(stage) => {
				f.push_str("INSERT");
				for node in stage.nodes.iter() {
					f.push_str(" (");
					f.push_str(self.binding_name(node.binding));
					f.push(':');
					EscapeIdent(node.label.as_str()).fmt_sql(f, SqlFormat::SingleLine);
					f.push(' ');
					node.props.fmt_sql(f, SqlFormat::SingleLine);
					f.push(')');
				}
				for edge in stage.edges.iter() {
					f.push(' ');
					f.push_str(self.binding_name(edge.from));
					f.push_str("-[");
					f.push_str(self.binding_name(edge.binding));
					f.push(':');
					EscapeIdent(edge.label.as_str()).fmt_sql(f, SqlFormat::SingleLine);
					f.push_str("]->");
					f.push_str(self.binding_name(edge.to));
				}
			}
		}
	}

	/// Render one pattern as `p = (a:person)-[k:knows]->(b:person)`.
	fn render_pattern(&self, f: &mut String, pattern: &PatternPlan) {
		if let Some(path_var) = pattern.path_var {
			f.push_str(self.binding_name(path_var));
			f.push_str(" = ");
		}
		if let Some(prefix) = pattern.search.as_ref() {
			Self::render_prefix(f, prefix);
		}
		self.render_node(f, &pattern.start);
		for (edge, node) in pattern.steps.iter() {
			self.render_edge(f, edge);
			self.render_node(f, node);
		}
	}

	/// Render a path-search / path-mode prefix in canonical form, trailing a
	/// single space (so it abuts the start node). The default mode (`Walk`)
	/// renders implicitly; an `ANY 1` / `SHORTEST 1 GROUP` count renders its
	/// canonical short form.
	fn render_prefix(f: &mut String, prefix: &PathPrefixPlan) {
		match prefix.search {
			PathSearch::All => f.push_str("ALL"),
			PathSearch::Any {
				count,
			} => {
				f.push_str("ANY");
				if count != 1 {
					f.push(' ');
					f.push_str(&count.to_string());
				}
			}
			PathSearch::AllShortest => f.push_str("ALL SHORTEST"),
			PathSearch::AnyShortest => f.push_str("ANY SHORTEST"),
			PathSearch::ShortestCounted {
				count,
			} => {
				f.push_str("SHORTEST ");
				f.push_str(&count.to_string());
			}
			PathSearch::ShortestGroups {
				count,
			} => {
				f.push_str("SHORTEST ");
				f.push_str(&count.to_string());
				f.push_str(if count == 1 {
					" GROUP"
				} else {
					" GROUPS"
				});
			}
		}
		match prefix.mode {
			PathMode::Walk => {}
			PathMode::Trail => f.push_str(" TRAIL"),
			PathMode::Simple => f.push_str(" SIMPLE"),
			PathMode::Acyclic => f.push_str(" ACYCLIC"),
		}
		f.push(' ');
	}

	/// Render a node element as `(a:person)`, `(a)`, or `(:person)`.
	fn render_node(&self, f: &mut String, node: &NodeStep) {
		f.push('(');
		let def = self.bindings.get(node.binding as usize);
		if let Some(def) = def
			&& def.user_named
		{
			f.push_str(&def.name);
		}
		if let Some(label) = node.label.as_ref() {
			f.push(':');
			EscapeIdent(label.as_str()).fmt_sql(f, SqlFormat::SingleLine);
		}
		f.push(')');
	}

	/// Render an edge element including direction arrows and any quantifier.
	fn render_edge(&self, f: &mut String, edge: &EdgeStep) {
		match edge.direction {
			ExpandDirection::Out => f.push('-'),
			ExpandDirection::In => f.push_str("<-"),
		}
		f.push('[');
		let def = self.bindings.get(edge.binding as usize);
		if let Some(def) = def
			&& def.user_named
		{
			f.push_str(&def.name);
		}
		if let Some(label) = edge.label.as_ref() {
			f.push(':');
			EscapeIdent(label.as_str()).fmt_sql(f, SqlFormat::SingleLine);
		}
		f.push(']');
		match edge.direction {
			ExpandDirection::Out => f.push_str("->"),
			ExpandDirection::In => f.push('-'),
		}
		if let Some(quantifier) = edge.quantifier.as_ref() {
			self.render_quantifier(f, quantifier);
		}
	}

	/// Render a quantifier as the canonical brace form `{min,max}`.
	fn render_quantifier(&self, f: &mut String, quantifier: &EdgeQuantifier) {
		f.push('{');
		if quantifier.is_exact() {
			f.push_str(&quantifier.min.to_string());
		} else {
			f.push_str(&quantifier.min.to_string());
			f.push(',');
			if let Some(max) = quantifier.max {
				f.push_str(&max.to_string());
			}
		}
		f.push('}');
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::ToSql;

	use super::*;
	use crate::expr::{Expr, Idiom, Literal, Part};

	/// Wrap read clauses as the plan's [`MatchStage::Read`] steps (test builders
	/// are all read-only).
	fn stages_of(clauses: Vec<MatchClausePlan>) -> Vec<MatchStage> {
		clauses.into_iter().map(MatchStage::Read).collect()
	}

	/// Mutable access to the read clause at step `i` (panics if it is not a read).
	fn clause_mut(plan: &mut MatchPlan, i: usize) -> &mut MatchClausePlan {
		match &mut plan.stages[i] {
			MatchStage::Read(c) => c,
			MatchStage::Mutate(_) => panic!("stage {i} is not a read clause"),
		}
	}

	/// Shared access to the read clause at step `i` (panics if it is not a read).
	fn clause(plan: &MatchPlan, i: usize) -> &MatchClausePlan {
		match &plan.stages[i] {
			MatchStage::Read(c) => c,
			MatchStage::Mutate(_) => panic!("stage {i} is not a read clause"),
		}
	}

	/// Build `a.x` as a binding-row scoped idiom expression.
	fn field_path(binding: &str, field: &str) -> Expr {
		Expr::Idiom(Idiom(vec![
			Part::Field(binding.to_string().into()),
			Part::Field(field.to_string().into()),
		]))
	}

	/// `MATCH (a:person)-[k:knows]->(b:person) WHERE k.since > 2020
	///  RETURN a.name AS a_name, b.name AS b_name`
	fn sample_plan() -> MatchPlan {
		let bindings = vec![
			BindingDef {
				name: "a".to_string(),
				kind: BindingKind::Node,
				user_named: true,
			},
			BindingDef {
				name: "k".to_string(),
				kind: BindingKind::Edge,
				user_named: true,
			},
			BindingDef {
				name: "b".to_string(),
				kind: BindingKind::Node,
				user_named: true,
			},
		];
		let predicate = MatchPredicate {
			expr: Expr::Binary {
				left: Box::new(field_path("k", "since")),
				op: crate::expr::BinaryOperator::MoreThan,
				right: Box::new(Expr::Literal(Literal::Integer(2020))),
			},
			deps: vec![1],
		};
		let pattern = PatternPlan {
			path_var: None,
			search: None,
			start: NodeStep {
				binding: 0,
				label: Some(TableName::new("person".to_string())),
			},
			steps: vec![(
				EdgeStep {
					binding: 1,
					label: Some(TableName::new("knows".to_string())),
					direction: ExpandDirection::Out,
					quantifier: None,
				},
				NodeStep {
					binding: 2,
					label: Some(TableName::new("person".to_string())),
				},
			)],
		};
		MatchPlan {
			bindings,
			stages: stages_of(vec![MatchClausePlan {
				optional_group: None,
				patterns: vec![pattern],
				predicates: vec![predicate],
			}]),
			output: Some(MatchOutput {
				columns: vec![
					MatchColumn {
						name: "a_name".to_string(),
						expr: field_path("a", "name"),
						hidden: false,
					},
					MatchColumn {
						name: "b_name".to_string(),
						expr: field_path("b", "name"),
						hidden: false,
					},
				],
				distinct: false,
				group_by: None,
				order: Vec::new(),
				skip: None,
				limit: None,
			}),
		}
	}

	#[test]
	fn to_sql_renders_single_pattern() {
		let plan = sample_plan();
		assert_eq!(
			plan.to_sql(),
			"MATCH (a:person)-[k:knows]->(b:person) WHERE k.since > 2020 RETURN a.name AS \
			 a_name, b.name AS b_name"
		);
	}

	#[test]
	fn to_sql_renders_distinct_order_quantifier_path() {
		let mut plan = sample_plan();
		// Promote to a path-var, quantified, distinct, ordered query.
		plan.bindings.push(BindingDef {
			name: "p".to_string(),
			kind: BindingKind::Path,
			user_named: true,
		});
		plan.bindings[1].kind = BindingKind::EdgeGroup;
		let path_id = (plan.bindings.len() - 1) as BindingId;
		let clause = clause_mut(&mut plan, 0);
		clause.patterns[0].path_var = Some(path_id);
		clause.patterns[0].steps[0].0.quantifier = Some(EdgeQuantifier {
			min: 1,
			max: Some(3),
		});
		clause.predicates.clear();
		let output = plan.output.as_mut().unwrap();
		output.distinct = true;
		output.order = vec![MatchOrder {
			expr: field_path("a", "age"),
			ascending: false,
		}];
		output.skip = Some(Expr::Literal(Literal::Integer(5)));
		output.limit = Some(Expr::Literal(Literal::Integer(10)));

		assert_eq!(
			plan.to_sql(),
			"MATCH p = (a:person)-[k:knows]->{1,3}(b:person) RETURN DISTINCT a.name AS a_name, \
			 b.name AS b_name ORDER BY a.age DESC SKIP 5 LIMIT 10"
		);
	}

	#[test]
	fn to_sql_renders_path_search_prefix() {
		let mut plan = sample_plan();
		// Promote to a quantified, prefixed pattern: `ANY SHORTEST SIMPLE`.
		plan.bindings[1].kind = BindingKind::EdgeGroup;
		clause_mut(&mut plan, 0).patterns[0].steps[0].0.quantifier = Some(EdgeQuantifier {
			min: 1,
			max: None,
		});
		clause_mut(&mut plan, 0).patterns[0].search = Some(PathPrefixPlan {
			search: PathSearch::AnyShortest,
			mode: PathMode::Simple,
		});
		clause_mut(&mut plan, 0).predicates.clear();
		plan.output.as_mut().unwrap().columns.truncate(1);
		assert_eq!(
			plan.to_sql(),
			"MATCH ANY SHORTEST SIMPLE (a:person)-[k:knows]->{1,}(b:person) RETURN a.name AS a_name"
		);
	}

	#[test]
	fn to_sql_omits_default_search_prefix() {
		// `search: None` (no prefix written) renders nothing — keeping unprefixed
		// plans byte-identical.
		let plan = sample_plan();
		assert!(clause(&plan, 0).patterns[0].search.is_none());
		let rendered = plan.to_sql();
		assert!(!rendered.contains("SHORTEST"), "{rendered}");
		assert!(!rendered.contains("ANY"), "{rendered}");
		assert!(rendered.starts_with("MATCH (a:person)"), "{rendered}");
	}

	#[test]
	fn to_sql_renders_shortest_group_count() {
		let mut plan = sample_plan();
		plan.bindings[1].kind = BindingKind::EdgeGroup;
		clause_mut(&mut plan, 0).patterns[0].steps[0].0.quantifier = Some(EdgeQuantifier {
			min: 1,
			max: None,
		});
		clause_mut(&mut plan, 0).patterns[0].search = Some(PathPrefixPlan {
			search: PathSearch::ShortestGroups {
				count: 2,
			},
			mode: PathMode::Walk,
		});
		clause_mut(&mut plan, 0).predicates.clear();
		plan.output.as_mut().unwrap().columns.truncate(1);
		// `WALK` (the default mode) renders implicitly; `GROUPS` is plural for k > 1.
		assert_eq!(
			plan.to_sql(),
			"MATCH SHORTEST 2 GROUPS (a:person)-[k:knows]->{1,}(b:person) RETURN a.name AS a_name"
		);
	}

	#[test]
	fn to_sql_renders_hidden_bindings_anonymously() {
		let mut plan = sample_plan();
		// A hidden edge binding renders without its name.
		plan.bindings[1].user_named = false;
		plan.bindings[1].name = "__e0".to_string();
		clause_mut(&mut plan, 0).predicates.clear();
		plan.output.as_mut().unwrap().columns.truncate(1);
		assert_eq!(plan.to_sql(), "MATCH (a:person)-[:knows]->(b:person) RETURN a.name AS a_name");
	}

	#[test]
	fn debug_does_not_panic() {
		let plan = sample_plan();
		let rendered = format!("{plan:?}");
		assert!(rendered.contains("MatchPlan"));
	}

	#[test]
	fn binding_accessor_returns_def() {
		let plan = sample_plan();
		assert_eq!(plan.binding(1).name, "k");
		assert_eq!(plan.binding(1).kind, BindingKind::Edge);
	}

	#[test]
	fn to_sql_tolerates_out_of_range_binding() {
		// A malformed plan must still render without panicking.
		let mut plan = sample_plan();
		clause_mut(&mut plan, 0).patterns[0].start.binding = 99;
		let rendered = plan.to_sql();
		assert!(rendered.contains("(:person)"));
	}

	#[test]
	fn revisioned_serialize_of_match_fails_loud() {
		// `Expr::Match` must never enter Revisioned serialization (V2_DESIGN §2;
		// SECURITY_GUIDE §15a): its `to_sql()` renders GQL-ish text that the
		// SurrealQL-only deserialize path cannot round-trip. The serialize path
		// mirrors the `From<expr::Expr> for sql::Expr` arm and fails loud via
		// `debug_assert!`, so a future regression that nests `Expr::Match` is
		// caught in debug builds rather than silently emitting corrupt bytes.
		use revision::SerializeRevisioned;

		let expr = Expr::Match(Box::new(sample_plan()));
		let mut bytes = Vec::new();
		let serialize = std::panic::AssertUnwindSafe(|| {
			let _ = SerializeRevisioned::serialize_revisioned(&expr, &mut bytes);
		});
		let outcome = std::panic::catch_unwind(serialize);

		if cfg!(debug_assertions) {
			// Debug: the guard's `debug_assert!(false)` must trip.
			assert!(
				outcome.is_err(),
				"Expr::Match serialize_revisioned must panic in debug builds"
			);
		} else {
			// Release: no panic, but the bytes it would emit are GQL text that
			// is NOT valid SurrealQL — the invariant is upheld structurally
			// (this path is unreachable by construction), the guard is purely a
			// debug tripwire.
			assert!(outcome.is_ok());
		}
	}
	/// MATCH labels are table names, so a name colliding with a reserved word
	/// must render quoted or the plan text cannot be read back.
	///
	/// The engine's `TableName` deliberately has no `ToSql` and derefs to `str`,
	/// which also implements it — so a bare `label.fmt_sql(..)` here compiles
	/// and silently emits the unescaped name. This asserts the escaping, not
	/// which impl supplies it.
	#[test]
	fn to_sql_escapes_labels_that_collide_with_keywords() {
		let plan = MatchPlan {
			bindings: vec![BindingDef {
				name: "a".to_string(),
				kind: BindingKind::Node,
				user_named: true,
			}],
			stages: stages_of(vec![MatchClausePlan {
				optional_group: None,
				patterns: vec![PatternPlan {
					search: None,
					path_var: None,
					start: NodeStep {
						binding: 0,
						label: Some(TableName::new("select".to_string())),
					},
					steps: Vec::new(),
				}],
				predicates: Vec::new(),
			}]),
			output: None,
		};

		let rendered = plan.to_sql();
		assert!(rendered.contains("`select`"), "a keyword label must be quoted, got: {rendered}");
	}
}
