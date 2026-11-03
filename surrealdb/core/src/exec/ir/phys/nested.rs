//! The strategy of a statement written in value position.
//!
//! [`run`] gives every `expr.subquery` of a container in the physical phase the
//! [`NestedStrategy`] it runs under, innermost statement first, and installs the plan an arm other
//! than `Apply` runs: a `phys.probe` beneath the innermost predicate for `Hash`, a
//! `phys.index_lookup` in place of the table scan for `Lookup`. Every arm delivers the rows, order
//! and errors of `Apply`. Reference: `strategy` and the nested-statement step of `rewrite_plan`
//! in the planner's rewrites.
//!
//! The arm follows from four facts of the statement ([`NestedFacts`]) and from whether the
//! statement has a join key ([`JoinSite`]), as [`arm`] states. A fact the container cannot state
//! is read as the answer that keeps `Apply`:
//!
//! - *correlated*: the statement reads a value defined outside it that depends on a row, a loop
//!   element, a closure parameter or a function argument, or names `$this`, `$self`, `$parent` or
//!   `$value` as a free name.
//! - *repeatable*: every op of the statement is deterministic, writes nothing and is not an op
//!   whose result may differ between two evaluations: a nested statement that does not run once, a
//!   method, a closure, a field read that may fetch a record, a call of anything but a builtin. A
//!   `MATCHES` or nearest-neighbour operator, and the search that answers it, repeat only in a
//!   statement that is not correlated ([`Search`]): run whole, the search answers the same each
//!   time, but a probe or lookup would run it once over rows its per-row form never searches.
//! - *reads rows*: the statement yields the result of a read, not of a write, an expression, a
//!   schema statement or a block construct.
//! - *enclosing writes*: the statement that encloses it writes, or runs a body whose effects are
//!   not known.
//!
//! A statement whose enclosing statement is a block construct keeps `Apply`: the planner runs each
//! such statement as a program of its own.
//!
//! A statement reads its rows through the body of a select loop inside a pipeline, which the walks
//! below read through ([`chain`]): the predicate, the permission and the layers of the body are the
//! ops beneath the statement's last op, and the source they read is the loop's.
//!
//! The join key is the first conjunct of the innermost predicate of the statement, where it is
//! `field = outer`, `field` a field of the statement's row and `outer` a path of field reads from
//! the row of the frame the statement is written in (`$parent`), and the statement less `outer` is
//! neither correlated nor unrepeatable. The probe and the lookup read `outer` through a copy of the
//! ops that compute it, placed in front of the op that takes it; the predicate keeps judging the
//! whole conjunct.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::smallvec;
use surrealdb_strand::{Strand, TableName};

use super::access::{self, DecisionCx, ReadVersion};
use super::facts::writes_by_shape;
use super::ops::{IndexLookupAttrs, LookupKeyShape, PhysOp, PhysTag};
use super::{chain, conjunct, duplicate};
use crate::exec::index::access_path::{AccessPath, BTreeAccess};
use crate::exec::ir::analysis::effects;
use crate::exec::ir::build::phys::index_ref;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::{FxHashMap, FxHashSet};
use crate::exec::ir::core::ir::{Ir, OpList, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ancestors, ops_in};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::membership::is_membership_set;
use crate::exec::ir::expr::ops::{ExprOp, KeyShape, NestedStrategy};
use crate::exec::ir::iface::catalog::{CatalogAnswer, EffectCx, PhysCx, SourceKey};
use crate::exec::ir::iface::choose::IndexRef;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::LoopKind;
use crate::exec::ir::op_kind::OpKind;
use crate::expr::logical::{EffectSet, LogicalExpr, LogicalLiteral, ROW_PARAMS};
use crate::expr::{BinaryOperator, Function, Part};
use crate::kvs::Direction;

/// The most hops followed from a value to the block argument it depends on.
const MAX_DEPTH: usize = 64;

/// What decides how a statement in value position runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct NestedFacts {
	/// The statement reads a value that varies with the row, loop element, closure parameter or
	/// function argument around it.
	pub(super) correlated: bool,
	/// Evaluating it twice answers what evaluating it once does, and evaluating it fewer times
	/// than written changes nothing.
	pub(super) repeatable: bool,
	/// It yields the result of a read.
	pub(super) reads_rows: bool,
	/// The statement it is written in may write, so a read of it may see what an earlier row
	/// wrote.
	pub(super) enclosing_writes: bool,
}

/// How a statement runs before the plan an arm needs is installed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Arm {
	/// Run for every evaluation.
	Apply,
	/// Run at the first evaluation that reaches it, its value kept for the later ones.
	Once,
	/// Run over the rows beneath its join predicate, read once and kept by the join field.
	Hash,
}

/// The arm of a statement with `facts`, where `joined` says it has a join key.
///
/// - A statement that does not read rows, or whose enclosing statement writes, is applied.
/// - One that reads nothing of the row it is evaluated for and is repeatable runs once.
/// - One that reads the row only through a join key runs over a probe.
/// - Every other statement is applied.
pub(super) fn arm(facts: NestedFacts, joined: bool) -> Arm {
	if !facts.reads_rows || facts.enclosing_writes {
		Arm::Apply
	} else if !facts.correlated && facts.repeatable {
		Arm::Once
	} else if facts.correlated && joined {
		Arm::Hash
	} else {
		Arm::Apply
	}
}

/// The ops that compute `outer`, in definition order with the root last, and the value of the
/// root.
#[derive(Clone, Debug)]
struct Outer {
	tree: Vec<OpId>,
	root: ValueId,
}

/// The join key of a statement.
#[derive(Clone, Debug)]
struct JoinSite {
	/// The innermost predicate.
	filter: OpId,
	/// The field of the statement's row the first conjunct compares.
	field: Strand,
	/// The path into the row of the enclosing frame the field is compared with.
	outer: Outer,
}

/// Where a probe stands relative to the predicate whose rows it keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbeAt {
	/// Between the filter and the rows it reads.
	Filter,
	/// Between the source of the select loop `select` and the loop, in front of `before`, the op
	/// that stands for the loop in its block: the filter is the first op of the loop's body, and a
	/// per-row body cannot hold a probe, which keeps every row of its input.
	Loop {
		select: OpId,
		before: OpId,
	},
}

/// The plan an arm other than `Apply` and `Once` installs.
enum Edit {
	None,
	/// A `phys.probe` keyed by `field` over the rows `filter` reads, at `at`.
	Probe {
		at: ProbeAt,
		filter: OpId,
		field: Strand,
		outer: Outer,
	},
	/// A `phys.index_lookup` through `index`, keyed by `field`, in place of the table scan `scan`.
	/// With `hashed`, the statement as it was is also kept, with the probe, as the second region.
	Lookup {
		scan: OpId,
		field: Strand,
		outer: Outer,
		index: Box<IndexRef>,
		hashed: Option<HashedPlan>,
	},
}

/// Where the probe of the hashed plan of a lookup stands, in the statement as it is before the
/// lookup is installed.
#[derive(Clone, Copy, Debug)]
struct HashedPlan {
	at: ProbeAt,
	filter: OpId,
}

/// Decides the strategy of every `expr.subquery` of `ir`, which is in the physical phase, and
/// answers how many it gave an arm other than `Apply`.
///
/// A statement is decided after the statements nested in it. A statement written in a
/// stored-clause region is not run by this plan and keeps `Apply`.
pub(super) fn run(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) -> Result<u32, IrError> {
	let mut sites = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)))
		) && !in_stored_clause(ir, op)
		{
			sites.push(op);
		}
		Walk::Continue
	});
	let mut decided = 0;
	for site in sites.into_iter().rev() {
		let Some((strategy, edit)) = decide(ir, facts, cx, site) else {
			continue;
		};
		install(ir, facts, site, strategy, edit)?;
		decided += 1;
	}
	Ok(decided)
}

/// Whether `op` stands in a region that holds code the statement stores and never runs.
pub(super) fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

/// The strategy of the statement `subquery` and the plan it needs, or `None` to leave it applied.
fn decide(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &PhysCx<'_>,
	subquery: OpId,
) -> Option<(NestedStrategy, Edit)> {
	let region = *ir.op(subquery)?.regions.first()?;
	let (enclosing, owner) = enclosing_statement(ir, subquery)?;
	if holds_control(ir, enclosing) {
		return None;
	}
	let tree = Subtree::of(ir, subquery);
	let none = FxHashSet::default();
	let correlated = tree.correlated(ir, &none);
	// Only `Once` reads `repeatable` of an uncorrelated statement, and it runs the statement
	// whole, the search included.
	let search = if correlated {
		Search::Varies
	} else {
		Search::Repeats
	};
	let nested = NestedFacts {
		correlated,
		repeatable: tree.repeatable(ir, facts, cx, &none, search),
		reads_rows: reads_rows(ir, region),
		enclosing_writes: statement_writes(ir, facts, &cx.effects, owner),
	};
	let join = if nested.correlated && nested.reads_rows {
		join_site(ir, facts, cx, &tree, region)
	} else {
		None
	};
	let base = arm(nested, join.is_some());
	if let Some(join) = &join
		&& matches!(base, Arm::Hash | Arm::Apply)
		&& let Some((scan, index)) = lookup_site(ir, cx, join)
	{
		// A statement that could be hashed keeps its probe plan beside the lookup, for the
		// evaluations past the first few of a run.
		let hashed = if base == Arm::Hash {
			probe_site(ir, join).map(|at| HashedPlan {
				at,
				filter: join.filter,
			})
		} else {
			None
		};
		return Some((
			NestedStrategy::Lookup {
				index: Box::new(index.clone()),
				hashed: hashed.is_some(),
			},
			Edit::Lookup {
				scan,
				field: join.field.clone(),
				outer: join.outer.clone(),
				index: Box::new(index),
				hashed,
			},
		));
	}
	match (base, join) {
		(Arm::Once, _) => Some((NestedStrategy::Once, Edit::None)),
		(Arm::Hash, Some(join)) => probe_site(ir, &join).map(|at| {
			(
				NestedStrategy::Hash {
					field: join.field.clone(),
				},
				Edit::Probe {
					at,
					filter: join.filter,
					field: join.field,
					outer: join.outer,
				},
			)
		}),
		(Arm::Hash, None) | (Arm::Apply, _) => None,
	}
}

// ---------------------------------------------------------------------------------------------
// The statement around the subquery
// ---------------------------------------------------------------------------------------------

/// The nearest `Statement` or `Subquery` region around `op` and the op that owns it.
pub(super) fn enclosing_statement(ir: &Ir, op: OpId) -> Option<(RegionId, OpId)> {
	ancestors(ir, op).find_map(|(region, owner)| {
		let kind = ir.region(region)?.kind;
		if matches!(kind, RegionKind::Statement(_) | RegionKind::Subquery) {
			owner.map(|owner| (region, owner))
		} else {
			None
		}
	})
}

/// Whether a block construct stands directly in `region`: a block, a `LET`, an `IF`, a `FOR`, a
/// `SLEEP`, a `RETURN`, a `BREAK`, a `CONTINUE` or a `THROW`.
fn holds_control(ir: &Ir, region: RegionId) -> bool {
	ops_in(ir, region).any(|op| match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Phys(kind)) => {
			matches!(
				kind.tag(),
				PhysTag::Block
					| PhysTag::Let | PhysTag::If
					| PhysTag::Sleep
					| PhysTag::Return
					| PhysTag::Break
					| PhysTag::Next
			) || matches!(
				kind,
				PhysOp::Loop {
					kind: LoopKind::For,
					..
				}
			)
		}
		Some(OpKind::Expr(ExprOp::Throw | ExprOp::If)) => true,
		Some(OpKind::Expr(_) | OpKind::Logical(_)) | None => false,
	})
}

/// Whether the statement `owner` writes: an op in it, a statement nested in it included, writes
/// or locks by its own shape, or an expression in it may write data, write outside the
/// transaction or run a body whose effects are not known.
///
/// The answer is read from the ops themselves. The effects of a physical read op are not stated
/// by the op, so they are not consulted: a read of the statement is a write by nothing but its
/// shape and its expressions. A step that fetches a record ([`fetches`]) derives the record's
/// computed fields through bodies it does not show, and is not a write: the planner's basic
/// effects state it as a read.
pub(super) fn statement_writes(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	owner: OpId,
) -> bool {
	let mut ops = OpList::new();
	ops.push(owner);
	if let Some(data) = ir.op(owner) {
		for region in &data.regions {
			ir.walk(*region, WalkOrder::PreOrder, &mut |op| {
				ops.push(op);
				Walk::Continue
			});
		}
	}
	ops.into_iter().any(|op| match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Phys(kind)) => writes_by_shape(kind),
		Some(OpKind::Expr(kind)) => {
			!fetches(kind) && {
				let set = effects::local(ir, facts, cx, op).set;
				set.writes_data()
					|| set.intersects(EffectSet::OPAQUE_BODY | EffectSet::WRITES_EXTERNAL)
			}
		}
		Some(OpKind::Logical(_)) | None => true,
	})
}

/// Whether `op` reads the record its base may name, which derives the record's computed fields.
fn fetches(op: &ExprOp) -> bool {
	matches!(
		op,
		ExprOp::Access { .. }
			| ExprOp::Each
			| ExprOp::Destructure { .. }
			| ExprOp::Deref { .. }
			| ExprOp::ComputedRead(_)
	)
}

/// The value the region `region` yields.
pub(super) fn yielded(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let last = ops_in(ir, region).last()?;
	let data = ir.op(last)?;
	match (&data.kind, data.operands.as_slice()) {
		(OpKind::Expr(ExprOp::Yield) | OpKind::Phys(PhysOp::Yield), [value]) => Some(*value),
		_ => None,
	}
}

/// The op that defines the value the statement in `region` yields, read through the pipeline and
/// the select loop that hold its rows.
fn root_of(ir: &Ir, region: RegionId) -> Option<OpId> {
	let mut value = yielded(ir, region)?;
	while let Some(through) = chain::enter(ir, value) {
		value = through;
	}
	conjunct::def_op(ir, value)
}

/// Whether the statement in `region` yields the result of a read: the op that defines its value,
/// read through the pipeline and the select loop that hold its rows, is a physical op that is not
/// a write, an expression, a schema statement, an `INFO`, an `EXPLAIN` or a block construct. An
/// `EXPLAIN` answers the rendering of a plan, plain or analysed, never the rows it explains.
fn reads_rows(ir: &Ir, region: RegionId) -> bool {
	let Some(root) = root_of(ir, region) else {
		return false;
	};
	match ir.op(root).map(|data| &data.kind) {
		Some(OpKind::Phys(kind)) => {
			!(matches!(
				kind.tag(),
				PhysTag::Eval
					| PhysTag::Write
					| PhysTag::Define
					| PhysTag::Remove
					| PhysTag::Alter
					| PhysTag::Rebuild
					| PhysTag::DdlGuard
					| PhysTag::Info | PhysTag::Explain
					| PhysTag::Block
					| PhysTag::Let | PhysTag::Sleep
					| PhysTag::If | PhysTag::Return
					| PhysTag::Break
					| PhysTag::Next
			) || matches!(
				kind,
				PhysOp::Loop {
					kind: LoopKind::For,
					..
				}
			))
		}
		Some(OpKind::Expr(_) | OpKind::Logical(_)) | None => false,
	}
}

/// Whether the statement in value position `subquery` yields rows that its readers only test
/// membership in, so that a `Once` strategy keeps them as an index: the statement reads rows
/// ([`reads_rows`]), yields more than the single value of an `ONLY`, and every reader of its
/// result is the right operand of an `INSIDE` or a `NOT INSIDE`.
pub(crate) fn held_as_members(ir: &Ir, subquery: OpId) -> bool {
	let Some(region) = ir.op(subquery).and_then(|data| data.regions.first().copied()) else {
		return false;
	};
	let one_value = root_of(ir, region).is_some_and(|root| {
		matches!(ir.op(root).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::Only)))
	});
	is_membership_set(ir, subquery) && reads_rows(ir, region) && !one_value
}

// ---------------------------------------------------------------------------------------------
// What the statement holds
// ---------------------------------------------------------------------------------------------

/// The ops and regions inside a statement in value position.
struct Subtree {
	/// Every op in the regions of the subquery, in pre-order.
	ops: Vec<OpId>,
	/// The regions of the subquery and every region inside them.
	regions: FxHashSet<RegionId>,
}

impl Subtree {
	fn of(ir: &Ir, subquery: OpId) -> Self {
		let mut tree = Self {
			ops: Vec::new(),
			regions: FxHashSet::default(),
		};
		let Some(data) = ir.op(subquery) else {
			return tree;
		};
		for region in &data.regions {
			tree.regions.insert(*region);
			ir.walk(*region, WalkOrder::PreOrder, &mut |op| {
				tree.ops.push(op);
				if let Some(inner) = ir.op(op) {
					tree.regions.extend(inner.regions.iter().copied());
				}
				Walk::Continue
			});
		}
		tree
	}

	/// Whether `value` is defined by an op of the statement or is an argument of a block of one of
	/// its regions.
	fn defines(&self, ir: &Ir, value: ValueId) -> bool {
		match ir.value(value).map(|data| data.def) {
			Some(ValueDef::OpResult {
				op,
				..
			}) => self.regions.contains(&ir.enclosing_region(op)),
			Some(ValueDef::BlockArg {
				block,
				..
			}) => ir.block(block).is_some_and(|held| self.regions.contains(&held.parent)),
			None => false,
		}
	}

	/// Whether the statement, less the ops of `exclude`, reads a value defined outside it that
	/// varies with a row, or names a row parameter as a free name.
	fn correlated(&self, ir: &Ir, exclude: &FxHashSet<OpId>) -> bool {
		self.ops.iter().filter(|op| !exclude.contains(*op)).any(|op| {
			let Some(data) = ir.op(*op) else {
				return true;
			};
			if let OpKind::Expr(ExprOp::FreeParam {
				name,
			}) = &data.kind
				&& ROW_PARAMS.contains(&name.as_str())
			{
				return true;
			}
			data.operands.iter().any(|operand| {
				!self.defines(ir, *operand)
					&& row_dependent(ir, *operand, &mut FxHashSet::default(), MAX_DEPTH)
			})
		})
	}

	/// Whether every op of the statement, less the ops of `exclude`, may be evaluated again or
	/// fewer times than written: it is deterministic, writes nothing, is not an op whose result may
	/// differ between two evaluations, and is an index operator or search only where `search`
	/// says those repeat.
	fn repeatable(
		&self,
		ir: &Ir,
		facts: &mut FactCache,
		cx: &PhysCx<'_>,
		exclude: &FxHashSet<OpId>,
		search: Search,
	) -> bool {
		self.ops.iter().filter(|op| !exclude.contains(*op)).all(|op| {
			let Some(data) = ir.op(*op) else {
				return false;
			};
			match &data.kind {
				OpKind::Expr(kind) => {
					(!impure(kind) || runs_once(kind))
						&& (search == Search::Repeats || !index_operator(kind))
						&& effects::local(ir, facts, &cx.effects, *op).set.is_deterministic()
				}
				OpKind::Phys(kind) => {
					reads_deterministically(kind) || (search == Search::Repeats && searches(kind))
				}
				OpKind::Logical(_) => false,
			}
		})
	}
}

/// Whether the physical op `op` reads, or passes on rows, the same way every time it is
/// evaluated within a statement that writes nothing: a read of storage by a path no index operator
/// names, a per-row step of a read, the select loop that runs the steps, and the structure around
/// them.
///
/// The effects of a physical op are those of the logical ops it stands for. The physical
/// container does not keep those logical ops, so the answer is the op's kind: every op that is
/// not named here is not repeatable. A write, a schema statement, a `SLEEP`, a locked read, a
/// `MATCHES` or nearest-neighbour search and a block construct are not named.
fn reads_deterministically(op: &PhysOp) -> bool {
	if writes_by_shape(op) {
		return false;
	}
	// The rows of a read are the elements of a select loop; the ops of its body are judged as
	// ops of the statement.
	if matches!(
		op,
		PhysOp::Loop {
			kind: LoopKind::Select,
			..
		}
	) {
		return true;
	}
	matches!(
		op.tag(),
		PhysTag::TableScan
			| PhysTag::IndexScan
			| PhysTag::IndexUnion
			| PhysTag::Bitmap
			| PhysTag::IndexLookup
			| PhysTag::KeyRangeScan
			| PhysTag::Count
			| PhysTag::RecordScan
			| PhysTag::Vacant
			| PhysTag::Enforced
			| PhysTag::Values
			| PhysTag::Subquery
			| PhysTag::Probe
			| PhysTag::Union
			| PhysTag::Join
			| PhysTag::Distinct
			| PhysTag::PermissionFilter
			| PhysTag::Layer
			| PhysTag::Filter
			| PhysTag::Fetch
			| PhysTag::Split
			| PhysTag::Sort
			| PhysTag::Window
			| PhysTag::Aggregate
			| PhysTag::Limit
			| PhysTag::Project
			| PhysTag::VersionScope
			| PhysTag::Only
			| PhysTag::Pipeline
			| PhysTag::Choose
			| PhysTag::Prefetch
			| PhysTag::Materialize
			| PhysTag::Yield
	)
}

/// Whether `value` depends on an argument of a region that stands for a row, an element, a
/// parameter or an argument. The arguments of a request, a transaction, a statement and a catalog
/// unit are the statement's own context and do not vary with a row. A chain longer than `depth`
/// is taken to depend.
fn row_dependent(ir: &Ir, value: ValueId, seen: &mut FxHashSet<ValueId>, depth: usize) -> bool {
	if depth == 0 {
		return true;
	}
	if !seen.insert(value) {
		return false;
	}
	match ir.value(value).map(|data| data.def) {
		Some(ValueDef::BlockArg {
			block,
			..
		}) => ir.block(block).and_then(|held| ir.region(held.parent)).is_none_or(|region| {
			!matches!(
				region.kind,
				RegionKind::Request
					| RegionKind::Transaction(_)
					| RegionKind::Statement(_)
					| RegionKind::CatalogUnit
			)
		}),
		Some(ValueDef::OpResult {
			op,
			..
		}) => ir.op(op).is_none_or(|data| {
			data.operands.iter().any(|operand| row_dependent(ir, *operand, seen, depth - 1))
		}),
		None => true,
	}
}

/// Whether two evaluations of `op` may answer differently, or evaluating it fewer times than
/// written may change what a statement does, whatever its effects say.
pub(super) fn impure(op: &ExprOp) -> bool {
	match op {
		ExprOp::Literal(_)
		| ExprOp::Array
		| ExprOp::Set
		| ExprOp::Object {
			..
		}
		| ExprOp::FreeParam {
			..
		}
		| ExprOp::SessionParam {
			..
		}
		| ExprOp::CatalogParam {
			..
		}
		| ExprOp::Table {
			..
		}
		| ExprOp::Mock(_)
		| ExprOp::Constant {
			..
		}
		| ExprOp::Field {
			..
		}
		| ExprOp::Doc
		| ExprOp::Index
		| ExprOp::First
		| ExprOp::Last
		| ExprOp::Flatten {
			..
		}
		| ExprOp::Optional
		| ExprOp::RepeatRecurse
		| ExprOp::SetField {
			..
		}
		| ExprOp::Prefix(_)
		| ExprOp::Convert(_)
		| ExprOp::Postfix {
			..
		}
		| ExprOp::Binary {
			..
		}
		| ExprOp::ShortCircuit {
			..
		}
		| ExprOp::If
		| ExprOp::Yield
		| ExprOp::Truthy
		| ExprOp::Agg {
			..
		}
		| ExprOp::Now {
			..
		}
		| ExprOp::DepthCharge {
			..
		} => false,
		ExprOp::RecordId(attrs) => matches!(attrs.key_shape, KeyShape::Generate(_)),
		ExprOp::Call(function) => !matches!(function.function, Function::Normal(_)),
		ExprOp::ComputedRead(_)
		| ExprOp::Access {
			..
		}
		| ExprOp::Deref {
			..
		}
		| ExprOp::Each
		| ExprOp::Dispatch(_)
		| ExprOp::Method {
			..
		}
		| ExprOp::Destructure {
			..
		}
		| ExprOp::Invoke
		| ExprOp::ClosureCreate(_)
		| ExprOp::Throw
		| ExprOp::Subquery(_)
		| ExprOp::TupleRow {
			..
		}
		| ExprOp::Raise(_) => true,
	}
}

/// Whether `op` is a statement that runs once ([`NestedStrategy::Once`]). Statements are decided
/// innermost first, so a statement nested in the one being judged has its strategy already: one
/// that runs once reads nothing of a row around it, writes nothing and answers the same each time,
/// and its value is a constant of the statement it stands in, however often that runs.
pub(super) fn runs_once(op: &ExprOp) -> bool {
	matches!(op, ExprOp::Subquery(NestedStrategy::Once))
}

/// Whether a `MATCHES` or nearest-neighbour search repeats in the statement being judged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Search {
	/// The statement runs whole at each evaluation it is read for, so a search in it answers the
	/// same each time: the search reads the index under the statement's own predicate and limit.
	Repeats,
	/// The statement is split around a join key, so its rows would be read once and judged per
	/// evaluation, where the per-row form searches only the rows the key admits: a
	/// nearest-neighbour search would rank other rows and a `MATCHES` would score them apart from
	/// the key.
	Varies,
}

/// Whether the physical op `op` is a full-text or nearest-neighbour search, which reads the same
/// rows each time a statement that runs whole reads it.
fn searches(op: &PhysOp) -> bool {
	matches!(op.tag(), PhysTag::FullTextScan | PhysTag::KnnScan | PhysTag::KnnTopK)
}

/// Whether `op` is a `MATCHES` or a nearest-neighbour operator, which an access path answers and
/// evaluation does not.
pub(super) fn index_operator(op: &ExprOp) -> bool {
	matches!(
		op,
		ExprOp::Binary {
			op: BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_)
		}
	)
}

// ---------------------------------------------------------------------------------------------
// The join key
// ---------------------------------------------------------------------------------------------

/// The innermost filter on the chain of single inputs that starts at the value the statement in
/// `region` yields: the chain crosses a permission, a layer, a split, a fetch, a sort, a limit, an
/// aggregate and a projection, is read through the pipeline and select loop that hold the per-row
/// ops, and the innermost filter is the last one it meets.
fn innermost_filter(ir: &Ir, region: RegionId) -> Option<OpId> {
	let mut value = yielded(ir, region)?;
	let mut found = None;
	loop {
		if let Some(through) = chain::enter(ir, value) {
			value = through;
			continue;
		}
		let Some(op) = conjunct::def_op(ir, value) else {
			return found;
		};
		let data = ir.op(op)?;
		let OpKind::Phys(kind) = &data.kind else {
			return found;
		};
		let tag = kind.tag();
		if tag == PhysTag::Filter {
			found = Some(op);
		} else if !matches!(
			tag,
			PhysTag::PermissionFilter
				| PhysTag::Layer
				| PhysTag::Split
				| PhysTag::Fetch
				| PhysTag::Sort
				| PhysTag::Limit
				| PhysTag::Aggregate
				| PhysTag::Project
		) {
			return found;
		}
		value = *data.operands.first()?;
	}
}

/// The op beneath `from` that no op of `crossing` stands in front of, following first operands
/// through the ops of `crossing` and through the pipeline and select loop that hold them. Every
/// value on the way has no other reader.
fn beneath(ir: &Ir, from: ValueId, crossing: fn(PhysTag) -> bool) -> Option<OpId> {
	let mut value = from;
	loop {
		if ir.uses(value).len() != 1 {
			return None;
		}
		if let Some(through) = chain::enter(ir, value) {
			value = through;
			continue;
		}
		let op = conjunct::def_op(ir, value)?;
		let data = ir.op(op)?;
		let OpKind::Phys(kind) = &data.kind else {
			return None;
		};
		if !crossing(kind.tag()) {
			return Some(op);
		}
		value = *data.operands.first()?;
	}
}

/// The ops a probe reads through: a permission and a layer.
fn probe_crossing(tag: PhysTag) -> bool {
	matches!(tag, PhysTag::PermissionFilter | PhysTag::Layer)
}

/// The ops a lookup reads through: a permission, a layer and table enforcement.
fn lookup_crossing(tag: PhysTag) -> bool {
	matches!(tag, PhysTag::PermissionFilter | PhysTag::Layer | PhysTag::Enforced)
}

/// The join key of the statement in `region`, when it has one and the statement less the ops that
/// compute the outer path is neither correlated nor unrepeatable.
fn join_site(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &PhysCx<'_>,
	tree: &Subtree,
	region: RegionId,
) -> Option<JoinSite> {
	let filter = innermost_filter(ir, region)?;
	let site = conjunct::site(ir, filter)?;
	let first = conjunct::strip_truthy(ir, *conjunct::leaves(ir, &site).first()?);
	let comparison = ir.op(conjunct::def_op(ir, first)?)?;
	let (
		OpKind::Expr(ExprOp::Binary {
			op: BinaryOperator::Equal,
		}),
		[left, right],
	) = (&comparison.kind, comparison.operands.as_slice())
	else {
		return None;
	};
	let (field, outer) = field_and_outer(ir, tree, *left, *right)
		.or_else(|| field_and_outer(ir, tree, *right, *left))?;
	let exclude: FxHashSet<OpId> = outer.tree.iter().copied().collect();
	if tree.correlated(ir, &exclude) || !tree.repeatable(ir, facts, cx, &exclude, Search::Varies) {
		return None;
	}
	Some(JoinSite {
		filter,
		field,
		outer,
	})
}

/// The field `field_side` reads of the row of its `WHERE` region, and the path `outer_side`
/// reads into the row of the frame the statement is written in.
fn field_and_outer(
	ir: &Ir,
	tree: &Subtree,
	field_side: ValueId,
	outer_side: ValueId,
) -> Option<(Strand, Outer)> {
	let data = ir.op(conjunct::def_op(ir, field_side)?)?;
	let OpKind::Expr(ExprOp::Field {
		name,
	}) = &data.kind
	else {
		return None;
	};
	if !conjunct::is_row(ir, data.operands.first()) {
		return None;
	}
	let mut ops = Vec::new();
	outer_path(ir, tree, outer_side, &mut ops)?;
	Some((
		name.clone(),
		Outer {
			tree: ops,
			root: outer_side,
		},
	))
}

/// Pushes the ops that compute `value` onto `ops`, leaf first, when `value` is `$parent` or a
/// chain of field reads from it, and no value of the chain has another reader.
///
/// `$parent` is the row argument of a loop or clause region outside the statement, or the free
/// name `parent`.
fn outer_path(ir: &Ir, tree: &Subtree, value: ValueId, ops: &mut Vec<OpId>) -> Option<()> {
	match ir.value(value)?.def {
		ValueDef::BlockArg {
			block,
			index,
		} => {
			let region = ir.block(block)?.parent;
			let kind = ir.region(region)?.kind;
			let row = index == 0 && matches!(kind, RegionKind::Loop | RegionKind::Clause(_));
			(row && !tree.regions.contains(&region)).then_some(())
		}
		ValueDef::OpResult {
			op,
			..
		} => {
			if ir.uses(value).len() != 1 {
				return None;
			}
			let data = ir.op(op)?;
			match &data.kind {
				OpKind::Expr(ExprOp::FreeParam {
					name,
				}) if name.as_str() == "parent" => {}
				OpKind::Expr(ExprOp::Access {
					..
				}) => outer_path(ir, tree, *data.operands.first()?, ops)?,
				OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => return None,
			}
			ops.push(op);
			Some(())
		}
	}
}

/// Where a probe can be put to key the rows the filter of `join` reads: only a permission and a
/// layer lie between the filter and a read of storage.
///
/// A filter in the body of a select loop reads the rows of the loop's source through the loop's
/// element. A probe keeps every row of its input, so it stands in front of the loop, and only where
/// the filter is the first op of the body: a permission or a layer of the body judges the rows
/// before the filter does, and a probe in front of it would key rows that op raises on or hides.
/// Such a statement answers `None` and is applied.
fn probe_site(ir: &Ir, join: &JoinSite) -> Option<ProbeAt> {
	let input = *ir.op(join.filter)?.operands.first()?;
	let (at, from) = match chain::element_loop(ir, input) {
		Some(select) => {
			let source = *ir.op(select)?.operands.first()?;
			let before = chain::container_of(ir, select);
			(
				ProbeAt::Loop {
					select,
					before,
				},
				source,
			)
		}
		None if chain::in_select_body(ir, join.filter) => return None,
		None => (ProbeAt::Filter, input),
	};
	let read = beneath(ir, from, probe_crossing).and_then(|op| ir.op(op))?;
	let storage = match &read.kind {
		OpKind::Phys(kind) => matches!(
			kind.tag(),
			PhysTag::TableScan
				| PhysTag::IndexScan
				| PhysTag::IndexLookup
				| PhysTag::IndexUnion
				| PhysTag::Bitmap
				| PhysTag::RecordScan
				| PhysTag::FullTextScan
				| PhysTag::KnnScan
		),
		OpKind::Expr(_) | OpKind::Logical(_) => false,
	};
	storage.then_some(at)
}

/// The forward table scan beneath the filter of `join`, under a permission, layers and
/// enforcement alone, with the index of its table an equality on the join field is read through.
///
/// A scan that takes a `WITH` hint or a `VERSION`, and a field no B-tree index over that field
/// alone serves, have none. A scan's top-k key only rejects rows the sort above it would drop, so
/// the lookup reads without one and the sort keeps judging order and count.
fn lookup_site(ir: &Ir, cx: &PhysCx<'_>, join: &JoinSite) -> Option<(OpId, IndexRef)> {
	let input = *ir.op(join.filter)?.operands.first()?;
	let scan = beneath(ir, input, lookup_crossing)?;
	let OpKind::Phys(PhysOp::TableScan(attrs)) = &ir.op(scan)?.kind else {
		return None;
	};
	if attrs.direction != Direction::Forward || attrs.read.with.is_some() {
		return None;
	}
	if holds_version(ir, join.filter) {
		return None;
	}
	let key_ty = ir.value(join.outer.root).map(|value| &value.ty);
	let index = index_for(cx, &attrs.table, &join.field, key_ty)?;
	Some((scan, index))
}

/// Whether a `VERSION` scopes the statement `filter` stands in.
pub(super) fn holds_version(ir: &Ir, filter: OpId) -> bool {
	let Some((region, _)) = enclosing_statement(ir, filter) else {
		return true;
	};
	let mut found = false;
	ir.walk(region, WalkOrder::PreOrder, &mut |op| {
		found |=
			matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::VersionScope)));
		Walk::Continue
	});
	found
}

/// The stand-in literal for `ty` in an access-path equality probe, or `None` when `ty` states a
/// kind no B-tree equality lookup serves.
fn stand_in_literal(ty: Option<&TypeKnowledge>) -> Option<LogicalLiteral> {
	match ty {
		Some(
			TypeKnowledge::Fact {
				kind,
				..
			}
			| TypeKnowledge::Hint {
				kind,
				..
			},
		) => stand_in_literal_for_kind(kind),
		Some(TypeKnowledge::Token(_)) => None,
		Some(TypeKnowledge::Unknown) | None => Some(LogicalLiteral::Integer(0)),
	}
}

fn stand_in_literal_for_kind(kind: &crate::expr::Kind) -> Option<LogicalLiteral> {
	use crate::expr::Kind;
	match kind {
		Kind::None | Kind::Regex | Kind::Function(..) => None,
		Kind::Null => Some(LogicalLiteral::Null),
		Kind::Bool => Some(LogicalLiteral::Bool(false)),
		Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::Int
		| Kind::Number
		| Kind::Any
		| Kind::Named(_) => Some(LogicalLiteral::Integer(0)),
		Kind::F16 | Kind::F32 | Kind::Float => Some(LogicalLiteral::Float(0.0)),
		Kind::Decimal => Some(LogicalLiteral::Decimal(rust_decimal::Decimal::ZERO)),
		Kind::BigDecimal => {
			Some(LogicalLiteral::BigDecimal(Box::new(surrealdb_types::BigDecimal::from(0_i64))))
		}
		Kind::String => Some(LogicalLiteral::String(Strand::default())),
		Kind::Bytes => Some(LogicalLiteral::Bytes(crate::val::Bytes::default())),
		Kind::Datetime => Some(LogicalLiteral::Datetime(crate::val::Datetime::MIN_UTC)),
		Kind::Date => Some(LogicalLiteral::Date(crate::val::Date(0))),
		Kind::Time => Some(LogicalLiteral::Time(crate::val::Time(0))),
		Kind::Timestamp => Some(LogicalLiteral::Timestamp(crate::val::Timestamp {
			secs: 0,
			nanos: 0,
		})),
		Kind::Interval => Some(LogicalLiteral::Interval(crate::val::Interval::new(0, 0, 0))),
		Kind::Duration => Some(LogicalLiteral::Duration(crate::val::Duration::default())),
		Kind::Uuid => Some(LogicalLiteral::Uuid(crate::val::Uuid::default())),
		Kind::Either(kinds) => kinds.iter().find_map(stand_in_literal_for_kind),
		Kind::Array(..)
		| Kind::Set(..)
		| Kind::Object
		| Kind::Record(_)
		| Kind::Table(_)
		| Kind::Geometry(_)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_) => Some(LogicalLiteral::Integer(0)),
	}
}

/// The B-tree index of `table` over the plain field `field` alone that the access-path decision
/// reads an equality on `field` through.
pub(super) fn index_for(
	cx: &PhysCx<'_>,
	table: &TableName,
	field: &Strand,
	key_ty: Option<&TypeKnowledge>,
) -> Option<IndexRef> {
	let key: SourceKey = (table.clone(), None, ReadVersion::Current);
	let CatalogAnswer::Found(answers) = cx.physical.table_answers(&key) else {
		return None;
	};
	if !answers.defined {
		return None;
	}
	let literal = stand_in_literal(key_ty)?;
	// The value is a stand-in: the decision reads only the conjunct's shape.
	let probe = LogicalExpr::Binary {
		left: Box::new(LogicalExpr::Field(field.clone())),
		op: BinaryOperator::Equal,
		right: Box::new(LogicalExpr::Literal(literal)),
	};
	let no_vectors = crate::exec::index::analysis::KnnQueryVectors::new();
	let decision_cx = DecisionCx {
		registry: cx.effects.registry,
		knn_vectors: &no_vectors,
		// The lookup reads its key's rows for a statement that writes none of them.
		id_bounds: true,
	};
	let decision =
		access::decide(&answers, None, Some(&probe), None, false, &decision_cx).ok()??;
	let AccessPath::BTreeScan {
		index_ref: chosen,
		access: BTreeAccess::Equality(_),
		..
	} = decision.0
	else {
		return None;
	};
	let [column] = chosen.cols.as_slice() else {
		return None;
	};
	matches!(column.0.as_slice(), [Part::Field(name)] if name == field).then(|| index_ref(&chosen))
}

// ---------------------------------------------------------------------------------------------
// Installing the plan
// ---------------------------------------------------------------------------------------------

/// Gives `subquery` the strategy `strategy` and installs the plan `edit` describes.
fn install(
	ir: &mut Ir,
	facts: &mut FactCache,
	subquery: OpId,
	strategy: NestedStrategy,
	edit: Edit,
) -> Result<(), IrError> {
	let mut rw = Rewriter::new(ir, facts);
	match edit {
		Edit::None => {}
		Edit::Probe {
			at,
			filter,
			field,
			outer,
		} => install_probe(&mut rw, at, filter, field, &outer)?,
		Edit::Lookup {
			scan,
			field,
			outer,
			index,
			hashed,
		} => {
			if let Some(plan) = hashed {
				install_hashed(&mut rw, subquery, plan, &field, &outer)?;
			}
			let lookup = install_lookup(&mut rw, scan, field, &outer, *index)?;
			if let Some(region) =
				rw.ir().op(subquery).and_then(|data| data.regions.first().copied())
			{
				quiet_sorts_over(&mut rw, region, lookup)?;
			}
		}
	}
	rw.update_kind(subquery, |kind| {
		if let OpKind::Expr(ExprOp::Subquery(held)) = kind {
			*held = strategy;
		}
	})
}

/// Copies the ops that compute `outer` to `at` and answers the copy's value. The copy reads what
/// the original reads, which stands outside the statement.
fn copy_outer(rw: &mut Rewriter<'_>, at: InsertPoint, outer: &Outer) -> Result<ValueId, IrError> {
	let mut renamed: FxHashMap<ValueId, ValueId> = FxHashMap::default();
	for op in &outer.tree {
		let data = rw.ir().op(*op).ok_or(IrError::Stale("op"))?.clone();
		let spec = OpSpec {
			kind: data.kind.clone(),
			operands: data
				.operands
				.iter()
				.map(|operand| renamed.get(operand).copied().unwrap_or(*operand))
				.collect(),
			results: data
				.results
				.iter()
				.map(|result| {
					rw.ir().value(*result).map_or(TypeKnowledge::Unknown, |value| value.ty.clone())
				})
				.collect(),
			frame: data.frame,
			snapshot: data.snapshot,
			loc: data.loc.clone(),
		};
		let created = rw.create(at, spec)?;
		let results = rw.ir().op(created).ok_or(IrError::Stale("op"))?.results.clone();
		for (before, after) in data.results.iter().zip(&results) {
			renamed.insert(*before, *after);
		}
	}
	renamed.get(&outer.root).copied().ok_or(IrError::Stale("value"))
}

/// Puts a probe keyed by the join field at `at`: between `filter` and its input, or between the
/// source of the select loop `filter` stands in and the loop.
fn install_probe(
	rw: &mut Rewriter<'_>,
	at: ProbeAt,
	filter: OpId,
	field: Strand,
	outer: &Outer,
) -> Result<(), IrError> {
	// The op that reads the rows the probe keys, and the place the probe goes in front of.
	let (anchor, slot) = match at {
		ProbeAt::Filter => (filter, InsertPoint::Before(filter)),
		ProbeAt::Loop {
			select,
			before,
		} => (select, InsertPoint::Before(before)),
	};
	let data = rw.ir().op(anchor).ok_or(IrError::Stale("op"))?.clone();
	let input = *data.operands.first().ok_or(IrError::Stale("operand"))?;
	let ty = rw.ir().value(input).map_or(TypeKnowledge::Unknown, |value| value.ty.clone());
	let probe = rw.derived(&[filter], |rw| {
		let key = copy_outer(rw, slot, outer)?;
		rw.create(
			slot,
			OpSpec {
				kind: OpKind::Phys(PhysOp::Probe {
					field,
				}),
				operands: smallvec![input, key],
				results: smallvec![ty],
				frame: data.frame,
				snapshot: data.snapshot,
				loc: data.loc.clone(),
			},
		)
	})?;
	let result = *rw
		.ir()
		.op(probe)
		.and_then(|created| created.results.first())
		.ok_or(IrError::Stale("result"))?;
	rw.set_operand(anchor, 0, result)
}

/// Keeps the statement of `subquery` as it stands as a second region of the op, with a probe
/// keyed by the join field installed at `plan`: the plan the lookup falls back to.
///
/// The copy is made before the lookup replaces the scan of the first region, so the two regions
/// are the statement in its two plans.
fn install_hashed(
	rw: &mut Rewriter<'_>,
	subquery: OpId,
	plan: HashedPlan,
	field: &Strand,
	outer: &Outer,
) -> Result<(), IrError> {
	let first = *rw
		.ir()
		.op(subquery)
		.and_then(|data| data.regions.first())
		.ok_or(IrError::Stale("region"))?;
	let copied = duplicate::region(rw, first, subquery)?;
	let counterpart = |op: &OpId| copied.ops.get(op).copied().ok_or(IrError::Stale("op"));
	let at = match plan.at {
		ProbeAt::Filter => ProbeAt::Filter,
		ProbeAt::Loop {
			select,
			before,
		} => ProbeAt::Loop {
			select: counterpart(&select)?,
			before: counterpart(&before)?,
		},
	};
	install_probe(rw, at, counterpart(&plan.filter)?, field.clone(), outer)
}

/// Replaces the table scan `scan` with a lookup through `index` keyed by the outer path, and
/// answers the lookup.
fn install_lookup(
	rw: &mut Rewriter<'_>,
	scan: OpId,
	field: Strand,
	outer: &Outer,
	index: IndexRef,
) -> Result<OpId, IrError> {
	let data = rw.ir().op(scan).ok_or(IrError::Stale("op"))?.clone();
	let OpKind::Phys(PhysOp::TableScan(attrs)) = &data.kind else {
		return Err(IrError::Stale("a table scan"));
	};
	let result = *data.results.first().ok_or(IrError::Stale("result"))?;
	let ty = rw.ir().value(result).map_or(TypeKnowledge::Unknown, |value| value.ty.clone());
	let lookup = rw.derived(&[scan], |rw| {
		let key = copy_outer(rw, InsertPoint::Before(scan), outer)?;
		rw.create(
			InsertPoint::Before(scan),
			OpSpec {
				kind: OpKind::Phys(PhysOp::IndexLookup(Box::new(IndexLookupAttrs {
					table: attrs.table.clone(),
					read: attrs.read.clone(),
					index,
					field,
					key: LookupKeyShape::Parent,
				}))),
				operands: smallvec![key],
				results: smallvec![ty],
				frame: data.frame,
				snapshot: data.snapshot,
				loc: data.loc.clone(),
			},
		)
	})?;
	let replacement = *rw
		.ir()
		.op(lookup)
		.and_then(|created| created.results.first())
		.ok_or(IrError::Stale("result"))?;
	rw.replace_op(scan, &[replacement])?;
	Ok(lookup)
}

/// Stops every sort of `region` that reads the rows of `lookup` through predicates alone from
/// publishing a top-k threshold: a lookup takes no top-k key, so nothing reads one.
fn quiet_sorts_over(rw: &mut Rewriter<'_>, region: RegionId, lookup: OpId) -> Result<(), IrError> {
	let mut sorts = Vec::new();
	let ir = rw.ir();
	ir.walk(region, WalkOrder::PreOrder, &mut |op| {
		if let Some(data) = ir.op(op)
			&& let OpKind::Phys(PhysOp::Sort(attrs)) = &data.kind
			&& attrs.pushdown
			&& data
				.operands
				.first()
				.copied()
				.and_then(|input| super::rewrite::beneath(ir, input, super::rewrite::keeps_scan))
				.is_some_and(|chain| chain.source == lookup)
		{
			sorts.push(op);
		}
		Walk::Continue
	});
	for sort in sorts {
		rw.update_kind(sort, |kind| {
			if let OpKind::Phys(PhysOp::Sort(attrs)) = kind {
				attrs.pushdown = false;
			}
		})?;
	}
	Ok(())
}
