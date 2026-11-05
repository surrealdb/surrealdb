//! The tree `EXPLAIN` prints for the physical plan of a statement.
//!
//! [`explained_tree`] reads the plan the operand of a `phys.explain` stands for and builds the
//! tree ([`PlanTree`]) `EXPLAIN` renders: one node per physical op, with the attributes the op's
//! own attributes, operands and regions state. The tree is plain data that
//! [`render`](crate::exec::vm::explain::render) turns into the value `EXPLAIN` answers. Nothing
//! here plans or decides: where the plan prints a clause as written, the text is the one the binder
//! recorded on the op that computes it
//! ([`Location::text`](crate::exec::ir::core::location::Location::text)), so the tree prints what
//! the statement said and not what the IR computes.
//!
//! # What a node reads
//!
//! - A source op is a leaf. A `phys.pipeline` and a `phys.choose` are transparent: the rows of a
//!   pipeline are the rows its region yields, and a choice prints its generic arm. A select loop is
//!   its source with the ops of its body wrapped around it in order, the last outermost.
//! - A clause is the text of the op that yields the region. A conjunction prints its conjuncts in
//!   the order the plan evaluates them, joined by `AND`, and a conjunct that tests membership in a
//!   statement prints the statement as `(subquery)` and the statement's plan as a child.
//! - A `LIMIT` that stands over one scan, or over the layers of one table, is the scan's own
//!   paging: the scan prints it and no `Limit` node stands over it.
//! - A filter whose predicate a scan judges of each record's stored bytes (the scan's `prefilter`)
//!   is the scan's own: the scan prints the predicate with `pre_decode_filter: yes` and no `Filter`
//!   node stands over it. A filter that rechecks what an index scan's access states (the scan's
//!   `rechecked`) is the scan's own too, and prints nothing.
//! - A caller whose reads are permission checked is shown `<permission>` where a definer's
//!   permission predicate would print, `<computed>` where a computed body would, and no metrics
//!   beneath a node that enforces a definer's declaration ([`PlanNode::hides_metrics`]).

use surrealdb_strand::TableName;
use surrealdb_types::ToSql;

use super::super::plain::ValueData;
use super::super::program::{Analysis, BlockInstr, Instr, Operand, Program, Source};
use super::emit::Emitter;
use super::explain_hook::{Explained, Purpose};
use super::expr::Lowerer;
use super::pipe::{PoolBase, lower_scope};
use super::place::{Lowered, Provenance};
use super::read::{IndexResolver, bitmap_plan, producer_of};
use super::units::LowerCx;
use crate::exec::AccessMode;
use crate::exec::context::ContextLevel;
use crate::exec::index::access_path::{self, BTreeAccess, access_repeats_records};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ancestors, ops_in};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, KeyShape, NestedStrategy, RaiseKind, ShortCircuitOp};
use crate::exec::ir::iface::IndexRef;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::statements::{BindStepShape, MatchData};
use crate::exec::ir::logical::ops::{
	ComputedReadsShape, FieldShape, FieldsShape, LoopKind, StepAttrs,
};
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::{
	BitmapShape, BranchReadShape, CountShape, IndexBranchShape, KeyBoundShape, KeyEqShape,
	KeyRangeShape, KnnTargetShape, MatchStepAttrs, OmissionShape, PhysOp, PhysTag, ProjectionShape,
	SortKeyReadShape, WriteSubjectsShape,
};
use crate::exec::operators::scan::MergeMode;
use crate::exec::vm::explain::{MeasureKind, MeasureSpec, PlanNode, PlanTree};
use crate::exec::vm::payload::{JoinBuild, JoinMatch, LOOKUP_EVALUATIONS, ValuesRead};
use crate::expr::BinaryOperator;
use crate::expr::logical::{
	EvalMode, OutputPath, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunction,
};
use crate::expr::match_plan::DetachMode;
use crate::kvs::Direction;

/// The deepest plan the tree descends through before it answers a capacity error.
const MAX_DEPTH: usize = 256;

/// What the tree of a plan depends on besides the container.
pub(crate) struct ExplainScope<'a> {
	/// Whether the plan reads under the permissions of a caller they are enforced on.
	pub(crate) checked: bool,
	/// The references of the indexes the plan opens, as planning resolved them.
	pub(crate) indexes: &'a dyn IndexResolver,
}

/// The tree of the plan the op `explain`, a `phys.explain`, explains.
///
/// Answers an internal error for an op that is not a `phys.explain` and for a plan whose ops or
/// regions do not have the shape their tags state.
pub(crate) fn explained_tree(
	ir: &Ir,
	explain: OpId,
	scope: &ExplainScope<'_>,
	purpose: Purpose,
) -> Result<PlanTree, IrError> {
	let data = ir.op(explain).ok_or(IrError::Stale("ir_lower::explain: op"))?;
	let OpKind::Phys(PhysOp::Explain {
		..
	}) = &data.kind
	else {
		return Err(invariant("an explained tree of an op that is not a phys.explain"));
	};
	let [input] = <[ValueId; 1]>::try_from(data.operands.as_slice())
		.map_err(|_| invariant("a phys.explain with other than one operand"))?;
	if let Some(refusal) = refused_statement(ir, input) {
		return Err(IrError::Rejected(Box::new(refusal)));
	}
	let mut walk = TreeWalk {
		ir,
		scope,
		analysed: purpose != Purpose::Plan,
		measure: purpose == Purpose::Measured,
		versioned: false,
		measures: Vec::new(),
		paging: None,
		prefilter: None,
		topk: false,
		depth: 0,
	};
	let root = walk.explained(input)?;
	Ok(PlanTree {
		root,
		measures: walk.measures,
	})
}

/// The name of the operator at the top of the plan `EXPLAIN` renders for the statement the region
/// `region` holds, as an expression holding that statement names it. `None` where the plan cannot
/// be rendered without the indexes planning resolved.
pub(super) fn region_root_name(ir: &Ir, region: RegionId) -> Option<String> {
	let scope = ExplainScope {
		checked: false,
		indexes: &super::read::NoIndexes,
	};
	let mut walk = TreeWalk {
		ir,
		scope: &scope,
		analysed: false,
		measure: false,
		versioned: false,
		measures: Vec::new(),
		paging: None,
		prefilter: None,
		topk: false,
		depth: 0,
	};
	walk.region_plan(region).ok().map(|node| node.name)
}

/// The refusal planning answered for a statement of the plan `value` stands for, if any: the raise
/// that replaced every op of a statement group's list. An `EXPLAIN` answers it as the statement
/// would.
fn refused_statement(ir: &Ir, value: ValueId) -> Option<crate::exec::vm::payload::Refusal> {
	let root = producer_of(ir, value).ok()?;
	let refused = |op: OpId| -> Option<crate::exec::vm::payload::Refusal> {
		let data = ir.op(op)?;
		let OpKind::Phys(PhysOp::Statement {
			..
		}) = &data.kind
		else {
			return None;
		};
		let region = ir.region(*data.regions.first()?)?;
		let [block] = region.blocks.as_slice() else {
			return None;
		};
		let [raise] = ir.block(*block)?.ops.as_slice() else {
			return None;
		};
		if let OpKind::Expr(ExprOp::Raise(kind)) = &ir.op(*raise)?.kind
			&& let RaiseKind::Refused(refusal) = kind.as_ref()
		{
			return Some(refusal.as_ref().clone());
		}
		None
	};
	let mut found = refused(root);
	for region in ir.op(root)?.regions.iter().copied() {
		ir.walk(region, WalkOrder::PreOrder, &mut |op| {
			if found.is_none() {
				found = refused(op);
			}
			Walk::Continue
		});
	}
	found
}

/// The tree that displays the bitmap plan `plan`: one node per node of the plan, none of which
/// redacts or hides anything.
fn bitmap_tree(plan: &access_path::BitmapPlan) -> PlanNode {
	use access_path::BitmapPlan;
	let node = |name: &str, attrs: Vec<(String, String)>, children: Vec<PlanNode>| {
		PlanNode::new(name, ContextLevel::Database, attrs, children)
	};
	match plan {
		BitmapPlan::BTree {
			index_ref,
			access,
		} => {
			let mut attrs = vec![
				("index".to_string(), index_ref.name.to_string()),
				("access".to_string(), access.describe()),
			];
			if let Some(pf) = access.point_filter() {
				attrs.push(("point_filter".to_string(), pf.describe()));
			}
			node("BitmapIndexScan", attrs, vec![])
		}
		BitmapPlan::FullText {
			index_ref,
			query,
			..
		} => node(
			"BitmapFullTextScan",
			vec![
				("index".to_string(), index_ref.name.to_string()),
				("query".to_string(), query.clone()),
			],
			vec![],
		),
		BitmapPlan::Graph {
			source,
			direction,
			edge_tables,
		} => node(
			"BitmapGraphScan",
			vec![
				("source".to_string(), source.to_sql()),
				(
					"direction".to_string(),
					match direction {
						crate::expr::Dir::In => "<-",
						crate::expr::Dir::Out => "->",
						crate::expr::Dir::Both => "<->",
					}
					.to_string(),
				),
				(
					"edges".to_string(),
					edge_tables.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(", "),
				),
			],
			vec![],
		),
		BitmapPlan::And(children) => {
			node("BitmapAnd", vec![], children.iter().map(bitmap_tree).collect())
		}
		BitmapPlan::Or(children) => {
			node("BitmapOr", vec![], children.iter().map(bitmap_tree).collect())
		}
		BitmapPlan::AndNot {
			base,
			subtract,
		} => node("BitmapAndNot", vec![], vec![bitmap_tree(base), bitmap_tree(subtract)]),
	}
}

/// The nodes of rows that no projection has shaped: the sources, and the ops that keep the rows
/// they are given.
const UNPROJECTED_ROWS: &[&str] = &[
	"TableScan",
	"IndexScan",
	"RecordIdScan",
	"FullTextScan",
	"KnnScan",
	"BitmapScan",
	"Union",
	"Subquery",
	"Filter",
	"ProjectLayer",
	"PermissionFilter",
	"Split",
	"Sort",
	"Window",
	"Limit",
	"DistinctRecords",
];

/// The SQL name of a window function.
fn window_function_name(function: WindowFunction) -> &'static str {
	match function {
		WindowFunction::RowNumber => "row_number",
		WindowFunction::Rank => "rank",
		WindowFunction::DenseRank => "dense_rank",
		WindowFunction::PercentRank => "percent_rank",
		WindowFunction::CumeDist => "cume_dist",
		WindowFunction::Ntile => "ntile",
		WindowFunction::Lag => "lag",
		WindowFunction::Lead => "lead",
		WindowFunction::FirstValue => "first_value",
		WindowFunction::LastValue => "last_value",
		WindowFunction::NthValue => "nth_value",
		WindowFunction::Sum => "sum",
		WindowFunction::Avg => "avg",
		WindowFunction::Count => "count",
		WindowFunction::Min => "min",
		WindowFunction::Max => "max",
	}
}

/// A window frame as SQL writes it: its units, then its two bounds.
fn frame_text(frame: &WindowFrame) -> String {
	let units = match frame.units {
		WindowFrameUnits::Rows => "ROWS",
		WindowFrameUnits::Range => "RANGE",
	};
	format!("{units} BETWEEN {} AND {}", bound_text(frame.start), bound_text(frame.end))
}

/// One end of a window frame as SQL writes it.
fn bound_text(bound: WindowFrameBound) -> String {
	match bound {
		WindowFrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
		WindowFrameBound::Preceding(offset) => format!("{offset} PRECEDING"),
		WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
		WindowFrameBound::Following(offset) => format!("{offset} FOLLOWING"),
		WindowFrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
	}
}

/// The name and the attributes of a projection's node.
type Projected = (&'static str, Vec<(String, String)>);

/// The statements nested in the membership conjuncts of a filter, which print beneath it.
type Memberships = Vec<PlanNode>;

/// How a plan is turned into nodes.
struct TreeWalk<'a> {
	ir: &'a Ir,
	scope: &'a ExplainScope<'a>,
	/// Whether the tree is for `EXPLAIN ANALYZE`, whose nodes report the one set of metrics the
	/// run records.
	analysed: bool,
	/// Whether the bitmap trees of the plan are measured by evaluating them after the statement
	/// runs, rather than described with the metrics the nodes share.
	measure: bool,
	/// Whether the plan being walked reads at a `VERSION`.
	versioned: bool,
	/// The bitmap trees the plan is measured by.
	measures: Vec<MeasureSpec>,
	/// The paging a scan below takes, for the scan to show as its own: set where a `Limit` stands
	/// directly over the scan, taken by the scan.
	paging: Option<Vec<(String, String)>>,
	/// The text of the predicate a scan below judges of each record's stored bytes: set where the
	/// filter that predicate belongs to stands directly over the scan, taken by the scan.
	prefilter: Option<String>,
	/// Whether a sort above that keeps a bounded top hands its key to the scan below: set where
	/// the sort stands over its input, taken by the first source rendered beneath it.
	topk: bool,
	depth: usize,
}

impl<'a> TreeWalk<'a> {
	fn node(
		&self,
		name: &'static str,
		context: ContextLevel,
		attrs: Vec<(String, String)>,
		children: Vec<PlanNode>,
	) -> PlanNode {
		PlanNode::new(name, context, attrs, children)
	}

	// -----------------------------------------------------------------------------------------
	// Structure
	// -----------------------------------------------------------------------------------------

	/// The node of the plan that delivers `value`.
	fn plan_of(&mut self, value: ValueId) -> Result<PlanNode, IrError> {
		if self.depth >= MAX_DEPTH {
			return Err(IrError::Capacity("explained plan depth"));
		}
		self.depth += 1;
		let node = self.plan_of_value(value);
		self.depth = self.depth.saturating_sub(1);
		node
	}

	fn plan_of_value(&mut self, value: ValueId) -> Result<PlanNode, IrError> {
		let ValueDef::OpResult {
			op,
			..
		} = self.ir.value(value).ok_or(IrError::Stale("ir_lower::explain: value"))?.def
		else {
			// A value no op produces, a block's argument, is read by its name.
			return Ok(self.expression(value));
		};
		let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		match &data.kind {
			// A value a `LET` binds is read by its name; the `LET` is a statement of its own.
			OpKind::Phys(PhysOp::Let(_)) => Ok(self.expression(value)),
			// The rendering a plain `EXPLAIN` answers is a value, which no plan stands for.
			OpKind::Phys(PhysOp::Explain {
				analyze: false,
				..
			}) => Ok(self.expression(value)),
			OpKind::Phys(kind) => self.plan_of_op(op, kind),
			// A value that is no plan is the value the statement evaluates.
			OpKind::Expr(_) => Ok(self.expression(value)),
			OpKind::Logical(_) => Err(invariant("a logical op in a physical plan")),
		}
	}

	/// The node of the plan `kind`, the op `op`, delivers.
	fn plan_of_op(&mut self, op: OpId, kind: &PhysOp) -> Result<PlanNode, IrError> {
		let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		let operands = data.operands.as_slice();
		let regions = data.regions.as_slice();
		match kind {
			PhysOp::Pipeline(_) => {
				let region =
					*regions.first().ok_or_else(|| invariant("a pipeline has no region"))?;
				let yielded = self.yielded(region)?;
				self.plan_of(yielded)
			}
			PhysOp::Choose(spec) => {
				// The generic arm is the one every other arm's answer equals.
				let last = spec.arms.len().saturating_sub(1);
				let region = *regions
					.get(last)
					.ok_or_else(|| invariant("a choice holds a region per arm"))?;
				let yielded = self.yielded(region)?;
				self.plan_of(yielded)
			}
			PhysOp::Loop {
				kind: LoopKind::Select,
				..
			} => self.select_loop(op),
			PhysOp::Loop {
				kind: LoopKind::For,
				..
			} => self.foreach(op),
			// The loop over a write's subjects stands for the rows it reads them from.
			PhysOp::Loop {
				kind: LoopKind::Write,
				..
			} => self.write_source(self.operand(operands, 0)?),
			// Any other loop (a graph step, a closure, an element filter, a recursion) computes a
			// value of the expression the statement evaluates, which is the plan.
			PhysOp::Loop {
				..
			} => {
				let result = data
					.results
					.first()
					.copied()
					.ok_or(IrError::Stale("ir_lower::explain: loop result"))?;
				Ok(self.expression(result))
			}
			PhysOp::Only => {
				let child = self.plan_of(self.operand(operands, 0)?)?;
				Ok(self.node("UnwrapExactlyOne", ContextLevel::Root, vec![], vec![child]))
			}
			PhysOp::VersionScope => {
				let version = self.text_of(self.operand(operands, 1)?);
				let outer = std::mem::replace(&mut self.versioned, true);
				let child = self.plan_of(self.operand(operands, 0)?);
				self.versioned = outer;
				Ok(self.node(
					"VersionScope",
					ContextLevel::Database,
					vec![("version".to_string(), version)],
					vec![child?],
				))
			}
			PhysOp::Timeout => {
				let timeout = self.text_of(self.operand(operands, 0)?);
				let region =
					*regions.first().ok_or_else(|| invariant("a timeout has no region"))?;
				let yielded = self.yielded(region)?;
				let child = self.plan_of(yielded)?;
				Ok(self.node(
					"Timeout",
					ContextLevel::Root,
					vec![("timeout".to_string(), timeout)],
					vec![child],
				))
			}
			PhysOp::Subquery => {
				let region =
					*regions.first().ok_or_else(|| invariant("a subquery has no region"))?;
				let child = self.nested(region)?;
				Ok(self.node("Subquery", ContextLevel::Root, vec![], vec![child]))
			}
			PhysOp::TableScan(_)
			| PhysOp::IndexScan(_)
			| PhysOp::IndexUnion(_)
			| PhysOp::Bitmap(_)
			| PhysOp::IndexLookup(_)
			| PhysOp::FullTextScan(_)
			| PhysOp::KeyRangeScan(_)
			| PhysOp::KnnScan(_)
			| PhysOp::Count(_)
			| PhysOp::RecordScan(..)
			| PhysOp::Vacant {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Union => self.source(op, kind),
			PhysOp::Enforced {
				..
			}
			| PhysOp::KnnTopK(_)
			| PhysOp::Probe {
				..
			}
			| PhysOp::Join(_)
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(_)
			| PhysOp::Layer(_)
			| PhysOp::Filter
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Sort(_)
			| PhysOp::Window(_)
			| PhysOp::Aggregate(_)
			| PhysOp::Limit {
				..
			}
			| PhysOp::Project(_) => self.relational(op, kind),
			PhysOp::Eval => {
				let value = self.operand(operands, 0)?;
				let node = self.expression(value);
				if regions.is_empty() {
					return Ok(node);
				}
				let fetches = regions
					.iter()
					.map(|region| self.region_text(*region))
					.collect::<Result<Vec<_>, _>>()?;
				Ok(self.node(
					"Fetch",
					ContextLevel::Database,
					vec![("fetches".to_string(), fetches.join(", "))],
					vec![node],
				))
			}
			PhysOp::Write(attrs) => {
				let form = match attrs.write.form {
					crate::exec::ir::iface::statements::WriteForm::Create => "CREATE",
					crate::exec::ir::iface::statements::WriteForm::Update => "UPDATE",
					crate::exec::ir::iface::statements::WriteForm::Upsert => "UPSERT",
					crate::exec::ir::iface::statements::WriteForm::Delete => "DELETE",
					crate::exec::ir::iface::statements::WriteForm::Insert => "INSERT",
					crate::exec::ir::iface::statements::WriteForm::Relate => "RELATE",
				};
				let children = match attrs.subjects {
					WriteSubjectsShape::Rows | WriteSubjectsShape::Guaranteed => {
						vec![self.write_rows(self.operand(operands, 0)?)?]
					}
					WriteSubjectsShape::Targets
					| WriteSubjectsShape::Inserts
					| WriteSubjectsShape::Edges => vec![],
				};
				Ok(self.node(
					"WriteOperator",
					ContextLevel::Database,
					vec![("form".to_string(), form.to_string())],
					children,
				))
			}
			PhysOp::Define(_) | PhysOp::Remove(_) | PhysOp::Alter(_) | PhysOp::Rebuild(_) => {
				Ok(self.node("DdlPlan", ContextLevel::Root, vec![], vec![]))
			}
			PhysOp::Info {
				..
			} => Ok(self.node("Info", ContextLevel::Database, vec![], vec![])),
			PhysOp::Statement {
				..
			} => {
				let region =
					*regions.first().ok_or_else(|| invariant("a statement has no region"))?;
				self.region_plan(region)
			}
			PhysOp::Block => {
				let region = *regions.first().ok_or_else(|| invariant("a block has no region"))?;
				self.sequence(region)
			}
			PhysOp::Let(attrs) => {
				let child = self.plan_of(self.operand(operands, 0)?)?;
				Ok(self.node(
					"Let",
					ContextLevel::Root,
					vec![("name".to_string(), format!("${}", attrs.name))],
					vec![child],
				))
			}
			PhysOp::Sleep => {
				let duration = self.text_of(self.operand(operands, 0)?);
				Ok(self.node(
					"Sleep",
					ContextLevel::Root,
					vec![("duration".to_string(), duration)],
					vec![],
				))
			}
			PhysOp::Return => {
				let child = self.plan_of(self.operand(operands, 0)?)?;
				Ok(self.node("Return", ContextLevel::Root, vec![], vec![child]))
			}
			PhysOp::If {
				..
			} => {
				// An `IF` whose condition is settled true is the block of its arm.
				if self.settled_true(self.operand(operands, 0)?) {
					let then = *regions.first().ok_or_else(|| invariant("an if has no arm"))?;
					return self.sequence(then);
				}
				let mut children = Vec::new();
				self.branches(op, &mut children)?;
				Ok(self.node("If", ContextLevel::Root, vec![], children))
			}
			// A `MATCH` is the plan its one region holds: the sources, filters and binding steps
			// of the fold.
			PhysOp::Match {
				..
			} => {
				let region = *regions.first().ok_or_else(|| invariant("a MATCH has no region"))?;
				self.nested(region)
			}
			PhysOp::MatchStep(attrs) => match &**attrs {
				MatchStepAttrs::Bind(shape) => self.bind_step(op, shape),
				MatchStepAttrs::Read {
					..
				}
				| MatchStepAttrs::Update(_)
				| MatchStepAttrs::Delete {
					..
				}
				| MatchStepAttrs::Insert(_)
				| MatchStepAttrs::Output(_) => Err(invariant("a MATCH stage op in a plan the fold built")),
			},
			PhysOp::Break => Ok(self.node("Break", ContextLevel::Root, vec![], vec![])),
			PhysOp::Next => Ok(self.node("Continue", ContextLevel::Root, vec![], vec![])),
			PhysOp::DdlGuard {
				..
			}
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Explain {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(_)
			| PhysOp::Kill
			| PhysOp::Live(_)
			| PhysOp::Subscriber(_)
			| PhysOp::Show(_)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(_)
			| PhysOp::Assign(_)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Prefetch(_)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(_)
			| PhysOp::Yield => Err(invariant("an op that no EXPLAIN tree stands for in a plan")),
		}
	}

	/// The operand `at` of an op.
	fn operand(&self, operands: &[ValueId], at: usize) -> Result<ValueId, IrError> {
		operands.get(at).copied().ok_or_else(|| invariant("a physical op lacks an operand"))
	}

	/// The value the region `region` yields: the operand of the `phys.yield` its one block ends in.
	fn yielded(&self, region: RegionId) -> Result<ValueId, IrError> {
		let data = self.ir.region(region).ok_or(IrError::Stale("ir_lower::explain: region"))?;
		let [block] = data.blocks.as_slice() else {
			return Err(invariant("a plan region holds one block"));
		};
		let last = self
			.ir
			.block(*block)
			.and_then(|held| held.ops.last().copied())
			.ok_or_else(|| invariant("a plan region holds no op"))?;
		let closing = self.ir.op(last).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		match (&closing.kind, closing.operands.as_slice()) {
			(OpKind::Phys(PhysOp::Yield) | OpKind::Expr(ExprOp::Yield), [value]) => Ok(*value),
			_ => Err(invariant("a plan region ends in a yield of one value")),
		}
	}

	/// The plan of the statement whose rows `value` holds. A statement delivers projected rows:
	/// rows its plan does not project, a grouped statement's among them, print beneath
	/// `SelectProject` with every field, above whatever sorts and pages them.
	fn statement_plan(&mut self, value: ValueId) -> Result<PlanNode, IrError> {
		let node = self.plan_of(value)?;
		if self.aggregated(value) || UNPROJECTED_ROWS.contains(&node.name.as_str()) {
			return Ok(self.projected_whole_always(node));
		}
		Ok(node)
	}

	fn projected_whole_always(&self, node: PlanNode) -> PlanNode {
		self.node(
			"SelectProject",
			ContextLevel::Database,
			vec![("projections".to_string(), "*".to_string())],
			vec![node],
		)
	}

	/// The plan of the statement an `EXPLAIN` explains, whose value is `value`.
	///
	/// A statement bound as the one statement of a statement group is the statement its region
	/// lists: the one statement it holds, a `Sequence` of the statements of more, or, for a
	/// statement that leaves no op in the plan, the text the subquery carries. A `LET` standing
	/// alone is answered in the context of the plan of its value.
	fn explained(&mut self, value: ValueId) -> Result<PlanNode, IrError> {
		let region = producer_of(self.ir, value).ok().and_then(|op| {
			let data = self.ir.op(op)?;
			match (&data.kind, data.regions.as_slice()) {
				(
					OpKind::Phys(PhysOp::Statement {
						..
					}),
					[region],
				) => Some(*region),
				_ => None,
			}
		});
		let Some(region) = region else {
			return self.statement_plan(value);
		};
		let mut items = self.statements(region)?;
		match items.len() {
			0 => Ok(self.expression(value)),
			1 => {
				let mut item = items.remove(0);
				if item.name == "Let"
					&& let Some(context) = item.children.first().map(|child| child.context)
				{
					item.context = context;
				}
				Ok(item)
			}
			count => Ok(self.node(
				"Sequence",
				ContextLevel::Root,
				vec![("statements".to_string(), count.to_string())],
				items,
			)),
		}
	}

	/// Whether the rows `value` holds are those of an aggregate or a count, beneath what sorts,
	/// windows and pages them alone.
	fn aggregated(&self, value: ValueId) -> bool {
		let Ok(op) = producer_of(self.ir, value) else {
			return false;
		};
		let Some(data) = self.ir.op(op) else {
			return false;
		};
		match &data.kind {
			OpKind::Phys(PhysOp::Aggregate(_) | PhysOp::Count(_)) => true,
			OpKind::Phys(
				PhysOp::Sort(_)
				| PhysOp::Window(_)
				| PhysOp::Limit {
					..
				},
			) => data.operands.first().is_some_and(|input| self.aggregated(*input)),
			OpKind::Phys(PhysOp::Pipeline(_)) => data
				.regions
				.first()
				.and_then(|region| self.yielded(*region).ok())
				.is_some_and(|yielded| self.aggregated(yielded)),
			OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => false,
		}
	}

	/// The plan a nested statement's region holds. The `VERSION` in force beneath a statement's
	/// plan belongs to that statement alone.
	fn nested(&mut self, region: RegionId) -> Result<PlanNode, IrError> {
		let outer = std::mem::replace(&mut self.versioned, false);
		let node = self.region_plan(region);
		self.versioned = outer;
		node
	}

	/// The plan a region holds: the plan of the one value it yields, or of its one statement.
	fn region_plan(&mut self, region: RegionId) -> Result<PlanNode, IrError> {
		let last = self
			.ir
			.region(region)
			.and_then(|data| data.blocks.last().copied())
			.and_then(|block| self.ir.block(block))
			.and_then(|block| block.ops.last().copied())
			.and_then(|op| self.ir.op(op));
		match last.map(|closing| (&closing.kind, closing.operands.as_slice())) {
			Some((OpKind::Phys(PhysOp::Yield) | OpKind::Expr(ExprOp::Yield), [value])) => {
				self.statement_plan(*value)
			}
			_ => {
				let mut items = self.statements(region)?;
				match items.len() {
					1 => Ok(items.remove(0)),
					count => Ok(self.node(
						"Sequence",
						ContextLevel::Root,
						vec![("statements".to_string(), count.to_string())],
						items,
					)),
				}
			}
		}
	}

	/// The plans of the statements a list holds, in order: see [`Self::statement_ops`].
	fn statements(&mut self, region: RegionId) -> Result<Vec<PlanNode>, IrError> {
		let mut items = Vec::new();
		for op in self.statement_ops(region)? {
			let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
			let OpKind::Phys(kind) = &data.kind else {
				continue;
			};
			items.push(self.plan_of_op(op, kind)?);
		}
		Ok(items)
	}

	/// The statements a list holds: the physical ops of the region that nothing in it or beneath
	/// it reads but the yield that ends it, whose value is the last statement's, in order. A `LET`
	/// is a statement of its own, whatever reads the name it binds.
	fn statement_ops(&self, region: RegionId) -> Result<Vec<OpId>, IrError> {
		let mut statements = Vec::new();
		for op in ops_in(self.ir, region) {
			let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
			let OpKind::Phys(kind) = &data.kind else {
				continue;
			};
			if matches!(kind, PhysOp::Yield | PhysOp::DdlGuard { .. } | PhysOp::Prefetch(_)) {
				continue;
			}
			let read = !matches!(kind, PhysOp::Let(_))
				&& data.results.iter().any(|result| {
					self.ir.uses(*result).iter().any(|used| {
						!matches!(
							self.ir.op(used.user).map(|user| &user.kind),
							Some(OpKind::Phys(PhysOp::Yield) | OpKind::Expr(ExprOp::Yield))
						) && (self.ir.enclosing_region(used.user) == region
							|| ancestors(self.ir, used.user).any(|(held, _)| held == region))
					})
				});
			if !read {
				statements.push(op);
			}
		}
		Ok(statements)
	}

	/// The plans of the statements of the list `region` holds: its statements, then the value it
	/// yields where no statement computes that value and the value is one the statement wrote.
	/// The `NONE` a list that ends in a `LET` yields is no statement.
	fn list(&mut self, region: RegionId) -> Result<Vec<PlanNode>, IrError> {
		let mut items = self.statements(region)?;
		if let Ok(yielded) = self.yielded(region)
			&& let Ok(producer) = producer_of(self.ir, yielded)
			&& let Some(data) = self.ir.op(producer)
			&& let OpKind::Expr(_) = &data.kind
			&& data.loc.text().is_some()
		{
			items.push(self.plan_of(yielded)?);
		}
		Ok(items)
	}

	/// The `Sequence` of the statements of the list `region` holds.
	fn sequence(&mut self, region: RegionId) -> Result<PlanNode, IrError> {
		let items = self.list(region)?;
		Ok(self.sequence_of(items))
	}

	fn sequence_of(&self, items: Vec<PlanNode>) -> PlanNode {
		self.node(
			"Sequence",
			ContextLevel::Root,
			vec![("statements".to_string(), items.len().to_string())],
			items,
		)
	}

	/// Whether `value` is the literal `true`.
	fn settled_true(&self, value: ValueId) -> bool {
		producer_of(self.ir, value).ok().and_then(|op| self.ir.op(op)).is_some_and(|data| {
			matches!(&data.kind, OpKind::Expr(ExprOp::Literal(literal))
				if matches!(literal.as_ref(), crate::val::Value::Bool(true)))
		})
	}

	/// The conditions and bodies of the `IF` `op` and of the `IF`s that follow it, then the `ELSE`.
	fn branches(&mut self, op: OpId, out: &mut Vec<PlanNode>) -> Result<(), IrError> {
		let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		let condition = self.operand(data.operands.as_slice(), 0)?;
		let [then, otherwise] = <[RegionId; 2]>::try_from(data.regions.as_slice())
			.map_err(|_| invariant("an if holds a region per arm"))?;
		out.push(self.plan_of(condition)?);
		out.push(self.sequence(then)?);
		// An `ELSE` that holds one `IF` and nothing else is an `ELSE IF`.
		let statements = self.statement_ops(otherwise)?;
		if let [chained] = statements.as_slice()
			&& matches!(
				self.ir.op(*chained).map(|data| &data.kind),
				Some(OpKind::Phys(PhysOp::If { .. }))
			) {
			return self.branches(*chained, out);
		}
		let items = self.list(otherwise)?;
		if !items.is_empty() {
			out.push(self.sequence_of(items));
		}
		Ok(())
	}

	/// A `FOR`: its range and its body.
	fn foreach(&mut self, op: OpId) -> Result<PlanNode, IrError> {
		let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		let range = self.plan_of(self.operand(data.operands.as_slice(), 0)?)?;
		let region = *data.regions.first().ok_or_else(|| invariant("a FOR has no body"))?;
		// The parameter the loop states; a loop built with none prints the name the binder gave
		// its element, if any.
		let param = if let OpKind::Phys(PhysOp::Loop {
			step,
			..
		}) = &data.kind
		{
			if let Some(StepAttrs::Param(name)) = step.as_deref() {
				Some(name.clone())
			} else {
				None
			}
		} else {
			None
		};
		let param = param
			.or_else(|| {
				let held = self.ir.region(region)?;
				let block = self.ir.block(*held.blocks.first()?)?;
				self.ir.value(*block.args.first()?)?.hint_name.clone()
			})
			.map(|name| format!("${name}"))
			.unwrap_or_default();
		let body = self.sequence(region)?;
		Ok(self.node(
			"Foreach",
			ContextLevel::Database,
			vec![("param".to_string(), param)],
			vec![range, body],
		))
	}

	/// The rows of the loop that delivers a write's subjects, as the write's input.
	fn write_rows(&mut self, subjects: ValueId) -> Result<PlanNode, IrError> {
		let producer = producer_of(self.ir, subjects)?;
		let data = self.ir.op(producer).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		match &data.kind {
			OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::Write,
				..
			}) => self.write_source(self.operand(data.operands.as_slice(), 0)?),
			OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => self.plan_of(subjects),
		}
	}

	/// The rows a write reads its subjects from: the plan of `source`, each row projected whole.
	fn write_source(&mut self, source: ValueId) -> Result<PlanNode, IrError> {
		let node = self.plan_of(source)?;
		Ok(self.projected_whole(node))
	}

	// -----------------------------------------------------------------------------------------
	// Text
	// -----------------------------------------------------------------------------------------

	/// The expression `value` computes, as the statement wrote it.
	///
	/// The text the binder recorded on the op that computes the value; for a value no op computes,
	/// the name it carries; otherwise the literal or the parameter the op is, or the op's name.
	fn text_of(&self, value: ValueId) -> String {
		let Some(held) = self.ir.value(value) else {
			return String::new();
		};
		let ValueDef::OpResult {
			op,
			..
		} = held.def
		else {
			return held.hint_name.as_ref().map(|name| format!("${name}")).unwrap_or_default();
		};
		let Some(data) = self.ir.op(op) else {
			return String::new();
		};
		// A plain `EXPLAIN` is the rendering of the plan it explains, whatever text it was written
		// as.
		if let OpKind::Phys(PhysOp::Explain {
			analyze: false,
			format,
		}) = &data.kind
		{
			return match explained_tree(self.ir, op, self.scope, Purpose::Plan) {
				Ok(tree) => crate::exec::vm::explain::render(&tree, *format, None).to_sql(),
				Err(_) => PhysTag::Explain.mnemonic().to_string(),
			};
		}
		if let Some(text) = data.loc.text() {
			return text.to_owned();
		}
		match &data.kind {
			OpKind::Phys(PhysOp::Let(attrs)) => format!("${}", attrs.name),
			OpKind::Expr(ExprOp::Literal(literal)) => literal.to_sql(),
			// A record id whose key the op states in full is the id it builds.
			OpKind::Expr(op @ ExprOp::RecordId(attrs)) => {
				let key = match &attrs.key_shape {
					KeyShape::Number(number) => Some(crate::val::RecordIdKey::from(*number)),
					KeyShape::String(string) => Some(crate::val::RecordIdKey::from(string.clone())),
					KeyShape::Uuid(_)
					| KeyShape::Generate(_)
					| KeyShape::Array {
						..
					}
					| KeyShape::Object {
						..
					}
					| KeyShape::Range(_) => None,
				};
				match key {
					Some(key) => crate::val::RecordId {
						table: attrs.table.clone(),
						key,
					}
					.to_sql(),
					None => op.tag().mnemonic().to_string(),
				}
			}
			OpKind::Expr(ExprOp::FreeParam {
				name,
			})
			| OpKind::Expr(ExprOp::SessionParam {
				name,
			}) => format!("${name}"),
			OpKind::Expr(op) => op.tag().mnemonic().to_string(),
			OpKind::Phys(op) => op.tag().mnemonic().to_string(),
			OpKind::Logical(op) => op.tag().mnemonic().to_string(),
		}
	}

	/// The text of the value a region yields.
	fn region_text(&self, region: RegionId) -> Result<String, IrError> {
		Ok(self.text_of(self.yielded(region)?))
	}

	/// The node of a value that no plan stands for: the statement is the expression.
	fn expression(&self, value: ValueId) -> PlanNode {
		self.node(
			"ExprPlan",
			ContextLevel::Root,
			vec![("expr".to_string(), self.text_of(value))],
			vec![],
		)
	}

	/// The conjuncts of the predicate a region yields, in the order the plan evaluates them: the
	/// values the chain of lazy `AND`s joins, the first conjunct first.
	fn conjuncts(&self, value: ValueId, out: &mut Vec<ValueId>) -> Result<(), IrError> {
		if let Some(held) = self.ir.value(value)
			&& let ValueDef::OpResult {
				op,
				..
			} = held.def
			&& let Some(data) = self.ir.op(op)
			&& let OpKind::Expr(ExprOp::ShortCircuit {
				op: ShortCircuitOp::And,
			}) = &data.kind
		{
			let [left] = <[ValueId; 1]>::try_from(data.operands.as_slice())
				.map_err(|_| invariant("a lazy AND with other than one operand"))?;
			let [lazy] = <[RegionId; 1]>::try_from(data.regions.as_slice())
				.map_err(|_| invariant("a lazy AND with other than one region"))?;
			self.conjuncts(left, out)?;
			let right = self.yielded(lazy)?;
			return self.conjuncts(right, out);
		}
		out.push(value);
		Ok(())
	}

	/// Whether `value` is a conjunct that an operator of lower precedence than `AND` heads: it is
	/// written in parentheses where it stands among others.
	fn is_loose(&self, value: ValueId) -> bool {
		let op = self.ir.value(value).and_then(|held| match held.def {
			ValueDef::OpResult {
				op,
				..
			} => self.ir.op(op),
			ValueDef::BlockArg {
				..
			} => None,
		});
		matches!(
			op.map(|data| &data.kind),
			Some(OpKind::Expr(ExprOp::ShortCircuit {
				op: ShortCircuitOp::Or
					| ShortCircuitOp::NullCoalescing
					| ShortCircuitOp::TernaryCondition,
			}))
		)
	}

	/// The statement a conjunct tests membership in (`x INSIDE (statement)`), when it is one.
	fn membership(&self, value: ValueId) -> Option<(ValueId, &'static str, OpId)> {
		let ValueDef::OpResult {
			op,
			..
		} = self.ir.value(value)?.def
		else {
			return None;
		};
		let data = self.ir.op(op)?;
		let OpKind::Expr(ExprOp::Binary {
			op: operator,
		}) = &data.kind
		else {
			return None;
		};
		let word = match operator {
			BinaryOperator::Inside => "INSIDE",
			BinaryOperator::NotInside => "NOT INSIDE",
			BinaryOperator::Subtract
			| BinaryOperator::Add
			| BinaryOperator::Multiply
			| BinaryOperator::Divide
			| BinaryOperator::Remainder
			| BinaryOperator::Power
			| BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual
			| BinaryOperator::Or
			| BinaryOperator::And
			| BinaryOperator::NullCoalescing
			| BinaryOperator::TenaryCondition
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::Contain
			| BinaryOperator::NotContain
			| BinaryOperator::ContainAll
			| BinaryOperator::ContainAny
			| BinaryOperator::ContainNone
			| BinaryOperator::AllInside
			| BinaryOperator::AnyInside
			| BinaryOperator::NoneInside
			| BinaryOperator::Outside
			| BinaryOperator::Intersects
			| BinaryOperator::Range
			| BinaryOperator::RangeInclusive
			| BinaryOperator::RangeSkip
			| BinaryOperator::RangeSkipInclusive
			| BinaryOperator::Matches(_)
			| BinaryOperator::NearestNeighbor(_) => return None,
		};
		let [left, right] = <[ValueId; 2]>::try_from(data.operands.as_slice()).ok()?;
		let ValueDef::OpResult {
			op: statement,
			..
		} = self.ir.value(right)?.def
		else {
			return None;
		};
		matches!(self.ir.op(statement)?.kind, OpKind::Expr(ExprOp::Subquery(_)))
			.then_some((left, word, statement))
	}

	/// The text of the predicate a filter's region `region` yields, and the statements its
	/// conjuncts test membership in. A statement is shown as `(subquery)`: its plan is the child
	/// the filter holds.
	fn predicate(&mut self, region: RegionId) -> Result<(String, Memberships), IrError> {
		let yielded = self.yielded(region)?;
		let mut conjuncts = Vec::new();
		self.conjuncts(yielded, &mut conjuncts)?;
		let mut texts = Vec::with_capacity(conjuncts.len());
		let mut children: Memberships = Vec::new();
		let mut members = false;
		for conjunct in &conjuncts {
			if self.membership(*conjunct).is_some() {
				members = true;
			}
		}
		for conjunct in &conjuncts {
			match self.membership(*conjunct) {
				Some((left, word, statement)) => {
					texts.push(format!("{} {word} (subquery)", self.text_of(left)));
					let data =
						self.ir.op(statement).ok_or(IrError::Stale("ir_lower::explain: op"))?;
					let OpKind::Expr(ExprOp::Subquery(strategy)) = &data.kind else {
						return Err(invariant("a membership statement that is no subquery"));
					};
					let strategy = strategy_text(strategy);
					let nested = *data
						.regions
						.first()
						.ok_or_else(|| invariant("a subquery expression has no region"))?;
					let plan = self.nested(nested)?;
					children.push(self.node(
						"Subquery",
						ContextLevel::Database,
						vec![("strategy".to_string(), strategy.to_string())],
						vec![plan],
					));
				}
				None => {
					let text = self.text_of(*conjunct);
					// A disjunction among conjuncts is written in parentheses.
					let loose = conjuncts.len() > 1 || members;
					texts.push(if loose && self.is_loose(*conjunct) {
						format!("({text})")
					} else {
						text
					});
				}
			}
		}
		Ok((texts.join(" AND "), children))
	}

	/// Whether the predicate a region yields is the literal `true`: what a filter whose every
	/// conjunct is judged once judges per row.
	fn is_true(&self, region: RegionId) -> bool {
		self.yielded(region)
			.ok()
			.and_then(|value| match self.ir.value(value)?.def {
				ValueDef::OpResult {
					op,
					..
				} => self.ir.op(op),
				ValueDef::BlockArg {
					..
				} => None,
			})
			.is_some_and(|data| {
				matches!(&data.kind, OpKind::Expr(ExprOp::Literal(value))
					if matches!(**value, crate::val::Value::Bool(true)))
			})
	}

	// -----------------------------------------------------------------------------------------
	// Sources
	// -----------------------------------------------------------------------------------------

	fn resolve(&self, index: &IndexRef) -> Result<access_path::IndexRef, IrError> {
		self.scope
			.indexes
			.resolve(index)
			.ok_or_else(|| invariant("an index the planning session never resolved"))
	}

	/// A B-tree scan. A key range that leaves a fanning column open reaches a record through
	/// several entries, which the host collapses to one row per record, so the scan sits beneath a
	/// `DistinctRecords`.
	fn btree_scan(
		&mut self,
		table: &TableName,
		index: &IndexRef,
		access: &BTreeAccess,
		direction: Direction,
		decode: Option<&crate::exec::operators::scan::common::DecodeFields>,
	) -> Result<PlanNode, IrError> {
		let resolved = self.resolve(index)?;
		let repeats = access_repeats_records(&resolved, access);
		let mut attrs = index_scan_attrs(table, index.index.as_str(), access, direction);
		if let Some(decode) = decode {
			if access_path::index_scan_is_id_only(repeats, access, self.versioned, Some(decode)) {
				attrs.push(("id_only".to_string(), "yes".to_string()));
			} else {
				attrs.push(("decode".to_string(), decode.describe()));
			}
		}
		attrs.extend(self.paging.take().unwrap_or_default());
		let scan = self.node("IndexScan", ContextLevel::Database, attrs, vec![]);
		Ok(if repeats {
			self.node("DistinctRecords", ContextLevel::Database, vec![], vec![scan])
		} else {
			scan
		})
	}

	/// The children that display the bitmap tree `plan`. Under `EXPLAIN ANALYZE` its nodes report
	/// the metrics the nodes of this walk share.
	fn bitmap_nodes(&self, plan: &access_path::BitmapPlan) -> Vec<PlanNode> {
		vec![bitmap_tree(plan)]
	}

	fn bitmap_plan_of(&self, shape: &BitmapShape) -> Result<access_path::BitmapPlan, IrError> {
		bitmap_plan(shape, &|index| self.resolve(index))
	}

	/// Makes the bitmap tree `plan` of `table` the one child of `node`, measured as `kind` states
	/// after the statement runs: its nodes report what that evaluation produced, and `node` what
	/// it decided.
	fn measured_beneath(
		&mut self,
		node: &mut PlanNode,
		table: &TableName,
		plan: access_path::BitmapPlan,
		kind: MeasureKind,
	) -> Result<(), IrError> {
		let tree = bitmap_tree(&plan);
		let root = ValueData::new(plan, "a measured bitmap plan")
			.map_err(|_| invariant("a measured bitmap plan holds a closure value"))?;
		let at = u16::try_from(self.measures.len())
			.map_err(|_| IrError::Capacity("measured bitmap trees"))?;
		node.children = vec![tree];
		node.measure = Some(at);
		self.measures.push(MeasureSpec {
			table: table.clone(),
			root,
			kind,
		});
		Ok(())
	}

	fn source(&mut self, op: OpId, kind: &PhysOp) -> Result<PlanNode, IrError> {
		let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		let operands = data.operands.as_slice();
		let regions = data.regions.as_slice();
		let topk = std::mem::take(&mut self.topk);
		match kind {
			PhysOp::TableScan(attrs) => {
				let mut scan = vec![
					("table".to_string(), attrs.table.to_string()),
					("direction".to_string(), format!("{:?}", attrs.direction)),
				];
				let prefiltered = self.prefilter.take().filter(|_| attrs.prefilter.is_some());
				if let Some(predicate) = &prefiltered {
					scan.push(("predicate".to_string(), predicate.clone()));
				}
				scan.extend(self.paging.take().unwrap_or_default());
				if prefiltered.is_some() {
					scan.push(("pre_decode_filter".to_string(), "yes".to_string()));
				}
				if attrs.topk.is_some() {
					scan.push(("topk_pushdown".to_string(), "yes".to_string()));
				}
				Ok(self.node("TableScan", ContextLevel::Database, scan, vec![]))
			}
			// A table that is not defined is shown as the scan of it that raises its absence.
			PhysOp::Vacant {
				table,
			} => {
				let mut scan = vec![
					("table".to_string(), table.to_string()),
					("direction".to_string(), "Forward".to_string()),
				];
				scan.extend(self.paging.take().unwrap_or_default());
				if topk {
					scan.push(("topk_pushdown".to_string(), "yes".to_string()));
				}
				Ok(self.node("TableScan", ContextLevel::Database, scan, vec![]))
			}
			PhysOp::KeyRangeScan(attrs) => {
				let mut scan = vec![("table".to_string(), attrs.table.to_string())];
				scan.extend(self.key_range_attrs(&attrs.access, operands));
				let prefiltered = self.prefilter.take().filter(|_| attrs.prefilter.is_some());
				if let Some(predicate) = &prefiltered {
					scan.push(("predicate".to_string(), predicate.clone()));
				}
				scan.extend(self.paging.take().unwrap_or_default());
				if prefiltered.is_some() {
					scan.push(("pre_decode_filter".to_string(), "yes".to_string()));
				}
				match attrs.topk.as_ref().map(|topk| topk.coarse_cutoff.is_some()) {
					Some(true) => scan.push(("topk_cutoff".to_string(), "second".to_string())),
					Some(false) => scan.push(("topk_pushdown".to_string(), "yes".to_string())),
					None => {}
				}
				Ok(self.node("KeyRangeScan", ContextLevel::Database, scan, vec![]))
			}
			PhysOp::IndexScan(attrs) => {
				let decode = super::pipe::spatial_source_decode(self.ir, self.scope.indexes, op)?;
				self.btree_scan(
					&attrs.table,
					&attrs.index,
					&attrs.access,
					attrs.direction,
					decode.as_ref(),
				)
			}
			PhysOp::IndexLookup(attrs) => {
				let key = self.text_of(self.operand(operands, 0)?);
				let mut lookup = vec![
					("table".to_string(), attrs.table.to_string()),
					("index".to_string(), attrs.index.index.to_string()),
					("access".to_string(), format!("{} = {key}", attrs.field)),
				];
				lookup.extend(self.paging.take().unwrap_or_default());
				Ok(self.node("IndexLookup", ContextLevel::Database, lookup, vec![]))
			}
			PhysOp::IndexUnion(attrs) => {
				let mut children = Vec::with_capacity(attrs.branches.len());
				for branch in &attrs.branches {
					children.push(self.branch(&attrs.table, branch)?);
				}
				let mut union = vec![
					("table".to_string(), attrs.table.to_string()),
					("branches".to_string(), attrs.branches.len().to_string()),
				];
				match &attrs.merge {
					Some(MergeMode::ById(direction)) => {
						union.push(("merge_by_id".to_string(), format!("{direction:?}")));
					}
					Some(MergeMode::ByIndexKey {
						path,
						direction,
					}) => {
						union.push((
							"merge_by_index_key".to_string(),
							format!("{path} {direction:?}"),
						));
					}
					Some(MergeMode::ByIndexKeyDedup {
						path,
						direction,
					}) => {
						union.push((
							"merge_by_index_key_dedup".to_string(),
							format!("{path} {direction:?}"),
						));
					}
					None => {}
				}
				if let Some(decode) =
					super::pipe::spatial_source_decode(self.ir, self.scope.indexes, op)?
				{
					let disjoint_point_ranges = attrs.merge.is_none()
						&& !attrs.branches.is_empty()
						&& attrs.branches.iter().all(|b| {
							self.resolve(&b.index).is_ok_and(|r| {
								matches!(r.definition().index, crate::catalog::Index::Spatial)
							}) && matches!(b.read, BranchReadShape::BTree(_))
						})
						&& access_path::disjoint_spatial_point_union_filter(
							attrs.branches.iter().map(|b| {
								let index_id = self
									.resolve(&b.index)
									.map_or(crate::catalog::IndexId(0), |r| {
										r.definition().index_id
									});
								let access = match &b.read {
									BranchReadShape::BTree(access) => Some(&**access),
									BranchReadShape::FullText {
										..
									} => None,
								};
								(index_id, access)
							}),
						)
						.is_some();
					if access_path::spatial_union_is_id_only(
						disjoint_point_ranges,
						self.versioned,
						Some(&decode),
					) {
						union.push(("id_only".to_string(), "yes".to_string()));
					} else {
						union.push(("decode".to_string(), decode.describe()));
					}
				}
				// The branches reject a hit whose sum of clause scores cannot enter the
				// sort's top k.
				if attrs.topk.as_ref().is_some_and(|(key, _)| key.rejects_entries) {
					union.push(("topk_pushdown".to_string(), "score".to_string()));
				}
				Ok(self.node("UnionIndexScan", ContextLevel::Database, union, children))
			}
			PhysOp::Bitmap(attrs) => {
				let plan = self.bitmap_plan_of(&attrs.root)?;
				let mut table = vec![("table".to_string(), attrs.table.to_string())];
				if let Some(decode) =
					super::pipe::spatial_source_decode(self.ir, self.scope.indexes, op)?
				{
					table.push(("decode".to_string(), decode.describe()));
				}
				if self.measure {
					let mut node = self.node("BitmapScan", ContextLevel::Database, table, vec![]);
					let kind = MeasureKind::Fusion {
						// A bitmap with a region has a fallback; one with none is anchored.
						anchored: regions.is_empty(),
					};
					self.measured_beneath(&mut node, &attrs.table, plan, kind)?;
					return Ok(node);
				}
				let children = self.bitmap_nodes(&plan);
				Ok(self.node("BitmapScan", ContextLevel::Database, table, children))
			}
			PhysOp::FullTextScan(attrs) => {
				let mut scan = vec![
					("table".to_string(), attrs.table.to_string()),
					("index".to_string(), attrs.index.index.to_string()),
					("query".to_string(), attrs.query.clone()),
				];
				// The scan rejects a hit whose score cannot enter the sort's top k.
				if attrs.topk.as_ref().is_some_and(|key| key.rejects_entries) {
					scan.push(("topk_pushdown".to_string(), "score".to_string()));
				}
				Ok(self.node("FullTextScan", ContextLevel::Database, scan, vec![]))
			}
			PhysOp::KnnScan(attrs) => self.knn_scan(attrs, operands, regions),
			PhysOp::Count(attrs) => self.count(attrs, operands, regions),
			PhysOp::RecordScan(attrs) => {
				let mut scan =
					vec![("record_id".to_string(), self.text_of(self.operand(operands, 0)?))];
				if attrs.for_update {
					scan.push(("for_update".to_string(), "true".to_string()));
				}
				Ok(self.node("RecordIdScan", ContextLevel::Database, scan, vec![]))
			}
			PhysOp::Values {
				read,
			} => {
				let mut values =
					vec![("expr".to_string(), self.text_of(self.operand(operands, 0)?))];
				if matches!(read, ValuesRead::Locked) {
					values.push(("for_update".to_string(), "true".to_string()));
				}
				Ok(self.node("Values", ContextLevel::Root, values, vec![]))
			}
			PhysOp::Union => {
				let mut children = Vec::with_capacity(operands.len());
				for value in operands {
					children.push(self.plan_of(*value)?);
				}
				Ok(self.node("Union", ContextLevel::Database, vec![], children))
			}
			PhysOp::KnnTopK(_)
			| PhysOp::Enforced {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Probe {
				..
			}
			| PhysOp::Join(_)
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(_)
			| PhysOp::Layer(_)
			| PhysOp::Filter
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Sort(_)
			| PhysOp::Window(_)
			| PhysOp::Aggregate(_)
			| PhysOp::Limit {
				..
			}
			| PhysOp::Project(_)
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Eval
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::MatchStep(_)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(_)
			| PhysOp::Remove(_)
			| PhysOp::Alter(_)
			| PhysOp::Rebuild(_)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(_)
			| PhysOp::Kill
			| PhysOp::Live(_)
			| PhysOp::Subscriber(_)
			| PhysOp::Show(_)
			| PhysOp::Write(_)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(_)
			| PhysOp::Assign(_)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Block
			| PhysOp::Let(_)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Pipeline(_)
			| PhysOp::Choose(_)
			| PhysOp::Prefetch(_)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(_)
			| PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next => Err(invariant("a source op that is no source")),
		}
	}

	/// A branch of an index union: one scan of one index.
	fn branch(
		&mut self,
		table: &TableName,
		branch: &IndexBranchShape,
	) -> Result<PlanNode, IrError> {
		match &branch.read {
			BranchReadShape::BTree(access) => {
				self.btree_scan(table, &branch.index, access, branch.direction, None)
			}
			BranchReadShape::FullText {
				query,
				..
			} => Ok(self.node(
				"FullTextScan",
				ContextLevel::Database,
				vec![
					("table".to_string(), table.to_string()),
					("index".to_string(), branch.index.index.to_string()),
					("query".to_string(), query.clone()),
				],
				vec![],
			)),
		}
	}

	/// The attributes of key-range access `access`: the components the ranges pin (`key`), the
	/// range on the next one (`range`), how many ranges an `IN` expands to where more than one
	/// (`ranges`), the direction, the order the scan delivers (`ordered`: `exact`, or `second`
	/// for a datetime component whose keys order by the whole second), `granularity: second`
	/// where a bound on a datetime component widens to its whole second, and `residual: yes`: the
	/// predicate the ranges were planned from judges every row.
	///
	/// `key`, `range` and `ranges` name what the access reads at, so they are among the attributes
	/// a checked caller beneath a permission is shown `<permission>` in place of.
	fn key_range_attrs(
		&self,
		access: &KeyRangeShape,
		operands: &[ValueId],
	) -> Vec<(String, String)> {
		use crate::catalog::ComponentKind;
		use crate::exec::index::key_range::DeliveredKeyOrder;
		let name = |position: u32| {
			access
				.key
				.name_of(position)
				.map_or_else(|| format!("id[{position}]"), |name| name.to_string())
		};
		let bound = |bound: &KeyBoundShape| match bound {
			KeyBoundShape::Literal(value) => value.to_sql(),
			KeyBoundShape::Operand(slot) => operands
				.get(usize::from(*slot))
				.map(|value| self.text_of(*value))
				.unwrap_or_default(),
		};
		let datetime = |position: u32| {
			matches!(
				access.key.components.get(position as usize).map(|c| &c.kind),
				Some(ComponentKind::Datetime)
			)
		};
		let mut attrs = Vec::new();
		let mut pinned = Vec::new();
		let mut ranges = 1usize;
		let mut widened = false;
		for (position, eq) in access.eq.iter().enumerate() {
			let position = position as u32;
			widened |= datetime(position)
				&& matches!(eq, KeyEqShape::One(KeyBoundShape::Operand(_)) | KeyEqShape::In(_));
			pinned.push(match eq {
				KeyEqShape::One(value) => format!("{} = {}", name(position), bound(value)),
				KeyEqShape::In(values) => {
					ranges = ranges.saturating_mul(values.len());
					format!(
						"{} IN [{}]",
						name(position),
						values.iter().map(bound).collect::<Vec<_>>().join(", ")
					)
				}
			});
		}
		if !pinned.is_empty() {
			attrs.push(("key".to_string(), pinned.join(", ")));
		}
		if let Some(range) = &access.range {
			let component = name(range.position);
			let mut sides = Vec::new();
			if let Some(low) = &range.low {
				let op = if low.inclusive {
					">="
				} else {
					">"
				};
				sides.push(format!("{component} {op} {}", bound(&low.bound)));
			}
			if let Some(high) = &range.high {
				let op = if high.inclusive {
					"<="
				} else {
					"<"
				};
				sides.push(format!("{component} {op} {}", bound(&high.bound)));
			}
			attrs.push(("range".to_string(), sides.join(" AND ")));
			widened |= datetime(range.position);
		}
		if ranges > 1 {
			attrs.push(("ranges".to_string(), ranges.to_string()));
		}
		attrs.push(("direction".to_string(), format!("{:?}", access.direction)));
		match access.delivers {
			DeliveredKeyOrder::None => {}
			DeliveredKeyOrder::Exact => attrs.push(("ordered".to_string(), "exact".to_string())),
			DeliveredKeyOrder::Coarse {
				..
			} => attrs.push(("ordered".to_string(), "second".to_string())),
		}
		if widened {
			attrs.push(("granularity".to_string(), "second".to_string()));
		}
		attrs.push(("residual".to_string(), "yes".to_string()));
		attrs
	}

	fn count(
		&mut self,
		attrs: &crate::exec::ir::phys::ops::CountAttrs,
		operands: &[ValueId],
		regions: &[RegionId],
	) -> Result<PlanNode, IrError> {
		match &attrs.shape {
			CountShape::All {
				source,
				..
			} => {
				use crate::exec::ir::logical::ops::SourceRefShape;
				let source = match source {
					SourceRefShape::Table(table) => table.to_string(),
					SourceRefShape::RecordId => self.text_of(self.operand(operands, 0)?),
					SourceRefShape::Mock | SourceRefShape::Param | SourceRefShape::Dynamic => {
						String::new()
					}
				};
				Ok(self.node(
					"CountScan",
					ContextLevel::Database,
					vec![("source".to_string(), source)],
					vec![],
				))
			}
			CountShape::Indexed {
				table,
				btree,
				bitmap,
				..
			} => {
				let condition = self.region_text(
					*regions
						.first()
						.ok_or_else(|| invariant("an indexed count has no condition"))?,
				)?;
				let children = match (btree, bitmap) {
					(_, Some(shape)) => {
						let plan = self.bitmap_plan_of(shape)?;
						self.bitmap_nodes(&plan)
					}
					// The walks of a count summed over several accesses, each counted by its keys.
					(Some(walks), None) if walks.len() > 1 => walks
						.iter()
						.map(|(index, access)| {
							let mut attrs = index_scan_attrs(
								table,
								index.index.as_str(),
								access,
								Direction::Forward,
							);
							attrs.push(("keys_only".to_string(), "yes".to_string()));
							self.node("IndexScan", ContextLevel::Database, attrs, vec![])
						})
						.collect(),
					(_, None) => vec![],
				};
				Ok(self.node(
					"IndexCountScan",
					ContextLevel::Database,
					vec![
						("source".to_string(), table.to_string()),
						("condition".to_string(), condition),
					],
					children,
				))
			}
		}
	}

	fn knn_scan(
		&mut self,
		attrs: &crate::exec::ir::phys::ops::KnnScanAttrs,
		operands: &[ValueId],
		regions: &[RegionId],
	) -> Result<PlanNode, IrError> {
		let index = self.resolve(&attrs.index)?;
		// A query vector known only when the statement runs is evaluated as the scan opens: the
		// plan holds no dimension for it. A parameter of the request is known when the statement
		// is reached, and has the index's dimension, as any vector the search accepts does.
		let request_param = operands.first().is_some_and(|vector| {
			producer_of(self.ir, *vector).ok().and_then(|op| self.ir.op(op)).is_some_and(|data| {
				matches!(
					data.kind,
					OpKind::Expr(ExprOp::FreeParam { .. } | ExprOp::SessionParam { .. })
				)
			})
		});
		let dimension = match (attrs.vector.is_empty(), &index.definition().index) {
			(true, crate::catalog::Index::Hnsw(params)) if request_param => {
				params.dimension.to_string()
			}
			(true, crate::catalog::Index::DiskAnn(params)) if request_param => {
				params.dimension.to_string()
			}
			(true, _) => "deferred".to_string(),
			(false, _) => attrs.vector.len().to_string(),
		};
		let mut scan = vec![
			("index".to_string(), attrs.index.index.to_string()),
			("k".to_string(), attrs.k.to_string()),
			("ef".to_string(), attrs.ef.to_string()),
			("dimension".to_string(), dimension),
		];
		// A run that built its rows from the ids the search returned, and whose search judged no
		// candidate on its record, read no record.
		if attrs.ids_only && attrs.filter.is_none() && self.analysed {
			scan.push(("records".to_string(), "not read".to_string()));
		}
		// The conjunct the search judges on each candidate: what the allow-list does not cover
		// when there is one.
		let shared_ids = index.definition().uses_shared_doc_ids();
		let mut prefilter_node = None;
		let mut judged: Option<RegionId> = None;
		// Whether the exact tier judges a condition on each member it reads, and whether the
		// table's permission is judged on each candidate.
		let mut judges_condition = false;
		let mut judges_permission = false;
		if let Some(filter) = &attrs.filter {
			judges_permission = !filter.permissions.is_empty();
			let mut slots = regions.iter().copied().skip(filter.permission_regions());
			let residual = filter.residual.then(|| slots.next()).flatten();
			if filter.matches_condition {
				slots.next();
			}
			let prefilter = filter.prefilter.as_ref().filter(|_| shared_ids);
			judged = match prefilter {
				Some(prefilter) => {
					prefilter_node = Some(self.bitmap_plan_of(&prefilter.root)?);
					judges_condition = prefilter.residual || prefilter.uncovered_matches;
					prefilter.residual.then(|| slots.next()).flatten()
				}
				None => residual,
			};
		}
		if let Some(region) = judged {
			scan.push(("predicate".to_string(), self.region_text(region)?));
		}
		match prefilter_node {
			Some(plan) if self.measure => {
				let mut node = self.node("KnnScan", ContextLevel::Database, scan, vec![]);
				let kind = MeasureKind::Prefilter {
					versioned: self.versioned,
					index,
					ef: attrs.ef,
					judges_condition,
					judges_permission,
				};
				self.measured_beneath(&mut node, &attrs.table, plan, kind)?;
				Ok(node)
			}
			Some(plan) => {
				let children = self.bitmap_nodes(&plan);
				Ok(self.node("KnnScan", ContextLevel::Database, scan, children))
			}
			None => Ok(self.node("KnnScan", ContextLevel::Database, scan, vec![])),
		}
	}

	// -----------------------------------------------------------------------------------------
	// Relational ops
	// -----------------------------------------------------------------------------------------

	/// A select loop: the rows of its source, with the ops of its body wrapped around them in
	/// order.
	fn select_loop(&mut self, op: OpId) -> Result<PlanNode, IrError> {
		let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		let source = self.operand(data.operands.as_slice(), 0)?;
		let [body] = <[RegionId; 1]>::try_from(data.regions.as_slice())
			.map_err(|_| invariant("a select loop with other than one body"))?;
		let mut ops: Vec<OpId> = ops_in(self.ir, body).collect();
		ops.pop().ok_or_else(|| invariant("a loop body with no op"))?;
		let outermost = ops.len();
		self.row_chain(source, &ops, outermost)
	}

	/// `node`, rows a statement selects, under a `SelectProject` of every field where it is no
	/// projection of its own.
	fn projected_whole(&self, node: PlanNode) -> PlanNode {
		if matches!(node.name.as_str(), "SelectProject" | "ProjectValue" | "Aggregate") {
			return node;
		}
		self.projected_whole_always(node)
	}

	/// The node of the first `count` ops of `ops`, each over the rows the one before it delivers,
	/// the rows of `source` before the first.
	fn row_chain(
		&mut self,
		source: ValueId,
		ops: &[OpId],
		count: usize,
	) -> Result<PlanNode, IrError> {
		let Some(at) = count.checked_sub(1) else {
			return self.plan_of(source);
		};
		let op = ops[at];
		let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		let OpKind::Phys(kind) = &data.kind else {
			return Err(invariant("a loop body holds an op outside the physical dialect"));
		};
		match kind {
			// The scan's own enforcement is part of the scan it renders.
			PhysOp::Enforced {
				..
			} => self.row_chain(source, ops, at),
			PhysOp::Limit {
				limit,
				start,
			} => {
				let paging = self.paging_of(data.operands.as_slice(), *limit, *start);
				let takes = limit.is_some() && self.body_takes_paging(source, &ops[..at]);
				if takes {
					self.paging = Some(paging);
					let node = self.row_chain(source, ops, at);
					self.paging = None;
					return node;
				}
				let child = self.row_chain(source, ops, at)?;
				Ok(self.node("Limit", ContextLevel::Database, paging, vec![child]))
			}
			PhysOp::Filter if at == 0 && self.prefiltered_scan(source) => {
				self.filter_into_scan(op, |this| this.plan_of(source))
			}
			PhysOp::Filter if at == 0 && self.rechecked_scan(source) => self.plan_of(source),
			PhysOp::PermissionFilter(_)
			| PhysOp::Layer(_)
			| PhysOp::Filter
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Distinct {
				..
			}
			| PhysOp::Project(_) => {
				let child = self.row_chain(source, ops, at)?;
				self.relational_over(op, kind, child)
			}
			PhysOp::TableScan(_)
			| PhysOp::IndexScan(_)
			| PhysOp::IndexUnion(_)
			| PhysOp::Bitmap(_)
			| PhysOp::IndexLookup(_)
			| PhysOp::FullTextScan(_)
			| PhysOp::KeyRangeScan(_)
			| PhysOp::KnnScan(_)
			| PhysOp::KnnTopK(_)
			| PhysOp::Count(_)
			| PhysOp::RecordScan(_)
			| PhysOp::Vacant {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Probe {
				..
			}
			| PhysOp::Union
			| PhysOp::Join(_)
			| PhysOp::Sort(_)
			| PhysOp::Window(_)
			| PhysOp::Aggregate(_)
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Eval
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::MatchStep(_)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(_)
			| PhysOp::Remove(_)
			| PhysOp::Alter(_)
			| PhysOp::Rebuild(_)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(_)
			| PhysOp::Kill
			| PhysOp::Live(_)
			| PhysOp::Subscriber(_)
			| PhysOp::Show(_)
			| PhysOp::Write(_)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(_)
			| PhysOp::Assign(_)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Block
			| PhysOp::Let(_)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Pipeline(_)
			| PhysOp::Choose(_)
			| PhysOp::Prefetch(_)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(_)
			| PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next => Err(invariant("a loop body holds an op that is not per-row")),
		}
	}

	/// The attributes of a `Limit` over the operands `operands`.
	fn paging_of(
		&self,
		operands: &[ValueId],
		limit: Option<u16>,
		start: Option<u16>,
	) -> Vec<(String, String)> {
		let mut attrs = vec![];
		if let Some(slot) = limit
			&& let Some(value) = operands.get(usize::from(slot))
		{
			attrs.push(("limit".to_string(), self.text_of(*value)));
		}
		if let Some(slot) = start
			&& let Some(value) = operands.get(usize::from(slot))
		{
			attrs.push(("offset".to_string(), self.text_of(*value)));
		}
		attrs
	}

	/// Whether the rows of `source`, beneath the ops `body` of a loop, are those of one scan that
	/// reads each record it lists once, beneath the layers of its table and the filter it judges
	/// of stored bytes alone: a limit over such rows counts the scan's.
	fn body_takes_paging(&self, source: ValueId, body: &[OpId]) -> bool {
		let layered =
			body.iter().enumerate().all(|(at, op)| match self.ir.op(*op).map(|data| &data.kind) {
				Some(OpKind::Phys(
					PhysOp::Layer(_)
					| PhysOp::Enforced {
						..
					},
				)) => true,
				Some(OpKind::Phys(PhysOp::Filter)) => {
					at == 0 && (self.prefiltered_scan(source) || self.rechecked_scan(source))
				}
				_ => false,
			});
		layered && self.takes_paging(source)
	}

	/// Whether the rows `value` holds are those of one scan of one table, beneath the layers of
	/// its table and the filter it judges of stored bytes alone, that reads each record it lists
	/// once: a table scan, or a B-tree scan whose key range does not reach a record through
	/// several entries.
	fn takes_paging(&self, value: ValueId) -> bool {
		let Ok(op) = producer_of(self.ir, value) else {
			return false;
		};
		let Some(data) = self.ir.op(op) else {
			return false;
		};
		let OpKind::Phys(kind) = &data.kind else {
			return false;
		};
		match kind {
			PhysOp::TableScan(_)
			| PhysOp::KeyRangeScan(_)
			| PhysOp::Vacant {
				..
			} => true,
			PhysOp::IndexScan(attrs) => self
				.resolve(&attrs.index)
				.is_ok_and(|index| !access_repeats_records(&index, &attrs.access)),
			PhysOp::Pipeline(_) => data
				.regions
				.first()
				.and_then(|region| self.yielded(*region).ok())
				.is_some_and(|yielded| self.takes_paging(yielded)),
			PhysOp::Layer(_)
			| PhysOp::Enforced {
				..
			} => data.operands.first().is_some_and(|input| self.takes_paging(*input)),
			PhysOp::Filter => data.operands.first().is_some_and(|input| {
				self.prefiltered_scan(*input)
					|| (self.rechecked_scan(*input) && self.takes_paging(*input))
			}),
			PhysOp::Loop {
				kind: LoopKind::Select,
				..
			} => {
				let Some(source) = data.operands.first() else {
					return false;
				};
				let Some(body) = data.regions.first() else {
					return false;
				};
				let mut ops: Vec<OpId> = ops_in(self.ir, *body).collect();
				ops.pop();
				self.body_takes_paging(*source, &ops)
			}
			PhysOp::IndexUnion(_)
			| PhysOp::Bitmap(_)
			| PhysOp::IndexLookup(_)
			| PhysOp::FullTextScan(_)
			| PhysOp::KnnScan(_)
			| PhysOp::KnnTopK(_)
			| PhysOp::Count(_)
			| PhysOp::RecordScan(_)
			| PhysOp::Values {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Probe {
				..
			}
			| PhysOp::Union
			| PhysOp::Join(_)
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(_)
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Sort(_)
			| PhysOp::Window(_)
			| PhysOp::Aggregate(_)
			| PhysOp::Limit {
				..
			}
			| PhysOp::Project(_)
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Eval
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::MatchStep(_)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(_)
			| PhysOp::Remove(_)
			| PhysOp::Alter(_)
			| PhysOp::Rebuild(_)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(_)
			| PhysOp::Kill
			| PhysOp::Live(_)
			| PhysOp::Subscriber(_)
			| PhysOp::Show(_)
			| PhysOp::Write(_)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(_)
			| PhysOp::Assign(_)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Block
			| PhysOp::Let(_)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Choose(_)
			| PhysOp::Prefetch(_)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(_)
			| PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next => false,
		}
	}

	/// A relational op in a chain: the node of the op over the plan of its input operand.
	fn relational(&mut self, op: OpId, kind: &PhysOp) -> Result<PlanNode, IrError> {
		let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		let operands = data.operands.as_slice();
		match kind {
			// The scan's own enforcement is part of the scan it renders.
			PhysOp::Enforced {
				..
			} => self.plan_of(self.operand(operands, 0)?),
			PhysOp::Limit {
				limit,
				start,
			} => {
				let input = self.operand(operands, 0)?;
				let paging = self.paging_of(operands, *limit, *start);
				if limit.is_some() && self.takes_paging(input) {
					self.paging = Some(paging);
					let node = self.plan_of(input);
					self.paging = None;
					return node;
				}
				let child = self.plan_of(input)?;
				Ok(self.node("Limit", ContextLevel::Database, paging, vec![child]))
			}
			PhysOp::Join(attrs) => self.join(op, attrs),
			PhysOp::Probe {
				field,
			} => {
				let child = self.plan_of(self.operand(operands, 0)?)?;
				Ok(self.node(
					"Probe",
					ContextLevel::Database,
					vec![
						("field".to_string(), field.to_string()),
						("outer".to_string(), self.text_of(self.operand(operands, 1)?)),
					],
					vec![child],
				))
			}
			PhysOp::Filter
				if operands.first().is_some_and(|input| self.prefiltered_scan(*input)) =>
			{
				let input = self.operand(operands, 0)?;
				self.filter_into_scan(op, |this| this.plan_of(input))
			}
			PhysOp::Filter if operands.first().is_some_and(|input| self.rechecked_scan(*input)) => {
				self.plan_of(self.operand(operands, 0)?)
			}
			PhysOp::KnnTopK(_)
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(_)
			| PhysOp::Layer(_)
			| PhysOp::Filter
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Sort(_)
			| PhysOp::Window(_)
			| PhysOp::Aggregate(_)
			| PhysOp::Project(_) => {
				self.topk =
					matches!(kind, PhysOp::Sort(attrs) if attrs.pushdown && attrs.keep.is_some());
				let input = self.operand(operands, 0)?;
				let child = self.plan_of(input);
				self.topk = false;
				self.relational_over(op, kind, child?)
			}
			PhysOp::TableScan(_)
			| PhysOp::IndexScan(_)
			| PhysOp::IndexUnion(_)
			| PhysOp::Bitmap(_)
			| PhysOp::IndexLookup(_)
			| PhysOp::FullTextScan(_)
			| PhysOp::KeyRangeScan(_)
			| PhysOp::KnnScan(_)
			| PhysOp::Count(_)
			| PhysOp::RecordScan(_)
			| PhysOp::Vacant {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Union
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Eval
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::MatchStep(_)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(_)
			| PhysOp::Remove(_)
			| PhysOp::Alter(_)
			| PhysOp::Rebuild(_)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(_)
			| PhysOp::Kill
			| PhysOp::Live(_)
			| PhysOp::Subscriber(_)
			| PhysOp::Show(_)
			| PhysOp::Write(_)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(_)
			| PhysOp::Assign(_)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Block
			| PhysOp::Let(_)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Pipeline(_)
			| PhysOp::Choose(_)
			| PhysOp::Prefetch(_)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(_)
			| PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next => Err(invariant("an op that is no relational op")),
		}
	}

	/// Whether `rows` are the rows of an index scan whose access the filter reading them only
	/// rechecks.
	fn rechecked_scan(&self, rows: ValueId) -> bool {
		let Some(ValueDef::OpResult {
			op,
			..
		}) = self.ir.value(rows).map(|data| data.def)
		else {
			return false;
		};
		matches!(
			self.ir.op(op).map(|data| &data.kind),
			Some(OpKind::Phys(PhysOp::IndexScan(attrs))) if attrs.rechecked
		)
	}

	/// Whether `rows` are the rows of a scan that judges the predicate of the filter reading them
	/// of each record's stored bytes.
	fn prefiltered_scan(&self, rows: ValueId) -> bool {
		let Some(ValueDef::OpResult {
			op,
			..
		}) = self.ir.value(rows).map(|data| data.def)
		else {
			return false;
		};
		match self.ir.op(op).map(|data| &data.kind) {
			Some(OpKind::Phys(PhysOp::TableScan(attrs))) => attrs.prefilter.is_some(),
			Some(OpKind::Phys(PhysOp::KeyRangeScan(attrs))) => attrs.prefilter.is_some(),
			Some(OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_)) | None => false,
		}
	}

	/// The node of the scan `scan` renders, with the predicate of the filter `filter` over it as
	/// the predicate the scan judges of each record's stored bytes.
	fn filter_into_scan(
		&mut self,
		filter: OpId,
		scan: impl FnOnce(&mut Self) -> Result<PlanNode, IrError>,
	) -> Result<PlanNode, IrError> {
		let data = self.ir.op(filter).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		let [predicate] = data.regions.as_slice() else {
			return Err(invariant("a prefiltered scan's filter holds other than one region"));
		};
		self.prefilter = Some(self.predicate(*predicate)?.0);
		let node = scan(self);
		self.prefilter = None;
		node
	}

	/// The node of the relational op `op` over the node `child`.
	fn relational_over(
		&mut self,
		op: OpId,
		kind: &PhysOp,
		child: PlanNode,
	) -> Result<PlanNode, IrError> {
		let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		let regions = data.regions.as_slice();
		match kind {
			PhysOp::KnnTopK(attrs) => {
				let field = attrs.field.to_string();
				let dimension = match &attrs.vector {
					KnnTargetShape::Literal(vector) => vector.len().to_string(),
					KnnTargetShape::Deferred(..) => "deferred".to_string(),
				};
				Ok(self.node(
					"KnnTopK",
					ContextLevel::Database,
					vec![
						("field".to_string(), field),
						("k".to_string(), attrs.k.to_string()),
						("distance".to_string(), format!("{:?}", attrs.distance)),
						("dimension".to_string(), dimension),
					],
					vec![child],
				))
			}
			PhysOp::Distinct {
				..
			} => Ok(self.node("Distinct", ContextLevel::Database, vec![], vec![child])),
			PhysOp::PermissionFilter(attrs) => {
				let [predicate, once @ ..] = regions else {
					return Err(invariant("a permission filter has no predicate region"));
				};
				// The predicate is the definer's: a caller it is enforced on is shown a
				// placeholder in its place.
				let shown = |this: &Self, region: RegionId| -> Result<String, IrError> {
					if this.scope.checked {
						Ok("<permission>".to_string())
					} else {
						this.region_text(region)
					}
				};
				let mut shown_attrs = Vec::new();
				if let Some(region) = once.first() {
					shown_attrs.push(("once".to_string(), shown(self, *region)?));
				}
				if once.is_empty() || !self.is_true(*predicate) {
					shown_attrs.push(("predicate".to_string(), shown(self, *predicate)?));
				}
				if self.ir.frame(data.frame).is_some_and(|frame| frame.eval.auth.is_some()) {
					shown_attrs.push(("auth".to_string(), "limited".to_string()));
				}
				let mut node =
					self.node("PermissionFilter", ContextLevel::Database, shown_attrs, vec![child]);
				node.redacts_access_paths = attrs.hides_access_paths;
				node.hides_metrics = self.scope.checked;
				Ok(node)
			}
			PhysOp::Layer(attrs) => {
				// The derivations the guards read follow the columns' regions.
				let judged = attrs.judged.as_ref().map_or(0, ComputedReadsShape::derived_regions);
				let regions = regions
					.len()
					.checked_sub(judged)
					.map(|columns| &regions[..columns])
					.ok_or_else(|| invariant("a layer hosts a region for each field it derives"))?;
				let columns = self.layer_attrs(&attrs.fields, &attrs.names, regions)?;
				let mut node =
					self.node("ProjectLayer", ContextLevel::Database, columns, vec![child]);
				node.hides_metrics = self.scope.checked;
				Ok(node)
			}
			PhysOp::Filter => {
				let [predicate, once @ ..] = regions else {
					return Err(invariant("a filter has no predicate region"));
				};
				let mut children = vec![child];
				let (text, memberships) = self.predicate(*predicate)?;
				children.extend(memberships);
				let mut filter = Vec::new();
				// The conjuncts judged once per run of the statement, before those judged per row.
				if let Some(region) = once.first() {
					filter.push(("once".to_string(), self.predicate(*region)?.0));
				}
				if once.is_empty() || !self.is_true(*predicate) {
					filter.push(("predicate".to_string(), text));
				}
				Ok(self.node("Filter", ContextLevel::Database, filter, children))
			}
			PhysOp::Fetch => {
				let fetches = regions
					.iter()
					.map(|region| self.region_text(*region))
					.collect::<Result<Vec<_>, _>>()?;
				Ok(self.node(
					"Fetch",
					ContextLevel::Database,
					vec![("fetches".to_string(), fetches.join(", "))],
					vec![child],
				))
			}
			PhysOp::Split {
				..
			} => {
				let targets = regions
					.iter()
					.map(|region| self.region_text(*region))
					.collect::<Result<Vec<_>, _>>()?;
				// A split unnests the rows the statement selects, each projected whole.
				let child = self.projected_whole(child);
				Ok(self.node(
					"Split",
					ContextLevel::Database,
					vec![("targets".to_string(), targets.join(", "))],
					vec![child],
				))
			}
			PhysOp::Window(shape) => {
				let mut slots = regions.iter().copied();
				let mut window = Vec::new();
				if shape.partitions > 0 {
					let mut keys = Vec::with_capacity(usize::from(shape.partitions));
					for _ in 0..shape.partitions {
						let region = slots
							.next()
							.ok_or_else(|| invariant("a window lacks a partition key region"))?;
						keys.push(self.region_text(region)?);
					}
					window.push(("partition_by".to_string(), keys.join(", ")));
				}
				if !shape.order.is_empty() {
					let mut keys = Vec::with_capacity(shape.order.len());
					for key in shape.order.iter() {
						let region = slots
							.next()
							.ok_or_else(|| invariant("a window lacks an order key region"))?;
						let mut text = format!(
							"{} {}",
							self.region_text(region)?,
							if key.ascending {
								"ASC"
							} else {
								"DESC"
							}
						);
						if key.collate {
							text.push_str(" COLLATE");
						}
						if key.numeric {
							text.push_str(" NUMERIC");
						}
						keys.push(text);
					}
					window.push(("order_by".to_string(), keys.join(", ")));
				}
				let mut calls = Vec::with_capacity(shape.calls.len());
				for call in shape.calls.iter() {
					let mut args = Vec::with_capacity(usize::from(call.args));
					for _ in 0..call.args {
						let region = slots
							.next()
							.ok_or_else(|| invariant("a window lacks an argument region"))?;
						args.push(self.region_text(region)?);
					}
					calls.push(format!(
						"{}({}) {} AS {}",
						window_function_name(call.function),
						args.join(", "),
						frame_text(&call.frame),
						call.output.as_str()
					));
				}
				window.push(("calls".to_string(), calls.join(", ")));
				Ok(self.node("Window", ContextLevel::Database, window, vec![child]))
			}
			PhysOp::Sort(attrs) => {
				let mut sort = vec![];
				let mut slots = regions.iter().copied();
				if attrs.random {
					sort.push(("sort_keys".to_string(), "RAND()".to_string()));
				} else if !attrs.keys.is_empty() {
					let mut keys = Vec::with_capacity(attrs.keys.len());
					for key in &attrs.keys {
						let read = match &key.read {
							SortKeyReadShape::Path(path) => path.to_string(),
							SortKeyReadShape::Computed => {
								let region = slots
									.next()
									.ok_or_else(|| invariant("a sort lacks a key region"))?;
								self.region_text(region)?
							}
						};
						let mut text = format!(
							"{read} {}",
							if key.ascending {
								"ASC"
							} else {
								"DESC"
							}
						);
						if key.collate {
							text.push_str(" COLLATE");
						}
						if key.numeric {
							text.push_str(" NUMERIC");
						}
						keys.push(text);
					}
					sort.push(("sort_keys".to_string(), keys.join(", ")));
				}
				if let Some(keep) = attrs.keep {
					sort.push(("keep".to_string(), keep.to_string()));
				} else if attrs.tempfiles && !attrs.random {
					sort.push(("tempfiles".to_string(), "true".to_string()));
				}
				Ok(self.node("Sort", ContextLevel::Database, sort, vec![child]))
			}
			PhysOp::Aggregate(attrs) => {
				let groups = usize::from(attrs.groups);
				let aggregate = if groups == 0 {
					vec![("mode".to_string(), "GROUP ALL".to_string())]
				} else {
					let keys = regions
						.iter()
						.take(groups)
						.map(|region| self.region_text(*region))
						.collect::<Result<Vec<_>, _>>()?;
					vec![("by".to_string(), keys.join(", "))]
				};
				Ok(self.node("Aggregate", ContextLevel::Database, aggregate, vec![child]))
			}
			PhysOp::Project(shape) => {
				let (name, project) = self.projection(shape, regions)?;
				Ok(self.node(name, ContextLevel::Database, project, vec![child]))
			}
			PhysOp::TableScan(_)
			| PhysOp::IndexScan(_)
			| PhysOp::IndexUnion(_)
			| PhysOp::Bitmap(_)
			| PhysOp::IndexLookup(_)
			| PhysOp::FullTextScan(_)
			| PhysOp::KeyRangeScan(_)
			| PhysOp::KnnScan(_)
			| PhysOp::Count(_)
			| PhysOp::RecordScan(_)
			| PhysOp::Vacant {
				..
			}
			| PhysOp::Enforced {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Probe {
				..
			}
			| PhysOp::Union
			| PhysOp::Join(_)
			| PhysOp::Limit {
				..
			}
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Eval
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::MatchStep(_)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(_)
			| PhysOp::Remove(_)
			| PhysOp::Alter(_)
			| PhysOp::Rebuild(_)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(_)
			| PhysOp::Kill
			| PhysOp::Live(_)
			| PhysOp::Subscriber(_)
			| PhysOp::Show(_)
			| PhysOp::Write(_)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(_)
			| PhysOp::Assign(_)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Block
			| PhysOp::Let(_)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Pipeline(_)
			| PhysOp::Choose(_)
			| PhysOp::Prefetch(_)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(_)
			| PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next => Err(invariant("an op that is no relational op over one input")),
		}
	}

	/// A step of the binding table of a `MATCH`, over the plans of its operands: the build rows of
	/// a join first, then the rows the step extends.
	fn bind_step(&mut self, op: OpId, shape: &BindStepShape) -> Result<PlanNode, IrError> {
		use crate::exec::operators::{EdgeBinding, ExpandDir};
		let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		let operands = data.operands.as_slice();
		let regions = data.regions.as_slice();
		let tables = |tables: &[TableName]| {
			if tables.is_empty() {
				"*".to_string()
			} else {
				tables.iter().map(|table| table.as_str()).collect::<Vec<_>>().join(", ")
			}
		};
		let arrow = |direction: ExpandDir| match direction {
			ExpandDir::Out => "->",
			ExpandDir::In => "<-",
		};
		let mut attrs: Vec<(String, String)> = Vec::new();
		let name = match shape {
			BindStepShape::Bind {
				name,
			} => {
				attrs.push(("binding".to_string(), name.clone()));
				"Bind"
			}
			BindStepShape::Expand(expand) => {
				attrs.push(("source".to_string(), expand.source.clone()));
				attrs.push(("direction".to_string(), arrow(expand.direction).to_string()));
				attrs.push(("tables".to_string(), tables(&expand.edge_tables)));
				attrs.push((
					"edge".to_string(),
					match &expand.edge_binding {
						EdgeBinding::Full(name) | EdgeBinding::IdOnly(name) => name.clone(),
					},
				));
				attrs.push(("target".to_string(), expand.target_binding.clone()));
				if let Some(label) = &expand.target_label {
					attrs.push(("target_label".to_string(), label.as_str().to_string()));
				}
				if expand.predicate {
					let region = *regions
						.first()
						.ok_or_else(|| invariant("an expansion lacks its predicate region"))?;
					attrs.push(("predicate".to_string(), self.predicate(region)?.0));
				}
				if expand.optional {
					"OptionalExpand"
				} else {
					"Expand"
				}
			}
			BindStepShape::Endpoint {
				edge,
				field,
				name,
				label,
			} => {
				attrs.push(("edge".to_string(), edge.clone()));
				attrs.push(("field".to_string(), field.field().to_string()));
				attrs.push(("node".to_string(), name.clone()));
				if let Some(label) = label {
					attrs.push(("target_label".to_string(), label.as_str().to_string()));
				}
				"EndpointBind"
			}
			BindStepShape::DistinctEdges {
				edges,
			} => {
				attrs.push(("edges".to_string(), edges.join(", ")));
				"DistinctEdges"
			}
			BindStepShape::Distinct => "Distinct",
			BindStepShape::Drain => "DrainSink",
			BindStepShape::Path(path) => {
				attrs.push(("source".to_string(), path.source.clone()));
				attrs.push(("direction".to_string(), arrow(path.direction).to_string()));
				attrs.push(("tables".to_string(), tables(&path.edge_tables)));
				attrs.push(("min".to_string(), path.min.to_string()));
				attrs.push((
					"max".to_string(),
					path.max.map_or_else(|| "\u{221e}".to_string(), |max| max.to_string()),
				));
				attrs.push(("target_binding".to_string(), path.target_binding.clone()));
				if let Some(selector) = path.selector {
					attrs.push(("search".to_string(), selector.attr()));
				}
				if let Some(label) = &path.target_label {
					attrs.push(("target_label".to_string(), label.as_str().to_string()));
				}
				if let Some(group) = &path.group_binding {
					attrs.push(("group".to_string(), group.clone()));
				}
				if let Some(binding) = &path.path_binding {
					attrs.push(("path".to_string(), binding.clone()));
				}
				if let Some(mode) = path.mode.attr() {
					attrs.push(("mode".to_string(), mode.to_string()));
				}
				if path.reversed {
					attrs.push(("reversed".to_string(), "true".to_string()));
				}
				if path.selector.is_some() {
					"ShortestPathExpand"
				} else {
					"PathExpand"
				}
			}
			BindStepShape::Update {
				target,
				data,
			} => {
				attrs.push(("binding".to_string(), target.clone()));
				attrs.push((
					"op".to_string(),
					match data {
						MatchData::Set(_) => "set",
						MatchData::Unset(_) => "unset",
						MatchData::Content => "content",
					}
					.to_string(),
				));
				"UpdateBinding"
			}
			BindStepShape::Delete {
				target,
				detach,
				..
			} => {
				attrs.push(("binding".to_string(), target.clone()));
				attrs.push((
					"mode".to_string(),
					match detach {
						DetachMode::Detach => "detach",
						DetachMode::NoDetach => "nodetach",
					}
					.to_string(),
				));
				"DeleteBinding"
			}
			BindStepShape::Insert {
				nodes,
				edges,
			} => {
				attrs.push(("nodes".to_string(), nodes.len().to_string()));
				attrs.push(("edges".to_string(), edges.len().to_string()));
				"InsertGraph"
			}
			BindStepShape::Join(join) => {
				attrs.push(("type".to_string(), join.join_type.label().to_string()));
				if !join.keys.is_empty() {
					attrs.push(("keys".to_string(), join.keys.join(", ")));
				}
				if !join.null_template.is_empty() {
					attrs.push(("null_template".to_string(), join.null_template.join(", ")));
				}
				if join.residual {
					let region = *regions
						.first()
						.ok_or_else(|| invariant("a join lacks its residual region"))?;
					attrs.push(("residual".to_string(), self.predicate(region)?.0));
				}
				"HashJoin"
			}
		};
		let mut children = Vec::with_capacity(usize::from(shape.operands()));
		for value in operands {
			children.push(self.plan_of(*value)?);
		}
		Ok(self.node(name, ContextLevel::Database, attrs, children))
	}

	/// A join, with the plans of its inputs and, for a lookup, of the right input the lookup reads.
	fn join(
		&mut self,
		op: OpId,
		attrs: &crate::exec::ir::phys::ops::JoinAttrs,
	) -> Result<PlanNode, IrError> {
		let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::explain: op"))?;
		let operands = data.operands.as_slice();
		let regions = data.regions.as_slice();
		let key_regions = usize::from(attrs.keys) * 2;
		let mut keys = Vec::with_capacity(usize::from(attrs.keys));
		for pair in regions.chunks(2).take(usize::from(attrs.keys)) {
			let [left, right] = pair else {
				return Err(invariant("a join key holds a region per side"));
			};
			keys.push(format!("{} = {}", self.region_text(*left)?, self.region_text(*right)?));
		}
		let residual = match attrs.residual {
			true => Some(
				self.region_text(
					*regions
						.get(key_regions)
						.ok_or_else(|| invariant("a join lacks its residual region"))?,
				)?,
			),
			false => None,
		};
		let looked_up = match attrs.lookup {
			Some(_) => {
				let region = *regions
					.get(key_regions + usize::from(attrs.residual))
					.ok_or_else(|| invariant("a join lacks its lookup region"))?;
				let yielded = self.yielded(region)?;
				Some(self.plan_of(yielded)?)
			}
			None => None,
		};
		let build = self.plan_of(self.operand(operands, 1)?)?;
		let probe = self.plan_of(self.operand(operands, 0)?)?;
		let mut join = vec![
			(
				"join_type".to_string(),
				match attrs.matching {
					JoinMatch::Inner => "Inner",
					JoinMatch::Left => "Left",
					JoinMatch::Semi => "Semi",
				}
				.to_string(),
			),
			("alias".to_string(), attrs.alias.clone()),
		];
		if !keys.is_empty() {
			join.push(("keys".to_string(), keys.join(", ")));
		}
		if let Some(residual) = residual {
			join.push(("residual".to_string(), residual));
		}
		if attrs.build == JoinBuild::Left {
			// The left input whose one row the join holds, then the right input it streams past
			// that row.
			return Ok(self.node(
				"SingleRowJoin",
				ContextLevel::Database,
				join,
				vec![probe, build],
			));
		}
		Ok(match looked_up {
			// The lookup's right input, the hash join's it hands the rest of the left rows to, and
			// the left input.
			Some(looked_up) => {
				join.push(("lookups".to_string(), format!("{LOOKUP_EVALUATIONS}, then hash")));
				self.node(
					"IndexLoopJoin",
					ContextLevel::Database,
					join,
					vec![looked_up, build, probe],
				)
			}
			None => self.node("HashJoin", ContextLevel::Database, join, vec![build, probe]),
		})
	}

	/// The name and attributes of a projection's node.
	fn projection(
		&self,
		shape: &ProjectionShape,
		regions: &[RegionId],
	) -> Result<Projected, IrError> {
		let mut slots = regions.iter().copied();
		let omitted = |this: &Self,
		               omit: &[OmissionShape],
		               slots: &mut dyn Iterator<Item = RegionId>|
		 -> Result<String, IrError> {
			let mut texts = Vec::with_capacity(omit.len());
			for omission in omit {
				match omission {
					OmissionShape::Path(path) => texts.push(path.to_string()),
					OmissionShape::Walk {
						..
					} => {
						let region = slots
							.next()
							.ok_or_else(|| invariant("an omission lacks its source"))?;
						texts.push(this.region_text(region)?);
					}
				}
			}
			Ok(texts.join(", "))
		};
		Ok(match shape {
			ProjectionShape::All => {
				("SelectProject", vec![("projections".to_string(), "*".to_string())])
			}
			ProjectionShape::Value => {
				let region =
					slots.next().ok_or_else(|| invariant("a projection lacks its value"))?;
				("ProjectValue", vec![("expr".to_string(), self.region_text(region)?)])
			}
			ProjectionShape::OmittedValue {
				omit,
			} => {
				let region =
					slots.next().ok_or_else(|| invariant("a projection lacks its value"))?;
				let expr = self.region_text(region)?;
				(
					"ProjectValue",
					vec![
						("expr".to_string(), expr),
						("omit".to_string(), omitted(self, omit, &mut slots)?),
					],
				)
			}
			ProjectionShape::SortedValue(_) => ("ProjectValue", vec![]),
			ProjectionShape::Star {
				fields,
				omit,
			} => {
				let mut text = "*".to_string();
				for field in fields {
					text.push_str(&format!(", {}", field.output));
				}
				let mut project = vec![("projections".to_string(), text)];
				// The fields' regions precede the omissions'.
				for _ in fields {
					slots.next();
				}
				if !omit.is_empty() {
					project.push(("omit".to_string(), omitted(self, omit, &mut slots)?));
				}
				("SelectProject", project)
			}
			ProjectionShape::Fields {
				fields,
				omit,
			} => {
				// A field landing at a nested output path needs the full operator's object
				// construction, which lists nothing.
				if fields.iter().any(|field| field.output.0.len() > 1) {
					("Project", Vec::new())
				} else {
					let text =
						fields.iter().map(|field| field.output.to_string()).collect::<Vec<_>>();
					let mut project = vec![("projections".to_string(), text.join(", "))];
					for _ in fields {
						slots.next();
					}
					if !omit.is_empty() {
						project.push(("omit".to_string(), omitted(self, omit, &mut slots)?));
					}
					("SelectProject", project)
				}
			}
		})
	}

	/// The attributes of a projection layer over `shape`: one rendered column per entry,
	/// `name = <expression> [: kind] @frame`. A permission predicate and a computed body are the
	/// definer's, so a `checked` caller is shown `<permission>` or `<computed>` where the
	/// expression stands, and no text of either. A column that stands for a component of the
	/// record id is labelled `@key` rather than `@computed`, which is what tells it from a
	/// computed field that happens to read the id. An unaliased column is named by the output name
	/// the layer carries for it, in field order (`names`), taken from the written expression's
	/// output path.
	fn layer_attrs(
		&self,
		shape: &FieldsShape,
		names: &[OutputPath],
		regions: &[RegionId],
	) -> Result<Vec<(String, String)>, IrError> {
		let FieldsShape::Select(fields) = shape else {
			return Err(invariant("a projection layer is a VALUE projection"));
		};
		let count =
			fields.iter().filter(|field| matches!(field, FieldShape::Single { .. })).count();
		let frames = match regions.split_at_checked(count) {
			Some((_, frames)) if frames.len() == count => frames,
			_ => return Err(invariant("a layer holds a frame region per column")),
		};
		let mut columns = Vec::with_capacity(count);
		let mut at = 0usize;
		let mut unaliased = names.iter();
		for field in fields.iter() {
			let FieldShape::Single {
				alias,
				kind,
			} = field
			else {
				continue;
			};
			// The column's text is the expression its frame evaluates; the value region reads the
			// field that expression lands in.
			let value = self.yielded(frames[at])?;
			let frame = self.frame_of(frames[at])?;
			at += 1;
			let name = match alias {
				Some(alias) => alias.name().clone(),
				None => unaliased
					.next()
					.ok_or_else(|| invariant("a layer names each unaliased column"))?
					.name()
					.clone(),
			};
			let mut text = format!("{name} = ");
			let judged = self.judgement(value, &frame);
			match (frame.mode, &judged) {
				(EvalMode::Permission | EvalMode::MutablePermission, _) if self.scope.checked => {
					text.push_str("<permission>");
				}
				(EvalMode::Computed, _) if self.scope.checked => text.push_str("<computed>"),
				(_, Some((guard, then))) => text.push_str(&format!(
					"IF {} THEN {} ELSE NONE END",
					self.text_of(*guard),
					self.text_of(*then)
				)),
				(_, None) => text.push_str(&self.text_of(value)),
			}
			if let Some(kind) = kind {
				text.push_str(&format!(" : {}", kind.to_sql()));
			}
			text.push_str(if frame.key_part {
				" @key"
			} else {
				match frame.mode {
					EvalMode::Caller => " @caller",
					EvalMode::Permission => " @permission",
					EvalMode::MutablePermission => " @mutable-permission",
					EvalMode::Computed => " @computed",
				}
			});
			columns.push(text);
		}
		Ok(vec![("columns".to_string(), columns.join(", "))])
	}

	/// The guard and the value of a permission column written `IF p THEN v ELSE NONE END`.
	fn judgement(
		&self,
		value: ValueId,
		frame: &crate::expr::logical::EvalFrame,
	) -> Option<(ValueId, ValueId)> {
		if !matches!(frame.mode, EvalMode::Permission | EvalMode::MutablePermission) {
			return None;
		}
		let ValueDef::OpResult {
			op,
			..
		} = self.ir.value(value)?.def
		else {
			return None;
		};
		let data = self.ir.op(op)?;
		let OpKind::Expr(ExprOp::If) = &data.kind else {
			return None;
		};
		let [guard] = <[ValueId; 1]>::try_from(data.operands.as_slice()).ok()?;
		let [then, otherwise] = <[RegionId; 2]>::try_from(data.regions.as_slice()).ok()?;
		let otherwise = self.yielded(otherwise).ok()?;
		let ValueDef::OpResult {
			op: none,
			..
		} = self.ir.value(otherwise)?.def
		else {
			return None;
		};
		matches!(
			&self.ir.op(none)?.kind,
			OpKind::Expr(ExprOp::Literal(literal)) if matches!(**literal, crate::val::Value::None)
		)
		.then_some((guard, self.yielded(then).ok()?))
	}

	/// The frame the first op of the region `region` runs under.
	fn frame_of(&self, region: RegionId) -> Result<crate::expr::logical::EvalFrame, IrError> {
		let first = self
			.ir
			.region(region)
			.and_then(|data| data.blocks.first())
			.and_then(|block| self.ir.block(*block))
			.and_then(|block| block.ops.first())
			.and_then(|op| self.ir.op(*op))
			.ok_or_else(|| invariant("a clause region of a field holds no op"))?;
		self.ir
			.frame(first.frame)
			.map(|frame| frame.eval.clone())
			.ok_or(IrError::Stale("authority frame"))
	}
}

/// How `EXPLAIN` names the strategy a nested statement runs by.
fn strategy_text(strategy: &NestedStrategy) -> &'static str {
	match strategy {
		NestedStrategy::Apply => "apply",
		NestedStrategy::Once => "once",
		NestedStrategy::Hash {
			..
		} => "hash",
		NestedStrategy::Lookup {
			hashed: true,
			..
		} => "lookup, then hash",
		NestedStrategy::Lookup {
			hashed: false,
			..
		} => "lookup",
	}
}

/// The attributes of an index scan of `table` through `index`: the key range it reads, which an
/// open range does not name, and the direction it reads in.
fn index_scan_attrs(
	table: &TableName,
	index: &str,
	access: &BTreeAccess,
	direction: Direction,
) -> Vec<(String, String)> {
	let mut attrs =
		vec![("table".to_string(), table.to_string()), ("index".to_string(), index.to_string())];
	let access_desc = access.describe();
	if !access_desc.is_empty() {
		attrs.push(("access".to_string(), access_desc));
	}
	if let Some(pf) = access.point_filter() {
		attrs.push(("point_filter".to_string(), pf.describe()));
	}
	attrs.push(("direction".to_string(), format!("{direction:?}")));
	attrs
}

/// The scope the context `cx` lowers for: whether its frame reads under permissions enforced on
/// the caller, and the indexes its compile resolved.
fn scope_of<'a>(cx: &LowerCx<'a>) -> ExplainScope<'a> {
	ExplainScope {
		checked: cx.reads_checked(),
		indexes: cx.indexes(),
	}
}

/// The rendering of the plan the plain `EXPLAIN` `explain` explains, a function of the plan alone.
///
/// Answers an invariant error for an op that is not a `phys.explain` of no `ANALYZE`.
fn render_plain(cx: &LowerCx<'_>, ir: &Ir, explain: OpId) -> Result<crate::val::Value, IrError> {
	let data = ir.op(explain).ok_or(IrError::Stale("ir_lower::explain: op"))?;
	let OpKind::Phys(PhysOp::Explain {
		analyze: false,
		format,
	}) = &data.kind
	else {
		return Err(invariant("a plain EXPLAIN that is not a phys.explain of no ANALYZE"));
	};
	let tree = explained_tree(ir, explain, &scope_of(cx), Purpose::Plan)?;
	Ok(crate::exec::vm::explain::render(&tree, *format, None))
}

/// Lowers the container `ir`, whose `phys.explain` `explain` is a plain `EXPLAIN`, to a program
/// that answers the rendering of the plan it explains.
///
/// The rendering is a function of the plan alone, so it is a constant of the program: one scalar
/// pipeline that loads it, emits it and ends its row. Nothing of the explained statement runs.
pub(super) fn lower_plain(cx: &LowerCx<'_>, ir: &Ir, explain: OpId) -> Result<Lowered, IrError> {
	let rendered = render_plain(cx, ir, explain)?;
	let mut em = Emitter::new();
	emit_constant_row(&mut em, rendered)?;
	let pipeline = em.finish_pipeline(Source::Once)?;
	let mut program: Program = em.finish_program(vec![pipeline], AccessMode::ReadOnly)?;
	program.scalar = true;
	Ok(Lowered {
		program,
		provenance: Provenance::default(),
		needs: Vec::new(),
	})
}

/// Emits the row that answers `value` and ends the pipeline.
fn emit_constant_row(em: &mut Emitter, value: crate::val::Value) -> Result<(), IrError> {
	let value = Operand::Const(em.constant(value)?);
	let dst = em.reg()?;
	em.emit(Instr::Load {
		dst,
		src: value,
	});
	em.emit(Instr::Emit {
		src: dst,
	});
	em.emit(Instr::EndRow);
	Ok(())
}

/// Lowers the `EXPLAIN` the statement `region` holds, whose op is `explain`, into `em` for the
/// context `cx`, numbering the pools of an analysed statement from `base`.
///
/// A plain `EXPLAIN` is [`lower_plain`]'s rendering as one pipeline over [`Source::Once`]. An
/// analysed one lowers the statement it explains as the read group of a block's statement
/// ([`lower_scope`]), and states the analysis its run renders ([`analysis_of`], redacting what
/// the context redacts), its tree built for `purpose`.
pub(super) fn lower_explained(
	cx: &mut LowerCx<'_>,
	em: &mut Emitter,
	ir: &Ir,
	region: RegionId,
	explain: OpId,
	base: PoolBase,
	purpose: Purpose,
) -> Result<Explained, IrError> {
	let data = ir.op(explain).ok_or(IrError::Stale("ir_lower::explain: op"))?;
	let OpKind::Phys(PhysOp::Explain {
		analyze,
		..
	}) = &data.kind
	else {
		return Err(invariant("an explained statement that is not a phys.explain"));
	};
	if !*analyze {
		let rendered = render_plain(cx, ir, explain)?;
		emit_constant_row(em, rendered)?;
		return Ok(Explained::Plain(em.finish_pipeline(Source::Once)?));
	}
	let analysis = analysis_of(ir, explain, &scope_of(cx), cx.redact_volatile(), purpose)?;
	let [input] = <[ValueId; 1]>::try_from(data.operands.as_slice())
		.map_err(|_| invariant("a phys.explain with other than one operand"))?;
	let producer = producer_of(ir, input)?;
	let rendered = if let OpKind::Phys(PhysOp::Explain {
		analyze: false,
		..
	}) = &ir.op(producer).ok_or(IrError::Stale("ir_lower::explain: op"))?.kind
	{
		Some(render_plain(cx, ir, producer)?)
	} else {
		None
	};
	if rendered.is_some() || reads_no_rows(ir, producer) {
		let mut lowerer = Lowerer::in_context(em, ir, cx);
		let value = match rendered {
			Some(rendered) => Operand::Const(lowerer.emitter().constant(rendered)?),
			None => match ir.op(producer).map(|held| (&held.kind, held.regions.as_slice())) {
				Some((
					OpKind::Phys(PhysOp::Statement {
						..
					}),
					[statement],
				)) => {
					let held = lowerer.enter(*statement);
					let value = statement_value(&mut lowerer, ir, *statement);
					lowerer.leave(held);
					value?
				}
				_ => lowerer.value(input)?,
			},
		};
		let pipeline = lowerer.emit_value_pipeline(value)?;
		return Ok(Explained::AnalysedValue {
			pipeline,
			claims: lowerer.take_claims(),
			needs: lowerer.needs(),
			analysis,
		});
	}
	let scope = lower_scope(em, cx, ir, &mut FactCache::new(), region, base, true)?;
	Ok(Explained::Analysed {
		scope,
		analysis,
	})
}

/// The value of the control-flow statement whose region is `region`, lowered where the analysed
/// run evaluates it: the value its region yields, or the value of the `RETURN` that ends it, which
/// leaves the run with no row.
fn statement_value(
	lowerer: &mut Lowerer<'_>,
	ir: &Ir,
	region: RegionId,
) -> Result<Operand, IrError> {
	let last = ops_in(ir, region).last().ok_or_else(|| invariant("a statement with no end"))?;
	let data = ir.op(last).ok_or(IrError::Stale("ir_lower::explain: op"))?;
	match (&data.kind, data.operands.as_slice()) {
		(OpKind::Phys(PhysOp::Yield) | OpKind::Expr(ExprOp::Yield), [value]) => {
			lowerer.value(*value)
		}
		(OpKind::Phys(PhysOp::Return), [value]) => {
			let value = lowerer.value(*value)?;
			let em = lowerer.emitter();
			let returned = em.reg()?;
			em.emit(Instr::Load {
				dst: returned,
				src: value,
			});
			em.emit(Instr::Block(BlockInstr::Return {
				value: returned,
			}));
			Ok(Operand::Reg(returned))
		}
		_ => Err(IrError::NotYet {
			what: "ir_lower::explain: EXPLAIN ANALYZE of a statement that ends in no value",
		}),
	}
}

/// Whether the op `producer`, which defines the value an `EXPLAIN ANALYZE` explains, is a
/// statement that reads no rows: an expression, or a statement of control flow.
fn reads_no_rows(ir: &Ir, producer: OpId) -> bool {
	matches!(
		ir.op(producer).map(|data| &data.kind),
		Some(OpKind::Expr(_) | OpKind::Phys(PhysOp::Statement { .. }))
	)
}

/// [`analysis_of`] for the context `cx`: its tree reads what the context's compile resolved, and
/// is redacted as the context redacts.
pub(super) fn analysis_in(
	cx: &LowerCx<'_>,
	ir: &Ir,
	explain: OpId,
	purpose: Purpose,
) -> Result<Analysis, IrError> {
	analysis_of(ir, explain, &scope_of(cx), cx.redact_volatile(), purpose)
}

/// What `EXPLAIN ANALYZE` of the statement the `phys.explain` `explain` explains renders: the
/// display tree of its plan, every operator of which reports the one set of metrics the run of the
/// statement records, built for `purpose`.
///
/// `redact` is whether elapsed time and batch counts are left out of the rendering. Answers an
/// invariant error for [`Purpose::Plan`].
pub(crate) fn analysis_of(
	ir: &Ir,
	explain: OpId,
	scope: &ExplainScope<'_>,
	redact: bool,
	purpose: Purpose,
) -> Result<Analysis, IrError> {
	if purpose == Purpose::Plan {
		return Err(invariant("an analysis of a plan that reports no metrics"));
	}
	let data = ir.op(explain).ok_or(IrError::Stale("ir_lower::explain: op"))?;
	let OpKind::Phys(PhysOp::Explain {
		format,
		..
	}) = &data.kind
	else {
		return Err(invariant("an analysis of an op that is not a phys.explain"));
	};
	Ok(Analysis {
		plan: explained_tree(ir, explain, scope, purpose)?,
		format: *format,
		redact,
	})
}
