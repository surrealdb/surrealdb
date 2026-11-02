//! The nearest-neighbour operators of a `SELECT`'s predicates.
//!
//! A `KNN` operator is answered by a search, never evaluated per row: an index search
//! (`phys.knn_scan`, chosen by the access path) or a brute-force search (`phys.knn_topk`), which
//! ranks the rows read beneath it and keeps the `k` nearest. This module decides what the
//! statement's predicates make of their operators.
//!
//! The predicates are the *stages* of a select loop over a read: the filters, permission filters
//! and resolver layers at the head of the loop's body, before the first op that reshapes or
//! breaks the rows. The read is a source op, a union of sources, or either under a table's
//! enforcement.
//!
//! - **Placement** ([`placement_refusal`]). A `KNN` operator that is not a conjunct of a stage's
//!   top-level `AND` chain (one under an `OR`, a `NOT` or a call), and every `<|k|>` operator, is
//!   refused for the statement before any row is read, whatever the read's access path would
//!   answer.
//! - **Brute force** ([`ranked`]). The first brute-force operator (`<|k, DISTANCE|>`) of the
//!   innermost stage that holds one, whose left side is a path and whose right side is a literal
//!   vector or an expression computable without a row, is ranked by a `phys.knn_topk` that stands
//!   directly above that stage: the stages beneath it, its own other conjuncts included, judge the
//!   rows first, and the stages above it judge the `k` it keeps. The access path is decided as if
//!   no handled `KNN` operator were in the predicate ([`withhold_handled`]), so an index search
//!   never answers a statement a brute-force search ranks.
//! - **Stripping** ([`run`]). Where a search answers the statement (a brute-force rank or an index
//!   search beneath the loop), every conjunct of every stage that is a handled `KNN` operator is
//!   dropped: evaluating it would judge each row again against a search that already admitted it. A
//!   loop over a union of sources is not stripped, nor its selection answered: a member no search
//!   reads yields rows the search did not admit.
//! - **The selection** ([`run`]). Every handled `KNN` operator in the statement's projection is
//!   `true`, because every row the search yields is one it admits. A statement in value position
//!   inside the projection is not entered, and an aggregate's fields are not rewritten.
//! - **The unanswered** ([`run`]). Where no search runs for a select loop over one read, whose
//!   predicate holds a `KNN` operator (no index search was planned, as under `WITH NOINDEX` or over
//!   an index being removed, and no brute-force search ranks its rows, as when its query vector is
//!   one only a row binds), each `KNN` operator of the loop and of the clauses that shape its rows
//!   is `false`, the value it takes against a search context that holds no result. A statement in
//!   value position is a select loop of its own. A loop over a union of sources, and a write whose
//!   subjects no search reads, keep their operators: lowering judges each per row, holding for a
//!   row whose record a search of the statement yielded before the row was read. A write's operator
//!   over a vector that is not a literal is marked unsearched, and has no instruction.
//!
//! The query vector of a brute-force search is a literal where the plan knows it: a literal, a
//! parameter the catalog holds as a non-empty array of numbers, or an expression the planner
//! folds to one. Any other vector is the op's second operand, evaluated when the statement starts,
//! in the frame of the predicate that held it; its ops are copied out of the predicate's region
//! to stand before the search.

use smallvec::{SmallVec, smallvec};
use surrealdb_types::ToSql;

use super::access_expr::{Reconstructed, conjunct_values, predicate_of_region};
use super::access_regions::{cone_is_expr, conjunct_region, defined_in, rowless_value_copy};
use super::ops::{KnnTargetShape, KnnTopKAttrs, PhysOp};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::effects;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::catalog::{EffectCx, ExactAnswer, PhysCx};
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::effects::Determinism;
use crate::exec::ir::iface::fold::FoldConsumer;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{LogOp, LoopKind, LoopSink, WriteStageKind};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::planner::util::{
	BruteForceKnnParams, KnnQueryVector, KnnVectorClass, classify_knn_vector,
	extract_bruteforce_knn, has_knn_ktree_operator, has_knn_operator, is_rankable_query_vector,
	knn_query_vectors, resolve_knn_field_operands, strip_knn_from_condition,
};
use crate::exec::vm::payload::Refusal;
use crate::expr::BinaryOperator;
use crate::expr::logical::LogicalExpr;
use crate::expr::operator::NearestNeighbor;
use crate::val::{Number, Value};

/// The stages of a select loop: the ops at the head of its body that judge or resolve the rows of
/// the read beneath it.
#[derive(Debug)]
pub(super) struct Stages {
	/// The select loop.
	pub(super) looped: OpId,
	/// The stage ops, in the order the body runs them: innermost first.
	pub(super) ops: Vec<OpId>,
	/// Whether the loop reads the rows of a union of sources.
	pub(super) united: bool,
}

/// What a stage op is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
	/// A filter, with its `Clause(Where)` region.
	Filter(RegionId),
	/// A permission filter or a resolver layer.
	Guard,
}

/// The stage kind of the op `op`, logical or physical, or `None` for an op that ends the stages.
fn stage_of(ir: &Ir, op: OpId) -> Option<Stage> {
	let data = ir.op(op)?;
	let filter = || {
		let [region] = data.regions.as_slice() else {
			return None;
		};
		(ir.region(*region)?.kind == RegionKind::Clause(ClauseRole::Where))
			.then_some(Stage::Filter(*region))
	};
	match &data.kind {
		OpKind::Logical(LogOp::Filter) | OpKind::Phys(PhysOp::Filter) => filter(),
		OpKind::Logical(LogOp::PermissionFilter {
			..
		})
		| OpKind::Phys(PhysOp::PermissionFilter(..) | PhysOp::Layer(..)) => Some(Stage::Guard),
		OpKind::Logical(LogOp::Project(attrs)) if attrs.layer => Some(Stage::Guard),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// Whether the op `data` is a select loop that delivers each row it is given.
pub(super) fn is_select_loop(kind: &OpKind) -> bool {
	matches!(
		kind,
		OpKind::Logical(LogOp::Loop {
			kind: LoopKind::Select,
			sink: LoopSink::Output,
			..
		}) | OpKind::Phys(PhysOp::Loop {
			kind: LoopKind::Select,
			first: false,
			..
		})
	)
}

/// The stages of the select loop that reads the rows `rows`, through a union of sources or a
/// table's enforcement; `None` when no select loop is the only reader of those rows.
pub(super) fn stages_over(ir: &Ir, mut rows: ValueId) -> Option<Stages> {
	let mut united = false;
	loop {
		let [only] = ir.uses(rows) else {
			return None;
		};
		let user = ir.op(only.user)?;
		match &user.kind {
			OpKind::Logical(LogOp::UnionAll) | OpKind::Phys(PhysOp::Union) => united = true,
			OpKind::Phys(PhysOp::Enforced {
				..
			}) if only.operand == 0 => {}
			kind if is_select_loop(kind) && only.operand == 0 => {
				return Some(Stages {
					united,
					..stages_of_loop(ir, only.user)
				});
			}
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => return None,
		}
		rows = *user.results.first()?;
	}
}

/// The stages of the select loop `looped`: the ops at the head of its body, each reading the row
/// the one before it delivers.
pub(super) fn stages_of_loop(ir: &Ir, looped: OpId) -> Stages {
	let mut ops = Vec::new();
	let body = ir.op(looped).and_then(|data| data.regions.first().copied());
	let element = body.and_then(|body| first_arg(ir, body));
	if let (Some(body), Some(mut row)) = (body, element) {
		for op in ops_in(ir, body) {
			let Some(data) = ir.op(op) else {
				break;
			};
			if stage_of(ir, op).is_none() || data.operands.first() != Some(&row) {
				break;
			}
			let Some(next) = data.results.first() else {
				break;
			};
			ops.push(op);
			row = *next;
		}
	}
	Stages {
		looped,
		ops,
		united: false,
	}
}

/// Whether the select loop `looped` only resolves the rows it reads: its body holds guard stages
/// (permission filters and resolver layers) and yields the row the last of them delivers, so each
/// row it yields is a row of the read beneath it.
pub(super) fn yields_its_rows(ir: &Ir, looped: OpId) -> bool {
	let Some(data) = ir.op(looped) else {
		return false;
	};
	if !is_select_loop(&data.kind) {
		return false;
	}
	let Some(body) = data.regions.first().copied() else {
		return false;
	};
	let Some(element) = first_arg(ir, body) else {
		return false;
	};
	let stages = stages_of_loop(ir, looped);
	if stages.ops.iter().any(|op| stage_of(ir, *op) != Some(Stage::Guard)) {
		return false;
	}
	let row = match stages.ops.last() {
		Some(last) => match ir.op(*last).and_then(|data| data.results.first().copied()) {
			Some(row) => row,
			None => return false,
		},
		None => element,
	};
	let mut rest = ops_in(ir, body).skip(stages.ops.len());
	let yielded = rest.next().and_then(|op| ir.op(op)).is_some_and(|data| {
		matches!(data.kind, OpKind::Logical(LogOp::Yield)) && data.operands.as_slice() == [row]
	});
	yielded && rest.next().is_none()
}

/// The first block argument of `region`.
fn first_arg(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let block = ir.region(region)?.blocks.first()?;
	ir.block(*block)?.args.first().copied()
}

/// The predicate of each filter stage, with its position in `stages.ops`, read with each operator
/// `exact` names as the operator it is rewritten to.
fn filter_predicates(
	ir: &Ir,
	stages: &Stages,
	exact: &[Exact],
) -> Result<Vec<(usize, RegionId, Reconstructed)>, IrError> {
	let mut found = Vec::new();
	for (at, op) in stages.ops.iter().enumerate() {
		let Some(Stage::Filter(region)) = stage_of(ir, *op) else {
			continue;
		};
		let mut predicate = Reconstructed::default();
		if !predicate_of_region(ir, region, &mut predicate)? {
			continue;
		}
		if !exact.is_empty()
			&& let Some((leaves, _)) = conjunct_values(ir, region)
		{
			for (position, conjunct) in &mut predicate.conjuncts {
				let leaf = leaves.get(*position).and_then(|leaf| defining_op(ir, *leaf));
				if let Some(rewrite) = exact.iter().find(|rewrite| Some(rewrite.op) == leaf)
					&& let LogicalExpr::Binary {
						op: BinaryOperator::NearestNeighbor(nn),
						..
					} = conjunct
				{
					**nn = rewrite.operator.clone();
				}
			}
		}
		found.push((at, region, predicate));
	}
	Ok(found)
}

/// An approximate `KNN` operator a read at a stamp ranks as the brute-force operator over the
/// distance of the vector index its field had at the stamp.
#[derive(Clone, Debug)]
pub(super) struct Exact {
	/// The `expr.binary` op of the operator.
	pub(super) op: OpId,
	/// The brute-force operator it is rewritten to.
	pub(super) operator: NearestNeighbor,
}

/// The approximate operators of the stages over the one table source a read at a stamp makes,
/// each with the brute-force operator it is ranked as: an operator whose field is the first column
/// of a vector index of `indexes` (the table's indexes at the stamp, or those it declares now for a
/// stamp only the run evaluates), over that index's distance.
/// An index entry describes the present, so the nearest rows are ranked among the records the
/// read finds at the stamp.
pub(super) fn exact_at_stamp(
	ir: &Ir,
	stages: &Stages,
	indexes: &[crate::catalog::IndexDefinition],
) -> Result<Vec<Exact>, IrError> {
	if indexes.is_empty() {
		return Ok(Vec::new());
	}
	if stages.united {
		return Ok(Vec::new());
	}
	let distance_of = |field: &LogicalExpr| {
		indexes.iter().find_map(|index| {
			let distance = match &index.index {
				crate::catalog::Index::Hnsw(params) => &params.distance,
				crate::catalog::Index::DiskAnn(params) => &params.distance,
				crate::catalog::Index::Idx
				| crate::catalog::Index::Uniq
				| crate::catalog::Index::FullText(_)
				| crate::catalog::Index::Count(_)
				| crate::catalog::Index::Spatial => return None,
			};
			let column = index.cols.first()?;
			(!index.prepare_remove
				&& crate::exec::planner::util::idiom_matches_column(field, column))
			.then(|| distance.clone())
		})
	};
	let mut found = Vec::new();
	for (_, region, predicate) in filter_predicates(ir, stages, &[])? {
		let Some((leaves, _)) = conjunct_values(ir, region) else {
			continue;
		};
		for (position, conjunct) in &predicate.conjuncts {
			let LogicalExpr::Binary {
				left,
				op: BinaryOperator::NearestNeighbor(nn),
				..
			} = conjunct
			else {
				continue;
			};
			let NearestNeighbor::Approximate(k, _) = **nn else {
				continue;
			};
			let (Some(distance), Some(op)) =
				(distance_of(left), leaves.get(*position).and_then(|leaf| defining_op(ir, *leaf)))
			else {
				continue;
			};
			found.push(Exact {
				op,
				operator: NearestNeighbor::K(k, distance.into()),
			});
		}
	}
	Ok(found)
}

/// Rewrites each operator `exact` names to its brute-force operator.
pub(super) fn apply_exact(rw: &mut Rewriter<'_>, exact: &[Exact]) -> Result<(), IrError> {
	for rewrite in exact {
		rw.update_kind(rewrite.op, |kind| {
			if let OpKind::Expr(ExprOp::Binary {
				op,
			}) = kind
			{
				*op = BinaryOperator::NearestNeighbor(Box::new(rewrite.operator.clone()));
			}
		})?;
	}
	Ok(())
}

/// The refusal planning raises for a `KNN` operator of the stages no search can answer, before
/// any row is read, checked in this order after the refusals the binder raises for a vector that
/// reads the row being scanned and for more than one operator a search would serve:
///
/// - a query vector known when the statement is planned ([`known_query_vector`]) that does not
///   coerce to `array<number>`, refused with the coercion's error, or that no distance ranks a
///   record against ([`is_rankable_query_vector`]). A vector computed when the statement runs is
///   checked as it is evaluated, with the same errors;
/// - one of the `<|k|>` form, or one under an `OR`, a `NOT` or a call, which stripping the handled
///   operators from the top-level `AND` chain leaves in place.
pub(super) fn placement_refusal(
	ir: &Ir,
	cx: &PhysCx<'_>,
	stages: &Stages,
) -> Result<Option<Refusal>, IrError> {
	let registry = cx.effects.registry;
	let mut predicates = Vec::new();
	for (_, region, reconstructed) in filter_predicates(ir, stages, &[])? {
		if let Some(mut predicate) = reconstructed.condition() {
			if has_knn_operator(&predicate) {
				resolve_knn_field_operands(&mut predicate, registry);
			}
			predicates.push((region, predicate, reconstructed.operands));
		}
	}
	for (region, predicate, operands) in &predicates {
		for vector in knn_query_vectors(predicate) {
			let known = match classify_knn_vector(&vector, registry) {
				KnnVectorClass::Vector(KnnQueryVector::Literal(known)) => known,
				KnnVectorClass::Vector(KnnQueryVector::Deferred(expr)) => {
					match known_query_vector(ir, cx, *region, &expr)? {
						Some(Ok(known)) => known,
						Some(Err(error)) => {
							return Ok(Some(Refusal::UncoercibleKnnVector {
								message: error.to_string(),
							}));
						}
						None => continue,
					}
				}
				KnnVectorClass::ReadsRow => continue,
			};
			if !is_rankable_query_vector(&known) {
				return Ok(Some(Refusal::InvalidKnnVector {
					vector: written(ir, operands, &vector),
				}));
			}
		}
	}
	for (_, predicate, _) in &predicates {
		if !has_knn_operator(predicate) {
			continue;
		}
		let Some(rest) = strip_knn_from_condition(predicate) else {
			continue;
		};
		if !has_knn_operator(&rest) {
			continue;
		}
		let message = if has_knn_ktree_operator(&rest) {
			"The `<|k|>` KNN operator (KTree / M-Tree) is no longer supported. \
			 Use `<|k, EF|>` against an HNSW index (e.g. \
			 `DEFINE INDEX … HNSW DIMENSION N`), or `<|k, DISTANCE|>` for a \
			 brute-force KNN with an explicit distance metric."
		} else {
			"KNN operators must appear at the top level of the WHERE clause \
			 (joined with AND); nesting `<|k, …|>` inside OR or NOT is not \
			 supported."
		};
		return Ok(Some(Refusal::Query {
			message: message.to_string(),
		}));
	}
	Ok(None)
}

/// What a `KNN` operator's right-hand side `expr`, held by the filter whose `Clause(Where)` region
/// is `region`, is known to be when the statement is planned: a constant expression's value, or a
/// parameter's plan-time value, coerced to `array<number>` as the vector computed when the
/// statement runs is: the vector, or the coercion's error. `None` where nothing is known: a
/// parameter with no plan-time value, or whose value is `NONE`, and any other expression.
fn known_query_vector(
	ir: &Ir,
	cx: &PhysCx<'_>,
	region: RegionId,
	expr: &LogicalExpr,
) -> Result<Option<KnownVector>, IrError> {
	let value = match expr {
		// A value the predicate reads from the statement around it is known only when it runs.
		LogicalExpr::Param(param) if is_operand(param.as_str()) => return Ok(None),
		LogicalExpr::Param(param) => {
			let frame = filter_frame(ir, region)?;
			match cx.effects.catalog.free_value_exact(
				param.as_str(),
				frame,
				FoldConsumer::IndexChoice,
			) {
				ExactAnswer::Value(value) if !value.is_none() => value,
				ExactAnswer::NotLoaded => {
					return Err(IrError::NotLoaded {
						what: "knn::placement_refusal: a stored parameter a KNN query reads",
					});
				}
				ExactAnswer::Value(_) | ExactAnswer::RunTime => return Ok(None),
			}
		}
		other @ (LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
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
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
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
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_)) => match crate::exec::planner::static_value(other) {
			Some(value) => value,
			None => return Ok(None),
		},
	};
	Ok(Some(value.coerce_to::<Vec<Number>>().map(std::sync::Arc::from)))
}

/// Whether `name` is a parameter a rebuilt predicate stands in for a value the statement around
/// it computes ([`Reconstructed::operands`]).
fn is_operand(name: &str) -> bool {
	name.strip_prefix("__operand").is_some_and(|at| at.parse::<usize>().is_ok())
}

/// `expr`, rebuilt over `operands` ([`Reconstructed::operands`]), as the statement wrote it: each
/// operand parameter that stands for a value a name binds is that name.
fn written(ir: &Ir, operands: &[ValueId], expr: &LogicalExpr) -> String {
	let mut expr = expr.clone();
	let _ = expr.visit_mut(&mut |node| {
		if let LogicalExpr::Param(param) = node
			&& let Some(at) = param.as_str().strip_prefix("__operand")
			&& let Ok(at) = at.parse::<usize>()
			&& let Some(name) = operands.get(at).and_then(|value| value_name(ir, *value))
		{
			*param = crate::expr::Param::from(name);
		}
		Ok::<_, std::convert::Infallible>(())
	});
	expr.to_sql()
}

/// The name a read of `value` names: the name it is bound to, or the name of the `LET` that
/// computes it.
fn value_name(ir: &Ir, value: ValueId) -> Option<String> {
	let held = ir.value(value)?;
	if let Some(name) = &held.hint_name {
		return Some(name.to_string());
	}
	let ValueDef::OpResult {
		op,
		..
	} = held.def
	else {
		return None;
	};
	match &ir.op(op)?.kind {
		OpKind::Phys(PhysOp::Let(attrs)) => Some(attrs.name.to_string()),
		OpKind::Logical(LogOp::Let(attrs)) => Some(attrs.name.to_string()),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// A query vector known when the statement is planned, or the error its coercion to
/// `array<number>` raises.
type KnownVector = Result<std::sync::Arc<[Number]>, crate::val::CoerceError>;

/// The frame of the filter whose `Clause(Where)` region is `region`.
fn filter_frame(
	ir: &Ir,
	region: RegionId,
) -> Result<crate::exec::ir::iface::ids::FrameId, IrError> {
	ir.region(region)
		.and_then(|region| region.parent)
		.and_then(|filter| ir.op(filter))
		.map(|filter| filter.frame)
		.ok_or(IrError::Stale("the filter of a KNN operator"))
}

/// The brute-force search a statement's stages rank their rows by.
pub(super) struct Ranked {
	/// The position in `Stages::ops` of the filter that holds the operator.
	stage: usize,
	/// The filter's `Clause(Where)` region.
	region: RegionId,
	/// The position of the operator's conjunct in the filter's predicate.
	position: usize,
	params: BruteForceKnnParams,
}

/// The brute-force operator the stages rank by: the first one held by the innermost filter that
/// holds one.
fn ranked(
	ir: &Ir,
	registry: &FunctionRegistry,
	stages: &Stages,
	exact: &[Exact],
) -> Result<Option<Ranked>, IrError> {
	Ok(filter_predicates(ir, stages, exact)?.into_iter().find_map(|(stage, region, predicate)| {
		predicate.conjuncts.iter().find_map(|(position, conjunct)| {
			let mut conjunct = conjunct.clone();
			resolve_knn_field_operands(&mut conjunct, registry);
			extract_bruteforce_knn(&conjunct, registry).map(|params| Ranked {
				stage,
				region,
				position: *position,
				params,
			})
		})
	}))
}

/// Whether the stages rank their rows by a brute-force search, with the operators `exact` names
/// rewritten: [`ranked`] answers one whose query vector is known to the plan or can be copied out
/// of the predicate that holds it.
pub(super) fn ranks(
	ir: &Ir,
	registry: &FunctionRegistry,
	stages: &Stages,
	exact: &[Exact],
) -> Result<bool, IrError> {
	Ok(ranked(ir, registry, stages, exact)?.is_some_and(|ranked| match &ranked.params.vector {
		KnnQueryVector::Literal(_) => true,
		KnnQueryVector::Deferred(_) => vector_operand(ir, &ranked).is_ok(),
	}))
}

/// Moves the query vector of each `KNN` operator of a select loop's filter stages that the plan
/// does not know ([`known_to_the_plan`]) to stand before the loop's input, where every source the
/// loop reads shares it: a copy of the cone of the operator's right operand, which reads the
/// row being scanned as `$this` ([`rowless_value_copy`]), that the operator then reads. A search
/// takes the copy as its query vector, which it evaluates once, with the row being scanned
/// forbidden to it, as it does any vector known only when the statement runs. Runs on the bound
/// container, before its conversion.
pub(crate) fn hoist_vectors(ir: &mut Ir, facts: &mut FactCache) -> Result<(), IrError> {
	let mut loops = SmallVec::<[OpId; 4]>::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if ir.op(op).is_some_and(|data| is_select_loop(&data.kind)) {
			loops.push(op);
		}
		Walk::Continue
	});
	let mut hoisted = Vec::new();
	for looped in loops {
		for op in stages_of_loop(ir, looped).ops {
			let Some(Stage::Filter(region)) = stage_of(ir, op) else {
				continue;
			};
			let Some((leaves, _)) = conjunct_values(ir, region) else {
				continue;
			};
			for leaf in leaves {
				let Some(knn) = defining_op(ir, leaf) else {
					continue;
				};
				let Some(data) = ir.op(knn) else {
					continue;
				};
				let (
					OpKind::Expr(ExprOp::Binary {
						op: BinaryOperator::NearestNeighbor(nn),
					}),
					[_, right],
				) = (&data.kind, data.operands.as_slice())
				else {
					continue;
				};
				if matches!(**nn, NearestNeighbor::KTree(..))
					|| !defined_in(ir, region, *right)
					|| (cone_is_expr(ir, region, *right) && known_to_the_plan(ir, *right))
				{
					continue;
				}
				hoisted.push((looped, region, knn, *right));
			}
		}
	}
	let mut rw = Rewriter::new(ir, facts);
	for (looped, region, knn, right) in hoisted {
		let at = first_input_op(rw.ir(), looped);
		rw.derived(&[knn], |rw| -> Result<(), IrError> {
			let copied = rowless_value_copy(rw, region, right, at)?;
			rw.set_operand(knn, 1, copied)
		})?;
	}
	Ok(())
}

/// The first op, in the order of the block `looped` stands in, among `looped` and the ops of that
/// block that compute its input: where a value every op reading the loop's rows can read stands.
fn first_input_op(ir: &Ir, looped: OpId) -> OpId {
	let Some(block) = ir.op(looped).map(|data| data.parent) else {
		return looped;
	};
	let Some(ops) = ir.block(block).map(|data| data.ops.as_slice()) else {
		return looped;
	};
	let position = |op: OpId| ops.iter().position(|held| *held == op);
	let mut first = (position(looped).unwrap_or(usize::MAX), looped);
	let mut pending: Vec<ValueId> =
		ir.op(looped).map(|data| data.operands.to_vec()).unwrap_or_default();
	let mut seen = FxHashSet::default();
	while let Some(value) = pending.pop() {
		if !seen.insert(value) {
			continue;
		}
		let Some(op) = defining_op(ir, value) else {
			continue;
		};
		let Some(at) = position(op) else {
			continue;
		};
		if at < first.0 {
			first = (at, op);
		}
		pending.extend(ir.op(op).map(|data| data.operands.to_vec()).unwrap_or_default());
	}
	first.1
}

/// Whether the plan may know the value of the query vector `value`: it rebuilds whole
/// ([`super::access_expr::predicate`]) as a parameter, whose plan-time value the search and the
/// refusals read, or as an expression the planner folds.
fn known_to_the_plan(ir: &Ir, value: ValueId) -> bool {
	super::access_expr::predicate(ir, value).is_ok_and(|rebuilt| {
		rebuilt.withheld.is_empty()
			&& matches!(rebuilt.conjuncts.as_slice(), [(_, expr)]
				if !expr.to_sql().contains("__opaque")
					&& (matches!(expr, LogicalExpr::Param(param) if !is_operand(param.as_str()))
						|| crate::exec::planner::static_value(expr).is_some()))
	})
}

/// Gives each `KNN` scan whose query vector the conversion left to be copied the vector it searches
/// with, in place of the `NONE` that stands for it: a copy, standing before the scan, of the right
/// operand of the operator the stages of the select loop over the scan's rows hold
/// ([`run_time_vector`]). The scans beneath one loop that stand in one block share one copy, which
/// a run of the statement evaluates once.
fn attach_scan_vectors(ir: &mut Ir, facts: &mut FactCache) -> Result<(), IrError> {
	let mut pending = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if ir.op(op).is_some_and(|data| is_select_loop(&data.kind)) {
			let scans = pending_scans(ir, op);
			if !scans.is_empty() {
				pending.push((op, scans));
			}
		}
		Walk::Continue
	});
	let mut rw = Rewriter::new(ir, facts);
	for (looped, scans) in pending {
		let stages = stages_of_loop(rw.ir(), looped);
		let Some((region, value)) = run_time_vector(rw.ir(), &stages) else {
			return Err(IrError::NotYet {
				what: "knn::attach_scan_vectors: a KNN scan whose run-time vector no stage holds",
			});
		};
		let mut copies: Vec<(crate::exec::ir::iface::ids::BlockId, ValueId)> = Vec::new();
		for scan in scans {
			let data = rw.ir().op(scan).ok_or(IrError::Stale("a KNN scan"))?;
			let (block, placeholder) = (data.parent, data.operands[0]);
			let copied = match copies.iter().find(|held| held.0 == block) {
				Some(held) => held.1,
				None => {
					let copied =
						rw.derived(&[scan], |rw| rowless_value_copy(rw, region, value, scan))?;
					copies.push((block, copied));
					copied
				}
			};
			rw.set_operand(scan, 0, copied)?;
			if let Some(ValueDef::OpResult {
				op,
				..
			}) = rw.ir().value(placeholder).map(|held| held.def)
				&& rw.ir().uses(placeholder).is_empty()
			{
				rw.erase(op, EliminationReason::DeadPure)?;
			}
		}
	}
	Ok(())
}

/// The `KNN` scans the select loop `looped` reads directly, through unions, enforcements, layers
/// and permission filters, whose query vector the conversion left to be copied: a scan with an
/// empty vector that reads a `NONE` literal for it.
fn pending_scans(ir: &Ir, looped: OpId) -> Vec<OpId> {
	let mut scans = Vec::new();
	let mut rows: Vec<ValueId> =
		ir.op(looped).and_then(|data| data.operands.first().copied()).into_iter().collect();
	while let Some(at) = rows.pop() {
		let Some(ValueDef::OpResult {
			op,
			..
		}) = ir.value(at).map(|held| held.def)
		else {
			continue;
		};
		let Some(data) = ir.op(op) else {
			continue;
		};
		match &data.kind {
			OpKind::Phys(PhysOp::Union) => rows.extend(data.operands.iter().copied()),
			OpKind::Phys(
				PhysOp::Enforced {
					..
				}
				| PhysOp::Layer(..)
				| PhysOp::PermissionFilter(..),
			) => rows.extend(data.operands.first().copied()),
			OpKind::Phys(PhysOp::KnnScan(attrs))
				if attrs.vector.is_empty()
					&& let [placeholder] = data.operands.as_slice()
					&& let Some((ExprOp::Literal(literal), _)) =
						super::access_expr::expr_def(ir, *placeholder)
					&& literal.is_none() =>
			{
				scans.push(op);
			}
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {}
		}
	}
	scans
}

/// Whether the conjunct `conjunct` is a `KNN` operator a search answers whole.
fn is_handled(conjunct: &LogicalExpr) -> bool {
	strip_knn_from_condition(conjunct).is_none()
}

/// Moves every readable conjunct of `predicate` that is a handled `KNN` operator to the withheld
/// ones, so the access path decided over it sees no such operator and the conjuncts keep their
/// positions.
pub(super) fn withhold_handled(predicate: &mut Reconstructed) {
	let mut kept = Vec::with_capacity(predicate.conjuncts.len());
	for (at, conjunct) in std::mem::take(&mut predicate.conjuncts) {
		if is_handled(&conjunct) {
			predicate.withheld.push(at);
		} else {
			kept.push((at, conjunct));
		}
	}
	predicate.withheld.sort_unstable();
	predicate.conjuncts = kept;
}

/// Ranks, strips and answers the `KNN` operators of every select loop of `ir` over a read, as the
/// module states. Runs on the converted container, before its pipelines are built.
pub(crate) fn run(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) -> Result<(), IrError> {
	attach_scan_vectors(ir, facts)?;
	let mut loops = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if ir.op(op).is_some_and(|data| is_select_loop(&data.kind)) {
			loops.push(op);
		}
		Walk::Continue
	});
	for looped in loops {
		let Some(searched) = read_beneath(ir, looped) else {
			continue;
		};
		let stages = stages_of_loop(ir, looped);
		let ranked = match ranked(ir, cx.effects.registry, &stages, &[])? {
			Some(ranked) => target_of(ir, cx, &ranked)?.map(|target| (ranked, target)),
			None => None,
		};
		if ranked.is_none() && (!searched || united(ir, looped)) {
			continue;
		}
		let mut rw = Rewriter::new(ir, facts);
		rw.derived(&[looped], |rw| -> Result<(), IrError> {
			// A query vector the search reads when the statement runs is copied out of the
			// predicate before the stripping drops the operator that holds it.
			let ranked = match ranked {
				Some((ranked, target)) => {
					let target = settle(rw, looped, &ranked, target)?;
					Some((ranked, target))
				}
				None => None,
			};
			let kept = strip(rw, &stages)?;
			if let Some((ranked, (vector, deferred))) = ranked {
				rank(rw, looped, &kept, &ranked, vector, deferred)?;
			}
			answer_selection(rw, looped)
		})?;
	}
	// The loops are read again: ranking moves the stages of a loop, and the statements in value
	// position within them, into loops of their own.
	let mut loops = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if ir.op(op).is_some_and(|data| is_select_loop(&data.kind)) {
			loops.push(op);
		}
		Walk::Continue
	});
	let mut unanswered = Vec::new();
	for looped in loops {
		if read_beneath(ir, looped) == Some(false) && !united(ir, looped) {
			unanswered.extend(unsearched_operators(ir, looped));
		}
	}
	// The loop over a write's subjects keeps its `KNN` operators: they hold for the subjects a
	// search yields. One over a vector that is not a literal, of a write no search reads, is
	// marked unsearched.
	let mut writes = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::Write,
				..
			}))
		) {
			writes.push(op);
		}
		Walk::Continue
	});
	let mut unsearched = Vec::new();
	for write in writes {
		if read_beneath(ir, write) != Some(true) {
			let mut held = Vec::new();
			if let Some(data) = ir.op(write) {
				for region in &data.regions {
					nearest_in(ir, *region, &mut held);
				}
			}
			unsearched.extend(held.into_iter().filter(|op| !literal_vector(ir, *op)));
		}
	}
	for op in unsearched {
		ir.matches_table_mut().mark_unsearched(op);
	}
	if !unanswered.is_empty() {
		let mut literals = Vec::with_capacity(unanswered.len());
		let mut rw = Rewriter::new(ir, facts);
		for op in unanswered {
			literals.push(rw.derived(&[op], |rw| answer(rw, op, false))?);
		}
		// The literal stands for an operator that reads the row, so a filter judges it per row.
		for literal in literals {
			ir.matches_table_mut().mark_unanswered(literal);
		}
	}
	Ok(())
}

/// The refusal of a write whose rows the source `source` reads, when the write's condition holds a
/// `KNN` operator. A write's operator is answered only by an index search over its subjects, so a
/// write no index search reads is refused rather than judging each row against no search.
///
/// The rows reach the write through unions and enforcement, as [`stages_over`] follows them.
pub(super) fn write_refusal(ir: &Ir, source: OpId) -> Result<Option<Refusal>, IrError> {
	let Some(mut rows) = ir.op(source).and_then(|data| data.results.first().copied()) else {
		return Ok(None);
	};
	let write = loop {
		let [only] = ir.uses(rows) else {
			return Ok(None);
		};
		let Some(user) = ir.op(only.user) else {
			return Ok(None);
		};
		match &user.kind {
			OpKind::Logical(LogOp::UnionAll) | OpKind::Phys(PhysOp::Union) => {
				rows = match user.results.first() {
					Some(next) => *next,
					None => return Ok(None),
				};
			}
			OpKind::Phys(PhysOp::Enforced {
				..
			}) if only.operand == 0 => {
				rows = match user.results.first() {
					Some(next) => *next,
					None => return Ok(None),
				};
			}
			OpKind::Logical(LogOp::Loop {
				kind: LoopKind::Write,
				..
			}) if only.operand == 0 => break only.user,
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => return Ok(None),
		}
	};
	let Some(body) = ir.op(write).and_then(|data| data.regions.first().copied()) else {
		return Ok(None);
	};
	let mut held = Vec::new();
	for op in ops_in(ir, body) {
		let Some(data) = ir.op(op) else {
			continue;
		};
		if !matches!(
			data.kind,
			OpKind::Logical(LogOp::WriteStage {
				stage: WriteStageKind::CheckWhereCondition,
				..
			})
		) {
			continue;
		}
		for region in &data.regions {
			if ir
				.region(*region)
				.is_some_and(|found| found.kind == RegionKind::Clause(ClauseRole::WriteCond))
			{
				collect_operators(ir, *region, &mut held);
			}
		}
	}
	Ok((!held.is_empty()).then(|| Refusal::Query {
		message: WRITE_KNN_NEEDS_INDEX.to_owned(),
	}))
}

/// What a write whose condition holds a `KNN` operator that no index search answers is refused
/// with ([`write_refusal`]).
const WRITE_KNN_NEEDS_INDEX: &str = "A KNN operator in the WHERE of an UPDATE or DELETE needs a \
	vector index on the field; define one, or write `UPDATE (SELECT id FROM ... WHERE ...)`.";

/// Whether the query vector of the `KNN` operator `op` is a literal: a search over it is planned
/// wherever its index may serve the caller, so a read no search reads has none that may.
fn literal_vector(ir: &Ir, op: OpId) -> bool {
	ir.op(op)
		.and_then(|data| data.operands.get(1).copied())
		.and_then(|right| defining_op(ir, right))
		.and_then(|right| ir.op(right))
		.is_some_and(|data| matches!(data.kind, OpKind::Expr(ExprOp::Literal(_))))
}

/// Whether the select loop `looped` reads the rows of a union of sources.
fn united(ir: &Ir, looped: OpId) -> bool {
	let mut rows = ir.op(looped).and_then(|data| data.operands.first().copied());
	while let Some(value) = rows.take() {
		let Some(op) = defining_op(ir, value) else {
			return false;
		};
		let Some(data) = ir.op(op) else {
			return false;
		};
		match &data.kind {
			OpKind::Phys(PhysOp::Enforced {
				..
			}) => rows = data.operands.first().copied(),
			OpKind::Phys(PhysOp::Union) => return true,
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => return false,
		}
	}
	false
}

/// The `KNN` operators a search would be in scope for of the select loop `looped`, over which no
/// search runs: those of its regions, and of the ops after it in its region up to the next select
/// loop, which shape its rows. A statement in value position, a closure or function body and a
/// permission, field, event or subscriber frame are not entered.
fn unsearched_operators(ir: &Ir, looped: OpId) -> Vec<OpId> {
	let mut out = Vec::new();
	let held = |op: OpId, out: &mut Vec<OpId>| {
		if let Some(data) = ir.op(op) {
			for region in &data.regions {
				nearest_in(ir, *region, out);
			}
		}
	};
	held(looped, &mut out);
	for after in ops_in(ir, ir.enclosing_region(looped)).skip_while(|op| *op != looped).skip(1) {
		if ir.op(after).is_none_or(|data| is_select_loop(&data.kind)) {
			break;
		}
		if is_scoped_nearest(ir, after) {
			out.push(after);
		}
		held(after, &mut out);
	}
	out
}

/// Adds to `out` every `KNN` operator a search would be in scope for of `region` and of the regions
/// of its ops, entering no statement in value position and none of the bodies and frames
/// [`unsearched_operators`] names.
fn nearest_in(ir: &Ir, region: RegionId, out: &mut Vec<OpId>) {
	let entered = ir.region(region).is_some_and(|held| {
		!matches!(
			held.kind,
			RegionKind::ClosureBody
				| RegionKind::FunctionBody
				| RegionKind::PermissionFrame
				| RegionKind::FieldFrame(_)
				| RegionKind::EventFrame
				| RegionKind::SubscriberFrame
		)
	});
	if !entered {
		return;
	}
	for op in ops_in(ir, region) {
		let Some(data) = ir.op(op) else {
			continue;
		};
		if matches!(data.kind, OpKind::Expr(ExprOp::Subquery(..))) {
			continue;
		}
		if is_scoped_nearest(ir, op) {
			out.push(op);
		}
		for nested in &data.regions {
			nearest_in(ir, *nested, out);
		}
	}
}

/// Whether `op` is a `KNN` operator a search would be in scope for.
fn is_scoped_nearest(ir: &Ir, op: OpId) -> bool {
	ir.knn_scoped(op)
		&& matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Expr(ExprOp::Binary {
				op: BinaryOperator::NearestNeighbor(_),
			}))
		)
}

/// Whether the select loop `looped` reads a read (a source, a union of sources, or either under a
/// table's enforcement, permission or resolver layer, or a loop of stages alone), and if so whether
/// an index search is among them: `None` for a loop over any other rows.
fn read_beneath(ir: &Ir, looped: OpId) -> Option<bool> {
	let rows = *ir.op(looped)?.operands.first()?;
	let mut pending = vec![rows];
	let mut searched = false;
	while let Some(rows) = pending.pop() {
		let ValueDef::OpResult {
			op,
			..
		} = ir.value(rows)?.def
		else {
			return None;
		};
		let data = ir.op(op)?;
		if is_stage_loop(ir, op) {
			pending.extend(data.operands.first().copied());
			continue;
		}
		let OpKind::Phys(kind) = &data.kind else {
			return None;
		};
		match kind {
			PhysOp::Union => pending.extend(data.operands.iter().copied()),
			PhysOp::Enforced {
				..
			}
			| PhysOp::Layer(..)
			| PhysOp::PermissionFilter(..) => pending.extend(data.operands.first().copied()),
			PhysOp::KnnScan(..) => searched = true,
			PhysOp::TableScan(..)
			| PhysOp::KeyRangeScan(..)
			| PhysOp::IndexScan(..)
			| PhysOp::IndexUnion(..)
			| PhysOp::Bitmap(..)
			| PhysOp::FullTextScan(..)
			| PhysOp::IndexLookup(..)
			| PhysOp::RecordScan(..)
			| PhysOp::Subquery
			| PhysOp::Values {
				..
			}
			| PhysOp::Vacant {
				..
			} => {}
			PhysOp::KnnTopK(_)
			| PhysOp::Count(_)
			| PhysOp::Probe {
				..
			}
			| PhysOp::Join(_)
			| PhysOp::Distinct {
				..
			}
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
			| PhysOp::Next => return None,
		}
	}
	Some(searched)
}

/// Whether `op` is a select loop whose body holds stages alone: the loop a member of a union of
/// sources is resolved or judged in before the union reads its rows.
fn is_stage_loop(ir: &Ir, op: OpId) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	if !is_select_loop(&data.kind) {
		return false;
	}
	let Some(body) = data.regions.first() else {
		return false;
	};
	let stages = stages_of_loop(ir, op).ops.len();
	let held = ops_in(ir, *body).count();
	let ends_in_yield = ops_in(ir, *body).last().is_some_and(|last| {
		matches!(
			ir.op(last).map(|held| &held.kind),
			Some(OpKind::Phys(PhysOp::Yield) | OpKind::Logical(LogOp::Yield))
		)
	});
	ends_in_yield && held == stages + 1
}

/// The query vector of the brute-force search `ranked`: a literal where the plan knows it,
/// otherwise the value of the operator's right operand, which `rank` copies out of the predicate.
/// `None` for a vector computed from what only the predicate's region binds, which no copy outside
/// it can read: the operator then stays in the predicate.
fn target_of(ir: &Ir, cx: &PhysCx<'_>, ranked: &Ranked) -> Result<Option<Target>, IrError> {
	let deferred = match &ranked.params.vector {
		KnnQueryVector::Literal(vector) => return Ok(Some(Target::Literal(vector.to_vec()))),
		KnnQueryVector::Deferred(expr) => expr,
	};
	let resolved = match deferred {
		LogicalExpr::Param(param) => {
			let frame = ir
				.region(ranked.region)
				.and_then(|region| region.parent)
				.and_then(|filter| ir.op(filter))
				.map(|filter| filter.frame)
				.ok_or(IrError::Stale("the filter of a brute-force search"))?;
			match cx.effects.catalog.free_value_exact(
				param.as_str(),
				frame,
				FoldConsumer::IndexChoice,
			) {
				ExactAnswer::Value(Value::Array(array)) if !array.is_empty() => array
					.iter()
					.map(|value| {
						if let Value::Number(number) = value {
							Some(number.clone())
						} else {
							None
						}
					})
					.collect::<Option<Vec<Number>>>(),
				ExactAnswer::NotLoaded => {
					return Err(IrError::NotLoaded {
						what: "knn::run: a stored parameter a brute-force search reads",
					});
				}
				ExactAnswer::Value(_) | ExactAnswer::RunTime => None,
			}
		}
		other @ (LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
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
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
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
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_)) => crate::exec::planner::static_value(other)
			.and_then(|value| value.coerce_to::<Vec<Number>>().ok()),
	};
	if let Some(vector) = resolved {
		return Ok(Some(Target::Literal(vector)));
	}
	Ok(Some(Target::Deferred(vector_operand(ir, ranked)?)))
}

/// The query vector of a brute-force search.
#[derive(Clone, Debug)]
enum Target {
	Literal(Vec<Number>),
	/// The value of the operator's right operand, in the predicate's region.
	Deferred(ValueId),
}

/// The right operand of the operator `ranked` names: the value of the conjunct at its position is
/// the operator.
fn vector_operand(ir: &Ir, ranked: &Ranked) -> Result<ValueId, IrError> {
	let (leaves, _) = conjunct_values(ir, ranked.region)
		.ok_or(IrError::Stale("a where clause that yields no value"))?;
	let leaf = *leaves.get(ranked.position).ok_or(IrError::Stale("a conjunct of the clause"))?;
	let op = defining_op(ir, leaf).ok_or(IrError::Stale("the operator of a brute-force search"))?;
	let data = ir.op(op).ok_or(IrError::Stale("op"))?;
	match (&data.kind, data.operands.as_slice()) {
		(
			OpKind::Expr(ExprOp::Binary {
				op: BinaryOperator::NearestNeighbor(nn),
			}),
			[_, right],
		) if matches!(**nn, NearestNeighbor::K(..)) => Ok(*right),
		_ => Err(IrError::Invariant {
			what: "knn::run: the conjunct of a brute-force search is no KNN operator",
		}),
	}
}

/// The op that defines `value`, when an op does.
fn defining_op(ir: &Ir, value: ValueId) -> Option<OpId> {
	match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			..
		} => Some(op),
		ValueDef::BlockArg {
			..
		} => None,
	}
}

/// Drops every conjunct of every filter stage that is a handled `KNN` operator: a filter left
/// with no conjunct is replaced by its input, and one left with some by a filter of those.
/// Answers each stage op as it stands afterwards, `None` for a filter removed.
fn strip(rw: &mut Rewriter<'_>, stages: &Stages) -> Result<Vec<Option<OpId>>, IrError> {
	let mut kept = Vec::with_capacity(stages.ops.len());
	for op in &stages.ops {
		let Some(Stage::Filter(region)) = stage_of(rw.ir(), *op) else {
			kept.push(Some(*op));
			continue;
		};
		let mut predicate = Reconstructed::default();
		predicate_of_region(rw.ir(), region, &mut predicate)?;
		let total = predicate.len();
		let mut keep: Vec<usize> = predicate.withheld.clone();
		keep.extend(predicate.conjuncts.iter().filter(|(_, c)| !is_handled(c)).map(|(at, _)| *at));
		keep.sort_unstable();
		if keep.len() == total {
			kept.push(Some(*op));
			continue;
		}
		let held = rw.ir().op(*op).ok_or(IrError::Stale("filter"))?;
		let (operands, frame, snapshot, loc) =
			(held.operands.clone(), held.frame, held.snapshot, held.loc.clone());
		let rows = *operands.first().ok_or(IrError::Stale("filter input"))?;
		if keep.is_empty() {
			rw.replace_op(*op, &[rows])?;
			kept.push(None);
			continue;
		}
		let narrowed = rw.create(
			InsertPoint::Before(*op),
			OpSpec {
				kind: OpKind::Phys(PhysOp::Filter),
				operands,
				results: smallvec![TypeKnowledge::Unknown],
				frame,
				snapshot,
				loc,
			},
		)?;
		conjunct_region(rw, narrowed, *op, region, &keep)?;
		let produced = result(rw.ir(), narrowed)?;
		rw.replace_op(*op, &[produced])?;
		kept.push(Some(narrowed));
	}
	Ok(kept)
}

/// The first result of `op`.
fn result(ir: &Ir, op: OpId) -> Result<ValueId, IrError> {
	ir.op(op).and_then(|data| data.results.first().copied()).ok_or(IrError::Stale("result"))
}

/// The query vector of the brute-force search `ranked` as the search states it, and the value it
/// reads when the statement runs: a copy, standing before `looped`, of the operand the predicate
/// computes it with.
fn settle(
	rw: &mut Rewriter<'_>,
	looped: OpId,
	ranked: &Ranked,
	target: Target,
) -> Result<(KnnTargetShape, Option<ValueId>), IrError> {
	Ok(match target {
		Target::Literal(vector) => (KnnTargetShape::Literal(vector), None),
		Target::Deferred(value) => {
			let text = vector_text(rw.ir(), ranked.region, value)?;
			let before_rows = matches!(
				defining_op(rw.ir(), value).and_then(|op| rw.ir().op(op)).map(|data| &data.kind),
				Some(OpKind::Expr(ExprOp::FreeParam { .. } | ExprOp::CatalogParam { .. }))
			);
			(
				KnnTargetShape::Deferred(text, before_rows),
				Some(rowless_value_copy(rw, ranked.region, value, looped)?),
			)
		}
	})
}

/// Puts the brute-force search `ranked` above the stage that held its operator: the stage ops up
/// to it (`kept`, as [`strip`] left them) move into a select loop of their own over the loop's
/// rows, the search reads that loop's rows, and `looped` reads the search's. Where no stage op
/// stands at or beneath the ranked one, that loop yields each row it reads: the search still reads
/// a loop, which is the pipeline the placement pass may run in morsels, each keeping its own
/// nearest rows, where a search reading the scan directly would read it on the statement's task
/// alone. `deferred` is the query vector a `Deferred` target reads ([`settle`]).
fn rank(
	rw: &mut Rewriter<'_>,
	looped: OpId,
	kept: &[Option<OpId>],
	ranked: &Ranked,
	vector: KnnTargetShape,
	deferred: Option<ValueId>,
) -> Result<(), IrError> {
	let data = rw.ir().op(looped).ok_or(IrError::Stale("select loop"))?.clone();
	let rows = *data.operands.first().ok_or(IrError::Stale("select loop rows"))?;
	let body = *data.regions.first().ok_or(IrError::Stale("select loop body"))?;
	let spec = |kind: OpKind, operands: SmallVec<[ValueId; 4]>, results: usize| OpSpec {
		kind,
		operands,
		results: vec![TypeKnowledge::Unknown; results].into_iter().collect(),
		frame: data.frame,
		snapshot: data.snapshot,
		loc: data.loc.clone(),
	};
	let moved: Vec<OpId> = kept[..=ranked.stage].iter().flatten().copied().collect();
	let ranked_rows = beneath(rw, looped, body, rows, &moved, &spec)?;
	let mut operands: SmallVec<[ValueId; 4]> = smallvec![ranked_rows];
	operands.extend(deferred);
	let search = rw.create(
		InsertPoint::Before(looped),
		spec(
			OpKind::Phys(PhysOp::KnnTopK(Box::new(KnnTopKAttrs {
				field: crate::exec::planner::output_field_path(&ranked.params.field.output_path()),
				vector,
				k: ranked.params.k,
				distance: ranked.params.distance.clone(),
			}))),
			operands,
			1,
		),
	)?;
	let searched = result(rw.ir(), search)?;
	rw.set_operand(looped, 0, searched)
}

/// Moves the stage ops `moved`, the head of the body `body` of the select loop `looped`, into a
/// new select loop over `rows` that stands before `looped`, and answers its rows. The rows the
/// last moved op delivered are `looped`'s row from then on; with no op to move, the new loop
/// yields each row of `rows` as it reads it.
fn beneath(
	rw: &mut Rewriter<'_>,
	looped: OpId,
	body: RegionId,
	rows: ValueId,
	moved: &[OpId],
	spec: &dyn Fn(OpKind, SmallVec<[ValueId; 4]>, usize) -> OpSpec,
) -> Result<ValueId, IrError> {
	let ir = rw.ir();
	let kind = ir.op(looped).ok_or(IrError::Stale("select loop"))?.kind.clone();
	let block =
		*ir.region(body).and_then(|data| data.blocks.first()).ok_or(IrError::Stale("body"))?;
	let old_args: Vec<ValueId> = ir.block(block).map(|held| held.args.to_vec()).unwrap_or_default();
	let arg_types: Vec<TypeKnowledge> = old_args
		.iter()
		.map(|arg| ir.value(*arg).map(|value| value.ty.clone()).ok_or(IrError::Stale("value")))
		.collect::<Result<_, _>>()?;
	let element = *old_args.first().ok_or(IrError::Stale("select loop row"))?;
	let inner = rw.create(InsertPoint::Before(looped), spec(kind, smallvec![rows], 1))?;
	let (_, new_block) = rw.create_region(inner, RegionKind::Loop, &arg_types)?;
	let new_args: Vec<ValueId> =
		rw.ir().block(new_block).map(|held| held.args.to_vec()).unwrap_or_default();
	let mut mapped: Vec<(ValueId, ValueId)> =
		old_args.iter().copied().zip(new_args.iter().copied()).collect();
	let mut clones = Vec::with_capacity(moved.len());
	for old in moved {
		let held = rw.ir().op(*old).ok_or(IrError::Stale("stage op"))?.clone();
		let operands = held
			.operands
			.iter()
			.map(|operand| {
				mapped.iter().find(|(from, _)| from == operand).map_or(*operand, |(_, to)| *to)
			})
			.collect();
		let results = held
			.results
			.iter()
			.map(|value| {
				rw.ir().value(*value).map(|value| value.ty.clone()).ok_or(IrError::Stale("value"))
			})
			.collect::<Result<_, _>>()?;
		let clone = rw.create(
			InsertPoint::End(new_block),
			OpSpec {
				kind: held.kind.clone(),
				operands,
				results,
				frame: held.frame,
				snapshot: held.snapshot,
				loc: held.loc.clone(),
			},
		)?;
		for region in &held.regions {
			rw.move_region(*region, clone)?;
		}
		let made = rw.ir().op(clone).map(|data| data.results.clone()).unwrap_or_default();
		mapped.extend(held.results.iter().copied().zip(made.iter().copied()));
		clones.push(clone);
	}
	let delivered = match clones.last() {
		Some(last) => result(rw.ir(), *last)?,
		None => *new_args.first().ok_or(IrError::Stale("select loop row"))?,
	};
	rw.create(
		InsertPoint::End(new_block),
		spec(OpKind::Phys(PhysOp::Yield), smallvec![delivered], 0),
	)?;
	// Retire the originals from the last to the first: the ops after them read the loop's row.
	for (at, old) in moved.iter().enumerate().rev() {
		let replacement = match at + 1 == moved.len() {
			true => element,
			false => result(rw.ir(), clones[at])?,
		};
		rw.replace_op(*old, &[replacement])?;
	}
	result(rw.ir(), inner)
}

/// Writes every handled `KNN` operator of the statement's projection as `true`: the projections
/// of the body of `looped`, and of the select loops, sorts, limits and distincts that read its
/// rows in turn, up to an aggregate or any other op. A stage is never a projection, so the
/// predicates keep their operators.
fn answer_selection(rw: &mut Rewriter<'_>, looped: OpId) -> Result<(), IrError> {
	let mut projections = Vec::new();
	let mut at = Some(looped);
	while let Some(op) = at.take() {
		let ir = rw.ir();
		let Some(data) = ir.op(op) else {
			break;
		};
		if is_select_loop(&data.kind)
			&& let Some(body) = data.regions.first()
		{
			projections.extend(ops_in(ir, *body).filter(|member| {
				matches!(
					ir.op(*member).map(|held| &held.kind),
					Some(OpKind::Phys(PhysOp::Project(..)))
				)
			}));
		}
		let Some(rows) = data.results.first() else {
			break;
		};
		let [only] = ir.uses(*rows) else {
			break;
		};
		let next = ir.op(only.user).map(|user| &user.kind);
		let passes = matches!(
			next,
			Some(OpKind::Phys(
				PhysOp::Sort(..)
					| PhysOp::Limit { .. }
					| PhysOp::Distinct { .. }
					| PhysOp::Loop {
						kind: LoopKind::Select,
						..
					}
			))
		);
		if passes && only.operand == 0 {
			at = Some(only.user);
		}
	}
	let mut operators = Vec::new();
	for projection in projections {
		let regions = rw.ir().op(projection).map(|data| data.regions.to_vec()).unwrap_or_default();
		for region in regions {
			collect_operators(rw.ir(), region, &mut operators);
		}
	}
	for operator in operators {
		answer(rw, operator, true)?;
	}
	Ok(())
}

/// The handled `KNN` operators of `region`, and of the regions of its ops, except a statement in
/// value position.
fn collect_operators(ir: &Ir, region: RegionId, out: &mut Vec<OpId>) {
	for op in ops_in(ir, region) {
		let Some(data) = ir.op(op) else {
			continue;
		};
		match &data.kind {
			OpKind::Expr(ExprOp::Subquery(..)) => continue,
			OpKind::Expr(ExprOp::Binary {
				op: BinaryOperator::NearestNeighbor(nn),
			}) if matches!(**nn, NearestNeighbor::K(..) | NearestNeighbor::Approximate(..)) => {
				out.push(op);
			}
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {}
		}
		for nested in &data.regions {
			collect_operators(ir, *nested, out);
		}
	}
}

/// Replaces the operator `op` by the literal `answered`, erases what only it read, and answers the
/// literal's op.
fn answer(rw: &mut Rewriter<'_>, op: OpId, answered: bool) -> Result<OpId, IrError> {
	let data = rw.ir().op(op).ok_or(IrError::Stale("op"))?.clone();
	let kind = ExprOp::Literal(Box::new(Value::Bool(answered)));
	let ty = result_type(&kind, &[]);
	let literal = rw.create(
		InsertPoint::Before(op),
		OpSpec {
			kind: OpKind::Expr(kind),
			operands: SmallVec::new(),
			results: smallvec![ty],
			frame: data.frame,
			snapshot: data.snapshot,
			loc: data.loc.clone(),
		},
	)?;
	let answered = result(rw.ir(), literal)?;
	rw.replace_op(op, &[answered])?;
	let mut pending: Vec<ValueId> = data.operands.to_vec();
	while let Some(value) = pending.pop() {
		if !rw.ir().uses(value).is_empty() {
			continue;
		}
		let Some(producer) = defining_op(rw.ir(), value) else {
			continue;
		};
		let Some(held) = rw.ir().op(producer) else {
			continue;
		};
		let dead = matches!(held.kind, OpKind::Expr(_))
			&& held.results.iter().all(|result| rw.ir().uses(*result).is_empty());
		if !dead {
			continue;
		}
		let operands = held.operands.to_vec();
		rw.erase(producer, EliminationReason::DeadPure)?;
		pending.extend(operands);
	}
	Ok(literal)
}

/// The query vector `value`, which the `Clause(Where)` region `region` computes, as the statement
/// wrote it: the text the binder recorded on the op that computes it, or the name a read of it
/// names, else the vector the region's predicate rebuilds as. It is the text a refusal of the
/// vector's value names.
pub(super) fn vector_text(ir: &Ir, region: RegionId, value: ValueId) -> Result<String, IrError> {
	let held = ir.value(value).ok_or(IrError::Stale("value"))?;
	if let ValueDef::OpResult {
		op,
		..
	} = held.def
		&& let Some(text) = ir.op(op).and_then(|data| data.loc.text())
	{
		return Ok(text.to_owned());
	}
	if let Some(name) = value_name(ir, value) {
		return Ok(format!("${name}"));
	}
	let mut predicate = Reconstructed::default();
	if predicate_of_region(ir, region, &mut predicate)?
		&& let Some(vector) = predicate
			.conjuncts
			.iter()
			.find_map(|(_, conjunct)| knn_query_vectors(conjunct).into_iter().next())
	{
		return Ok(written(ir, &predicate.operands, &vector));
	}
	if defined_in(ir, region, value)
		|| ir
			.region(region)
			.is_some_and(|r| matches!(r.kind, RegionKind::Clause(ClauseRole::OrderKey)))
	{
		return Ok(String::new());
	}
	Err(IrError::Invariant {
		what: "knn::vector_text: a KNN query vector with no text",
	})
}

/// The region of the filter stage of `stages` holding the first `KNN` operator, with its right
/// operand: the query vector an index search reads when the statement runs. `None` when no stage
/// holds one. A vector that reads the row being scanned is refused when the statement is bound;
/// one that reads it in a way its expression does not show is refused when it is evaluated
/// ([`rowless_value_copy`]).
pub(super) fn run_time_vector(ir: &Ir, stages: &Stages) -> Option<(RegionId, ValueId)> {
	for op in &stages.ops {
		let Some(Stage::Filter(region)) = stage_of(ir, *op) else {
			continue;
		};
		let mut found = None;
		ir.walk(region, WalkOrder::PreOrder, &mut |inner| {
			if let Some(data) = ir.op(inner)
				&& let OpKind::Expr(ExprOp::Binary {
					op: BinaryOperator::NearestNeighbor(_),
				}) = &data.kind
				&& let Some(right) = data.operands.get(1)
			{
				found = Some((region, *right));
				return Walk::Stop;
			}
			Walk::Continue
		});
		if found.is_some() {
			return found;
		}
	}
	sort_key_vector(ir, stages)
}

/// The query vector of an `ORDER BY vector::distance::<metric>(field, vector)` key over the rows
/// of `stages`, with the region of the sort key: the first of the call's two leading operands that
/// reads no row. `None` when no sort over the rows measures a distance.
fn sort_key_vector(ir: &Ir, stages: &Stages) -> Option<(RegionId, ValueId)> {
	if let Some(rows) = ir.op(stages.looped).and_then(|d| d.results.first().copied())
		&& let [only] = ir.uses(rows)
		&& let Some(user) = ir.op(only.user)
		&& matches!(user.kind, OpKind::Logical(LogOp::Sort(_)) | OpKind::Phys(PhysOp::Sort(_)))
		&& let Some(&region) = user.regions.first()
	{
		let mut found = None;
		ir.walk(region, WalkOrder::PreOrder, &mut |inner| {
			if let Some(data) = ir.op(inner)
				&& let OpKind::Expr(ExprOp::Call(attrs)) = &data.kind
				&& matches!(
					&attrs.function,
					crate::expr::Function::Normal(name) if name.starts_with("vector::distance::")
				) && let Some(&vec_val) = data
				.operands
				.iter()
				.take(2)
				.find(|&&op| !super::access_regions::reads_region_args(ir, region, op))
			{
				found = Some((region, vec_val));
				return Walk::Stop;
			}
			Walk::Continue
		});
		return found;
	}
	None
}

/// The most ops of a sort key's query vector [`sort_vector_is_stable`] reads; a larger cone is
/// not shown stable.
const STABLE_VECTOR_OPS: usize = 1024;

/// Whether the query vector of an `ORDER BY vector::distance::<metric>(...)` key over the rows of
/// `stages` ([`sort_key_vector`]) answers one value however often it is evaluated. A `KNN` scan
/// answering that order evaluates the vector once, before the scan, where the sort evaluates it
/// per row, so only a vector whose cone draws no entropy and has no effect a second evaluation
/// would repeat may move to the scan. A vector that may raise still moves: its cone reads no
/// row, so it raises for every row the sort would measure, and the scan differs only in raising
/// where no row reaches the sort, as a `KNN` operator's vector or a `LIMIT` evaluated before its
/// scan does. `true` when no sort over the rows measures a distance.
pub(super) fn sort_vector_is_stable(ir: &Ir, cx: &EffectCx<'_>, stages: &Stages) -> bool {
	let Some((_, vector)) = sort_key_vector(ir, stages) else {
		return true;
	};
	let mut facts = FactCache::new();
	let mut seen: FxHashSet<OpId> = FxHashSet::default();
	let mut pending = vec![vector];
	while let Some(next) = pending.pop() {
		let Some(ValueDef::OpResult {
			op,
			..
		}) = ir.value(next).map(|data| data.def)
		else {
			continue;
		};
		if !seen.insert(op) {
			continue;
		}
		if seen.len() > STABLE_VECTOR_OPS {
			return false;
		}
		let held = effects::of_with(ir, &mut facts, cx, op);
		if held.determinism == Determinism::NonDeterministic
			|| !held.set.is_deterministic_but_for_clock()
		{
			return false;
		}
		if let Some(data) = ir.op(op) {
			pending.extend(data.operands.iter().copied());
		}
	}
	true
}
