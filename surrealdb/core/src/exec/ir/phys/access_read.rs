//! A conjunct that names an index operator, read whole as the planner's expression.
//!
//! The index analysis decides from [`LogicalExpr`], so a conjunct the analysis must see is rebuilt
//! from its ops in full, as the planner holds the expression it was lowered from. This is the
//! reading for a conjunct that [`owes_index_operator`]: it holds a `MATCHES` operator the
//! planner's `expr_contains_matches` finds, or a `KNN` operator its `has_knn_operator` finds. The
//! planner walks `Binary`, `Prefix` and `Call` argument chains for the first, and every
//! expression but a nested statement for the second, and so does this module: a `MATCHES` or
//! `KNN` operator in a nested statement is the nested plan's, and the conjunct that holds one is
//! left to the filter.
//!
//! Every form the planner's analysis can inspect is read as the planner holds it: literals,
//! parameters, field paths, collections, record ids, calls, methods, casts, prefix, postfix and
//! binary operators, short-circuit operators, `IF`, optional steps, destructures, `SetField`,
//! broadcasts, and the bare hops of a lookup path (a hop that names whole tables and no clause).
//! A field read stands for the row of the filter (or the value a path step applies to, inside the
//! body of a step). A form with no `LogicalExpr` the analysis reads (a nested statement, a
//! closure, any other loop, a value of a name that no region of the conjunct binds, a field read
//! off a value that is not the row) is an opaque node, a call of a function no registry holds,
//! named after the op that defines it. An opaque node is never a path, a literal, a parameter, the
//! statement clock or a call the analysis matches, so it keys no index and states no operand, and
//! two of them are never equal, so two conjuncts that differ only there stay distinct. An opaque
//! node carries, as its arguments, the `KNN` operators of the ops it stands for and a field read
//! for every field they read (a nested statement carries none, as the planner's walks stop at
//! one), so the planner's walk for `KNN` and its check for a field a permission restricts find
//! them where they would find them in the expression. A field read carried this way is a whole
//! read of the field, which is the conservative answer of that check.
//!
//! A value of the statement that is fixed per execution and unknown at plan time (the statement
//! timestamp, the clock) is a parameter named after its position in the operands the predicate
//! collects, exactly as [`super::access_expr`] rebuilds it.
//!
//! The forms no binder emits inside an expression (`expr.deref`, a conversion that is not a cast,
//! `expr.depth_charge`, `expr.tuple_row`, a bare dispatch check, a terminator read as a value)
//! answer the invariant break of the binder's seam ([`IrError::Verify`]).

#![deny(clippy::wildcard_enum_match_arm)]

use std::ops::Bound;

use slotmap::Key;
use surrealdb_strand::Strand;

use super::access_expr::{
	expr_def, is_row, is_statement_argument, operand_param, strip_truthy, yielded,
};
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{
	ConvertMode, DestructureEntry, ExprOp, KeyBound, KeyShape, OnFail, ShortCircuitOp,
};
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId, ValueId};
use crate::exec::ir::logical::ops::{LogOp, LookupAttrs, LoopKind, StepAttrs};
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::planner::util::try_value_to_logical_expr;
use crate::expr::logical::{
	LogicalCall, LogicalDestructureField, LogicalExpr, LogicalIf, LogicalLiteral, LogicalLookup,
	LogicalLookupSubject, LogicalRecordIdKey, LogicalRecordIdKeyRange, LogicalRecordIdLit,
};
use crate::expr::{BinaryOperator, Function, Param, PrefixOperator};

/// The deepest expression the reading descends through before it answers a capacity error. The
/// parser bounds the depth of an expression, so a conjunct of a statement the parser accepted
/// is read within it.
const MAX_DEPTH: usize = 512;

/// Whether the conjunct `leaf` holds an index operator the planner's walks find: a `MATCHES`
/// operator reached through `Binary`, `Prefix` and `Call` argument forms (a cast is a prefix
/// form), or a `KNN` operator anywhere but in a nested statement.
pub(super) fn owes_index_operator(ir: &Ir, leaf: ValueId) -> bool {
	matches_in_chain(ir, leaf) || !knn_in_cone(ir, leaf, true).is_empty()
}

/// Whether a `MATCHES` operator stands in the chain of `Binary`, `Prefix` and `Call` argument
/// forms that `value` starts: the walk of the planner's `expr_contains_matches`.
fn matches_in_chain(ir: &Ir, value: ValueId) -> bool {
	let mut seen: FxHashSet<ValueId> = FxHashSet::default();
	let mut pending = vec![value];
	while let Some(value) = pending.pop() {
		let value = strip_truthy(ir, value);
		if !seen.insert(value) {
			continue;
		}
		let Some((kind, data)) = expr_def(ir, value) else {
			continue;
		};
		match kind {
			ExprOp::Binary {
				op,
			} => {
				if matches!(op, BinaryOperator::Matches(_)) {
					return true;
				}
				pending.extend(data.operands.iter().copied());
			}
			ExprOp::ShortCircuit {
				..
			} => {
				pending.extend(data.operands.first().copied());
				pending.extend(data.regions.first().and_then(|region| yielded(ir, *region)));
			}
			ExprOp::Prefix(_) => pending.extend(data.operands.first().copied()),
			ExprOp::Convert(attrs)
				if matches!((attrs.mode, attrs.on_fail), (ConvertMode::Cast, OnFail::Error)) =>
			{
				pending.extend(data.operands.first().copied());
			}
			ExprOp::Call(call) => {
				let arguments = data.operands.len().saturating_sub(usize::from(call.row_names));
				pending.extend(data.operands.iter().take(arguments).copied());
			}
			ExprOp::Literal(_)
			| ExprOp::Array
			| ExprOp::Set
			| ExprOp::Object {
				..
			}
			| ExprOp::RecordId(_)
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
			| ExprOp::ComputedRead(_)
			| ExprOp::Doc
			| ExprOp::Access {
				..
			}
			| ExprOp::Deref {
				..
			}
			| ExprOp::Index
			| ExprOp::First
			| ExprOp::Last
			| ExprOp::Each
			| ExprOp::Flatten {
				..
			}
			| ExprOp::Dispatch(_)
			| ExprOp::Method {
				..
			}
			| ExprOp::Destructure {
				..
			}
			| ExprOp::Optional
			| ExprOp::RepeatRecurse
			| ExprOp::SetField {
				..
			}
			| ExprOp::TupleRow {
				..
			}
			| ExprOp::Convert(_)
			| ExprOp::Postfix {
				..
			}
			| ExprOp::Invoke
			| ExprOp::ClosureCreate(_)
			| ExprOp::If
			| ExprOp::Throw
			| ExprOp::Subquery(_)
			| ExprOp::Yield
			| ExprOp::Truthy
			| ExprOp::Agg {
				..
			}
			| ExprOp::Now {
				..
			}
			| ExprOp::Raise(_)
			| ExprOp::DepthCharge {
				..
			} => {}
		}
	}
	false
}

/// What the cone of some ops holds that the planner's walks look for.
#[derive(Default)]
struct Cone {
	/// The results of the `KNN` operators.
	knn: Vec<ValueId>,
	/// The fields read, in the order met, each once.
	fields: Vec<Strand>,
}

/// The results of the `KNN` operators in the cone of `value`. With `first_only` the search ends
/// at the first.
fn knn_in_cone(ir: &Ir, value: ValueId, first_only: bool) -> Vec<ValueId> {
	cone_of(ir, defining_op(ir, value).into_iter().collect(), first_only).knn
}

/// What the ops `pending` and the ops they stand on hold: the ops that define their operands and
/// the ops in their regions. A nested statement's region is not entered, and a `KNN` operator's
/// own operands are not searched (reading the operator reads them). A `KNN` operator in the body
/// of a closure literal runs only where the closure is invoked, as an ordinary expression, so it
/// is no operator of the cone; the fields that body reads are. With `first_only` the search ends
/// at the first `KNN` operator.
fn cone_of(ir: &Ir, pending: Vec<OpId>, first_only: bool) -> Cone {
	let mut cone = Cone::default();
	let mut seen: FxHashSet<OpId> = FxHashSet::default();
	let mut pending: Vec<(OpId, bool)> = pending.into_iter().map(|op| (op, false)).collect();
	while let Some((op, in_closure)) = pending.pop() {
		if !seen.insert(op) {
			continue;
		}
		let Some(data) = ir.op(op) else {
			continue;
		};
		let mut body_in_closure = in_closure;
		match &data.kind {
			OpKind::Expr(ExprOp::Binary {
				op: BinaryOperator::NearestNeighbor(_),
			}) if !in_closure => {
				cone.knn.extend(data.results.first().copied());
				if first_only {
					return cone;
				}
				continue;
			}
			OpKind::Expr(ExprOp::Subquery(_)) => continue,
			OpKind::Expr(ExprOp::ClosureCreate(_)) => body_in_closure = true,
			OpKind::Expr(ExprOp::Field {
				name,
			}) => note_field(&mut cone, name),
			OpKind::Expr(ExprOp::ComputedRead(attrs)) => note_field(&mut cone, &attrs.field),
			OpKind::Expr(ExprOp::Doc) => note_field(&mut cone, &Strand::from("id")),
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {}
		}
		pending.extend(
			data.operands
				.iter()
				.filter_map(|value| defining_op(ir, *value))
				.map(|op| (op, in_closure)),
		);
		for region in &data.regions {
			pending.extend(ops_in(ir, *region).map(|op| (op, body_in_closure)));
		}
	}
	cone
}

fn note_field(cone: &mut Cone, name: &Strand) {
	if !cone.fields.contains(name) {
		cone.fields.push(name.clone());
	}
}

/// The op that defines `value`, or `None` for a block argument.
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

/// The conjunct `leaf` read whole, with the values it stands for per execution appended to
/// `operands` as [`super::access_expr`] numbers them.
pub(super) fn read_conjunct(
	ir: &Ir,
	leaf: ValueId,
	operands: &mut Vec<ValueId>,
) -> Result<LogicalExpr, IrError> {
	read_conjunct_whole(ir, leaf, operands).map(|(expr, _)| expr)
}

/// The conjunct `leaf` read as [`read_conjunct`] reads it, and whether the reading made an opaque
/// node: a form the analysis cannot inspect.
pub(super) fn read_conjunct_whole(
	ir: &Ir,
	leaf: ValueId,
	operands: &mut Vec<ValueId>,
) -> Result<(LogicalExpr, bool), IrError> {
	let mut read = Read {
		ir,
		operands,
		currents: Vec::new(),
		depth: 0,
		made_opaque: false,
	};
	let expr = read.value(leaf)?;
	Ok((expr, read.made_opaque))
}

/// The state of reading one conjunct.
struct Read<'a> {
	ir: &'a Ir,
	/// The values the parameters standing for per-execution operands name.
	operands: &'a mut Vec<ValueId>,
	/// The block arguments that denote the value a step applies to ([`LogicalExpr::Current`]).
	currents: Vec<ValueId>,
	depth: usize,
	/// Whether the reading made an opaque node.
	made_opaque: bool,
}

impl Read<'_> {
	/// `value` as the planner holds it.
	fn value(&mut self, value: ValueId) -> Result<LogicalExpr, IrError> {
		if self.depth >= MAX_DEPTH {
			return Err(IrError::Capacity("access predicate depth"));
		}
		self.depth += 1;
		let read = self.value_inner(value);
		self.depth -= 1;
		read
	}

	fn value_inner(&mut self, value: ValueId) -> Result<LogicalExpr, IrError> {
		let value = strip_truthy(self.ir, value);
		if self.currents.contains(&value) {
			return Ok(LogicalExpr::Current);
		}
		if is_statement_argument(self.ir, value) {
			return Ok(operand_param(self.operands, value));
		}
		let Some(op) = defining_op(self.ir, value) else {
			self.made_opaque = true;
			return Ok(unique_call(format!("{OPAQUE}_value{}", value.data().as_ffi()), Vec::new()));
		};
		let data = self.ir.op(op).ok_or(IrError::Stale("op"))?;
		match &data.kind {
			OpKind::Expr(kind) => self.expr_op(op, kind, data),
			OpKind::Logical(LogOp::Loop {
				kind: LoopKind::GraphStep,
				step: Some(step),
				..
			})
			| OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::GraphStep,
				step: Some(step),
				..
			}) => match self.hop(step, &data.operands)? {
				Some(hop) => Ok(hop),
				None => self.opaque(op),
			},
			OpKind::Logical(LogOp::Loop {
				kind: LoopKind::Broadcast,
				step,
				..
			})
			| OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::Broadcast,
				step,
				..
			}) => {
				let flatten = match step.as_deref() {
					None => false,
					Some(StepAttrs::Flatten) => true,
					Some(_) => return self.opaque(op),
				};
				let ([source], [body]) = (data.operands.as_slice(), data.regions.as_slice()) else {
					return self.opaque(op);
				};
				let (source, body) = (*source, *body);
				let base = match is_row(self.ir, source) {
					true => LogicalExpr::Current,
					false => self.value(source)?,
				};
				Ok(LogicalExpr::Broadcast {
					base: Box::new(base),
					body: Box::new(self.region(body, true)?),
					flatten,
				})
			}
			OpKind::Logical(_) | OpKind::Phys(_) => self.opaque(op),
		}
	}

	/// A bare hop of a lookup path as the planner holds it: a lookup over the value its source
	/// is, naming whole tables and no clause. `None` for a hop with any clause, a key range, `ONLY`
	/// or an alias, which the analysis never matches and which stays an opaque node.
	fn hop(
		&mut self,
		step: &StepAttrs,
		operands: &[ValueId],
	) -> Result<Option<LogicalExpr>, IrError> {
		let StepAttrs::Lookup(attrs) = step else {
			return Ok(None);
		};
		let LookupAttrs {
			kind,
			only,
			only_path,
			subjects,
			fields,
			order,
			alias,
			cond,
			split,
			group,
			limit,
			start,
			trace,
		} = attrs;
		let bare = !*only
			&& !*only_path
			&& fields.is_none()
			&& order.is_none()
			&& alias.is_none()
			&& !*cond && split.is_none()
			&& group.is_none()
			&& limit.is_none()
			&& start.is_none()
			&& subjects.iter().all(|subject| subject.range.is_none());
		let ([source], true) = (operands, bare) else {
			return Ok(None);
		};
		let base = match is_row(self.ir, *source) {
			true => LogicalExpr::Current,
			false => self.value(*source)?,
		};
		let what = subjects
			.iter()
			.map(|subject| LogicalLookupSubject::Table {
				table: subject.table.clone(),
				referencing_field: subject.referencing_field.clone(),
			})
			.collect();
		Ok(Some(LogicalExpr::Lookup {
			base: Box::new(base),
			lookup: Box::new(LogicalLookup {
				kind: kind.clone(),
				expr: None,
				only: false,
				what,
				cond: None,
				split: None,
				group: None,
				order: None,
				limit: None,
				start: None,
				alias: None,
				trace: *trace,
			}),
		}))
	}

	/// Whether a field read off `base` reads the row of the filter or the value a step applies to.
	fn reads_row(&self, base: Option<&ValueId>) -> bool {
		base.is_some_and(|base| is_row(self.ir, *base) || self.currents.contains(base))
	}

	/// The opaque node for the op `op`, carrying the `KNN` operators it stands for and the fields
	/// it reads.
	fn opaque(&mut self, op: OpId) -> Result<LogicalExpr, IrError> {
		let cone = cone_of(self.ir, vec![op], false);
		let mut carried = Vec::new();
		for knn in cone.knn {
			carried.push(self.value(knn)?);
		}
		carried.extend(cone.fields.into_iter().map(LogicalExpr::Field));
		self.made_opaque = true;
		Ok(unique_call(format!("{OPAQUE}{}", op.data().as_ffi()), carried))
	}

	fn each(&mut self, values: &[ValueId]) -> Result<Vec<LogicalExpr>, IrError> {
		values.iter().map(|value| self.value(*value)).collect()
	}

	/// The value the body of `region` yields, or the expression a body that diverges is. The
	/// region's argument is [`LogicalExpr::Current`] when `current`.
	fn region(&mut self, region: RegionId, current: bool) -> Result<LogicalExpr, IrError> {
		let data = self.ir.region(region).ok_or(IrError::Stale("region"))?;
		let [block] = data.blocks.as_slice() else {
			return Err(invariant("an expression region holds one block"));
		};
		let block = *block;
		let argument = current
			.then(|| self.ir.block(block).and_then(|held| held.args.first().copied()))
			.flatten();
		self.currents.extend(argument);
		let read = self.terminator(block);
		if argument.is_some() {
			self.currents.pop();
		}
		read
	}

	fn terminator(&mut self, block: BlockId) -> Result<LogicalExpr, IrError> {
		let data = self.ir.block(block).ok_or(IrError::Stale("block"))?;
		let last = *data.ops.last().ok_or_else(|| invariant("an expression region holds no op"))?;
		let closing = self.ir.op(last).ok_or(IrError::Stale("op"))?;
		match &closing.kind {
			OpKind::Expr(ExprOp::Yield)
			| OpKind::Phys(PhysOp::Yield)
			| OpKind::Logical(LogOp::Yield) => {
				let [yielded] = closing.operands.as_slice() else {
					return Err(invariant("a region yields one value"));
				};
				self.value(*yielded)
			}
			OpKind::Expr(ExprOp::Throw) => {
				let [thrown] = closing.operands.as_slice() else {
					return Err(invariant("a throw holds one operand"));
				};
				Ok(LogicalExpr::Throw(Box::new(self.value(*thrown)?)))
			}
			OpKind::Phys(PhysOp::Break) | OpKind::Logical(LogOp::Break) => Ok(LogicalExpr::Break),
			OpKind::Phys(PhysOp::Next) | OpKind::Logical(LogOp::Next) => Ok(LogicalExpr::Continue),
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => self.opaque(last),
		}
	}

	/// The values every yield of the body of `region` delivers, in order.
	fn region_values(&mut self, region: RegionId) -> Result<Vec<LogicalExpr>, IrError> {
		let data = self.ir.region(region).ok_or(IrError::Stale("region"))?;
		let [block] = data.blocks.as_slice() else {
			return Err(invariant("an expression region holds one block"));
		};
		let last = self
			.ir
			.block(*block)
			.and_then(|held| held.ops.last().copied())
			.ok_or_else(|| invariant("an expression region holds no op"))?;
		let closing = self.ir.op(last).ok_or(IrError::Stale("op"))?;
		match &closing.kind {
			OpKind::Expr(ExprOp::Yield) | OpKind::Phys(PhysOp::Yield) => {
				let operands = closing.operands.clone();
				self.each(&operands)
			}
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {
				Err(invariant("an aggregate's arguments are yielded"))
			}
		}
	}

	/// The subject of the dispatch check `dispatch`: the receiver of a method or the callee of an
	/// invocation.
	fn dispatch_subject(&self, dispatch: ValueId) -> Result<ValueId, IrError> {
		match expr_def(self.ir, dispatch) {
			Some((ExprOp::Dispatch(_), data)) => data
				.operands
				.first()
				.copied()
				.ok_or_else(|| invariant("a dispatch check holds its subject")),
			_ => Err(invariant("a method call or an invocation holds its dispatch")),
		}
	}

	fn expr_op(&mut self, op: OpId, kind: &ExprOp, data: &OpData) -> Result<LogicalExpr, IrError> {
		let operands = data.operands.as_slice();
		let regions = data.regions.as_slice();
		match kind {
			ExprOp::Literal(value) => match try_value_to_logical_expr((**value).clone()) {
				Some(expr) => Ok(expr),
				None => self.opaque(op),
			},
			ExprOp::Array => Ok(LogicalExpr::Array(self.each(operands)?)),
			ExprOp::Set => Ok(LogicalExpr::Set(self.each(operands)?)),
			ExprOp::Object {
				keys,
			} => {
				if keys.len() != operands.len() {
					return Err(invariant("an object holds one value per key"));
				}
				let mut entries = Vec::with_capacity(keys.len());
				for (key, operand) in keys.iter().zip(operands) {
					entries.push((key.clone(), self.value(*operand)?));
				}
				Ok(LogicalExpr::Object(entries))
			}
			ExprOp::RecordId(attrs) => {
				let mut taken = operands.iter();
				let key = self.key(&attrs.key_shape, &mut taken)?;
				if taken.next().is_some() {
					return Err(invariant("a record id holds an operand its key does not use"));
				}
				Ok(LogicalExpr::RecordId(Box::new(LogicalRecordIdLit {
					table: attrs.table.clone(),
					key,
				})))
			}
			ExprOp::FreeParam {
				name,
			}
			| ExprOp::SessionParam {
				name,
			}
			| ExprOp::CatalogParam {
				name,
			} => Ok(LogicalExpr::Param(Param::from(name.clone()))),
			ExprOp::Table {
				name,
			} => Ok(LogicalExpr::Table(name.clone())),
			ExprOp::Mock(mock) => Ok(LogicalExpr::Mock((**mock).clone())),
			ExprOp::Constant {
				c,
			} => Ok(LogicalExpr::Constant(c.clone())),
			ExprOp::Field {
				name,
			} => match self.reads_row(operands.first()) {
				true => Ok(LogicalExpr::Field(name.clone())),
				false => self.opaque(op),
			},
			ExprOp::ComputedRead(attrs) => match self.reads_row(operands.first()) {
				true => Ok(LogicalExpr::Field(attrs.field.clone())),
				false => self.opaque(op),
			},
			ExprOp::Doc => match operands.first() {
				Some(base) if is_row(self.ir, *base) => Ok(LogicalExpr::Field(Strand::from("id"))),
				Some(base) if self.currents.contains(base) => Ok(LogicalExpr::Doc),
				Some(_) | None => self.opaque(op),
			},
			ExprOp::Access {
				field,
			} => {
				let [base] = exactly(operands)?;
				Ok(LogicalExpr::Access {
					base: Box::new(self.value(base)?),
					field: field.clone(),
				})
			}
			ExprOp::Deref {
				..
			} => Err(invariant("expr.deref is built by no binder")),
			ExprOp::Index => {
				let [base, index] = exactly(operands)?;
				Ok(LogicalExpr::Index {
					base: Box::new(self.value(base)?),
					index: Box::new(self.value(index)?),
				})
			}
			ExprOp::First => {
				let [base] = exactly(operands)?;
				Ok(LogicalExpr::First(Box::new(self.value(base)?)))
			}
			ExprOp::Last => {
				let [base] = exactly(operands)?;
				Ok(LogicalExpr::Last(Box::new(self.value(base)?)))
			}
			ExprOp::Each => {
				let [base] = exactly(operands)?;
				Ok(LogicalExpr::Each(Box::new(self.value(base)?)))
			}
			ExprOp::Flatten {
				explicit,
			} => {
				let [base] = exactly(operands)?;
				Ok(LogicalExpr::Flatten {
					base: Box::new(self.value(base)?),
					explicit: *explicit,
				})
			}
			ExprOp::Dispatch(_) => {
				Err(invariant("a dispatch check is read through its method or invocation"))
			}
			ExprOp::Method {
				name,
			} => {
				let [dispatch, receiver, arguments @ ..] = operands else {
					return Err(invariant("a method call holds its dispatch and its receiver"));
				};
				if self.dispatch_subject(*dispatch)? != *receiver {
					return Err(invariant("a method call's dispatch checks its receiver"));
				}
				Ok(LogicalExpr::Method {
					base: Box::new(self.value(*receiver)?),
					name: name.clone(),
					args: self.each(arguments)?,
				})
			}
			ExprOp::Destructure {
				shape,
			} => {
				let [base] = exactly(operands)?;
				let base = self.value(base)?;
				let mut taken = regions.iter().copied();
				let fields = self.destructure(shape, &mut taken)?;
				if taken.next().is_some() {
					return Err(invariant("a destructure holds a region no alias uses"));
				}
				Ok(LogicalExpr::Destructure {
					base: Box::new(base),
					fields,
				})
			}
			ExprOp::Optional => {
				let [base] = exactly(operands)?;
				let base = self.value(base)?;
				let then = self.region(region_at(regions, 0)?, true)?;
				Ok(LogicalExpr::Optional {
					base: Box::new(base),
					then: Box::new(then),
				})
			}
			ExprOp::RepeatRecurse => {
				let [base] = exactly(operands)?;
				Ok(LogicalExpr::RepeatRecurse(Box::new(self.value(base)?)))
			}
			ExprOp::SetField {
				field,
			} => {
				let [base, value] = exactly(operands)?;
				Ok(LogicalExpr::SetField {
					base: Box::new(self.value(base)?),
					field: field.clone(),
					value: Box::new(self.value(value)?),
				})
			}
			ExprOp::TupleRow {
				..
			} => Err(invariant("expr.tuple_row stands only as an element of an INSERT")),
			ExprOp::Prefix(op) => {
				let [operand] = exactly(operands)?;
				Ok(LogicalExpr::Prefix {
					op: (**op).clone(),
					expr: Box::new(self.value(operand)?),
				})
			}
			ExprOp::Convert(attrs) => {
				let [operand] = exactly(operands)?;
				match (attrs.mode, attrs.on_fail) {
					(ConvertMode::Cast, OnFail::Error) => Ok(LogicalExpr::Prefix {
						op: PrefixOperator::Cast(attrs.kind.clone()),
						expr: Box::new(self.value(operand)?),
					}),
					(ConvertMode::Cast, OnFail::None)
					| (ConvertMode::Coerce, OnFail::Error)
					| (ConvertMode::Coerce, OnFail::None) => {
						Err(invariant("a binder writes a conversion only as a cast"))
					}
				}
			}
			ExprOp::Postfix {
				op,
			} => {
				let [operand] = exactly(operands)?;
				Ok(LogicalExpr::Postfix {
					expr: Box::new(self.value(operand)?),
					op: op.clone(),
				})
			}
			ExprOp::Invoke => {
				let [dispatch, arguments @ ..] = operands else {
					return Err(invariant("an invocation holds its dispatch"));
				};
				let callee = self.dispatch_subject(*dispatch)?;
				Ok(LogicalExpr::Invoke {
					target: Box::new(self.value(callee)?),
					args: self.each(arguments)?,
				})
			}
			ExprOp::Binary {
				op,
			} => {
				let [left, right] = exactly(operands)?;
				Ok(LogicalExpr::Binary {
					left: Box::new(self.value(left)?),
					op: op.clone(),
					right: Box::new(self.value(right)?),
				})
			}
			ExprOp::ShortCircuit {
				op,
			} => {
				let [left] = exactly(operands)?;
				let left = self.value(left)?;
				let right = self.region(region_at(regions, 0)?, false)?;
				Ok(LogicalExpr::Binary {
					left: Box::new(left),
					op: binary_of(*op),
					right: Box::new(right),
				})
			}
			ExprOp::Call(call) => {
				let arguments = operands
					.len()
					.checked_sub(usize::from(call.row_names))
					.and_then(|count| operands.get(..count))
					.ok_or_else(|| invariant("a call holds the row names it passes"))?;
				Ok(LogicalExpr::Call(Box::new(LogicalCall {
					receiver: call.function.clone(),
					args: self.each(arguments)?,
				})))
			}
			ExprOp::ClosureCreate(_) => self.opaque(op),
			ExprOp::Subquery(_) => {
				self.made_opaque = true;
				Ok(unique_call(format!("{OPAQUE}{}", op.data().as_ffi()), Vec::new()))
			}
			ExprOp::If => {
				let [condition] = exactly(operands)?;
				let condition = self.value(condition)?;
				let then = self.region(region_at(regions, 0)?, false)?;
				let rest = self.region(region_at(regions, 1)?, false)?;
				Ok(if_of(condition, then, rest))
			}
			ExprOp::Throw | ExprOp::Yield | ExprOp::Raise(_) => {
				Err(invariant("a terminator is not a value"))
			}
			ExprOp::Truthy => {
				let [operand] = exactly(operands)?;
				self.value(operand)
			}
			ExprOp::Agg {
				kind: function,
				..
			} => {
				let args = self.region_values(region_at(regions, 0)?)?;
				Ok(LogicalExpr::Call(Box::new(LogicalCall {
					receiver: Function::Normal((*function).to_owned()),
					args,
				})))
			}
			ExprOp::Now {
				..
			} => {
				let value = data
					.results
					.first()
					.copied()
					.ok_or_else(|| invariant("a clock read has a result"))?;
				Ok(operand_param(self.operands, value))
			}
			ExprOp::DepthCharge {
				..
			} => Err(invariant("expr.depth_charge stands in no conjunct")),
		}
	}

	fn destructure(
		&mut self,
		shape: &[DestructureEntry],
		regions: &mut impl Iterator<Item = RegionId>,
	) -> Result<Vec<LogicalDestructureField>, IrError> {
		let mut fields = Vec::with_capacity(shape.len());
		for entry in shape {
			fields.push(match entry {
				DestructureEntry::All(name) => LogicalDestructureField::All(name.clone()),
				DestructureEntry::Field(name) => LogicalDestructureField::Field(name.clone()),
				DestructureEntry::Aliased(name) => {
					let region = regions
						.next()
						.ok_or_else(|| invariant("a destructure alias has no region"))?;
					LogicalDestructureField::Aliased(name.clone(), self.region(region, true)?)
				}
				DestructureEntry::Destructure(name, nested) => {
					LogicalDestructureField::Destructure(
						name.clone(),
						self.destructure(nested, regions)?,
					)
				}
				DestructureEntry::Rest => LogicalDestructureField::Rest,
			});
		}
		Ok(fields)
	}

	/// The key of a record id, taking the operands the shape names in the order it names them.
	fn key(
		&mut self,
		shape: &KeyShape,
		operands: &mut std::slice::Iter<'_, ValueId>,
	) -> Result<LogicalRecordIdKey, IrError> {
		Ok(match shape {
			KeyShape::Number(n) => LogicalRecordIdKey::Number(*n),
			KeyShape::String(s) => LogicalRecordIdKey::String(s.clone()),
			KeyShape::Uuid(u) => LogicalRecordIdKey::Uuid(*u),
			KeyShape::Generate(g) => LogicalRecordIdKey::Generate(g.clone()),
			KeyShape::Array {
				len,
			} => LogicalRecordIdKey::Array(self.take_exprs(operands, *len)?),
			KeyShape::Object {
				keys,
			} => LogicalRecordIdKey::Object(self.take_entries(operands, keys)?),
			KeyShape::Range(range) => {
				LogicalRecordIdKey::Range(Box::new(LogicalRecordIdKeyRange {
					start: self.bound(&range.start, operands)?,
					end: self.bound(&range.end, operands)?,
				}))
			}
		})
	}

	fn bound(
		&mut self,
		bound: &Bound<KeyBound>,
		operands: &mut std::slice::Iter<'_, ValueId>,
	) -> Result<Bound<LogicalRecordIdKey>, IrError> {
		let (key, wrap): (&KeyBound, fn(LogicalRecordIdKey) -> Bound<LogicalRecordIdKey>) =
			match bound {
				Bound::Unbounded => return Ok(Bound::Unbounded),
				Bound::Included(key) => (key, Bound::Included),
				Bound::Excluded(key) => (key, Bound::Excluded),
			};
		Ok(wrap(match key {
			KeyBound::Number(n) => LogicalRecordIdKey::Number(*n),
			KeyBound::String(s) => LogicalRecordIdKey::String(s.clone()),
			KeyBound::Uuid(u) => LogicalRecordIdKey::Uuid(*u),
			KeyBound::Generate(g) => LogicalRecordIdKey::Generate(g.clone()),
			KeyBound::Array {
				len,
			} => LogicalRecordIdKey::Array(self.take_exprs(operands, *len)?),
			KeyBound::Object {
				keys,
			} => LogicalRecordIdKey::Object(self.take_entries(operands, keys)?),
		}))
	}

	fn take_expr(
		&mut self,
		operands: &mut std::slice::Iter<'_, ValueId>,
	) -> Result<LogicalExpr, IrError> {
		let operand =
			operands.next().ok_or_else(|| invariant("a record id key lacks an operand"))?;
		self.value(*operand)
	}

	fn take_exprs(
		&mut self,
		operands: &mut std::slice::Iter<'_, ValueId>,
		len: u32,
	) -> Result<Vec<LogicalExpr>, IrError> {
		let len = usize::try_from(len).map_err(|_| IrError::Capacity("record id key elements"))?;
		let mut out = Vec::with_capacity(len.min(operands.len()));
		for _ in 0..len {
			out.push(self.take_expr(operands)?);
		}
		Ok(out)
	}

	fn take_entries(
		&mut self,
		operands: &mut std::slice::Iter<'_, ValueId>,
		keys: &[String],
	) -> Result<Vec<(String, LogicalExpr)>, IrError> {
		let mut out = Vec::with_capacity(keys.len());
		for key in keys {
			out.push((key.clone(), self.take_expr(operands)?));
		}
		Ok(out)
	}
}

/// The prefix of the name of every opaque node's call.
const OPAQUE: &str = "__opaque";

/// The call of the function `name`, which no registry holds, with `carried` as its arguments.
fn unique_call(name: String, carried: Vec<LogicalExpr>) -> LogicalExpr {
	LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal(name),
		args: carried,
	}))
}

fn exactly<const N: usize>(operands: &[ValueId]) -> Result<[ValueId; N], IrError> {
	<[ValueId; N]>::try_from(operands)
		.map_err(|_| invariant("an expression op holds the operands its tag declares"))
}

fn region_at(regions: &[RegionId], at: usize) -> Result<RegionId, IrError> {
	regions
		.get(at)
		.copied()
		.ok_or_else(|| invariant("an expression op holds the regions its tag declares"))
}

fn binary_of(op: ShortCircuitOp) -> BinaryOperator {
	match op {
		ShortCircuitOp::And => BinaryOperator::And,
		ShortCircuitOp::Or => BinaryOperator::Or,
		ShortCircuitOp::NullCoalescing => BinaryOperator::NullCoalescing,
		ShortCircuitOp::TernaryCondition => BinaryOperator::TenaryCondition,
	}
}

/// An `IF` chain from its first condition, its value and the value of the rest. The rest is
/// another chain, which joins this one, a `NONE` literal, which stands for no `ELSE`, or the
/// `ELSE` value.
fn if_of(condition: LogicalExpr, then: LogicalExpr, rest: LogicalExpr) -> LogicalExpr {
	let mut branches = vec![(condition, then)];
	let otherwise = if let LogicalExpr::If(chain) = rest {
		let LogicalIf {
			branches: more,
			otherwise,
		} = *chain;
		branches.extend(more);
		otherwise
	} else if matches!(rest, LogicalExpr::Literal(LogicalLiteral::None)) {
		None
	} else {
		Some(rest)
	};
	LogicalExpr::If(Box::new(LogicalIf {
		branches,
		otherwise,
	}))
}
