//! `phys.aggregate`: the groups the rows of a pipeline fold into, and the host code that finishes
//! each group.
//!
//! An aggregate has one `ErrorScope(ShapeToNone)` region per group key, then one
//! `Clause(Projection)` region per field. A field's region holds the whole column: the aggregate
//! calls it makes are `expr.agg` ops, each with an error-scope region that yields the call's
//! arguments, and the expression around them is evaluated once for the group.
//!
//! # What a field is
//!
//! [`plan`] reads each field in this order, as the aggregate operator plans it:
//!
//! - **A group key.** The field is an expression that a written key also is, or it is aliased under
//!   the text of a key. A key that names the alias of a field is evaluated as that field's
//!   expression. The group answers the field by its key.
//! - **Aggregate calls.** The field holds `expr.agg` ops, outside any statement in value position.
//!   Each call folds the value of its first argument for every row. The arguments after the first
//!   are evaluated once for the group, over an object that holds each key as `_gN`; one that raises
//!   whatever the row raises once for the group, a `GROUP ALL` over no row included. What remains
//!   of the field around the calls is evaluated once for the group over an object that holds the
//!   answer of each call as `_aN` and each key as `_gN`; a field that is exactly one call needs no
//!   such expression.
//! - **The group's first value.** A field the op lists in `first_value` takes the value of the
//!   group's first row.
//! - **A value the row cannot influence.** Such a field is evaluated once for the group, as the
//!   group finishes where there is one group, and with the first row of each group otherwise.
//! - **An expression over expression keys.** The field holds no aggregate call and reads the row
//!   only through values that a key written as an expression also is. It has one value for the
//!   group, which the host evaluates over an object that holds each key as `_gN`.
//! - **Anything else** reads the row, so each row's value is collected, duplicates kept.
//!
//! A value reads a key where it is an expression that a written key is. A path of fields,
//! positions and every-element steps from the row is compared step by step; any other expression (a
//! path of another form such as a method call or a destructure, or an operator or a call over the
//! row) is compared as written, op by op, as the planner compares two expressions.
//!
//! The rows fold in the body of the pipeline that feeds the aggregate: each key is set, the group
//! is selected, and each call's first argument or each first value is folded or set. An error
//! about the shape of the data in a key or an argument leaves its value `NONE`.

use super::{Body, Needs, row_arg, yielded_value};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::fxhash::{FxHashMap, FxHashSet};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::iface::identity::AttrDigest;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::logical::ops::{FieldShape, FieldsShape};
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::AggregateAttrs;
use crate::exec::operators::{AggregateId, aggregate_field_name, group_key_field_name};
use crate::exec::vm::ir_lower::expr::Lowerer;
use crate::exec::vm::ir_lower::units::LowerCx;
use crate::exec::vm::program::{AggCall, AggExpr, AggSpec, AggregateSink, Instr, Operand, ROW};
use crate::expr::logical::{OutputPath, OutputPathPart};
use crate::expr::{BinaryOperator, Function};
use crate::val::{Number, Value};

/// A value evaluated in a region, over the row of the pipeline.
#[derive(Clone, Copy, Debug)]
pub(super) struct Eval {
	/// The region the value is lowered in.
	region: RegionId,
	/// The block argument that is the row the value is read from.
	row: ValueId,
	/// The value.
	value: ValueId,
}

/// One aggregate call of a field.
#[derive(Debug)]
struct Call {
	/// The value the call defines.
	result: ValueId,
	/// The registered name of the function.
	kind: &'static str,
	/// The region that yields the arguments.
	region: RegionId,
	/// The arguments, in order. Empty where the region raises.
	args: Vec<ValueId>,
	/// Whether the region raises in place of yielding: an argument raises whatever the row, and
	/// the binder binds nothing after it.
	raises: bool,
	/// Whether the argument that raises is one after the first: the first argument's value was
	/// computed before the raise and is read by nothing. Such an argument describes the group, so
	/// the raise is owed to every group, a `GROUP ALL` over no row included.
	raises_after_first: bool,
}

impl Call {
	/// Whether the call is written with an argument: one that raises is one.
	fn has_argument(&self) -> bool {
		self.raises || !self.args.is_empty()
	}
}

/// What a field of the aggregate is.
#[derive(Debug)]
enum Column {
	/// The key at the position.
	Key(usize),
	/// Aggregate calls, and whether an expression over their answers follows.
	Calls {
		calls: Vec<Call>,
		post: bool,
	},
	/// The value of each row, collected.
	Collect,
	/// A value the row cannot influence.
	Constant,
	/// The value of the group's first row.
	First,
}

/// A field of the aggregate.
#[derive(Debug)]
struct Field {
	/// Where the field lands in the group's object.
	path: Vec<String>,
	/// The whole column.
	eval: Eval,
	column: Column,
}

/// One step of a path from the row.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Step {
	/// A field, read directly or through an access.
	Name(String),
	/// The record id of the row.
	Doc,
	First,
	Last,
	Each,
	/// A literal position.
	Position(i64),
}

/// A group key as it is written.
#[derive(Debug)]
enum Written {
	/// A path of plain steps.
	Plain(Vec<Step>),
	/// A key that is not a plain path: a path of another form, or an expression over the row. A
	/// value matches it when it is written the same way.
	Other(Eval),
}

impl Written {
	/// The steps of a plain path.
	fn plain(&self) -> Option<&[Step]> {
		match self {
			Written::Plain(steps) => Some(steps),
			Written::Other(_) => None,
		}
	}
}

/// The position of the key in `written` that `value`, read from the row `row`, is: a plain path
/// with the same steps, or a path of another form written the same way. The row itself is no key
/// unless `whole_row`.
fn written_key(
	ir: &Ir,
	row: ValueId,
	value: ValueId,
	written: &[Written],
	whole_row: bool,
) -> Option<usize> {
	match path_of(ir, row, value) {
		PathForm::Plain(steps) if whole_row || !steps.is_empty() => {
			written.iter().position(|key| key.plain() == Some(steps.as_slice()))
		}
		PathForm::Plain(_) => None,
		PathForm::Other | PathForm::Not => written.iter().position(|key| match key {
			Written::Other(eval) => Congruence::new(ir).same((row, value), (eval.row, eval.value)),
			Written::Plain(_) => false,
		}),
	}
}

/// Whether two expressions are written the same way: each op of one has the kind and attributes
/// of the op at the same place in the other, with operands and regions that are written the same
/// way. The row of each side corresponds, and so does every value an op or a region block of one
/// side defines with the value at the same place in the other. A value defined outside both is
/// the same only as itself. An op that is not an expression op is never the same as another.
struct Congruence<'i> {
	ir: &'i Ir,
	/// The value of the second side each value of the first side corresponds to.
	map: FxHashMap<ValueId, ValueId>,
}

impl<'i> Congruence<'i> {
	fn new(ir: &'i Ir) -> Self {
		Self {
			ir,
			map: FxHashMap::default(),
		}
	}

	/// Whether `a`, read from its row, is written as `b` is, read from its own.
	fn same(&mut self, (row_a, a): (ValueId, ValueId), (row_b, b): (ValueId, ValueId)) -> bool {
		if !self.roots_comparable(a, b) {
			return false;
		}
		self.map.insert(row_a, row_b);
		self.values(a, b)
	}

	/// Whether the ops that define `a` and `b` have the same kind and attributes, which settles
	/// most comparisons before any map is built. A value that is no op result is not ruled out.
	fn roots_comparable(&self, a: ValueId, b: ValueId) -> bool {
		let defining = |value: ValueId| {
			let ValueDef::OpResult {
				op,
				..
			} = self.ir.value(value)?.def
			else {
				return None;
			};
			match &self.ir.op(op)?.kind {
				OpKind::Expr(kind) => Some(kind),
				OpKind::Logical(_) | OpKind::Phys(_) => None,
			}
		};
		match (defining(a), defining(b)) {
			(Some(left), Some(right)) => same_attrs(left, right),
			_ => true,
		}
	}

	fn values(&mut self, a: ValueId, b: ValueId) -> bool {
		if let Some(held) = self.map.get(&a) {
			return *held == b;
		}
		if a == b {
			return true;
		}
		let (Some(left), Some(right)) = (self.ir.value(a), self.ir.value(b)) else {
			return false;
		};
		match (left.def, right.def) {
			(
				ValueDef::OpResult {
					op: op_a,
					index: index_a,
				},
				ValueDef::OpResult {
					op: op_b,
					index: index_b,
				},
			) if index_a == index_b => self.ops(op_a, op_b) && self.map.get(&a) == Some(&b),
			_ => false,
		}
	}

	fn ops(&mut self, a: OpId, b: OpId) -> bool {
		let ir = self.ir;
		let (Some(left), Some(right)) = (ir.op(a), ir.op(b)) else {
			return false;
		};
		let (OpKind::Expr(kind_a), OpKind::Expr(kind_b)) = (&left.kind, &right.kind) else {
			return false;
		};
		if !same_attrs(kind_a, kind_b)
			|| left.operands.len() != right.operands.len()
			|| left.regions.len() != right.regions.len()
			|| left.results.len() != right.results.len()
		{
			return false;
		}
		if !left.operands.iter().zip(&right.operands).all(|(x, y)| self.values(*x, *y)) {
			return false;
		}
		if !left.regions.iter().zip(&right.regions).all(|(x, y)| self.regions(*x, *y)) {
			return false;
		}
		for (x, y) in left.results.iter().zip(&right.results) {
			self.map.insert(*x, *y);
		}
		true
	}

	fn regions(&mut self, a: RegionId, b: RegionId) -> bool {
		let ir = self.ir;
		let (Some(left), Some(right)) = (ir.region(a), ir.region(b)) else {
			return false;
		};
		if left.kind != right.kind || left.blocks.len() != right.blocks.len() {
			return false;
		}
		for (x, y) in left.blocks.iter().zip(&right.blocks) {
			let (Some(x), Some(y)) = (ir.block(*x), ir.block(*y)) else {
				return false;
			};
			if x.args.len() != y.args.len() || x.ops.len() != y.ops.len() {
				return false;
			}
			for (arg_x, arg_y) in x.args.iter().zip(&y.args) {
				self.map.insert(*arg_x, *arg_y);
			}
			if !x.ops.iter().zip(&y.ops).all(|(op_x, op_y)| self.ops(*op_x, *op_y)) {
				return false;
			}
		}
		true
	}
}

/// Whether two expression ops have the same attributes. A conversion is the same wherever it is
/// written: its site only names the error it raises, and an error in a key reads as `NONE`.
fn same_attrs(a: &ExprOp, b: &ExprOp) -> bool {
	match (a, b) {
		(ExprOp::Convert(x), ExprOp::Convert(y)) => {
			x.mode == y.mode && x.kind == y.kind && x.on_fail == y.on_fail
		}
		_ => {
			let digest = a.attr_digest();
			digest != AttrDigest::OPAQUE && digest == b.attr_digest() && a.attrs_eq(b)
		}
	}
}

/// What an expression is, as a path from a row.
enum PathForm {
	/// A path of plain steps.
	Plain(Vec<Step>),
	/// A path of another form.
	Other,
	/// Not a path.
	Not,
}

/// What an aggregate op reads and does with its regions.
#[derive(Debug)]
pub(super) struct Plan {
	/// The evaluation of each group key, as the aggregate operator evaluates it.
	keys: Vec<Eval>,
	/// The path each key is written as.
	written: Vec<Written>,
	fields: Vec<Field>,
}

impl Plan {
	/// Whether the rows fall into groups by key: with no key, they are one group.
	fn grouped(&self) -> bool {
		!self.keys.is_empty()
	}
}

/// The fields `fields` lists, each with the alias it is grouped under.
///
/// `*` is never a field of an aggregate. The binder refuses a grouped `*` in place of the
/// aggregate, as the planner's aggregation analysis does. The fold of a `MATCH` lists only named
/// columns, so a `*` here is an internal error.
fn listed(fields: &FieldsShape) -> Result<Vec<Option<&OutputPath>>, IrError> {
	match fields {
		FieldsShape::Value {
			alias,
		} => Ok(vec![alias.as_ref()]),
		FieldsShape::Select(list) => list
			.iter()
			.map(|field| match field {
				FieldShape::Single {
					alias,
					..
				} => Ok(alias.as_ref()),
				FieldShape::All => Err(IrError::Invariant {
					what: "ir_lower::pipe: an aggregate over the whole row",
				}),
			})
			.collect(),
	}
}

/// The keys of an output path, as the group's object holds them.
fn output_keys(path: &OutputPath) -> Vec<String> {
	path.parts()
		.iter()
		.map(|part| match part {
			OutputPathPart::Field(key) | OutputPathPart::Lookup(key) => key.clone(),
		})
		.collect()
}

/// The field `name` is read as: a plain field name, or the computed field of that name.
fn field_name(op: &ExprOp) -> Option<&str> {
	if let ExprOp::Field {
		name,
	} = op
	{
		Some(name.as_str())
	} else if let ExprOp::ComputedRead(attrs) = op {
		Some(attrs.field.as_str())
	} else {
		None
	}
}

/// What `value`, read from the row `row`, is as a path.
fn path_of(ir: &Ir, row: ValueId, value: ValueId) -> PathForm {
	if value == row {
		return PathForm::Plain(Vec::new());
	}
	let Some(ValueDef::OpResult {
		op,
		index: 0,
	}) = ir.value(value).map(|data| data.def)
	else {
		return PathForm::Not;
	};
	let Some(data) = ir.op(op) else {
		return PathForm::Not;
	};
	let OpKind::Expr(kind) = &data.kind else {
		return PathForm::Not;
	};
	let extend = |base: &ValueId, step: Step| match path_of(ir, row, *base) {
		PathForm::Plain(mut steps) => {
			steps.push(step);
			PathForm::Plain(steps)
		}
		PathForm::Other | PathForm::Not => PathForm::Other,
	};
	match (kind, data.operands.as_slice()) {
		(kind, [base]) if field_name(kind).is_some() => match *base == row {
			true => {
				PathForm::Plain(vec![Step::Name(field_name(kind).unwrap_or_default().to_owned())])
			}
			false => PathForm::Other,
		},
		(ExprOp::Doc, [base]) if *base == row => PathForm::Plain(vec![Step::Doc]),
		(
			ExprOp::Access {
				field,
			},
			[base],
		) => extend(base, Step::Name(field.as_str().to_owned())),
		(ExprOp::First, [base]) => extend(base, Step::First),
		(ExprOp::Last, [base]) => extend(base, Step::Last),
		(ExprOp::Each, [base]) => extend(base, Step::Each),
		(ExprOp::Index, [base, index]) => match literal_position(ir, *index) {
			Some(position) => extend(base, Step::Position(position)),
			None => PathForm::Other,
		},
		(
			ExprOp::Doc
			| ExprOp::Index
			| ExprOp::Flatten {
				..
			}
			| ExprOp::Method {
				..
			}
			| ExprOp::Destructure {
				..
			}
			| ExprOp::Optional
			| ExprOp::RepeatRecurse,
			_,
		) => PathForm::Other,
		_ => PathForm::Not,
	}
}

/// The integer `value` is, when it is a literal one.
fn literal_position(ir: &Ir, value: ValueId) -> Option<i64> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	if let OpKind::Expr(ExprOp::Literal(held)) = &ir.op(op)?.kind
		&& let Value::Number(Number::Int(position)) = &**held
	{
		return Some(*position);
	}
	None
}

/// The ops of the region `region` and of the regions beneath it, in the order they are written.
///
/// A statement in value position and an aggregate call are neither entered: the first plans on its
/// own, and the arguments of the second are read from each row, not from the group.
fn visit_ops(ir: &Ir, region: RegionId, visit: &mut dyn FnMut(OpId, &OpData)) {
	for op in ops_in(ir, region) {
		let Some(data) = ir.op(op) else {
			continue;
		};
		visit(op, data);
		if matches!(data.kind, OpKind::Expr(ExprOp::Subquery(_) | ExprOp::Agg { .. })) {
			continue;
		}
		for inner in &data.regions {
			visit_ops(ir, *inner, &mut *visit);
		}
	}
}

/// The aggregate calls of the field region `region`, in the order they are written.
fn calls_of(ir: &Ir, region: RegionId) -> Result<Vec<Call>, IrError> {
	let mut found = Vec::new();
	visit_ops(ir, region, &mut |op, data| {
		if let OpKind::Expr(ExprOp::Agg {
			kind,
			..
		}) = &data.kind
		{
			found.push((op, *kind));
		}
	});
	let mut calls = Vec::with_capacity(found.len());
	for (op, kind) in found {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		let [inner] = <[RegionId; 1]>::try_from(data.regions.as_slice())
			.map_err(|_| invariant("an aggregate call with other than one region"))?;
		let mut nested = false;
		visit_ops(ir, inner, &mut |_, held| {
			nested |= matches!(held.kind, OpKind::Expr(ExprOp::Agg { .. }));
		});
		// The binder refuses a grouping that nests one aggregate call in another, as the planner
		// does, and binds none of its fields.
		if nested {
			return Err(IrError::Invariant {
				what: "ir_lower::pipe: an aggregate call inside an aggregate call",
			});
		}
		let closing = ops_in(ir, inner)
			.last()
			.and_then(|last| ir.op(last))
			.ok_or_else(|| invariant("an aggregate call with an empty region"))?;
		// An argument that raises whatever the row ends the region with the raise.
		let (args, raises) = match &closing.kind {
			OpKind::Expr(ExprOp::Yield) => (closing.operands.to_vec(), false),
			OpKind::Expr(ExprOp::Throw | ExprOp::Raise(_)) => (Vec::new(), true),
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {
				return Err(invariant("an aggregate call whose region neither yields nor raises"));
			}
		};
		let raises_after_first = raises && {
			let mut ops: Vec<OpId> = ops_in(ir, inner).collect();
			ops.pop();
			ops.iter().any(|held| {
				ir.op(*held).is_some_and(|data| {
					data.results.iter().any(|result| ir.uses(*result).is_empty())
				})
			})
		};
		let result =
			*data.results.first().ok_or_else(|| invariant("an aggregate with no result"))?;
		calls.push(Call {
			result,
			kind,
			region: inner,
			args,
			raises,
			raises_after_first,
		});
	}
	Ok(calls)
}

/// The values of the regions beneath `region` that are a path that a written key is, each with
/// the position of the key.
fn key_reads(
	ir: &Ir,
	region: RegionId,
	row: ValueId,
	written: &[Written],
) -> Vec<(ValueId, usize)> {
	let mut found = Vec::new();
	visit_ops(ir, region, &mut |_, data| {
		let [result] = data.results.as_slice() else {
			return;
		};
		if let Some(key) = written_key(ir, row, *result, written, false) {
			found.push((*result, key));
		}
	});
	found
}

/// Whether the value of `value` may depend on the row `row` of the field region `region`.
///
/// A value the row cannot influence has one value for the group. Reading the row, or anything a
/// body the lowering cannot see may reach through it, is what the row influences: a field, the
/// record id, a parameter bound to the row being projected, a table or a mock (which generates
/// afresh), a user function, a method, a closure, a conditional, a statement, and the `@@` and
/// `<|..|>` operators. A builtin call is free of the row where its arguments are, unless it is a
/// projection or an index function, an aggregate, a writer, or it changes state the statement
/// outlives.
///
/// The values `keyed` are the group's keys the field reads: they have one value for the group, so
/// the row does not influence them.
fn reads_row(
	ir: &Ir,
	registry: &FunctionRegistry,
	region: RegionId,
	row: ValueId,
	value: ValueId,
	keyed: &[(ValueId, usize)],
	through_if: bool,
) -> bool {
	let mut inside = FxHashSet::default();
	visit_ops(ir, region, &mut |_, data| {
		inside.extend(data.results.iter().copied());
	});
	// A value already seen is not followed again, which is how the keys stop the walk.
	let mut seen: FxHashSet<ValueId> = keyed.iter().map(|(value, _)| *value).collect();
	depends(ir, registry, &inside, &mut seen, row, value, through_if)
}

fn depends(
	ir: &Ir,
	registry: &FunctionRegistry,
	inside: &FxHashSet<ValueId>,
	seen: &mut FxHashSet<ValueId>,
	row: ValueId,
	value: ValueId,
	through_if: bool,
) -> bool {
	if value == row {
		return true;
	}
	// A value defined outside the field's region is the same for every row of the group.
	if !inside.contains(&value) {
		return false;
	}
	if !seen.insert(value) {
		return false;
	}
	let Some(ValueDef::OpResult {
		op,
		..
	}) = ir.value(value).map(|data| data.def)
	else {
		return true;
	};
	let Some(data) = ir.op(op) else {
		return true;
	};
	let OpKind::Expr(kind) = &data.kind else {
		return true;
	};
	let operands_depend = |seen: &mut FxHashSet<ValueId>| {
		data.operands
			.iter()
			.any(|operand| depends(ir, registry, inside, seen, row, *operand, through_if))
	};
	match kind {
		ExprOp::Literal(_)
		| ExprOp::Constant {
			..
		}
		| ExprOp::SessionParam {
			..
		}
		| ExprOp::CatalogParam {
			..
		} => false,
		ExprOp::FreeParam {
			name,
		} => crate::exec::planner::util::SELECT_ITERATION_PARAMS.contains(&name.as_str()),
		ExprOp::Array
		| ExprOp::Set
		| ExprOp::Object {
			..
		}
		| ExprOp::RecordId(_)
		| ExprOp::Access {
			..
		}
		| ExprOp::First
		| ExprOp::Last
		| ExprOp::Each
		| ExprOp::Flatten {
			..
		}
		| ExprOp::Index
		| ExprOp::SetField {
			..
		}
		| ExprOp::Prefix(_)
		| ExprOp::Postfix {
			..
		}
		| ExprOp::Convert(_)
		| ExprOp::Truthy
		| ExprOp::Now {
			..
		} => operands_depend(seen),
		ExprOp::Binary {
			op,
		} => {
			!matches!(op, BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_))
				&& operands_depend(seen)
		}
		ExprOp::ShortCircuit {
			op,
		} => {
			if !matches!(op, ShortCircuitOp::And | ShortCircuitOp::Or) {
				return true;
			}
			operands_depend(seen)
				|| data.regions.iter().any(|region| {
					yielded(ir, *region).is_none_or(|held| {
						depends(ir, registry, inside, seen, row, held, through_if)
					})
				})
		}
		// A conditional is free of the row where its condition and both arms are, which a field
		// built from expression keys relies on; elsewhere it is read as the row's.
		ExprOp::If if through_if => {
			operands_depend(seen)
				|| data.regions.iter().any(|region| {
					yielded(ir, *region).is_none_or(|held| {
						depends(ir, registry, inside, seen, row, held, through_if)
					})
				})
		}
		ExprOp::Call(call) => match &call.function {
			Function::Normal(name) => {
				let name = name.as_str();
				let free = registry.get(name).is_some_and(|callee| !callee.mutates_state())
					&& !registry.is_projection(name)
					&& !registry.is_index_function(name)
					&& registry.get_aggregate(name).is_none()
					&& !crate::expr::method::is_writer_builtin(name);
				!free || operands_depend(seen)
			}
			Function::Custom(_)
			| Function::Script(_)
			| Function::Model(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => true,
		},
		ExprOp::Table {
			..
		}
		| ExprOp::Mock(_)
		| ExprOp::Field {
			..
		}
		| ExprOp::ComputedRead(_)
		| ExprOp::Doc
		| ExprOp::Deref {
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
		| ExprOp::TupleRow {
			..
		}
		| ExprOp::Invoke
		| ExprOp::ClosureCreate(_)
		| ExprOp::If
		| ExprOp::Throw
		| ExprOp::Subquery(_)
		| ExprOp::Yield
		| ExprOp::Agg {
			..
		}
		| ExprOp::Raise(_)
		| ExprOp::DepthCharge {
			..
		} => true,
	}
}

/// The one value the region `region` yields.
fn yielded(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let last = ops_in(ir, region).last()?;
	match ir.op(last).map(|data| (&data.kind, data.operands.as_slice())) {
		Some((OpKind::Expr(ExprOp::Yield), [value])) => Some(*value),
		_ => None,
	}
}

/// Plans the aggregate op `data`, whose attributes are `attrs`.
pub(super) fn plan(
	ir: &Ir,
	registry: &FunctionRegistry,
	data: &OpData,
	attrs: &AggregateAttrs,
) -> Result<Plan, IrError> {
	let (key_regions, field_regions) = data
		.regions
		.split_at_checked(usize::from(attrs.groups))
		.ok_or_else(|| invariant("an aggregate with fewer regions than keys"))?;
	let aliases = listed(&attrs.fields)?;
	if aliases.len() != field_regions.len() {
		return Err(invariant("an aggregate whose fields and regions differ"));
	}
	let mut named = attrs.names.iter();
	let mut paths = Vec::with_capacity(aliases.len());
	for (alias, _) in aliases.iter().zip(field_regions) {
		paths.push(match &attrs.fields {
			FieldsShape::Value {
				..
			} => vec![String::new()],
			FieldsShape::Select(_) => {
				let name = named
					.next()
					.or(*alias)
					.ok_or_else(|| invariant("an aggregate field with no name"))?;
				output_keys(name)
			}
		});
	}

	// The keys as written.
	let mut written = Vec::with_capacity(key_regions.len());
	let mut written_evals = Vec::with_capacity(key_regions.len());
	for region in key_regions {
		let row = row_arg(ir, *region).ok_or_else(|| invariant("a group key with no row"))?;
		let value = yielded_value(ir, *region)?;
		let eval = Eval {
			region: *region,
			row,
			value,
		};
		written.push(match path_of(ir, row, value) {
			PathForm::Plain(steps) => Written::Plain(steps),
			PathForm::Other | PathForm::Not => Written::Other(eval),
		});
		written_evals.push(eval);
	}

	// A field whose region is read as the column's own.
	let mut fields = Vec::with_capacity(field_regions.len());
	for (path, region) in paths.into_iter().zip(field_regions) {
		let row =
			row_arg(ir, *region).ok_or_else(|| invariant("an aggregate field with no row"))?;
		fields.push(Field {
			path,
			eval: Eval {
				region: *region,
				row,
				value: yielded_value(ir, *region)?,
			},
			column: Column::Constant,
		});
	}

	// A key written as the alias of a field is evaluated as that field's expression; where two
	// fields share an alias, the later one is the one the key names.
	let mut keys = Vec::with_capacity(written.len());
	for (index, eval) in written_evals.iter().enumerate() {
		let named = written[index].plain().and_then(plain_names);
		let aliased = named.and_then(|names| {
			aliases
				.iter()
				.zip(&fields)
				.rev()
				.find(|(alias, _)| alias.is_some_and(|alias| output_keys(alias) == names))
				.map(|(_, field)| field.eval)
		});
		keys.push(aliased.unwrap_or(*eval));
	}

	for (at, alias) in aliases.iter().enumerate() {
		let key = key_of(ir, &fields[at].eval, *alias, &written);
		let column = match key {
			Some(index) => Column::Key(index),
			None => {
				let field = &fields[at];
				let calls = calls_of(ir, field.eval.region)?;
				if attrs.first_value.contains(&at) {
					Column::First
				} else if !calls.is_empty() {
					let post = calls.len() != 1 || calls[0].result != field.eval.value;
					Column::Calls {
						calls,
						post,
					}
				} else {
					let keyed = key_reads(ir, field.eval.region, field.eval.row, &written);
					let over_expression_key =
						keyed.iter().any(|(_, key)| matches!(written[*key], Written::Other(_)));
					let (region, row, value) =
						(field.eval.region, field.eval.row, field.eval.value);
					if over_expression_key
						&& !reads_row(ir, registry, region, row, value, &keyed, true)
					{
						// Built from the group's keys alone: one value for the group, read from
						// the keys as an expression over the aggregate calls is.
						Column::Calls {
							calls: Vec::new(),
							post: true,
						}
					} else if reads_row(ir, registry, region, row, value, &[], false) {
						Column::Collect
					} else {
						Column::Constant
					}
				}
			}
		};
		fields[at].column = column;
	}
	Ok(Plan {
		keys,
		written,
		fields,
	})
}

/// The names of a path of plain field steps alone.
fn plain_names(steps: &[Step]) -> Option<Vec<String>> {
	steps
		.iter()
		.map(|step| match step {
			Step::Name(name) => Some(name.clone()),
			Step::Doc | Step::First | Step::Last | Step::Each | Step::Position(_) => None,
		})
		.collect()
}

/// The position of the key a field answers: its alias is the text of a key, or else it is a path a
/// written key is.
///
/// The alias is judged first because a key written as the alias of a field means that field (the
/// keys are evaluated that way), whatever else the name spells. In
/// `SELECT g1 AS g0, g2 AS g1 ... GROUP BY g0, g1` the path `g1` the first field reads is also the
/// text of the second key, but the first field is key 0.
fn key_of(ir: &Ir, eval: &Eval, alias: Option<&OutputPath>, written: &[Written]) -> Option<usize> {
	if let Some(alias) = alias {
		let alias = output_keys(alias);
		if let Some(index) = written
			.iter()
			.position(|key| key.plain().and_then(plain_names).is_some_and(|names| names == alias))
		{
			return Some(index);
		}
	}
	written_key(ir, eval.row, eval.value, written, true)
}

/// A value the host evaluates once for a group, over an object of placeholder fields.
struct DocRead<'p> {
	region: RegionId,
	row: ValueId,
	value: ValueId,
	/// The values that are read from the object, each under the name of its field.
	replaced: &'p [(ValueId, String)],
}

/// The host pipeline, of the program's, that emits the value `read` names over an object row.
fn doc_pipeline(
	em: &mut crate::exec::vm::ir_lower::emit::Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	needs: &mut Needs,
	read: &DocRead<'_>,
) -> Result<u16, IrError> {
	let mut lowerer = Lowerer::in_context(em, ir, cx);
	let aside = lowerer.emitter().set_aside();
	let beside = lowerer.beside();
	let built = (|| {
		lowerer.bind_row(read.row, true);
		for (value, name) in read.replaced {
			let field = lowerer.emitter().field(name)?;
			lowerer.bind_arg(*value, Operand::Field(field));
		}
		let value = lowerer.region_operand(read.region, read.value)?;
		lowerer.emit_value_pipeline(value)
	})();
	lowerer.rejoin(beside);
	lowerer.emitter().restore(aside);
	*needs = needs.merged(&lowerer.needs());
	lowerer.emitter().host_code(built?)
}

/// The host pipeline, of the program's, that runs the region `region` of an aggregate call that
/// raises in place of yielding, over an object row: the row is `row`, and the values `replaced`
/// names are read from the object under their fields.
fn raise_pipeline(
	em: &mut crate::exec::vm::ir_lower::emit::Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	needs: &mut Needs,
	region: RegionId,
	row: ValueId,
	replaced: &[(ValueId, String)],
) -> Result<u16, IrError> {
	let mut lowerer = Lowerer::in_context(em, ir, cx);
	let aside = lowerer.emitter().set_aside();
	let beside = lowerer.beside();
	let built = (|| {
		lowerer.bind_row(row, true);
		for (value, name) in replaced {
			let field = lowerer.emitter().field(name)?;
			lowerer.bind_arg(*value, Operand::Field(field));
		}
		let value = lowerer.region_value(region)?;
		lowerer.emit_value_pipeline(value)
	})();
	lowerer.rejoin(beside);
	lowerer.emitter().restore(aside);
	*needs = needs.merged(&lowerer.needs());
	lowerer.emitter().host_code(built?)
}

/// The host pipeline that emits the value `value` of the field region `region` once, over no row.
fn scalar_pipeline(
	em: &mut crate::exec::vm::ir_lower::emit::Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	needs: &mut Needs,
	value: ValueId,
) -> Result<u16, IrError> {
	let mut lowerer = Lowerer::in_context(em, ir, cx);
	let code = lowerer.host_code(value);
	*needs = needs.merged(&lowerer.needs());
	code
}

/// The sink of the aggregate `plan` stands for: its fields, and the host pipelines they finish
/// with, built into the program of `em`.
pub(super) fn sink_spec(
	em: &mut crate::exec::vm::ir_lower::emit::Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	plan: &Plan,
	attrs: &AggregateAttrs,
	needs: &mut Needs,
) -> Result<AggregateSink, IrError> {
	let mut specs = Vec::with_capacity(plan.fields.len());
	for field in &plan.fields {
		let mut spec = AggSpec {
			output_path: field.path.clone(),
			is_group_key: false,
			group_key_index: None,
			aggregate: None,
			fallback_is_constant: false,
			finish: None,
		};
		match &field.column {
			Column::Key(index) => {
				spec.is_group_key = true;
				spec.group_key_index = Some(*index);
			}
			Column::Calls {
				calls,
				post,
			} => {
				let mut held = Vec::with_capacity(calls.len());
				for call in calls {
					let id = AggregateId::of_call(call.kind, call.has_argument());
					registered(&id, cx.registry())?;
					let replaced: Vec<(ValueId, String)> =
						key_reads(ir, call.region, field.eval.row, &plan.written)
							.into_iter()
							.map(|(value, key)| (value, group_key_field_name(key)))
							.collect();
					let mut extra_args = Vec::new();
					if call.raises_after_first {
						extra_args.push(raise_pipeline(
							em,
							cx,
							ir,
							needs,
							call.region,
							field.eval.row,
							&replaced,
						)?);
					}
					for arg in call.args.iter().skip(1) {
						extra_args.push(doc_pipeline(
							em,
							cx,
							ir,
							needs,
							&DocRead {
								region: call.region,
								row: field.eval.row,
								value: *arg,
								replaced: &replaced,
							},
						)?);
					}
					held.push(AggCall {
						name: id,
						extra_args,
					});
				}
				let post = match post {
					true => {
						let mut replaced: Vec<(ValueId, String)> = calls
							.iter()
							.enumerate()
							.map(|(at, call)| (call.result, aggregate_field_name(at)))
							.collect();
						replaced.extend(
							key_reads(ir, field.eval.region, field.eval.row, &plan.written)
								.into_iter()
								.map(|(value, key)| (value, group_key_field_name(key))),
						);
						Some(doc_pipeline(
							em,
							cx,
							ir,
							needs,
							&DocRead {
								region: field.eval.region,
								row: field.eval.row,
								value: field.eval.value,
								replaced: &replaced,
							},
						)?)
					}
					false => None,
				};
				spec.aggregate = Some(AggExpr {
					calls: held,
					post,
				});
			}
			Column::Collect => {
				spec.aggregate = Some(AggExpr {
					calls: vec![AggCall {
						name: AggregateId::ImplicitCollect,
						extra_args: Vec::new(),
					}],
					post: None,
				});
			}
			Column::Constant => {
				spec.fallback_is_constant = true;
				// The one group of `GROUP ALL` answers it as the group finishes.
				if !plan.grouped() {
					spec.finish = Some(scalar_pipeline(em, cx, ir, needs, field.eval.value)?);
				}
			}
			Column::First => {}
		}
		specs.push(spec);
	}
	Ok(AggregateSink {
		fields: specs,
		keys: plan.keys.len(),
		held: attrs.spill,
	})
}

impl Body<'_, '_, '_> {
	/// The value `eval` names, read from the row of the body.
	fn evaluated(&mut self, eval: Eval) -> Result<Operand, IrError> {
		let ir = self.ir;
		let (objects, schema) = (self.objects, self.schema.clone());
		let mut lowerer = Lowerer::in_context(&mut *self.em, ir, self.cx).with_schema(schema);
		lowerer.bind_arg(eval.row, Operand::Reg(ROW));
		lowerer.bind_row(eval.row, objects);
		let value = lowerer.region_operand(eval.region, eval.value)?;
		self.claims.extend(lowerer.take_claims());
		self.needs = self.needs.merged(&lowerer.needs());
		Ok(value)
	}

	/// Evaluates the region `region`, which raises in place of yielding, over the row `row`: the
	/// operand is a register nothing reads, since control does not reach past the raise.
	fn raised(&mut self, region: RegionId, row: ValueId) -> Result<Operand, IrError> {
		let ir = self.ir;
		let (objects, schema) = (self.objects, self.schema.clone());
		let mut lowerer = Lowerer::in_context(&mut *self.em, ir, self.cx).with_schema(schema);
		lowerer.bind_arg(row, Operand::Reg(ROW));
		lowerer.bind_row(row, objects);
		let value = lowerer.region_value(region)?;
		self.claims.extend(lowerer.take_claims());
		self.needs = self.needs.merged(&lowerer.needs());
		Ok(value)
	}

	/// The value `eval` names, read as `NONE` where evaluating it fails only about the shape of the
	/// data, as the aggregate operator evaluates a key or an argument.
	fn evaluated_or_none(&mut self, eval: Eval) -> Result<Operand, IrError> {
		let catch = self.em.label()?;
		let begin = self.em.emit(Instr::TryNone {
			catch,
			dst: None,
		});
		let value = self.evaluated(eval)?;
		// An operand the body reads where it stands has nothing to fail.
		if self.em.here() == begin + 1 {
			self.em.code_mut().pop();
			return Ok(value);
		}
		let dst = self.em.reg()?;
		self.em.emit(Instr::Load {
			dst,
			src: value,
		});
		self.em.emit(Instr::TryEnd);
		self.em.bind(catch)?;
		self.em.code_mut()[begin as usize] = Instr::TryNone {
			catch,
			dst: Some(dst),
		};
		Ok(Operand::Reg(dst))
	}

	/// Gives column `field` of the row's group in aggregate `id` the value of `eval` while it needs
	/// one; an error about the shape of the data leaves the column as it was.
	fn group_first(&mut self, id: u16, field: u16, eval: Eval) -> Result<(), IrError> {
		let done = self.em.label()?;
		self.em.emit(Instr::GroupFirstNeeded {
			aggregate: id,
			field,
			done,
		});
		let catch = self.em.label()?;
		let begin = self.em.emit(Instr::TryNone {
			catch,
			dst: None,
		});
		let operand = self.evaluated(eval)?;
		let guarded = self.em.here() != begin + 1;
		if guarded {
			self.em.emit(Instr::TryEnd);
		} else {
			self.em.code_mut().pop();
		}
		self.em.emit(Instr::GroupFirstSet {
			aggregate: id,
			field,
			src: operand,
		});
		self.em.bind(done)?;
		if guarded {
			self.em.bind(catch)?;
		}
		Ok(())
	}

	/// The end of the body for a row folded into the aggregate `id` of `plan`: its keys, its group,
	/// then each field's arguments into their accumulators.
	pub(super) fn fold_into(&mut self, id: u16, plan: &Plan) -> Result<(), IrError> {
		// The operator evaluates a row's keys, then every call's argument, before it folds the row
		// into its group field by field.
		for (index, key) in plan.keys.iter().enumerate() {
			let value = self.evaluated_or_none(*key)?;
			let index = u16::try_from(index).map_err(|_| IrError::Capacity("group keys"))?;
			self.em.emit(Instr::GroupKey {
				aggregate: id,
				index,
				value,
			});
		}
		let mut arguments = Vec::new();
		for (at, field) in plan.fields.iter().enumerate() {
			match &field.column {
				Column::Calls {
					calls,
					..
				} => {
					for (call_at, call) in calls.iter().enumerate() {
						let value = match call.args.first() {
							// An argument that raises raises for the row, as the operator
							// evaluating it does; nothing reads the value it stands for.
							_ if call.raises => self.raised(call.region, field.eval.row)?,
							Some(first) => self.evaluated_or_none(Eval {
								region: call.region,
								row: field.eval.row,
								value: *first,
							})?,
							// A call with no argument folds `NONE`, as `count()` counts the row.
							None => Operand::Const(self.em.constant(Value::None)?),
						};
						arguments.push((at, call_at, value));
					}
				}
				Column::Collect => {
					let value = self.evaluated_or_none(field.eval)?;
					arguments.push((at, 0, value));
				}
				Column::Key(_) | Column::Constant | Column::First => {}
			}
		}
		if plan.grouped() {
			self.em.emit(Instr::GroupSelect {
				aggregate: id,
			});
		}
		for (at, field) in plan.fields.iter().enumerate() {
			let index = u16::try_from(at).map_err(|_| IrError::Capacity("aggregate fields"))?;
			match &field.column {
				Column::Key(_) => {}
				Column::Calls {
					..
				}
				| Column::Collect => {
					for (_, call_at, value) in arguments.iter().filter(|(held, ..)| *held == at) {
						let agg =
							u16::try_from(*call_at).map_err(|_| IrError::Capacity("aggregates"))?;
						self.em.emit(Instr::AccUpdate {
							aggregate: id,
							field: index,
							agg,
							value: *value,
						});
					}
				}
				// `GROUP ALL`'s constant columns are evaluated once, as the group finishes; every
				// other column as the group takes rows.
				Column::Constant if !plan.grouped() => {}
				Column::Constant | Column::First => self.group_first(id, index, field.eval)?,
			}
		}
		Ok(())
	}
}

/// Checks that the registry holds the aggregate function `id` names.
///
/// The binder makes an aggregate call only of a function the registry holds as an aggregate, and
/// names it by the name the registry gives it. A function the registry lacks is an internal error.
fn registered(id: &AggregateId, registry: &FunctionRegistry) -> Result<(), IrError> {
	match id.resolve(registry) {
		Some(_) => Ok(()),
		None => Err(IrError::Invariant {
			what: "ir_lower::pipe: an aggregate function the registry lacks",
		}),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::analysis::cx::builtin_registry;

	/// Every name the binder gives an aggregate call resolves, with an argument and without; a
	/// name no registry holds is the internal error.
	#[test]
	fn every_aggregate_the_binder_names_is_registered() {
		let registry = builtin_registry();
		assert!(registry.get_aggregate("count").is_some());
		for written in
			["count", "math::sum", "math::mean", "math::max", "math::min", "array::group"]
		{
			let Some(function) = registry.get_aggregate(written) else {
				continue;
			};
			for has_argument in [false, true] {
				let id = AggregateId::of_call(function.name(), has_argument);
				assert!(registered(&id, registry).is_ok(), "{written} ({has_argument})");
			}
		}
		assert!(matches!(
			registered(&AggregateId::of_call("no::such", true), registry),
			Err(IrError::Invariant {
				what: "ir_lower::pipe: an aggregate function the registry lacks"
			})
		));
	}

	/// The planner's analysis, which the binder runs before it binds an aggregate, refuses a
	/// grouped `*`, so an aggregate that lists one is the internal error.
	#[test]
	fn a_grouped_star_is_refused_before_any_aggregate_is_bound() {
		use crate::expr::logical::{LogicalField, LogicalFields};
		let refused = crate::exec::planner::analyse_aggregation(
			builtin_registry(),
			&LogicalFields::Select(vec![LogicalField::All]),
			&[],
		);
		assert!(refused.is_err_and(|error| error.to_string().contains("cannot be aggregated")));
		assert!(matches!(
			listed(&FieldsShape::Select(Box::new([FieldShape::All]))),
			Err(IrError::Invariant {
				what: "ir_lower::pipe: an aggregate over the whole row"
			})
		));
	}
}
