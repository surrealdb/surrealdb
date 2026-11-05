//! The expression dialect's own verifier rules, added to the generic rules of the container.
//!
//! Each rule answers [`VerifyError`] with [`VerifyRule::PerOp`] and a `detail` that starts with
//! the rule's id. An op outside the dialect answers `Ok`. A rule that looks outward walks the
//! enclosing regions of the op once, so its cost is the nesting depth of the op, and it stops at
//! the first region that is isolated from above, because nothing outside that region is visible
//! from inside it.
//!
//! | Id | Rule |
//! |---|---|
//! | V-E1 | `now` reads the first block argument of the entry block of its nearest `Statement` or isolated region |
//! | V-E2 | `repeat_recurse` stands in the body of a `Recurse` loop, or is a step of a `FETCH` path |
//! | V-E3 | `prefix` never holds a cast |
//! | V-E4 | `method` and `invoke` follow their own dispatch, which has no other use |
//! | V-E5 | a destructure's `Rest` entry stands only last in its list |
//! | V-E6 | `session_param` does not stand in a `SubscriberFrame` |
//! | V-E7 | `depth_charge` removes at least one level |
//! | V-E8 | `literal` holds no closure value |
//! | V-E9 | `subquery` takes a strategy other than `Apply` only in the physical phase |
//! | V-E10 | `closure_create` takes one operand per captured name |
//! | V-E11 | `tuple_row` addresses only operands and regions it holds, and its one use is an element of an `array` |
//! | V-E12 | `call` passes row names only to a user-defined, module or silo function, a builtin projection function or an index function, no more than its operands, and holds a callee's facts only for a user-defined function in the physical phase |
//! | V-E13 | `closure_create` holds one body whose entry arguments are the timestamp, the parameters, one per operand, then the invoking row where it reads one |

#![deny(clippy::wildcard_enum_match_arm)]

use std::ops::Bound;

use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::verify::{VerifyError, VerifyRule};
use crate::exec::ir::expr::ops::{
	CallAttrs, ClosureAttrs, DestructureEntry, DispatchTarget, ExprOp, NestedStrategy,
	passes_row_names,
};
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::conversion::Phase;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{RegionKind, RegionProperties};
use crate::exec::ir::iface::statements::{Place, PlacePart};
use crate::exec::ir::logical::ops::{LogOp, LoopKind};
use crate::exec::ir::logical::verify::is_timestamp;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::expr::{Function, PrefixOperator};
use crate::val::{RecordIdKey, Value};

/// The outcome of one rule: the `detail` of the error it raises.
type Rule = Result<(), String>;

/// Checks the rules of the expression op `op` that the generic rules do not cover. The callee of
/// a call is judged against the registry of `cx`.
pub(crate) fn verify_op(ir: &Ir, op: OpId, cx: &EffectCx<'_>) -> Result<(), VerifyError> {
	let Some(data) = ir.op(op) else {
		return Ok(());
	};
	let OpKind::Expr(expr) = &data.kind else {
		return Ok(());
	};
	let outcome = match expr {
		ExprOp::Now {
			..
		} => now_reads_statement_timestamp(ir, op, data),
		ExprOp::RepeatRecurse => repeat_recurse_in_recurse(ir, op),
		ExprOp::Prefix(prefix) => prefix_is_not_cast(prefix),
		ExprOp::Dispatch(target) => dispatch_has_one_consumer(ir, data, target),
		ExprOp::Method {
			name,
		} => method_follows_dispatch(ir, data, name.as_str()),
		ExprOp::Invoke => invoke_follows_dispatch(ir, data),
		ExprOp::ClosureCreate(attrs) => closure_operands_follow_captures(attrs, data)
			.and_then(|()| closure_body_takes_entry_layout(ir, attrs, data)),
		ExprOp::Destructure {
			shape,
		} => rest_is_last(shape),
		ExprOp::SessionParam {
			..
		} => session_param_outside_subscriber(ir, op),
		ExprOp::DepthCharge {
			levels,
		} => depth_charge_levels(*levels),
		ExprOp::Literal(value) => literal_holds_no_closure(value),
		ExprOp::Subquery(strategy) => subquery_strategy_in_physical_phase(ir, strategy),
		ExprOp::TupleRow {
			places,
		} => tuple_row_is_a_row(ir, data, places),
		ExprOp::Call(call) => call_row_names_and_callee(ir, data, call, cx),
		ExprOp::Array
		| ExprOp::Set
		| ExprOp::Object {
			..
		}
		| ExprOp::RecordId(_)
		| ExprOp::FreeParam {
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
		| ExprOp::Optional
		| ExprOp::SetField {
			..
		}
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
		| ExprOp::Throw
		| ExprOp::Yield
		| ExprOp::Truthy
		| ExprOp::Agg {
			..
		}
		| ExprOp::Raise(_) => Ok(()),
	};
	outcome.map_err(|detail| VerifyError {
		op: Some(op),
		rule: VerifyRule::PerOp,
		detail,
	})
}

/// V-E11: every place of a tuple row is addressed, each of its subscripts is an operand that
/// follows the row's values, each of its filters is a region the op holds, and the row has one use,
/// an element of an `array`: a tuple row is never a value of its own.
fn tuple_row_is_a_row(ir: &Ir, data: &OpData, places: &[Place]) -> Rule {
	let values = places.len();
	for place in places {
		if place.parts.is_empty() {
			return Err("V-E11: a tuple row writes at an empty place".into());
		}
		for part in place.parts.iter() {
			match part {
				PlacePart::Dynamic(slot) | PlacePart::Index(slot) => {
					let slot = usize::from(*slot);
					if slot < values || slot >= data.operands.len() {
						return Err(format!(
							"V-E11: a subscript names operand {slot}, outside the {} operands that \
							 follow the {values} values",
							data.operands.len().saturating_sub(values)
						));
					}
				}
				PlacePart::Filter(slot) => {
					if usize::from(*slot) >= data.regions.len() {
						return Err(format!(
							"V-E11: a filter names region {slot} of an op that holds {}",
							data.regions.len()
						));
					}
				}
				PlacePart::All
				| PlacePart::Flatten
				| PlacePart::Last
				| PlacePart::First
				| PlacePart::Field(_)
				| PlacePart::Optional
				| PlacePart::Doc
				| PlacePart::RepeatRecurse
				| PlacePart::Unaddressed(..) => {}
			}
		}
	}
	let Some(result) = data.results.first().copied() else {
		return Err("V-E11: a tuple row has no result".into());
	};
	let uses = ir.uses(result);
	let in_array = matches!(
		uses,
		[only] if matches!(
			ir.op(only.user).map(|user| &user.kind),
			Some(OpKind::Expr(ExprOp::Array))
		)
	);
	if in_array {
		Ok(())
	} else {
		Err(format!(
			"V-E11: a tuple row has {} uses, and its one use is not an element of an array",
			uses.len()
		))
	}
}

/// The regions that enclose `op`, innermost first and ending with the top region. A dangling id
/// ends the walk.
fn enclosing_regions(ir: &Ir, op: OpId) -> impl Iterator<Item = (RegionId, RegionKind)> + '_ {
	let mut next = Some(ir.enclosing_region(op));
	std::iter::from_fn(move || {
		let region = next?;
		let kind = ir.region(region)?.kind;
		next = ir.parent_op(region).map(|owner| ir.enclosing_region(owner));
		Some((region, kind))
	})
}

/// V-E1: the operand of `now` is the first block argument of the entry block of the nearest
/// enclosing region that is a `Statement` or is isolated from above, which is where every such
/// region receives the statement timestamp.
fn now_reads_statement_timestamp(ir: &Ir, op: OpId, data: &OpData) -> Rule {
	let nearest = enclosing_regions(ir, op)
		.find(|(_, kind)| matches!(kind, RegionKind::Statement(_)) || kind.isolated_from_above());
	let Some((region, _)) = nearest else {
		return Err("V-E1: no Statement or isolated region encloses the clock read".into());
	};
	let entry = ir.region(region).and_then(|region| region.blocks.first().copied());
	let argument =
		data.operands.first().and_then(|operand| ir.value(*operand)).and_then(|value| match value
			.def
		{
			ValueDef::BlockArg {
				block,
				index,
			} => Some((block, index)),
			ValueDef::OpResult {
				..
			} => None,
		});
	match (argument, entry) {
		(Some((block, 0)), Some(entry)) if block == entry => Ok(()),
		_ => Err("V-E1: the clock read's operand is not the first block argument of the entry \
		          block of its Statement or isolated region"
			.into()),
	}
}

/// V-E2: the nearest loop that encloses `op`, with no isolated region between them, must be a
/// `Recurse` loop. Logical and physical loops both count, because a physical loop holds the same
/// body. A marker that stands directly in a `FETCH` path region is a step that path's walk does
/// not go through, and needs no loop.
fn repeat_recurse_in_recurse(ir: &Ir, op: OpId) -> Rule {
	if is_fetch_path(ir, ir.enclosing_region(op)) {
		return Ok(());
	}
	for (region, kind) in enclosing_regions(ir, op) {
		let owner = ir.parent_op(region).and_then(|owner| ir.op(owner));
		if let Some(OpData {
			kind:
				OpKind::Logical(LogOp::Loop {
					kind: loop_kind,
					..
				})
				| OpKind::Phys(PhysOp::Loop {
					kind: loop_kind,
					..
				}),
			..
		}) = owner
		{
			return match loop_kind {
				LoopKind::Recurse => Ok(()),
				LoopKind::Select
				| LoopKind::Write
				| LoopKind::For
				| LoopKind::Closure
				| LoopKind::ElementFilter
				| LoopKind::GraphStep
				| LoopKind::Broadcast => Err(format!(
					"V-E2: the nearest enclosing loop of the recursion marker is {loop_kind:?}, \
					 not Recurse"
				)),
			};
		}
		if kind.isolated_from_above() {
			break;
		}
	}
	Err("V-E2: the recursion marker stands outside the body of a Recurse loop".into())
}

/// Whether `region` is a `FETCH` path: a region of a fetch, or one of the trailing regions of a
/// subscriber its attributes count as fetch paths.
fn is_fetch_path(ir: &Ir, region: RegionId) -> bool {
	let Some(owner) = ir.parent_op(region).and_then(|owner| ir.op(owner)) else {
		return false;
	};
	let fetch = match &owner.kind {
		OpKind::Logical(LogOp::Fetch) | OpKind::Phys(PhysOp::Fetch) => return true,
		OpKind::Logical(LogOp::Subscriber(attrs)) | OpKind::Phys(PhysOp::Subscriber(attrs)) => {
			usize::from(attrs.fetch)
		}
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => return false,
	};
	owner
		.regions
		.iter()
		.rposition(|held| *held == region)
		.is_some_and(|at| at + fetch >= owner.regions.len())
}

/// V-E3: a cast is a `convert`, never a prefix operator.
fn prefix_is_not_cast(prefix: &PrefixOperator) -> Rule {
	match prefix {
		PrefixOperator::Cast(_) => {
			Err("V-E3: a prefix op holds a cast; a cast is a convert".into())
		}
		PrefixOperator::Not
		| PrefixOperator::Positive
		| PrefixOperator::Negate
		| PrefixOperator::Range
		| PrefixOperator::RangeInclusive => Ok(()),
	}
}

/// V-E4, dispatch side: the result of a dispatch has no use, or exactly one, which is operand 0
/// of the `method` or `invoke` that names the same target.
fn dispatch_has_one_consumer(ir: &Ir, data: &OpData, target: &DispatchTarget) -> Rule {
	let Some(result) = data.results.first().copied() else {
		return Ok(());
	};
	let uses = ir.uses(result);
	let Some(only) = uses.first() else {
		return Ok(());
	};
	if uses.len() != 1 {
		return Err(format!("V-E4: the dispatch result has {} uses, not one", uses.len()));
	}
	let consumes = only.operand == 0
		&& match ir.op(only.user).map(|user| &user.kind) {
			Some(OpKind::Expr(ExprOp::Method {
				name,
			})) => matches!(target, DispatchTarget::Method(wanted) if wanted == name),
			Some(OpKind::Expr(ExprOp::Invoke)) => matches!(target, DispatchTarget::Invoke),
			Some(OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_)) | None => false,
		};
	if consumes {
		Ok(())
	} else {
		Err("V-E4: the dispatch result is used by something other than the method or invoke \
		     that names it as operand 0"
			.into())
	}
}

/// V-E4, method side: operand 0 is the result of a `Method(name)` dispatch with one use, and
/// operand 1 is that dispatch's operand.
fn method_follows_dispatch(ir: &Ir, data: &OpData, name: &str) -> Rule {
	let Some((dispatch, checked)) = dispatch_of(ir, data) else {
		return Err("V-E4: a method's operand 0 is not the result of a dispatch".into());
	};
	match dispatch_target(dispatch) {
		Some(DispatchTarget::Method(wanted)) if wanted.as_str() == name => {}
		Some(DispatchTarget::Method(_) | DispatchTarget::Invoke) | None => {
			return Err(format!("V-E4: a method's dispatch does not name the method `{name}`"));
		}
	}
	if data.operands.get(1) != dispatch.operands.first() {
		return Err("V-E4: a method's operand 1 is not the base its dispatch checked".into());
	}
	one_use(ir, checked)
}

/// V-E4, invoke side: operand 0 is the result of an `Invoke` dispatch with one use.
fn invoke_follows_dispatch(ir: &Ir, data: &OpData) -> Rule {
	let Some((dispatch, checked)) = dispatch_of(ir, data) else {
		return Err("V-E4: an invoke's operand 0 is not the result of a dispatch".into());
	};
	match dispatch_target(dispatch) {
		Some(DispatchTarget::Invoke) => one_use(ir, checked),
		Some(DispatchTarget::Method(_)) | None => {
			Err("V-E4: an invoke's dispatch does not name an invocation".into())
		}
	}
}

/// The dispatch op that defines operand 0 of `data`, and that operand.
fn dispatch_of<'a>(ir: &'a Ir, data: &OpData) -> Option<(&'a OpData, ValueId)> {
	let operand = data.operands.first().copied()?;
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(operand)?.def
	else {
		return None;
	};
	let dispatch = ir.op(op)?;
	match &dispatch.kind {
		OpKind::Expr(ExprOp::Dispatch(_)) => Some((dispatch, operand)),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

fn dispatch_target(dispatch: &OpData) -> Option<&DispatchTarget> {
	match &dispatch.kind {
		OpKind::Expr(ExprOp::Dispatch(target)) => Some(target),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

fn one_use(ir: &Ir, dispatch_result: ValueId) -> Rule {
	match ir.uses(dispatch_result).len() {
		1 => Ok(()),
		count => Err(format!("V-E4: the dispatch result has {count} uses, not one")),
	}
}

/// V-E10: a closure creation's operands are the values it captures, one per name in `captures` in
/// order.
fn closure_operands_follow_captures(attrs: &ClosureAttrs, data: &OpData) -> Rule {
	let names = attrs.captures.len();
	if data.operands.len() == names {
		Ok(())
	} else {
		Err(format!(
			"V-E10: a closure creation names {names} captures and takes {} operands",
			data.operands.len()
		))
	}
}

/// V-E13: a closure creation holds one body, whose entry block takes the statement timestamp first,
/// then one argument per parameter, then one per operand of the creation, then the invoking row
/// where the creation states that its body reads one.
fn closure_body_takes_entry_layout(ir: &Ir, attrs: &ClosureAttrs, data: &OpData) -> Rule {
	let [region] = data.regions.as_slice() else {
		return Err(format!(
			"V-E13: a closure creation holds {} regions, not one body",
			data.regions.len()
		));
	};
	let entry = ir
		.region(*region)
		.and_then(|region| region.blocks.first().copied())
		.and_then(|block| ir.block(block));
	let Some(entry) = entry else {
		return Err("V-E13: a closure body holds no entry block".into());
	};
	let wanted = 1
		+ attrs.params.len()
		+ data.operands.len()
		+ usize::from(attrs.reads_row)
		+ usize::from(attrs.reads_root);
	if entry.args.len() != wanted {
		return Err(format!(
			"V-E13: a closure body takes {} entry arguments, not the timestamp, {} parameters, \
			 {} operands and {} rows",
			entry.args.len(),
			attrs.params.len(),
			data.operands.len(),
			usize::from(attrs.reads_row)
		));
	}
	if !entry.args.first().is_some_and(|first| is_timestamp(ir, *first)) {
		return Err("V-E13: the first entry argument of a closure body is not the timestamp".into());
	}
	Ok(())
}

/// V-E5: in every entry list of the pattern, nested lists included, `Rest` is the last entry.
fn rest_is_last(entries: &[DestructureEntry]) -> Rule {
	for (position, entry) in entries.iter().enumerate() {
		match entry {
			DestructureEntry::Rest if position + 1 != entries.len() => {
				return Err("V-E5: a destructure's Rest entry is not last in its list".into());
			}
			DestructureEntry::Destructure(_, nested) => rest_is_last(nested)?,
			DestructureEntry::Rest
			| DestructureEntry::All(_)
			| DestructureEntry::Field(_)
			| DestructureEntry::Aliased(_) => {}
		}
	}
	Ok(())
}

/// V-E6: the nearest isolated region around `op` is not a `SubscriberFrame`, where the session
/// names are block arguments.
fn session_param_outside_subscriber(ir: &Ir, op: OpId) -> Rule {
	let isolated = enclosing_regions(ir, op).find(|(_, kind)| kind.isolated_from_above());
	match isolated {
		Some((_, RegionKind::SubscriberFrame)) => {
			Err("V-E6: a session name is read inside a SubscriberFrame, which binds it as a \
			     block argument"
				.into())
		}
		Some(_) | None => Ok(()),
	}
}

/// V-E7: a depth charge removes at least one level.
fn depth_charge_levels(levels: u32) -> Rule {
	if levels >= 1 {
		Ok(())
	} else {
		Err("V-E7: a depth charge removes no level".into())
	}
}

/// V-E9: a subquery in value position takes its strategy from the facts of the physical plan. A
/// strategy other than `Apply` may appear only in a container that is in the physical phase.
fn subquery_strategy_in_physical_phase(ir: &Ir, strategy: &NestedStrategy) -> Rule {
	if matches!(strategy, NestedStrategy::Apply) || ir.phase() >= Phase::Physical {
		Ok(())
	} else {
		Err("V-E9: a subquery holds a strategy other than Apply before the physical phase".into())
	}
}

/// V-E12: a call passes no more row names than it has operands, and only a function that reads the
/// row may receive them. A callee's facts belong to a user-defined function and are
/// read only in the physical phase.
fn call_row_names_and_callee(ir: &Ir, data: &OpData, call: &CallAttrs, cx: &EffectCx<'_>) -> Rule {
	let takes_row = passes_row_names(&call.function, cx.registry);
	if call.row_names > 0 && !takes_row {
		return Err("V-E12: a call passes row names to a function whose body reads no row".into());
	}
	if usize::from(call.row_names) > data.operands.len() {
		return Err("V-E12: a call passes more row names than it has operands".into());
	}
	if call.callee.is_some()
		&& (!matches!(call.function, Function::Custom(_)) || ir.phase() < Phase::Physical)
	{
		return Err(
			"V-E12: a call holds a callee's facts for a function that is not user-defined or before the physical phase"
				.into(),
		);
	}
	Ok(())
}

/// V-E8: a literal holds any value except a closure, at any depth, because a closure's captures
/// have no literal form.
fn literal_holds_no_closure(value: &Value) -> Rule {
	if holds_closure(value) {
		Err("V-E8: a literal holds a closure value".into())
	} else {
		Ok(())
	}
}

/// Whether `value` is a closure or holds one at any depth of its elements, range bounds or
/// record id key.
fn holds_closure(value: &Value) -> bool {
	match value {
		Value::Closure(_) => true,
		Value::Array(items) => items.iter().any(holds_closure),
		Value::Set(items) => items.iter().any(holds_closure),
		Value::Object(object) => object.values().any(holds_closure),
		Value::Range(range) => bound_holds_closure(&range.start) || bound_holds_closure(&range.end),
		Value::RecordId(id) => key_holds_closure(&id.key),
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Regex(_) => false,
	}
}

fn bound_holds_closure(bound: &Bound<Value>) -> bool {
	match bound {
		Bound::Included(value) | Bound::Excluded(value) => holds_closure(value),
		Bound::Unbounded => false,
	}
}

fn key_holds_closure(key: &RecordIdKey) -> bool {
	fn bound(bound: &Bound<RecordIdKey>) -> bool {
		match bound {
			Bound::Included(key) | Bound::Excluded(key) => key_holds_closure(key),
			Bound::Unbounded => false,
		}
	}
	match key {
		RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_) => false,
		RecordIdKey::Array(items) => items.iter().any(holds_closure),
		RecordIdKey::Object(object) => object.values().any(holds_closure),
		RecordIdKey::Range(range) => bound(&range.start) || bound(&range.end),
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use smallvec::SmallVec;
	use surrealdb_strand::{Strand, TableName};

	use super::verify_op;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::core::ir::{Ir, IrScope};
	use crate::exec::ir::core::location::{LocOrigin, Location};
	use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
	use crate::exec::ir::core::value::ValueDef;
	use crate::exec::ir::core::verify::{VerifyError, VerifyRule};
	use crate::exec::ir::expr::ops::{
		CallAttrs, CalleeFacts, ClosureAttrs, DestructureEntry, DispatchTarget, ExprOp,
		NestedStrategy, NowWhich, ShortCircuitOp,
	};
	use crate::exec::ir::iface::conversion::Phase;
	use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, RegionId, SnapshotId, ValueId};
	use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind, StatementCommit};
	use crate::exec::ir::iface::statements::{Place, PlacePart};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::{LogOp, LoopKind, LoopSink, SourceOrder};
	use crate::exec::ir::logical::verify::timestamp_arg;
	use crate::exec::ir::op_kind::OpKind;
	use crate::exec::ir::phys::ops::{Batch, PhysOp};
	use crate::expr::{Function, Kind, PrefixOperator};
	use crate::val::{Number, Range, RecordId, RecordIdKey, Value};

	/// A container under construction through its rewriter.
	struct Fixture {
		ir: Ir,
		facts: FactCache,
	}

	impl Fixture {
		fn new() -> Self {
			Self {
				ir: Ir::new(
					IrScope::Request {
						fragment: None,
					},
					RegionKind::Request,
				),
				facts: FactCache::new(),
			}
		}

		fn top_block(&self) -> BlockId {
			self.ir
				.region(self.ir.top())
				.and_then(|region| region.blocks.first().copied())
				.expect("the top region holds one block")
		}

		/// Appends an op with `results` results to `block`.
		fn add(
			&mut self,
			block: BlockId,
			kind: OpKind,
			operands: &[ValueId],
			results: usize,
		) -> OpId {
			Rewriter::new(&mut self.ir, &mut self.facts)
				.create(
					InsertPoint::End(block),
					OpSpec {
						kind,
						operands: SmallVec::from_slice(operands),
						results: vec![TypeKnowledge::Unknown; results].into(),
						frame: FrameId::new(0),
						snapshot: SnapshotId::new(0),
						loc: Location {
							stmt: 0,
							span: None,
							origin: LocOrigin::Query,
						},
					},
				)
				.expect("the op is created")
		}

		/// Appends an expression op with one result.
		fn expr(&mut self, block: BlockId, op: ExprOp, operands: &[ValueId]) -> OpId {
			self.add(block, OpKind::Expr(op), operands, 1)
		}

		/// Appends an op of the logical dialect with no result.
		fn logical(&mut self, block: BlockId, op: LogOp) -> OpId {
			self.add(block, OpKind::Logical(op), &[], 0)
		}

		/// A region of `kind` under `owner`, with `args` block arguments.
		fn region(&mut self, owner: OpId, kind: RegionKind, args: usize) -> (RegionId, BlockId) {
			Rewriter::new(&mut self.ir, &mut self.facts)
				.create_region(owner, kind, &vec![TypeKnowledge::Unknown; args])
				.expect("the region is created")
		}

		fn result(&self, op: OpId) -> ValueId {
			self.ir.op(op).and_then(|data| data.results.first().copied()).expect("a result")
		}

		fn arg(&self, block: BlockId, index: usize) -> ValueId {
			self.ir
				.block(block)
				.and_then(|data| data.args.get(index).copied())
				.expect("a block argument")
		}

		/// The op that defines `value`.
		fn definition(&self, value: ValueId) -> OpId {
			match self.ir.value(value).map(|data| data.def) {
				Some(ValueDef::OpResult {
					op,
					..
				}) => op,
				Some(ValueDef::BlockArg {
					..
				})
				| None => panic!("the value is an op result"),
			}
		}

		/// A statement region under the top block: its block, whose argument is the timestamp.
		fn statement(&mut self) -> BlockId {
			let top = self.top_block();
			let owner = self.logical(
				top,
				LogOp::Stmt {
					commit: StatementCommit::Auto,
					control: None,
				},
			);
			self.region(owner, RegionKind::Statement(StatementCommit::Auto), 1).1
		}

		/// A literal in `block`.
		fn literal(&mut self, block: BlockId) -> ValueId {
			let op = self.expr(block, ExprOp::Literal(Box::new(int(1))), &[]);
			self.result(op)
		}

		/// A closure value in `block` and its body block.
		fn closure_body(&mut self, block: BlockId) -> BlockId {
			let op = self.expr(
				block,
				ExprOp::ClosureCreate(Box::new(ClosureAttrs {
					params: Box::new([]),
					returns: None,
					body_sql: "1".into(),
					captures: Box::new([]),
					reads_row: false,
					reads_root: false,
					literal: Default::default(),
				})),
				&[],
			);
			self.region(op, RegionKind::ClosureBody, 0).1
		}

		/// A closure creation in `block` over `operands` whose body takes the entry arguments
		/// `entry`.
		fn closure_with_entry(
			&mut self,
			block: BlockId,
			params: &[&str],
			captures: &[&str],
			operands: &[ValueId],
			entry: &[TypeKnowledge],
		) -> OpId {
			let op = self.expr(
				block,
				ExprOp::ClosureCreate(Box::new(ClosureAttrs {
					params: params.iter().map(|name| (strand(name), Kind::Any)).collect(),
					returns: None,
					body_sql: "1".into(),
					captures: captures.iter().map(|name| strand(name)).collect(),
					reads_row: false,
					reads_root: false,
					literal: Default::default(),
				})),
				operands,
			);
			Rewriter::new(&mut self.ir, &mut self.facts)
				.create_region(op, RegionKind::ClosureBody, entry)
				.expect("the region is created");
			op
		}

		/// A base and its dispatch of `target` in a fresh statement block: the block, the base
		/// and the dispatch result.
		fn dispatched(&mut self, target: DispatchTarget) -> (BlockId, ValueId, ValueId) {
			let block = self.statement();
			let base = self.literal(block);
			let dispatch = self.expr(block, ExprOp::Dispatch(Box::new(target)), &[base]);
			(block, base, self.result(dispatch))
		}

		/// Asserts that `op` passes every rule.
		fn passes(&self, op: OpId) {
			assert_eq!(
				verify_op(&self.ir, op, &crate::exec::ir::analysis::cx::default_cx(&self.ir)),
				Ok(())
			);
		}

		/// Asserts that `op` fails the rule `id`.
		fn fails(&self, op: OpId, id: &str) {
			let error: VerifyError =
				verify_op(&self.ir, op, &crate::exec::ir::analysis::cx::default_cx(&self.ir))
					.expect_err("the op breaks a rule");
			assert_eq!(error.rule, VerifyRule::PerOp);
			assert_eq!(error.op, Some(op));
			assert!(error.detail.starts_with(id), "{} does not start with {id}", error.detail);
		}
	}

	fn int(n: i64) -> Value {
		Value::Number(Number::Int(n))
	}

	fn strand(text: &str) -> Strand {
		Strand::new(text)
	}

	fn closure() -> Value {
		Value::Closure(Box::new(crate::val::Closure::Expr {
			args: Vec::new(),
			returns: None,
			body: crate::expr::Expr::Literal(crate::expr::Literal::None),
			captures: Default::default(),
		}))
	}

	fn loop_of(kind: LoopKind) -> LogOp {
		LogOp::Loop {
			kind,
			sink: LoopSink::Output,
			order: SourceOrder::Positional,
			step: None,
		}
	}

	fn phys_loop_of(kind: LoopKind) -> PhysOp {
		PhysOp::Loop {
			kind,
			first: false,
			batch: Batch {
				max_iterations: 1,
				max_bytes: 0,
			},
			unroll: 1,
			value: false,
			step: None,
		}
	}

	fn now() -> ExprOp {
		ExprOp::Now {
			which: NowWhich::Datetime,
		}
	}

	fn method(name: &str) -> ExprOp {
		ExprOp::Method {
			name: strand(name),
		}
	}

	fn session() -> ExprOp {
		ExprOp::SessionParam {
			name: strand("auth"),
		}
	}

	fn destructure(shape: Vec<DestructureEntry>) -> ExprOp {
		ExprOp::Destructure {
			shape: shape.into_boxed_slice(),
		}
	}

	#[test]
	fn now_passes_on_the_statement_block_argument() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let timestamp = fx.arg(block, 0);
		let read = fx.expr(block, now(), &[timestamp]);
		fx.passes(read);
	}

	#[test]
	fn now_passes_in_a_nested_region_of_its_statement() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let timestamp = fx.arg(block, 0);
		let owner = fx.logical(block, loop_of(LoopKind::Select));
		let (_, body) = fx.region(owner, RegionKind::Loop, 0);
		let read = fx.expr(body, now(), &[timestamp]);
		fx.passes(read);
	}

	#[test]
	fn now_fails_on_an_operand_that_is_not_a_block_argument() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let computed = fx.literal(block);
		let read = fx.expr(block, now(), &[computed]);
		fx.fails(read, "V-E1");
	}

	#[test]
	fn now_fails_on_the_argument_of_a_block_that_is_not_the_statement_entry() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let owner = fx.logical(block, loop_of(LoopKind::Select));
		let (_, body) = fx.region(owner, RegionKind::Loop, 1);
		let element = fx.arg(body, 0);
		let read = fx.expr(body, now(), &[element]);
		fx.fails(read, "V-E1");
	}

	#[test]
	fn now_fails_on_an_entry_argument_after_the_first() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let owner = fx.expr(
			block,
			ExprOp::ClosureCreate(Box::new(ClosureAttrs {
				params: Box::new([]),
				returns: None,
				body_sql: "1".into(),
				captures: Box::new([]),
				reads_row: false,
				reads_root: false,
				literal: Default::default(),
			})),
			&[],
		);
		let (_, body) = fx.region(owner, RegionKind::ClosureBody, 2);
		let parameter = fx.arg(body, 1);
		let read = fx.expr(body, now(), &[parameter]);
		fx.fails(read, "V-E1");
	}

	#[test]
	fn now_fails_outside_a_statement() {
		let mut fx = Fixture::new();
		let top = fx.top_block();
		let literal = fx.literal(top);
		let read = fx.expr(top, now(), &[literal]);
		fx.fails(read, "V-E1");
	}

	#[test]
	fn repeat_recurse_passes_in_a_recurse_body() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let owner = fx.logical(block, loop_of(LoopKind::Recurse));
		let (_, body) = fx.region(owner, RegionKind::Loop, 1);
		let value = fx.arg(body, 0);
		let marker = fx.expr(body, ExprOp::RepeatRecurse, &[value]);
		fx.passes(marker);
	}

	#[test]
	fn repeat_recurse_passes_in_a_lowered_recurse_body() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let owner = fx.add(block, OpKind::Phys(phys_loop_of(LoopKind::Recurse)), &[], 0);
		let (_, body) = fx.region(owner, RegionKind::Loop, 1);
		let value = fx.arg(body, 0);
		let marker = fx.expr(body, ExprOp::RepeatRecurse, &[value]);
		fx.passes(marker);
	}

	#[test]
	fn repeat_recurse_fails_in_the_body_of_another_lowered_loop() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let owner = fx.add(block, OpKind::Phys(phys_loop_of(LoopKind::For)), &[], 0);
		let (_, body) = fx.region(owner, RegionKind::Loop, 1);
		let value = fx.arg(body, 0);
		let marker = fx.expr(body, ExprOp::RepeatRecurse, &[value]);
		fx.fails(marker, "V-E2");
	}

	#[test]
	fn repeat_recurse_fails_in_the_body_of_another_loop() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let owner = fx.logical(block, loop_of(LoopKind::Broadcast));
		let (_, body) = fx.region(owner, RegionKind::Loop, 1);
		let value = fx.arg(body, 0);
		let marker = fx.expr(body, ExprOp::RepeatRecurse, &[value]);
		fx.fails(marker, "V-E2");
	}

	#[test]
	fn repeat_recurse_fails_across_an_isolated_region() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let owner = fx.logical(block, loop_of(LoopKind::Recurse));
		let (_, body) = fx.region(owner, RegionKind::Loop, 0);
		let inner = fx.closure_body(body);
		let value = fx.literal(inner);
		let marker = fx.expr(inner, ExprOp::RepeatRecurse, &[value]);
		fx.fails(marker, "V-E2");
	}

	#[test]
	fn repeat_recurse_passes_as_a_step_of_a_fetch_path() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let owner = fx.logical(block, LogOp::Fetch);
		let (_, path) = fx.region(owner, RegionKind::Clause(ClauseRole::Projection), 1);
		let row = fx.arg(path, 0);
		let marker = fx.expr(path, ExprOp::RepeatRecurse, &[row]);
		fx.passes(marker);
	}

	#[test]
	fn repeat_recurse_fails_in_a_loop_within_a_fetch_path() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let owner = fx.logical(block, LogOp::Fetch);
		let (_, path) = fx.region(owner, RegionKind::Clause(ClauseRole::Projection), 1);
		let each = fx.logical(path, loop_of(LoopKind::Broadcast));
		let (_, body) = fx.region(each, RegionKind::Loop, 1);
		let value = fx.arg(body, 0);
		let marker = fx.expr(body, ExprOp::RepeatRecurse, &[value]);
		fx.fails(marker, "V-E2");
	}

	#[test]
	fn repeat_recurse_fails_outside_any_loop() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let value = fx.literal(block);
		let marker = fx.expr(block, ExprOp::RepeatRecurse, &[value]);
		fx.fails(marker, "V-E2");
	}

	#[test]
	fn prefix_passes_on_a_unary_operator() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let value = fx.literal(block);
		let negate = fx.expr(block, ExprOp::Prefix(Box::new(PrefixOperator::Negate)), &[value]);
		fx.passes(negate);
	}

	#[test]
	fn prefix_fails_on_a_cast() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let value = fx.literal(block);
		let cast =
			fx.expr(block, ExprOp::Prefix(Box::new(PrefixOperator::Cast(Kind::Int))), &[value]);
		fx.fails(cast, "V-E3");
	}

	#[test]
	fn method_passes_after_its_own_dispatch() {
		let mut fx = Fixture::new();
		let (block, base, checked) = fx.dispatched(DispatchTarget::Method(strand("len")));
		let call = fx.expr(block, method("len"), &[checked, base]);
		fx.passes(call);
		fx.passes(fx.definition(checked));
	}

	#[test]
	fn method_fails_on_a_dispatch_of_another_name() {
		let mut fx = Fixture::new();
		let (block, base, checked) = fx.dispatched(DispatchTarget::Method(strand("len")));
		let call = fx.expr(block, method("first"), &[checked, base]);
		fx.fails(call, "V-E4");
	}

	#[test]
	fn method_fails_on_a_base_the_dispatch_did_not_check() {
		let mut fx = Fixture::new();
		let (block, _, checked) = fx.dispatched(DispatchTarget::Method(strand("len")));
		let other = fx.literal(block);
		let call = fx.expr(block, method("len"), &[checked, other]);
		fx.fails(call, "V-E4");
	}

	#[test]
	fn a_dispatch_with_a_second_use_fails_at_the_dispatch_and_at_both_users() {
		let mut fx = Fixture::new();
		let (block, base, checked) = fx.dispatched(DispatchTarget::Method(strand("len")));
		let first = fx.expr(block, method("len"), &[checked, base]);
		let second = fx.expr(block, method("len"), &[checked, base]);
		fx.fails(first, "V-E4");
		fx.fails(second, "V-E4");
		fx.fails(fx.definition(checked), "V-E4");
	}

	#[test]
	fn invoke_passes_after_an_invoke_dispatch() {
		let mut fx = Fixture::new();
		let (block, _, checked) = fx.dispatched(DispatchTarget::Invoke);
		let call = fx.expr(block, ExprOp::Invoke, &[checked]);
		fx.passes(call);
		fx.passes(fx.definition(checked));
	}

	#[test]
	fn invoke_fails_after_a_method_dispatch() {
		let mut fx = Fixture::new();
		let (block, _, checked) = fx.dispatched(DispatchTarget::Method(strand("len")));
		let call = fx.expr(block, ExprOp::Invoke, &[checked]);
		fx.fails(call, "V-E4");
	}

	#[test]
	fn invoke_fails_on_an_operand_no_dispatch_defines() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let value = fx.literal(block);
		let call = fx.expr(block, ExprOp::Invoke, &[value]);
		fx.fails(call, "V-E4");
	}

	#[test]
	fn destructure_passes_with_rest_last() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let base = fx.literal(block);
		let shape = vec![DestructureEntry::Field(strand("a")), DestructureEntry::Rest];
		let op = fx.expr(block, destructure(shape), &[base]);
		fx.passes(op);
	}

	#[test]
	fn destructure_fails_with_rest_before_another_entry() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let base = fx.literal(block);
		let shape = vec![DestructureEntry::Rest, DestructureEntry::Field(strand("a"))];
		let op = fx.expr(block, destructure(shape), &[base]);
		fx.fails(op, "V-E5");
	}

	#[test]
	fn destructure_checks_a_nested_entry_list() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let base = fx.literal(block);
		let nested = |inner: Vec<DestructureEntry>| {
			destructure(vec![DestructureEntry::Destructure(strand("a"), inner.into_boxed_slice())])
		};
		let ok = fx.expr(
			block,
			nested(vec![DestructureEntry::Field(strand("b")), DestructureEntry::Rest]),
			&[base],
		);
		fx.passes(ok);
		let bad = fx.expr(
			block,
			nested(vec![DestructureEntry::Rest, DestructureEntry::Field(strand("b"))]),
			&[base],
		);
		fx.fails(bad, "V-E5");
	}

	#[test]
	fn session_param_passes_in_a_statement() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let read = fx.expr(block, session(), &[]);
		fx.passes(read);
	}

	#[test]
	fn session_param_fails_in_a_subscriber_frame_and_in_a_region_nested_in_one() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let owner = fx.logical(block, loop_of(LoopKind::Select));
		let (_, frame) = fx.region(owner, RegionKind::SubscriberFrame, 0);
		let direct = fx.expr(frame, session(), &[]);
		fx.fails(direct, "V-E6");
		let left = fx.literal(frame);
		let lazy = fx.expr(
			frame,
			ExprOp::ShortCircuit {
				op: ShortCircuitOp::And,
			},
			&[left],
		);
		let (_, operand) = fx.region(lazy, RegionKind::LazyOperand, 0);
		let nested = fx.expr(operand, session(), &[]);
		fx.fails(nested, "V-E6");
	}

	#[test]
	fn session_param_in_a_closure_inside_a_subscriber_frame_passes() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let owner = fx.logical(block, loop_of(LoopKind::Select));
		let (_, frame) = fx.region(owner, RegionKind::SubscriberFrame, 0);
		let body = fx.closure_body(frame);
		let read = fx.expr(body, session(), &[]);
		fx.passes(read);
	}

	#[test]
	fn depth_charge_passes_with_a_level() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let charge = fx.expr(
			block,
			ExprOp::DepthCharge {
				levels: 1,
			},
			&[],
		);
		fx.passes(charge);
	}

	#[test]
	fn depth_charge_fails_with_no_level() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let charge = fx.expr(
			block,
			ExprOp::DepthCharge {
				levels: 0,
			},
			&[],
		);
		fx.fails(charge, "V-E7");
	}

	#[test]
	fn literal_passes_on_plain_data() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let plain = Value::Array(crate::val::Array(vec![int(1), Value::String(strand("a"))]));
		let op = fx.expr(block, ExprOp::Literal(Box::new(plain)), &[]);
		fx.passes(op);
	}

	#[test]
	fn literal_passes_on_a_table_a_record_id_and_a_bounded_range() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let table = TableName::new(strand("t"));
		let values = [
			Value::Table(table.clone()),
			Value::RecordId(RecordId {
				table,
				key: RecordIdKey::Number(1),
			}),
			Value::Range(Box::new(Range {
				start: Bound::Included(int(1)),
				end: Bound::Excluded(int(2)),
			})),
		];
		for value in values {
			let op = fx.expr(block, ExprOp::Literal(Box::new(value)), &[]);
			fx.passes(op);
		}
	}

	#[test]
	fn literal_fails_on_a_closure() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let op = fx.expr(block, ExprOp::Literal(Box::new(closure())), &[]);
		fx.fails(op, "V-E8");
	}

	#[test]
	fn literal_fails_on_a_closure_at_any_depth() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let in_array = Value::Array(crate::val::Array(vec![int(1), closure()]));
		let in_range = Value::Range(Box::new(Range {
			start: Bound::Included(int(1)),
			end: Bound::Excluded(closure()),
		}));
		let in_key = Value::RecordId(RecordId {
			table: TableName::new(strand("t")),
			key: RecordIdKey::Array(crate::val::Array(vec![closure()])),
		});
		for value in [in_array, in_range, in_key] {
			let op = fx.expr(block, ExprOp::Literal(Box::new(value)), &[]);
			fx.fails(op, "V-E8");
		}
	}

	#[test]
	fn subquery_strategy_stands_only_in_the_physical_phase() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let apply = fx.expr(block, ExprOp::Subquery(NestedStrategy::Apply), &[]);
		let once = fx.expr(block, ExprOp::Subquery(NestedStrategy::Once), &[]);
		fx.passes(apply);
		fx.fails(once, "V-E9");
		Rewriter::new(&mut fx.ir, &mut fx.facts)
			.advance_phase(Phase::Physical)
			.expect("the phase advances");
		fx.passes(apply);
		fx.passes(once);
	}

	#[test]
	fn a_call_passes_row_names_only_to_a_body_that_reads_the_row() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let row = fx.literal(block);
		let call = |function: Function, row_names: u8, callee: Option<CalleeFacts>| {
			ExprOp::Call(Box::new(CallAttrs {
				function,
				row_names,
				callee,
			}))
		};
		let custom = || Function::Custom("f".to_owned());
		let builtin = || Function::Normal("math::abs".to_owned());
		let passed = fx.expr(block, call(custom(), 1, None), &[row]);
		let to_builtin = fx.expr(block, call(builtin(), 1, None), &[row]);
		let to_projection =
			fx.expr(block, call(Function::Normal("type::field".to_owned()), 1, None), &[row]);
		let too_many = fx.expr(block, call(custom(), 2, None), &[row]);
		fx.passes(passed);
		fx.fails(to_builtin, "V-E12");
		fx.passes(to_projection);
		fx.fails(too_many, "V-E12");
		let facts = || Some(CalleeFacts::Absent);
		let custom_facts = fx.expr(block, call(custom(), 0, facts()), &[]);
		let builtin_facts = fx.expr(block, call(builtin(), 0, facts()), &[]);
		fx.fails(custom_facts, "V-E12");
		Rewriter::new(&mut fx.ir, &mut fx.facts)
			.advance_phase(Phase::Physical)
			.expect("the phase advances");
		fx.passes(custom_facts);
		fx.fails(builtin_facts, "V-E12");
	}

	#[test]
	fn a_closure_creation_takes_one_operand_per_capture() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let one = fx.literal(block);
		let two = fx.literal(block);
		let mut create = |captures: &[&str], operands: &[ValueId]| {
			let mut entry = vec![timestamp_arg()];
			entry.extend(operands.iter().map(|_| TypeKnowledge::Unknown));
			fx.closure_with_entry(block, &[], captures, operands, &entry)
		};
		let none = create(&[], &[]);
		let captured = create(&["a"], &[one]);
		let two_captured = create(&["a", "b"], &[one, two]);
		let missing = create(&["a", "b"], &[one]);
		let extra = create(&["a"], &[one, two]);
		fx.passes(none);
		fx.passes(captured);
		fx.passes(two_captured);
		fx.fails(missing, "V-E10");
		fx.fails(extra, "V-E10");
	}

	#[test]
	fn a_closure_body_takes_the_timestamp_the_parameters_then_the_operands() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let captured = fx.literal(block);
		let unknown = || TypeKnowledge::Unknown;
		let layout = [timestamp_arg(), unknown(), unknown()];
		let sound = fx.closure_with_entry(block, &["p"], &["a"], &[captured], &layout);
		fx.passes(sound);
		let bare = fx.closure_with_entry(block, &[], &[], &[], &layout[..1]);
		fx.passes(bare);

		let short = fx.closure_with_entry(block, &["p"], &["a"], &[captured], &layout[..2]);
		fx.fails(short, "V-E13");
		let long = fx.closure_with_entry(block, &["p"], &[], &[], &layout);
		fx.fails(long, "V-E13");
		let no_timestamp = [unknown(), unknown(), unknown()];
		let untyped = fx.closure_with_entry(block, &["p"], &["a"], &[captured], &no_timestamp);
		fx.fails(untyped, "V-E13");
		let shifted = [unknown(), timestamp_arg(), unknown()];
		let late = fx.closure_with_entry(block, &["p"], &["a"], &[captured], &shifted);
		fx.fails(late, "V-E13");
		let bodiless = fx.expr(
			block,
			ExprOp::ClosureCreate(Box::new(ClosureAttrs {
				params: Box::new([]),
				returns: None,
				body_sql: "1".into(),
				captures: Box::new([]),
				reads_row: false,
				reads_root: false,
				literal: Default::default(),
			})),
			&[],
		);
		fx.fails(bodiless, "V-E13");
	}

	/// A tuple row over `places` with the operands `operands`, and the array that holds it.
	fn tuple_row_in_array(
		fx: &mut Fixture,
		block: BlockId,
		places: Vec<Place>,
		operands: &[ValueId],
	) -> OpId {
		let row = fx.expr(
			block,
			ExprOp::TupleRow {
				places: places.into_boxed_slice(),
			},
			operands,
		);
		let value = fx.result(row);
		fx.expr(block, ExprOp::Array, &[value]);
		row
	}

	fn place(parts: Vec<PlacePart>) -> Place {
		Place {
			parts: parts.into_boxed_slice(),
		}
	}

	#[test]
	fn tuple_row_passes_as_an_element_of_an_array() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let value = fx.literal(block);
		let subscript = fx.literal(block);
		let row = tuple_row_in_array(
			&mut fx,
			block,
			vec![place(vec![PlacePart::Field(strand("a")), PlacePart::Dynamic(1)])],
			&[value, subscript],
		);
		fx.passes(row);
	}

	#[test]
	fn tuple_row_fails_outside_an_array() {
		let mut fx = Fixture::new();
		let block = fx.statement();
		let value = fx.literal(block);
		let row = fx.expr(
			block,
			ExprOp::TupleRow {
				places: Box::new([place(vec![PlacePart::Field(strand("a"))])]),
			},
			&[value],
		);
		fx.fails(row, "V-E11");

		// Two uses are no row either: the value is shared.
		let mut fx = Fixture::new();
		let block = fx.statement();
		let value = fx.literal(block);
		let row = tuple_row_in_array(
			&mut fx,
			block,
			vec![place(vec![PlacePart::Field(strand("a"))])],
			&[value],
		);
		let shared = fx.result(row);
		fx.expr(block, ExprOp::Array, &[shared]);
		fx.fails(row, "V-E11");
	}

	#[test]
	fn tuple_row_fails_on_a_place_it_does_not_address() {
		for parts in [
			// An empty place writes the whole row.
			vec![],
			// A subscript that names a value instead of a subscript operand.
			vec![PlacePart::Dynamic(0)],
			// A subscript past the operands.
			vec![PlacePart::Index(5)],
		] {
			let mut fx = Fixture::new();
			let block = fx.statement();
			let value = fx.literal(block);
			let row = tuple_row_in_array(&mut fx, block, vec![place(parts)], &[value]);
			fx.fails(row, "V-E11");
		}
	}
}
