//! The places a write's clauses address and the shape of its data clause, as the steps and shapes
//! the write instructions read.
//!
//! A [`Place`] names where a `SET`, an `UNSET` or an `INSERT` column writes. The instructions that
//! apply a clause walk a [`PlaceStepSpec`] per place, and read the clause's [`DataShape`] to know
//! what it does; neither holds the expressions that compute a value. This module derives both from
//! the attributes of the clause's ops and plans nothing: what a subscript or a filter evaluates is
//! built by a [`PlaceHosts`], which the caller supplies.
//!
//! # What a place addresses
//!
//! - A field, a lookup (addressed as the constant key its text spells), `[*]`, `[0]`, `[$]`, a
//!   subscript and a filter are steps.
//! - Every other part (`...`, `.?`, `@`, `.@`, a method call, `[START n]`, a destructure, a
//!   recursion) is a step that stops the walk: a write that reaches it lands on something it does
//!   not address and writes nothing.
//!
//! A subscript's [`PlaceStepSpec::Subscript::literal_index`] is whether the statement wrote a
//! number there ([`PlacePart::Index`]). A step after a filter counts within the filtered selection
//! only for a written number, so the flag is read from the part and never inferred from the
//! operand's value.
//!
//! # The idiom of a place
//!
//! The instructions that cut a path out of a record (`UNSET`) and the ones that look for the field
//! an assignment starts at read a place as an [`Idiom`]. [`PlacePath::idiom`] holds the parts those
//! readers distinguish as written: fields, `*`, `[0]`, `[$]` and a written number. Every other part
//! is an inert part of the same arity, which those readers treat as the parts they do not
//! distinguish; the expressions a place embeds are never in it.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::program::{AssignShape, DataShape, PlaceStepSpec, WriteShape};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::iface::statements::{
	ContentMode, OperandSlot, Place, PlacePart, RegionSlot, UnaddressedPart,
};
use crate::exec::ir::logical::ops::{LogOp, WriteStageKind};
use crate::exec::ir::logical::stages::write_stage_ops;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::expr::{Expr, Idiom, Literal, Part};
use crate::val::{Number, Value};

/// What evaluates the expressions a place embeds: each answers the index of the host pipeline that
/// computes it.
pub(super) trait PlaceHosts {
	/// The pipeline that computes the subscript `value`. It runs with no record in scope.
	fn subscript(&mut self, value: ValueId) -> Result<u16, IrError>;

	/// The pipeline that judges one array element against the predicate region `region`, the
	/// element being the region's argument.
	fn filter(&mut self, region: RegionId) -> Result<u16, IrError>;
}

/// A place, as the steps it is walked by and the idiom that stands for it.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct PlacePath {
	pub(super) steps: Vec<PlaceStepSpec>,
	pub(super) idiom: Idiom,
}

/// The operand `slot` of the op `data`.
fn operand_at(data: &OpData, slot: OperandSlot) -> Result<ValueId, IrError> {
	data.operands
		.get(usize::from(slot))
		.copied()
		.ok_or(IrError::Stale("ir_lower::write_place: a place names an operand its op lacks"))
}

/// The region `slot` of the op `data`.
fn region_at(data: &OpData, slot: RegionSlot) -> Result<RegionId, IrError> {
	data.regions
		.get(usize::from(slot))
		.copied()
		.ok_or(IrError::Stale("ir_lower::write_place: a place names a region its op lacks"))
}

/// The literal the value `value` holds, if an op of the literal form defines it.
fn literal_of(ir: &Ir, value: ValueId) -> Option<&Value> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	match &ir.op(op)?.kind {
		OpKind::Expr(ExprOp::Literal(literal)) => Some(literal),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// The written number `value` stands for as a part of an idiom, which is what its subscript was
/// written as.
fn written_index(value: &Value) -> Option<Part> {
	let Value::Number(number) = value else {
		return None;
	};
	let literal = match number {
		Number::Int(int) => Literal::Integer(*int),
		Number::Float(float) => Literal::Float(*float),
		Number::Decimal(decimal) => Literal::Decimal(*decimal),
		Number::BigDecimal(decimal) => Literal::BigDecimal(decimal.clone()),
		Number::I8(v) => Literal::I8(*v),
		Number::U8(v) => Literal::U8(*v),
		Number::I16(v) => Literal::I16(*v),
		Number::U16(v) => Literal::U16(*v),
		Number::I32(v) => Literal::I32(*v),
		Number::U32(v) => Literal::U32(*v),
		Number::U64(v) => Literal::U64(*v),
		Number::F16(v) => Literal::F16(*v),
		Number::F32(v) => Literal::F32(*v),
	};
	Some(Part::Value(Expr::Literal(literal)))
}

/// The part that stands for a part no reader of an idiom distinguishes.
fn inert() -> Part {
	Part::Destructure(Vec::new())
}

/// The steps and the idiom of `place`, whose subscripts are operands and whose filters are regions
/// of the op `op`.
pub(super) fn place_path(
	ir: &Ir,
	op: OpId,
	place: &Place,
	hosts: &mut dyn PlaceHosts,
) -> Result<PlacePath, IrError> {
	let data =
		ir.op(op).ok_or(IrError::Stale("ir_lower::write_place: the op of a place is gone"))?;
	let mut steps = Vec::with_capacity(place.parts.len());
	let mut parts = Vec::with_capacity(place.parts.len());
	for part in place.parts.iter() {
		let (step, written) = match part {
			PlacePart::Field(name) => {
				(PlaceStepSpec::Field(name.as_str().to_owned()), Part::Field(name.clone()))
			}
			PlacePart::All => (PlaceStepSpec::All, Part::All),
			PlacePart::First => (PlaceStepSpec::First, Part::First),
			PlacePart::Last => (PlaceStepSpec::Last, Part::Last),
			PlacePart::Filter(slot) => {
				let pipeline = hosts.filter(region_at(data, *slot)?)?;
				(PlaceStepSpec::Where(pipeline), inert())
			}
			PlacePart::Dynamic(slot) => {
				let expr = hosts.subscript(operand_at(data, *slot)?)?;
				(
					PlaceStepSpec::Subscript {
						expr,
						literal_index: false,
					},
					inert(),
				)
			}
			PlacePart::Index(slot) => {
				let value = operand_at(data, *slot)?;
				let written = literal_of(ir, value).and_then(written_index).ok_or(
					IrError::Stale("ir_lower::write_place: a written index holds no number"),
				)?;
				let expr = hosts.subscript(value)?;
				(
					PlaceStepSpec::Subscript {
						expr,
						literal_index: true,
					},
					written,
				)
			}
			PlacePart::Unaddressed(UnaddressedPart::Lookup, text) => {
				(PlaceStepSpec::Lookup(text.to_string()), inert())
			}
			PlacePart::Unaddressed(
				UnaddressedPart::Destructure
				| UnaddressedPart::Recurse
				| UnaddressedPart::Start
				| UnaddressedPart::Method
				| UnaddressedPart::Filter,
				_,
			) => (PlaceStepSpec::Unaddressed, inert()),
			PlacePart::Flatten => (PlaceStepSpec::Unaddressed, Part::Flatten),
			PlacePart::Optional => (PlaceStepSpec::Unaddressed, Part::Optional),
			PlacePart::Doc => (PlaceStepSpec::Unaddressed, Part::Doc),
			PlacePart::RepeatRecurse => (PlaceStepSpec::Unaddressed, Part::RepeatRecurse),
		};
		steps.push(step);
		parts.push(written);
	}
	Ok(PlacePath {
		steps,
		idiom: Idiom(parts),
	})
}

/// The shape of the data clause whose region is `region`, hosted by a stage op.
///
/// A `SET` and an `ON DUPLICATE KEY UPDATE` hold one `assign` op per assignment, in written
/// order; an `UNSET` holds one `unset` per path; every other clause holds the one `content` op that
/// says what it writes. The ops are logical-dialect ops in an unconverted container and
/// physical-dialect ops in a converted one, and both map to the same shapes. An `INSERT`'s rows
/// are not a data clause.
pub(super) fn data_shape(
	ir: &Ir,
	region: RegionId,
	hosts: &mut dyn PlaceHosts,
) -> Result<DataShape, IrError> {
	let kind = ir
		.region(region)
		.ok_or(IrError::Stale("ir_lower::write_place: the region of a clause is gone"))?
		.kind;
	let mut assigns = Vec::new();
	let mut unsets = Vec::new();
	let mut content = None;
	for op in ops_in(ir, region) {
		match &ir
			.op(op)
			.ok_or(IrError::Stale("ir_lower::write_place: an op of a clause is gone"))?
			.kind
		{
			OpKind::Logical(LogOp::Assign(attrs)) | OpKind::Phys(PhysOp::Assign(attrs)) => {
				let path = place_path(ir, op, &attrs.place, hosts)?;
				assigns.push(AssignShape {
					path: path.idiom,
					operator: attrs.op.clone(),
					steps: path.steps,
				});
			}
			OpKind::Logical(LogOp::Unset {
				place,
			})
			| OpKind::Phys(PhysOp::Unset {
				place,
			}) => unsets.push(place_path(ir, op, place, hosts)?.idiom),
			OpKind::Logical(LogOp::Content {
				mode,
			})
			| OpKind::Phys(PhysOp::Content {
				mode,
			}) => {
				if content.replace(*mode).is_some() {
					return Err(IrError::Stale(
						"ir_lower::write_place: a data clause holds two content ops",
					));
				}
			}
			OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => {}
		}
	}
	match (kind, content) {
		(RegionKind::Clause(ClauseRole::SetValue | ClauseRole::DuplicateUpdate), None)
			if unsets.is_empty() =>
		{
			Ok(DataShape::Set(assigns))
		}
		(RegionKind::Clause(ClauseRole::Content), None)
			if assigns.is_empty() && !unsets.is_empty() =>
		{
			Ok(DataShape::Unset(unsets))
		}
		// A clause whose value raises before the clause states what it writes fails when
		// evaluated, so its shape is never applied.
		(RegionKind::Clause(ClauseRole::Content), None)
			if assigns.is_empty() && unsets.is_empty() && raises_before_storing(ir, region) =>
		{
			Ok(DataShape::Content)
		}
		(RegionKind::Clause(ClauseRole::Content), Some(mode))
			if assigns.is_empty() && unsets.is_empty() =>
		{
			match mode {
				ContentMode::Content => Ok(DataShape::Content),
				ContentMode::Merge => Ok(DataShape::Merge),
				ContentMode::Patch => Ok(DataShape::Patch),
				ContentMode::Replace => Ok(DataShape::Replace),
				ContentMode::Single => Err(IrError::Stale(
					"ir_lower::write_place: an INSERT's rows are not a data clause",
				)),
			}
		}
		_ => {
			Err(IrError::Stale("ir_lower::write_place: a data clause holds ops of another clause"))
		}
	}
}

/// Whether the clause region `region` ends in a raise: a value of the clause throws before the
/// clause states what it writes.
pub(super) fn raises_before_storing(ir: &Ir, region: RegionId) -> bool {
	ops_in(ir, region)
		.last()
		.and_then(|op| ir.op(op))
		.is_some_and(|data| matches!(data.kind, OpKind::Expr(ExprOp::Throw | ExprOp::Raise(_))))
}

/// The clauses of the write `write`, as the instructions of its body read them: whether the
/// statement has a `WHERE` condition, and the shape of its data clause and of its `ON DUPLICATE
/// KEY UPDATE`.
pub(super) fn write_shape(
	ir: &Ir,
	write: &OpData,
	hosts: &mut dyn PlaceHosts,
) -> Result<WriteShape, IrError> {
	let stages = write_stage_ops(ir, write).ok_or(IrError::Stale(
		"ir_lower::write_place: the subjects of a write are no write loop",
	))?;
	let mut shape = WriteShape::default();
	for (op, stage) in stages {
		let regions =
			&ir.op(op).ok_or(IrError::Stale("ir_lower::write_place: a stage op is gone"))?.regions;
		if stage == WriteStageKind::CheckWhereCondition {
			shape.cond |= regions
				.iter()
				.filter_map(|region| ir.region(*region))
				.any(|region| region.kind == RegionKind::Clause(ClauseRole::WriteCond));
		} else if stage == WriteStageKind::ComputeInputData {
			for region in regions.iter().copied() {
				let Some(RegionKind::Clause(role)) = ir.region(region).map(|held| held.kind) else {
					continue;
				};
				let slot = if role == ClauseRole::DuplicateUpdate {
					&mut shape.duplicate_update
				} else if role == ClauseRole::SetValue || role == ClauseRole::Content {
					&mut shape.data
				} else {
					continue;
				};
				if slot.replace(data_shape(ir, region, hosts)?).is_some() {
					return Err(IrError::Stale(
						"ir_lower::write_place: a statement holds two data clauses",
					));
				}
			}
		}
	}
	Ok(shape)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::super::tests::Builder;
	use super::*;
	use crate::exec::ir::core::walk::ops_in;
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::AssignAttrs;
	use crate::expr::AssignOperator;

	/// Hosts that number every pipeline they are asked for, in the order they are asked, and keep
	/// what each stands for.
	#[derive(Default)]
	struct Numbering {
		subscripts: Vec<ValueId>,
		filters: Vec<RegionId>,
	}

	impl PlaceHosts for Numbering {
		fn subscript(&mut self, value: ValueId) -> Result<u16, IrError> {
			self.subscripts.push(value);
			Ok(100 + self.subscripts.len() as u16 - 1)
		}

		fn filter(&mut self, region: RegionId) -> Result<u16, IrError> {
			self.filters.push(region);
			Ok(200 + self.filters.len() as u16 - 1)
		}
	}

	fn field(name: &str) -> PlacePart {
		PlacePart::Field(Strand::new(name))
	}

	fn place(parts: Vec<PlacePart>) -> Place {
		Place {
			parts: parts.into_boxed_slice(),
		}
	}

	/// An assignment of `place` over the value `1` and the operands `extra`.
	fn assign(b: &mut Builder, place: Place, extra: &[ValueId]) -> OpId {
		let value = b.literal(Value::from(1_i64));
		let mut operands = vec![value];
		operands.extend_from_slice(extra);
		b.add_logical(
			LogOp::Assign(Box::new(AssignAttrs {
				place,
				op: AssignOperator::Assign,
			})),
			&operands,
		)
	}

	#[test]
	fn every_part_a_place_addresses_is_a_step() {
		let mut b = Builder::new();
		let key = b.literal(Value::from("k"));
		let two = b.literal(Value::from(2_i64));
		let op = assign(
			&mut b,
			place(vec![
				field("a"),
				PlacePart::Dynamic(1),
				PlacePart::Index(2),
				PlacePart::All,
				PlacePart::First,
				PlacePart::Last,
				PlacePart::Unaddressed(UnaddressedPart::Lookup, "->edge".into()),
			]),
			&[key, two],
		);
		let mut hosts = Numbering::default();
		let attrs = match b.ir.op(op).map(|data| &data.kind) {
			Some(OpKind::Logical(LogOp::Assign(attrs))) => attrs.clone(),
			other => panic!("the op is an assignment: {other:?}"),
		};
		let path = place_path(&b.ir, op, &attrs.place, &mut hosts).expect("the place maps");
		assert_eq!(
			path.steps,
			[
				PlaceStepSpec::Field("a".to_owned()),
				PlaceStepSpec::Subscript {
					expr: 100,
					literal_index: false,
				},
				PlaceStepSpec::Subscript {
					expr: 101,
					literal_index: true,
				},
				PlaceStepSpec::All,
				PlaceStepSpec::First,
				PlaceStepSpec::Last,
				PlaceStepSpec::Lookup("->edge".to_owned()),
			]
		);
		assert_eq!(hosts.subscripts, [key, two]);
	}

	#[test]
	fn a_part_no_place_addresses_stops_the_walk() {
		let mut b = Builder::new();
		let op = assign(
			&mut b,
			place(vec![
				PlacePart::Flatten,
				PlacePart::Optional,
				PlacePart::Doc,
				PlacePart::RepeatRecurse,
				PlacePart::Unaddressed(UnaddressedPart::Destructure, ".{ a }".into()),
				PlacePart::Unaddressed(UnaddressedPart::Recurse, ".{1..2}".into()),
				PlacePart::Unaddressed(UnaddressedPart::Start, "1".into()),
				PlacePart::Unaddressed(UnaddressedPart::Method, ".push(1)".into()),
			]),
			&[],
		);
		let mut hosts = Numbering::default();
		let attrs = match b.ir.op(op).map(|data| &data.kind) {
			Some(OpKind::Logical(LogOp::Assign(attrs))) => attrs.clone(),
			other => panic!("the op is an assignment: {other:?}"),
		};
		let path = place_path(&b.ir, op, &attrs.place, &mut hosts).expect("the place maps");
		assert_eq!(path.steps, vec![PlaceStepSpec::Unaddressed; 8]);
		assert!(hosts.subscripts.is_empty() && hosts.filters.is_empty());
	}

	#[test]
	fn the_idiom_keeps_what_a_cut_and_a_field_lookup_distinguish() {
		let mut b = Builder::new();
		let two = b.literal(Value::from(2_i64));
		let key = b.literal(Value::from("k"));
		let op = assign(
			&mut b,
			place(vec![
				field("a"),
				PlacePart::Index(1),
				PlacePart::Dynamic(2),
				PlacePart::All,
				PlacePart::First,
				PlacePart::Last,
			]),
			&[two, key],
		);
		let attrs = match b.ir.op(op).map(|data| &data.kind) {
			Some(OpKind::Logical(LogOp::Assign(attrs))) => attrs.clone(),
			other => panic!("the op is an assignment: {other:?}"),
		};
		let path =
			place_path(&b.ir, op, &attrs.place, &mut Numbering::default()).expect("the place maps");
		let parts = &path.idiom.0;
		assert_eq!(parts[0], Part::Field(Strand::new("a")));
		assert_eq!(parts[1].as_old_index(), Some(2));
		assert_eq!(parts[2].as_old_index(), None);
		assert_eq!(parts[3..], [Part::All, Part::First, Part::Last]);
	}

	#[test]
	fn a_written_index_that_holds_no_number_is_refused() {
		let mut b = Builder::new();
		let text = b.literal(Value::from("k"));
		let op = assign(&mut b, place(vec![PlacePart::Index(1)]), &[text]);
		let attrs = match b.ir.op(op).map(|data| &data.kind) {
			Some(OpKind::Logical(LogOp::Assign(attrs))) => attrs.clone(),
			other => panic!("the op is an assignment: {other:?}"),
		};
		let refused = place_path(&b.ir, op, &attrs.place, &mut Numbering::default());
		assert!(matches!(refused, Err(IrError::Stale(what)) if what.contains("holds no number")));
	}

	#[test]
	fn a_place_that_names_an_operand_its_op_lacks_is_refused() {
		let mut b = Builder::new();
		let op = assign(&mut b, place(vec![PlacePart::Dynamic(4), PlacePart::Filter(0)]), &[]);
		let attrs = match b.ir.op(op).map(|data| &data.kind) {
			Some(OpKind::Logical(LogOp::Assign(attrs))) => attrs.clone(),
			other => panic!("the op is an assignment: {other:?}"),
		};
		let refused = place_path(&b.ir, op, &attrs.place, &mut Numbering::default());
		assert!(matches!(refused, Err(IrError::Stale(what)) if what.contains("operand")));
		let filter =
			place_path(&b.ir, op, &place(vec![PlacePart::Filter(0)]), &mut Numbering::default());
		assert!(matches!(filter, Err(IrError::Stale(what)) if what.contains("region")));
	}

	/// A stage op hosting one data region of `role`, filled by `body`.
	fn clause(b: &mut Builder, role: ClauseRole, body: impl FnOnce(&mut Builder)) -> RegionId {
		let stage = b.add_logical(
			LogOp::WriteStage {
				stage: WriteStageKind::ComputeInputData,
				mark: crate::exec::ir::logical::ops::WriteMark::Compute,
			},
			&[],
		);
		b.open(stage, RegionKind::Clause(role), &[TypeKnowledge::Unknown]);
		body(b);
		b.close();
		*b.ir.op(stage).and_then(|data| data.regions.first()).expect("the stage holds the region")
	}

	#[test]
	fn a_set_clause_is_one_shape_per_assignment_in_written_order() {
		let mut b = Builder::new();
		let region = clause(&mut b, ClauseRole::SetValue, |b| {
			assign(b, place(vec![field("a")]), &[]);
			assign(b, place(vec![field("b"), PlacePart::All]), &[]);
		});
		let DataShape::Set(shapes) =
			data_shape(&b.ir, region, &mut Numbering::default()).expect("the clause maps")
		else {
			panic!("a SET is a set shape");
		};
		assert_eq!(shapes.len(), 2);
		assert_eq!(shapes[0].steps, [PlaceStepSpec::Field("a".to_owned())]);
		assert_eq!(shapes[1].steps, [PlaceStepSpec::Field("b".to_owned()), PlaceStepSpec::All]);
		// Mapping the clause leaves its two assignments, each after the literal it assigns.
		let assigns = ops_in(&b.ir, region)
			.filter(|op| {
				matches!(
					b.ir.op(*op).map(|data| &data.kind),
					Some(OpKind::Logical(LogOp::Assign(_)))
				)
			})
			.count();
		assert_eq!(assigns, 2);
	}

	#[test]
	fn an_unset_clause_is_the_idioms_it_cuts() {
		let mut b = Builder::new();
		let region = clause(&mut b, ClauseRole::Content, |b| {
			b.add_logical(
				LogOp::Unset {
					place: place(vec![field("a"), PlacePart::All]),
				},
				&[],
			);
		});
		let shape = data_shape(&b.ir, region, &mut Numbering::default()).expect("the clause maps");
		assert_eq!(
			shape,
			DataShape::Unset(vec![Idiom(vec![Part::Field(Strand::new("a")), Part::All])])
		);
	}

	#[test]
	fn a_content_clause_is_its_mode() {
		for (mode, shape) in [
			(ContentMode::Content, DataShape::Content),
			(ContentMode::Merge, DataShape::Merge),
			(ContentMode::Patch, DataShape::Patch),
			(ContentMode::Replace, DataShape::Replace),
		] {
			let mut b = Builder::new();
			let region = clause(&mut b, ClauseRole::Content, |b| {
				let value = b.literal(Value::from(1_i64));
				b.add_logical(
					LogOp::Content {
						mode,
					},
					&[value],
				);
			});
			assert_eq!(
				data_shape(&b.ir, region, &mut Numbering::default()).expect("the clause maps"),
				shape
			);
		}
	}

	/// The clause of a converted container holds the physical dialect's ops, which map to the same
	/// shapes as the logical dialect's.
	#[test]
	fn the_clause_ops_of_the_physical_dialect_map_to_the_same_shapes() {
		let mut b = Builder::new();
		let region = clause(&mut b, ClauseRole::SetValue, |b| {
			let value = b.literal(Value::from(1_i64));
			b.add_phys(
				PhysOp::Assign(Box::new(AssignAttrs {
					place: place(vec![field("a")]),
					op: AssignOperator::Assign,
				})),
				&[value],
			);
		});
		let DataShape::Set(shapes) =
			data_shape(&b.ir, region, &mut Numbering::default()).expect("the clause maps")
		else {
			panic!("a SET is a set shape");
		};
		assert_eq!(shapes.len(), 1);
		assert_eq!(shapes[0].steps, [PlaceStepSpec::Field("a".to_owned())]);

		let mut b = Builder::new();
		let region = clause(&mut b, ClauseRole::Content, |b| {
			b.add_phys(
				PhysOp::Unset {
					place: place(vec![field("a")]),
				},
				&[],
			);
		});
		assert_eq!(
			data_shape(&b.ir, region, &mut Numbering::default()).expect("the clause maps"),
			DataShape::Unset(vec![Idiom(vec![Part::Field(Strand::new("a"))])])
		);

		let mut b = Builder::new();
		let region = clause(&mut b, ClauseRole::Content, |b| {
			let value = b.literal(Value::from(1_i64));
			b.add_phys(
				PhysOp::Content {
					mode: ContentMode::Patch,
				},
				&[value],
			);
		});
		assert_eq!(
			data_shape(&b.ir, region, &mut Numbering::default()).expect("the clause maps"),
			DataShape::Patch
		);
	}

	#[test]
	fn an_inserts_rows_are_no_data_clause() {
		let mut b = Builder::new();
		let region = clause(&mut b, ClauseRole::Content, |b| {
			let value = b.literal(Value::from(1_i64));
			b.add_logical(
				LogOp::Content {
					mode: ContentMode::Single,
				},
				&[value],
			);
		});
		let refused = data_shape(&b.ir, region, &mut Numbering::default());
		assert!(matches!(refused, Err(IrError::Stale(what)) if what.contains("INSERT")));
	}

	#[test]
	fn a_clause_holding_the_ops_of_another_is_refused() {
		let mut b = Builder::new();
		let region = clause(&mut b, ClauseRole::SetValue, |b| {
			b.add_logical(
				LogOp::Unset {
					place: place(vec![field("a")]),
				},
				&[],
			);
		});
		assert!(data_shape(&b.ir, region, &mut Numbering::default()).is_err());
		let mut b = Builder::new();
		let region = clause(&mut b, ClauseRole::Content, |_| {});
		assert!(data_shape(&b.ir, region, &mut Numbering::default()).is_err(), "an empty clause");
	}
}
