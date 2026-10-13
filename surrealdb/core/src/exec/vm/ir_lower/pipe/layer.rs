//! `phys.layer`: the columns a table's field permissions and `COMPUTED` fields add to the row.
//!
//! A layer holds one column per field it lands. Each column is the field it lands under, read from
//! the row by the op's first group of regions, and its value, evaluated in a frame of its own by
//! the second group: a permission column in the frame of its guard, a computed column in the
//! frame of the field's body, both under the `AUTH LIMIT` of the definition that supplied them.
//!
//! The columns on stored fields come first, then those on computed fields, each group in the
//! order of the op, and every one is evaluated before any lands on the row. A column on a computed
//! field reads the row with each stored column's landing applied and each computed value derived
//! before it set, when a stored column is present; that record is built in its own register.
//! Otherwise it reads the row. Every guard reads the row.
//!
//! A permission column is `IF guard THEN value ELSE NONE`: the value is evaluated first, coerced to
//! the field's declared kind, and the guard then judges it. A value the guard refuses, or `NONE`
//! where the field has no guard, cuts the field from the row.
//!
//! A column on a computed field is one whose expression runs in a computed frame, one the layer's
//! input declares `COMPUTED` (`LayerAttrs::computed` for a statement's layer,
//! `SubscriberLayer::computed` for a subscriber's, carried from the bound plan whatever source,
//! breaker or join gives the rows) or one whose field is read as a computed field. Every layer
//! carries that set, so the classification comes from the input's schema. A
//! column's frame region takes the row and the value the column judges; the guard reads the second
//! as `$value`, which is bound to the column's value once it is evaluated; a computed column whose
//! guard does not read `$value`, and that lands on no reduced record, is judged first, and its
//! value is evaluated only once admitted. The
//! guard of a stored
//! column has its `$value` written out as the field it reads, outside any statement the guard
//! holds. A computed column's guard admits its value as it is, `NONE` included; a stored column's
//! admitted `NONE` leaves the field as the row has it.

use surrealdb_strand::Strand;

use super::{
	Body, Branching, Emitted, EndJumps, PoolBase, Reads, region_frame, row_arg, yielded_value,
};
use crate::exec::field_path::FieldPath;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{FrameId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::RegionProperties;
use crate::exec::ir::logical::ops::{FieldShape, FieldsShape, SubscriberLayer};
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::vm::ir_lower::emit::Emitter;
use crate::exec::vm::ir_lower::expr::{Lowerer, Needs};
use crate::exec::vm::ir_lower::units::LowerCx;
use crate::exec::vm::program::{
	FrameMode, Instr, Operand, PermissionSpec, Pipeline, ROW, Reg, Source,
};
use crate::expr::Kind;
use crate::expr::logical::{EvalMode, GuardKind};
use crate::val::Value;

/// One column of a layer.
struct Column {
	/// The field the column lands under.
	name: Strand,
	/// The kind the value is coerced to.
	kind: Option<Kind>,
	/// The region that evaluates the column's expression.
	region: RegionId,
	/// The frame of the container the expression runs under.
	container: FrameId,
	mode: EvalMode,
	auth: Option<surrealdb_iam::AuthLimit>,
	/// Whether the field is read as a `COMPUTED` field.
	reads_computed: bool,
}

/// The registers every column of a layer lands in.
struct Targets {
	/// The object whose fields the row takes.
	sets: Reg,
	/// The object that names the fields the row loses.
	cuts: Reg,
	/// The record the computed columns read, when stored columns are present too.
	reduced: Option<Reg>,
}

impl Body<'_, '_, '_> {
	/// Lowers the layer whose columns are `fields` and whose regions are `regions`, over the row
	/// the body holds. `declared` is the fields the layer's input declares `COMPUTED`
	/// (`LayerAttrs::computed`, `SubscriberLayer::computed`).
	///
	/// Once the layer lands, the row's type is unknown, so the input's schema no longer describes
	/// it.
	pub(super) fn layer(
		&mut self,
		regions: &[RegionId],
		fields: &FieldsShape,
		declared: &[Strand],
	) -> Result<(), IrError> {
		self.require_row("a layer over a projection")?;
		let (stored, computed) = self.split_columns(regions, fields, declared)?;
		self.land_columns(&stored, &computed, declared)
	}

	/// Lowers the columns of the layer whose columns are `fields` and whose regions are `regions`
	/// that guard a stored field, over the row the body holds, and answers how many it lowered.
	/// Nothing is derived: the columns on computed fields are not evaluated, and a layer with no
	/// stored guard emits nothing.
	pub(super) fn stored_guards(
		&mut self,
		regions: &[RegionId],
		fields: &FieldsShape,
		declared: &[Strand],
	) -> Result<usize, IrError> {
		self.require_row("a layer over a projection")?;
		let (stored, _) = self.split_columns(regions, fields, declared)?;
		let count = stored.len();
		if count > 0 {
			self.land_columns(&stored, &[], declared)?;
		}
		Ok(count)
	}

	/// The columns of a layer, split into those that guard a stored field and those on a computed
	/// field: a column in a computed frame, one whose field is read as a computed field, or one
	/// under a name `declared` holds.
	fn split_columns(
		&self,
		regions: &[RegionId],
		fields: &FieldsShape,
		declared: &[Strand],
	) -> Result<(Vec<Column>, Vec<Column>), IrError> {
		let (mut stored, mut computed) = (Vec::new(), Vec::new());
		for column in self.layer_columns(regions, fields)? {
			match column.mode {
				EvalMode::Caller => {
					return Err(invariant("a layer column in the caller's frame"));
				}
				EvalMode::Computed => computed.push(column),
				EvalMode::Permission | EvalMode::MutablePermission => {
					if column.reads_computed || declared.contains(&column.name) {
						computed.push(column);
					} else {
						stored.push(column);
					}
				}
			}
		}
		Ok((stored, computed))
	}

	/// Evaluates the columns `stored` and `computed` of a layer whose input declares `declared`
	/// `COMPUTED`, and lands them on the row.
	fn land_columns(
		&mut self,
		stored: &[Column],
		computed: &[Column],
		declared: &[Strand],
	) -> Result<(), IrError> {
		let sets = self.em.reg()?;
		self.em.emit(Instr::NewObject {
			dst: sets,
		});
		let cuts = self.em.reg()?;
		self.em.emit(Instr::NewObject {
			dst: cuts,
		});
		// Each stored column is a permission, so each one is a stored guard that the computed
		// columns read through the reduced record.
		let reduced = if !stored.is_empty() && !computed.is_empty() {
			let reduced = self.em.reg()?;
			self.em.emit(Instr::LoadRow {
				dst: reduced,
			});
			Some(reduced)
		} else {
			None
		};
		let targets = Targets {
			sets,
			cuts,
			reduced,
		};
		for column in stored {
			self.layer_column(column, false, &targets)?;
		}
		if !computed.is_empty() {
			let names = computed
				.iter()
				.map(|column| column.name.clone())
				.chain(declared.iter().cloned())
				.collect();
			let names = self.em.computed_names(names)?;
			let id = self.em.field("id")?;
			self.em.emit(Instr::BeginComputing {
				id,
				doc: reduced,
				names,
			});
			for column in computed {
				self.layer_column(column, true, &targets)?;
			}
		}
		self.em.emit(Instr::LandRow {
			sets,
			cuts,
		});
		self.schema = None;
		Ok(())
	}

	/// The columns of the layer whose regions are `regions`, in the order of its fields.
	///
	/// The first group of regions reads each field from the row, which names it, and the second
	/// evaluates its expression in its frame.
	fn layer_columns(
		&self,
		regions: &[RegionId],
		fields: &FieldsShape,
	) -> Result<Vec<Column>, IrError> {
		let ir = self.ir;
		let FieldsShape::Select(list) = fields else {
			return Err(invariant("a layer of a value selection"));
		};
		let kinds: Vec<&Option<Kind>> = list
			.iter()
			.filter_map(|field| match field {
				FieldShape::Single {
					kind,
					..
				} => Some(kind),
				FieldShape::All => None,
			})
			.collect();
		let count = kinds.len();
		if regions.len() != count * 2 {
			return Err(invariant("a layer without a read and a frame region for each column"));
		}
		let mut columns = Vec::with_capacity(count);
		for (at, kind) in kinds.into_iter().enumerate() {
			let (read, region) = (regions[at], regions[count + at]);
			let container = region_frame(ir, region)?;
			let held = ir.frame(container).ok_or(IrError::Stale("ir_lower::pipe: frame"))?;
			let (name, reads_computed) = field_read(ir, read)?;
			columns.push(Column {
				name,
				kind: kind.clone(),
				region,
				container,
				mode: held.eval.mode,
				auth: held.eval.auth.clone(),
				reads_computed,
			});
		}
		Ok(columns)
	}

	/// Evaluates one column and records where it lands: in the object of fields to set, in the
	/// object of fields to cut and, where the column is read by the columns after it, in the
	/// record they read. `computed` says whether the column is on a computed field.
	fn layer_column(
		&mut self,
		column: &Column,
		computed: bool,
		targets: &Targets,
	) -> Result<(), IrError> {
		let ir = self.ir;
		let mode = match column.mode {
			EvalMode::Permission => FrameMode::Guard(GuardKind::Read),
			EvalMode::MutablePermission => FrameMode::Guard(GuardKind::Write),
			EvalMode::Computed => FrameMode::Computed,
			EvalMode::Caller => return Err(invariant("a layer column in the caller's frame")),
		};
		let is_guard = matches!(mode, FrameMode::Guard(_));
		let frame = self.em.permission_frame(PermissionSpec {
			mode,
			auth: column.auth.clone(),
			column: true,
		})?;
		// The value of a permission column on a computed field is the field's re-derivation,
		// evaluated as the field's body is.
		let value_frame = if computed && is_guard {
			self.em.permission_frame(PermissionSpec {
				mode: FrameMode::Computed,
				auth: column.auth.clone(),
				column: true,
			})?
		} else {
			frame
		};
		let judgement = if is_guard {
			judgement_of(ir, column.region)?
		} else {
			None
		};
		// A computed column reads the reduced record where there is one.
		let doc = if computed {
			targets.reduced
		} else {
			None
		};
		let value_region = judgement.map_or(column.region, |(_, kept)| kept);
		let value = self.em.reg()?;
		let (cut, unchanged) = (self.em.label()?, self.em.label()?);
		// A computed column whose guard does not read `$value`, and whose value no later body reads
		// off a reduced record, is judged before its body runs, so a body the guard refuses is
		// never evaluated.
		let judged_first = match judgement {
			Some((guard, _))
				if computed && targets.reduced.is_none() && !reads_judged(ir, column.region) =>
			{
				self.column_guard(column, guard, value, frame, cut)?;
				true
			}
			Some(_) | None => false,
		};
		let src = self.column_value(column, value_region, doc, value_frame)?;
		self.em.emit(Instr::Load {
			dst: value,
			src,
		});
		let name = self.em.field(column.name.as_str())?;
		// A permission column whose value is the literal `NONE` is a field the caller is denied
		// outright. It cuts the field and has no value to hold to the declared kind.
		let denied = is_guard && yields_none(ir, value_region)?;
		if let (Some(kind), false) = (&column.kind, denied) {
			let kind = self.em.kind(kind.clone())?;
			self.em.emit(Instr::CoerceField {
				reg: value,
				kind,
				field: name,
			});
		}
		let path = self.em.path(FieldPath::field(column.name.as_str()))?;
		// Every later computed body reads the field as this column derived it, whatever its guard
		// says of it.
		if let (true, Some(reduced)) = (computed, targets.reduced) {
			self.em.emit(Instr::SetField {
				object: reduced,
				path,
				value: Operand::Reg(value),
			});
		}
		// A stored column's landing is the reduced record's too.
		let reduced = if computed {
			None
		} else {
			targets.reduced
		};
		match judgement {
			Some((guard, _)) => {
				if !judged_first {
					self.column_guard(column, guard, value, frame, cut)?;
				}
				// An admitted stored field is as the row has it.
				if !computed {
					self.em.emit(Instr::JumpIfNone {
						cond: Operand::Reg(value),
						none: true,
						target: unchanged,
					});
				}
			}
			// A permission without a guard cuts the field its `NONE` leaves.
			None if is_guard => {
				self.em.emit(Instr::JumpIfNone {
					cond: Operand::Reg(value),
					none: true,
					target: cut,
				});
			}
			None => {}
		}
		self.em.emit(Instr::SetField {
			object: targets.sets,
			path,
			value: Operand::Reg(value),
		});
		self.em.emit(Instr::RemoveField {
			object: targets.cuts,
			name,
		});
		if let Some(reduced) = reduced {
			self.em.emit(Instr::SetField {
				object: reduced,
				path,
				value: Operand::Reg(value),
			});
		}
		if is_guard {
			self.em.emit(Instr::Jump {
				target: unchanged,
			});
			self.em.bind(cut)?;
			self.em.emit(Instr::RemoveField {
				object: targets.sets,
				name,
			});
			let marker = Operand::Const(self.em.constant(Value::Bool(true))?);
			self.em.emit(Instr::SetField {
				object: targets.cuts,
				path,
				value: marker,
			});
			if let Some(reduced) = reduced {
				self.em.emit(Instr::RemoveField {
					object: reduced,
					name,
				});
			}
		}
		self.em.bind(unchanged)
	}

	/// The value the region `region` of `column` yields, evaluated in the permission frame
	/// `frame` over the record `doc` where there is one, and over the row otherwise.
	fn column_value(
		&mut self,
		column: &Column,
		region: RegionId,
		doc: Option<Reg>,
		frame: u16,
	) -> Result<Operand, IrError> {
		let ir = self.ir;
		let (objects, schema) = (self.objects, self.schema.clone());
		let mut lowerer = Lowerer::in_context(&mut *self.em, ir, self.cx).with_schema(schema);
		if let Some(arg) = row_arg(ir, column.region) {
			match doc {
				Some(reduced) => lowerer.bind_arg(arg, Operand::Reg(reduced)),
				None => {
					lowerer.bind_arg(arg, Operand::Reg(ROW));
					lowerer.bind_row(arg, objects);
				}
			}
		}
		let value =
			lowerer.in_frame(column.container, frame, |lowerer| lowerer.region_value(region))?;
		self.claims.extend(lowerer.take_claims());
		self.needs = self.needs.merged(&lowerer.needs());
		Ok(value)
	}

	/// Jumps to `cut` where the guard `guard` of `column`, a value of its region, refuses the
	/// column's value `value`, evaluated in the permission frame `frame` over the row with `$value`
	/// naming `value`.
	fn column_guard(
		&mut self,
		column: &Column,
		guard: ValueId,
		value: Reg,
		frame: u16,
		cut: crate::exec::vm::program::Label,
	) -> Result<(), IrError> {
		let ir = self.ir;
		let end = self.end;
		let (objects, schema) = (self.objects, self.schema.clone());
		let mut lowerer = Lowerer::in_context(&mut *self.em, ir, self.cx).with_schema(schema);
		if let Some(arg) = row_arg(ir, column.region) {
			lowerer.bind_arg(arg, Operand::Reg(ROW));
			lowerer.bind_row(arg, objects);
		}
		if let Some(judged) = judged_arg(ir, column.region) {
			lowerer.bind_arg(judged, Operand::Reg(value));
		}
		let jumps = &mut self.jumps;
		lowerer.in_frame(column.container, frame, |lowerer| {
			Branching {
				lowerer,
				ir,
				end,
				jumps,
			}
			.branch(guard, false, cut)
		})?;
		self.claims.extend(lowerer.take_claims());
		self.needs = self.needs.merged(&lowerer.needs());
		Ok(())
	}
}

/// Lowers the layers of a field view as one host pipeline over the image the host hands it, and
/// answers the pipeline with what it needs of the program.
///
/// Each of `layers` is one layer, its columns and the fields its input declares `COMPUTED`, with
/// the regions that hold them, as the regions of a `phys.layer`, in the order the layers apply. The
/// image is the pipeline's row and an object. Each layer lands its columns on the row before the
/// next layer reads it, and the row the last leaves is emitted.
pub(in crate::exec::vm::ir_lower) fn lower_layers(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	layers: &[(&SubscriberLayer, &[RegionId])],
) -> Result<(Pipeline, Needs), IrError> {
	let (pipeline, needs, _) = lower_layer_stack(em, cx, ir, layers, false)?;
	Ok((pipeline, needs))
}

/// Lowers the columns of the layers of a field view that guard a stored field as one host pipeline
/// over the image the host hands it, as [`lower_layers`] lowers every column: the image with each
/// stored field the caller may not select cut, and nothing derived. `None` where no layer has
/// such a column.
pub(in crate::exec::vm::ir_lower) fn lower_stored_guards(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	layers: &[(&SubscriberLayer, &[RegionId])],
) -> Result<Option<(Pipeline, Needs)>, IrError> {
	let (pipeline, needs, guards) = lower_layer_stack(em, cx, ir, layers, true)?;
	Ok((guards > 0).then_some((pipeline, needs)))
}

/// The pipeline of [`lower_layers`], or of [`lower_stored_guards`] where `stored_only`, with the
/// number of stored guards it lowered.
fn lower_layer_stack(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	layers: &[(&SubscriberLayer, &[RegionId])],
	stored_only: bool,
) -> Result<(Pipeline, Needs, usize), IrError> {
	let mut reads = Reads::empty(PoolBase::default());
	let source = Source::Once;
	let mut body = Body {
		em,
		cx,
		ir,
		reads: &mut reads,
		source: &source,
		schema: None,
		objects: true,
		ids: false,
		fetched: None,
		one_record: false,
		end: 0,
		halt: 0,
		jumps: EndJumps::default(),
		limit_checks: Vec::new(),
		pipeline_limits: Vec::new(),
		enforce: None,
		emitted: Emitted::default(),
		claims: Vec::new(),
		needs: Needs::none(),
		current: ROW,
	};
	body.end = body.em.label()?;
	let mut guards = 0;
	for (layer, regions) in layers {
		if stored_only {
			guards += body.stored_guards(regions, &layer.fields, &layer.computed)?;
		} else {
			body.layer(regions, &layer.fields, &layer.computed)?;
		}
	}
	body.em.emit(Instr::Emit {
		src: ROW,
	});
	body.em.bind(body.end)?;
	body.em.emit(Instr::EndRow);
	let pipeline = body.em.finish_pipeline(Source::Once)?;
	Ok((pipeline, body.needs, guards))
}

/// The field a column lands under, which the region `region` reads from the row, and whether it
/// reads it as a `COMPUTED` field.
fn field_read(ir: &Ir, region: RegionId) -> Result<(Strand, bool), IrError> {
	let producer = producer_op(ir, yielded_value(ir, region)?)?;
	match &producer.kind {
		OpKind::Expr(ExprOp::Field {
			name,
		}) => Ok((name.clone(), false)),
		OpKind::Expr(ExprOp::ComputedRead(attrs)) => Ok((attrs.field.clone(), true)),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {
			Err(invariant("a layer column that does not read the field it lands under"))
		}
	}
}

/// The block argument of a column's frame region `region` that holds the value the column judges:
/// the one after the row. The binder gives every layer column one; a region built without it has
/// no guard that reads `$value`.
fn judged_arg(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let held = ir.region(region)?;
	let block = ir.block(*held.blocks.first()?)?;
	block.args.get(usize::from(held.kind.isolated_from_above()) + 1).copied()
}

/// Whether the guard of the column whose frame region is `region` reads the value it judges.
fn reads_judged(ir: &Ir, region: RegionId) -> bool {
	judged_arg(ir, region).is_some_and(|judged| !ir.uses(judged).is_empty())
}

/// The op that defines `value`.
fn producer_op(ir: &Ir, value: ValueId) -> Result<&OpData, IrError> {
	use crate::exec::ir::core::value::ValueDef;
	match ir.value(value).ok_or(IrError::Stale("ir_lower::pipe: value"))?.def {
		ValueDef::OpResult {
			op,
			..
		} => ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op")),
		ValueDef::BlockArg {
			..
		} => Err(invariant("a layer column that is the row itself")),
	}
}

/// The guard and the kept value of a permission column written `IF guard THEN value ELSE NONE`,
/// as the condition and the region of the value; `None` for any other column.
fn judgement_of(ir: &Ir, region: RegionId) -> Result<Option<(ValueId, RegionId)>, IrError> {
	let yielded = yielded_value(ir, region)?;
	let Ok(producer) = producer_op(ir, yielded) else {
		return Ok(None);
	};
	let (OpKind::Expr(ExprOp::If), [condition], [kept, otherwise]) =
		(&producer.kind, producer.operands.as_slice(), producer.regions.as_slice())
	else {
		return Ok(None);
	};
	Ok(yields_none(ir, *otherwise)?.then_some((*condition, *kept)))
}

/// Whether the region `region` yields the literal `NONE`.
fn yields_none(ir: &Ir, region: RegionId) -> Result<bool, IrError> {
	let Ok(producer) = producer_op(ir, yielded_value(ir, region)?) else {
		return Ok(false);
	};
	Ok(
		matches!(&producer.kind, OpKind::Expr(ExprOp::Literal(value)) if matches!(**value, Value::None)),
	)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::{Strand, TableName};

	use super::super::tests::{
		key_range_declaring, lowered, pipeline, placed, scan, scan_declaring, step,
	};
	use crate::exec::ir::expr::ops::{ComputedReadAttrs, ExprOp};
	use crate::exec::ir::iface::CatalogObjectKey;
	use crate::exec::ir::iface::ids::{FrameId, OpId, ValueId};
	use crate::exec::ir::iface::region_kind::{ClauseRole, FieldRole, RegionKind};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::{FieldShape, FieldsShape};
	use crate::exec::ir::phys::ops::{LayerAttrs, PhysOp};
	use crate::exec::logical::footprint::CatalogKind;
	use crate::exec::vm::compile_cache::ObjectVersion;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::program::{FrameMode, Instr, Operand, PermissionSpec};
	use crate::expr::logical::{EvalFrame, GuardKind};
	use crate::expr::{BinaryOperator, Kind};
	use crate::val::Value;

	/// What one column of a layer is built from.
	struct Spec {
		field: &'static str,
		kind: Option<Kind>,
		frame: EvalFrame,
		/// Whether the column's field is read as a `COMPUTED` field.
		computed_read: bool,
		/// The expression of the column, built over the row of its region and the value the column
		/// judges.
		expression: fn(&mut Builder, ValueId, ValueId) -> ValueId,
	}

	/// The field `name` of the row.
	fn field(b: &mut Builder, row: ValueId, name: &str) -> ValueId {
		b.add(
			ExprOp::Field {
				name: Strand::from(name),
			},
			&[row],
		)
	}

	/// The computed field `name` of the row.
	fn computed_field(b: &mut Builder, row: ValueId, name: &str) -> ValueId {
		b.add(
			ExprOp::ComputedRead(Box::new(ComputedReadAttrs {
				field: Strand::from(name),
				def: (
					CatalogObjectKey {
						kind: CatalogKind::Field,
						ns: None,
						db: None,
						table: Some(TableName::from("t")),
						name: Some(Strand::from(name)),
					},
					ObjectVersion::Inline,
				),
			})),
			&[row],
		)
	}

	/// `IF guard THEN <field> ELSE NONE END` over the row, with the guard `guard` builds over the
	/// row and the judged value.
	fn judged_by(
		b: &mut Builder,
		row: ValueId,
		value: ValueId,
		guard: fn(&mut Builder, ValueId, ValueId) -> ValueId,
	) -> ValueId {
		let guard = guard(b, row, value);
		let owner = b.add_with_regions(ExprOp::If, &[guard]);
		b.region_with_args(owner, RegionKind::ValueIf, &[], |b, _| field(b, row, "a"));
		b.region_with_args(owner, RegionKind::ValueIf, &[], |b, _| b.literal(Value::None));
		b.result_of(owner)
	}

	/// `IF owner = 1 THEN <field> ELSE NONE END` over the row: a permission column whose guard
	/// reads a field of the row.
	fn judged(b: &mut Builder, row: ValueId, value: ValueId) -> ValueId {
		judged_by(b, row, value, |b, row, _| {
			let owner = field(b, row, "owner");
			let one = b.literal(Value::from(1_i64));
			b.binary(BinaryOperator::Equal, owner, one)
		})
	}

	/// `IF $value > 1 THEN <field> ELSE NONE END`: a permission column whose guard reads the row as
	/// a whole.
	fn judged_whole(b: &mut Builder, row: ValueId, value: ValueId) -> ValueId {
		judged_by(b, row, value, |b, row, _| {
			let one = b.literal(Value::from(1_i64));
			b.binary(BinaryOperator::MoreThan, row, one)
		})
	}

	/// `IF $value > 1 THEN <field> ELSE NONE END`: a permission column whose guard reads the value
	/// it judges.
	fn judged_value(b: &mut Builder, row: ValueId, value: ValueId) -> ValueId {
		judged_by(b, row, value, |b, _, value| {
			let one = b.literal(Value::from(1_i64));
			b.binary(BinaryOperator::MoreThan, value, one)
		})
	}

	/// A scan whose rows a layer of `specs` lands columns on.
	fn layered(specs: &[Spec]) -> Builder {
		stacked(&[specs], scan)
	}

	/// The rows `source` reads, with one layer of each of `layers` above them, the first nearest.
	/// No layer's input declares a `COMPUTED` field.
	fn stacked(layers: &[&[Spec]], source: fn(&mut Builder) -> ValueId) -> Builder {
		stacked_declaring(layers, source, &[])
	}

	/// As [`stacked`], with each layer's input declaring the fields `computed` `COMPUTED`.
	fn stacked_declaring(
		layers: &[&[Spec]],
		source: fn(&mut Builder) -> ValueId,
		computed: &[&str],
	) -> Builder {
		let mut b = Builder::new();
		let frames: Vec<Vec<FrameId>> = layers
			.iter()
			.map(|specs| specs.iter().map(|spec| b.frame(spec.frame.clone())).collect())
			.collect();
		pipeline(&mut b, 0, |b| {
			let mut rows = source(b);
			for (specs, frames) in layers.iter().zip(&frames) {
				let fields = FieldsShape::Select(
					std::iter::once(FieldShape::All)
						.chain(specs.iter().map(|spec| FieldShape::Single {
							alias: None,
							kind: spec.kind.clone(),
						}))
						.collect(),
				);
				let layer: OpId = step(
					b,
					PhysOp::Layer(Box::new(LayerAttrs {
						fields,
						names: Vec::new(),
						computed: computed.iter().map(|name| Strand::from(*name)).collect(),
					})),
					rows,
					&[],
				);
				for spec in *specs {
					b.region_with_args(
						layer,
						RegionKind::Clause(ClauseRole::Projection),
						&[TypeKnowledge::Unknown],
						|b, args| match spec.computed_read {
							true => computed_field(b, args[0], spec.field),
							false => field(b, args[0], spec.field),
						},
					);
				}
				for (spec, frame) in specs.iter().zip(frames) {
					b.in_frame(*frame, |b| {
						b.region_with_args(
							layer,
							RegionKind::FieldFrame(FieldRole::Computed),
							&[
								TypeKnowledge::Unknown,
								TypeKnowledge::Unknown,
								TypeKnowledge::Unknown,
							],
							|b, args| (spec.expression)(b, args[1], args[2]),
						);
					});
				}
				rows = b.result_of(layer);
			}
			rows
		});
		placed(b)
	}

	fn permission(field: &'static str, kind: Option<Kind>) -> Spec {
		Spec {
			field,
			kind,
			frame: EvalFrame::permission(None),
			computed_read: false,
			expression: judged,
		}
	}

	#[test]
	fn a_permission_column_lands_the_field_it_admits_and_cuts_the_one_it_refuses() {
		let b = layered(&[permission("a", Some(Kind::Int))]);
		let lowered = lowered(&b);
		let program = &lowered.program;
		assert_eq!(
			program.permissions,
			vec![PermissionSpec {
				mode: FrameMode::Guard(GuardKind::Read),
				auth: None,
				column: true,
			}]
		);
		let code = &program.pipelines[0].code;
		let position = |wanted: fn(&Instr) -> bool| code.iter().position(wanted);
		let coerce = position(|instr| matches!(instr, Instr::CoerceField { .. }));
		let guard =
			position(|instr| matches!(instr, Instr::JumpIfCompare { .. } | Instr::JumpIf { .. }));
		let land = position(|instr| matches!(instr, Instr::LandRow { .. }));
		assert!(coerce < guard && guard < land, "{code:?}");
		assert!(code.iter().any(|instr| matches!(
			instr,
			Instr::JumpIfNone {
				none: true,
				..
			}
		)));
		// The cut removes the field from the fields to set and names it in the fields to cut.
		assert_eq!(
			code.iter().filter(|instr| matches!(instr, Instr::RemoveField { .. })).count(),
			2,
			"{code:?}"
		);
		assert!(!code.iter().any(|instr| matches!(instr, Instr::BeginComputing { .. })));
	}

	#[test]
	fn the_columns_of_a_layer_land_together_and_the_row_is_no_more_typed() {
		let b = layered(&[permission("a", None), permission("b", None)]);
		let lowered = lowered(&b);
		let code = &lowered.program.pipelines[0].code;
		assert_eq!(
			code.iter().filter(|instr| matches!(instr, Instr::LandRow { .. })).count(),
			1,
			"{code:?}"
		);
		assert_eq!(
			code.iter().filter(|instr| matches!(instr, Instr::NewObject { .. })).count(),
			2,
			"the fields to set and the fields to cut: {code:?}"
		);
	}

	#[test]
	fn a_computed_column_begins_computing_over_the_reduced_record_after_the_stored_columns() {
		let b = layered(&[
			permission("a", None),
			Spec {
				field: "c",
				kind: None,
				frame: EvalFrame::computed(None),
				computed_read: false,
				expression: |b, row, _| field(b, row, "a"),
			},
		]);
		let lowered = lowered(&b);
		let program = &lowered.program;
		assert_eq!(program.computed_names.len(), 1);
		assert_eq!(program.computed_names[0], vec![Strand::from("c")]);
		let code = &program.pipelines[0].code;
		let reduced = code.iter().find_map(|instr| match instr {
			Instr::LoadRow {
				dst,
			} => Some(*dst),
			_ => None,
		});
		assert!(reduced.is_some(), "{code:?}");
		let begin = code.iter().find_map(|instr| match instr {
			Instr::BeginComputing {
				doc,
				..
			} => Some(*doc),
			_ => None,
		});
		assert_eq!(begin, Some(reduced), "{code:?}");
		// The computed column reads the reduced record and not the row.
		assert!(
			code.iter().any(|instr| matches!(
				instr,
				Instr::Access { src: Operand::Reg(reg), .. } if Some(*reg) == reduced
			)),
			"{code:?}"
		);
	}

	#[test]
	fn a_guard_on_a_column_read_as_a_computed_field_reads_the_value_it_judges() {
		let first = [Spec {
			field: "c",
			kind: None,
			frame: EvalFrame::computed(None),
			computed_read: false,
			expression: |b, row, _| field(b, row, "a"),
		}];
		let second = [Spec {
			computed_read: true,
			expression: judged_value,
			..permission("c", None)
		}];
		let b = stacked(&[&first, &second], scan);
		let lowered = lowered(&b);
		assert!(!lowered.program.pipelines[0].code.is_empty());
		assert!(
			lowered
				.program
				.permissions
				.iter()
				.any(|spec| spec.mode == FrameMode::Guard(GuardKind::Read)),
			"{:?}",
			lowered.program.permissions
		);
	}

	/// The layer's declared set decides whether a guard that reads `$value` judges a computed or a
	/// stored column, whatever source gives the rows: a column the set names is derived before it
	/// is judged, and one it does not name is judged as stored.
	#[test]
	fn a_guard_that_reads_its_value_is_judged_as_its_input_declares() {
		let guarded = [Spec {
			expression: judged_value,
			..permission("c", None)
		}];
		let begins = |b: &Builder| {
			lowered(b).program.pipelines[0]
				.code
				.iter()
				.any(|instr| matches!(instr, Instr::BeginComputing { .. }))
		};
		for source in [scan as fn(&mut Builder) -> ValueId, |b| scan_declaring(b, "c", Kind::Int)] {
			assert!(!begins(&stacked(&[&guarded], source)));
			assert!(begins(&stacked_declaring(&[&guarded], source, &["c"])));
		}
	}

	#[test]
	fn a_guard_that_reads_the_row_as_a_whole_lowers_with_or_without_a_schema() {
		let whole = [Spec {
			expression: judged_whole,
			..permission("a", None)
		}];
		// The guard's `$value` is the judged value and not the row, so a read of the row as a whole
		// is a read of `$this` whether or not the column is a stored one.
		let b = stacked(&[&whole], scan);
		assert!(!lowered(&b).program.pipelines[0].code.is_empty());
		let b = stacked(&[&whole], |b| scan_declaring(b, "a", Kind::Int));
		assert!(!lowered(&b).program.pipelines[0].code.is_empty());
	}

	#[test]
	fn a_key_range_scan_holds_the_schema_its_table_declares() {
		let whole = [Spec {
			expression: judged_whole,
			..permission("a", None)
		}];
		// A key-range scan is judged as a table scan is: it holds its table's schema.
		let b = stacked(&[&whole], |b| key_range_declaring(b, "a", Kind::Int));
		assert!(!lowered(&b).program.pipelines[0].code.is_empty());
	}
}
