//! `phys.project`: the row a projection builds, for the code that holds a row to project.
//!
//! A projection is lowered over the row in [`ROW`]. Every column is a region of the op: either the
//! value of a field, or a call to a projection function that names the fields it writes itself.
//! The code that holds the row (a pipeline body, or a host pipeline over the image of a record)
//! passes what lowering a region needs as a [`ProjectionSite`]. One definition of the object a
//! projection builds therefore serves both.
//!
//! The row is an object only where the site says so, and the projection reads its fields itself.
//! A `*` over rows that may not be objects tests the row first. An object gets the fields written
//! onto it. Any other row is projected by the rules in [`project`]. An `OMIT` path that is not a
//! plain path is walked by the host, and each expression in the walk is a host pipeline over the
//! value the walk applies it to.

use std::sync::Arc;

use super::{first_arg, producer_of, yielded_value};
use crate::exec::ContextLevel;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{DestructureEntry, ExprOp};
use crate::exec::ir::iface::ids::{RegionId, ValueId};
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::{
	OmissionShape, ProjectedFieldShape, ProjectionShape, WalkActionShape, WalkStepShape,
};
use crate::exec::vm::ir_lower::emit::Emitter;
use crate::exec::vm::ir_lower::expr::{Lowerer, call_args, ends_context};
use crate::exec::vm::payload::{Walk, WalkAction, WalkStep};
use crate::exec::vm::program::{CallSpec, Instr, Operand, ROW, Reg, ReturnCatch};
use crate::expr::Function;
use crate::expr::logical::{LogicalDestructureField, LogicalExpr, LogicalLiteral};

/// What lowering a projection needs of the code that holds the row it projects.
pub(super) trait ProjectionSite {
	/// The emitter the projection's instructions go to.
	fn emitter(&mut self) -> &mut Emitter;

	/// The function registry the projection's calls are resolved against.
	fn registry(&self) -> &FunctionRegistry;

	/// Whether the rows are records, whose fields the projection reads itself.
	fn objects(&self) -> bool;

	/// The value the clause region `region` yields, evaluated against the row in `row`. A
	/// statement held in the value takes a `RETURN` it signals as its own value ([`PROJECTED`]).
	fn clause_value(&mut self, region: RegionId, row: Reg) -> Result<Operand, IrError>;

	/// The operands for `args`, the values the call that the region `region` yields is applied
	/// to, read from the row in [`ROW`].
	///
	/// Where a `RETURN` held in the region ends it ([`ends_context`]), the operands are lowered as
	/// the evaluation of a context, and the catch they are lowered under is answered with them: a
	/// `RETURN` leaves its value in the catch's register and jumps to its label, which the caller
	/// binds past the call.
	fn call_operands(
		&mut self,
		region: RegionId,
		args: &[ValueId],
	) -> Result<(Vec<Operand>, Option<ReturnCatch>), IrError>;

	/// Records that the projection calls a function that needs at least `context`, and keeps the
	/// order of the rows it is applied to.
	fn needs_call(&mut self, context: ContextLevel);

	/// The register that holds the value of the key `key` of the sort that handed over the rows,
	/// for a site whose rows come from a sort.
	fn sort_key(&mut self, key: u16) -> Result<Reg, IrError>;

	/// The register that holds the record a `*` beside fields reads in place of the row, where the
	/// row is a record id the site fetched; the fields then read the record.
	fn fetched(&self) -> Option<Reg>;

	/// The host pipeline that emits the value the region `region` yields over the value the host
	/// hands it, which the region's one block argument names.
	fn host_over_value(&mut self, region: RegionId) -> Result<u16, IrError>;
}

/// The image of a record as the row of a host pipeline: `lowerer` lowers the pipeline, whose row is
/// the image in [`ROW`] and is an object when `objects`.
struct ImageSite<'l, 'a> {
	lowerer: &'l mut Lowerer<'a>,
	objects: bool,
}

impl ProjectionSite for ImageSite<'_, '_> {
	fn emitter(&mut self) -> &mut Emitter {
		self.lowerer.em
	}

	fn registry(&self) -> &FunctionRegistry {
		self.lowerer.registry()
	}

	fn objects(&self) -> bool {
		self.objects
	}

	fn clause_value(&mut self, region: RegionId, row: Reg) -> Result<Operand, IrError> {
		if let Some(arg) = first_arg(self.lowerer.ir, region) {
			self.lowerer.bind_arg(arg, Operand::Reg(row));
			if row == ROW {
				self.lowerer.bind_row(arg, self.objects);
			}
		}
		self.lowerer.region_value(region)
	}

	/// The operands are lowered in a scope of their own, which is forgotten once they are read:
	/// a projection may lower the same call on two paths of which one runs.
	fn call_operands(
		&mut self,
		region: RegionId,
		args: &[ValueId],
	) -> Result<(Vec<Operand>, Option<ReturnCatch>), IrError> {
		let held = self.lowerer.scope();
		if let Some(arg) = first_arg(self.lowerer.ir, region) {
			self.lowerer.bind_arg(arg, Operand::Reg(ROW));
			self.lowerer.bind_row(arg, self.objects);
		}
		let operands = operands_in(self.lowerer, region, args);
		self.lowerer.restore_scope(held);
		operands
	}

	fn needs_call(&mut self, context: ContextLevel) {
		self.lowerer.requires(context);
		self.lowerer.keeps_row_order = true;
	}

	fn sort_key(&mut self, _key: u16) -> Result<Reg, IrError> {
		Err(invariant("a sort key read in a pipeline over the image of a record"))
	}

	fn fetched(&self) -> Option<Reg> {
		None
	}

	fn host_over_value(&mut self, region: RegionId) -> Result<u16, IrError> {
		value_pipeline(self.lowerer, region)
	}
}

/// Lowers the projection `shape`, whose regions are `regions`, over the image of a record that is
/// the row of the host pipeline `lowerer` lowers, and answers the register that holds the
/// projected value. The image is an object when `objects`.
pub(in crate::exec::vm::ir_lower) fn project_image(
	lowerer: &mut Lowerer<'_>,
	shape: &ProjectionShape,
	regions: &[RegionId],
	objects: bool,
) -> Result<Reg, IrError> {
	let ir = lowerer.ir;
	project(
		&mut ImageSite {
			lowerer,
			objects,
		},
		ir,
		shape,
		regions,
	)
}

/// Lowers the projection `shape`, whose regions are `regions`, over the row in [`ROW`], and answers
/// the register that holds the projected row.
pub(super) fn project(
	site: &mut dyn ProjectionSite,
	ir: &Ir,
	shape: &ProjectionShape,
	regions: &[RegionId],
) -> Result<Reg, IrError> {
	match shape {
		ProjectionShape::All => Ok(ROW),
		ProjectionShape::SortedValue(key) => site.sort_key(*key),
		ProjectionShape::Value => {
			let [region] = <[RegionId; 1]>::try_from(regions)
				.map_err(|_| invariant("a value projection with other than one region"))?;
			let value = site.clause_value(region, ROW)?;
			in_register(site.emitter(), value)
		}
		ProjectionShape::OmittedValue {
			omit,
		} => {
			let (region, omissions) = regions
				.split_first()
				.ok_or_else(|| invariant("a value projection with no region"))?;
			let object = site.emitter().reg()?;
			site.emitter().emit(Instr::LoadRow {
				dst: object,
			});
			let objects = site.objects();
			omit_all(site, object, omit, omissions, objects)?;
			// The value reads the row as it stands without the omitted paths.
			let value = site.clause_value(*region, object)?;
			in_register(site.emitter(), value)
		}
		ProjectionShape::Fields {
			fields,
			omit,
		} => {
			let object = site.emitter().reg()?;
			if !site.objects() && !fields_make_an_object(site, ir, fields, regions)? {
				fields_over_any_row(site, ir, object, fields, omit, regions)?;
				return Ok(object);
			}
			site.emitter().emit(Instr::NewObject {
				dst: object,
			});
			write_fields(site, ir, object, fields, omit, regions)?;
			Ok(object)
		}
		ProjectionShape::Star {
			fields,
			omit,
		} => {
			let object = site.emitter().reg()?;
			let load = match site.fetched() {
				Some(fetched) => Instr::Load {
					dst: object,
					src: Operand::Reg(fetched),
				},
				None => Instr::LoadRow {
					dst: object,
				},
			};
			site.emitter().emit(load);
			if site.objects() {
				write_fields(site, ir, object, fields, omit, regions)?;
				return Ok(object);
			}
			star_over_any_row(site, ir, object, fields, omit, regions)?;
			Ok(object)
		}
	}
}

/// `*` and `fields` over the row loaded into `object`, which may not be an object.
///
/// An object has the fields written onto it and the omissions removed. A row that is not an object
/// stands as it is beside fields that only read it, and has its omissions walked. A field that
/// computes a value makes an object of the row (a geometry as its members, any other value as
/// nothing) to write the fields onto, and one the full projection builds (a projection function,
/// or an output path of more than one part) makes an object of the fields alone. Each arm lowers
/// the fields in a scope of its own, since only one of them runs for a row.
fn star_over_any_row(
	site: &mut dyn ProjectionSite,
	ir: &Ir,
	object: Reg,
	fields: &[ProjectedFieldShape],
	omit: &[OmissionShape],
	regions: &[RegionId],
) -> Result<(), IrError> {
	let (builds, computes) = field_effects(site, ir, fields, regions)?;
	let not_object = site.emitter().label()?;
	let done = site.emitter().label()?;
	site.emitter().emit(Instr::JumpIfNotObject {
		cond: Operand::Reg(object),
		target: not_object,
	});
	write_fields(site, ir, object, fields, omit, regions)?;
	site.emitter().emit(Instr::Jump {
		target: done,
	});
	site.emitter().bind(not_object)?;
	if builds {
		site.emitter().emit(Instr::NewObject {
			dst: object,
		});
		write_fields(site, ir, object, fields, omit, regions)?;
	} else if computes {
		site.emitter().emit(Instr::SeedObject {
			dst: object,
		});
		write_fields(site, ir, object, fields, omit, regions)?;
	} else {
		let omissions = regions
			.get(fields.len()..)
			.ok_or_else(|| invariant("a projection with fewer regions than fields"))?;
		omit_all(site, object, omit, omissions, false)?;
	}
	site.emitter().bind(done)
}

/// Whether any of `fields` is written by a projection function or an output path of more than one
/// part (`builds`), and whether any computes a value rather than reading a field of the row
/// (`computes`).
fn field_effects(
	site: &mut dyn ProjectionSite,
	ir: &Ir,
	fields: &[ProjectedFieldShape],
	regions: &[RegionId],
) -> Result<(bool, bool), IrError> {
	let field_regions = regions
		.get(..fields.len())
		.ok_or_else(|| invariant("a projection with fewer regions than fields"))?;
	let builds = fields.iter().zip(field_regions).any(|(field, region)| {
		projection_call(ir, site.registry(), *region).is_some() || field.output.0.len() > 1
	});
	let computes = fields
		.iter()
		.zip(field_regions)
		.any(|(field, region)| !reads_a_field(ir, *region) || field.output.0.len() > 1);
	Ok((builds, computes))
}

/// Whether `fields` make an object of any row: a field that computes a value or builds an output
/// path does, and fields that only read the row do for an object, a geometry or `NONE`.
pub(super) fn fields_make_an_object(
	site: &mut dyn ProjectionSite,
	ir: &Ir,
	fields: &[ProjectedFieldShape],
	regions: &[RegionId],
) -> Result<bool, IrError> {
	let (builds, computes) = field_effects(site, ir, fields, regions)?;
	Ok(builds || computes)
}

/// `fields` that only read their row, over the row loaded into `object`, which may not be an
/// object.
///
/// An object, a geometry or `NONE` projects to an object of the fields. Any other row stands as it
/// is, with its omissions walked.
fn fields_over_any_row(
	site: &mut dyn ProjectionSite,
	ir: &Ir,
	object: Reg,
	fields: &[ProjectedFieldShape],
	omit: &[OmissionShape],
	regions: &[RegionId],
) -> Result<(), IrError> {
	site.emitter().emit(Instr::LoadRow {
		dst: object,
	});
	let opaque = site.emitter().label()?;
	let done = site.emitter().label()?;
	site.emitter().emit(Instr::JumpIfOpaque {
		cond: Operand::Reg(object),
		target: opaque,
	});
	site.emitter().emit(Instr::NewObject {
		dst: object,
	});
	write_fields(site, ir, object, fields, omit, regions)?;
	site.emitter().emit(Instr::Jump {
		target: done,
	});
	site.emitter().bind(opaque)?;
	let omissions = regions
		.get(fields.len()..)
		.ok_or_else(|| invariant("a projection with fewer regions than fields"))?;
	omit_all(site, object, omit, omissions, false)?;
	site.emitter().bind(done)
}

/// Whether the region `region` yields a field of its row as it stands: a stored field, or the
/// memo of a computed one.
fn reads_a_field(ir: &Ir, region: RegionId) -> bool {
	let Ok(yielded) = yielded_value(ir, region) else {
		return false;
	};
	let Some(data) = producer_of(ir, yielded).ok().and_then(|op| ir.op(op)) else {
		return false;
	};
	matches!(data.kind, OpKind::Expr(ExprOp::Field { .. } | ExprOp::ComputedRead(_)))
		&& data.operands.first().copied() == first_arg(ir, region)
}

/// The register that holds `value`, loading it where it is not in one.
fn in_register(em: &mut Emitter, value: Operand) -> Result<Reg, IrError> {
	match value {
		Operand::Reg(reg) => Ok(reg),
		other @ (Operand::Const(_) | Operand::Field(_)) => {
			let dst = em.reg()?;
			em.emit(Instr::Load {
				dst,
				src: other,
			});
			Ok(dst)
		}
	}
}

/// Sets each of `fields` on the object in `object`, then removes the omitted paths from it.
///
/// The regions of the projection are one per field in order, then the omissions'. A field that
/// calls a projection function is written by the call, which names its own output.
fn write_fields(
	site: &mut dyn ProjectionSite,
	ir: &Ir,
	object: Reg,
	fields: &[ProjectedFieldShape],
	omit: &[OmissionShape],
	regions: &[RegionId],
) -> Result<(), IrError> {
	let (field_regions, omissions) = regions
		.split_at_checked(fields.len())
		.ok_or_else(|| invariant("a projection with fewer regions than fields"))?;
	for (field, region) in fields.iter().zip(field_regions) {
		let path = site.emitter().path(field.output.clone())?;
		if let Some(key) = field.sort_key {
			let value = site.sort_key(key)?;
			site.emitter().emit(Instr::SetField {
				object,
				path,
				value: Operand::Reg(value),
			});
			continue;
		}
		if let Some((name, args)) = projection_call(ir, site.registry(), *region) {
			write_projection(site, *region, &name, &args, object, path, field.aliased)?;
			continue;
		}
		let value = site.clause_value(*region, ROW)?;
		site.emitter().emit(Instr::SetField {
			object,
			path,
			value,
		});
	}
	omit_all(site, object, omit, omissions, true)
}

/// The write of a projection function's output into `object`, from the call the region `region`
/// yields: its arguments are read from the row, and the function names the fields it writes unless
/// the field is aliased.
fn write_projection(
	site: &mut dyn ProjectionSite,
	region: RegionId,
	name: &str,
	args: &[ValueId],
	object: Reg,
	path: u32,
	aliased: bool,
) -> Result<(), IrError> {
	let context = site
		.registry()
		.get_projection(name)
		.map_or(ContextLevel::Database, |projection| projection.required_context());
	site.needs_call(context);
	let call = site.emitter().call(CallSpec {
		name: name.to_owned(),
		plan_depth: 0,
		index: None,
		closure_args: Vec::new(),
	})?;
	let (args, returned) = site.call_operands(region, args)?;
	let em = site.emitter();
	em.emit(Instr::WriteProjection {
		call,
		args,
		object,
		path,
		aliased,
	});
	// A `RETURN` in an argument ends the field's evaluation: its value is the field's, and the
	// call is not made.
	if let Some(catch) = returned {
		let past = em.label()?;
		em.emit(Instr::Jump {
			target: past,
		});
		em.bind(catch.label)?;
		em.emit(Instr::SetField {
			object,
			path,
			value: Operand::Reg(catch.value),
		});
		em.bind(past)?;
	}
	Ok(())
}

/// The operands for `args`, lowered by `lowerer` in the region `region`, as
/// [`ProjectionSite::call_operands`] answers them.
pub(super) fn operands_in(
	lowerer: &mut Lowerer<'_>,
	region: RegionId,
	args: &[ValueId],
) -> Result<(Vec<Operand>, Option<ReturnCatch>), IrError> {
	let lower = |lowerer: &mut Lowerer<'_>| {
		args.iter().map(|arg| lowerer.value(*arg)).collect::<Result<Vec<_>, _>>()
	};
	match ends_context(lowerer.ir, region) {
		true => lowerer.context_catch(lower).map(|(operands, catch)| (operands, Some(catch))),
		false => lower(lowerer).map(|operands| (operands, None)),
	}
}

/// Removes each omission from the value in `object`, which is an object when `object_only`.
///
/// A plain path is removed from an object in place and any other is walked by the host. `regions`
/// are the regions the walks hold, in the order the omissions list them: each walk's source, which
/// the walk does not read since its steps name the locations, then a region for each evaluated
/// position, element filter and aliased destructure entry. A position and a filter are host
/// pipelines over the value the walk applies them to; an aliased entry names no location, so the
/// host never evaluates its body.
fn omit_all(
	site: &mut dyn ProjectionSite,
	object: Reg,
	omit: &[OmissionShape],
	regions: &[RegionId],
	object_only: bool,
) -> Result<(), IrError> {
	let mut regions = regions.iter().copied();
	for omission in omit {
		let path = match omission {
			OmissionShape::Path(path) => path,
			OmissionShape::Walk {
				steps,
				action,
			} => {
				next_region(&mut regions)?;
				let walk = walk_of(site, steps, action, &mut regions)?;
				site.emitter().emit(Instr::Omit {
					object,
					walk: Arc::new(walk),
				});
				continue;
			}
		};
		let em = site.emitter();
		match path.0.as_slice() {
			[FieldPathPart::Field(name)] if object_only => {
				let name = em.field(name)?;
				em.emit(Instr::RemoveField {
					object,
					name,
				});
			}
			_ if object_only => {
				let path = em.path(path.clone())?;
				em.emit(Instr::RemovePath {
					object,
					path,
				});
			}
			_ => {
				em.emit(Instr::Omit {
					object,
					walk: Arc::new(walk_along(path)),
				});
			}
		}
	}
	if regions.next().is_some() {
		return Err(invariant("an omission region no walk reads"));
	}
	Ok(())
}

/// The host pipeline that emits the value the region `region` yields over the value the host hands
/// it, which the region's one block argument names, built by `lowerer` beside the pipeline in
/// flight.
///
/// The value is of any kind, as an element of an array or a position's `NONE` is. A field of it is
/// read as the field of a value, and a record id in it is fetched.
pub(super) fn value_pipeline(lowerer: &mut Lowerer<'_>, region: RegionId) -> Result<u16, IrError> {
	let [value] = lowerer.region_args(region)?[..] else {
		return Err(invariant("an omission's expression region with other than one argument"));
	};
	let aside = lowerer.em.set_aside();
	let beside = lowerer.beside();
	lowerer.bind_row(value, false);
	let built = lowerer.region_value(region).and_then(|value| lowerer.emit_value_pipeline(value));
	lowerer.rejoin(beside);
	lowerer.em.restore(aside);
	lowerer.em.host_code(built?)
}

/// The next region of an omission's walk.
fn next_region(regions: &mut impl Iterator<Item = RegionId>) -> Result<RegionId, IrError> {
	regions.next().ok_or_else(|| invariant("an omission walk with fewer regions than it reads"))
}

/// Lowers the walk `steps` and `action`. Each expression the walk evaluates takes the host pipeline
/// of the next region, in order.
fn walk_of(
	site: &mut dyn ProjectionSite,
	steps: &[WalkStepShape],
	action: &WalkActionShape,
	regions: &mut impl Iterator<Item = RegionId>,
) -> Result<Walk<u16>, IrError> {
	let mut walked = Vec::with_capacity(steps.len());
	for step in steps {
		walked.push(match step {
			WalkStepShape::Field(name) => WalkStep::Field(name.clone()),
			WalkStepShape::Index(at) => WalkStep::Index(*at),
			WalkStepShape::IndexAt => {
				WalkStep::IndexAt(site.host_over_value(next_region(regions)?)?)
			}
			WalkStepShape::Matching => {
				WalkStep::Matching(site.host_over_value(next_region(regions)?)?)
			}
			WalkStepShape::Each => WalkStep::Each,
			WalkStepShape::First => WalkStep::First,
			WalkStepShape::Last => WalkStep::Last,
			WalkStepShape::Destructure(entries) => {
				WalkStep::Destructure(destructure_fields(entries, regions)?)
			}
			WalkStepShape::Nowhere => WalkStep::Nowhere,
		});
	}
	let action = match action {
		WalkActionShape::RemoveField(name) => WalkAction::RemoveField(name.clone()),
		WalkActionShape::RemoveIndex(at) => WalkAction::RemoveIndex(*at),
		WalkActionShape::RemoveIndexAt => {
			WalkAction::RemoveIndexAt(site.host_over_value(next_region(regions)?)?)
		}
		WalkActionShape::RemoveMatching => {
			WalkAction::RemoveMatching(site.host_over_value(next_region(regions)?)?)
		}
		WalkActionShape::RemoveFirst => WalkAction::RemoveFirst,
		WalkActionShape::RemoveLast => WalkAction::RemoveLast,
		WalkActionShape::Clear => WalkAction::Clear,
		WalkActionShape::Destructure(entries) => {
			WalkAction::Destructure(destructure_fields(entries, regions)?)
		}
		WalkActionShape::Nothing => WalkAction::Nothing,
	};
	Ok(Walk {
		steps: walked,
		action,
	})
}

/// The fields of a destructure a walk lists, in depth-first order. An aliased entry takes the
/// next region and names no location: the host skips it on a step and raises the destructure's
/// error on the action, and never evaluates its body, which is held as `NONE`.
fn destructure_fields(
	entries: &[DestructureEntry],
	regions: &mut impl Iterator<Item = RegionId>,
) -> Result<Vec<LogicalDestructureField>, IrError> {
	let mut fields = Vec::with_capacity(entries.len());
	for entry in entries {
		fields.push(match entry {
			DestructureEntry::All(name) => LogicalDestructureField::All(name.clone()),
			DestructureEntry::Field(name) => LogicalDestructureField::Field(name.clone()),
			DestructureEntry::Aliased(name) => {
				next_region(regions)?;
				LogicalDestructureField::Aliased(
					name.clone(),
					LogicalExpr::Literal(LogicalLiteral::None),
				)
			}
			DestructureEntry::Destructure(name, nested) => LogicalDestructureField::Destructure(
				name.clone(),
				destructure_fields(nested, regions)?,
			),
			DestructureEntry::Rest => LogicalDestructureField::Rest,
		});
	}
	Ok(fields)
}

/// The projection function the region `region` yields a call of, with the arguments of the call.
fn projection_call(
	ir: &Ir,
	registry: &FunctionRegistry,
	region: RegionId,
) -> Option<(String, Vec<ValueId>)> {
	let yielded = yielded_value(ir, region).ok()?;
	let op = producer_of(ir, yielded).ok()?;
	let data = ir.op(op)?;
	if let OpKind::Expr(ExprOp::Call(function)) = &data.kind
		&& let Function::Normal(name) = &function.function
		&& registry.is_projection(name)
	{
		let arguments = call_args(data, function).ok()?;
		return Some((name.clone(), arguments.to_vec()));
	}
	None
}

/// The walk that removes the value at `path`: the fields and indices before the last part reach
/// it, and the last part is what is removed.
fn walk_along(path: &FieldPath) -> Walk<u16> {
	let Some((last, reach)) = path.0.split_last() else {
		return Walk {
			steps: Vec::new(),
			action: WalkAction::Nothing,
		};
	};
	let steps = reach
		.iter()
		.map(|part| match part {
			FieldPathPart::Field(name) => WalkStep::Field(name.clone()),
			FieldPathPart::Index(index) => WalkStep::Index(*index),
			FieldPathPart::First => WalkStep::First,
			FieldPathPart::Last => WalkStep::Last,
			FieldPathPart::Lookup(_) => WalkStep::Nowhere,
		})
		.collect();
	Walk {
		steps,
		action: match last {
			FieldPathPart::Field(name) => WalkAction::RemoveField(name.clone()),
			FieldPathPart::Index(index) => WalkAction::RemoveIndex(*index),
			FieldPathPart::First => WalkAction::RemoveFirst,
			FieldPathPart::Last => WalkAction::RemoveLast,
			FieldPathPart::Lookup(_) => WalkAction::Nothing,
		},
	}
}
