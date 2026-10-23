//! The notification unit of a live subscription, lowered from the container
//! [`bind_subscription`](crate::exec::ir::build::subscription::bind_subscription) builds.
//!
//! The top region of the container is a [`SubscriberFrame`](RegionKind::SubscriberFrame) holding
//! one `phys.subscriber` op and the yield that ends the region. The op states what the subscriber
//! is shown ([`SubscriberAttrs`]) and hosts the clauses that decide it, as regions in the order
//! [`SubscriberAttrs::region_kinds`] lists them. The unit is a program of host pipelines and no
//! pipeline of its own: a notification runs the pipelines the plan names over the images of the
//! record it is about, in the subscriber's frame.
//!
//! - **Admission.** `FULL`, and a subscriber whose reads are not checked, admit every record, and
//!   `NONE` admits none. A predicate is the `PermissionFrame` region: a host pipeline over a copy
//!   of the image, judged in the read gate frame of its guard, whose `COMPUTED` fields the
//!   predicate reads are derived onto the copy first, one `FieldFrame(Computed)` region each. A
//!   predicate is judged only for a checked subscriber. A record the predicate does not admit is
//!   never notified.
//! - **Field view.** A plain view, and the view of a table whose only cut fields belong to a
//!   subscriber whose reads are not checked, change nothing and hold no pipeline. Any other view is
//!   a stack of layers, which the op holds as regions after its admission's: one host pipeline
//!   ([`lower_layers`]) over the image the host hands it, which lands each layer's columns on the
//!   image before the next layer reads it. A view whose layers the op does not hold is a broken
//!   container, never an image shown with a field cut or derived by nothing.
//! - **`WHERE`.** The `Clause(Where)` region: a host pipeline over the image the notification is
//!   about, as an object.
//! - **Projection.** `DIFF` and `*` hold no pipeline, as the run builds the patch and the record
//!   itself. Any other projection is the host pipeline [`project_image`] lowers over the image.
//! - **`FETCH`.** Each path is the steps its region reads from the image, which the host walks over
//!   what the projection made.
//!
//! The eight names of the subscriber's frame are the top block's arguments after the statement
//! timestamp. Each is read by its name through the frame of the run, in every pipeline of the unit.

use std::sync::Arc;

use surrealdb_strand::Strand;

use super::dispatch::apply_needs;
use super::emit::Emitter;
use super::expr::{Lowerer, Needs};
use super::pipe::{lower_layers, path_steps, project_image};
use super::place::{Lowered, Provenance};
use super::stage::gate_pipeline;
use super::units::LowerCx;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::conversion::Phase;
use crate::exec::ir::iface::ids::RegionId;
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::{
	DerivedField, ImageView, SubscriberAdmission, SubscriberAttrs, SubscriberProjection,
};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::conversion::projection_shape;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::ir::unit::SUBSCRIBER_NAMES;
use crate::exec::vm::live::LiveUnit;
use crate::exec::vm::program::{Instr, Pipeline, Reg, Source};
use crate::exec::vm::write::{Admission, LoweredSubscription, SubscriptionFieldsLowered};
use crate::expr::logical::GuardKind;

/// A subscription's unit as lowered: the program of its host pipelines, with the units it asks the
/// compile driver for, and the plan that names its pipelines.
#[derive(Debug)]
pub(crate) struct LoweredLive {
	/// The program and the requests the lowering made.
	pub(crate) lowered: Lowered,
	/// What the notification runs of the program.
	pub(crate) plan: LoweredSubscription,
}

impl LoweredLive {
	/// The unit a run holds, once the compile driver has filled the program's unit table.
	pub(crate) fn into_unit(self) -> LiveUnit {
		LiveUnit {
			program: Arc::new(self.lowered.program),
			plan: self.plan,
		}
	}
}

/// Lowers the physical container `ir`, whose top region is a subscriber frame, to the unit a
/// subscription runs for each record of its table.
///
/// The compile driver hands over only a container its planning moved past the physical boundary,
/// so one short of it is an invariant break.
pub(crate) fn lower_subscriber(cx: &mut LowerCx<'_>, ir: &Ir) -> Result<LoweredLive, IrError> {
	if ir.phase() < Phase::Physical {
		return Err(IrError::Invariant {
			what: "ir_lower::subscription: a subscription's container is not physical",
		});
	}
	let top = ir.top();
	if ir.region(top).map(|region| region.kind) != Some(RegionKind::SubscriberFrame) {
		return Err(IrError::Invariant {
			what: "ir_lower::subscription: a subscription's container does not top a subscriber frame",
		});
	}
	let ops: Vec<_> = ops_in(ir, top).collect();
	let [subscriber, end] = <[_; 2]>::try_from(ops).map_err(|_| IrError::Invariant {
		what: "ir_lower::subscription: a subscriber frame holds the subscriber and its yield alone",
	})?;
	let data = ir.op(subscriber).ok_or(IrError::Stale("ir_lower::subscription: op"))?;
	let OpKind::Phys(PhysOp::Subscriber(attrs)) = &data.kind else {
		return Err(IrError::Invariant {
			what: "ir_lower::subscription: a subscriber frame holds no subscriber",
		});
	};
	if !matches!(
		ir.op(end).map(|data| (&data.kind, data.operands.len())),
		Some((OpKind::Phys(PhysOp::Yield), 0))
	) {
		return Err(IrError::Invariant {
			what: "ir_lower::subscription: a subscriber frame does not end in a yield of nothing",
		});
	}
	if data.regions.len() != attrs.region_kinds().len() {
		return Err(IrError::Stale("ir_lower::subscription: the regions of a subscriber"));
	}
	let layered = shown_view(attrs)?;
	if layered == attrs.layers.is_empty() {
		return Err(IrError::Invariant {
			what: "ir_lower::subscription: a subscriber holds the layers its field view calls for",
		});
	}
	let regions = data.regions.as_slice();
	let (judging, rest) = regions.split_at(attrs.admitted());
	let (viewing, rest) = rest.split_at(attrs.viewed());

	let mut em = Emitter::new();
	let mut needs = Needs::none();
	let view = if layered {
		let mut held = viewing;
		let mut layers = Vec::with_capacity(attrs.layers.len());
		for layer in &attrs.layers {
			let (columns, after) = held.split_at(layer.regions());
			layers.push((layer, columns));
			held = after;
		}
		let (pipeline, held) = lower_layers(&mut em, cx, ir, &layers)?;
		needs = needs.merged(&held);
		Some(em.host_code(pipeline)?)
	} else {
		None
	};
	let plan = {
		let mut lowerer = Lowerer::in_context(&mut em, ir, cx);
		let names = lowerer.region_args(top)?;
		if names.len() != 1 + SUBSCRIBER_NAMES.len() {
			return Err(IrError::Invariant {
				what: "ir_lower::subscription: a subscriber frame does not take the names of its frame",
			});
		}
		for (arg, name) in names.iter().skip(1).zip(SUBSCRIBER_NAMES) {
			lowerer.bind_named_arg(*arg, Strand::new(name));
		}
		let admission = admission(&mut lowerer, attrs, &mut judging.iter().copied())?;
		let mut rest = rest.iter().copied();
		let cond = match attrs.cond {
			true => {
				let region = next(&mut rest)?;
				Some(host(&mut lowerer, |lowerer| lowerer.host_code_over_row(region))?)
			}
			false => None,
		};
		let fields = projection(&mut lowerer, attrs, &mut rest)?;
		let mut fetch = Vec::with_capacity(usize::from(attrs.fetch));
		for _ in 0..attrs.fetch {
			let region = next(&mut rest)?;
			fetch.push(path_steps(&mut lowerer, ir, region)?);
		}
		if rest.next().is_some() {
			return Err(IrError::Stale("ir_lower::subscription: a region no clause reads"));
		}
		needs = needs.merged(&lowerer.needs());
		LoweredSubscription {
			admission,
			view,
			cond,
			fields,
			fetch,
		}
	};
	let mut program = em.finish_program(Vec::new(), needs.access_mode)?;
	program.scalar = true;
	apply_needs(&mut program, &needs);
	Ok(LoweredLive {
		lowered: Lowered {
			program,
			provenance: Provenance::default(),
			needs: cx.take_needed(),
		},
		plan,
	})
}

/// Whether the field view the subscriber is shown is a stack of layers: a plain view is not, and
/// neither is a view that only cuts for a subscriber whose reads are not checked, which changes
/// nothing for it.
fn shown_view(attrs: &SubscriberAttrs) -> Result<bool, IrError> {
	match (attrs.view, attrs.checked) {
		(ImageView::Plain, _) | (ImageView::Cut, false) => Ok(false),
		(ImageView::Cut | ImageView::Derived, _) => Ok(true),
		(ImageView::Dynamic, _) => Err(IrError::Invariant {
			what: "ir_lower::subscription: a subscriber that names no one table's view",
		}),
		(ImageView::LateBound, _) => Err(IrError::LateBind {
			what: "ir_lower::subscription: the fields of a table read before a catalog change",
		}),
	}
}

/// The next region of the op the clauses are read from.
fn next(regions: &mut impl Iterator<Item = RegionId>) -> Result<RegionId, IrError> {
	regions.next().ok_or(IrError::Invariant {
		what: "ir_lower::subscription: a subscriber hosts a region for each clause it states",
	})
}

/// A host pipeline, built by `build` beside the code in flight, that holds none of its registers.
/// Answers its index among the program's host pipelines.
fn host(
	lowerer: &mut Lowerer<'_>,
	build: impl FnOnce(&mut Lowerer<'_>) -> Result<u16, IrError>,
) -> Result<u16, IrError> {
	let aside = lowerer.em.set_aside();
	let beside = lowerer.beside();
	let built = build(lowerer);
	lowerer.rejoin(beside);
	lowerer.em.restore(aside);
	built
}

/// What the table's `PERMISSIONS FOR select` shows the subscriber, and the host pipeline that
/// judges a predicate.
///
/// A predicate states a subscriber whose reads are checked: one that is not is admitted whole, so
/// a predicate beside it is a broken invariant, never a verdict that skips the check.
fn admission(
	lowerer: &mut Lowerer<'_>,
	attrs: &SubscriberAttrs,
	regions: &mut impl Iterator<Item = RegionId>,
) -> Result<Admission, IrError> {
	match &attrs.admission {
		SubscriberAdmission::All => Ok(Admission::All),
		SubscriberAdmission::Nothing => Ok(Admission::Nothing),
		SubscriberAdmission::Judge {
			derived,
		} => {
			if !attrs.checked {
				return Err(IrError::Invariant {
					what: "ir_lower::subscription: a predicate for a subscriber whose reads are not checked",
				});
			}
			let predicate = next(regions)?;
			let fields = derived
				.iter()
				.map(|field| next(regions).map(|region| (field, region)))
				.collect::<Result<Vec<(&DerivedField, RegionId)>, IrError>>()?;
			let pipeline = host(lowerer, |lowerer| {
				host_pipeline(gate_pipeline(lowerer, predicate, GuardKind::Read, &fields), lowerer)
			})?;
			Ok(Admission::Judge(pipeline))
		}
	}
}

/// What a notification carries: the patch, the record, or the host pipeline that projects the
/// image the notification is about.
fn projection(
	lowerer: &mut Lowerer<'_>,
	attrs: &SubscriberAttrs,
	regions: &mut impl Iterator<Item = RegionId>,
) -> Result<SubscriptionFieldsLowered, IrError> {
	match &attrs.projection {
		SubscriberProjection::Diff => Ok(SubscriptionFieldsLowered::Diff),
		SubscriberProjection::Subject => Ok(SubscriptionFieldsLowered::Subject),
		SubscriberProjection::Fields {
			fields,
			names,
		} => {
			let shape = projection_shape(fields, names, Vec::new())?.ok_or(IrError::Invariant {
				what: "ir_lower::subscription: a projected field coerced to a kind",
			})?;
			let columns = (0..attrs.projected())
				.map(|_| next(regions))
				.collect::<Result<Vec<RegionId>, IrError>>()?;
			let pipeline = host(lowerer, |lowerer| {
				let out = project_image(lowerer, &shape, &columns, true)?;
				host_pipeline(emit_row(lowerer, out), lowerer)
			})?;
			Ok(SubscriptionFieldsLowered::Pipeline(pipeline))
		}
	}
}

/// Ends the pipeline of a clause as one that emits the value in `out` and ends its row.
fn emit_row(lowerer: &mut Lowerer<'_>, out: Reg) -> Result<Pipeline, IrError> {
	lowerer.emit(Instr::Emit {
		src: out,
	});
	lowerer.emit(Instr::EndRow);
	lowerer.em.finish_pipeline(Source::Once)
}

/// The index among the program's host pipelines of `pipeline`, once it is built.
fn host_pipeline(
	pipeline: Result<Pipeline, IrError>,
	lowerer: &mut Lowerer<'_>,
) -> Result<u16, IrError> {
	lowerer.em.host_code(pipeline?)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::*;
	use crate::exec::ir::core::ir::Ir;
	use crate::exec::ir::expr::ops::ExprOp;
	use crate::exec::ir::iface::ids::{OpId, ValueId};
	use crate::exec::ir::iface::region_kind::{ClauseRole, FieldRole};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::{FieldShape, FieldsShape, SubscriberLayer};
	use crate::exec::ir::logical::verify::timestamp_arg;
	use crate::exec::ir::unit::UnitKind;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::program::FetchStepSpec;
	use crate::expr::logical::{EvalFrame, OutputPath};

	/// The attributes of `LIVE SELECT * FROM t` for a subscriber whose reads are checked, over a
	/// table with no permission and no field view.
	fn shown() -> SubscriberAttrs {
		SubscriberAttrs {
			checked: true,
			admission: SubscriberAdmission::All,
			view: ImageView::Plain,
			layers: Vec::new(),
			projection: SubscriberProjection::Subject,
			cond: false,
			fetch: 0,
		}
	}

	/// The container of a subscription whose op is `attrs`, past the physical boundary. `clauses`
	/// opens the regions of the op, in the order its attributes list them.
	fn container(attrs: SubscriberAttrs, clauses: impl FnOnce(&mut Builder, OpId)) -> Builder {
		let unit = UnitKind::Subscription {
			table: TableName::from("t"),
		};
		let mut b = Builder::with_top(unit.top_region(), &unit.top_args());
		let op = b.add_phys(PhysOp::Subscriber(Box::new(attrs)), &[]);
		clauses(&mut b, op);
		b.add_phys(PhysOp::Yield, &[]);
		b.physical();
		b
	}

	fn lowered(b: &Builder) -> Result<LoweredLive, IrError> {
		lower_subscriber(&mut LowerCx::detached(), &b.ir)
	}

	/// A region of `kind` that takes the row alone and yields what `build` makes of it.
	fn clause(
		b: &mut Builder,
		op: OpId,
		kind: RegionKind,
		build: impl FnOnce(&mut Builder, ValueId) -> ValueId,
	) {
		b.region_with_args(op, kind, &[TypeKnowledge::Unknown], |b, args| build(b, args[0]));
	}

	/// The field `name` of the row.
	fn field(b: &mut Builder, row: ValueId, name: &str) -> ValueId {
		b.add(
			ExprOp::Field {
				name: Strand::new(name),
			},
			&[row],
		)
	}

	/// The predicate region of a permission, in the read frame of its guard: it takes the
	/// statement timestamp and the image, and yields `true`.
	fn predicate(b: &mut Builder, op: OpId) {
		let frame = b.frame(EvalFrame::permission(None));
		b.in_frame(frame, |b| {
			b.region_with_args(
				op,
				RegionKind::PermissionFrame,
				&[timestamp_arg(), TypeKnowledge::Unknown],
				|b, _| b.literal(crate::val::Value::Bool(true)),
			);
		});
	}

	#[test]
	fn a_permission_that_shows_every_record_holds_no_pipeline() {
		let b = container(shown(), |_, _| {});
		let live = lowered(&b).expect("the unit lowers");
		assert_eq!(live.plan.admission, Admission::All);
		assert_eq!(live.plan.view, None);
		assert_eq!(live.plan.cond, None);
		assert_eq!(live.plan.fields, SubscriptionFieldsLowered::Subject);
		assert!(live.plan.fetch.is_empty());
		assert!(live.lowered.program.host_code.is_empty());
	}

	#[test]
	fn a_permission_that_is_none_shows_no_record() {
		let attrs = SubscriberAttrs {
			admission: SubscriberAdmission::Nothing,
			..shown()
		};
		let live = lowered(&container(attrs, |_, _| {})).expect("the unit lowers");
		assert_eq!(live.plan.admission, Admission::Nothing);
	}

	#[test]
	fn a_predicate_is_one_host_pipeline_judged_for_a_checked_subscriber() {
		let attrs = SubscriberAttrs {
			admission: SubscriberAdmission::Judge {
				derived: Box::default(),
			},
			..shown()
		};
		let live = lowered(&container(attrs, predicate)).expect("the unit lowers");
		assert_eq!(live.plan.admission, Admission::Judge(0));
		assert_eq!(live.lowered.program.host_code.len(), 1);
	}

	/// A subscriber whose reads are not checked is shown every record, so a predicate beside it
	/// is a broken container and never a verdict that skips the check.
	#[test]
	fn a_predicate_beside_an_unchecked_subscriber_is_refused() {
		let attrs = SubscriberAttrs {
			checked: false,
			admission: SubscriberAdmission::Judge {
				derived: Box::default(),
			},
			..shown()
		};
		assert!(matches!(lowered(&container(attrs, predicate)), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_where_is_one_host_pipeline_over_the_image() {
		let attrs = SubscriberAttrs {
			cond: true,
			..shown()
		};
		let b = container(attrs, |b, op| {
			clause(b, op, RegionKind::Clause(ClauseRole::Where), |b, row| field(b, row, "a"));
		});
		let live = lowered(&b).expect("the unit lowers");
		assert_eq!(live.plan.cond, Some(0));
		assert_eq!(live.lowered.program.host_code.len(), 1);
	}

	/// A name of the subscriber's frame is read by its name through the frame in a pipeline of the
	/// unit, which runs beside the code that notifies.
	#[test]
	fn a_name_of_the_subscribers_frame_is_read_through_the_frame() {
		let attrs = SubscriberAttrs {
			cond: true,
			..shown()
		};
		let unit = UnitKind::Subscription {
			table: TableName::from("t"),
		};
		let mut b = Builder::with_top(unit.top_region(), &unit.top_args());
		let before = b.block_args()[1];
		let op = b.add_phys(PhysOp::Subscriber(Box::new(attrs)), &[]);
		clause(&mut b, op, RegionKind::Clause(ClauseRole::Where), |_, _| before);
		b.add_phys(PhysOp::Yield, &[]);
		b.physical();
		let live = lowered(&b).expect("the unit lowers");
		assert!(live.lowered.program.params.iter().any(|param| param.name.as_str() == "before"));
		assert!(
			live.lowered.program.host_code[0]
				.code
				.iter()
				.any(|instr| matches!(instr, Instr::LoadParam { .. }))
		);
	}

	#[test]
	fn a_diff_carries_the_patch_and_holds_no_pipeline() {
		let attrs = SubscriberAttrs {
			projection: SubscriberProjection::Diff,
			..shown()
		};
		let live = lowered(&container(attrs, |_, _| {})).expect("the unit lowers");
		assert_eq!(live.plan.fields, SubscriptionFieldsLowered::Diff);
		assert!(live.lowered.program.host_code.is_empty());
	}

	#[test]
	fn a_projection_of_fields_is_one_host_pipeline() {
		let attrs = SubscriberAttrs {
			projection: SubscriberProjection::Fields {
				fields: FieldsShape::Select(Box::new([FieldShape::Single {
					alias: None,
					kind: None,
				}])),
				names: vec![OutputPath::field("a")],
			},
			..shown()
		};
		let b = container(attrs, |b, op| {
			clause(b, op, RegionKind::Clause(ClauseRole::Projection), |b, row| field(b, row, "a"));
		});
		let live = lowered(&b).expect("the unit lowers");
		assert_eq!(live.plan.fields, SubscriptionFieldsLowered::Pipeline(0));
		assert_eq!(live.lowered.program.host_code.len(), 1);
	}

	#[test]
	fn a_fetch_path_is_the_steps_its_region_reads() {
		let attrs = SubscriberAttrs {
			fetch: 1,
			..shown()
		};
		let b = container(attrs, |b, op| {
			clause(b, op, RegionKind::Clause(ClauseRole::Projection), |b, row| {
				field(b, row, "author")
			});
		});
		let live = lowered(&b).expect("the unit lowers");
		assert_eq!(live.plan.fetch, vec![vec![FetchStepSpec::Field(Strand::new("author"))]]);
	}

	/// A view that cuts or derives is never shown whole: a subscriber whose view is a stack of
	/// layers that the op does not hold is a broken container, and so is layers beside a view that
	/// changes nothing.
	#[test]
	fn a_field_view_and_the_layers_the_op_holds_agree() {
		let viewed = |view, checked| SubscriberAttrs {
			view,
			checked,
			..shown()
		};
		for (view, checked) in
			[(ImageView::Cut, true), (ImageView::Derived, true), (ImageView::Derived, false)]
		{
			assert!(
				matches!(
					lowered(&container(viewed(view, checked), |_, _| {})),
					Err(IrError::Invariant { .. })
				),
				"{view:?} checked {checked}"
			);
		}
		for (view, checked) in [(ImageView::Plain, true), (ImageView::Cut, false)] {
			let live = lowered(&container(viewed(view, checked), |_, _| {}))
				.expect("a view that changes nothing lowers");
			assert_eq!(live.plan.view, None);
			let layered = SubscriberAttrs {
				layers: vec![computed_layer()],
				..viewed(view, checked)
			};
			assert!(matches!(
				lowered(&container(layered, computed_regions)),
				Err(IrError::Invariant { .. })
			));
		}
		assert!(matches!(
			lowered(&container(viewed(ImageView::LateBound, true), |_, _| {})),
			Err(IrError::LateBind { .. })
		));
		assert!(matches!(
			lowered(&container(viewed(ImageView::Dynamic, true), |_, _| {})),
			Err(IrError::Invariant { .. })
		));
	}

	/// One layer with the column `c`, over an input that declares `c` `COMPUTED`.
	fn computed_layer() -> SubscriberLayer {
		SubscriberLayer {
			fields: FieldsShape::Select(Box::new([FieldShape::Single {
				alias: Some(OutputPath::field("c")),
				kind: None,
			}])),
			computed: vec![Strand::new("c")],
			judged: None,
		}
	}

	/// The regions of the layer [`computed_layer`] names: the read of `c`, then its expression
	/// `a`, derived in a computed frame.
	fn computed_regions(b: &mut Builder, op: OpId) {
		clause(b, op, RegionKind::Clause(ClauseRole::Projection), |b, row| field(b, row, "c"));
		let frame = b.frame(EvalFrame::computed(None));
		b.in_frame(frame, |b| {
			b.region_with_args(
				op,
				RegionKind::FieldFrame(FieldRole::Computed),
				&[timestamp_arg(), TypeKnowledge::Unknown],
				|b, args| field(b, args[1], "a"),
			);
		});
	}

	/// The layers of a view are one host pipeline over the image, and the view is that pipeline.
	#[test]
	fn the_layers_of_a_view_are_one_host_pipeline_over_the_image() {
		let attrs = SubscriberAttrs {
			view: ImageView::Derived,
			layers: vec![computed_layer()],
			..shown()
		};
		let live = lowered(&container(attrs, computed_regions)).expect("the unit lowers");
		assert_eq!(live.plan.view, Some(0));
		assert_eq!(live.lowered.program.host_code.len(), 1);
	}

	/// A layer column's guard that reads `$value` on a field the layer's input declares `COMPUTED`
	/// judges the field's derived value, as a statement's layer does.
	#[test]
	fn a_guard_that_reads_its_value_on_a_declared_computed_field_lowers() {
		let attrs = SubscriberAttrs {
			view: ImageView::Cut,
			layers: vec![computed_layer()],
			..shown()
		};
		let b = container(attrs, |b, op| {
			clause(b, op, RegionKind::Clause(ClauseRole::Projection), |b, row| field(b, row, "c"));
			let frame = b.frame(EvalFrame::permission(None));
			b.in_frame(frame, |b| {
				b.region_with_args(
					op,
					RegionKind::FieldFrame(FieldRole::Computed),
					&[timestamp_arg(), TypeKnowledge::Unknown, TypeKnowledge::Unknown],
					|b, args| {
						let (row, value) = (args[1], args[2]);
						let one = b.literal(crate::val::Value::from(1_i64));
						let guard = b.binary(crate::expr::BinaryOperator::MoreThan, value, one);
						let owner = b.add_with_regions(ExprOp::If, &[guard]);
						b.region_with_args(owner, RegionKind::ValueIf, &[], |b, _| {
							field(b, row, "c")
						});
						b.region_with_args(owner, RegionKind::ValueIf, &[], |b, _| {
							b.literal(crate::val::Value::None)
						});
						b.result_of(owner)
					},
				);
			});
		});
		let live = lowered(&b).expect("the unit lowers");
		assert_eq!(live.plan.view, Some(0));
	}

	/// The pipelines of the unit are numbered in the order they are lowered: the view first, then
	/// the clauses in the order the op lists them.
	#[test]
	fn the_view_is_lowered_before_the_clauses_that_read_through_it() {
		let attrs = SubscriberAttrs {
			view: ImageView::Derived,
			layers: vec![computed_layer()],
			cond: true,
			..shown()
		};
		let b = container(attrs, |b, op| {
			computed_regions(b, op);
			clause(b, op, RegionKind::Clause(ClauseRole::Where), |b, row| field(b, row, "c"));
		});
		let live = lowered(&b).expect("the unit lowers");
		assert_eq!((live.plan.view, live.plan.cond), (Some(0), Some(1)));
	}

	#[test]
	fn a_container_short_of_the_physical_boundary_is_an_invariant_break() {
		let unit = UnitKind::Subscription {
			table: TableName::from("t"),
		};
		let mut b = Builder::with_top(unit.top_region(), &unit.top_args());
		b.add_phys(PhysOp::Subscriber(Box::new(shown())), &[]);
		b.add_phys(PhysOp::Yield, &[]);
		assert!(b.ir.phase() < Phase::Physical);
		assert!(matches!(
			lowered(&b),
			Err(IrError::Invariant {
				what: "ir_lower::subscription: a subscription's container is not physical"
			})
		));
	}

	#[test]
	fn a_container_that_tops_no_subscriber_frame_is_refused() {
		let mut b = Builder::new();
		b.physical();
		assert!(matches!(lowered(&b), Err(IrError::Invariant { .. })));
	}

	/// The unit is a program of host pipelines and no pipeline of its own.
	#[test]
	fn the_unit_holds_no_pipeline_of_its_own() {
		let b = container(shown(), |_, _| {});
		let live = lowered(&b).expect("the unit lowers");
		assert!(live.lowered.program.pipelines.is_empty());
		assert!(live.lowered.program.scalar);
		let unit: Ir = b.ir;
		assert_eq!(unit.phase(), Phase::Physical);
	}
}
