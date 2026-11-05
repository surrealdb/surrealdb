//! The notification unit of a live subscription, bound as a container.
//!
//! A live query registers a subscription that outlives the statement that made it, and a write
//! to the table it watches runs the subscription's unit once for each notification. The unit is
//! planned as [`UnitKind::Subscription`]: its top region is a
//! [`SubscriberFrame`](RegionKind::SubscriberFrame), whose block arguments after the statement
//! timestamp are the names a subscriber's frame binds, in [`SUBSCRIBER_NAMES`] order. This binder
//! builds its body from the subscription as it is stored ([`CompiledSubscription`]) and the
//! table's catalog answers, never from the `LIVE` statement's own ops: the target table is known
//! only when the statement runs, and the unit outlives the statement.
//!
//! # The body
//!
//! The body of the unit is one `logical.subscriber` op, which states what the subscriber is shown
//! ([`SubscriberAttrs`]) and holds, as regions, the clauses that decide it:
//!
//! - The table's `PERMISSIONS FOR select`, for a subscriber whose reads are checked. `FULL` and an
//!   unchecked subscriber state `All`, `NONE` states `Nothing`, and a predicate is a
//!   `PermissionFrame` region judged in the permission frame of a read guard, followed by one
//!   `FieldFrame(Computed)` region for each `COMPUTED` field the predicate reads, in the order the
//!   table's view derives them. A record the subscriber may not select is never notified.
//! - The `WHERE`, a `Clause(Where)` region over the record the notification is about.
//! - The projection, one `Clause(Projection)` region for each field that is an expression. `DIFF`
//!   and `*` hold none.
//! - The `FETCH` paths, one `Clause(Projection)` region for each path. The paths are the ones the
//!   planner resolves the subscription's `FETCH` expressions to ([`SubscriptionInput::fetch`]): a
//!   parameter names a path only through the values the planner holds, which the binder does not.
//!
//! - The field view, one layer for each resolver layer the planner binds over the table for the
//!   subscriber ([`SubscriptionInput::view`]): per column, a `Clause(Projection)` region that reads
//!   the field the column lands under, then, after the layer's reads, a `FieldFrame(Computed)`
//!   region that holds the column's expression in its frame, as the regions of a `phys.layer`. A
//!   view that derives a `COMPUTED` field, or cuts a field a checked subscriber may not select,
//!   holds layers, and any other holds none ([`ImageView`]).
//!
//! # Names
//!
//! The eight names of the subscriber's frame are bound to the top block's arguments, so a read of
//! any of them in a clause that sees the top region's bindings (the `WHERE`, the projection and
//! the `FETCH` paths) is a block-argument read and never a session-name read. A permission
//! predicate and a computed body are isolated from above and read the names it binds as free
//! names, which the subscriber's frame supplies when the unit runs. A parameter the subscription
//! captured is a free name too, in every clause, and is never folded.
//!
//! # Catalog reads
//!
//! The binder reads the table's definition and fields through [`EffectCx::catalog`] and nothing
//! else. A table the memo has not loaded answers [`IrError::NotLoaded`]; the caller loads it and
//! binds again. A table that changed answers [`IrError::LateBind`]. A table with no definition,
//! a clause with no logical form, or a `FETCH` path that does not resolve answers
//! [`IrError::Unsupported`], which leaves the subscription without a unit.

#![deny(clippy::wildcard_enum_match_arm)]

use surrealdb_strand::{Strand, TableName};

use super::logical::{
	ComputedReads, bind_derivations, columns_of, declared_computed, fetch_path_clause,
	fields_shape, framed_clause, judged_reads_of, row_clause, unaliased_names,
};
use crate::catalog::{CompiledSubscription, Permission, SubscriptionFields};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, IrScope};
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::catalog::{CatalogAnswer, EffectCx, SemanticCatalog};
use crate::exec::ir::iface::region_kind::{ClauseRole, FieldRole, RegionKind};
use crate::exec::ir::logical::ops::{
	FieldShape, FieldsShape, ImageView, LogOp, SubscriberAdmission, SubscriberAttrs,
	SubscriberLayer, SubscriberProjection,
};
use crate::exec::ir::logical::seam::{BindCx, ExprHost, invariant};
use crate::exec::ir::logical::stages::{Derivation, derivations_of, view_of};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::unit::{SUBSCRIBER_NAMES, UnitKind};
use crate::exec::surface::Surface;
use crate::expr::computed_deps::extract_computed_deps;
use crate::expr::logical::{
	EvalFrame, LogicalExpr, LogicalFields, LogicalPermission, OutputPath, Schema, lower_expr,
	lower_fields,
};

/// What a subscription's unit is bound from, besides the catalog's answers for its table.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SubscriptionInput<'a> {
	/// The scope the container is planned in.
	pub(crate) scope: &'a IrScope,
	/// The table the subscription watches.
	pub(crate) table: &'a TableName,
	/// The subscription as it is stored.
	pub(crate) compiled: &'a CompiledSubscription,
	/// The paths `compiled.fetch` resolves to, in order, as the planner resolves them for the
	/// subscriber's frame. Each is a path expression.
	pub(crate) fetch: &'a [LogicalExpr],
	/// The layers of the table's field view for the subscriber, in the order they apply: the
	/// resolver layers the planner binds over `SELECT * FROM t` for its principal, bottom first,
	/// each with the schema of its input. None for a view that changes nothing for the subscriber.
	pub(crate) view: &'a [(LogicalFields, Schema)],
	/// Whether the subscriber's reads are permission-checked and permissions are not suppressed,
	/// which is whether the table's permissions and field view are judged for it.
	pub(crate) checked: bool,
}

/// Binds the notification unit of the subscription `input` names, as a container whose top region
/// is a [`SubscriberFrame`](RegionKind::SubscriberFrame).
///
/// The binding is synchronous, so that no binder state is held across a suspension point.
pub(crate) fn bind_subscription(
	input: &SubscriptionInput<'_>,
	effects: &EffectCx<'_>,
	surface: Option<&'static Surface>,
) -> Result<Ir, IrError> {
	let unit = UnitKind::Subscription {
		table: input.table.clone(),
	};
	let mut ir = Ir::with_top_args(input.scope.clone(), unit.top_region(), &unit.top_args());
	let mut facts = FactCache::new();
	{
		let mut cx = BindCx::new(&mut ir, &mut facts, *effects)?;
		cx.set_surface(surface);
		bind_body_of(&mut cx, input)?;
	}
	Ok(ir)
}

/// The inputs a clause is bound from, once the catalog has answered.
struct Judged {
	guard: LogicalExpr,
	derivations: Vec<Derivation>,
}

/// The refusal that leaves a subscription with no unit.
fn unsupported(reason: impl Into<String>) -> IrError {
	IrError::Unsupported {
		reason: reason.into(),
	}
}

fn bind_body_of(cx: &mut BindCx<'_>, input: &SubscriptionInput<'_>) -> Result<(), IrError> {
	let snapshot = match cx.catalog().table(input.table) {
		CatalogAnswer::Found(snapshot) => snapshot,
		CatalogAnswer::Absent => {
			return Err(unsupported("the subscription's table is not defined"));
		}
		CatalogAnswer::NotLoaded => {
			return Err(IrError::NotLoaded {
				what: "ir::build::subscription: the subscription's table",
			});
		}
		CatalogAnswer::Unknown => {
			return Err(IrError::LateBind {
				what: "ir::build::subscription: the subscription's table",
			});
		}
	};
	let Some(definition) = snapshot.definition.as_ref() else {
		return Err(unsupported("the subscription's table is not defined"));
	};
	// The names the subscriber's frame binds.
	let args = cx.block_args()?;
	if args.len() != 1 + SUBSCRIBER_NAMES.len() {
		return Err(invariant("a subscriber's top block does not take the names of its frame"));
	}
	for (name, value) in SUBSCRIBER_NAMES.iter().zip(args.iter().skip(1)) {
		cx.declare(Strand::new(*name), *value);
	}

	let (admission, judged) = if !input.checked {
		(SubscriberAdmission::All, None)
	} else {
		match &definition.permissions.select {
			Permission::Full => (SubscriberAdmission::All, None),
			Permission::None => (SubscriberAdmission::Nothing, None),
			Permission::Specific(stored) => {
				let guard = match definition.select_permission_logical() {
					Ok(LogicalPermission::Specific(guard)) => guard,
					Ok(LogicalPermission::Full | LogicalPermission::None) => {
						return Err(invariant("a specific permission has no predicate"));
					}
					Err(error) => {
						return Err(unsupported(format!(
							"a permission has no logical form: {error}"
						)));
					}
				};
				if let Some(why) = crate::exec::logical::unplannable_write_in_expr(&guard) {
					return Err(unsupported(format!(
						"a permission holds a write no operator runs: {why}"
					)));
				}
				// The computed fields the predicate reads are derived onto the copy of the record
				// it judges: every one when it reads the record other than by name, and none when
				// it reads no field of it.
				let deps = extract_computed_deps(stored);
				let derivations = if deps.is_complete && deps.fields.is_empty() {
					Vec::new()
				} else {
					let reads = deps.is_complete.then_some(deps.fields);
					derivations_of(&snapshot.fields, reads.as_deref()).ok_or_else(|| {
						unsupported("a computed field has no logical form or no single name")
					})?
				};
				let derived =
					derivations.iter().map(|derivation| derivation.field.clone()).collect();
				(
					SubscriberAdmission::Judge {
						derived,
					},
					Some(Judged {
						guard,
						derivations,
					}),
				)
			}
		}
	};

	let view = view_of(&snapshot.fields);
	debug_assert!(view != ImageView::Dynamic && view != ImageView::LateBound);
	let layered = match view {
		ImageView::Derived => true,
		ImageView::Cut => input.checked,
		ImageView::Plain | ImageView::Dynamic | ImageView::LateBound => false,
	};
	if layered == input.view.is_empty() {
		return Err(invariant(
			"a subscription's field view is bound with the layers its table's fields call for",
		));
	}
	let mut layers = Vec::with_capacity(input.view.len());
	let mut judged_bodies = Vec::with_capacity(input.view.len());
	for (fields, schema) in input.view {
		let (layer, bodies) =
			subscriber_layer(cx.registry(), cx.catalog(), Some(input.table), fields, schema)?;
		layers.push(layer);
		judged_bodies.push(bodies);
	}

	let cond = input
		.compiled
		.cond
		.as_ref()
		.map(|expr| {
			lower_expr(expr.clone()).map_err(|error| {
				unsupported(format!("the subscription's WHERE has no logical form: {error}"))
			})
		})
		.transpose()?;

	let (projection, columns) = match &input.compiled.fields {
		SubscriptionFields::Diff => (SubscriberProjection::Diff, None),
		SubscriptionFields::Select(fields) => {
			let logical = lower_fields(fields.clone()).map_err(|error| {
				unsupported(format!("the subscription's projection has no logical form: {error}"))
			})?;
			let shape = fields_shape(&logical);
			match &shape {
				FieldsShape::Select(list) if matches!(&**list, [FieldShape::All]) => {
					(SubscriberProjection::Subject, None)
				}
				FieldsShape::Select(list)
					if list.iter().any(|field| {
						matches!(
							field,
							FieldShape::Single {
								kind: Some(_),
								..
							}
						)
					}) =>
				{
					return Err(unsupported("a projected field coerced to a kind"));
				}
				FieldsShape::Select(_)
				| FieldsShape::Value {
					..
				} => (
					SubscriberProjection::Fields {
						fields: shape,
						names: unaliased_names(&logical),
					},
					Some(logical),
				),
			}
		}
	};

	if let Some(expr) = input.fetch.iter().find(|expr| !expr.is_path()) {
		return Err(invariant(if matches!(expr, LogicalExpr::Param(_)) {
			"a FETCH path that is a parameter the planner did not resolve"
		} else {
			"a FETCH path that is not a path"
		}));
	}
	let fetch = u16::try_from(input.fetch.len()).map_err(|_| IrError::Capacity("fetch paths"))?;

	let attrs = SubscriberAttrs {
		checked: input.checked,
		admission,
		view,
		layers,
		projection,
		cond: cond.is_some(),
		fetch,
	};
	let op = cx.emit(OpKind::Logical(LogOp::Subscriber(Box::new(attrs))), &[], &[])?;
	if let Some(Judged {
		guard,
		derivations,
	}) = &judged
	{
		framed_clause(
			cx,
			op,
			RegionKind::PermissionFrame,
			Some(&EvalFrame::permission(None)),
			guard,
		)?;
		for derivation in derivations {
			let eval = EvalFrame::permission(derivation.auth.clone());
			let kind = RegionKind::FieldFrame(FieldRole::Computed);
			framed_clause(cx, op, kind, Some(&eval), &derivation.body)?;
		}
	}
	for ((fields, _), bodies) in input.view.iter().zip(&judged_bodies) {
		bind_layer(cx, op, fields, bodies)?;
	}
	if let Some(cond) = &cond {
		row_clause(cx, op, RegionKind::Clause(ClauseRole::Where), cond)?;
	}
	if let Some(fields) = &columns {
		bind_columns(cx, op, fields)?;
	}
	for path in input.fetch {
		fetch_path_clause(cx, op, path)?;
	}
	cx.yield_logical(&[])
}

/// One resolver layer of the field view, whose columns are `fields` over rows of `schema`, the
/// records of `table`, with the bodies of the computed fields its guards read. It carries the
/// fields `schema` declares `COMPUTED` ([`declared_computed`]) and the computed fields its guards
/// read ([`judged_reads_of`]), as a statement's layer carries them, so subscriber columns are
/// judged computed exactly where statement columns are, and over the same copy.
pub(super) fn subscriber_layer(
	registry: &FunctionRegistry,
	catalog: &dyn SemanticCatalog,
	table: Option<&TableName>,
	fields: &LogicalFields,
	schema: &Schema,
) -> Result<(SubscriberLayer, Vec<Derivation>), IrError> {
	match fields_shape(fields) {
		shape @ FieldsShape::Select(_) => {
			let (judged, bodies) = match judged_reads_of(registry, catalog, table, schema, fields)?
			{
				Some(ComputedReads {
					shape,
					bodies,
				}) => (Some(shape), bodies),
				None => (None, Vec::new()),
			};
			let layer = SubscriberLayer {
				fields: shape,
				computed: declared_computed(schema),
				judged,
			};
			Ok((layer, bodies))
		}
		FieldsShape::Value {
			..
		} => Err(invariant("a layer of a value selection")),
	}
}

/// The regions of one resolver layer of the field view: the read of the field each column lands
/// under, for every column, then the expression of every column in its frame, then the body of
/// each computed field its guards read, `bodies` ([`bind_derivations`]).
pub(super) fn bind_layer(
	cx: &mut BindCx<'_>,
	op: crate::exec::ir::iface::ids::OpId,
	fields: &LogicalFields,
	bodies: &[Derivation],
) -> Result<(), IrError> {
	let columns = columns_of(fields);
	for column in &columns {
		let name = column
			.path
			.as_ref()
			.and_then(OutputPath::as_field)
			.ok_or_else(|| invariant("a layer column lands under one field"))?;
		let stored = LogicalExpr::Field(Strand::new(name));
		row_clause(cx, op, RegionKind::Clause(ClauseRole::Projection), &stored)?;
	}
	for column in &columns {
		framed_clause(
			cx,
			op,
			RegionKind::FieldFrame(FieldRole::Computed),
			column.frame,
			column.expr,
		)?;
	}
	bind_derivations(cx, op, bodies)
}

/// One `Clause(Projection)` region for each column of `fields`, in order.
fn bind_columns(
	cx: &mut BindCx<'_>,
	op: crate::exec::ir::iface::ids::OpId,
	fields: &LogicalFields,
) -> Result<(), IrError> {
	let projected = RegionKind::Clause(ClauseRole::Projection);
	for column in columns_of(fields) {
		framed_clause(cx, op, projected, column.frame, column.expr)?;
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::iface::catalog::SemanticCatalog;
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::expr::{Expr, Fields};
	use crate::kvs::cache::ds::TableSnapshot;

	/// A catalog that answers every table with one fixed answer that holds no snapshot.
	struct TableAnswer(fn() -> CatalogAnswer<Arc<TableSnapshot>>);

	impl SemanticCatalog for TableAnswer {
		fn table(&self, _name: &TableName) -> CatalogAnswer<Arc<TableSnapshot>> {
			(self.0)()
		}
	}

	/// The unit of `LIVE SELECT * FROM t`, bound over a catalog that answers `answer` for `t`.
	fn bound(answer: &TableAnswer) -> Result<Ir, IrError> {
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let effects = EffectCx {
			registry: &registry,
			catalog: answer,
			frames: &frames,
		};
		let table = TableName::from("t");
		let compiled = CompiledSubscription {
			fields: SubscriptionFields::Select(Fields::all()),
			what: Expr::Table(table.clone()),
			cond: None,
			fetch: None,
		};
		let scope = IrScope::Request {
			fragment: None,
		};
		bind_subscription(
			&SubscriptionInput {
				scope: &scope,
				table: &table,
				compiled: &compiled,
				fetch: &[],
				view: &[],
				checked: true,
			},
			&effects,
			Some(&crate::exec::surface::FULL),
		)
	}

	/// A table the catalog does not define leaves the subscription with no unit. An answer the
	/// catalog cannot give is never read as a table with no clause: a table that is not loaded
	/// is loaded and bound again, and a table that changed is bound again.
	#[test]
	fn a_table_the_catalog_cannot_describe_binds_no_unit() {
		assert!(matches!(
			bound(&TableAnswer(|| CatalogAnswer::Absent)),
			Err(IrError::Unsupported { .. })
		));
		assert!(matches!(
			bound(&TableAnswer(|| CatalogAnswer::NotLoaded)),
			Err(IrError::NotLoaded { .. })
		));
		assert!(matches!(
			bound(&TableAnswer(|| CatalogAnswer::Unknown)),
			Err(IrError::LateBind { .. })
		));
	}

	/// A layer carries the fields its input's schema declares `COMPUTED` and no stored one.
	#[test]
	fn a_layer_carries_the_computed_fields_its_input_declares() {
		use crate::expr::Kind;
		use crate::expr::logical::{ComputedBody, FieldPath, FieldSchema};

		let mut computed = FieldSchema::new(FieldPath::field("c"), Kind::Int);
		computed.computed = Some(ComputedBody {
			expr: LogicalExpr::Field(Strand::new("s")),
			kind: Some(Kind::Int),
			auth: None,
		});
		let schema =
			Schema::closed(vec![FieldSchema::new(FieldPath::field("s"), Kind::Int), computed]);
		let (layer, _) = subscriber_layer(
			&FunctionRegistry::with_builtins(),
			&TableAnswer(|| CatalogAnswer::Unknown),
			None,
			&LogicalFields::all(),
			&schema,
		)
		.expect("a layer binds");
		assert_eq!(layer.computed, vec![Strand::new("c")]);
		assert_eq!(layer.fields, fields_shape(&LogicalFields::all()));
	}

	/// The top region of the container is the subscriber frame, which takes the statement
	/// timestamp and then the eight names of the subscriber's frame.
	#[test]
	fn the_unit_tops_a_subscriber_frame_that_takes_the_names_of_its_frame() {
		let unit = UnitKind::Subscription {
			table: TableName::from("t"),
		};
		assert_eq!(unit.top_region(), RegionKind::SubscriberFrame);
		assert_eq!(unit.top_args().len(), 1 + SUBSCRIBER_NAMES.len());
	}
}
