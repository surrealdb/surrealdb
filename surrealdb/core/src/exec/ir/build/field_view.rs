//! The field view of a table, as the planner binds it for the principal a request is planned for.
//!
//! A write that shows or reads an image of a record shows it through the table's field view: every
//! `COMPUTED` field derived onto it and, for a caller whose reads are permission-checked, each
//! field the caller may not select cut. The view is the stack of resolver layers the planner puts
//! above the table's source when it binds `SELECT * FROM t`, which only the planner can bind. The
//! synchronous binder reads it through [`SemanticCatalog::field_view`]; the owner of the planner
//! answers the tables the memo reports pending with [`bind_field_view`] before the binder runs
//! again.
//!
//! The answer is bottom first, the order the layers apply. A table that declares no `COMPUTED`
//! field, and no field a checked caller may not select, has no layer, and is answered without
//! binding. A bound plan that is not a stack of layers above a source whose scan enforces the
//! table's declarations has no layer form ([`FieldView::Enforced`]).

use crate::catalog::Permission;
use crate::exec::ir::cache::planning::PlanningCatalog;
use crate::exec::ir::iface::catalog::{CatalogAnswer, FieldView};
use crate::exec::planner::Planner;
use crate::expr::logical::{
	LogicalField, LogicalFields, LogicalOp, LogicalProject, LogicalSource, SourceRef,
};
use crate::val::TableName;

/// The field view of `table` for the principal `planner` plans for.
///
/// The read is bound apart from the request's own plan, on a fresh cycle guard. A table whose
/// events or views are being planned still binds, and its view is the same as that of a statement
/// that writes to the table from inside them. A read that fails to bind is answered
/// [`FieldView::Enforced`].
pub(crate) async fn bind_field_view(
	planner: &Planner<'_>,
	memo: &PlanningCatalog,
	table: &TableName,
) -> FieldView {
	let checked = planner.reads_are_checked();
	match memo.table(table) {
		CatalogAnswer::Absent => return FieldView::Layers(Vec::new()),
		CatalogAnswer::Found(read) => {
			if read.definition.is_some()
				&& read.fields.iter().all(|field| {
					field.computed.is_none()
						&& (!checked || matches!(field.select_permission, Permission::Full))
				}) {
				return FieldView::Layers(Vec::new());
			}
		}
		CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => {}
	}
	let read = LogicalOp::Project(LogicalProject {
		input: Box::new(LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(table.clone()),
			schema: None,
			with: None,
			for_update: false,
		})),
		fields: LogicalFields::Select(vec![LogicalField::All]),
		omit: Vec::new(),
	});
	let apart = planner.shadowing(std::iter::empty()).with_cycle_guard(Default::default());
	let Ok(bound) = apart.bind(read).await else {
		return FieldView::Enforced;
	};
	layers_of(&bound)
}

/// The layers of the bound plan `bound`, bottom first, or [`FieldView::Enforced`] where the plan
/// is not a stack of layers above a source with a schema. An all-fields projection and a
/// permission filter pass through.
fn layers_of(bound: &LogicalOp) -> FieldView {
	let mut layers = Vec::new();
	let mut node = bound;
	loop {
		match node {
			LogicalOp::Project(layer) if node.is_layer() => {
				layers.push((layer.fields.clone(), layer.input.schema()));
				node = &layer.input;
			}
			LogicalOp::Project(project)
				if matches!(&project.fields, LogicalFields::Select(fields)
					if matches!(fields.as_slice(), [LogicalField::All]))
					&& project.omit.is_empty() =>
			{
				node = &project.input;
			}
			LogicalOp::PermissionFilter(filter) => node = &filter.input,
			LogicalOp::Source(LogicalSource {
				schema: Some(_),
				..
			}) => break,
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Window(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => return FieldView::Enforced,
		}
	}
	layers.reverse();
	FieldView::Layers(layers)
}
