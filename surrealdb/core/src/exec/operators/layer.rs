//! ProjectLayer operator: the columns of a projection layer, set on each row.
//!
//! A projection layer is a `Project` of the form `*, e₁ AS n₁, …` whose
//! every column is evaluated in a frame that is not the caller's
//! (`LogicalOp::is_layer`). The analyzer writes a table's `COMPUTED` fields
//! and its field `SELECT` permissions as a stack of such layers directly
//! above the table's `PermissionFilter`, so the row leaving the stack carries
//! each computed field as its computed value and each restricted field as
//! the value the caller may see, under the field's own name. Every user
//! expression above the stack reads plain field names.
//!
//! [`ProjectLayer`] evaluates each column once per row and then sets every
//! column on the row under its name. What a column evaluates against follows
//! the scan pipeline's order of checks (`materialise_fields_with_permissions`):
//!
//! 1. The columns on *stored* fields run first, each against the input row in its own frame: a
//!    guard is judged against the record as stored, so a clause naming a field another column cuts
//!    reads the stored value, never `NONE`.
//! 2. The columns on *computed* fields — those the input schema declares `COMPUTED`
//!    ([`ProjectLayer::with_input_schema`]) — run next, in plan order. Each value is evaluated in
//!    the `Computed` frame against the record *as the stored columns left it*: the input row with
//!    every stored field's guard applied, carrying each computed value the columns before it
//!    landed. That is the reduced record the pipeline re-derives computed fields against, so a body
//!    reading a cut field sees `NONE`, a body reading a re-derived field sees its re-derivation,
//!    and a self-dereference through `id` (see below) reads the same view. When the layer has no
//!    stored guard the reduced record is the input row itself and nothing is copied.
//!
//! A column's value lands on the row as follows:
//!
//! - A `Computed`-frame column is inserted whatever its value, `NONE` included: a computed field
//!   whose body yields `NONE` is present and `NONE`, as it is when a scan derives it.
//! - A `Permission`-frame column is a field's `SELECT` permission. One written `WHERE p` is a
//!   *guard* `p` over a value `v` — the field as the layer's input holds it, or a computed field's
//!   re-derivation. The value is evaluated first, coerced to the column's kind, and the guard is
//!   judged in the permission frame against the input row with `$value` bound to it. A guard that
//!   does not hold removes the key, which is the cut of a field the caller may not read: `SELECT *`
//!   shows a denied field absent rather than `NONE`. A guard that holds sets the key to `v`. A
//!   `NONE` admitted for a stored field leaves the row as it is — the field is as absent as it is
//!   stored — while a `NONE` admitted for a computed field is present and `NONE`, as the scan
//!   pipeline inserts every computed field it derives. A permission column without a guard —
//!   `PERMISSIONS FOR select NONE`, or a nested guard rebuilding its root field — lands its value,
//!   and a `NONE` removes the key.
//!
//! A column carrying a kind is coerced to it before it lands or is judged; a
//! value that does not coerce fails the read with an error naming the field.
//!
//! A body is a scalar or a block; a block ends in `RETURN`, whose value is the
//! column's. `BREAK` and `CONTINUE` have no loop to reach and are errors.
//!
//! # Self-dereference
//!
//! A `COMPUTED` body may reach its own record through the id — `id.f`,
//! `$this.id.f` — and that dereference must not re-enter computed-field
//! evaluation for the record (it would recurse without end) nor read the
//! record as written. It is answered from the row as this layer presents it
//! ([`ComputingRecord::stored`]): the reduced record of step 2 minus every
//! field this layer computes and every field the input schema declares
//! `COMPUTED`, since a computed field has no stored value and a stored key
//! under its name is shadowed by the definition. The view is built only when
//! it can differ from storage, that is when the layer has a stored guard or
//! the row carries a key under a computed name; otherwise the dereference
//! reads storage, as it does for a record that has nothing to hide.

#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use futures::StreamExt;
#[cfg(test)]
use surrealdb_strand::Strand;
use surrealdb_types::ToSql;

#[cfg(test)]
use crate::err::EngineError;
#[cfg(test)]
use crate::exec::physical_expr::ComputingRecord;
#[cfg(test)]
use crate::exec::{
	AccessMode, CardinalityHint, CombineAccessModes, ContextLevel, EvalContext, ExecOperator,
	ExecutionContext, FlowResult, OperatorMetrics, PhysicalExpr, ValueBatchStream, buffer_stream,
	monitor_stream,
};
#[cfg(test)]
use crate::expr::logical::{EvalFrame, EvalMode, Schema};
#[cfg(test)]
use crate::expr::{ControlFlow, Kind};
#[cfg(test)]
use crate::val::Object;
use crate::val::Value;

#[cfg(test)]
/// One column of a layer: the field name it lands under, the expression
/// whose value lands there, the kind the value is coerced to, and the frame
/// the column evaluates in.
#[derive(Debug, Clone)]
pub(crate) struct LayerColumn {
	pub(crate) name: Strand,
	/// The guard of a field `SELECT` permission written `WHERE p`: judged
	/// before `value`, in the column's frame, and cutting the field when it
	/// does not hold (see the module documentation). Only a permission column
	/// carries one.
	pub(crate) guard: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) value: Arc<dyn PhysicalExpr>,
	pub(crate) kind: Option<Kind>,
	pub(crate) frame: EvalFrame,
}

#[cfg(test)]
impl LayerColumn {
	#[cfg(test)]
	/// A column whose `value` lands as the module documentation states for
	/// its frame.
	pub(crate) fn new(
		name: impl Into<Strand>,
		value: Arc<dyn PhysicalExpr>,
		kind: Option<Kind>,
		frame: EvalFrame,
	) -> Self {
		Self {
			name: name.into(),
			guard: None,
			value,
			kind,
			frame,
		}
	}

	#[cfg(test)]
	/// A field's `SELECT` permission `WHERE guard`, admitting `value`: the
	/// column `IF guard THEN value ELSE NONE END` in the permission `frame`,
	/// with the guard's verdict kept apart from the value it admits, which is
	/// coerced to `kind` before the guard reads it as `$value`.
	pub(crate) fn guarded(
		name: impl Into<Strand>,
		guard: Arc<dyn PhysicalExpr>,
		value: Arc<dyn PhysicalExpr>,
		kind: Option<Kind>,
		frame: EvalFrame,
	) -> Self {
		Self {
			name: name.into(),
			guard: Some(guard),
			value,
			kind,
			frame,
		}
	}
}

#[cfg(test)]
/// Sets each of `columns` on every row of `input`; see the module docs.
#[derive(Debug, Clone)]
pub struct ProjectLayer {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) columns: Vec<LayerColumn>,
	/// The fields the input's schema declares `COMPUTED`, computed here or
	/// not; see [`Self::with_input_schema`].
	pub(crate) computed_fields: Arc<[Strand]>,
	/// Whether the plan was compiled for a caller the layer enforces on; see
	/// [`Self::for_checked_caller`].
	pub(crate) checked_caller: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl ProjectLayer {
	#[cfg(test)]
	pub(crate) fn new(input: Arc<dyn ExecOperator>, columns: Vec<LayerColumn>) -> Self {
		Self {
			input,
			columns,
			computed_fields: Arc::from([]),
			checked_caller: false,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	#[cfg(test)]
	/// Records what the input's schema declares `COMPUTED`.
	///
	/// A computed field has no stored value, so a stored key under its name is
	/// never what a self-dereference reads, and it is present on the row the
	/// scan pipeline yields whatever its value, so an admitted `NONE` lands on
	/// it (see the module documentation). The analyzer emits a column only for
	/// the computed fields the statement reads, so the schema is what names
	/// the rest. The physical planner calls this with the schema of the
	/// layer's input when it compiles the layer.
	pub(crate) fn with_input_schema(mut self, schema: &Schema) -> Self {
		self.computed_fields = schema
			.fields
			.iter()
			.filter(|field| field.computed.is_some())
			.filter_map(|field| field.path.as_field().cloned())
			.collect();
		self
	}

	#[cfg(test)]
	/// Records whether the plan is compiled for a caller whose reads are
	/// permission-checked.
	///
	/// A permission predicate and a computed body are the definer's, and a
	/// principal they are enforced on must not learn them from the plan: for
	/// a checked caller the `EXPLAIN` attributes render each column's
	/// expression as `<permission>` or `<computed>`, and the operator reports
	/// no expressions, so neither the structured plan's `expressions` nor the
	/// embedded plan of a body's nested statement carries their text. An
	/// unchecked caller (a root or database owner) sees the expressions in
	/// full.
	pub(crate) fn for_checked_caller(mut self, checked: bool) -> Self {
		self.checked_caller = checked;
		self
	}

	#[cfg(test)]
	/// The text of one column in the `EXPLAIN` output:
	/// `name = <expression> [: kind] @frame`, a guarded column's expression
	/// being the `IF guard THEN value ELSE NONE END` it was compiled from.
	fn render_column(&self, column: &LayerColumn) -> String {
		let LayerColumn {
			name,
			guard,
			value,
			kind,
			frame,
		} = column;
		let redacted = match frame.mode {
			EvalMode::Permission | EvalMode::MutablePermission if self.checked_caller => {
				Some("<permission>")
			}
			EvalMode::Computed if self.checked_caller => Some("<computed>"),
			EvalMode::Caller
			| EvalMode::Permission
			| EvalMode::MutablePermission
			| EvalMode::Computed => None,
		};
		let mut rendered = format!("{name} = ");
		match (redacted, guard) {
			(Some(text), _) => rendered.push_str(text),
			(None, Some(guard)) => rendered.push_str(&format!(
				"IF {} THEN {} ELSE NONE END",
				guard.to_sql(),
				value.to_sql()
			)),
			(None, None) => rendered.push_str(&value.to_sql()),
		}
		if let Some(kind) = kind {
			rendered.push_str(&format!(" : {}", kind.to_sql()));
		}
		rendered.push_str(match frame.mode {
			EvalMode::Caller => " @caller",
			EvalMode::Permission => " @permission",
			EvalMode::MutablePermission => " @mutable-permission",
			EvalMode::Computed => " @computed",
		});
		rendered
	}

	#[cfg(test)]
	/// Every expression the columns hold, guards included, each under its
	/// column's name.
	fn column_exprs(&self) -> impl Iterator<Item = (&str, &Arc<dyn PhysicalExpr>)> {
		self.columns.iter().flat_map(|column| {
			column
				.guard
				.iter()
				.chain(std::iter::once(&column.value))
				.map(move |expr| (column.name.as_str(), expr))
		})
	}
}

#[cfg(test)]
/// A column with its evaluation context resolved once for the whole stream:
/// the frame differs per column, not per row.
struct PreparedColumn {
	name: Strand,
	guard: Option<Arc<dyn PhysicalExpr>>,
	value: Arc<dyn PhysicalExpr>,
	kind: Option<Kind>,
	mode: EvalMode,
	/// Whether the column lands on a field the input schema declares
	/// `COMPUTED`; see the module documentation for what that changes.
	computed: bool,
	/// The context the guard, and the value of a column on a stored field,
	/// evaluate in: the column's frame.
	ctx: ExecutionContext,
	/// The context the value of a column on a computed field evaluates in:
	/// the `Computed` frame under the column's `AUTH LIMIT`.
	value_ctx: ExecutionContext,
}

#[cfg(test)]
/// The columns of one layer, prepared for its stream.
struct Prepared {
	/// Stored-field columns first, then the columns on computed fields, each
	/// group in plan order.
	columns: Vec<PreparedColumn>,
	/// Whether a stored field's guard is among them, so the columns on
	/// computed fields evaluate against a reduced record.
	stored_guards: bool,
}

#[cfg(test)]
/// What one column does to the row once evaluated; see the module docs.
#[derive(Clone)]
enum Landing {
	/// The key is removed: a field the caller may not read.
	Cut,
	/// The key is set to the value — an admitted `NONE` landing only on a
	/// computed field.
	Set(Value),
}

#[cfg(test)]
impl PreparedColumn {
	#[cfg(test)]
	/// Whether the column is a field's `SELECT` permission: its `NONE` cuts
	/// the field, and its value is part of the view a self-dereference reads.
	fn is_guard(&self) -> bool {
		matches!(self.mode, EvalMode::Permission | EvalMode::MutablePermission)
	}

	#[cfg(test)]
	/// Whether the column is a stored field's `SELECT` permission, which the
	/// reduced record the computed columns read applies.
	fn is_stored_guard(&self) -> bool {
		self.is_guard() && !self.computed
	}
}

#[cfg(test)]
/// The context a column's expression evaluates in.
///
/// - [`EvalMode::Permission`] and [`EvalMode::MutablePermission`]: reads are not
///   permission-checked. [`EvalMode::Permission`] additionally refuses a write, which
///   [`EvalMode::MutablePermission`] does not: a write predicate is reached from a mutation and may
///   carry side effects.
/// - [`EvalMode::Computed`]: reads stay permission-checked as the definer; the frame rejects writes
///   (`computing_field`).
/// - [`EvalMode::Caller`]: the caller's own context.
///
/// An `auth` limit narrows the authority in every mode.
fn frame_context(ctx: &ExecutionContext, frame: &EvalFrame) -> ExecutionContext {
	let mut frame_ctx = match frame.mode {
		EvalMode::Caller => ctx.clone(),
		// A read predicate must be free of observable side effects; a write
		// one is reached from a mutation and may carry them, which is the one
		// thing the two modes differ in here.
		EvalMode::Permission => ctx
			.clone()
			.with_skip_fetch_perms(true)
			.refusing_writes(crate::dbs::NoWriteFrame::PermissionPredicate),
		EvalMode::MutablePermission => ctx.clone().with_skip_fetch_perms(true),
		EvalMode::Computed => ctx.clone().computing_field(),
	};
	if let Some(limit) = &frame.auth {
		frame_ctx = frame_ctx.with_limited_auth(limit);
	}
	frame_ctx
}

#[cfg(test)]
impl ExecOperator for ProjectLayer {
	fn name(&self) -> &'static str {
		"ProjectLayer"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let columns: Vec<String> =
			self.columns.iter().map(|column| self.render_column(column)).collect();
		vec![("columns".to_string(), columns.join(", "))]
	}

	fn required_context(&self) -> ContextLevel {
		self.column_exprs()
			.map(|(_, expr)| expr.required_context())
			.max()
			.unwrap_or(ContextLevel::Root)
			.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		self.input
			.access_mode()
			.combine(self.column_exprs().map(|(_, expr)| expr.access_mode()).combine_all())
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		self.input.cardinality_hint()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn hides_metrics_beneath(&self) -> bool {
		// The rows beneath are the stored records before the guards judged
		// them; a checked caller is shown the counts of what reaches it.
		self.checked_caller
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		// The expressions are the definer's; a checked caller is shown none
		// of them (see `for_checked_caller`).
		if self.checked_caller {
			return Vec::new();
		}
		self.column_exprs().collect()
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	fn constant_output_fields(&self) -> Vec<crate::exec::field_path::FieldPath> {
		self.input.constant_output_fields()
	}

	#[cfg_attr(
		feature = "observability",
		tracing::instrument(name = "ProjectLayer::execute", level = "trace", skip_all)
	)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.access_mode().combine(self.input.access_mode()),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);

		let layer = self.prepare(ctx);
		let layered = input_stream.then(move |batch_result| {
			let layer = layer.clone();
			async move {
				let mut batch = batch_result?;
				for row in batch.values_mut() {
					layer.apply(row).await?;
				}
				Ok(batch)
			}
		});

		Ok(monitor_stream(Box::pin(layered), "ProjectLayer", &self.metrics))
	}
}

#[cfg(test)]
impl ProjectLayer {
	#[cfg(test)]
	/// The layer's columns made ready to apply under `ctx`: each with the
	/// frames it is evaluated in, the columns on stored fields first. Every
	/// engine that applies a projection layer applies it through this.
	pub(crate) fn prepare(&self, ctx: &ExecutionContext) -> PreparedLayer {
		let prepared: Vec<PreparedColumn> = self
			.columns
			.iter()
			.map(|column| {
				// A `Computed`-frame column is a computed field's derivation; a
				// permission column is on a computed field when the input schema
				// declares it so.
				let computed = column.frame.mode == EvalMode::Computed
					|| self.computed_fields.contains(&column.name);
				let frame_ctx = frame_context(ctx, &column.frame);
				let value_ctx = if computed && column.frame.mode != EvalMode::Computed {
					frame_context(ctx, &EvalFrame::computed(column.frame.auth.clone()))
				} else {
					frame_ctx.clone()
				};
				PreparedColumn {
					name: column.name.clone(),
					guard: column.guard.clone(),
					value: Arc::clone(&column.value),
					kind: column.kind.clone(),
					mode: column.frame.mode,
					computed,
					ctx: frame_ctx,
					value_ctx,
				}
			})
			.collect();
		let (stored, computed): (Vec<_>, Vec<_>) =
			prepared.into_iter().partition(|column| !column.computed);
		let stored_guards = stored.iter().any(PreparedColumn::is_guard);
		let prepared = Arc::new(Prepared {
			columns: stored.into_iter().chain(computed).collect(),
			stored_guards,
		});
		let computed_fields = Arc::clone(&self.computed_fields);

		PreparedLayer {
			prepared,
			computed_fields,
		}
	}
}

#[cfg(test)]
/// A [`ProjectLayer`] ready to apply to rows; see [`ProjectLayer::prepare`].
#[derive(Clone)]
pub(crate) struct PreparedLayer {
	prepared: Arc<Prepared>,
	computed_fields: Arc<[Strand]>,
}

#[cfg(test)]
impl PreparedLayer {
	#[cfg(test)]
	/// Set the layer's columns on `row`, as the module documentation states.
	pub(crate) async fn apply(&self, row: &mut Value) -> FlowResult<()> {
		layer_row(row, &self.prepared, &self.computed_fields).await
	}
}

#[cfg(test)]
/// Evaluate every column against `row`, in the order and against the record
/// the module documentation states, then land each result on the row under
/// its name.
async fn layer_row(
	row: &mut Value,
	prepared: &Prepared,
	computed_fields: &[Strand],
) -> FlowResult<()> {
	if !matches!(row, Value::Object(_)) {
		// A layer over a non-object row has nowhere to set its columns; the
		// analyzer only layers record rows, so this is unreachable in a plan
		// it produced.
		return Err(ControlFlow::Err(anyhow::Error::new(EngineError::Internal(format!(
			"cannot project a layer onto a {} row",
			row.kind_of()
		)))));
	}
	let columns = &prepared.columns;
	let mut landings: Vec<Landing> = Vec::with_capacity(columns.len());
	// The reduced record the columns on computed fields evaluate against,
	// built at the first of them: every stored column has landed by then.
	// `None` while the layer has no stored guard, when the input row is that
	// record.
	let mut reduced: Option<Reduced> = None;
	// The record a self-dereference resolves against when nothing reduces
	// the row, built at the first computed column and shared by the rest.
	let mut computing: Option<Option<Arc<ComputingRecord>>> = None;
	for column in columns {
		let value = if column.computed {
			let (doc, record) = if prepared.stored_guards {
				let reduced = reduced
					.get_or_insert_with(|| Reduced::new(row, columns, &landings, computed_fields));
				(&reduced.doc, reduced.computing.clone())
			} else {
				let record = computing.get_or_insert_with(|| {
					computing_record(row, columns, &landings, computed_fields)
				});
				(&*row, record.clone())
			};
			let mut eval_ctx =
				EvalContext::from_exec_ctx(&column.value_ctx).with_value_and_doc(doc);
			eval_ctx.computing_record = record;
			let value = coerce(column, evaluate(&column.value, eval_ctx).await?, row)?;
			// Every later body reads this field as this column derived it,
			// whatever its guard says of it.
			if let Some(reduced) = &mut reduced {
				reduced.land(&column.name, &value);
			}
			value
		} else {
			let eval_ctx = EvalContext::from_exec_ctx(&column.ctx).with_value_and_doc(row);
			coerce(column, evaluate(&column.value, eval_ctx).await?, row)?
		};
		let landing = match &column.guard {
			Some(guard) => {
				let bound = HashMap::from([(Strand::new("value"), value.clone())]);
				let mut eval_ctx = EvalContext::from_exec_ctx(&column.ctx).with_value_and_doc(row);
				eval_ctx.local_params = Some(&bound);
				if evaluate(guard, eval_ctx).await?.is_truthy() {
					Landing::Set(value)
				} else {
					Landing::Cut
				}
			}
			None => match value {
				Value::None if column.is_guard() => Landing::Cut,
				value => Landing::Set(value),
			},
		};
		landings.push(landing);
	}
	let Value::Object(obj) = row else {
		unreachable!("checked above");
	};
	for (column, landing) in columns.iter().zip(landings) {
		set_column(obj, column, landing);
	}
	Ok(())
}

#[cfg(test)]
/// Evaluate one expression of a column. A block body ends in `RETURN`, whose
/// value is the body's; no other control flow has anywhere to go.
async fn evaluate(expr: &Arc<dyn PhysicalExpr>, ctx: EvalContext<'_>) -> FlowResult<Value> {
	match expr.evaluate(ctx).await {
		Ok(value) => Ok(value),
		Err(ControlFlow::Return(value)) => Ok(value),
		Err(ControlFlow::Err(e)) => Err(ControlFlow::Err(e)),
		Err(other) => Err(ControlFlow::Err(anyhow::Error::new(EngineError::Internal(format!(
			"unexpected control flow in a layer column: {other}"
		))))),
	}
}

#[cfg(test)]
/// `value` coerced to the column's kind, when it carries one. A value that
/// does not coerce fails the read with the field-coercion error,
/// naming the record `row` is.
fn coerce(column: &PreparedColumn, value: Value, row: &Value) -> FlowResult<Value> {
	match &column.kind {
		Some(kind) => value.coerce_to_kind(kind).map_err(|error| {
			ControlFlow::Err(anyhow::Error::new(crate::doc::Error::FieldCoerce {
				record: record_of(row),
				field_name: column.name.to_string(),
				error: Box::new(error),
			}))
		}),
		None => Ok(value),
	}
}

#[cfg(test)]
/// Land one column on the row; see the module docs.
fn set_column(row: &mut Object, column: &PreparedColumn, landing: Landing) {
	match landing {
		Landing::Cut => {
			row.remove(column.name.as_str());
		}
		// An admitted stored field is as the row has it.
		Landing::Set(Value::None) if column.is_guard() && !column.computed => {}
		Landing::Set(value) => {
			row.insert(column.name.as_str(), value);
		}
	}
}

/// The record a coercion error names: the row's id, or the row itself when
/// it has none.
pub(crate) fn record_of(row: &Value) -> String {
	match row {
		Value::Object(obj) => match obj.get("id") {
			Some(Value::RecordId(rid)) => rid.to_sql(),
			_ => row.to_sql(),
		},
		other => other.to_sql(),
	}
}

#[cfg(test)]
/// The record as the layer's stored columns leave it; see the module
/// documentation.
struct Reduced {
	/// The input row with every stored column landed, carrying each computed
	/// value landed so far.
	doc: Value,
	/// The record a self-dereference resolves against, when the row carries
	/// one: `doc`'s stored fields alone.
	computing: Option<Arc<ComputingRecord>>,
}

#[cfg(test)]
impl Reduced {
	#[cfg(test)]
	/// The reduced record of `row`, `landings` being those of the stored
	/// columns, which precede every computed one.
	fn new(
		row: &Value,
		columns: &[PreparedColumn],
		landings: &[Landing],
		computed_fields: &[Strand],
	) -> Self {
		let Value::Object(obj) = row else {
			unreachable!("layer_row checks the row is an object");
		};
		let mut doc = obj.clone();
		for (column, landing) in columns.iter().zip(landings) {
			if column.is_stored_guard() {
				set_column(&mut doc, column, landing.clone());
			}
		}
		let computing = match obj.get("id") {
			Some(Value::RecordId(rid)) => {
				let mut stored = doc.clone();
				for name in computed_names(columns, computed_fields) {
					stored.remove(name);
				}
				Some(Arc::new(ComputingRecord {
					rid: rid.clone(),
					stored: Some(Value::Object(stored)),
				}))
			}
			_ => None,
		};
		Self {
			doc: Value::Object(doc),
			computing,
		}
	}

	#[cfg(test)]
	/// Records the value a column on a computed field derived, for the
	/// columns after it to read.
	fn land(&mut self, name: &str, value: &Value) {
		if let Value::Object(doc) = &mut self.doc {
			doc.insert(name, value.clone());
		}
	}
}

#[cfg(test)]
/// Every field name that is computed: this layer's columns on computed fields
/// and what the input schema declares `COMPUTED`.
fn computed_names<'a>(
	columns: &'a [PreparedColumn],
	computed_fields: &'a [Strand],
) -> impl Iterator<Item = &'a str> + Clone {
	columns
		.iter()
		.filter(|column| column.computed)
		.map(|column| column.name.as_str())
		.chain(computed_fields.iter().map(Strand::as_str))
}

#[cfg(test)]
/// The record a computed column is computed for when nothing reduces the
/// row — the layer has no stored guard — with the view a self-dereference
/// reads: storage, unless the row carries a key under a computed name, which
/// the definition shadows.
fn computing_record(
	row: &Value,
	columns: &[PreparedColumn],
	landings: &[Landing],
	computed_fields: &[Strand],
) -> Option<Arc<ComputingRecord>> {
	let Value::Object(obj) = row else {
		return None;
	};
	let Some(Value::RecordId(rid)) = obj.get("id") else {
		return None;
	};
	let names = computed_names(columns, computed_fields);
	let shadowed = names.clone().any(|name| obj.contains_key(name));
	let stored = shadowed.then(|| {
		let mut view = obj.clone();
		for (column, landing) in columns.iter().zip(landings) {
			if column.is_stored_guard() {
				set_column(&mut view, column, landing.clone());
			}
		}
		for name in names {
			view.remove(name);
		}
		Value::Object(view)
	});
	Some(Arc::new(ComputingRecord {
		rid: rid.clone(),
		stored,
	}))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{TestDb, ValuesOperator, collect, physical_expr, val};

	async fn rows(srcs: &[&str]) -> Vec<Value> {
		let mut out = Vec::with_capacity(srcs.len());
		for src in srcs {
			out.push(val(src).await);
		}
		out
	}

	fn field(row: &Value, name: &str) -> Value {
		match row {
			Value::Object(obj) => obj.get(name).cloned().unwrap_or(Value::None),
			_ => Value::None,
		}
	}

	fn has_key(row: &Value, name: &str) -> bool {
		matches!(row, Value::Object(obj) if obj.contains_key(name))
	}

	async fn computed(name: &str, src: &str, ctx: &ExecutionContext) -> LayerColumn {
		LayerColumn::new(name, physical_expr(src, ctx).await, None, EvalFrame::computed(None))
	}

	/// A permission column without a guard: `PERMISSIONS FOR select NONE` is
	/// `permission(f, "NONE")`, `FULL` over the stored value `permission(f, "f")`.
	async fn permission(name: &str, src: &str, ctx: &ExecutionContext) -> LayerColumn {
		LayerColumn::new(name, physical_expr(src, ctx).await, None, EvalFrame::permission(None))
	}

	/// `PERMISSIONS FOR select WHERE guard` admitting `value`.
	async fn guard(name: &str, guard: &str, value: &str, ctx: &ExecutionContext) -> LayerColumn {
		LayerColumn::guarded(
			name,
			physical_expr(guard, ctx).await,
			physical_expr(value, ctx).await,
			None,
			EvalFrame::permission(None),
		)
	}

	fn layer(input: Arc<dyn ExecOperator>, columns: Vec<LayerColumn>) -> Arc<dyn ExecOperator> {
		Arc::new(ProjectLayer::new(input, columns))
	}

	#[tokio::test]
	async fn each_column_is_set_on_the_row_under_its_name() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let input =
			ValuesOperator::new(rows(&["{ price: 2, qty: 3 }", "{ price: 5, qty: 1 }"]).await);
		let op = layer(input, vec![computed("total", "price * qty", &ctx).await]);
		let out = collect(&op, &ctx).await;
		assert_eq!(field(&out[0], "total"), Value::from(6));
		assert_eq!(field(&out[1], "total"), Value::from(5));
		// The stored fields are untouched.
		assert_eq!(field(&out[0], "price"), Value::from(2));
	}

	#[tokio::test]
	async fn a_column_reads_the_input_row_not_its_siblings() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		// Both columns read the input row: `b` sees the stored `a`, not the
		// column `a` this layer sets.
		let input = ValuesOperator::new(rows(&["{ n: 4, a: 100 }"]).await);
		let op = layer(
			input,
			vec![computed("a", "n * 2", &ctx).await, computed("b", "a + 1", &ctx).await],
		);
		let out = collect(&op, &ctx).await;
		assert_eq!(field(&out[0], "a"), Value::from(8));
		assert_eq!(field(&out[0], "b"), Value::from(101));

		// A layer above reads the column the layer below set.
		let lower = layer(
			ValuesOperator::new(rows(&["{ n: 4 }"]).await),
			vec![computed("a", "n * 2", &ctx).await],
		);
		let upper = layer(lower, vec![computed("b", "a + 1", &ctx).await]);
		let out = collect(&upper, &ctx).await;
		assert_eq!(field(&out[0], "a"), Value::from(8));
		assert_eq!(field(&out[0], "b"), Value::from(9));
	}

	#[tokio::test]
	async fn the_value_is_coerced_to_the_declared_kind() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let input = ValuesOperator::new(rows(&["{ n: 4 }"]).await);
		let op = layer(
			input,
			vec![LayerColumn::new(
				"f",
				physical_expr("n", &ctx).await,
				Some(Kind::Float),
				EvalFrame::computed(None),
			)],
		);
		let out = collect(&op, &ctx).await;
		assert_eq!(field(&out[0], "f"), Value::from(4.0f64));
	}

	#[tokio::test]
	async fn a_value_that_does_not_coerce_is_an_error_naming_the_field() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let input = ValuesOperator::new(rows(&["{ id: t:one, n: 'x' }"]).await);
		let op = ProjectLayer::new(
			input,
			vec![LayerColumn::new(
				"f",
				physical_expr("n", &ctx).await,
				Some(Kind::Int),
				EvalFrame::computed(None),
			)],
		);
		let err = crate::exec::operators::test_util::drain_err(&op, &ctx).await;
		assert!(
			matches!(
				err.downcast_ref::<crate::doc::Error>(),
				Some(crate::doc::Error::FieldCoerce { field_name, record, .. })
					if field_name == "f" && record == "t:one"
			),
			"expected a coercion error naming the field, got {err:?}"
		);
	}

	#[tokio::test]
	async fn a_block_body_returns_its_value_and_other_control_flow_is_an_error() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let input = ValuesOperator::new(rows(&["{ n: 4 }"]).await);
		let op = layer(
			input,
			vec![computed("c", "{ LET $twice = n * 2; RETURN $twice + 1 }", &ctx).await],
		);
		let out = collect(&op, &ctx).await;
		assert_eq!(field(&out[0], "c"), Value::from(9));

		let op = ProjectLayer::new(
			ValuesOperator::new(rows(&["{ n: 4 }"]).await),
			vec![computed("c", "{ BREAK }", &ctx).await],
		);
		let err = crate::exec::operators::test_util::drain_err(&op, &ctx).await;
		assert!(err.to_string().contains("unexpected control flow"), "{err}");
	}

	#[tokio::test]
	async fn a_guard_cuts_the_field_it_denies_and_sets_the_one_it_admits() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let input = ValuesOperator::new(
			rows(&[
				"{ id: t:one, owner: 'a', secret: 's' }",
				"{ id: t:two, owner: 'b', secret: 't' }",
			])
			.await,
		);
		let op = layer(input, vec![guard("secret", "owner = 'a'", "secret", &ctx).await]);
		let out = collect(&op, &ctx).await;
		assert_eq!(field(&out[0], "secret"), Value::from("s"));
		// Denied: the key is absent, not `NONE`.
		assert!(!has_key(&out[1], "secret"), "{:?}", out[1]);

		// A permission column without a guard lands its value; `NONE` cuts.
		let op = layer(
			ValuesOperator::new(rows(&["{ id: t:one, secret: 's', open: 'o' }"]).await),
			vec![permission("secret", "NONE", &ctx).await, permission("open", "open", &ctx).await],
		);
		let out = collect(&op, &ctx).await;
		assert!(!has_key(&out[0], "secret"), "{:?}", out[0]);
		assert_eq!(field(&out[0], "open"), Value::from("o"));

		// A computed column landing `NONE` is present and `NONE`.
		let op = layer(
			ValuesOperator::new(rows(&["{ id: t:one }"]).await),
			vec![computed("c", "NONE", &ctx).await],
		);
		let out = collect(&op, &ctx).await;
		assert!(has_key(&out[0], "c"), "{:?}", out[0]);
	}

	/// A schema declaring each of `names` as a `COMPUTED` field.
	fn computed_schema(names: &[&str]) -> Schema {
		use crate::expr::logical::{
			ComputedBody, FieldPath, FieldSchema, LogicalExpr, LogicalLiteral,
		};
		let fields = names
			.iter()
			.map(|name| {
				let mut field = FieldSchema::new(FieldPath::field(*name), Kind::Any);
				field.computed = Some(ComputedBody {
					expr: LogicalExpr::Literal(LogicalLiteral::None),
					kind: None,
					auth: None,
				});
				field
			})
			.collect();
		Schema::closed(fields)
	}

	#[tokio::test]
	async fn an_admitted_none_lands_on_a_computed_field_and_leaves_a_stored_one_as_it_is() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		// `c` and `d` are computed fields whose re-derivation against the
		// reduced record is `NONE`: admitted, each is present and `NONE` —
		// whether the row carries a first derivation (`c`) or none was
		// derived (`d`) — as the scan pipeline inserts every computed field.
		// `s` is a stored field the row lacks: admitted, it stays absent.
		let input = ValuesOperator::new(rows(&["{ id: t:one, c: 'x', secret: 'x' }"]).await);
		let op: Arc<dyn ExecOperator> = Arc::new(
			ProjectLayer::new(
				input,
				vec![
					guard("c", "$value != 'x'", "IF false THEN secret ELSE NONE END", &ctx).await,
					guard("d", "true", "NONE", &ctx).await,
					guard("s", "true", "s", &ctx).await,
				],
			)
			.with_input_schema(&computed_schema(&["c", "d"])),
		);
		let out = collect(&op, &ctx).await;
		assert!(has_key(&out[0], "c"), "{:?}", out[0]);
		assert_eq!(field(&out[0], "c"), Value::None);
		assert!(has_key(&out[0], "d"), "{:?}", out[0]);
		assert_eq!(field(&out[0], "d"), Value::None);
		assert!(!has_key(&out[0], "s"), "{:?}", out[0]);

		// A denied computed field is cut whatever the row carried.
		let input = ValuesOperator::new(rows(&["{ id: t:one, c: 'x' }"]).await);
		let op: Arc<dyn ExecOperator> = Arc::new(
			ProjectLayer::new(input, vec![guard("c", "false", "'kept'", &ctx).await])
				.with_input_schema(&computed_schema(&["c"])),
		);
		let out = collect(&op, &ctx).await;
		assert!(!has_key(&out[0], "c"), "{:?}", out[0]);
	}

	#[tokio::test]
	async fn a_column_on_a_computed_field_reads_the_record_as_the_stored_guards_leave_it() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		// `g` is cut. The value of every column on a computed field is
		// evaluated against the record with that cut applied — `g` reads
		// `NONE` in a guarded column and in a derivation alike — and each
		// landed value is what the columns after it read: `e` reads the `c`
		// this layer derived, not the `c` the row arrived with.
		let input = ValuesOperator::new(rows(&["{ id: t:one, g: 'x', n: 1, c: 100 }"]).await);
		let op: Arc<dyn ExecOperator> = Arc::new(
			ProjectLayer::new(
				input,
				vec![
					permission("g", "NONE", &ctx).await,
					guard("c", "$value = NONE", "g", &ctx).await,
					computed("d", "g", &ctx).await,
					guard("s", "true", "n + 1", &ctx).await,
					computed("e", "s * 10", &ctx).await,
				],
			)
			.with_input_schema(&computed_schema(&["c", "d", "s", "e"])),
		);
		let out = collect(&op, &ctx).await;
		assert!(!has_key(&out[0], "g"), "{:?}", out[0]);
		assert!(has_key(&out[0], "c"), "{:?}", out[0]);
		assert_eq!(field(&out[0], "c"), Value::None);
		assert_eq!(field(&out[0], "d"), Value::None);
		assert_eq!(field(&out[0], "s"), Value::from(2));
		assert_eq!(field(&out[0], "e"), Value::from(20));
	}

	#[tokio::test]
	async fn a_guard_on_a_computed_field_is_judged_against_the_record_as_stored() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		// The guards read `g` raw — `'x'`, though the layer cuts it — with
		// `$value` bound to the value the column derived, coerced to its kind.
		let input = ValuesOperator::new(rows(&["{ id: t:one, g: 'x', n: 1 }"]).await);
		let coerced = LayerColumn::guarded(
			"k",
			physical_expr("type::is_float($value)", &ctx).await,
			physical_expr("2", &ctx).await,
			Some(Kind::Float),
			EvalFrame::permission(None),
		);
		let op: Arc<dyn ExecOperator> = Arc::new(
			ProjectLayer::new(
				input,
				vec![
					permission("g", "NONE", &ctx).await,
					guard("c", "g = 'x'", "n", &ctx).await,
					guard("d", "g = NONE", "n", &ctx).await,
					guard("v", "$value = 1", "n", &ctx).await,
					coerced,
				],
			)
			.with_input_schema(&computed_schema(&["c", "d", "v", "k"])),
		);
		let out = collect(&op, &ctx).await;
		assert_eq!(field(&out[0], "c"), Value::from(1));
		assert!(!has_key(&out[0], "d"), "{:?}", out[0]);
		assert_eq!(field(&out[0], "v"), Value::from(1));
		assert_eq!(field(&out[0], "k"), Value::from(2.0f64));
	}

	/// The rows of `t:one` as the scan yields them: the record as stored.
	async fn stored_row() -> Vec<Value> {
		rows(&["{ id: t:one, hidden: 'STALE', note: 'n', secret: 's' }"]).await
	}

	const STORED: &str = "DEFINE TABLE t SCHEMALESS;
		 CREATE t:one SET hidden = 'STALE', note = 'n', secret = 's';";

	#[tokio::test]
	async fn a_self_dereference_does_not_read_a_stored_key_under_a_computed_name() {
		let db = TestDb::new(STORED).await;
		let ctx = db.exec_ctx().await;
		let op = layer(
			ValuesOperator::new(stored_row().await),
			vec![
				computed("hidden", "'REDACTED'", &ctx).await,
				computed("viaid", "id.hidden", &ctx).await,
				computed("vianote", "$this.id.note", &ctx).await,
			],
		);
		let out = collect(&op, &ctx).await;
		// The computed name has no stored value; a plain field reads as stored.
		assert_eq!(field(&out[0], "viaid"), Value::None);
		assert_eq!(field(&out[0], "vianote"), Value::from("n"));
		// The column shadows the stored key on the row itself.
		assert_eq!(field(&out[0], "hidden"), Value::from("REDACTED"));
	}

	#[tokio::test]
	async fn a_self_dereference_reads_a_restricted_field_through_its_guard() {
		let db = TestDb::new(STORED).await;
		let ctx = db.exec_ctx().await;
		let read_secret_via_id = |guard_src: &'static str, guard_first: bool| {
			let ctx = ctx.clone();
			async move {
				let g = permission("secret", guard_src, &ctx).await;
				let c = computed("c", "id.secret", &ctx).await;
				let columns = if guard_first {
					vec![g, c]
				} else {
					vec![c, g]
				};
				let op = layer(ValuesOperator::new(stored_row().await), columns);
				field(&collect(&op, &ctx).await[0], "c")
			}
		};
		// Denied: absent. Allowed: the stored value.
		assert_eq!(read_secret_via_id("NONE", true).await, Value::None);
		assert_eq!(read_secret_via_id("secret", true).await, Value::from("s"));
		// The permission columns run first whatever their position in the
		// column list, so the body sees the guard's decision either way.
		assert_eq!(read_secret_via_id("NONE", false).await, Value::None);
		assert_eq!(read_secret_via_id("secret", false).await, Value::from("s"));
	}

	#[tokio::test]
	async fn the_input_schema_names_the_computed_fields_the_statement_did_not_compute() {
		use crate::expr::logical::{ComputedBody, FieldPath, FieldSchema, LogicalLiteral};
		let db = TestDb::new(STORED).await;
		let ctx = db.exec_ctx().await;
		let columns = || {
			let ctx = ctx.clone();
			async move { vec![computed("viaid", "id.hidden", &ctx).await] }
		};
		// Without the schema nothing says `hidden` is computed, and the row
		// has nothing to hide from storage: the dereference reads storage.
		let op = layer(ValuesOperator::new(stored_row().await), columns().await);
		assert_eq!(field(&collect(&op, &ctx).await[0], "viaid"), Value::from("STALE"));
		// With it, the stored key under the computed name is shadowed.
		let mut hidden = FieldSchema::new(FieldPath::field("hidden"), Kind::Any);
		hidden.computed = Some(ComputedBody {
			expr: crate::expr::logical::LogicalExpr::Literal(LogicalLiteral::String(
				"REDACTED".into(),
			)),
			kind: None,
			auth: None,
		});
		let schema = crate::expr::logical::Schema::closed(vec![hidden]);
		let op: Arc<dyn ExecOperator> = Arc::new(
			ProjectLayer::new(ValuesOperator::new(stored_row().await), columns().await)
				.with_input_schema(&schema),
		);
		assert_eq!(field(&collect(&op, &ctx).await[0], "viaid"), Value::None);
	}

	#[tokio::test]
	async fn a_planned_select_star_carries_the_computed_field() {
		use crate::exec::operators::test_util::plan_statement;
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMAFULL;
			 DEFINE FIELD a ON t TYPE int;
			 DEFINE FIELD c ON t TYPE int COMPUTED a + 1;
			 CREATE t:one SET a = 1;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		for sql in
			["SELECT * FROM t", "SELECT * FROM t WHERE c = 2", "SELECT * FROM (SELECT * FROM t)"]
		{
			let plan = plan_statement(sql, &ctx).await;
			let out = collect(&plan, &ctx).await;
			assert_eq!(out, rows(&["{ id: t:one, a: 1, c: 2 }"]).await, "{sql}");
		}
	}

	#[tokio::test]
	async fn a_planned_read_keeps_an_admitted_computed_field_whose_value_is_none() {
		use crate::dbs::Session;
		use crate::exec::operators::test_util::plan_statement;
		use crate::kvs::TransactionType;
		// `secret` is cut for the caller, so every field computed from it is
		// re-derived to `NONE` in the guard layer. `denied` is cut in its own
		// right; `gated`'s clause admits the re-derived `NONE`, so the field
		// is present and `NONE`, as the scan pipeline yields it.
		let db = TestDb::new_with_auth(
			"DEFINE ACCESS user ON DATABASE TYPE RECORD
			     SIGNUP (CREATE user) SIGNIN (SELECT * FROM user)
			     DURATION FOR TOKEN 1h, FOR SESSION 1h;
			 DEFINE TABLE items SCHEMALESS PERMISSIONS FOR select FULL;
			 DEFINE FIELD secret ON items PERMISSIONS FOR select NONE;
			 DEFINE FIELD denied ON items COMPUTED secret PERMISSIONS FOR select NONE;
			 DEFINE FIELD gated ON items COMPUTED secret PERMISSIONS FOR select WHERE $value != 'x';
			 CREATE items:one SET secret = 'x';",
		)
		.await;
		let session = Session::for_record(
			"test",
			"test",
			"user",
			crate::types::PublicValue::String("user:tobie".to_owned()),
		);
		let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;
		for sql in ["SELECT * FROM items:one", "SELECT * FROM items WHERE id = items:one"] {
			let plan = plan_statement(sql, &ctx).await;
			let out = collect(&plan, &ctx).await;
			assert_eq!(out, rows(&["{ gated: NONE, id: items:one }"]).await, "{sql}");
		}
	}

	#[tokio::test]
	async fn the_operator_is_named_and_reports_its_columns() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let columns = vec![
			LayerColumn::new(
				"c",
				physical_expr("a + 1", &ctx).await,
				Some(Kind::Int),
				EvalFrame::computed(None),
			),
			guard("secret", "owner = $auth.id", "secret", &ctx).await,
		];
		let op = ProjectLayer::new(ValuesOperator::new(Vec::new()), columns.clone());
		assert_eq!(op.name(), "ProjectLayer");
		// The guard is an expression of the operator in its own right.
		assert_eq!(op.expressions().len(), 3);
		// An unchecked caller sees the expressions in full.
		assert_eq!(
			op.attrs(),
			vec![(
				"columns".to_string(),
				"c = a + 1 : int @computed, \
				 secret = IF owner = $auth.id THEN secret ELSE NONE END @permission"
					.to_string()
			)]
		);
		// A checked caller never sees a body or a predicate it is enforced on,
		// in the attributes or as an expression of the operator.
		let op =
			ProjectLayer::new(ValuesOperator::new(Vec::new()), columns).for_checked_caller(true);
		assert_eq!(
			op.attrs(),
			vec![(
				"columns".to_string(),
				"c = <computed> : int @computed, secret = <permission> @permission".to_string()
			)]
		);
		assert!(op.expressions().is_empty());
	}
}
