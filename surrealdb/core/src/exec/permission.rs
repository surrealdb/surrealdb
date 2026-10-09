//! Permission resolution, and the compilation route for every other stored
//! catalog clause, for the stream executor.
//!
//! This module provides utilities for resolving and checking table/field permissions
//! at execution time. Since SurrealQL allows DDL and DML interleaving within transactions,
//! permissions must be resolved from the current transaction's schema view rather than
//! at planning time.
//!
//! A stored clause is compiled from its text into the engine's
//! scalar. A clause that cannot be — one the lowering has no form for, or one
//! holding a write no operator compiles — is carried as an
//! [`UncompilableClause`], which fails with `PlannerUnsupported` where the
//! clause is evaluated and nowhere else, so a statement that merely reaches a
//! table declaring such a clause is not refused.
//!
//! # The clauses, and who evaluates them
//!
//! | Clause | Route | Evaluated by |
//! | --- | --- | --- |
//! | a table or field `PERMISSIONS FOR select` | [`permission_to_physical`] | a scan enforcing permissions |
//! | a field `COMPUTED` body | `scan::pipeline` | a scan materialising a row |
//! | a field `DEFAULT` | [`field_default_physical`] | the write path, deriving a record id |
//! | an event's `WHEN` and `THEN` | [`event_physical`] | the write path, after the record is stored |
//! | a materialized view's condition and field list | [`materialized_view_physical`] | the write path, after the record is stored |
//! | an index's columns and a `COUNT` index's `WHERE` guard | [`index_entry_physical`] | the write path and the index build's replay |
//!
//! Each route's documentation states the frame its clause is evaluated in —
//! whose authority, with permissions enforced or not, and what is bound as the
//! document and as the parameters. The `*_frame` helper beside each route
//! builds that frame from the caller's context, so the decision lives here
//! rather than in each caller.
//!
//! A subscription's `WHERE`, field list and `FETCH` have no route here. Their
//! frame is the subscription owner's authority rather than a narrowing of the
//! writer's, and a scalar is compiled for one principal
//! ([`CompiledGuard`]) — so both halves of that clause need something this
//! module does not have.

use std::collections::HashSet;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use surrealdb_strand::TableName;
use surrealdb_types::{SqlFormat, ToSql};

#[cfg(test)]
use crate::catalog::Record;
use crate::catalog::aggregation::AggregateFields;
use crate::catalog::{
	EventDefinition, FieldDefinition, IndexDefinition, LogicalBody, Permission, TableDefinition,
	ViewDefinition,
};
use crate::doc::Action as DocAction;
use crate::err::{EngineError, Error};
use crate::exe::FlowResultExt as _;
use crate::exec::operators::scan::pipeline::{
	FieldState, build_field_state, compute_fields_for_value, narrowing_auth_limit,
};
use crate::exec::operators::write::place::WritePlace;
use crate::exec::planner::{Planner, compile_expr};
use crate::exec::{
	AccessMode, BoxFut, ContextLevel, DatabaseContext, Error as ExecError, EvalContext,
	ExecutionContext, FlowResult, PhysicalExpr,
};
use crate::expr::computed_deps::extract_computed_deps;
pub(crate) use crate::expr::logical::GuardKind;
use crate::expr::logical::{LogicalExpr, LogicalPermission, LogicalPermissionRef};
use crate::expr::{ControlFlow, Expr, Fields};
use crate::iam::{Action, Auth};
#[cfg(test)]
use crate::idx::trees::gate::{BoxGateFut, TableSelectGate};
use crate::kvs::Transaction;
#[cfg(test)]
use crate::val::RecordId;
use crate::val::Value;

/// Result of a permission check.
#[derive(Debug, Clone)]
pub enum PhysicalPermission {
	/// Permission allows access unconditionally
	Allow,
	/// Permission denies access unconditionally
	Deny,
	/// Permission requires per-record evaluation
	Conditional(Arc<dyn PhysicalExpr>),
}

/// The `SELECT` permission of a table as the engine applies it,
/// compiled through `planner`.
///
/// The guard is compiled from the definition's logical body — the clause's
/// stored text, lowered once for the life of the definition and shared with
/// the analyzer — so a statement that plans the table does not lower the
/// clause again. A guard whose text has no logical form is carried as an
/// [`UncompilableClause`].
/// A table with no definition denies, as
/// [`table_select_permission`](crate::catalog::table_select_permission)
/// resolves it. Inner subqueries inherit the planner's `CycleGuard`, so a
/// self-referential permission (`WHERE (SELECT FROM same_table) != NONE`)
/// falls back to runtime resolution for that subtree instead of recursing.
///
/// A conditional guard is judged against the row with the computed fields it
/// reads derived; see [`ComputedFieldGuard`].
pub(crate) async fn table_select_permission_physical(
	table_def: Option<&TableDefinition>,
	planner: &Planner<'_>,
) -> Result<PhysicalPermission, Error> {
	let Some(def) = table_def else {
		return Ok(PhysicalPermission::Deny);
	};
	let permission = read_permission_to_physical(
		&def.permissions.select,
		def.select_permission_logical(),
		GuardKind::Read,
		planner,
	)
	.await?;
	Ok(match (permission, &def.permissions.select) {
		(PhysicalPermission::Conditional(guard), Permission::Specific(clause)) => {
			PhysicalPermission::Conditional(ComputedFieldGuard::for_clause(
				guard, &def.name, clause,
			))
		}
		(permission, _) => permission,
	})
}

/// A table's `SELECT` guard, judged against the row with the computed fields
/// the guard reads derived onto it.
///
/// A computed field is never stored, so a guard naming one reads `NONE` off
/// the record as stored: a clause written to deny (`WHERE hidden != true`)
/// admits every row, and one written to allow (`WHERE visible = true`) admits
/// none. This judges the guard instead against a copy of the row carrying the
/// computed fields the guard reads, and those they read in turn, derived in
/// dependency order in the frame the guard itself is evaluated in: reads are
/// not permission-checked, writes are refused, and each body runs under its
/// field's `AUTH LIMIT`. A computed field therefore reads, inside the guard,
/// what its body would read written there in its place — which is also how the
/// computed fields of a record the guard follows are derived
/// ([`fetch_record_no_perms`](crate::exec::operators::fetch::fetch_record_no_perms)),
/// and how the document pipeline derives them for its own permission checks
/// (`Document::permission_view`). Derived as the caller, a body reading a
/// record the caller may not select would derive from `NONE`, satisfying a
/// clause written to deny, and a body reading its own table would re-enter
/// this guard.
///
/// The copy is the guard's alone: the row it judges goes on as it came, and
/// whatever reads the row next derives what it reads for itself, as the
/// caller.
///
/// The table's field state is resolved on the node's first evaluation and
/// kept for the life of the node, as a scan planned with its field state keeps
/// it for the life of its plan. Where the guard reads no computed field of the
/// table, it is evaluated on the row as it came, and a batch through the
/// guard's own batch evaluation; once the field state is resolved that way, a
/// row or batch is handed to the guard without passing through this node.
pub(crate) struct ComputedFieldGuard {
	guard: Arc<dyn PhysicalExpr>,
	table: TableName,
	/// The fields the guard reads by name, or `None` when it reads the row
	/// other than by name and every computed field is derived.
	reads: Option<HashSet<String>>,
	/// The table's field state narrowed to the computed fields `reads`
	/// reaches, or `None` once resolved to hold none.
	state: tokio::sync::OnceCell<Option<FieldState>>,
}

impl ComputedFieldGuard {
	/// `guard`, the compiled form of the stored clause `clause` of `table`,
	/// judged as the type documentation states — or `guard` itself when the
	/// clause reads no field of the row at all.
	pub(crate) fn for_clause(
		guard: Arc<dyn PhysicalExpr>,
		table: &TableName,
		clause: &Expr,
	) -> Arc<dyn PhysicalExpr> {
		let deps = extract_computed_deps(clause);
		if deps.is_complete && deps.fields.is_empty() {
			return guard;
		}
		Self::for_reads(
			guard,
			table.clone(),
			deps.is_complete.then(|| deps.fields.into_iter().collect()),
		)
	}

	/// `guard` over rows of `table`, reading `reads` of them by name, or the
	/// row as a whole when `reads` is `None`.
	pub(crate) fn for_reads(
		guard: Arc<dyn PhysicalExpr>,
		table: TableName,
		reads: Option<HashSet<String>>,
	) -> Arc<dyn PhysicalExpr> {
		Arc::new(Self {
			guard,
			table,
			reads,
			state: tokio::sync::OnceCell::new(),
		})
	}

	/// The field state the guard's reads need, or `None` when they need no
	/// computed field.
	///
	/// `ctx` is the derivation's frame ([`Self::derive`]), so the bodies are
	/// compiled for reads that are not permission-checked.
	async fn state(&self, ctx: &ExecutionContext) -> FlowResult<Option<&FieldState>> {
		let state = self
			.state
			.get_or_try_init(|| async {
				let state = build_field_state(ctx, &self.table, false, self.reads.as_ref()).await?;
				Ok::<_, ControlFlow>((!state.computed_fields.is_empty()).then_some(state))
			})
			.await?;
		Ok(state.as_ref())
	}

	/// `row` with the computed fields of `state` derived onto a copy, in
	/// `frame`; see the type documentation.
	async fn derive(
		frame: &ExecutionContext,
		state: &FieldState,
		row: &Value,
	) -> FlowResult<Value> {
		let mut derived = row.clone();
		compute_fields_for_value(frame, state, &mut derived, true).await?;
		Ok(derived)
	}

	/// The frame the computed fields are derived in: `ctx` with its reads not
	/// permission-checked. Each body narrows it to its field's `AUTH LIMIT`
	/// when it runs, and the guard's own frame refuses writes.
	fn frame(ctx: &ExecutionContext) -> ExecutionContext {
		ctx.with_perms_suppressed(true)
	}
}

/// The field state is an evaluation-time cache and says nothing about the
/// node, so it is not printed.
impl std::fmt::Debug for ComputedFieldGuard {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("ComputedFieldGuard")
			.field("guard", &self.guard)
			.field("table", &self.table)
			.field("reads", &self.reads)
			.finish_non_exhaustive()
	}
}

impl PhysicalExpr for ComputedFieldGuard {
	fn name(&self) -> &'static str {
		"ComputedFieldGuard"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		// The field definitions belong to a table of a database.
		ContextLevel::Database.max(self.guard.required_context())
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		// Resolved to derive nothing: the guard judges the row as it came.
		if matches!(self.state.get(), Some(None)) {
			return self.guard.evaluate(ctx);
		}
		Box::pin(async move {
			// Only a record carries fields to derive.
			let Some(row @ Value::Object(_)) = ctx.document_root.or(ctx.current_value) else {
				return self.guard.evaluate(ctx).await;
			};
			let frame = Self::frame(ctx.exec_ctx);
			let Some(state) = self.state(&frame).await? else {
				return self.guard.evaluate(ctx).await;
			};
			let derived = Self::derive(&frame, state, row).await?;
			self.guard.evaluate(ctx.with_value_and_doc(&derived)).await
		})
	}

	fn evaluate_batch<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		values: &'a [Value],
	) -> BoxFut<'a, FlowResult<Vec<Value>>> {
		// Resolved to derive nothing: the guard judges the batch as it came.
		if matches!(self.state.get(), Some(None)) {
			return self.guard.evaluate_batch(ctx, values);
		}
		Box::pin(async move {
			let frame = Self::frame(ctx.exec_ctx);
			let Some(state) = self.state(&frame).await? else {
				return self.guard.evaluate_batch(ctx, values).await;
			};
			let mut derived = Vec::with_capacity(values.len());
			for row in values {
				derived.push(match row {
					Value::Object(_) => Self::derive(&frame, state, row).await?,
					other => other.clone(),
				});
			}
			self.guard.evaluate_batch(ctx, &derived).await
		})
	}

	fn access_mode(&self) -> AccessMode {
		// The derivation runs in a frame that refuses writes.
		self.guard.access_mode()
	}

	fn expr_children(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("guard", &self.guard)]
	}
}

/// Renders as the guard it judges.
impl ToSql for ComputedFieldGuard {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.guard.fmt_sql(f, fmt);
	}
}

/// A stored `SELECT` clause as the engine applies it: `logical`,
/// the clause compiled from its stored text by the definition that holds it,
/// or — when the text has no logical form — an [`UncompilableClause`] carrying
/// the reason. `permission` is the definition's expression form of the same
/// clause; the two are the same clause of the same definition, and a caller
/// passes both halves of one definition, never a body of its own making.
///
/// A `RETURN` leaving the guard is its verdict, as a write's gate judges it.
pub(crate) async fn permission_to_physical(
	permission: &Permission,
	logical: anyhow::Result<LogicalPermission>,
	guard: GuardKind,
	planner: &Planner<'_>,
) -> Result<PhysicalPermission, Error> {
	permission_to_physical_as(permission, logical, guard, planner, false).await
}

/// [`permission_to_physical`] for a guard a read applies to the record it
/// reads — a record read by id or dereferenced, and the fields it shows —
/// where a `RETURN`, `BREAK` or `CONTINUE` leaving the guard is an error
/// ([`crate::exec::scalar_unit::ScalarUnit::predicate`]).
pub(crate) async fn read_permission_to_physical(
	permission: &Permission,
	logical: anyhow::Result<LogicalPermission>,
	guard: GuardKind,
	planner: &Planner<'_>,
) -> Result<PhysicalPermission, Error> {
	permission_to_physical_as(permission, logical, guard, planner, true).await
}

async fn permission_to_physical_as(
	permission: &Permission,
	logical: anyhow::Result<LogicalPermission>,
	_guard: GuardKind,
	planner: &Planner<'_>,
	predicate: bool,
) -> Result<PhysicalPermission, Error> {
	// A clause that lowers but holds a write no operator compiles yet is
	// carried as an uncompilable clause, exactly as a clause with no logical
	// form is. Compiling it instead would decline the read that merely touches
	// the table — and a stored clause reaches the planner as an expression, so
	// the statement-level pre-flight never sees it.
	let unplannable = match &logical {
		Ok(LogicalPermission::Specific(guard)) => {
			crate::exec::logical::unplannable_write_in_expr(guard)
		}
		_ => None,
	};
	match logical {
		Ok(LogicalPermission::None) => Ok(PhysicalPermission::Deny),
		Ok(LogicalPermission::Full) => Ok(PhysicalPermission::Allow),
		Ok(LogicalPermission::Specific(guard)) if unplannable.is_none() => {
			let compiled = planner.physical_expr(guard.clone()).await?;
			Ok(PhysicalPermission::Conditional(match predicate {
				true => crate::exec::scalar_unit::UnitClause::predicate(guard, compiled),
				false => crate::exec::scalar_unit::UnitClause::of(guard, compiled),
			}))
		}
		Ok(LogicalPermission::Specific(_)) => Ok(match permission {
			Permission::None => PhysicalPermission::Deny,
			Permission::Full => PhysicalPermission::Allow,
			Permission::Specific(_) => PhysicalPermission::Conditional(UncompilableClause::of(
				unplannable.unwrap_or_default(),
			)),
		}),
		Err(reason) => Ok(match permission {
			Permission::None => PhysicalPermission::Deny,
			Permission::Full => PhysicalPermission::Allow,
			Permission::Specific(_) => {
				PhysicalPermission::Conditional(UncompilableClause::of(reason.to_string()))
			}
		}),
	}
}

/// A stored clause the engine cannot compile.
///
/// The engine compiles a catalog clause — a table or field
/// `PERMISSIONS FOR select` guard, a `COMPUTED` or `DEFAULT` body, an event's
/// `WHEN` or `THEN`, a materialized view's condition, a stored field list —
/// from its stored text into a [`PhysicalExpr`]. A clause whose text has no
/// logical form, and one whose logical form holds a write no operator
/// compiles, cannot be compiled that way. It is carried as this expression
/// instead, which fails with `PlannerUnsupported` naming the reason when it is
/// evaluated. Compiling is the decision to run a clause, so a statement that
/// never evaluates it is not refused; one that does fails closed, and a
/// permission guard that fails denies.
#[derive(Debug)]
pub(crate) struct UncompilableClause {
	reason: String,
}

impl UncompilableClause {
	/// The clause that cannot be compiled, for `reason`.
	pub(crate) fn of(reason: String) -> Arc<dyn PhysicalExpr> {
		Arc::new(Self {
			reason,
		})
	}
}

impl PhysicalExpr for UncompilableClause {
	fn name(&self) -> &'static str {
		"UncompilableClause"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		// A clause belongs to a table, so it is only ever evaluated with a
		// database selected.
		ContextLevel::Database
	}

	fn evaluate<'a>(&'a self, _ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			Err(ExecError::PlannerUnsupported {
				reason: self.reason.clone(),
			}
			.into())
		})
	}

	fn access_mode(&self) -> AccessMode {
		// Evaluating it does nothing but fail.
		AccessMode::ReadOnly
	}
}

impl ToSql for UncompilableClause {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("<uncompilable clause>");
	}
}

// ===========================================================================
// The stored clauses that are not permissions
// ===========================================================================

/// A stored clause as the engine evaluates it: `logical`, the clause
/// compiled from its text by the definition that holds it, or — when that text
/// has no logical form, or holds a write no operator compiles — an
/// [`UncompilableClause`] naming why.
///
/// A clause holding a write the planner declines is carried rather than
/// failed: compiling one would fail the read that merely reached the clause. A
/// write whose own clauses compile is not that case — a clause may write, and
/// the frames that must not are refused where the write runs.
///
/// A clause that has a logical form the planner *declines* is an error rather
/// than a carried clause, as it is for a permission guard. That makes
/// compiling a clause the decision to run it, and puts the matching obligation
/// on the caller: compile a clause only where that clause would be evaluated,
/// so a decline stands exactly where the clause's own failure would have stood
/// and no statement fails over a branch it would not have taken. A caller that
/// resolves its definitions before the statement runs has that for free; one
/// that resolves them per record — a field's `DEFAULT`
/// (`write/stages/schema.rs` and `write/stages/data.rs`) and an event's
/// clauses ([`event_physical`]) — holds each compilation behind the same test
/// that decides whether that clause runs.
async fn clause_to_physical(
	logical: anyhow::Result<&LogicalExpr>,
	planner: &Planner<'_>,
) -> Result<Arc<dyn PhysicalExpr>, Error> {
	match logical {
		Ok(body) => match crate::exec::logical::unplannable_write_in_expr(body) {
			None => {
				let compiled = planner.physical_expr(body.clone()).await?;
				Ok(crate::exec::scalar_unit::UnitClause::of(body.clone(), compiled))
			}
			Some(reason) => Ok(UncompilableClause::of(reason)),
		},
		Err(reason) => Ok(UncompilableClause::of(reason.to_string())),
	}
}

/// A field's `VALUE` or `ASSERT` clause as the engine evaluates it,
/// compiled through `planner`.
///
/// `body` is the logical form the definition carries, lowered once and kept
/// for as long as the catalog holds the definition. Building one here instead
/// would render the stored expression back to text and re-parse it for every
/// record the clause is applied to.
///
/// The clause is compiled, or carried as an [`UncompilableClause`], on the
/// same terms as the `DEFAULT`: a definition the engine cannot
/// compile must still be enforceable, or a table would become unwritable for
/// having declared one. Compiling is the decision to run, so the caller
/// reaches this only where the clause is about to be evaluated.
pub(crate) async fn field_body_physical(
	_expr: &Expr,
	body: &LogicalBody,
	planner: &Planner<'_>,
) -> Result<Arc<dyn PhysicalExpr>, Error> {
	clause_to_physical(body.logical(), planner).await
}

/// A field's `DEFAULT` clause as the engine evaluates it, compiled
/// through `planner`; `None` for a field that declares none.
///
/// The clause is compiled from the definition's logical body — the stored
/// text, lowered once for the life of the definition — or carried as an
/// [`UncompilableClause`]; see [`clause_to_physical`].
///
/// # The frame it is evaluated in
///
/// Record-id generation is the caller, and it evaluates
/// the `id` field's clause in this frame:
///
/// - **Authority**: the caller's, narrowed to the field's `AUTH LIMIT` — ADR 0001's
///   definer's-rights clamp, so a lower-privileged definer's `DEFAULT` cannot run with the caller's
///   broader privileges. [`field_default_frame`] applies it.
/// - **Permissions**: enforced exactly as the statement enforces them. The site overrides nothing,
///   so a checked caller stays checked and every read the clause makes is checked as the caller's
///   own.
/// - **Document**: the record as it stands before the data clause is merged — `self.current` —
///   except under `INSERT`, where the row payload is bound instead, because the id has to be known
///   before the payload is merged. Nothing is bound as `$value`, `$before`, `$after` or `$input`.
/// - **Control flow**: the caller catches a `RETURN`, so a clause written as a block yields the
///   value it returns rather than propagating it out of the statement.
/// - **Bindings**: the session's own values and the database's parameters; none of the caller's
///   `LET`s, `FOR` variables, query parameters or session `LET`s.
/// - **Writes**: not refused.
///
/// Record-id generation reads the `DEFAULT` spelling alone
/// — `DEFAULT ALWAYS` is rejected on the `id` field — and which spelling a
/// field declares is [`FieldDefinition::default`]'s to say; this compiles
/// whichever one it holds.
pub(crate) async fn field_default_physical(
	field: &FieldDefinition,
	planner: &Planner<'_>,
) -> Result<Option<Arc<dyn PhysicalExpr>>, Error> {
	// Both halves name the same clause, so they are `Some` together; a field
	// with no `DEFAULT` has neither.
	let (Some(_), Some(logical)) = (field.default.expression(), field.default_logical()) else {
		return Ok(None);
	};
	clause_to_physical(logical, planner).await.map(Some)
}

/// The frame a field's clauses — `DEFAULT`, `VALUE`, `ASSERT` and
/// `PERMISSIONS` — are evaluated in, derived from the caller's context: the
/// caller's authority, narrowed to the field's `AUTH LIMIT`. See
/// [`field_default_physical`] for the whole frame.
///
/// A clause is definer code, so the frame is privileged
/// ([`ExecutionContext::privileged_frame`]): it holds the session's own values
/// and none of the caller's bindings. A stamp that can narrow no caller — the
/// root-`Owner` stamp every field written before `AUTH LIMIT` existed carries
/// — leaves the authority alone.
pub(crate) fn field_default_frame(
	ctx: &ExecutionContext,
	field: &FieldDefinition,
) -> anyhow::Result<ExecutionContext> {
	let frame = ctx.clone().privileged_frame();
	Ok(match narrowing_auth_limit(&field.auth_limit)? {
		Some(limit) => frame.with_limited_auth(&limit),
		None => frame,
	})
}

/// An event's bodies as the engine evaluates them, compiled through
/// `planner`.
///
/// The `WHEN` is compiled when the event is resolved; the `THEN` clauses are
/// compiled by [`PhysicalEvent::then`], which a caller reaches only once the
/// `WHEN` has fired. Compiling a clause is the decision to run it (see
/// [`clause_to_physical`]) and an event's clauses are compiled per record, so
/// compiling a `THEN` alongside the `WHEN` would fail the statement over a
/// body the `WHEN` never admits — one the authority never looks at.
pub(crate) struct PhysicalEvent<'a, 'p> {
	/// The `WHEN` clause, whose truthiness decides whether the event fires
	///.
	pub(crate) when: Arc<dyn PhysicalExpr>,
	/// The definition the clauses come from, held so the `THEN` clauses can be
	/// compiled from it once the `WHEN` has fired.
	event: &'a EventDefinition,
	/// The planner that compiled the `WHEN`, so every clause of one event is
	/// compiled the same way.
	planner: &'a Planner<'p>,
}

impl PhysicalEvent<'_, '_> {
	/// The event's `THEN` clauses, in the order they run
	///, compiled here.
	///
	/// One call compiles the whole list, so a firing event compiles each of
	/// its bodies once however many it has, and an event that does not fire
	/// compiles none of them. Call it only for an event whose `WHEN` was
	/// truthy.
	///
	/// The gate the compilations are held behind is the event's, not each
	/// body's: every body of a firing event is compiled before the first one
	/// runs, so a decline in a later body is raised ahead of an earlier body's
	/// own failure. Both fail the statement, which is the only outcome either
	/// has.
	pub(crate) async fn then(&self) -> Result<Vec<Arc<dyn PhysicalExpr>>, Error> {
		let mut then = Vec::with_capacity(self.event.then.len());
		for logical in self.event.then_logical() {
			then.push(clause_to_physical(logical, self.planner).await?);
		}
		Ok(then)
	}
}

/// An event's `WHEN` as the engine evaluates it, compiled through
/// `planner`, and the route from it to the event's `THEN` clauses
/// ([`PhysicalEvent::then`]), which compiles those only once the `WHEN` has
/// fired.
///
/// Each clause is compiled from the definition's logical body — the stored
/// text, lowered once for the life of the definition — or carried as a
/// [`UncompilableClause`]; see [`clause_to_physical`].
///
/// # The frame they are evaluated in
///
/// The event stage is the caller, and it evaluates both clauses in this
/// frame:
///
/// - **Authority**: the writer's, narrowed to the event's own `AUTH LIMIT` — ADR 0001's
///   definer's-rights clamp. [`event_frame`] applies it.
/// - **Permissions**: not enforced, for the whole loop. Every read the bodies make is unchecked, so
///   an event body sees the record and everything it reaches whatever the writer may select.
/// - **Parameters**: `$before` the pre-image, `$after` the post-image, `$event` the action as
///   `"CREATE"` / `"UPDATE"` / `"DELETE"`, `$value` the image the event is about — the pre-image on
///   a delete and the post-image otherwise — and `$input` the statement's evaluated data clause,
///   `NONE` where it has none.
/// - **Document**: the image `$value` names, so a bare field path in a body reads the record the
///   event is about.
/// - **Control flow**: the caller catches a `RETURN` in each clause.
/// - **Writes**: allowed. A `THEN` is a write body, and the frame is the one frame here that
///   disables permissions without also refusing writes.
///
/// What the caller decides, not this: that an import and an unmodified record
/// run no event at all, and that an `ASYNC` event is
/// queued rather than run.
pub(crate) async fn event_physical<'a, 'p>(
	event: &'a EventDefinition,
	planner: &'a Planner<'p>,
) -> Result<PhysicalEvent<'a, 'p>, Error> {
	let when = clause_to_physical(event.when_logical(), planner).await?;
	Ok(PhysicalEvent {
		when,
		event,
		planner,
	})
}

/// The record images an event body reads, as the write that fired it holds
/// them.
pub(crate) struct EventImages {
	/// The record before the write.
	pub(crate) before: Value,
	/// The record after it.
	pub(crate) after: Value,
	/// The statement's evaluated data clause, or `NONE` where it has none.
	pub(crate) input: Value,
}

/// The frame an event's bodies are evaluated in, and the record they read as
/// their document.
///
/// Derived from the caller's context: permission enforcement off, the event's
/// `AUTH LIMIT` applied, and the five parameters bound. See
/// [`event_physical`] for the whole frame.
///
/// The parameters are bound on the context rather than on the evaluation
/// ([`EvalContext`]'s write bindings), so a body reached through a nested
/// evaluation (a closure, a nested `eval`) reads them as well. Parameter
/// resolution consults the write bindings first and the context chain after,
/// and both name the same images.
///
/// The document is returned rather than bound, because binding a document is
/// the evaluation's to do ([`EvalContext::with_value_and_doc`]) rather than the
/// context's; it is the same image `$value` names, which is the pre-image on a
/// delete and the post-image otherwise.
///
/// "Not enforced" is expressed as `skip_fetch_perms` rather than as the
/// suppression [`view_frame`] uses. The signal is also what several readers
/// take as "a guard is running" — the cyclic-link short circuit in
/// [`check_permission_for_value`] among them — so moving an event to
/// suppression is a change to those readers and not to this line.
pub(crate) fn event_frame(
	ctx: &ExecutionContext,
	event: &EventDefinition,
	action: DocAction,
	images: EventImages,
) -> anyhow::Result<(ExecutionContext, Value)> {
	let subject = match action {
		DocAction::Delete => images.before.clone(),
		DocAction::Create | DocAction::Update => images.after.clone(),
	};
	let name = match action {
		DocAction::Create => "CREATE",
		DocAction::Update => "UPDATE",
		DocAction::Delete => "DELETE",
	};
	let frame = ctx.clone().privileged_frame().with_skip_fetch_perms(true).with_params([
		("before", images.before),
		("after", images.after),
		("event", Value::from(name)),
		("value", subject.clone()),
		("input", images.input),
	]);
	let frame = match narrowing_auth_limit(&event.auth_limit)? {
		Some(limit) => frame.with_limited_auth(&limit),
		None => frame,
	};
	Ok((frame, subject))
}

/// A materialized view's clauses as the engine evaluates them,
/// compiled through `planner`.
#[derive(Debug)]
pub(crate) struct PhysicalView {
	/// The view's condition, which decides whether the written record belongs
	/// in the view. `None` for a view with no condition,
	/// where a record belongs in the view unless the write deleted it.
	pub(crate) condition: Option<Arc<dyn PhysicalExpr>>,
	/// The view's field list, which projects the row stored on the view table
	///.
	pub(crate) projection: Arc<dyn PhysicalExpr>,
}

/// A stored `SELECT` field list, compiled: the object it projects from the
/// record the caller binds as the document.
///
/// It is compiled as a write's `RETURN <fields>` is ([`Planner::write_output`])
/// and applied as that clause and a `SELECT`'s projection apply a field — `*`
/// seeding the object with the record, each field set in the order written
/// ([`evaluate_and_set_field`](crate::exec::operators::evaluate_and_set_field)).
/// It binds nothing: the frame, the authority and whether reads are checked
/// are the caller's, which evaluates it in the frame it evaluates the clause
/// beside it in.
#[derive(Debug)]
pub(crate) struct CompiledProjection {
	projection: crate::exec::operators::write::clauses::WriteProjection,
}

impl PhysicalExpr for CompiledProjection {
	fn name(&self) -> &'static str {
		"CompiledProjection"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		// A field list belongs to a view of a table, so it is only ever
		// evaluated with a database selected.
		ContextLevel::Database
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		use crate::exec::operators::write::clauses::WriteProjection;
		Box::pin(async move {
			let row = ctx.document_root.or(ctx.current_value).cloned().unwrap_or(Value::None);
			match &self.projection {
				WriteProjection::Value(expr) => expr.evaluate(ctx).await,
				// `*` and nothing else is the record itself.
				WriteProjection::Fields {
					fields,
					include_all: true,
				} if fields.is_empty() => Ok(row),
				WriteProjection::Fields {
					fields,
					include_all,
				} => {
					let mut object = crate::val::Object::default();
					if *include_all && let Value::Object(record) = &row {
						object = record.clone();
					}
					for field in fields {
						crate::exec::operators::evaluate_and_set_field(
							&mut object,
							field,
							ctx.clone(),
						)
						.await?;
					}
					Ok(Value::Object(object))
				}
			}
		})
	}

	fn access_mode(&self) -> AccessMode {
		use crate::exec::operators::write::clauses::WriteProjection;
		match &self.projection {
			WriteProjection::Value(expr) => expr.access_mode(),
			WriteProjection::Fields {
				fields,
				..
			} => crate::exec::CombineAccessModes::combine_all(
				fields.iter().map(|field| field.expr.access_mode()),
			),
		}
	}
}

impl ToSql for CompiledProjection {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		use crate::exec::operators::write::clauses::WriteProjection;
		match &self.projection {
			WriteProjection::Value(expr) => {
				f.push_str("VALUE ");
				expr.fmt_sql(f, fmt);
			}
			WriteProjection::Fields {
				fields,
				include_all,
			} => {
				let mut first = true;
				if *include_all {
					f.push('*');
					first = false;
				}
				for field in fields {
					if !first {
						f.push_str(", ");
					}
					first = false;
					field.expr.fmt_sql(f, fmt);
				}
			}
		}
	}
}

/// A stored `SELECT` field list as the engine evaluates it,
/// compiled through `planner`: a [`CompiledProjection`], or an
/// [`UncompilableClause`] naming why where the list does not compile.
async fn projection_physical(fields: &Fields, planner: &Planner<'_>) -> Arc<dyn PhysicalExpr> {
	let output = crate::expr::output::Output::Fields(fields.clone());
	match planner.write_output(Some(&output)).await {
		Ok(Some(projection)) => Arc::new(CompiledProjection {
			projection,
		}),
		Ok(None) => UncompilableClause::of("the field list has no projection".to_owned()),
		Err(e) => UncompilableClause::of(e.to_string()),
	}
}

/// A materialized view's condition and field list as the engine
/// evaluates them, compiled through `planner`; `None` for a view that is
/// maintained some other way.
///
/// The condition is compiled from the definition's logical body — the stored
/// text, lowered once for the life of the definition — or carried as a
/// [`UncompilableClause`]; see [`clause_to_physical`]. The field list is
/// compiled as a [`CompiledProjection`]; see [`projection_physical`].
///
/// Only [`ViewDefinition::Materialized`] has a route. An aggregated view's
/// maintenance runs a statement over the group rather than evaluating the
/// clauses against the written record, and a `Select` view is not maintained
/// at all.
///
/// # The frame they are evaluated in
///
/// The view stage is the caller, and it evaluates both clauses in this
/// frame:
///
/// - **Authority**: the *writer's*, unnarrowed. A view definition records no `AUTH LIMIT`, and the
///   caller applies none, so ADR 0001's definer's rights do not reach a view body: it runs as
///   whoever wrote the record. This is the one clause here for which that is true.
/// - **Permissions**: not enforced, for every view of the table.
/// - **Document**: the post-image, `self.current`, for both clauses — including on a delete, where
///   the caller has already cleared it, so a condition judges the cleared image and the view row is
///   removed. With no condition the caller decides by the action alone. No parameters are bound.
/// - **Control flow**: the caller catches a `RETURN` in the condition.
/// - **Writes**: not refused.
///
/// What the caller decides, not this: that an import and an unmodified record
/// maintain no view, and what is done with the projected
/// row — stored on the view table under the record's own key, or deleted from
/// it.
pub(crate) async fn materialized_view_physical(
	view: &ViewDefinition,
	planner: &Planner<'_>,
) -> Result<Option<PhysicalView>, Error> {
	let ViewDefinition::Materialized {
		fields,
		condition,
		..
	} = view
	else {
		return Ok(None);
	};
	// Both halves name the same clause, so they are `Some` together.
	let condition = match (condition, view.materialized_condition_logical()) {
		(Some(_), Some(logical)) => Some(clause_to_physical(logical, planner).await?),
		_ => None,
	};
	Ok(Some(PhysicalView {
		condition,
		projection: projection_physical(fields, planner).await,
	}))
}

/// A view's clauses as the engine evaluates them, whichever kind of
/// view it is.
///
/// The one route from a stored [`ViewDefinition`] to the clauses that maintain
/// it, so the write path asks what a view *is* in one place rather than
/// matching the definition itself at each use.
#[derive(Debug)]
pub(crate) enum PhysicalViewKind {
	/// A view nothing maintains: it was classified as unmaintained, and a
	/// write to its source table does nothing for it.
	Unmaintained,
	/// A view holding one row per source record.
	Materialized(PhysicalView),
	/// A view holding one row per group.
	Aggregated(PhysicalAggregatedView),
}

/// Compile the clauses that maintain `view`, whichever kind it is.
pub(crate) async fn view_physical(
	view: &ViewDefinition,
	planner: &Planner<'_>,
) -> Result<PhysicalViewKind, Error> {
	if let Some(materialized) = materialized_view_physical(view, planner).await? {
		return Ok(PhysicalViewKind::Materialized(materialized));
	}
	if let Some(aggregated) = aggregated_view_physical(view, planner).await? {
		return Ok(PhysicalViewKind::Aggregated(aggregated));
	}
	Ok(PhysicalViewKind::Unmaintained)
}

/// An aggregated materialized view's clauses as the engine evaluates
/// them, compiled through `planner`; `None` for a view of another kind.
///
/// Where [`materialized_view_physical`] projects a row per record, this one
/// maintains a row per *group*, and its clauses are the four the maintenance
/// reads: the view's condition, the group key, the arguments the stored
/// aggregates accumulate, and the field list that renders a group's stats into
/// the row.
///
/// The definition stores no logical form of any of them — an
/// [`AggregationAnalysis`] holds bare expressions — so each is lowered on
/// demand and carried as an [`UncompilableClause`] when it has none, exactly as
/// the materialized condition is.
///
/// The frame is the same for all four, and is [`view_frame`]'s.
pub(crate) async fn aggregated_view_physical(
	view: &ViewDefinition,
	planner: &Planner<'_>,
) -> Result<Option<PhysicalAggregatedView>, Error> {
	let ViewDefinition::Aggregated {
		analysis,
		condition,
		..
	} = view
	else {
		return Ok(None);
	};
	let condition = match condition {
		Some(expr) => Some(view_expr_physical(expr, planner).await?),
		None => None,
	};
	let mut group = Vec::with_capacity(analysis.group_expressions.len());
	for expr in &analysis.group_expressions {
		group.push(view_expr_physical(expr, planner).await?);
	}
	let mut arguments = Vec::with_capacity(analysis.aggregate_arguments.len());
	for expr in &analysis.aggregate_arguments {
		arguments.push(view_expr_physical(expr, planner).await?);
	}
	let AggregateFields::Fields(items) = &analysis.fields else {
		return Err(Error::Exec(ExecError::InvalidStatement(
			"a VALUE selector is not supported on a view".to_string(),
		)));
	};
	let mut fields = Vec::with_capacity(items.len());
	for (name, expr) in items {
		fields.push((
			WritePlace::compile(planner, name.clone()).await?,
			view_expr_physical(expr, planner).await?,
		));
	}
	Ok(Some(PhysicalAggregatedView {
		condition,
		group,
		arguments,
		fields,
	}))
}

/// One clause of a view, compiled or carried; see [`clause_to_physical`].
async fn view_expr_physical(
	expr: &Expr,
	planner: &Planner<'_>,
) -> Result<Arc<dyn PhysicalExpr>, Error> {
	let logical = crate::expr::logical::lower_expr(expr.clone().into());
	let logical = logical.as_ref().map_err(|err| anyhow::Error::new(err.clone()));
	clause_to_physical(logical, planner).await
}

/// An aggregated materialized view's clauses as the engine evaluates
/// them. See [`aggregated_view_physical`].
pub(crate) struct PhysicalAggregatedView {
	/// The view's condition, which decides whether a record belongs to the
	/// view at all — evaluated against the before-image and the post-image
	/// separately, because a record can enter or leave the view
	///. `None` for a view with no condition, where every
	/// record belongs.
	pub(crate) condition: Option<Arc<dyn PhysicalExpr>>,
	/// The `GROUP BY` targets, whose values are the view row's record id
	///.
	pub(crate) group: Vec<Arc<dyn PhysicalExpr>>,
	/// The arguments the stored aggregates accumulate, one per entry of the
	/// analysis.
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
	/// The field list, evaluated against the document a group's stats make
	/// rather than against the record, and written to the places it names
	///.
	pub(crate) fields: Vec<(WritePlace, Arc<dyn PhysicalExpr>)>,
}

impl std::fmt::Debug for PhysicalAggregatedView {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("PhysicalAggregatedView")
			.field("condition", &self.condition.is_some())
			.field("group", &self.group.len())
			.field("arguments", &self.arguments.len())
			.field("fields", &self.fields.len())
			.finish()
	}
}

/// A `REFERENCE ON DELETE THEN` expression as the engine evaluates
/// it, compiled through `planner`.
///
/// Unlike every sibling here, the definition stores no logical form of this
/// clause: a `Reference` holds the expression alone. It is lowered on demand,
/// and a clause that has no logical form — or holds a write no operator
/// compiles — is carried as an [`UncompilableClause`]; see
/// [`clause_to_physical`].
///
/// # The frame it is evaluated in
///
/// - **Authority**: the writer's. A reference definition records no `AUTH LIMIT`, so there is
///   nothing to narrow to, exactly as for a view body.
/// - **Permissions**: enforcement off and writes allowed — the strategy repairs the referencing
///   record whatever the actor may do to it. That is `with_perms(false)`, not the permission frame,
///   which would refuse the write the strategy exists to make.
/// - **Parameters**: `$reference` is the record being deleted, bound by the caller.
/// - **Document**: the referencing record's *id*, not its content. The strategy is told which
///   record to repair and reads it itself.
/// - **Control flow**: the caller catches a `RETURN`.
pub(crate) async fn reference_custom_physical(
	expr: &Expr,
	planner: &Planner<'_>,
) -> Result<Arc<dyn PhysicalExpr>, Error> {
	let logical = crate::expr::logical::lower_expr(expr.clone().into());
	let logical = logical.as_ref().map_err(|err| anyhow::Error::new(err.clone()));
	clause_to_physical(logical, planner).await
}

/// An index's stored expressions as the engine evaluates them: the
/// columns whose values the index holds, and a `COUNT` index's `WHERE` guard.
///
/// Compiled once per index definition and evaluated per record image, by both
/// the write path's index stage and the index build's replay, so a record
/// indexed by a write and a record indexed by a build are judged by the same
/// expressions.
///
/// # The frame they are evaluated in
///
/// - **Authority**: the caller's. A write evaluates them as the writer, a build as whoever started
///   it — the definer for a `DEFINE`/`REBUILD INDEX`, and root Owner for a build a maintenance scan
///   resumed. An index definition records no `AUTH LIMIT`, so there is nothing to narrow to.
/// - **Permissions**: enforced exactly as the caller enforces them. Neither site overrides
///   anything, so a record dereference inside a column is checked as the caller's own read.
/// - **Document**: the record image, bound as both the evaluated value and the document root, which
///   is what lets `$parent` inside an idiom-level filter resolve to the record under test. `@` is
///   the identity the image itself carries, so an image that does not exist — a creating order's
///   before-image, a delete's emptied after-image — has none.
/// - **Self-reference**: a column or guard naming the record's own id reads the image under test
///   rather than storage. The two images are evaluated after the record is stored, so answering
///   from storage would make them equal and the entry would never move.
/// - **Parameters**: the caller's. A write sees the statement's bindings; a build's context carries
///   none, so a `$param` inside a column reads `NONE` there.
/// - **Control flow**: a `RETURN` out of a column or a guard is its value, caught by
///   [`PhysicalIndexEntry::values`] and [`PhysicalIndexEntry::guard_matches`]. A `BREAK` or
///   `CONTINUE` has no loop to leave and is an error.
/// - **Writes**: not refused. The frame is the caller's own.
pub(crate) struct PhysicalIndexEntry {
	/// One expression per column of `IndexDefinition::cols`, in the order the
	/// definition holds them, because the values they produce are the index
	/// key and the key's order is the definition's.
	cols: Vec<Arc<dyn PhysicalExpr>>,
	/// A `COUNT` index's `WHERE` guard; `None` for every other index kind and
	/// for an unguarded count index.
	guard: Option<Arc<dyn PhysicalExpr>>,
}

impl std::fmt::Debug for PhysicalIndexEntry {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("PhysicalIndexEntry")
			.field("cols", &self.cols.len())
			.field("guard", &self.guard.is_some())
			.finish()
	}
}

/// Compile one index definition's stored expressions; see
/// [`PhysicalIndexEntry`].
///
/// An expression the lowering has no form for — or one holding a write no
/// operator compiles — is carried as an [`UncompilableClause`], as every other
/// stored clause is; see [`clause_to_physical`].
///
/// Compile only a definition whose entries this caller would evaluate: an
/// index the catalog has marked for removal is never maintained, and
/// compiling its clauses would fail a write over an expression it would not
/// have run.
pub(crate) async fn index_entry_physical(
	ix: &IndexDefinition,
	planner: &Planner<'_>,
) -> Result<PhysicalIndexEntry, Error> {
	let mut cols = Vec::with_capacity(ix.cols.len());
	for col in &ix.cols {
		cols.push(index_column_physical(col, planner).await?);
	}
	let guard = match &ix.count_cond {
		Some(cond) => Some(index_expr_physical(&cond.0, planner).await?),
		None => None,
	};
	Ok(PhysicalIndexEntry {
		cols,
		guard,
	})
}

/// One column of an index, compiled or carried.
///
/// A column is stored as an [`Idiom`](crate::expr::Idiom) — a path applied to
/// the record whole — so it is lowered as the expression that path is, which
/// is what makes a leading field read the record's field and an empty path
/// yield the record itself.
async fn index_column_physical(
	col: &crate::expr::Idiom,
	planner: &Planner<'_>,
) -> Result<Arc<dyn PhysicalExpr>, Error> {
	index_expr_physical(&Expr::Idiom(col.clone()), planner).await
}

/// One stored expression of an index, compiled or carried; see
/// [`clause_to_physical`].
async fn index_expr_physical(
	expr: &Expr,
	planner: &Planner<'_>,
) -> Result<Arc<dyn PhysicalExpr>, Error> {
	let logical = crate::expr::logical::lower_expr(expr.clone().into());
	let logical = logical.as_ref().map_err(|err| anyhow::Error::new(err.clone()));
	clause_to_physical(logical, planner).await
}

impl PhysicalIndexEntry {
	/// The values this index holds for one image of the record, or `None` when
	/// the image is nullish and the index holds nothing for it.
	///
	/// The nullish answer is the record's absence rather than a vector of
	/// `NONE`s: an image that does not exist has no entry, which is how a
	/// `DELETE`'s emptied post-image removes every entry the record held.
	pub(crate) async fn values(
		&self,
		image: &Value,
		exec: &ExecutionContext,
	) -> anyhow::Result<Option<Vec<Value>>> {
		if image.is_nullish() {
			return Ok(None);
		}
		let base = index_entry_eval(exec, image);
		let mut out = Vec::with_capacity(self.cols.len());
		for col in &self.cols {
			out.push(col.evaluate(base.with_value_and_doc(image)).await.catch_return()?);
		}
		Ok(Some(out))
	}

	/// Whether one image satisfies this index's `COUNT` guard, and `None` for
	/// an index that has none.
	///
	/// A `COUNT WHERE` index stores no column values, so the guard is the whole
	/// of what decides whether the record is in the count: the count moves when
	/// the answer differs between the two images, not when the columns do.
	pub(crate) async fn guard_matches(
		&self,
		image: &Value,
		exec: &ExecutionContext,
	) -> anyhow::Result<Option<bool>> {
		let Some(guard) = &self.guard else {
			return Ok(None);
		};
		let base = index_entry_eval(exec, image);
		let v = guard.evaluate(base.with_value_and_doc(image)).await.catch_return()?;
		Ok(Some(v.is_truthy()))
	}
}

/// The evaluation context an index's columns and `COUNT` guard read, for one
/// image of one record.
///
/// The image names itself: an entry is derived for a record, and a column or a
/// guard may reach that record's own id — `FIELDS id.v`, `COUNT WHERE id.v =
/// 7`. Such a dereference must answer from the image under test rather than
/// from storage, or the before-image evaluation would read the record as the
/// write has already stored it and the entry would never move. Binding the
/// image as [`ComputingRecord::stored`] is what makes the two images differ,
/// and it also spares the per-record read a fetch would cost.
///
/// The identity is read off the image's `id` field, so an image carrying none
/// binds nothing and a dereference falls back to storage, which is what a
/// record with no identity can be answered from.
fn index_entry_eval<'a>(exec: &'a ExecutionContext, image: &Value) -> EvalContext<'a> {
	let mut base = EvalContext::from_exec_ctx(exec);
	if let Value::Object(obj) = image
		&& let Some(Value::RecordId(rid)) = obj.get("id")
	{
		base.computing_record = Some(Arc::new(crate::exec::physical_expr::ComputingRecord {
			rid: rid.clone(),
			stored: Some(image.clone()),
		}));
	}
	base
}

/// The frame a materialized view's clauses are evaluated in, and the frame the
/// rows it writes are maintained in: permission enforcement off, writes
/// allowed, and the writer's own authority — a view narrows it by nothing.
/// See [`materialized_view_physical`] for the whole frame.
///
/// Enforcement off is expressed as suppression rather than as
/// `skip_fetch_perms`, which also refuses writes: the view's own clauses may
/// write, and so may an event on the view table that the row this frame stores
/// goes on to fire. The predicate frame would refuse both at the bridge. Contrast [`event_frame`],
/// which keeps the stricter signal.
pub(crate) fn view_frame(ctx: &ExecutionContext) -> ExecutionContext {
	ctx.with_perms_suppressed(true)
}

/// [`table_select_permission_physical`] for a scan that resolves its table
/// when it runs: a txn-less planner is built from `ctx` — no plan-time index
/// resolution, no cycle guard interaction.
///
/// Cycle safety note: this path is intentionally *txn-less*. A planner
/// without a transaction resolves no table context, so a self-referential
/// permission compiled here (e.g. a cache-miss runtime build for a permission
/// that contains `SELECT FROM same_table`) cannot recurse into table-context
/// resolution. Do **not** switch this helper to [`Planner::with_txn`] without
/// re-deriving cycle safety — in particular, runtime callers don't share a
/// parent [`CycleGuard`] the way plan-time nested planners do, so the inner
/// subtree would either need its own guard or a different cycle-break
/// mechanism.
#[cfg(test)]
pub(crate) async fn table_select_permission_physical_runtime(
	table_def: Option<&TableDefinition>,
	ctx: &ExecutionContext,
) -> Result<PhysicalPermission, Error> {
	table_select_permission_physical(table_def, &Planner::new(ctx.ctx(), ctx.function_registry()))
		.await
}

/// Whether `action` by `auth` is permission-checked, by role and actor level
/// alone.
///
/// Never, while server auth is disabled and the session is anonymous.
/// Otherwise a principal is exempt only when it holds the role the action
/// needs — Viewer for `View`, Editor for `Edit` — and its actor level covers
/// the target: a root user covers everything, a namespace user the namespace
/// named in `ns`, a database user the database named in `ns`/`db`. With no
/// namespace named only a root user can be exempt.
///
/// This is the one rule behind every gate, at run time and at plan time:
/// [`ExecutionContext::should_check_perms`], which adds the permission-frame
/// bypass in front of it, `Planner::should_check_perms_for_view` and the
/// analyzer's `Caller`. `Context::check_perms` applies the same rule to
/// `Options`.
pub(crate) fn action_is_checked(
	auth_enabled: bool,
	auth: &Auth,
	action: Action,
	ns: Option<&str>,
	db: Option<&str>,
) -> bool {
	if !auth_enabled && auth.is_anon() {
		return false;
	}
	let allowed = match action {
		Action::Edit => auth.has_editor_role(),
		Action::View => auth.has_viewer_role(),
	};
	let in_level = auth.is_root()
		|| match (ns, db) {
			(Some(ns), Some(db)) => auth.is_ns_check(ns) || auth.is_db_check(ns, db),
			(Some(ns), None) => auth.is_ns_check(ns),
			(None, _) => false,
		};
	!allowed || !in_level
}

/// [`ExecutionContext::should_check_perms`] for a database-level context: the
/// gate every scan, graph, reference and join operator consults before it
/// enforces anything. `true` when permission checks apply, `false` when they
/// are bypassed — inside a permission predicate, for an exempt principal, or
/// for an anonymous session on a datastore with auth disabled.
pub(crate) fn should_check_perms(db_ctx: &DatabaseContext, action: Action) -> Result<bool, Error> {
	db_ctx.should_check_perms(action)
}

/// Whether every table in `tables` carries `Permission::Full` for `SELECT`.
///
/// `Permission::Full` is the only declaration under which a fast path may
/// resolve a value embedded in an index or adjacency key — a doc-ID, a
/// target vertex — without fetching and permission-checking the record
/// itself: with no per-row predicate to evaluate, skipping the fetch
/// changes nothing observable. A missing table or catalog error is
/// conservatively `false`.
///
/// This only ever gates an optimization, never a correctness requirement,
/// so callers always have a slower, per-record-checked path to fall back
/// to. Callers are responsible for their own `should_check_perms` /
/// `should_check_perms_for_view` gate: when permission evaluation would be
/// skipped for the session regardless, the fast path is safe independent
/// of the declared permission, and this function should not be reached.
pub(crate) async fn tables_have_full_select_permission(
	txn: &crate::kvs::Transaction,
	ns: &str,
	db: &str,
	tables: &[crate::val::TableName],
) -> bool {
	use crate::catalog::providers::TableProvider;
	for table in tables {
		let Ok(Some(td)) =
			crate::exec::vm::descriptor::object(txn.get_tb_by_name(ns, db, table, None)).await
		else {
			return false;
		};
		if !matches!(td.permissions.select, Permission::Full) {
			return false;
		}
	}
	true
}

/// Validate that the authenticated principal may read in the current namespace
/// and database (GHSA-2v9j tenant-boundary gate for the read path).
///
/// The session's *selected* ns/db can be steered with `USE` - including the RPC
/// `use` method, which sets the context without rejecting a cross-tenant switch -
/// so it must not be trusted on its own. The authenticated auth level is the
/// source of truth: root and anonymous principals are unconfined, a namespace
/// principal may read any database within its namespace, and database and record
/// principals are pinned to their exact namespace/database. Enforcing this here,
/// before table/field `PERMISSIONS` are consulted, stops a principal that has
/// pivoted into a foreign tenant from reading a table there whose `PERMISSIONS`
/// happen to be satisfiable.
///
/// Returns `Ok(())` if access is allowed, `Err` with an appropriate error if denied.
pub(crate) fn validate_record_user_access(db_ctx: &DatabaseContext) -> Result<(), Error> {
	let root = &db_ctx.ns_ctx.root;

	let ns = db_ctx.ns_name();
	let db = db_ctx.db_name();

	if root.auth.can_access_ns_db(ns, db) {
		return Ok(());
	}

	// A namespace match with a database mismatch is a database-scope violation;
	// otherwise the namespace itself is out of the principal's reach.
	if root.auth.level().ns() == Some(ns) {
		return Err(ExecError::DbNotAllowed {
			db: db.into(),
		}
		.into());
	}
	Err(ExecError::NsNotAllowed {
		ns: ns.into(),
	}
	.into())
}

/// Check a `PERMISSIONS FOR select` guard against a specific record value.
///
/// Returns `true` if access is allowed, `false` if denied.
///
/// `value_param` lets field-level callers bind the `$value` parameter to
/// the field's picked value (the value the field's own clause reads). Pass
/// `None` for table-level checks, where `$value` has no meaning.
///
/// The guard is judged in a frame that refuses a write
/// ([`crate::dbs::NoWriteFrame::PermissionPredicate`]): it is evaluated with
/// enforcement disabled, so a write reached from it is a side effect the
/// reading statement never asked for (GHSA-66r2-5gwj-gxm2).
/// [`check_write_permission_for_value`] is the create/update/delete
/// counterpart, which is reached from a statement that is already writing.
pub(crate) async fn check_permission_for_value(
	permission: &PhysicalPermission,
	value: &Value,
	value_param: Option<&Value>,
	ctx: &ExecutionContext,
) -> anyhow::Result<bool> {
	check_guard_for_value(permission, value, value_param, ctx, GuardKind::Read).await
}

/// Check a `PERMISSIONS FOR create`, `update` or `delete` guard against a
/// specific record value.
///
/// As [`check_permission_for_value`], but the guard may write: it is reached
/// from a statement that is already writing, so a guard that records the
/// attempt performs its write rather than being refused. That is the line
/// between [`crate::dbs::Options::new_for_permission_predicate`] and
/// `Options::new_for_mutable_permission_predicate`.
pub(crate) async fn check_write_permission_for_value(
	permission: &PhysicalPermission,
	value: &Value,
	value_param: Option<&Value>,
	ctx: &ExecutionContext,
) -> anyhow::Result<bool> {
	check_guard_for_value(permission, value, value_param, ctx, GuardKind::Write).await
}

/// The body of both, with the frame the guard is judged in named by `guard`.
async fn check_guard_for_value(
	permission: &PhysicalPermission,
	value: &Value,
	value_param: Option<&Value>,
	ctx: &ExecutionContext,
	guard: GuardKind,
) -> anyhow::Result<bool> {
	match permission {
		PhysicalPermission::Deny => Ok(false),
		PhysicalPermission::Allow => Ok(true),
		PhysicalPermission::Conditional(physical_expr) => {
			// Inside a permission predicate evaluation (propagated via
			// skip_fetch_perms), allow unconditionally so cyclic links
			// don't recurse forever.
			if ctx.root().skip_fetch_perms {
				return Ok(true);
			}

			// The guard is definer code, so it runs in a privileged frame: it
			// holds none of the caller's own bindings.
			let isolated = ctx.clone().privileged_frame();
			let ctx = &isolated;
			// A read guard refuses a write for the whole of its evaluation,
			// however deep a call takes it. The signal is on the context
			// rather than on the evaluation so that a nested evaluation reads
			// the frame off the context alone.
			let guarded_ctx;
			let ctx = match guard {
				GuardKind::Read => {
					guarded_ctx =
						ctx.clone().refusing_writes(crate::dbs::NoWriteFrame::PermissionPredicate);
					&guarded_ctx
				}
				GuardKind::Write => ctx,
			};

			let bound_ctx;
			let exec_ctx = match value_param {
				Some(v) => {
					bound_ctx = ctx.with_param("value", v.clone());
					&bound_ctx
				}
				None => ctx,
			};
			// Bind the record as both the current value and the document root.
			// The document root is what lets `$parent` inside an idiom-level filter — e.g.
			// `PERMISSIONS FOR select WHERE acl[WHERE $parent.owner = $auth.id]`
			// — resolve to the row under test. `$parent` is the one reader that
			// does not fall back to `current_value`, so binding only the value
			// left it unresolved and the whole predicate falsy, denying every row.
			let mut eval_ctx = EvalContext::from_exec_ctx(exec_ctx).with_value_and_doc(value);
			eval_ctx.skip_fetch_perms = true;

			// The concrete error is carried through rather than collapsed to
			// `Internal`: this runs per permission-checked row, and a write
			// conflict raised inside a predicate has to stay downcastable for
			// the transactor to retry it, while a cancelled query has to keep
			// reporting as cancelled rather than as an internal failure.
			//
			// A predicate that signals control flow has nowhere to send it —
			// there is no surrounding block — so those stay an error, as
			// before.
			let result = physical_expr.evaluate(eval_ctx).await.map_err(|ctrl| match ctrl {
				ControlFlow::Err(e) => e,
				other => anyhow::Error::new(EngineError::Internal(format!(
					"unexpected control flow in a permission predicate: {other}"
				))),
			})?;
			Ok(result.is_truthy())
		}
	}
}

#[cfg(test)]
/// The engine's SELECT-permission gate for ANN truthy-document
/// filters.
///
/// Pairs a permission already resolved for the surrounding scan with the
/// context it is evaluated against, so the ANN search gates each candidate on
/// exactly the permission that filters the fetched batch after the search and
/// the two checks cannot disagree.
pub(crate) struct PhysicalTableSelect {
	permission: PhysicalPermission,
	ctx: ExecutionContext,
}

#[cfg(test)]
impl PhysicalTableSelect {
	#[cfg(test)]
	pub(crate) fn new(permission: PhysicalPermission, ctx: ExecutionContext) -> Self {
		Self {
			permission,
			ctx,
		}
	}
}

#[cfg(test)]
impl TableSelectGate for PhysicalTableSelect {
	fn allows_every_doc(&self) -> Option<bool> {
		match self.permission {
			PhysicalPermission::Allow => Some(true),
			PhysicalPermission::Deny => Some(false),
			PhysicalPermission::Conditional(_) => None,
		}
	}

	fn allows_doc<'a>(
		&'a self,
		_rid: &'a Arc<RecordId>,
		record: &'a Arc<Record>,
	) -> BoxGateFut<'a> {
		Box::pin(async move {
			check_permission_for_value(&self.permission, &record.data, None, &self.ctx).await
		})
	}
}

/// A stored `PERMISSIONS` clause, compiled once for the definition it was read
/// from rather than once per evaluation.
///
/// A clause of this kind is judged per *evaluation*, not per statement: a
/// `DEFINE PARAM` guard against the row the parameter is read on, a function's
/// against the frame of the call. Compiling it there puts plan-time work
/// inside a row loop, and a parameter or a call in a projection is evaluated
/// once per row.
///
/// Only the compilation is kept. The definition is still read afresh, and the
/// compiled predicate is still evaluated, on every evaluation — what the
/// predicate answers belongs to that evaluation, so a guard that admits one
/// row still refuses the next.
///
/// Reuse is keyed on the definition and the depth. The transaction cache hands
/// out one `Arc` per definition, so an evaluation that gets this very
/// allocation back has the clause the compilation came from without comparing
/// it, and a `DEFINE` that replaces the definition matches nothing. The depth
/// is part of the key because a guard is compiled one re-entry deeper than the
/// expression reading it, and compilation is bounded by that depth.
///
/// What the key pins is the clause, not everything compiling it resolved: a
/// subquery inside a predicate is bound against the catalog the first
/// evaluation saw, and a later evaluation of the same node reads that binding.
/// It is the rule the rest of the plan already follows — a statement binds
/// once, before anything it goes on to define — and it is why the guard is
/// compiled for the definition rather than for the statement.
///
/// The caller's authority is *not* in the key, and the reuse rests on it being
/// fixed: compiling a scalar consults the principal — a read the caller
/// bypasses is planned without the permission the check would apply
/// ([`Planner::should_check_perms_for_view`]) — so a guard compiled for one
/// principal states another's plan wrongly. What holds it is that a physical
/// node belongs to one plan, one statement and one principal, and the authority
/// at the node is a function of that principal and the definition, both of which
/// the key already pins: an `AUTH LIMIT` derives a fresh `Arc` per call, but
/// from the same caller and the same definition, so its value does not move.
/// A cache that let two principals reach one compiled node would break that,
/// and would have to key this memo on the principal as well.
///
/// The swap cell is made when the first guard is compiled. Most nodes that
/// carry one never compile a guard, and a node is dropped with every plan
/// holding it, so a cell made up front would be paid for — its drop waits out
/// every thread's readers — by plans that never use it.
pub(crate) struct CompiledGuard<D> {
	memo: std::sync::OnceLock<ArcSwapOption<Memo<D>>>,
}

/// Judge `compiled`, a guard compiled from a permission clause, for the
/// evaluation `ctx`: in the permission frame, one re-entry deeper than the
/// expression reading it, with enforcement off so the guard does not recurse
/// into its own subject, and writes refused, since the guard is evaluated on a
/// read of the definition it protects. The guard is definer code, so the frame
/// is privileged: it holds none of the caller's bindings, neither its
/// context's nor its block's. A guard that raises anything but an error
/// denies; an error keeps its type.
pub(crate) async fn judge_compiled_guard(
	compiled: &Arc<dyn PhysicalExpr>,
	ctx: &EvalContext<'_>,
	denied: impl Fn() -> ExecError,
) -> FlowResult<()> {
	let guarded_ctx = ctx
		.exec_ctx
		.clone()
		.privileged_frame()
		.refusing_writes(crate::dbs::NoWriteFrame::PermissionPredicate);
	let mut frame = ctx.clone();
	frame.exec_ctx = &guarded_ctx;
	frame.local_params = None;
	frame.skip_fetch_perms = true;
	match compiled.evaluate(frame).await {
		Ok(result) if result.is_truthy() => Ok(()),
		Ok(_) => Err(denied().into()),
		Err(ControlFlow::Err(e)) => Err(ControlFlow::Err(e)),
		Err(_) => Err(denied().into()),
	}
}

/// A compiled guard and the definition and depth it was compiled for.
struct Memo<D> {
	definition: Arc<D>,
	plan_depth: u32,
	compiled: Arc<dyn PhysicalExpr>,
}

/// A cloned node carries the compilation the original made. Reuse is keyed on
/// the definition, which every evaluation checks, so the copy is used only
/// where the original's would have been.
impl<D> Clone for CompiledGuard<D> {
	fn clone(&self) -> Self {
		let memo = std::sync::OnceLock::new();
		if let Some(kept) = self.memo.get() {
			let _ = memo.set(ArcSwapOption::new(kept.load_full()));
		}
		Self {
			memo,
		}
	}
}

impl<D> Default for CompiledGuard<D> {
	fn default() -> Self {
		Self {
			memo: std::sync::OnceLock::new(),
		}
	}
}

/// The memo is an evaluation-time cache and says nothing about the node, so it
/// is not printed.
impl<D> std::fmt::Debug for CompiledGuard<D> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CompiledGuard").finish_non_exhaustive()
	}
}

impl<D> CompiledGuard<D> {
	/// Judge `permission`, the clause `definition` stores, for this
	/// evaluation, and answer whether it admits the caller.
	///
	/// The guard is evaluated in the permission frame — reads inside it are
	/// not themselves permission-checked, which is what stops a predicate
	/// following a record link from recursing into itself — one re-entry
	/// deeper than the expression reading it. `denied` builds the error a
	/// refusal raises; a guard that fails to compile denies, and so does one
	/// that raises anything but an error, while an error keeps its type so a
	/// write conflict raised inside a predicate stays retryable.
	pub(crate) async fn check(
		&self,
		permission: LogicalPermissionRef<'_>,
		definition: &Arc<D>,
		ctx: &EvalContext<'_>,
		txn: Option<Arc<Transaction>>,
		denied: impl Fn() -> ExecError,
	) -> FlowResult<()> {
		let guard = match permission {
			LogicalPermissionRef::Full => return Ok(()),
			LogicalPermissionRef::None => return Err(denied().into()),
			LogicalPermissionRef::Specific(guard) => guard,
		};
		let compiled = match self.cached(definition, ctx.plan_depth) {
			Some(compiled) => compiled,
			None => {
				// The guard is judged in a privileged frame, so it is compiled
				// against the session's own values alone, never a caller's
				// bindings.
				let frame = crate::ctx::Context::new_isolated(ctx.exec_ctx.ctx()).freeze();
				let Ok(compiled) = compile_expr(
					guard.clone(),
					&frame,
					ctx.exec_ctx.function_registry(),
					txn,
					Some(Arc::clone(&ctx.exec_ctx.root().auth)),
					ctx.plan_depth + 1,
				)
				.await
				else {
					return Err(denied().into());
				};
				self.memo.get_or_init(ArcSwapOption::empty).store(Some(Arc::new(Memo {
					definition: Arc::clone(definition),
					plan_depth: ctx.plan_depth,
					compiled: Arc::clone(&compiled),
				})));
				compiled
			}
		};
		judge_compiled_guard(&compiled, ctx, denied).await
	}

	/// The kept compilation, if it was made for this definition at this depth.
	fn cached(&self, definition: &Arc<D>, plan_depth: u32) -> Option<Arc<dyn PhysicalExpr>> {
		let memo = self.memo.get()?.load();
		let memo = memo.as_ref()?;
		(Arc::ptr_eq(&memo.definition, definition) && memo.plan_depth == plan_depth)
			.then(|| Arc::clone(&memo.compiled))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::exec::operators::test_util::{
		TestDb, field_idiom, parse_expr, parse_sql_expr, physical_expr, root_ctx,
		root_ctx_with_options, val,
	};
	use crate::expr::Groups;
	use crate::iam::{Level, Role};
	use crate::kvs::TransactionType;

	// =========================================================================
	// table_select_permission_physical
	// =========================================================================

	/// A table definition whose `SELECT` clause is `select`, both halves set
	/// from it as a `DEFINE` statement sets them.
	fn table_with_select(select: Permission) -> TableDefinition {
		let mut def = table_with_disagreeing_halves(select.clone(), &select);
		def.permissions.select = select;
		def
	}

	/// A table definition whose expression half of the `SELECT` clause is
	/// `permission` and whose logical body is compiled from `body`. Only a
	/// test builds one: the two halves of a real definition name the same
	/// clause.
	fn table_with_disagreeing_halves(permission: Permission, body: &Permission) -> TableDefinition {
		use crate::catalog::{
			DatabaseId, FromStored, LogicalPermissionBody, NamespaceId, StoredTableDefinition,
			TableId,
		};
		let mut def = TableDefinition::from_stored(&StoredTableDefinition::new(
			NamespaceId(0),
			DatabaseId(0),
			TableId(0),
			"t".into(),
		))
		.expect("a table definition");
		def.permissions.select = permission;
		def.permissions_body.select = LogicalPermissionBody::of(body);
		def
	}

	#[tokio::test]
	async fn catalog_permission_maps_onto_the_three_physical_arms() {
		let ctx = root_ctx();
		let planner = Planner::new(ctx.ctx(), ctx.function_registry());

		let none = table_with_select(Permission::None);
		let none = table_select_permission_physical(Some(&none), &planner).await.unwrap();
		assert!(matches!(none, PhysicalPermission::Deny));

		let full = table_with_select(Permission::Full);
		let full = table_select_permission_physical(Some(&full), &planner).await.unwrap();
		assert!(matches!(full, PhysicalPermission::Allow));

		let specific =
			table_with_select(Permission::Specific(parse_sql_expr("owner = $auth.id").into()));
		let conditional =
			table_select_permission_physical(Some(&specific), &planner).await.unwrap();
		assert!(matches!(conditional, PhysicalPermission::Conditional(_)));

		// A table with no definition denies.
		let absent = table_select_permission_physical(None, &planner).await.unwrap();
		assert!(matches!(absent, PhysicalPermission::Deny));
	}

	/// The guard is compiled from the definition's logical body, compiled
	/// once for the life of the definition, and not lowered again from the
	/// expression form: a body that disagrees with the expression form is
	/// what the predicate evaluates.
	#[tokio::test]
	async fn the_guard_is_compiled_from_the_definitions_logical_body() {
		let ctx = root_ctx();
		let planner = Planner::new(ctx.ctx(), ctx.function_registry());
		let def = table_with_disagreeing_halves(
			Permission::Specific(parse_sql_expr("false").into()),
			&Permission::Specific(parse_sql_expr("true").into()),
		);
		let permission = table_select_permission_physical(Some(&def), &planner).await.unwrap();
		let allowed =
			check_permission_for_value(&permission, &val("{ }").await, None, &ctx).await.unwrap();
		assert!(allowed, "the logical body (`true`) decides, not the expression form (`false`)");
	}

	#[tokio::test]
	async fn runtime_conversion_matches_the_planner_conversion() {
		// A table guard is evaluated with the table's database selected: a
		// guard reading a field resolves the table's field definitions there.
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let def = table_with_select(Permission::Specific(parse_sql_expr("age > 18").into()));

		let resolved = table_select_permission_physical_runtime(Some(&def), &ctx).await.unwrap();
		let PhysicalPermission::Conditional(expr) = resolved else {
			panic!("a Specific permission resolves to Conditional");
		};

		// The txn-less runtime path must still produce an evaluable predicate,
		// not a deferred/unsupported placeholder.
		let allowed = check_permission_for_value(
			&PhysicalPermission::Conditional(expr),
			&val("{ age: 21 }").await,
			None,
			&ctx,
		)
		.await
		.unwrap();
		assert!(allowed);
	}

	/// The `SELECT` guard of `table` in `db`, as a scan resolving the table at
	/// runtime compiles it.
	async fn table_guard(ctx: &ExecutionContext, table: &str) -> PhysicalPermission {
		let def = ctx
			.database()
			.expect("a database context")
			.get_table_def(&TableName::from(table), None)
			.await
			.expect("catalog read")
			.expect("the table is defined");
		table_select_permission_physical_runtime(Some(&def), ctx).await.expect("compiles")
	}

	/// A guard naming a computed field is judged against the value the field
	/// computes to, derived after the computed field it reads in turn, not
	/// against the record as stored, where it reads `NONE`.
	#[tokio::test]
	async fn a_table_guard_reads_the_computed_fields_it_names() {
		let db = TestDb::new(
			"DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE visible = true;
			 DEFINE FIELD visible ON doc COMPUTED !withheld;
			 DEFINE FIELD withheld ON doc COMPUTED status != 'public';",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let guard = table_guard(&ctx, "doc").await;
		let public = val("{ id: doc:1, status: 'public' }").await;
		let secret = val("{ id: doc:2, status: 'secret' }").await;
		assert!(check_permission_for_value(&guard, &public, None, &ctx).await.unwrap());
		assert!(!check_permission_for_value(&guard, &secret, None, &ctx).await.unwrap());
		// A stored value under the computed name is not what the guard reads.
		let forged = val("{ id: doc:3, status: 'secret', visible: true }").await;
		assert!(!check_permission_for_value(&guard, &forged, None, &ctx).await.unwrap());
	}

	/// The copy the guard judges is its own: a batch keeps the rows it was
	/// handed, and the guard answers per row as it does one at a time.
	#[tokio::test]
	async fn a_table_guard_judges_a_batch_on_copies_of_its_rows() {
		let db = TestDb::new(
			"DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE hidden != true;
			 DEFINE FIELD hidden ON doc COMPUTED status = 'secret';",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let PhysicalPermission::Conditional(guard) = table_guard(&ctx, "doc").await else {
			panic!("a conditional guard");
		};
		let rows = vec![
			val("{ id: doc:1, status: 'public' }").await,
			val("{ id: doc:2, status: 'secret' }").await,
		];
		let before = rows.clone();
		let verdicts = guard.evaluate_batch(EvalContext::from_exec_ctx(&ctx), &rows).await.unwrap();
		assert_eq!(verdicts, vec![Value::Bool(true), Value::Bool(false)]);
		assert_eq!(rows, before, "the rows are not derived in place");
	}

	/// A clause that reads no field of the row — a constant, or the session
	/// alone — is compiled as it stands.
	#[tokio::test]
	async fn a_guard_reading_no_field_of_the_row_is_not_wrapped() {
		let ctx = root_ctx();
		let planner = Planner::new(ctx.ctx(), ctx.function_registry());
		for clause in ["true", "$auth.admin = true", "$session.ns = 'test'"] {
			let def = table_with_select(Permission::Specific(parse_sql_expr(clause).into()));
			let PhysicalPermission::Conditional(guard) =
				table_select_permission_physical(Some(&def), &planner).await.unwrap()
			else {
				panic!("`{clause}` is conditional");
			};
			assert_ne!(guard.name(), "ComputedFieldGuard", "`{clause}` reads no field");
		}
		let def =
			table_with_select(Permission::Specific(parse_sql_expr("owner = $auth.id").into()));
		let PhysicalPermission::Conditional(guard) =
			table_select_permission_physical(Some(&def), &planner).await.unwrap()
		else {
			panic!("a conditional guard");
		};
		assert_eq!(guard.name(), "ComputedFieldGuard", "a guard reading a field is wrapped");
	}

	// =========================================================================
	// should_check_perms
	// =========================================================================

	/// Build a database context for `session` and ask whether `action` needs
	/// permission enforcement.
	async fn checks_perms(db: &TestDb, session: &Session, action: Action) -> bool {
		let ctx = db.exec_ctx_as(session, TransactionType::Read).await;
		should_check_perms(ctx.database().unwrap(), action).unwrap()
	}

	#[tokio::test]
	async fn root_owner_is_exempt_from_both_actions() {
		let db = TestDb::new("").await;
		let owner = TestDb::owner();
		assert!(!checks_perms(&db, &owner, Action::View).await);
		assert!(!checks_perms(&db, &owner, Action::Edit).await);
	}

	#[tokio::test]
	async fn viewer_is_exempt_from_view_but_not_from_edit() {
		let db = TestDb::new("").await;
		let viewer =
			Session::for_level(Level::Database("test".to_owned(), "test".to_owned()), Role::Viewer);
		assert!(!checks_perms(&db, &viewer, Action::View).await);
		// A Viewer has no editor role, so writes stay permission-checked.
		assert!(checks_perms(&db, &viewer, Action::Edit).await);
	}

	#[tokio::test]
	async fn a_role_outside_the_target_database_is_not_exempt() {
		let db = TestDb::new("").await;
		// Owner of a *different* database: has the role, but the database under
		// query is not inside its actor level, so checks stay on.
		let elsewhere =
			Session::for_level(Level::Database("test".to_owned(), "other".to_owned()), Role::Owner);
		assert!(checks_perms(&db, &elsewhere, Action::View).await);
		assert!(checks_perms(&db, &elsewhere, Action::Edit).await);
	}

	#[tokio::test]
	async fn record_users_are_always_permission_checked() {
		let db = TestDb::new("").await;
		let record = Session::for_record(
			"test",
			"test",
			"user",
			crate::types::PublicValue::String("user:tobie".to_owned()),
		);
		assert!(checks_perms(&db, &record, Action::View).await);
		assert!(checks_perms(&db, &record, Action::Edit).await);
	}

	#[tokio::test]
	async fn anonymous_is_exempt_only_while_server_auth_is_disabled() {
		let anon = Session::default().with_ns("test").with_db("test");

		let open = TestDb::new("").await;
		assert!(!checks_perms(&open, &anon, Action::View).await);

		let secured = TestDb::new_with_auth("").await;
		assert!(checks_perms(&secured, &anon, Action::View).await);
	}

	#[tokio::test]
	async fn skip_fetch_perms_disables_every_check() {
		let db = TestDb::new_with_auth("").await;
		let record = Session::for_record(
			"test",
			"test",
			"user",
			crate::types::PublicValue::String("user:tobie".to_owned()),
		);
		let ctx = db.exec_ctx_as(&record, TransactionType::Read).await;

		// Sanity: this identity is checked before the bypass is set.
		assert!(should_check_perms(ctx.database().unwrap(), Action::View).unwrap());

		let ExecutionContext::Database(mut inner) = ctx else {
			panic!("exec_ctx_as builds a Database context");
		};
		inner.ns_ctx.root.skip_fetch_perms = true;
		assert!(!should_check_perms(&inner, Action::View).unwrap());
		assert!(!should_check_perms(&inner, Action::Edit).unwrap());
	}

	// =========================================================================
	// validate_record_user_access
	// =========================================================================

	#[tokio::test]
	async fn root_and_anonymous_identities_are_unconfined() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		assert!(validate_record_user_access(ctx.database().unwrap()).is_ok());
	}

	#[tokio::test]
	async fn a_database_user_is_confined_to_its_own_database() {
		let db = TestDb::new("").await;
		let rid = crate::types::PublicValue::String("user:tobie".to_owned());

		// A database Viewer of test/test reading its own database is allowed.
		let own = Session {
			au: Arc::new(crate::iam::Auth::for_db(crate::iam::Role::Viewer, "test", "test")),
			..Session::for_record("test", "test", "user", rid.clone())
		};
		let ctx = db.exec_ctx_as(&own, TransactionType::Read).await;
		assert!(validate_record_user_access(ctx.database().unwrap()).is_ok());

		// The same principal whose authenticated level names a different database
		// than the one it is operating in (the state left by an RPC `use` pivot)
		// is denied at the read gate, before any table PERMISSIONS are consulted.
		let pivoted = Session {
			au: Arc::new(crate::iam::Auth::for_db(crate::iam::Role::Viewer, "test", "other")),
			..Session::for_record("test", "test", "user", rid)
		};
		let ctx = db.exec_ctx_as(&pivoted, TransactionType::Read).await;
		let err = validate_record_user_access(ctx.database().unwrap()).unwrap_err();
		assert!(
			matches!(err, Error::Exec(ExecError::DbNotAllowed { .. })),
			"expected DbNotAllowed, got {err:?}"
		);
	}

	#[tokio::test]
	async fn a_namespace_user_is_confined_to_its_own_namespace() {
		let db = TestDb::new("").await;
		let rid = crate::types::PublicValue::String("user:tobie".to_owned());

		// A namespace Viewer of `test` may read within its own namespace
		// (a namespace level authorizes any database inside it).
		let own_ns = Session {
			au: Arc::new(crate::iam::Auth::for_ns(crate::iam::Role::Viewer, "test")),
			..Session::for_record("test", "test", "user", rid.clone())
		};
		let ctx = db.exec_ctx_as(&own_ns, TransactionType::Read).await;
		assert!(validate_record_user_access(ctx.database().unwrap()).is_ok());

		// A namespace principal operating in a different namespace is refused.
		let other_ns = Session {
			au: Arc::new(crate::iam::Auth::for_ns(crate::iam::Role::Viewer, "other")),
			..Session::for_record("test", "test", "user", rid)
		};
		let ctx = db.exec_ctx_as(&other_ns, TransactionType::Read).await;
		let err = validate_record_user_access(ctx.database().unwrap()).unwrap_err();
		assert!(
			matches!(err, Error::Exec(ExecError::NsNotAllowed { .. })),
			"expected NsNotAllowed, got {err:?}"
		);
	}

	#[tokio::test]
	async fn a_record_user_is_confined_to_its_own_namespace_and_database() {
		let db = TestDb::new("").await;
		let rid = crate::types::PublicValue::String("user:tobie".to_owned());

		let matching = Session::for_record("test", "test", "user", rid.clone());
		let ctx = db.exec_ctx_as(&matching, TransactionType::Read).await;
		assert!(validate_record_user_access(ctx.database().unwrap()).is_ok());

		// The session names test/test (so the context resolves), but the token's
		// own level names a different namespace / database.
		let wrong_ns = Session {
			au: Arc::new(crate::iam::Auth::for_record(
				"user:tobie".to_owned(),
				"other",
				"test",
				"user",
			)),
			..Session::for_record("test", "test", "user", rid.clone())
		};
		let ctx = db.exec_ctx_as(&wrong_ns, TransactionType::Read).await;
		let err = validate_record_user_access(ctx.database().unwrap()).unwrap_err();
		assert!(
			matches!(err, Error::Exec(ExecError::NsNotAllowed { .. })),
			"expected NsNotAllowed, got {err:?}"
		);

		let wrong_db = Session {
			au: Arc::new(crate::iam::Auth::for_record(
				"user:tobie".to_owned(),
				"test",
				"other",
				"user",
			)),
			..Session::for_record("test", "test", "user", rid)
		};
		let ctx = db.exec_ctx_as(&wrong_db, TransactionType::Read).await;
		let err = validate_record_user_access(ctx.database().unwrap()).unwrap_err();
		assert!(
			matches!(err, Error::Exec(ExecError::DbNotAllowed { .. })),
			"expected DbNotAllowed, got {err:?}"
		);
	}

	// =========================================================================
	// CompiledGuard
	// =========================================================================

	/// Judge `clause`, stored by `definition`, against `row`, and report
	/// whether it admitted.
	async fn guard_admits<D>(
		guard: &CompiledGuard<D>,
		clause: &crate::expr::logical::LogicalExpr,
		definition: &Arc<D>,
		row: &Value,
		ctx: &ExecutionContext,
	) -> bool {
		let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value_and_doc(row);
		guard
			.check(LogicalPermissionRef::Specific(clause), definition, &eval_ctx, None, || {
				ExecError::Thrown("denied".to_owned())
			})
			.await
			.is_ok()
	}

	/// The memo keeps a compilation, never an answer: the kept predicate is
	/// evaluated afresh against every row, so a guard that admits one row
	/// still refuses the next.
	#[tokio::test]
	async fn a_kept_compilation_is_judged_against_each_row() {
		// A stored clause runs as a unit, which is lowered over a transaction.
		let db = crate::exec::operators::test_util::TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let guard: CompiledGuard<String> = CompiledGuard::default();
		// To the memo a definition is an identity and nothing more, so a
		// `String` stands in for the catalog struct a caller passes.
		let definition = Arc::new("v1".to_owned());
		let clause = parse_expr("public = true");

		assert!(
			guard_admits(&guard, &clause, &definition, &val("{ public: true }").await, &ctx).await
		);
		assert!(
			!guard_admits(&guard, &clause, &definition, &val("{ public: false }").await, &ctx)
				.await
		);
	}

	/// A definition the caller re-read is a different `Arc`, and the clause it
	/// carries is compiled afresh. This is what stops a redefinition from
	/// being judged by the clause it replaced: every caller re-reads the
	/// definition per evaluation, so the memo cannot outlive the one it was
	/// made for.
	#[tokio::test]
	async fn a_redefinition_is_judged_by_its_own_clause() {
		// A stored clause runs as a unit, which is lowered over a transaction.
		let db = crate::exec::operators::test_util::TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let guard: CompiledGuard<String> = CompiledGuard::default();
		let admits = parse_expr("public = true");
		let refuses = parse_expr("public = false");

		let first = Arc::new("v1".to_owned());
		assert!(guard_admits(&guard, &admits, &first, &val("{ public: true }").await, &ctx).await);

		// The same row, the same guard node, a definition read afresh.
		let second = Arc::new("v2".to_owned());
		assert!(
			!guard_admits(&guard, &refuses, &second, &val("{ public: true }").await, &ctx).await
		);

		// And back: the memo now holds the second definition's compilation,
		// so the first's clause is compiled again rather than answered by it.
		assert!(guard_admits(&guard, &admits, &first, &val("{ public: true }").await, &ctx).await);
	}

	/// The re-entry depth is the other half of the key. A guard reached one
	/// level deeper is compiled with a depth budget of its own, so a
	/// compilation made at one depth is not served at another.
	#[tokio::test]
	async fn a_kept_compilation_belongs_to_the_depth_it_was_made_at() {
		// A stored clause runs as a unit, which is lowered over a transaction.
		let db = crate::exec::operators::test_util::TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let guard: CompiledGuard<String> = CompiledGuard::default();
		let definition = Arc::new("v1".to_owned());
		let clause = parse_expr("public = true");
		let row = val("{ public: true }").await;

		let shallow = EvalContext::from_exec_ctx(&ctx).with_value_and_doc(&row);
		assert!(
			guard
				.check(LogicalPermissionRef::Specific(&clause), &definition, &shallow, None, || {
					ExecError::Thrown("denied".to_owned())
				})
				.await
				.is_ok()
		);
		assert!(guard.cached(&definition, 0).is_some());
		assert!(guard.cached(&definition, 1).is_none());
	}

	// =========================================================================
	// check_permission_for_value
	// =========================================================================

	#[tokio::test]
	async fn unconditional_arms_short_circuit_without_evaluating() {
		let ctx = root_ctx();
		let row = val("{ id: person:tobie }").await;

		assert!(
			check_permission_for_value(&PhysicalPermission::Allow, &row, None, &ctx).await.unwrap()
		);
		assert!(
			!check_permission_for_value(&PhysicalPermission::Deny, &row, None, &ctx).await.unwrap()
		);
	}

	#[tokio::test]
	async fn a_conditional_permission_is_evaluated_against_the_record() {
		let ctx = root_ctx();
		let perm = PhysicalPermission::Conditional(physical_expr("public = true", &ctx).await);

		let allowed = check_permission_for_value(&perm, &val("{ public: true }").await, None, &ctx)
			.await
			.unwrap();
		assert!(allowed);

		let denied = check_permission_for_value(&perm, &val("{ public: false }").await, None, &ctx)
			.await
			.unwrap();
		assert!(!denied);

		// A missing field is NONE, which is not truthy — deny, not error.
		let absent = check_permission_for_value(&perm, &val("{ other: 1 }").await, None, &ctx)
			.await
			.unwrap();
		assert!(!absent);
	}

	#[tokio::test]
	async fn value_param_binds_the_picked_field_for_field_level_checks() {
		let ctx = root_ctx();
		let perm = PhysicalPermission::Conditional(physical_expr("$value > 10", &ctx).await);
		let row = val("{ score: 42 }").await;

		let allowed =
			check_permission_for_value(&perm, &row, Some(&Value::from(42)), &ctx).await.unwrap();
		assert!(allowed);

		let denied =
			check_permission_for_value(&perm, &row, Some(&Value::from(3)), &ctx).await.unwrap();
		assert!(!denied);
	}

	#[tokio::test]
	async fn the_record_is_bound_as_the_document_root_so_parent_resolves() {
		// `$parent` is the one reader that does not fall back to the current
		// value, so binding only the value would leave this predicate falsy and
		// deny every row.
		let ctx = root_ctx();
		let perm = PhysicalPermission::Conditional(
			physical_expr("acl[WHERE $parent.owner = 'tobie'] != []", &ctx).await,
		);

		let row = val("{ owner: 'tobie', acl: [{ read: true }] }").await;
		assert!(check_permission_for_value(&perm, &row, None, &ctx).await.unwrap());

		let other = val("{ owner: 'jaime', acl: [{ read: true }] }").await;
		assert!(!check_permission_for_value(&perm, &other, None, &ctx).await.unwrap());
	}

	#[tokio::test]
	async fn skip_fetch_perms_allows_a_conditional_permission_unconditionally() {
		let ctx = root_ctx();
		// A predicate that would otherwise deny.
		let perm = PhysicalPermission::Conditional(physical_expr("false", &ctx).await);
		let row = val("{ id: person:tobie }").await;
		assert!(!check_permission_for_value(&perm, &row, None, &ctx).await.unwrap());

		let ExecutionContext::Root(mut root) = ctx else {
			panic!("root_ctx builds a Root context");
		};
		root.skip_fetch_perms = true;
		let inner = ExecutionContext::Root(root);
		assert!(check_permission_for_value(&perm, &row, None, &inner).await.unwrap());
	}

	#[tokio::test]
	async fn control_flow_out_of_a_predicate_becomes_an_error_not_a_panic() {
		let ctx = root_ctx();
		// BREAK has no surrounding loop inside a permission predicate, so it has
		// nowhere to go and must surface as an error.
		let perm = PhysicalPermission::Conditional(physical_expr("BREAK", &ctx).await);
		let row = val("{ id: person:tobie }").await;

		let err = check_permission_for_value(&perm, &row, None, &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("unexpected control flow"),
			"expected the control-flow message, got {err}"
		);
	}

	#[tokio::test]
	async fn an_error_inside_a_predicate_is_carried_through_unwrapped() {
		let ctx = root_ctx();
		// A thrown error must stay downcastable rather than collapse to
		// `Internal`, so a write conflict raised per row can still be retried.
		let perm = PhysicalPermission::Conditional(physical_expr("THROW 'boom'", &ctx).await);
		let row = val("{ id: person:tobie }").await;

		let err = check_permission_for_value(&perm, &row, None, &ctx).await.unwrap_err();
		assert!(err.to_string().contains("boom"), "expected the thrown message, got {err}");
		assert!(
			!err.to_string().contains("unexpected control flow"),
			"a THROW is an error, not a stray control-flow signal: {err}"
		);
	}

	#[tokio::test]
	async fn a_conditional_permission_reads_through_a_record_link() {
		// A table permission that dereferences a link needs a transaction, so
		// this is the path that `root_ctx` cannot serve.
		let db = TestDb::new(
			"DEFINE TABLE org SCHEMALESS;
			 DEFINE TABLE doc SCHEMALESS;
			 CREATE org:acme SET public = true;
			 CREATE org:secret SET public = false;
			 CREATE doc:1 SET org = org:acme;
			 CREATE doc:2 SET org = org:secret;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let perm = PhysicalPermission::Conditional(physical_expr("org.public = true", &ctx).await);

		let doc1 = val("{ id: doc:1, org: org:acme }").await;
		let doc2 = val("{ id: doc:2, org: org:secret }").await;
		assert!(check_permission_for_value(&perm, &doc1, None, &ctx).await.unwrap());
		assert!(!check_permission_for_value(&perm, &doc2, None, &ctx).await.unwrap());
	}

	// =========================================================================
	// PhysicalTableSelect
	// =========================================================================

	#[tokio::test]
	async fn the_ann_gate_reports_whole_index_decisions_up_front() {
		let ctx = root_ctx();

		let allow = PhysicalTableSelect::new(PhysicalPermission::Allow, ctx.clone());
		assert_eq!(allow.allows_every_doc(), Some(true));

		let deny = PhysicalTableSelect::new(PhysicalPermission::Deny, ctx.clone());
		assert_eq!(deny.allows_every_doc(), Some(false));

		// Conditional cannot be decided for the whole index, so the search must
		// gate each candidate individually.
		let conditional = PhysicalTableSelect::new(
			PhysicalPermission::Conditional(physical_expr("public = true", &ctx).await),
			ctx.clone(),
		);
		assert_eq!(conditional.allows_every_doc(), None);
	}

	#[tokio::test]
	async fn the_ann_gate_decides_a_candidate_with_the_same_predicate() {
		let ctx = root_ctx();
		let gate = PhysicalTableSelect::new(
			PhysicalPermission::Conditional(physical_expr("public = true", &ctx).await),
			ctx.clone(),
		);

		let rid = Arc::new(RecordId {
			table: "doc".into(),
			key: crate::val::RecordIdKey::Number(1),
		});
		let allowed = Arc::new(Record::new(val("{ public: true }").await));
		let denied = Arc::new(Record::new(val("{ public: false }").await));

		let (yes, no) = async {
			let yes = gate.allows_doc(&rid, &allowed).await.unwrap();
			let no = gate.allows_doc(&rid, &denied).await.unwrap();
			(yes, no)
		}
		.await;
		assert!(yes);
		assert!(!no);
	}

	// =========================================================================
	// The stored clauses that are not permissions
	// =========================================================================

	/// A field definition whose `DEFAULT` is `src`, both halves set from it as
	/// a `DEFINE` statement sets them.
	///
	/// The `AUTH LIMIT` is set explicitly to the stamp that narrows nobody,
	/// which is what a definition carries when its definer was unconstrained
	/// and what one written before `AUTH LIMIT` existed decodes to;
	/// `FieldDefinition`'s `Default` is not that stamp.
	fn field_with_default(src: &str) -> FieldDefinition {
		let expr: Expr = parse_sql_expr(src).into();
		FieldDefinition {
			name: field_idiom("id"),
			table: "t".into(),
			default_body: Some(crate::catalog::LogicalBody::of(&expr)),
			default: crate::catalog::DefineDefault::Set(expr),
			auth_limit: crate::catalog::auth::AuthLimit::new_no_limit(),
			..Default::default()
		}
	}

	/// An event on `t` whose `WHEN` is `when` and whose `THEN` clauses are
	/// `then`, in order, with no `AUTH LIMIT`.
	fn event(when: &str, then: &[&str]) -> EventDefinition {
		EventDefinition::new(
			"ev".into(),
			"t".into(),
			parse_sql_expr(when).into(),
			then.iter().map(|t| parse_sql_expr(t).into()).collect(),
			crate::catalog::EventKind::Sync,
			crate::catalog::auth::AuthLimit::new_no_limit(),
			None,
		)
	}

	/// A view over `source`, classified as [`view_to_definition`] classifies a
	/// parsed one.
	fn view(fields: &str, condition: Option<&str>, groups: Option<Groups>) -> ViewDefinition {
		let view = crate::expr::View {
			expr: crate::catalog::FieldsText::from_raw(fields)
				.compile()
				.expect("the field list should compile"),
			what: vec!["source".into()],
			cond: condition.map(|c| crate::expr::Cond(parse_sql_expr(c).into())),
			group: groups,
		};
		crate::catalog::view_to_definition(&view).expect("the view should classify")
	}

	/// A txn-less planner over `ctx`, the way runtime clause compilation
	/// builds one.
	fn planner_for(ctx: &ExecutionContext) -> Planner<'_> {
		Planner::new(ctx.ctx(), ctx.function_registry())
	}

	/// Evaluate a compiled clause against `row`, bound as both the value and
	/// the document, the way every caller of a stored clause binds the record.
	async fn eval_clause(
		clause: &Arc<dyn PhysicalExpr>,
		row: &Value,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		clause.evaluate(EvalContext::from_exec_ctx(ctx).with_value_and_doc(row)).await
	}

	#[tokio::test]
	async fn a_field_default_is_compiled_from_the_definitions_logical_body() {
		// A stored clause runs as a unit, which is lowered over a transaction.
		let db = crate::exec::operators::test_util::TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let field = field_with_default("n + 1");
		let compiled = field_default_physical(&field, &planner_for(&ctx))
			.await
			.unwrap()
			.expect("a field that declares a DEFAULT compiles one");
		assert_ne!(
			compiled.name(),
			"UncompilableClause",
			"a clause with a logical form is compiled, not carried"
		);
		let row = val("{ n: 1 }").await;
		assert_eq!(eval_clause(&compiled, &row, &ctx).await.unwrap(), Value::from(2));
	}

	#[tokio::test]
	async fn a_field_with_no_default_compiles_nothing() {
		let ctx = root_ctx();
		let field = FieldDefinition {
			name: field_idiom("n"),
			table: "t".into(),
			..Default::default()
		};
		assert!(field_default_physical(&field, &planner_for(&ctx)).await.unwrap().is_none());
	}

	/// A clause holding a write compiles like any other. The frames that must
	/// not write — a `SELECT` permission predicate, a `COMPUTED` body — refuse
	/// the write where it runs rather than refusing to compile the clause that
	/// holds it, and a `DEFAULT` is neither of those.
	#[tokio::test]
	async fn a_clause_holding_a_write_compiles() {
		let ctx = root_ctx();
		let field = field_with_default("(CREATE ONLY other:1).id");
		let compiled =
			field_default_physical(&field, &planner_for(&ctx)).await.unwrap().expect("a DEFAULT");
		assert_ne!(compiled.name(), "UncompilableClause");
	}

	/// The field's `AUTH LIMIT` is ADR 0001's definer's-rights clamp, and a
	/// stamp that can narrow no caller leaves the context as it was.
	#[test]
	fn the_default_frame_applies_the_fields_auth_limit() {
		let ctx = root_ctx();
		let inert = field_with_default("1");
		let frame = field_default_frame(&ctx, &inert).unwrap();
		assert!(
			Arc::ptr_eq(&frame.root().auth, &ctx.root().auth),
			"the root-Owner stamp narrows nobody"
		);

		let mut limited = field_with_default("1");
		limited.auth_limit = crate::catalog::auth::AuthLimit::new(
			crate::catalog::auth::AuthLevel::Namespace("test".to_owned()),
			Some("Viewer".to_owned()),
		);
		let frame = field_default_frame(&ctx, &limited).unwrap();
		assert!(
			!Arc::ptr_eq(&frame.root().auth, &ctx.root().auth),
			"a narrowing stamp is applied to the frame's authority"
		);
	}

	#[tokio::test]
	async fn an_events_clauses_are_compiled_in_the_order_they_run() {
		let ctx = root_ctx();
		let definition = event("$event = 'CREATE'", &["$value.n", "(UPDATE other:1 SET n = 1)"]);
		let planner = planner_for(&ctx);
		let compiled = event_physical(&definition, &planner).await.unwrap();
		assert_ne!(compiled.when.name(), "UncompilableClause");
		let then = compiled.then().await.unwrap();
		assert_eq!(then.len(), 2, "one body per THEN clause");
		assert_ne!(then[0].name(), "UncompilableClause");
		assert_ne!(
			then[1].name(),
			"UncompilableClause",
			"an event body may write, so a THEN that writes compiles like any other"
		);
	}

	/// Resolving an event compiles its `WHEN` alone. A `THEN` the planner
	/// declines is only declined where the body would have run, which is
	/// inside the gate the `WHEN` opens.
	#[tokio::test]
	async fn a_declined_then_is_only_declined_when_it_is_asked_for() {
		let ctx = root_ctx();
		let definition = event("true", &["[LET $x = 1]"]);
		let planner = planner_for(&ctx);
		let compiled = event_physical(&definition, &planner)
			.await
			.expect("resolving the event compiles the WHEN alone");
		let declined = compiled.then().await.expect_err("the planner declines the body");
		assert!(
			declined.to_string().contains("LET statements can only appear"),
			"the decline names the clause the planner could not compile: {declined}"
		);
	}

	#[test]
	fn the_event_frame_binds_the_five_parameters_and_names_the_document() {
		let ctx = root_ctx();
		let definition = event("true", &[]);
		let before = Value::from(1);
		let after = Value::from(2);
		let input = Value::from(3);
		let images = || EventImages {
			before: before.clone(),
			after: after.clone(),
			input: input.clone(),
		};

		let (frame, document) =
			event_frame(&ctx, &definition, DocAction::Update, images()).unwrap();
		assert_eq!(frame.value("before"), Some(&before));
		assert_eq!(frame.value("after"), Some(&after));
		assert_eq!(frame.value("input"), Some(&input));
		assert_eq!(frame.value("event"), Some(&Value::from("UPDATE")));
		assert_eq!(frame.value("value"), Some(&after), "an update's event is about the post-image");
		assert_eq!(document, after, "the document is the image `$value` names");
		assert!(frame.root().skip_fetch_perms, "an event body runs with enforcement off");

		let (frame, document) =
			event_frame(&ctx, &definition, DocAction::Delete, images()).unwrap();
		assert_eq!(frame.value("event"), Some(&Value::from("DELETE")));
		assert_eq!(frame.value("value"), Some(&before), "a delete's event is about the pre-image");
		assert_eq!(document, before);
	}

	#[tokio::test]
	async fn a_materialized_views_condition_and_field_list_are_compiled() {
		// A stored clause runs as a unit, which is lowered over a transaction.
		let db = crate::exec::operators::test_util::TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let definition = view("name", Some("paid = true"), None);
		let compiled = materialized_view_physical(&definition, &planner_for(&ctx))
			.await
			.unwrap()
			.expect("a materialized view has a route");
		let condition = compiled.condition.expect("the view declares a condition");
		assert_ne!(condition.name(), "UncompilableClause");
		assert_eq!(compiled.projection.name(), "CompiledProjection");

		let paid = val("{ paid: true }").await;
		assert!(eval_clause(&condition, &paid, &ctx).await.unwrap().is_truthy());
		let unpaid = val("{ paid: false }").await;
		assert!(!eval_clause(&condition, &unpaid, &ctx).await.unwrap().is_truthy());
	}

	#[tokio::test]
	async fn a_view_with_no_condition_compiles_none() {
		let ctx = root_ctx();
		let definition = view("name", None, None);
		let compiled =
			materialized_view_physical(&definition, &planner_for(&ctx)).await.unwrap().unwrap();
		assert!(compiled.condition.is_none());
	}

	/// Only a materialized view is maintained by judging the written record
	/// against its clauses, so only it has a route here.
	#[tokio::test]
	async fn a_view_maintained_some_other_way_has_no_route() {
		let ctx = root_ctx();
		let aggregated = view("count()", None, Some(Groups::default()));
		assert!(matches!(aggregated, ViewDefinition::Aggregated { .. }));
		assert!(
			materialized_view_physical(&aggregated, &planner_for(&ctx)).await.unwrap().is_none()
		);

		let unmaintained = ViewDefinition::Select {
			fields: crate::expr::Fields::all(),
			tables: vec!["source".into()],
			condition: None,
			groups: None,
		};
		assert!(
			materialized_view_physical(&unmaintained, &planner_for(&ctx)).await.unwrap().is_none()
		);
	}

	/// A view definition records no `AUTH LIMIT`, so a view body runs as whoever wrote the record —
	/// the one clause here that ADR 0001's definer's rights do not reach.
	#[test]
	fn the_view_frame_disables_enforcement_without_narrowing_authority() {
		let ctx = root_ctx_with_options();
		let frame = view_frame(&ctx);
		assert!(frame.root().perms_suppressed);
		assert!(Arc::ptr_eq(&frame.root().auth, &ctx.root().auth));

		// Suppression, not the permission frame: the view's own clauses may
		// write, and so may an event the row it stores goes on to fire.
		assert!(!frame.root().skip_fetch_perms);
		let options = frame.options().expect("the frame carries options");
		assert!(!options.perms, "a view body is not enforced");
		assert_eq!(options.no_write, None, "a view body may write");
	}

	/// A cascade's bypass is scoped to the record it repairs: nested work with
	/// its own permission contract — an edge table's `PERMISSIONS FOR delete`,
	/// reached because deleting a vertex deletes its edges — runs as the actor
	/// again. That is the whole of what `caller_perms` names, and it is
	/// observable only when *this* delete was itself a cascade.
	#[test]
	fn a_reference_cascade_keeps_the_actor_state_for_a_nested_scope() {
		let ctx = root_ctx_with_options();
		assert!(!ctx.root().perms_suppressed, "the actor's own frame is enforced");

		let cascade = ctx.for_reference_cascade();
		assert!(cascade.root().perms_suppressed);
		assert!(!cascade.root().caller_perms_suppressed);
		let options = cascade.options().expect("the frame carries options");
		assert!(!options.perms);
		assert!(options.caller_perms);
		assert_eq!(options.no_write, None, "a cascade exists to write");

		let nested = cascade.with_caller_perms();
		assert!(!nested.root().perms_suppressed, "the actor's state is restored");
		assert!(nested.options().expect("options").perms);
	}

	/// The difference between the two suppressing frames, stated as a test: one
	/// leaves the actor's state to return to and the other does not.
	#[test]
	fn a_suppressed_frame_leaves_nothing_to_return_to() {
		let ctx = root_ctx_with_options();
		let suppressed = ctx.with_perms_suppressed(true);
		assert!(suppressed.root().perms_suppressed);
		assert!(suppressed.root().caller_perms_suppressed);

		let nested = suppressed.with_caller_perms();
		assert!(nested.root().perms_suppressed, "there was no enforced state to return to");
		assert!(!nested.options().expect("options").perms);
	}

	/// A subscriber's clauses run as the subscriber, not as the writer, and
	/// under the checks that principal would face — a write may have reached
	/// the notification loop from a frame that suppressed them.
	#[test]
	fn a_replaced_principal_runs_as_the_subscriber_with_enforcement_on() {
		let ctx = root_ctx_with_options().with_perms_suppressed(true);
		let subscriber =
			Arc::new(crate::iam::Auth::for_db(crate::iam::Role::Viewer, "test", "test"));

		let frame = ctx.with_replaced_principal(Arc::clone(ctx.ctx()), &subscriber);
		assert!(Arc::ptr_eq(&frame.root().auth, &subscriber));
		assert!(!frame.root().perms_suppressed, "each subscriber is owed its own checks");
		assert!(!frame.root().caller_perms_suppressed);
		assert!(!frame.root().skip_fetch_perms);

		let options = frame.options().expect("the frame carries options");
		assert!(Arc::ptr_eq(&options.auth, &subscriber));
		assert!(options.perms);
	}

	/// A field list projects the record it is evaluated against, which is
	/// what the view's row is stored from.
	#[tokio::test]
	async fn a_field_list_projects_the_record_it_is_evaluated_against() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = db.exec_ctx().await;
		let row = val("{ name: 'a', price: 2, qty: 3 }").await;
		for (text, expected) in [
			("name, price * qty AS total", "{ name: 'a', total: 6 }"),
			("*", "{ name: 'a', price: 2, qty: 3 }"),
			("*, price * qty AS price", "{ name: 'a', price: 6, qty: 3 }"),
			("VALUE qty", "3"),
		] {
			let fields = crate::catalog::FieldsText::from_raw(text)
				.compile()
				.expect("the field list should compile");
			let compiled = projection_physical(&fields, &planner_for(&ctx)).await;
			assert_eq!(compiled.name(), "CompiledProjection", "`{text}`");
			let expected = val(expected).await;
			assert_eq!(eval_clause(&compiled, &row, &ctx).await.unwrap(), expected, "`{text}`");
		}
	}

	/// A clause the engine cannot compile fails where it is evaluated, naming
	/// why, and is read-only.
	#[tokio::test]
	async fn an_uncompilable_clause_fails_when_evaluated() {
		let ctx = root_ctx();
		let clause = UncompilableClause::of("a shape".to_owned());
		assert_eq!(clause.access_mode(), AccessMode::ReadOnly);
		let err = eval_clause(&clause, &Value::None, &ctx).await.expect_err("it never evaluates");
		assert!(err.to_string().contains("a shape"), "the reason is reported: {err}");
	}
}
