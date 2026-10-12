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
//! [`Clause::Uncompilable`], which fails with `PlannerUnsupported` where the
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

use surrealdb_strand::TableName;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{
	EventDefinition, FieldDefinition, IndexDefinition, KeyStructure, Permission, TableDefinition,
};
use crate::doc::Action as DocAction;
use crate::err::{EngineError, Error};
use crate::exe::FlowResultExt as _;
use crate::exec::operators::scan::pipeline::{
	FieldState, build_field_state, compute_fields_for_value, narrowing_auth_limit,
};
use crate::exec::planner::Planner;
use crate::exec::{
	Clause, DatabaseContext, Error as ExecError, EvalContext, ExecutionContext, FlowResult,
};
use crate::expr::computed_deps::extract_computed_deps;
pub(crate) use crate::expr::logical::GuardKind;
use crate::expr::logical::{LogicalExpr, LogicalPermission};
use crate::expr::{ControlFlow, Expr};
use crate::iam::{Action, Auth};
use crate::val::Value;

/// Result of a permission check.
#[derive(Debug, Clone)]
pub enum PhysicalPermission {
	/// Permission allows access unconditionally
	Allow,
	/// Permission denies access unconditionally
	Deny,
	/// Permission requires per-record evaluation
	Conditional(Clause),
}

/// The `SELECT` permission of a table as the engine applies it,
/// compiled through `planner`.
///
/// The guard is compiled from the definition's logical body — the clause's
/// stored text, lowered once for the life of the definition and shared with
/// the analyzer — so a statement that plans the table does not lower the
/// clause again. A guard whose text has no logical form is carried as an
/// [`Clause::Uncompilable`].
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
	guard: Clause,
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
	pub(crate) fn for_clause(guard: Clause, table: &TableName, clause: &Expr) -> Clause {
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
		guard: Clause,
		table: TableName,
		reads: Option<HashSet<String>>,
	) -> Clause {
		Clause::Guard(Arc::new(Self {
			guard,
			table,
			reads,
			state: tokio::sync::OnceCell::new(),
		}))
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

impl ComputedFieldGuard {
	/// The guard's verdict on the value in `ctx`: the row's derived copy
	/// where the guard reads a computed field, the row as it came otherwise.
	pub(crate) async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		// Resolved to derive nothing: the guard judges the row as it came.
		if matches!(self.state.get(), Some(None)) {
			return self.guard.evaluate(ctx).await;
		}
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
	}

	/// The guard's verdict on each of `values`, each judged as
	/// [`Self::evaluate`] judges the current value.
	pub(crate) async fn evaluate_batch(
		&self,
		ctx: EvalContext<'_>,
		values: &[Value],
	) -> FlowResult<Vec<Value>> {
		// Resolved to derive nothing: the guard judges the batch as it came.
		if matches!(self.state.get(), Some(None)) {
			return self.guard.evaluate_batch(ctx, values).await;
		}
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
	}
}

/// Renders as the guard it judges.
impl ToSql for ComputedFieldGuard {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.guard.fmt_sql(f, fmt);
	}
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
			let compiled = planner.scalar_metadata(guard.clone()).await?;
			Ok(PhysicalPermission::Conditional(match predicate {
				true => Clause::predicate(guard, compiled),
				false => Clause::of(guard, compiled),
			}))
		}
		Ok(LogicalPermission::Specific(_)) => Ok(match permission {
			Permission::None => PhysicalPermission::Deny,
			Permission::Full => PhysicalPermission::Allow,
			Permission::Specific(_) => PhysicalPermission::Conditional(Clause::uncompilable(
				unplannable.unwrap_or_default(),
			)),
		}),
		Err(reason) => Ok(match permission {
			Permission::None => PhysicalPermission::Deny,
			Permission::Full => PhysicalPermission::Allow,
			Permission::Specific(_) => {
				PhysicalPermission::Conditional(Clause::uncompilable(reason.to_string()))
			}
		}),
	}
}

// ===========================================================================
// The stored clauses that are not permissions
// ===========================================================================

/// A stored clause as the engine evaluates it: `logical`, the clause
/// compiled from its text by the definition that holds it, or — when that text
/// has no logical form, or holds a write no operator compiles — an
/// [`Clause::Uncompilable`] naming why.
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
) -> Result<Clause, Error> {
	match logical {
		Ok(body) => match crate::exec::logical::unplannable_write_in_expr(body) {
			None => {
				let compiled = planner.scalar_metadata(body.clone()).await?;
				Ok(Clause::of(body.clone(), compiled))
			}
			Some(reason) => Ok(Clause::uncompilable(reason)),
		},
		Err(reason) => Ok(Clause::uncompilable(reason.to_string())),
	}
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
	cols: Vec<Clause>,
	/// A `COUNT` index's `WHERE` guard; `None` for every other index kind and
	/// for an unguarded count index.
	guard: Option<Clause>,
	/// Whether a column or the guard may read a component of the record id, as
	/// the element of the id, so the image it is evaluated over must carry its
	/// `id`: a column that addresses a component by its name or as `id[i]`, or
	/// any guard over a table whose record ids name their components.
	reads_record_id: bool,
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
/// `key` is the structure of the table's record ids where they name their
/// components. A column that addresses a component, and a guard that reads one,
/// is evaluated as the element of the image's own id (`id[i]`), the one place a
/// component has a value ([`crate::exec::index::derived_columns`]); a table
/// with no named components passes `None` and its expressions are compiled as
/// they are stored.
///
/// An expression the lowering has no form for — or one holding a write no
/// operator compiles — is carried as a [`Clause::Uncompilable`], as every other
/// stored clause is; see [`clause_to_physical`].
///
/// Compile only a definition whose entries this caller would evaluate: an
/// index the catalog has marked for removal is never maintained, and
/// compiling its clauses would fail a write over an expression it would not
/// have run.
pub(crate) async fn index_entry_physical(
	ix: &IndexDefinition,
	planner: &Planner<'_>,
	key: Option<&KeyStructure>,
) -> Result<PhysicalIndexEntry, Error> {
	use crate::exec::index::derived_columns::{canonical_columns, canonical_guard};

	// A column reads a component whichever way it spells it, by name or as
	// `id[i]`; only the first is rewritten.
	let key = key.filter(|key| key.origin == crate::catalog::KeyOrigin::IdField);
	let columns = canonical_columns(key, &ix.cols);
	let mut reads_record_id =
		key.is_some_and(|key| ix.cols.iter().any(|col| key.component_of(col).is_some()));
	let mut cols = Vec::with_capacity(columns.len());
	for col in columns.iter() {
		cols.push(index_column_physical(col, planner).await?);
	}
	let guard = match &ix.count_cond {
		Some(cond) => {
			let logical = crate::expr::logical::lower_expr(cond.0.clone());
			let canonical = logical.as_ref().ok().and_then(|guard| canonical_guard(key, guard));
			// A guard over a table whose record ids name their components may
			// read one as `id[i]` as well as by name.
			reads_record_id |= key.is_some();
			let body = match (&canonical, &logical) {
				(Some(canonical), _) => Ok(canonical),
				(None, Ok(guard)) => Ok(guard),
				(None, Err(err)) => Err(anyhow::Error::new(err.clone())),
			};
			Some(clause_to_physical(body, planner).await?)
		}
		None => None,
	};
	Ok(PhysicalIndexEntry {
		cols,
		guard,
		reads_record_id,
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
) -> Result<Clause, Error> {
	index_expr_physical(&Expr::Idiom(col.clone()), planner).await
}

/// One stored expression of an index, compiled or carried; see
/// [`clause_to_physical`].
async fn index_expr_physical(expr: &Expr, planner: &Planner<'_>) -> Result<Clause, Error> {
	let logical = crate::expr::logical::lower_expr(expr.clone());
	let logical = logical.as_ref().map_err(|err| anyhow::Error::new(err.clone()));
	clause_to_physical(logical, planner).await
}

impl PhysicalIndexEntry {
	/// Whether a column or the guard may read a component of the record id, so
	/// the image the entry is evaluated over must carry its `id` field: a column
	/// that addresses a component by its name or as `id[i]`, or any guard over a
	/// table whose record ids name their components. An image decoded from
	/// storage may carry none; the caller that holds the record's id puts it
	/// there before evaluating.
	pub(crate) fn reads_record_id(&self) -> bool {
		self.reads_record_id
	}

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
		base.computing_record = Some(Arc::new(crate::exec::eval_context::ComputingRecord {
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
		PhysicalPermission::Conditional(clause) => {
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
			// there is no surrounding block — so those stay an error.
			let result = clause.evaluate(eval_ctx).await.map_err(|ctrl| match ctrl {
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
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::exec::operators::test_util::{
		TestDb, field_idiom, parse_sql_expr, root_ctx, root_ctx_with_options,
	};
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

		let specific = table_with_select(Permission::Specific(parse_sql_expr("owner = $auth.id")));
		let conditional =
			table_select_permission_physical(Some(&specific), &planner).await.unwrap();
		assert!(matches!(conditional, PhysicalPermission::Conditional(_)));

		// A table with no definition denies.
		let absent = table_select_permission_physical(None, &planner).await.unwrap();
		assert!(matches!(absent, PhysicalPermission::Deny));
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

	// =========================================================================
	// check_permission_for_value
	// =========================================================================

	// =========================================================================
	// PhysicalTableSelect
	// =========================================================================

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
		let expr: Expr = parse_sql_expr(src);
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
			parse_sql_expr(when),
			then.iter().map(|t| parse_sql_expr(t)).collect(),
			crate::catalog::EventKind::Sync,
			crate::catalog::auth::AuthLimit::new_no_limit(),
			None,
		)
	}

	/// Evaluate a compiled clause against `row`, bound as both the value and
	/// the document, the way every caller of a stored clause binds the record.
	async fn eval_clause(
		clause: &Clause,
		row: &Value,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		clause.evaluate(EvalContext::from_exec_ctx(ctx).with_value_and_doc(row)).await
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

	/// A clause the engine cannot compile fails where it is evaluated, naming
	/// why.
	#[tokio::test]
	async fn an_uncompilable_clause_fails_when_evaluated() {
		let ctx = root_ctx();
		let clause = Clause::uncompilable("a shape".to_owned());
		let err = eval_clause(&clause, &Value::None, &ctx).await.expect_err("it never evaluates");
		assert!(err.to_string().contains("a shape"), "the reason is reported: {err}");
	}

	// =========================================================================
	// index_entry_physical over a table whose record id names its components
	// =========================================================================

	mod keyed_index_entries {
		use chrono::{TimeZone, Utc};
		use surrealdb_strand::Strand;

		use super::*;
		use crate::catalog::{Index, IndexId};
		use crate::expr::{Cond, KeyNames, Kind, KindLiteral};
		use crate::val::{Array, Datetime, Object, RecordId, RecordIdKey};

		/// `[city: string, time: datetime]`.
		fn reading() -> KeyStructure {
			let names = ["city", "time"].iter().map(|name| Strand::new(*name)).collect();
			KeyStructure::from_id_field(
				&Kind::Literal(KindLiteral::Array(vec![Kind::String, Kind::Datetime])),
				&KeyNames::new(names, 2).expect("valid names"),
			)
			.expect("a keyed type")
		}

		fn datetime(h: u32, m: u32, s: u32, nanos: u32) -> Datetime {
			let whole = Utc.with_ymd_and_hms(2025, 1, 1, h, m, s).unwrap();
			Datetime(whole + chrono::TimeDelta::nanoseconds(i64::from(nanos)))
		}

		/// The image of `reading:['london', 2025-01-01T10:00:05.5Z]`, which also
		/// holds a stored field `other`.
		fn image() -> Value {
			let id = RecordId {
				table: "reading".into(),
				key: RecordIdKey::Array(Array(vec![
					Value::from("london"),
					Value::Datetime(datetime(10, 0, 5, 500_000_000)),
				])),
			};
			Value::Object(Object::from_iter([
				("id".to_owned(), Value::RecordId(id)),
				("other".to_owned(), Value::from(7)),
			]))
		}

		fn index(cols: &[&str], guard: Option<&str>) -> IndexDefinition {
			IndexDefinition {
				index_id: IndexId(1),
				name: Strand::from("i"),
				table_name: "reading".into(),
				cols: cols.iter().map(|col| field_idiom(col)).collect(),
				index: match guard {
					Some(_) => Index::Count(None),
					None => Index::Idx,
				},
				count_cond: guard.map(|guard| Cond(parse_sql_expr(guard))),
				comment: None,
				prepare_remove: false,
				format_version: 1,
			}
		}

		/// A database-level context over an open transaction: a compiled column
		/// is a closure unit, and a unit is invoked inside a transaction.
		async fn db_ctx() -> ExecutionContext {
			TestDb::new("").await.exec_ctx().await
		}

		async fn entry(
			ix: &IndexDefinition,
			key: Option<&KeyStructure>,
			ctx: &ExecutionContext,
		) -> PhysicalIndexEntry {
			let planner = Planner::new(ctx.ctx(), ctx.function_registry());
			index_entry_physical(ix, &planner, key).await.expect("the entry compiles")
		}

		#[tokio::test]
		async fn a_column_that_names_a_component_holds_the_element_of_the_id() {
			let ctx = db_ctx().await;
			let key = reading();
			let ix = index(&["time", "city", "other"], None);
			let entry = entry(&ix, Some(&key), &ctx).await;
			assert!(entry.reads_record_id());
			assert_eq!(
				entry.values(&image(), &ctx).await.unwrap(),
				Some(vec![
					Value::Datetime(datetime(10, 0, 5, 500_000_000)),
					Value::from("london"),
					Value::from(7),
				])
			);
		}

		#[tokio::test]
		async fn a_component_spelled_as_an_element_of_the_id_holds_the_same_value() {
			let ctx = db_ctx().await;
			let key = reading();
			let named = entry(&index(&["time"], None), Some(&key), &ctx).await;
			let element = entry(&index(&["id[1]"], None), Some(&key), &ctx).await;
			assert!(element.reads_record_id());
			assert_eq!(
				named.values(&image(), &ctx).await.unwrap(),
				element.values(&image(), &ctx).await.unwrap()
			);
		}

		#[tokio::test]
		async fn a_table_with_no_named_components_compiles_a_column_as_stored() {
			let ctx = db_ctx().await;
			let entry = entry(&index(&["time", "other"], None), None, &ctx).await;
			assert!(!entry.reads_record_id());
			// `time` is not a stored field of the image, so it holds nothing.
			assert_eq!(
				entry.values(&image(), &ctx).await.unwrap(),
				Some(vec![Value::None, Value::from(7)])
			);
		}

		#[tokio::test]
		async fn a_column_that_names_no_component_reads_no_part_of_the_id() {
			let ctx = db_ctx().await;
			let key = reading();
			let entry = entry(&index(&["other"], None), Some(&key), &ctx).await;
			assert!(!entry.reads_record_id());
			assert_eq!(entry.values(&image(), &ctx).await.unwrap(), Some(vec![Value::from(7)]));
		}

		#[tokio::test]
		async fn an_image_with_no_id_holds_no_component() {
			let ctx = db_ctx().await;
			let key = reading();
			let entry = entry(&index(&["time"], None), Some(&key), &ctx).await;
			let bare = Value::Object(Object::from_iter([("other".to_owned(), Value::from(7))]));
			assert_eq!(entry.values(&bare, &ctx).await.unwrap(), Some(vec![Value::None]));
			assert_eq!(entry.values(&Value::None, &ctx).await.unwrap(), None);
		}

		#[tokio::test]
		async fn a_guard_reads_a_component_as_the_element_of_the_id() {
			let ctx = db_ctx().await;
			let key = reading();
			let ix = index(&[], Some("city = 'london' AND time > d'2025-01-01T10:00:00Z'"));
			let entry = entry(&ix, Some(&key), &ctx).await;
			assert!(entry.reads_record_id());
			assert_eq!(entry.guard_matches(&image(), &ctx).await.unwrap(), Some(true));

			let elsewhere = index(&[], Some("city = 'paris'"));
			let entry = self::entry(&elsewhere, Some(&key), &ctx).await;
			assert_eq!(entry.guard_matches(&image(), &ctx).await.unwrap(), Some(false));

			// Stored as written, the guard reads fields the image does not hold.
			let stored = self::entry(&ix, None, &ctx).await;
			assert!(!stored.reads_record_id());
			assert_eq!(stored.guard_matches(&image(), &ctx).await.unwrap(), Some(false));
		}

		#[tokio::test]
		async fn a_guard_that_reads_no_component_is_compiled_as_stored() {
			let ctx = db_ctx().await;
			let key = reading();
			let entry = entry(&index(&[], Some("other = 7")), Some(&key), &ctx).await;
			// Any guard over a keyed table is evaluated over an image with its id.
			assert!(entry.reads_record_id());
			assert_eq!(entry.guard_matches(&image(), &ctx).await.unwrap(), Some(true));
			assert_eq!(entry.guard_matches(&Value::None, &ctx).await.unwrap(), Some(false));
		}
	}
}
