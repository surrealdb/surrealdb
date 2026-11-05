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
//! | a table or field `PERMISSIONS FOR select` | [`permission_to_physical`] | a scan enforcing permissions; a `RETURN` ends the guard with its value, a `BREAK` or `CONTINUE` is the loop-barrier error |
//! | a field `COMPUTED` body | `scan::pipeline` | a scan materialising a row; for a checked caller its select guard is judged before the body where [`plan_table_derivations`] says so, and a field that guard denies is never derived |
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

use surrealdb_strand::{Strand, TableName};
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{
	EventDefinition, FieldDefinition, IndexDefinition, KeyStructure, Permission, TableDefinition,
};
use crate::doc::Action as DocAction;
use crate::err::Error;
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
use crate::expr::visit::{Visit as _, Visitor};
use crate::expr::{ControlFlow, Expr, Idiom, Part};
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
		// Only a record carries fields to derive. Where there is none, the
		// guard's own evaluation meets the row as the frame holds it.
		let crate::exec::context::RowAccess::Present(row @ Value::Object(_)) = ctx.document()
		else {
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
			derived.push(if matches!(row, Value::Object(_)) {
				Self::derive(&frame, state, row).await?
			} else {
				row.clone()
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
/// where a `RETURN` ends the guard with its value and a `BREAK` or `CONTINUE`
/// is the loop-barrier error ([`ExecError::InvalidControlFlow`]), as
/// [`crate::exec::scalar_unit::ScalarUnit::predicate`] and
/// [`check_permission_for_value`] both answer.
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
/// and none of the caller's bindings. A stamp that can narrow no caller, such as
/// the root-`Owner` stamp a field with no stored `AUTH LIMIT` decodes to, leaves
/// the authority alone.
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
	/// For each column of `cols` that is a path of plain field names, those
	/// names, so the column is read off the image without running its clause
	/// ([`field_path_value`]).
	paths: Vec<Option<Vec<Strand>>>,
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
	let paths = columns.iter().map(field_path).collect();
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
		paths,
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

	/// The field names of each column, when every column is a path of plain
	/// fields that does not start at the record's `id` and the index has no
	/// guard: the values such an index holds for a record are the record's
	/// fields at those paths, wherever every step is an object holding the next
	/// field ([`field_path_value`]).
	#[cfg_attr(all(target_family = "wasm", target_os = "unknown"), allow(dead_code))]
	pub(crate) fn field_paths(&self) -> Option<Vec<&[Strand]>> {
		if self.guard.is_some() {
			return None;
		}
		self.paths
			.iter()
			.map(|path| path.as_deref().filter(|path| path[0].as_str() != "id"))
			.collect()
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
		let mut base = None;
		let mut out = Vec::with_capacity(self.cols.len());
		for (col, path) in self.cols.iter().zip(&self.paths) {
			if let Some(value) = path.as_deref().and_then(|path| field_path_value(image, path)) {
				out.push(value);
				continue;
			}
			let base = base.get_or_insert_with(|| index_entry_eval(exec, image));
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

/// The field names of `col` when it is nothing but a path of plain fields,
/// such as `status` or `geography.code`.
fn field_path(col: &Idiom) -> Option<Vec<Strand>> {
	col.0
		.iter()
		.map(|part| match part {
			Part::Field(name) => Some(name.clone()),
			Part::All
			| Part::Flatten
			| Part::Last
			| Part::First
			| Part::Where(_)
			| Part::Lookup(_)
			| Part::Value(_)
			| Part::Start(_)
			| Part::Method(..)
			| Part::Destructure(_)
			| Part::Optional
			| Part::Recurse(..)
			| Part::Doc
			| Part::RepeatRecurse => None,
		})
		.collect::<Option<Vec<_>>>()
		.filter(|path| !path.is_empty())
}

/// The value at `path` in `image`, read directly where every step of the path
/// is an object that holds the next field, which is what the column's clause
/// reads there too. `None` everywhere else — a missing field, an array the
/// path fans out over, a record link it would fetch through — leaves the
/// column to its clause.
fn field_path_value(image: &Value, path: &[Strand]) -> Option<Value> {
	let mut current = image;
	for name in path {
		let Value::Object(object) = current else {
			return None;
		};
		current = object.get(name.as_str())?;
	}
	Some(current.clone())
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
/// goes on to fire. The predicate frame would refuse both.
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
/// and database. This is the tenant-boundary gate for the read path.
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
/// reading statement never asked for.
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
					bound_ctx = ctx.with_params([("value", v.clone())]);
					&bound_ctx
				}
				None => ctx,
			};
			// Bind the record as both the current value and the document root.
			// The document root is what lets `$parent` inside an idiom-level filter — e.g.
			// `PERMISSIONS FOR select WHERE acl[WHERE $parent.owner = $auth.id]`
			// — resolve to the row under test. `$parent` is the one reader that
			// does not fall back to `current_value`, so the document root must be bound too.
			let mut eval_ctx = EvalContext::from_exec_ctx(exec_ctx).with_value_and_doc(value);
			eval_ctx.skip_fetch_perms = true;

			// What leaves the guard is its answer: a value, or a `RETURN`,
			// which ends the guard's block with its value. A guard is a loop
			// barrier, so a `BREAK` or `CONTINUE` has no loop to leave and is
			// an error. The error is passed through unchanged: a write conflict stays
			// downcastable for the transactor to retry, and a cancelled query still
			// reports as cancelled.
			match clause.evaluate(eval_ctx).await {
				Ok(value) | Err(ControlFlow::Return(value)) => Ok(value.is_truthy()),
				Err(ControlFlow::Break | ControlFlow::Continue) => {
					Err(anyhow::Error::new(ExecError::InvalidControlFlow))
				}
				Err(ControlFlow::Err(e)) => Err(e),
			}
		}
	}
}

/// Whether `permission`, a field's select guard planned
/// [`Derivation::JudgeFirst`], admits the record `stored`: the row as read
/// from storage, with nothing derived onto it.
///
/// This is [`check_permission_for_value`] with `$value` unbound, which an
/// eligible guard does not read, so the guard is judged in its own frame:
/// privileged, holding none of the caller's bindings, refusing a write, and
/// admitting unconditionally under `skip_fetch_perms`. It is judged once per
/// row per field: a row-independent guard is not hoisted here, and the
/// answer is not cached.
pub(crate) async fn judge_before_derivation(
	permission: &PhysicalPermission,
	stored: &Value,
	ctx: &ExecutionContext,
) -> anyhow::Result<bool> {
	check_permission_for_value(permission, stored, None, ctx).await
}

/// How a field's select guard reads the record, as far as the definition
/// alone can say.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GuardShape {
	/// `PERMISSIONS FOR select FULL`, or a caller whose reads are not
	/// checked: nothing is hidden.
	Full,
	/// `PERMISSIONS FOR select NONE`: nothing is shown.
	Deny,
	/// `WHERE p`, reading exactly `reads` of the record by root field name,
	/// and no `$value`, no parameter but the session's, no subquery and no
	/// call that runs against the record.
	Where {
		reads: HashSet<String>,
	},
	/// `WHERE p` whose reads are not known: the analysis is incomplete
	/// (`$value`, another parameter, a subquery, an opaque call).
	Opaque,
}

/// One `COMPUTED` field of a table, as [`plan_derivations`] needs it.
#[derive(Debug)]
pub(crate) struct ComputedShape<'a> {
	/// The root name other clauses use to read the field.
	pub(crate) name: &'a str,
	/// How the field's select guard reads the record.
	pub(crate) guard: GuardShape,
	/// The fields the body reads by root name, or `None` when its reads are
	/// not known.
	pub(crate) body_reads: Option<&'a [String]>,
}

/// When a `COMPUTED` field is derived relative to its own select guard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Derivation {
	/// Derived before the guard, which then masks the value.
	AsMain,
	/// Never derived for this caller: the guard denies outright.
	Hidden,
	/// The guard is judged first, per row, against the record as stored; the
	/// body runs only when it admits, and the guard is not judged again.
	JudgeFirst,
}

/// The shape of `permission` as a field's select guard for a caller whose
/// reads are `checked`.
///
/// An unchecked caller sees everything, so its guard is [`GuardShape::Full`]
/// whatever the definition says. A conditional guard is [`GuardShape::Where`]
/// only when [`accounted_reads`] accounts for everything it reads; `$value`
/// and every parameter the session does not bind make the analysis
/// incomplete, as does any read of the current row that is not rooted at a
/// field name (`@.c`), while `$auth`, `$access`, `$token` and `$session` do
/// not.
pub(crate) fn guard_shape(permission: &Permission, checked: bool) -> GuardShape {
	if !checked {
		return GuardShape::Full;
	}
	match permission {
		Permission::Full => GuardShape::Full,
		Permission::None => GuardShape::Deny,
		Permission::Specific(expr) => match accounted_reads(expr) {
			Some(reads) => GuardShape::Where {
				reads: reads.into_iter().collect(),
			},
			None => GuardShape::Opaque,
		},
	}
}

/// When each `COMPUTED` field of one table is derived relative to its select
/// guard; one answer per entry of `fields`, in the same order.
///
/// `other_guard_reads` is the union of what every non-computed field's guard
/// reads, or `None` when any such guard is [`GuardShape::Opaque`].
///
/// A field is anything other than [`Derivation::AsMain`] only when no other
/// reader can observe its value and its body reads nothing masked, so that
/// withholding or deferring the derivation leaves every visible answer equal
/// to the one derive-then-mask gives. The one exception is a denied field read
/// only by a body whose reads are not known, which reads it as `NONE`. A
/// [`Derivation::Hidden`] field is never derived, so it reads nothing: only the
/// bodies and guards of the fields that are not hidden count as readers, and a
/// field's own body is never its reader. The answer is the largest set of hidden
/// fields closed under that rule, reached by demoting to `AsMain` until nothing
/// changes:
///
/// 1. a [`GuardShape::Full`] guard hides nothing: `AsMain`;
/// 2. a field that another live computed body names, that any non-computed guard reads, or that
///    another live computed guard reads is `AsMain`, as is every field of a table where a live
///    computed guard's reads or the non-computed guards' reads are not known;
/// 3. a [`GuardShape::Deny`] guard: [`Derivation::Hidden`], whatever its own body reads, since that
///    body never runs;
/// 4. a judged field is `AsMain` when another live body's reads are not known, since that body may
///    read it and must see the value the field holds when its guard admits;
/// 5. a body reading a computed field whose guard is not `Full` would read a masked value:
///    `AsMain`;
/// 6. a [`GuardShape::Where`] guard reading no computed field: [`Derivation::JudgeFirst`], since
///    the stored record decides it;
/// 7. anything else (an opaque guard, or a guard reading a computed field): `AsMain`.
///
/// Reads only the definitions: no data, no context.
pub(crate) fn plan_derivations(
	fields: &[ComputedShape<'_>],
	other_guard_reads: Option<&HashSet<String>>,
) -> Vec<Derivation> {
	let names: HashSet<&str> = fields.iter().map(|f| f.name).collect();
	let mut plan: Vec<Derivation> = fields
		.iter()
		.map(|c| match &c.guard {
			// Full or opaque: derived with the main read.
			GuardShape::Full | GuardShape::Opaque => Derivation::AsMain,
			// Denied: hidden.
			GuardShape::Deny => Derivation::Hidden,
			// Judged first when the guard reads no computed field.
			GuardShape::Where {
				reads,
			} if !reads.iter().any(|r| names.contains(r.as_str())) => Derivation::JudgeFirst,
			// Anything else is derived with the main read.
			GuardShape::Where {
				..
			} => Derivation::AsMain,
		})
		.collect();

	// Every pass only moves a field to `AsMain`, so this ends within
	// `fields.len() + 1` passes.
	loop {
		let live = |j: usize| plan[j] != Derivation::Hidden;
		let guards_unknown = other_guard_reads.is_none();
		let next: Vec<Derivation> = fields
			.iter()
			.enumerate()
			.map(|(i, c)| {
				if plan[i] == Derivation::AsMain {
					return Derivation::AsMain;
				}
				// A guard needs the real value, or its reads cannot be shown not to need it.
				let other_live = |j: usize| j != i && live(j);
				let guard_unknown = fields
					.iter()
					.enumerate()
					.any(|(j, f)| other_live(j) && f.guard == GuardShape::Opaque);
				if guards_unknown
					|| guard_unknown
					|| other_guard_reads.is_some_and(|reads| reads.contains(c.name))
				{
					return Derivation::AsMain;
				}
				// A body whose reads are not known may read a judged field, which then has
				// to hold its value; a denied field is read as `NONE` by such a body.
				if plan[i] == Derivation::JudgeFirst
					&& fields
						.iter()
						.enumerate()
						.any(|(j, f)| other_live(j) && f.body_reads.is_none())
				{
					return Derivation::AsMain;
				}
				let read_by_another = fields.iter().enumerate().any(|(j, other)| {
					other_live(j)
						&& (other.body_reads.is_some_and(|reads| reads.iter().any(|r| r == c.name))
							|| matches!(&other.guard, GuardShape::Where { reads } if reads.contains(c.name)))
				});
				if read_by_another {
					return Derivation::AsMain;
				}
				// A judged body reading a value that is itself masked.
				if plan[i] == Derivation::JudgeFirst {
					let reads_masked = c.body_reads.is_some_and(|body_reads| {
						fields.iter().any(|other| {
							other.guard != GuardShape::Full
								&& body_reads.iter().any(|r| r == other.name)
						})
					});
					if reads_masked {
						return Derivation::AsMain;
					}
				}
				plan[i]
			})
			.collect();
		if next == plan {
			return plan;
		}
		plan = next;
	}
}

/// [`plan_derivations`] over the `COMPUTED` fields of `fields`, the
/// definitions of one table, for a caller whose reads are `checked`.
///
/// One entry per field that has a `COMPUTED` clause, keyed by the field's
/// path as written and in the order of `fields`; a stored field has none.
/// Every derivation site reaches the rule through this function, so a
/// `SELECT *` and a `FETCH` of the same record derive the same fields.
///
/// A field is read by its root name — `a` for `a.b` — both where a body or a
/// guard names it and where a non-computed field's guard does. A computed
/// field's guard is the conjunction of its own and those of every field whose
/// path is a proper prefix of its own (`a` for `a.b`), because masking a field
/// removes everything beneath it: a `NONE` ancestor hides the field, and a
/// conditional ancestor adds its reads to the field's own. Reads only the
/// definitions: no data, no context.
pub(crate) fn plan_table_derivations(
	fields: &[FieldDefinition],
	checked: bool,
) -> Vec<(String, Derivation)> {
	let mut keys: Vec<String> = Vec::new();
	let mut roots: Vec<String> = Vec::new();
	let mut guards: Vec<GuardShape> = Vec::new();
	let mut bodies: Vec<Option<Vec<String>>> = Vec::new();
	// `None` once any non-computed guard is opaque.
	let mut other_reads: Option<HashSet<String>> = Some(HashSet::new());

	for fd in fields {
		let guard = guard_shape(&fd.select_permission, checked);
		match &fd.computed {
			Some(body) => {
				let guard = fields
					.iter()
					.filter(|anc| {
						anc.name.0.len() < fd.name.0.len() && fd.name.0.starts_with(&anc.name.0)
					})
					.fold(guard, |acc, anc| {
						conjoin(acc, guard_shape(&anc.select_permission, checked))
					});
				keys.push(fd.name.to_raw_string());
				roots.push(root_name(&fd.name));
				guards.push(guard);
				bodies.push(accounted_reads(body));
			}
			None => match guard {
				GuardShape::Full | GuardShape::Deny => {}
				GuardShape::Where {
					reads,
				} => {
					if let Some(all) = other_reads.as_mut() {
						all.extend(reads);
					}
				}
				GuardShape::Opaque => other_reads = None,
			},
		}
	}

	let shapes: Vec<ComputedShape<'_>> = roots
		.iter()
		.zip(guards)
		.zip(&bodies)
		.map(|((name, guard), body_reads)| ComputedShape {
			name,
			guard,
			body_reads: body_reads.as_deref(),
		})
		.collect();

	keys.into_iter().zip(plan_derivations(&shapes, other_reads.as_ref())).collect()
}

/// The shape of two guards that must both admit: nothing is shown when either
/// denies, nothing is hidden when both are full, and the reads of two
/// conditional guards are the union of theirs, unknown when either's are.
fn conjoin(a: GuardShape, b: GuardShape) -> GuardShape {
	match (a, b) {
		(GuardShape::Deny, _) | (_, GuardShape::Deny) => GuardShape::Deny,
		(GuardShape::Full, other) | (other, GuardShape::Full) => other,
		(GuardShape::Opaque, _) | (_, GuardShape::Opaque) => GuardShape::Opaque,
		(
			GuardShape::Where {
				mut reads,
			},
			GuardShape::Where {
				reads: more,
			},
		) => {
			reads.extend(more);
			GuardShape::Where {
				reads,
			}
		}
	}
}

/// The fields `expr` reads of the current row by root name, or `None` when
/// that set is not known.
///
/// [`extract_computed_deps`] is the source of the names and of the completeness
/// answer, and it only recognises a read rooted at a field name or at
/// `$this`/`$self`. An idiom headed by any other part that is not a field name
/// reads the row without being counted, and a `@` part restarts at the row; the
/// scan in [`reads_row_unaccounted`] makes the set unknown for each, so a field
/// such a read can reach is never withheld or deferred, whatever completeness
/// the analysis reports.
fn accounted_reads(expr: &Expr) -> Option<Vec<String>> {
	let deps = extract_computed_deps(expr);
	if !deps.is_complete || reads_row_unaccounted(expr) {
		return None;
	}
	Some(deps.fields)
}

/// Whether `expr` holds an idiom that reads the current row through a shape
/// [`extract_computed_deps`] does not list: a `@` part anywhere, or a head that
/// is neither a field name nor a path start (a session parameter or a
/// subexpression, which that analysis already classifies).
fn reads_row_unaccounted(expr: &Expr) -> bool {
	struct Scan {
		found: bool,
	}
	impl Visitor for Scan {
		type Error = std::convert::Infallible;

		fn visit_idiom(&mut self, idiom: &Idiom) -> Result<(), Self::Error> {
			if !matches!(idiom.0.first(), Some(Part::Field(_) | Part::Start(_))) {
				self.found = true;
			}
			for part in &idiom.0 {
				self.visit_part(part)?;
			}
			Ok(())
		}

		fn visit_part(&mut self, part: &Part) -> Result<(), Self::Error> {
			if matches!(part, Part::Doc | Part::Lookup(_)) {
				self.found = true;
			}
			part.visit(self)
		}
	}
	let mut scan = Scan {
		found: false,
	};
	let _ = expr.visit(&mut scan);
	scan.found
}

/// The name other clauses use to read the field at `path`: its first part,
/// as [`extract_computed_deps`] reports reads.
fn root_name(path: &Idiom) -> String {
	match path.0.first() {
		Some(Part::Field(name)) => name.as_str().to_owned(),
		_ => path.to_raw_string(),
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use surrealdb_strand::Strand;

	use super::*;
	use crate::dbs::Session;
	use crate::exec::operators::test_util::{
		TestDb, field_idiom, parse_sql_expr, root_ctx, root_ctx_with_options,
	};
	use crate::iam::{Level, Role};
	use crate::kvs::TransactionType;
	use crate::val::Object;

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
		// than the one it is operating in, as after an RPC `use` switches database,
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

	/// A root owner's context over a database, with the transaction a guard
	/// unit runs in; the datastore is returned to outlive it.
	async fn guard_ctx() -> (TestDb, ExecutionContext) {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		(db, ctx)
	}

	/// The table `SELECT` guard `src`, compiled through the production route,
	/// so the clause is a predicate unit.
	async fn select_guard(src: &str, ctx: &ExecutionContext) -> PhysicalPermission {
		let planner = Planner::new(ctx.ctx(), ctx.function_registry());
		let def = table_with_select(Permission::Specific(parse_sql_expr(src)));
		table_select_permission_physical(Some(&def), &planner).await.expect("the guard compiles")
	}

	/// A `RETURN` ends the guard's block with its value, which is the answer.
	///
	/// The guard is a predicate unit, so the answer is decided first by
	/// `ScalarUnit::evaluate`'s predicate branch and then by
	/// `check_guard_for_value`; both apply the same signal table, and this
	/// passes once both mappings hold.
	#[tokio::test]
	async fn a_return_in_a_guard_is_its_value() {
		let (_db, ctx) = guard_ctx().await;
		let row = Value::None;

		let admits = select_guard("{ RETURN true; false }", &ctx).await;
		let answer = check_permission_for_value(&admits, &row, None, &ctx).await;
		assert!(answer.expect("a RETURN is a value"), "RETURN true admits");

		let denies = select_guard("{ RETURN false; true }", &ctx).await;
		let answer = check_permission_for_value(&denies, &row, None, &ctx).await;
		assert!(!answer.expect("a RETURN is a value"), "RETURN false denies");
	}

	/// A guard is a loop barrier: a `BREAK` reaching its end has no loop to
	/// leave and is the loop-barrier error.
	///
	/// The guard is a predicate unit, so the answer is decided first by
	/// `ScalarUnit::evaluate`'s predicate branch and then by
	/// `check_guard_for_value`; both apply the same signal table, and this
	/// passes once both mappings hold.
	#[tokio::test]
	async fn a_break_in_a_guard_is_the_loop_barrier_error() {
		let (_db, ctx) = guard_ctx().await;
		let guard = select_guard("IF true { BREAK }", &ctx).await;
		let err = check_permission_for_value(&guard, &Value::None, None, &ctx)
			.await
			.expect_err("a BREAK has no loop to leave");
		assert!(
			matches!(err.downcast_ref::<ExecError>(), Some(ExecError::InvalidControlFlow)),
			"expected InvalidControlFlow, got {err:?}"
		);
	}

	/// An error raised in a guard leaves it as that error, not as an internal
	/// failure, so a caller can still tell it apart.
	///
	/// The guard is a predicate unit, so the answer is decided first by
	/// `ScalarUnit::evaluate`'s predicate branch and then by
	/// `check_guard_for_value`; both apply the same signal table, and this
	/// passes once both mappings hold.
	#[tokio::test]
	async fn an_error_in_a_guard_is_carried_through() {
		let (_db, ctx) = guard_ctx().await;
		let guard = select_guard("THROW 'x'", &ctx).await;
		let err = check_permission_for_value(&guard, &Value::None, None, &ctx)
			.await
			.expect_err("a THROW is an error");
		assert!(
			matches!(err.downcast_ref::<ExecError>(), Some(ExecError::Thrown(_))),
			"expected the thrown error, got {err:?}"
		);
		assert!(err.to_string().contains('x'), "the thrown message is kept: {err}");
	}

	// =========================================================================
	// judge_before_derivation
	// =========================================================================

	/// A stored row `{ flag }`.
	fn flag_row(flag: bool) -> Value {
		Value::Object(Object::from(BTreeMap::from([(Strand::from("flag"), Value::Bool(flag))])))
	}

	/// `Deny` refuses and `Allow` admits, whatever the row.
	#[tokio::test]
	async fn the_verdict_of_a_deny_and_an_allow_is_fixed() {
		let ctx = root_ctx();
		let row = flag_row(true);
		let deny = judge_before_derivation(&PhysicalPermission::Deny, &row, &ctx).await;
		assert!(!deny.expect("a Deny is an answer"), "Deny refuses");
		let allow = judge_before_derivation(&PhysicalPermission::Allow, &row, &ctx).await;
		assert!(allow.expect("an Allow is an answer"), "Allow admits");
	}

	/// A conditional guard is judged against the stored record.
	#[tokio::test]
	async fn the_verdict_of_a_where_guard_follows_the_stored_row() {
		let (_db, ctx) = guard_ctx().await;
		let guard = select_guard("flag = true", &ctx).await;
		let admitted = judge_before_derivation(&guard, &flag_row(true), &ctx).await;
		assert!(admitted.expect("the guard answers"), "{{ flag: true }} is admitted");
		let refused = judge_before_derivation(&guard, &flag_row(false), &ctx).await;
		assert!(!refused.expect("the guard answers"), "{{ flag: false }} is refused");
	}

	/// A guard that writes is refused as a write from a permission clause,
	/// not judged: the verdict is judged in the guard's own write-refusing
	/// frame.
	#[tokio::test]
	async fn a_guard_that_writes_is_refused() {
		let db = TestDb::new("DEFINE TABLE x SCHEMALESS;").await;
		let ctx = db.exec_ctx().await;
		let guard = select_guard("(CREATE x:1) != NONE", &ctx).await;
		let err = judge_before_derivation(&guard, &flag_row(true), &ctx)
			.await
			.expect_err("a write from a guard is refused");
		assert!(
			err.to_string()
				.contains("The PERMISSIONS clause must not have effects: `CREATE` writes data"),
			"expected the permission-clause write refusal, got {err:?}"
		);
	}

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
	/// the stamp a definition carries when its definer was unconstrained, and what
	/// a definition with no stored `AUTH LIMIT` decodes to;
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

	/// The field's `AUTH LIMIT` applies the definer's-rights clamp, and a
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
	/// the one clause here that the definer's-rights clamp does not reach.
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
	// guard_shape and plan_derivations
	// =========================================================================

	fn names(items: &[&str]) -> HashSet<String> {
		items.iter().map(|s| (*s).to_owned()).collect()
	}

	fn specific(src: &str) -> Permission {
		Permission::Specific(parse_sql_expr(src))
	}

	fn where_reading(items: &[&str]) -> GuardShape {
		GuardShape::Where {
			reads: names(items),
		}
	}

	/// A computed field for `plan_derivations`.
	fn shape<'a>(
		name: &'a str,
		guard: GuardShape,
		body_reads: Option<&'a [String]>,
	) -> ComputedShape<'a> {
		ComputedShape {
			name,
			guard,
			body_reads,
		}
	}

	#[test]
	fn a_guard_reading_the_row_through_a_doc_part_is_opaque() {
		assert_eq!(guard_shape(&specific("@.c = NONE"), true), GuardShape::Opaque);
		assert_eq!(guard_shape(&specific("owner = $auth.id AND @.c"), true), GuardShape::Opaque);
	}

	#[test]
	fn a_guard_reading_the_row_through_a_lookup_is_opaque() {
		assert_eq!(guard_shape(&specific("->friends"), true), GuardShape::Opaque);
		assert_eq!(guard_shape(&specific("<~doc"), true), GuardShape::Opaque);
	}

	#[test]
	fn a_full_or_unchecked_guard_is_full() {
		assert_eq!(guard_shape(&Permission::Full, true), GuardShape::Full);
		assert_eq!(guard_shape(&Permission::None, false), GuardShape::Full);
		assert_eq!(guard_shape(&specific("$value != NONE"), false), GuardShape::Full);
	}

	#[test]
	fn a_none_guard_is_deny() {
		assert_eq!(guard_shape(&Permission::None, true), GuardShape::Deny);
	}

	#[test]
	fn a_guard_reading_only_the_session_reads_no_field() {
		assert_eq!(guard_shape(&specific("$auth.admin"), true), where_reading(&[]));
	}

	#[test]
	fn a_guard_reading_a_field_names_it() {
		assert_eq!(guard_shape(&specific("owner = $auth.id"), true), where_reading(&["owner"]));
	}

	#[test]
	fn a_guard_reading_value_is_opaque() {
		assert_eq!(guard_shape(&specific("$value != NONE"), true), GuardShape::Opaque);
	}

	#[test]
	fn a_guard_holding_a_subquery_is_opaque() {
		assert_eq!(guard_shape(&specific("(SELECT * FROM other)"), true), GuardShape::Opaque);
	}

	/// A field whose guard hides nothing is derived as before.
	#[test]
	fn a_full_guard_is_derived_as_main() {
		let reads: Vec<String> = Vec::new();
		let fields = [shape("c", GuardShape::Full, Some(&reads))];
		assert_eq!(plan_derivations(&fields, Some(&names(&[]))), [Derivation::AsMain]);
	}

	/// A denied field nobody reads is never derived.
	#[test]
	fn a_denied_field_no_one_reads_is_hidden() {
		let reads: Vec<String> = Vec::new();
		let fields = [shape("c", GuardShape::Deny, Some(&reads))];
		assert_eq!(plan_derivations(&fields, Some(&names(&[]))), [Derivation::Hidden]);
	}

	/// Another computed body reads the denied field, so its value
	/// is needed.
	#[test]
	fn a_denied_field_another_body_reads_is_derived_as_main() {
		let none: Vec<String> = Vec::new();
		let reads_c = vec!["c".to_owned()];
		let fields = [
			shape("c", GuardShape::Deny, Some(&none)),
			shape("d", GuardShape::Full, Some(&reads_c)),
		];
		assert_eq!(
			plan_derivations(&fields, Some(&names(&[]))),
			[Derivation::AsMain, Derivation::AsMain]
		);
	}

	/// Another computed field's guard reads the field.
	#[test]
	fn a_field_another_computed_guard_reads_is_derived_as_main() {
		let none: Vec<String> = Vec::new();
		let fields = [
			shape("c", GuardShape::Deny, Some(&none)),
			shape("d", where_reading(&["c"]), Some(&none)),
		];
		assert_eq!(plan_derivations(&fields, Some(&names(&[])))[0], Derivation::AsMain);
	}

	/// A non-computed field's guard reads the field.
	#[test]
	fn a_field_another_guard_reads_is_derived_as_main() {
		let none: Vec<String> = Vec::new();
		let fields = [shape("c", GuardShape::Deny, Some(&none))];
		assert_eq!(plan_derivations(&fields, Some(&names(&["c"]))), [Derivation::AsMain]);
	}

	/// A non-computed guard whose reads are unknown may read it.
	#[test]
	fn an_opaque_other_guard_makes_every_field_derived_as_main() {
		let none: Vec<String> = Vec::new();
		let fields = [shape("c", GuardShape::Deny, Some(&none))];
		assert_eq!(plan_derivations(&fields, None), [Derivation::AsMain]);
	}

	/// A guard reading `$value` needs the value it judges.
	#[test]
	fn a_guard_reading_value_is_judged_after_derivation() {
		let none: Vec<String> = Vec::new();
		let fields = [shape("c", GuardShape::Opaque, Some(&none))];
		assert_eq!(plan_derivations(&fields, Some(&names(&[]))), [Derivation::AsMain]);
	}

	/// A guard reading a computed field, here its own, needs that
	/// field derived first.
	#[test]
	fn a_guard_reading_a_computed_field_is_judged_after_derivation() {
		let none: Vec<String> = Vec::new();
		let fields = [
			shape("c", where_reading(&["d"]), Some(&none)),
			shape("d", GuardShape::Full, Some(&none)),
		];
		assert_eq!(
			plan_derivations(&fields, Some(&names(&[]))),
			[Derivation::AsMain, Derivation::AsMain]
		);
	}

	/// A judged body reads a computed field that is itself masked.
	#[test]
	fn a_body_reading_a_masked_computed_field_is_derived_as_main() {
		let none: Vec<String> = Vec::new();
		let reads_d = vec!["d".to_owned()];
		let fields = [
			shape("c", where_reading(&["flag"]), Some(&reads_d)),
			shape("d", where_reading(&["flag"]), Some(&none)),
		];
		assert_eq!(
			plan_derivations(&fields, Some(&names(&[]))),
			[Derivation::AsMain, Derivation::AsMain]
		);
	}

	/// A denied field's body never runs, so its reads, known or not,
	/// change nothing.
	#[test]
	fn a_denied_field_with_an_opaque_body_is_hidden() {
		let fields = [shape("c", GuardShape::Deny, None)];
		assert_eq!(plan_derivations(&fields, Some(&names(&[]))), [Derivation::Hidden]);
	}

	/// A body whose reads are unknown is no reader of its own field, and a denied
	/// field it may read is never derived: the body reads it as `NONE`.
	#[test]
	fn a_judged_field_with_an_opaque_body_is_judged_first_beside_denied_fields() {
		let none: Vec<String> = Vec::new();
		let fields = [
			shape("shown", GuardShape::Full, Some(&none)),
			shape("hidden", GuardShape::Deny, Some(&none)),
			shape("admin_only", where_reading(&[]), None),
			shape("bad", GuardShape::Deny, None),
		];
		assert_eq!(
			plan_derivations(&fields, Some(&names(&[]))),
			[Derivation::AsMain, Derivation::Hidden, Derivation::JudgeFirst, Derivation::Hidden]
		);
	}

	/// A body whose reads are unknown may read another judged field, which then
	/// holds its value; the opaque body's own field is not read by itself.
	#[test]
	fn a_judged_field_is_derived_as_main_beside_another_opaque_body() {
		let none: Vec<String> = Vec::new();
		let fields = [
			shape("c", where_reading(&["flag"]), Some(&none)),
			shape("d", where_reading(&["flag"]), None),
		];
		assert_eq!(
			plan_derivations(&fields, Some(&names(&[]))),
			[Derivation::AsMain, Derivation::JudgeFirst]
		);
	}

	/// A hidden field is not a reader, so what its body reads stays
	/// hidden or judged first, however many hidden bodies read one another.
	#[test]
	fn a_hidden_bodys_reads_do_not_count() {
		let none: Vec<String> = Vec::new();
		let reads_d = vec!["d".to_owned()];
		let reads_c = vec!["c".to_owned()];
		let fields = [
			shape("c", GuardShape::Deny, Some(&reads_d)),
			shape("d", GuardShape::Deny, Some(&reads_c)),
			shape("e", where_reading(&["flag"]), Some(&none)),
			shape("f", GuardShape::Deny, Some(&none)),
		];
		assert_eq!(
			plan_derivations(&fields, Some(&names(&[]))),
			[Derivation::Hidden, Derivation::Hidden, Derivation::JudgeFirst, Derivation::Hidden]
		);
	}

	/// A field a live body reads is derived, and so is the field that
	/// body's own reads then reach.
	#[test]
	fn a_field_made_live_by_a_reader_counts_as_a_reader() {
		let none: Vec<String> = Vec::new();
		let reads_c = vec!["c".to_owned()];
		let reads_d = vec!["d".to_owned()];
		let fields = [
			shape("c", GuardShape::Deny, Some(&reads_d)),
			shape("d", GuardShape::Deny, Some(&none)),
			shape("e", GuardShape::Full, Some(&reads_c)),
		];
		assert_eq!(
			plan_derivations(&fields, Some(&names(&[]))),
			[Derivation::AsMain, Derivation::AsMain, Derivation::AsMain]
		);
	}

	/// A guard reading stored fields is decided by the stored
	/// record.
	#[test]
	fn a_guard_reading_stored_fields_is_judged_first() {
		let none: Vec<String> = Vec::new();
		let fields = [shape("c", where_reading(&["flag"]), Some(&none))];
		assert_eq!(plan_derivations(&fields, Some(&names(&[]))), [Derivation::JudgeFirst]);
	}

	/// A guard reading nothing of the record is judged first too.
	#[test]
	fn a_session_only_guard_is_judged_first() {
		let none: Vec<String> = Vec::new();
		let fields = [shape("c", where_reading(&[]), Some(&none))];
		assert_eq!(plan_derivations(&fields, Some(&names(&[]))), [Derivation::JudgeFirst]);
	}

	// =========================================================================
	// plan_table_derivations
	// =========================================================================

	/// A stored field named `path` with the given select guard.
	fn stored_field(path: &str, select: Permission) -> FieldDefinition {
		FieldDefinition {
			name: field_idiom(path),
			select_permission: select,
			..Default::default()
		}
	}

	/// A computed field named `path` with body `body` and the given select
	/// guard.
	fn computed_field(path: &str, body: &str, select: Permission) -> FieldDefinition {
		FieldDefinition {
			name: field_idiom(path),
			computed: Some(parse_sql_expr(body)),
			select_permission: select,
			..Default::default()
		}
	}

	fn derivation_of(plan: &[(String, Derivation)], name: &str) -> Derivation {
		plan.iter().find(|(n, _)| n == name).map(|(_, d)| *d).expect("a planned field")
	}

	#[test]
	fn a_table_plan_covers_the_computed_fields_in_order() {
		let fields = [
			stored_field("flag", Permission::Full),
			computed_field("hidden", "1 + 1", Permission::None),
			computed_field("judged", "flag", specific("flag = true")),
			computed_field("open", "2", Permission::Full),
			computed_field("by_auth", "3", specific("$auth.admin")),
		];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(
			plan,
			[
				("hidden".to_owned(), Derivation::Hidden),
				("judged".to_owned(), Derivation::JudgeFirst),
				("open".to_owned(), Derivation::AsMain),
				("by_auth".to_owned(), Derivation::JudgeFirst),
			]
		);
	}

	#[test]
	fn an_unchecked_table_plan_derives_every_field_as_main() {
		let fields = [
			computed_field("hidden", "1 + 1", Permission::None),
			computed_field("judged", "2", specific("flag = true")),
		];
		let plan = plan_table_derivations(&fields, false);
		assert!(plan.iter().all(|(_, d)| *d == Derivation::AsMain));
	}

	#[test]
	fn a_stored_fields_guard_reading_a_computed_field_makes_it_derived_as_main() {
		let fields = [
			stored_field("note", specific("hidden = 2")),
			computed_field("hidden", "1 + 1", Permission::None),
		];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "hidden"), Derivation::AsMain);
	}

	#[test]
	fn a_nested_stored_fields_guard_contributes_its_root_name() {
		let fields = [
			stored_field("meta.owner", specific("meta.owner = $auth.id")),
			computed_field("meta", "{ owner: 1 }", Permission::None),
		];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "meta"), Derivation::AsMain);
	}

	#[test]
	fn an_opaque_stored_guard_makes_every_computed_field_derived_as_main() {
		let fields = [
			stored_field("note", specific("$value != NONE")),
			computed_field("hidden", "1 + 1", Permission::None),
		];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "hidden"), Derivation::AsMain);
	}

	#[test]
	fn a_computed_body_reading_another_field_is_derived_as_main() {
		let fields = [
			computed_field("hidden", "1 + 1", Permission::None),
			computed_field("shown", "hidden + 1", Permission::Full),
		];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "hidden"), Derivation::AsMain);
	}

	/// A denied field is never derived for a body whose reads are unknown; the
	/// body reads it as `NONE`.
	#[test]
	fn a_visible_computed_body_with_unknown_reads_leaves_a_denied_field_hidden() {
		let fields = [
			computed_field("hidden", "1 + 1", Permission::None),
			computed_field("wide", "(SELECT * FROM other)", Permission::Full),
		];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "hidden"), Derivation::Hidden);
	}

	/// A table with a shown field, a hidden one, one judged by `$auth`, and a
	/// hidden one that calls a user-defined function: the function's reads are
	/// unknown, but its field is never derived, so it blocks nothing.
	#[test]
	fn a_hidden_field_calling_a_function_is_hidden_and_blocks_no_other_field() {
		let fields = [
			computed_field("shown", "sequence::nextval('s')", Permission::Full),
			computed_field("hidden", "sequence::nextval('s')", Permission::None),
			computed_field("admin_only", "sequence::nextval('s')", specific("$auth.admin = true")),
			computed_field("bad", "fn::r()", Permission::None),
		];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "shown"), Derivation::AsMain);
		assert_eq!(derivation_of(&plan, "hidden"), Derivation::Hidden);
		assert_eq!(derivation_of(&plan, "admin_only"), Derivation::JudgeFirst);
		assert_eq!(derivation_of(&plan, "bad"), Derivation::Hidden);
	}

	/// The same table with the function call on a field that is shown: that
	/// body may read a judged field, which is then derived first, and reads a
	/// denied one as `NONE`.
	#[test]
	fn a_shown_field_calling_a_function_derives_a_judged_field_and_hides_a_denied_one() {
		let fields = [
			computed_field("shown", "fn::r()", Permission::Full),
			computed_field("hidden", "1", Permission::None),
			computed_field("admin_only", "2", specific("$auth.admin = true")),
		];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "shown"), Derivation::AsMain);
		assert_eq!(derivation_of(&plan, "hidden"), Derivation::Hidden);
		assert_eq!(derivation_of(&plan, "admin_only"), Derivation::AsMain);
	}

	/// Masking `a` removes everything beneath it, so `a.b` is never derived.
	#[test]
	fn a_computed_field_under_a_denied_ancestor_is_hidden() {
		let fields =
			[stored_field("a", Permission::None), computed_field("a.b", "1 + 1", Permission::Full)];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "a.b"), Derivation::Hidden);
	}

	/// An ancestor's `WHERE` over stored fields decides the field beneath it
	/// together with the field's own.
	#[test]
	fn a_computed_field_under_a_conditional_ancestor_is_judged_first() {
		let fields = [
			stored_field("flag", Permission::Full),
			stored_field("a", specific("flag = true")),
			computed_field("a.b", "1 + 1", Permission::Full),
			computed_field("a.c", "2", specific("$auth.admin")),
		];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "a.b"), Derivation::JudgeFirst);
		assert_eq!(derivation_of(&plan, "a.c"), Derivation::JudgeFirst);
	}

	/// A conditional ancestor reading a computed field must be judged after
	/// that field is derived, and so is a field beneath it.
	#[test]
	fn a_computed_field_under_an_ancestor_reading_a_computed_field_is_derived_as_main() {
		let fields = [
			stored_field("a", specific("d = 1")),
			computed_field("d", "1", Permission::Full),
			computed_field("a.b", "1 + 1", Permission::Full),
		];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "a.b"), Derivation::AsMain);
	}

	/// A sibling or a longer path is not an ancestor.
	#[test]
	fn a_field_that_is_not_a_proper_prefix_does_not_guard_a_computed_field() {
		let fields = [
			stored_field("ab", Permission::None),
			stored_field("a.b.c", Permission::None),
			computed_field("a.b", "1", Permission::Full),
		];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "a.b"), Derivation::AsMain);
	}

	/// An unchecked caller sees every field, ancestors included.
	#[test]
	fn an_unchecked_plan_ignores_an_ancestors_guard() {
		let fields =
			[stored_field("a", Permission::None), computed_field("a.b", "1 + 1", Permission::None)];
		let plan = plan_table_derivations(&fields, false);
		assert_eq!(derivation_of(&plan, "a.b"), Derivation::AsMain);
	}

	#[test]
	fn a_stored_guard_reading_a_computed_field_through_a_doc_part_is_derived_as_main() {
		let fields =
			[stored_field("s", specific("@.c = NONE")), computed_field("c", "1", Permission::None)];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "c"), Derivation::AsMain);
	}

	#[test]
	fn a_computed_body_reading_a_denied_field_through_a_doc_part_reads_it_as_none() {
		let fields = [
			computed_field("c", "1", Permission::None),
			computed_field("d", "@.c + 1", Permission::Full),
		];
		let plan = plan_table_derivations(&fields, true);
		assert_eq!(derivation_of(&plan, "c"), Derivation::Hidden);
		assert_eq!(derivation_of(&plan, "d"), Derivation::AsMain);
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
				nulls_not_distinct: false,
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
