//! Output projection for the document pipeline.
//!
//! The functions here split what used to be a single `output_document!`
//! macro into one async function per `Output` variant plus the SELECT
//! projection and a small dispatcher. Specialising per variant keeps
//! each call site doing only the work it actually needs — `Output::After`
//! never materialises the `initial` view, `Output::Before` never
//! materialises `current`, etc.
//!
//! The variants share `apply_select_field_permissions` for the post-
//! projection field-level permission pass.

use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;

use super::IgnoreError;
use crate::catalog::Permission;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Statement};
use crate::doc::compute::DocKind;
use crate::doc::{CursorDoc, Document};
use crate::exe::FlowResultExt;
use crate::expr::field::Fields;
use crate::expr::part::Part;
use crate::expr::{Idiom, Operation, Output, SelectStatement};
use crate::iam::{Action, AuthLimit};
use crate::val::Value;

impl Document {
	/// Project a SELECT statement's row to its output value.
	///
	/// Computes only the closure of computed fields the projection plus
	/// any field-level `PERMISSIONS FOR select` predicates need, then
	/// reduces, then projects via `stmt.fields.compute`. The
	/// `SELECT VALUE … ORDER BY …` deferral branch is preserved so the
	/// sort comparator can read the ordering keys before the VALUE
	/// projection collapses the row.
	pub(super) async fn output_select(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stmt: &SelectStatement,
		omit: &[Idiom],
	) -> Result<Value, IgnoreError> {
		// Exit early for count and key-only iteration
		if self.is_key_only_iteration() {
			return Ok(self.current.doc.as_ref().clone());
		}
		// Resolve the closure of computed fields the projection touches
		let mut needed_roots = {
			let omit_exprs: Vec<crate::expr::Expr> =
				omit.iter().cloned().map(crate::expr::Expr::Idiom).collect();
			crate::exec::planner::Planner::extract_needed_fields(
				&stmt.fields,
				&omit_exprs,
				stmt.cond.as_ref(),
				stmt.order.as_ref(),
				stmt.group.as_ref(),
				stmt.split.as_ref(),
			)
		};
		// Augment with deps of field-level select permissions
		if let Some(ref mut roots) = needed_roots
			&& self.id.is_some()
			&& ctx.check_perms(opt, Action::View)?
		{
			let table_fields = self.doc_ctx.fd()?;
			let mut opaque = false;
			for fd in table_fields.iter() {
				if let Permission::Specific(ref e) = fd.select_permission {
					let deps = crate::expr::computed_deps::extract_computed_deps(e);
					if !deps.is_complete {
						opaque = true;
						break;
					}
					roots.extend(deps.fields);
				}
			}
			if opaque {
				needed_roots = None;
			}
		}
		// Compute on `self.current` BEFORE reducing
		self.compute_fields(stk, ctx, opt, DocKind::Current, needed_roots.as_ref()).await?;
		// Materialise the reduced view, if required
		let _ = self.reduce_current(stk, ctx, opt).await?;
		// Check if there is a reduced document
		if self.current_reduced.is_some() {
			// Re-evaluate computed fields against the reduced view.
			self.compute_fields(stk, ctx, opt, DocKind::CurrentReduced, needed_roots.as_ref())
				.await?;
			// SECURITY: those fields did not exist when the reduce applied
			// field permissions, and the projection below can rename one,
			// which puts it beyond the reach of the name-keyed pass in
			// `apply_select_field_permissions`.
			self.filter_reduced_computed_fields(stk, ctx, opt).await?;
		}
		// Re-borrow the view we just materialised
		let current: &CursorDoc = self.current_reduced.as_ref().unwrap_or(&self.current);
		// Project the SELECT fields
		let mut out = if stmt.group.is_some()
			|| (stmt.order.is_some() && matches!(stmt.fields, Fields::Value(_)))
		{
			// Defer projection so GROUP BY / ORDER BY can see the row.
			// OMIT is also deferred — for SELECT VALUE + ORDER BY it
			// runs in `Results::project_value` after sorting; without it
			// here the sort comparator would lose the OMITed field
			// before it could read the ordering key.
			let mut doc = current.doc.as_ref().clone();
			// Materialise SELECT VALUE alias for ORDER BY by name
			if stmt.order.is_some()
				&& let Fields::Value(ref sel) = stmt.fields
				&& let Some(ref alias) = sel.alias
				&& alias.len() == 1
				&& let Some(Part::Field(name)) = alias.first()
			{
				let val = stk
					.run(|stk| crate::legacy::expr_compute(&sel.expr, stk, ctx, opt, Some(current)))
					.await
					.catch_return()?;
				if let Value::Object(ref mut obj) = doc {
					obj.insert(name.clone(), val);
				}
			}
			doc
		} else if !omit.is_empty() && matches!(stmt.fields, Fields::Value(_)) {
			// For SELECT VALUE with OMIT (no ORDER BY/GROUP BY), apply
			// OMIT to the document BEFORE the VALUE extraction so an
			// omitted field resolves to NONE in the VALUE result.
			let mut doc = current.doc.as_ref().clone();
			for field in omit {
				crate::legacy::value_del(&mut doc, stk, ctx, opt, field).await?;
			}
			let projection_doc = CursorDoc::new(current.rid.clone(), None, doc);
			crate::legacy::fields_compute(&stmt.fields, stk, ctx, opt, Some(&projection_doc))
				.await?
		} else {
			crate::legacy::fields_compute(&stmt.fields, stk, ctx, opt, Some(current)).await?
		};
		// Apply field-level select permissions to the output
		self.apply_select_field_permissions(stk, ctx, opt, &mut out).await?;
		// Drop omitted fields from the output. Skip when GROUP BY is
		// present (OMIT runs after aggregation) and when SELECT VALUE
		// (OMIT is applied either in the pre-projection branch above or
		// post-sort in `Results::project_value`).
		if stmt.group.is_none() && !matches!(stmt.fields, Fields::Value(_)) {
			for field in omit {
				crate::legacy::value_del(&mut out, stk, ctx, opt, field).await?;
			}
		}
		// Output the document
		Ok(out)
	}

	/// Dispatcher for write statements with an explicit `Output` clause.
	///
	/// `None` represents an unspecified `RETURN` and falls back to the
	/// per-statement default.
	pub(super) async fn output_write(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		output: Option<&Output>,
		statement: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		// Under `OPTION IMPORT` the writer must not surface any per-row
		// output — write statements always resolve to an empty array so
		// imports stay idempotent. Without this, `CREATE`/`UPSERT` under
		// `OPTION IMPORT` would echo the new record back.
		if opt.import {
			return Err(IgnoreError::Ignore);
		}
		// Invalidate any reduced views
		self.current_reduced = None;
		// Process the desired output
		let mut out = match output {
			Some(Output::None) => return Err(IgnoreError::Ignore),
			Some(Output::Null) => Value::Null,
			Some(Output::Diff) => self.output_diff(stk, ctx, opt).await?,
			Some(Output::After) => self.output_after(stk, ctx, opt).await?,
			Some(Output::Before) => self.output_before(stk, ctx, opt).await?,
			Some(Output::Fields(v)) => self.output_fields(stk, ctx, opt, v).await?,
			None => match statement {
				Statement::Create(_)
				| Statement::Upsert(_)
				| Statement::Update(_)
				| Statement::Relate(_)
				| Statement::Insert(_) => self.output_after(stk, ctx, opt).await?,
				_ => return Err(IgnoreError::Ignore),
			},
		};
		// Apply field-level select permissions to the output. `Output::Diff` is
		// excluded because a patch-op list is not a record image: its objects
		// are keyed `op` / `path` / `value`, and a field's name only ever
		// matches one of those by coincidence — a table defining a field named
		// `value` would have the op's payload cut out from under it, leaving a
		// malformed op. Nor could the pass protect anything here, since it keys
		// on field names and an op names its field inside a `path` string. The
		// ops need no pass: `output_diff` diffs the two permission-reduced
		// views, so a field the caller cannot select is absent from both sides
		// and cannot reach a path in the first place.
		if !matches!(output, Some(Output::Diff)) {
			self.apply_select_field_permissions(stk, ctx, opt, &mut out).await?;
		}
		// Output the document
		Ok(out)
	}

	/// `RETURN AFTER` / unspecified default: return the post-mutation
	/// record.
	async fn output_after(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<Value> {
		// Reduce the current document if necessary
		let _ = self.reduce_current(stk, ctx, opt).await?;
		// Populate computed fields on the chosen view
		let kind = if self.current_reduced.is_some() {
			DocKind::CurrentReduced
		} else {
			DocKind::Current
		};
		self.compute_fields_if_present(stk, ctx, opt, kind, None).await?;
		// Re-borrow the view we just materialised
		let current: &CursorDoc = self.current_reduced.as_ref().unwrap_or(&self.current);
		// Output the document
		Ok(current.doc.as_ref().to_owned())
	}

	/// `RETURN BEFORE`: return the pre-mutation record.
	async fn output_before(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<Value> {
		// Materialise the reduced view via the cached helper.
		let _ = self.reduce_initial(stk, ctx, opt).await?;
		// Populate computed fields on the chosen view
		let kind = if self.initial_reduced.is_some() {
			DocKind::InitialReduced
		} else {
			DocKind::Initial
		};
		self.compute_fields_if_present(stk, ctx, opt, kind, None).await?;
		// Re-borrow the view we just materialised
		let initial: &CursorDoc = self.initial_reduced.as_ref().unwrap_or(&self.initial);
		// Output the document
		Ok(initial.doc.as_ref().to_owned())
	}

	/// `RETURN DIFF`: emit a JSON Patch describing the mutation.
	async fn output_diff(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<Value> {
		// Materialise the reduced views via the cached helpers.
		let _ = self.reduce_initial(stk, ctx, opt).await?;
		let _ = self.reduce_current(stk, ctx, opt).await?;
		// Compute every computed field on both sides. A creating statement has
		// no pre-mutation record and a deleting one has no post-mutation record,
		// and that view is left as it is rather than derived from nothing.
		let reduced = self.initial_reduced.is_some();
		let (ki, kc) = if reduced {
			(DocKind::InitialReduced, DocKind::CurrentReduced)
		} else {
			(DocKind::Initial, DocKind::Current)
		};
		self.compute_fields_if_present(stk, ctx, opt, ki, None).await?;
		self.compute_fields_if_present(stk, ctx, opt, kc, None).await?;
		// SECURITY: those fields did not exist when the reduce applied field
		// permissions, and a patch op naming one is beyond the reach of the
		// name-keyed pass in `apply_select_field_permissions`.
		self.filter_reduced_computed_fields(stk, ctx, opt).await?;
		// Re-borrow the views we just materialised
		let initial: &CursorDoc = self.initial_reduced.as_ref().unwrap_or(&self.initial);
		let current: &CursorDoc = self.current_reduced.as_ref().unwrap_or(&self.current);
		// Diff the two views
		let ops = initial.doc.as_ref().diff(current.doc.as_ref());
		Ok(Operation::operations_to_value(ops))
	}

	/// `RETURN <fields>`: compute the user-supplied field list with
	/// `$before` / `$after` bound in the context.
	async fn output_fields(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		fields: &Fields,
	) -> Result<Value> {
		// Materialise the reduced views via the cached helpers.
		let _ = self.reduce_initial(stk, ctx, opt).await?;
		let _ = self.reduce_current(stk, ctx, opt).await?;
		// Compute every computed field on both sides. A creating statement has
		// no pre-mutation record and a deleting one has no post-mutation record,
		// and that view is left as it is rather than derived from nothing.
		let reduced = self.initial_reduced.is_some();
		let (ki, kc) = if reduced {
			(DocKind::InitialReduced, DocKind::CurrentReduced)
		} else {
			(DocKind::Initial, DocKind::Current)
		};
		self.compute_fields_if_present(stk, ctx, opt, ki, None).await?;
		self.compute_fields_if_present(stk, ctx, opt, kc, None).await?;
		// SECURITY: those fields did not exist when the reduce applied field
		// permissions, and this projection can rename one — or read it through
		// `$before` / `$after` — which puts it beyond the reach of the
		// name-keyed pass in `apply_select_field_permissions`.
		self.filter_reduced_computed_fields(stk, ctx, opt).await?;
		// Re-borrow the views we just materialised
		let initial: &CursorDoc = self.initial_reduced.as_ref().unwrap_or(&self.initial);
		let current: &CursorDoc = self.current_reduced.as_ref().unwrap_or(&self.current);
		// Configure the context
		let mut child_ctx = Context::new_child(ctx);
		child_ctx.add_value("after", current.doc.as_arc());
		child_ctx.add_value("before", initial.doc.as_arc());
		let child_ctx = child_ctx.freeze();
		// Output the specified fields
		crate::legacy::fields_compute(fields, stk, &child_ctx, opt, Some(current)).await
	}

	/// Apply each field's `PERMISSIONS FOR select` clause to the
	/// already-projected output value, cutting any fields the viewer
	/// is not allowed to see.
	///
	/// `out` must be an image of the record — a projection of it, or the whole
	/// thing. The pass matches field names against `out`'s own keys, so a value
	/// shaped like anything else is matched against the wrong structure: this
	/// is why `Output::Diff` does not come through here.
	///
	/// Uses `Value::cut` (sync) rather than `Value::del` (async,
	/// idiom-aware): the extra `Part` variants `del` handles
	/// (`Part::Where`, `Part::Value`, `Part::Destructure`) are rejected
	/// at `DEFINE FIELD` time, parameterized indices resolve to literals
	/// before storage, and `each()` has already expanded any wildcards
	/// into concrete paths. All remaining parts are equivalent under
	/// both implementations.
	async fn apply_select_field_permissions(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		out: &mut Value,
	) -> Result<()> {
		// Ensure this is not a temporary document
		if self.id.is_none() {
			return Ok(());
		}
		// Should we run permissions checks?
		if !ctx.check_perms(opt, Action::View)? {
			return Ok(());
		}
		// Apply each field's select permission to the PROJECTED output.
		// `reduce_document` already filtered the stored fields, so this
		// post-projection pass must be IDEMPOTENT. SECURITY: read `each`
		// and the `$value` pick from a stable snapshot of `out` (not the
		// un-reduced `self.current`) so array-element paths stay aligned
		// with the array we cut — otherwise a denied element is removed a
		// second time at a shifted index, dropping a permitted sibling
		// (issue #7356). Cuts accumulate on `out`; the snapshot is cloned
		// lazily so tables with no element-bearing permissions pay nothing.
		let mut snapshot: Option<Value> = None;
		for fd in self.doc_ctx.fd()?.iter() {
			// SECURITY: apply the field's AUTH LIMIT before evaluating
			// PERMISSIONS FOR select so the predicate runs under the
			// definer's downgraded auth, not the caller's. Mirrors the
			// pluck.rs / field.rs paths.
			let opt = opt.limited_by(&AuthLimit::try_from(&fd.auth_limit)?);
			// Process the field permissions
			match &fd.select_permission {
				Permission::Full => (),
				Permission::None => {
					let original = snapshot.get_or_insert_with(|| out.clone());
					// SECURITY: iterate in reverse so `Value::cut`'s `Vec::remove`
					// shift doesn't invalidate the pending array indices (#7356).
					for k in original.each(&fd.name).iter().rev() {
						out.cut(k);
					}
				}
				Permission::Specific(e) => {
					let original = snapshot.get_or_insert_with(|| out.clone());
					// SECURITY: iterate in reverse (see the None arm above, #7356).
					for k in original.each(&fd.name).iter().rev() {
						// Disable permission recursion and block side effects
						let opt = &opt.new_for_permission_predicate();
						// Get the projected value from the snapshot
						let val = Arc::new(original.pick(k));
						// Configure the context
						let mut child_ctx = Context::new_child(ctx);
						child_ctx.add_value("value", val);
						let child_ctx = child_ctx.freeze();
						// Process the PERMISSION clause
						if !stk
							.run(|stk| {
								crate::legacy::expr_compute(
									e,
									stk,
									&child_ctx,
									opt,
									Some(&self.current),
								)
							})
							.await
							.catch_return()?
							.is_truthy()
						{
							out.cut(k);
						}
					}
				}
			}
		}
		Ok(())
	}
}
