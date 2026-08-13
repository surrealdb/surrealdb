use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;
use tracing::instrument;

use crate::catalog::{FieldDefinition, Permission};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document};
use crate::exe::FlowResultExt as _;
use crate::iam::{Action, AuthLimit};

impl Document {
	/// Checks if reduction is required
	///
	/// This function checks if reduction is required for a document based on the following
	/// criteria:
	/// - The document has an ID and is not a temporary document
	/// - The current actor needs permission to be processed
	#[inline]
	pub(crate) fn reduction_required(&self, ctx: &FrozenContext, opt: &Options) -> Result<bool> {
		// Check if this record exists
		if self.id.is_none() {
			return Ok(false);
		}
		// Are permissions being skipped?
		if !ctx.check_perms(opt, Action::View)? {
			return Ok(false);
		}
		// Reduction is required
		Ok(true)
	}

	/// Reduces `self.current` based on field-level select permissions and
	/// returns a reference to the materialised reduced view. Caches the
	/// result in `self.current_reduced` so repeated calls in the same
	/// pipeline are cheap; callers that mutate `self.current.doc` are
	/// responsible for invalidating the cache (e.g.
	/// `process_record_data` clears it after applying the data clause).
	///
	/// When reduction is not required (owner sessions / perms disabled)
	/// the function short-circuits and returns `&mut self.current` so
	/// callers can use one borrow regardless of whether reduction kicked
	/// in.
	pub(crate) async fn reduce_current(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<&mut CursorDoc> {
		// Check if we need to reduce the document
		if self.reduction_required(ctx, opt)? {
			// Compute the reduced document if not yet done
			if self.current_reduced.is_none() {
				self.current_reduced =
					Some(self.reduce_document(stk, ctx, opt, &self.current).await?);
			}
			// Return the reduced document which was just populated above
			self.current_reduced
				.as_mut()
				.ok_or_else(|| anyhow::anyhow!("current_reduced should be set"))
		} else {
			Ok(&mut self.current)
		}
	}

	/// Reduces `self.initial` based on field-level select permissions and
	/// returns a reference to the materialised reduced view. Mirrors
	/// [`Self::reduce_current`] but for the pre-mutation document — used
	/// by `RETURN BEFORE` / `RETURN DIFF` / `RETURN <fields>` output
	/// paths, and by the LIVE-query DELETE branch where the visible
	/// payload is the pre-delete record.
	///
	/// Caches the result in `self.initial_reduced`. `self.initial` is
	/// immutable after the document is constructed, so callers don't
	/// need to invalidate this cache once it's been populated for the
	/// row.
	pub(crate) async fn reduce_initial(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<&mut CursorDoc> {
		// Check if we need to reduce the document
		if self.reduction_required(ctx, opt)? {
			// Compute the reduced document if not yet done
			if self.initial_reduced.is_none() {
				self.initial_reduced =
					Some(self.reduce_document(stk, ctx, opt, &self.initial).await?);
			}
			// Return the reduced document which was just populated above
			self.initial_reduced
				.as_mut()
				.ok_or_else(|| anyhow::anyhow!("initial_reduced should be set"))
		} else {
			Ok(&mut self.initial)
		}
	}

	/// Reduces an arbitrary [`CursorDoc`] (typically a fresh local — not
	/// one of `self.initial` / `self.current`) and returns the owned
	/// reduced view. When reduction is not required this just clones
	/// `full`, so callers can write
	///
	/// ```ignore
	/// let local = self.reduce_to_owned(stk, ctx, opt, &self.current).await?;
	/// // …mutate `local` without touching the cache…
	/// ```
	///
	/// without having to repeat the `reduction_required` check at every
	/// call site. Used by `lq_compute` (live-query payload projection),
	/// which needs a transient reduced view that does not pollute
	/// `self.{current,initial}_reduced` (the per-subscription doc is
	/// then mutated independently of the cached views).
	pub(crate) async fn reduce_to_owned(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		full: &CursorDoc,
	) -> Result<CursorDoc> {
		if self.reduction_required(ctx, opt)? {
			self.reduce_document(stk, ctx, opt, full).await
		} else {
			Ok(full.clone())
		}
	}

	/// Apply the COMPUTED fields' own `PERMISSIONS FOR select` to every
	/// permission-reduced view this document has materialised.
	///
	/// The reduce kernel cannot do this itself — it runs before any computed
	/// field exists — so every site that populates computed fields on a
	/// reduced view has to call this before handing that view to a
	/// user-supplied expression. Without it the caller reads a value their
	/// permissions deny, and a write clause can copy that value into a field
	/// they are allowed to select.
	///
	/// Unreduced views are left alone: one is only ever handed to a session
	/// that bypasses field-level permissions entirely.
	pub(super) async fn filter_reduced_computed_fields(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<()> {
		// Nothing to filter when neither reduced view was materialised
		if self.current_reduced.is_none() && self.initial_reduced.is_none() {
			return Ok(());
		}
		// Cloning the handle ends the borrow on `self` before the views below
		// are borrowed mutably
		let fields = Arc::clone(self.doc_ctx.fd()?);
		// Each reduced view is filtered against the record it was reduced from,
		// so a permission predicate reads the whole record rather than the view
		// its own clause is about to cut from.
		if let Some(doc) = self.current_reduced.as_mut() {
			Document::filter_computed_field_permissions(stk, ctx, opt, &fields, &self.current, doc)
				.await?;
		}
		if let Some(doc) = self.initial_reduced.as_mut() {
			Document::filter_computed_field_permissions(stk, ctx, opt, &fields, &self.initial, doc)
				.await?;
		}
		Ok(())
	}

	/// Apply `PERMISSIONS FOR select` to the COMPUTED fields of `doc`
	/// after they have been populated by
	/// [`Document::computed_fields_inner`]. The reduce kernel
	/// ([`Self::reduce_document`]) runs *before* computed fields exist,
	/// so without this step a reader without permission to read a computed
	/// field still receives its value: in a LIVE notification (and, for
	/// `StoredSubscriptionFields::Diff`, in the patch ops), in a projection
	/// that renames it, or in whatever a write clause copies it into.
	/// Stored fields are intentionally skipped here — they were already
	/// filtered by `reduce_document`.
	///
	/// `full` is the record `doc` was reduced from, and every `PERMISSIONS FOR
	/// select` predicate is evaluated against it. SECURITY: a predicate must
	/// read the record, not the reduced view — `doc` is missing the fields the
	/// caller may not select, so a clause like `WHERE secret = NONE` would hold
	/// on it for a record whose `secret` is set, and pass a computed field the
	/// caller was meant to be denied. This mirrors `reduce_document`, which
	/// evaluates a stored field's clause against `full` for the same reason.
	/// Only the field's own value and the paths it occupies come from `doc`,
	/// which is the sole view a computed field exists on.
	#[instrument(level = "trace", target = "surrealdb::core::doc::reduce", skip_all)]
	pub(crate) async fn filter_computed_field_permissions(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		fields: &[FieldDefinition],
		full: &CursorDoc,
		doc: &mut CursorDoc,
	) -> Result<()> {
		// If permissions are disabled, nothing to do.
		if !ctx.check_perms(opt, Action::View)? {
			return Ok(());
		}
		// Skip unless some computed field actually restricts reads; avoids the
		// `doc.clone()` and the field-iteration on wide schemas, and on the
		// ones whose computed fields are all readable. A field with no
		// `COMPUTED` clause was already filtered by `reduce_document`.
		if !fields
			.iter()
			.any(|fd| fd.computed.is_some() && !matches!(fd.select_permission, Permission::Full))
		{
			return Ok(());
		}
		// Snapshot once; cuts accumulate on `doc`, but `each` and the `$value`
		// pick both read from the snapshot so later cuts don't perturb earlier
		// evaluations. The predicates themselves read `full`, which no cut here
		// touches.
		let original = doc.clone();
		for fd in fields.iter() {
			// Only filter computed fields here; stored fields were
			// already handled by `reduce_document`.
			if fd.computed.is_none() {
				continue;
			}
			// SECURITY: apply the field's AUTH LIMIT before evaluating
			// PERMISSIONS FOR select so the predicate runs under the
			// definer's downgraded auth, not the caller's.
			let opt = opt.limited_by(&AuthLimit::try_from(&fd.auth_limit)?);
			// SECURITY: `each` yields paths in ascending array-index order, and
			// `Value::cut` removes an element via `Vec::remove`, which shifts
			// every later index down. Cutting in forward order would make each
			// removal invalidate the remaining indices, leaking the odd-indexed
			// elements (issue #7356). Iterate in reverse so a higher index is
			// always removed before any lower index that is still pending.
			// Predicates read `full` and `original`, both immutable here, so
			// evaluation order is irrelevant.
			match &fd.select_permission {
				Permission::Full => (),
				Permission::None => {
					for k in original.doc.as_ref().each(&fd.name).iter().rev() {
						doc.doc.to_mut().cut(k);
					}
				}
				Permission::Specific(e) => {
					for k in original.doc.as_ref().each(&fd.name).iter().rev() {
						// Disable permission recursion and block side effects
						let opt = &opt.new_for_permission_predicate();
						// Get the computed value, which only the reduced view holds
						let val = Arc::new(original.doc.as_ref().pick(k));
						// Configure the context
						let mut child_ctx = Context::new_child(ctx);
						child_ctx.add_value("value", val);
						let child_ctx = child_ctx.freeze();
						// Process the PERMISSION clause against the whole record
						if !stk
							.run(|stk| {
								crate::legacy::expr_compute(e, stk, &child_ctx, opt, Some(full))
							})
							.await
							.catch_return()?
							.is_truthy()
						{
							doc.doc.to_mut().cut(k);
						}
					}
				}
			}
		}
		Ok(())
	}

	/// Reduce-the-fields kernel. Iterates the table's field definitions
	/// and cuts each field from a clone of `full.doc` based on its
	/// `PERMISSIONS FOR select` clause. The four public entry points
	/// ([`Self::reduce_current`], [`Self::reduce_initial`],
	/// [`Self::reduce_to_owned`]) layer caching / source-picking on top.
	async fn reduce_document(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		full: &CursorDoc,
	) -> Result<CursorDoc> {
		// The document to be reduced
		let mut reduced = full.doc.clone();
		// Loop over each field in document
		for fd in self.doc_ctx.fd()?.iter() {
			// SECURITY: apply the field's AUTH LIMIT before evaluating
			// PERMISSIONS FOR select so the predicate runs under the
			// definer's downgraded auth, not the caller's. Mirrors the
			// pluck.rs / field.rs paths.
			let opt = opt.limited_by(&AuthLimit::try_from(&fd.auth_limit)?);
			// Process the field permissions. SECURITY: iterate in reverse —
			// `each` yields ascending array indices and `Value::cut` removes via
			// `Vec::remove` (shifting later indices down), so a forward pass
			// would let each removal invalidate the pending indices and leak the
			// odd-indexed elements (issue #7356). Removing higher indices first
			// keeps the not-yet-processed lower indices valid; predicates read
			// from the immutable `full`, so evaluation order is irrelevant.
			match &fd.select_permission {
				Permission::Full => (),
				Permission::None => {
					for k in reduced.as_ref().each(&fd.name).iter().rev() {
						reduced.to_mut().cut(k);
					}
				}
				Permission::Specific(e) => {
					for k in reduced.as_ref().each(&fd.name).iter().rev() {
						// Disable permission recursion and block side effects
						let opt = &opt.new_for_permission_predicate();
						// Get the initial value
						let val = Arc::new(full.doc.as_ref().pick(k));
						// Configure the context
						let mut ctx = Context::new_child(ctx);
						ctx.add_value("value", val);
						let ctx = ctx.freeze();
						// Process the PERMISSION clause
						if !stk
							.run(|stk| crate::legacy::expr_compute(e, stk, &ctx, opt, Some(full)))
							.await
							.catch_return()?
							.is_truthy()
						{
							reduced.to_mut().cut(k);
						}
					}
				}
			}
		}
		// Ok
		Ok(CursorDoc::new(full.rid.clone(), full.ir.clone(), reduced))
	}
}
