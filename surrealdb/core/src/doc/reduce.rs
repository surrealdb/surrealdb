use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;
use tracing::instrument;

use crate::catalog::Permission;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document};
use crate::expr::FlowResultExt as _;
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

	/// Apply `PERMISSIONS FOR select` to the COMPUTED fields of `doc`
	/// after they have been populated by
	/// [`Document::computed_fields_inner`]. The reduce kernel
	/// ([`Self::reduce_document`]) runs *before* computed fields exist,
	/// so without this step a subscriber without permission to read a
	/// computed field would still receive its value in the LIVE
	/// notification (and, for `SubscriptionFields::Diff`, in the patch
	/// ops). Stored fields are intentionally skipped here — they were
	/// already filtered by `reduce_document`.
	#[instrument(level = "trace", target = "surrealdb::core::doc::reduce", skip_all)]
	pub(crate) async fn filter_computed_field_permissions(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: &mut CursorDoc,
	) -> Result<()> {
		// If permissions are disabled, nothing to do.
		if !ctx.check_perms(opt, Action::View)? {
			return Ok(());
		}
		// Skip when the table has no computed fields at all; avoids the
		// `doc.clone()` and the field-iteration on wide schemas that
		// only declare stored fields.
		if !self.has_computed_fields() {
			return Ok(());
		}
		// Snapshot once; cuts accumulate on `doc`, but `each`, `pick` and
		// the cursor passed to permission predicates all read from the
		// snapshot so later cuts don't perturb earlier evaluations.
		let original = doc.clone();
		for fd in self.doc_ctx.fd()?.iter() {
			// Only filter computed fields here; stored fields were
			// already handled by `reduce_document`.
			if fd.computed.is_none() {
				continue;
			}
			// SECURITY: apply the field's AUTH LIMIT before evaluating
			// PERMISSIONS FOR select so the predicate runs under the
			// definer's downgraded auth, not the caller's.
			let opt = AuthLimit::try_from(&fd.auth_limit)?.limit_opt(opt);
			match &fd.select_permission {
				Permission::Full => (),
				Permission::None => {
					for k in original.doc.as_ref().each(&fd.name).iter() {
						doc.doc.to_mut().cut(k);
					}
				}
				Permission::Specific(e) => {
					for k in original.doc.as_ref().each(&fd.name).iter() {
						// Disable permissions
						let opt = &opt.new_with_perms(false);
						// Get the computed value
						let val = Arc::new(original.doc.as_ref().pick(k));
						// Configure the context
						let mut child_ctx = Context::new_child(ctx);
						child_ctx.add_value("value", val);
						let child_ctx = child_ctx.freeze();
						// Process the PERMISSION clause
						if !stk
							.run(|stk| e.compute(stk, &child_ctx, opt, Some(&original)))
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
			let opt = AuthLimit::try_from(&fd.auth_limit)?.limit_opt(opt);
			// Loop over each field in document
			for k in reduced.as_ref().each(&fd.name).iter() {
				// Process the field permissions
				match &fd.select_permission {
					Permission::Full => (),
					Permission::None => reduced.to_mut().cut(k),
					Permission::Specific(e) => {
						// Disable permissions
						let opt = &opt.new_with_perms(false);
						// Get the initial value
						let val = Arc::new(full.doc.as_ref().pick(k));
						// Configure the context
						let mut ctx = Context::new_child(ctx);
						ctx.add_value("value", val);
						let ctx = ctx.freeze();
						// Process the PERMISSION clause
						if !stk
							.run(|stk| e.compute(stk, &ctx, opt, Some(full)))
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
