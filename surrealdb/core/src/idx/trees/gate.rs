//! SELECT-permission gating for ANN truthy-document filters.
//!
//! The HNSW and DiskANN filters evaluate a caller-supplied WHERE condition
//! against candidate records inside the search. Each candidate must first pass
//! the table's SELECT permission, otherwise the condition observes records the
//! caller cannot see and the result count, ordering and timing leak their
//! field values.
//!
//! Two executors drive those filters and resolve that permission differently:
//! the legacy path resolves the catalog permission from the transaction and
//! evaluates it through the legacy compute path, while the streaming executor
//! has already resolved a physical permission for the surrounding scan and
//! passes it down. The gate is declared here, at the layer that calls it, so
//! the second case is a [`TableSelectGate`] implemented by the execution layer
//! rather than a dependency on it.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use reblessive::tree::Stk;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Permission, TableDefinition, table_select_permission};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::iam::Action;
use crate::idx::IndexKeyBase;

/// A boxed future returned by [`TableSelectGate::allows_doc`].
///
/// Boxes at the trait boundary because the gate is held as a trait object.
/// `Send` matches the `Send + Sync` bound on [`TableSelectGate`] itself, so a
/// gate check can be awaited from a `Send` future — which the ANN search is,
/// once it leaves the heap-allocated `TreeStack` that erases auto-traits.
pub(crate) type BoxGateFut<'a> = Pin<Box<dyn Future<Output = anyhow::Result<bool>> + Send + 'a>>;

/// A SELECT permission that has already been resolved against an execution
/// context, ready to be checked per candidate document.
///
/// Implemented by the streaming executor, which owns both the resolved
/// permission and the context it evaluates against. Declared here so the ANN
/// filters can apply it without naming the execution layer.
pub(crate) trait TableSelectGate: Send + Sync {
	/// The verdict for every candidate, when the resolved permission grants or
	/// denies the whole table outright. `None` when the permission is a
	/// predicate and each candidate has to go through [`Self::allows_doc`].
	///
	/// The ANN filters consult this first so the common unconditional case
	/// costs neither a boxed future nor a virtual expression evaluation per
	/// candidate.
	fn allows_every_doc(&self) -> Option<bool>;

	/// Returns `true` when `cursor_doc` is visible to the current session.
	fn allows_doc<'a>(&'a self, cursor_doc: &'a CursorDoc) -> BoxGateFut<'a>;
}

/// A table's SELECT permission, pre-resolved once for repeated per-candidate
/// checks on the legacy path. Mirrors the catalog's [`Permission`] shape,
/// owning a clone of the guard expression so the "resolve once per filter,
/// reuse for every candidate" property [`CachedTableSelect`] provides holds
/// without keeping the table definition alive.
#[derive(Clone)]
pub(crate) enum ResolvedTableSelect {
	None,
	Full,
	Specific(crate::expr::Expr),
}

impl ResolvedTableSelect {
	fn resolve(permission: &Permission) -> Self {
		match permission {
			Permission::None => Self::None,
			Permission::Full => Self::Full,
			Permission::Specific(expr) => Self::Specific(expr.clone()),
		}
	}
}

/// Cached resolution of a table's SELECT permission check, for callers that
/// need to evaluate the permission per candidate row. On the legacy path,
/// resolve once per filter via [`resolve_cached_table_select`]; on the
/// streaming path, the scan operator pre-seeds a [`CachedTableSelect::Gate`].
/// Either way, check each candidate via [`check_cached_table_select_for_doc`].
#[derive(Clone)]
pub(crate) enum CachedTableSelect {
	/// Permission checks are bypassed (auth disabled / privileged session).
	Skip,
	/// Permission must be evaluated against each candidate document via the
	/// legacy compute path.
	Apply(ResolvedTableSelect),
	/// Permission was pre-resolved by the streaming executor and is evaluated
	/// through the gate it supplied. The scan operator passes the same
	/// permission that filters the fetched batch after the search, so the
	/// in-search and post-search checks cannot disagree.
	Gate(Arc<dyn TableSelectGate>),
}

/// Evaluate a catalog SELECT [`Permission`] against a [`CursorDoc`] using the
/// legacy compute path. Returns `true` when access is allowed.
///
/// Used by KNN truthy-document filters (HNSW, DiskANN) when the search is
/// driven by the legacy executor, where the table's SELECT permission must be
/// checked per candidate before the caller-supplied WHERE condition runs. The
/// streaming executor supplies a [`CachedTableSelect::Gate`] instead, so this
/// compute path is never reached from a successfully planned query.
///
/// `Specific` expressions are evaluated with permissions disabled so the
/// permission expression itself doesn't recurse into permission checks against
/// its own table.
pub(crate) async fn evaluate_table_select_for_doc(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	resolved: &ResolvedTableSelect,
	cursor_doc: &CursorDoc,
) -> anyhow::Result<bool> {
	match resolved {
		ResolvedTableSelect::None => Ok(false),
		ResolvedTableSelect::Full => Ok(true),
		ResolvedTableSelect::Specific(e) => {
			let opt_no_perms = opt.new_for_permission_predicate();
			Ok(stk
				.run(|stk| {
					crate::legacy::expr_compute(e, stk, ctx, &opt_no_perms, Some(cursor_doc))
				})
				.await
				.catch_return()?
				.is_truthy())
		}
	}
}

/// Resolve a table's SELECT permission for caching across per-row checks in an
/// ANN truthy-doc filter. Returns `Skip` when [`crate::ctx::Context::check_perms`]
/// reports `false`; otherwise returns `Apply(p)` with the table's SELECT
/// permission (or `Permission::None` if the table is missing — which denies
/// access by design).
pub(crate) async fn resolve_cached_table_select(
	ctx: &FrozenContext,
	opt: &Options,
	table_def: Option<&TableDefinition>,
) -> anyhow::Result<CachedTableSelect> {
	if !ctx.check_perms(opt, Action::View)? {
		return Ok(CachedTableSelect::Skip);
	}
	Ok(CachedTableSelect::Apply(ResolvedTableSelect::resolve(table_select_permission(table_def))))
}

/// Check a previously-resolved [`CachedTableSelect`] against a [`CursorDoc`].
/// Companion to [`resolve_cached_table_select`].
pub(crate) async fn check_cached_table_select_for_doc(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	cached: &CachedTableSelect,
	cursor_doc: &CursorDoc,
) -> anyhow::Result<bool> {
	match cached {
		CachedTableSelect::Skip => Ok(true),
		CachedTableSelect::Apply(p) => {
			evaluate_table_select_for_doc(stk, ctx, opt, p, cursor_doc).await
		}
		// The candidate value carries its canonical `id` (spliced in from the
		// storage key on decode), so id-referencing permissions see the same
		// document here as in the post-search batch check.
		CachedTableSelect::Gate(gate) => match gate.allows_every_doc() {
			Some(allowed) => Ok(allowed),
			None => gate.allows_doc(cursor_doc).await,
		},
	}
}

/// Populate `slot` with the table's cached SELECT permission on first call,
/// then return a reference to it. Subsequent calls reuse the cached value
/// without re-fetching the table definition. Shared between the HNSW and
/// DiskANN truthy-doc filters, both of which resolve the permission once per
/// filter and reuse it for every candidate. A slot pre-seeded with a
/// [`CachedTableSelect::Gate`] (streaming executor) is returned as-is without
/// touching the transaction.
pub(crate) async fn ensure_cached_table_select<'a>(
	ctx: &FrozenContext,
	opt: &Options,
	txn: &crate::kvs::Transaction,
	ikb: &IndexKeyBase,
	slot: &'a mut Option<CachedTableSelect>,
) -> anyhow::Result<&'a CachedTableSelect> {
	if slot.is_none() {
		let table = txn.get_tb(ikb.ns(), ikb.db(), ikb.table(), None).await?;
		*slot = Some(resolve_cached_table_select(ctx, opt, table.as_deref()).await?);
	}
	Ok(slot.as_ref().expect("just populated above"))
}
