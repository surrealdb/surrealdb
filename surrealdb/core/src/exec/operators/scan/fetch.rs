//! FieldState-aware batch record resolution for GQL MATCH binding fetches.
//!
//! This is the security foundation for GQL v2 binding rows. Every node /
//! edge binding that names a *fetched* record (Expand targets and edges,
//! EndpointBind nodes, PathExpand intermediate + terminal nodes and edges) is
//! resolved through [`resolve_with_field_state`], guaranteeing that the
//! contents of a binding are exactly what a `SELECT` on that table would
//! return for the same user.
//!
//! Concretely, this applies — in the same order the scan pipeline documents
//! and relies on (`scan/pipeline.rs::filter_and_process_batch`) —
//!
//! 1. **table-level SELECT permission**: a record whose table permission denies the caller is
//!    dropped (returned as `None`), so neither its existence nor its contents leak;
//! 2. **computed fields**: every `DEFINE FIELD ... VALUE ...` computed field is evaluated and
//!    injected, exactly as a projection would see it;
//! 3. **field-level SELECT permissions**: fields the caller cannot read are cut from the object.
//!
//! The caller-supplied WHERE predicate (Expand's `predicate`, etc.) is **not**
//! applied here — it is evaluated by the operator against the assembled binding
//! row, mirroring how the scan pipeline runs the WHERE predicate *after* the
//! permission-reduced document is produced.
//!
//! Do NOT substitute [`crate::exec::operators::scan::common::resolve_record_batch`]
//! for this helper: that path applies only the table-level permission and skips
//! the entire FieldState machinery (computed fields + field-level permissions),
//! which would leak restricted fields into binding rows.

// This FieldState-aware fetch helper is used only by the GQL v2 MATCH
// operators, which are constructed only by the gql-gated planner
// (`Expr::Match` is `#[cfg(feature = "gql")]`), so it is dead code when the
// feature is off — suppress the lint there only, keeping dead-code detection
// active in the default (gql-on) build.
#![cfg_attr(not(feature = "gql"), allow(dead_code))]

use std::collections::HashMap;
use std::sync::Arc;

use super::pipeline::{
	FieldState, build_field_state, compute_fields_for_value, filter_fields_by_permission,
};
use crate::catalog::providers::TableProvider;
use crate::catalog::table_select_permission;
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical_runtime,
	should_check_perms,
};
use crate::exec::{ControlFlowExt, ExecutionContext};
use crate::expr::ControlFlow;
use crate::iam::Action;
use crate::val::{RecordId, TableName, Value};

/// Per-table resolved state shared across batches for a single operator's
/// lifetime.
struct TableFetchState {
	/// Compiled table-level SELECT permission (mirrors the per-table
	/// `perm_cache` in `resolve_record_batch`). `Allow` when permission checks
	/// are bypassed for the session.
	permission: PhysicalPermission,
	/// Computed fields + field-level SELECT permissions for the table.
	field_state: FieldState,
}

/// Cache of per-table fetch state, owned by a MATCH operator and threaded
/// through every [`resolve_with_field_state`] call it makes.
///
/// Resolving a table's SELECT permission and building its [`FieldState`]
/// (computed-field compilation, dependency analysis, field-permission
/// compilation) is expensive; an operator that expands across many edges of
/// the same table would otherwise repeat that work per batch. The cache makes
/// each table's setup happen at most once for the operator's lifetime.
///
/// The cache also records `check_perms` (resolved once from the session) so the
/// per-batch path doesn't re-derive it.
pub(crate) struct FetchFieldStateCache {
	tables: HashMap<TableName, TableFetchState>,
	/// `Some(flag)` once resolved from the session; `None` until the first
	/// call. Resolved lazily so a never-used operator pays nothing.
	check_perms: Option<bool>,
}

impl FetchFieldStateCache {
	/// Create an empty cache. Each operator constructs one and holds it across
	/// all of its batches.
	pub(crate) fn new() -> Self {
		Self {
			tables: HashMap::new(),
			check_perms: None,
		}
	}

	/// Resolve (once) whether SELECT permissions must be enforced for this
	/// session. Bypassed for root/owner users, when auth is disabled, and
	/// inside permission-predicate evaluation (`skip_fetch_perms`).
	///
	/// Exposed so callers that bind an edge *id-only* (no record fetch) can
	/// decide whether they must still gate traversal on the edge table's SELECT
	/// permission — see `Expand`'s id-only edge perm-gate.
	pub(crate) fn check_perms(&mut self, ctx: &ExecutionContext) -> Result<bool, ControlFlow> {
		self.resolve_check_perms(ctx)
	}

	fn resolve_check_perms(&mut self, ctx: &ExecutionContext) -> Result<bool, ControlFlow> {
		if let Some(flag) = self.check_perms {
			return Ok(flag);
		}
		let db_ctx = ctx.database().context("MATCH binding fetch requires database context")?;
		// `should_check_perms` yields `crate::err::Error`; lift it into the
		// `ControlFlow::Err(anyhow::Error)` channel the rest of this path uses.
		let flag = should_check_perms(db_ctx, Action::View)
			.map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?;
		self.check_perms = Some(flag);
		Ok(flag)
	}

	/// Get or build the per-table fetch state, resolving the table-level
	/// permission and FieldState on first use of `table`.
	async fn state_for<'a>(
		&'a mut self,
		ctx: &ExecutionContext,
		table: &TableName,
		check_perms: bool,
	) -> Result<&'a TableFetchState, ControlFlow> {
		if !self.tables.contains_key(table) {
			let permission = if check_perms {
				let db_ctx =
					ctx.database().context("MATCH binding fetch requires database context")?;
				let version = ctx.version_stamp();
				let table_def = db_ctx
					.get_table_def(table, version)
					.await
					.context("Failed to get table definition")?;
				let catalog_perm = table_select_permission(table_def.as_deref());
				convert_permission_to_physical_runtime(catalog_perm, ctx.ctx())
					.await
					.context("Failed to convert permission")?
			} else {
				// Permission enforcement is bypassed for this session; treat
				// the table as unconditionally allowed and skip field-level
				// filtering (build_field_state with `check_perms = false`
				// produces no field permissions).
				PhysicalPermission::Allow
			};

			// Build the full FieldState (computed fields always; field-level
			// permissions only when `check_perms`). `build_field_state` keeps
			// its own per-`(table, check_perms)` cache on the DatabaseContext,
			// but we additionally hold it here so the per-batch hot path does
			// no async cache lookup once warmed.
			let field_state = build_field_state(ctx, table, check_perms, None).await?;

			self.tables.insert(
				table.clone(),
				TableFetchState {
					permission,
					field_state,
				},
			);
		}
		// `contains_key`/`insert` above guarantee presence.
		Ok(self.tables.get(table).expect("table state just inserted"))
	}
}

impl Default for FetchFieldStateCache {
	fn default() -> Self {
		Self::new()
	}
}

/// Resolve a batch of [`RecordId`]s into binding values, applying the full
/// SELECT-equivalent processing (table permission, computed fields, field
/// permissions) per record.
///
/// The output is **positional**: `out[i]` corresponds to `rids[i]`. An entry is
/// `None` when the record is missing or when the caller is not permitted to
/// SELECT it (table-level deny) — the caller drops such candidates. This
/// differs from
/// [`resolve_record_batch`](crate::exec::operators::scan::common::resolve_record_batch),
/// which collapses dropped records out of the result; binding fetches need the
/// position preserved so the candidate row can be discarded by the operator.
///
/// `cache` is owned by the calling operator and reused across batches so that
/// each distinct table's permission + FieldState are resolved at most once.
pub(crate) async fn resolve_with_field_state(
	ctx: &ExecutionContext,
	cache: &mut FetchFieldStateCache,
	rids: &[RecordId],
) -> Result<Vec<Option<Value>>, ControlFlow> {
	if rids.is_empty() {
		return Ok(Vec::new());
	}

	let check_perms = cache.resolve_check_perms(ctx)?;
	let version = ctx.version_stamp();

	// Pre-warm the per-table state for every distinct table in the batch so
	// the mutable-borrow loop below can take shared references. Resolving here
	// (rather than inside the record loop) keeps the borrow checker happy: the
	// record loop only ever reads from `cache`.
	for rid in rids {
		if !cache.tables.contains_key(&rid.table) {
			cache.state_for(ctx, &rid.table, check_perms).await?;
		}
	}

	let db_ctx = ctx.database().context("MATCH binding fetch requires database context")?;
	let txn = ctx.txn();
	let ns_id = db_ctx.ns_ctx.ns.namespace_id;
	let db_id = db_ctx.db.database_id;

	// One batched multi-get for the whole batch; `get_records` returns records
	// positionally aligned with `rids` and splices the canonical id in.
	let records = txn
		.get_records(ns_id, db_id, rids, version, crate::kvs::CachePolicy::ReadWrite)
		.await
		.context("Failed to fetch records")?;

	let mut out: Vec<Option<Value>> = Vec::with_capacity(rids.len());
	for (rid, record) in rids.iter().zip(records) {
		// Missing records cannot disclose information.
		if record.data.is_none() {
			out.push(None);
			continue;
		}

		let table_state = cache
			.tables
			.get(&rid.table)
			.expect("table state pre-warmed above for every batch table");

		// 1. Table-level SELECT permission (mirrors resolve_record_batch). A denied record is
		//    dropped so neither existence nor contents leak.
		if check_perms {
			let allowed =
				check_permission_for_value(&table_state.permission, &record.data, None, ctx)
					.await
					.context("Failed to check table permission")?;
			if !allowed {
				out.push(None);
				continue;
			}
		}

		// Move the data out of the Arc when we hold the only reference,
		// otherwise clone (same trick as resolve_record_batch).
		let mut value = match Arc::try_unwrap(record) {
			Ok(rec) => rec.data,
			Err(arc) => arc.data.clone(),
		};

		// 2. Computed fields: evaluated and injected before field permissions, exactly as
		//    filter_and_process_batch orders it.
		compute_fields_for_value(ctx, &table_state.field_state, &mut value, false).await?;

		// 3. Field-level SELECT permissions: cut fields the caller cannot read.
		if check_perms {
			filter_fields_by_permission(ctx, &table_state.field_state, &mut value).await?;
		}

		out.push(Some(value));
	}

	Ok(out)
}

#[cfg(test)]
mod tests {
	use super::*;

	// The cache is a plain owned struct; verify it constructs and resolves
	// `check_perms` lazily without a datastore for the trivial paths. The
	// full permission/computed-field behavior is exercised by the language
	// tests over the streaming engine (graph/lookup + gql corpora).

	#[test]
	fn empty_cache_starts_uninitialised() {
		let cache = FetchFieldStateCache::new();
		assert!(cache.tables.is_empty());
		assert!(cache.check_perms.is_none());
	}

	#[test]
	fn default_matches_new() {
		let a = FetchFieldStateCache::default();
		assert!(a.tables.is_empty());
		assert!(a.check_perms.is_none());
	}
}
