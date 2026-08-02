//! The legacy evaluator's per-candidate seams for ANN truthy-document filters.
//!
//! The HNSW and DiskANN filters gate each candidate on the table's SELECT
//! permission and then evaluate the pushed-down WHERE condition, through the
//! two traits declared in [`crate::idx::trees::gate`]. Both implementations
//! live here: each holds the execution environment (`FrozenContext`,
//! `Options`) and evaluates its expression through the legacy compute path, so
//! the index layer names neither.
//!
//! [`LegacyCondition`] serves both executors — the streaming executor pushes
//! its residual WHERE into the ANN search and it is evaluated here too.
//! [`LegacyTableSelect`] serves only the legacy path; the streaming executor
//! supplies its own gate over the permission it resolved for the surrounding
//! scan.

use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, Permission, Record, table_select_permission};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::expr::{Cond, Expr};
use crate::iam::Action;
use crate::idx::trees::KnnCondFilter;
use crate::idx::trees::gate::{BoxGateFut, CachedTableSelect, CandidateCondition, TableSelectGate};
use crate::legacy::expr_compute;
use crate::val::{RecordId, TableName};

/// Wraps a candidate in the [`CursorDoc`] the legacy compute path evaluates
/// against. The record carries its canonical `id`, so an expression that
/// references `id` sees the same document the post-search checks do.
fn candidate_doc(rid: &Arc<RecordId>, record: &Arc<Record>) -> CursorDoc {
	CursorDoc {
		rid: Some(Arc::clone(rid)),
		ir: None,
		doc: Arc::clone(record).into(),
		fields_computed: false,
	}
}

/// A table's SELECT permission, pre-resolved once for repeated per-candidate
/// checks. Mirrors the catalog's [`Permission`] shape, owning a clone of the
/// guard expression so the "resolve once per filter, reuse for every
/// candidate" property holds without keeping the table definition alive.
enum ResolvedTableSelect {
	None,
	Full,
	Specific(Expr),
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

/// The legacy executor's [`TableSelectGate`], holding the environment the
/// table's SELECT permission is evaluated against.
pub(crate) struct LegacyTableSelect<'a> {
	ctx: &'a FrozenContext,
	opt: &'a Options,
	permission: ResolvedTableSelect,
}

impl TableSelectGate for LegacyTableSelect<'_> {
	fn allows_every_doc(&self) -> Option<bool> {
		match self.permission {
			ResolvedTableSelect::None => Some(false),
			ResolvedTableSelect::Full => Some(true),
			ResolvedTableSelect::Specific(_) => None,
		}
	}

	/// `Specific` expressions are evaluated with permissions disabled so the
	/// permission expression itself doesn't recurse into permission checks
	/// against its own table.
	fn allows_doc<'a>(
		&'a self,
		stk: &'a mut Stk,
		rid: &'a Arc<RecordId>,
		record: &'a Arc<Record>,
	) -> BoxGateFut<'a> {
		Box::pin(async move {
			match &self.permission {
				ResolvedTableSelect::None => Ok(false),
				ResolvedTableSelect::Full => Ok(true),
				ResolvedTableSelect::Specific(e) => {
					let cursor_doc = candidate_doc(rid, record);
					let opt_no_perms = self.opt.new_for_permission_predicate();
					Ok(stk
						.run(|stk| expr_compute(e, stk, self.ctx, &opt_no_perms, Some(&cursor_doc)))
						.await
						.catch_return()?
						.is_truthy())
				}
			}
		})
	}
}

/// The [`CandidateCondition`] both executors use: the pushed-down WHERE,
/// evaluated against each admitted candidate through the legacy compute path.
pub(crate) struct LegacyCondition<'a> {
	ctx: &'a FrozenContext,
	opt: &'a Options,
	cond: Arc<Cond>,
}

impl<'a> LegacyCondition<'a> {
	pub(crate) fn new(ctx: &'a FrozenContext, opt: &'a Options, cond: Arc<Cond>) -> Self {
		Self {
			ctx,
			opt,
			cond,
		}
	}
}

impl CandidateCondition for LegacyCondition<'_> {
	fn matches<'a>(
		&'a self,
		stk: &'a mut Stk,
		rid: &'a Arc<RecordId>,
		record: &'a Arc<Record>,
	) -> BoxGateFut<'a> {
		Box::pin(async move {
			let cursor_doc = candidate_doc(rid, record);
			Ok(stk
				.run(|stk| expr_compute(&self.cond.0, stk, self.ctx, self.opt, Some(&cursor_doc)))
				.await
				.catch_return()?
				.is_truthy())
		})
	}
}

/// Builds the filter the legacy planner pushes into an ANN search: the
/// condition to evaluate against each candidate, and the SELECT-permission
/// gate that must admit the candidate before that condition runs.
///
/// The permission is resolved once here and reused for every candidate.
/// A session whose permission checks are bypassed
/// ([`crate::ctx::Context::check_perms`]) gates nothing; otherwise the gate
/// carries the table's SELECT permission, a missing table denying access by
/// design (see [`table_select_permission`]).
pub(crate) async fn knn_cond_filter<'a>(
	ctx: &'a FrozenContext,
	opt: &'a Options,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	cond: Option<Arc<Cond>>,
) -> Result<Option<KnnCondFilter<'a>>> {
	let Some(cond) = cond else {
		return Ok(None);
	};
	let table = ctx.tx().get_tb(ns, db, tb, None).await?;
	let select_gate = if ctx.check_perms(opt, Action::View)? {
		CachedTableSelect::Gate(Arc::new(LegacyTableSelect {
			ctx,
			opt,
			permission: ResolvedTableSelect::resolve(table_select_permission(table.as_deref())),
		}))
	} else {
		CachedTableSelect::Skip
	};
	Ok(Some(KnnCondFilter {
		select_gate,
		cond: Arc::new(LegacyCondition::new(ctx, opt, cond)),
	}))
}
