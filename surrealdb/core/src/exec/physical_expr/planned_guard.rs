//! A definition's `PERMISSIONS` predicate compiled with the plan that reads
//! the definition.
//!
//! A user-defined function, a parameter, a model or a module is judged against
//! the clause of the definition the evaluation reads. The node that reads it
//! carries the clause compiled when the node was planned, for the definition
//! the planner read, so judging it compiles nothing while the program runs.
//! The compilation stands for the definition the evaluation reads only where
//! that definition holds the same clause under the same `AUTH LIMIT`; any
//! other is judged through the node's [`CompiledGuard`], compiled for it.
//!
//! The node may be part of code the compile cache serves to every principal
//! of a class, so the predicate is planned with the session's own values
//! shadowed ([`Planner::reading_session_values`]): it reads `$auth`,
//! `$session`, `$token` and `$access` when it is judged.
//!
//! [`CompiledGuard`]: crate::exec::permission::CompiledGuard

use std::sync::Arc;

use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::planner::Planner;
use crate::expr::logical::{LogicalExpr, LogicalPermissionRef};

/// The predicate a node's definition held when the node was planned,
/// compiled; empty where the planner read no definition, the definition holds
/// no predicate, or the predicate did not compile.
pub(crate) struct PlannedGuard<D> {
	planned: Option<Arc<Planned<D>>>,
}

/// A predicate and what it was compiled from.
struct Planned<D> {
	/// The definition the planner read: an evaluation that reads this very
	/// allocation holds the clause without comparing it.
	compiled_from: Arc<D>,
	clause: LogicalExpr,
	limit: Option<crate::catalog::auth::AuthLimit>,
	compiled: Arc<dyn PhysicalExpr>,
	scope: crate::exec::guard_verdicts::GuardScope,
}

/// A predicate a node carries compiled, as an evaluation judges it.
pub(crate) struct Matched<'a> {
	pub(crate) compiled: &'a Arc<dyn PhysicalExpr>,
	/// How often its verdict stands within a statement.
	pub(crate) scope: crate::exec::guard_verdicts::GuardScope,
	/// The identity its verdict is kept under: the compilation, which the
	/// plan holding the node keeps alive for the statement.
	pub(crate) id: u64,
}

impl<D> Clone for PlannedGuard<D> {
	fn clone(&self) -> Self {
		Self {
			planned: self.planned.clone(),
		}
	}
}

impl<D> Default for PlannedGuard<D> {
	fn default() -> Self {
		Self {
			planned: None,
		}
	}
}

impl<D> std::fmt::Debug for PlannedGuard<D> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("PlannedGuard").field("planned", &self.planned.is_some()).finish()
	}
}

impl<D> PlannedGuard<D> {
	/// `permission`, the clause `definition` holds, compiled by `planner` one
	/// re-entry deeper than the node, for the planner's principal capped by
	/// `limit` where the definition carries an `AUTH LIMIT`. Empty where the
	/// clause is not a predicate, the principal's reads are not checked, or
	/// the predicate does not compile.
	pub(crate) async fn compile(
		planner: &Planner<'_>,
		definition: Arc<D>,
		permission: LogicalPermissionRef<'_>,
		limit: Option<&crate::catalog::auth::AuthLimit>,
	) -> Self {
		let LogicalPermissionRef::Specific(clause) = permission else {
			return Self::default();
		};
		let mut guarded =
			planner.reading_session_values().with_depth(planner.current_depth().saturating_add(1));
		if let (Some(limit), Some(auth)) = (limit, planner.auth.as_deref()) {
			let Ok(iam_limit) = crate::iam::AuthLimit::try_from(limit) else {
				return Self::default();
			};
			guarded = guarded.with_auth(Arc::new(auth.new_limited(&iam_limit)));
		}
		if let (Some(ns), Some(db)) = (&guarded.ns, &guarded.db)
			&& !guarded.should_check_perms_for_view(ns, db)
		{
			return Self::default();
		}
		match Box::pin(guarded.physical_expr(clause.clone())).await {
			Ok(compiled) => Self {
				planned: Some(Arc::new(Planned {
					compiled_from: definition,
					clause: clause.clone(),
					limit: limit.cloned(),
					compiled,
					scope: crate::exec::guard_verdicts::GuardScope::of(
						clause,
						planner.function_registry(),
					),
				})),
			},
			Err(_) => Self::default(),
		}
	}

	/// The compiled predicate, where `definition`, the definition an
	/// evaluation read, holds the clause `permission` and the `AUTH LIMIT`
	/// `limit` it was compiled from.
	pub(crate) fn matching(
		&self,
		definition: &Arc<D>,
		permission: LogicalPermissionRef<'_>,
		limit: Option<&crate::catalog::auth::AuthLimit>,
	) -> Option<Matched<'_>> {
		let held = self.planned.as_ref()?;
		let planned = held.as_ref();
		let stands = Arc::ptr_eq(&planned.compiled_from, definition)
			|| matches!(permission, LogicalPermissionRef::Specific(clause)
				if *clause == planned.clause && limit == planned.limit.as_ref());
		stands.then_some(Matched {
			compiled: &planned.compiled,
			scope: planned.scope,
			id: Arc::as_ptr(held) as *const () as usize as u64,
		})
	}
}
