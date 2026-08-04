//! Permission resolution utilities for the stream executor.
//!
//! This module provides utilities for resolving and checking table/field permissions
//! at execution time. Since SurrealQL allows DDL and DML interleaving within transactions,
//! permissions must be resolved from the current transaction's schema view rather than
//! at planning time.

use std::sync::Arc;

use reblessive::tree::Stk;

use crate::catalog::{Permission, Record};
use crate::err::{EngineError, Error};
use crate::exec::planner::Planner;
use crate::exec::{
	DatabaseContext, Error as ExecError, EvalContext, ExecutionContext, PhysicalExpr,
};
use crate::expr::ControlFlow;
use crate::iam::Action;
use crate::idx::trees::gate::{BoxGateFut, TableSelectGate};
use crate::val::{RecordId, Value};

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

/// Convert a catalog [`Permission`] to a PhysicalPermission via the
/// given planner. Inner subqueries inherit the planner's `CycleGuard`, so a
/// self-referential permission (`WHERE (SELECT FROM same_table) != NONE`)
/// falls back to runtime resolution for that subtree instead of recursing.
pub(crate) async fn convert_permission_to_physical(
	permission: &Permission,
	planner: &Planner<'_>,
) -> Result<PhysicalPermission, Error> {
	match permission {
		Permission::None => Ok(PhysicalPermission::Deny),
		Permission::Full => Ok(PhysicalPermission::Allow),
		Permission::Specific(expr) => {
			let physical_expr = planner.physical_expr(expr.clone()).await?;
			Ok(PhysicalPermission::Conditional(physical_expr))
		}
	}
}

/// Runtime convenience wrapper: build a txn-less planner from `ctx` and
/// convert. Equivalent to today's per-scan permission resolution path —
/// no plan-time index resolution, no cycle guard interaction.
///
/// Cycle safety note: this path is intentionally *txn-less*. The txn-less
/// shim in `expr_to_physical_expr` short-circuits `try_resolve_table_ctx`,
/// so a self-referential permission compiled here (e.g. a cache-miss
/// runtime build for a permission that contains `SELECT FROM same_table`)
/// can't recurse into table-context resolution. Do **not** switch this
/// helper to [`Planner::with_txn`] without re-deriving cycle safety —
/// in particular, runtime callers don't share a parent [`CycleGuard`]
/// the way plan-time nested planners do, so the inner subtree would
/// either need its own guard or a different cycle-break mechanism.
#[inline]
pub(crate) async fn convert_permission_to_physical_runtime(
	permission: &Permission,
	ctx: &ExecutionContext,
) -> Result<PhysicalPermission, Error> {
	convert_permission_to_physical(permission, &Planner::new(ctx.ctx(), ctx.function_registry()))
		.await
}

/// Check if permission should be checked for the given action.
///
/// Returns `true` if permission checks should be performed, `false` if they
/// should be bypassed (e.g., for root/owner users or when auth is disabled).
pub(crate) fn should_check_perms(db_ctx: &DatabaseContext, action: Action) -> Result<bool, Error> {
	let root = &db_ctx.ns_ctx.root;

	// Inside a permission predicate (`skip_fetch_perms`), enforcement is
	// bypassed so the definer-authored predicate can read freely — matching the
	// legacy `Options::new_with_perms(false)` path. This is the single gate
	// every scan, graph and reference operator consults, so exempting it here
	// disables the whole-scan `Deny` short-circuits, per-row table/field
	// permission filtering, and edge/target enforcement in one place.
	if root.skip_fetch_perms {
		return Ok(false);
	}

	// Check if server auth is disabled
	if !root.ctx.auth_enabled() && root.auth.is_anon() {
		return Ok(false);
	}

	let ns = db_ctx.ns_name();
	let db = db_ctx.db_name();

	match action {
		Action::Edit => {
			let allowed = root.auth.has_editor_role();
			let db_in_actor_level =
				root.auth.is_root() || root.auth.is_ns_check(ns) || root.auth.is_db_check(ns, db);
			Ok(!allowed || !db_in_actor_level)
		}
		Action::View => {
			let allowed = root.auth.has_viewer_role();
			let db_in_actor_level =
				root.auth.is_root() || root.auth.is_ns_check(ns) || root.auth.is_db_check(ns, db);
			Ok(!allowed || !db_in_actor_level)
		}
	}
}

/// Validate that a record user has access to the current namespace and database.
///
/// Record users (tokens scoped to a specific record) should only be able to access
/// data within their authenticated namespace and database. This check ensures that
/// a record user cannot access data in other namespaces or databases.
///
/// Returns `Ok(())` if access is allowed, `Err` with an appropriate error if denied.
pub(crate) fn validate_record_user_access(db_ctx: &DatabaseContext) -> Result<(), Error> {
	let root = &db_ctx.ns_ctx.root;

	// Only check for record users
	if !root.auth.is_record() {
		return Ok(());
	}

	let ns = db_ctx.ns_name();
	let db = db_ctx.db_name();

	// Verify namespace matches
	if root.auth.level().ns() != Some(ns) {
		return Err(ExecError::NsNotAllowed {
			ns: ns.into(),
		}
		.into());
	}

	// Verify database matches
	if root.auth.level().db() != Some(db) {
		return Err(ExecError::DbNotAllowed {
			db: db.into(),
		}
		.into());
	}

	Ok(())
}

/// Check a physical permission against a specific record value.
///
/// Returns `true` if access is allowed, `false` if denied.
///
/// `value_param` lets field-level callers bind the `$value` parameter to
/// the field's picked value (matching legacy `pluck.rs` semantics). Pass
/// `None` for table-level checks, where `$value` has no meaning.
pub(crate) async fn check_permission_for_value(
	permission: &PhysicalPermission,
	value: &Value,
	value_param: Option<&Value>,
	ctx: &ExecutionContext,
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

			let bound_ctx;
			let exec_ctx = match value_param {
				Some(v) => {
					bound_ctx = ctx.with_param("value", v.clone());
					&bound_ctx
				}
				None => ctx,
			};
			// Bind the record as both the current value and the document root.
			// The legacy path passes it as the `CursorDoc`
			// (`doc/check.rs::process_permissions`, `doc/reduce.rs`), which is
			// what lets `$parent` inside an idiom-level filter — e.g.
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

/// The streaming executor's SELECT-permission gate for ANN truthy-document
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

impl PhysicalTableSelect {
	pub(crate) fn new(permission: PhysicalPermission, ctx: ExecutionContext) -> Self {
		Self {
			permission,
			ctx,
		}
	}
}

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
		_stk: &'a mut Stk,
		_rid: &'a Arc<RecordId>,
		record: &'a Arc<Record>,
	) -> BoxGateFut<'a> {
		Box::pin(async move {
			check_permission_for_value(&self.permission, &record.data, None, &self.ctx).await
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::exec::operators::test_util::{TestDb, parse_expr, physical_expr, root_ctx, val};
	use crate::iam::{Level, Role};
	use crate::kvs::TransactionType;

	// =========================================================================
	// convert_permission_to_physical
	// =========================================================================

	#[tokio::test]
	async fn catalog_permission_maps_onto_the_three_physical_arms() {
		let ctx = root_ctx();
		let planner = Planner::new(ctx.ctx(), ctx.function_registry());

		let none = convert_permission_to_physical(&Permission::None, &planner).await.unwrap();
		assert!(matches!(none, PhysicalPermission::Deny));

		let full = convert_permission_to_physical(&Permission::Full, &planner).await.unwrap();
		assert!(matches!(full, PhysicalPermission::Allow));

		let specific = Permission::Specific(parse_expr("owner = $auth.id"));
		let conditional = convert_permission_to_physical(&specific, &planner).await.unwrap();
		assert!(matches!(conditional, PhysicalPermission::Conditional(_)));
	}

	#[tokio::test]
	async fn runtime_conversion_matches_the_planner_conversion() {
		let ctx = root_ctx();
		let permission = Permission::Specific(parse_expr("age > 18"));

		let resolved = convert_permission_to_physical_runtime(&permission, &ctx).await.unwrap();
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
	async fn non_record_identities_bypass_the_ns_db_confinement_check() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Read).await;
		assert!(validate_record_user_access(ctx.database().unwrap()).is_ok());
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

		let mut stack = reblessive::tree::TreeStack::new();
		let (yes, no) = stack
			.enter(|stk| async {
				let yes = gate.allows_doc(stk, &rid, &allowed).await.unwrap();
				let no = gate.allows_doc(stk, &rid, &denied).await.unwrap();
				(yes, no)
			})
			.finish()
			.await;
		assert!(yes);
		assert!(!no);
	}
}
