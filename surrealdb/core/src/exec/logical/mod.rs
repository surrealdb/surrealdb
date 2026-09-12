//! Binding: from the unbound logical plan to the plan of one transaction.
//!
//! `surrealdb_expr::expr::logical::lower` produces an *unbound* plan: pure,
//! schema-free, identical for every caller, and so cacheable by statement
//! text. [`bind`] turns it into a *bound* plan by running the passes that need
//! the caller's context and the transaction's snapshot of the catalog. Both are
//! the same `LogicalOp` type; the difference is what has been resolved, and a
//! bound plan is valid only for the transaction and the principal it was bound
//! for. It is never a cache key.
//!
//! A closure body is the one part of a plan that binding leaves as lowered.
//! The closure is a value, invoked by later statements each in a transaction
//! of its own, so nothing of the creating statement's catalog or principal may
//! be resolved into it: its sources keep no schema, its parameters stay
//! parameters, its calls stay calls, and no rule rewrites it. The physical
//! planner compiles it without a transaction, and it resolves tables,
//! permissions, indexes, functions and parameters where it runs.
//!
//! Every pass is a synchronous rewrite. A pass that needs the catalog gathers
//! what it needs with a read-only walk, awaits the lookups, then rewrites with
//! the results in hand, so no rewrite closure is ever async.
//!
//! Pass order, first to last:
//!
//! 1. [`resolve`]: each table source gets its schema, and the table's `PERMISSIONS FOR select`,
//!    `COMPUTED` fields and field permissions become a `PermissionFilter` and a stack of projection
//!    layers above it. Everything after this pass can see what the catalog declares.
//! 2. [`params`]: statement-scoped `$param`s become literals, so every later pass sees a literal
//!    where it needs one.
//! 3. [`fold`]: row-independent subexpressions become literals, which needs the parameters in place
//!    first and the resolved schema to know which field reads may be discarded.
//! 4. [`optimise`]: the rewrite rules of [`rules`], applied at every node to a fixpoint — filters
//!    merge and move through projections, beneath layers and around joins; a projection that does
//!    nothing and a layer column nothing reads go; a filter that admits every row goes; an `IN` /
//!    `CONTAINS` subquery becomes a semi or anti join; a call to a `DEFINE FUNCTION` whose body is
//!    one expression becomes that expression; a cast to a kind an expression already has goes. The
//!    functions the last of those may inline are gathered first ([`functions`]), through the node's
//!    function cache, so that the rules themselves consult nothing but the plan.
//!
//! # Catalog access
//!
//! [`resolve`] is the one pass that reads the catalog. A current-version table
//! comes from the node-wide catalog snapshot
//! ([`crate::kvs::cache::ds::CatalogSnapshot`]), one entry per table, which
//! one read of that table's catalog version through the transaction
//! validates: an entry another transaction built under the same version of
//! the table is reused as it stands, a DDL on another table leaves it in
//! place, and a transaction's own uncommitted DDL, which mints a version only
//! it can read, misses the shared entry and loads through the transaction. A
//! table no catalog write has stamped is shared under the content hash of its
//! definition. A `VERSION`ed read goes to the transaction. The contract in
//! full is in the module documentation of [`resolve`].
//!
//! # Traversal
//!
//! A plan holds plans: a `LogicalExpr::Subquery` in any scalar position carries
//! a `LogicalOp` of its own, and the per-node traversals — `LogicalOp::children`
//! and `LogicalOp::visit_scalars` — stop at it. The passes here treat such a
//! plan as part of the statement, since it runs in the same transaction as the
//! same principal, so they reach it with [`visit_deep`] reading and
//! [`rewrite_deep`] rewriting. [`visit_deep`] is the infallible adapter over
//! [`LogicalOp::visit_ops_deep`], which holds the read half of this walk and
//! its closure rule; [`rewrite_deep`] keeps its own descent, over
//! [`visit_binding_mut`]. A scalar of one node is walked by [`visit_binding`]
//! and [`visit_binding_mut`], which stop at a closure body: a closure is bound
//! where it is invoked, not where it is written, so a plan inside one is not a
//! plan of this statement. A `LogicalOp::Match` is a leaf to the operator
//! walks — its stages are a binding table rather than rows — but its scalars
//! are reached like any other node's, so a plan one of them holds is walked.
//!
//! # No cycle guard
//!
//! The physical planner breaks compile-time cycles — a permission on `t` whose
//! subquery reads `t` — with a guard keyed on the table. The resolve pass has
//! no such guard because it never re-enters a table: it expands a table's
//! bodies once, in a scope that records the tables being expanded, and a
//! source of one of those tables inside a body is left unresolved (`schema:
//! None`), which the physical planner compiles with runtime enforcement.
//! Expansion is therefore bounded by the number of tables in the database, and
//! the cycle never forms.

// Adding a `LogicalOp` variant must break every pass that has not stated its
// position on it, which a wildcard arm would silently absorb. The rewrite
// rules are the exception, and say so at each site: a rule reaches only the
// kinds it declares and declines by leaving the node as it is, so a variant
// none of them names is left alone rather than mishandled.
#![warn(clippy::wildcard_enum_match_arm)]

mod effects;
mod fold;
mod functions;
mod optimise;
mod params;
pub(crate) mod reads;
mod resolve;
mod rules;

use std::convert::Infallible;
use std::sync::Arc;

pub(crate) use rules::{and, conjuncts, is_effect_free, is_false, plan_may_raise};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::function::FunctionRegistry;
use crate::expr::With;
use crate::expr::logical::{
	LogicalEval, LogicalExpr, LogicalOp, LogicalSource, LogicalValues, RelationAlias, Schema,
	SourceRef, is_value_literal,
};
use crate::iam::Auth;
use crate::kvs::Transaction;
use crate::val::Value;

/// What binding a plan needs from the statement's surroundings.
pub(crate) struct BindContext<'a> {
	/// The statement's context: `LET` bindings, client bind parameters,
	/// session values, capabilities.
	pub ctx: &'a FrozenContext,
	/// The transaction whose catalog snapshot the plan is bound against.
	/// `None` leaves every catalog-dependent resolution to runtime.
	pub txn: Option<Arc<Transaction>>,
	/// The namespace and database the statement runs in, when known. `None`
	/// leaves catalog-dependent resolution to runtime.
	pub ns_db: Option<(NamespaceId, DatabaseId)>,
	/// The principal the statement runs as. `None` means the principal is not
	/// known at bind time, and the resolve pass then inserts nothing: the
	/// physical planner keeps runtime enforcement for every source whose
	/// schema is unresolved.
	pub auth: Option<Arc<Auth>>,
	/// Parameter names the statement binds per row (`$this`, `$parent`, …),
	/// which are never substituted.
	pub row_scoped: &'a [&'a str],
	/// Decides which calls fold and which name a projection function.
	pub registry: &'a FunctionRegistry,
}

/// Whether some name of `names` is read in `op`, or a plan nested in it,
/// where binding substitutes a statement-scoped value — a predicate, a join
/// key or residual, a source expression — and is not a name the plan binds
/// itself. A plan bound with such a name left as a parameter carries a name
/// where the access path needs a value.
pub(crate) fn reads_in_resolvable_position(op: &LogicalOp, names: &[&str]) -> bool {
	params::reads_in_resolvable_position(op, names)
}

/// Bind `op` for one transaction and one principal, every closure body in it
/// excepted. See the module documentation for the pass order.
pub(crate) async fn bind(op: LogicalOp, bx: &BindContext<'_>) -> Result<LogicalOp, Error> {
	let caller = Caller::new(bx);
	let op = resolve::resolve(op, bx, caller.as_ref()).await?;
	let op = params::resolve_params(op, bx).await;
	let capabilities = bx.ctx.get_capabilities();
	let checks = caller.as_ref().is_none_or(Caller::checks_permissions);
	let op = fold::fold_constants(op, bx.registry, &capabilities, checks);
	let functions = functions::Functions::gather(&op, bx, caller.as_ref()).await;
	let permission_checked_params = params::permission_checked_names(&op, bx).await;
	let mut cx = rules::RuleContext {
		row_scoped: bx.row_scoped,
		registry: bx.registry,
		functions: &functions,
		permission_checked_params: &permission_checked_params,
		next_alias: 0,
		capabilities: &capabilities,
	};
	Ok(optimise::optimise(op, &mut cx))
}

/// The principal a statement runs as, and what the runtime's permission gates
/// decide for it.
///
/// Applies `exec::permission::action_is_checked` for `Action::View` — the
/// rule behind the runtime's `should_check_perms` — and mirrors
/// `exec::permission::validate_record_user_access`: the analyzer inserts a
/// check exactly where the scan operators would apply one, and refuses a
/// principal exactly where they would.
pub(super) struct Caller {
	auth: Arc<Auth>,
	auth_enabled: bool,
	/// The namespace and database the session is using, by name. The actor
	/// level of a namespace or database user is compared against these.
	ns: Option<String>,
	db: Option<String>,
}

impl Caller {
	/// `None` when the principal is not known, in which case nothing may be
	/// decided about permissions at bind time.
	fn new(bx: &BindContext<'_>) -> Option<Self> {
		let auth = bx.auth.clone()?;
		let (ns, db) = session_ns_db(bx.ctx);
		Some(Self {
			auth,
			auth_enabled: bx.ctx.auth_enabled(),
			ns,
			db,
		})
	}

	pub(super) fn auth(&self) -> &Arc<Auth> {
		&self.auth
	}

	/// Whether reads by this principal are permission-checked, as
	/// `should_check_perms(View)` decides: never when auth is disabled and the
	/// session is anonymous; otherwise only a viewer whose actor level covers
	/// the session's namespace and database is exempt.
	pub(super) fn checks_permissions(&self) -> bool {
		self.checks_permissions_as(&self.auth)
	}

	/// [`Self::checks_permissions`] for a principal derived from this one — a
	/// `COMPUTED` body narrowed by its `AUTH LIMIT`.
	pub(super) fn checks_permissions_as(&self, auth: &Auth) -> bool {
		crate::exec::permission::action_is_checked(
			self.auth_enabled,
			auth,
			crate::iam::Action::View,
			self.ns.as_deref(),
			self.db.as_deref(),
		)
	}

	/// Refuses a namespace or database user reading outside its own scope, as
	/// `validate_record_user_access` does before every table read.
	pub(super) fn validate_access(&self) -> Result<(), Error> {
		use crate::exec::Error as ExecError;
		let (Some(ns), Some(db)) = (&self.ns, &self.db) else {
			return Ok(());
		};
		if self.auth.can_access_ns_db(ns, db) {
			return Ok(());
		}
		if self.auth.level().ns() == Some(ns.as_str()) {
			return Err(ExecError::DbNotAllowed {
				db: db.clone(),
			}
			.into());
		}
		Err(ExecError::NsNotAllowed {
			ns: ns.clone(),
		}
		.into())
	}
}

/// The namespace and database the session is using, when it named them.
fn session_ns_db(ctx: &FrozenContext) -> (Option<String>, Option<String>) {
	let read = |key: &str| {
		ctx.value("session").and_then(|v| v.as_object()).and_then(|o| o.get(key)).and_then(|v| {
			if let Value::String(s) = v {
				Some(s.as_str().to_owned())
			} else {
				None
			}
		})
	};
	(read("ns"), read("db"))
}

/// Rewrites every node of the plan bottom-up, including the nodes of every
/// plan nested in a scalar subquery at any depth. `f` sees each node after its
/// inputs and after the scalars it owns have had their subqueries rewritten.
/// A plan inside a closure body is not reached ([`visit_binding_mut`]).
fn rewrite_deep<F: FnMut(LogicalOp) -> LogicalOp>(op: LogicalOp, f: &mut F) -> LogicalOp {
	let Ok(op) = op.transform_up(&mut |mut node| {
		let Ok(()) = node.visit_scalars_mut(&mut |scalar| {
			visit_binding_mut(scalar, &mut |expr| {
				if let LogicalExpr::Subquery(plan) = expr {
					let taken = std::mem::replace(
						plan.as_mut(),
						LogicalOp::Eval(LogicalEval {
							expr: LogicalExpr::Break,
						}),
					);
					**plan = rewrite_deep(taken, f);
				}
				Ok::<_, Infallible>(())
			})
		});
		Ok::<_, Infallible>(f(node))
	});
	op
}

/// Walks a scalar in pre-order for a bind pass: `expr` and every expression
/// nested in it, stopping at the first `Break`, except that a closure body is
/// not entered — the closure literal itself is visited, what it holds is not.
/// A closure body is bound by nothing here: it is compiled without a catalog
/// and resolves its sources, parameters and calls where the closure is
/// invoked, in the invoking statement's transaction (see the module
/// documentation). A nested plan is not entered either; the passes reach
/// those through [`rewrite_deep`] and [`visit_deep`].
pub(super) fn visit_binding<B>(
	expr: &LogicalExpr,
	f: &mut impl FnMut(&LogicalExpr) -> std::ops::ControlFlow<B>,
) -> std::ops::ControlFlow<B> {
	// The recursion is over `dyn FnMut` rather than the caller's closure type:
	// a generic function that passed a fresh closure to itself at every level
	// would have no monomorphic form.
	fn walk<B>(
		expr: &LogicalExpr,
		f: &mut dyn FnMut(&LogicalExpr) -> std::ops::ControlFlow<B>,
	) -> std::ops::ControlFlow<B> {
		f(expr)?;
		if matches!(expr, LogicalExpr::Closure(_)) {
			return std::ops::ControlFlow::Continue(());
		}
		let mut flow = std::ops::ControlFlow::Continue(());
		expr.for_each_child(&mut |child| {
			if flow.is_continue() {
				flow = walk(child, &mut *f);
			}
		});
		flow
	}
	walk(expr, f)
}

/// [`visit_binding`], rewriting in place bottom-up: every nested expression
/// is rewritten before `f` sees the expression holding it, and a closure body
/// is left as written.
pub(super) fn visit_binding_mut<E>(
	expr: &mut LogicalExpr,
	f: &mut impl FnMut(&mut LogicalExpr) -> Result<(), E>,
) -> Result<(), E> {
	if !matches!(expr, LogicalExpr::Closure(_)) {
		for child in expr.children_mut() {
			visit_binding_mut(child, f)?;
		}
	}
	f(expr)
}

/// One index past the highest index any name of the minted form carries in
/// `op` — the alias of a join, and any field a scalar reads or names — so a
/// pass adding joins continues the plan's numbering and mints no alias the
/// plan already names.
///
/// A join names the matched right row on the joined row, and its residual is
/// judged over that row, so an alias equal to a field the plan reads would be
/// read there as the right row rather than as the field. A field named
/// `_join0` reached through `$parent` is exactly that: the decorrelation rule
/// rewrites `$parent._join0` to the field `_join0` of the left row.
fn next_alias_index(op: &LogicalOp) -> usize {
	let mut next = 0;
	let mut reserve = |index: Option<usize>| {
		if let Some(index) = index {
			next = next.max(index + 1);
		}
	};
	visit_deep(op, &mut |node| {
		if let LogicalOp::Join(join) = node {
			reserve(join.alias.index());
		}
		// Every scalar, closure bodies included: a name the plan reads
		// anywhere is a name the joined row must not shadow.
		let Ok(()) = node.visit_scalars(&mut |scalar| {
			let _ = scalar.visit(&mut |expr| {
				if let LogicalExpr::Field(name) = expr {
					reserve(RelationAlias::index_of(name.as_str()));
				} else if let LogicalExpr::Access {
					field,
					..
				} = expr
				{
					reserve(RelationAlias::index_of(field.as_str()));
				}
				std::ops::ControlFlow::<()>::Continue(())
			});
			Ok::<_, Infallible>(())
		});
	});
	next
}

/// [`LogicalOp::visit_ops_deep`] for a pass that cannot fail: every node of
/// the plan in pre-order, including the nodes of every plan nested in a
/// scalar subquery at any depth, and none from inside a closure body. The
/// callback runs to the end of the plan.
fn visit_deep<F: FnMut(&LogicalOp)>(op: &LogicalOp, f: &mut F) {
	let _ = op.visit_ops_deep(&mut |node| {
		f(node);
		std::ops::ControlFlow::<()>::Continue(())
	});
}

/// The leaf a `FROM` expression lowers to: a `Values` leaf when the expression
/// is a value literal, otherwise a `Source` classified by its syntax. Applied
/// by every pass that rewrites a source's expression, so the leaf kinds of a
/// bound plan are the ones lowering would have produced for the resolved
/// statement. A rewritten source has no schema until the resolve pass sees
/// it, so `schema` is what the caller knows about it — `None` after a rewrite.
fn classify_source(
	expr: LogicalExpr,
	schema: Option<Arc<Schema>>,
	with: Option<With>,
	for_update: bool,
) -> LogicalOp {
	if is_value_literal(&expr) {
		LogicalOp::Values(LogicalValues {
			expr,
			for_update,
		})
	} else {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::classify(expr),
			schema,
			with,
			for_update,
		})
	}
}

/// Parses one `SELECT` statement to its unbound plan. Test fixture shared by
/// the passes' tests.
#[cfg(test)]
fn lower_test(sql: &str) -> LogicalOp {
	let ast = crate::syn::parse(sql).expect("statement should parse");
	let top = ast.expressions.into_iter().next().expect("one statement");
	let crate::sql::TopLevelExpr::Expr(expr) = top else {
		panic!("expected a bare expression");
	};
	crate::expr::logical::lower(expr).expect("statement should lower")
}

#[cfg(all(test, feature = "kv-mem"))]
pub(super) mod test_fixture {
	//! An in-memory datastore with a schema, and the [`BindContext`] a
	//! statement binds under for a given session.

	use std::sync::Arc;

	use super::*;
	use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
	use crate::dbs::Session;
	use crate::exec::planner::util::SELECT_ITERATION_PARAMS;
	use crate::kvs::{Datastore, TransactionType};

	pub(in crate::exec::logical) struct Fixture {
		ds: Arc<Datastore>,
		registry: FunctionRegistry,
	}

	/// Everything a [`BindContext`] borrows, owned.
	pub(in crate::exec::logical) struct Bound {
		pub(in crate::exec::logical) ctx: FrozenContext,
		pub(in crate::exec::logical) txn: Arc<Transaction>,
		pub(in crate::exec::logical) ns_db: Option<(NamespaceId, DatabaseId)>,
		pub(in crate::exec::logical) auth: Option<Arc<Auth>>,
	}

	impl Fixture {
		/// `test`/`test` on a fresh in-memory datastore with server auth
		/// enabled, after running `setup` as the root Owner.
		pub(in crate::exec::logical) async fn new(setup: &str) -> Self {
			let ds = Datastore::builder()
				.with_capabilities(crate::dbs::Capabilities::all())
				.with_auth(true)
				.build_with_path("memory")
				.await
				.expect("in-memory datastore");
			{
				let txn = ds.transaction(TransactionType::Write).await.expect("write txn");
				txn.ensure_ns_db(None, "test", "test").await.expect("ensure test/test");
				txn.commit().await.expect("commit");
			}
			let fixture = Self {
				ds,
				registry: FunctionRegistry::with_builtins(),
			};
			if !setup.trim().is_empty() {
				for response in
					fixture.ds.execute(setup, &owner(), None).await.expect("setup executes")
				{
					response.result.expect("setup statement succeeds");
				}
			}
			fixture
		}

		/// Runs `sql` as `session`, expecting every statement to succeed.
		pub(in crate::exec::logical) async fn execute_as(&self, session: &Session, sql: &str) {
			for response in self.ds.execute(sql, session, None).await.expect("statement executes") {
				response.result.expect("statement succeeds");
			}
		}

		/// The borrowed halves of a [`BindContext`] for `session`, over a
		/// fresh read transaction, with `values` added to the context.
		pub(in crate::exec::logical) async fn bound_as(
			&self,
			session: &Session,
			values: Vec<(&str, Value)>,
		) -> Bound {
			let txn = Arc::new(self.ds.transaction(TransactionType::Read).await.expect("txn"));
			let mut ctx = self.ds.setup_ctx().expect("context");
			ctx.attach_session(session).expect("attach session");
			ctx.set_transaction(Arc::clone(&txn));
			for (name, value) in values {
				ctx.add_value(name.to_owned(), Arc::new(value));
			}
			let ns_db = if let (Some(ns), Some(db)) = (&session.ns, &session.db) {
				let db = txn.expect_db_by_name(ns, db).await.expect("database");
				Some((db.namespace_id, db.database_id))
			} else {
				None
			};
			Bound {
				ctx: ctx.freeze(),
				txn,
				ns_db,
				auth: Some(Arc::clone(&session.au)),
			}
		}

		pub(in crate::exec::logical) fn bind_context<'a>(
			&'a self,
			bound: &'a Bound,
		) -> BindContext<'a> {
			BindContext {
				ctx: &bound.ctx,
				txn: Some(Arc::clone(&bound.txn)),
				ns_db: bound.ns_db,
				auth: bound.auth.clone(),
				row_scoped: SELECT_ITERATION_PARAMS,
				registry: &self.registry,
			}
		}

		/// Binds `sql` for `session` and renders the plan.
		pub(in crate::exec::logical) async fn bind_as(
			&self,
			session: &Session,
			sql: &str,
		) -> String {
			let bound = self.bound_as(session, vec![]).await;
			let bx = self.bind_context(&bound);
			bind(lower_test(sql), &bx).await.expect("binds").to_string()
		}
	}

	pub(in crate::exec::logical) fn owner() -> Session {
		Session::owner().with_ns("test").with_db("test")
	}

	pub(in crate::exec::logical) fn record_user() -> Session {
		Session::for_record(
			"test",
			"test",
			"user",
			crate::types::PublicValue::String("user:tobie".to_owned()),
		)
	}

	pub(in crate::exec::logical) fn db_viewer() -> Session {
		Session::for_level(
			crate::iam::Level::Database("test".to_owned(), "test".to_owned()),
			crate::iam::Role::Viewer,
		)
		.with_ns("test")
		.with_db("test")
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::test_fixture::{Fixture, owner, record_user};
	use super::*;

	#[tokio::test]
	async fn each_pass_feeds_the_next() {
		// The parameter resolves to `true`, the short circuit folds it away,
		// and the arithmetic beside it folds too.
		let fixture = Fixture::new("DEFINE TABLE t SCHEMALESS;").await;
		let bound = fixture.bound_as(&owner(), vec![("p", Value::Bool(true))]).await;
		let bx = fixture.bind_context(&bound);
		let plan = bind(lower_test("SELECT * FROM t WHERE $p AND a > 1 + 1"), &bx)
			.await
			.expect("binds")
			.to_string();
		assert!(plan.contains("Filter[a > 2]"), "{plan}");
	}

	#[tokio::test]
	async fn a_predicate_that_folds_to_true_loses_its_filter() {
		let fixture = Fixture::new("DEFINE TABLE t SCHEMALESS;").await;
		let bound = fixture.bound_as(&owner(), vec![("p", Value::Bool(true))]).await;
		let bx = fixture.bind_context(&bound);
		let plan =
			bind(lower_test("SELECT * FROM t WHERE $p"), &bx).await.expect("binds").to_string();
		assert!(!plan.contains("Filter"), "{plan}");
		assert!(plan.contains("Source[t]"), "{plan}");
	}

	#[tokio::test]
	async fn a_subquery_predicate_merges_into_the_inner_plan() {
		let fixture = Fixture::new("DEFINE TABLE t SCHEMALESS;").await;
		let bound = fixture.bound_as(&owner(), vec![("p", Value::from(2i64))]).await;
		let bx = fixture.bind_context(&bound);
		let plan = bind(lower_test("SELECT * FROM (SELECT * FROM t WHERE a) WHERE b = $p"), &bx)
			.await
			.expect("binds")
			.to_string();
		assert!(plan.contains("Filter[a AND b = 2]"), "{plan}");
	}

	#[tokio::test]
	async fn the_resolved_schema_and_permissions_flow_through_every_pass() {
		// The table permission is resolved first, the parameter in the user's
		// predicate is substituted, the constant folds, and the user's filter
		// stays above the permission filter throughout.
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select WHERE published = true;
			 DEFINE FIELD a ON t TYPE int;
			 DEFINE FIELD published ON t TYPE bool;",
		)
		.await;
		let bound = fixture.bound_as(&record_user(), vec![("p", Value::from(1i64))]).await;
		let bx = fixture.bind_context(&bound);
		let plan = bind(lower_test("SELECT * FROM t WHERE a > $p + 1"), &bx)
			.await
			.expect("binds")
			.to_string();
		let filter = plan.find("Filter[a > 2]").expect(&plan);
		let permission = plan.find("PermissionFilter[published = true]").expect(&plan);
		assert!(filter < permission, "the user's filter must stay above the barrier:\n{plan}");
	}

	#[tokio::test]
	async fn without_a_principal_nothing_is_resolved() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select WHERE published = true;
			 DEFINE FIELD a ON t TYPE int;",
		)
		.await;
		let mut bound = fixture.bound_as(&record_user(), vec![]).await;
		bound.auth = None;
		let bx = fixture.bind_context(&bound);
		let plan = bind(lower_test("SELECT * FROM t WHERE a > 1"), &bx).await.expect("binds");
		assert!(
			!plan.any(|n| matches!(n, LogicalOp::PermissionFilter(_))),
			"no principal, no permission decision:\n{plan}"
		);
		assert!(
			plan.any(|n| matches!(
				n,
				LogicalOp::Source(LogicalSource {
					schema: None,
					..
				})
			)),
			"the source must stay unresolved:\n{plan}"
		);
	}

	/// A descent written straight from the contract — node, then the plan of
	/// every scalar subquery, then the inputs, with a closure body left alone
	/// — and composed out of the per-node walks rather than out of
	/// [`LogicalOp::visit_ops_deep`]. Two implementations that agree on a
	/// corpus is what makes the delegation checkable at all; one that called
	/// the walk under test would assert nothing.
	fn visit_deep_reference<F: FnMut(&LogicalOp)>(op: &LogicalOp, f: &mut F) {
		f(op);
		let Ok(()) = op.visit_scalars(&mut |scalar| {
			let _ = visit_binding(scalar, &mut |expr| {
				if let LogicalExpr::Subquery(plan) = expr {
					visit_deep_reference(plan, f);
				}
				std::ops::ControlFlow::<()>::Continue(())
			});
			Ok::<_, Infallible>(())
		});
		for child in op.children() {
			visit_deep_reference(child, f);
		}
	}

	/// The nodes [`visit_deep_reference`] reaches, by address, in order.
	fn seq_reference(op: &LogicalOp) -> Vec<*const LogicalOp> {
		let mut v = Vec::new();
		visit_deep_reference(op, &mut |n| v.push(n as *const LogicalOp));
		v
	}

	/// The nodes [`visit_deep`] reaches, by address, in order.
	fn seq_walk(op: &LogicalOp) -> Vec<*const LogicalOp> {
		let mut v = Vec::new();
		visit_deep(op, &mut |n| v.push(n as *const LogicalOp));
		v
	}

	fn lower_opt(sql: &str) -> Option<LogicalOp> {
		let ast = crate::syn::parse(sql).ok()?;
		let top = ast.expressions.into_iter().next()?;
		let crate::sql::TopLevelExpr::Expr(expr) = top else {
			return None;
		};
		crate::expr::logical::lower(expr).ok()
	}

	const CORPUS: &[&str] = &[
		"SELECT * FROM t",
		"SELECT (SELECT * FROM inner) AS p FROM t",
		"SELECT * FROM t WHERE k IN (SELECT VALUE id FROM p WHERE j IN (SELECT VALUE id FROM q))",
		"SELECT * FROM (SELECT * FROM t WHERE a) WHERE b",
		"SELECT |$x| (SELECT * FROM cb) AS f FROM t",
		"SELECT * FROM t WHERE (|$x| (SELECT * FROM cb2))(1)",
		"SELECT { a: (SELECT * FROM o1), b: [ (SELECT * FROM o2), |$y| (SELECT * FROM o3) ] } FROM t",
		"SELECT a FROM t GROUP BY (SELECT * FROM g) ",
		"SELECT a FROM t ORDER BY (SELECT VALUE x FROM s)[0]",
		"SELECT a FROM t LIMIT (SELECT VALUE n FROM l)[0] START (SELECT VALUE n FROM st)[0]",
		"SELECT a FROM t FETCH b TIMEOUT 5s",
		"SELECT a FROM t SPLIT b",
		"SELECT a FROM t VERSION d'2024-01-01T00:00:00Z'",
		"SELECT * FROM (SELECT * FROM a), (SELECT * FROM b)",
		"SELECT * FROM t WHERE a AND (SELECT VALUE x FROM u)[0] OR (SELECT VALUE y FROM v)[0]",
		"SELECT math::sum((SELECT VALUE n FROM agg)) FROM t GROUP ALL",
		"LET $x = (SELECT * FROM letv); RETURN (SELECT * FROM retv)",
		"FOR $i IN (SELECT * FROM fr) { SELECT * FROM body }",
		"IF (SELECT VALUE c FROM cond)[0] THEN (SELECT * FROM thn) ELSE (SELECT * FROM els) END",
		"SELECT * FROM t WHERE a.b[WHERE c = (SELECT VALUE d FROM filt)[0]]",
		"SELECT type::field((SELECT VALUE s FROM ff)[0]) FROM t",
		"SELECT ->likes->(SELECT * FROM lk) FROM t",
		"RETURN (SELECT * FROM r1) ?: (SELECT * FROM r2)",
		"SELECT * FROM ONLY (SELECT * FROM only1) LIMIT 1",
		"SELECT * FROM t WHERE a IN [(SELECT * FROM arr1), (SELECT * FROM arr2)]",
		"SLEEP 1s",
		"INFO FOR DB",
		"SELECT VALUE (SELECT * FROM vv) FROM t",
		"SELECT * FROM (SELECT |$z| (SELECT * FROM deepcb) AS q FROM inner2)",
		"SELECT * FROM t WHERE $parent.a = (SELECT VALUE b FROM par)[0]",
		"SELECT * FROM t1, t2 WHERE (SELECT VALUE n FROM m)[0] > 1 ORDER BY a DESC LIMIT 3",
		"SELECT * FROM t WHERE a = (SELECT VALUE x FROM x1)[0] AND b = (SELECT VALUE y FROM y1)[0]",
		"SELECT (SELECT (SELECT (SELECT * FROM d4) FROM d3) FROM d2) FROM d1",
	];

	#[test]
	fn the_statement_walk_agrees_with_a_reference_descent() {
		let mut lowered = 0usize;
		let mut with_nested = 0usize;
		for sql in CORPUS {
			let Some(plan) = lower_opt(sql) else {
				continue;
			};
			lowered += 1;
			let reference = seq_reference(&plan);
			let walked = seq_walk(&plan);
			assert_eq!(reference, walked, "walk order diverged for: {sql}");
			// A plan whose walk reaches more nodes than the operator tree
			// alone proves the corpus entry actually nests a subquery.
			let mut tree = Vec::new();
			let _ = plan.visit(&mut |n| {
				tree.push(n as *const LogicalOp);
				std::ops::ControlFlow::<()>::Continue(())
			});
			if walked.len() > tree.len() {
				with_nested += 1;
			}
		}
		assert!(lowered >= 25, "corpus mostly failed to lower: {lowered}");
		assert!(with_nested >= 15, "corpus barely nests any plans: {with_nested}");
	}

	/// Counts the closure bodies and the plans inside them, so the corpus is
	/// known to exercise the closure stopping rule at all.
	#[test]
	fn the_corpus_contains_closure_bodies_holding_plans() {
		let mut closures = 0usize;
		let mut plans_in_closures = 0usize;
		for sql in CORPUS {
			let Some(plan) = lower_opt(sql) else {
				continue;
			};
			// Walk every scalar of the whole operator tree with the expr
			// walk that DOES enter closure bodies.
			let _ = plan.visit_scalars_deep(&mut |scalar| {
				let _ = scalar.visit(&mut |e| {
					if matches!(e, LogicalExpr::Closure(_)) {
						closures += 1;
					}
					std::ops::ControlFlow::<()>::Continue(())
				});
				Ok::<_, Infallible>(())
			});
			let _ = plan.visit_scalars_deep(&mut |scalar| {
				let _ = scalar.visit(&mut |e| {
					if let LogicalExpr::Closure(c) = e {
						let _ = c.body.visit(&mut |b| {
							if matches!(b, LogicalExpr::Subquery(_)) {
								plans_in_closures += 1;
							}
							std::ops::ControlFlow::<()>::Continue(())
						});
					}
					std::ops::ControlFlow::<()>::Continue(())
				});
				Ok::<_, Infallible>(())
			});
		}
		assert!(closures >= 3, "corpus has too few closures: {closures}");
		assert!(
			plans_in_closures >= 3,
			"corpus has too few plans in closures: {plans_in_closures}"
		);
	}
}
