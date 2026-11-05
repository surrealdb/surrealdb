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
//! parameters, its calls stay calls, and nothing rewrites it. The physical
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
//!
//! Binding rewrites nothing else. The rewrites a statement is owed (folding, filter movement,
//! pruning, function inlining, the decorrelation of a membership subquery) run on the IR the
//! binder builds from the bound plan, where each is stated once.
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
// position on it, which a wildcard arm would silently absorb.
#![deny(clippy::wildcard_enum_match_arm)]

pub(crate) mod constants;
pub(crate) mod effects;
mod enum_order;
pub(crate) mod footprint;
mod params;
pub(crate) mod pipelines;
pub(crate) mod reads;
mod resolve;
pub(crate) mod summaries;
use std::convert::Infallible;
use std::sync::Arc;

pub(crate) use effects::{effects_of_body, effects_of_expr, may_read_clock};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::function::FunctionRegistry;
use crate::expr::With;
use crate::expr::logical::{
	LogicalDdl, LogicalDdlStatement, LogicalEval, LogicalExpr, LogicalOp, LogicalSource,
	LogicalValues, Schema, SourceRef, is_value_literal,
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
	/// Whether the frame this plan is compiled for enforces permissions at
	/// all, mirroring
	/// [`RootContext::perms_suppressed`](crate::exec::context::RootContext).
	/// A suppressed frame reads every row of every table it names whatever
	/// its principal may select, so the resolve pass plants neither a
	/// permission filter nor a field guard for it: the plan a write compiles
	/// to maintain a materialized view computes the view's value and not the
	/// writer's view of it.
	pub perms_suppressed: bool,
	/// Parameter names the statement binds per row (`$this`, `$parent`, …),
	/// which are never substituted.
	pub row_scoped: &'a [&'a str],
	/// Decides which calls fold and which name a projection function.
	pub registry: &'a FunctionRegistry,
	/// Whether a catalog definition may be inlined into the plan: a `DEFINE PARAM` read
	/// replaced by its value. A statement that reads each definition as the
	/// catalog stands when it is reached inlines none
	/// ([`CatalogWithin::calls_current`](crate::exec::planner::CatalogWithin)).
	pub inlines_definitions: bool,
	/// Whether the statements written in the plan's expressions are bound
	/// with it. A statement that plans them late-bound leaves them as written
	/// ([`CatalogWithin::nested_late`](crate::exec::planner::CatalogWithin)):
	/// each is bound when it is planned, against the catalog it then finds.
	pub binds_nested: bool,
}

/// Bind `op` for one transaction and one principal, every closure body in it
/// excepted. See the module documentation for the pass order.
pub(crate) async fn bind(op: LogicalOp, bx: &BindContext<'_>) -> Result<LogicalOp, Error> {
	// Statements left as written are taken out of the plan while it is bound,
	// and put back in its bound form where their places are left.
	let mut nested = Vec::new();
	let op = match bx.binds_nested {
		true => op,
		false => take_nested(op, &mut nested),
	};
	let bound = bind_taken(op, bx).await?;
	Ok(match nested.is_empty() {
		true => bound,
		false => put_nested(bound, &nested),
	})
}

/// The prefix of the name of the parameter that holds the place of a
/// statement [`take_nested`] took out of a plan, followed by its index: a
/// name no query text can write.
const NESTED_PLACE: &str = "\u{0}nested";

/// The name of the parameter that holds the place of the statement at
/// `index` of those [`take_nested`] took out of a plan.
fn nested_place(index: usize) -> String {
	format!("{NESTED_PLACE}{index}")
}

/// `op` with each statement written in its own expressions, closure bodies
/// apart, replaced by a statement that reads a parameter no plan can name
/// ([`nested_place`]), the statements taken pushed to `taken` in order.
fn take_nested(op: LogicalOp, taken: &mut Vec<LogicalOp>) -> LogicalOp {
	let Ok(op) = op.transform_up(&mut |mut node| {
		let Ok(()) = node.visit_scalars_mut(&mut |scalar| {
			visit_binding_mut(scalar, &mut |expr| {
				if let LogicalExpr::Subquery(plan) = expr {
					let place = LogicalOp::Eval(LogicalEval {
						expr: LogicalExpr::Param(crate::expr::Param::from(nested_place(
							taken.len(),
						))),
					});
					taken.push(std::mem::replace(plan.as_mut(), place));
				}
				Ok::<_, Infallible>(())
			})
		});
		Ok::<_, Infallible>(node)
	});
	op
}

/// `op` with each place [`take_nested`] left put back as the statement it
/// held, from `taken`.
fn put_nested(op: LogicalOp, taken: &[LogicalOp]) -> LogicalOp {
	let Ok(op) = op.transform_up(&mut |mut node| {
		let Ok(()) = node.visit_scalars_mut(&mut |scalar| {
			visit_binding_mut(scalar, &mut |expr| {
				if let LogicalExpr::Subquery(plan) = expr
					&& let LogicalOp::Eval(LogicalEval {
						expr: LogicalExpr::Param(param),
					}) = plan.as_ref()
					&& let Some(statement) = param
						.as_str()
						.strip_prefix(NESTED_PLACE)
						.and_then(|at| at.parse::<usize>().ok())
						.and_then(|at| taken.get(at))
				{
					**plan = statement.clone();
				}
				Ok::<_, Infallible>(())
			})
		});
		Ok::<_, Infallible>(node)
	});
	op
}

/// `op` with each `type::field('<path>')` inside an element filter's
/// predicate (`[WHERE …]`) written as the path it names. The call reads the
/// path of the current value, which inside the predicate is the element, as
/// the path does.
fn element_field_paths(op: LogicalOp) -> LogicalOp {
	fn in_filters(expr: &mut LogicalExpr) {
		let Ok(()) = expr.visit_mut(&mut |node| {
			if let LogicalExpr::Filter {
				predicate,
				..
			} = node
			{
				let Ok(()) = predicate.visit_mut(&mut |inner| {
					if let LogicalExpr::Call(call) = inner
						&& matches!(&call.receiver, crate::expr::Function::Normal(name) if name == "type::field")
						&& let [
							LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::String(
								path,
							)),
						] = call.args.as_slice()
						&& let Ok(idiom) = crate::syn::idiom(path.as_str())
						&& let Ok(field) =
							crate::expr::logical::lower_expr(crate::expr::Expr::Idiom(idiom))
						&& field.is_path()
					{
						*inner = field;
					}
					Ok::<_, Infallible>(())
				});
			}
			Ok::<_, Infallible>(())
		});
	}
	rewrite_deep(op, &mut |mut node| {
		let Ok(()) = node.visit_scalars_mut(&mut |scalar| {
			in_filters(scalar);
			Ok::<_, Infallible>(())
		});
		node
	})
}

/// [`bind`] over a plan whose nested statements are bound with it or taken out.
async fn bind_taken(op: LogicalOp, bx: &BindContext<'_>) -> Result<LogicalOp, Error> {
	let caller = Caller::new(bx);
	let op = resolve::resolve(op, bx, caller.as_ref()).await?;
	let op = params::resolve_params(op, bx).await;
	Ok(element_field_paths(op))
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
	/// Whether the frame suppresses enforcement, which decides for every
	/// principal at once: see [`BindContext::perms_suppressed`].
	perms_suppressed: bool,
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
			perms_suppressed: bx.perms_suppressed,
		})
	}

	pub(super) fn auth(&self) -> &Arc<Auth> {
		&self.auth
	}

	/// Whether reads by this principal are permission-checked, as
	/// `should_check_perms(View)` decides: never in a frame that suppresses
	/// enforcement, never when auth is disabled and the session is anonymous;
	/// otherwise only a viewer whose actor level covers the session's
	/// namespace and database is exempt.
	pub(super) fn checks_permissions(&self) -> bool {
		self.checks_permissions_as(&self.auth)
	}

	/// [`Self::checks_permissions`] for a principal derived from this one — a
	/// `COMPUTED` body narrowed by its `AUTH LIMIT`. Suppression is the
	/// frame's and outranks the derived principal, as the runtime gate takes
	/// it before it reads an authority at all.
	pub(super) fn checks_permissions_as(&self, auth: &Auth) -> bool {
		!self.perms_suppressed
			&& crate::exec::permission::action_is_checked(
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
pub(crate) fn rewrite_deep<F: FnMut(LogicalOp) -> LogicalOp>(
	op: LogicalOp,
	f: &mut F,
) -> LogicalOp {
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
	// Over `dyn FnMut` for the reason [`visit_binding`] gives.
	fn walk<E>(
		expr: &mut LogicalExpr,
		f: &mut dyn FnMut(&mut LogicalExpr) -> Result<(), E>,
	) -> Result<(), E> {
		if !matches!(expr, LogicalExpr::Closure(_)) {
			for child in expr.children_mut() {
				walk(child, &mut *f)?;
			}
		}
		f(expr)
	}
	walk(expr, f)
}

/// Whether a pruning walk enters the expressions nested in the one it has
/// just visited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Descend {
	/// Visit the nested expressions next.
	Into,
	/// Skip them: the visitor has handled, or deliberately left, the whole
	/// subtree.
	Over,
}

/// [`visit_binding`] with pruning: `f` sees an expression before the
/// expressions nested in it and decides whether the walk enters them. A
/// closure body is never entered.
pub(crate) fn visit_binding_pruning<'a>(
	expr: &'a LogicalExpr,
	f: &mut impl FnMut(&'a LogicalExpr) -> Descend,
) {
	// Over `dyn FnMut` for the reason [`visit_binding`] gives. The visitor
	// sees each expression for the tree's lifetime, so it may keep borrows of
	// it.
	fn walk<'a>(expr: &'a LogicalExpr, f: &mut dyn FnMut(&'a LogicalExpr) -> Descend) {
		if f(expr) == Descend::Over || matches!(expr, LogicalExpr::Closure(_)) {
			return;
		}
		expr.for_each_child(&mut |child| walk(child, &mut *f));
	}
	walk(expr, f)
}

/// [`visit_binding_pruning`], rewriting in place: `f` may replace the
/// expression it sees, and the walk then enters what `f` left, unless `f`
/// answers [`Descend::Over`]. A closure body is never entered.
pub(super) fn visit_binding_pruning_mut(
	expr: &mut LogicalExpr,
	f: &mut impl FnMut(&mut LogicalExpr) -> Descend,
) {
	// Over `dyn FnMut` for the reason [`visit_binding`] gives.
	fn walk(expr: &mut LogicalExpr, f: &mut dyn FnMut(&mut LogicalExpr) -> Descend) {
		if f(expr) == Descend::Over || matches!(expr, LogicalExpr::Closure(_)) {
			return;
		}
		for child in expr.children_mut() {
			walk(child, &mut *f);
		}
	}
	walk(expr, f)
}

/// The first write in `op` that cannot run, named by the clause the planner
/// cannot compile — or `None` when every write it holds can run.
///
/// A mutation has a logical form, so a statement holding one reaches the
/// planner instead of being refused by statement kind. A write is declined
/// exactly when one of its clauses has no logical form. *Where* a decline
/// happens is load-bearing, which is why this runs at
/// [`crate::exec::planner::plan_statement_inner`], before anything is bound
/// or compiled.
///
/// Every caller that can fall back does so on `ExecError::PlannerUnsupported`
/// alone; each other planning error fails the statement outright. A sibling
/// branch can raise one of those first — `RETURN IF … { SELECT … @@ … } ELSE
/// { CREATE person }` compiles the untaken `SELECT`, which raises
/// `NoIndexFoundForMatch` — so declining only once the walk reaches the write
/// would make the fallback depend on the order branches are compiled in, and
/// would fail a statement the compute executor runs to completion.
///
/// # The traversal is the write's own, not a bind pass's
///
/// It reaches every position a write can be written into, which is more than
/// any bind pass walks:
///
/// - operator children, and the plans nested in operator scalars;
/// - **closure bodies**, which [`visit_binding`] deliberately does not enter. A bind pass stops
///   there because a closure resolves where it is invoked; this walk asks a different question —
///   whether the statement holds a write at all — for which the body counts.
///   `LogicalExpr::children()` exposes it, so entering it is a matter of not re-applying the
///   closure guard.
///
/// A write in a closure the statement never calls still declines. The
/// question is whether the statement holds a write anywhere, and the compute
/// executor runs such a statement to completion; a narrower walk would decline
/// only some of them, and which ones would depend on compile order.
///
/// The per-node declines in [`crate::exec::planner::fold`] remain the
/// authority for a write that reaches them.
pub(crate) fn unplannable_write(op: &LogicalOp) -> Option<String> {
	let mut found = None;
	write_walk::in_op(op, &mut found, &mut write_walk::cannot_run);
	found
}

/// The same question as [`unplannable_write`], asked of a bare expression.
///
/// A stored catalog clause is an expression rather than a statement, and it
/// reaches the planner through [`crate::exec::Planner::scalar_metadata`] instead
/// of `plan_statement_inner`, so the statement-level pre-flight never sees it.
/// A caller compiling such a clause asks here, and carries the clause as an
/// uncompilable one when the answer is `Some`.
pub(crate) fn unplannable_write_in_expr(expr: &LogicalExpr) -> Option<String> {
	let mut found = None;
	write_walk::in_expr(expr, &mut found, &mut write_walk::cannot_run);
	found
}

/// The error raised for a `LET` written as an expression.
pub(crate) const LET_IN_AN_EXPRESSION: &str =
	"LET statements can only appear at the top level of a query or inside a block expression";

/// The error of the first `LET` in `op` that no statement may hold: a `LET`
/// of a protected name (`$access`, `$auth`, `$token`, `$session`) or of a name
/// the system binds (`$this`, `$value`, ...; [`crate::expr::logical::LogicalLet::is_refused`]), or
/// a `LET` written as an expression — in a clause, an array, an operand or an `IF`
/// arm's value, rather than a statement of a block or the value of a `RETURN`
/// or another `LET`.
///
/// Either is a property of the text written, not of what runs, so it is asked
/// of the statement as lowered, before binding folds anything: it is refused
/// wherever it stands — in an `IF` arm that never runs, a loop body that runs
/// no times, a statement after a `RETURN` — and whatever a later pass removes.
///
/// "First" is the order of one deterministic walk of the plan: a node, then
/// each of its expressions with every statement nested in it, in turn, then
/// its child nodes — for a statement `IF`, each condition then its arm. That is
/// the written order within an expression and across a block's statements and
/// an `IF`'s arms; across a relational statement's clauses it is the plan's
/// order, which need not be the order the clauses are written in. A closure
/// body is entered where the closure is written: the closure is refused with
/// the statement that writes it, whether or not it is ever called.
pub(crate) fn invalid_let(op: &LogicalOp) -> Option<Error> {
	use std::ops::ControlFlow;

	use crate::exec::Error as ExecError;

	fn let_in_an_expression() -> ControlFlow<ExecError> {
		ControlFlow::Break(ExecError::InvalidStatement(LET_IN_AN_EXPRESSION.to_owned()))
	}

	fn in_op(op: &LogicalOp, session: bool) -> ControlFlow<ExecError> {
		if let LogicalOp::Let(stmt) = op
			&& stmt.is_refused(session)
		{
			return ControlFlow::Break(ExecError::InvalidParam {
				name: stmt.name.clone(),
			});
		}
		// A statement `IF` is walked a condition and its arm at a time, then
		// its `ELSE`, as they are written. An arm written as a `LET`, rather
		// than as a block holding one, is the arm's value: a `LET` written as
		// an expression.
		if let LogicalOp::If(ifs) = op {
			fn arm(arm: &LogicalOp) -> ControlFlow<ExecError> {
				if matches!(arm, LogicalOp::Let(_)) {
					return let_in_an_expression();
				}
				in_op(arm, false)
			}
			for (condition, then) in ifs.branches() {
				in_expr(condition)?;
				arm(then)?;
			}
			return ifs.otherwise().map_or(ControlFlow::Continue(()), arm);
		}
		// Every expression a node holds is an expression position, a `FOR`'s
		// range and a `SLEEP`'s duration among them; a statement taken as a
		// statement — a block's, a `LET`'s value, a `RETURN`'s, a loop's body
		// — is a child node.
		let mut flow = ControlFlow::Continue(());
		op.for_each_scalar(&mut |scalar| {
			if flow.is_continue() {
				flow = in_expr(scalar);
			}
		});
		flow?;
		op.children().iter().try_for_each(|child| in_op(child, false))
	}

	/// `expr` and the statements nested in it.
	fn in_expr(expr: &LogicalExpr) -> ControlFlow<ExecError> {
		if let LogicalExpr::Subquery(op) = expr {
			if matches!(op.as_ref(), LogicalOp::Let(_)) {
				return let_in_an_expression();
			}
			return in_op(op, false);
		}
		if let LogicalExpr::Closure(closure) = expr {
			return in_expr(&closure.body);
		}
		let mut flow = ControlFlow::Continue(());
		expr.for_each_child(&mut |child| {
			if flow.is_continue() {
				flow = in_expr(child);
			}
		});
		flow
	}

	match in_op(op, true) {
		ControlFlow::Break(error) => Some(Error::Exec(error)),
		ControlFlow::Continue(()) => None,
	}
}

/// Whether a schema statement changes what the session's namespace and
/// database resolve to. Every other one leaves name resolution where it was
/// and changes only the catalog inside it.
pub(crate) fn changes_name_resolution(ddl: &LogicalDdl) -> bool {
	use crate::expr::statements::{DefineStatement, RemoveStatement};
	match &ddl.statement {
		LogicalDdlStatement::Define(define) => {
			matches!(**define, DefineStatement::Namespace(_) | DefineStatement::Database(_))
		}
		LogicalDdlStatement::Remove(remove) => {
			matches!(**remove, RemoveStatement::Namespace(_) | RemoveStatement::Database(_))
		}
		LogicalDdlStatement::Alter(_) | LogicalDdlStatement::Rebuild(_) => false,
	}
}

/// Why a plan cannot be bound by a planner that does not plan what follows
/// its schema statements late-bound, because a schema statement changes a
/// name the plan already resolved, if one does.
///
/// Such a planner binds the statement against the catalog it finds, once. The
/// session's namespace and database are resolved into the execution context
/// before the plan runs, so a block that creates or removes the namespace or
/// database the session names has been given a context resolved against the
/// catalog from before it ran, and a table, field or index defined by one of a
/// block's own statements is resolved by the ones after it only where the block
/// plans what follows a schema statement late-bound.
///
/// The decline is therefore for a namespace or database statement nested
/// inside another statement, and for any schema statement nested where no
/// block covers it. The VM plans what follows such a statement late-bound
/// ([`crate::exec::planner::Planner::late_binding`]), and the planners that
/// reach it through [`crate::exec::planner::Planner::binding_late_after_schema`]
/// do not ask.
pub(crate) fn unplannable_context_change(op: &LogicalOp) -> Option<String> {
	/// `covered` is whether every operator between here and the root is a
	/// block, which is the only shape that compiles its statements as it
	/// reaches them. It never becomes true again once false: a block inside a
	/// `FOR` is compiled once for every iteration the loop runs, and one
	/// inside an expression is compiled with the statement holding it.
	fn nested(op: &LogicalOp, covered: bool) -> Option<String> {
		if let LogicalOp::Ddl(ddl) = op {
			let why = |what: &str| Some(format!("{} {what}", ddl.statement.kind()));
			if changes_name_resolution(ddl) {
				// The execution context resolved the session's names
				// before the plan ran, and no later compilation revisits
				// it.
				why("inside another statement changes the name the plan resolved")
			} else if covered {
				None
			} else {
				why("inside another statement")
			}
		} else if let LogicalOp::Sequence(seq) = op {
			seq.stmts.iter().find_map(|op| nested(op, covered))
		} else {
			// Nothing below here is covered, however it nests, so the whole
			// subtree is one question. The walk reaches plans held in scalars
			// too — an `IF` is an expression, and the block in its branch is a
			// subquery rather than an operator child.
			let mut found = None;
			let mut seen_self = false;
			let _ = op.visit_ops_deep(&mut |node| {
				if !seen_self {
					seen_self = true;
					return std::ops::ControlFlow::Continue(());
				}
				if let LogicalOp::Ddl(ddl) = node {
					found = nested(node, false);
					let _ = ddl;
					return std::ops::ControlFlow::Break(());
				}
				std::ops::ControlFlow::Continue(())
			});
			found
		}
	}

	// The whole plan: nothing else has resolved anything against it, which is
	// what gives a schema statement reached from a function body or from
	// `eval` a compiled route.
	if matches!(op, LogicalOp::Ddl(_)) {
		return None;
	}
	nested(op, true)
}

/// The walk both questions share.
mod write_walk {
	use surrealdb_types::ToSql;

	use super::{LogicalExpr, LogicalOp};
	use crate::expr::logical::{LogicalWrite, LogicalWriteStatement};
	use crate::expr::{Data, Expr};

	/// What the walk asks of each write it reaches. `Some` stops the walk and
	/// becomes the answer.
	///
	/// A parameter rather than an inlined test so that the traversal and the
	/// decision are testable apart: the traversal's job is to reach a write
	/// wherever it is written, whatever the decision.
	pub(super) type Decide = dyn FnMut(&LogicalWrite) -> Option<String>;

	/// The production decision, asked of the statement alone: every clause it
	/// carries must have a compiled form.
	///
	/// The question is answered here rather than left to the planner because
	/// the answer decides whether the caller falls back to the compute
	/// executor. A decline raised once the fold has reached the write would
	/// let an unrelated refusal elsewhere in the statement — a `MATCHES` in an
	/// untaken branch that binds to no index, say — surface first and fail a
	/// statement the compute executor runs to completion, making the fallback
	/// depend on the order branches are compiled in.
	///
	/// The clause check lowers the statement's scalars a second time, since
	/// the planner lowers them again when it compiles them. A write's clauses
	/// are small and the work is per statement rather than per record, which
	/// is the price of an answer that does not depend on compile order.
	pub(super) fn cannot_run(write: &LogicalWrite) -> Option<String> {
		uncompilable_clause(&write.statement)
	}

	/// The first clause of the statement the planner would decline, named.
	///
	/// The clauses are the expressions the planner compiles once for the
	/// statement: its condition, its data clause and its `ON DUPLICATE KEY
	/// UPDATE` clause. A place needs no question of its own: a path made of
	/// parts no address follows compiles to a step that stops the walk.
	fn uncompilable_clause(statement: &LogicalWriteStatement) -> Option<String> {
		if let Some(cond) = statement.cond()
			&& let Some(why) = unlowerable(cond, "a condition")
		{
			return Some(why);
		}
		statement
			.data()
			.and_then(in_data)
			.or_else(|| statement.duplicate_update().and_then(in_data))
	}

	/// The first expression of one data clause the planner would decline.
	fn in_data(data: &Data) -> Option<String> {
		match data {
			Data::EmptyExpression => None,
			Data::PatchExpression(expr) => unlowerable(expr, "a PATCH clause"),
			Data::MergeExpression(expr) => unlowerable(expr, "a MERGE clause"),
			Data::ReplaceExpression(expr) => unlowerable(expr, "a REPLACE clause"),
			Data::ContentExpression(expr) => unlowerable(expr, "a CONTENT clause"),
			Data::SingleExpression(expr) => unlowerable(expr, "an INSERT row"),
			Data::UnsetExpression(_) => None,
			Data::SetExpression(assignments) | Data::UpdateExpression(assignments) => assignments
				.iter()
				.find_map(|assignment| unlowerable(&assignment.value, "an assignment")),
			Data::ValuesExpression(rows) => {
				rows.iter().flatten().find_map(|(_, expr)| unlowerable(expr, "an INSERT value"))
			}
		}
	}

	/// Why one clause expression has no logical form, if it has none.
	fn unlowerable(expr: &Expr, role: &str) -> Option<String> {
		crate::expr::logical::lower_expr(expr.clone())
			.err()
			.map(|why| format!("{role} cannot hold {}: {why}", expr.to_sql()))
	}

	pub(super) fn in_op(op: &LogicalOp, found: &mut Option<String>, decide: &mut Decide) {
		if found.is_some() {
			return;
		}
		if let LogicalOp::Write(write) = op {
			// A write the decision admits is walked into rather than returned
			// from: its clauses may hold a write of a form the planner declines.
			if let Some(why) = decide(write) {
				*found = Some(why);
				return;
			}
		}
		for scalar in op.scalars() {
			in_expr(scalar, found, decide);
		}
		for child in op.children() {
			in_op(child, found, decide);
		}
	}

	pub(super) fn in_expr(expr: &LogicalExpr, found: &mut Option<String>, decide: &mut Decide) {
		if found.is_some() {
			return;
		}
		if let LogicalExpr::Subquery(plan) = expr {
			in_op(plan, found, decide);
		}
		// No closure guard: `children()` yields a closure's body, and a write
		// in one is the statement's for this question.
		for child in expr.children() {
			in_expr(child, found, decide);
		}
	}

	/// The traversal alone: the kind of the first write anywhere in `op`,
	/// whatever its form can or cannot run today.
	#[cfg(all(test, feature = "kv-mem"))]
	pub(super) fn first_write(op: &LogicalOp) -> Option<String> {
		let mut found = None;
		in_op(op, &mut found, &mut |write| Some(write.statement.kind().to_owned()));
		found
	}
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
	// A source a pass rewrote into a literal is one value, not an array
	// written in `FROM`.
	if is_value_literal(&expr) {
		LogicalOp::Values(LogicalValues {
			expr,
			for_update,
			written: false,
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
	let surrealdb_expr::parser::TopLevelExpr::Expr(expr) = top else {
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
				perms_suppressed: false,
				row_scoped: SELECT_ITERATION_PARAMS,
				registry: &self.registry,
				inlines_definitions: true,
				binds_nested: true,
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
	use super::test_fixture::{Fixture, record_user};
	use super::*;

	/// A write is found wherever it sits, including a scalar position that
	/// `children()` does not reach.
	///
	/// The depth is the point. `LogicalOp::visit` and `children()` walk the
	/// plan's `LogicalOp` nodes, and a mutation written into a scalar — an `IF` branch, a
	/// call argument, a `LIMIT` — hangs off a subquery instead, so a walk over
	/// children alone reports a statement holding one as plannable. It would
	/// then be declined later, at the node, which is the ordering hazard
	/// `unplannable_write` exists to avoid.
	///
	/// Asked of the traversal alone, so it checks the walk independently of
	/// the clause decision that `unplannable_write` makes.
	#[test]
	fn a_write_is_found_through_a_scalar_as_well_as_a_child() {
		let unplannable_write = |op: &LogicalOp| write_walk::first_write(op);
		let some = |kind: &str| Some(kind.to_owned());
		assert_eq!(unplannable_write(&lower_test("CREATE person:tobie")), some("CREATE"));
		assert_eq!(
			unplannable_write(&lower_test(
				"RETURN IF $c { SELECT * FROM t } ELSE { CREATE person:tobie }"
			)),
			some("CREATE"),
			"a write in a scalar position is reached"
		);
		assert_eq!(
			unplannable_write(&lower_test("SELECT id, (CREATE log:1) AS made FROM person")),
			some("CREATE"),
			"a write in a projection is reached"
		);
		assert_eq!(
			unplannable_write(&lower_test("SELECT * FROM person LIMIT (CREATE log:1).n")),
			some("CREATE"),
			"a write in a LIMIT is reached"
		);
		assert_eq!(
			unplannable_write(&lower_test("RETURN array::map([1], |$x| { CREATE log:1 })")),
			some("CREATE"),
			"a write in a closure body is reached, which a bind pass's walk stops short of"
		);

		// The walk over children alone would answer the first of these and
		// miss the rest, so the control matters: a plan with no write must
		// answer `None`, or the assertions above would hold vacuously.
		assert_eq!(unplannable_write(&lower_test("SELECT * FROM person")), None);
		assert_eq!(
			unplannable_write(&lower_test("RETURN IF $c { SELECT * FROM t } ELSE { 1 }")),
			None
		);
		assert_eq!(unplannable_write(&lower_test("RETURN array::map([1], |$x| { $x + 1 })")), None);
	}

	#[tokio::test]
	async fn the_resolved_schema_and_permissions_flow_through_every_pass() {
		// The table permission is resolved first, the parameter in the user's
		// predicate is substituted, and the user's filter stays above the
		// permission filter throughout.
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
		let filter = plan.find("Filter[a > 1 + 1]").expect(&plan);
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
		let surrealdb_expr::parser::TopLevelExpr::Expr(expr) = top else {
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
			// A plan whose walk reaches more nodes than its `LogicalOp`
			// nodes alone proves the corpus entry actually nests a subquery.
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
			// Walk every scalar of the whole plan with the expr
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
