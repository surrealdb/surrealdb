//! `eval::surql` / `eval::gql` — evaluate a nested query string in the current
//! transaction and session context.
//!
//! ## Gating (defense in depth)
//!
//! A call must pass *all* of:
//! 1. the function-family capability (`allows_function_name("eval::surql"|...)`) — enforced by the
//!    engine before this code runs (legacy: `expr::function`, streaming: `BuiltinFunctionExec`);
//! 2. the arbitrary-query subject gate ([`Capabilities::allows_query`]) — an `eval` invocation *is*
//!    an arbitrary query, so it can never bypass the front-door gate by hiding inside a `DEFINE
//!    FUNCTION` / `DEFINE API` body;
//! 3. the dedicated eval subject gate ([`Capabilities::allows_eval_query`]), which defaults to
//!    denied for every subject.
//!
//! `eval::gql` routes through [`crate::gql::parse_with_capabilities`], which
//! derives syntax gating from the live capabilities.
//!
//! ## Subject derivation
//!
//! The subject (guest/record/system) is derived from the *current execution*
//! auth ([`Options::auth`]). This is safe against privilege escalation: auth
//! limiting (`Auth::new_limited`, used by user-defined functions) can only ever
//! narrow the subject class — a record/guest caller is returned unchanged, and
//! a system caller is only ever narrowed within `system` — never raised. So a
//! record-scoped user calling an owner-defined function that calls `eval` is
//! still seen as `record` here and remains denied.
//!
//! ## Scope
//!
//! `eval::surql` evaluates a *single* statement (a read, write, or DDL
//! expression) and returns its value; to run several statements, wrap them in an
//! explicit block `{ ... }` (well-defined last-value semantics). This keeps the
//! scalar return value unambiguous rather than silently returning the last of
//! many results. `eval::gql` accepts one GQL query, which may lower to several
//! SurrealQL statements internally. Transaction-control and session-level
//! top-level statements (`BEGIN`/`CANCEL`/`COMMIT`/`USE`/`LIVE`/`KILL`/`OPTION`/
//! `SHOW`/access statements) are rejected — `eval` runs nested inside the
//! caller's open transaction.
//!
//! ## Isolation & recursion
//!
//! The evaluated query runs in an *isolated* child context (like a user-defined
//! function body): it sees the transaction, capabilities and the explicitly
//! passed bindings, but never the call site's local parameters / scope
//! variables. A query evaluated by `eval` may itself call `eval`; that recursion
//! is bounded by expression-nesting **depth** against `max_computation_depth` —
//! the same number the legacy executor counts. On the legacy path here that is
//! `Options::dive`, decremented per expression step by `Block::compute` and
//! carried in via `opt` (and `compute` runs on a heap `TreeStack`). On the
//! streaming path the planner tracks the same depth and `eval` continues the
//! count when it re-plans its query string (see `exec/function/builtin/eval.rs`),
//! which is what keeps the native stack from growing without limit.
//!
//! ## Execution engine
//!
//! When invoked from the streaming executor (`exec/function/builtin/eval.rs`) the
//! nested query is planned and evaluated on the streaming engine — required for
//! `eval::gql`, since GQL `MATCH` only runs there. When invoked from the
//! legacy `compute` path the nested query runs via [`Block::compute`]; this
//! supports `eval::surql`, while `eval::gql` requires the streaming engine and
//! errors otherwise (consistent with top-level GQL). Both entry points share
//! [`prepare`] for gating, parsing and validation.

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::ctx::{Context, FrozenContext};
use crate::dbs::capabilities::{ArbitraryQueryTarget, Error as CapabilitiesError, EvalQueryTarget};
use crate::dbs::{Capabilities, Force, Options, Variables};
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::exec::Error as ExecError;
use crate::expr::{Block, LogicalPlan, TopLevelExpr};
use crate::fnc::args::Optional;
use crate::iam::Auth;
use crate::val::{Object, Value};

/// Which query language the supplied string is written in.
#[derive(Clone, Copy)]
pub(crate) enum Dialect {
	Surql,
	Gql,
}

impl Dialect {
	pub(crate) fn func_name(self) -> &'static str {
		match self {
			Dialect::Surql => "eval::surql",
			Dialect::Gql => "eval::gql",
		}
	}
}

/// Enforce the eval capability gates, parse the query, and validate it into the
/// block of statements to evaluate. Shared by the legacy `compute` path and the
/// streaming-executor adapters so the gating and statement rules are identical
/// regardless of engine.
///
/// The subject is derived from `auth` (the current execution auth), which is
/// safe because `Auth::new_limited` never raises the subject class.
pub(crate) fn prepare(
	caps: &Capabilities,
	auth: &Auth,
	dialect: Dialect,
	query: &str,
) -> Result<Block> {
	let name = dialect.func_name();

	// --- Gates #2 and #3: subject checks against the current execution auth.
	// Both must pass, so `eval` can never grant more query power than the
	// arbitrary-query gate.
	if !caps.allows_query(&ArbitraryQueryTarget::from(auth))
		|| !caps.allows_eval_query(&EvalQueryTarget::from(auth))
	{
		bail!(CapabilitiesError::FunctionNotAllowed(name.to_string()));
	}

	// --- Parse with the live capabilities (so any SurrealQL experimental
	// gates are honoured) and default parser
	// limits (matching the embedded-scripting `surrealdb.query()` bridge). Both
	// dialects are normalised to a `LogicalPlan`: SurrealQL parses to a `sql::Ast`
	// (converted via `From`); GQL lowers directly to a `PreparedGqlQuery`
	// wrapping a `LogicalPlan`.
	let config = crate::syn::ParserConfig::default();
	let plan: LogicalPlan = match dialect {
		Dialect::Surql => crate::syn::parse_with_capabilities(query, caps, &config)
			.map_err(|e| ExecError::InvalidFunction {
				name: name.to_string(),
				message: e.to_string(),
			})?
			.into(),
		#[cfg(feature = "gql")]
		Dialect::Gql => {
			crate::gql::parse_with_capabilities(query, caps, &config)
				.map_err(|e| ExecError::InvalidFunction {
					name: name.to_string(),
					message: e.to_string(),
				})?
				.0
		}
		#[cfg(not(feature = "gql"))]
		Dialect::Gql => bail!(ExecError::InvalidFunction {
			name: name.to_string(),
			message: "GQL support was not enabled at compile time".to_string(),
		}),
	};

	// --- Collect the expression statements, rejecting top-level-only forms that
	// do not belong nested inside the caller's transaction.
	let mut statements = Vec::with_capacity(plan.expressions.len());
	for expr in plan.expressions {
		match expr {
			TopLevelExpr::Expr(expr) => statements.push(expr),
			_ => bail!(ExecError::InvalidFunction {
				name: name.to_string(),
				message: "only query statements may be evaluated; transaction-control and \
				          session statements (BEGIN, CANCEL, COMMIT, USE, LIVE, KILL, OPTION, \
				          SHOW, and access statements) are not allowed inside eval"
					.to_string(),
			}),
		}
	}
	// `eval::surql` evaluates a *single* statement so its returned value is
	// unambiguous (it is a scalar function — it returns one value, not the
	// per-statement result array that `/sql` would). Multiple statements must be
	// wrapped in an explicit block `{ ... }`, which has well-defined last-value
	// semantics. `eval::gql` is exempt: one GQL query lowers to several SurrealQL
	// statements internally.
	if matches!(dialect, Dialect::Surql) && statements.len() > 1 {
		bail!(ExecError::InvalidFunction {
			name: name.to_string(),
			message: "eval::surql evaluates a single statement; wrap multiple statements in a \
			          block, e.g. eval::surql(\"{ ... }\")"
				.to_string(),
		});
	}
	Ok(Block(statements))
}

pub async fn surql(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(query, Optional(bindings)): (String, Optional<Object>),
) -> Result<Value> {
	run_eval(stk, ctx, opt, doc, Dialect::Surql, query, bindings).await
}

pub async fn gql(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(query, Optional(bindings)): (String, Optional<Object>),
) -> Result<Value> {
	run_eval(stk, ctx, opt, doc, Dialect::Gql, query, bindings).await
}

/// Legacy `compute`-path implementation behind `eval::surql` / `eval::gql`. Runs
/// the nested query via [`Block::compute`] against an isolated child of the
/// current context, reusing the current transaction. `eval::gql` requires the
/// streaming engine, so it surfaces the engine's "requires streaming" error here.
pub(crate) async fn run_eval(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	dialect: Dialect,
	query: String,
	bindings: Option<Object>,
) -> Result<Value> {
	let block = prepare(&ctx.get_capabilities(), opt.auth.as_ref(), dialect, &query)?;

	// --- Run in an *isolated* child context: the evaluated query sees only the
	// transaction, capabilities and the caller-supplied bindings — never the call
	// site's local parameters / scope variables. Protected param names (`access`,
	// `auth`, `token`, `session`) are rejected by `attach_variables`.
	let mut child = Context::new_isolated(ctx);
	if let Some(bindings) = bindings {
		child.attach_variables(Variables::from(bindings))?;
	}
	let child = child.freeze();

	// --- Execute the block in the current transaction. Force is reset so nested
	// execution does not inherit a parent table re-run. Recursion is bounded
	// naturally here: `Block::compute` decrements `opt.dive` on every expression
	// step against `max_computation_depth`, and the budget is carried in via
	// `opt`, so a nested `eval` continues the count rather than resetting it.
	let opt = opt.new_with_force(Force::None);
	crate::legacy::block_compute(&block, stk, &child, &opt, doc).await.catch_return()
}
