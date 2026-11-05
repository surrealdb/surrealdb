//! `eval::surql` / `eval::gql` — evaluate a nested query string in the current
//! transaction and session context.
//!
//! ## Gating (defense in depth)
//!
//! A call must pass *all* of:
//! 1. the function-family capability (`allows_function_name("eval::surql"|...)`) — enforced by the
//!    engine before this code runs (`BuiltinFunctionExec`);
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
//! is bounded by expression-nesting **depth** against `max_computation_depth`:
//! the planner tracks the depth and `eval` continues the count when it re-plans
//! its query string (see `exec/function/builtin/eval.rs`), which is what keeps
//! the native stack from growing without limit.
//!
//! ## Execution engine
//!
//! The nested query is planned and evaluated on the engine, from
//! inside a plan (`exec/function/builtin/eval.rs`) and from a function body
//! ([`surql`], [`gql`]) alike. GQL `MATCH` runs only there. Both entry points
//! share [`prepare`] for gating, parsing and validation.

use anyhow::{Result, bail};

use crate::ctx::FrozenContext;
use crate::dbs::capabilities::{ArbitraryQueryTarget, Error as CapabilitiesError, EvalQueryTarget};
use crate::dbs::{Capabilities, Options};
use crate::doc::CursorDoc;
use crate::exec::Error as ExecError;
use crate::fnc::args::Optional;
use crate::iam::Auth;
use crate::kvs::Dialect;
use crate::val::{Object, Value};

/// The `eval::*` builtin that evaluates a string in `dialect`, for the
/// capability gate and the error messages.
fn func_name(dialect: Dialect) -> &'static str {
	match dialect {
		Dialect::SurrealQl => "eval::surql",
		Dialect::Gql => "eval::gql",
		Dialect::PgSql => "eval::pgsql",
		Dialect::MySql => "eval::mysql",
	}
}

/// What a validated `eval` query turned out to be.
pub(crate) enum Prepared {
	/// The surface statements to evaluate in order, the last one's value being
	/// the result. At most one: the dialect restricts `eval::surql` to a single
	/// statement, and an empty query evaluates to `NONE`. A statement is
	/// converted the way any surface statement is, by lowering.
	Statements(Vec<crate::expr::Expr>),
	/// A query whose front end emits a plan rather than statements. GQL is the
	/// one front end that emits a plan, so without it nothing constructs this
	/// variant.
	#[cfg_attr(not(feature = "gql"), allow(dead_code))]
	Plan(Box<crate::expr::logical::LogicalOp>),
}

/// Enforce the eval capability gates, parse the query, and validate it into the
/// statements — or the plan — to evaluate. Shared by every entry point so the
/// gating and statement rules are identical.
///
/// The subject is derived from `auth` (the current execution auth), which is
/// safe because `Auth::new_limited` never raises the subject class.
pub(crate) fn prepare(
	caps: &Capabilities,
	auth: &Auth,
	dialect: Dialect,
	query: &str,
) -> Result<Prepared> {
	let name = func_name(dialect);

	// --- Gates #2 and #3: subject checks against the current execution auth.
	// Both must pass, so `eval` can never grant more query power than the
	// arbitrary-query gate.
	if !caps.allows_query(&ArbitraryQueryTarget::from(auth))
		|| !caps.allows_eval_query(&EvalQueryTarget::from(auth))
	{
		bail!(CapabilitiesError::FunctionNotAllowed(name.to_string()));
	}

	// --- Parse with the live capabilities (so any SurrealQL experimental
	// gates are honoured) and default parser limits (matching the
	// embedded-scripting `surrealdb.query()` bridge). SurrealQL parses to a
	// `parser::Query`. GQL lowers straight to the plan it runs as, which has no
	// statements to interleave, so it is answered here.
	let config = crate::syn::ParserConfig::default();
	let ast =
		match dialect {
			Dialect::SurrealQl => crate::syn::parse_with_capabilities(query, caps, &config)
				.map_err(|e| ExecError::InvalidFunction {
					name: name.to_string(),
					message: e.to_string(),
				})?,
			#[cfg(feature = "gql")]
			Dialect::Gql => {
				let prepared =
					crate::gql::parse_with_capabilities(query, caps, &config).map_err(|e| {
						ExecError::InvalidFunction {
							name: name.to_string(),
							message: e.to_string(),
						}
					})?;
				return Ok(Prepared::Plan(Box::new(prepared.0)));
			}
			#[cfg(not(feature = "gql"))]
			Dialect::Gql => bail!(surrealdb_types::Error::configuration(
				"This build does not support GQL queries".to_string(),
				surrealdb_types::ConfigurationError::DialectNotSupported,
			)),
			#[cfg(feature = "postgres")]
			Dialect::PgSql => {
				let plans = crate::pgsql::parse_to_plan(query, caps, &config).map_err(|e| {
					ExecError::InvalidFunction {
						name: name.to_string(),
						message: e.to_string(),
					}
				})?;
				if let Some(first) = plans.into_iter().next() {
					return Ok(Prepared::Plan(Box::new(first)));
				}
				return Ok(Prepared::Statements(Vec::new()));
			}
			#[cfg(not(feature = "postgres"))]
			Dialect::PgSql => bail!(surrealdb_types::Error::configuration(
				"This build does not support PostgreSQL queries".to_string(),
				surrealdb_types::ConfigurationError::DialectNotSupported,
			)),
			#[cfg(feature = "mysql")]
			Dialect::MySql => {
				let plans = crate::mysql::parse_to_plan(query, caps, &config).map_err(|e| {
					ExecError::InvalidFunction {
						name: name.to_string(),
						message: e.to_string(),
					}
				})?;
				if let Some(first) = plans.into_iter().next() {
					return Ok(Prepared::Plan(Box::new(first)));
				}
				return Ok(Prepared::Statements(Vec::new()));
			}
			#[cfg(not(feature = "mysql"))]
			Dialect::MySql => bail!(surrealdb_types::Error::configuration(
				"This build does not support MySQL queries".to_string(),
				surrealdb_types::ConfigurationError::DialectNotSupported,
			)),
		};

	// --- Collect the expression statements, rejecting top-level-only forms that
	// do not belong nested inside the caller's transaction.
	let mut statements = Vec::with_capacity(ast.expressions.len());
	for expr in ast.expressions {
		if let surrealdb_expr::parser::TopLevelExpr::Expr(expr) = expr {
			statements.push(expr);
		} else {
			bail!(ExecError::InvalidFunction {
				name: name.to_string(),
				message: "only query statements may be evaluated; transaction-control and \
				          session statements (BEGIN, CANCEL, COMMIT, USE, LIVE, KILL, OPTION, \
				          SHOW, and access statements) are not allowed inside eval"
					.to_string(),
			});
		}
	}
	// `eval::surql` evaluates a *single* statement so its returned value is
	// unambiguous (it is a scalar function — it returns one value, not the
	// per-statement result array that `/sql` would). Multiple statements must be
	// wrapped in an explicit block `{ ... }`, which has well-defined last-value
	// semantics.
	if statements.len() > 1 {
		bail!(ExecError::InvalidFunction {
			name: name.to_string(),
			message: "eval::surql evaluates a single statement; wrap multiple statements in a \
			          block, e.g. eval::surql(\"{ ... }\")"
				.to_string(),
		});
	}
	Ok(Prepared::Statements(statements))
}

pub async fn surql(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(query, Optional(bindings)): (String, Optional<Object>),
) -> Result<Value> {
	run_eval(ctx, opt, doc, Dialect::SurrealQl, query, bindings).await
}

pub async fn gql(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(query, Optional(bindings)): (String, Optional<Object>),
) -> Result<Value> {
	run_eval(ctx, opt, doc, Dialect::Gql, query, bindings).await
}

/// `eval::surql` / `eval::gql` for a caller that holds the handles of a
/// function body rather than a plan's evaluation frame: the frame is built
/// from `ctx` and `opt`, with `doc` as the row the call reads, and the query
/// runs on the engine exactly as it does from inside a plan.
async fn run_eval(
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	dialect: Dialect,
	query: String,
	bindings: Option<Object>,
) -> Result<Value> {
	let exec = crate::exec::entry::frame(ctx, opt, doc).await;
	let frame = crate::exec::entry::eval_frame(&exec);
	crate::exec::function::evaluate_streaming(&frame, dialect, query, bindings).await
}
