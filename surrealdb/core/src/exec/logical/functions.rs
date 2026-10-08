//! The `DEFINE FUNCTION`s a plan calls, as the inlining rule may replace a
//! call by its body.
//!
//! [`Functions::gather`] reads, once per statement, every function the plan
//! names, through the node's function cache
//! ([`crate::kvs::cache::ds::FunctionSnapshot`]), and keeps the ones whose
//! call the rule may replace by the body's expression with the arguments
//! substituted. The decision is made here, with the caller in hand, so the
//! rule itself consults nothing but the plan:
//!
//! - the caller's capabilities allow the call — a denied call stays in place, so it fails where it
//!   would have;
//! - no schema statement of the statement defines, removes or alters the function: a call reached
//!   after one reads the definition it leaves, which is not the one read here;
//! - the function's `PERMISSIONS` clause is `FULL`, so no caller is refused;
//! - the function's `AUTH LIMIT` does not narrow the caller: limiting the caller's authority to it
//!   yields the caller's authority unchanged. Without a principal at bind time only a function
//!   defined with no limit qualifies, since the limit would narrow some caller;
//! - the body is `{ RETURN e }` or `{ e }`, and `e` reads nothing of the calling row — no field,
//!   document or row parameter, no lookup, no nested plan, no call whose body could — so its value
//!   is a function of the arguments alone; it names no parameter but its arguments, since the body
//!   runs in a context of its own where the caller's parameters are not bound; it only reads, calls
//!   no user-defined, scripted, module, silo or model function (a body that calls another function
//!   is never inlined, which also excludes a recursive body), and holds no closure, recursion,
//!   control flow or `MATCHES` / `KNN`.
//!
//! What a call must satisfy at its site — the argument count, that each argument's kind is coerced
//! to the declared kind by identity, that the arguments are pure — is the rule's to check
//! (`rules::inline`).
//!
//! A statement that reads `VERSION`ed state gathers nothing: the definitions
//! it calls are read as of that version, which the shared cache does not
//! serve, and its calls stay runtime-bound. A `VERSION` held by a plan nested
//! in a scalar withholds them too — it is the same statement, and the call
//! beneath it is read at that version like any other.

use std::collections::{HashMap, HashSet};

use super::{BindContext, Caller, visit_binding, visit_deep};
use crate::catalog::Permission;
use crate::dbs::capabilities::Capabilities;
use crate::exec::function::FunctionRegistry;
use crate::exec::logical::reads::reads_row_opaquely;
use crate::expr::logical::{LogicalExpr, LogicalOp};
use crate::expr::{Function, Kind};
use crate::kvs::cache::ds::FunctionSnapshot;

/// The functions a call in the plan may be replaced by.
pub(super) struct Functions {
	inlinable: HashMap<String, InlineBody>,
}

/// One function the rule may inline.
pub(super) struct InlineBody {
	/// The declared arguments, in order: parameter name without its `$`, and
	/// the kind the value is coerced to.
	pub(super) args: Vec<(String, Kind)>,
	/// The kind the result is coerced to; `None` leaves it as the body
	/// produced it.
	pub(super) returns: Option<Kind>,
	/// The body's expression, in terms of `$argument`s only.
	pub(super) expr: LogicalExpr,
}

impl Functions {
	/// No function may be inlined.
	pub(super) fn none() -> Self {
		Self {
			inlinable: HashMap::new(),
		}
	}

	/// Exactly `bodies`, with the site checks left to the rule.
	#[cfg(test)]
	pub(super) fn from_bodies(bodies: impl IntoIterator<Item = (String, InlineBody)>) -> Self {
		Self {
			inlinable: bodies.into_iter().collect(),
		}
	}

	/// The body a call to `fn::name` may be replaced by, when the function
	/// qualifies; see the module documentation.
	pub(super) fn inlinable(&self, name: &str) -> Option<&InlineBody> {
		self.inlinable.get(name)
	}

	/// Whether no function qualifies.
	pub(super) fn is_empty(&self) -> bool {
		self.inlinable.is_empty()
	}

	/// The functions `op` calls that qualify for inlining, read through the
	/// node's cache. A function that cannot be read is left out, so that a
	/// read failure costs an optimisation and never the statement.
	pub(super) async fn gather(
		op: &LogicalOp,
		bx: &BindContext<'_>,
		caller: Option<&Caller>,
	) -> Self {
		if !bx.inlines_definitions || bx.versioned || reads_versioned_state(op) {
			return Self::none();
		}
		let (Some(txn), Some((ns, db))) = (&bx.txn, bx.ns_db) else {
			return Self::none();
		};
		let mut names = called_functions(op);
		if names.is_empty() {
			return Self::none();
		}
		for written in written_functions(op) {
			names.remove(&written);
		}
		let cache = bx.ctx.get_cache();
		let capabilities = bx.ctx.get_capabilities();
		let mut inlinable = HashMap::new();
		for name in names {
			let snapshot =
				match FunctionSnapshot::for_function(cache.as_deref(), txn, ns, db, &name).await {
					Ok(Some(snapshot)) => snapshot,
					Ok(None) => continue,
					Err(error) => {
						tracing::debug!("fn::{name} is not read for inlining: {error:#}");
						continue;
					}
				};
			if let Some(body) = inline_body(&snapshot, &capabilities, caller, bx.registry) {
				inlinable.insert(name, body);
			}
		}
		Self {
			inlinable,
		}
	}
}

/// Whether any plan in `op` reads `VERSION`ed state, a plan nested in a scalar
/// included.
///
/// A definition is read here as of now, which is what the node's function
/// cache serves; a call the rule leaves in place is read as of the statement's
/// version. Inlining under a version would therefore substitute a body the
/// call itself would not have found, so a `VERSION` anywhere in the statement
/// withholds every body — and a subquery carries its own, which the operator
/// inputs alone do not reach.
fn reads_versioned_state(op: &LogicalOp) -> bool {
	let mut versioned = false;
	visit_deep(op, &mut |node| versioned |= matches!(node, LogicalOp::VersionScope(_)));
	versioned
}

/// Every user-defined function `op` calls, at any depth, without the `fn::`
/// prefix. A `MATCH` plan's scalars and a closure body are not looked into:
/// the rule does not rewrite them.
fn called_functions(op: &LogicalOp) -> HashSet<String> {
	let mut names = HashSet::new();
	visit_deep(op, &mut |node| {
		if matches!(node, LogicalOp::Match(_)) {
			return;
		}
		let Ok(()) = node.visit_scalars(&mut |scalar| {
			let _ = visit_binding(scalar, &mut |e| {
				if let LogicalExpr::Call(call) = e
					&& let Function::Custom(name) = &call.receiver
				{
					names.insert(name.clone());
				}
				std::ops::ControlFlow::<()>::Continue(())
			});
			Ok::<_, std::convert::Infallible>(())
		});
	});
	names
}

/// Every user-defined function a schema statement in `op` defines, removes or
/// alters, at any depth, without the `fn::` prefix. A call the statement
/// makes after one of them runs reaches the definition it leaves, which is
/// not the one read here, so its body is withheld: the call stays in place
/// and is bound where it runs.
fn written_functions(op: &LogicalOp) -> HashSet<String> {
	use crate::expr::logical::LogicalDdlStatement;
	use crate::expr::statements::{AlterStatement, DefineStatement, RemoveStatement};
	let mut names = HashSet::new();
	visit_deep(op, &mut |node| {
		let LogicalOp::Ddl(ddl) = node else {
			return;
		};
		let name = match &ddl.statement {
			LogicalDdlStatement::Define(define) => {
				let DefineStatement::Function(stmt) = define.as_ref() else {
					return;
				};
				stmt.name.as_str()
			}
			LogicalDdlStatement::Remove(remove) => {
				let RemoveStatement::Function(stmt) = remove.as_ref() else {
					return;
				};
				stmt.name.as_str()
			}
			LogicalDdlStatement::Alter(alter) => {
				let AlterStatement::Function(stmt) = alter.as_ref() else {
					return;
				};
				stmt.name.as_str()
			}
			LogicalDdlStatement::Rebuild(_) => return,
		};
		names.insert(name.to_owned());
	});
	names
}

/// The body of `snapshot`'s function as the rule may inline it, or `None`
/// when the function does not qualify; see the module documentation.
fn inline_body(
	snapshot: &FunctionSnapshot,
	capabilities: &Capabilities,
	caller: Option<&Caller>,
	registry: &FunctionRegistry,
) -> Option<InlineBody> {
	let definition = snapshot.definition();
	if !capabilities.allows_function_name(&format!("fn::{}", definition.name)) {
		return None;
	}
	if !matches!(definition.permissions, Permission::Full) {
		return None;
	}
	if !limit_is_inert(definition, caller) {
		return None;
	}
	let facts = snapshot.facts();
	if facts.reads_row || !facts.calls.is_empty() {
		return None;
	}
	let expr = facts.return_expr.as_ref()?;
	let args: Vec<(String, Kind)> = definition.args.clone();
	if !body_is_inlinable(expr, &args, registry) {
		return None;
	}
	Some(InlineBody {
		args,
		returns: definition.returns.clone(),
		expr: expr.clone(),
	})
}

/// Whether the function's `AUTH LIMIT` leaves the caller's authority as it is.
fn limit_is_inert(
	definition: &crate::catalog::FunctionDefinition,
	caller: Option<&Caller>,
) -> bool {
	let Ok(limit) = crate::iam::AuthLimit::try_from(&definition.auth_limit) else {
		return false;
	};
	match caller {
		Some(caller) => caller.auth().new_limited(&limit) == **caller.auth(),
		None => definition.auth_limit == crate::catalog::auth::AuthLimit::new_no_limit(),
	}
}

/// Whether `expr`, the body's expression, is a function of `args` alone that
/// the rule may place at a call site; see the module documentation.
fn body_is_inlinable(
	expr: &LogicalExpr,
	args: &[(String, Kind)],
	registry: &FunctionRegistry,
) -> bool {
	if !expr.read_only()
		|| reads_row_opaquely(expr, registry)
		|| super::rules::mentions_index_operator(expr)
	{
		return false;
	}
	!expr.any(|e| match e {
		LogicalExpr::Param(param) => !args.iter().any(|(name, _)| name == param.as_str()),
		LogicalExpr::Call(call) => match &call.receiver {
			Function::Normal(_) => false,
			Function::Custom(_)
			| Function::Script(_)
			| Function::Model(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => true,
		},
		LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => true,
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::If(_) => false,
	})
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::super::test_fixture::{Fixture, db_viewer, owner, record_user};
	use super::*;

	async fn gathered(fixture: &Fixture, session: &crate::dbs::Session, sql: &str) -> Vec<String> {
		let bound = fixture.bound_as(session, vec![]).await;
		let bx = fixture.bind_context(&bound);
		let caller = Caller::new(&bx);
		let functions =
			Functions::gather(&super::super::lower_test(sql), &bx, caller.as_ref()).await;
		let mut names: Vec<String> = functions.inlinable.into_keys().collect();
		names.sort();
		names
	}

	#[tokio::test]
	async fn a_version_anywhere_in_the_statement_withholds_every_body() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE TABLE u SCHEMALESS;
			 DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2; };",
		)
		.await;
		// The body qualifies where the statement reads the current state.
		assert_eq!(gathered(&fixture, &owner(), "SELECT fn::double(a) FROM t").await, ["double"]);
		// A version on the statement withholds it: the call reads the
		// definition as of that version, which is not the one gathered here.
		assert!(
			gathered(
				&fixture,
				&owner(),
				"SELECT fn::double(a) FROM t VERSION d'2024-01-01T00:00:00Z'"
			)
			.await
			.is_empty()
		);
		// So does a version held by a plan nested in a scalar, which the
		// operator inputs alone do not reach.
		assert!(
			gathered(
				&fixture,
				&owner(),
				"SELECT fn::double(a) FROM t WHERE b IN \
				 (SELECT VALUE c FROM u VERSION d'2024-01-01T00:00:00Z')"
			)
			.await
			.is_empty()
		);
	}

	#[tokio::test]
	async fn a_pure_return_body_qualifies_and_the_others_do_not() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2; };
			 DEFINE FUNCTION fn::bare($x: int) { $x + 1 };
			 DEFINE FUNCTION fn::two($x: int) { LET $y = $x; RETURN $y; };
			 DEFINE FUNCTION fn::row() { RETURN c; };
			 DEFINE FUNCTION fn::this() { RETURN $this; };
			 DEFINE FUNCTION fn::sub() { RETURN (SELECT VALUE a FROM t); };
			 DEFINE FUNCTION fn::calls($x: int) { RETURN fn::double($x); };
			 DEFINE FUNCTION fn::outer($x: int) { RETURN $x + $other; };
			 DEFINE FUNCTION fn::guarded($x: int) { RETURN $x; } PERMISSIONS WHERE $auth.admin = true;
			 DEFINE FUNCTION fn::closed($x: int) { RETURN $x; } PERMISSIONS NONE;
			 DEFINE FUNCTION fn::searching($x: string) { RETURN $x @@ 'y'; };",
		)
		.await;
		let names = gathered(
			&fixture,
			&owner(),
			"SELECT fn::double(a), fn::bare(a), fn::two(a), fn::row(), fn::this(), fn::sub(), \
			 fn::calls(a), fn::outer(a), fn::guarded(a), fn::closed(a), fn::searching(a) FROM t",
		)
		.await;
		assert_eq!(names, ["bare", "double"]);
	}

	#[tokio::test]
	async fn an_auth_limit_that_narrows_the_caller_disqualifies() {
		// Defined by a database editor, the function carries that limit: the
		// owner would be narrowed by it, the editor is not.
		let editor = crate::dbs::Session::for_level(
			crate::iam::Level::Database("test".to_owned(), "test".to_owned()),
			crate::iam::Role::Editor,
		)
		.with_ns("test")
		.with_db("test");
		let fixture = Fixture::new("DEFINE TABLE t SCHEMALESS;").await;
		fixture.execute_as(&editor, "DEFINE FUNCTION fn::editor($x: int) { RETURN $x; };").await;
		assert_eq!(
			gathered(&fixture, &owner(), "SELECT fn::editor(a) FROM t").await,
			Vec::<String>::new()
		);
		assert_eq!(gathered(&fixture, &editor, "SELECT fn::editor(a) FROM t").await, ["editor"]);
		let _ = db_viewer();
	}

	#[tokio::test]
	async fn a_versioned_statement_and_an_undefined_function_gather_nothing() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2; };",
		)
		.await;
		assert!(
			gathered(
				&fixture,
				&owner(),
				"SELECT fn::double(a) FROM t VERSION d'2024-01-01T00:00:00Z'"
			)
			.await
			.is_empty()
		);
		assert!(gathered(&fixture, &owner(), "SELECT fn::ghost(a) FROM t").await.is_empty());
		// A record user has no capability to lose here: the datastore allows
		// every function.
		assert_eq!(
			gathered(&fixture, &record_user(), "SELECT fn::double(a) FROM t").await,
			["double"]
		);
	}

	#[tokio::test]
	async fn a_function_the_statement_redefines_is_withheld() {
		let fixture = Fixture::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2; };
			 DEFINE FUNCTION fn::bare($x: int) { $x + 1 };",
		)
		.await;
		for sql in [
			"{ DEFINE FUNCTION OVERWRITE fn::double($x: int) { RETURN $x * 3; }; RETURN [fn::double(1), fn::bare(1)] }",
			"{ IF true { REMOVE FUNCTION fn::double }; RETURN [fn::double(1), fn::bare(1)] }",
			"{ FOR $i IN 0..2 { ALTER FUNCTION fn::double DROP COMMENT }; RETURN [fn::double(1), fn::bare(1)] }",
		] {
			assert_eq!(gathered(&fixture, &owner(), sql).await, ["bare"], "`{sql}`");
		}
		assert_eq!(
			gathered(
				&fixture,
				&owner(),
				"{ DEFINE FUNCTION fn::other() { 1 }; RETURN [fn::double(1), fn::bare(1)] }"
			)
			.await,
			["bare", "double"]
		);
	}
}
