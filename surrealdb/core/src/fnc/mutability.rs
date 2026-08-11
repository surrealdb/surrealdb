//! Resolves whether an expression can modify data once the stored bodies of
//! the user-defined functions it calls are taken into account.
//!
//! [`FunctionFacts`] carries the per-body facts; this module walks the call
//! graph they induce against a catalog snapshot and combines them into the
//! two answers different callers need:
//!
//! - [`ResolvedMutability::provably_writes`]: a data-modifying statement is reachable through
//!   stored bodies alone. This is the *precise* polarity the definition-time checks use — it never
//!   condemns an opaque callable (a script, an `eval`, a closure arriving as data), because those
//!   can be pure and the runtime write refusal already backstops them.
//! - [`ResolvedMutability::possibly_writes`]: anything reachable might write, including opaque
//!   callables and callees that are not defined in this snapshot. This is the *conservative*
//!   polarity a planner must use before treating an expression as read-only.
//!
//! The answers are derived from the bodies in the snapshot, never from stored
//! attributes, so they cannot drift from what execution against the same
//! snapshot will run. Cycles in the call graph (recursion, mutual recursion)
//! are handled by the visited set: a cycle contributes whatever facts its
//! members carry, nothing more.

use std::collections::{BTreeSet, HashMap, HashSet};

use surrealdb_types::ToSql;

use crate::catalog::providers::{ApiProvider, BucketProvider, DatabaseProvider, TableProvider};
use crate::catalog::{DatabaseId, Error as CatalogError, NamespaceId, Permission};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::exec::Error as ExecError;
use crate::expr::Expr;
use crate::expr::function_facts::FunctionFacts;
use crate::kvs::Transaction;

/// Where a provable write was found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WriteSource {
	/// The starting expression itself contains a data-modifying statement.
	Body,
	/// The stored body of this function (name without the `fn::` prefix)
	/// does, and the function is reachable from the starting expression.
	Function(String),
}

/// The combined mutability answer for one starting expression.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ResolvedMutability {
	/// The first provable write found, if any. The walk stops there, so the
	/// other fields are meaningful only when this is `None`.
	pub(crate) write: Option<WriteSource>,
	/// Something reachable invokes a body that cannot be inspected.
	pub(crate) opaque_effects: bool,
	/// A reachable call names a function this snapshot does not define.
	pub(crate) undefined_callee: bool,
}

impl ResolvedMutability {
	/// A write is reachable through stored bodies alone.
	pub(crate) fn provably_writes(&self) -> bool {
		self.write.is_some()
	}

	/// A write may happen; the over-approximation planners must respect.
	pub(crate) fn possibly_writes(&self) -> bool {
		self.write.is_some() || self.opaque_effects || self.undefined_callee
	}
}

/// Resolve the mutability of an expression with the given [`FunctionFacts`]
/// against the catalog snapshot `txn` reads from.
///
/// `overrides` substitutes facts for named functions in place of their stored
/// bodies; a definition statement passes the body it is *about to store* so
/// that self-recursion resolves against the new body, not the old one.
pub(crate) async fn resolve_mutability(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	root: &FunctionFacts,
	overrides: &HashMap<String, FunctionFacts>,
) -> anyhow::Result<ResolvedMutability> {
	let mut resolved = ResolvedMutability {
		write: root.direct_writes.then_some(WriteSource::Body),
		opaque_effects: root.opaque_effects,
		undefined_callee: false,
	};

	let mut visited: HashSet<String> = root.calls.iter().cloned().collect();
	// Seed from the ordered `calls` set, not from `visited`, so which writer is
	// reported first when several are reachable does not depend on HashSet
	// iteration order (a nondeterministic error message otherwise).
	let mut pending: Vec<String> = root.calls.iter().cloned().collect();

	while resolved.write.is_none() {
		let Some(name) = pending.pop() else {
			break;
		};
		let facts = match overrides.get(&name) {
			Some(facts) => facts.clone(),
			None => match txn.get_db_function(ns, db, &name, None).await {
				Ok(def) => def.block.function_facts(),
				Err(e) => {
					if matches!(
						e.downcast_ref::<CatalogError>(),
						Some(CatalogError::FcNotFound { .. })
					) {
						// Late binding: the callee may be defined later, at
						// which point its own definition is checked against
						// this expression's requirement.
						resolved.undefined_callee = true;
						continue;
					}
					return Err(e);
				}
			},
		};
		if facts.direct_writes {
			resolved.write = Some(WriteSource::Function(name));
			break;
		}
		resolved.opaque_effects |= facts.opaque_effects;
		for call in facts.calls {
			if visited.insert(call.clone()) {
				pending.push(call);
			}
		}
	}

	Ok(resolved)
}

/// The name of a writing function provably reachable from `facts`' calls, if
/// any.
///
/// A write in the expression itself (`WriteSource::Body`) is deliberately not
/// reported: each caller carries its own, differently-scoped direct-write
/// check, and this helper only extends those checks to see through function
/// calls. The facts' `direct_writes` is masked so a direct write cannot
/// pre-empt the call-graph walk.
/// `overrides` substitutes facts for named functions in place of their stored
/// bodies, so a caller mid-definition can resolve a call to the function it is
/// about to store against the *new* body (see
/// [`ensure_function_stays_read_only_for_consumers`]).
pub(crate) async fn provable_writer_via_calls(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	facts: &FunctionFacts,
	overrides: &HashMap<String, FunctionFacts>,
) -> anyhow::Result<Option<String>> {
	if facts.calls.is_empty() {
		return Ok(None);
	}
	let calls_only = FunctionFacts {
		direct_writes: false,
		calls: facts.calls.clone(),
		opaque_effects: false,
	};
	let resolved = resolve_mutability(txn, ns, db, &calls_only, overrides).await?;
	Ok(match resolved.write {
		Some(WriteSource::Function(name)) => Some(name),
		Some(WriteSource::Body) | None => None,
	})
}

/// Union of the facts of every `Specific` guard among `perms`.
pub(crate) fn guard_facts<'a>(perms: impl IntoIterator<Item = &'a Permission>) -> FunctionFacts {
	let mut facts = FunctionFacts::default();
	for perm in perms {
		if let Permission::Specific(expr) = perm {
			let f = expr.function_facts();
			facts.direct_writes |= f.direct_writes;
			facts.opaque_effects |= f.opaque_effects;
			facts.calls.extend(f.calls);
		}
	}
	facts
}

/// Refuse the definition when any of its permission guards calls a function
/// whose stored body provably reaches a write.
///
/// A permission guard is evaluated on reads under a frame that refuses
/// writes at runtime, so a guard that reaches one can never succeed; this
/// surfaces the failure on the definition instead. Skipped under import so
/// existing exports keep restoring — the runtime refusal still holds for
/// whatever an import brings in.
pub(crate) async fn ensure_guards_call_read_only<'a>(
	ctx: &FrozenContext,
	opt: &Options,
	kind: &'static str,
	name: String,
	perms: impl IntoIterator<Item = &'a Permission>,
) -> anyhow::Result<()> {
	if opt.import {
		return Ok(());
	}
	let facts = guard_facts(perms);
	if facts.calls.is_empty() {
		return Ok(());
	}
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	if let Some(function) =
		provable_writer_via_calls(&ctx.tx(), ns, db, &facts, &HashMap::new()).await?
	{
		anyhow::bail!(ExecError::PermissionWriteViaFunction {
			kind,
			name,
			function,
		});
	}
	Ok(())
}

/// Enforce the read-only rule on a definition's permission clauses, split by
/// how the clause is evaluated:
///
/// - `read_clauses` (`SELECT`, and the single access-gate permission on
///   param/function/module/bucket/config) are evaluated on reads and are **always** required to be
///   read-only (GHSA-66r2-5gwj-gxm2), directly or through a function call.
/// - `write_clauses` (`create`/`update`/`delete` on tables, `create`/`update` on fields) are
///   evaluated during a write; they are required to be read-only too **unless** the
///   `mutable_permissions` capability is enabled, in which case a side effect there is permitted
///   and runs at request time.
///
/// The write-clause check is skipped under import (like
/// [`ensure_guards_call_read_only`]) so existing exports keep restoring; the
/// runtime frame still governs whether the write actually executes.
pub(crate) async fn ensure_permission_clauses_read_only<'a>(
	ctx: &FrozenContext,
	opt: &Options,
	kind: &'static str,
	name: String,
	read_clauses: impl IntoIterator<Item = &'a Permission>,
	write_clauses: impl IntoIterator<Item = &'a Permission>,
) -> anyhow::Result<()> {
	// SELECT / access-gate clauses: always read-only.
	let read_clauses: Vec<&Permission> = read_clauses.into_iter().collect();
	for perm in &read_clauses {
		if perm.has_direct_write() {
			anyhow::bail!(ExecError::PermissionClauseNotReadonly {
				kind,
				name,
			});
		}
	}
	ensure_guards_call_read_only(ctx, opt, kind, name.clone(), read_clauses).await?;

	// Write-triggered clauses: read-only unless the capability opens them up.
	// Skipped under import so a valid export always restores.
	let capability_on = ctx
		.get_capabilities()
		.allows_experimental(&crate::dbs::capabilities::ExperimentalTarget::MutablePermissions);
	if capability_on || opt.import {
		return Ok(());
	}
	let write_clauses: Vec<&Permission> = write_clauses.into_iter().collect();
	if write_clauses.iter().any(|p| p.has_direct_write()) {
		anyhow::bail!(ExecError::MutablePermissionsDisabled {
			kind,
			name,
		});
	}
	let facts = guard_facts(write_clauses);
	if !facts.calls.is_empty() {
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		if provable_writer_via_calls(&ctx.tx(), ns, db, &facts, &HashMap::new()).await?.is_some() {
			anyhow::bail!(ExecError::MutablePermissionsDisabled {
				kind,
				name,
			});
		}
	}
	Ok(())
}

/// Human descriptions of every catalog object that requires `target` to stay
/// read-only: COMPUTED field bodies and permission guards whose function
/// calls can reach it, directly or through other stored bodies.
///
/// `new_calls` substitutes for the target's stored edges, so an `OVERWRITE`
/// resolves against the body about to be stored. This runs when a definition
/// statement is about to store a provably-writing body — admin frequency —
/// so it prefers a straightforward fixpoint over the whole (small, cached)
/// catalog to any incremental bookkeeping.
pub(crate) async fn read_only_consumers_reaching(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	target: &str,
	new_calls: &BTreeSet<String>,
) -> anyhow::Result<Vec<String>> {
	// Forward call edges of every stored function, with the in-flight
	// definition's edges substituted for its stored ones.
	let functions = txn.all_db_functions(ns, db, None).await?;
	let mut forward: HashMap<String, BTreeSet<String>> =
		functions.iter().map(|f| (f.name.to_string(), f.block.function_facts().calls)).collect();
	forward.insert(target.to_owned(), new_calls.clone());

	// Everything that can reach `target`, itself included: grow to fixpoint.
	let mut affected: HashSet<String> = HashSet::from([target.to_owned()]);
	loop {
		let before = affected.len();
		for (name, calls) in forward.iter() {
			if !affected.contains(name) && calls.iter().any(|c| affected.contains(c)) {
				affected.insert(name.clone());
			}
		}
		if affected.len() == before {
			break;
		}
	}

	let reaches = |expr: &Expr| expr.function_facts().calls.iter().any(|c| affected.contains(c));
	let guard_reaches = |perm: &Permission| match perm {
		Permission::Specific(expr) => reaches(expr),
		Permission::None | Permission::Full => false,
	};

	let mut consumers = Vec::new();

	// Only clauses that must stay read-only impose a requirement on a function
	// they call: SELECT permission guards (evaluated on reads) and COMPUTED
	// field bodies. A create/update/delete permission guard may itself write
	// when the `mutable_permissions` capability is on (and is refused at its own
	// definition when off), so it does not pin its callees read-only.
	for tb in txn.all_tb(ns, db, None).await?.iter() {
		if guard_reaches(&tb.permissions.select) {
			consumers.push(format!("PERMISSIONS FOR select ON TABLE {}", tb.name));
		}
		for fd in txn.all_tb_fields(ns, db, &tb.name, None).await?.iter() {
			if let Some(computed) = &fd.computed
				&& reaches(computed)
			{
				consumers.push(format!("COMPUTED field {}.{}", tb.name, fd.name.to_sql()));
			}
			if guard_reaches(&fd.select_permission) {
				consumers.push(format!(
					"PERMISSIONS FOR select ON FIELD {}.{}",
					tb.name,
					fd.name.to_sql()
				));
			}
		}
	}

	// The target's own stored guard is skipped: the definition replacing it
	// checks its new guard at its own site.
	for f in functions.iter().filter(|f| f.name.as_str() != target) {
		if guard_reaches(&f.permissions) {
			consumers.push(format!("PERMISSIONS ON FUNCTION fn::{}", f.name));
		}
	}
	for p in txn.all_db_params(ns, db, None).await?.iter() {
		if guard_reaches(&p.permissions) {
			consumers.push(format!("PERMISSIONS ON PARAM ${}", p.name));
		}
	}
	for m in txn.all_db_models(ns, db, None).await?.iter() {
		if guard_reaches(&m.permissions) {
			consumers.push(format!("PERMISSIONS ON MODEL {}", m.name));
		}
	}
	for m in txn.all_db_modules(ns, db, None).await?.iter() {
		if guard_reaches(&m.permissions) {
			let name = m.name.as_deref().unwrap_or("(unnamed)");
			consumers.push(format!("PERMISSIONS ON MODULE {name}"));
		}
	}
	for b in txn.all_db_buckets(ns, db, None).await?.iter() {
		if guard_reaches(&b.permissions) {
			consumers.push(format!("PERMISSIONS ON BUCKET {}", b.name));
		}
	}
	for a in txn.all_db_apis(ns, db, None).await?.iter() {
		// An API carries a permission guard on its top-level config and one on
		// each method-specific action config; every one of them is evaluated on
		// a matching request under the no-write frame.
		if guard_reaches(&a.config.permissions) {
			consumers.push(format!("PERMISSIONS ON API {}", a.path));
		}
		for action in a.actions.iter() {
			if guard_reaches(&action.config.permissions) {
				consumers.push(format!("PERMISSIONS ON API {} action", a.path));
			}
		}
	}
	// The database-wide `DEFINE CONFIG API` guard applies to every API request
	// that does not override it, so it is a consumer too.
	for c in txn.all_db_configs(ns, db, None).await?.iter() {
		if let crate::catalog::ConfigDefinition::Api(api) = c
			&& guard_reaches(&api.permissions)
		{
			consumers.push("PERMISSIONS ON CONFIG API".to_owned());
		}
	}

	Ok(consumers)
}

/// Refuse a function definition whose new body provably writes while
/// read-only consumers depend on the name, naming those consumers.
///
/// Skipped under import (exports emit functions before the schema objects
/// that consume them, so nothing to check; and existing exports must keep
/// restoring). Best-effort by design: two concurrent definitions can each
/// pass this on their own snapshot, so the runtime write refusal remains the
/// enforcement — this exists to land the failure on the definition, with a
/// reason, in the ordinary sequential case.
pub(crate) async fn ensure_function_stays_read_only_for_consumers(
	ctx: &FrozenContext,
	opt: &Options,
	name: &str,
	block: &crate::expr::Block,
	guard: &Permission,
) -> anyhow::Result<()> {
	if opt.import {
		return Ok(());
	}
	let facts = block.function_facts();
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	let txn = ctx.tx();

	// Resolve the new body with itself substituted, so self-recursion is
	// answered against the body about to be stored, not the stored one.
	let overrides = HashMap::from([(name.to_owned(), facts.clone())]);
	let resolved = resolve_mutability(&txn, ns, db, &facts, &overrides).await?;
	if !resolved.provably_writes() {
		return Ok(());
	}

	let mut consumers = read_only_consumers_reaching(&txn, ns, db, name, &facts.calls).await?;

	// The function's own permission guard is evaluated on every call under the
	// no-write frame, so if it reaches this now-writing body — a self-referential
	// guard being the plain case — the function becomes uncallable.
	// `read_only_consumers_reaching` skips the target's *stored* guard (it is
	// stale during a redefinition); resolve the *new* guard against the *new*
	// body here instead. The separate guard check at the DEFINE/ALTER site
	// resolves the same guard against other functions' stored bodies, so the two
	// together cover both directions.
	if provable_writer_via_calls(&txn, ns, db, &guard_facts([guard]), &overrides).await?.is_some() {
		consumers.insert(0, format!("its own PERMISSIONS clause (fn::{name})"));
	}

	if consumers.is_empty() {
		return Ok(());
	}

	// Name enough consumers to act on without unbounded error text.
	const LISTED: usize = 3;
	let mut listed = consumers.iter().take(LISTED).cloned().collect::<Vec<_>>().join(", ");
	if consumers.len() > LISTED {
		listed.push_str(&format!(", and {} more", consumers.len() - LISTED));
	}
	anyhow::bail!(ExecError::FunctionRequiredReadOnly {
		name: name.to_owned(),
		consumers: listed,
	});
}

#[cfg(all(test, feature = "kv-mem"))]
// The combined cfg hides the test-ness from clippy's `allow-unwrap-in-tests`.
#[allow(clippy::unwrap_used)]
mod tests {
	use std::collections::HashMap;
	use std::sync::Arc;

	use super::*;
	use crate::dbs::Session;
	use crate::kvs::{Datastore, TransactionType};

	/// A datastore with `test`/`test` and the given statements applied.
	async fn datastore_with(setup: &str) -> Arc<Datastore> {
		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE test", &sess, None).await.unwrap();
		for res in ds.execute(setup, &sess, None).await.unwrap() {
			res.result.unwrap();
		}
		ds
	}

	/// The consumer descriptions `read_only_consumers_reaching` finds for a
	/// function `target` that is not itself being changed.
	async fn consumers_of(ds: &Datastore, target: &str) -> Vec<String> {
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
		let stored =
			txn.get_db_function(db.namespace_id, db.database_id, target, None).await.unwrap();
		let consumers = read_only_consumers_reaching(
			&txn,
			db.namespace_id,
			db.database_id,
			target,
			&stored.block.function_facts().calls,
		)
		.await
		.unwrap();
		txn.cancel().await.unwrap();
		consumers
	}

	/// A guard on an API's top-level config, on a method-specific action
	/// config, and on the database-wide `DEFINE CONFIG API` are all consumers
	/// that require a function they call to stay read-only.
	#[tokio::test]
	async fn api_action_and_config_guards_are_consumers() {
		let ds = datastore_with(
			"DEFINE FUNCTION fn::guard() { RETURN true; };
			 DEFINE CONFIG API PERMISSIONS WHERE fn::guard();
			 DEFINE API \"/x\" FOR get PERMISSIONS WHERE fn::guard() \
			 THEN { { status: 200, body: {} } };",
		)
		.await;
		let consumers = consumers_of(&ds, "guard").await;
		assert!(
			consumers.iter().any(|c| c.contains("API /x") && c.contains("action")),
			"missing the per-action API guard: {consumers:?}"
		);
		assert!(
			consumers.iter().any(|c| c == "PERMISSIONS ON CONFIG API"),
			"missing the database-wide API config guard: {consumers:?}"
		);
	}

	/// Parse a standalone expression and resolve it against the datastore.
	async fn resolve(ds: &Datastore, expression: &str) -> ResolvedMutability {
		let expr: crate::expr::Expr = crate::syn::expr(expression).unwrap().into();
		let facts = expr.function_facts();
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
		let resolved =
			resolve_mutability(&txn, db.namespace_id, db.database_id, &facts, &HashMap::new())
				.await
				.unwrap();
		txn.cancel().await.unwrap();
		resolved
	}

	/// Facts-level classification, one fixture per callable kind. The
	/// resolver tests above exercise the combinations; this pins what each
	/// shape contributes on its own.
	#[test]
	fn facts_classify_each_callable_kind() {
		let facts = |expression: &str| {
			let expr: crate::expr::Expr = crate::syn::expr(expression).unwrap().into();
			expr.function_facts()
		};

		// A builtin that cannot evaluate statements contributes nothing.
		assert_eq!(facts("math::abs(-1)"), FunctionFacts::default());

		// Custom calls are edges, at any depth including argument position.
		let f = facts("fn::a(fn::b(1))");
		assert!(!f.direct_writes && !f.opaque_effects);
		assert_eq!(f.calls.len(), 2);

		// Statement-evaluating builtins and calls on non-literal targets are
		// opaque; a script body would be too, but scripts cannot be parsed
		// without the scripting feature so the builtin set stands in here.
		for expression in ["api::invoke('/x')", "eval::surql('RETURN 1')", "$fn(1)"] {
			let f = facts(expression);
			assert!(f.opaque_effects && !f.direct_writes, "{expression}");
		}

		// A closure literal's body is visible wherever the literal appears,
		// and calling the literal is not opaque.
		for expression in ["|| { CREATE log }", "(|| { CREATE log })()"] {
			let f = facts(expression);
			assert!(f.direct_writes && !f.opaque_effects, "{expression}");
		}

		// Finding a write must not stop the walk: the call set feeds a call
		// graph and has to be complete.
		let f = facts("{ CREATE log; fn::after(); }");
		assert!(f.direct_writes);
		assert!(f.calls.contains("after"));
	}

	#[tokio::test]
	async fn a_direct_write_is_the_body_source() {
		let ds = datastore_with("").await;
		let resolved = resolve(&ds, "(CREATE log)").await;
		assert_eq!(resolved.write, Some(WriteSource::Body));
	}

	#[tokio::test]
	async fn a_write_two_calls_deep_names_the_writing_function() {
		let ds = datastore_with(
			"DEFINE FUNCTION fn::sink() { CREATE log; RETURN 1; };
			 DEFINE FUNCTION fn::relay() { RETURN fn::sink(); };",
		)
		.await;
		let resolved = resolve(&ds, "fn::relay()").await;
		assert_eq!(resolved.write, Some(WriteSource::Function("sink".to_owned())));
	}

	#[tokio::test]
	async fn a_write_free_cycle_resolves_clean() {
		let ds = datastore_with(
			"DEFINE FUNCTION fn::ping($n: number) { RETURN IF $n > 0 { fn::pong($n - 1) } ELSE { 0 }; };
			 DEFINE FUNCTION fn::pong($n: number) { RETURN IF $n > 0 { fn::ping($n - 1) } ELSE { 0 }; };",
		)
		.await;
		let resolved = resolve(&ds, "fn::ping(3)").await;
		assert_eq!(resolved.write, None);
		assert!(!resolved.possibly_writes());
	}

	#[tokio::test]
	async fn a_cycle_carrying_a_write_names_its_writer() {
		let ds = datastore_with(
			"DEFINE FUNCTION fn::a($n: number) { RETURN IF $n > 0 { fn::b($n - 1) } ELSE { 0 }; };
			 DEFINE FUNCTION fn::b($n: number) { CREATE log; RETURN fn::a($n); };",
		)
		.await;
		let resolved = resolve(&ds, "fn::a(3)").await;
		assert_eq!(resolved.write, Some(WriteSource::Function("b".to_owned())));
	}

	#[tokio::test]
	async fn an_undefined_callee_is_possible_but_not_provable() {
		let ds = datastore_with("").await;
		let resolved = resolve(&ds, "fn::ghost()").await;
		assert_eq!(resolved.write, None);
		assert!(resolved.undefined_callee);
		assert!(resolved.possibly_writes());
	}

	#[tokio::test]
	async fn opaque_callables_are_possible_but_not_provable() {
		let ds = datastore_with("").await;
		for expression in ["eval::surql('RETURN 1')", "$fn(1)"] {
			let resolved = resolve(&ds, expression).await;
			assert_eq!(resolved.write, None, "{expression}");
			assert!(resolved.opaque_effects, "{expression}");
			assert!(resolved.possibly_writes(), "{expression}");
		}
	}

	#[tokio::test]
	async fn a_write_in_a_call_argument_is_direct() {
		let ds = datastore_with("DEFINE FUNCTION fn::id($x: any) { RETURN $x; };").await;
		for expression in ["fn::id((CREATE log).id)", "$fn((CREATE log).id)"] {
			let resolved = resolve(&ds, expression).await;
			assert_eq!(resolved.write, Some(WriteSource::Body), "{expression}");
		}
	}

	#[tokio::test]
	async fn an_override_substitutes_for_the_stored_body() {
		let ds = datastore_with("DEFINE FUNCTION fn::self() { RETURN fn::self(); };").await;
		// The stored body is clean; the override (a body about to be stored)
		// writes, and self-recursion must resolve against it.
		let new_body: crate::expr::Expr = crate::syn::expr("(CREATE log)").unwrap().into();
		let mut overrides = HashMap::new();
		let mut facts = FunctionFacts::default();
		facts.calls.insert("self".to_owned());
		overrides.insert("self".to_owned(), {
			let mut f = new_body.function_facts();
			f.calls.insert("self".to_owned());
			f
		});
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
		let resolved =
			resolve_mutability(&txn, db.namespace_id, db.database_id, &facts, &overrides)
				.await
				.unwrap();
		txn.cancel().await.unwrap();
		assert_eq!(resolved.write, Some(WriteSource::Function("self".to_owned())));
	}
}
