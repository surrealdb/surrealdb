//! Resolves whether an expression can modify data once the stored bodies of
//! the user-defined functions it calls are taken into account.
//!
//! [`FunctionFacts`] carries the per-body facts; this module walks the call
//! graph they induce against a catalog snapshot and combines them into
//! [`ResolvedMutability::possibly_writes`]: a conservative over-approximation
//! that reports a write as possible when anything reachable might perform one,
//! including opaque callables (scripts, `eval`, closures arriving as data) and
//! callees this snapshot does not define. A planner must use that polarity
//! before treating an expression as read-only, since a `ReadOnly` answer
//! licenses the read-only transaction and the parallel fan-out paths.
//!
//! The answer is derived from the bodies in the snapshot, never from stored
//! attributes, so it cannot drift from what execution against the same
//! snapshot will run. Cycles in the call graph (recursion, mutual recursion)
//! are handled by the visited set: a cycle contributes whatever facts its
//! members carry, nothing more.
//!
//! The answer is an execution-planning input only. Whether a schema clause may
//! hold what it reaches is decided by [`crate::exec::clause_effects`].

use std::collections::HashSet;

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{DatabaseId, Error as CatalogError, NamespaceId};
use crate::expr::function_facts::{ClosurePositions, FunctionFacts};
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
	/// A reached body (the starting expression or a callee) calls a builtin
	/// or method that `keeps_row_order` flags, so calls made for several
	/// rows at once may return different answers than calls made one row
	/// at a time. Complete only when [`Self::write`] is `None`: a write
	/// already keeps every row in order.
	pub(crate) keeps_row_order: bool,
}

impl ResolvedMutability {
	/// A write may happen; the over-approximation planners must respect.
	pub(crate) fn possibly_writes(&self) -> bool {
		self.write.is_some() || self.opaque_effects || self.undefined_callee
	}
}

/// Resolve the mutability of an expression with the given [`FunctionFacts`]
/// against the catalog snapshot `txn` reads from, at `version`.
///
/// `version` is the stamp the bodies are read at, and must be the stamp the
/// call will run against: a definition that writes is a definition that
/// writes at the moment it is invoked, and a function redefined since then
/// says nothing about it. `None` reads current state.
///
/// `keeps_row_order` answers, from the builtins and methods one body names,
/// whether that body must keep row order ([`ResolvedMutability::keeps_row_order`]);
/// the declarations it reads live on the function registry, which this
/// module does not hold. `closures`, where given, says where builtins and
/// methods run a closure they are handed, so that a callee handing one a
/// closure that is not a literal of the call is noted for `keeps_row_order`
/// ([`FunctionFacts::unseen_closures`]).
pub(crate) async fn resolve_mutability(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	version: Option<u64>,
	root: &FunctionFacts,
	keeps_row_order: &(dyn Fn(&FunctionFacts) -> bool + Sync),
	closures: Option<&(dyn ClosurePositions + Sync)>,
) -> anyhow::Result<ResolvedMutability> {
	let mut resolved = ResolvedMutability {
		write: root.direct_writes.then_some(WriteSource::Body),
		opaque_effects: root.opaque_effects,
		undefined_callee: false,
		keeps_row_order: keeps_row_order(root),
	};

	let mut visited: HashSet<String> = root.calls.iter().cloned().collect();
	// Seed from the ordered `calls` set, not from `visited`, so the writer
	// reported when several are reachable does not depend on HashSet
	// iteration order.
	let mut pending: Vec<String> = root.calls.iter().cloned().collect();

	while resolved.write.is_none() {
		let Some(name) = pending.pop() else {
			break;
		};
		let facts = match txn.get_db_function(ns, db, &name, version).await {
			Ok(def) => match closures {
				Some(closures) => def.block.function_facts_with(Some(closures)),
				None => def.block.function_facts(),
			},
			Err(e) => {
				if matches!(e.downcast_ref::<CatalogError>(), Some(CatalogError::FcNotFound { .. }))
				{
					// Late binding: the callee may be defined later, so nothing
					// about its body can be assumed here.
					resolved.undefined_callee = true;
					continue;
				}
				return Err(e);
			}
		};
		if facts.direct_writes {
			resolved.write = Some(WriteSource::Function(name));
			break;
		}
		resolved.opaque_effects |= facts.opaque_effects;
		resolved.keeps_row_order |= keeps_row_order(&facts);
		for call in facts.calls {
			if visited.insert(call.clone()) {
				pending.push(call);
			}
		}
	}

	Ok(resolved)
}

#[cfg(all(test, feature = "kv-mem"))]
// Under this combined cfg clippy's `allow-unwrap-in-tests` does not apply, so the allow is set
// here.
#[allow(clippy::unwrap_used)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::dbs::Session;
	use crate::expr::Expr;
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

	/// Parse a standalone expression and resolve it against the datastore.
	async fn resolve(ds: &Datastore, expression: &str) -> ResolvedMutability {
		let expr: Expr = crate::syn::expr(expression).unwrap();
		let facts = expr.function_facts();
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
		let resolved = resolve_mutability(
			&txn,
			db.namespace_id,
			db.database_id,
			None,
			&facts,
			&|_| false,
			None,
		)
		.await
		.unwrap();
		txn.cancel().await.unwrap();
		resolved
	}

	/// Facts-level classification, one fixture per callable kind. The
	/// resolver tests below exercise the combinations; this pins what each
	/// shape contributes on its own.
	#[test]
	fn facts_classify_each_callable_kind() {
		let facts = |expression: &str| {
			let expr: Expr = crate::syn::expr(expression).unwrap();
			expr.function_facts()
		};

		// A builtin that cannot evaluate statements contributes only its name.
		let f = facts("math::abs(-1)");
		assert!(!f.direct_writes && !f.opaque_effects && f.calls.is_empty());
		assert_eq!(f.builtins.iter().collect::<Vec<_>>(), ["math::abs"]);

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
}
