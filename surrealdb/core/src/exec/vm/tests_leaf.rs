//! The leaf profile: the classification table covers the host interface, a
//! builtin's class follows its declared effects, and the profile of a lowered
//! statement names what bars it from a leaf.

use std::collections::BTreeSet;

use super::leaf::{Bar, Class, Seam, builtin, profile};
use super::program::Program;
use super::tests::{SCHEMA, bind, planner};
use crate::exec::ExecutionContext;
use crate::exec::function::FunctionRegistry;
use crate::exec::operators::test_util::TestDb;

/// Every method of the `Services` trait has a row in the table, and every
/// row names a method: a method added to the trait fails here until it is
/// classified.
#[test]
fn every_service_is_classified() {
	let source = include_str!("services.rs");
	let start = source.find("pub(super) trait Services {").expect("the trait is declared");
	let end = start + source[start..].find("\n}\n").expect("the trait ends");
	let declared = source[start..end]
		.split("fn ")
		.skip(1)
		.filter_map(|rest| rest.split(['(', '<']).next())
		.map(str::to_owned)
		.collect::<BTreeSet<_>>();
	let classified =
		super::leaf::SERVICES.iter().map(|(name, _)| (*name).to_owned()).collect::<BTreeSet<_>>();
	assert_eq!(declared, classified);
	assert_eq!(classified.len(), super::leaf::SERVICES.len(), "a method is classified twice");
}

/// A builtin is barred by the effects that reach outside the snapshot, write
/// or wait; a pure one is a leaf's own; a read is served through a seam.
#[test]
fn a_builtins_class_follows_its_effects() {
	let registry = FunctionRegistry::with_builtins();
	assert_eq!(builtin("http::get", &registry), Class::NonLeaf(Bar::Network));
	assert_eq!(builtin("sleep", &registry), Class::NonLeaf(Bar::Sleep));
	assert_eq!(builtin("sequence::nextval", &registry), Class::NonLeaf(Bar::Write));
	assert_eq!(builtin("eval::surql", &registry), Class::NonLeaf(Bar::Opaque));
	assert_eq!(builtin("string::len", &registry), Class::Leaf);
	assert_eq!(builtin("math::max", &registry), Class::Leaf);
	let Class::Kernel(seams) = builtin("record::exists", &registry) else {
		panic!("a record read is served through the engine");
	};
	assert!(seams.contains(&Seam::Storage));
}

async fn lowered(ctx: &ExecutionContext, sql: &str) -> Program {
	let bound = bind(ctx, sql).await;
	let physical = super::optimize::optimize(
		super::map::map(&planner(ctx), &bound).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
	);
	super::lower::lower(&planner(ctx), &physical).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
}

/// A read over records, its filters, projections, sorts and aggregates is
/// leaf-clean; a write, a wait and a call outside the snapshot are not, and
/// each is named by its bar.
#[tokio::test]
async fn a_statements_profile_names_what_bars_it() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let registry = FunctionRegistry::with_builtins();
	for sql in [
		"SELECT * FROM person WHERE age > 20",
		"SELECT name, age * 2 AS twice FROM person ORDER BY age DESC LIMIT 2",
		"SELECT city, count() AS c, math::max(age) AS hi FROM person GROUP BY city",
		"SELECT name, (SELECT VALUE age FROM person WHERE name = 'ann') AS a FROM person",
		"RETURN string::uppercase('a') + 'b'",
	] {
		let profile = profile(&lowered(&ctx, sql).await, &registry);
		assert!(profile.leaf_clean(), "`{sql}`: {:?}", profile.blockers);
	}
	assert!(
		profile(&lowered(&ctx, "RETURN [1, 2, 3].len() + 1").await, &registry).host_free(),
		"a pure expression needs nothing of the engine"
	);
	let scan = profile(&lowered(&ctx, "SELECT * FROM person").await, &registry);
	assert!(scan.seams.contains(&Seam::Storage), "{scan:?}");
	for (sql, bar) in [
		("CREATE person:9 SET age = 1", Bar::Write),
		("UPDATE person SET seen = true", Bar::Write),
		("RETURN sleep(1ms)", Bar::Sleep),
		("SELECT *, sleep(1ms) AS s FROM person", Bar::Sleep),
	] {
		let profile = profile(&lowered(&ctx, sql).await, &registry);
		assert!(
			profile.blockers.iter().any(|(found, _)| *found == bar),
			"`{sql}`: {:?}",
			profile.blockers
		);
	}
}

/// Native host dispatch stays conservative for missing entries and asynchronous calls.
#[test]
fn unknown_and_async_functions_require_the_host() {
	use crate::exec::function::{EffectSet, ScalarFunction, Signature};
	#[derive(Debug)]
	struct AsyncPure;
	impl ScalarFunction for AsyncPure {
		fn name(&self) -> &'static str {
			"test::async_pure"
		}
		fn signature(&self) -> Signature {
			Signature::new()
		}
		fn effects(&self) -> EffectSet {
			EffectSet::PURE
		}
		fn is_async(&self) -> bool {
			true
		}
	}
	let mut registry = FunctionRegistry::with_builtins();
	registry.register(AsyncPure);
	assert!(matches!(builtin("test::async_pure", &registry), Class::Kernel(_)));
	assert!(matches!(builtin("test::missing", &registry), Class::Kernel(_)));
	assert!(matches!(builtin("type::fields", &registry), Class::Kernel(_)));
}
