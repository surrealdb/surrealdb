//! A parameter's unit: the `PERMISSIONS` predicate of its definition,
//! compiled from the definition and served from the compile cache
//! ([`super::compile_cache`]).
//!
//! A read of a parameter reads its value from the catalog when it runs, and
//! judges the definition's predicate with the pipeline the program lowered
//! for the clause the catalog held when it was planned. Where the definition
//! read then holds another clause — it was defined, redefined or given a
//! predicate after the program was planned — the read judges it with the
//! parameter's unit instead: the clause it reads compiled as a program of its
//! own, against the catalog entries the clause consults, keyed by the
//! parameter's name and the hash of the clause, with one compilation per
//! frame, and kept while those entries hold as its lowering read them. The
//! load is a seam of the run-time seal
//! ([`crate::exec::compiles::seam`]); a unit the cache does not hold is
//! compiled in a fault ([`crate::exec::compiles::serving_fault`]) and kept.

use std::sync::Arc;

use super::program::{DefHash, Program};
use super::units::ContextDeps;
use crate::catalog::providers::DatabaseProvider;
use crate::exec::catalog_deps::CatalogDeps;
use crate::exec::planner::Planner;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::expr::logical::LogicalPermissionRef;

fn internal(why: String) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(why)))
}

/// The unit of parameter `name` for a read in `exec`, the definition as the
/// transaction reads it now, at the read's `VERSION` stamp where it has one:
/// the program whose pipeline `0` evaluates its predicate. `None` where the
/// definition is gone or its clause is no predicate.
pub(super) async fn guard(name: &str, exec: &ExecutionContext) -> FlowResult<Option<Arc<Program>>> {
	use super::compile_cache::{BuildId, ObjectVersion, UnitKey, UnitObject};
	crate::exec::compiles::seam(async {
		let txn = exec
			.ctx()
			.try_tx()
			.map(Arc::clone)
			.ok_or_else(|| internal(format!("${name} read with no transaction")))?;
		let db_ctx = exec.database().map_err(|e| ControlFlow::Err(e.into()))?;
		let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
		// The definition the read judges is the one it read: at the
		// statement's `VERSION` stamp where it has one.
		let Ok(definition) = txn.get_db_param(ns, db, name, exec.version_stamp()).await else {
			return Ok(None);
		};
		let LogicalPermissionRef::Specific(predicate) =
			definition.permissions_body.logical_ref().map_err(ControlFlow::Err)?
		else {
			return Ok(None);
		};
		let predicate = predicate.clone();
		let (ns_name, db_name) = super::closure::names_of(exec);
		// The predicate is judged one re-entry deeper than the read.
		let planner = Planner::with_txn(
			exec.ctx(),
			exec.function_registry(),
			Arc::clone(&txn),
			ns_name,
			db_name,
		)
		.with_auth(Arc::clone(&exec.root().auth))
		.with_perms_suppressed(exec.root().perms_suppressed)
		.with_depth(super::closure::depth_of(exec) + 1);
		// The unit, and what its lowering read of the catalog where the unit
		// can be kept: the definitions the clause consults, which the
		// lowering reads before it lowers the clause.
		let compile = || async {
			crate::exec::compiles::serving_fault(async {
				let recording = txn.record_catalog_reads();
				let lowered = super::lower::param_guard_unit(&planner, &predicate).await;
				let reads = recording.finish();
				let program = lowered
					.map(Arc::new)
					.map_err(|refused| ControlFlow::Err(super::declined(refused).into()))?;
				let deps = match txn.speculating() {
					false => CatalogDeps::resolve(&txn, &reads).await,
					true => None,
				};
				Ok::<_, ControlFlow>((program, deps))
			})
			.await
		};
		let Some(units) =
			exec.ctx().compiled_units().filter(|_| !txn.speculating()).map(Arc::clone)
		else {
			return compile().await.map(|(program, _)| Some(program));
		};
		let key = UnitKey {
			ns: db_ctx.ns_name().to_owned(),
			db: db_ctx.db_name().to_owned(),
			ids: (ns, db),
			object: UnitObject::ParamGuard(name.to_owned()),
			version: ObjectVersion::Content(DefHash::of_param(&definition).0),
			build: BuildId::current(exec.ctx().get_capabilities()),
		};
		let frame = super::lower::statement_frame(&planner);
		if let Some(kept) = units.param_guards().lookup(&key, &frame, &txn, exec.ctx()).await {
			return Ok(Some(kept));
		}
		let kept = units
			.param_guards()
			.get_or_compile(&key, &frame, &txn, exec.ctx(), || async {
				// The key names the clause; the unit stands while what the
				// clause consults holds as it was read.
				let (program, deps) = compile().await?;
				Ok::<_, ControlFlow>((program, deps.map(|deps| (deps, ContextDeps::default()))))
			})
			.await?;
		Ok(Some(kept))
	})
	.await
}

#[cfg(test)]
mod tests {
	use crate::dbs::Session;
	use crate::exec::operators::test_util::{TestDb, plan_statement, try_collect};
	use crate::expr::ControlFlow;
	use crate::kvs::TransactionType;
	use crate::val::Value;

	fn reader() -> Session {
		let rid =
			crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
		Session::for_record("test", "test", "user", rid)
	}

	/// A read of a parameter whose `PERMISSIONS` clause was redefined after
	/// the statement was planned judges the clause it meets, through what that
	/// clause reads of the catalog: a function it calls and a table it selects.
	#[tokio::test]
	async fn a_clause_redefined_after_planning_is_judged_through_what_it_reads() {
		let db = TestDb::new_with_auth(
			"DEFINE FUNCTION fn::can_read($a: any) { RETURN true };
			DEFINE TABLE gate PERMISSIONS NONE;
			DEFINE FIELD open ON gate COMPUTED true;
			CREATE gate:1;
			DEFINE PARAM $p VALUE 1 PERMISSIONS FULL;
			DEFINE PARAM $q VALUE 2 PERMISSIONS FULL;",
		)
		.await;
		let planned = db.exec_ctx_as(&reader(), TransactionType::Read).await;
		let p = plan_statement("RETURN $p", &planned).await;
		let q = plan_statement("RETURN $q", &planned).await;
		db.run(
			"DEFINE PARAM OVERWRITE $p VALUE 1 PERMISSIONS WHERE fn::can_read($auth);
			DEFINE PARAM OVERWRITE $q VALUE 2 PERMISSIONS WHERE (SELECT VALUE open FROM ONLY gate:1);",
		)
		.await;
		for (plan, expected) in [(&p, 1), (&q, 2)] {
			let ctx = db.exec_ctx_as(&reader(), TransactionType::Read).await;
			let read = try_collect(plan, &ctx).await;
			assert!(
				matches!(&read, Err(ControlFlow::Return(value)) if *value == Value::from(expected as i64)),
				"{read:?}"
			);
		}
		// What the clause reads changes, and the read judges it as it now
		// reads.
		db.run(
			"DEFINE FUNCTION OVERWRITE fn::can_read($a: any) { RETURN false };
			DEFINE FIELD OVERWRITE open ON gate COMPUTED false;",
		)
		.await;
		for plan in [&p, &q] {
			let ctx = db.exec_ctx_as(&reader(), TransactionType::Read).await;
			let read = try_collect(plan, &ctx).await;
			assert!(matches!(&read, Err(ControlFlow::Err(_))), "{read:?}");
		}
	}
}
