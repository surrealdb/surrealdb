//! A parameter's `PERMISSIONS` predicate, judged as a stored clause.
//!
//! A read of a parameter takes its value from the catalog when it runs, and
//! judges the predicate of the definition it reads, over the row the read is
//! made on. The predicate is evaluated as a closure body that takes no
//! argument ([`crate::exec::scalar_unit::ScalarUnit::predicate`]). Its unit is
//! cached by the clause as written and the frame it is judged in, and is reused
//! only while the catalog facts its compile read still hold. A clause defined,
//! redefined or given a predicate after the program was planned is judged as
//! it reads now.

use std::sync::Arc;

use crate::catalog::providers::DatabaseProvider;
use crate::exec::scalar_unit::ScalarUnit;
use crate::exec::{EvalContext, ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::expr::logical::LogicalPermissionRef;
use crate::val::Value;

fn internal(why: String) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(why)))
}

/// Reads the definition of parameter `name` through the transaction of
/// `frame`, the permission frame the read is judged in, and evaluates its
/// predicate over `row`. The read uses the statement's `VERSION` stamp when it
/// has one. Returns `None` if the definition does not exist or its clause is
/// not a predicate. Errors if `frame` has no transaction or no database.
pub(super) async fn verdict(
	name: &str,
	frame: &ExecutionContext,
	row: Option<&Value>,
) -> FlowResult<Option<Value>> {
	let txn = frame
		.ctx()
		.try_tx()
		.map(Arc::clone)
		.ok_or_else(|| internal(format!("${name} read with no transaction")))?;
	let db_ctx = frame.database().map_err(|e| ControlFlow::Err(e.into()))?;
	let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
	// Read the definition at the statement's `VERSION` stamp when it has one.
	let definition =
		crate::exec::compiles::seam(txn.get_db_param(ns, db, name, frame.version_stamp())).await;
	let Ok(definition) = definition else {
		return Ok(None);
	};
	let LogicalPermissionRef::Specific(predicate) =
		definition.permissions_body.logical_ref().map_err(ControlFlow::Err)?
	else {
		return Ok(None);
	};
	let unit = ScalarUnit::predicate(predicate.clone());
	let eval = EvalContext::from_exec_ctx(frame);
	let eval = match row {
		Some(row) => eval.with_value_and_doc(row),
		None => eval,
	};
	unit.evaluate(&eval).await.map(Some)
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
		// A change to a catalog fact the clause reads changes the verdict of
		// the next read.
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
