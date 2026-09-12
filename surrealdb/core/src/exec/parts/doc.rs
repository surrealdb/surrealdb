//! The record-identity root -- `@`.

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel};
use crate::expr::FlowResult;
use crate::val::Value;

/// The record identity of the row a statement is iterating -- `@`.
///
/// `@` names the row's identity, not a position inside it: a row that carries
/// none reads as `NONE`, and the rest of the path continues from there. A row
/// is a record when it is the record itself -- an object whose `id` is a
/// record id -- or the bare record id; any other row (a literal or computed
/// object, an array, a scalar) is a value with no identity, and an `id` key it
/// happens to hold is ordinary data rather than an identity.
///
/// The parser only ever produces `@` as the root of a path, so this part is
/// applied to the row rather than to a position reached inside it.
#[derive(Debug, Clone)]
pub struct DocPart;

impl PhysicalExpr for DocPart {
	fn name(&self) -> &'static str {
		"Doc"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		// The identity is read off the row; nothing is fetched.
		ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move { Ok(record_identity(ctx.current_value.unwrap_or(&Value::NONE))) })
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for DocPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push('@');
	}
}

/// The record identity `value` carries, or `NONE` when it carries none.
fn record_identity(value: &Value) -> Value {
	match value {
		Value::RecordId(_) => value.clone(),
		Value::Object(obj) => match obj.get("id") {
			Some(id @ Value::RecordId(_)) => id.clone(),
			_ => Value::None,
		},
		_ => Value::None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ExecutionContext;
	use crate::exec::operators::test_util::{TestDb, eval_on, root_ctx, val};

	/// Evaluate `@` against `value`, bound the way top-level idiom evaluation
	/// binds a row.
	async fn doc(value: &Value, ctx: &ExecutionContext) -> Value {
		let base = EvalContext::from_exec_ctx(ctx);
		DocPart
			.evaluate(base.with_value_and_doc(value))
			.await
			.expect("reading an identity should succeed")
	}

	#[tokio::test]
	async fn a_record_row_carries_its_identity() {
		let ctx = root_ctx();
		let record = val("{ id: t:1, a: 1 }").await;
		assert_eq!(doc(&record, &ctx).await, val("t:1").await);
		// A row that is the bare record id is that record.
		assert_eq!(doc(&val("t:1").await, &ctx).await, val("t:1").await);
	}

	#[tokio::test]
	async fn an_id_key_that_is_not_a_record_id_is_not_an_identity() {
		let ctx = root_ctx();
		// Whatever a literal row put under `id` is ordinary data, and reading
		// it as identity would hand it back as one.
		for src in ["{ id: 'fake', a: 1 }", "{ id: { k: 1 } }", "{ id: 7 }", "{ id: NULL }"] {
			let row = val(src).await;
			assert_eq!(doc(&row, &ctx).await, Value::None, "{src} carries no identity");
		}
	}

	#[tokio::test]
	async fn a_row_that_is_not_a_record_has_no_identity() {
		let ctx = root_ctx();
		// An array is not a record even when its elements are: mapping the
		// read over them would invent an identity the row does not have.
		for src in ["{ a: 1 }", "[{ id: t:5 }]", "[]", "7", "'text'", "NONE", "NULL"] {
			let row = val(src).await;
			assert_eq!(doc(&row, &ctx).await, Value::None, "{src} carries no identity");
		}
	}

	#[tokio::test]
	async fn the_path_continues_from_the_identity() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 CREATE t:1 SET a = 1;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		// `@` is a root and the steps after it apply to what it answers: a
		// record id, so a field read off it dereferences the record.
		let record = val("{ id: t:1, a: 1 }").await;
		assert_eq!(eval_on("@.id", &record, &ctx).await.unwrap(), val("t:1").await);
		assert_eq!(eval_on("@.a", &record, &ctx).await.unwrap(), Value::from(1));
		// A row with no identity descends from NONE rather than from the row,
		// so its own `a` is not what the path reads.
		let literal = val("{ id: 'fake', a: 1 }").await;
		assert_eq!(eval_on("@.a", &literal, &ctx).await.unwrap(), Value::None);
	}

	#[tokio::test]
	async fn the_identity_read_declares_neither_a_database_nor_a_write() {
		// Nothing is fetched, so the executor's preflight check needs no
		// database, and the read never promotes the plan to a write.
		assert_eq!(DocPart.required_context(), ContextLevel::Root);
		assert_eq!(DocPart.access_mode(), AccessMode::ReadOnly);
		// `@` is not a column of the row; a consumer that fires on a simple
		// field read must not take it for one.
		assert_eq!(DocPart.try_simple_field(), None);
	}
}
