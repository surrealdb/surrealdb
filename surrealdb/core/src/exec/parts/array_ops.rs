//! Array operation parts -- `[*]`, `...`, `[$]`, `[~]`.

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel};
use crate::expr::FlowResult;
use crate::val::Value;

/// Threshold below which we evaluate sequentially (no parallelism overhead).
const PARALLEL_BATCH_THRESHOLD: usize = 2;

// ============================================================================
// AllPart -- [*] or .*
// ============================================================================

/// All elements - `[*]` or `.*`.
///
/// When applied to a RecordId (e.g., `record.*`), fetches the record and returns it as an object.
/// When applied to an array of RecordIds (e.g., from `->edge->target.*`), fetches each record.
#[derive(Debug, Clone)]
pub struct AllPart;
impl PhysicalExpr for AllPart {
	fn name(&self) -> &'static str {
		"All"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		// All (.*) may trigger record fetch + computed field evaluation
		ContextLevel::Database
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.unwrap_or(&Value::NONE);
			evaluate_all(value, ctx).await
		})
	}

	/// Parallel batch evaluation for `[*]` / `.*`.
	///
	/// When applied to arrays of RecordIds, this triggers record fetches.
	/// Parallelizing across rows lets multiple fetches proceed concurrently.
	fn evaluate_batch<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		values: &'a [Value],
	) -> BoxFut<'a, FlowResult<Vec<Value>>> {
		Box::pin(async move {
			if values.len() < PARALLEL_BATCH_THRESHOLD {
				let mut results = Vec::with_capacity(values.len());
				for value in values {
					results.push(self.evaluate(ctx.with_value(value)).await?);
				}
				return Ok(results);
			}
			let futures: Vec<_> =
				values.iter().map(|value| self.evaluate(ctx.with_value(value))).collect();
			futures::future::try_join_all(futures).await
		})
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for AllPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("[*]");
	}
}

/// All elements evaluation.
pub(crate) async fn evaluate_all(value: &Value, ctx: EvalContext<'_>) -> FlowResult<Value> {
	match value {
		Value::Array(arr) => {
			let has_record_ids = arr.iter().any(|v| matches!(v, Value::RecordId(_)));
			if has_record_ids {
				let mut results = Vec::with_capacity(arr.len());
				for item in arr.iter() {
					// Match legacy `val/value/get.rs`: under `.*`, only RecordId
					// elements trigger a fetch; everything else passes through
					// unchanged.
					let processed = match item {
						Value::RecordId(_) => Box::pin(evaluate_all(item, ctx.clone())).await?,
						_ => item.clone(),
					};
					results.push(processed);
				}
				Ok(Value::Array(results.into()))
			} else {
				Ok(Value::Array(arr.clone()))
			}
		}
		Value::Object(_) => Ok(value.clone()),
		Value::RecordId(rid) => {
			if ctx.skip_fetch_perms {
				crate::exec::operators::fetch::fetch_record_no_perms(ctx.exec_ctx, rid).await
			} else {
				crate::exec::operators::fetch::fetch_record(ctx.exec_ctx, rid).await
			}
		}
		// Anything else (NONE, NULL, scalars, geometries) returns NONE, matching the
		// legacy compute path. Issue #7143: previously this wrapped the value in a
		// single-element array, so `none.*` produced `[NONE]` instead of `NONE`.
		_ => Ok(Value::None),
	}
}

// ============================================================================
// FlattenPart -- ...
// ============================================================================

/// Flatten nested arrays - `...` or flatten operation.
///
/// Also inserted by the planner between consecutive lookups.
#[derive(Debug, Clone)]
pub struct FlattenPart;
impl PhysicalExpr for FlattenPart {
	fn name(&self) -> &'static str {
		"Flatten"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.unwrap_or(&Value::NONE);
			Ok(evaluate_flatten(value)?)
		})
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for FlattenPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("...");
	}
}

/// Flatten nested arrays.
pub(crate) fn evaluate_flatten(value: &Value) -> anyhow::Result<Value> {
	match value {
		Value::Array(arr) => {
			let mut result = Vec::new();
			for item in arr.iter() {
				match item {
					Value::Array(inner) => result.extend(inner.iter().cloned()),
					other => result.push(other.clone()),
				}
			}
			Ok(Value::Array(result.into()))
		}
		other => Ok(other.clone()),
	}
}

// ============================================================================
// FirstPart -- [0]
// ============================================================================

/// First element - `[0]` or `.first()`.
#[derive(Debug, Clone)]
pub struct FirstPart;
impl PhysicalExpr for FirstPart {
	fn name(&self) -> &'static str {
		"First"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.cloned().unwrap_or(Value::None);
			match value {
				Value::Array(arr) => Ok(arr.first().cloned().unwrap_or(Value::None)),
				Value::Set(set) => Ok(set.first().cloned().unwrap_or(Value::None)),
				other => Ok(other),
			}
		})
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for FirstPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("[0]");
	}
}

// ============================================================================
// LastPart -- [$]
// ============================================================================

/// Last element - `[$]` or `.last()`.
#[derive(Debug, Clone)]
pub struct LastPart;
impl PhysicalExpr for LastPart {
	fn name(&self) -> &'static str {
		"Last"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.cloned().unwrap_or(Value::None);
			match value {
				Value::Array(arr) => Ok(arr.last().cloned().unwrap_or(Value::None)),
				Value::Set(set) => Ok(set.last().cloned().unwrap_or(Value::None)),
				other => Ok(other),
			}
		})
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for LastPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("[$]");
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::exec::ExecutionContext;
	use crate::exec::operators::test_util::{TestDb, root_ctx, val};
	use crate::kvs::TransactionType;
	use crate::val::Set;

	/// Evaluate one part against `value`, with `value` also bound as the
	/// document root the way top-level idiom evaluation binds it.
	async fn eval_part(part: &dyn PhysicalExpr, value: &Value, ctx: &ExecutionContext) -> Value {
		let base = EvalContext::from_exec_ctx(ctx);
		part.evaluate(base.with_value_and_doc(value)).await.expect("part should evaluate")
	}

	fn set_of(values: Vec<Value>) -> Value {
		Value::Set(Set::from_iter(values))
	}

	// =========================================================================
	// AllPart -- [*] / .*
	// =========================================================================

	#[tokio::test]
	async fn all_returns_an_array_with_no_record_ids_unchanged() {
		let ctx = root_ctx();
		// No fetch is needed, so this runs without a transaction and nothing is
		// flattened or reordered.
		for src in ["[]", "[1, 'x', true]", "[[1, 2], [3]]", "[{ a: 1 }, { b: 2 }]"] {
			let value = val(src).await;
			assert_eq!(eval_part(&AllPart, &value, &ctx).await, value, "unchanged for {src}");
		}
	}

	#[tokio::test]
	async fn all_on_an_object_returns_it_unchanged() {
		let ctx = root_ctx();
		let obj = val("{ a: 1, b: [2, 3] }").await;
		assert_eq!(eval_part(&AllPart, &obj, &ctx).await, obj);
	}

	#[tokio::test]
	async fn all_on_anything_other_than_a_collection_is_none() {
		let ctx = root_ctx();
		// NONE, NULL and scalars collapse to NONE rather than being wrapped in a
		// one-element array. A set is not treated as a collection here either,
		// unlike the legacy `compute` path which maps over set members.
		for src in ["NONE", "NULL", "42", "'text'", "true", "(1.0, 2.0)"] {
			let value = val(src).await;
			assert_eq!(eval_part(&AllPart, &value, &ctx).await, Value::None, "NONE for {src}");
		}
		let set = set_of(vec![Value::from(1), Value::from(2)]);
		assert_eq!(eval_part(&AllPart, &set, &ctx).await, Value::None);
	}

	#[tokio::test]
	async fn all_fetches_a_record_id_and_a_missing_record_is_none() {
		let db = TestDb::new(
			"DEFINE TABLE org SCHEMALESS;
			 CREATE org:acme SET name = 'Acme';",
		)
		.await;
		let ctx = db.exec_ctx().await;

		let fetched = eval_part(&AllPart, &val("org:acme").await, &ctx).await;
		assert_eq!(fetched, val("{ id: org:acme, name: 'Acme' }").await);
		assert_eq!(eval_part(&AllPart, &val("org:nope").await, &ctx).await, Value::None);
	}

	#[tokio::test]
	async fn all_fetches_only_the_record_id_elements_of_an_array() {
		let db = TestDb::new(
			"DEFINE TABLE org SCHEMALESS;
			 CREATE org:acme SET name = 'Acme';",
		)
		.await;
		let ctx = db.exec_ctx().await;

		// Non-record elements pass through untouched and keep their position.
		let mixed = val("[org:acme, 42, { a: 1 }, org:nope]").await;
		assert_eq!(
			eval_part(&AllPart, &mixed, &ctx).await,
			val("[{ id: org:acme, name: 'Acme' }, 42, { a: 1 }, NONE]").await
		);
	}

	#[tokio::test]
	async fn all_reads_through_a_denying_select_permission_when_skip_fetch_perms_is_set() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE secret SCHEMALESS PERMISSIONS FOR select NONE;
			 CREATE secret:1 SET code = 'hunter2';",
		)
		.await;
		// Server auth is on and this identity is anonymous, so the fetch is
		// permission-checked.
		let anon = Session::default().with_ns("test").with_db("test");
		let ctx = db.exec_ctx_as(&anon, TransactionType::Read).await;
		let link = val("secret:1").await;

		// A hidden record reads as NONE rather than raising a permission error.
		assert_eq!(eval_part(&AllPart, &link, &ctx).await, Value::None);

		// `skip_fetch_perms` is the reentrancy escape hatch used while a
		// permission predicate is being evaluated: it reads the record without
		// re-running the check.
		let mut eval_ctx = EvalContext::from_exec_ctx(&ctx);
		eval_ctx.skip_fetch_perms = true;
		let out = AllPart.evaluate(eval_ctx.with_value_and_doc(&link)).await.unwrap();
		assert_eq!(out, val("{ id: secret:1, code: 'hunter2' }").await);
	}

	#[tokio::test]
	async fn all_batch_evaluation_agrees_with_per_row_on_both_sides_of_the_threshold() {
		let db = TestDb::new(
			"DEFINE TABLE org SCHEMALESS;
			 CREATE org:acme SET name = 'Acme';
			 CREATE org:other SET name = 'Other';",
		)
		.await;
		let ctx = db.exec_ctx().await;

		// One row takes the sequential branch, three the concurrent one; both
		// must produce the same values in the same order, including the record
		// fetches the concurrent branch overlaps.
		let one = vec![val("org:acme").await];
		let many = vec![val("org:acme").await, val("{ a: 1 }").await, val("[org:other, 7]").await];

		for rows in [one, many] {
			let base = EvalContext::from_exec_ctx(&ctx);
			let batched = AllPart.evaluate_batch(base.clone(), &rows).await.unwrap();
			let mut sequential = Vec::with_capacity(rows.len());
			for row in &rows {
				sequential.push(AllPart.evaluate(base.with_value(row)).await.unwrap());
			}
			assert_eq!(batched, sequential, "batch and per-row results diverged for {rows:?}");
		}
	}

	// =========================================================================
	// FlattenPart -- ...
	// =========================================================================

	#[tokio::test]
	async fn flatten_lifts_exactly_one_level() {
		let ctx = root_ctx();
		// Inner arrays are spliced in place; anything already flat is kept, and
		// a doubly nested array only loses its outer level.
		assert_eq!(
			eval_part(&FlattenPart, &val("[[1, 2], 3, [[4]], []]").await, &ctx).await,
			val("[1, 2, 3, [4]]").await
		);
		assert_eq!(eval_part(&FlattenPart, &val("[]").await, &ctx).await, val("[]").await);
		assert_eq!(eval_part(&FlattenPart, &val("[1, 2]").await, &ctx).await, val("[1, 2]").await);
	}

	#[tokio::test]
	async fn flatten_passes_non_arrays_through_unchanged() {
		let ctx = root_ctx();
		// Unlike `[*]`, a non-array input is returned as it is rather than
		// collapsing to NONE. A set is not an array here, so it is untouched.
		for src in ["NONE", "NULL", "42", "'text'", "{ a: 1 }"] {
			let value = val(src).await;
			assert_eq!(eval_part(&FlattenPart, &value, &ctx).await, value, "unchanged for {src}");
		}
		let set = set_of(vec![Value::from(1), Value::from(2)]);
		assert_eq!(eval_part(&FlattenPart, &set, &ctx).await, set);
	}

	// =========================================================================
	// FirstPart / LastPart -- [0] / [$]
	// =========================================================================

	#[tokio::test]
	async fn first_and_last_read_the_ends_of_an_array() {
		let ctx = root_ctx();
		let arr = val("[1, 2, 3]").await;
		assert_eq!(eval_part(&FirstPart, &arr, &ctx).await, Value::from(1));
		assert_eq!(eval_part(&LastPart, &arr, &ctx).await, Value::from(3));

		// An empty array has no ends, so both are NONE.
		let empty = val("[]").await;
		assert_eq!(eval_part(&FirstPart, &empty, &ctx).await, Value::None);
		assert_eq!(eval_part(&LastPart, &empty, &ctx).await, Value::None);

		// A single element is both ends.
		let one = val("[9]").await;
		assert_eq!(eval_part(&FirstPart, &one, &ctx).await, Value::from(9));
		assert_eq!(eval_part(&LastPart, &one, &ctx).await, Value::from(9));
	}

	#[tokio::test]
	async fn first_and_last_on_a_set_follow_its_sorted_order() {
		let ctx = root_ctx();
		// A set stores its members sorted, so the ends are the smallest and
		// largest members rather than the first and last written.
		let set = set_of(vec![Value::from(30), Value::from(10), Value::from(20)]);
		assert_eq!(eval_part(&FirstPart, &set, &ctx).await, Value::from(10));
		assert_eq!(eval_part(&LastPart, &set, &ctx).await, Value::from(30));

		let empty = set_of(vec![]);
		assert_eq!(eval_part(&FirstPart, &empty, &ctx).await, Value::None);
		assert_eq!(eval_part(&LastPart, &empty, &ctx).await, Value::None);
	}

	#[tokio::test]
	async fn first_and_last_pass_non_collections_through_unchanged() {
		let ctx = root_ctx();
		// A scalar is its own first and last element — it is not NONE, which is
		// what a numeric index on the same value would give.
		for src in ["NONE", "NULL", "42", "'text'", "{ a: 1 }"] {
			let value = val(src).await;
			assert_eq!(eval_part(&FirstPart, &value, &ctx).await, value, "first of {src}");
			assert_eq!(eval_part(&LastPart, &value, &ctx).await, value, "last of {src}");
		}
	}
}
