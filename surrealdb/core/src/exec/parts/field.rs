//! Field access part -- `foo` in `obj.foo`.

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel};
use crate::expr::FlowResult;
use crate::val::Value;

/// Threshold below which we evaluate sequentially (no parallelism overhead).
const PARALLEL_BATCH_THRESHOLD: usize = 2;

/// Simple field access on an object - `foo`.
///
/// When applied to a RecordId, the record is automatically fetched from the
/// database and the field is accessed on the fetched object.
#[derive(Debug, Clone)]
pub struct FieldPart {
	pub name: String,
}
impl PhysicalExpr for FieldPart {
	fn name(&self) -> &'static str {
		"Field"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		// Field access might trigger record fetch if applied to RecordId,
		// so we conservatively require database context.
		ContextLevel::Database
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.unwrap_or(&Value::NONE);
			evaluate_field(value, &self.name, ctx).await
		})
	}

	/// Parallel batch evaluation for field access.
	///
	/// Field access on RecordIds triggers record fetches, which are I/O-bound.
	/// Parallelizing across rows lets multiple fetches proceed concurrently.
	fn evaluate_batch<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		values: &'a [Value],
	) -> BoxFut<'a, FlowResult<Vec<Value>>> {
		Box::pin(async move {
			if values.len() < PARALLEL_BATCH_THRESHOLD {
				// Small batches: avoid parallelism overhead
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

	fn try_simple_field(&self) -> Option<&str> {
		Some(&self.name)
	}
}

impl ToSql for FieldPart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push('.');
		f.push_str(&self.name);
	}
}

/// Field access on objects, with support for RecordId auto-fetch.
///
/// When accessing a field on a RecordId, the record is automatically fetched
/// from the database and the field is accessed on the fetched object.
pub(crate) async fn evaluate_field(
	value: &Value,
	name: &str,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	match value {
		Value::Object(obj) => Ok(obj.get(name).cloned().unwrap_or(Value::None)),

		Value::RecordId(rid) => {
			// SECURITY: when the record-id key is an Object and the field
			// name resolves to one of its components, return the immutable
			// key component directly. Falling through to `fetch_record`
			// runs `SELECT *` with permissions disabled and re-binds
			// `id.tenant` to the row's `tenant` document field — letting
			// permission predicates like
			// `WHERE id.tenant = $token.tenant` be spoofed (Codex finding
			// c67c7232), and during UPDATE the same self-fetch returns the
			// already-stored new value for both `o` and `n` in
			// `store_index_data`, leaving stale indexes (Codex finding
			// 9c442c96). When the requested name is not a key component,
			// keep the existing remote-fetch semantics (`record:foo.id`
			// returns the rid itself via the standard `id` projection).
			if let crate::val::RecordIdKey::Object(obj) = &rid.key
				&& obj.contains_key(name)
			{
				return Ok(obj.get(name).cloned().unwrap_or(Value::None));
			}
			// When we are already computing fields for this record, fetch the
			// raw stored data without re-evaluating computed fields. Otherwise
			// a computed field like `{ return $this.id.prop }` would re-enter
			// compute_fields_for_value for the same record and stack-overflow.
			if ctx.computing_record.as_ref() == Some(rid) {
				let version = ctx.exec_ctx.version_stamp();
				let raw =
					crate::exec::operators::fetch::fetch_raw_record(ctx.exec_ctx, rid, version)
						.await?;
				return match raw {
					Some(Value::Object(obj)) => Ok(obj.get(name).cloned().unwrap_or(Value::None)),
					_ => Ok(Value::None),
				};
			}
			let fetched = if ctx.skip_fetch_perms {
				crate::exec::operators::fetch::fetch_record_no_perms(ctx.exec_ctx, rid).await?
			} else {
				crate::exec::operators::fetch::fetch_record(ctx.exec_ctx, rid).await?
			};
			match fetched {
				Value::Object(obj) => Ok(obj.get(name).cloned().unwrap_or(Value::None)),
				_ => Ok(Value::None),
			}
		}

		Value::Array(arr) => {
			// Apply field access to each element (may involve fetches)
			let mut results = Vec::with_capacity(arr.len());
			for v in arr.iter() {
				results.push(Box::pin(evaluate_field(v, name, ctx.clone())).await?);
			}
			Ok(Value::Array(results.into()))
		}

		Value::Geometry(geo) => {
			// Geometry values support GeoJSON field access (type, coordinates, geometries)
			let obj = geo.as_object();
			Ok(obj.get(name).cloned().unwrap_or(Value::None))
		}

		_ => Ok(Value::None),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::exec::ExecutionContext;
	use crate::exec::operators::test_util::{TestDb, eval_on, root_ctx, val};
	use crate::kvs::TransactionType;
	use crate::val::{RecordId, RecordIdKey};

	/// Evaluate a `FieldPart` against `value`, with `value` also bound as the
	/// document root the way top-level idiom evaluation binds it.
	async fn field(value: &Value, name: &str, ctx: &ExecutionContext) -> Value {
		let part = FieldPart {
			name: name.to_owned(),
		};
		let base = EvalContext::from_exec_ctx(ctx);
		part.evaluate(base.with_value_and_doc(value)).await.expect("field access should succeed")
	}

	/// Pull the `RecordId` out of a record-id literal.
	async fn rid(src: &str) -> RecordId {
		match val(src).await {
			Value::RecordId(rid) => rid,
			other => panic!("expected a record id for {src:?}, got {other:?}"),
		}
	}

	// =========================================================================
	// Object / array / scalar inputs
	// =========================================================================

	#[tokio::test]
	async fn a_missing_object_key_is_none_rather_than_an_error() {
		let ctx = root_ctx();
		let obj = val("{ a: 1 }").await;
		assert_eq!(field(&obj, "a", &ctx).await, Value::from(1));
		// Idiom traversal treats an absent key as NONE so that longer chains
		// keep descending instead of aborting the row.
		assert_eq!(field(&obj, "missing", &ctx).await, Value::None);
	}

	#[tokio::test]
	async fn field_access_maps_over_every_array_element() {
		let ctx = root_ctx();
		let arr = val("[{ a: 1 }, { a: 2 }, { b: 3 }, 42]").await;
		// Per-element mapping: elements without the key, and elements that are
		// not objects at all, contribute NONE and keep their position.
		assert_eq!(
			field(&arr, "a", &ctx).await,
			val("[1, 2, NONE, NONE]").await,
			"element positions are preserved"
		);
		// Nesting is mapped recursively, one array level per nesting level.
		let nested = val("[[{ a: 1 }], [{ a: 2 }]]").await;
		assert_eq!(field(&nested, "a", &ctx).await, val("[[1], [2]]").await);
		assert_eq!(field(&val("[]").await, "a", &ctx).await, val("[]").await);
	}

	#[tokio::test]
	async fn field_access_on_none_null_and_scalars_is_none() {
		let ctx = root_ctx();
		// A set is included deliberately: only arrays are mapped over here, so a
		// set of objects yields NONE rather than the per-member mapping the
		// legacy `compute` path applies.
		let set = val("{ { a: 1 }, { a: 2 } }").await;
		assert!(matches!(set, Value::Set(_)), "expected a set literal, got {set:?}");
		assert_eq!(field(&set, "a", &ctx).await, Value::None);

		for src in ["NONE", "NULL", "42", "'text'", "true"] {
			let value = val(src).await;
			assert_eq!(
				field(&value, "a", &ctx).await,
				Value::None,
				"field access on {src} should be NONE"
			);
		}
	}

	#[tokio::test]
	async fn a_geometry_exposes_its_geojson_members() {
		let ctx = root_ctx();
		let point = val("(1.0, 2.0)").await;
		assert!(matches!(point, Value::Geometry(_)), "expected a geometry, got {point:?}");
		assert_eq!(field(&point, "type", &ctx).await, Value::from("Point"));
		let coords = field(&point, "coordinates", &ctx).await;
		assert!(matches!(&coords, Value::Array(a) if a.len() == 2), "got {coords:?}");
		// `geometries` is only a member of a geometry collection; on a point it
		// is absent, and an absent member is NONE.
		assert_eq!(field(&point, "geometries", &ctx).await, Value::None);
		assert_eq!(field(&point, "other", &ctx).await, Value::None);
	}

	#[tokio::test]
	async fn an_idiom_chain_threads_field_access_through_the_planner() {
		let ctx = root_ctx();
		let doc = val("{ a: { b: { c: 7 } } }").await;
		assert_eq!(eval_on("a.b.c", &doc, &ctx).await.unwrap(), Value::from(7));
		assert_eq!(eval_on("a.b.missing.deeper", &doc, &ctx).await.unwrap(), Value::None);
	}

	// =========================================================================
	// Record-id inputs
	// =========================================================================

	#[tokio::test]
	async fn an_object_key_component_is_read_from_the_key_not_the_stored_record() {
		// SECURITY: for an object-keyed record id, a name present in the key
		// resolves to the immutable key component. The stored document here
		// carries a *different* value under the same name; reading the document
		// instead would let a predicate such as `id.tenant = $token.tenant` be
		// satisfied by a mutable field.
		let db = TestDb::new(
			"DEFINE TABLE doc SCHEMALESS;
			 CREATE doc:{ tenant: 'acme' } SET tenant = 'evil', title = 'report';",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let key = Value::RecordId(rid("doc:{ tenant: 'acme' }").await);

		assert_eq!(field(&key, "tenant", &ctx).await, Value::from("acme"));
		// A name that is not a key component keeps the fetch semantics.
		assert_eq!(field(&key, "title", &ctx).await, Value::from("report"));
	}

	#[tokio::test]
	async fn a_key_component_resolves_without_any_database_access() {
		// The key-component read happens before the fetch, so it succeeds under
		// a context that has no transaction at all.
		let ctx = root_ctx();
		let key = Value::RecordId(rid("doc:{ tenant: 'acme' }").await);
		assert_eq!(field(&key, "tenant", &ctx).await, Value::from("acme"));

		// A non-component name falls through to the fetch, which needs a
		// database context and therefore errors here.
		let part = FieldPart {
			name: "title".to_owned(),
		};
		let base = EvalContext::from_exec_ctx(&ctx);
		let err = part.evaluate(base.with_value_and_doc(&key)).await.unwrap_err();
		assert!(
			err.to_string().contains("database context"),
			"expected a missing-database-context error, got {err}"
		);
	}

	#[tokio::test]
	async fn a_record_link_is_dereferenced_and_a_missing_target_is_none() {
		let db = TestDb::new(
			"DEFINE TABLE org SCHEMALESS;
			 DEFINE TABLE doc SCHEMALESS;
			 CREATE org:acme SET name = 'Acme';
			 CREATE doc:1 SET org = org:acme;",
		)
		.await;
		let ctx = db.exec_ctx().await;

		let link = Value::RecordId(rid("org:acme").await);
		assert_eq!(field(&link, "name", &ctx).await, Value::from("Acme"));

		// A dangling link is NONE, not an error.
		let dangling = Value::RecordId(rid("org:nope").await);
		assert_eq!(field(&dangling, "name", &ctx).await, Value::None);

		// Through a chain, and mapped over an array of links.
		let doc = val("{ org: org:acme, links: [org:acme, org:nope] }").await;
		assert_eq!(eval_on("org.name", &doc, &ctx).await.unwrap(), Value::from("Acme"));
		assert_eq!(eval_on("links.name", &doc, &ctx).await.unwrap(), val("['Acme', NONE]").await);
	}

	#[tokio::test]
	async fn skip_fetch_perms_reads_through_a_denying_select_permission() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE secret SCHEMALESS PERMISSIONS FOR select NONE;
			 CREATE secret:1 SET code = 'hunter2';",
		)
		.await;
		// Server auth is on and this identity is anonymous, so the dereference
		// is permission-checked.
		let anon = Session::default().with_ns("test").with_db("test");
		let ctx = db.exec_ctx_as(&anon, TransactionType::Read).await;
		let link = Value::RecordId(rid("secret:1").await);

		// A hidden record reads as NONE rather than raising a permission error.
		assert_eq!(field(&link, "code", &ctx).await, Value::None);

		// `skip_fetch_perms` is the reentrancy escape hatch used while a
		// permission predicate is being evaluated: it reads the record without
		// re-running the check.
		let part = FieldPart {
			name: "code".to_owned(),
		};
		let mut eval_ctx = EvalContext::from_exec_ctx(&ctx);
		eval_ctx.skip_fetch_perms = true;
		let out = part.evaluate(eval_ctx.with_value_and_doc(&link)).await.unwrap();
		assert_eq!(out, Value::from("hunter2"));
	}

	#[tokio::test]
	async fn computing_record_reads_raw_data_without_re_evaluating_computed_fields() {
		let db = TestDb::new(
			"DEFINE TABLE node SCHEMALESS;
			 DEFINE FIELD label ON node COMPUTED 'derived';
			 CREATE node:1 SET raw = 1;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let node = rid("node:1").await;
		let link = Value::RecordId(node.clone());

		// An ordinary dereference evaluates computed fields.
		assert_eq!(field(&link, "label", &ctx).await, Value::from("derived"));

		// While this record's own computed fields are being evaluated, a
		// dereference of the same record returns the raw stored data, so the
		// computed field is absent instead of recursing into itself.
		let part = FieldPart {
			name: "label".to_owned(),
		};
		let mut eval_ctx = EvalContext::from_exec_ctx(&ctx);
		eval_ctx.computing_record = Some(node.clone());
		let out = part.evaluate(eval_ctx.clone().with_value_and_doc(&link)).await.unwrap();
		assert_eq!(out, Value::None);

		// Stored fields are still readable on that path.
		let raw_part = FieldPart {
			name: "raw".to_owned(),
		};
		let out = raw_part.evaluate(eval_ctx.with_value_and_doc(&link)).await.unwrap();
		assert_eq!(out, Value::from(1));
	}

	#[tokio::test]
	async fn computing_record_only_short_circuits_the_record_being_computed() {
		let db = TestDb::new(
			"DEFINE TABLE node SCHEMALESS;
			 DEFINE FIELD label ON node COMPUTED 'derived';
			 CREATE node:1 SET raw = 1;
			 CREATE node:2 SET raw = 2;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let link = Value::RecordId(rid("node:2").await);

		let part = FieldPart {
			name: "label".to_owned(),
		};
		let mut eval_ctx = EvalContext::from_exec_ctx(&ctx);
		eval_ctx.computing_record = Some(rid("node:1").await);
		// A different record still goes through the full fetch, computed fields
		// included.
		let out = part.evaluate(eval_ctx.with_value_and_doc(&link)).await.unwrap();
		assert_eq!(out, Value::from("derived"));
	}

	#[tokio::test]
	async fn a_record_id_with_a_non_object_key_and_no_stored_object_is_none() {
		let db = TestDb::new("DEFINE TABLE doc SCHEMALESS;").await;
		let ctx = db.exec_ctx().await;
		let key = Value::RecordId(RecordId {
			table: "doc".into(),
			key: RecordIdKey::Number(7),
		});
		assert_eq!(field(&key, "anything", &ctx).await, Value::None);
	}

	// =========================================================================
	// Batch evaluation
	// =========================================================================

	#[tokio::test]
	async fn batch_evaluation_agrees_with_per_row_evaluation_on_both_sides_of_the_threshold() {
		let db = TestDb::new(
			"DEFINE TABLE org SCHEMALESS;
			 CREATE org:acme SET name = 'Acme';
			 CREATE org:other SET name = 'Other';",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let part = FieldPart {
			name: "name".to_owned(),
		};

		// One row takes the sequential branch, three rows the concurrent one;
		// both must produce the same values in the same order, including the
		// record fetches the concurrent branch overlaps.
		let one = vec![Value::RecordId(rid("org:acme").await)];
		let many = vec![
			Value::RecordId(rid("org:acme").await),
			val("{ name: 'literal' }").await,
			Value::RecordId(rid("org:other").await),
		];

		for rows in [one, many] {
			let base = EvalContext::from_exec_ctx(&ctx);
			let batched = part.evaluate_batch(base.clone(), &rows).await.unwrap();
			let mut sequential = Vec::with_capacity(rows.len());
			for row in &rows {
				sequential.push(part.evaluate(base.with_value(row)).await.unwrap());
			}
			assert_eq!(batched, sequential, "batch and per-row results diverged for {rows:?}");
		}
	}

	// =========================================================================
	// Declared metadata
	// =========================================================================

	#[tokio::test]
	async fn field_access_declares_database_context_for_the_executor_preflight_check() {
		// The executor validates the plan's `required_context` before running
		// it, and a field access may dereference a record id, so the part
		// declares Database even though object access alone needs nothing.
		let part = FieldPart {
			name: "a".to_owned(),
		};
		assert_eq!(part.required_context(), ContextLevel::Database);
		// A dereference reads; it never promotes the plan to a write
		// transaction.
		assert_eq!(part.access_mode(), AccessMode::ReadOnly);
	}
}
