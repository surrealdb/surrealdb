//! Destructure part -- `{ field1, field2: path, ... }`.

use std::sync::Arc;

use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, CombineAccessModes, ContextLevel};
use crate::expr::FlowResult;
use crate::val::Value;

/// Destructure - extract fields into a new object `{ field1, field2: path }`.
#[derive(Debug, Clone)]
pub struct DestructurePart {
	pub fields: Vec<DestructureField>,
}

/// A field in a destructure pattern.
#[derive(Debug, Clone)]
pub enum DestructureField {
	/// Include all fields from a nested object.
	All(Strand),
	/// Include a single field by name.
	Field(Strand),
	/// Include a field computed by a body evaluated with the destructured
	/// value as the current value.
	Aliased {
		field: Strand,
		body: Arc<dyn PhysicalExpr>,
	},
	/// Nested destructure on a field.
	Nested {
		field: Strand,
		parts: Vec<DestructureField>,
	},
	/// Include every field of the source object not named by another entry
	/// in the same pattern, unchanged.
	Rest,
}
impl PhysicalExpr for DestructurePart {
	fn name(&self) -> &'static str {
		"Destructure"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		fn field_context(fields: &[DestructureField]) -> ContextLevel {
			fields
				.iter()
				.map(|f| match f {
					DestructureField::All(_) | DestructureField::Field(_) => ContextLevel::Root,
					DestructureField::Aliased {
						body,
						..
					} => body.required_context(),
					DestructureField::Nested {
						parts,
						..
					} => field_context(parts),
					DestructureField::Rest => ContextLevel::Root,
				})
				.max()
				.unwrap_or(ContextLevel::Root)
		}
		// Destructure may need to fetch records (when applied to RecordId)
		ContextLevel::Database.max(field_context(&self.fields))
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.unwrap_or(&Value::NONE);
			evaluate_destructure(value, &self.fields, ctx).await
		})
	}

	fn access_mode(&self) -> AccessMode {
		fn field_access(fields: &[DestructureField]) -> AccessMode {
			fields
				.iter()
				.map(|f| match f {
					DestructureField::All(_) | DestructureField::Field(_) => AccessMode::ReadOnly,
					DestructureField::Aliased {
						body,
						..
					} => body.access_mode(),
					DestructureField::Nested {
						parts,
						..
					} => field_access(parts),
					DestructureField::Rest => AccessMode::ReadOnly,
				})
				.combine_all()
		}
		field_access(&self.fields)
	}
}

impl ToSql for DestructurePart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('{');
		for (i, field) in self.fields.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			field.fmt_sql(f, fmt);
		}
		f.push('}');
	}
}

impl ToSql for DestructureField {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			DestructureField::All(name) => {
				f.push_str(name);
				f.push_str(".*");
			}
			DestructureField::Field(name) => {
				f.push_str(name);
			}
			DestructureField::Aliased {
				field,
				body,
			} => {
				f.push_str(field);
				f.push_str(": ");
				body.fmt_sql(f, fmt);
			}
			DestructureField::Nested {
				field,
				parts,
			} => {
				f.push_str(field);
				f.push_str(": {");
				for (i, part) in parts.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					part.fmt_sql(f, fmt);
				}
				f.push('}');
			}
			DestructureField::Rest => f.push_str("..."),
		}
	}
}

/// Destructure evaluation - extract fields into a new object.
async fn evaluate_destructure(
	value: &Value,
	fields: &[DestructureField],
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	match value {
		Value::Object(obj) => {
			let mut result = std::collections::BTreeMap::new();

			for field in fields {
				match field {
					DestructureField::All(name) => {
						let field_val = obj.get(name.as_str()).cloned().unwrap_or(Value::None);
						let resolved = match field_val {
							Value::RecordId(rid) => {
								if ctx.skip_fetch_perms {
									crate::exec::operators::fetch::fetch_record_no_perms(
										ctx.exec_ctx,
										&rid,
									)
									.await?
								} else {
									crate::exec::operators::fetch::fetch_record(ctx.exec_ctx, &rid)
										.await?
								}
							}
							other => other,
						};
						if let Value::Object(nested) = &resolved {
							result.insert(name.clone(), Value::Object(nested.clone()));
						}
					}
					DestructureField::Field(name) => {
						let v = obj.get(name.as_str()).cloned().unwrap_or(Value::None);
						result.insert(name.clone(), v);
					}
					DestructureField::Aliased {
						field: name,
						body,
					} => {
						// The body reads the destructured value, not
						// `obj.get(field)`: the field name is only the output
						// label. The destructured value is also the document
						// for every expression inside the body -- a dynamic
						// index, a method argument, `$parent` -- so
						// `v.{ pick: items[idx] }` reads `idx` from `v`.
						let v = body.evaluate(ctx.with_value_and_doc(value)).await?;
						result.insert(name.clone(), v);
					}
					DestructureField::Nested {
						field: name,
						parts,
					} => {
						let nested_value = obj.get(name.as_str()).cloned().unwrap_or(Value::None);
						let v = Box::pin(evaluate_destructure(&nested_value, parts, ctx.clone()))
							.await?;
						result.insert(name.clone(), v);
					}
					// Copies every key the earlier entries did not already
					// claim. Only valid as this pattern's last entry, so
					// nothing inserted afterwards can be shadowed by it.
					DestructureField::Rest => {
						for (key, v) in obj.iter() {
							if !result.contains_key(key.as_str()) {
								result.insert(key.clone(), v.clone());
							}
						}
					}
				}
			}

			Ok(Value::Object(crate::val::Object::from(result)))
		}
		Value::RecordId(rid) => {
			let fetched = if ctx.skip_fetch_perms {
				crate::exec::operators::fetch::fetch_record_no_perms(ctx.exec_ctx, rid).await?
			} else {
				crate::exec::operators::fetch::fetch_record(ctx.exec_ctx, rid).await?
			};
			if fetched.is_none() {
				return Ok(Value::None);
			}

			// Continue destructure on the fetched object
			Box::pin(evaluate_destructure(&fetched, fields, ctx)).await
		}
		Value::Array(arr) => {
			// Apply destructure to each element
			let mut results = Vec::with_capacity(arr.len());
			for item in arr.iter() {
				let v = Box::pin(evaluate_destructure(item, fields, ctx.clone())).await?;
				results.push(v);
			}
			Ok(Value::Array(results.into()))
		}
		_ => Ok(Value::None),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ExecutionContext;
	use crate::exec::operators::test_util::{TestDb, physical_expr, root_ctx, val};
	use crate::exec::physical_expr::find_part;

	/// Evaluate `tail` (a destructure suffix such as `.{ a, b }`) against
	/// `value`, which is handed to the part as the value of field `v`.
	async fn shape(value: &str, tail: &str, ctx: &ExecutionContext) -> Value {
		let row = val(&format!("{{ v: {value} }}")).await;
		crate::exec::operators::test_util::eval_on(&format!("v{tail}"), &row, ctx)
			.await
			.expect("destructure should evaluate")
	}

	/// Compile `src` and pull the `DestructurePart` out of the resulting path.
	async fn compile_destructure(src: &str, ctx: &ExecutionContext) -> Arc<dyn PhysicalExpr> {
		let expr = physical_expr(src, ctx).await;
		find_part::<DestructurePart>(&expr).expect("source should contain a destructure part")
	}

	// =========================================================================
	// Object input -- the four field shapes
	// =========================================================================

	#[tokio::test]
	async fn a_named_field_is_copied_and_a_missing_one_is_kept_as_none() {
		let ctx = root_ctx();
		// The output key set is the pattern, not the input: an absent key still
		// appears, holding NONE.
		let out = shape("{ a: 1, b: 2, c: 3 }", ".{ a, c, missing }", &ctx).await;
		assert_eq!(out, val("{ a: 1, c: 3, missing: NONE }").await);
	}

	#[tokio::test]
	async fn an_aliased_path_is_resolved_against_the_input_not_against_the_alias_name() {
		let ctx = root_ctx();
		// `z` is only an output label. Were the path evaluated from the value at
		// key `z`, this would yield `{ z: NONE }`.
		let out = shape("{ a: 1 }", ".{ z: a }", &ctx).await;
		assert_eq!(out, val("{ z: 1 }").await);
	}

	#[tokio::test]
	async fn an_aliased_path_evaluates_its_expressions_against_the_destructured_value() {
		let ctx = root_ctx();
		// The row and the destructured value both have `idx` and `n`; inside
		// the pattern the destructured value is the document, so the dynamic
		// index and the method argument read its fields, never the row's.
		let row = val("{ v: { items: [10, 20, 30], idx: 2, n: 1 }, idx: 0, n: 5 }").await;
		let out = crate::exec::operators::test_util::eval_on(
			"v.{ picked: items[idx], head: items.slice(0, n) }",
			&row,
			&ctx,
		)
		.await
		.expect("destructure should evaluate");
		assert_eq!(out, val("{ picked: 30, head: [10] }").await);
	}

	#[tokio::test]
	async fn an_aliased_path_may_walk_more_than_one_step() {
		let ctx = root_ctx();
		let out = shape("{ inner: { x: 7 } }", ".{ deep: inner.x }", &ctx).await;
		assert_eq!(out, val("{ deep: 7 }").await);
	}

	#[tokio::test]
	async fn a_nested_destructure_reshapes_the_named_field_in_place() {
		let ctx = root_ctx();
		let out = shape("{ inner: { x: 1, y: 2 }, other: 3 }", ".{ inner.{ x } }", &ctx).await;
		assert_eq!(out, val("{ inner: { x: 1 } }").await);
	}

	#[tokio::test]
	async fn a_nested_destructure_of_a_missing_field_collapses_to_none() {
		let ctx = root_ctx();
		// The nested pattern is applied to NONE, which is not an object, array
		// or record id, so the whole nested result is NONE rather than an object
		// of NONEs.
		let out = shape("{ a: 1 }", ".{ inner.{ x } }", &ctx).await;
		assert_eq!(out, val("{ inner: NONE }").await);
	}

	#[tokio::test]
	async fn a_spread_field_keeps_an_object_and_omits_anything_else() {
		let ctx = root_ctx();

		let out = shape("{ inner: { x: 1 } }", ".{ inner.* }", &ctx).await;
		assert_eq!(out, val("{ inner: { x: 1 } }").await);

		// Unlike a plain field, a spread that does not resolve to an object
		// contributes no key at all -- the output is empty, not `{ inner: NONE }`.
		let out = shape("{ inner: 7 }", ".{ inner.* }", &ctx).await;
		assert_eq!(out, val("{}").await);

		let out = shape("{ a: 1 }", ".{ inner.* }", &ctx).await;
		assert_eq!(out, val("{}").await);
	}

	#[tokio::test]
	async fn a_rest_field_keeps_every_field_the_pattern_did_not_already_name() {
		let ctx = root_ctx();
		let out = shape("{ a: 1, b: 2, c: 3 }", ".{ a, ... }", &ctx).await;
		assert_eq!(out, val("{ a: 1, b: 2, c: 3 }").await);
	}

	#[tokio::test]
	async fn a_rest_field_does_not_shadow_an_earlier_alias() {
		let ctx = root_ctx();
		// `a` is renamed to `z`; the rest marker still sees `a` on the source
		// object and copies it too, since it was never claimed under that name.
		let out = shape("{ a: 1, b: 2 }", ".{ z: a, ... }", &ctx).await;
		assert_eq!(out, val("{ z: 1, a: 1, b: 2 }").await);
	}

	#[tokio::test]
	async fn a_rest_field_alone_is_the_identity() {
		let ctx = root_ctx();
		let out = shape("{ a: 1, b: 2 }", ".{ ... }", &ctx).await;
		assert_eq!(out, val("{ a: 1, b: 2 }").await);
	}

	// =========================================================================
	// Non-object input
	// =========================================================================

	#[tokio::test]
	async fn destructuring_an_array_maps_over_its_elements() {
		let ctx = root_ctx();
		let out = shape("[{ a: 1, b: 9 }, { a: 2 }]", ".{ a }", &ctx).await;
		assert_eq!(out, val("[{ a: 1 }, { a: 2 }]").await);
	}

	#[tokio::test]
	async fn destructuring_a_scalar_yields_none() {
		let ctx = root_ctx();
		// There is nothing to pick fields from, so the pattern does not produce
		// an object of NONEs.
		assert_eq!(shape("'text'", ".{ a }", &ctx).await, Value::None);
		assert_eq!(shape("7", ".{ a }", &ctx).await, Value::None);
		assert_eq!(shape("NONE", ".{ a }", &ctx).await, Value::None);
	}

	// =========================================================================
	// Record-id input and record links (needs a transaction)
	// =========================================================================

	async fn link_db() -> TestDb {
		TestDb::new(
			"DEFINE TABLE person SCHEMALESS;
			 CREATE person:tobie SET name = 'Tobie', age = 30, pet = pet:rex;
			 DEFINE TABLE pet SCHEMALESS;
			 CREATE pet:rex SET name = 'Rex', legs = 4;",
		)
		.await
	}

	#[tokio::test]
	async fn destructuring_a_record_id_fetches_the_record_first() {
		let db = link_db().await;
		let ctx = db.exec_ctx().await;
		let out = shape("person:tobie", ".{ name, age }", &ctx).await;
		assert_eq!(out, val("{ name: 'Tobie', age: 30 }").await);
	}

	#[tokio::test]
	async fn destructuring_a_dangling_record_id_yields_none() {
		let db = link_db().await;
		let ctx = db.exec_ctx().await;
		// The fetch produces NONE and the pattern is abandoned, rather than
		// being applied to NONE and producing `{ name: NONE }`.
		let out = shape("person:nobody", ".{ name }", &ctx).await;
		assert_eq!(out, Value::None);
	}

	#[tokio::test]
	async fn a_spread_field_dereferences_a_record_link() {
		let db = link_db().await;
		let ctx = db.exec_ctx().await;
		let out = shape("person:tobie", ".{ pet.* }", &ctx).await;
		assert_eq!(out, val("{ pet: { id: pet:rex, name: 'Rex', legs: 4 } }").await);
	}

	#[tokio::test]
	async fn a_spread_field_over_a_dangling_link_contributes_no_key() {
		let db = link_db().await;
		let ctx = db.exec_ctx().await;
		// The fetch yields NONE, which is not an object, so the key is dropped.
		let out = shape("{ pet: pet:missing }", ".{ pet.* }", &ctx).await;
		assert_eq!(out, val("{}").await);
	}

	#[tokio::test]
	async fn a_nested_destructure_dereferences_a_record_link_too() {
		let db = link_db().await;
		let ctx = db.exec_ctx().await;
		let out = shape("person:tobie", ".{ name, pet.{ name } }", &ctx).await;
		assert_eq!(out, val("{ name: 'Tobie', pet: { name: 'Rex' } }").await);
	}

	// =========================================================================
	// Plan metadata the engine acts on
	// =========================================================================

	#[tokio::test]
	async fn a_plain_field_pattern_still_requires_database_context_for_the_fetch_path() {
		// The executor validates `required_context` before evaluating, and the
		// input may be a record id that has to be dereferenced, so a pattern of
		// bare field names cannot claim Root.
		let part = DestructurePart {
			fields: vec![DestructureField::Field("a".into())],
		};
		assert_eq!(part.required_context(), ContextLevel::Database);
	}

	/// A stand-in for a mutating path step: the planner rejects DML subqueries,
	/// so a `ReadWrite` leaf has to be supplied directly.
	#[derive(Debug)]
	struct MutatingStep;

	impl ToSql for MutatingStep {
		fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
			f.push_str("<mutating>");
		}
	}

	impl PhysicalExpr for MutatingStep {
		fn name(&self) -> &'static str {
			"MutatingStep"
		}

		fn as_any(&self) -> &dyn std::any::Any {
			self
		}

		fn required_context(&self) -> ContextLevel {
			ContextLevel::Database
		}

		fn evaluate<'a>(&'a self, _ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
			Box::pin(async move { Ok(Value::None) })
		}

		fn access_mode(&self) -> AccessMode {
			AccessMode::ReadWrite
		}
	}

	#[tokio::test]
	async fn a_mutating_alias_body_propagates_read_write_through_nesting() {
		// Access-mode propagation decides the transaction mode, so a mutation
		// buried in an alias body -- at any nesting level -- has to surface.
		let mutating: Arc<dyn PhysicalExpr> = Arc::new(MutatingStep);

		let flat = DestructurePart {
			fields: vec![DestructureField::Aliased {
				field: "x".into(),
				body: Arc::clone(&mutating),
			}],
		};
		assert_eq!(flat.access_mode(), AccessMode::ReadWrite);

		let nested = DestructurePart {
			fields: vec![DestructureField::Nested {
				field: "outer".into(),
				parts: vec![DestructureField::Aliased {
					field: "x".into(),
					body: mutating,
				}],
			}],
		};
		assert_eq!(nested.access_mode(), AccessMode::ReadWrite);

		let readonly = DestructurePart {
			fields: vec![DestructureField::Field("a".into()), DestructureField::All("b".into())],
		};
		assert_eq!(readonly.access_mode(), AccessMode::ReadOnly);
	}

	#[tokio::test]
	async fn sql_rendering_covers_all_four_field_shapes() {
		let ctx = root_ctx();
		let part = compile_destructure("v.{ a, z: b, inner.{ c }, spread.* }", &ctx).await;
		assert_eq!(part.to_sql(), "{a, z: .b, inner: {c}, spread.*}");
	}

	#[tokio::test]
	async fn sql_rendering_covers_a_trailing_rest_field() {
		let ctx = root_ctx();
		let part = compile_destructure("v.{ a, ... }", &ctx).await;
		assert_eq!(part.to_sql(), "{a, ...}");
	}
}
