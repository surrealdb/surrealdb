use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, ValueBatch,
	ValueBatchStream,
};
use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::val::{RecordId, Value};

/// Fetches related records for specified fields.
///
/// The FETCH clause replaces record IDs with their full record data.
/// For example, if a field contains `author:tobie`, FETCH will replace
/// it with the full author record `{ id: author:tobie, name: 'Tobie', ... }`.
#[derive(Debug, Clone)]
pub struct Fetch {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// The fields to fetch. Each idiom points to a field that may contain
	/// record IDs to be resolved.
	pub(crate) fields: Vec<Idiom>,
}

#[async_trait]
impl ExecOperator for Fetch {
	fn name(&self) -> &'static str {
		"Fetch"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		use surrealdb_types::ToSql;
		vec![(
			"fields".to_string(),
			self.fields.iter().map(|f| f.to_sql()).collect::<Vec<_>>().join(", "),
		)]
	}

	fn required_context(&self) -> ContextLevel {
		// Fetch needs database context for reading related records
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// Fetch reads related records
		AccessMode::ReadOnly.combine(self.input.access_mode())
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn is_scalar(&self) -> bool {
		// Fetch preserves the scalar nature of its input.
		// If the input is a scalar expression (e.g., RETURN $var FETCH field),
		// the result should also be treated as scalar.
		self.input.is_scalar()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = self.input.execute(ctx)?;
		let fields = self.fields.clone();
		let ctx = ctx.clone();

		let fetch_stream = input_stream.then(move |batch_result| {
			let fields = fields.clone();
			let ctx = ctx.clone();

			async move {
				let batch = batch_result?;
				let mut fetched_values = Vec::with_capacity(batch.values.len());

				for value in batch.values {
					let fetched = fetch_fields(&ctx, value, &fields).await?;
					fetched_values.push(fetched);
				}

				Ok(ValueBatch {
					values: fetched_values,
				})
			}
		});

		Ok(Box::pin(fetch_stream))
	}
}

/// Fetch all specified fields for a value.
async fn fetch_fields(
	ctx: &ExecutionContext,
	mut value: Value,
	fields: &[Idiom],
) -> crate::expr::FlowResult<Value> {
	for field in fields {
		fetch_field_recursive(ctx, &mut value, &field.0, 0).await?;
	}
	Ok(value)
}

/// Recursively fetch a field path, handling wildcards and nested paths.
///
/// This traverses the path parts one by one:
/// - Field: navigate into the object field
/// - All (*): apply remaining path to all array/object elements
/// - At the end, if we have a RecordId, fetch it; if we have an array of RecordIds, fetch each
fn fetch_field_recursive<'a>(
	ctx: &'a ExecutionContext,
	value: &'a mut Value,
	path: &'a [Part],
	depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::expr::FlowResult<()>> + Send + 'a>> {
	Box::pin(async move {
		// If we've reached the end of the path, check if we need to fetch the current value
		if depth >= path.len() {
			return fetch_value_if_record(ctx, value).await;
		}

		let part = &path[depth];
		let remaining = &path[depth + 1..];

		match part {
			Part::Field(field_name) => {
				match value {
					Value::Object(obj) => {
						if let Some(field_value) = obj.get_mut(field_name.as_str()) {
							// If this field is a RecordId and we have more path to traverse,
							// we need to fetch it first so we can navigate into it
							if matches!(field_value, Value::RecordId(_)) && !remaining.is_empty() {
								fetch_value_if_record(ctx, field_value).await?;
							}
							// Continue traversing into this field
							fetch_field_recursive(ctx, field_value, path, depth + 1).await?;
						}
					}
					Value::Array(arr) => {
						// Apply path to each element in the array
						for item in arr.iter_mut() {
							fetch_field_recursive(ctx, item, path, depth).await?;
						}
					}
					Value::RecordId(_) => {
						// The current value is a RecordId - fetch it first, then navigate
						fetch_value_if_record(ctx, value).await?;
						// Now try again with the fetched value
						fetch_field_recursive(ctx, value, path, depth).await?;
					}
					_ => {}
				}
			}
			Part::All => {
				// Wildcard - apply remaining path to all elements
				match value {
					Value::Array(arr) => {
						for item in arr.iter_mut() {
							// If the item is a RecordId, we must fetch it first
							// before we can navigate into its fields
							if matches!(item, Value::RecordId(_)) {
								fetch_value_if_record(ctx, item).await?;
							}
							if remaining.is_empty() {
								// No more path - we're done (already fetched if needed)
							} else {
								// Continue with remaining path on the (possibly fetched) item
								fetch_field_recursive(ctx, item, path, depth + 1).await?;
							}
						}
					}
					Value::Object(obj) => {
						for (_, field_value) in obj.iter_mut() {
							// If the field value is a RecordId, fetch it first
							if matches!(field_value, Value::RecordId(_)) {
								fetch_value_if_record(ctx, field_value).await?;
							}
							if !remaining.is_empty() {
								// Continue with remaining path
								fetch_field_recursive(ctx, field_value, path, depth + 1).await?;
							}
						}
					}
					_ => {}
				}
			}
			Part::First => {
				if let Value::Array(arr) = value {
					if let Some(first) = arr.first_mut() {
						fetch_field_recursive(ctx, first, path, depth + 1).await?;
					}
				}
			}
			Part::Last => {
				if let Value::Array(arr) = value {
					if let Some(last) = arr.last_mut() {
						fetch_field_recursive(ctx, last, path, depth + 1).await?;
					}
				}
			}
			// For other path parts, we don't support fetching through them
			_ => {}
		}

		Ok(())
	})
}

/// If the value is a RecordId, fetch it and replace the value.
/// If it's an array of RecordIds, fetch each one.
async fn fetch_value_if_record(
	ctx: &ExecutionContext,
	value: &mut Value,
) -> crate::expr::FlowResult<()> {
	match value {
		Value::RecordId(rid) => {
			let fetched = fetch_record(ctx, rid).await?;
			*value = fetched;
		}
		Value::Array(arr) => {
			// Fetch each RecordId in the array
			for item in arr.iter_mut() {
				if let Value::RecordId(rid) = item {
					let fetched = fetch_record(ctx, rid).await?;
					*item = fetched;
				}
			}
		}
		_ => {}
	}
	Ok(())
}

/// Fetch a single record by its ID.
pub(crate) async fn fetch_record(
	ctx: &ExecutionContext,
	rid: &RecordId,
) -> crate::expr::FlowResult<Value> {
	// Get the database context
	let db_ctx = ctx.database().map_err(|e| crate::expr::ControlFlow::Err(e.into()))?;

	// Read the record from the datastore
	let txn = db_ctx.ns_ctx.root.txn.as_ref();
	let key = crate::key::record::new(
		db_ctx.ns_ctx.ns.namespace_id,
		db_ctx.db.database_id,
		&rid.table,
		&rid.key,
	);

	match txn.get(&key, None).await {
		Ok(Some(record)) => {
			// Extract the Value from the Record
			let mut val = record.data.as_ref().clone();
			// Inject the record ID
			val.def(rid);
			Ok(val)
		}
		Ok(None) => Ok(Value::None),
		Err(e) => Err(crate::expr::ControlFlow::Err(e.into())),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::physical_expr::Literal;

	#[test]
	fn test_fetch_name() {
		use crate::exec::operators::scan::Scan;
		use crate::expr::part::Part;

		let fields = vec![Idiom(vec![Part::Field("author".into())])];

		// Create a minimal scan for testing
		let scan = Arc::new(Scan {
			source: Arc::new(Literal(Value::from("test"))) as Arc<dyn crate::exec::PhysicalExpr>,
			version: None,
		});

		let fetch = Fetch {
			input: scan,
			fields,
		};

		assert_eq!(fetch.name(), "Fetch");
	}
}
