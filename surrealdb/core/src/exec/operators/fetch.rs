use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::err::Error;
use crate::exec::{
	AccessMode, ContextLevel, ExecutionContext, OperatorPlan, ValueBatch, ValueBatchStream,
};
use crate::expr::idiom::Idiom;
use crate::val::{RecordId, Value};

/// Fetches related records for specified fields.
///
/// The FETCH clause replaces record IDs with their full record data.
/// For example, if a field contains `author:tobie`, FETCH will replace
/// it with the full author record `{ id: author:tobie, name: 'Tobie', ... }`.
#[derive(Debug, Clone)]
pub struct Fetch {
	pub(crate) input: Arc<dyn OperatorPlan>,
	/// The fields to fetch. Each idiom points to a field that may contain
	/// record IDs to be resolved.
	pub(crate) fields: Vec<Idiom>,
}

#[async_trait]
impl OperatorPlan for Fetch {
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

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
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
		fetch_field_in_place(ctx, &mut value, field).await?;
	}
	Ok(value)
}

/// Fetch a single field, resolving record IDs to full records.
async fn fetch_field_in_place(
	ctx: &ExecutionContext,
	value: &mut Value,
	field: &Idiom,
) -> crate::expr::FlowResult<()> {
	// Get the value at the field path
	let field_value = value.pick(field);

	match field_value {
		Value::RecordId(ref rid) => {
			// Fetch the record and replace the field value
			let fetched = fetch_record(ctx, rid).await?;
			value.put(field, fetched);
		}
		Value::Array(ref arr) => {
			// For arrays, fetch each element that is a record ID
			let mut fetched_array = Vec::with_capacity(arr.len());
			for item in arr.iter() {
				if let Value::RecordId(rid) = item {
					fetched_array.push(fetch_record(ctx, rid).await?);
				} else {
					fetched_array.push(item.clone());
				}
			}
			value.put(field, Value::Array(fetched_array.into()));
		}
		// Other values are left unchanged
		_ => {}
	}

	Ok(())
}

/// Fetch a single record by its ID.
async fn fetch_record(ctx: &ExecutionContext, rid: &RecordId) -> crate::expr::FlowResult<Value> {
	// Get the database context
	let db_ctx = ctx.database().map_err(|e| crate::expr::ControlFlow::Err(e.into()))?;

	// Read the record from the datastore
	let txn = db_ctx.ns_ctx.root.txn.as_ref();
	let key = crate::key::record::new(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id, &rid.table, &rid.key);

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

	#[test]
	fn test_fetch_name() {
		use crate::exec::operators::scan::Scan;
		use crate::expr::part::Part;

		let fields = vec![Idiom(vec![Part::Field("author".into())])];

		// Create a minimal scan for testing
		let scan = Arc::new(Scan {
			table: "test".to_string(),
			fields: None,
			condition: None,
		});

		let fetch = Fetch {
			input: scan,
			fields,
		};

		assert_eq!(fetch.name(), "Fetch");
	}
}
