//! Omit operator for removing fields from output.
//!
//! The Omit operator removes specified fields from each record in the stream.
//! Used to implement the OMIT clause in SELECT statements.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::err::Error;
use crate::exec::{
	AccessMode, ContextLevel, ExecutionContext, OperatorPlan, ValueBatch, ValueBatchStream,
};
use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::val::Value;

/// Omit operator - removes specified fields from each record.
///
/// This operator is used to implement the OMIT clause in SELECT statements.
/// It removes the specified fields from each object in the stream.
#[derive(Debug, Clone)]
pub struct Omit {
	/// The input plan to omit fields from
	pub input: Arc<dyn OperatorPlan>,
	/// The fields to omit (as idioms for nested field support)
	pub fields: Vec<Idiom>,
}

#[async_trait]
impl OperatorPlan for Omit {
	fn name(&self) -> &'static str {
		"Omit"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		use surrealdb_types::ToSql;
		vec![(
			"fields".to_string(),
			self.fields.iter().map(|i| i.to_sql()).collect::<Vec<_>>().join(", "),
		)]
	}

	fn required_context(&self) -> ContextLevel {
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		self.input.access_mode()
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		let input_stream = self.input.execute(ctx)?;
		let fields = self.fields.clone();

		let omitted = input_stream.map(move |batch_result| {
			let batch = batch_result?;
			let mut omitted_values = Vec::with_capacity(batch.values.len());

			for mut value in batch.values {
				// Remove each omit field from the value
				for field in &fields {
					omit_field_sync(&mut value, field);
				}
				omitted_values.push(value);
			}

			Ok(ValueBatch {
				values: omitted_values,
			})
		});

		Ok(Box::pin(omitted))
	}
}

/// Synchronously remove a field from a value by idiom path.
/// This is a simplified version that only supports simple field paths.
fn omit_field_sync(value: &mut Value, idiom: &Idiom) {
	// For simple single-part idioms, directly remove from object
	if idiom.len() == 1 {
		if let Some(Part::Field(field_name)) = idiom.first() {
			if let Value::Object(obj) = value {
				obj.remove(&**field_name);
			}
		}
	} else {
		// For nested paths, traverse and remove
		omit_nested_field(value, idiom, 0);
	}
}

/// Recursively traverse and remove a nested field.
fn omit_nested_field(value: &mut Value, idiom: &Idiom, depth: usize) {
	if depth >= idiom.len() {
		return;
	}

	let Some(part) = idiom.get(depth) else {
		return;
	};

	match part {
		Part::Field(field_name) => {
			if let Value::Object(obj) = value {
				if depth == idiom.len() - 1 {
					// Last part - remove the field
					obj.remove(&**field_name);
				} else {
					// Not last part - recurse into the field
					if let Some(nested) = obj.get_mut(&**field_name) {
						omit_nested_field(nested, idiom, depth + 1);
					}
				}
			}
		}
		Part::All => {
			// Apply to all elements
			match value {
				Value::Object(obj) => {
					for (_, v) in obj.iter_mut() {
						omit_nested_field(v, idiom, depth + 1);
					}
				}
				Value::Array(arr) => {
					for v in arr.iter_mut() {
						omit_nested_field(v, idiom, depth + 1);
					}
				}
				_ => {}
			}
		}
Part::Value(expr) => {
			// Handle array index access: [0], [1], etc.
			if let crate::expr::Expr::Literal(crate::expr::Literal::Integer(idx)) = expr {
				if let Value::Array(arr) = value {
					if let Some(nested) = arr.get_mut(*idx as usize) {
						if depth == idiom.len() - 1 {
							// Can't "remove" an array element by index, set to None
							*nested = Value::None;
						} else {
							omit_nested_field(nested, idiom, depth + 1);
						}
					}
				}
			}
		}
		_ => {
			// Other part types are not supported for omit
		}
	}
}
