use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::idiom::Idiom;
use crate::val::Value;

/// Splits values on array/set fields, producing the cartesian product.
///
/// For example, if a record has `{ a: [1, 2], b: [3, 4] }` and we split on `a, b`,
/// we get four records: `{ a: 1, b: 3 }`, `{ a: 1, b: 4 }`, `{ a: 2, b: 3 }`, `{ a: 2, b: 4 }`.
///
/// Non-array values pass through unchanged.
#[derive(Debug, Clone)]
pub struct Split {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) idioms: Vec<Idiom>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Split {
	pub(crate) fn new(input: Arc<dyn ExecOperator>, idioms: Vec<Idiom>) -> Self {
		Self {
			input,
			idioms,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for Split {
	fn name(&self) -> &'static str {
		"Split"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		use surrealdb_types::ToSql;
		vec![(
			"on".to_string(),
			self.idioms.iter().map(|i| i.to_sql()).collect::<Vec<_>>().join(", "),
		)]
	}

	fn required_context(&self) -> ContextLevel {
		// Split inherits its input's context requirements
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		// Split doesn't modify data, just expands it
		self.input.access_mode()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let idioms = self.idioms.clone();

		let split_stream = input_stream.map(move |batch_result| {
			let idioms = idioms.clone();
			match batch_result {
				Ok(batch) => {
					let mut expanded_values = Vec::with_capacity(batch.values.len() * idioms.len());

					for value in batch.values {
						// Split this value on all idioms sequentially
						let mut current_values = vec![value];

						for idiom in &idioms {
							let mut next_values = Vec::new();
							for val in current_values {
								split_value_on_idiom(val, idiom, &mut next_values);
							}
							current_values = next_values;
						}

						expanded_values.extend(current_values);
					}

					Ok(ValueBatch {
						values: expanded_values,
					})
				}
				Err(e) => Err(e),
			}
		});

		Ok(monitor_stream(Box::pin(split_stream), "Split", &self.metrics))
	}
}

/// Split a single value on an idiom field.
///
/// If the field at the idiom is an array or set, produces multiple values
/// with that field replaced by each element. Otherwise, returns the value unchanged.
fn split_value_on_idiom(value: Value, idiom: &Idiom, output: &mut Vec<Value>) {
	// Get the value at the idiom path
	let field_value = value.pick(idiom);

	match field_value {
		Value::Array(arr) => {
			if arr.is_empty() {
				output.push(value);
			} else {
				let len = arr.len();
				for (i, element) in arr.into_iter().enumerate() {
					if i == len - 1 {
						// Last element: move value instead of cloning
						let mut val = value;
						val.put(idiom, element);
						output.push(val);
						return;
					}
					let mut cloned = value.clone();
					cloned.put(idiom, element);
					output.push(cloned);
				}
			}
		}
		Value::Set(set) => {
			if set.is_empty() {
				output.push(value);
			} else {
				let len = set.len();
				for (i, element) in set.into_iter().enumerate() {
					if i == len - 1 {
						// Last element: move value instead of cloning
						let mut val = value;
						val.put(idiom, element);
						output.push(val);
						return;
					}
					let mut cloned = value.clone();
					cloned.put(idiom, element);
					output.push(cloned);
				}
			}
		}
		// Non-array/set values pass through unchanged
		_ => {
			output.push(value);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::part::Part;
	use crate::val::{Array, Object, Set};

	#[test]
	fn test_split_on_array() {
		let idiom = Idiom(vec![Part::Field("tags".into())]);
		let value = Value::Object(Object::from_iter([
			("id".to_string(), Value::from("t:1")),
			("tags".to_string(), Value::Array(Array::from(vec![1, 2, 3]))),
		]));

		let mut output = Vec::new();
		split_value_on_idiom(value, &idiom, &mut output);

		assert_eq!(output.len(), 3);
	}

	#[test]
	fn test_split_on_set() {
		let idiom = Idiom(vec![Part::Field("tags".into())]);
		let value = Value::Object(Object::from_iter([
			("id".to_string(), Value::from("t:1")),
			("tags".to_string(), Value::Set(Set::from(vec![1, 2, 3]))),
		]));

		let mut output = Vec::new();
		split_value_on_idiom(value, &idiom, &mut output);

		assert_eq!(output.len(), 3);
	}

	#[test]
	fn test_split_on_non_array() {
		let idiom = Idiom(vec![Part::Field("name".into())]);
		let value = Value::Object(Object::from_iter([
			("id".to_string(), Value::from("t:1")),
			("name".to_string(), Value::from("test")),
		]));

		let mut output = Vec::new();
		split_value_on_idiom(value.clone(), &idiom, &mut output);

		assert_eq!(output.len(), 1);
		assert_eq!(output[0], value);
	}

	#[test]
	fn test_split_empty_array() {
		let idiom = Idiom(vec![Part::Field("tags".into())]);
		let value = Value::Object(Object::from_iter([
			("id".to_string(), Value::from("t:1")),
			("tags".to_string(), Value::Array(Array::from(Vec::<Value>::new()))),
		]));

		let mut output = Vec::new();
		split_value_on_idiom(value, &idiom, &mut output);

		assert_eq!(output.len(), 1);
	}

	#[test]
	fn test_split_empty_set() {
		let idiom = Idiom(vec![Part::Field("tags".into())]);
		let value = Value::Object(Object::from_iter([
			("id".to_string(), Value::from("t:1")),
			("tags".to_string(), Value::Set(Set::new())),
		]));

		let mut output = Vec::new();
		split_value_on_idiom(value, &idiom, &mut output);

		assert_eq!(output.len(), 1);
	}
}
