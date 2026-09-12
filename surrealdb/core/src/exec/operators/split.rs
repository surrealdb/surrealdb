use std::sync::Arc;

use futures::StreamExt;
use surrealdb_types::ToSql as _;

use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::logical::{LogicalExpr, LogicalLiteral, OutputPath, OutputPathPart};
use crate::val::Value;
use crate::val::field_path::{FieldPath, FieldPathPart};

/// Splits values on array/set fields, producing the cartesian product.
///
/// For example, if a record has `{ a: [1, 2], b: [3, 4] }` and we split on `a, b`,
/// we get four records: `{ a: 1, b: 3 }`, `{ a: 1, b: 4 }`, `{ a: 2, b: 3 }`, `{ a: 2, b: 4 }`.
///
/// Non-array values pass through unchanged.
///
/// Each `SPLIT` target is read per row at the position the target itself
/// names (see [`split_write_path`]) to find the collection to split on, and
/// each element is written back at that same position. Read and write are one
/// path: splitting `a[0]` replaces the first element of `a` and leaves the
/// rest of `a` standing.
///
/// The read navigates the row and does not dereference — a target crossing a
/// record link, `link.tags` over `link: cfg:one`, reads `NONE` rather than the
/// linked record's field, so it is not a collection and the row does not
/// split. A `SPLIT` names a position on the row to replace, and a field of
/// another record is not one.
#[derive(Debug, Clone)]
pub struct Split {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// The `SPLIT` targets as written, for EXPLAIN.
	pub(crate) exprs: Vec<LogicalExpr>,
	/// The compiled targets, one per entry of `exprs`. They report what the
	/// operator requires and render it for `EXPLAIN`; the collection to split
	/// on is read structurally at `paths`, never through these.
	pub(crate) keys: Vec<Arc<dyn PhysicalExpr>>,
	/// Where each target's elements are written back, one per entry of `exprs`.
	paths: Vec<FieldPath>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Split {
	/// `keys` are the compiled forms of `exprs`, in the same order.
	///
	/// Declines a target whose position the write path cannot name, so the
	/// statement runs on the per-row engine rather than writing an element
	/// back somewhere the target did not point at.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		exprs: Vec<LogicalExpr>,
		keys: Vec<Arc<dyn PhysicalExpr>>,
	) -> Result<Self, crate::exec::Error> {
		debug_assert_eq!(exprs.len(), keys.len(), "one compiled key per SPLIT target");
		let paths = exprs
			.iter()
			.map(|expr| {
				split_write_path(expr).ok_or_else(|| {
					crate::exec::Error::PlannerUnsupported(format!(
						"a SPLIT target of this shape has no write position: {}",
						expr.to_sql()
					))
				})
			})
			.collect::<Result<Vec<_>, _>>()?;
		Ok(Self {
			input,
			exprs,
			keys,
			paths,
			metrics: Arc::new(OperatorMetrics::new()),
		})
	}
}

/// Where an element of a `SPLIT` target's collection is written back: the
/// position the target itself names, not the name it would project under.
///
/// The two differ wherever the target ends in a positional step. `SPLIT a[0]`
/// projects under `a` — that is what an output name is for — but it reads the
/// first element of `a` and must write each of its elements back over that
/// element alone, leaving `a[1]` as it stands. Building the write path from
/// the projection name would set the whole of `a` and lose the rest of it.
///
/// `None` for a target this cannot name, `a[*]` among them: it reads every
/// element, so writing one back is a position no single path holds.
pub(crate) fn split_write_path(expr: &LogicalExpr) -> Option<FieldPath> {
	fn walk(expr: &LogicalExpr, parts: &mut Vec<FieldPathPart>) -> Option<()> {
		match expr {
			LogicalExpr::Field(name) => {
				parts.push(FieldPathPart::Field(name.as_str().to_owned()));
				Some(())
			}
			LogicalExpr::Access {
				base,
				field,
			} => {
				walk(base, parts)?;
				parts.push(FieldPathPart::Field(field.as_str().to_owned()));
				Some(())
			}
			LogicalExpr::Index {
				base,
				index,
			} => {
				walk(base, parts)?;
				match index.as_ref() {
					LogicalExpr::Literal(LogicalLiteral::Integer(i)) if *i >= 0 => {
						parts.push(FieldPathPart::Index(*i as usize));
						Some(())
					}
					_ => None,
				}
			}
			LogicalExpr::First(base) => {
				walk(base, parts)?;
				parts.push(FieldPathPart::First);
				Some(())
			}
			LogicalExpr::Last(base) => {
				walk(base, parts)?;
				parts.push(FieldPathPart::Last);
				Some(())
			}
			LogicalExpr::Lookup {
				..
			} => {
				let path = output_field_path(&expr.output_path());
				parts.extend(path.0);
				Some(())
			}
			_ => None,
		}
	}
	let mut parts = Vec::new();
	walk(expr, &mut parts)?;
	Some(FieldPath(parts))
}

/// The navigable position an [`OutputPath`] names in a row: every key,
/// a lookup's rendering included, is a field of the value it lands in.
pub(crate) fn output_field_path(path: &OutputPath) -> FieldPath {
	FieldPath(
		path.parts()
			.iter()
			.map(|part| match part {
				OutputPathPart::Field(name) => FieldPathPart::Field(name.clone()),
				OutputPathPart::Lookup(lookup) => FieldPathPart::Lookup(lookup.clone()),
			})
			.collect(),
	)
}

impl ExecOperator for Split {
	fn name(&self) -> &'static str {
		"Split"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		use surrealdb_types::ToSql;
		vec![(
			"on".to_string(),
			self.exprs.iter().map(|e| e.to_sql()).collect::<Vec<_>>().join(", "),
		)]
	}

	fn required_context(&self) -> ContextLevel {
		self.keys
			.iter()
			.map(|key| key.required_context())
			.max()
			.unwrap_or(ContextLevel::Root)
			.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// Split doesn't modify data, just expands it; the keys it evaluates
		// may carry their own requirements.
		self.keys.iter().fold(self.input.access_mode(), |mode, key| mode.combine(key.access_mode()))
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		self.keys.iter().map(|key| ("on", key)).collect()
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let paths = self.paths.clone();

		let split_stream = input_stream.then(move |batch_result| {
			let paths = paths.clone();
			async move {
				let batch = batch_result?;
				let mut expanded_values = Vec::with_capacity(batch.len() * paths.len());

				for value in batch.into_values() {
					// Split this value on all targets sequentially
					let mut current_values = vec![value];

					for path in paths.iter() {
						let mut next_values = Vec::new();
						for val in current_values {
							let on = path.extract(&val).into_owned();
							split_value_on_path(val, on, path, &mut next_values);
						}
						current_values = next_values;
					}

					expanded_values.extend(current_values);
				}

				Ok(ValueBatch::new(expanded_values))
			}
		});

		Ok(monitor_stream(Box::pin(split_stream), "Split", &self.metrics))
	}
}

/// Split a single value on the collection `on` found at `path`.
///
/// If `on` is an array or set, produces one copy of `value` per element with
/// the field at `path` replaced by that element. Otherwise, returns the value
/// unchanged.
fn split_value_on_path(value: Value, on: Value, path: &FieldPath, output: &mut Vec<Value>) {
	match on {
		Value::Array(arr) => {
			if arr.is_empty() {
				output.push(value);
			} else {
				let len = arr.len();
				for (i, element) in arr.into_iter().enumerate() {
					if i == len - 1 {
						// Last element: move value instead of cloning
						let mut val = value;
						val.set_at_field_path_owned(path, element);
						output.push(val);
						return;
					}
					let mut cloned = value.clone();
					cloned.set_at_field_path_owned(path, element);
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
						val.set_at_field_path_owned(path, element);
						output.push(val);
						return;
					}
					let mut cloned = value.clone();
					cloned.set_at_field_path_owned(path, element);
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
	use crate::val::{Array, Object, Set};

	/// Split `value` on the field `name`, reading the collection off the value
	/// the way the operator's compiled key would for a stored field.
	fn split_on_field(value: Value, name: &str, output: &mut Vec<Value>) {
		let path = FieldPath::field(name);
		let on = path.extract(&value).into_owned();
		split_value_on_path(value, on, &path, output);
	}

	#[test]
	fn test_split_on_array() {
		let value = Value::Object(Object::from_iter([
			("id".to_string(), Value::from("t:1")),
			("tags".to_string(), Value::Array(Array::from(vec![1, 2, 3]))),
		]));

		let mut output = Vec::new();
		split_on_field(value, "tags", &mut output);

		assert_eq!(output.len(), 3);
	}

	#[test]
	fn test_split_on_set() {
		let value = Value::Object(Object::from_iter([
			("id".to_string(), Value::from("t:1")),
			("tags".to_string(), Value::Set(Set::from(vec![1, 2, 3]))),
		]));

		let mut output = Vec::new();
		split_on_field(value, "tags", &mut output);

		assert_eq!(output.len(), 3);
	}

	#[test]
	fn test_split_on_non_array() {
		let value = Value::Object(Object::from_iter([
			("id".to_string(), Value::from("t:1")),
			("name".to_string(), Value::from("test")),
		]));

		let mut output = Vec::new();
		split_on_field(value.clone(), "name", &mut output);

		assert_eq!(output.len(), 1);
		assert_eq!(output[0], value);
	}

	#[test]
	fn test_split_empty_array() {
		let value = Value::Object(Object::from_iter([
			("id".to_string(), Value::from("t:1")),
			("tags".to_string(), Value::Array(Array::from(Vec::<Value>::new()))),
		]));

		let mut output = Vec::new();
		split_on_field(value, "tags", &mut output);

		assert_eq!(output.len(), 1);
	}

	#[test]
	fn test_split_empty_set() {
		let value = Value::Object(Object::from_iter([
			("id".to_string(), Value::from("t:1")),
			("tags".to_string(), Value::Set(Set::new())),
		]));

		let mut output = Vec::new();
		split_on_field(value, "tags", &mut output);

		assert_eq!(output.len(), 1);
	}

	#[test]
	fn an_output_path_navigates_as_its_keys() {
		let path = OutputPath::keys(vec![
			OutputPathPart::Field("a".to_string()),
			OutputPathPart::Lookup("->likes".to_string()),
			OutputPathPart::Field("b".to_string()),
		]);
		assert_eq!(
			output_field_path(&path),
			FieldPath(vec![
				FieldPathPart::Field("a".to_string()),
				FieldPathPart::Lookup("->likes".to_string()),
				FieldPathPart::Field("b".to_string()),
			])
		);
	}
}
