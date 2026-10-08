use std::sync::Arc;

use futures::StreamExt;
use surrealdb_types::ToSql as _;

use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::logical::{LogicalExpr, LogicalLiteral, OutputPath, OutputPathPart};
use crate::val::field_path::{FieldPath, FieldPathPart};
use crate::val::{Number, Value};

/// Splits values on array/set fields, producing the cartesian product.
///
/// For example, if a record has `{ a: [1, 2], b: [3, 4] }` and we split on `a, b`,
/// we get four records: `{ a: 1, b: 3 }`, `{ a: 1, b: 4 }`, `{ a: 2, b: 3 }`, `{ a: 2, b: 4 }`.
///
/// Non-array values pass through unchanged.
///
/// Each `SPLIT` target is read per row at the position the target itself
/// names (see [`split_target`]) to find the collection to split on, and
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
	targets: Vec<SplitTarget>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Split {
	/// `keys` are the compiled forms of `exprs`, in the same order.
	///
	/// Fails only for a target that is not a path over the row, which the
	/// grammar of `SPLIT` does not produce.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		exprs: Vec<LogicalExpr>,
		keys: Vec<Arc<dyn PhysicalExpr>>,
	) -> Result<Self, crate::exec::Error> {
		debug_assert_eq!(exprs.len(), keys.len(), "one compiled key per SPLIT target");
		let targets = exprs
			.iter()
			.map(|expr| {
				split_target(expr).ok_or_else(|| {
					crate::exec::Error::Unimplemented(format!(
						"a SPLIT target that is not a path over the row: {}",
						expr.to_sql()
					))
				})
			})
			.collect::<Result<Vec<_>, _>>()?;
		Ok(Self {
			input,
			exprs,
			keys,
			targets,
			metrics: Arc::new(OperatorMetrics::new()),
		})
	}
}

/// Where a `SPLIT` writes each element of the collection it read.
///
/// A target names one position and the element replaces what is there. `a[*]`
/// names every position instead: it reads every element, so writing one back
/// puts it at each of them — `SPLIT a[*]` over `{ a: [[1, 2], [3, 4]] }` gives
/// `{ a: [[1, 2], [1, 2]] }` and `{ a: [[3, 4], [3, 4]] }`, where `SPLIT a`
/// gives `{ a: [1, 2] }` and `{ a: [3, 4] }`. Both read the same collection;
/// only the write differs.
///
/// A target that reaches through the elements of a collection names one
/// position in each of them. `SPLIT a[*].b` over `{ a: [{ b: 1 }, { b: 2 }] }`
/// reads `[1, 2]` and gives `{ a: [{ b: 1 }, { b: 1 }] }` and
/// `{ a: [{ b: 2 }, { b: 2 }] }`: every element of `a` takes the value the row
/// was split on at its own position.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SplitTarget {
	/// One position, named by the path.
	At(FieldPath),
	/// Every element of the collection at the path.
	Every(FieldPath),
	/// A position within each element of the collection at the path, named
	/// by the inner target relative to that element.
	Through(FieldPath, Box<SplitTarget>),
	/// A position no row holds: the read yields `NONE`, so the row is not
	/// split and nothing is written. `a[1.5]` names an element no array has.
	Nowhere,
}

impl SplitTarget {
	/// What the split reads from `value`: the collection whose elements the
	/// row is split on, or a value that is not one when the position holds
	/// none.
	///
	/// A target reaching through a collection reads the inner target of each
	/// of its elements and collects the results, so its read is a collection
	/// of the same length.
	pub(crate) fn read(&self, value: &Value) -> Value {
		match self {
			SplitTarget::At(path) | SplitTarget::Every(path) => path.extract(value).into_owned(),
			SplitTarget::Through(path, inner) => match path.extract(value).as_ref() {
				Value::Array(arr) => Value::Array(
					arr.iter().map(|element| inner.read(element)).collect::<Vec<_>>().into(),
				),
				Value::Object(obj) => Value::Array(
					obj.values().map(|element| inner.read(element)).collect::<Vec<_>>().into(),
				),
				_ => Value::None,
			},
			SplitTarget::Nowhere => Value::None,
		}
	}

	/// Writes `element` where this target points.
	fn write(&self, value: &mut Value, element: Value) {
		match self {
			SplitTarget::At(path) => value.set_at_field_path_owned(path, element),
			SplitTarget::Every(path) => {
				// Each position takes the element, so the collection keeps its
				// shape and every slot holds what this row was split on.
				let filled = match path.extract(value).as_ref() {
					Value::Array(arr) => Value::Array(vec![element; arr.len()].into()),
					Value::Object(obj) => {
						let mut out = crate::val::Object::default();
						for key in obj.keys().cloned().collect::<Vec<_>>() {
							out.insert(key, element.clone());
						}
						Value::Object(out)
					}
					// Not a collection: there are no positions to fill, and
					// the read yielded nothing to split on either.
					_ => return,
				};
				value.set_at_field_path_owned(path, filled);
			}
			SplitTarget::Through(path, inner) => {
				let mut collection = path.extract(value).into_owned();
				match &mut collection {
					Value::Array(arr) => {
						for slot in arr.iter_mut() {
							inner.write(slot, element.clone());
						}
					}
					Value::Object(obj) => {
						for slot in obj.values_mut() {
							inner.write(slot, element.clone());
						}
					}
					// Not a collection: no element holds a position to write.
					_ => return,
				}
				value.set_at_field_path_owned(path, collection);
			}
			SplitTarget::Nowhere => {}
		}
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
/// A step that reaches through every element (`a[*].b`) divides the path:
/// what precedes it locates the collection, and what follows it is the
/// position within each element, which may itself reach through another
/// collection.
///
/// `None` only for a target the path grammar of `SPLIT` does not produce: an
/// expression that is not a path over the row.
pub(crate) fn split_target(expr: &LogicalExpr) -> Option<SplitTarget> {
	/// The path walked so far, divided where a step reached through every
	/// element: `through` holds the paths that located each collection, and
	/// `parts` the path since the last of them.
	struct Walk {
		parts: Vec<FieldPathPart>,
		through: Vec<FieldPath>,
		/// Whether the walk ended on the step that reached through the
		/// elements, which names every position of the collection itself.
		ends_on_every: bool,
		/// A step that names an element no collection holds.
		nowhere: bool,
	}

	fn walk(expr: &LogicalExpr, w: &mut Walk) -> Option<()> {
		w.ends_on_every = false;
		match expr {
			// The value a body is applied to: the walk is already there.
			LogicalExpr::Current => Some(()),
			LogicalExpr::Field(name) => {
				w.parts.push(FieldPathPart::Field(name.as_str().to_owned()));
				Some(())
			}
			LogicalExpr::Access {
				base,
				field,
			} => {
				walk(base, w)?;
				w.parts.push(FieldPathPart::Field(field.as_str().to_owned()));
				w.ends_on_every = false;
				Some(())
			}
			LogicalExpr::Index {
				base,
				index,
			} => {
				walk(base, w)?;
				w.ends_on_every = false;
				let number = match index.as_ref() {
					LogicalExpr::Literal(LogicalLiteral::Integer(i)) => Number::Int(*i),
					LogicalExpr::Literal(LogicalLiteral::Float(f)) => Number::Float(*f),
					LogicalExpr::Literal(LogicalLiteral::Decimal(d)) => Number::Decimal(*d),
					_ => return None,
				};
				match number.as_array_index() {
					Some(i) => w.parts.push(FieldPathPart::Index(i)),
					// A fractional or negative index names no element of any
					// array, whatever the row holds.
					None => w.nowhere = true,
				}
				Some(())
			}
			LogicalExpr::First(base) => {
				walk(base, w)?;
				w.parts.push(FieldPathPart::First);
				w.ends_on_every = false;
				Some(())
			}
			LogicalExpr::Last(base) => {
				walk(base, w)?;
				w.parts.push(FieldPathPart::Last);
				w.ends_on_every = false;
				Some(())
			}
			// `a[*]`: every position of what the base reached.
			LogicalExpr::Each(base) => {
				walk(base, w)?;
				w.through.push(FieldPath(std::mem::take(&mut w.parts)));
				w.ends_on_every = true;
				Some(())
			}
			// A path nested under a field lowers to a body applied to what
			// the base reaches, so the position it names is the base's
			// followed by the body's.
			LogicalExpr::Broadcast {
				base,
				body,
				..
			} => {
				walk(base, w)?;
				walk(body, w)
			}
			LogicalExpr::Lookup {
				..
			} => {
				let path = output_field_path(&expr.output_path());
				w.parts.extend(path.0);
				w.ends_on_every = false;
				Some(())
			}
			_ => None,
		}
	}

	let mut w = Walk {
		parts: Vec::new(),
		through: Vec::new(),
		ends_on_every: false,
		nowhere: false,
	};
	walk(expr, &mut w)?;
	if w.nowhere {
		return Some(SplitTarget::Nowhere);
	}
	let Walk {
		parts,
		mut through,
		ends_on_every,
		..
	} = w;
	// The innermost collection is the last `[*]`: it is either the whole
	// target (`a[*]`, every position of it) or locates the elements that
	// the trailing path is relative to.
	let mut target = match through.pop() {
		None => SplitTarget::At(FieldPath(parts)),
		Some(collection) if ends_on_every => SplitTarget::Every(collection),
		Some(collection) => {
			SplitTarget::Through(collection, Box::new(SplitTarget::At(FieldPath(parts))))
		}
	};
	while let Some(collection) = through.pop() {
		target = SplitTarget::Through(collection, Box::new(target));
	}
	Some(target)
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
			self.access_mode().combine(self.input.access_mode()),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let targets = self.targets.clone();

		let split_stream = input_stream.then(move |batch_result| {
			let targets = targets.clone();
			async move {
				let batch = batch_result?;
				let mut expanded_values = Vec::with_capacity(batch.len() * targets.len());

				for value in batch.into_values() {
					split_row(value, &targets, &mut expanded_values);
				}

				Ok(ValueBatch::new(expanded_values))
			}
		});

		Ok(monitor_stream(Box::pin(split_stream), "Split", &self.metrics))
	}
}

/// Split `value` on each of `targets` in turn, appending the rows it becomes
/// to `output`: the cartesian product of the collections the targets read,
/// in the order of the targets and of each collection's elements. A target
/// that reads no array or set leaves the rows it is applied to as they are.
pub(crate) fn split_row(value: Value, targets: &[SplitTarget], output: &mut Vec<Value>) {
	let mut current = vec![value];
	for target in targets {
		let mut next = Vec::with_capacity(current.len());
		for value in current {
			let on = target.read(&value);
			split_value_on_path(value, on, target, &mut next);
		}
		current = next;
	}
	output.extend(current);
}

/// Split a single value on the collection `on` found at `path`.
///
/// If `on` is an array or set, produces one copy of `value` per element with
/// the field at `path` replaced by that element. Otherwise, returns the value
/// unchanged.
fn split_value_on_path(value: Value, on: Value, target: &SplitTarget, output: &mut Vec<Value>) {
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
						target.write(&mut val, element);
						output.push(val);
						return;
					}
					let mut cloned = value.clone();
					target.write(&mut cloned, element);
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
						target.write(&mut val, element);
						output.push(val);
						return;
					}
					let mut cloned = value.clone();
					target.write(&mut cloned, element);
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
		let target = SplitTarget::At(FieldPath::field(name));
		let on = target.read(&value);
		split_value_on_path(value, on, &target, output);
	}

	/// The same, for `name[*]`: every position of the collection takes the
	/// element the row was split on.
	fn split_on_every(value: Value, name: &str, output: &mut Vec<Value>) {
		let target = SplitTarget::Every(FieldPath::field(name));
		let on = target.read(&value);
		split_value_on_path(value, on, &target, output);
	}

	/// The logical form of an idiom, as the planner's `SPLIT` targets reach
	/// this module.
	fn target_of(sql: &str) -> Option<SplitTarget> {
		let expr = crate::expr::logical::lower_expr(crate::sql::Expr::Idiom(
			crate::syn::idiom(sql).expect("idiom"),
		))
		.expect("lowers");
		split_target(&expr)
	}

	/// Every written-down position a `SPLIT` can name has a write path,
	/// nesting included: the walk to a position does not stop at the first
	/// field.
	#[test]
	fn every_named_position_has_a_write_path() {
		for sql in [
			"a",
			"a[0]",
			"a[1]",
			"a[$]",
			"o.b",
			"o.b[0]",
			"o.b.c[2]",
			"a[*]",
			"o.b[*]",
			"a[*].b",
			"a.*.b",
			"a[*].b.c",
			"a[*].b[*].c",
			"a[*].b[*]",
			"a[*][0]",
			"a[1.5]",
		] {
			let expr = crate::expr::logical::lower_expr(crate::sql::Expr::Idiom(
				crate::syn::idiom(sql).expect("idiom"),
			))
			.expect("lowers");
			assert!(target_of(sql).is_some(), "`{sql}` names a position: {expr:?}");
		}
	}

	/// The split of `sql` over `row`, through the target the planner builds
	/// for it.
	fn split_by(sql: &str, row: Value) -> Vec<String> {
		let target = target_of(sql).expect("a position");
		let mut out = Vec::new();
		split_row(row, &[target], &mut out);
		out.iter().map(|row| row.to_sql()).collect()
	}

	fn row_of(sql: &str) -> Value {
		crate::val::convert_public_value_to_internal(crate::syn::value(sql).expect("value"))
	}

	/// `SPLIT a[*].b` reads the `b` of every element and writes the value the
	/// row was split on back at the `b` of each one.
	#[test]
	fn split_through_every_element_writes_each_elements_position() {
		assert_eq!(
			split_by("a[*].b", row_of("{ a: [{ b: 1 }, { b: 2 }], z: 9 }")),
			["{ a: [{ b: 1 }, { b: 1 }], z: 9 }", "{ a: [{ b: 2 }, { b: 2 }], z: 9 }"]
		);
		// Chained: each element of `a` contributes the values at its own
		// inner positions, and the row is split on those collections.
		assert_eq!(
			split_by(
				"a[*].b[*].c",
				row_of("{ a: [{ b: [{ c: 1 }, { c: 2 }] }, { b: [{ c: 3 }] }] }")
			),
			[
				"{ a: [{ b: [{ c: [1, 2] }, { c: [1, 2] }] }, { b: [{ c: [1, 2] }] }] }",
				"{ a: [{ b: [{ c: [3] }, { c: [3] }] }, { b: [{ c: [3] }] }] }",
			]
		);
		// An empty collection, and a value that is not one, do not split.
		assert_eq!(split_by("a[*].b", row_of("{ a: [] }")), ["{ a: [] }"]);
		assert_eq!(split_by("a[*].b", row_of("{ a: 5 }")), ["{ a: 5 }"]);
	}

	/// The values of an object are its elements.
	#[test]
	fn split_through_an_objects_values() {
		assert_eq!(
			split_by("a[*].b", row_of("{ a: { x: { b: 1 }, y: { b: 2 } } }")),
			["{ a: { x: { b: 1 }, y: { b: 1 } } }", "{ a: { x: { b: 2 }, y: { b: 2 } } }"]
		);
	}

	/// An index no array has names no position: the row is not split.
	#[test]
	fn a_fractional_index_splits_nothing() {
		assert_eq!(split_by("a[1.5]", row_of("{ a: [1, 2] }")), ["{ a: [1, 2] }"]);
	}

	/// `SPLIT a` replaces the collection with one element; `SPLIT a[*]`
	/// replaces every position with it, keeping the collection's shape.
	#[test]
	fn split_on_every_position_fills_the_collection() {
		let row = || {
			Value::Object(Object::from_iter([(
				"a".to_string(),
				Value::Array(Array::from(vec![
					Value::Array(Array::from(vec![1, 2])),
					Value::Array(Array::from(vec![3, 4])),
				])),
			)]))
		};

		let mut at = Vec::new();
		split_on_field(row(), "a", &mut at);
		assert_eq!(at.len(), 2, "one row per element");
		assert_eq!(at[0].to_sql(), "{ a: [1, 2] }");
		assert_eq!(at[1].to_sql(), "{ a: [3, 4] }");

		let mut every = Vec::new();
		split_on_every(row(), "a", &mut every);
		assert_eq!(every.len(), 2, "the read is the same, so the row count is");
		assert_eq!(every[0].to_sql(), "{ a: [[1, 2], [1, 2]] }");
		assert_eq!(every[1].to_sql(), "{ a: [[3, 4], [3, 4]] }");
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
