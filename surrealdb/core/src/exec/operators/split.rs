use crate::expr::logical::{LogicalExpr, LogicalLiteral, OutputPath, OutputPathPart};
use crate::val::field_path::{FieldPath, FieldPathPart};
use crate::val::{Number, Value};

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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
			SplitTarget::Through(path, inner) => {
				let extracted = path.extract(value);
				if let Value::Array(arr) = extracted.as_ref() {
					Value::Array(
						arr.iter().map(|element| inner.read(element)).collect::<Vec<_>>().into(),
					)
				} else if let Value::Set(set) = extracted.as_ref() {
					Value::Array(
						set.iter().map(|element| inner.read(element)).collect::<Vec<_>>().into(),
					)
				} else if let Value::Object(obj) = extracted.as_ref() {
					Value::Array(
						obj.values().map(|element| inner.read(element)).collect::<Vec<_>>().into(),
					)
				} else {
					Value::None
				}
			}
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
				let extracted = path.extract(value);
				let filled = if let Value::Array(arr) = extracted.as_ref() {
					Value::Array(vec![element; arr.len()].into())
				} else if let Value::Set(set) = extracted.as_ref() {
					let mut out = crate::val::Set::new();
					if !set.is_empty() {
						out.insert(element);
					}
					Value::Set(out)
				} else if let Value::Object(obj) = extracted.as_ref() {
					let mut out = crate::val::Object::default();
					for key in obj.keys().cloned().collect::<Vec<_>>() {
						out.insert(key, element.clone());
					}
					Value::Object(out)
				} else {
					// Not a collection: there are no positions to fill, and
					// the read yielded nothing to split on either.
					return;
				};
				value.set_at_field_path_owned(path, filled);
			}
			SplitTarget::Through(path, inner) => {
				let mut collection = path.extract(value).into_owned();
				if let Value::Array(arr) = &mut collection {
					for slot in arr.iter_mut() {
						inner.write(slot, element.clone());
					}
				} else if let Value::Set(set) = &collection {
					let mut out = crate::val::Set::new();
					for item in set.iter() {
						let mut slot = item.clone();
						inner.write(&mut slot, element.clone());
						out.insert(slot);
					}
					collection = Value::Set(out);
				} else if let Value::Object(obj) = &mut collection {
					for slot in obj.values_mut() {
						inner.write(slot, element.clone());
					}
				} else {
					// Not a collection: no element holds a position to write.
					return;
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
			// The current value. The walk already points at it, so nothing is added.
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
				let number = if let LogicalExpr::Literal(LogicalLiteral::Integer(i)) =
					index.as_ref()
				{
					Number::Int(*i)
				} else if let LogicalExpr::Literal(LogicalLiteral::Float(f)) = index.as_ref() {
					Number::Float(*f)
				} else if let LogicalExpr::Literal(LogicalLiteral::Decimal(d)) = index.as_ref() {
					Number::Decimal(*d)
				} else {
					return None;
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
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Doc
			| LogicalExpr::Flatten {
				..
			}
			| LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Optional {
				..
			}
			| LogicalExpr::Recurse(_)
			| LogicalExpr::RepeatRecurse(_)
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Prefix {
				..
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Invoke {
				..
			}
			| LogicalExpr::Binary {
				..
			}
			| LogicalExpr::Call(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => None,
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

/// Split a single value on the collection `on` read at `target`.
///
/// If `on` is an array or set, produces one copy of `value` per element, with
/// the position `target` names set to that element. An empty array or set,
/// and any other value, leaves `value` as one row, unchanged.
fn split_value_on_path(value: Value, on: Value, target: &SplitTarget, output: &mut Vec<Value>) {
	if let Value::Array(arr) = on {
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
	} else if let Value::Set(set) = on {
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
	} else {
		// Non-array/set values pass through unchanged
		output.push(value);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::{Array, Object, Set};

	/// Split `value` on the field `name`, reading the collection from that field.
	fn split_on_field(value: Value, name: &str, output: &mut Vec<Value>) {
		let target = SplitTarget::At(FieldPath::field(name));
		let on = target.read(&value);
		split_value_on_path(value, on, &target, output);
	}

	/// Parses `sql` as an idiom, lowers it, and returns its `SPLIT` target, or
	/// `None` if the target names no position. Panics if `sql` does not parse or lower.
	fn target_of(sql: &str) -> Option<SplitTarget> {
		let expr = crate::expr::logical::lower_expr(crate::expr::Expr::Idiom(
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
			let expr = crate::expr::logical::lower_expr(crate::expr::Expr::Idiom(
				crate::syn::idiom(sql).expect("idiom"),
			))
			.expect("lowers");
			assert!(target_of(sql).is_some(), "`{sql}` names a position: {expr:?}");
		}
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
