//! Conversions between schema values and the values the VM's programs build.
//!
//! A struct is an object keyed by its field names, an enum is `{Variant: body}`, an `Option` is its
//! value or `NONE`, a tuple is an array, a map is an array of `[key, value]` pairs, and an unsigned
//! integer is an `Int`, or a `Decimal` above `i64::MAX`. These are the shapes the VM's catalog
//! codec presents to a program: a program written against them sees the same shapes whichever codec
//! produced the bytes.

use std::collections::BTreeMap;

use rust_decimal::Decimal;

use super::{SchemaError, StoredField};
use crate::val::{Number, Object, Strand, Value};

/// An integer as a VM value: `Int`, or an integral `Decimal` when it does not fit.
pub fn int(v: i128) -> Result<Value, SchemaError> {
	Ok(Value::Number(match i64::try_from(v) {
		Ok(v) => Number::Int(v),
		Err(_) => Number::Decimal(
			Decimal::try_from_i128_with_scale(v, 0)
				.map_err(|_| SchemaError::vm("int", "an integer past a decimal"))?,
		),
	}))
}

/// The integer a VM value holds: an `Int`, or an integral `Decimal`.
pub fn integer(value: &Value) -> Result<i128, SchemaError> {
	if let Value::Number(n) = value {
		match n {
			Number::Int(v) => Ok(i128::from(*v)),
			Number::I8(v) => Ok(i128::from(*v)),
			Number::U8(v) => Ok(i128::from(*v)),
			Number::I16(v) => Ok(i128::from(*v)),
			Number::U16(v) => Ok(i128::from(*v)),
			Number::I32(v) => Ok(i128::from(*v)),
			Number::U32(v) => Ok(i128::from(*v)),
			Number::U64(v) => Ok(i128::from(*v)),
			Number::BigDecimal(b) => {
				b.to_i64().map(i128::from).ok_or(SchemaError::vm("int", "an integer bigdecimal"))
			}
			Number::Decimal(d) if d.fract().is_zero() => d
				.mantissa()
				.checked_div(10i128.pow(d.scale()))
				.ok_or(SchemaError::vm("int", "an integer decimal")),
			Number::Decimal(_) | Number::Float(_) | Number::F16(_) | Number::F32(_) => {
				Err(SchemaError::vm("int", "expected an integer"))
			}
		}
	} else {
		Err(SchemaError::vm("int", "expected an integer"))
	}
}

/// The float a VM value holds.
pub fn float(value: &Value) -> Result<f64, SchemaError> {
	if let Value::Number(n) = value {
		match n {
			Number::Float(v) => Ok(*v),
			Number::F16(v) => Ok(v.to_f64()),
			Number::F32(v) => Ok(*v as f64),
			Number::Int(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_) => Err(SchemaError::vm("float", "expected a float")),
		}
	} else {
		Err(SchemaError::vm("float", "expected a float"))
	}
}

/// A string-like VM value: a string, or a table name.
pub fn string_of(value: Value, ty: &'static str) -> Result<Strand, SchemaError> {
	if let Value::String(s) = value {
		Ok(s)
	} else if let Value::Table(t) = value {
		Ok(Strand::from(t))
	} else {
		Err(SchemaError::vm(ty, "expected a string"))
	}
}

/// An object of the fields.
pub fn object<const N: usize>(entries: [(&'static str, Value); N]) -> Value {
	Value::Object(entries.into_iter().collect::<Object>())
}

/// Builds the object of a struct's fields, one field at a time, in the order they are pushed.
///
/// [`finish`](ObjectBuilder::finish) gives what [`object`] gives for the same entries. The methods
/// are out of line so that every generated conversion shares one copy of them.
pub struct ObjectBuilder(Vec<(&'static str, Value)>);

impl ObjectBuilder {
	/// A builder for an object of `capacity` fields.
	#[inline(never)]
	pub fn new(capacity: usize) -> Self {
		Self(Vec::with_capacity(capacity))
	}

	/// Adds field `name`.
	#[inline(never)]
	pub fn push(&mut self, name: &'static str, value: Value) {
		self.0.push((name, value));
	}

	/// The object of the fields pushed.
	#[inline(never)]
	pub fn finish(self) -> Value {
		Value::Object(self.0.into_iter().collect::<Object>())
	}
}

/// An enum variant: `{name: body}`.
pub fn variant(name: &'static str, body: Value) -> Value {
	object([(name, body)])
}

/// A tuple: an array of its elements.
pub fn tuple<const N: usize>(items: [Value; N]) -> Value {
	Value::Array(Vec::from(items).into())
}

/// The object a struct's VM value must be.
pub fn object_of(value: Value, ty: &'static str) -> Result<Object, SchemaError> {
	if let Value::Object(object) = value {
		Ok(object)
	} else {
		Err(SchemaError::vm(ty, "expected an object"))
	}
}

/// Takes field `name` out of `object`. A field the object lacks reads as `NONE`.
pub fn field<T: StoredField>(object: &mut Object, name: &str) -> Result<T, SchemaError> {
	T::from_vm_value(object.remove(name).unwrap_or(Value::None))
}

/// Takes field `name` out of `object`; a field the object lacks is `default()`.
pub fn field_or<T: StoredField>(
	object: &mut Object,
	name: &str,
	default: fn() -> T,
) -> Result<T, SchemaError> {
	match object.remove(name) {
		Some(value) => T::from_vm_value(value),
		None => Ok(default()),
	}
}

/// The variant name and body of `{Variant: body}`.
pub fn split_variant(value: Value, ty: &'static str) -> Result<(String, Value), SchemaError> {
	let Value::Object(object) = value else {
		return Err(SchemaError::vm(ty, "a variant is an object with one key"));
	};
	if object.len() != 1 {
		return Err(SchemaError::vm(ty, "a variant is an object with one key"));
	}
	match object.0.into_iter().next() {
		Some((name, body)) => Ok((name.into_string(), body)),
		None => Err(SchemaError::vm(ty, "a variant is an object with one key")),
	}
}

/// The `n` elements of a tuple's VM array.
pub fn tuple_of(
	value: Value,
	n: usize,
	ty: &'static str,
) -> Result<std::vec::IntoIter<Value>, SchemaError> {
	if let Value::Array(array) = value
		&& array.0.len() == n
	{
		Ok(array.0.into_iter())
	} else {
		Err(SchemaError::vm(ty, "a tuple has the wrong number of elements"))
	}
}

/// The next element of a tuple.
pub fn next(items: &mut std::vec::IntoIter<Value>, ty: &'static str) -> Result<Value, SchemaError> {
	items.next().ok_or(SchemaError::vm(ty, "a tuple has too few elements"))
}

/// An `Option` as `NONE` or its value, wrapped as `[x]` when the type can itself be `NONE`.
pub fn option_into<T: StoredField>(value: Option<T>) -> Result<Value, SchemaError> {
	match value {
		None => Ok(Value::None),
		Some(inner) => {
			let value = inner.into_vm_value()?;
			Ok(if T::NULLABLE {
				Value::Array(vec![value].into())
			} else {
				value
			})
		}
	}
}

/// The inverse of [`option_into`].
pub fn option_from<T: StoredField>(value: Value) -> Result<Option<T>, SchemaError> {
	if value.is_none() {
		Ok(None)
	} else if T::NULLABLE {
		if let Value::Array(array) = value
			&& array.0.len() == 1
		{
			let mut items = array.0.into_iter();
			let inner = next(&mut items, "option")?;
			Ok(Some(T::from_vm_value(inner)?))
		} else {
			Err(SchemaError::vm("option", "an option of a value is NONE or a one-element array"))
		}
	} else {
		Ok(Some(T::from_vm_value(value)?))
	}
}

/// A list as an array.
pub fn list_into<T: StoredField>(items: Vec<T>) -> Result<Value, SchemaError> {
	let values = items.into_iter().map(T::into_vm_value).collect::<Result<Vec<_>, _>>()?;
	Ok(Value::Array(values.into()))
}

/// The inverse of [`list_into`].
pub fn list_from<T: StoredField>(value: Value) -> Result<Vec<T>, SchemaError> {
	if let Value::Array(array) = value {
		array.0.into_iter().map(T::from_vm_value).collect()
	} else {
		Err(SchemaError::vm("list", "expected an array"))
	}
}

/// A map as an array of `[key, value]` pairs, in key order.
pub fn pairs_into(map: BTreeMap<String, Value>) -> Value {
	let pairs: Vec<Value> = map
		.into_iter()
		.map(|(k, v)| Value::Array(vec![Value::String(Strand::from(k)), v].into()))
		.collect();
	Value::Array(pairs.into())
}

/// The inverse of [`pairs_into`]. A repeated key is refused.
pub fn pairs_from(value: Value) -> Result<BTreeMap<String, Value>, SchemaError> {
	let Value::Array(array) = value else {
		return Err(SchemaError::vm("map", "expected an array of pairs"));
	};
	let mut out = BTreeMap::new();
	for entry in array.0 {
		let mut pair = tuple_of(entry, 2, "map")?;
		let key = string_of(next(&mut pair, "map")?, "map")?.into_string();
		let value = next(&mut pair, "map")?;
		if out.insert(key, value).is_some() {
			return Err(SchemaError::vm("map", "a map with a repeated key"));
		}
	}
	Ok(out)
}
