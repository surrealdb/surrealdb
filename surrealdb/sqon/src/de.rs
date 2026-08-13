use std::marker::PhantomData;
use std::ops::Bound;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use surrealdb_types::{
	Array, Number, Object, Range, RecordId, RecordIdKey, RecordIdKeyRange, Set, Table, Value,
};

use super::{ArrayAccess, ObjectAccess, SetAccess};
use crate::{
	Kind, Parser, RecordIdKeyAccess, SqonDeserialize, SqonKeyValueVisitor, SqonKeyVisitor,
	SqonValueVisitor, SqonVisitor, ValueAccess,
};

pub struct ValueVisitor;

impl SqonDeserialize for Value {
	type Visitor = ValueVisitor;

	fn visitor() -> Self::Visitor {
		ValueVisitor
	}
}

impl SqonVisitor for ValueVisitor {
	type Visitor = ValueValueVisitor;

	type Value = Value;

	fn visitor(&mut self) -> Self::Visitor {
		ValueValueVisitor
	}

	fn finish_range(
		self,
		start: Bound<<Self::Visitor as SqonValueVisitor>::Value>,
		end: Bound<ValueAccess<'_, '_>>,
	) -> Result<Self::Value, super::Error> {
		let end = match end {
			Bound::Unbounded => Bound::Unbounded,
			Bound::Included(x) => Bound::Included(x.parse(ValueVisitor)?),
			Bound::Excluded(x) => Bound::Excluded(x.parse(ValueVisitor)?),
		};

		let range = Range {
			start,
			end,
		};

		Ok(Value::Range(Box::new(range)))
	}

	fn finish(
		self,
		v: <Self::Visitor as SqonValueVisitor>::Value,
	) -> Result<Self::Value, super::Error> {
		Ok(v)
	}
}

pub struct ValueValueVisitor;

impl SqonValueVisitor for ValueValueVisitor {
	type Value = Value;

	fn expected(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
		fmt.write_str("any value")
	}

	fn visit_none(self) -> Result<Self::Value, super::Error> {
		Ok(Value::None)
	}

	fn visit_null(self) -> Result<Self::Value, super::Error> {
		Ok(Value::Null)
	}

	fn visit_bool(self, v: bool) -> Result<Self::Value, super::Error> {
		Ok(Value::Bool(v))
	}

	fn visit_f64(self, v: f64) -> Result<Self::Value, super::Error> {
		Ok(Value::Number(Number::Float(v)))
	}

	fn visit_i64(self, v: i64) -> Result<Self::Value, super::Error> {
		Ok(Value::Number(Number::Int(v)))
	}

	fn visit_decimal(self, v: rust_decimal::Decimal) -> Result<Self::Value, super::Error> {
		Ok(Value::Number(Number::Decimal(v)))
	}

	fn visit_string(self, v: String) -> Result<Self::Value, super::Error> {
		Ok(Value::String(v))
	}

	fn visit_bytes(self, v: Vec<u8>) -> Result<Self::Value, super::Error> {
		Ok(Value::Bytes(v.into()))
	}

	fn visit_duration(self, v: Duration) -> Result<Self::Value, super::Error> {
		Ok(Value::Duration(v.into()))
	}

	fn visit_datetime(self, v: chrono::DateTime<chrono::Utc>) -> Result<Self::Value, super::Error> {
		Ok(Value::Datetime(v.into()))
	}

	fn visit_uuid(self, v: uuid::Uuid) -> Result<Self::Value, super::Error> {
		Ok(Value::Uuid(v.into()))
	}

	fn visit_file(self, v: surrealdb_types::File) -> Result<Self::Value, super::Error> {
		Ok(Value::File(v))
	}

	fn visit_array(self, mut v: ArrayAccess<'_, '_>) -> Result<Self::Value, super::Error> {
		let mut res = Array::new();

		while let Some(x) = v.next_entry::<Value>() {
			let v = x?;
			res.push(v);
		}

		Ok(Value::Array(res))
	}

	fn visit_object(self, mut v: ObjectAccess<'_, '_>) -> Result<Self::Value, super::Error> {
		let mut res = Object::default();

		while let Some(x) = v.next_entry::<Value>() {
			let (k, v) = x?;
			res.insert(k, v);
		}

		Ok(Value::Object(res))
	}

	fn visit_set(self, mut v: SetAccess<'_, '_>) -> Result<Self::Value, super::Error> {
		let mut res = Set::new();

		while let Some(x) = v.next_entry::<Value>() {
			let v = x?;
			res.insert(v);
		}

		Ok(Value::Set(res))
	}

	fn visit_record_id(
		self,
		table: Table,
		v: RecordIdKeyAccess<'_, '_>,
	) -> Result<Self::Value, super::Error> {
		let v = v.parse_key(KeyVisitor)?;

		let record_id = RecordId {
			table,
			key: v,
		};

		Ok(Value::RecordId(record_id))
	}
}

struct KeyVisitor;

impl SqonKeyVisitor for KeyVisitor {
	type Visitor = KeyValueVisitor;

	type Value = RecordIdKey;

	fn visitor(&mut self) -> Self::Visitor {
		KeyValueVisitor
	}

	fn finish(
		self,
		v: <Self::Visitor as crate::SqonKeyValueVisitor>::Value,
	) -> Result<Self::Value, crate::Error> {
		Ok(v)
	}

	fn finish_range(
		self,
		start: Bound<<Self::Visitor as crate::SqonKeyValueVisitor>::Value>,
		end: Bound<RecordIdKeyAccess<'_, '_>>,
	) -> Result<Self::Value, crate::Error> {
		let end = match end {
			Bound::Included(p) => Bound::Included(p.parse_key(KeyVisitor)?),
			Bound::Excluded(p) => Bound::Excluded(p.parse_key(KeyVisitor)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		let range = RecordIdKeyRange {
			start,
			end,
		};

		Ok(RecordIdKey::Range(Box::new(range)))
	}
}

struct KeyValueVisitor;

impl SqonKeyValueVisitor for KeyValueVisitor {
	type Value = RecordIdKey;

	fn expected(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
		fmt.write_str("any record id key")
	}

	fn visit_string(self, i: String) -> Result<Self::Value, crate::Error> {
		Ok(RecordIdKey::String(i))
	}

	fn visit_number(self, i: i64) -> Result<Self::Value, crate::Error> {
		Ok(RecordIdKey::Number(i))
	}

	fn visit_uuid(self, u: uuid::Uuid) -> Result<Self::Value, crate::Error> {
		Ok(RecordIdKey::Uuid(u.into()))
	}

	fn visit_array(self, mut p: ArrayAccess<'_, '_>) -> Result<Self::Value, crate::Error> {
		let mut res = Array::new();

		while let Some(v) = p.next_entry::<Value>() {
			let v = v?;
			res.push(v);
		}

		Ok(RecordIdKey::Array(res))
	}

	fn visit_object(self, mut o: ObjectAccess<'_, '_>) -> Result<Self::Value, crate::Error> {
		let mut res = Object::new();

		while let Some(v) = o.next_entry::<Value>() {
			let (k, v) = v?;
			res.insert(k, v);
		}

		Ok(RecordIdKey::Object(res))
	}
}

macro_rules! simple_value_visitor {
	($name:ident) => {
		impl SqonVisitor for $name {
			type Visitor = $name;

			type Value = <$name as SqonValueVisitor>::Value;

			fn visitor(&mut self) -> Self::Visitor {
				$name
			}

			fn finish(self, v: Self::Value) -> Result<Self::Value, super::Error> {
				Ok(v)
			}
		}
	};
}

macro_rules! direct_visitor {
	($name:ident,$ty:ty,$fn:ident,$expect:literal) => {
		pub struct $name;
		impl SqonValueVisitor for $name {
			type Value = $ty;

			fn expected(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
				fmt.write_str($expect)
			}

			fn $fn(self, v: $ty) -> Result<Self::Value, super::Error> {
				Ok(v)
			}
		}
		simple_value_visitor!($name);

		impl SqonDeserialize for $ty {
			type Visitor = $name;

			fn visitor() -> Self::Visitor {
				$name
			}
		}
	};
}

direct_visitor!(BoolVisitor, bool, visit_bool, "a boolean");
direct_visitor!(F64Visitor, f64, visit_f64, "a float");
direct_visitor!(I64Visitor, i64, visit_i64, "an integer");
direct_visitor!(DecimalVisitor, Decimal, visit_decimal, "a decimal");
direct_visitor!(DurationVisitor, Duration, visit_duration, "a duration");
direct_visitor!(DatetimeVisitor, DateTime<Utc>, visit_datetime, "a datetime");
direct_visitor!(StringVisitor, String, visit_string, "a string");

impl<T: SqonDeserialize> SqonDeserialize for Vec<T> {
	type Visitor = VecVisitor<T>;

	fn visitor() -> Self::Visitor {
		VecVisitor(PhantomData)
	}
}

pub struct VecVisitor<T>(PhantomData<T>);

impl<T: SqonDeserialize> SqonVisitor for VecVisitor<T> {
	type Visitor = Self;

	type Value = Vec<T>;

	fn visitor(&mut self) -> Self::Visitor {
		VecVisitor(PhantomData)
	}

	fn finish(
		self,
		v: <Self::Visitor as SqonValueVisitor>::Value,
	) -> Result<Self::Value, crate::Error> {
		Ok(v)
	}
}

impl<T: SqonDeserialize> SqonValueVisitor for VecVisitor<T> {
	type Value = Vec<T>;

	fn expected(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
		fmt.write_str("an array")
	}

	fn visit_array(self, mut v: ArrayAccess<'_, '_>) -> Result<Self::Value, crate::Error> {
		let mut res = Vec::new();

		while let Some(v) = v.next_entry::<T>() {
			let v = v?;
			res.push(v);
		}

		Ok(res)
	}
}

impl<T: SqonDeserialize> SqonDeserialize for (Bound<T>, Bound<T>) {
	type Visitor = RangeVisitor<T>;

	fn visitor() -> Self::Visitor {
		RangeVisitor(PhantomData)
	}
}

pub struct RangeVisitor<T>(PhantomData<T>);

impl<T: SqonDeserialize> SqonVisitor for RangeVisitor<T> {
	// The start bound is parsed with `T`'s own value visitor, so bound type mismatches are
	// reported where they occur.
	type Visitor = <T::Visitor as SqonVisitor>::Visitor;

	type Value = (Bound<T>, Bound<T>);

	fn visitor(&mut self) -> Self::Visitor {
		T::visitor().visitor()
	}

	fn finish(
		self,
		_: <Self::Visitor as SqonValueVisitor>::Value,
	) -> Result<Self::Value, crate::Error> {
		Err(crate::Error::UnexpectedType {
			found: Kind::Single,
			expected: "a range".to_string(),
		})
	}

	fn finish_range(
		self,
		start: Bound<<Self::Visitor as SqonValueVisitor>::Value>,
		end: Bound<ValueAccess<'_, '_>>,
	) -> Result<Self::Value, crate::Error> {
		// `T::Visitor::finish` converts the value visitor's output into a `T`, the same
		// step the parser runs for a plain value.
		let start = match start {
			Bound::Included(x) => Bound::Included(T::visitor().finish(x)?),
			Bound::Excluded(x) => Bound::Excluded(T::visitor().finish(x)?),
			Bound::Unbounded => Bound::Unbounded,
		};
		let end = match end {
			Bound::Included(p) => Bound::Included(p.parse(T::visitor())?),
			Bound::Excluded(p) => Bound::Excluded(p.parse(T::visitor())?),
			Bound::Unbounded => Bound::Unbounded,
		};

		Ok((start, end))
	}
}
