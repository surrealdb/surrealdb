use std::fmt;
use std::ops::Bound;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use surrealdb_types::{File, Table};
use uuid::Uuid;

use crate::{
	ArrayAccess, Error, Kind, ObjectAccess, RecordIdKeyAccess, RecordIdKeyKind, SetAccess,
	ValueAccess, ValueKind,
};

fn unexpected_error<T: SqonValueVisitor>(this: &T, found: ValueKind) -> Error {
	Error::UnexpectedType {
		expected: std::fmt::from_fn(|fmt| this.expected(fmt)).to_string(),
		found: Kind::Value(found),
	}
}

fn unexpected_key_error<T: SqonKeyValueVisitor>(this: &T, found: RecordIdKeyKind) -> Error {
	Error::UnexpectedType {
		expected: std::fmt::from_fn(|fmt| this.expected(fmt)).to_string(),
		found: Kind::Key(found),
	}
}

pub trait SqonVisitor: Sized {
	type Visitor: SqonValueVisitor;

	type Value;

	fn visitor(&mut self) -> Self::Visitor;

	fn finish(self, v: <Self::Visitor as SqonValueVisitor>::Value) -> Result<Self::Value, Error>;

	fn finish_range(
		mut self,
		start: Bound<<Self::Visitor as SqonValueVisitor>::Value>,
		bound: Bound<ValueAccess<'_, '_>>,
	) -> Result<Self::Value, Error> {
		let _ = start;
		let _ = bound;

		let visitor = self.visitor();
		let expected = fmt::from_fn(|f| visitor.expected(f)).to_string();
		Err(Error::UnexpectedType {
			found: Kind::Value(ValueKind::Range),
			expected,
		})
	}
}

pub trait SqonValueVisitor: Sized {
	type Value;

	fn expected(&self, fmt: &mut fmt::Formatter) -> fmt::Result;

	fn visit_none(self) -> Result<Self::Value, Error> {
		Err(unexpected_error(&self, ValueKind::None))
	}

	fn visit_null(self) -> Result<Self::Value, Error> {
		Err(unexpected_error(&self, ValueKind::Null))
	}

	fn visit_bool(self, v: bool) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::Bool))
	}

	fn visit_f64(self, v: f64) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::F64))
	}

	fn visit_i64(self, v: i64) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::I64))
	}

	fn visit_decimal(self, v: Decimal) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::Decimal))
	}

	fn visit_string(self, v: String) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::String))
	}

	fn visit_bytes(self, v: Vec<u8>) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::Bytes))
	}

	fn visit_duration(self, v: Duration) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::Duration))
	}

	fn visit_datetime(self, v: DateTime<Utc>) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::Datetime))
	}

	fn visit_uuid(self, v: Uuid) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::Uuid))
	}

	fn visit_file(self, v: File) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::File))
	}

	fn visit_array(self, v: ArrayAccess<'_, '_>) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::Array))
	}

	fn visit_object(self, v: ObjectAccess<'_, '_>) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::Object))
	}

	fn visit_set(self, v: SetAccess<'_, '_>) -> Result<Self::Value, Error> {
		let _ = v;
		Err(unexpected_error(&self, ValueKind::Set))
	}

	fn visit_record_id(
		self,
		table: Table,
		v: RecordIdKeyAccess<'_, '_>,
	) -> Result<Self::Value, Error> {
		let _ = (table, v);
		Err(unexpected_error(&self, ValueKind::RecordId))
	}
}

pub trait SqonKeyVisitor: Sized {
	type Visitor: SqonKeyValueVisitor;

	type Value;

	fn visitor(&mut self) -> Self::Visitor;

	fn finish(self, v: <Self::Visitor as SqonKeyValueVisitor>::Value)
	-> Result<Self::Value, Error>;

	fn finish_range(
		mut self,
		start: Bound<<Self::Visitor as SqonKeyValueVisitor>::Value>,
		bound: Bound<RecordIdKeyAccess<'_, '_>>,
	) -> Result<Self::Value, Error> {
		let _ = start;
		let _ = bound;

		let visitor = self.visitor();
		let expected = fmt::from_fn(|f| visitor.expected(f)).to_string();
		Err(Error::UnexpectedType {
			found: Kind::Key(RecordIdKeyKind::Range),
			expected,
		})
	}
}

pub trait SqonKeyValueVisitor: Sized {
	type Value;

	fn expected(&self, fmt: &mut fmt::Formatter) -> fmt::Result;

	fn visit_string(self, i: String) -> Result<Self::Value, Error> {
		let _ = i;
		Err(unexpected_key_error(&self, RecordIdKeyKind::String))
	}

	fn visit_number(self, i: i64) -> Result<Self::Value, Error> {
		let _ = i;
		Err(unexpected_key_error(&self, RecordIdKeyKind::Number))
	}

	fn visit_uuid(self, u: Uuid) -> Result<Self::Value, Error> {
		let _ = u;
		Err(unexpected_key_error(&self, RecordIdKeyKind::Uuid))
	}

	fn visit_array(self, p: ArrayAccess<'_, '_>) -> Result<Self::Value, Error> {
		let _ = p;
		Err(unexpected_key_error(&self, RecordIdKeyKind::Array))
	}

	fn visit_object(self, o: ObjectAccess<'_, '_>) -> Result<Self::Value, Error> {
		let _ = o;
		Err(unexpected_key_error(&self, RecordIdKeyKind::Object))
	}
}
