use crate::err::Error;
use crate::sql::unary::Unary;
use crate::sql::value::serde::ser;
use crate::sql::Operator;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Unary;
	type Error = Error;

	type SerializeSeq = Impossible<Unary, Error>;
	type SerializeTuple = Impossible<Unary, Error>;
	type SerializeTupleStruct = SerializeUnary;
	type SerializeTupleVariant = Impossible<Unary, Error>;
	type SerializeMap = Impossible<Unary, Error>;
	type SerializeStruct = Impossible<Unary, Error>;
	type SerializeStructVariant = Impossible<Unary, Error>;

	const EXPECTED: &'static str = "a struct `Unary`";

	#[inline]
	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleStruct, Error> {
		Ok(SerializeUnary::default())
	}
}

#[derive(Default)]
pub(super) struct SerializeUnary {
	o: Option<Operator>,
	v: Option<Value>,
}

impl serde::ser::SerializeTupleStruct for SerializeUnary {
	type Ok = Unary;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		if self.o.is_none() {
			self.o = Some(value.serialize(ser::operator::Serializer.wrap())?);
		} else if self.v.is_none() {
			self.v = Some(value.serialize(ser::value::Serializer.wrap())?);
		} else {
			return Err(Error::custom(format!("unexpected 3rd `Unary` field")));
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.o, self.v) {
			(Some(o), Some(v)) => Ok(Unary(o, v)),
			_ => Err(Error::custom("`Unary` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde::Serialize;

	#[test]
	fn not_true() {
		let expression = Unary(Operator::Not, Value::Bool(true));
		let serialized = expression.serialize(Serializer.wrap()).unwrap();
		assert_eq!(expression, serialized);
	}
}
