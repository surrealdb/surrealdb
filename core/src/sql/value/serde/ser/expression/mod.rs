use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Expression;
use crate::sql::Operator;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Serialize;

pub(super) enum SerializeExpression {
	Unary(SerializeUnary),
	Binary(SerializeBinary),
}

impl serde::ser::SerializeStructVariant for SerializeExpression {
	type Ok = Expression;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match self {
			Self::Unary(unary) => unary.serialize_field(key, value),
			Self::Binary(binary) => binary.serialize_field(key, value),
		}
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match self {
			Self::Unary(unary) => unary.end(),
			Self::Binary(binary) => binary.end(),
		}
	}
}

#[derive(Default)]
pub(super) struct SerializeUnary {
	o: Option<Operator>,
	v: Option<Value>,
}

impl serde::ser::SerializeStructVariant for SerializeUnary {
	type Ok = Expression;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"o" => {
				self.o = Some(value.serialize(ser::operator::Serializer.wrap())?);
			}
			"v" => {
				self.v = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `Expression::Unary{{{key}}}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.o, self.v) {
			(Some(o), Some(v)) => Ok(Expression::Unary {
				o,
				v,
			}),
			_ => Err(Error::custom("`Expression::Unary` missing required field(s)")),
		}
	}
}

#[derive(Default)]
pub(super) struct SerializeBinary {
	l: Option<Value>,
	o: Option<Operator>,
	r: Option<Value>,
}

impl serde::ser::SerializeStructVariant for SerializeBinary {
	type Ok = Expression;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"l" => {
				self.l = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			"o" => {
				self.o = Some(value.serialize(ser::operator::Serializer.wrap())?);
			}
			"r" => {
				self.r = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `Expression::Binary{{{key}}}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.l, self.o, self.r) {
			(Some(l), Some(o), Some(r)) => Ok(Expression::Binary {
				l,
				o,
				r,
			}),
			_ => Err(Error::custom("`Expression::Binary` missing required field(s)")),
		}
	}
}
