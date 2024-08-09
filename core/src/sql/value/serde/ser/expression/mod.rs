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

#[cfg(test)]
mod tests {
	use super::*;
	use serde::ser::Impossible;
	use serde::Serialize;

	pub(super) struct Serializer;

	impl ser::Serializer for Serializer {
		type Ok = Expression;
		type Error = Error;

		type SerializeSeq = Impossible<Expression, Error>;
		type SerializeTuple = Impossible<Expression, Error>;
		type SerializeTupleStruct = Impossible<Expression, Error>;
		type SerializeTupleVariant = Impossible<Expression, Error>;
		type SerializeMap = Impossible<Expression, Error>;
		type SerializeStruct = Impossible<Expression, Error>;
		type SerializeStructVariant = SerializeExpression;

		const EXPECTED: &'static str = "an enum `Expression`";

		#[inline]
		fn serialize_struct_variant(
			self,
			name: &'static str,
			_variant_index: u32,
			variant: &'static str,
			_len: usize,
		) -> Result<Self::SerializeStructVariant, Self::Error> {
			debug_assert_eq!(name, crate::sql::expression::TOKEN);
			match variant {
				"Unary" => Ok(SerializeExpression::Unary(Default::default())),
				"Binary" => Ok(SerializeExpression::Binary(Default::default())),
				_ => Err(Error::custom(format!("unexpected `Expression::{name}`"))),
			}
		}
	}

	#[test]
	fn default() {
		let expression = Expression::default();
		let serialized = expression.serialize(Serializer.wrap()).unwrap();
		assert_eq!(expression, serialized);
	}

	#[test]
	fn unary() {
		let expression = Expression::Unary {
			o: Operator::Not,
			v: "Bar".into(),
		};
		let serialized = expression.serialize(Serializer.wrap()).unwrap();
		assert_eq!(expression, serialized);
	}

	#[test]
	fn foo_equals_bar() {
		let expression = Expression::Binary {
			l: "foo".into(),
			o: Operator::Equal,
			r: "Bar".into(),
		};
		let serialized = expression.serialize(Serializer.wrap()).unwrap();
		assert_eq!(expression, serialized);
	}
}
