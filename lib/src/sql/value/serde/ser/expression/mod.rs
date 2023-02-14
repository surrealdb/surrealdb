use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Expression;
use crate::sql::Operator;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Expression;
	type Error = Error;

	type SerializeSeq = Impossible<Expression, Error>;
	type SerializeTuple = Impossible<Expression, Error>;
	type SerializeTupleStruct = Impossible<Expression, Error>;
	type SerializeTupleVariant = Impossible<Expression, Error>;
	type SerializeMap = Impossible<Expression, Error>;
	type SerializeStruct = SerializeExpression;
	type SerializeStructVariant = Impossible<Expression, Error>;

	const EXPECTED: &'static str = "a struct `Expression`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeExpression::default())
	}
}

#[derive(Default)]
pub(super) struct SerializeExpression {
	l: Option<Value>,
	o: Option<Operator>,
	r: Option<Value>,
}

impl serde::ser::SerializeStruct for SerializeExpression {
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
				return Err(Error::custom(format!("unexpected field `Expression::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.l, self.o, self.r) {
			(Some(l), Some(o), Some(r)) => Ok(Expression {
				l,
				o,
				r,
			}),
			_ => Err(Error::custom("`Expression` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;
	use serde::Serialize;

	#[test]
	fn default() {
		let expression = Expression::default();
		let serialized = serialize_internal(|| expression.serialize(Serializer.wrap())).unwrap();
		assert_eq!(expression, serialized);
	}

	#[test]
	fn foo_equals_bar() {
		let expression = Expression {
			l: "foo".into(),
			o: Operator::Equal,
			r: "Bar".into(),
		};
		let serialized = serialize_internal(|| expression.serialize(Serializer.wrap())).unwrap();
		assert_eq!(expression, serialized);
	}
}
