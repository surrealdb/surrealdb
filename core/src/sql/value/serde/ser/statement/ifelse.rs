use crate::err::Error;
use crate::sql::statements::IfelseStatement;
use crate::sql::value::serde::ser;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = IfelseStatement;
	type Error = Error;

	type SerializeSeq = Impossible<IfelseStatement, Error>;
	type SerializeTuple = Impossible<IfelseStatement, Error>;
	type SerializeTupleStruct = Impossible<IfelseStatement, Error>;
	type SerializeTupleVariant = Impossible<IfelseStatement, Error>;
	type SerializeMap = Impossible<IfelseStatement, Error>;
	type SerializeStruct = SerializeIfelseStatement;
	type SerializeStructVariant = Impossible<IfelseStatement, Error>;

	const EXPECTED: &'static str = "a struct `IfelseStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeIfelseStatement::default())
	}
}

type ValueValueTuple = (Value, Value);

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeIfelseStatement {
	exprs: Vec<ValueValueTuple>,
	close: Option<Value>,
}

impl serde::ser::SerializeStruct for SerializeIfelseStatement {
	type Ok = IfelseStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"exprs" => {
				self.exprs = value.serialize(ValueValueVecSerializer.wrap())?;
			}
			"close" => {
				self.close = value.serialize(ser::value::opt::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `IfelseStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(IfelseStatement {
			exprs: self.exprs,
			close: self.close,
		})
	}
}

#[non_exhaustive]
pub struct ValueValueVecSerializer;

impl ser::Serializer for ValueValueVecSerializer {
	type Ok = Vec<ValueValueTuple>;
	type Error = Error;

	type SerializeSeq = SerializeValueValueVec;
	type SerializeTuple = Impossible<Vec<ValueValueTuple>, Error>;
	type SerializeTupleStruct = Impossible<Vec<ValueValueTuple>, Error>;
	type SerializeTupleVariant = Impossible<Vec<ValueValueTuple>, Error>;
	type SerializeMap = Impossible<Vec<ValueValueTuple>, Error>;
	type SerializeStruct = Impossible<Vec<ValueValueTuple>, Error>;
	type SerializeStructVariant = Impossible<Vec<ValueValueTuple>, Error>;

	const EXPECTED: &'static str = "a `Vec<(Value, Value)>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeValueValueVec(Vec::with_capacity(len.unwrap_or_default())))
	}
}

#[non_exhaustive]
pub struct SerializeValueValueVec(Vec<ValueValueTuple>);

impl serde::ser::SerializeSeq for SerializeValueValueVec {
	type Ok = Vec<ValueValueTuple>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(ValueValueTupleSerializer.wrap())?);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.0)
	}
}

struct ValueValueTupleSerializer;

impl ser::Serializer for ValueValueTupleSerializer {
	type Ok = ValueValueTuple;
	type Error = Error;

	type SerializeSeq = Impossible<ValueValueTuple, Error>;
	type SerializeTuple = SerializeValueValueTuple;
	type SerializeTupleStruct = Impossible<ValueValueTuple, Error>;
	type SerializeTupleVariant = Impossible<ValueValueTuple, Error>;
	type SerializeMap = Impossible<ValueValueTuple, Error>;
	type SerializeStruct = Impossible<ValueValueTuple, Error>;
	type SerializeStructVariant = Impossible<ValueValueTuple, Error>;

	const EXPECTED: &'static str = "a `(Value, Value)`";

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		Ok(SerializeValueValueTuple::default())
	}
}

#[derive(Default)]
struct SerializeValueValueTuple {
	index: usize,
	zero: Option<Value>,
	one: Option<Value>,
}

impl serde::ser::SerializeTuple for SerializeValueValueTuple {
	type Ok = ValueValueTuple;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.index {
			0 => {
				self.zero = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			1 => {
				self.one = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			index => {
				return Err(Error::custom(format!(
					"unexpected tuple index `{index}` for `(Value, Value)`"
				)));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match (self.zero, self.one) {
			(Some(zero), Some(one)) => Ok((zero, one)),
			_ => Err(Error::custom("`(Value, Value)` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = IfelseStatement::default();
		let value: IfelseStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_exprs() {
		let stmt = IfelseStatement {
			exprs: vec![(Default::default(), Default::default())],
			..Default::default()
		};
		let value: IfelseStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_close() {
		let stmt = IfelseStatement {
			close: Some(Default::default()),
			..Default::default()
		};
		let value: IfelseStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
