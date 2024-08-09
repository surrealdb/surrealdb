use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Closure;
use crate::sql::Ident;
use crate::sql::Kind;
use crate::sql::Value;
use ser::statement::define::function::IdentKindVecSerializer;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Serialize;

#[derive(Default)]
pub(super) struct SerializeClosure {
	args: Option<Vec<(Ident, Kind)>>,
	returns: Option<Option<Kind>>,
	body: Option<Value>,
}

impl serde::ser::SerializeStruct for SerializeClosure {
	type Ok = Closure;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"args" => {
				self.args = Some(value.serialize(IdentKindVecSerializer.wrap())?);
			}
			"returns" => {
				self.returns = Some(value.serialize(ser::kind::opt::Serializer.wrap())?);
			}
			"body" => {
				self.body = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Closure::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.args, self.returns, self.body) {
			(Some(args), Some(returns), Some(body)) => Ok(Closure {
				args,
				returns,
				body,
			}),
			_ => Err(Error::custom("`Closure` missing required field(s)")),
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
		type Ok = Closure;
		type Error = Error;

		type SerializeSeq = Impossible<Closure, Error>;
		type SerializeTuple = Impossible<Closure, Error>;
		type SerializeTupleStruct = Impossible<Closure, Error>;
		type SerializeTupleVariant = Impossible<Closure, Error>;
		type SerializeMap = Impossible<Closure, Error>;
		type SerializeStruct = SerializeClosure;
		type SerializeStructVariant = Impossible<Closure, Error>;

		const EXPECTED: &'static str = "a struct `Closure`";

		#[inline]
		fn serialize_struct(
			self,
			_name: &'static str,
			_len: usize,
		) -> Result<Self::SerializeStruct, Error> {
			Ok(SerializeClosure::default())
		}
	}

	#[test]
	fn closure() {
		let closure = Closure {
			args: Vec::new(),
			returns: None,
			body: Value::default(),
		};
		let serialized = closure.serialize(Serializer.wrap()).unwrap();
		assert_eq!(closure, serialized);
	}
}
