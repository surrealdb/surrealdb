use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Cast;
use crate::sql::Kind;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Serialize;

#[derive(Default)]
pub(super) struct SerializeCast {
	index: usize,
	kind: Option<Kind>,
	value: Option<Value>,
}

impl serde::ser::SerializeTupleStruct for SerializeCast {
	type Ok = Cast;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.index {
			0 => {
				self.kind = Some(value.serialize(ser::kind::Serializer.wrap())?);
			}
			1 => {
				self.value = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			index => {
				return Err(Error::custom(format!("unexpected `Cast` index `{index}`")));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match (self.kind, self.value) {
			(Some(kind), Some(value)) => Ok(Cast(kind, value)),
			_ => Err(Error::custom("`Cast` missing required value(s)")),
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
		type Ok = Cast;
		type Error = Error;

		type SerializeSeq = Impossible<Cast, Error>;
		type SerializeTuple = Impossible<Cast, Error>;
		type SerializeTupleStruct = SerializeCast;
		type SerializeTupleVariant = Impossible<Cast, Error>;
		type SerializeMap = Impossible<Cast, Error>;
		type SerializeStruct = Impossible<Cast, Error>;
		type SerializeStructVariant = Impossible<Cast, Error>;

		const EXPECTED: &'static str = "an struct `Cast`";

		fn serialize_tuple_struct(
			self,
			_name: &'static str,
			_len: usize,
		) -> Result<Self::SerializeTupleStruct, Error> {
			Ok(SerializeCast::default())
		}
	}

	#[test]
	fn cast() {
		let cast = Cast(Default::default(), Default::default());
		let serialized = cast.serialize(Serializer.wrap()).unwrap();
		assert_eq!(cast, serialized);
	}
}
