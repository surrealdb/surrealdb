pub(super) mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Kind;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = (Option<Kind>, Option<Kind>);
	type Error = Error;

	type SerializeSeq = Impossible<(Option<Kind>, Option<Kind>), Error>;
	type SerializeTuple = SerializeKindTuple;
	type SerializeTupleStruct = Impossible<(Option<Kind>, Option<Kind>), Error>;
	type SerializeTupleVariant = Impossible<(Option<Kind>, Option<Kind>), Error>;
	type SerializeMap = Impossible<(Option<Kind>, Option<Kind>), Error>;
	type SerializeStruct = Impossible<(Option<Kind>, Option<Kind>), Error>;
	type SerializeStructVariant = Impossible<(Option<Kind>, Option<Kind>), Error>;

	const EXPECTED: &'static str = "an `(Option<Kind>, Option<Kind>)`";

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		Ok(SerializeKindTuple::default())
	}
}

#[derive(Default)]
struct SerializeKindTuple {
	index: usize,
	tuple: (Option<Kind>, Option<Kind>),
}

impl serde::ser::SerializeTuple for SerializeKindTuple {
	type Ok = (Option<Kind>, Option<Kind>);
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.index {
			0 => {
				self.tuple.0 = value.serialize(ser::kind::opt::Serializer.wrap())?;
			}
			1 => {
				self.tuple.1 = value.serialize(ser::kind::opt::Serializer.wrap())?;
			}
			index => {
				return Err(Error::custom(format!(
					"unexpected tuple index `{index}` for `(Option<Kind>, Option<Kind>)`"
				)));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.tuple)
	}
}
