use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Mock;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Mock;
	type Error = Error;

	type SerializeSeq = Impossible<Mock, Error>;
	type SerializeTuple = Impossible<Mock, Error>;
	type SerializeTupleStruct = Impossible<Mock, Error>;
	type SerializeTupleVariant = SerializeMock;
	type SerializeMap = Impossible<Mock, Error>;
	type SerializeStruct = Impossible<Mock, Error>;
	type SerializeStructVariant = Impossible<Mock, Error>;

	const EXPECTED: &'static str = "an enum `Mock`";

	fn serialize_tuple_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		let inner = match variant {
			"Count" => Inner::Count(None, None),
			"Range" => Inner::Range(None, None, None),
			variant => {
				return Err(Error::custom(format!("unexpected tuple variant `{name}::{variant}`")));
			}
		};
		Ok(SerializeMock {
			inner,
			index: 0,
		})
	}
}

pub(super) struct SerializeMock {
	index: usize,
	inner: Inner,
}

enum Inner {
	Count(Option<String>, Option<u64>),
	Range(Option<String>, Option<u64>, Option<u64>),
}

impl serde::ser::SerializeTupleVariant for SerializeMock {
	type Ok = Mock;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match (self.index, &mut self.inner) {
			(0, Inner::Count(ref mut var, _) | Inner::Range(ref mut var, ..)) => {
				*var = Some(value.serialize(ser::string::Serializer.wrap())?);
			}
			(1, Inner::Count(_, ref mut var) | Inner::Range(_, ref mut var, _)) => {
				*var = Some(value.serialize(ser::primitive::u64::Serializer.wrap())?);
			}
			(2, Inner::Range(.., ref mut var)) => {
				*var = Some(value.serialize(ser::primitive::u64::Serializer.wrap())?);
			}
			(index, inner) => {
				let variant = match inner {
					Inner::Count(..) => "Count",
					Inner::Range(..) => "Range",
				};
				return Err(Error::custom(format!("unexpected `Mock::{variant}` index `{index}`")));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match self.inner {
			Inner::Count(Some(one), Some(two)) => Ok(Mock::Count(one, two)),
			Inner::Range(Some(one), Some(two), Some(three)) => Ok(Mock::Range(one, two, three)),
			_ => Err(Error::custom("`Mock` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde::Serialize;

	#[test]
	fn count() {
		let model = Mock::Count(Default::default(), Default::default());
		let serialized = model.serialize(Serializer.wrap()).unwrap();
		assert_eq!(model, serialized);
	}

	#[test]
	fn range() {
		let model = Mock::Range(Default::default(), 1, 2);
		let serialized = model.serialize(Serializer.wrap()).unwrap();
		assert_eq!(model, serialized);
	}
}
