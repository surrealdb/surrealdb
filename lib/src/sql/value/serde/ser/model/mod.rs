use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Model;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Model;
	type Error = Error;

	type SerializeSeq = Impossible<Model, Error>;
	type SerializeTuple = Impossible<Model, Error>;
	type SerializeTupleStruct = Impossible<Model, Error>;
	type SerializeTupleVariant = SerializeModel;
	type SerializeMap = Impossible<Model, Error>;
	type SerializeStruct = Impossible<Model, Error>;
	type SerializeStructVariant = Impossible<Model, Error>;

	const EXPECTED: &'static str = "an enum `Model`";

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
		Ok(SerializeModel {
			inner,
			index: 0,
		})
	}
}

pub(super) struct SerializeModel {
	index: usize,
	inner: Inner,
}

enum Inner {
	Count(Option<String>, Option<u64>),
	Range(Option<String>, Option<u64>, Option<u64>),
}

impl serde::ser::SerializeTupleVariant for SerializeModel {
	type Ok = Model;
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
				return Err(Error::custom(format!(
					"unexpected `Model::{variant}` index `{index}`"
				)));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match self.inner {
			Inner::Count(Some(one), Some(two)) => Ok(Model::Count(one, two)),
			Inner::Range(Some(one), Some(two), Some(three)) => Ok(Model::Range(one, two, three)),
			_ => Err(Error::custom("`Model` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;
	use serde::Serialize;

	#[test]
	fn count() {
		let model = Model::Count(Default::default(), Default::default());
		let serialized = serialize_internal(|| model.serialize(Serializer.wrap())).unwrap();
		assert_eq!(model, serialized);
	}

	#[test]
	fn range() {
		let model = Model::Range(Default::default(), 1, 2);
		let serialized = serialize_internal(|| model.serialize(Serializer.wrap())).unwrap();
		assert_eq!(model, serialized);
	}
}
