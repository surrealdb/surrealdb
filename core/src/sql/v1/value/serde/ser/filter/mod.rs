pub(super) mod vec;

use crate::err::Error;
use crate::sql::filter::Filter;
use crate::sql::value::serde::ser;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Filter;
	type Error = Error;

	type SerializeSeq = Impossible<Filter, Error>;
	type SerializeTuple = Impossible<Filter, Error>;
	type SerializeTupleStruct = Impossible<Filter, Error>;
	type SerializeTupleVariant = SerializeFilter;
	type SerializeMap = Impossible<Filter, Error>;
	type SerializeStruct = Impossible<Filter, Error>;
	type SerializeStructVariant = Impossible<Filter, Error>;

	const EXPECTED: &'static str = "an enum `Filter`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Ascii" => Ok(Filter::Ascii),
			"Lowercase" => Ok(Filter::Lowercase),
			"Uppercase" => Ok(Filter::Uppercase),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}

	#[inline]
	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Snowball" => Ok(Filter::Snowball(value.serialize(ser::language::Serializer.wrap())?)),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}

	fn serialize_tuple_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		let inner = match variant {
			"EdgeNgram" => Inner::EdgeNgram(Default::default(), Default::default()),
			"Ngram" => Inner::Ngram(Default::default(), Default::default()),
			variant => {
				return Err(Error::custom(format!("unexpected tuple variant `{name}::{variant}`")));
			}
		};
		Ok(SerializeFilter {
			inner,
			index: 0,
		})
	}
}

pub(super) struct SerializeFilter {
	index: usize,
	inner: Inner,
}

enum Inner {
	EdgeNgram(u16, u16),
	Ngram(u16, u16),
}

impl serde::ser::SerializeTupleVariant for SerializeFilter {
	type Ok = Filter;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match (self.index, &mut self.inner) {
			(0, Inner::EdgeNgram(ref mut var, _) | Inner::Ngram(ref mut var, _)) => {
				*var = value.serialize(ser::primitive::u16::Serializer.wrap())?;
			}
			(1, Inner::EdgeNgram(_, ref mut var) | Inner::Ngram(_, ref mut var)) => {
				*var = value.serialize(ser::primitive::u16::Serializer.wrap())?;
			}
			(index, inner) => {
				let variant = match inner {
					Inner::EdgeNgram(..) => "EdgeNgram",
					Inner::Ngram(..) => "Ngram",
				};
				return Err(Error::custom(format!(
					"unexpected `Filter::{variant}` index `{index}`"
				)));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match self.inner {
			Inner::EdgeNgram(one, two) => Ok(Filter::EdgeNgram(one, two)),
			Inner::Ngram(one, two) => Ok(Filter::Ngram(one, two)),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::language::Language;

	#[test]
	fn ascii() {
		let filter = Filter::Ascii;
		let serialized = filter.serialize(Serializer.wrap()).unwrap();
		assert_eq!(filter, serialized);
	}

	#[test]
	fn lowercase() {
		let filter = Filter::Lowercase;
		let serialized = filter.serialize(Serializer.wrap()).unwrap();
		assert_eq!(filter, serialized);
	}

	#[test]
	fn snowball() {
		let filter = Filter::Snowball(Language::English);
		let serialized = filter.serialize(Serializer.wrap()).unwrap();
		assert_eq!(filter, serialized);
	}
}
