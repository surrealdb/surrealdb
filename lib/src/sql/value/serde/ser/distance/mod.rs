use crate::err::Error;
use crate::sql::index::Distance;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Distance;
	type Error = Error;

	type SerializeSeq = Impossible<Distance, Error>;
	type SerializeTuple = Impossible<Distance, Error>;
	type SerializeTupleStruct = Impossible<Distance, Error>;
	type SerializeTupleVariant = Impossible<Distance, Error>;
	type SerializeMap = Impossible<Distance, Error>;
	type SerializeStruct = Impossible<Distance, Error>;
	type SerializeStructVariant = Impossible<Distance, Error>;

	const EXPECTED: &'static str = "an enum `Distance`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Euclidean" => Ok(Distance::Euclidean),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::value::serde::ser::Serializer;
	use serde::Serialize;

	#[test]
	fn euclidean() {
		let dist = Distance::Euclidean;
		let serialized = dist.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dist, serialized);
	}
}
