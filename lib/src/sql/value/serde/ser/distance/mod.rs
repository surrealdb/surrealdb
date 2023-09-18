use crate::err::Error;
use crate::sql::index::Distance;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;

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
			"Manhattan" => Ok(Distance::Manhattan),
			"Cosine" => Ok(Distance::Cosine),
			"Hamming" => Ok(Distance::Hamming),
			"Mahalanobis" => Ok(Distance::Mahalanobis),
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
			"Minkowski" => {
				Ok(Distance::Minkowski(value.serialize(ser::number::Serializer.wrap())?))
			}
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::value::serde::ser::Serializer;
	use serde::Serialize;

	#[test]
	fn distance_euclidean() {
		let dist = Distance::Euclidean;
		let serialized = dist.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dist, serialized);
	}

	#[test]
	fn distance_manhattan() {
		let dist = Distance::Manhattan;
		let serialized = dist.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dist, serialized);
	}

	#[test]
	fn distance_mahalanobis() {
		let dist = Distance::Mahalanobis;
		let serialized = dist.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dist, serialized);
	}

	#[test]
	fn distance_hamming() {
		let dist = Distance::Hamming;
		let serialized = dist.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dist, serialized);
	}

	#[test]
	fn distance_cosine() {
		let dist = Distance::Cosine;
		let serialized = dist.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dist, serialized);
	}

	#[test]
	fn distance_minkowski() {
		let dist = Distance::Minkowski(7.into());
		let serialized = dist.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dist, serialized);
	}
}
