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
			"Chebyshev" => Ok(Distance::Chebyshev),
			"Cosine" => Ok(Distance::Cosine),
			"Euclidean" => Ok(Distance::Euclidean),
			"Hamming" => Ok(Distance::Hamming),
			"Jaccard" => Ok(Distance::Jaccard),
			"Manhattan" => Ok(Distance::Manhattan),
			"Pearson" => Ok(Distance::Pearson),
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

	#[test]
	fn distance_euclidean() {
		for dist in [
			Distance::Chebyshev,
			Distance::Cosine,
			Distance::Euclidean,
			Distance::Jaccard,
			Distance::Hamming,
			Distance::Manhattan,
			Distance::Minkowski(7.into()),
			Distance::Pearson,
		] {
			let serialized = dist.serialize(Serializer.wrap()).unwrap();
			assert_eq!(dist, serialized, "{}", dist);
		}
	}
}
