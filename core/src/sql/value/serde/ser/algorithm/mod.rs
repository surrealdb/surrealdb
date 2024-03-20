use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Algorithm;
use serde::ser::Error as _;
use serde::ser::Impossible;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Algorithm;
	type Error = Error;

	type SerializeSeq = Impossible<Algorithm, Error>;
	type SerializeTuple = Impossible<Algorithm, Error>;
	type SerializeTupleStruct = Impossible<Algorithm, Error>;
	type SerializeTupleVariant = Impossible<Algorithm, Error>;
	type SerializeMap = Impossible<Algorithm, Error>;
	type SerializeStruct = Impossible<Algorithm, Error>;
	type SerializeStructVariant = Impossible<Algorithm, Error>;

	const EXPECTED: &'static str = "an enum `Algorithm`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"EdDSA" => Ok(Algorithm::EdDSA),
			"Es256" => Ok(Algorithm::Es256),
			"Es384" => Ok(Algorithm::Es384),
			"Es512" => Ok(Algorithm::Es512),
			"Hs256" => Ok(Algorithm::Hs256),
			"Hs384" => Ok(Algorithm::Hs384),
			"Hs512" => Ok(Algorithm::Hs512),
			"Ps256" => Ok(Algorithm::Ps256),
			"Ps384" => Ok(Algorithm::Ps384),
			"Ps512" => Ok(Algorithm::Ps512),
			"Rs256" => Ok(Algorithm::Rs256),
			"Rs384" => Ok(Algorithm::Rs384),
			"Rs512" => Ok(Algorithm::Rs512),
			variant => Err(Error::custom(format!("unknown variant `{name}::{variant}`"))),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn ed_dsa() {
		let algo = Algorithm::EdDSA;
		let serialized = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}

	#[test]
	fn es256() {
		let algo = Algorithm::Es256;
		let serialized: Algorithm = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}

	#[test]
	fn es384() {
		let algo = Algorithm::Es384;
		let serialized = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}

	#[test]
	fn es512() {
		let algo = Algorithm::Es512;
		let serialized = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}

	#[test]
	fn hs256() {
		let algo = Algorithm::Hs256;
		let serialized = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}

	#[test]
	fn hs384() {
		let algo = Algorithm::Hs384;
		let serialized = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}

	#[test]
	fn hs512() {
		let algo = Algorithm::Hs512;
		let serialized = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}

	#[test]
	fn ps256() {
		let algo = Algorithm::Ps256;
		let serialized = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}

	#[test]
	fn ps384() {
		let algo = Algorithm::Ps384;
		let serialized = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}

	#[test]
	fn ps512() {
		let algo = Algorithm::Ps512;
		let serialized = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}

	#[test]
	fn rs256() {
		let algo = Algorithm::Rs256;
		let serialized = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}

	#[test]
	fn rs384() {
		let algo = Algorithm::Rs384;
		let serialized = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}

	#[test]
	fn rs512() {
		let algo = Algorithm::Rs512;
		let serialized = algo.serialize(Serializer.wrap()).unwrap();
		assert_eq!(algo, serialized);
	}
}
