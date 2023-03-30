use crate::err::Error;
use crate::sql::constant::Constant;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Constant;
	type Error = Error;

	type SerializeSeq = Impossible<Constant, Error>;
	type SerializeTuple = Impossible<Constant, Error>;
	type SerializeTupleStruct = Impossible<Constant, Error>;
	type SerializeTupleVariant = Impossible<Constant, Error>;
	type SerializeMap = Impossible<Constant, Error>;
	type SerializeStruct = Impossible<Constant, Error>;
	type SerializeStructVariant = Impossible<Constant, Error>;

	const EXPECTED: &'static str = "an enum `Constant`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"MathE" => Ok(Constant::MathE),
			"MathFrac1Pi" => Ok(Constant::MathFrac1Pi),
			"MathFrac1Sqrt2" => Ok(Constant::MathFrac1Sqrt2),
			"MathFrac2Pi" => Ok(Constant::MathFrac2Pi),
			"MathFrac2SqrtPi" => Ok(Constant::MathFrac2SqrtPi),
			"MathFracPi2" => Ok(Constant::MathFracPi2),
			"MathFracPi3" => Ok(Constant::MathFracPi3),
			"MathFracPi4" => Ok(Constant::MathFracPi4),
			"MathFracPi6" => Ok(Constant::MathFracPi6),
			"MathFracPi8" => Ok(Constant::MathFracPi8),
			"MathLn10" => Ok(Constant::MathLn10),
			"MathLn2" => Ok(Constant::MathLn2),
			"MathLog102" => Ok(Constant::MathLog102),
			"MathLog10E" => Ok(Constant::MathLog10E),
			"MathLog210" => Ok(Constant::MathLog210),
			"MathLog2E" => Ok(Constant::MathLog2E),
			"MathPi" => Ok(Constant::MathPi),
			"MathSqrt2" => Ok(Constant::MathSqrt2),
			"MathTau" => Ok(Constant::MathTau),
			variant => Err(Error::custom(format!("unknown variant `{name}::{variant}`"))),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn math_e() {
		let constant = Constant::MathE;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_frac_1pi() {
		let constant = Constant::MathFrac1Pi;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_frac_1sqrt2() {
		let constant = Constant::MathFrac1Sqrt2;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_frac_2pi() {
		let constant = Constant::MathFrac2Pi;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_frac_2sqrt_pi() {
		let constant = Constant::MathFrac2SqrtPi;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_frac_pi2() {
		let constant = Constant::MathFracPi2;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_frac_pi3() {
		let constant = Constant::MathFracPi3;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_frac_pi4() {
		let constant = Constant::MathFracPi4;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_frac_pi6() {
		let constant = Constant::MathFracPi6;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_frac_pi8() {
		let constant = Constant::MathFracPi8;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_ln10() {
		let constant = Constant::MathLn10;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_ln2() {
		let constant = Constant::MathLn2;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_log102() {
		let constant = Constant::MathLog102;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_log10_e() {
		let constant = Constant::MathLog10E;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_log210() {
		let constant = Constant::MathLog210;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_log2_e() {
		let constant = Constant::MathLog2E;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_pi() {
		let constant = Constant::MathPi;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_sqrt2() {
		let constant = Constant::MathSqrt2;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}

	#[test]
	fn math_tau() {
		let constant = Constant::MathTau;
		let serialized = serialize_internal(|| constant.serialize(Serializer.wrap())).unwrap();
		assert_eq!(constant, serialized);
	}
}
