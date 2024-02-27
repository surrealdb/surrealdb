use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Operator;
use serde::ser::Error as _;
use serde::ser::Impossible;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Operator;
	type Error = Error;

	type SerializeSeq = Impossible<Operator, Error>;
	type SerializeTuple = Impossible<Operator, Error>;
	type SerializeTupleStruct = Impossible<Operator, Error>;
	type SerializeTupleVariant = Impossible<Operator, Error>;
	type SerializeMap = Impossible<Operator, Error>;
	type SerializeStruct = Impossible<Operator, Error>;
	type SerializeStructVariant = Impossible<Operator, Error>;

	const EXPECTED: &'static str = "an enum `Operator`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Neg" => Ok(Operator::Neg),
			"Not" => Ok(Operator::Not),
			"Or" => Ok(Operator::Or),
			"And" => Ok(Operator::And),
			"Tco" => Ok(Operator::Tco),
			"Nco" => Ok(Operator::Nco),
			"Add" => Ok(Operator::Add),
			"Sub" => Ok(Operator::Sub),
			"Mul" => Ok(Operator::Mul),
			"Div" => Ok(Operator::Div),
			"Pow" => Ok(Operator::Pow),
			"Inc" => Ok(Operator::Inc),
			"Dec" => Ok(Operator::Dec),
			"Equal" => Ok(Operator::Equal),
			"Exact" => Ok(Operator::Exact),
			"NotEqual" => Ok(Operator::NotEqual),
			"AllEqual" => Ok(Operator::AllEqual),
			"AnyEqual" => Ok(Operator::AnyEqual),
			"Like" => Ok(Operator::Like),
			"NotLike" => Ok(Operator::NotLike),
			"AllLike" => Ok(Operator::AllLike),
			"AnyLike" => Ok(Operator::AnyLike),
			"LessThan" => Ok(Operator::LessThan),
			"LessThanOrEqual" => Ok(Operator::LessThanOrEqual),
			"MoreThan" => Ok(Operator::MoreThan),
			"MoreThanOrEqual" => Ok(Operator::MoreThanOrEqual),
			"Contain" => Ok(Operator::Contain),
			"NotContain" => Ok(Operator::NotContain),
			"ContainAll" => Ok(Operator::ContainAll),
			"ContainAny" => Ok(Operator::ContainAny),
			"ContainNone" => Ok(Operator::ContainNone),
			"Inside" => Ok(Operator::Inside),
			"NotInside" => Ok(Operator::NotInside),
			"AllInside" => Ok(Operator::AllInside),
			"AnyInside" => Ok(Operator::AnyInside),
			"NoneInside" => Ok(Operator::NoneInside),
			"Outside" => Ok(Operator::Outside),
			"Intersects" => Ok(Operator::Intersects),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn or() {
		let dir = Operator::Or;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn and() {
		let dir = Operator::And;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn tco() {
		let dir = Operator::Tco;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn nco() {
		let dir = Operator::Nco;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn add() {
		let dir = Operator::Add;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn sub() {
		let dir = Operator::Sub;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn mul() {
		let dir = Operator::Mul;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn div() {
		let dir = Operator::Div;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn pow() {
		let dir = Operator::Pow;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn inc() {
		let dir = Operator::Inc;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn dec() {
		let dir = Operator::Dec;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn equal() {
		let dir = Operator::Equal;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn exact() {
		let dir = Operator::Exact;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn not_equal() {
		let dir = Operator::NotEqual;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn all_equal() {
		let dir = Operator::AllEqual;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn any_equal() {
		let dir = Operator::AnyEqual;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn like() {
		let dir = Operator::Like;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn not_like() {
		let dir = Operator::NotLike;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn all_like() {
		let dir = Operator::AllLike;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn any_like() {
		let dir = Operator::AnyLike;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn less_than() {
		let dir = Operator::LessThan;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn less_than_or_equal() {
		let dir = Operator::LessThanOrEqual;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn more_than() {
		let dir = Operator::MoreThan;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn more_than_or_equal() {
		let dir = Operator::MoreThanOrEqual;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn contain() {
		let dir = Operator::Contain;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn not_contain() {
		let dir = Operator::NotContain;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn contain_all() {
		let dir = Operator::ContainAll;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn contain_any() {
		let dir = Operator::ContainAny;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn contain_none() {
		let dir = Operator::ContainNone;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn inside() {
		let dir = Operator::Inside;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn not_inside() {
		let dir = Operator::NotInside;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn all_inside() {
		let dir = Operator::AllInside;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn any_inside() {
		let dir = Operator::AnyInside;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn none_inside() {
		let dir = Operator::NoneInside;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn outside() {
		let dir = Operator::Outside;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn intersects() {
		let dir = Operator::Intersects;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}
}
