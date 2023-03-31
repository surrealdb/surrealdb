pub(super) mod vec;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Idiom;
use crate::sql::Order;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Order;
	type Error = Error;

	type SerializeSeq = Impossible<Order, Error>;
	type SerializeTuple = Impossible<Order, Error>;
	type SerializeTupleStruct = Impossible<Order, Error>;
	type SerializeTupleVariant = Impossible<Order, Error>;
	type SerializeMap = Impossible<Order, Error>;
	type SerializeStruct = SerializeOrder;
	type SerializeStructVariant = Impossible<Order, Error>;

	const EXPECTED: &'static str = "a struct `Order`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeOrder::default())
	}
}

#[derive(Default)]
pub(super) struct SerializeOrder {
	order: Option<Idiom>,
	random: Option<bool>,
	collate: Option<bool>,
	numeric: Option<bool>,
	direction: Option<bool>,
}

impl serde::ser::SerializeStruct for SerializeOrder {
	type Ok = Order;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"order" => {
				self.order = Some(Idiom(value.serialize(ser::part::vec::Serializer.wrap())?));
			}
			"random" => {
				self.random = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			"collate" => {
				self.collate = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			"numeric" => {
				self.numeric = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			"direction" => {
				self.direction = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Order::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.order, self.random, self.collate, self.numeric, self.direction) {
			(Some(order), Some(random), Some(collate), Some(numeric), Some(direction)) => {
				Ok(Order {
					order,
					random,
					collate,
					numeric,
					direction,
				})
			}
			_ => Err(Error::custom("`Order` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;
	use serde::Serialize;

	#[test]
	fn default() {
		let order = Order::default();
		let serialized = serialize_internal(|| order.serialize(Serializer.wrap())).unwrap();
		assert_eq!(order, serialized);
	}
}
