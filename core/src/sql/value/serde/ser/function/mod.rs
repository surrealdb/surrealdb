use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Function;
use crate::sql::Script;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Function;
	type Error = Error;

	type SerializeSeq = Impossible<Function, Error>;
	type SerializeTuple = Impossible<Function, Error>;
	type SerializeTupleStruct = Impossible<Function, Error>;
	type SerializeTupleVariant = SerializeFunction;
	type SerializeMap = Impossible<Function, Error>;
	type SerializeStruct = Impossible<Function, Error>;
	type SerializeStructVariant = Impossible<Function, Error>;

	const EXPECTED: &'static str = "an enum `Function`";

	fn serialize_tuple_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		let inner = match variant {
			"Normal" => Inner::Normal(None, None),
			"Custom" => Inner::Custom(None, None),
			"Script" => Inner::Script(None, None),
			"Anonymous" => Inner::Anonymous(None, None),
			variant => {
				return Err(Error::custom(format!("unexpected tuple variant `{name}::{variant}`")));
			}
		};
		Ok(SerializeFunction {
			inner,
			index: 0,
		})
	}
}

pub(super) struct SerializeFunction {
	index: usize,
	inner: Inner,
}

enum Inner {
	Normal(Option<String>, Option<Vec<Value>>),
	Custom(Option<String>, Option<Vec<Value>>),
	Script(Option<Script>, Option<Vec<Value>>),
	Anonymous(Option<Value>, Option<Vec<Value>>),
}

impl serde::ser::SerializeTupleVariant for SerializeFunction {
	type Ok = Function;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match (self.index, &mut self.inner) {
			(0, Inner::Normal(ref mut var, _) | Inner::Custom(ref mut var, _)) => {
				*var = Some(value.serialize(ser::string::Serializer.wrap())?);
			}
			(0, Inner::Script(ref mut var, _)) => {
				*var = Some(Script(value.serialize(ser::string::Serializer.wrap())?));
			}
			(0, Inner::Anonymous(ref mut var, _)) => {
				*var = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			(
				1,
				Inner::Normal(_, ref mut var)
				| Inner::Custom(_, ref mut var)
				| Inner::Script(_, ref mut var)
				| Inner::Anonymous(_, ref mut var),
			) => {
				*var = Some(value.serialize(ser::value::vec::Serializer.wrap())?);
			}
			(index, inner) => {
				let variant = match inner {
					Inner::Normal(..) => "Normal",
					Inner::Custom(..) => "Custom",
					Inner::Script(..) => "Script",
					Inner::Anonymous(..) => "Anonymous",
				};
				return Err(Error::custom(format!(
					"unexpected `Function::{variant}` index `{index}`"
				)));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match self.inner {
			Inner::Normal(Some(one), Some(two)) => Ok(Function::Normal(one, two)),
			Inner::Custom(Some(one), Some(two)) => Ok(Function::Custom(one, two)),
			Inner::Script(Some(one), Some(two)) => Ok(Function::Script(one, two)),
			Inner::Anonymous(Some(one), Some(two)) => Ok(Function::Anonymous(one, two)),
			_ => Err(Error::custom("`Function` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde::Serialize;

	#[test]
	fn normal() {
		let function = Function::Normal(Default::default(), vec![Default::default()]);
		let serialized = function.serialize(Serializer.wrap()).unwrap();
		assert_eq!(function, serialized);
	}

	#[test]
	fn custom() {
		let function = Function::Custom(Default::default(), vec![Default::default()]);
		let serialized = function.serialize(Serializer.wrap()).unwrap();
		assert_eq!(function, serialized);
	}

	#[test]
	fn script() {
		let function = Function::Script(Default::default(), vec![Default::default()]);
		let serialized = function.serialize(Serializer.wrap()).unwrap();
		assert_eq!(function, serialized);
	}

	#[test]
	fn anonymous() {
		let function = Function::Anonymous(Default::default(), vec![Default::default()]);
		let serialized = function.serialize(Serializer.wrap()).unwrap();
		assert_eq!(function, serialized);
	}
}
