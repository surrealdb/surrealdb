pub(super) mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Data;
use crate::sql::Idiom;
use crate::sql::Operator;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Data;
	type Error = Error;

	type SerializeSeq = Impossible<Data, Error>;
	type SerializeTuple = Impossible<Data, Error>;
	type SerializeTupleStruct = Impossible<Data, Error>;
	type SerializeTupleVariant = Impossible<Data, Error>;
	type SerializeMap = Impossible<Data, Error>;
	type SerializeStruct = Impossible<Data, Error>;
	type SerializeStructVariant = Impossible<Data, Error>;

	const EXPECTED: &'static str = "an enum `Data`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"EmptyExpression" => Ok(Data::EmptyExpression),
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
			"SetExpression" => {
				Ok(Data::SetExpression(value.serialize(IdiomOperatorValueVecSerializer.wrap())?))
			}
			"UnsetExpression" => {
				Ok(Data::UnsetExpression(value.serialize(IdiomVecSerializer.wrap())?))
			}
			"PatchExpression" => {
				Ok(Data::PatchExpression(value.serialize(ser::value::Serializer.wrap())?))
			}
			"MergeExpression" => {
				Ok(Data::MergeExpression(value.serialize(ser::value::Serializer.wrap())?))
			}
			"ReplaceExpression" => {
				Ok(Data::ReplaceExpression(value.serialize(ser::value::Serializer.wrap())?))
			}
			"ContentExpression" => {
				Ok(Data::ContentExpression(value.serialize(ser::value::Serializer.wrap())?))
			}
			"SingleExpression" => {
				Ok(Data::SingleExpression(value.serialize(ser::value::Serializer.wrap())?))
			}
			"ValuesExpression" => {
				Ok(Data::ValuesExpression(value.serialize(IdiomValueVecVecSerializer.wrap())?))
			}
			"UpdateExpression" => {
				Ok(Data::UpdateExpression(value.serialize(IdiomOperatorValueVecSerializer.wrap())?))
			}
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

struct IdiomVecSerializer;

impl ser::Serializer for IdiomVecSerializer {
	type Ok = Vec<Idiom>;
	type Error = Error;

	type SerializeSeq = SerializeIdiomVec;
	type SerializeTuple = Impossible<Vec<Idiom>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Idiom>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Idiom>, Error>;
	type SerializeMap = Impossible<Vec<Idiom>, Error>;
	type SerializeStruct = Impossible<Vec<Idiom>, Error>;
	type SerializeStructVariant = Impossible<Vec<Idiom>, Error>;

	const EXPECTED: &'static str = "an `Vec<Idiom>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeIdiomVec(Vec::with_capacity(len.unwrap_or_default())))
	}
}

struct SerializeIdiomVec(Vec<Idiom>);

impl serde::ser::SerializeSeq for SerializeIdiomVec {
	type Ok = Vec<Idiom>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(Idiom(value.serialize(ser::part::vec::Serializer.wrap())?));
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.0)
	}
}

type IdiomOperatorValueTuple = (Idiom, Operator, Value);

struct IdiomOperatorValueVecSerializer;

impl ser::Serializer for IdiomOperatorValueVecSerializer {
	type Ok = Vec<IdiomOperatorValueTuple>;
	type Error = Error;

	type SerializeSeq = SerializeIdiomOperatorValueVec;
	type SerializeTuple = Impossible<Vec<IdiomOperatorValueTuple>, Error>;
	type SerializeTupleStruct = Impossible<Vec<IdiomOperatorValueTuple>, Error>;
	type SerializeTupleVariant = Impossible<Vec<IdiomOperatorValueTuple>, Error>;
	type SerializeMap = Impossible<Vec<IdiomOperatorValueTuple>, Error>;
	type SerializeStruct = Impossible<Vec<IdiomOperatorValueTuple>, Error>;
	type SerializeStructVariant = Impossible<Vec<IdiomOperatorValueTuple>, Error>;

	const EXPECTED: &'static str = "an `(Idiom, Operator, Value)`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeIdiomOperatorValueVec(Vec::with_capacity(len.unwrap_or_default())))
	}
}

struct SerializeIdiomOperatorValueVec(Vec<IdiomOperatorValueTuple>);

impl serde::ser::SerializeSeq for SerializeIdiomOperatorValueVec {
	type Ok = Vec<IdiomOperatorValueTuple>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(IdiomOperatorValueTupleSerializer.wrap())?);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.0)
	}
}

struct IdiomOperatorValueTupleSerializer;

impl ser::Serializer for IdiomOperatorValueTupleSerializer {
	type Ok = IdiomOperatorValueTuple;
	type Error = Error;

	type SerializeSeq = Impossible<IdiomOperatorValueTuple, Error>;
	type SerializeTuple = SerializeIdiomOperatorValueTuple;
	type SerializeTupleStruct = Impossible<IdiomOperatorValueTuple, Error>;
	type SerializeTupleVariant = Impossible<IdiomOperatorValueTuple, Error>;
	type SerializeMap = Impossible<IdiomOperatorValueTuple, Error>;
	type SerializeStruct = Impossible<IdiomOperatorValueTuple, Error>;
	type SerializeStructVariant = Impossible<IdiomOperatorValueTuple, Error>;

	const EXPECTED: &'static str = "an `(Idiom, Operator, Value)`";

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		Ok(SerializeIdiomOperatorValueTuple::default())
	}
}

#[derive(Default)]
struct SerializeIdiomOperatorValueTuple {
	index: usize,
	idiom: Option<Idiom>,
	operator: Option<Operator>,
	value: Option<Value>,
}

impl serde::ser::SerializeTuple for SerializeIdiomOperatorValueTuple {
	type Ok = IdiomOperatorValueTuple;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.index {
			0 => {
				self.idiom = Some(Idiom(value.serialize(ser::part::vec::Serializer.wrap())?));
			}
			1 => {
				self.operator = Some(value.serialize(ser::operator::Serializer.wrap())?);
			}
			2 => {
				self.value = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			index => {
				return Err(Error::custom(format!(
					"unexpected tuple index `{index}` for `(Idiom, Operator, Value)`"
				)));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match (self.idiom, self.operator, self.value) {
			(Some(idiom), Some(operator), Some(value)) => Ok((idiom, operator, value)),
			_ => Err(Error::custom("`(Idiom, Operator, Value)` missing required value(s)")),
		}
	}
}

type IdiomValueTuple = (Idiom, Value);

struct IdiomValueVecVecSerializer;

impl ser::Serializer for IdiomValueVecVecSerializer {
	type Ok = Vec<Vec<IdiomValueTuple>>;
	type Error = Error;

	type SerializeSeq = SerializeIdiomValueVecVec;
	type SerializeTuple = Impossible<Vec<Vec<IdiomValueTuple>>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Vec<IdiomValueTuple>>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Vec<IdiomValueTuple>>, Error>;
	type SerializeMap = Impossible<Vec<Vec<IdiomValueTuple>>, Error>;
	type SerializeStruct = Impossible<Vec<Vec<IdiomValueTuple>>, Error>;
	type SerializeStructVariant = Impossible<Vec<Vec<IdiomValueTuple>>, Error>;

	const EXPECTED: &'static str = "a `Vec<Vec<(Idiom, Value)>>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeIdiomValueVecVec(Vec::with_capacity(len.unwrap_or_default())))
	}
}

struct SerializeIdiomValueVecVec(Vec<Vec<IdiomValueTuple>>);

impl serde::ser::SerializeSeq for SerializeIdiomValueVecVec {
	type Ok = Vec<Vec<IdiomValueTuple>>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(IdiomValueVecSerializer.wrap())?);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.0)
	}
}

struct IdiomValueVecSerializer;

impl ser::Serializer for IdiomValueVecSerializer {
	type Ok = Vec<IdiomValueTuple>;
	type Error = Error;

	type SerializeSeq = SerializeIdiomValueVec;
	type SerializeTuple = Impossible<Vec<IdiomValueTuple>, Error>;
	type SerializeTupleStruct = Impossible<Vec<IdiomValueTuple>, Error>;
	type SerializeTupleVariant = Impossible<Vec<IdiomValueTuple>, Error>;
	type SerializeMap = Impossible<Vec<IdiomValueTuple>, Error>;
	type SerializeStruct = Impossible<Vec<IdiomValueTuple>, Error>;
	type SerializeStructVariant = Impossible<Vec<IdiomValueTuple>, Error>;

	const EXPECTED: &'static str = "a `Vec<(Idiom, Value)>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeIdiomValueVec(Vec::with_capacity(len.unwrap_or_default())))
	}
}

struct SerializeIdiomValueVec(Vec<IdiomValueTuple>);

impl serde::ser::SerializeSeq for SerializeIdiomValueVec {
	type Ok = Vec<IdiomValueTuple>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(IdiomValueTupleSerializer.wrap())?);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.0)
	}
}

struct IdiomValueTupleSerializer;

impl ser::Serializer for IdiomValueTupleSerializer {
	type Ok = IdiomValueTuple;
	type Error = Error;

	type SerializeSeq = Impossible<IdiomValueTuple, Error>;
	type SerializeTuple = SerializeIdiomValueTuple;
	type SerializeTupleStruct = Impossible<IdiomValueTuple, Error>;
	type SerializeTupleVariant = Impossible<IdiomValueTuple, Error>;
	type SerializeMap = Impossible<IdiomValueTuple, Error>;
	type SerializeStruct = Impossible<IdiomValueTuple, Error>;
	type SerializeStructVariant = Impossible<IdiomValueTuple, Error>;

	const EXPECTED: &'static str = "an `(Idiom, Value)`";

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		Ok(SerializeIdiomValueTuple::default())
	}
}

#[derive(Default)]
struct SerializeIdiomValueTuple {
	index: usize,
	idiom: Option<Idiom>,
	value: Option<Value>,
}

impl serde::ser::SerializeTuple for SerializeIdiomValueTuple {
	type Ok = IdiomValueTuple;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.index {
			0 => {
				self.idiom = Some(Idiom(value.serialize(ser::part::vec::Serializer.wrap())?));
			}
			1 => {
				self.value = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			index => {
				return Err(Error::custom(format!(
					"unexpected tuple index `{index}` for `(Idiom, Value)`"
				)));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match (self.idiom, self.value) {
			(Some(idiom), Some(value)) => Ok((idiom, value)),
			_ => Err(Error::custom("`(Idiom, Value)` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn empty_expression() {
		let data = Data::EmptyExpression;
		let serialized = data.serialize(Serializer.wrap()).unwrap();
		assert_eq!(data, serialized);
	}

	#[test]
	fn set_expression() {
		let data =
			Data::SetExpression(vec![(Default::default(), Default::default(), Default::default())]);
		let serialized = data.serialize(Serializer.wrap()).unwrap();
		assert_eq!(data, serialized);
	}

	#[test]
	fn unset_expression() {
		let data = Data::UnsetExpression(vec![Default::default()]);
		let serialized = data.serialize(Serializer.wrap()).unwrap();
		assert_eq!(data, serialized);
	}

	#[test]
	fn patch_expression() {
		let data = Data::PatchExpression(Default::default());
		let serialized = data.serialize(Serializer.wrap()).unwrap();
		assert_eq!(data, serialized);
	}

	#[test]
	fn merge_expression() {
		let data = Data::MergeExpression(Default::default());
		let serialized = data.serialize(Serializer.wrap()).unwrap();
		assert_eq!(data, serialized);
	}

	#[test]
	fn replace_expression() {
		let data = Data::ReplaceExpression(Default::default());
		let serialized = data.serialize(Serializer.wrap()).unwrap();
		assert_eq!(data, serialized);
	}

	#[test]
	fn content_expression() {
		let data = Data::ContentExpression(Default::default());
		let serialized = data.serialize(Serializer.wrap()).unwrap();
		assert_eq!(data, serialized);
	}

	#[test]
	fn single_expression() {
		let data = Data::SingleExpression(Default::default());
		let serialized = data.serialize(Serializer.wrap()).unwrap();
		assert_eq!(data, serialized);
	}

	#[test]
	fn values_expression() {
		let data = Data::ValuesExpression(vec![vec![(Default::default(), Default::default())]]);
		let serialized = data.serialize(Serializer.wrap()).unwrap();
		assert_eq!(data, serialized);
	}

	#[test]
	fn update_expression() {
		let data = Data::UpdateExpression(vec![(
			Default::default(),
			Default::default(),
			Default::default(),
		)]);
		let serialized = data.serialize(Serializer.wrap()).unwrap();
		assert_eq!(data, serialized);
	}
}
