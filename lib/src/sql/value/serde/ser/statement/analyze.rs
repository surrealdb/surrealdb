use crate::err::Error;
use crate::sql::statements::analyze::AnalyzeStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = AnalyzeStatement;
	type Error = Error;

	type SerializeSeq = Impossible<AnalyzeStatement, Error>;
	type SerializeTuple = Impossible<AnalyzeStatement, Error>;
	type SerializeTupleStruct = Impossible<AnalyzeStatement, Error>;
	type SerializeTupleVariant = SerializeAnalyzeStatement;
	type SerializeMap = Impossible<AnalyzeStatement, Error>;
	type SerializeStruct = Impossible<AnalyzeStatement, Error>;
	type SerializeStructVariant = Impossible<AnalyzeStatement, Error>;

	const EXPECTED: &'static str = "an enum `AnalyzeStatement`";

	fn serialize_tuple_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		let tuple = match variant {
			"Idx" => (None, None),
			variant => {
				return Err(Error::custom(format!("unexpected tuple variant `{name}::{variant}`")));
			}
		};
		Ok(SerializeAnalyzeStatement {
			tuple,
			index: 0,
		})
	}
}

pub(super) struct SerializeAnalyzeStatement {
	index: usize,
	tuple: (Option<Ident>, Option<Ident>),
}

impl serde::ser::SerializeTupleVariant for SerializeAnalyzeStatement {
	type Ok = AnalyzeStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.index {
			0 => {
				self.tuple.0 = Some(Ident(value.serialize(ser::string::Serializer.wrap())?));
			}
			1 => {
				self.tuple.1 = Some(Ident(value.serialize(ser::string::Serializer.wrap())?));
			}
			index => {
				return Err(Error::custom(format!(
					"unexpected `AnalyzeStatement::Idx` index `{index}`"
				)));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match self.tuple {
			(Some(one), Some(two)) => Ok(AnalyzeStatement::Idx(one, two)),
			_ => Err(Error::custom("`AnalyzeStatement` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde::Serialize;

	#[test]
	fn idx() {
		let stmt = AnalyzeStatement::Idx(Default::default(), Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}
}
