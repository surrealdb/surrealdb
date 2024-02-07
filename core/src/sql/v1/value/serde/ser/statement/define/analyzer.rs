use crate::err::Error;
use crate::sql::filter::Filter;
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::tokenizer::Tokenizer;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use crate::sql::Strand;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DefineAnalyzerStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineAnalyzerStatement, Error>;
	type SerializeTuple = Impossible<DefineAnalyzerStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineAnalyzerStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineAnalyzerStatement, Error>;
	type SerializeMap = Impossible<DefineAnalyzerStatement, Error>;
	type SerializeStruct = SerializeDefineAnalyzerStatement;
	type SerializeStructVariant = Impossible<DefineAnalyzerStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineAnalyzerStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineAnalyzerStatement::default())
	}
}

#[derive(Default)]
pub struct SerializeDefineAnalyzerStatement {
	name: Ident,
	tokenizers: Option<Vec<Tokenizer>>,
	filters: Option<Vec<Filter>>,
	comment: Option<Strand>,
}

impl serde::ser::SerializeStruct for SerializeDefineAnalyzerStatement {
	type Ok = DefineAnalyzerStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"tokenizers" => {
				self.tokenizers = value.serialize(ser::tokenizer::vec::opt::Serializer.wrap())?;
			}
			"filters" => {
				self.filters = value.serialize(ser::filter::vec::opt::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineAnalyzerStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineAnalyzerStatement {
			name: self.name,
			tokenizers: self.tokenizers,
			filters: self.filters,
			comment: self.comment,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = DefineAnalyzerStatement::default();
		let value: DefineAnalyzerStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
