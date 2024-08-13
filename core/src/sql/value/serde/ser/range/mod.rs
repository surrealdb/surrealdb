mod bound;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Id;
use crate::sql::Range;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Serialize;
use std::ops::Bound;

#[derive(Default)]
pub(super) struct SerializeRange {
	tb: Option<String>,
	beg: Option<Bound<Id>>,
	end: Option<Bound<Id>>,
}

impl serde::ser::SerializeStruct for SerializeRange {
	type Ok = Range;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"tb" => {
				self.tb = Some(value.serialize(ser::string::Serializer.wrap())?);
			}
			"beg" => {
				self.beg = Some(value.serialize(bound::Serializer.wrap())?);
			}
			"end" => {
				self.end = Some(value.serialize(bound::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Range::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.tb, self.beg, self.end) {
			(Some(tb), Some(beg), Some(end)) => Ok(Range {
				tb,
				beg,
				end,
			}),
			_ => Err(Error::custom("`Range` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde::ser::Impossible;
	use serde::Serialize;

	pub(super) struct Serializer;

	impl ser::Serializer for Serializer {
		type Ok = Range;
		type Error = Error;

		type SerializeSeq = Impossible<Range, Error>;
		type SerializeTuple = Impossible<Range, Error>;
		type SerializeTupleStruct = Impossible<Range, Error>;
		type SerializeTupleVariant = Impossible<Range, Error>;
		type SerializeMap = Impossible<Range, Error>;
		type SerializeStruct = SerializeRange;
		type SerializeStructVariant = Impossible<Range, Error>;

		const EXPECTED: &'static str = "a struct `Range`";

		#[inline]
		fn serialize_struct(
			self,
			_name: &'static str,
			_len: usize,
		) -> Result<Self::SerializeStruct, Error> {
			Ok(SerializeRange::default())
		}
	}

	#[test]
	fn range() {
		let range = Range {
			tb: "foobar".to_owned(),
			beg: Bound::Included("bar".into()),
			end: Bound::Excluded("foo".into()),
		};
		let serialized = range.serialize(Serializer.wrap()).unwrap();
		assert_eq!(range, serialized);
	}
}
