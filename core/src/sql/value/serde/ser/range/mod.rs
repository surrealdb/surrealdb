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
