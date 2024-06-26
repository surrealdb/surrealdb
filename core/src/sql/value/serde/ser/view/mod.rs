pub(super) mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Cond;
use crate::sql::Fields;
use crate::sql::Groups;
use crate::sql::Tables;
use crate::sql::View;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = View;
	type Error = Error;

	type SerializeSeq = Impossible<View, Error>;
	type SerializeTuple = Impossible<View, Error>;
	type SerializeTupleStruct = Impossible<View, Error>;
	type SerializeTupleVariant = Impossible<View, Error>;
	type SerializeMap = Impossible<View, Error>;
	type SerializeStruct = SerializeView;
	type SerializeStructVariant = Impossible<View, Error>;

	const EXPECTED: &'static str = "a struct `View`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeView::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeView {
	expr: Fields,
	what: Tables,
	cond: Option<Cond>,
	group: Option<Groups>,
}

impl serde::ser::SerializeStruct for SerializeView {
	type Ok = View;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"expr" => {
				self.expr = value.serialize(ser::fields::Serializer.wrap())?;
			}
			"what" => {
				self.what = Tables(value.serialize(ser::table::vec::Serializer.wrap())?);
			}
			"cond" => {
				self.cond = value.serialize(ser::cond::opt::Serializer.wrap())?;
			}
			"group" => {
				self.group = value.serialize(ser::group::vec::opt::Serializer.wrap())?.map(Groups);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `View::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(View {
			expr: self.expr,
			what: self.what,
			cond: self.cond,
			group: self.group,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = View::default();
		let value: View = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
