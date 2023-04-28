use crate::err::Error;
use crate::sql::statements::SelectStatement;
use crate::sql::value::serde::ser;
use crate::sql::Cond;
use crate::sql::Fetchs;
use crate::sql::Fields;
use crate::sql::Groups;
use crate::sql::Limit;
use crate::sql::Orders;
use crate::sql::Splits;
use crate::sql::Start;
use crate::sql::Timeout;
use crate::sql::Values;
use crate::sql::Version;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = SelectStatement;
	type Error = Error;

	type SerializeSeq = Impossible<SelectStatement, Error>;
	type SerializeTuple = Impossible<SelectStatement, Error>;
	type SerializeTupleStruct = Impossible<SelectStatement, Error>;
	type SerializeTupleVariant = Impossible<SelectStatement, Error>;
	type SerializeMap = Impossible<SelectStatement, Error>;
	type SerializeStruct = SerializeSelectStatement;
	type SerializeStructVariant = Impossible<SelectStatement, Error>;

	const EXPECTED: &'static str = "a struct `SelectStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeSelectStatement::default())
	}
}

#[derive(Default)]
pub struct SerializeSelectStatement {
	expr: Option<Fields>,
	what: Option<Values>,
	cond: Option<Cond>,
	split: Option<Splits>,
	group: Option<Groups>,
	order: Option<Orders>,
	limit: Option<Limit>,
	start: Option<Start>,
	fetch: Option<Fetchs>,
	version: Option<Version>,
	timeout: Option<Timeout>,
	parallel: Option<bool>,
}

impl serde::ser::SerializeStruct for SerializeSelectStatement {
	type Ok = SelectStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"expr" => {
				self.expr = Some(value.serialize(ser::fields::Serializer.wrap())?);
			}
			"what" => {
				self.what = Some(Values(value.serialize(ser::value::vec::Serializer.wrap())?));
			}
			"cond" => {
				self.cond = value.serialize(ser::cond::opt::Serializer.wrap())?;
			}
			"split" => {
				self.split = value.serialize(ser::split::vec::opt::Serializer.wrap())?.map(Splits);
			}
			"group" => {
				self.group = value.serialize(ser::group::vec::opt::Serializer.wrap())?.map(Groups);
			}
			"order" => {
				self.order = value.serialize(ser::order::vec::opt::Serializer.wrap())?.map(Orders);
			}
			"limit" => {
				self.limit = value.serialize(ser::limit::opt::Serializer.wrap())?;
			}
			"start" => {
				self.start = value.serialize(ser::start::opt::Serializer.wrap())?;
			}
			"fetch" => {
				self.fetch = value.serialize(ser::fetch::vec::opt::Serializer.wrap())?.map(Fetchs);
			}
			"version" => {
				self.version = value.serialize(ser::version::opt::Serializer.wrap())?;
			}
			"timeout" => {
				self.timeout = value.serialize(ser::timeout::opt::Serializer.wrap())?;
			}
			"parallel" => {
				self.parallel = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `SelectStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.expr, self.what, self.parallel) {
			(Some(expr), Some(what), Some(parallel)) => Ok(SelectStatement {
				expr,
				what,
				parallel,
				cond: self.cond,
				split: self.split,
				group: self.group,
				order: self.order,
				limit: self.limit,
				start: self.start,
				fetch: self.fetch,
				version: self.version,
				timeout: self.timeout,
			}),
			_ => Err(Error::custom("`SelectStatement` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = SelectStatement::default();
		let value: SelectStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_cond() {
		let stmt = SelectStatement {
			cond: Some(Default::default()),
			..Default::default()
		};
		let value: SelectStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_split() {
		let stmt = SelectStatement {
			split: Some(Default::default()),
			..Default::default()
		};
		let value: SelectStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_group() {
		let stmt = SelectStatement {
			group: Some(Default::default()),
			..Default::default()
		};
		let value: SelectStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_order() {
		let stmt = SelectStatement {
			order: Some(Default::default()),
			..Default::default()
		};
		let value: SelectStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_limit() {
		let stmt = SelectStatement {
			limit: Some(Default::default()),
			..Default::default()
		};
		let value: SelectStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_start() {
		let stmt = SelectStatement {
			start: Some(Default::default()),
			..Default::default()
		};
		let value: SelectStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_fetch() {
		let stmt = SelectStatement {
			fetch: Some(Default::default()),
			..Default::default()
		};
		let value: SelectStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_version() {
		let stmt = SelectStatement {
			version: Some(Default::default()),
			..Default::default()
		};
		let value: SelectStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_timeout() {
		let stmt = SelectStatement {
			timeout: Some(Default::default()),
			..Default::default()
		};
		let value: SelectStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
