use crate::err::Error;
use crate::iam::Auth;
use crate::sql::statements::live::MaybeSession;
use crate::sql::statements::LiveStatement;
use crate::sql::value::serde::ser;
use crate::sql::Cond;
use crate::sql::Fetchs;
use crate::sql::Fields;
use crate::sql::Uuid;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = LiveStatement;
	type Error = Error;

	type SerializeSeq = Impossible<LiveStatement, Error>;
	type SerializeTuple = Impossible<LiveStatement, Error>;
	type SerializeTupleStruct = Impossible<LiveStatement, Error>;
	type SerializeTupleVariant = Impossible<LiveStatement, Error>;
	type SerializeMap = Impossible<LiveStatement, Error>;
	type SerializeStruct = SerializeLiveStatement;
	type SerializeStructVariant = Impossible<LiveStatement, Error>;

	const EXPECTED: &'static str = "a struct `LiveStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeLiveStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeLiveStatement {
	id: Uuid,
	node: Uuid,
	expr: Fields,
	what: Value,
	cond: Option<Cond>,
	fetch: Option<Fetchs>,
	archived: Option<Uuid>,
	session: Option<Value>,
	auth: Option<Auth>,
}

impl serde::ser::SerializeStruct for SerializeLiveStatement {
	type Ok = LiveStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"id" => {
				self.id = Uuid(value.serialize(ser::uuid::Serializer.wrap())?);
			}
			"node" => {
				self.node = Uuid(value.serialize(ser::uuid::Serializer.wrap())?);
			}
			"expr" => {
				self.expr = value.serialize(ser::fields::Serializer.wrap())?;
			}
			"what" => {
				self.what = value.serialize(ser::value::Serializer.wrap())?;
			}
			"cond" => {
				self.cond = value.serialize(ser::cond::opt::Serializer.wrap())?;
			}
			"fetch" => {
				self.fetch = value.serialize(ser::fetchs::opt::Serializer.wrap())?;
			}
			"archived" => {
				self.archived = value.serialize(ser::uuid::opt::Serializer.wrap())?.map(Uuid);
			}
			"session" => {
				self.session = None;
			}
			"auth" => {
				self.auth = None;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `LiveStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(LiveStatement {
			id: self.id,
			node: self.node,
			expr: self.expr,
			what: self.what,
			cond: self.cond,
			fetch: self.fetch,
			archived: self.archived,
			session: None,
			auth: None,
			session_id: MaybeSession::default(),
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = LiveStatement::default();
		let value: LiveStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
