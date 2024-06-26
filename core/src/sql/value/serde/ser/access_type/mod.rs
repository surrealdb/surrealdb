use crate::err::Error;
use crate::sql::access_type::{
	AccessType, JwtAccess, JwtAccessIssue, JwtAccessVerify, JwtAccessVerifyJwks,
	JwtAccessVerifyKey, RecordAccess,
};
use crate::sql::value::serde::ser;
use crate::sql::Algorithm;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

// Serialize Access Method

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = AccessType;
	type Error = Error;

	type SerializeSeq = Impossible<AccessType, Error>;
	type SerializeTuple = Impossible<AccessType, Error>;
	type SerializeTupleStruct = Impossible<AccessType, Error>;
	type SerializeTupleVariant = Impossible<AccessType, Error>;
	type SerializeMap = Impossible<AccessType, Error>;
	type SerializeStruct = Impossible<AccessType, Error>;
	type SerializeStructVariant = Impossible<AccessType, Error>;

	const EXPECTED: &'static str = "a `AccessType`";

	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Record" => Ok(AccessType::Record(value.serialize(SerializerRecord.wrap())?)),
			"Jwt" => Ok(AccessType::Jwt(value.serialize(SerializerJwt.wrap())?)),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

// Serialize Record Access

pub struct SerializerRecord;

impl ser::Serializer for SerializerRecord {
	type Ok = RecordAccess;
	type Error = Error;

	type SerializeSeq = Impossible<RecordAccess, Error>;
	type SerializeTuple = Impossible<RecordAccess, Error>;
	type SerializeTupleStruct = Impossible<RecordAccess, Error>;
	type SerializeTupleVariant = Impossible<RecordAccess, Error>;
	type SerializeMap = Impossible<RecordAccess, Error>;
	type SerializeStruct = SerializeRecord;
	type SerializeStructVariant = Impossible<RecordAccess, Error>;

	const EXPECTED: &'static str = "a struct `RecordAccess`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRecord::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRecord {
	pub signup: Option<Value>,
	pub signin: Option<Value>,
	pub jwt: JwtAccess,
}

impl serde::ser::SerializeStruct for SerializeRecord {
	type Ok = RecordAccess;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"signup" => {
				self.signup = value.serialize(ser::value::opt::Serializer.wrap())?;
			}
			"signin" => {
				self.signin = value.serialize(ser::value::opt::Serializer.wrap())?;
			}
			"jwt" => {
				self.jwt = value.serialize(SerializerJwt.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `RecordAccess::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RecordAccess {
			signup: self.signup,
			signin: self.signin,
			jwt: self.jwt,
		})
	}
}

// Serialize JWT Access

pub struct SerializerJwt;

impl ser::Serializer for SerializerJwt {
	type Ok = JwtAccess;
	type Error = Error;

	type SerializeSeq = Impossible<JwtAccess, Error>;
	type SerializeTuple = Impossible<JwtAccess, Error>;
	type SerializeTupleStruct = Impossible<JwtAccess, Error>;
	type SerializeTupleVariant = Impossible<JwtAccess, Error>;
	type SerializeMap = Impossible<JwtAccess, Error>;
	type SerializeStruct = SerializeJwt;
	type SerializeStructVariant = Impossible<JwtAccess, Error>;

	const EXPECTED: &'static str = "a struct `JwtAccess`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeJwt::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeJwt {
	pub verify: JwtAccessVerify,
	pub issue: Option<JwtAccessIssue>,
}

impl serde::ser::SerializeStruct for SerializeJwt {
	type Ok = JwtAccess;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"verify" => {
				self.verify = value.serialize(SerializerJwtVerify.wrap())?;
			}
			"issue" => {
				self.issue = value.serialize(SerializerJwtIssueOpt.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `JwtAccess::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(JwtAccess {
			verify: self.verify,
			issue: self.issue,
		})
	}
}

// Serialize JWT Access Verify

pub struct SerializerJwtVerify;

impl ser::Serializer for SerializerJwtVerify {
	type Ok = JwtAccessVerify;
	type Error = Error;

	type SerializeSeq = Impossible<JwtAccessVerify, Error>;
	type SerializeTuple = Impossible<JwtAccessVerify, Error>;
	type SerializeTupleStruct = Impossible<JwtAccessVerify, Error>;
	type SerializeTupleVariant = Impossible<JwtAccessVerify, Error>;
	type SerializeMap = Impossible<JwtAccessVerify, Error>;
	type SerializeStruct = Impossible<JwtAccessVerify, Error>;
	type SerializeStructVariant = Impossible<JwtAccessVerify, Error>;

	const EXPECTED: &'static str = "a `JwtAccessVerify`";

	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Key" => Ok(JwtAccessVerify::Key(value.serialize(SerializerJwtVerifyKey.wrap())?)),
			"Jwks" => Ok(JwtAccessVerify::Jwks(value.serialize(SerializerJwtVerifyJwks.wrap())?)),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

// Serialize JWT Access Verify Key

pub struct SerializerJwtVerifyKey;

impl ser::Serializer for SerializerJwtVerifyKey {
	type Ok = JwtAccessVerifyKey;
	type Error = Error;

	type SerializeSeq = Impossible<JwtAccessVerifyKey, Error>;
	type SerializeTuple = Impossible<JwtAccessVerifyKey, Error>;
	type SerializeTupleStruct = Impossible<JwtAccessVerifyKey, Error>;
	type SerializeTupleVariant = Impossible<JwtAccessVerifyKey, Error>;
	type SerializeMap = Impossible<JwtAccessVerifyKey, Error>;
	type SerializeStruct = SerializeJwtVerifyKey;
	type SerializeStructVariant = Impossible<JwtAccessVerifyKey, Error>;

	const EXPECTED: &'static str = "a struct `JwtAccessVerifyKey`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeJwtVerifyKey::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeJwtVerifyKey {
	pub alg: Algorithm,
	pub key: String,
}

impl serde::ser::SerializeStruct for SerializeJwtVerifyKey {
	type Ok = JwtAccessVerifyKey;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"alg" => {
				self.alg = value.serialize(ser::algorithm::Serializer.wrap())?;
			}
			"key" => {
				self.key = value.serialize(ser::string::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `JwtAccessVerifyKey::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(JwtAccessVerifyKey {
			alg: self.alg,
			key: self.key,
		})
	}
}

// Serialize JWT Access Verify JWKS

pub struct SerializerJwtVerifyJwks;

impl ser::Serializer for SerializerJwtVerifyJwks {
	type Ok = JwtAccessVerifyJwks;
	type Error = Error;

	type SerializeSeq = Impossible<JwtAccessVerifyJwks, Error>;
	type SerializeTuple = Impossible<JwtAccessVerifyJwks, Error>;
	type SerializeTupleStruct = Impossible<JwtAccessVerifyJwks, Error>;
	type SerializeTupleVariant = Impossible<JwtAccessVerifyJwks, Error>;
	type SerializeMap = Impossible<JwtAccessVerifyJwks, Error>;
	type SerializeStruct = SerializeJwtVerifyJwks;
	type SerializeStructVariant = Impossible<JwtAccessVerifyJwks, Error>;

	const EXPECTED: &'static str = "a struct `JwtAccessVerifyJwks`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeJwtVerifyJwks::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeJwtVerifyJwks {
	pub url: String,
}

impl serde::ser::SerializeStruct for SerializeJwtVerifyJwks {
	type Ok = JwtAccessVerifyJwks;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"url" => {
				self.url = value.serialize(ser::string::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `JwtAccessVerifyJwks::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(JwtAccessVerifyJwks {
			url: self.url,
		})
	}
}

// Serialize JWT Access Issue

pub struct SerializerJwtIssueOpt;

impl ser::Serializer for SerializerJwtIssueOpt {
	type Ok = Option<JwtAccessIssue>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<JwtAccessIssue>, Error>;
	type SerializeTuple = Impossible<Option<JwtAccessIssue>, Error>;
	type SerializeTupleStruct = Impossible<Option<JwtAccessIssue>, Error>;
	type SerializeTupleVariant = Impossible<Option<JwtAccessIssue>, Error>;
	type SerializeMap = Impossible<Option<JwtAccessIssue>, Error>;
	type SerializeStruct = Impossible<Option<JwtAccessIssue>, Error>;
	type SerializeStructVariant = Impossible<Option<JwtAccessIssue>, Error>;

	const EXPECTED: &'static str = "an `Option<JwtAccessIssue>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(value.serialize(SerializerJwtIssue.wrap())?))
	}
}

pub struct SerializerJwtIssue;

impl ser::Serializer for SerializerJwtIssue {
	type Ok = JwtAccessIssue;
	type Error = Error;

	type SerializeSeq = Impossible<JwtAccessIssue, Error>;
	type SerializeTuple = Impossible<JwtAccessIssue, Error>;
	type SerializeTupleStruct = Impossible<JwtAccessIssue, Error>;
	type SerializeTupleVariant = Impossible<JwtAccessIssue, Error>;
	type SerializeMap = Impossible<JwtAccessIssue, Error>;
	type SerializeStruct = SerializeJwtIssue;
	type SerializeStructVariant = Impossible<JwtAccessIssue, Error>;

	const EXPECTED: &'static str = "a struct `JwtAccessIssue`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeJwtIssue::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeJwtIssue {
	pub alg: Algorithm,
	pub key: String,
}

impl serde::ser::SerializeStruct for SerializeJwtIssue {
	type Ok = JwtAccessIssue;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"alg" => {
				self.alg = value.serialize(ser::algorithm::Serializer.wrap())?;
			}
			"key" => {
				self.key = value.serialize(ser::string::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `JwtAccessIssue::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(JwtAccessIssue {
			alg: self.alg,
			key: self.key,
		})
	}
}
