use std::collections::BTreeMap;

use anyhow::Context;
use std::ops::Bound;

use crate::iam::AccessMethod;
use crate::iam::SigninParams;
use crate::iam::SignupParams;

use super::types::{
	V1Array, V1Bytes, V1Datetime, V1Duration, V1File, V1Gen, V1Geometry, V1Id, V1IdRange, V1Number,
	V1Object, V1RecordId, V1Strand, V1Uuid, V1Value,
};
use crate::expr::id::Gen as ExprGen;
use crate::expr::{
	Array as ExprArray, Bytes as ExprBytes, Datetime as ExprDatetime, Duration as ExprDuration,
	File as ExprFile, Geometry as ExprGeometry, Id as ExprId, IdRange as ExprIdRange,
	Number as ExprNumber, Object as ExprObject, Strand as ExprStrand, Thing as ExprRecordId,
	Uuid as ExprUuid, Value as ExprValue,
};
use crate::sql::id::Gen as SqlGen;
use crate::sql::{
	Array as SqlArray, Bytes as SqlBytes, Datetime as SqlDatetime, Duration as SqlDuration,
	File as SqlFile, Geometry as SqlGeometry, Id as SqlId, IdRange as SqlIdRange,
	Number as SqlNumber, Object as SqlObject, SqlValue, Strand as SqlStrand, Thing as SqlRecordId,
	Uuid as SqlUuid,
};

impl TryFrom<V1Object> for SignupParams {
	type Error = anyhow::Error;

	fn try_from(obj: V1Object) -> Result<Self, Self::Error> {
		let namespace = obj
			.get("NS")
			.or_else(|| obj.get("ns"))
			.context("namespace is required")?
			.to_raw_string();
		let database = obj
			.get("DB")
			.or_else(|| obj.get("db"))
			.context("database is required")?
			.to_raw_string();
		let access_name = obj
			.get("AC")
			.or_else(|| obj.get("ac"))
			.context("access_name is required")?
			.to_raw_string();
		let variables = obj.0;

		Ok(Self {
			namespace,
			database,
			access_name,
			variables: variables.try_into()?,
		})
	}
}

impl TryFrom<V1Value> for SignupParams {
	type Error = anyhow::Error;

	fn try_from(value: V1Value) -> Result<Self, Self::Error> {
		let V1Value::Object(obj) = value else {
			return Err(anyhow::anyhow!("value is not an object"));
		};

		Self::try_from(obj)
	}
}

impl TryFrom<V1Object> for SigninParams {
	type Error = anyhow::Error;

	fn try_from(obj: V1Object) -> Result<Self, Self::Error> {
		// Parse the specified variables
		let ns = obj.get("NS").or_else(|| obj.get("ns"));
		let db = obj.get("DB").or_else(|| obj.get("db"));
		let ac = obj.get("AC").or_else(|| obj.get("ac"));

		// Check if the parameters exist
		match (ns, db, ac) {
			// DB signin with access method
			(Some(ns), Some(db), Some(ac)) => {
				// Process the provided values
				let namespace = ns.to_raw_string();
				let database = db.to_raw_string();
				let access_name = ac.to_raw_string();
				let key = obj.get("key").context("key is required")?.to_raw_string();
				let refresh_token = obj.get("refresh_token").map(|v| v.to_raw_string());

				Ok(SigninParams {
					access_method: AccessMethod::DatabaseAccess {
						namespace,
						database,
						access_name,
						key,
						refresh_token,
					},
				})
			}
			// DB signin with user credentials
			(Some(ns), Some(db), None) => {
				// Get the provided user and pass
				let user = obj.get("user");
				let pass = obj.get("pass");
				// Validate the user and pass
				match (user, pass) {
					// There is a username and password
					(Some(user), Some(pass)) => {
						// Process the provided values
						let namespace = ns.to_raw_string();
						let database = db.to_raw_string();
						let username = user.to_raw_string();
						let password = pass.to_raw_string();

						Ok(SigninParams {
							access_method: AccessMethod::DatabaseUser {
								namespace,
								database,
								username,
								password,
							},
						})
					}
					_ => Err(anyhow::Error::new(crate::err::Error::MissingUserOrPass)),
				}
			}
			// NS signin with access method
			(Some(ns), None, Some(ac)) => {
				// Process the provided values
				let namespace = ns.to_raw_string();
				let access_name = ac.to_raw_string();
				let key = obj.get("key").context("key is required")?.to_raw_string();

				Ok(SigninParams {
					access_method: AccessMethod::NamespaceAccess {
						namespace,
						access_name,
						key,
					},
				})
			}
			// NS signin with user credentials
			(Some(ns), None, None) => {
				// Get the provided user and pass
				let user = obj.get("user");
				let pass = obj.get("pass");
				// Validate the user and pass
				match (user, pass) {
					// There is a username and password
					(Some(user), Some(pass)) => {
						// Process the provided values
						let namespace = ns.to_raw_string();
						let username = user.to_raw_string();
						let password = pass.to_raw_string();
						// Attempt to signin to namespace
						Ok(SigninParams {
							access_method: AccessMethod::NamespaceUser {
								namespace,
								username,
								password,
							},
						})
					}
					_ => Err(anyhow::Error::new(crate::err::Error::MissingUserOrPass)),
				}
			}
			// ROOT signin with user credentials
			(None, None, None) => {
				// Get the provided user and pass
				let user = obj.get("user");
				let pass = obj.get("pass");
				// Validate the user and pass
				match (user, pass) {
					// There is a username and password
					(Some(user), Some(pass)) => {
						// Process the provided values
						let username = user.to_raw_string();
						let password = pass.to_raw_string();
						// Attempt to signin to root
						Ok(SigninParams {
							access_method: AccessMethod::RootUser {
								username,
								password,
							},
						})
					}
					_ => Err(anyhow::Error::new(crate::err::Error::MissingUserOrPass)),
				}
			}
			_ => Err(anyhow::Error::new(crate::err::Error::NoSigninTarget)),
		}
	}
}

impl TryFrom<V1Value> for SigninParams {
	type Error = anyhow::Error;

	fn try_from(value: V1Value) -> Result<Self, Self::Error> {
		let V1Value::Object(obj) = value else {
			return Err(anyhow::anyhow!("value is not an object"));
		};

		Self::try_from(obj)
	}
}

impl<T> From<Option<T>> for V1Value
where
	V1Value: From<T>,
{
	fn from(value: Option<T>) -> Self {
		if let Some(x) = value {
			V1Value::from(x)
		} else {
			V1Value::None
		}
	}
}

impl TryFrom<Option<&crate::expr::Value>> for V1Value {
	type Error = anyhow::Error;

	fn try_from(value: Option<&crate::expr::Value>) -> Result<Self, Self::Error> {
		let Some(x) = value else {
			return Ok(Self::None);
		};

		Self::try_from(x.clone())
	}
}

macro_rules! impl_from_value {
	($value:ident) => {
		impl TryFrom<$value> for V1Value {
			type Error = anyhow::Error;

			fn try_from(expr_value: $value) -> Result<Self, Self::Error> {
				match expr_value {
					$value::None => Ok(V1Value::None),
					$value::Null => Ok(V1Value::Null),
					$value::Bool(bool) => Ok(V1Value::Bool(bool)),
					$value::Number(number) => Ok(V1Value::Number(number.into())),
					$value::Strand(strand) => Ok(V1Value::Strand(strand.into())),
					$value::Duration(duration) => Ok(V1Value::Duration(duration.into())),
					$value::Datetime(datetime) => Ok(V1Value::Datetime(datetime.into())),
					$value::Uuid(uuid) => Ok(V1Value::Uuid(uuid.into())),
					$value::Array(array) => Ok(V1Value::Array(array.try_into()?)),
					$value::Object(object) => Ok(V1Value::Object(object.try_into()?)),
					$value::Geometry(geometry) => Ok(V1Value::Geometry(geometry.into())),
					$value::Bytes(bytes) => Ok(V1Value::Bytes(bytes.into())),
					$value::Thing(thing) => Ok(V1Value::RecordId(thing.try_into()?)),
					$value::File(file) => Ok(V1Value::File(file.into())),
					unexpected => Err(anyhow::Error::msg(format!(
						"Attempted to convert unexpected expr::value to v1::value: {:?}",
						unexpected
					))),
				}
			}
		}

		impl TryFrom<V1Value> for $value {
			type Error = anyhow::Error;

			fn try_from(value: V1Value) -> Result<Self, Self::Error> {
				match value {
					V1Value::None => Ok($value::None),
					V1Value::Null => Ok($value::Null),
					V1Value::Bool(bool) => Ok($value::Bool(bool)),
					V1Value::Number(number) => Ok($value::Number(number.into())),
					V1Value::Strand(strand) => Ok($value::Strand(strand.into())),
					V1Value::Duration(duration) => Ok($value::Duration(duration.into())),
					V1Value::Datetime(datetime) => Ok($value::Datetime(datetime.into())),
					V1Value::Uuid(uuid) => Ok($value::Uuid(uuid.into())),
					V1Value::Array(array) => Ok($value::Array(array.try_into()?)),
					V1Value::Object(object) => Ok($value::Object(object.try_into()?)),
					V1Value::Geometry(geometry) => Ok($value::Geometry(geometry.into())),
					V1Value::Bytes(bytes) => Ok($value::Bytes(bytes.into())),
					V1Value::RecordId(thing) => Ok($value::Thing(thing.try_into()?)),
					V1Value::File(file) => Ok($value::File(file.into())),
					unexpected => Err(anyhow::Error::msg(format!(
						"Attempted to convert unexpected v1::value to expr::value: {:?}",
						unexpected
					))),
				}
			}
		}
	};
}

macro_rules! impl_from_number {
	($value:ident) => {
		impl From<$value> for V1Number {
			fn from(value: $value) -> Self {
				match value {
					$value::Int(int) => V1Number::Int(int),
					$value::Float(float) => V1Number::Float(float),
					$value::Decimal(decimal) => V1Number::Decimal(decimal),
				}
			}
		}

		impl From<V1Number> for $value {
			fn from(value: V1Number) -> Self {
				match value {
					V1Number::Int(int) => $value::Int(int),
					V1Number::Float(float) => $value::Float(float),
					V1Number::Decimal(decimal) => $value::Decimal(decimal),
				}
			}
		}
	};
}

macro_rules! impl_from_uuid {
	($value:ident) => {
		impl From<$value> for V1Uuid {
			fn from(uuid: $value) -> Self {
				V1Uuid(uuid.0)
			}
		}

		impl From<V1Uuid> for $value {
			fn from(uuid: V1Uuid) -> Self {
				Self(uuid.0)
			}
		}
	};
}

macro_rules! impl_from_bytes {
	($value:ident) => {
		impl From<$value> for V1Bytes {
			fn from(bytes: $value) -> Self {
				V1Bytes(bytes.0)
			}
		}

		impl From<V1Bytes> for $value {
			fn from(bytes: V1Bytes) -> Self {
				Self(bytes.0)
			}
		}
	};
}

macro_rules! impl_from_record_id {
	($value:ident) => {
		impl TryFrom<$value> for V1RecordId {
			type Error = anyhow::Error;

			fn try_from(record_id: $value) -> Result<Self, Self::Error> {
				Ok(Self {
					tb: record_id.tb,
					id: record_id.id.try_into()?,
				})
			}
		}

		impl TryFrom<V1RecordId> for $value {
			type Error = anyhow::Error;

			fn try_from(record_id: V1RecordId) -> Result<Self, Self::Error> {
				Ok(Self {
					tb: record_id.tb,
					id: record_id.id.try_into()?,
				})
			}
		}
	};
}

macro_rules! impl_from_duration {
	($value:ident) => {
		impl From<$value> for V1Duration {
			fn from(duration: $value) -> Self {
				V1Duration(duration.0)
			}
		}

		impl From<V1Duration> for $value {
			fn from(duration: V1Duration) -> Self {
				Self(duration.0)
			}
		}
	};
}

macro_rules! impl_from_datetime {
	($value:ident) => {
		impl From<$value> for V1Datetime {
			fn from(datetime: $value) -> Self {
				V1Datetime(datetime.0)
			}
		}

		impl From<V1Datetime> for $value {
			fn from(datetime: V1Datetime) -> Self {
				Self(datetime.0)
			}
		}
	};
}

macro_rules! impl_from_strand {
	($value:ident) => {
		impl From<$value> for V1Strand {
			fn from(strand: $value) -> Self {
				V1Strand(strand.0)
			}
		}

		impl From<V1Strand> for $value {
			fn from(strand: V1Strand) -> Self {
				Self(strand.0)
			}
		}
	};
}

macro_rules! impl_from_array {
	($value:ident) => {
		impl TryFrom<$value> for V1Array {
			type Error = anyhow::Error;

			fn try_from(array: $value) -> Result<Self, Self::Error> {
				let mut v1_array = Vec::new();
				for item in array.0 {
					v1_array.push(item.try_into()?);
				}
				Ok(Self(v1_array))
			}
		}

		impl TryFrom<V1Array> for $value {
			type Error = anyhow::Error;

			fn try_from(array: V1Array) -> Result<Self, Self::Error> {
				let mut out_array = Vec::new();
				for item in array.0 {
					out_array.push(item.try_into()?);
				}
				Ok(Self(out_array))
			}
		}
	};
}

macro_rules! impl_from_object {
	($value:ident) => {
		impl TryFrom<$value> for V1Object {
			type Error = anyhow::Error;

			fn try_from(object: $value) -> Result<Self, Self::Error> {
				let mut v1_object = BTreeMap::new();
				for (key, value) in object.0 {
					v1_object.insert(key, value.try_into()?);
				}
				Ok(Self(v1_object))
			}
		}

		impl TryFrom<V1Object> for $value {
			type Error = anyhow::Error;

			fn try_from(object: V1Object) -> Result<Self, Self::Error> {
				let mut out_object = BTreeMap::new();
				for (key, value) in object.0 {
					out_object.insert(key, value.try_into()?);
				}
				Ok(Self(out_object))
			}
		}
	};
}

macro_rules! impl_from_geometry {
	($value:ident) => {
		impl From<$value> for V1Geometry {
			fn from(geometry: $value) -> Self {
				match geometry {
					$value::Point(point) => V1Geometry::Point(point),
					$value::Line(line) => V1Geometry::Line(line),
					$value::Polygon(polygon) => V1Geometry::Polygon(polygon),
					$value::MultiPoint(multi_point) => V1Geometry::MultiPoint(multi_point),
					$value::MultiLine(multi_line) => V1Geometry::MultiLine(multi_line),
					$value::MultiPolygon(multi_polygon) => V1Geometry::MultiPolygon(multi_polygon),
					$value::Collection(collection) => {
						V1Geometry::Collection(collection.into_iter().map(Into::into).collect())
					}
				}
			}
		}

		impl From<V1Geometry> for $value {
			fn from(geometry: V1Geometry) -> Self {
				match geometry {
					V1Geometry::Point(point) => $value::Point(point),
					V1Geometry::Line(line) => $value::Line(line),
					V1Geometry::Polygon(polygon) => $value::Polygon(polygon),
					V1Geometry::MultiPoint(multi_point) => $value::MultiPoint(multi_point),
					V1Geometry::MultiLine(multi_line) => $value::MultiLine(multi_line),
					V1Geometry::MultiPolygon(multi_polygon) => $value::MultiPolygon(multi_polygon),
					V1Geometry::Collection(collection) => {
						$value::Collection(collection.into_iter().map(Into::into).collect())
					}
				}
			}
		}
	};
}

macro_rules! impl_from_id {
	($value:ident) => {
		impl TryFrom<$value> for V1Id {
			type Error = anyhow::Error;

			fn try_from(id: $value) -> Result<Self, Self::Error> {
				match id {
					$value::Number(number) => Ok(V1Id::Number(number)),
					$value::String(string) => Ok(V1Id::String(string)),
					$value::Uuid(uuid) => Ok(V1Id::Uuid(uuid.into())),
					$value::Array(array) => Ok(V1Id::Array(array.try_into()?)),
					$value::Object(object) => Ok(V1Id::Object(object.try_into()?)),
					$value::Generate(g) => Ok(V1Id::Generate(g.into())),
					$value::Range(range) => {
						Ok(V1Id::Range(Box::new(range.as_ref().clone().try_into()?)))
					}
				}
			}
		}

		impl TryFrom<V1Id> for $value {
			type Error = anyhow::Error;

			fn try_from(id: V1Id) -> Result<Self, Self::Error> {
				match id {
					V1Id::Number(number) => Ok($value::Number(number)),
					V1Id::String(string) => Ok($value::String(string)),
					V1Id::Uuid(uuid) => Ok($value::Uuid(uuid.into())),
					V1Id::Array(array) => Ok($value::Array(array.try_into()?)),
					V1Id::Object(object) => Ok($value::Object(object.try_into()?)),
					V1Id::Generate(g) => Ok($value::Generate(g.into())),
					V1Id::Range(range) => {
						Ok($value::Range(Box::new(range.as_ref().clone().try_into()?)))
					}
				}
			}
		}
	};
}

macro_rules! convert_bound {
	($value:expr) => {
		match $value {
			Bound::Included(id) => Bound::Included(id.try_into()?),
			Bound::Excluded(id) => Bound::Excluded(id.try_into()?),
			Bound::Unbounded => Bound::Unbounded,
		}
	};
}

macro_rules! impl_from_id_range {
	($value:ident) => {
		impl TryFrom<$value> for V1IdRange {
			type Error = anyhow::Error;

			fn try_from(range: $value) -> Result<Self, Self::Error> {
				Ok(V1IdRange {
					beg: convert_bound!(range.beg),
					end: convert_bound!(range.end),
				})
			}
		}

		impl TryFrom<V1IdRange> for $value {
			type Error = anyhow::Error;

			fn try_from(range: V1IdRange) -> Result<Self, Self::Error> {
				Ok(Self {
					beg: convert_bound!(range.beg),
					end: convert_bound!(range.end),
				})
			}
		}
	};
}

macro_rules! impl_from_gen {
	($value:ident) => {
		impl From<$value> for V1Gen {
			fn from(g: $value) -> Self {
				match g {
					$value::Rand => V1Gen::Rand,
					$value::Ulid => V1Gen::Ulid,
					$value::Uuid => V1Gen::Uuid,
				}
			}
		}

		impl From<V1Gen> for $value {
			fn from(g: V1Gen) -> Self {
				match g {
					V1Gen::Rand => $value::Rand,
					V1Gen::Ulid => $value::Ulid,
					V1Gen::Uuid => $value::Uuid,
				}
			}
		}
	};
}

macro_rules! impl_from_file {
	($value:ident) => {
		impl From<$value> for V1File {
			fn from(file: $value) -> Self {
				Self {
					bucket: file.bucket,
					key: file.key,
				}
			}
		}

		impl From<V1File> for $value {
			fn from(file: V1File) -> Self {
				Self {
					bucket: file.bucket,
					key: file.key,
				}
			}
		}
	};
}

impl_from_value!(ExprValue);
impl_from_value!(SqlValue);

impl_from_number!(ExprNumber);
impl_from_number!(SqlNumber);

impl_from_uuid!(ExprUuid);
impl_from_uuid!(SqlUuid);

impl_from_id!(ExprId);
impl_from_id!(SqlId);

impl_from_id_range!(ExprIdRange);
impl_from_id_range!(SqlIdRange);

impl_from_gen!(ExprGen);
impl_from_gen!(SqlGen);

impl_from_record_id!(ExprRecordId);
impl_from_record_id!(SqlRecordId);

impl_from_duration!(ExprDuration);
impl_from_duration!(SqlDuration);

impl_from_datetime!(ExprDatetime);
impl_from_datetime!(SqlDatetime);

impl_from_strand!(ExprStrand);
impl_from_strand!(SqlStrand);

impl_from_array!(ExprArray);
impl_from_array!(SqlArray);

impl_from_object!(ExprObject);
impl_from_object!(SqlObject);

impl_from_geometry!(ExprGeometry);
impl_from_geometry!(SqlGeometry);

impl_from_bytes!(ExprBytes);
impl_from_bytes!(SqlBytes);

impl_from_file!(ExprFile);
impl_from_file!(SqlFile);

impl TryFrom<BTreeMap<String, V1Value>> for crate::dbs::Variables {
	type Error = anyhow::Error;

	fn try_from(variables: BTreeMap<String, V1Value>) -> Result<Self, Self::Error> {
		let mut vars = Self::default();
		for (key, value) in variables {
			vars.insert(key, value.try_into()?);
		}
		Ok(vars)
	}
}
