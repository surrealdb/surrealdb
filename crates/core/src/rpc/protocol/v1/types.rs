use anyhow::Context;
use revision::revisioned;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

use crate::sql::number::decimal::DecimalExt;
use chrono::{DateTime, SecondsFormat, Utc};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::time;

use crate::dbs::QueryResult;
use std::fmt::{self, Display, Formatter, Write};

#[revisioned(revision = 1)]
#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum QueryType {
	// Any kind of query
	#[default]
	Other,
	// Indicates that the response live query id must be tracked
	Live,
	// Indicates that the live query should be removed from tracking
	Kill,
}

impl QueryType {
	fn is_other(&self) -> bool {
		matches!(self, Self::Other)
	}
}

impl From<&crate::sql::statement::Statement> for QueryType {
	fn from(stmt: &crate::sql::statement::Statement) -> Self {
		match stmt {
			crate::sql::statement::Statement::Live(_) => QueryType::Live,
			crate::sql::statement::Statement::Kill(_) => QueryType::Kill,
			_ => QueryType::Other,
		}
	}
}

/// The return value when running a query set on the database.
///
/// This is the same as `dbs::Response` in 2.x.
#[derive(Debug)]
#[non_exhaustive]
pub struct V1QueryResponse {
	pub time: V1Duration,
	pub result: anyhow::Result<V1Value>,
	// Record the query type in case processing the response is necessary (such as tracking live queries).
	pub query_type: QueryType,
}

impl V1QueryResponse {
	/// Return the transaction duration as a string
	pub fn speed(&self) -> String {
		format!("{:?}", self.time)
	}

	/// Retrieve the response as a normal result
	pub fn output(self) -> anyhow::Result<V1Value> {
		self.result
	}

	pub fn from_query_result(
		QueryResult {
			stats,
			values,
		}: QueryResult,
		query_type: QueryType,
	) -> anyhow::Result<Self> {
		let values = values.context("Query result is empty")?;
		let values =
			values.into_iter().map(|v| V1Value::try_from(v)).collect::<Result<Vec<_>, _>>()?;
		Ok(Self {
			time: V1Duration(stats.execution_duration),
			result: Ok(V1Value::Array(V1Array(values))),
			query_type,
		})
	}
}

impl Serialize for V1QueryResponse {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		use serde::ser::SerializeStruct;

		let includes_type = !self.query_type.is_other();
		let mut val = serializer.serialize_struct(
			"$surrealdb::private::sql::Response",
			if includes_type {
				3
			} else {
				4
			},
		)?;

		val.serialize_field("time", self.speed().as_str())?;
		if includes_type {
			val.serialize_field("type", &self.query_type)?;
		}

		match &self.result {
			Ok(v) => {
				val.serialize_field("status", &Status::Ok)?;
				val.serialize_field("result", v)?;
			}
			Err(e) => {
				val.serialize_field("status", &Status::Err)?;
				val.serialize_field("result", &V1Value::from(e.to_string()))?;
			}
		}
		val.end()
	}
}

impl revision::Revisioned for V1QueryResponse {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		QueryMethodResponse::from(self).serialize_revisioned(writer)
	}

	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(_reader: &mut R) -> Result<Self, revision::Error> {
		unreachable!("deserialising `Response` directly is not supported")
	}

	fn revision() -> u16 {
		1
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueryMethodResponse {
	pub time: String,
	pub status: Status,
	pub result: V1Value,
}

impl From<&V1QueryResponse> for QueryMethodResponse {
	fn from(res: &V1QueryResponse) -> Self {
		let time = res.speed();
		let (status, result) = match &res.result {
			Ok(value) => (Status::Ok, value.clone()),
			Err(error) => (Status::Err, V1Value::from(error.to_string())),
		};
		Self {
			status,
			result,
			time,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[non_exhaustive]
pub enum Status {
	Ok,
	Err,
}

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Value")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum V1Value {
	#[default]
	None,
	Null,
	Bool(bool),
	Number(V1Number),
	Strand(V1Strand),
	Duration(V1Duration),
	Datetime(V1Datetime),
	Uuid(V1Uuid),
	Array(V1Array),
	Object(V1Object),
	Geometry(V1Geometry),
	Bytes(V1Bytes),
	RecordId(V1RecordId),
	Model(Box<V1Model>),
	File(V1File),
	Table(V1Table),
	// These Value types are un-computed values
	// and are not used in query responses sent
	// to the client. These types need to be
	// computed, in order to convert them into
	// one of the simple types listed above.
	// These types are first computed into a
	// simple type before being used in indexes.
	// Param(Param),
	// Idiom(Idiom),
	// Table(Table),
	// Mock(Mock),
	// Regex(Regex),
	// Cast(Box<Cast>),
	// Block(Box<Block>),
	// #[revision(end = 2, convert_fn = "convert_old_range", fields_name = "OldValueRangeFields")]
	// Range(OldRange),
	// #[revision(start = 2)]
	// Range(Box<Range>),
	// Edges(Box<Edges>),
	// Future(Box<Future>),
	// Constant(Constant),
	// Function(Box<Function>),
	// Subquery(Box<Subquery>),
	// Expression(Box<Expression>),
	// Query(Query),
	// Model(Box<Model>),
	// Closure(Box<Closure>),
	// Refs(Refs),
	// File(File),
	// Add new variants here
}

impl V1Value {
	/// Converts this Value into an unquoted String
	pub fn to_raw_string(&self) -> String {
		match self {
			V1Value::Strand(v) => v.0.clone(),
			V1Value::Uuid(v) => v.to_raw(),
			V1Value::Datetime(v) => v.to_raw(),
			_ => self.to_string(),
		}
	}

	/// Converts this Value into an unquoted String
	pub fn as_string(self) -> String {
		match self {
			V1Value::Strand(v) => v.0,
			V1Value::Uuid(v) => v.to_raw(),
			V1Value::Datetime(v) => v.to_raw(),
			_ => self.to_string(),
		}
	}

	pub fn get_field_value(&self, name: &str) -> V1Value {
		match self {
			V1Value::Object(v) => v.get(name).cloned().unwrap_or(V1Value::None),
			_ => V1Value::None,
		}
	}

	/// Check if this Value is TRUE or 'true'
	pub fn is_true(&self) -> bool {
		matches!(self, V1Value::Bool(true))
	}

	pub fn into_json(self) -> serde_json::Value {
		self.into()
	}
}

impl From<String> for V1Value {
	fn from(v: String) -> Self {
		Self::Strand(V1Strand::from(v))
	}
}

impl From<&str> for V1Value {
	fn from(v: &str) -> Self {
		Self::Strand(V1Strand::from(v.to_string()))
	}
}

impl From<V1Strand> for V1Value {
	fn from(v: V1Strand) -> Self {
		Self::Strand(v)
	}
}

impl From<bool> for V1Value {
	fn from(v: bool) -> Self {
		Self::Bool(v)
	}
}

impl From<Decimal> for V1Value {
	fn from(v: Decimal) -> Self {
		Self::Number(V1Number::Decimal(v))
	}
}

impl From<time::Duration> for V1Value {
	fn from(v: time::Duration) -> Self {
		Self::Duration(V1Duration(v))
	}
}

impl From<V1Duration> for V1Value {
	fn from(v: V1Duration) -> Self {
		Self::Duration(v)
	}
}

impl From<DateTime<Utc>> for V1Value {
	fn from(v: DateTime<Utc>) -> Self {
		Self::Datetime(V1Datetime(v))
	}
}

impl From<V1Datetime> for V1Value {
	fn from(v: V1Datetime) -> Self {
		Self::Datetime(v)
	}
}

impl From<uuid::Uuid> for V1Value {
	fn from(v: uuid::Uuid) -> Self {
		Self::Uuid(V1Uuid(v))
	}
}

impl From<V1Uuid> for V1Value {
	fn from(v: V1Uuid) -> Self {
		Self::Uuid(v)
	}
}

impl From<V1Number> for V1Value {
	fn from(v: V1Number) -> Self {
		Self::Number(v)
	}
}

impl From<V1Array> for V1Value {
	fn from(v: V1Array) -> Self {
		Self::Array(v)
	}
}

impl From<V1Object> for V1Value {
	fn from(v: V1Object) -> Self {
		Self::Object(v)
	}
}

impl From<BTreeMap<String, V1Value>> for V1Value {
	fn from(v: BTreeMap<String, V1Value>) -> Self {
		Self::Object(V1Object(v))
	}
}

impl From<V1Geometry> for V1Value {
	fn from(v: V1Geometry) -> Self {
		Self::Geometry(v)
	}
}

impl From<geo::Point<f64>> for V1Value {
	fn from(v: geo::Point<f64>) -> Self {
		Self::Geometry(V1Geometry::Point(v))
	}
}

impl From<geo::LineString<f64>> for V1Value {
	fn from(v: geo::LineString<f64>) -> Self {
		Self::Geometry(V1Geometry::Line(v))
	}
}

impl From<geo::Polygon<f64>> for V1Value {
	fn from(v: geo::Polygon<f64>) -> Self {
		Self::Geometry(V1Geometry::Polygon(v))
	}
}

impl From<geo::MultiPoint<f64>> for V1Value {
	fn from(v: geo::MultiPoint<f64>) -> Self {
		Self::Geometry(V1Geometry::MultiPoint(v))
	}
}

impl From<geo::MultiLineString<f64>> for V1Value {
	fn from(v: geo::MultiLineString<f64>) -> Self {
		Self::Geometry(V1Geometry::MultiLine(v))
	}
}

impl From<geo::MultiPolygon<f64>> for V1Value {
	fn from(v: geo::MultiPolygon<f64>) -> Self {
		Self::Geometry(V1Geometry::MultiPolygon(v))
	}
}

impl From<V1Bytes> for V1Value {
	fn from(v: V1Bytes) -> Self {
		Self::Bytes(v)
	}
}

impl From<V1RecordId> for V1Value {
	fn from(v: V1RecordId) -> Self {
		Self::RecordId(v)
	}
}

impl From<V1Model> for V1Value {
	fn from(v: V1Model) -> Self {
		Self::Model(Box::new(v))
	}
}

impl From<V1File> for V1Value {
	fn from(v: V1File) -> Self {
		Self::File(v)
	}
}

macro_rules! impl_from_number_prims {
    ($($t:ty),*) => {
        $(
            impl From<$t> for V1Value {
                fn from(v: $t) -> Self {
                    Self::Number(V1Number::Int(v as i64))
                }
            }
        )*
    };
}

impl_from_number_prims!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);

impl From<f32> for V1Value {
	fn from(v: f32) -> Self {
		Self::Number(V1Number::Float(v as f64))
	}
}

impl From<f64> for V1Value {
	fn from(v: f64) -> Self {
		Self::Number(V1Number::Float(v))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename = "$surrealdb::private::sql::Number")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum V1Number {
	Int(i64),
	Float(f64),
	Decimal(Decimal),
	// Add new variants here
}

impl V1Number {
	pub fn as_float(self) -> f64 {
		match self {
			V1Number::Int(v) => v as f64,
			V1Number::Float(v) => v,
			V1Number::Decimal(v) => v.try_into().unwrap_or_default(),
		}
	}
}

/// A string that doesn't contain NUL bytes.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Strand")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct V1Strand(#[serde(with = "no_nul_bytes")] pub String);

impl From<String> for V1Strand {
	fn from(v: String) -> Self {
		Self(v)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Duration")]
#[non_exhaustive]
pub struct V1Duration(pub time::Duration);

impl V1Duration {
	pub fn new(seconds: i64, nanos: u32) -> Self {
		Self(time::Duration::new(seconds as u64, nanos))
	}

	/// Convert the Duration to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
}

impl TryFrom<String> for V1Duration {
	type Error = anyhow::Error;
	fn try_from(v: String) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<V1Strand> for V1Duration {
	type Error = anyhow::Error;
	fn try_from(v: V1Strand) -> Result<Self, Self::Error> {
		Self::try_from(v.0.as_str())
	}
}

impl TryFrom<&str> for V1Duration {
	type Error = anyhow::Error;
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		match crate::syn::duration(v) {
			Ok(v) => Ok(v.into()),
			_ => Err(anyhow::anyhow!("Invalid duration")),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Datetime")]
#[non_exhaustive]
pub struct V1Datetime(pub DateTime<Utc>);

impl V1Datetime {
	/// Convert the Datetime to a raw String
	pub fn to_raw(&self) -> String {
		self.0.to_rfc3339_opts(SecondsFormat::AutoSi, true)
	}
}

impl TryFrom<String> for V1Datetime {
	type Error = anyhow::Error;
	fn try_from(v: String) -> Result<Self, Self::Error> {
		Ok(Self(DateTime::parse_from_rfc3339(&v).context("Invalid RFC3339 datetime")?.into()))
	}
}

impl TryFrom<(i64, u32)> for V1Datetime {
	type Error = anyhow::Error;
	fn try_from((seconds, nanos): (i64, u32)) -> Result<Self, Self::Error> {
		Ok(Self(DateTime::from_timestamp(seconds, nanos).context("Invalid datetime")?))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Uuid")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct V1Uuid(pub uuid::Uuid);

impl V1Uuid {
	/// Convert the Uuid to a raw String
	pub fn to_raw(&self) -> String {
		self.0.to_string()
	}
}

impl TryFrom<String> for V1Uuid {
	type Error = anyhow::Error;

	fn try_from(value: String) -> Result<Self, Self::Error> {
		Ok(Self(uuid::Uuid::parse_str(&value).context("Invalid UUID")?))
	}
}

impl From<uuid::Uuid> for V1Uuid {
	fn from(v: uuid::Uuid) -> Self {
		Self(v)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Array")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct V1Array(pub Vec<V1Value>);

impl V1Array {
	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}
}

impl IntoIterator for V1Array {
	type Item = V1Value;
	type IntoIter = std::vec::IntoIter<V1Value>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl FromIterator<V1Value> for V1Array {
	fn from_iter<T: IntoIterator<Item = V1Value>>(iter: T) -> Self {
		Self(iter.into_iter().collect())
	}
}

/// Invariant: Keys never contain NUL bytes.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Object")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct V1Object(#[serde(with = "no_nul_bytes_in_keys")] pub BTreeMap<String, V1Value>);

impl std::ops::Deref for V1Object {
	type Target = BTreeMap<String, V1Value>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for V1Object {
	type Item = (String, V1Value);
	type IntoIter = std::collections::btree_map::IntoIter<String, V1Value>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl FromIterator<(String, V1Value)> for V1Object {
	fn from_iter<T: IntoIterator<Item = (String, V1Value)>>(iter: T) -> Self {
		Self(iter.into_iter().collect())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Geometry")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum V1Geometry {
	Point(geo::Point<f64>),
	Line(geo::LineString<f64>),
	Polygon(geo::Polygon<f64>),
	MultiPoint(geo::MultiPoint<f64>),
	MultiLine(geo::MultiLineString<f64>),
	MultiPolygon(geo::MultiPolygon<f64>),
	Collection(Vec<V1Geometry>),
	// Add new variants here
}

impl From<geo::Point<f64>> for V1Geometry {
	fn from(v: geo::Point<f64>) -> Self {
		Self::Point(v)
	}
}

impl From<geo::LineString<f64>> for V1Geometry {
	fn from(v: geo::LineString<f64>) -> Self {
		Self::Line(v)
	}
}

impl From<geo::Polygon<f64>> for V1Geometry {
	fn from(v: geo::Polygon<f64>) -> Self {
		Self::Polygon(v)
	}
}

impl From<geo::MultiPoint<f64>> for V1Geometry {
	fn from(v: geo::MultiPoint<f64>) -> Self {
		Self::MultiPoint(v)
	}
}

impl From<geo::MultiLineString<f64>> for V1Geometry {
	fn from(v: geo::MultiLineString<f64>) -> Self {
		Self::MultiLine(v)
	}
}

impl From<geo::MultiPolygon<f64>> for V1Geometry {
	fn from(v: geo::MultiPolygon<f64>) -> Self {
		Self::MultiPolygon(v)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct V1Bytes(pub(crate) Vec<u8>);

impl From<Vec<u8>> for V1Bytes {
	fn from(v: Vec<u8>) -> Self {
		Self(v)
	}
}

impl Serialize for V1Bytes {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_bytes(&self.0)
	}
}

impl<'de> Deserialize<'de> for V1Bytes {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		struct RawBytesVisitor;

		impl<'de> serde::de::Visitor<'de> for RawBytesVisitor {
			type Value = V1Bytes;

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				formatter.write_str("bytes or sequence of bytes")
			}

			fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(V1Bytes(v))
			}

			fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				Ok(V1Bytes(v.to_owned()))
			}

			fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
			where
				A: serde::de::SeqAccess<'de>,
			{
				let capacity = seq.size_hint().unwrap_or_default();
				let mut vec = Vec::with_capacity(capacity);
				while let Some(byte) = seq.next_element()? {
					vec.push(byte);
				}
				Ok(V1Bytes(vec))
			}
		}

		deserializer.deserialize_byte_buf(RawBytesVisitor)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Ord)]
#[serde(rename = "$surrealdb::private::sql::Table")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct V1Table(#[serde(with = "no_nul_bytes")] pub String);

impl From<String> for V1Table {
	fn from(v: String) -> Self {
		Self(v)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Thing")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct V1RecordId {
	/// Table name
	pub tb: String,
	pub id: V1Id,
}

impl TryFrom<String> for V1RecordId {
	type Error = anyhow::Error;

	fn try_from(value: String) -> Result<Self, Self::Error> {
		Ok(Self::try_from(value.as_str())?)
	}
}

impl TryFrom<V1Strand> for V1RecordId {
	type Error = anyhow::Error;
	fn try_from(v: V1Strand) -> Result<Self, Self::Error> {
		Self::try_from(v.0.as_str())
	}
}

impl TryFrom<&str> for V1RecordId {
	type Error = anyhow::Error;
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		match crate::syn::thing_with_range(v) {
			Ok(v) => Ok(v.try_into()?),
			_ => Err(anyhow::anyhow!("Invalid record id")),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum V1Gen {
	Rand,
	Ulid,
	Uuid,
}

#[revisioned(revision = 2)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum V1Id {
	Number(i64),
	String(String),
	#[revision(start = 2)]
	Uuid(V1Uuid),
	Array(V1Array),
	Object(V1Object),
	Generate(V1Gen),
	Range(Box<V1IdRange>),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct V1IdRange {
	pub beg: Bound<V1Id>,
	pub end: Bound<V1Id>,
}

impl From<(Bound<V1Id>, Bound<V1Id>)> for V1IdRange {
	fn from((beg, end): (Bound<V1Id>, Bound<V1Id>)) -> Self {
		Self {
			beg,
			end,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Model")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct V1Model {
	pub name: String,
	pub version: String,
	pub args: Vec<V1Value>,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::File")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct V1File {
	pub bucket: String,
	pub key: String,
}

impl V1File {
	pub fn display_inner(&self) -> String {
		format!("{}:{}", Self::fmt_inner(&self.bucket, true), Self::fmt_inner(&self.key, false))
	}

	fn fmt_inner(v: &str, escape_slash: bool) -> String {
		v.chars()
			.flat_map(|c| {
				if c.is_ascii_alphanumeric()
					|| matches!(c, '-' | '_' | '.')
					|| (!escape_slash && c == '/')
				{
					vec![c]
				} else {
					vec!['\\', c]
				}
			})
			.collect::<String>()
	}
}

// serde(with = no_nul_bytes) will (de)serialize with no NUL bytes.
pub(crate) mod no_nul_bytes {
	use serde::{
		Deserializer, Serializer,
		de::{self, Visitor},
	};
	use std::fmt;

	pub(crate) fn serialize<S>(s: &str, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		if s.contains('\0') {
			return Err(<S::Error as serde::ser::Error>::custom(
				"to be serialized string contained a null byte",
			));
		}
		serializer.serialize_str(s)
	}

	pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct NoNulBytesVisitor;

		impl Visitor<'_> for NoNulBytesVisitor {
			type Value = String;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("a string without any NUL bytes")
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				if value.contains('\0') {
					Err(de::Error::custom("contained NUL byte"))
				} else {
					Ok(value.to_owned())
				}
			}

			fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				if value.contains('\0') {
					Err(de::Error::custom("contained NUL byte"))
				} else {
					Ok(value)
				}
			}
		}

		deserializer.deserialize_string(NoNulBytesVisitor)
	}
}

mod no_nul_bytes_in_keys {
	use serde::{
		Deserializer, Serializer,
		de::{self, Visitor},
		ser::SerializeMap,
	};
	use std::{collections::BTreeMap, fmt};

	use super::V1Value;

	pub(crate) fn serialize<S>(
		m: &BTreeMap<String, V1Value>,
		serializer: S,
	) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let mut s = serializer.serialize_map(Some(m.len()))?;
		for (k, v) in m {
			debug_assert!(!k.contains('\0'));
			s.serialize_entry(k, v)?;
		}
		s.end()
	}

	pub(crate) fn deserialize<'de, D>(
		deserializer: D,
	) -> Result<BTreeMap<String, V1Value>, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct NoNulBytesInKeysVisitor;

		impl<'de> Visitor<'de> for NoNulBytesInKeysVisitor {
			type Value = BTreeMap<String, V1Value>;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("a map without any NUL bytes in its keys")
			}

			fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
			where
				A: de::MapAccess<'de>,
			{
				let mut ret = BTreeMap::new();
				while let Some((k, v)) = map.next_entry()? {
					ret.insert(k, v);
				}
				Ok(ret)
			}
		}

		deserializer.deserialize_map(NoNulBytesInKeysVisitor)
	}
}

impl Display for V1Value {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		let mut f = crate::sql::fmt::Pretty::from(f);
		match self {
			V1Value::None => write!(f, "NONE"),
			V1Value::Null => write!(f, "NULL"),
			V1Value::Array(v) => write!(f, "{v}"),
			V1Value::Bool(v) => write!(f, "{v}"),
			V1Value::Bytes(v) => write!(f, "{v}"),
			V1Value::Datetime(v) => write!(f, "{v}"),
			V1Value::Duration(v) => write!(f, "{v}"),
			V1Value::Geometry(v) => write!(f, "{v}"),
			V1Value::Number(v) => write!(f, "{v}"),
			V1Value::Object(v) => write!(f, "{v}"),
			V1Value::Strand(v) => write!(f, "{v}"),
			V1Value::RecordId(v) => write!(f, "{v}"),
			V1Value::Table(v) => write!(f, "{v}"),
			V1Value::Uuid(v) => write!(f, "{v}"),
			V1Value::Model(v) => write!(f, "{v}"),
			V1Value::File(v) => write!(f, "{v}"),
		}
	}
}

impl Display for V1Array {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		let mut f = crate::sql::fmt::Pretty::from(f);
		f.write_char('[')?;
		if !self.is_empty() {
			let indent = crate::sql::fmt::pretty_indent();
			write!(f, "{}", crate::sql::fmt::Fmt::pretty_comma_separated(self.0.as_slice()))?;
			drop(indent);
		}
		f.write_char(']')
	}
}

impl Display for V1Bytes {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "b\"{}\"", hex::encode_upper(&self.0))
	}
}

impl Display for V1Datetime {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "d{}", &crate::sql::escape::QuoteStr(&self.to_raw()))
	}
}

impl Display for V1Duration {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		use crate::sql::duration::{
			NANOSECONDS_PER_MICROSECOND, NANOSECONDS_PER_MILLISECOND, SECONDS_PER_DAY,
			SECONDS_PER_HOUR, SECONDS_PER_MINUTE, SECONDS_PER_WEEK, SECONDS_PER_YEAR,
		};
		// Split up the duration
		let secs = self.0.as_secs();
		let nano = self.0.subsec_nanos();
		// Ensure no empty output
		if secs == 0 && nano == 0 {
			return write!(f, "0ns");
		}
		// Calculate the total years
		let year = secs / SECONDS_PER_YEAR;
		let secs = secs % SECONDS_PER_YEAR;
		// Calculate the total weeks
		let week = secs / SECONDS_PER_WEEK;
		let secs = secs % SECONDS_PER_WEEK;
		// Calculate the total days
		let days = secs / SECONDS_PER_DAY;
		let secs = secs % SECONDS_PER_DAY;
		// Calculate the total hours
		let hour = secs / SECONDS_PER_HOUR;
		let secs = secs % SECONDS_PER_HOUR;
		// Calculate the total minutes
		let mins = secs / SECONDS_PER_MINUTE;
		let secs = secs % SECONDS_PER_MINUTE;
		// Calculate the total milliseconds
		let msec = nano / NANOSECONDS_PER_MILLISECOND;
		let nano = nano % NANOSECONDS_PER_MILLISECOND;
		// Calculate the total microseconds
		let usec = nano / NANOSECONDS_PER_MICROSECOND;
		let nano = nano % NANOSECONDS_PER_MICROSECOND;
		// Write the different parts
		if year > 0 {
			write!(f, "{year}y")?;
		}
		if week > 0 {
			write!(f, "{week}w")?;
		}
		if days > 0 {
			write!(f, "{days}d")?;
		}
		if hour > 0 {
			write!(f, "{hour}h")?;
		}
		if mins > 0 {
			write!(f, "{mins}m")?;
		}
		if secs > 0 {
			write!(f, "{secs}s")?;
		}
		if msec > 0 {
			write!(f, "{msec}ms")?;
		}
		if usec > 0 {
			write!(f, "{usec}µs")?;
		}
		if nano > 0 {
			write!(f, "{nano}ns")?;
		}
		Ok(())
	}
}

impl Display for V1Number {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			V1Number::Int(v) => Display::fmt(v, f),
			V1Number::Float(v) => {
				if v.is_finite() {
					// Add suffix to distinguish between int and float
					write!(f, "{v}f")
				} else {
					// Don't add suffix for NaN, inf, -inf
					Display::fmt(v, f)
				}
			}
			V1Number::Decimal(v) => write!(f, "{v}dec"),
		}
	}
}

impl Display for V1Strand {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		crate::sql::escape::QuoteStr(&self.0).fmt(f)
	}
}

impl Display for V1Uuid {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "u{}", crate::sql::escape::QuoteStr(&self.0.to_string()))
	}
}

impl Display for V1Model {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ml::{}<{}>(", self.name, self.version)?;
		for (idx, p) in self.args.iter().enumerate() {
			if idx != 0 {
				write!(f, ",")?;
			}
			write!(f, "{}", p)?;
		}
		write!(f, ")")
	}
}

impl Display for V1File {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "f\"{}\"", self.display_inner())
	}
}

impl Display for V1Object {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		use crate::sql::escape::EscapeKey;
		use crate::sql::fmt::{Fmt, Pretty, is_pretty, pretty_indent};

		let mut f = Pretty::from(f);
		if is_pretty() {
			f.write_char('{')?;
		} else {
			f.write_str("{ ")?;
		}
		if !self.is_empty() {
			let indent = pretty_indent();
			write!(
				f,
				"{}",
				Fmt::pretty_comma_separated(
					self.0.iter().map(|args| Fmt::new(args, |(k, v), f| write!(
						f,
						"{}: {}",
						EscapeKey(k),
						v
					))),
				)
			)?;
			drop(indent);
		}
		if is_pretty() {
			f.write_char('}')
		} else {
			f.write_str(" }")
		}
	}
}

impl Display for V1Table {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		crate::sql::escape::EscapeIdent(&self.0).fmt(f)
	}
}

impl Display for V1RecordId {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", crate::sql::escape::EscapeRid(&self.tb), self.id)
	}
}

impl Display for V1Id {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		use crate::sql::escape::EscapeRid;
		match self {
			Self::Number(v) => Display::fmt(v, f),
			Self::String(v) => EscapeRid(v).fmt(f),
			Self::Uuid(v) => Display::fmt(v, f),
			Self::Array(v) => Display::fmt(v, f),
			Self::Object(v) => Display::fmt(v, f),
			Self::Generate(v) => match v {
				V1Gen::Rand => Display::fmt("rand()", f),
				V1Gen::Ulid => Display::fmt("ulid()", f),
				V1Gen::Uuid => Display::fmt("uuid()", f),
			},
			Self::Range(v) => Display::fmt(v, f),
		}
	}
}

impl Display for V1IdRange {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match &self.beg {
			Bound::Unbounded => write!(f, ""),
			Bound::Included(v) => write!(f, "{v}"),
			Bound::Excluded(v) => write!(f, "{v}>"),
		}?;
		match &self.end {
			Bound::Unbounded => write!(f, ".."),
			Bound::Excluded(v) => write!(f, "..{v}"),
			Bound::Included(v) => write!(f, "..={v}"),
		}?;
		Ok(())
	}
}

impl Display for V1Geometry {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		use crate::sql::fmt::Fmt;
		use std::iter::once;

		match self {
			Self::Point(v) => {
				write!(f, "({}, {})", v.x(), v.y())
			}
			Self::Line(v) => write!(
				f,
				"{{ type: 'LineString', coordinates: [{}] }}",
				Fmt::comma_separated(v.points().map(|v| Fmt::new(v, |v, f| write!(
					f,
					"[{}, {}]",
					v.x(),
					v.y()
				))))
			),
			Self::Polygon(v) => write!(
				f,
				"{{ type: 'Polygon', coordinates: [{}] }}",
				Fmt::comma_separated(once(v.exterior()).chain(v.interiors()).map(|v| Fmt::new(
					v,
					|v, f| write!(
						f,
						"[{}]",
						Fmt::comma_separated(v.points().map(|v| Fmt::new(v, |v, f| write!(
							f,
							"[{}, {}]",
							v.x(),
							v.y()
						))))
					)
				)))
			),
			Self::MultiPoint(v) => {
				write!(
					f,
					"{{ type: 'MultiPoint', coordinates: [{}] }}",
					Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f| write!(
						f,
						"[{}, {}]",
						v.x(),
						v.y()
					))))
				)
			}
			Self::MultiLine(v) => write!(
				f,
				"{{ type: 'MultiLineString', coordinates: [{}] }}",
				Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f| write!(
					f,
					"[{}]",
					Fmt::comma_separated(v.points().map(|v| Fmt::new(v, |v, f| write!(
						f,
						"[{}, {}]",
						v.x(),
						v.y()
					))))
				))))
			),
			Self::MultiPolygon(v) => {
				write!(
					f,
					"{{ type: 'MultiPolygon', coordinates: [{}] }}",
					Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f| {
						write!(
							f,
							"[{}]",
							Fmt::comma_separated(once(v.exterior()).chain(v.interiors()).map(
								|v| Fmt::new(v, |v, f| write!(
									f,
									"[{}]",
									Fmt::comma_separated(v.points().map(|v| Fmt::new(
										v,
										|v, f| write!(f, "[{}, {}]", v.x(), v.y())
									)))
								))
							))
						)
					}))),
				)
			}
			Self::Collection(v) => {
				write!(
					f,
					"{{ type: 'GeometryCollection', geometries: [{}] }}",
					Fmt::comma_separated(v)
				)
			}
		}
	}
}
