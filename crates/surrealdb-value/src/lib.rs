pub mod array;
pub mod bytes;
pub mod closure;
pub mod datetime;
pub mod duration;
pub mod file;
pub mod geometry;
pub mod number;
pub mod object;
pub mod range;
pub mod regex;
pub mod strand;
pub mod table;
pub mod thing;
pub mod uuid;

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

pub use self::array::Array;
pub use self::bytes::Bytes;
pub use self::closure::Closure;
pub use self::datetime::Datetime;
pub use self::duration::Duration;
pub use self::file::File;
pub use self::geometry::Geometry;
pub use self::number::{DecimalExt, Number};
pub use self::object::Object;
pub use self::range::Range;
pub use self::regex::Regex;
pub use self::strand::{Strand, StrandRef};
pub use self::table::Table;
pub use self::thing::{RecordId, RecordIdKey, RecordIdKeyRange};
pub use self::uuid::Uuid;

/// Marker type for value conversions from Value::None
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd)]
pub struct SurrealNone;

/// Marker type for value conversions from Value::Null
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd)]
pub struct SurrealNull;

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::public::Value")]
pub enum Value {
	#[default]
	None,
	Null,
	Bool(bool),
	Number(Number),
	Strand(Strand),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Array(Array),
	Object(Object),
	Geometry(Geometry),
	Bytes(Bytes),
	RecordId(RecordId),
	Table(Table),
	File(File),
	#[serde(skip)]
	Regex(Regex),
	Range(Box<Range>),
	#[serde(skip)]
	Closure(Box<Closure>),
	// Add new variants here
}

impl Eq for Value {}

impl Ord for Value {
	fn cmp(&self, other: &Self) -> Ordering {
		self.partial_cmp(other).unwrap_or(Ordering::Equal)
	}
}