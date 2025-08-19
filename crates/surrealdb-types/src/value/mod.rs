pub mod array;
pub mod bytes;
pub mod datetime;
pub mod duration;
pub mod file;
pub mod geometry;
pub mod number;
pub mod object;
pub mod range;
pub mod strand;
pub mod recordid;
pub mod uuid;

use std::cmp::Ordering;

pub use self::array::Array;
pub use self::bytes::Bytes;
pub use self::datetime::Datetime;
pub use self::duration::Duration;
pub use self::file::File;
pub use self::geometry::Geometry;
pub use self::number::Number;
pub use self::object::Object;
pub use self::range::Range;
pub use self::strand::{Strand, StrandRef};
pub use self::recordid::{RecordId, RecordIdKey, RecordIdKeyRange};
pub use self::uuid::Uuid;

/// Marker type for value conversions from Value::None
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SurrealNone;

/// Marker type for value conversions from Value::Null
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SurrealNull;

#[derive(Clone, Debug, Default, Hash, PartialEq, PartialOrd)]
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
	File(File),
	Range(Box<Range>),
}

impl Eq for Value {}

impl Ord for Value {
	fn cmp(&self, other: &Self) -> Ordering {
		self.partial_cmp(other).unwrap_or(Ordering::Equal)
	}
}