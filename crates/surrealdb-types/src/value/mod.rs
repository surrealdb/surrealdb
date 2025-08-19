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

macro_rules! impl_value {
    ($($variant:ident => $is:ident),*$(,)?) => {
        impl Value {
            $(
                pub fn $is(&self) -> bool {
                    matches!(self, Value::$variant)
                }
            )*
        }
    };

    ($($variant:ident($type:ty) => ($is:ident, $from:ident, $into:ident)),*$(,)?) => {
        impl Value {
            $(
                pub fn $is(&self) -> bool {
                    matches!(self, Value::$variant(_))
                }

                pub fn $from(x: $type) -> Self {
                    Value::$variant(x)
                }

                pub fn $into(self) -> Option<$type> {
                    if let Value::$variant(x) = self {
						Some(x)
					} else {
						None
					}
                }
            )*
        }
    };
}

impl_value! {
	None => is_none,
	Null => is_null,
	Bool(bool) => (is_bool, from_bool, into_bool),
	Number(Number) => (is_number, from_number, into_number),
	Strand(Strand) => (is_strand, from_strand, into_strand),
	Duration(Duration) => (is_duration, from_duration, into_duration),
	Datetime(Datetime) => (is_datetime, from_datetime, into_datetime),
	Uuid(Uuid) => (is_uuid, from_uuid, into_uuid),
	Array(Array) => (is_array, from_array, into_array),
	Object(Object) => (is_object, from_object, into_object),
	Geometry(Geometry) => (is_geometry, from_geometry, into_geometry),
	Bytes(Bytes) => (is_bytes, from_bytes, into_bytes),
	RecordId(RecordId) => (is_thing, from_thing, into_thing),
	File(File) => (is_file, from_file, into_file),
	Range(Box<Range>) => (is_range, from_range, into_range),
	Closure(Box<Closure>) => (is_closure, from_closure, into_closure),
	Refs(Refs) => (is_refs, from_refs, into_refs),
	File(File) => (is_file, from_file, into_file),
}