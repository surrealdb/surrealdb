use std::collections::BTreeMap;

use crate::{Bytes, Datetime, Duration, File, KindGeometry, Number, Object, Range, RecordId, Value};
use chrono::{DateTime, Utc};
use geo::Geometry;
use rust_decimal::Decimal;

use crate::{Kind, Strand, SurrealNone, SurrealNull, Uuid};

pub trait KindOf {
    fn kind_of() -> Kind;
}

macro_rules! impl_basic_kind_of {
	($($type:ty => $kind:expr),*$(,)?) => {
		$(
			impl KindOf for $type {
				fn kind_of() -> Kind {
					$kind
				}
			}
		)*
	}
}

impl_basic_kind_of! {
    // None, Null, Bool
    () => Kind::None,
    SurrealNone => Kind::None,
    SurrealNull => Kind::Null,
    bool => Kind::Bool,

    // Bytes
    Vec<u8> => Kind::Bytes,
    Bytes => Kind::Bytes,
    bytes::Bytes => Kind::Bytes,

    // Datetime
    Datetime => Kind::Datetime,
    DateTime<Utc> => Kind::Datetime,

    // Decimal
    Decimal => Kind::Decimal,

    // Duration
    Duration => Kind::Duration,
    std::time::Duration => Kind::Duration,

    // Numbers
    f64 => Kind::Float,
    i64 => Kind::Int,
    Number => Kind::Number,

    // Object
    BTreeMap<String, Value> => Kind::Object,
    Object => Kind::Object,

    // String
    String => Kind::String,
    Strand => Kind::String,

    // UUID
    Uuid => Kind::Uuid,
    uuid::Uuid => Kind::Uuid,

    // Record
    RecordId => Kind::Record(Vec::new()),

    // Geometry
    Geometry => Kind::Geometry(Vec::new()),
    geo::Point => Kind::Geometry(vec![KindGeometry::Point]),
    geo::LineString => Kind::Geometry(vec![KindGeometry::Line]),
    geo::Polygon => Kind::Geometry(vec![KindGeometry::Polygon]),
    geo::MultiPoint => Kind::Geometry(vec![KindGeometry::MultiPoint]),
    geo::MultiLineString => Kind::Geometry(vec![KindGeometry::MultiLine]),
    geo::MultiPolygon => Kind::Geometry(vec![KindGeometry::MultiPolygon]),

    // Range
    Range => Kind::Range,

    // File
    File => Kind::File(Vec::new()),
}

impl<T: KindOf> KindOf for Option<T> {
    fn kind_of() -> Kind {
        Kind::Option(Box::new(T::kind_of()))
    }
}

impl<T: KindOf> KindOf for Vec<T> {
    fn kind_of() -> Kind {
        Kind::Array(Box::new(T::kind_of()), None)
    }
}


impl<T: KindOf> KindOf for std::ops::Range<T> {
    fn kind_of() -> Kind {
        Kind::Range
    }
}

impl<T: KindOf> KindOf for std::ops::RangeFrom<T> {
    fn kind_of() -> Kind {
        Kind::Range
    }
}

impl<T: KindOf> KindOf for std::ops::RangeTo<T> {
    fn kind_of() -> Kind {
        Kind::Range
    }
}

impl<T: KindOf> KindOf for std::ops::RangeInclusive<T> {
    fn kind_of() -> Kind {
        Kind::Range
    }
}

impl<T: KindOf> KindOf for std::ops::RangeToInclusive<T> {
    fn kind_of() -> Kind {
        Kind::Range
    }
}

