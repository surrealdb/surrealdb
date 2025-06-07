use std::ops::Bound;

use crate::{
	expr::Value,
	sql::{
		Array, Bytes, Datetime, Duration, Future, Geometry, Id, IdRange, Number, Object, Range,
		SqlValue, Strand, Table, Thing, Uuid,
	},
};
use geo_types::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use rust_decimal::Decimal;

pub trait TypeName {
	fn type_name() -> String;
}

macro_rules! impl_type_name {
    ($($type:ty => $name:expr),* $(,)?) => {
        $(
            impl TypeName for $type {
                fn type_name() -> String {
                    $name.to_string()
                }
            }
        )*
    };
}

impl_type_name!(
	String => "a string",
	Strand => "a string",
	Bytes => "bytes",
	Array => "an array",
	Object => "an object",
	i64 => "a positive integer",
	u64 => "a u64 integer",
	u32 => "a u32 integer",
	Uuid => "a uuid",
	uuid::Uuid => "a uuid",
	Id => "an id part",
	Table => "a table",
	Thing => "a record id",
	Value => "a value",
	SqlValue => "a value",
	Decimal => "a decimal",
	Number => "a number",
	Geometry => "a geometry",
	Point => "a geometry point",
	LineString => "a geometry line",
	Polygon => "a geometry polygon",
	MultiPoint => "a geometry multi-point",
	MultiLineString => "a geometry multi-line",
	MultiPolygon => "a geometry multi-polygon",
	Datetime => "a datetime",
	Duration => "a duration",
	Range => "a range",
	Future => "a future",
	IdRange => "an id range",
);

impl<T> TypeName for Bound<T>
where
	T: TypeName,
{
	fn type_name() -> String {
		format!("a range bound with {}", T::type_name())
	}
}

impl<T> TypeName for Vec<T>
where
	T: TypeName,
{
	fn type_name() -> String {
		format!("a vec with {}", T::type_name())
	}
}
