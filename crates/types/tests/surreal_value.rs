use std::collections::{BTreeMap, HashMap};

use geo::Point;
use rust_decimal::Decimal;
use surrealdb_types::{
	Array, Bytes, Datetime, Duration, File, Geometry, Kind, KindLiteral, Number, Object, Range,
	RecordId, SurrealNone, SurrealNull, SurrealValue, Uuid, Value,
};

// Simple macro for concrete types
macro_rules! test_surreal_value {
    (
        $fnc:ident<$type:ty>(
            $val:expr => ($value:pat, $kind:pat),
            $(is($is:ident),)?
            $(into($into:ident),)?
            $(from($from:ident),)?
        )
    ) => {
        #[rustfmt::skip]
        #[test]
        fn $fnc() {
            let val = $val;
            let value = val.clone().into_value();

            assert!(matches!(value, $value));
            assert!(matches!(<$type>::kind_of(), $kind));

            assert!(<$type>::is_value(&value));
            assert!(value.is::<$type>());
            assert_eq!(<$type>::from_value(value.clone()).unwrap(), val.clone());

            $(assert!(value.$is());)?
            $(assert_eq!(value.clone().$into().unwrap(), val.clone());)?
            $(assert_eq!(value, Value::$from(val.clone()));)?
        }
    };

    (
        $fnc:ident<$type:ty>(
            $val:expr => ($value:pat, $kind:pat),
            $(is($is:ident<$is_type:ty>),)?
            $(into($into:ident<$into_type:ty>),)?
            $(from($from:ident<$from_type:ty>),)?
        )
    ) => {
        #[rustfmt::skip]
        #[test]
        fn $fnc() {
            let val = $val;
            let value = val.clone().into_value();

            assert!(matches!(value, $value));
            assert!(matches!(<$type>::kind_of(), $kind));

            assert!(<$type>::is_value(&value));
            assert!(value.is::<$type>());
            assert_eq!(<$type>::from_value(value.clone()).unwrap(), val.clone());

            $(assert!(value.$is::<$is_type>());)?
            $(assert_eq!(value.clone().$into::<$into_type>().unwrap(), val.clone());)?
            $(assert_eq!(value, Value::$from::<$from_type>(val.clone()));)?
        }
    };
}

test_surreal_value!(
	none_unit<()>(
		() => (
			Value::None,
			Kind::None
		),
		is(is_none),
	)
);

test_surreal_value!(
	none_struct<SurrealNone>(
		SurrealNone => (
			Value::None,
			Kind::None
		),
		is(is_none),
	)
);

test_surreal_value!(
	null_struct<SurrealNull>(
		SurrealNull => (
			Value::Null,
			Kind::Null
		),
		is(is_null),
	)
);

test_surreal_value!(
	bool<bool>(
		true => (
			Value::Bool(_),
			Kind::Bool
		),
		is(is_bool),
		into(into_bool),
		from(from_bool),
	)
);

test_surreal_value!(
	bool_true<bool>(
		true => (
			Value::Bool(_),
			Kind::Bool
		),
		is(is_true),
	)
);

test_surreal_value!(
	bool_false<bool>(
		false => (
			Value::Bool(_),
			Kind::Bool
		),
		is(is_false),
	)
);

test_surreal_value!(
	number<Number>(
		Number::Int(10) => (
			Value::Number(_),
			Kind::Number
		),
		is(is_number),
		into(into_number),
		from(from_number),
	)
);

test_surreal_value!(
	integer<i64>(
		10 => (
			Value::Number(Number::Int(_)),
			Kind::Int
		),
		is(is_number),
		into(into_int),
		from(from_int),
	)
);

test_surreal_value!(
	float<f64>(
		10.0 => (
			Value::Number(Number::Float(_)),
			Kind::Float
		),
		is(is_float),
		into(into_float),
		from(from_float),
	)
);

test_surreal_value!(
	decimal<Decimal>(
		Decimal::from(10) => (
			Value::Number(Number::Decimal(_)),
			Kind::Decimal
		),
		is(is_decimal),
		into(into_decimal),
		from(from_decimal),
	)
);

test_surreal_value!(
	string<String>(
		"Hello, world!".to_string() => (
			Value::String(_),
			Kind::String
		),
		is(is_string),
		into(into_string),
		from(from_string),
	)
);

test_surreal_value!(
	duration<Duration>(
		Duration::from_secs(10) => (
			Value::Duration(_),
			Kind::Duration
		),
		is(is_duration),
		into(into_duration),
		from(from_duration),
	)
);

test_surreal_value!(
	duration_std<std::time::Duration>(
		std::time::Duration::from_secs(10) => (
			Value::Duration(_),
			Kind::Duration
		),
	)
);

test_surreal_value!(
	datetime<Datetime>(
		Datetime::MIN_UTC => (
			Value::Datetime(_),
			Kind::Datetime
		),
		is(is_datetime),
		into(into_datetime),
		from(from_datetime),
	)
);

test_surreal_value!(
	datetime_chrono<chrono::DateTime<chrono::Utc>>(
		chrono::Utc::now() => (
			Value::Datetime(_),
			Kind::Datetime
		),
	)
);

test_surreal_value!(
	uuid<Uuid>(
		Uuid::new_v4() => (
			Value::Uuid(_),
			Kind::Uuid
		),
		is(is_uuid),
		into(into_uuid),
		from(from_uuid),
	)
);

test_surreal_value!(
	uuid_std<Uuid>(
		Uuid::new_v4() => (
			Value::Uuid(_),
			Kind::Uuid
		),
	)
);

test_surreal_value!(
	array<Array>(
		Array::new() => (
			Value::Array(_),
			Kind::Array(_, _)
		),
		is(is_array),
		into(into_array),
		from(from_array),
	)
);

test_surreal_value!(
	object<Object>(
		Object::new() => (
			Value::Object(_),
			Kind::Object
		),
		is(is_object),
		into(into_object),
		from(from_object),
	)
);

test_surreal_value!(
	geometry<Geometry>(
		Geometry::Point(Point::new(10.0, 10.0)) => (
			Value::Geometry(_),
			Kind::Geometry(_)
		),
		is(is_geometry),
		into(into_geometry),
		from(from_geometry),
	)
);

test_surreal_value!(
	bytes<Bytes>(
		Bytes::from(vec![1, 2, 3]) => (
			Value::Bytes(_),
			Kind::Bytes
		),
		is(is_bytes),
		into(into_bytes),
		from(from_bytes),
	)
);

test_surreal_value!(
	bytes_vec<Vec<u8>>(
		vec![1_u8, 2, 3] => (
			Value::Bytes(_),
			Kind::Bytes
		),
	)
);

test_surreal_value!(
	bytes_bytes<bytes::Bytes>(
		bytes::Bytes::from(vec![1, 2, 3]) => (
			Value::Bytes(_),
			Kind::Bytes
		),
	)
);

test_surreal_value!(
	record_id<RecordId>(
		RecordId::new("test", 123) => (
			Value::RecordId(_),
			Kind::Record(_)
		),
		is(is_record),
		into(into_record),
		from(from_record),
	)
);

test_surreal_value!(
	file<File>(
		File::new("test", "test") => (
			Value::File(_),
			Kind::File(_)
		),
		is(is_file),
		into(into_file),
		from(from_file),
	)
);

test_surreal_value!(
	range<Range>(
		Range::from(..) => (
			Value::Range(_),
			Kind::Range
		),
	)
);

test_surreal_value!(
	vec<Vec<i64>>(
		Vec::<i64>::new() => (
			Value::Array(_),
			Kind::Array(_, _)
		),
		is(is_vec<i64>),
		into(into_vec<i64>),
		from(from_vec<i64>),
	)
);

test_surreal_value!(
	option_some<Option<i64>>(
		Some(1) => (
			Value::Number(_),
			Kind::Option(_)
		),
		is(is_option<i64>),
		into(into_option<i64>),
		from(from_option<i64>),
	)
);

test_surreal_value!(
	option_none<Option<i64>>(
		Option::<i64>::None => (
			Value::None,
			Kind::Option(_)
		),
		is(is_option<i64>),
		into(into_option<i64>),
		from(from_option<i64>),
	)
);

test_surreal_value!(
	btreemap<BTreeMap<String, i64>>(
		BTreeMap::<String, i64>::new() => (
			Value::Object(_),
			Kind::Object
		),
		into(into_btreemap<i64>),
		from(from_btreemap<i64>),
	)
);

test_surreal_value!(
	hashmap<HashMap<String, i64>>(
		HashMap::<String, i64>::new() => (
			Value::Object(_),
			Kind::Object
		),
		into(into_hashmap<i64>),
		from(from_hashmap<i64>),
	)
);

fn geo_point() -> geo::Point {
	geo::Point::new(10.0, 10.0)
}

fn geo_line() -> geo::LineString {
	geo::LineString::new(vec![
		geo::Coord {
			x: 10.0,
			y: 10.0,
		},
		geo::Coord {
			x: 20.0,
			y: 20.0,
		},
	])
}

fn geo_polygon() -> geo::Polygon {
	geo::Polygon::new(geo_line(), vec![geo_line()])
}

fn geo_multipoint() -> geo::MultiPoint {
	geo::MultiPoint::new(vec![geo::Point::new(10.0, 10.0), geo::Point::new(20.0, 20.0)])
}

fn geo_multilinestring() -> geo::MultiLineString {
	geo::MultiLineString::new(vec![geo_line()])
}

fn geo_multipolygon() -> geo::MultiPolygon {
	geo::MultiPolygon::new(vec![geo_polygon()])
}

test_surreal_value!(
	point<geo::Point>(
		geo_point() => (
			Value::Geometry(_),
			Kind::Geometry(_)
		),
		is(is_point),
		into(into_point),
		from(from_point),
	)
);

test_surreal_value!(
	line<geo::LineString>(
		geo_line() => (
			Value::Geometry(_),
			Kind::Geometry(_)
		),
		is(is_line),
		into(into_line),
		from(from_line),
	)
);

test_surreal_value!(
	polygon<geo::Polygon>(
		geo_polygon() => (
			Value::Geometry(_),
			Kind::Geometry(_)
		),
		is(is_polygon),
		into(into_polygon),
		from(from_polygon),
	)
);

test_surreal_value!(
	multipoint<geo::MultiPoint>(
		geo_multipoint() => (
			Value::Geometry(_),
			Kind::Geometry(_)
		),
		is(is_multipoint),
		into(into_multipoint),
		from(from_multipoint),
	)
);

test_surreal_value!(
	multilinestring<geo::MultiLineString>(
		geo_multilinestring() => (
			Value::Geometry(_),
			Kind::Geometry(_)
		),
	)
);

test_surreal_value!(
	multipolygon<geo::MultiPolygon>(
		geo_multipolygon() => (
			Value::Geometry(_),
			Kind::Geometry(_)
		),
	)
);

macro_rules! test_tuples {
    ($($name:ident => ($($t:ty),+)),+ $(,)?) => {
        $(
            test_surreal_value!(
                $name<($($t,)+)>(
                    ($(<$t>::default(),)+) => (
                        Value::Array(_),
                        Kind::Literal(KindLiteral::Array(_))
                    ),
                )
            );
        )+
    }
}

test_tuples! {
	tuple_1 => (i64),
	tuple_2 => (i64, i64),
	tuple_3 => (i64, i64, i64),
	tuple_4 => (i64, i64, i64, i64),
	tuple_5 => (i64, i64, i64, i64, i64),
	tuple_6 => (i64, i64, i64, i64, i64, i64),
	tuple_7 => (i64, i64, i64, i64, i64, i64, i64),
	tuple_8 => (i64, i64, i64, i64, i64, i64, i64, i64),
	tuple_9 => (i64, i64, i64, i64, i64, i64, i64, i64, i64),
	tuple_10 => (i64, i64, i64, i64, i64, i64, i64, i64, i64, i64)
}
