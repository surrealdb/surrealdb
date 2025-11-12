use surrealdb_types::{GeometryKind, Kind, kind};

#[test]
fn test_kind_proc_macro() {
	macro_rules! cases {
        ($(($($x:tt)+) => $y:expr),*$(,)?) => {
            $(
                assert_eq!(kind!($($x)+), $y);
            )*
        }
    }

	cases! {
		(any) => Kind::Any,
		(none) => Kind::None,
		(null) => Kind::Null,
		(bool) => Kind::Bool,
		(bytes) => Kind::Bytes,
		(datetime) => Kind::Datetime,
		(decimal) => Kind::Decimal,
		(duration) => Kind::Duration,
		(float) => Kind::Float,
		(int) => Kind::Int,
		(number) => Kind::Number,
		(object) => Kind::Object,
		(string) => Kind::String,
		(uuid) => Kind::Uuid,
		(regex) => Kind::Regex,
		(range) => Kind::Range,
		(function) => Kind::Function(None, None),

	// Tables
	(table) => Kind::Table(vec![]),
	(table<user>) => Kind::Table(vec!["user".to_string()]),
	(table<user | post>) => Kind::Table(vec!["user".to_string(), "post".to_string()]),

	// Records
	(record) => Kind::Record(vec![]),
	(record<user>) => Kind::Record(vec!["user".to_string()]),
	(record<user | post>) => Kind::Record(vec!["user".to_string(), "post".to_string()]),

		// Geometries
		(geometry) => Kind::Geometry(vec![]),
		(geometry<point>) => Kind::Geometry(vec![GeometryKind::Point]),
		(geometry<point | line | polygon | multipoint | multiline | multipolygon | collection>) => Kind::Geometry(vec![
			GeometryKind::Point,
			GeometryKind::Line,
			GeometryKind::Polygon,
			GeometryKind::MultiPoint,
			GeometryKind::MultiLine,
			GeometryKind::MultiPolygon,
			GeometryKind::Collection
		]),

		// Sets
		(set) => Kind::Set(Box::new(Kind::Any), None),
		(set<string>) => Kind::Set(Box::new(Kind::String), None),
		(set<string | int>) => Kind::Set(Box::new(Kind::Either(vec![Kind::String, Kind::Int])), None),
		(set<string, 10>) => Kind::Set(Box::new(Kind::String), Some(10)),
		(set<string | int, 10>) => Kind::Set(Box::new(Kind::Either(vec![Kind::String, Kind::Int])), Some(10)),

		// Arrays
		(array) => Kind::Array(Box::new(Kind::Any), None),
		(array<string>) => Kind::Array(Box::new(Kind::String), None),
		(array<string | int>) => Kind::Array(Box::new(Kind::Either(vec![Kind::String, Kind::Int])), None),
		(array<string, 10>) => Kind::Array(Box::new(Kind::String), Some(10)),
		(array<string | int, 10>) => Kind::Array(Box::new(Kind::Either(vec![Kind::String, Kind::Int])), Some(10)),

		// Files
		(file) => Kind::File(vec![]),
		(file<one>) => Kind::File(vec!["one".to_string()]),
		(file<one | two>) => Kind::File(vec!["one".to_string(), "two".to_string()]),

		// Union types
		(string | bool) => Kind::Either(vec![Kind::String, Kind::Bool]),
		(string | int | bool) => Kind::Either(vec![Kind::String, Kind::Int, Kind::Bool]),

		// Literals
		(true) => Kind::Literal(surrealdb_types::KindLiteral::Bool(true)),
		(false) => Kind::Literal(surrealdb_types::KindLiteral::Bool(false)),
		(42) => Kind::Literal(surrealdb_types::KindLiteral::Integer(42)),
		("hello") => Kind::Literal(surrealdb_types::KindLiteral::String("hello".to_string())),

		// Kind:: prefix
		(Kind::String) => Kind::String,
		(Kind::Bool) => Kind::Bool,

		// Literal:: prefix
		(Literal::Bool(true)) => Kind::Literal(surrealdb_types::KindLiteral::Bool(true)),
		(Literal::Integer(42)) => Kind::Literal(surrealdb_types::KindLiteral::Integer(42)),

		// Parenthesized expressions (escape hatch)
		((surrealdb_types::Kind::String)) => Kind::String,
		(array<(surrealdb_types::Kind::String)>) => Kind::Array(Box::new(Kind::String), None),
		(set<(surrealdb_types::Kind::Int), 5>) => Kind::Set(Box::new(Kind::Int), Some(5)),

		// Object literals
		({ status: string }) => Kind::Literal(surrealdb_types::KindLiteral::Object(
			std::collections::BTreeMap::from([
				("status".to_string(), Kind::String)
			])
		)),
		({ status: string, "user-id": int }) => Kind::Literal(surrealdb_types::KindLiteral::Object(
			std::collections::BTreeMap::from([
				("status".to_string(), Kind::String),
				("user-id".to_string(), Kind::Int)
			])
		)),

		// Union of object literals (like the user's example)
		({ status: Literal::String("OK".to_string()), result: any } | { status: Literal::String("ERR".to_string()), result: string }) => Kind::Either(vec![
			Kind::Literal(surrealdb_types::KindLiteral::Object(
				std::collections::BTreeMap::from([
					("status".to_string(), Kind::Literal(surrealdb_types::KindLiteral::String("OK".to_string()))),
					("result".to_string(), Kind::Any)
				])
			)),
			Kind::Literal(surrealdb_types::KindLiteral::Object(
				std::collections::BTreeMap::from([
					("status".to_string(), Kind::Literal(surrealdb_types::KindLiteral::String("ERR".to_string()))),
					("result".to_string(), Kind::String)
				])
			))
		]),

		// Array literals
		([]) => Kind::Literal(surrealdb_types::KindLiteral::Array(vec![])),
		([string]) => Kind::Literal(surrealdb_types::KindLiteral::Array(vec![Kind::String])),
		([string, int, bool]) => Kind::Literal(surrealdb_types::KindLiteral::Array(vec![
			Kind::String,
			Kind::Int,
			Kind::Bool
		])),
		([string | int, bool]) => Kind::Literal(surrealdb_types::KindLiteral::Array(vec![
			Kind::Either(vec![Kind::String, Kind::Int]),
			Kind::Bool
		]))
	}
}
