use std::ops::Bound;

use surrealdb_expr::expr::Expr;
use surrealdb_expr::parser;
use surrealdb_types::{Array, Object, Range, Set, ToSql, Value};

#[test]
fn catalog_public_literals_keep_canonical_sets_and_ranges() {
	let set = Value::Set(Set::from(vec![
		Value::Number(surrealdb_types::Number::Int(1)),
		Value::Number(surrealdb_types::Number::Int(2)),
	]));
	let object = Value::Object(Object::from(std::collections::BTreeMap::from([(
		"nested".to_owned(),
		set.clone(),
	)])));
	let mut values = vec![
		set.clone(),
		object.clone(),
		Value::Array(Array::from(vec![object.clone(), set.clone()])),
	];
	let keys = [
		surrealdb_types::RecordIdKey::Array(Array::from(vec![set])),
		surrealdb_types::RecordIdKey::Object(match object {
			Value::Object(object) => object,
			_ => unreachable!(),
		}),
	];
	for key in keys {
		values.push(Value::RecordId(surrealdb_types::RecordId::new("t", key.clone())));
		values.push(Value::RecordId(surrealdb_types::RecordId::new(
			"t",
			surrealdb_types::RecordIdKey::Range(Box::new(surrealdb_types::RecordIdKeyRange {
				start: Bound::Included(key),
				end: Bound::Unbounded,
			})),
		)));
	}

	for start in [
		Bound::Unbounded,
		Bound::Included(Value::Number(surrealdb_types::Number::Int(1))),
		Bound::Excluded(Value::Number(surrealdb_types::Number::Int(1))),
	] {
		for end in [
			Bound::Unbounded,
			Bound::Included(Value::Number(surrealdb_types::Number::Int(2))),
			Bound::Excluded(Value::Number(surrealdb_types::Number::Int(2))),
		] {
			values.push(Value::Range(Box::new(Range::new(start.clone(), end))));
		}
	}
	for value in values {
		let direct = Expr::from_surface_value(value);
		let restored = parser::expr_for_definition(&direct.to_sql()).unwrap();
		assert_eq!(direct.to_sql(), restored.to_sql());
		assert_eq!(direct.to_sql_pretty(), restored.to_sql_pretty());
	}
}

#[test]
fn parameter_keywords_keep_the_canonical_schema_spelling() {
	for source in ["DEFINE PARAM $value VALUE 1", "REMOVE PARAM $value"] {
		let direct = parser::parse(source).unwrap();
		let restored = parser::parse(&direct.to_sql()).unwrap();
		assert_eq!(direct.to_sql(), restored.to_sql());
		assert_eq!(direct.to_sql_pretty(), restored.to_sql_pretty());
		assert!(!direct.to_sql().contains("$`value`"));
	}
}

#[test]
fn geojson_preserves_canonical_text_and_geometry_semantics() {
	use revision::{DeserializeRevisioned, SerializeRevisioned};
	use surrealdb_expr::expr::logical::{LogicalExpr, LogicalLiteral, lower_expr};
	use surrealdb_expr::val::Value;

	for source in [
		"{ type: 'Point', coordinates: [1, 2] }",
		"{ type: 'GeometryCollection', geometries: [{ type: 'Point', coordinates: [1, 2] }] }",
	] {
		let expr = parser::expr_for_definition(source).unwrap();
		let Some(Value::Geometry(static_geometry)) = expr.as_static_value() else {
			panic!("GeoJSON must denote a geometry in static schema clauses: {source}");
		};
		let LogicalExpr::Literal(LogicalLiteral::Geometry(planned_geometry)) =
			lower_expr(expr.clone()).unwrap()
		else {
			panic!("GeoJSON must denote a geometry in planned expressions: {source}");
		};
		assert_eq!(static_geometry, planned_geometry);
		assert!(expr.to_sql().contains("[1, 2]"), "canonical coordinates retain their spelling");
		let mut bytes = Vec::new();
		expr.serialize_revisioned(&mut bytes).unwrap();
		let restored = Expr::deserialize_revisioned(&mut bytes.as_slice()).unwrap();
		assert_eq!(expr.to_stored_sql(), restored.to_stored_sql());
	}
}
