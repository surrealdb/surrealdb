use super::super::super::SqlTypeDialect;
use super::super::super::declared_type::DeclaredSqlType;
use super::super::validate;
use crate::val::{Array, Number, TableName, Value};

fn json(declared: &str, value: Value) -> Result<Value, String> {
	let declared = DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, declared)
		.expect("a valid declared type");
	validate(&declared, value, "c")
		.map_err(|violation| format!("{}: {}", violation.code, violation.message))
}

#[test]
fn a_json_column_holds_an_array_or_a_scalar() {
	let array = Value::Array(Array(vec![Value::Number(Number::Int(1)), Value::Null]));
	assert!(json("json", array).is_ok());
	assert!(json("jsonb", Value::Bool(true)).is_ok());
	assert!(json("jsonb", Value::Number(Number::Float(1.5))).is_ok());
}

#[test]
fn a_json_column_refuses_a_value_json_cannot_carry() {
	let refused = json("jsonb", Value::Table(TableName::from("t"))).unwrap_err();
	assert_eq!(refused, "22032: JSON cannot hold a table");
	let nested = Value::Array(Array(vec![Value::Number(Number::Float(f64::NAN))]));
	assert_eq!(json("json", nested).unwrap_err(), "22032: JSON cannot hold a non-finite number");
	for narrow in [Number::F32(f32::NAN), Number::F16(half::f16::INFINITY)] {
		assert!(json("jsonb", Value::Number(narrow.clone())).is_err(), "{narrow:?}");
	}
	assert!(json("jsonb", Value::Number(Number::F32(1.5))).is_ok());
}

#[test]
fn a_type_that_is_not_json_is_left_to_the_other_families() {
	assert!(json("text", Value::Table(TableName::from("t"))).is_ok());
}
