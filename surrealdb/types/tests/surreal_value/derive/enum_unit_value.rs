use surrealdb_types::{Number, SurrealValue, Value};

#[derive(SurrealValue, Debug, PartialEq)]
#[surreal(untagged)]
enum EnumUnitValue {
	#[surreal(value = true)]
	True,
	#[surreal(value = false)]
	False,
	#[surreal(value = null)]
	Null,
	#[surreal(value = none)]
	None,
	#[surreal(value = "Hello")]
	String,
	#[surreal(value = 123)]
	Int,
	#[surreal(value = 123.45)]
	Float,
}

#[test]
fn test_enum_unit_value() {
	// kind_of
	let kind_true = "Literal(Bool(true))";
	let kind_false = "Literal(Bool(false))";
	let kind_null = "Null";
	let kind_none = "None";
	let kind_string = "Literal(String(\"Hello\"))";
	let kind_number = "Literal(Integer(123))";
	let kind_float = "Literal(Float(123.45))";
	assert_eq!(
		format!("{:?}", EnumUnitValue::kind_of()),
		format!(
			"Either([{kind_true}, {kind_false}, {kind_null}, {kind_none}, {kind_string}, {kind_number}, {kind_float}])"
		)
	);

	// is_value
	assert!(EnumUnitValue::is_value(&Value::Bool(true)));
	assert!(EnumUnitValue::is_value(&Value::Bool(false)));
	assert!(EnumUnitValue::is_value(&Value::Null));
	assert!(EnumUnitValue::is_value(&Value::None));
	assert!(EnumUnitValue::is_value(&Value::String("Hello".to_string())));
	assert!(EnumUnitValue::is_value(&Value::Number(Number::Int(123))));
	assert!(EnumUnitValue::is_value(&Value::Number(Number::Float(123.45))));
}

#[test]
fn test_enum_unit_value_true() {
	// into_value
	let enum_unit_value = EnumUnitValue::True;
	let value = enum_unit_value.into_value();
	assert_eq!(value, Value::Bool(true));

	// from_value
	let converted = EnumUnitValue::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUnitValue::True);

	// is_value
	assert!(EnumUnitValue::is_value(&value));
	assert!(value.is::<EnumUnitValue>());
}

#[test]
fn test_enum_unit_value_false() {
	// into_value
	let enum_unit_value = EnumUnitValue::False;
	let value = enum_unit_value.into_value();
	assert_eq!(value, Value::Bool(false));

	// from_value
	let converted = EnumUnitValue::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUnitValue::False);

	// is_value
	assert!(EnumUnitValue::is_value(&value));
	assert!(value.is::<EnumUnitValue>());
}

#[test]
fn test_enum_unit_value_null() {
	// into_value
	let enum_unit_value = EnumUnitValue::Null;
	let value = enum_unit_value.into_value();
	assert_eq!(value, Value::Null);

	// from_value
	let converted = EnumUnitValue::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUnitValue::Null);

	// is_value
	assert!(EnumUnitValue::is_value(&value));
	assert!(value.is::<EnumUnitValue>());
}

#[test]
fn test_enum_unit_value_none() {
	// into_value
	let enum_unit_value = EnumUnitValue::None;
	let value = enum_unit_value.into_value();
	assert_eq!(value, Value::None);

	// from_value
	let converted = EnumUnitValue::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUnitValue::None);

	// is_value
	assert!(EnumUnitValue::is_value(&value));
	assert!(value.is::<EnumUnitValue>());
}

#[test]
fn test_enum_unit_value_string() {
	// into_value
	let enum_unit_value = EnumUnitValue::String;
	let value = enum_unit_value.into_value();
	assert_eq!(value, Value::String("Hello".to_string()));

	// from_value
	let converted = EnumUnitValue::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUnitValue::String);

	// is_value
	assert!(EnumUnitValue::is_value(&value));
	assert!(value.is::<EnumUnitValue>());
}

#[test]
fn test_enum_unit_value_int() {
	// into_value
	let enum_unit_value = EnumUnitValue::Int;
	let value = enum_unit_value.into_value();
	assert_eq!(value, Value::Number(Number::Int(123)));

	// from_value
	let converted = EnumUnitValue::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUnitValue::Int);

	// is_value
	assert!(EnumUnitValue::is_value(&value));
	assert!(value.is::<EnumUnitValue>());
}

#[test]
fn test_enum_unit_value_float() {
	// into_value
	let enum_unit_value = EnumUnitValue::Float;
	let value = enum_unit_value.into_value();
	assert_eq!(value, Value::Number(Number::Float(123.45)));

	// from_value
	let converted = EnumUnitValue::from_value(value.clone()).unwrap();
	assert_eq!(converted, EnumUnitValue::Float);

	// is_value
	assert!(EnumUnitValue::is_value(&value));
	assert!(value.is::<EnumUnitValue>());
}
