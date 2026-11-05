#![allow(clippy::unwrap_used)]
//! Round-trip tests for sized number kinds (`i8`…`u64`, `f16`, `f32`) through
//! the Rust SDK's `SurrealValue` trait and the embedded engine.

use surrealdb::types::{Kind, Number, SurrealValue, Value, f16};

#[derive(Debug, Clone, PartialEq, SurrealValue)]
struct SizedRow {
	a_i8: i8,
	a_u8: u8,
	a_i16: i16,
	a_u16: u16,
	a_i32: i32,
	a_u32: u32,
	a_u64: u64,
	a_i64: i64,
	a_f16: f16,
	a_f32: f32,
	a_f64: f64,
}

#[test]
fn surreal_value_into_value_widens_and_from_value_round_trips() {
	let row = SizedRow {
		a_i8: -12,
		a_u8: 250,
		a_i16: -3000,
		a_u16: 60000,
		a_i32: -100_000,
		a_u32: 4_000_000_000,
		a_u64: 10_000_000_000_000_000_000,
		a_i64: -9_000_000_000,
		a_f16: f16::from_f32(1.5),
		a_f32: 2.25,
		a_f64: 3.125,
	};

	let val = row.clone().into_value();
	let Value::Object(ref obj) = val else {
		panic!("expected object value");
	};
	let expect_kind = |field: &str, kind: Kind| {
		let Some(Value::Number(n)) = obj.get(field) else {
			panic!("expected number for {field}");
		};
		assert_eq!(n.kind(), kind, "field {field}");
	};
	expect_kind("a_i8", Kind::Int);
	expect_kind("a_u8", Kind::Int);
	expect_kind("a_i16", Kind::Int);
	expect_kind("a_u16", Kind::Int);
	expect_kind("a_i32", Kind::Int);
	expect_kind("a_u32", Kind::Int);
	expect_kind("a_u64", Kind::Decimal);
	expect_kind("a_i64", Kind::Int);
	expect_kind("a_f16", Kind::Float);
	expect_kind("a_f32", Kind::Float);
	expect_kind("a_f64", Kind::Float);

	let decoded = SizedRow::from_value(val).unwrap();
	assert_eq!(decoded, row);
}

#[test]
fn surreal_value_from_value_accepts_any_fitting_number_kind_and_refuses_overflow() {
	assert_eq!(u8::from_value(Value::Number(Number::Int(200))).unwrap(), 200u8);
	assert_eq!(u8::from_value(Value::Number(Number::I16(200))).unwrap(), 200u8);
	assert!(u8::from_value(Value::Number(Number::Int(256))).is_err());
	assert!(u8::from_value(Value::Number(Number::I8(-1))).is_err());

	assert_eq!(i8::from_value(Value::Number(Number::U8(127))).unwrap(), 127i8);
	assert!(i8::from_value(Value::Number(Number::U8(128))).is_err());

	assert_eq!(i64::from_value(Value::Number(Number::U8(42))).unwrap(), 42i64);
	assert_eq!(f32::from_value(Value::Number(Number::F16(f16::from_f32(1.5)))).unwrap(), 1.5f32);
	assert_eq!(f16::from_value(Value::Number(Number::Float(1.5))).unwrap(), f16::from_f32(1.5));
	assert_eq!(f64::from_value(Value::Number(Number::F32(2.25))).unwrap(), 2.25f64);
}

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn embedded_engine_round_trips_sized_numbers_and_bound_parameters() {
	use surrealdb::Surreal;
	use surrealdb::engine::local::Mem;

	let db = Surreal::new::<Mem>(()).await.unwrap();
	db.use_ns("test").use_db("test").await.unwrap();

	db.query(
		"DEFINE TABLE metrics SCHEMAFULL;
		 DEFINE FIELD a_i8 ON metrics TYPE i8;
		 DEFINE FIELD a_u8 ON metrics TYPE u8;
		 DEFINE FIELD a_i16 ON metrics TYPE i16;
		 DEFINE FIELD a_u16 ON metrics TYPE u16;
		 DEFINE FIELD a_i32 ON metrics TYPE i32;
		 DEFINE FIELD a_u32 ON metrics TYPE u32;
		 DEFINE FIELD a_u64 ON metrics TYPE u64;
		 DEFINE FIELD a_i64 ON metrics TYPE int;
		 DEFINE FIELD a_f16 ON metrics TYPE f16;
		 DEFINE FIELD a_f32 ON metrics TYPE f32;
		 DEFINE FIELD a_f64 ON metrics TYPE float;",
	)
	.await
	.unwrap()
	.check()
	.unwrap();

	let input = SizedRow {
		a_i8: -128,
		a_u8: 255,
		a_i16: -32768,
		a_u16: 65535,
		a_i32: -2_147_483_648,
		a_u32: 4_294_967_295,
		a_u64: u64::MAX,
		a_i64: i64::MIN,
		a_f16: f16::from_f32(-6.5),
		a_f32: 123.5,
		a_f64: -987.125,
	};

	let mut resp = db
		.query(
			"CREATE ONLY metrics:1 CONTENT {
				a_i8: $i8,
				a_u8: $u8,
				a_i16: $i16,
				a_u16: $u16,
				a_i32: $i32,
				a_u32: $u32,
				a_u64: $u64,
				a_i64: $i64,
				a_f16: $f16,
				a_f32: $f32,
				a_f64: $f64
			};
			RETURN [
				type::of($i8),
				type::of($u8),
				type::of($i16),
				type::of($u16),
				type::of($i32),
				type::of($u32),
				type::of($u64),
				type::of($i64),
				type::of($f16),
				type::of($f32),
				type::of($f64)
			];",
		)
		.bind(("i8", input.a_i8))
		.bind(("u8", input.a_u8))
		.bind(("i16", input.a_i16))
		.bind(("u16", input.a_u16))
		.bind(("i32", input.a_i32))
		.bind(("u32", input.a_u32))
		.bind(("u64", input.a_u64))
		.bind(("i64", input.a_i64))
		.bind(("f16", input.a_f16))
		.bind(("f32", input.a_f32))
		.bind(("f64", input.a_f64))
		.await
		.unwrap()
		.check()
		.unwrap();

	let created: Option<SizedRow> = resp.take(0).unwrap();
	assert_eq!(created.as_ref(), Some(&input));

	let kinds: Vec<String> = resp.take(1).unwrap();
	assert_eq!(
		kinds,
		vec!["int", "int", "int", "int", "int", "int", "decimal", "int", "float", "float", "float"]
	);

	let mut select_resp =
		db.query("SELECT * OMIT id FROM ONLY metrics:1").await.unwrap().check().unwrap();
	let raw: Value = select_resp.take(0).unwrap();
	let Value::Object(ref obj) = raw else {
		panic!("expected object from SELECT: {raw:?}");
	};
	assert_eq!(obj.get("a_u8"), Some(&Value::Number(Number::U8(255))));
	assert_eq!(obj.get("a_u64"), Some(&Value::Number(Number::U64(u64::MAX))));
	assert_eq!(obj.get("a_f16"), Some(&Value::Number(Number::F16(f16::from_f32(-6.5)))));
	assert_eq!(obj.get("a_f32"), Some(&Value::Number(Number::F32(123.5))));
}
