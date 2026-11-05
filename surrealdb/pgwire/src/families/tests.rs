//! The declared-type families through the wire's own entry points: a cell encoded in each format
//! for its column type, and a parameter of the type's OID decoded from each format.

use std::ops::Bound;

use rust_decimal::Decimal;
use surrealdb_types::{Array, Date, Geometry, Number, Object, Range, Value};

use crate::encode::{decode_param, encode_binary, encode_text};
use crate::typing::PgType;

fn text(value: Value, ty: &PgType) -> String {
	String::from_utf8(encode_text(value, ty).expect("encodes as text")).expect("UTF-8")
}

fn binary(value: Value, ty: &PgType) -> Vec<u8> {
	encode_binary(value, ty).expect("encodes as binary")
}

/// A parameter of `ty` sent in the text format.
fn from_text(input: &str, ty: &PgType) -> Value {
	decode_param(Some(input.as_bytes()), ty.oid(), 0).expect("decodes from text")
}

/// A parameter of `ty` sent in the binary format.
fn from_binary(bytes: &[u8], ty: &PgType) -> Value {
	decode_param(Some(bytes), ty.oid(), 1).expect("decodes from binary")
}

fn decimal(text: &str) -> Value {
	Value::Number(Number::Decimal(text.parse::<Decimal>().expect("a decimal")))
}

fn point(x: f64, y: f64) -> Value {
	Value::Geometry(Geometry::Point((x, y).into()))
}

fn int_range(start: Bound<i32>, end: Bound<i32>) -> Value {
	let bound = |bound: Bound<i32>| match bound {
		Bound::Included(n) => Bound::Included(Value::Number(Number::I32(n))),
		Bound::Excluded(n) => Bound::Excluded(Value::Number(Number::I32(n))),
		Bound::Unbounded => Bound::Unbounded,
	};
	Value::Range(Box::new(Range::new(bound(start), bound(end))))
}

#[test]
fn money_is_its_currency_text_and_its_cents() {
	let ty = PgType::Money;
	assert_eq!(text(decimal("1234.56"), &ty), "$1,234.56");
	assert_eq!(text(decimal("-0.5"), &ty), "-$0.50");
	assert_eq!(binary(decimal("1234.56"), &ty), 123_456i64.to_be_bytes().to_vec());
	assert_eq!(from_text("$1,234.56", &ty), decimal("1234.56"));
	assert_eq!(from_binary(&(-5i64).to_be_bytes(), &ty), decimal("-0.05"));
	assert!(decode_param(Some(b"twelve"), ty.oid(), 0).is_err());
}

#[test]
fn pg_lsn_is_its_two_halves_and_its_u64() {
	let ty = PgType::PgLsn;
	let lsn = Value::Number(Number::U64(0x16_B374_D848));
	assert_eq!(text(lsn.clone(), &ty), "16/B374D848");
	assert_eq!(binary(lsn.clone(), &ty), 0x16_B374_D848u64.to_be_bytes().to_vec());
	assert_eq!(from_text("16/b374d848", &ty), lsn);
	assert_eq!(from_binary(&0x16_B374_D848u64.to_be_bytes(), &ty), lsn);
}

#[test]
fn a_binary_cidr_has_the_cidr_flag_and_no_host_bits() {
	let refused = |bytes: &[u8], ty: &PgType| decode_param(Some(bytes), ty.oid(), 1).is_err();
	// 10.0.0.1/8 has a host bit set past the prefix.
	assert!(refused(&[2, 8, 1, 4, 10, 0, 0, 1], &PgType::Cidr));
	// The header's type flag is not read: clients send it clear for a `cidr` and set for an
	// `inet`, and the parameter's own type decides.
	assert!(!refused(&[2, 8, 0, 4, 10, 0, 0, 0], &PgType::Cidr));
	assert!(!refused(&[2, 24, 1, 4, 192, 168, 1, 0], &PgType::Inet));
	assert!(refused(&[2, 8, 0, 4, 10, 0, 0, 1], &PgType::Cidr));
	assert!(!refused(&[2, 24, 0, 4, 192, 168, 1, 5], &PgType::Inet));
}

#[test]
fn network_and_mac_types_send_their_canonical_text_and_binary_layouts() {
	let inet = Value::String("192.168.1.5/24".to_string());
	assert_eq!(text(inet.clone(), &PgType::Inet), "192.168.1.5/24");
	assert_eq!(binary(inet, &PgType::Inet), vec![2, 24, 0, 4, 192, 168, 1, 5]);
	let cidr = binary(Value::String("10.0.0.0/8".to_string()), &PgType::Cidr);
	assert_eq!(cidr, vec![2, 8, 1, 4, 10, 0, 0, 0]);
	assert_eq!(from_binary(&cidr, &PgType::Cidr), Value::String("10.0.0.0/8".to_string()));
	assert_eq!(
		from_binary(
			&[3, 128, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
			&PgType::Inet
		),
		Value::String("::1".to_string())
	);
	let mac8 = Value::String("08:00:2b:01:02:03:04:05".to_string());
	assert_eq!(binary(mac8.clone(), &PgType::Macaddr8), vec![8, 0, 0x2b, 1, 2, 3, 4, 5]);
	assert_eq!(from_binary(&[8, 0, 0x2b, 1, 2, 3, 4, 5], &PgType::Macaddr8), mac8);
	assert_eq!(
		from_binary(&[8, 0, 0x2b, 1, 2, 3], &PgType::MacAddr),
		Value::String("08:00:2b:01:02:03".to_string())
	);
}

#[test]
fn bit_strings_send_their_length_and_packed_bits() {
	let bits = Value::String("101".to_string());
	assert_eq!(text(bits.clone(), &PgType::Varbit), "101");
	assert_eq!(binary(bits.clone(), &PgType::Bit), vec![0, 0, 0, 3, 0b1010_0000]);
	assert_eq!(from_binary(&[0, 0, 0, 3, 0b1010_0000], &PgType::Varbit), bits);
}

#[test]
fn identifiers_are_their_digits_and_their_width() {
	let id = Value::Number(Number::U32(2_206));
	assert_eq!(text(id.clone(), &PgType::Regtype), "2206");
	assert_eq!(binary(id.clone(), &PgType::Xid), 2_206u32.to_be_bytes().to_vec());
	assert_eq!(from_binary(&2_206u32.to_be_bytes(), &PgType::Cid), id);
	let wide = Value::Number(Number::U64(1 << 40));
	assert_eq!(binary(wide.clone(), &PgType::Xid8), (1u64 << 40).to_be_bytes().to_vec());
	assert_eq!(from_text("1099511627776", &PgType::Xid8), wide);
}

#[test]
fn geometric_values_take_the_postgresql_forms_of_their_column() {
	assert_eq!(text(point(1.0, -2.5), &PgType::Point), "(1,-2.5)");
	let mut bytes = 1.0f64.to_be_bytes().to_vec();
	bytes.extend_from_slice(&(-2.5f64).to_be_bytes());
	assert_eq!(binary(point(1.0, -2.5), &PgType::Point), bytes);
	assert_eq!(from_binary(&bytes, &PgType::Point), point(1.0, -2.5));
	assert_eq!(from_text("(1,-2.5)", &PgType::Point), point(1.0, -2.5));

	let segment = Value::Geometry(Geometry::Line(vec![(0.0, 0.0), (3.0, 4.0)].into()));
	assert_eq!(text(segment.clone(), &PgType::Lseg), "[(0,0),(3,4)]");
	assert_eq!(text(segment.clone(), &PgType::Path), "[(0,0),(3,4)]");
	assert_eq!(from_text("[(0,0),(3,4)]", &PgType::Lseg), segment);

	let square = from_text("((0,0),(1,0),(1,1),(0,1))", &PgType::Polygon);
	assert_eq!(text(square.clone(), &PgType::Polygon), "((0,0),(1,0),(1,1),(0,1))");
	assert_eq!(text(square.clone(), &PgType::Path), "((0,0),(1,0),(1,1),(0,1))");
	assert_eq!(text(square.clone(), &PgType::PgBox), "(1,1),(0,0)");
	let boxed = binary(square, &PgType::PgBox);
	assert_eq!(&boxed[..8], &1.0f64.to_be_bytes(), "the high corner comes first");
	assert_eq!(
		text(from_binary(&boxed, &PgType::PgBox), &PgType::PgBox),
		"(1,1),(0,0)",
		"a box read from the wire prints as it was sent"
	);

	let mut line = Object::new();
	line.insert("a".to_string(), Value::Number(Number::Float(1.0)));
	line.insert("b".to_string(), Value::Number(Number::Float(-1.0)));
	line.insert("c".to_string(), Value::Number(Number::Float(0.0)));
	assert_eq!(text(Value::Object(line.clone()), &PgType::Line), "{1,-1,0}");
	assert_eq!(from_text("{1,-1,0}", &PgType::Line), Value::Object(line));
	assert_eq!(from_text("<(1,2),3>", &PgType::Circle), {
		let mut circle = Object::new();
		circle.insert("x".to_string(), Value::Number(Number::Float(1.0)));
		circle.insert("y".to_string(), Value::Number(Number::Float(2.0)));
		circle.insert("r".to_string(), Value::Number(Number::Float(3.0)));
		Value::Object(circle)
	});
}

#[test]
fn a_geometric_column_refuses_a_value_of_another_shape() {
	assert!(encode_text(point(1.0, 2.0), &PgType::Polygon).is_err());
	assert!(encode_binary(Value::Number(Number::Int(1)), &PgType::Point).is_err());
}

#[test]
fn a_string_cell_is_the_text_of_its_column_type() {
	assert_eq!(text(Value::String("(1,2)".to_string()), &PgType::Point), "(1,2)");
	let mut bytes = 1.0f64.to_be_bytes().to_vec();
	bytes.extend_from_slice(&2.0f64.to_be_bytes());
	assert_eq!(binary(Value::String("(1,2)".to_string()), &PgType::Point), bytes);
}

#[test]
fn ranges_use_the_codec_of_their_element() {
	let range = int_range(Bound::Included(1), Bound::Excluded(5));
	assert_eq!(text(range.clone(), &PgType::Int4range), "[1,5)");
	assert_eq!(
		binary(range.clone(), &PgType::Int4range),
		vec![0x02, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 5]
	);
	let decoded = from_binary(&binary(range, &PgType::Int4range), &PgType::Int4range);
	assert_eq!(text(decoded, &PgType::Int4range), "[1,5)");
	let empty = int_range(Bound::Included(3), Bound::Excluded(3));
	assert_eq!(text(empty.clone(), &PgType::Int4range), "empty");
	assert_eq!(binary(empty, &PgType::Int4range), vec![0x01]);
	let unbounded = int_range(Bound::Unbounded, Bound::Included(7));
	assert_eq!(text(unbounded, &PgType::Int4range), "(,7]");

	let days = from_text("[2024-01-01,2024-02-01)", &PgType::Daterange);
	let Value::Range(range) = &days else {
		panic!("a date range, found {days:?}");
	};
	assert_eq!(range.start, Bound::Included(Value::Date("2024-01-01".parse::<Date>().unwrap())));
	assert_eq!(text(days, &PgType::Daterange), "[2024-01-01,2024-02-01)");

	let stamps = from_text("[\"2024-01-01 10:00:00\",)", &PgType::Tsrange);
	assert_eq!(text(stamps, &PgType::Tsrange), "[\"2024-01-01 10:00:00\",)");
	assert_eq!(text(from_text("(1.5,2]", &PgType::Numrange), &PgType::Numrange), "(1.5,2]");
}

#[test]
fn multiranges_are_arrays_of_ranges_on_both_formats() {
	let ranges = Value::Array(Array::from(vec![
		int_range(Bound::Included(1), Bound::Excluded(3)),
		int_range(Bound::Included(5), Bound::Excluded(7)),
	]));
	assert_eq!(text(ranges.clone(), &PgType::Int4multirange), "{[1,3),[5,7)}");
	let bytes = binary(ranges, &PgType::Int4multirange);
	assert_eq!(&bytes[..4], &2i32.to_be_bytes());
	let decoded = from_binary(&bytes, &PgType::Int4multirange);
	assert_eq!(text(decoded, &PgType::Int4multirange), "{[1,3),[5,7)}");
	assert_eq!(text(from_text("{}", &PgType::Int8multirange), &PgType::Int8multirange), "{}");
}

#[test]
fn an_array_of_a_family_type_decodes_each_element_by_its_type() {
	let points = decode_param(Some(b"{\"(1,2)\",\"(3,4)\"}"), 1017, 0).expect("a point array");
	assert_eq!(points, Value::Array(Array::from(vec![point(1.0, 2.0), point(3.0, 4.0)])));
}
