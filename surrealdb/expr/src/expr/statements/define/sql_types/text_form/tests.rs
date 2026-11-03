use super::*;
use crate::expr::statements::define::sql_types::validate;

fn declared(text: &str) -> DeclaredSqlType {
	DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, text).expect("a PostgreSQL type")
}

/// The value a text is stored as: read, then canonicalised by the type's family.
fn stored(ty: &str, text: &str) -> Value {
	let declared = declared(ty);
	let value = read(&declared, text).expect("a type with a text form").expect("a valid text");
	validate(&declared, value, "c").expect("a value of the type")
}

/// The text a value is printed as after it is stored.
fn round_trip(ty: &str, text: &str) -> String {
	let declared = declared(ty);
	write(&declared, &stored(ty, text)).expect("a stored value of the type")
}

#[test]
fn money_reads_to_cents_and_prints_as_postgresql_does() {
	assert_eq!(
		stored("money", "$1,234.567"),
		Value::Number(Number::Decimal(Decimal::new(123457, 2)))
	);
	assert_eq!(round_trip("money", "-1234.5"), "-$1,234.50");
	assert_eq!(read(&declared("money"), "12 dollars").unwrap().unwrap_err().code, "22P02");
}

#[test]
fn pg_lsn_reads_its_halves() {
	assert_eq!(stored("pg_lsn", "16/B374D848"), Value::Number(Number::U64(0x16_B374_D848)));
	assert_eq!(round_trip("pg_lsn", "16/b374d848"), "16/B374D848");
}

#[test]
fn geometric_texts_read_to_their_stored_shapes_and_print_back() {
	assert_eq!(round_trip("point", "( 1.5 , -2 )"), "(1.5,-2)");
	assert_eq!(round_trip("lseg", "(1,2),(3,4)"), "[(1,2),(3,4)]");
	assert_eq!(round_trip("path", "[(0,0),(1,1)]"), "[(0,0),(1,1)]");
	assert_eq!(round_trip("path", "((0,0),(1,1),(2,0))"), "((0,0),(1,1),(2,0))");
	assert_eq!(round_trip("polygon", "((0,0),(1,0),(1,1))"), "((0,0),(1,0),(1,1))");
	assert_eq!(round_trip("box", "(0,0),(3,4)"), "(3,4),(0,0)");
	assert_eq!(round_trip("line", "{1,-1,0}"), "{1,-1,0}");
	assert_eq!(round_trip("circle", "<(1,2),3>"), "<(1,2),3>");
}

#[test]
fn a_geometric_text_of_another_shape_is_refused() {
	assert!(read(&declared("point"), "(1,2),(3,4)").unwrap().is_err());
	let line = read(&declared("line"), "{0,0,1}").unwrap().unwrap();
	assert!(validate(&declared("line"), line, "c").is_err());
}

#[test]
fn ranges_read_their_bounds_as_the_element_kind_and_print_canonically() {
	assert_eq!(round_trip("int4range", "[1,5]"), "[1,6)");
	assert_eq!(round_trip("int4range", "(1,5)"), "[2,5)");
	assert_eq!(round_trip("int8range", "(,10]"), "(,11)");
	assert_eq!(round_trip("int4range", "empty"), "empty");
	assert_eq!(round_trip("int4range", "[3,3)"), "empty");
	assert_eq!(round_trip("numrange", "(1.5,2.25]"), "(1.5,2.25]");
	assert_eq!(round_trip("daterange", "[2024-01-01,2024-01-31]"), "[2024-01-01,2024-02-01)");
	assert_eq!(
		round_trip("tsrange", "[2024-01-01 10:00,2024-01-01 12:30:00.5)"),
		"[\"2024-01-01 10:00:00\",\"2024-01-01 12:30:00.5\")"
	);
	assert_eq!(
		round_trip("tstzrange", "[\"2024-01-01 10:00:00+02\",)"),
		"[\"2024-01-01 08:00:00+00\",)"
	);
}

#[test]
fn a_numeric_range_bound_keeps_its_written_scale() {
	assert_eq!(round_trip("numrange", "[1.50,2.00)"), "[1.50,2.00)");
}

#[test]
fn a_timestamptz_range_bound_with_a_non_ascii_offset_is_refused_not_a_panic() {
	assert!(
		read(&declared("tstzrange"), "[\"2024-01-01 10:00:00+\u{e9}\u{e9}\",)").unwrap().is_err()
	);
	assert!(read(&declared("tstzrange"), "[\"2024-01-01 10:00:00+\u{e9}\",)").unwrap().is_err());
}

#[test]
fn a_range_bound_that_is_not_its_element_is_refused() {
	assert_eq!(read(&declared("int4range"), "[a,5)").unwrap().unwrap_err().code, "22P02");
	assert!(read(&declared("int4range"), "[1,5").unwrap().is_err());
	let reversed = read(&declared("int4range"), "[5,1)").unwrap().unwrap();
	assert_eq!(validate(&declared("int4range"), reversed, "c").unwrap_err().code, "22000");
}

#[test]
fn multiranges_are_merged_and_printed_in_braces() {
	assert_eq!(round_trip("int4multirange", "{[5,7), [1,3), [3,4)}"), "{[1,4),[5,7)}");
	assert_eq!(round_trip("int4multirange", "{}"), "{}");
}

#[test]
fn types_without_a_text_form_of_their_own_are_not_read() {
	assert!(read(&declared("inet"), "10.0.0.1").is_none());
	assert!(read(&declared("text"), "x").is_none());
	assert!(read(&declared("point[]"), "{}").is_none());
	let mysql = DeclaredSqlType::parse(SqlTypeDialect::MySql, "point").unwrap();
	assert!(read(&mysql, "(1,2)").is_none());
}
