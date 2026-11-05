use super::*;

fn pg(text: &str) -> DeclaredSqlType {
	DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, text).expect("a PostgreSQL type")
}

fn my(text: &str) -> DeclaredSqlType {
	DeclaredSqlType::parse(SqlTypeDialect::MySql, text).expect("a MySQL type")
}

fn pg_report(declared: &DeclaredSqlType) -> PgReportedType {
	pg_reported_type(declared).expect("a PostgreSQL report")
}

fn my_report(declared: &DeclaredSqlType) -> MySqlReportedType {
	mysql_reported_type(declared).expect("a MySQL report")
}

#[test]
fn postgres_lengths_have_the_typmod_of_their_length_plus_four() {
	let varchar = pg_report(&pg("varchar(20)"));
	assert_eq!((varchar.oid, varchar.typmod), (1043, 24));
	assert_eq!(varchar.format, "character varying(20)");
	assert_eq!(varchar.data_type, "character varying");
	assert_eq!(varchar.udt_name, "varchar");
	assert_eq!(varchar.character_maximum_length, Some(20));
	assert_eq!(varchar.character_octet_length, Some(80));
	assert_eq!(varchar.blank_padded_to, None);

	let bpchar = pg_report(&pg("char(5)"));
	assert_eq!((bpchar.oid, bpchar.typmod), (1042, 9));
	assert_eq!(bpchar.format, "character(5)");
	assert_eq!(bpchar.data_type, "character");
	assert_eq!(bpchar.udt_name, "bpchar");
	assert_eq!(bpchar.character_maximum_length, Some(5));
	assert_eq!(bpchar.blank_padded_to, Some(5));

	// `char` alone is `char(1)`; `varchar` alone has no limit.
	assert_eq!(pg_report(&pg("character")).typmod, 5);
	let unbounded = pg_report(&pg("character varying"));
	assert_eq!((unbounded.typmod, unbounded.format.as_str()), (-1, "character varying"));
	assert_eq!(unbounded.character_maximum_length, None);
	assert_eq!(unbounded.character_octet_length, Some(1_073_741_824));
}

#[test]
fn postgres_numeric_packs_precision_and_scale() {
	let numeric = pg_report(&pg("numeric(10,2)"));
	assert_eq!((numeric.oid, numeric.typmod), (1700, (10 << 16 | 2) + 4));
	assert_eq!(numeric.format, "numeric(10,2)");
	assert_eq!(numeric.numeric_precision, Some(10));
	assert_eq!(numeric.numeric_precision_radix, Some(10));
	assert_eq!(numeric.numeric_scale, Some(2));
	assert_eq!(pg_report(&pg("decimal(7)")).format, "numeric(7,0)");
	let unconstrained = pg_report(&pg("numeric"));
	assert_eq!((unconstrained.typmod, unconstrained.format.as_str()), (-1, "numeric"));
	assert_eq!(unconstrained.numeric_precision, None);
	assert_eq!(unconstrained.numeric_scale, None);
}

#[test]
fn postgres_temporal_precision_sits_before_the_zone_words() {
	let ts = pg_report(&pg("timestamp(3)"));
	assert_eq!((ts.oid, ts.typmod), (1114, 3));
	assert_eq!(ts.format, "timestamp(3) without time zone");
	assert_eq!(ts.data_type, "timestamp without time zone");
	assert_eq!(ts.datetime_precision, Some(3));
	let tstz = pg_report(&pg("timestamp(0) with time zone"));
	assert_eq!((tstz.oid, tstz.typmod), (1184, 0));
	assert_eq!(tstz.format, "timestamp(0) with time zone");
	assert_eq!(pg_report(&pg("timetz(2)")).format, "time(2) with time zone");
	assert_eq!(pg_report(&pg("time")).format, "time without time zone");
	// PostgreSQL reduces a precision past 6 to 6.
	let wide = pg_report(&pg("timestamp(9)"));
	assert_eq!((wide.typmod, wide.format.as_str()), (6, "timestamp(6) without time zone"));
	assert_eq!(wide.datetime_precision, Some(6));
	assert_eq!(pg_report(&pg("interval(9)")).format, "interval(6)");
	assert_eq!(pg_report(&pg("time")).datetime_precision, Some(6));
	let interval = pg_report(&pg("interval(3)"));
	assert_eq!(interval.typmod, (0x7FFF << 16) | 3);
	assert_eq!(interval.format, "interval(3)");
	assert_eq!(pg_report(&pg("date")).datetime_precision, Some(0));
}

#[test]
fn postgres_bits_have_the_typmod_of_their_length() {
	let bit = pg_report(&pg("bit(8)"));
	assert_eq!((bit.oid, bit.typmod, bit.format.as_str()), (1560, 8, "bit(8)"));
	assert_eq!(bit.character_maximum_length, Some(8));
	let varbit = pg_report(&pg("bit varying(12)"));
	assert_eq!((varbit.oid, varbit.typmod, varbit.format.as_str()), (1562, 12, "bit varying(12)"));
	assert_eq!(pg_report(&pg("bit")).typmod, 1);
}

#[test]
fn postgres_arrays_keep_the_element_modifier() {
	let array = pg_report(&pg("varchar(20)[]"));
	assert_eq!((array.oid, array.typmod), (1015, 24));
	assert_eq!(array.format, "character varying(20)[]");
	assert_eq!(array.data_type, "ARRAY");
	assert_eq!(array.udt_name, "_varchar");
	assert_eq!(array.character_maximum_length, None);
	assert_eq!(array.blank_padded_to, None);
	assert_eq!(pg_report(&pg("int4[][]")).oid, 1007);
}

#[test]
fn postgres_types_without_modifiers_are_named() {
	for (text, oid, format) in [
		("inet", 869, "inet"),
		("money", 790, "money"),
		("jsonb", 3802, "jsonb"),
		("int8", 20, "bigint"),
		("serial", 23, "integer"),
		("float(10)", 700, "real"),
		("float(30)", 701, "double precision"),
		("\"char\"", 18, "\"char\""),
		("pg_lsn", 3220, "pg_lsn"),
		("int4range", 3904, "int4range"),
	] {
		let report = pg_report(&pg(text));
		assert_eq!(
			(report.oid, report.typmod, report.format.as_str()),
			(oid, -1, format),
			"{text}"
		);
	}
	let int2 = pg_report(&pg("smallint"));
	assert_eq!(
		(int2.numeric_precision, int2.numeric_precision_radix, int2.numeric_scale),
		(Some(16), Some(2), Some(0))
	);
}

#[test]
fn an_unknown_postgres_type_is_reported_by_its_kind() {
	assert_eq!(pg_reported_type(&pg("mood")), None);
}

#[test]
fn mysql_types_read_through_postgres_as_the_nearest_type() {
	for (text, oid, typmod, format) in [
		("varchar(20)", 1043, 24, "character varying(20)"),
		("char(5)", 1042, 9, "character(5)"),
		("decimal(10,2)", 1700, (10 << 16 | 2) + 4, "numeric(10,2)"),
		("decimal", 1700, (10 << 16) + 4, "numeric(10,0)"),
		("datetime(3)", 1114, 3, "timestamp(3) without time zone"),
		("datetime", 1114, 0, "timestamp(0) without time zone"),
		("timestamp(6)", 1184, 6, "timestamp(6) with time zone"),
		("time(2)", 1186, (0x7FFF << 16) | 2, "interval(2)"),
		("year", 21, -1, "smallint"),
		("bit(8)", 20, -1, "bigint"),
		("bit(64)", 1700, (20 << 16) + 4, "numeric(20,0)"),
		("bigint unsigned", 1700, (20 << 16) + 4, "numeric(20,0)"),
		("int unsigned", 20, -1, "bigint"),
		("tinyint(1)", 21, -1, "smallint"),
		("boolean", 16, -1, "boolean"),
		("text", 25, -1, "text"),
		("varbinary(16)", 17, -1, "bytea"),
		("json", 3802, -1, "jsonb"),
	] {
		let report = pg_report(&my(text));
		assert_eq!(
			(report.oid, report.typmod, report.format.as_str()),
			(oid, typmod, format),
			"{text}"
		);
	}
	assert_eq!(pg_reported_type(&my("multipolygon")), None);
}

#[test]
fn format_type_reads_the_typmod_it_is_given() {
	assert_eq!(pg_format_type(1043, 24).as_deref(), Some("character varying(20)"));
	assert_eq!(pg_format_type(1043, -1).as_deref(), Some("character varying"));
	assert_eq!(pg_format_type(1015, 24).as_deref(), Some("character varying(20)[]"));
	assert_eq!(pg_format_type(1700, (10 << 16 | 2) + 4).as_deref(), Some("numeric(10,2)"));
	assert_eq!(pg_format_type(1114, 3).as_deref(), Some("timestamp(3) without time zone"));
	assert_eq!(pg_format_type(1560, 8).as_deref(), Some("bit(8)"));
	assert_eq!(pg_format_type(23, -1).as_deref(), Some("integer"));
	assert_eq!(pg_format_type(99_999, -1), None);
	// A negative scale is stored in 11 bits.
	let typmod = pg_report(&pg("numeric(5,-2)")).typmod;
	assert_eq!(pg_format_type(1700, i64::from(typmod)).as_deref(), Some("numeric(5,-2)"));
}

#[test]
fn mysql_column_definitions_follow_the_declaration() {
	let varchar = my_report(&my("varchar(20)"));
	assert_eq!((varchar.type_code, varchar.length, varchar.decimals), (0xfd, 80, 0));
	assert_eq!(
		(varchar.column_type.as_str(), varchar.data_type.as_str()),
		("varchar(20)", "varchar")
	);
	assert_eq!(varchar.character_maximum_length, Some(20));
	assert_eq!(varchar.character_octet_length, Some(80));

	let char = my_report(&my("char(5)"));
	assert_eq!((char.type_code, char.length, char.column_type.as_str()), (0xfe, 20, "char(5)"));

	let decimal = my_report(&my("decimal(10,2)"));
	assert_eq!((decimal.type_code, decimal.length, decimal.decimals), (0xf6, 12, 2));
	assert_eq!(decimal.column_type, "decimal(10,2)");
	assert_eq!((decimal.numeric_precision, decimal.numeric_scale), (Some(10), Some(2)));

	let year = my_report(&my("year"));
	assert_eq!((year.type_code, year.length, year.unsigned, year.zerofill), (0x0d, 4, true, true));
	assert_eq!((year.column_type.as_str(), year.data_type.as_str()), ("year", "year"));

	let bit = my_report(&my("bit(8)"));
	assert_eq!((bit.type_code, bit.length, bit.bit_width), (0x10, 8, Some(8)));
	assert_eq!((bit.column_type.as_str(), bit.numeric_precision), ("bit(8)", Some(8)));

	let datetime = my_report(&my("datetime(3)"));
	assert_eq!((datetime.type_code, datetime.length, datetime.decimals), (0x0c, 23, 3));
	assert_eq!(
		(datetime.column_type.as_str(), datetime.datetime_precision),
		("datetime(3)", Some(3))
	);
	let timestamp = my_report(&my("timestamp"));
	assert_eq!((timestamp.type_code, timestamp.length, timestamp.decimals), (0x07, 19, 0));
	assert_eq!(my_report(&my("time(6)")).length, 17);

	let unsigned = my_report(&my("int(10) unsigned"));
	assert_eq!((unsigned.type_code, unsigned.length, unsigned.unsigned), (0x03, 10, true));
	assert_eq!(unsigned.column_type, "int unsigned");

	let binary = my_report(&my("binary(16)"));
	assert_eq!((binary.type_code, binary.length, binary.binary), (0xfe, 16, true));

	let point = my_report(&my("point srid 4326"));
	assert_eq!(
		(point.type_code, point.srid, point.data_type.as_str()),
		(0xff, Some(4326), "point")
	);
	assert_eq!(my_report(&my("geometry")).type_code, 0xff);
	assert_eq!(my_report(&my("text")).length, 262_140);
}

#[test]
fn postgres_types_read_through_mysql_as_the_nearest_type() {
	for (text, code, column_type) in [
		("varchar(20)", 0xfd, "varchar(20)"),
		("char(5)", 0xfe, "char(5)"),
		("numeric(10,2)", 0xf6, "decimal(10,2)"),
		("numeric", 0xf6, "decimal(65,30)"),
		("timestamp(3)", 0x0c, "datetime(3)"),
		("timestamptz", 0x07, "timestamp(6)"),
		("time", 0x0b, "time(6)"),
		("int2", 0x02, "smallint"),
		("bigint", 0x08, "bigint"),
		("boolean", 0x01, "tinyint(1)"),
		("text", 0xfc, "longtext"),
		("bytea", 0xfc, "longblob"),
		("jsonb", 0xf5, "json"),
		("int4[]", 0xf5, "json"),
		("uuid", 0xfe, "char(36)"),
		("inet", 0xfd, "varchar(43)"),
		("bit(8)", 0xfd, "varchar(8)"),
		("point", 0xff, "point"),
	] {
		let report = my_report(&pg(text));
		assert_eq!((report.type_code, report.column_type.as_str()), (code, column_type), "{text}");
	}
	assert_eq!(mysql_reported_type(&pg("mood")), None);
}

#[test]
fn postgres_reports_an_interval_field_qualifier_in_the_typmod_and_format() {
	// PostgreSQL's INTERVAL_TYPMOD: the field mask (DAY 1<<3, HOUR 1<<10, MINUTE 1<<11,
	// SECOND 1<<12, MONTH 1<<1, YEAR 1<<2) above the precision, 0xFFFF when none is written.
	let day_to_second = pg_report(&pg("interval day to second(3)"));
	assert_eq!((day_to_second.oid, day_to_second.typmod), (1186, (7176 << 16) | 3));
	assert_eq!(day_to_second.format, "interval day to second(3)");
	assert_eq!(day_to_second.data_type, "interval");
	assert_eq!(day_to_second.udt_name, "interval");
	assert_eq!(day_to_second.datetime_precision, Some(3));

	let year_to_month = pg_report(&pg("interval year to month"));
	assert_eq!(year_to_month.typmod, (6 << 16) | 0xFFFF);
	assert_eq!(year_to_month.format, "interval year to month");
	assert_eq!(year_to_month.datetime_precision, Some(6));

	let hour = pg_report(&pg("interval hour"));
	assert_eq!(hour.typmod, (1024 << 16) | 0xFFFF);
	assert_eq!(hour.format, "interval hour");

	let seconds = pg_report(&pg("interval second(0)[]"));
	assert_eq!((seconds.oid, seconds.typmod), (1187, (4096 << 16)));
	assert_eq!(seconds.format, "interval second(0)[]");

	assert_eq!(pg_report(&pg("interval")).typmod, -1);
	assert_eq!(
		pg_format_type(1186, i64::from((7176 << 16) | 3)).as_deref(),
		Some("interval day to second(3)")
	);
	assert_eq!(
		pg_format_type(1186, i64::from((2048 << 16) | 0xFFFF)).as_deref(),
		Some("interval minute")
	);
	assert_eq!(pg_format_type(1186, i64::from((0x7FFF << 16) | 2)).as_deref(), Some("interval(2)"));
	assert_eq!(pg_format_type(1186, -1).as_deref(), Some("interval"));

	// Through MySQL, a qualified interval is a TIME, as an interval is.
	assert_eq!(my_report(&pg("interval day to second(3)")).column_type, "time(3)");
}

#[test]
fn a_mysql_text_or_blob_length_reports_the_class_it_selects() {
	for (declared, column_type, length) in [
		("text(10)", "tinytext", 255),
		("text", "text", 65_535),
		("text(70000)", "mediumtext", 16_777_215),
		("blob(10)", "tinyblob", 255),
		("blob", "blob", 65_535),
		("blob(70000)", "mediumblob", 16_777_215),
		("blob(16777216)", "longblob", 4_294_967_295),
	] {
		let report = my_report(&my(declared));
		assert_eq!(report.column_type, column_type, "{declared}");
		assert_eq!(report.data_type, column_type, "{declared}");
		if declared.starts_with("blob") {
			assert_eq!(report.length, length, "{declared}");
		}
	}
}

#[test]
fn a_bare_bpchar_is_unbounded_where_a_bare_char_is_char_one() {
	let bare = pg_report(&pg("bpchar"));
	assert_eq!((bare.oid, bare.typmod), (1042, -1));
	assert_eq!(bare.format, "bpchar");
	assert_eq!(bare.character_maximum_length, None);
	assert_eq!(bare.blank_padded_to, None);
	let one = pg_report(&pg("char"));
	assert_eq!((one.oid, one.typmod), (1042, 5));
	assert_eq!(one.character_maximum_length, Some(1));
}

#[test]
fn a_mysql_point_is_reported_as_the_json_the_wire_sends() {
	assert_eq!(pg_report(&my("point")).oid, 114);
}

#[test]
fn postgres_wire_types_follow_the_declared_type() {
	use crate::SqlType;
	let wire = |text: &str| pg_wire_type(&pg(text));
	assert_eq!(wire("float(10)"), Some(SqlType::Float4));
	assert_eq!(wire("float(40)"), Some(SqlType::Float8));
	assert_eq!(wire("inet"), Some(SqlType::Inet));
	assert_eq!(wire("cidr"), Some(SqlType::Cidr));
	assert_eq!(wire("macaddr"), Some(SqlType::MacAddr));
	assert_eq!(wire("timestamp(3) with time zone"), Some(SqlType::TimestampTz));
	assert_eq!(wire("varchar(10)[]"), SqlType::array_of(SqlType::Varchar));
	assert_eq!(wire("jsonb"), Some(SqlType::Json));
}
