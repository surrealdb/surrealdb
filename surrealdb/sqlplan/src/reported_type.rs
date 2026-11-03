//! How a column's declared SQL type is reported to a client: the PostgreSQL type of a
//! RowDescription, `pg_attribute`, `format_type` and `information_schema.columns`, and the MySQL
//! column definition and `information_schema.COLUMNS` row.
//!
//! A declared type is reported in the dialect of the wire that reads it. A type declared in the
//! other dialect is reported as the nearest type of the reading one that carries the column's
//! values: MySQL `DATETIME(3)` reads through PostgreSQL as `timestamp(3) without time zone`, and
//! PostgreSQL `varchar(20)` reads through MySQL as `VARCHAR(20)`. A type with no such counterpart,
//! and every DynamoDB type, has no report, and the column is reported by its kind.

use surrealdb_expr::expr::statements::define::SqlTypeDialect;
use surrealdb_expr::expr::statements::define::declared_type::{self, IntervalFields};

use crate::declared_type::DeclaredSqlType;

/// The bytes a character takes at most in the encoding both wires use (UTF-8, `utf8mb4`).
const BYTES_PER_CHAR: i64 = 4;
/// The `character_octet_length` PostgreSQL reports for a character type without a length.
const PG_UNBOUNDED_OCTETS: i64 = 1_073_741_824;
/// The `typmod` field mask PostgreSQL stores for an interval of every field.
const PG_INTERVAL_FULL_RANGE: i32 = 0x7FFF;
/// The `typmod` precision PostgreSQL stores for an interval with a field qualifier and no
/// precision.
const PG_INTERVAL_FULL_PRECISION: i32 = 0xFFFF;

/// A built-in PostgreSQL type a declared type is reported as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PgBase {
	/// `pg_type.oid`.
	oid: i32,
	/// The OID of the array of this type.
	array_oid: i32,
	/// `pg_type.typname`, which `information_schema.columns.udt_name` reports.
	typname: &'static str,
	/// The name `format_type` and `information_schema.columns.data_type` give the type.
	name: &'static str,
	/// What the modifiers of the type say.
	modifiers: PgModifiers,
}

/// The meaning of the modifiers of a PostgreSQL type, which decides its `typmod`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PgModifiers {
	/// The type takes no modifier.
	None,
	/// `varchar(n)`: a maximum length in characters, with no limit when omitted.
	VarLength,
	/// `char(n)`: a fixed length in characters, 1 when omitted.
	FixedLength,
	/// `bit(n)`: a fixed length in bits, 1 when omitted.
	FixedBits,
	/// `bit varying(n)`: a maximum length in bits, with no limit when omitted.
	VarBits,
	/// `numeric(p,s)`.
	Numeric,
	/// `time(p)`, `timestamp(p)` and their zoned forms: fractional-second digits, written
	/// after the first word of the name.
	Precision,
	/// `interval(p)` and an interval with a field qualifier, `interval day to second(p)`.
	Interval,
	/// pgvector's `vector(n)`: the dimensions.
	Dimensions,
}

/// Each built-in PostgreSQL type a declared type can name, with the names it is written as.
const PG_BASES: &[(&[&str], PgBase)] = &[
	(&["bool", "boolean"], base(16, 1000, "bool", "boolean", PgModifiers::None)),
	(&["bytea"], base(17, 1001, "bytea", "bytea", PgModifiers::None)),
	(&["\"char\""], base(18, 1002, "char", "\"char\"", PgModifiers::None)),
	(&["name"], base(19, 1003, "name", "name", PgModifiers::None)),
	(
		&["int8", "bigint", "bigserial", "serial8"],
		base(20, 1016, "int8", "bigint", PgModifiers::None),
	),
	(
		&["int2", "smallint", "smallserial", "serial2"],
		base(21, 1005, "int2", "smallint", PgModifiers::None),
	),
	(
		&["int4", "int", "integer", "serial", "serial4"],
		base(23, 1007, "int4", "integer", PgModifiers::None),
	),
	(&["regproc"], base(24, 1008, "regproc", "regproc", PgModifiers::None)),
	(&["text", "citext", "string"], base(25, 1009, "text", "text", PgModifiers::None)),
	(&["oid"], base(26, 1028, "oid", "oid", PgModifiers::None)),
	(&["tid"], base(27, 1010, "tid", "tid", PgModifiers::None)),
	(&["xid"], base(28, 1011, "xid", "xid", PgModifiers::None)),
	(&["cid"], base(29, 1012, "cid", "cid", PgModifiers::None)),
	(&["json"], base(114, 199, "json", "json", PgModifiers::None)),
	(&["xml"], base(142, 143, "xml", "xml", PgModifiers::None)),
	(&["point"], base(600, 1017, "point", "point", PgModifiers::None)),
	(&["lseg"], base(601, 1018, "lseg", "lseg", PgModifiers::None)),
	(&["path"], base(602, 1019, "path", "path", PgModifiers::None)),
	(&["box"], base(603, 1020, "box", "box", PgModifiers::None)),
	(&["polygon"], base(604, 1027, "polygon", "polygon", PgModifiers::None)),
	(&["line"], base(628, 629, "line", "line", PgModifiers::None)),
	(&["cidr"], base(650, 651, "cidr", "cidr", PgModifiers::None)),
	(&["float4", "real"], base(700, 1021, "float4", "real", PgModifiers::None)),
	(
		&["float8", "double precision", "double"],
		base(701, 1022, "float8", "double precision", PgModifiers::None),
	),
	(&["circle"], base(718, 719, "circle", "circle", PgModifiers::None)),
	(&["macaddr8"], base(774, 775, "macaddr8", "macaddr8", PgModifiers::None)),
	(&["money"], base(790, 791, "money", "money", PgModifiers::None)),
	(&["macaddr"], base(829, 1040, "macaddr", "macaddr", PgModifiers::None)),
	(&["inet"], base(869, 1041, "inet", "inet", PgModifiers::None)),
	(
		&["bpchar", "char", "character"],
		base(1042, 1014, "bpchar", "character", PgModifiers::FixedLength),
	),
	(
		&["varchar", "character varying"],
		base(1043, 1015, "varchar", "character varying", PgModifiers::VarLength),
	),
	(&["date"], base(1082, 1182, "date", "date", PgModifiers::None)),
	(
		&["time", "time without time zone"],
		base(1083, 1183, "time", "time without time zone", PgModifiers::Precision),
	),
	(
		&["timestamp", "timestamp without time zone"],
		base(1114, 1115, "timestamp", "timestamp without time zone", PgModifiers::Precision),
	),
	(
		&["timestamptz", "timestamp with time zone"],
		base(1184, 1185, "timestamptz", "timestamp with time zone", PgModifiers::Precision),
	),
	(&["interval"], base(1186, 1187, "interval", "interval", PgModifiers::Interval)),
	(
		&["timetz", "time with time zone"],
		base(1266, 1270, "timetz", "time with time zone", PgModifiers::Precision),
	),
	(&["bit"], base(1560, 1561, "bit", "bit", PgModifiers::FixedBits)),
	(&["varbit", "bit varying"], base(1562, 1563, "varbit", "bit varying", PgModifiers::VarBits)),
	(&["numeric", "decimal", "dec"], base(1700, 1231, "numeric", "numeric", PgModifiers::Numeric)),
	(&["regprocedure"], base(2202, 2207, "regprocedure", "regprocedure", PgModifiers::None)),
	(&["regoper"], base(2203, 2208, "regoper", "regoper", PgModifiers::None)),
	(&["regoperator"], base(2204, 2209, "regoperator", "regoperator", PgModifiers::None)),
	(&["regclass"], base(2205, 2210, "regclass", "regclass", PgModifiers::None)),
	(&["regtype"], base(2206, 2211, "regtype", "regtype", PgModifiers::None)),
	(&["uuid"], base(2950, 2951, "uuid", "uuid", PgModifiers::None)),
	(&["txid_snapshot"], base(2970, 2949, "txid_snapshot", "txid_snapshot", PgModifiers::None)),
	(&["pg_lsn"], base(3220, 3221, "pg_lsn", "pg_lsn", PgModifiers::None)),
	(&["tsvector"], base(3614, 3643, "tsvector", "tsvector", PgModifiers::None)),
	(&["tsquery"], base(3615, 3645, "tsquery", "tsquery", PgModifiers::None)),
	(&["regconfig"], base(3734, 3735, "regconfig", "regconfig", PgModifiers::None)),
	(&["regdictionary"], base(3769, 3770, "regdictionary", "regdictionary", PgModifiers::None)),
	(&["jsonb"], base(3802, 3807, "jsonb", "jsonb", PgModifiers::None)),
	(&["int4range"], base(3904, 3905, "int4range", "int4range", PgModifiers::None)),
	(&["numrange"], base(3906, 3907, "numrange", "numrange", PgModifiers::None)),
	(&["tsrange"], base(3908, 3909, "tsrange", "tsrange", PgModifiers::None)),
	(&["tstzrange"], base(3910, 3911, "tstzrange", "tstzrange", PgModifiers::None)),
	(&["daterange"], base(3912, 3913, "daterange", "daterange", PgModifiers::None)),
	(&["int8range"], base(3926, 3927, "int8range", "int8range", PgModifiers::None)),
	(&["jsonpath"], base(4072, 4073, "jsonpath", "jsonpath", PgModifiers::None)),
	(&["regnamespace"], base(4089, 4090, "regnamespace", "regnamespace", PgModifiers::None)),
	(&["regrole"], base(4096, 4097, "regrole", "regrole", PgModifiers::None)),
	(&["regcollation"], base(4191, 4192, "regcollation", "regcollation", PgModifiers::None)),
	(&["int4multirange"], base(4451, 6150, "int4multirange", "int4multirange", PgModifiers::None)),
	(&["nummultirange"], base(4532, 6151, "nummultirange", "nummultirange", PgModifiers::None)),
	(&["tsmultirange"], base(4533, 6152, "tsmultirange", "tsmultirange", PgModifiers::None)),
	(&["tstzmultirange"], base(4534, 6153, "tstzmultirange", "tstzmultirange", PgModifiers::None)),
	(&["datemultirange"], base(4535, 6155, "datemultirange", "datemultirange", PgModifiers::None)),
	(&["int8multirange"], base(4536, 6157, "int8multirange", "int8multirange", PgModifiers::None)),
	(&["pg_snapshot"], base(5038, 5039, "pg_snapshot", "pg_snapshot", PgModifiers::None)),
	(&["xid8"], base(5069, 271, "xid8", "xid8", PgModifiers::None)),
	(
		&["vector"],
		base(
			crate::descriptor::VECTOR_OID,
			crate::descriptor::VECTOR_ARRAY_OID,
			"vector",
			"vector",
			PgModifiers::Dimensions,
		),
	),
];

const fn base(
	oid: i32,
	array_oid: i32,
	typname: &'static str,
	name: &'static str,
	modifiers: PgModifiers,
) -> PgBase {
	PgBase {
		oid,
		array_oid,
		typname,
		name,
		modifiers,
	}
}

/// The PostgreSQL type named `name` (as a declared type's base, lower case).
fn pg_base(name: &str) -> Option<PgBase> {
	PG_BASES.iter().find(|(names, _)| names.contains(&name)).map(|(_, base)| *base)
}

/// The PostgreSQL type with OID `oid`, as an element type or as the array of one: the base and
/// whether the OID is its array's.
fn pg_base_of_oid(oid: i64) -> Option<(PgBase, bool)> {
	PG_BASES.iter().find_map(|(_, base)| {
		if i64::from(base.oid) == oid {
			Some((*base, false))
		} else if i64::from(base.array_oid) == oid {
			Some((*base, true))
		} else {
			None
		}
	})
}

/// A declared type as PostgreSQL reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgReportedType {
	/// The `pg_type` OID of the column's type: the array type's for an array column.
	pub oid: i32,
	/// The type modifier (`atttypmod`, and the RowDescription field): `-1` for none. An array
	/// column has its element's modifier.
	pub typmod: i32,
	/// The text `format_type(oid, typmod)` gives, such as `character varying(20)` or
	/// `timestamp(3) without time zone`.
	pub format: String,
	/// `information_schema.columns.data_type`: the type's name, or `ARRAY`.
	pub data_type: String,
	/// `information_schema.columns.udt_name`: `pg_type.typname`, prefixed `_` for an array.
	pub udt_name: String,
	pub character_maximum_length: Option<i64>,
	pub character_octet_length: Option<i64>,
	pub numeric_precision: Option<i64>,
	pub numeric_precision_radix: Option<i64>,
	pub numeric_scale: Option<i64>,
	pub datetime_precision: Option<i64>,
	/// The length a `character(n)` value is padded to with spaces on output. `None` for every
	/// other type, and for an array.
	pub blank_padded_to: Option<u32>,
}

/// The scalar PostgreSQL type of a declared type, before its array dimensions.
struct PgScalar {
	base: PgBase,
	/// The first modifier, or the precision and scale of a `numeric`. `None` when not written.
	length: Option<i64>,
	scale: Option<i64>,
	/// The field qualifier of an interval, `None` for one of every field and any other type.
	interval_fields: Option<IntervalFields>,
}

/// How PostgreSQL reports `declared`, or `None` when the reading wire has no counterpart for it
/// and the column is reported by its kind.
pub fn pg_reported_type(declared: &DeclaredSqlType) -> Option<PgReportedType> {
	let scalar = match declared.dialect() {
		SqlTypeDialect::PostgreSql => pg_scalar(declared)?,
		SqlTypeDialect::MySql => mysql_as_pg(declared)?,
		SqlTypeDialect::DynamoDb => return None,
	};
	let dimensions = declared.array_dimensions().len();
	Some(report_pg(&scalar, dimensions))
}

/// The wire type the values of `declared` travel as through PostgreSQL: the type
/// [`crate::SqlType::of_declared`] names, or else the built-in type [`pg_reported_type`] reports,
/// as an array for each array dimension. `None` when neither names a wire type, and the values
/// travel as their kind reads.
pub fn pg_wire_type(declared: &DeclaredSqlType) -> Option<crate::SqlType> {
	if let Some(sql_type) = crate::SqlType::of_declared(declared) {
		return Some(sql_type);
	}
	let scalar = match declared.dialect() {
		SqlTypeDialect::PostgreSql => pg_scalar(declared)?,
		SqlTypeDialect::MySql => mysql_as_pg(declared)?,
		SqlTypeDialect::DynamoDb => return None,
	};
	// A type `from_pg_oid` does not read, such as `jsonb`, travels as the type its name reads as.
	let mut sql_type = crate::SqlType::from_pg_oid(scalar.base.oid).or_else(|| {
		Some(crate::SqlType::from_name(scalar.base.typname))
			.filter(|sql_type| *sql_type != crate::SqlType::Unknown)
	})?;
	for _ in declared.array_dimensions() {
		sql_type = crate::SqlType::array_of(sql_type)?;
	}
	Some(sql_type)
}

/// The scalar PostgreSQL type a PostgreSQL declaration names.
fn pg_scalar(declared: &DeclaredSqlType) -> Option<PgScalar> {
	// An interval's field qualifier is in its base, `interval day to second`, and is read
	// whether or not the column is an array.
	let interval_fields =
		declared.base().strip_prefix("interval ").and_then(IntervalFields::from_words);
	let name = if interval_fields.is_some() {
		"interval"
	} else {
		declared.base()
	};
	let modifier = |index: usize| declared.modifiers().get(index).and_then(|m| m.parse().ok());
	// `float(p)` is `real` up to 24 binary digits and `double precision` above.
	let base = if name == "float" {
		let single = modifier(0).is_some_and(|p: i64| p <= 24);
		pg_base(if single {
			"float4"
		} else {
			"float8"
		})?
	} else {
		pg_base(name)?
	};
	// A `bpchar` without a length is unbounded, where `char` and `character` without one are
	// `char(1)`.
	let base = if name == "bpchar" && modifier(0).is_none() {
		PgBase {
			modifiers: PgModifiers::VarLength,
			..base
		}
	} else {
		base
	};
	let (length, scale) = match base.modifiers {
		PgModifiers::None => (None, None),
		PgModifiers::Numeric => {
			(declared.precision().and_then(|p| i64::try_from(p).ok()), declared.scale())
		}
		PgModifiers::VarLength
		| PgModifiers::FixedLength
		| PgModifiers::FixedBits
		| PgModifiers::VarBits
		| PgModifiers::Precision
		| PgModifiers::Interval
		| PgModifiers::Dimensions => (modifier(0), None),
	};
	Some(PgScalar {
		base,
		length,
		scale,
		interval_fields,
	})
}

/// The scalar PostgreSQL type nearest a MySQL declaration: the one whose values the column's
/// kind holds.
fn mysql_as_pg(declared: &DeclaredSqlType) -> Option<PgScalar> {
	let my = mysql_native(declared)?;
	let pg = |name: &str, length: Option<i64>, scale: Option<i64>| {
		pg_base(name).map(|base| PgScalar {
			base,
			length,
			scale,
			interval_fields: None,
		})
	};
	let unsigned = my.unsigned;
	match my.data_type.as_str() {
		// A `BOOLEAN` column is a `TINYINT(1)` that holds booleans.
		"tinyint" if matches!(declared.base(), "bool" | "boolean") => pg("bool", None, None),
		"tinyint" => pg("int2", None, None),
		"smallint" if unsigned => pg("int4", None, None),
		"smallint" => pg("int2", None, None),
		"mediumint" => pg("int4", None, None),
		"int" if unsigned => pg("int8", None, None),
		"int" => pg("int4", None, None),
		// PostgreSQL has no unsigned integer: the `numeric` of 20 digits holds a `u64`.
		"bigint" if unsigned => pg("numeric", Some(20), Some(0)),
		"bigint" => pg("int8", None, None),
		"float" => pg("float4", None, None),
		"double" => pg("float8", None, None),
		"decimal" => pg("numeric", my.numeric_precision, my.numeric_scale),
		"char" => pg("bpchar", my.character_maximum_length, None),
		"varchar" => pg("varchar", my.character_maximum_length, None),
		"tinytext" | "text" | "mediumtext" | "longtext" => pg("text", None, None),
		"binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
			pg("bytea", None, None)
		}
		"date" => pg("date", None, None),
		"datetime" => pg("timestamp", my.datetime_precision, None),
		"timestamp" => pg("timestamptz", my.datetime_precision, None),
		// A MySQL `TIME` is a signed duration, which PostgreSQL holds as an interval.
		"time" => pg("interval", my.datetime_precision, None),
		"year" => pg("int2", None, None),
		// `BIT(n)` holds the unsigned integer of its bits: a `bigint` holds up to 63 of them.
		"bit" if my.bit_width.is_some_and(|width| width <= 63) => pg("int8", None, None),
		"bit" => pg("numeric", Some(20), Some(0)),
		"json" => pg("jsonb", None, None),
		// A MySQL geometry value travels as its GeoJSON object, which the wire types `json`.
		"point" => pg("json", None, None),
		"vector" => pg("vector", declared.modifiers().first().and_then(|m| m.parse().ok()), None),
		_ => None,
	}
}

/// The report of a scalar PostgreSQL type with `dimensions` array dimensions.
fn report_pg(scalar: &PgScalar, dimensions: usize) -> PgReportedType {
	let base = scalar.base;
	let typmod = pg_typmod(base, scalar.length, scalar.scale, scalar.interval_fields);
	let scalar_format = pg_format(base, typmod);
	let array = dimensions > 0;
	let (oid, format, data_type, udt_name) = if array {
		(
			base.array_oid,
			format!("{scalar_format}[]"),
			"ARRAY".to_owned(),
			format!("_{}", base.typname),
		)
	} else {
		(base.oid, scalar_format, base.name.to_owned(), base.typname.to_owned())
	};
	let mut report = PgReportedType {
		oid,
		typmod,
		format,
		data_type,
		udt_name,
		character_maximum_length: None,
		character_octet_length: None,
		numeric_precision: None,
		numeric_precision_radix: None,
		numeric_scale: None,
		datetime_precision: None,
		blank_padded_to: None,
	};
	if array {
		return report;
	}
	match base.modifiers {
		PgModifiers::VarLength | PgModifiers::FixedLength => {
			let length = if base.modifiers == PgModifiers::FixedLength {
				Some(scalar.length.unwrap_or(1))
			} else {
				scalar.length
			};
			report.character_maximum_length = length;
			report.character_octet_length =
				Some(length.map_or(PG_UNBOUNDED_OCTETS, |n| n.saturating_mul(BYTES_PER_CHAR)));
			if base.modifiers == PgModifiers::FixedLength {
				report.blank_padded_to = length.and_then(|n| u32::try_from(n).ok());
			}
		}
		PgModifiers::FixedBits => {
			report.character_maximum_length = Some(scalar.length.unwrap_or(1));
		}
		PgModifiers::VarBits => report.character_maximum_length = scalar.length,
		PgModifiers::Numeric => {
			report.numeric_precision = scalar.length;
			report.numeric_precision_radix = Some(10);
			report.numeric_scale = scalar.length.map(|_| scalar.scale.unwrap_or(0));
		}
		PgModifiers::Precision | PgModifiers::Interval => {
			report.datetime_precision = Some(scalar.length.map_or(6, |p| p.min(6)));
		}
		PgModifiers::None if base.typname == "text" => {
			report.character_octet_length = Some(PG_UNBOUNDED_OCTETS);
		}
		PgModifiers::None if base.typname == "date" => report.datetime_precision = Some(0),
		PgModifiers::None => {
			let (precision, radix, scale) = match base.typname {
				"int2" => (Some(16), Some(2), Some(0)),
				"int4" => (Some(32), Some(2), Some(0)),
				"int8" => (Some(64), Some(2), Some(0)),
				"float4" => (Some(24), Some(2), None),
				"float8" => (Some(53), Some(2), None),
				_ => (None, None, None),
			};
			report.numeric_precision = precision;
			report.numeric_precision_radix = radix;
			report.numeric_scale = scale;
		}
		PgModifiers::Dimensions => {}
	}
	report
}

/// The `typmod` PostgreSQL stores for `base` with the first modifier `length` and, for a
/// `numeric`, the scale `scale`, or for an interval, the field qualifier `interval_fields`: `-1`
/// when the type has no modifier. An interval's is PostgreSQL's `INTERVAL_TYPMOD`: the field mask
/// in the upper half and the precision in the lower.
/// The most fractional-seconds digits a PostgreSQL time, timestamp or interval holds.
const PG_MAX_FRACTIONAL_DIGITS: i64 = 6;

fn pg_typmod(
	base: PgBase,
	length: Option<i64>,
	scale: Option<i64>,
	interval_fields: Option<IntervalFields>,
) -> i32 {
	let clamp = |value: i64| i32::try_from(value).unwrap_or(i32::MAX);
	match base.modifiers {
		PgModifiers::None => -1,
		PgModifiers::VarLength => length.map_or(-1, |n| clamp(n.saturating_add(4))),
		PgModifiers::FixedLength => clamp(length.unwrap_or(1).saturating_add(4)),
		PgModifiers::FixedBits => clamp(length.unwrap_or(1)),
		PgModifiers::VarBits | PgModifiers::Dimensions => length.map_or(-1, clamp),
		// PostgreSQL reduces a fractional-seconds precision past 6 to 6 (with a warning), and
		// the declared type holds six digits.
		PgModifiers::Precision => length.map_or(-1, |p| clamp(p.min(PG_MAX_FRACTIONAL_DIGITS))),
		PgModifiers::Interval => match (interval_fields, length) {
			(None, None) => -1,
			(fields, precision) => {
				let range = fields.map_or(PG_INTERVAL_FULL_RANGE, IntervalFields::pg_range_mask);
				let precision = precision.map_or(PG_INTERVAL_FULL_PRECISION, |p| {
					clamp(p.min(PG_MAX_FRACTIONAL_DIGITS)) & 0xffff
				});
				(range << 16) | precision
			}
		},
		PgModifiers::Numeric => match length {
			// The scale is stored in 11 bits, so a negative scale keeps its two's complement.
			Some(precision) => {
				let scale = scale.unwrap_or(0);
				clamp(((precision << 16) | (scale & 0x7ff)).saturating_add(4))
			}
			None => -1,
		},
	}
}

/// The text `format_type` gives `base` with the modifier `typmod`.
fn pg_format(base: PgBase, typmod: i32) -> String {
	let with = |modifier: String| match base.modifiers {
		PgModifiers::Precision => match base.name.split_once(' ') {
			Some((first, rest)) => format!("{first}({modifier}) {rest}"),
			None => format!("{}({modifier})", base.name),
		},
		PgModifiers::None
		| PgModifiers::VarLength
		| PgModifiers::FixedLength
		| PgModifiers::FixedBits
		| PgModifiers::VarBits
		| PgModifiers::Numeric
		| PgModifiers::Interval
		| PgModifiers::Dimensions => format!("{}({modifier})", base.name),
	};
	if typmod < 0 {
		// `format_type` spells the unbounded blank-padded type `bpchar`.
		return if base.typname == "bpchar" {
			"bpchar".to_owned()
		} else {
			base.name.to_owned()
		};
	}
	match base.modifiers {
		PgModifiers::None => base.name.to_owned(),
		PgModifiers::VarLength | PgModifiers::FixedLength if typmod >= 4 => {
			with((typmod - 4).to_string())
		}
		PgModifiers::VarLength | PgModifiers::FixedLength => base.name.to_owned(),
		PgModifiers::FixedBits
		| PgModifiers::VarBits
		| PgModifiers::Precision
		| PgModifiers::Dimensions => with(typmod.to_string()),
		PgModifiers::Interval => {
			// intervaltypmodout: the qualifier's words, then the precision unless it is the full
			// one.
			let range = (typmod >> 16) & PG_INTERVAL_FULL_RANGE;
			let precision = typmod & 0xffff;
			let mut text = base.name.to_owned();
			if range != PG_INTERVAL_FULL_RANGE
				&& let Some(fields) = IntervalFields::from_pg_range_mask(range)
			{
				text.push(' ');
				text.push_str(fields.words());
			}
			if precision != PG_INTERVAL_FULL_PRECISION {
				text.push_str(&format!("({precision})"));
			}
			text
		}
		PgModifiers::Numeric if typmod >= 4 => {
			let packed = typmod - 4;
			let precision = (packed >> 16) & 0xffff;
			// The scale is an 11-bit two's complement number.
			let scale = ((packed & 0x7ff) ^ 0x400) - 0x400;
			with(format!("{precision},{scale}"))
		}
		PgModifiers::Numeric => base.name.to_owned(),
	}
}

/// The text `format_type(oid, typmod)` gives for a built-in type or an array of one, or `None`
/// for an OID that names neither. A negative `typmod` is no modifier.
pub fn pg_format_type(oid: i64, typmod: i64) -> Option<String> {
	let (base, array) = pg_base_of_oid(oid)?;
	let typmod = i32::try_from(typmod).unwrap_or(-1);
	let text = pg_format(base, typmod);
	Some(if array {
		format!("{text}[]")
	} else {
		text
	})
}

/// A declared type as MySQL reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MySqlReportedType {
	/// The column definition's type code (`MYSQL_TYPE_*`).
	pub type_code: u8,
	/// The column definition's length: the most bytes a value's text takes, or the bits of a
	/// `BIT(n)`.
	pub length: u32,
	/// The column definition's decimals: the scale of a `DECIMAL`, the fractional-second digits
	/// of a temporal type, and 31 for a float.
	pub decimals: u8,
	/// The `UNSIGNED` flag.
	pub unsigned: bool,
	/// The `ZEROFILL` flag, which MySQL sets on a `YEAR`.
	pub zerofill: bool,
	/// The `BINARY` flag and the binary character set.
	pub binary: bool,
	/// The `BLOB` flag, set on the `TEXT`, `BLOB`, `JSON` and spatial types.
	pub blob: bool,
	/// The width of a `BIT(n)`, whose values travel as `ceil(n / 8)` big-endian bytes.
	pub bit_width: Option<u32>,
	/// The SRID of a spatial type, which its values carry on the wire.
	pub srid: Option<u32>,
	/// `information_schema.COLUMNS.COLUMN_TYPE`, such as `varchar(20)` or `int unsigned`.
	pub column_type: String,
	/// `information_schema.COLUMNS.DATA_TYPE`, such as `varchar`.
	pub data_type: String,
	pub character_maximum_length: Option<i64>,
	pub character_octet_length: Option<i64>,
	pub numeric_precision: Option<i64>,
	pub numeric_scale: Option<i64>,
	pub datetime_precision: Option<i64>,
}

/// The `MYSQL_TYPE_*` codes a reported type uses.
mod mysql_code {
	pub const TINY: u8 = 0x01;
	pub const SHORT: u8 = 0x02;
	pub const LONG: u8 = 0x03;
	pub const FLOAT: u8 = 0x04;
	pub const DOUBLE: u8 = 0x05;
	pub const TIMESTAMP: u8 = 0x07;
	pub const LONGLONG: u8 = 0x08;
	pub const INT24: u8 = 0x09;
	pub const DATE: u8 = 0x0a;
	pub const TIME: u8 = 0x0b;
	pub const DATETIME: u8 = 0x0c;
	pub const YEAR: u8 = 0x0d;
	pub const BIT: u8 = 0x10;
	pub const JSON: u8 = 0xf5;
	pub const NEWDECIMAL: u8 = 0xf6;
	pub const BLOB: u8 = 0xfc;
	pub const VAR_STRING: u8 = 0xfd;
	pub const STRING: u8 = 0xfe;
	pub const GEOMETRY: u8 = 0xff;
}

/// The decimals MySQL reports for a float or double column: no fixed scale.
const NOT_FIXED_DEC: u8 = 31;
/// The length MySQL reports for the longest `TEXT`, `BLOB`, `JSON` and spatial values.
const MYSQL_LONG_LENGTH: u32 = u32::MAX;

/// How MySQL reports `declared`, or `None` when the reading wire has no counterpart for it and
/// the column is reported by its kind.
pub fn mysql_reported_type(declared: &DeclaredSqlType) -> Option<MySqlReportedType> {
	match declared.dialect() {
		SqlTypeDialect::MySql => mysql_native(declared),
		SqlTypeDialect::PostgreSql => pg_as_mysql(declared),
		SqlTypeDialect::DynamoDb => None,
	}
}

/// A MySQL report with every optional field empty.
fn my(type_code: u8, length: u32, column_type: String, data_type: &str) -> MySqlReportedType {
	MySqlReportedType {
		type_code,
		length,
		decimals: 0,
		unsigned: false,
		zerofill: false,
		binary: false,
		blob: false,
		bit_width: None,
		srid: None,
		column_type,
		data_type: data_type.to_owned(),
		character_maximum_length: None,
		character_octet_length: None,
		numeric_precision: None,
		numeric_scale: None,
		datetime_precision: None,
	}
}

/// An integer type of MySQL, with its display length signed and unsigned and its precision
/// signed and unsigned.
fn mysql_integer(
	code: u8,
	name: &str,
	unsigned: bool,
	lengths: (u32, u32),
	digits: (i64, i64),
) -> MySqlReportedType {
	let column_type = if unsigned {
		format!("{name} unsigned")
	} else {
		name.to_owned()
	};
	let mut report = my(
		code,
		if unsigned {
			lengths.1
		} else {
			lengths.0
		},
		column_type,
		name,
	);
	report.unsigned = unsigned;
	report.numeric_precision = Some(if unsigned {
		digits.1
	} else {
		digits.0
	});
	report.numeric_scale = Some(0);
	report
}

/// A character type of MySQL of `chars` characters at most.
fn mysql_chars(code: u8, name: &str, chars: i64) -> MySqlReportedType {
	let length = u32::try_from(chars.saturating_mul(BYTES_PER_CHAR)).unwrap_or(MYSQL_LONG_LENGTH);
	let mut report = my(code, length, format!("{name}({chars})"), name);
	report.character_maximum_length = Some(chars);
	report.character_octet_length = Some(chars.saturating_mul(BYTES_PER_CHAR));
	report
}

/// A `TEXT` type of MySQL holding `bytes` bytes at most.
/// The class a MySQL `TEXT(n)` or `BLOB(n)` selects, as the prefix of its name (`tiny`, none,
/// `medium`, `long`) and its capacity in bytes: the class the column is enforced as.
fn storage_class(modifier: Option<i64>, is_text: bool) -> (&'static str, i64) {
	let modifier = modifier.and_then(|n| u64::try_from(n).ok());
	let bytes = if is_text {
		declared_type::text_storage_class(modifier)
	} else {
		declared_type::storage_class(modifier)
	};
	let prefix = match bytes {
		255 => "tiny",
		16_777_215 => "medium",
		4_294_967_295 => "long",
		_ => "",
	};
	(prefix, i64::try_from(bytes).unwrap_or(i64::MAX))
}

fn mysql_text(name: &str, bytes: i64) -> MySqlReportedType {
	let length = u32::try_from(bytes.saturating_mul(BYTES_PER_CHAR)).unwrap_or(MYSQL_LONG_LENGTH);
	let mut report = my(mysql_code::BLOB, length, name.to_owned(), name);
	report.blob = true;
	report.character_maximum_length = Some(bytes);
	report.character_octet_length = Some(bytes);
	report
}

/// A binary type of MySQL holding `bytes` bytes at most.
fn mysql_bytes(code: u8, name: &str, bytes: i64, column_type: String) -> MySqlReportedType {
	let mut report = my(code, u32::try_from(bytes).unwrap_or(MYSQL_LONG_LENGTH), column_type, name);
	report.binary = true;
	report.blob = code == mysql_code::BLOB;
	report.character_maximum_length = Some(bytes);
	report.character_octet_length = Some(bytes);
	report
}

/// A temporal type of MySQL with `fsp` fractional-second digits and `width` characters before
/// them.
fn mysql_temporal(code: u8, name: &str, width: u32, fsp: u32) -> MySqlReportedType {
	let column_type = if fsp > 0 {
		format!("{name}({fsp})")
	} else {
		name.to_owned()
	};
	let length = if fsp > 0 {
		width + fsp + 1
	} else {
		width
	};
	let mut report = my(code, length, column_type, name);
	report.decimals = u8::try_from(fsp).unwrap_or(6);
	report.datetime_precision = Some(i64::from(fsp));
	report
}

/// A `DECIMAL(precision, scale)` of MySQL.
fn mysql_decimal(precision: i64, scale: i64, unsigned: bool) -> MySqlReportedType {
	let column_type = if unsigned {
		format!("decimal({precision},{scale}) unsigned")
	} else {
		format!("decimal({precision},{scale})")
	};
	// The longest text: the digits, a point when there is a scale, and a sign unless unsigned.
	let length = precision + i64::from(scale > 0) + i64::from(!unsigned);
	let mut report = my(
		mysql_code::NEWDECIMAL,
		u32::try_from(length).unwrap_or(u32::MAX),
		column_type,
		"decimal",
	);
	report.decimals = u8::try_from(scale.max(0)).unwrap_or(u8::MAX);
	report.unsigned = unsigned;
	report.numeric_precision = Some(precision);
	report.numeric_scale = Some(scale);
	report
}

/// A spatial type of MySQL with the SRID `srid`.
fn mysql_spatial(name: &str, srid: Option<u32>) -> MySqlReportedType {
	let column_type = match srid {
		Some(srid) => format!("{name} /*!80003 SRID {srid} */"),
		None => name.to_owned(),
	};
	let mut report = my(mysql_code::GEOMETRY, MYSQL_LONG_LENGTH, column_type, name);
	report.binary = true;
	report.blob = true;
	report.srid = srid;
	report
}

/// How MySQL reports a MySQL declaration.
fn mysql_native(declared: &DeclaredSqlType) -> Option<MySqlReportedType> {
	if !declared.array_dimensions().is_empty() {
		return None;
	}
	let mut words = declared.base().split(' ');
	let first = words.next().unwrap_or_default();
	let base = declared.base();
	let modifier = |index: usize| -> Option<i64> {
		declared.modifiers().get(index).and_then(|m| m.parse().ok())
	};
	let unsigned = declared.attributes().iter().any(|word| word == "unsigned");
	let zerofill = declared.attributes().iter().any(|word| word == "zerofill");
	let fsp = || u32::try_from(modifier(0).unwrap_or(0).clamp(0, 6)).unwrap_or(0);
	let mut report = match base {
		"double precision" => double(),
		"national char" | "national character" => {
			mysql_chars(mysql_code::STRING, "char", modifier(0).unwrap_or(1))
		}
		"national varchar"
		| "national char varying"
		| "national character varying"
		| "character varying"
		| "char varying" => mysql_chars(mysql_code::VAR_STRING, "varchar", modifier(0)?),
		"long" | "long varchar" => mysql_text("mediumtext", 16_777_215),
		"long varbinary" => {
			mysql_bytes(mysql_code::BLOB, "mediumblob", 16_777_215, "mediumblob".to_owned())
		}
		_ => match first {
			"bool" | "boolean" => {
				let mut report = my(mysql_code::TINY, 1, "tinyint(1)".to_owned(), "tinyint");
				report.numeric_precision = Some(3);
				report.numeric_scale = Some(0);
				report
			}
			"tinyint" | "int1" if modifier(0) == Some(1) && !unsigned => {
				let mut report = my(mysql_code::TINY, 1, "tinyint(1)".to_owned(), "tinyint");
				report.numeric_precision = Some(3);
				report.numeric_scale = Some(0);
				report
			}
			"tinyint" | "int1" => {
				mysql_integer(mysql_code::TINY, "tinyint", unsigned, (4, 3), (3, 3))
			}
			"smallint" | "int2" => {
				mysql_integer(mysql_code::SHORT, "smallint", unsigned, (6, 5), (5, 5))
			}
			"mediumint" | "middleint" | "int3" => {
				mysql_integer(mysql_code::INT24, "mediumint", unsigned, (9, 8), (7, 7))
			}
			"int" | "integer" | "int4" => {
				mysql_integer(mysql_code::LONG, "int", unsigned, (11, 10), (10, 10))
			}
			"bigint" | "int8" => {
				mysql_integer(mysql_code::LONGLONG, "bigint", unsigned, (20, 20), (19, 20))
			}
			"serial" => mysql_integer(mysql_code::LONGLONG, "bigint", true, (20, 20), (19, 20)),
			"float4" => float(),
			"float" if modifier(0).is_some_and(|p| p > 24) && modifier(1).is_none() => double(),
			"float" => float(),
			"float8" | "double" | "real" => double(),
			"decimal" | "dec" | "numeric" | "fixed" => mysql_decimal(
				declared.precision().and_then(|p| i64::try_from(p).ok()).unwrap_or(10),
				declared.scale().unwrap_or(0),
				unsigned,
			),
			"char" | "nchar" => mysql_chars(mysql_code::STRING, "char", modifier(0).unwrap_or(1)),
			"varchar" | "nvarchar" => mysql_chars(mysql_code::VAR_STRING, "varchar", modifier(0)?),
			"tinytext" => mysql_text("tinytext", 255),
			"text" => {
				let (prefix, bytes) = storage_class(modifier(0), true);
				mysql_text(&format!("{prefix}text"), bytes)
			}
			"mediumtext" => mysql_text("mediumtext", 16_777_215),
			"longtext" => mysql_text("longtext", 4_294_967_295),
			"binary" => {
				let n = modifier(0).unwrap_or(1);
				mysql_bytes(mysql_code::STRING, "binary", n, format!("binary({n})"))
			}
			"varbinary" => {
				let n = modifier(0)?;
				mysql_bytes(mysql_code::VAR_STRING, "varbinary", n, format!("varbinary({n})"))
			}
			"tinyblob" => mysql_bytes(mysql_code::BLOB, "tinyblob", 255, "tinyblob".to_owned()),
			"blob" => {
				let (prefix, bytes) = storage_class(modifier(0), false);
				let name = format!("{prefix}blob");
				mysql_bytes(mysql_code::BLOB, &name, bytes, name.clone())
			}
			"mediumblob" => {
				mysql_bytes(mysql_code::BLOB, "mediumblob", 16_777_215, "mediumblob".to_owned())
			}
			"longblob" => {
				mysql_bytes(mysql_code::BLOB, "longblob", 4_294_967_295, "longblob".to_owned())
			}
			"date" => my(mysql_code::DATE, 10, "date".to_owned(), "date"),
			"datetime" => mysql_temporal(mysql_code::DATETIME, "datetime", 19, fsp()),
			"timestamp" => mysql_temporal(mysql_code::TIMESTAMP, "timestamp", 19, fsp()),
			"time" => mysql_temporal(mysql_code::TIME, "time", 10, fsp()),
			"year" => {
				let mut report = my(mysql_code::YEAR, 4, "year".to_owned(), "year");
				report.unsigned = true;
				report.zerofill = true;
				report
			}
			"bit" => {
				let width = modifier(0).unwrap_or(1);
				let mut report =
					my(mysql_code::BIT, u32::try_from(width).ok()?, format!("bit({width})"), "bit");
				report.unsigned = true;
				report.bit_width = u32::try_from(width).ok();
				report.numeric_precision = Some(width);
				report
			}
			"json" => {
				let mut report = my(mysql_code::JSON, MYSQL_LONG_LENGTH, "json".to_owned(), "json");
				report.binary = true;
				report.blob = true;
				report
			}
			"geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
			| "multipolygon" | "geometrycollection" | "geomcollection" => {
				let name = if first == "geometrycollection" {
					"geomcollection"
				} else {
					first
				};
				let srid = match (words.next(), words.next()) {
					(Some("srid"), Some(srid)) => srid.parse().ok(),
					_ => None,
				};
				mysql_spatial(name, srid)
			}
			_ => return None,
		},
	};
	if zerofill {
		report.zerofill = true;
		report.unsigned = true;
		if !report.column_type.contains("zerofill") {
			if !report.column_type.contains("unsigned") {
				report.column_type.push_str(" unsigned");
			}
			report.column_type.push_str(" zerofill");
		}
	}
	Some(report)
}

/// MySQL `FLOAT`.
fn float() -> MySqlReportedType {
	let mut report = my(mysql_code::FLOAT, 12, "float".to_owned(), "float");
	report.decimals = NOT_FIXED_DEC;
	report.numeric_precision = Some(12);
	report
}

/// MySQL `DOUBLE`.
fn double() -> MySqlReportedType {
	let mut report = my(mysql_code::DOUBLE, 22, "double".to_owned(), "double");
	report.decimals = NOT_FIXED_DEC;
	report.numeric_precision = Some(22);
	report
}

/// The MySQL type nearest a PostgreSQL declaration: the one whose values the column's kind
/// holds. An array is a JSON document.
fn pg_as_mysql(declared: &DeclaredSqlType) -> Option<MySqlReportedType> {
	let json = || {
		let mut report = my(mysql_code::JSON, MYSQL_LONG_LENGTH, "json".to_owned(), "json");
		report.binary = true;
		report.blob = true;
		report
	};
	if !declared.array_dimensions().is_empty() {
		return Some(json());
	}
	let scalar = pg_scalar(declared)?;
	let length = scalar.length;
	let fsp = || u32::try_from(length.unwrap_or(6).clamp(0, 6)).unwrap_or(6);
	let varchar = |chars: i64| mysql_chars(mysql_code::VAR_STRING, "varchar", chars);
	let report = match scalar.base.typname {
		"bool" => {
			let mut report = my(mysql_code::TINY, 1, "tinyint(1)".to_owned(), "tinyint");
			report.numeric_precision = Some(3);
			report.numeric_scale = Some(0);
			report
		}
		"int2" => mysql_integer(mysql_code::SHORT, "smallint", false, (6, 5), (5, 5)),
		"int4" => mysql_integer(mysql_code::LONG, "int", false, (11, 10), (10, 10)),
		"int8" => mysql_integer(mysql_code::LONGLONG, "bigint", false, (20, 20), (19, 20)),
		"oid" | "xid" | "cid" | "regproc" | "regprocedure" | "regoper" | "regoperator"
		| "regclass" | "regtype" | "regnamespace" | "regrole" | "regconfig" | "regdictionary"
		| "regcollation" => mysql_integer(mysql_code::LONG, "int", true, (11, 10), (10, 10)),
		"xid8" | "pg_lsn" => {
			mysql_integer(mysql_code::LONGLONG, "bigint", true, (20, 20), (19, 20))
		}
		"float4" => float(),
		"float8" => double(),
		// An unconstrained `numeric` is reported as the widest `DECIMAL`.
		"numeric" => mysql_decimal(length.unwrap_or(65), scalar.scale.unwrap_or(30), false),
		"money" => mysql_decimal(19, 2, false),
		"bpchar" => mysql_chars(mysql_code::STRING, "char", length.unwrap_or(1)),
		"varchar" => match length {
			Some(chars) => varchar(chars),
			None => mysql_text("longtext", 4_294_967_295),
		},
		// The text-shaped types hold canonical text no longer than these.
		"inet" | "cidr" => varchar(43),
		"macaddr" => varchar(17),
		"macaddr8" => varchar(23),
		"uuid" => mysql_chars(mysql_code::STRING, "char", 36),
		// A bit string is held as its text of `0` and `1`.
		"bit" => varchar(length.unwrap_or(1)),
		"varbit" => match length {
			Some(bits) => varchar(bits),
			None => mysql_text("longtext", 4_294_967_295),
		},
		"text" | "name" | "char" | "xml" | "tsvector" | "tsquery" | "jsonpath" | "tid"
		| "pg_snapshot" | "txid_snapshot" => mysql_text("longtext", 4_294_967_295),
		"bytea" => mysql_bytes(mysql_code::BLOB, "longblob", 4_294_967_295, "longblob".to_owned()),
		"date" => my(mysql_code::DATE, 10, "date".to_owned(), "date"),
		"time" | "timetz" => mysql_temporal(mysql_code::TIME, "time", 10, fsp()),
		"timestamp" => mysql_temporal(mysql_code::DATETIME, "datetime", 19, fsp()),
		"timestamptz" => mysql_temporal(mysql_code::TIMESTAMP, "timestamp", 19, fsp()),
		"interval" => mysql_temporal(mysql_code::TIME, "time", 10, fsp()),
		"json" | "jsonb" | "line" | "circle" => json(),
		"point" => mysql_spatial("point", None),
		"lseg" | "path" => mysql_spatial("linestring", None),
		"polygon" | "box" => mysql_spatial("polygon", None),
		_ => return None,
	};
	Some(report)
}

#[cfg(test)]
#[path = "reported_type_tests.rs"]
mod tests;
