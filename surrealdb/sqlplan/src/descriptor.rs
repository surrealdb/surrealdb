//! Result descriptors, static typing, and parameter description for SQL statements.

use std::collections::HashMap;

mod common_type;
pub mod pgsql;
#[cfg(test)]
mod user_types_tests;

use surrealdb_expr::expr::statements::define::SqlTypeDialect;
use surrealdb_expr::expr::statements::define::declared_type::DeclaredSqlType;
use surrealdb_expr::expr::statements::define::sql_types::range::shape as range_shape;

use self::common_type::is_null_literal;
use crate::catalog::BinderCatalog;
use crate::dialect::{DialectPolicy, SqlDialect};
use crate::functions::FunctionRegistry;
use crate::ir::{
	BoundBinaryOp, BoundExpr, BoundLiteral, BoundSelect, BoundTableSource, BoundTarget,
	BoundUnaryOp, BoundWindowFrameBound,
};
use crate::output_keys::output_keys;

/// Abstract strongly-typed SQL data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SqlType {
	#[default]
	Unknown,
	Bool,
	Int2,
	Int4,
	Int8,
	Float4,
	Float8,
	Numeric,
	/// An unsigned 64-bit integer (`u64`, MySQL `BIGINT UNSIGNED`). PostgreSQL has no unsigned
	/// integer, so a client of that wire reads it as `numeric`; a MySQL client reads it as
	/// `BIGINT` with the `UNSIGNED` flag.
	UInt8,
	Text,
	Bytea,
	Date,
	Time,
	Timestamp,
	TimestampTz,
	Interval,
	Json,
	/// A one-dimensional array of the element type.
	Array(&'static SqlType),
	Uuid,
	Char,
	Name,
	Varchar,
	Bpchar,
	Timetz,
	Inet,
	Cidr,
	MacAddr,
	Oid,
	Regclass,
	Record,
	/// `int2vector`: the values of an array of `int2`, numbered from zero.
	Int2Vector,
	/// `oidvector`: the values of an array of `oid`, numbered from zero.
	OidVector,
	/// A `vector(n)` of the pgvector extension: a fixed-length array of floats with its own wire
	/// type and text form (`[1,2,3]`).
	Vector,
	/// PostgreSQL `money`: a decimal of two fractional digits, printed with a currency symbol.
	Money,
	/// PostgreSQL `pg_lsn`: a `u64` printed as two hexadecimal halves.
	PgLsn,
	/// PostgreSQL `macaddr8`, held as its canonical text.
	MacAddr8,
	/// PostgreSQL `bit(n)`, held as a string of `0` and `1`.
	Bit,
	/// PostgreSQL `bit varying(n)`, held as a string of `0` and `1`.
	Varbit,
	/// PostgreSQL `point`, held as a point geometry.
	Point,
	/// PostgreSQL `lseg`, held as a line of two points.
	Lseg,
	/// PostgreSQL `path`, held as a line (open) or a polygon (closed).
	Path,
	/// PostgreSQL `box`, held as the polygon ring of the box.
	PgBox,
	/// PostgreSQL `polygon`, held as a polygon.
	Polygon,
	/// PostgreSQL `line`, held as an object `{a, b, c}`.
	Line,
	/// PostgreSQL `circle`, held as an object `{x, y, r}`.
	Circle,
	/// A PostgreSQL range of the element kind, held as a range.
	Range(RangeElement),
	/// A PostgreSQL multirange of the element kind, held as an array of ranges.
	Multirange(RangeElement),
	/// MySQL `YEAR`, held as a `u16`. A client of the PostgreSQL wire reads it as `integer`.
	Year,
	/// MySQL `BIT(n)` of `n` bits, held as a `u64`. A client of the PostgreSQL wire reads it as
	/// `numeric`.
	MySqlBit(u8),
	/// A MySQL spatial column with its SRID, held as a geometry. A client of the PostgreSQL wire
	/// reads it as `json`.
	Geometry(u32),
}

pub use surrealdb_expr::expr::statements::define::sql_types::range::Element as RangeElement;

/// The `pg_type` OIDs of a range of the element kind, of its multirange, and of an array of each.
impl SqlType {
	/// The type of a bound of a range of `element`.
	pub fn of_range_element(element: RangeElement) -> SqlType {
		match element {
			RangeElement::Int4 => SqlType::Int4,
			RangeElement::Int8 => SqlType::Int8,
			RangeElement::Numeric => SqlType::Numeric,
			RangeElement::Timestamp => SqlType::Timestamp,
			RangeElement::Timestamptz => SqlType::TimestampTz,
			RangeElement::Date => SqlType::Date,
		}
	}
}

fn range_oids(element: RangeElement) -> (i32, i32, i32, i32) {
	match element {
		RangeElement::Int4 => (3904, 4451, 3905, 6150),
		RangeElement::Numeric => (3906, 4532, 3907, 6151),
		RangeElement::Timestamp => (3908, 4533, 3909, 6152),
		RangeElement::Timestamptz => (3910, 4534, 3911, 6153),
		RangeElement::Date => (3912, 4535, 3913, 6155),
		RangeElement::Int8 => (3926, 4536, 3927, 6157),
	}
}

/// Every range element kind.
const RANGE_ELEMENTS: [RangeElement; 6] = [
	RangeElement::Int4,
	RangeElement::Numeric,
	RangeElement::Timestamp,
	RangeElement::Timestamptz,
	RangeElement::Date,
	RangeElement::Int8,
];

/// The PostgreSQL name prefix of a range of the element kind: `int4` of `int4range`.
fn range_prefix(element: RangeElement) -> &'static str {
	match element {
		RangeElement::Int4 => "int4",
		RangeElement::Int8 => "int8",
		RangeElement::Numeric => "num",
		RangeElement::Timestamp => "ts",
		RangeElement::Timestamptz => "tstz",
		RangeElement::Date => "date",
	}
}

/// The wire OID of the `vector` type and of an array of it. PostgreSQL assigns an extension's type
/// an OID when it is created, and clients find it by name in `pg_type`, so any OID outside the
/// built-in range serves.
pub const VECTOR_OID: i32 = 16400;
/// The wire OID of an array of `vector`.
pub const VECTOR_ARRAY_OID: i32 = 16401;

/// The first OID a user-defined enum type takes: a type's OID is this plus its database-scoped id.
pub const ENUM_TYPE_OID_BASE: u32 = 1 << 21;
/// The distance from the OID of an enum type to the OID of an array of it, which keeps the array
/// types clear of the enum types and stable as enums are added. The engine draws no enum id at or
/// past it (`EnumId::LIMIT` in the catalog), so the two ranges never meet.
pub const ENUM_ARRAY_OID_OFFSET: u32 = 1 << 20;

/// The `pg_type` OID of the enum with database-scoped id `id`.
pub fn enum_type_oid(id: u32) -> u32 {
	ENUM_TYPE_OID_BASE + id
}

/// The `pg_type` OID of an array of the enum with database-scoped id `id`.
pub fn enum_array_type_oid(id: u32) -> u32 {
	ENUM_TYPE_OID_BASE + ENUM_ARRAY_OID_OFFSET + id
}

/// The first `pg_enum` OID, above every type OID.
pub const ENUM_LABEL_OID_BASE: u32 = 3_000_000_000;
/// The label identities of one enum are below this (`ENUM_LABEL_IDENT_LIMIT` in the catalog), so
/// each enum's labels take a block of this many OIDs.
pub const ENUM_LABEL_OID_STRIDE: u32 = 1 << 10;

/// The `pg_enum` OID of the label of enum `id` with identity `ident`: the enum's block of
/// [`ENUM_LABEL_OID_STRIDE`] OIDs, at the label's identity. Enum ids are below
/// [`ENUM_ARRAY_OID_OFFSET`] and identities below the stride, so every label of every enum takes a
/// distinct OID, below the 32-bit limit. It is stable through a rename of the label and a
/// re-spacing of the enum's sort keys, which leave its identity alone.
pub fn enum_label_oid(id: u32, ident: u32) -> u32 {
	ENUM_LABEL_OID_BASE + id * ENUM_LABEL_OID_STRIDE + ident
}

/// The first `pg_type` OID a user-defined domain takes. Its range ends at
/// [`COMPOSITE_TYPE_OID_BASE`], and no domain is an array, so a domain has no array OID.
pub const DOMAIN_TYPE_OID_BASE: u32 = 1 << 30;
/// The first `pg_type` OID a user-defined composite type takes.
pub const COMPOSITE_TYPE_OID_BASE: u32 = 5 << 28;
/// The first `pg_class` OID of the relation that a composite type names through `typrelid`. It is
/// distinct from every type OID, as PostgreSQL's composite relations are.
pub const COMPOSITE_RELATION_OID_BASE: u32 = 7 << 28;
/// The bound of the id space of user-defined domains and composite types. An id at or above it
/// would carry an OID into the next range, so the catalog never allocates one.
pub const USER_TYPE_ID_LIMIT: u32 = 1 << 28;

/// The `pg_type` OID of the domain with database-scoped id `id`.
pub fn domain_type_oid(id: u32) -> u32 {
	DOMAIN_TYPE_OID_BASE + id
}

/// The `pg_type` OID of the composite type with database-scoped id `id`.
pub fn composite_type_oid(id: u32) -> u32 {
	COMPOSITE_TYPE_OID_BASE + id
}

/// The `pg_class` OID of the relation of the composite type with database-scoped id `id`.
pub fn composite_relation_oid(id: u32) -> u32 {
	COMPOSITE_RELATION_OID_BASE + id
}

impl SqlType {
	/// Whether the type is a date, a time of day, a timestamp or an interval.
	pub fn is_temporal(self) -> bool {
		matches!(
			self,
			Self::Date
				| Self::Time | Self::Timetz
				| Self::Timestamp
				| Self::TimestampTz
				| Self::Interval
		)
	}

	/// The type of `left op right` where either operand is a date, time, timestamp or interval,
	/// by PostgreSQL's operator table, or `None` where neither is. An operand of unknown or text
	/// type beside a date, time or timestamp is an interval, which is what a quoted literal
	/// resolves to there.
	pub(crate) fn temporal_arithmetic(left: Self, op: BoundBinaryOp, right: Self) -> Option<Self> {
		let is_number = |ty: Self| {
			matches!(
				ty,
				Self::Int2 | Self::Int4 | Self::Int8 | Self::Numeric | Self::Float4 | Self::Float8
			)
		};
		let untyped = |ty: Self| matches!(ty, Self::Unknown | Self::Text);
		let as_time = |ty: Self| {
			if ty == Self::Timetz {
				Self::Time
			} else {
				ty
			}
		};
		if !left.is_temporal() && !right.is_temporal() {
			return None;
		}
		let (mut l, mut r) = (as_time(left), as_time(right));
		if matches!(op, BoundBinaryOp::Add | BoundBinaryOp::Subtract) {
			if untyped(l) && r.is_temporal() {
				l = Self::Interval;
			}
			if untyped(r) && l.is_temporal() {
				r = Self::Interval;
			}
		}
		let instant = |ty: Self| matches!(ty, Self::Date | Self::Timestamp | Self::TimestampTz);
		if op == BoundBinaryOp::Add {
			Some(match (l, r) {
				(Self::Date, n) | (n, Self::Date) if is_number(n) => Self::Date,
				(Self::Date, Self::Time) | (Self::Time, Self::Date) => Self::Timestamp,
				(Self::Date, Self::Interval) | (Self::Interval, Self::Date) => Self::Timestamp,
				(Self::Time, Self::Interval) | (Self::Interval, Self::Time) => Self::Time,
				(Self::Timestamp, Self::Interval) | (Self::Interval, Self::Timestamp) => {
					Self::Timestamp
				}
				(Self::TimestampTz, Self::Interval) | (Self::Interval, Self::TimestampTz) => {
					Self::TimestampTz
				}
				(Self::Interval, Self::Interval) => Self::Interval,
				_ => return None,
			})
		} else if op == BoundBinaryOp::Subtract {
			Some(match (l, r) {
				(Self::Date, n) if is_number(n) => Self::Date,
				(Self::Date, Self::Date) => Self::Int4,
				(Self::Date, Self::Interval) => Self::Timestamp,
				(Self::Time, Self::Interval) => Self::Time,
				(Self::Time, Self::Time) => Self::Interval,
				(Self::Timestamp, Self::Interval) => Self::Timestamp,
				(Self::TimestampTz, Self::Interval) => Self::TimestampTz,
				(Self::Interval, Self::Interval) => Self::Interval,
				(a, b) if instant(a) && instant(b) => Self::Interval,
				_ => return None,
			})
		} else if op == BoundBinaryOp::Multiply {
			match (l, r) {
				(Self::Interval, n) | (n, Self::Interval) if is_number(n) => Some(Self::Interval),
				_ => None,
			}
		} else if op == BoundBinaryOp::Divide {
			match (l, r) {
				(Self::Interval, n) if is_number(n) => Some(Self::Interval),
				_ => None,
			}
		} else {
			None
		}
	}

	/// A scalar PostgreSQL OID that this descriptor can represent exactly.
	pub fn from_pg_oid(oid: i32) -> Option<Self> {
		match oid {
			16 => Some(Self::Bool),
			17 => Some(Self::Bytea),
			18 => Some(Self::Char),
			19 => Some(Self::Name),
			20 => Some(Self::Int8),
			21 => Some(Self::Int2),
			23 => Some(Self::Int4),
			25 => Some(Self::Text),
			26 => Some(Self::Oid),
			114 => Some(Self::Json),
			650 => Some(Self::Cidr),
			700 => Some(Self::Float4),
			701 => Some(Self::Float8),
			829 => Some(Self::MacAddr),
			869 => Some(Self::Inet),
			1042 => Some(Self::Bpchar),
			1043 => Some(Self::Varchar),
			1082 => Some(Self::Date),
			1083 => Some(Self::Time),
			1114 => Some(Self::Timestamp),
			1184 => Some(Self::TimestampTz),
			1186 => Some(Self::Interval),
			1266 => Some(Self::Timetz),
			1700 => Some(Self::Numeric),
			2205 => Some(Self::Regclass),
			2249 => Some(Self::Record),
			22 => Some(Self::Int2Vector),
			30 => Some(Self::OidVector),
			2950 => Some(Self::Uuid),
			VECTOR_OID => Some(Self::Vector),
			790 => Some(Self::Money),
			3220 => Some(Self::PgLsn),
			774 => Some(Self::MacAddr8),
			1560 => Some(Self::Bit),
			1562 => Some(Self::Varbit),
			600 => Some(Self::Point),
			601 => Some(Self::Lseg),
			602 => Some(Self::Path),
			603 => Some(Self::PgBox),
			604 => Some(Self::Polygon),
			628 => Some(Self::Line),
			718 => Some(Self::Circle),
			_ => RANGE_ELEMENTS.into_iter().find_map(|element| {
				let (range, multirange, _, _) = range_oids(element);
				if oid == range {
					Some(Self::Range(element))
				} else if oid == multirange {
					Some(Self::Multirange(element))
				} else {
					None
				}
			}),
		}
	}

	/// PostgreSQL wire type OID.
	pub fn to_pg_oid(&self) -> i32 {
		match self {
			Self::Unknown => 0, // 0 = unspecified
			Self::Bool => 16,
			Self::Bytea => 17,
			Self::Char => 18,
			Self::Name => 19,
			Self::Int8 => 20,
			Self::Int2 => 21,
			Self::Int4 => 23,
			Self::Text => 25,
			Self::Oid => 26,
			Self::Json => 114,
			Self::Cidr => 650,
			Self::Float4 => 700,
			Self::Float8 => 701,
			Self::MacAddr => 829,
			Self::Inet => 869,
			Self::Bpchar => 1042,
			Self::Varchar => 1043,
			Self::Date => 1082,
			Self::Time => 1083,
			Self::Timestamp => 1114,
			Self::TimestampTz => 1184,
			Self::Interval => 1186,
			Self::Timetz => 1266,
			Self::Numeric | Self::UInt8 => 1700,
			Self::Regclass => 2205,
			Self::Record => 2249,
			Self::Int2Vector => 22,
			Self::OidVector => 30,
			Self::Vector => VECTOR_OID,
			Self::Array(elem) => elem.pg_array_oid(),
			Self::Uuid => 2950,
			Self::Money => 790,
			Self::PgLsn => 3220,
			Self::MacAddr8 => 774,
			Self::Bit => 1560,
			Self::Varbit => 1562,
			Self::Point => 600,
			Self::Lseg => 601,
			Self::Path => 602,
			Self::PgBox => 603,
			Self::Polygon => 604,
			Self::Line => 628,
			Self::Circle => 718,
			Self::Range(element) => range_oids(*element).0,
			Self::Multirange(element) => range_oids(*element).1,
			Self::Year => 23,
			Self::MySqlBit(_) => 1700,
			Self::Geometry(_) => 114,
		}
	}

	/// PostgreSQL wire OID of a one-dimensional array of this type, or `anyarray` when
	/// PostgreSQL has no array type for it.
	fn pg_array_oid(self) -> i32 {
		match self {
			Self::Bool => 1000,
			Self::Bytea => 1001,
			Self::Char => 1002,
			Self::Name => 1003,
			Self::Int2 => 1005,
			Self::Int4 => 1007,
			Self::Text => 1009,
			Self::Bpchar => 1014,
			Self::Varchar => 1015,
			Self::Int8 => 1016,
			Self::Float4 => 1021,
			Self::Float8 => 1022,
			Self::Oid => 1028,
			Self::MacAddr => 1040,
			Self::Inet => 1041,
			Self::Cidr => 651,
			Self::Timestamp => 1115,
			Self::Date => 1182,
			Self::Time => 1183,
			Self::TimestampTz => 1185,
			Self::Interval => 1187,
			Self::Numeric | Self::UInt8 => 1231,
			Self::Timetz => 1270,
			Self::Json => 199,
			Self::Uuid => 2951,
			Self::Regclass => 2210,
			Self::Record => 2287,
			Self::Int2Vector => 1006,
			Self::OidVector => 1013,
			Self::Vector => VECTOR_ARRAY_OID,
			Self::Money => 791,
			Self::PgLsn => 3221,
			Self::MacAddr8 => 775,
			Self::Bit => 1561,
			Self::Varbit => 1563,
			Self::Point => 1017,
			Self::Lseg => 1018,
			Self::Path => 1019,
			Self::PgBox => 1020,
			Self::Polygon => 1027,
			Self::Line => 629,
			Self::Circle => 719,
			Self::Range(element) => range_oids(element).2,
			Self::Multirange(element) => range_oids(element).3,
			Self::Year => 1007,
			Self::MySqlBit(_) => 1231,
			Self::Geometry(_) => 199,
			// PostgreSQL gives every dimension of an array the OID of its scalar's array type.
			Self::Array(inner) => inner.pg_array_oid(),
			Self::Unknown => 2277,
		}
	}

	/// A one-dimensional array of `elem`, or `None` when `elem` is unknown or a vector-like type
	/// with no array type. An array of a scalar is a two-dimensional array of that scalar, and
	/// deeper nesting is `None`.
	pub fn array_of(elem: Self) -> Option<Self> {
		let elem: &'static Self = match elem {
			Self::Array(inner) => return array_of_array(inner),
			Self::Bool => &Self::Bool,
			Self::Int2 => &Self::Int2,
			Self::Int4 => &Self::Int4,
			Self::Int8 => &Self::Int8,
			Self::Float4 => &Self::Float4,
			Self::Float8 => &Self::Float8,
			Self::Numeric => &Self::Numeric,
			Self::UInt8 => &Self::UInt8,
			Self::Text => &Self::Text,
			Self::Bytea => &Self::Bytea,
			Self::Date => &Self::Date,
			Self::Time => &Self::Time,
			Self::Timestamp => &Self::Timestamp,
			Self::TimestampTz => &Self::TimestampTz,
			Self::Interval => &Self::Interval,
			Self::Json => &Self::Json,
			Self::Uuid => &Self::Uuid,
			Self::Char => &Self::Char,
			Self::Name => &Self::Name,
			Self::Varchar => &Self::Varchar,
			Self::Bpchar => &Self::Bpchar,
			Self::Timetz => &Self::Timetz,
			Self::Inet => &Self::Inet,
			Self::Cidr => &Self::Cidr,
			Self::MacAddr => &Self::MacAddr,
			Self::Oid => &Self::Oid,
			Self::Regclass => &Self::Regclass,
			Self::Record => &Self::Record,
			Self::Vector => &Self::Vector,
			Self::Money => &Self::Money,
			Self::PgLsn => &Self::PgLsn,
			Self::MacAddr8 => &Self::MacAddr8,
			Self::Bit => &Self::Bit,
			Self::Varbit => &Self::Varbit,
			Self::Point => &Self::Point,
			Self::Lseg => &Self::Lseg,
			Self::Path => &Self::Path,
			Self::PgBox => &Self::PgBox,
			Self::Polygon => &Self::Polygon,
			Self::Line => &Self::Line,
			Self::Circle => &Self::Circle,
			Self::Range(element) => match element {
				RangeElement::Int4 => &Self::Range(RangeElement::Int4),
				RangeElement::Int8 => &Self::Range(RangeElement::Int8),
				RangeElement::Numeric => &Self::Range(RangeElement::Numeric),
				RangeElement::Timestamp => &Self::Range(RangeElement::Timestamp),
				RangeElement::Timestamptz => &Self::Range(RangeElement::Timestamptz),
				RangeElement::Date => &Self::Range(RangeElement::Date),
			},
			Self::Multirange(element) => match element {
				RangeElement::Int4 => &Self::Multirange(RangeElement::Int4),
				RangeElement::Int8 => &Self::Multirange(RangeElement::Int8),
				RangeElement::Numeric => &Self::Multirange(RangeElement::Numeric),
				RangeElement::Timestamp => &Self::Multirange(RangeElement::Timestamp),
				RangeElement::Timestamptz => &Self::Multirange(RangeElement::Timestamptz),
				RangeElement::Date => &Self::Multirange(RangeElement::Date),
			},
			Self::Year => &Self::Year,
			// MySQL has no array of a bit string or of a geometry.
			Self::Unknown
			| Self::Int2Vector
			| Self::OidVector
			| Self::MySqlBit(_)
			| Self::Geometry(_) => return None,
		};
		Some(Self::Array(elem))
	}

	/// PostgreSQL type length in bytes (-1 for variable length).
	pub fn pg_typlen(&self) -> i16 {
		match self {
			Self::Bool | Self::Char => 1,
			Self::Int2 => 2,
			Self::Int4 | Self::Float4 | Self::Date | Self::Oid | Self::Regclass => 4,
			Self::MacAddr => 6,
			Self::Int8 | Self::Float8 | Self::Timestamp | Self::TimestampTz | Self::Time => 8,
			Self::Timetz => 12,
			Self::Interval | Self::Uuid | Self::Point => 16,
			Self::Name => 64,
			Self::Money | Self::PgLsn | Self::MacAddr8 => 8,
			Self::Line | Self::Circle => 24,
			Self::Lseg | Self::PgBox => 32,
			Self::Year => 4,
			Self::Bit
			| Self::Varbit
			| Self::Path
			| Self::Polygon
			| Self::Range(_)
			| Self::Multirange(_)
			| Self::MySqlBit(_)
			| Self::Geometry(_)
			| Self::Unknown
			| Self::Numeric
			| Self::UInt8
			| Self::Text
			| Self::Varchar
			| Self::Bpchar
			| Self::Bytea
			| Self::Json
			| Self::Array(_)
			| Self::Inet
			| Self::Cidr
			| Self::Int2Vector
			| Self::OidVector
			| Self::Vector
			| Self::Record => -1,
		}
	}

	/// Whether a MySQL column of this type is announced with the `UNSIGNED` flag.
	pub fn mysql_unsigned(&self) -> bool {
		matches!(self, Self::UInt8 | Self::PgLsn | Self::MySqlBit(_) | Self::Year)
	}

	/// MySQL wire field type.
	pub fn to_mysql_type(&self) -> u8 {
		match self {
			Self::Unknown => 0xfd,                           // MYSQL_TYPE_VAR_STRING
			Self::Bool => 0x01,                              // MYSQL_TYPE_TINY
			Self::Int2 => 0x02,                              // MYSQL_TYPE_SHORT
			Self::Int4 | Self::Oid | Self::Regclass => 0x03, // MYSQL_TYPE_LONG
			Self::Int8 => 0x08,                              // MYSQL_TYPE_LONGLONG
			Self::Float4 => 0x04,                            // MYSQL_TYPE_FLOAT
			Self::Float8 => 0x05,                            // MYSQL_TYPE_DOUBLE
			Self::Numeric => 0xf6,                           // MYSQL_TYPE_NEWDECIMAL
			Self::UInt8 => 0x08,                             // MYSQL_TYPE_LONGLONG, UNSIGNED
			Self::Text
			| Self::Name
			| Self::Varchar
			| Self::Bpchar
			| Self::Char
			| Self::Inet
			| Self::Cidr
			| Self::MacAddr
			| Self::MacAddr8
			| Self::Bit
			| Self::Varbit
			| Self::Range(_)
			| Self::Multirange(_) => 0xfd, // MYSQL_TYPE_VAR_STRING
			Self::Bytea => 0xfc,                             // MYSQL_TYPE_BLOB
			Self::Date => 0x0a,                              // MYSQL_TYPE_DATE
			Self::Time | Self::Timetz | Self::Interval => 0x0b, // MYSQL_TYPE_TIME
			Self::Timestamp => 0x0c,                         // MYSQL_TYPE_DATETIME
			Self::TimestampTz => 0x07,                       // MYSQL_TYPE_TIMESTAMP
			Self::Uuid => 0xfd,                              // MYSQL_TYPE_VAR_STRING
			Self::Money => 0xf6,                             // MYSQL_TYPE_NEWDECIMAL
			Self::PgLsn => 0x08,                             // MYSQL_TYPE_LONGLONG, UNSIGNED
			Self::Year => 0x0d,                              // MYSQL_TYPE_YEAR
			Self::MySqlBit(_) => 0x10,                       // MYSQL_TYPE_BIT
			Self::Point
			| Self::Lseg
			| Self::Path
			| Self::PgBox
			| Self::Polygon
			| Self::Geometry(_) => 0xff, // MYSQL_TYPE_GEOMETRY
			Self::Json
			| Self::Line
			| Self::Circle
			| Self::Array(_)
			| Self::Vector
			| Self::Int2Vector
			| Self::OidVector
			| Self::Record => 0xf5, // MYSQL_TYPE_JSON
		}
	}

	/// The name `information_schema.columns.data_type` gives this type, which
	/// [`Self::from_name`] reads back as the same type, except [`Self::UInt8`], which has the name
	/// of `numeric` because PostgreSQL has no unsigned integer. An array is its element's name
	/// followed by `[]`, and an array of arrays is named as the one-dimensional array of its
	/// scalar, since PostgreSQL names every dimension alike. [`Self::Unknown`] has no name.
	pub fn name(&self) -> Option<String> {
		let name = match self {
			Self::Unknown => return None,
			Self::Array(elem) => {
				let scalar = if let Self::Array(inner) = **elem {
					*inner
				} else {
					**elem
				};
				return scalar.name().map(|name| format!("{name}[]"));
			}
			Self::Bool => "boolean",
			Self::Int2 => "smallint",
			Self::Int4 => "integer",
			Self::Int8 => "bigint",
			Self::Float4 => "real",
			Self::Float8 => "double precision",
			Self::Numeric | Self::UInt8 => "numeric",
			Self::Text => "text",
			Self::Bytea => "bytea",
			Self::Date => "date",
			Self::Time => "time without time zone",
			Self::Timestamp => "timestamp without time zone",
			Self::TimestampTz => "timestamp with time zone",
			Self::Interval => "interval",
			Self::Json => "jsonb",
			Self::Uuid => "uuid",
			Self::Char => "\"char\"",
			Self::Name => "name",
			Self::Varchar => "character varying",
			Self::Bpchar => "character",
			Self::Timetz => "time with time zone",
			Self::Inet => "inet",
			Self::Cidr => "cidr",
			Self::MacAddr => "macaddr",
			Self::Oid => "oid",
			Self::Regclass => "regclass",
			Self::Record => "record",
			Self::Int2Vector => "int2vector",
			Self::OidVector => "oidvector",
			Self::Vector => "vector",
			Self::Money => "money",
			Self::PgLsn => "pg_lsn",
			Self::MacAddr8 => "macaddr8",
			Self::Bit => "bit",
			Self::Varbit => "bit varying",
			Self::Point => "point",
			Self::Lseg => "lseg",
			Self::Path => "path",
			Self::PgBox => "box",
			Self::Polygon => "polygon",
			Self::Line => "line",
			Self::Circle => "circle",
			Self::Range(element) => return Some(format!("{}range", range_prefix(*element))),
			Self::Multirange(element) => {
				return Some(format!("{}multirange", range_prefix(*element)));
			}
			Self::Year => "integer",
			Self::MySqlBit(_) => "numeric",
			Self::Geometry(_) => "jsonb",
		};
		Some(name.to_owned())
	}

	/// PostgreSQL's internal type name (`pg_type.typname`, `information_schema.columns.udt_name`):
	/// `int8`, `bool`, `timestamptz`, and `_text` for an array of `text`. [`Self::Unknown`] has
	/// no name.
	pub fn udt_name(&self) -> Option<String> {
		let name = match self {
			Self::Unknown => return None,
			Self::Array(elem) => {
				let scalar = if let Self::Array(inner) = **elem {
					*inner
				} else {
					**elem
				};
				return scalar.udt_name().map(|name| format!("_{name}"));
			}
			Self::Bool => "bool",
			Self::Int2 => "int2",
			Self::Int4 => "int4",
			Self::Int8 => "int8",
			Self::Float4 => "float4",
			Self::Float8 => "float8",
			Self::Numeric | Self::UInt8 => "numeric",
			Self::Text => "text",
			Self::Bytea => "bytea",
			Self::Date => "date",
			Self::Time => "time",
			Self::Timestamp => "timestamp",
			Self::TimestampTz => "timestamptz",
			Self::Interval => "interval",
			Self::Json => "jsonb",
			Self::Uuid => "uuid",
			Self::Char => "char",
			Self::Name => "name",
			Self::Varchar => "varchar",
			Self::Bpchar => "bpchar",
			Self::Timetz => "timetz",
			Self::Inet => "inet",
			Self::Cidr => "cidr",
			Self::MacAddr => "macaddr",
			Self::Oid => "oid",
			Self::Regclass => "regclass",
			Self::Record => "record",
			Self::Int2Vector => "int2vector",
			Self::OidVector => "oidvector",
			Self::Vector => "vector",
			Self::Money => "money",
			Self::PgLsn => "pg_lsn",
			Self::MacAddr8 => "macaddr8",
			Self::Bit => "bit",
			Self::Varbit => "varbit",
			Self::Point => "point",
			Self::Lseg => "lseg",
			Self::Path => "path",
			Self::PgBox => "box",
			Self::Polygon => "polygon",
			Self::Line => "line",
			Self::Circle => "circle",
			Self::Range(element) => return Some(format!("{}range", range_prefix(*element))),
			Self::Multirange(element) => {
				return Some(format!("{}multirange", range_prefix(*element)));
			}
			Self::Year => "int4",
			Self::MySqlBit(_) => "numeric",
			Self::Geometry(_) => "jsonb",
		};
		Some(name.to_owned())
	}

	/// Parse SQL type from a string name (e.g. "int4", "varchar", "boolean", "text", etc.). A name
	/// followed by `[]` is an array of that type.
	pub fn from_name(name: &str) -> Self {
		let lower = name.to_ascii_lowercase();
		// `vector(3)` is a vector: its dimensions do not change how it travels.
		let lower = match lower.strip_prefix("vector(") {
			Some(_) => "vector".to_string(),
			None => lower,
		};
		if let Some(elem) = lower.strip_suffix("[]") {
			return Self::array_of(Self::from_name(elem)).unwrap_or(Self::Unknown);
		}
		// `numeric(40,10)` is a numeric: its precision and scale do not change how it travels.
		let lower = match lower.split_once('(') {
			Some((base, _)) if matches!(base.trim(), "numeric" | "decimal" | "dec") => {
				base.trim().to_string()
			}
			Some(_) | None => lower,
		};
		// `interval day to second(3)` and `interval(2)` are intervals: the field qualifier and
		// the precision bound the value, not how it travels.
		let base = lower.split('(').next().unwrap_or_default().trim();
		if base == "interval" || base.strip_prefix("interval ").is_some_and(|f| !f.is_empty()) {
			return Self::Interval;
		}
		match lower.as_str() {
			"bool" | "boolean" => Self::Bool,
			"int2" | "smallint" => Self::Int2,
			"int4" | "int" | "integer" => Self::Int4,
			"int8" | "bigint" => Self::Int8,
			"float4" | "real" => Self::Float4,
			"float8" | "double" | "double precision" | "float" => Self::Float8,
			"numeric" | "decimal" => Self::Numeric,
			"text" | "string" => Self::Text,
			"name" => Self::Name,
			"varchar" | "character varying" => Self::Varchar,
			"bpchar" | "char" | "character" => Self::Bpchar,
			"\"char\"" => Self::Char,
			"bytea" | "blob" | "bytes" => Self::Bytea,
			"date" => Self::Date,
			"time" | "time without time zone" => Self::Time,
			"timetz" | "time with time zone" => Self::Timetz,
			"timestamp" | "timestamp without time zone" => Self::Timestamp,
			"timestamptz" | "timestamp with time zone" => Self::TimestampTz,
			"interval" => Self::Interval,
			"uuid" => Self::Uuid,
			"json" | "jsonb" => Self::Json,
			"array" => Self::Array(&Self::Text),
			"inet" => Self::Inet,
			"cidr" => Self::Cidr,
			"macaddr" => Self::MacAddr,
			"oid" => Self::Oid,
			"regclass" => Self::Regclass,
			"record" => Self::Record,
			"int2vector" => Self::Int2Vector,
			"oidvector" => Self::OidVector,
			"vector" => Self::Vector,
			"money" => Self::Money,
			"pg_lsn" => Self::PgLsn,
			"macaddr8" => Self::MacAddr8,
			"bit" => Self::Bit,
			"bit varying" | "varbit" => Self::Varbit,
			"point" => Self::Point,
			"lseg" => Self::Lseg,
			"path" => Self::Path,
			"box" => Self::PgBox,
			"polygon" => Self::Polygon,
			"line" => Self::Line,
			"circle" => Self::Circle,
			other => match range_shape(other) {
				Some((element, false)) => Self::Range(element),
				Some((element, true)) => Self::Multirange(element),
				None => Self::Unknown,
			},
		}
	}

	/// The wire type a declared SQL type names where the field's kind alone does not say it: the
	/// PostgreSQL network, bit-string, `money`, `pg_lsn`, geometric, and range types, and MySQL
	/// `YEAR`, `BIT(n)`, and the spatial types. An array of a PostgreSQL one is an array of it.
	/// `None` for every other declared type, whose wire type follows from the kind.
	pub fn of_declared(declared: &DeclaredSqlType) -> Option<Self> {
		let scalar = match declared.dialect() {
			SqlTypeDialect::PostgreSql => match declared.base() {
				"inet" => Self::Inet,
				"cidr" => Self::Cidr,
				"macaddr" => Self::MacAddr,
				"macaddr8" => Self::MacAddr8,
				"bit" => Self::Bit,
				"bit varying" | "varbit" => Self::Varbit,
				"money" => Self::Money,
				"pg_lsn" => Self::PgLsn,
				"point" => Self::Point,
				"lseg" => Self::Lseg,
				"path" => Self::Path,
				"box" => Self::PgBox,
				"polygon" => Self::Polygon,
				"line" => Self::Line,
				"circle" => Self::Circle,
				other => match range_shape(other)? {
					(element, false) => Self::Range(element),
					(element, true) => Self::Multirange(element),
				},
			},
			SqlTypeDialect::MySql => {
				if !declared.array_dimensions().is_empty() {
					return None;
				}
				let mut words = declared.base().split(' ');
				match words.next()? {
					"year" => Self::Year,
					"bit" => {
						let width = match declared.modifiers().first() {
							Some(width) => width.parse::<u8>().ok()?,
							None => 1,
						};
						Self::MySqlBit(width)
					}
					"geometry" | "point" | "linestring" | "polygon" | "multipoint"
					| "multilinestring" | "multipolygon" | "geometrycollection"
					| "geomcollection" => {
						let srid = match (words.next(), words.next()) {
							(Some("srid"), Some(srid)) => srid.parse::<u32>().ok()?,
							_ => 0,
						};
						Self::Geometry(srid)
					}
					_ => return None,
				}
			}
			SqlTypeDialect::DynamoDb => return None,
		};
		let mut ty = scalar;
		for _ in declared.array_dimensions() {
			ty = Self::array_of(ty)?;
		}
		Some(ty)
	}
}

/// Description of a single projected output column. A statement that is not a select describes
/// the columns of its result by their name and type and leaves every other field at its default.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResultColumn {
	/// Wire name of column (alias, column name, or ?column? in PG / expr in MySQL).
	pub name: String,
	/// Inferred SQL data type.
	pub ty: SqlType,
	/// Source table if resolved.
	pub table: Option<String>,
	/// Unqualified original column name if resolved.
	pub original_column: Option<String>,
	/// The field of each row of the plan of [`lower_bound_select`](crate::lower_bound_select) that
	/// holds this column's value, which is not the name a client is shown: two columns may share a
	/// name and a column may have none. It is distinct among the columns of one result. `None`
	/// where the rows are read by name: the select has a `*` or `t.*` the planner cannot list, so
	/// its columns are known only from its rows, or the rows are not those of a select.
	pub key: Option<String>,
	/// Whether the column is a `*` or `t.*` whose columns are known only from the rows that come
	/// back, as opposed to a column that merely has the name `*`.
	pub dynamic: bool,
	/// The enum whose labels the column holds, when the column reads a column declared as one (or
	/// as a set or an array of one). The wire types such a column by it rather than by its values.
	pub enum_type: Option<ResultEnum>,
	/// The composite type whose values the column holds, when the column reads a column declared
	/// as one. The wire types such a column by its fields.
	pub composite: Option<ResultComposite>,
	/// The SQL type the column is declared with, when the column reads a base-table column
	/// declared with one (`SQL TYPE`). The wire reports the column as that type, or as the
	/// nearest type of its own dialect, in place of [`Self::ty`].
	pub declared: Option<crate::declared_type::DeclaredSqlType>,
}

/// The composite type a result column holds the values of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultComposite {
	/// The database-scoped id of the type, which its wire type is allocated from.
	pub id: u32,
	/// The fields in declaration order, each with the SQL type its values travel as.
	pub fields: Vec<(String, SqlType)>,
}

/// The enum a result column holds the labels of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultEnum {
	/// The database-scoped id of a named enum, which its wire type is allocated from. `None` for
	/// an enum declared inline in a column, which has no type of its own.
	pub id: Option<u32>,
	/// The enum's labels in declaration order.
	pub labels: Vec<String>,
	/// Whether a value holds one label, a set of them or a list.
	pub shape: crate::catalog::EnumShape,
}

/// Descriptor of a query result set produced at plan time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultDescriptor {
	/// Ordered projected columns.
	pub columns: Vec<ResultColumn>,
}

/// The two-dimensional array of the scalar `inner`, or `None` when `inner` is not a scalar that
/// has an array type. [`SqlType::array_of`] is the only caller; each scalar is listed so that its
/// array is a constant and can be borrowed for `'static`.
fn array_of_array(inner: &SqlType) -> Option<SqlType> {
	let elem: &'static SqlType = match *inner {
		SqlType::Bool => &SqlType::Array(&SqlType::Bool),
		SqlType::Int2 => &SqlType::Array(&SqlType::Int2),
		SqlType::Int4 => &SqlType::Array(&SqlType::Int4),
		SqlType::Int8 => &SqlType::Array(&SqlType::Int8),
		SqlType::Float4 => &SqlType::Array(&SqlType::Float4),
		SqlType::Float8 => &SqlType::Array(&SqlType::Float8),
		SqlType::Numeric => &SqlType::Array(&SqlType::Numeric),
		SqlType::UInt8 => &SqlType::Array(&SqlType::UInt8),
		SqlType::Text => &SqlType::Array(&SqlType::Text),
		SqlType::Bytea => &SqlType::Array(&SqlType::Bytea),
		SqlType::Date => &SqlType::Array(&SqlType::Date),
		SqlType::Time => &SqlType::Array(&SqlType::Time),
		SqlType::Timestamp => &SqlType::Array(&SqlType::Timestamp),
		SqlType::TimestampTz => &SqlType::Array(&SqlType::TimestampTz),
		SqlType::Interval => &SqlType::Array(&SqlType::Interval),
		SqlType::Json => &SqlType::Array(&SqlType::Json),
		SqlType::Uuid => &SqlType::Array(&SqlType::Uuid),
		SqlType::Char => &SqlType::Array(&SqlType::Char),
		SqlType::Name => &SqlType::Array(&SqlType::Name),
		SqlType::Varchar => &SqlType::Array(&SqlType::Varchar),
		SqlType::Bpchar => &SqlType::Array(&SqlType::Bpchar),
		SqlType::Timetz => &SqlType::Array(&SqlType::Timetz),
		SqlType::Inet => &SqlType::Array(&SqlType::Inet),
		SqlType::Cidr => &SqlType::Array(&SqlType::Cidr),
		SqlType::MacAddr => &SqlType::Array(&SqlType::MacAddr),
		SqlType::Oid => &SqlType::Array(&SqlType::Oid),
		SqlType::Regclass => &SqlType::Array(&SqlType::Regclass),
		SqlType::Record => &SqlType::Array(&SqlType::Record),
		SqlType::Vector => &SqlType::Array(&SqlType::Vector),
		// No two-dimensional array of these is declared.
		SqlType::Unknown
		| SqlType::Array(_)
		| SqlType::Int2Vector
		| SqlType::OidVector
		| SqlType::Money
		| SqlType::PgLsn
		| SqlType::MacAddr8
		| SqlType::Bit
		| SqlType::Varbit
		| SqlType::Point
		| SqlType::Lseg
		| SqlType::Path
		| SqlType::PgBox
		| SqlType::Polygon
		| SqlType::Line
		| SqlType::Circle
		| SqlType::Range(_)
		| SqlType::Multirange(_)
		| SqlType::Year
		| SqlType::MySqlBit(_)
		| SqlType::Geometry(_) => {
			return None;
		}
	};
	Some(SqlType::Array(elem))
}

impl ResultDescriptor {
	/// Whether a column is `*` or `t.*` over a relation whose columns the binder could not list:
	/// the result's columns are known only from the rows that come back. A column that is aliased
	/// `*` is a column like any other.
	pub fn has_dynamic_columns(&self) -> bool {
		self.columns.iter().any(|column| column.dynamic)
	}
}

/// Descriptor of statement parameters ($1..$N or ?).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterDescriptor {
	/// Types for parameters 1..N.
	pub param_types: Vec<SqlType>,
	/// For parameters 1..N, the id of the enum type whose column the parameter is compared with
	/// or assigned to, which gives the parameter that type on the wire. Empty when no parameter
	/// has one.
	pub enum_params: Vec<Option<u32>>,
}

/// Descriptor of a prepared statement before execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementDescriptor {
	/// Statement returns a result set with known columns.
	Rows(ResultDescriptor),
	/// Non-returning statement (e.g. DML/DDL without RETURNING) yielding NoData reply.
	NoData {
		tag: &'static str,
	},
}

impl BoundSelect {
	/// Whether a projection is `*` or `t.*` over a relation whose columns the
	/// binder cannot enumerate, so the result's columns are known only from the
	/// rows that come back.
	pub fn has_dynamic_columns(&self) -> bool {
		self.targets.iter().any(BoundTarget::is_unlisted_wildcard)
	}

	/// The declared types of the columns a `*` or `t.*` projection stands for, when the query
	/// reads a single base table: each column the catalog declares by name. The columns the
	/// rows hold beyond these stay typed from their values. Empty when the projection lists
	/// its columns, or the query reads anything but one base table.
	pub fn wildcard_column_types(
		&self,
		catalog: &dyn BinderCatalog,
	) -> std::collections::HashMap<String, SqlType> {
		let Some(BoundTableSource::Table {
			schema,
			name,
			..
		}) = &self.from
		else {
			return std::collections::HashMap::new();
		};
		if !self.has_dynamic_columns() || self.set_op.is_some() {
			return std::collections::HashMap::new();
		}
		catalog
			.declared_column_types(schema.as_deref(), name)
			.into_iter()
			.filter(|(_, ty)| *ty != SqlType::Unknown)
			.collect()
	}

	/// The SQL types the columns a `*` or `t.*` projection stands for are declared with, by
	/// column name, when the query reads a single base table: see [`Self::wildcard_column_types`].
	pub fn wildcard_declared_sql_types(
		&self,
		catalog: &dyn BinderCatalog,
	) -> std::collections::HashMap<String, crate::declared_type::DeclaredSqlType> {
		let Some(BoundTableSource::Table {
			schema,
			name,
			..
		}) = &self.from
		else {
			return std::collections::HashMap::new();
		};
		if !self.has_dynamic_columns() || self.set_op.is_some() {
			return std::collections::HashMap::new();
		}
		catalog.declared_sql_types(schema.as_deref(), name).into_iter().collect()
	}

	/// The result descriptor, or `None` when the columns are not known before
	/// the statement runs (see [`Self::has_dynamic_columns`]). A caller holding
	/// `None` shapes the result from its rows.
	pub fn describe_static(
		&self,
		catalog: &dyn BinderCatalog,
		dialect: &DialectPolicy,
	) -> Option<ResultDescriptor> {
		if self.has_dynamic_columns() {
			None
		} else {
			Some(self.describe(catalog, dialect))
		}
	}

	/// Compute the result descriptor for this bound SELECT query.
	pub fn describe(
		&self,
		catalog: &dyn BinderCatalog,
		dialect: &DialectPolicy,
	) -> ResultDescriptor {
		let mut columns = Vec::with_capacity(self.targets.len());
		let mut keys = output_keys(&self.targets).map(Vec::into_iter);
		for target in &self.targets {
			let key = keys.as_mut().and_then(Iterator::next);
			if target.is_wildcard {
				columns.push(ResultColumn {
					name: "*".to_string(),
					ty: SqlType::Unknown,
					table: None,
					original_column: None,
					key,
					dynamic: true,
					enum_type: None,
					composite: None,
					declared: None,
				});
				continue;
			}
			let (name, table, orig_col) = match &target.alias {
				Some(alias) => (alias.clone(), None, None),
				None => match &target.expr {
					BoundExpr::Column(col) => (
						col.column.clone(),
						Some(col.relation_name.clone()),
						Some(col.column.clone()),
					),
					BoundExpr::FunctionCall {
						name,
						..
					}
					| BoundExpr::WindowFunction {
						name,
						..
					} => (name.clone(), None, None),
					// A field of a composite value, `(c).f`, is named by the field.
					BoundExpr::Access {
						field,
						..
					} => (field.clone(), None, None),
					BoundExpr::UserVar(name) => (format!("@{name}"), None, None),
					BoundExpr::SystemVar {
						name,
						is_global,
					} => (
						if *is_global {
							format!("@@global.{name}")
						} else {
							format!("@@{name}")
						},
						None,
						None,
					),
					BoundExpr::Literal(_)
					| BoundExpr::Param {
						..
					}
					| BoundExpr::Binary {
						..
					}
					| BoundExpr::Unary {
						..
					}
					| BoundExpr::Case {
						..
					}
					| BoundExpr::Between {
						..
					}
					| BoundExpr::InList {
						..
					}
					| BoundExpr::IsNull {
						..
					}
					| BoundExpr::Cast {
						..
					}
					| BoundExpr::ScalarSubquery(_)
					| BoundExpr::Exists(_)
					| BoundExpr::InSubquery {
						..
					}
					| BoundExpr::ArraySubquery(_) => {
						if dialect.dialect == SqlDialect::Postgres {
							("?column?".to_string(), None, None)
						} else {
							("?".to_string(), None, None)
						}
					}
				},
			};
			let mut ty = self.infer_expr_type(&target.expr, catalog);
			if dialect.dialect == SqlDialect::Postgres && is_case_of_integer_constants(&target.expr)
			{
				ty = SqlType::Int4;
			} else if dialect.dialect == SqlDialect::Postgres
				&& let Some(narrow) = self.pg_narrow_integer_arithmetic(&target.expr, catalog)
			{
				ty = narrow;
			} else if dialect.dialect == SqlDialect::MySql
				&& self.is_sum_of_float(&target.expr, catalog)
			{
				// MySQL adds approximate values as `DOUBLE`.
				ty = SqlType::Float8;
			}
			let dynamic = target.alias.is_none() && target.is_unlisted_wildcard();
			columns.push(ResultColumn {
				name,
				ty,
				table,
				original_column: orig_col,
				key,
				dynamic,
				enum_type: self.result_enum(&target.expr, catalog),
				composite: self.result_composite(&target.expr, catalog),
				declared: match &target.expr {
					BoundExpr::Column(col) => col.declared_sql.as_deref().cloned(),
					BoundExpr::Literal(_)
					| BoundExpr::Access {
						..
					}
					| BoundExpr::Param {
						..
					}
					| BoundExpr::Binary {
						..
					}
					| BoundExpr::Unary {
						..
					}
					| BoundExpr::FunctionCall {
						..
					}
					| BoundExpr::Case {
						..
					}
					| BoundExpr::Between {
						..
					}
					| BoundExpr::InList {
						..
					}
					| BoundExpr::IsNull {
						..
					}
					| BoundExpr::Cast {
						..
					}
					| BoundExpr::ScalarSubquery(_)
					| BoundExpr::Exists(_)
					| BoundExpr::InSubquery {
						..
					}
					| BoundExpr::ArraySubquery(_)
					| BoundExpr::WindowFunction {
						..
					}
					| BoundExpr::UserVar(_)
					| BoundExpr::SystemVar {
						..
					} => None,
				},
			});
		}
		ResultDescriptor {
			columns,
		}
	}

	/// The enum an expression holds the labels of: a column declared as one, or the `MIN` or
	/// `MAX` of such a column.
	fn result_enum(&self, expr: &BoundExpr, catalog: &dyn BinderCatalog) -> Option<ResultEnum> {
		let held = match expr {
			BoundExpr::Column(col) => col.enum_column.as_ref(),
			BoundExpr::Cast {
				target_type,
				..
			} if target_type.starts_with("enum:") => {
				let spec = &target_type["enum:".len()..];
				let (name, array) = match spec.strip_suffix("[]") {
					Some(name) => (name, true),
					None => (spec, false),
				};
				let definition = catalog.enum_type(name)?;
				return Some(ResultEnum {
					id: Some(definition.id),
					labels: definition.labels.into_iter().map(|l| l.label).collect(),
					shape: if array {
						crate::catalog::EnumShape::Array
					} else {
						crate::catalog::EnumShape::Scalar
					},
				});
			}
			BoundExpr::FunctionCall {
				name,
				args,
				..
			} if matches!(name.as_str(), "min" | "max") => match args.as_slice() {
				[BoundExpr::Column(col)] => col
					.enum_column
					.as_ref()
					.filter(|held| held.shape == crate::catalog::EnumShape::Scalar),
				_ => None,
			},
			BoundExpr::Literal(_)
			| BoundExpr::Access {
				..
			}
			| BoundExpr::Param {
				..
			}
			| BoundExpr::Binary {
				..
			}
			| BoundExpr::Unary {
				..
			}
			| BoundExpr::FunctionCall {
				..
			}
			| BoundExpr::Case {
				..
			}
			| BoundExpr::Between {
				..
			}
			| BoundExpr::InList {
				..
			}
			| BoundExpr::IsNull {
				..
			}
			| BoundExpr::Cast {
				..
			}
			| BoundExpr::ScalarSubquery(_)
			| BoundExpr::Exists(_)
			| BoundExpr::InSubquery {
				..
			}
			| BoundExpr::ArraySubquery(_)
			| BoundExpr::WindowFunction {
				..
			}
			| BoundExpr::UserVar(_)
			| BoundExpr::SystemVar {
				..
			} => None,
		}?;
		match &held.reference {
			crate::catalog::EnumRef::Named(name) => {
				let definition = catalog.enum_type(name)?;
				Some(ResultEnum {
					id: Some(definition.id),
					labels: definition.labels.into_iter().map(|l| l.label).collect(),
					shape: held.shape,
				})
			}
			crate::catalog::EnumRef::Inline(labels) => Some(ResultEnum {
				id: None,
				labels: labels.clone(),
				shape: held.shape,
			}),
		}
	}

	/// The composite type an expression holds the values of: a column declared as one.
	fn result_composite(
		&self,
		expr: &BoundExpr,
		catalog: &dyn BinderCatalog,
	) -> Option<ResultComposite> {
		let BoundExpr::Column(col) = expr else {
			return None;
		};
		let definition = catalog.composite_type(col.composite.as_deref()?)?;
		Some(ResultComposite {
			id: definition.id,
			fields: definition
				.fields
				.iter()
				.map(|(field, kind)| (field.clone(), definition.attribute_sql_type(field, kind)))
				.collect(),
		})
	}

	/// Infer the SQL type of an expression in the context of this SELECT.
	pub fn infer_expr_type(&self, expr: &BoundExpr, catalog: &dyn BinderCatalog) -> SqlType {
		match expr {
			BoundExpr::Literal(lit) => match lit {
				BoundLiteral::Integer(_) => SqlType::Int8,
				// An exact unsigned 64-bit value is announced as such, not as `numeric`.
				BoundLiteral::Unsigned(_) => SqlType::UInt8,
				BoundLiteral::Float(_) => SqlType::Float8,
				BoundLiteral::Numeric(_) => SqlType::Numeric,
				BoundLiteral::String(_) => SqlType::Text,
				BoundLiteral::Boolean(_) => SqlType::Bool,
				BoundLiteral::Null => SqlType::Unknown,
			},
			BoundExpr::Column(col) => {
				if col.declared_type != SqlType::Unknown {
					return col.declared_type;
				}
				if !col.relation_name.is_empty() {
					let dt = catalog.get_table(None, &col.relation_name).and_then(|tbl| {
						tbl.find_column(&col.column, true).and_then(|c| c.data_type.clone())
					});
					if let Some(dt) = dt {
						return SqlType::from_name(&dt);
					}
				}
				SqlType::Unknown
			}
			BoundExpr::Cast {
				target_type,
				..
			} => SqlType::from_name(target_type),
			BoundExpr::Binary {
				left,
				op,
				right,
			} => match op {
				BoundBinaryOp::Equal
				| BoundBinaryOp::NotEqual
				| BoundBinaryOp::LessThan
				| BoundBinaryOp::LessThanEqual
				| BoundBinaryOp::GreaterThan
				| BoundBinaryOp::GreaterThanEqual
				| BoundBinaryOp::And
				| BoundBinaryOp::Or
				| BoundBinaryOp::NullSafeEqual
				| BoundBinaryOp::DistinctFrom
				| BoundBinaryOp::Like
				| BoundBinaryOp::ILike
				| BoundBinaryOp::Regexp
				| BoundBinaryOp::RegexpCi => SqlType::Bool,
				BoundBinaryOp::Power => SqlType::Float8,
				BoundBinaryOp::Concat => SqlType::Text,
				BoundBinaryOp::IntDiv => SqlType::Int8,
				BoundBinaryOp::JsonExtract | BoundBinaryOp::JsonPathExtract => SqlType::Json,
				BoundBinaryOp::JsonExtractText | BoundBinaryOp::JsonPathExtractText => {
					SqlType::Text
				}
				BoundBinaryOp::JsonContains => SqlType::Bool,
				BoundBinaryOp::VectorDistance(_) => SqlType::Float8,
				BoundBinaryOp::Add
				| BoundBinaryOp::Subtract
				| BoundBinaryOp::Multiply
				| BoundBinaryOp::Divide
				| BoundBinaryOp::Remainder => {
					let lt = self.infer_expr_type(left, catalog);
					let rt = self.infer_expr_type(right, catalog);
					if let Some(temporal) = SqlType::temporal_arithmetic(lt, *op, rt) {
						return temporal;
					}
					if lt == SqlType::Float8
						|| rt == SqlType::Float8
						|| lt == SqlType::Float4
						|| rt == SqlType::Float4
					{
						SqlType::Float8
					} else if matches!(lt, SqlType::Numeric | SqlType::UInt8)
						|| matches!(rt, SqlType::Numeric | SqlType::UInt8)
					{
						SqlType::Numeric
					} else if lt == SqlType::Int8 || rt == SqlType::Int8 {
						SqlType::Int8
					} else if lt == SqlType::Int4 || rt == SqlType::Int4 {
						SqlType::Int4
					} else if lt == SqlType::Int2 || rt == SqlType::Int2 {
						SqlType::Int2
					} else {
						SqlType::Int8
					}
				}
			},
			BoundExpr::Unary {
				op,
				expr,
			} => match op {
				BoundUnaryOp::Not => SqlType::Bool,
				BoundUnaryOp::Positive | BoundUnaryOp::Negate => {
					self.infer_expr_type(expr, catalog)
				}
			},
			BoundExpr::IsNull {
				..
			}
			| BoundExpr::Between {
				..
			}
			| BoundExpr::InList {
				..
			} => SqlType::Bool,
			BoundExpr::FunctionCall {
				name,
				args,
				..
			} => {
				let arg_types: Vec<Option<SqlType>> = args
					.iter()
					.map(|a| (!is_null_literal(a)).then(|| self.infer_expr_type(a, catalog)))
					.collect();
				FunctionRegistry::infer_return_type_of_arguments(name, &arg_types)
			}
			BoundExpr::WindowFunction {
				name,
				args,
				..
			} => {
				let arg_types: Vec<_> =
					args.iter().map(|a| self.infer_expr_type(a, catalog)).collect();
				FunctionRegistry::infer_return_type_any(name, &arg_types)
			}
			BoundExpr::Param {
				..
			} => SqlType::Unknown,
			BoundExpr::Access {
				..
			} => SqlType::Unknown,
			BoundExpr::Case {
				branches,
				else_expr,
				..
			} => {
				// A NULL branch has no value of its own, so it does not take part in the type of
				// the others.
				SqlType::common(
					branches
						.iter()
						.map(|(_, then_expr)| then_expr)
						.chain(else_expr.as_deref())
						.filter(|branch| !is_null_literal(branch))
						.map(|branch| self.infer_expr_type(branch, catalog)),
				)
			}
			BoundExpr::ScalarSubquery(sub) => {
				if let Some(target) = sub.targets.first() {
					self.infer_expr_type(&target.expr, catalog)
				} else {
					SqlType::Unknown
				}
			}
			BoundExpr::Exists(_) => SqlType::Bool,
			BoundExpr::InSubquery {
				..
			} => SqlType::Bool,
			BoundExpr::ArraySubquery(_) => SqlType::Unknown,
			BoundExpr::UserVar(_) => SqlType::Unknown,
			BoundExpr::SystemVar {
				name,
				..
			} => match crate::system_vars::mysql_system_variable(name) {
				Some(
					crate::system_vars::SystemVariable::Int(_)
					| crate::system_vars::SystemVariable::Flag(_),
				) => SqlType::Int8,
				Some(crate::system_vars::SystemVariable::Text(_)) | None => SqlType::Text,
			},
		}
	}

	/// Infer parameter types based on their usage in expressions and predicates.
	pub fn describe_parameters(&self, catalog: &dyn BinderCatalog) -> ParameterDescriptor {
		let mut param_map = HashMap::new();

		if let Some(where_clause) = &self.where_clause {
			self.collect_param_types_from_expr(where_clause, catalog, &mut param_map);
		}

		for target in &self.targets {
			self.collect_param_types_from_expr(&target.expr, catalog, &mut param_map);
		}

		for order in &self.order_by {
			self.collect_param_types_from_expr(&order.expr, catalog, &mut param_map);
		}

		if let Some(with) = &self.with {
			for cte in &with.ctes {
				if let Some(where_clause) = &cte.query.where_clause {
					cte.query.collect_param_types_from_expr(where_clause, catalog, &mut param_map);
				}
				for target in &cte.query.targets {
					cte.query.collect_param_types_from_expr(&target.expr, catalog, &mut param_map);
				}
			}
		}

		for win in &self.windows {
			for p in &win.spec.partition_by {
				self.collect_param_types_from_expr(p, catalog, &mut param_map);
			}
			for o in &win.spec.order_by {
				self.collect_param_types_from_expr(&o.expr, catalog, &mut param_map);
			}
		}

		for expression in self.limit.iter().chain(self.offset.iter()) {
			self.collect_param_types_from_expr(expression, catalog, &mut param_map);
			if let BoundExpr::Param {
				index,
				..
			} = expression
			{
				let ty = param_map.entry(*index).or_insert(SqlType::Int8);
				if *ty == SqlType::Unknown {
					*ty = SqlType::Int8;
				}
			}
		}

		if let Some(source) = &self.from {
			self.collect_source_parameters(source, catalog, &mut param_map);
		}

		let max_idx = param_map.keys().copied().max().unwrap_or(0);
		let mut param_types = Vec::with_capacity(max_idx);
		for idx in 1..=max_idx {
			param_types.push(param_map.get(&idx).copied().unwrap_or(SqlType::Unknown));
		}

		let mut enum_params = HashMap::new();
		for expr in self
			.where_clause
			.iter()
			.chain(self.having.iter())
			.chain(self.targets.iter().map(|target| &target.expr))
		{
			Self::collect_enum_params(expr, catalog, &mut enum_params);
		}
		ParameterDescriptor {
			enum_params: Self::enum_param_slots(&enum_params, param_types.len()),
			param_types,
		}
	}

	/// The enum parameters `enum_params` holds as one slot per parameter of `count`.
	pub(crate) fn enum_param_slots(
		enum_params: &HashMap<usize, u32>,
		count: usize,
	) -> Vec<Option<u32>> {
		if enum_params.is_empty() {
			return Vec::new();
		}
		(1..=count).map(|index| enum_params.get(&index).copied()).collect()
	}

	/// Notes the parameters that `expr` compares with, or assigns to, a column holding one enum
	/// label, by the id of the enum's type.
	pub(crate) fn collect_enum_params(
		expr: &BoundExpr,
		catalog: &dyn BinderCatalog,
		out: &mut HashMap<usize, u32>,
	) {
		let enum_id = |expr: &BoundExpr| -> Option<u32> {
			let reference = crate::lower::enum_rank::scalar_enum(expr)?;
			match reference {
				crate::catalog::EnumRef::Named(name) => {
					catalog.enum_type(name).map(|definition| definition.id)
				}
				crate::catalog::EnumRef::Inline(_) => None,
			}
		};
		match expr {
			BoundExpr::Binary {
				left,
				right,
				..
			} => {
				for (param, other) in [(&**left, &**right), (&**right, &**left)] {
					if let BoundExpr::Param {
						index,
						..
					} = param && let Some(id) = enum_id(other)
					{
						out.insert(*index, id);
					}
				}
				Self::collect_enum_params(left, catalog, out);
				Self::collect_enum_params(right, catalog, out);
			}
			BoundExpr::Unary {
				expr,
				..
			}
			| BoundExpr::IsNull {
				expr,
				..
			} => Self::collect_enum_params(expr, catalog, out),
			BoundExpr::InList {
				expr,
				list,
				..
			} => {
				if let Some(id) = enum_id(expr) {
					for item in list {
						if let BoundExpr::Param {
							index,
							..
						} = item
						{
							out.insert(*index, id);
						}
					}
				}
			}
			BoundExpr::Between {
				expr,
				low,
				high,
				..
			} => {
				if let Some(id) = enum_id(expr) {
					for bound in [low, high] {
						if let BoundExpr::Param {
							index,
							..
						} = &**bound
						{
							out.insert(*index, id);
						}
					}
				}
			}
			BoundExpr::Literal(_)
			| BoundExpr::Column(_)
			| BoundExpr::Access {
				..
			}
			| BoundExpr::Param {
				..
			}
			| BoundExpr::FunctionCall {
				..
			}
			| BoundExpr::Case {
				..
			}
			| BoundExpr::Cast {
				..
			}
			| BoundExpr::ScalarSubquery(_)
			| BoundExpr::Exists(_)
			| BoundExpr::InSubquery {
				..
			}
			| BoundExpr::ArraySubquery(_)
			| BoundExpr::WindowFunction {
				..
			}
			| BoundExpr::UserVar(_)
			| BoundExpr::SystemVar {
				..
			} => {}
		}
	}

	fn collect_source_parameters(
		&self,
		source: &BoundTableSource,
		catalog: &dyn BinderCatalog,
		params: &mut HashMap<usize, SqlType>,
	) {
		match source {
			BoundTableSource::Subquery {
				query,
				..
			} => {
				for (index, ty) in
					query.describe_parameters(catalog).param_types.into_iter().enumerate()
				{
					let existing = params.entry(index + 1).or_insert(ty);
					if *existing == SqlType::Unknown {
						*existing = ty;
					}
				}
			}
			BoundTableSource::Join {
				left,
				right,
				..
			} => {
				self.collect_source_parameters(left, catalog, params);
				self.collect_source_parameters(right, catalog, params);
			}
			BoundTableSource::Lateral {
				targets,
				..
			} => {
				for target in targets {
					self.collect_param_types_from_expr(&target.expr, catalog, params);
				}
			}
			BoundTableSource::Table {
				..
			}
			| BoundTableSource::Values => {}
		}
	}

	/// Gives each parameter among `peers` (the operand and bounds of a `BETWEEN`, the subject
	/// and items of an `IN` list) the [common type](SqlType::common) of the peers whose type is
	/// known, as PostgreSQL resolves the operands of these predicates to one type. A string
	/// literal or `NULL` is of unknown type there, as a parameter is, and takes its type from the
	/// others. Peers with no common type leave the parameters untyped. A parameter already typed
	/// keeps its type.
	fn type_peer_params<'e>(
		&self,
		peers: impl IntoIterator<Item = &'e BoundExpr> + Clone,
		catalog: &dyn BinderCatalog,
		params: &mut HashMap<usize, SqlType>,
	) {
		let typed = peers
			.clone()
			.into_iter()
			.filter(|peer| {
				!matches!(
					peer,
					BoundExpr::Param { .. }
						| BoundExpr::Literal(BoundLiteral::String(_) | BoundLiteral::Null)
				)
			})
			.map(|peer| self.infer_expr_type(peer, catalog))
			.collect::<Vec<_>>();
		let ty = SqlType::common(typed);
		if ty == SqlType::Unknown {
			return;
		}
		for peer in peers {
			if let BoundExpr::Param {
				index,
				..
			} = peer
			{
				params
					.entry(*index)
					.and_modify(|existing| {
						if *existing == SqlType::Unknown {
							*existing = ty;
						}
					})
					.or_insert(ty);
			}
		}
	}

	/// Types a parameter operand of the comparison `left op right` from the other operand: its
	/// inferred type, or for another parameter the type already collected for it. A parameter
	/// that already has a known type keeps it.
	fn type_comparison_params(
		&self,
		left: &BoundExpr,
		right: &BoundExpr,
		catalog: &dyn BinderCatalog,
		params: &mut HashMap<usize, SqlType>,
	) {
		for (operand, other) in [(left, right), (right, left)] {
			let BoundExpr::Param {
				index,
				..
			} = operand
			else {
				continue;
			};
			let ty = if let BoundExpr::Param {
				index: other_index,
				..
			} = other
			{
				params.get(other_index).copied().unwrap_or(SqlType::Unknown)
			} else {
				self.infer_expr_type(other, catalog)
			};
			if ty != SqlType::Unknown {
				params
					.entry(*index)
					.and_modify(|existing| {
						if *existing == SqlType::Unknown {
							*existing = ty;
						}
					})
					.or_insert(ty);
			}
		}
	}

	fn collect_param_types_from_expr(
		&self,
		expr: &BoundExpr,
		catalog: &dyn BinderCatalog,
		params: &mut HashMap<usize, SqlType>,
	) {
		match expr {
			BoundExpr::Binary {
				left,
				op,
				right,
			} => {
				// A parameter of a distance is the vector it is the distance to.
				if let BoundBinaryOp::VectorDistance(_) = op {
					for operand in [left, right] {
						if let BoundExpr::Param {
							index,
							..
						} = &**operand
						{
							params.insert(*index, SqlType::Vector);
						}
					}
				}
				self.type_comparison_params(left, right, catalog, params);
				self.collect_param_types_from_expr(left, catalog, params);
				self.collect_param_types_from_expr(right, catalog, params);
			}
			BoundExpr::Cast {
				expr,
				target_type,
			} => {
				if let BoundExpr::Param {
					index,
					..
				} = &**expr
				{
					let ty = SqlType::from_name(target_type);
					if ty != SqlType::Unknown {
						params.insert(*index, ty);
					}
				}
				self.collect_param_types_from_expr(expr, catalog, params);
			}
			BoundExpr::Unary {
				expr,
				..
			}
			| BoundExpr::IsNull {
				expr,
				..
			} => {
				self.collect_param_types_from_expr(expr, catalog, params);
			}
			BoundExpr::FunctionCall {
				name,
				args,
				..
			} => {
				// `x op ANY/ALL ($n)`: the parameter is an array of x's type. An operand whose
				// type the catalog cannot state is presented as text everywhere else, so its
				// array is `text[]`.
				if name == crate::functions::QUANTIFIED_ARRAY
					&& let [
						lhs,
						BoundExpr::Param {
							index,
							..
						},
						..,
					] = args.as_slice()
					&& let element = self.infer_expr_type(lhs, catalog)
					&& let Some(array_ty) = SqlType::array_of(if element == SqlType::Unknown {
						SqlType::Text
					} else {
						element
					}) {
					params
						.entry(*index)
						.and_modify(|existing| {
							if *existing == SqlType::Unknown {
								*existing = array_ty;
							}
						})
						.or_insert(array_ty);
				}
				// A parameter handed to a vector function is the vector it takes.
				if crate::vector::is_distance_function(name) || name == "inner_product" {
					for arg in args {
						if let BoundExpr::Param {
							index,
							..
						} = arg
						{
							params.insert(*index, SqlType::Vector);
						}
					}
				}
				for arg in args {
					self.collect_param_types_from_expr(arg, catalog, params);
				}
			}
			BoundExpr::Case {
				operand,
				branches,
				else_expr,
			} => {
				if let Some(op) = operand {
					self.collect_param_types_from_expr(op, catalog, params);
				}
				for (cond, val) in branches {
					self.collect_param_types_from_expr(cond, catalog, params);
					self.collect_param_types_from_expr(val, catalog, params);
				}
				if let Some(el) = else_expr {
					self.collect_param_types_from_expr(el, catalog, params);
				}
			}
			BoundExpr::Between {
				expr,
				low,
				high,
				..
			} => {
				// `a BETWEEN b AND c` is typed as `a >= b` then `a <= c`, so a parameter takes its
				// type from the first comparison that fixes it.
				self.type_comparison_params(expr, low, catalog, params);
				self.type_comparison_params(expr, high, catalog, params);
				self.collect_param_types_from_expr(expr, catalog, params);
				self.collect_param_types_from_expr(low, catalog, params);
				self.collect_param_types_from_expr(high, catalog, params);
			}
			BoundExpr::InList {
				expr,
				list,
				..
			} => {
				self.type_peer_params(std::iter::once(&**expr).chain(list), catalog, params);
				self.collect_param_types_from_expr(expr, catalog, params);
				for item in list {
					self.collect_param_types_from_expr(item, catalog, params);
				}
			}
			BoundExpr::InSubquery {
				expr,
				..
			} => {
				self.collect_param_types_from_expr(expr, catalog, params);
			}
			BoundExpr::WindowFunction {
				args,
				spec,
				..
			} => {
				for arg in args {
					self.collect_param_types_from_expr(arg, catalog, params);
				}
				for p in &spec.partition_by {
					self.collect_param_types_from_expr(p, catalog, params);
				}
				for o in &spec.order_by {
					self.collect_param_types_from_expr(&o.expr, catalog, params);
				}
				if let Some(frame) = &spec.frame {
					if let BoundWindowFrameBound::Preceding(e)
					| BoundWindowFrameBound::Following(e) = &frame.start
					{
						self.collect_param_types_from_expr(e, catalog, params);
					}
					if let Some(
						BoundWindowFrameBound::Preceding(e) | BoundWindowFrameBound::Following(e),
					) = &frame.end
					{
						self.collect_param_types_from_expr(e, catalog, params);
					}
				}
			}
			BoundExpr::Param {
				index,
				..
			} => {
				params.entry(*index).or_insert(SqlType::Unknown);
			}
			BoundExpr::Literal(_)
			| BoundExpr::Column(_)
			| BoundExpr::Access {
				..
			}
			| BoundExpr::ScalarSubquery(_)
			| BoundExpr::Exists(_)
			| BoundExpr::ArraySubquery(_)
			| BoundExpr::UserVar(_)
			| BoundExpr::SystemVar {
				..
			} => {}
		}
	}
}

impl BoundSelect {
	/// The type PostgreSQL gives an arithmetic expression over a `smallint` or `integer` operand
	/// and an integer constant that fits `integer`: the constant is an `integer` there, so the
	/// result keeps the narrow type (`smallint` only for two `smallint` operands). `None` for any
	/// other expression.
	fn pg_narrow_integer_arithmetic(
		&self,
		expr: &BoundExpr,
		catalog: &dyn BinderCatalog,
	) -> Option<SqlType> {
		match expr {
			BoundExpr::Binary {
				left,
				op,
				right,
			} => {
				if !matches!(
					op,
					BoundBinaryOp::Add
						| BoundBinaryOp::Subtract
						| BoundBinaryOp::Multiply
						| BoundBinaryOp::Divide
						| BoundBinaryOp::Remainder
				) {
					return None;
				}
				match (
					self.pg_narrow_operand(left, catalog)?,
					self.pg_narrow_operand(right, catalog)?,
				) {
					(SqlType::Int2, SqlType::Int2) => Some(SqlType::Int2),
					(SqlType::Float4, SqlType::Float4) => Some(SqlType::Float4),
					(SqlType::Float4, _) | (_, SqlType::Float4) => None,
					_ => Some(SqlType::Int4),
				}
			}
			BoundExpr::Unary {
				op: BoundUnaryOp::Negate | BoundUnaryOp::Positive,
				expr,
			} => self.pg_narrow_operand(expr, catalog),
			BoundExpr::Literal(_)
			| BoundExpr::Column(_)
			| BoundExpr::Access {
				..
			}
			| BoundExpr::Param {
				..
			}
			| BoundExpr::Unary {
				..
			}
			| BoundExpr::FunctionCall {
				..
			}
			| BoundExpr::Case {
				..
			}
			| BoundExpr::Between {
				..
			}
			| BoundExpr::InList {
				..
			}
			| BoundExpr::IsNull {
				..
			}
			| BoundExpr::Cast {
				..
			}
			| BoundExpr::ScalarSubquery(_)
			| BoundExpr::Exists(_)
			| BoundExpr::InSubquery {
				..
			}
			| BoundExpr::ArraySubquery(_)
			| BoundExpr::WindowFunction {
				..
			}
			| BoundExpr::UserVar(_)
			| BoundExpr::SystemVar {
				..
			} => None,
		}
	}

	/// Whether `expr` is a `SUM`, plain or windowed, over a single-precision column or expression.
	fn is_sum_of_float(&self, expr: &BoundExpr, catalog: &dyn BinderCatalog) -> bool {
		let (BoundExpr::FunctionCall {
			name,
			args,
			..
		}
		| BoundExpr::WindowFunction {
			name,
			args,
			..
		}) = expr
		else {
			return false;
		};
		name.eq_ignore_ascii_case("sum")
			&& matches!(args.as_slice(), [arg] if self.infer_expr_type(arg, catalog) == SqlType::Float4)
	}

	/// The narrow type of an operand of such an arithmetic expression: an integer constant that
	/// fits `integer` is an `integer`, a `smallint`, `integer` or `real` column or expression keeps
	/// its type, and a sign or an enclosed arithmetic expression is read through.
	fn pg_narrow_operand(&self, expr: &BoundExpr, catalog: &dyn BinderCatalog) -> Option<SqlType> {
		if let BoundExpr::Literal(BoundLiteral::Integer(n)) = expr
			&& i32::try_from(*n).is_ok()
		{
			return Some(SqlType::Int4);
		}
		if is_case_of_integer_constants(expr) {
			return Some(SqlType::Int4);
		}
		if let Some(ty) = self.pg_narrow_choice(expr, catalog) {
			return Some(ty);
		}
		let ty = self.infer_expr_type(expr, catalog);
		if matches!(ty, SqlType::Int2 | SqlType::Int4 | SqlType::Float4) {
			Some(ty)
		} else {
			self.pg_narrow_integer_arithmetic(expr, catalog)
		}
	}
}

impl BoundSelect {
	/// The narrow type of a `COALESCE`, `GREATEST` or `LEAST` whose every argument other than a
	/// NULL constant is a narrow operand of one type family, or of a `NULLIF`, whose type is its
	/// first argument's: PostgreSQL gives these calls the type their arguments share, so
	/// `i + COALESCE(1, 2)` is `integer` as `i + 1` is. `None` for any other expression.
	fn pg_narrow_choice(&self, expr: &BoundExpr, catalog: &dyn BinderCatalog) -> Option<SqlType> {
		let BoundExpr::FunctionCall {
			name,
			args,
			..
		} = expr
		else {
			return None;
		};
		let chosen: Vec<&BoundExpr> = match name.to_ascii_lowercase().as_str() {
			"nullif" => args.first().into_iter().collect(),
			"coalesce" | "greatest" | "least" => args
				.iter()
				.filter(|arg| !matches!(arg, BoundExpr::Literal(BoundLiteral::Null)))
				.collect(),
			_ => return None,
		};
		let mut types = Vec::with_capacity(chosen.len());
		for arg in chosen {
			types.push(self.pg_narrow_operand(arg, catalog)?);
		}
		let integer = |ty: &SqlType| matches!(ty, SqlType::Int2 | SqlType::Int4);
		if types.is_empty() {
			None
		} else if types.iter().all(|ty| *ty == SqlType::Int2) {
			Some(SqlType::Int2)
		} else if types.iter().all(integer) {
			Some(SqlType::Int4)
		} else if types.iter().all(|ty| *ty == SqlType::Float4) {
			Some(SqlType::Float4)
		} else {
			None
		}
	}
}

/// Whether `expr` is a CASE whose every result is an integer constant that fits `integer`, which
/// PostgreSQL types as `integer`, as it types the constants.
fn is_case_of_integer_constants(expr: &BoundExpr) -> bool {
	let BoundExpr::Case {
		branches,
		else_expr,
		..
	} = expr
	else {
		return false;
	};
	let fits_int4 = |expr: &BoundExpr| matches!(expr, BoundExpr::Literal(BoundLiteral::Integer(n)) if i32::try_from(*n).is_ok());
	!branches.is_empty()
		&& branches.iter().all(|(_, then)| fits_int4(then))
		&& else_expr.as_deref().is_none_or(fits_int4)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::EmptyCatalog;

	#[test]
	fn a_type_name_reads_back_as_the_same_type() {
		let scalars = [
			SqlType::Bool,
			SqlType::Int2,
			SqlType::Int4,
			SqlType::Int8,
			SqlType::Float4,
			SqlType::Float8,
			SqlType::Numeric,
			SqlType::Text,
			SqlType::Bytea,
			SqlType::Date,
			SqlType::Time,
			SqlType::Timestamp,
			SqlType::TimestampTz,
			SqlType::Interval,
			SqlType::Json,
			SqlType::Uuid,
			SqlType::Char,
			SqlType::Name,
			SqlType::Varchar,
			SqlType::Bpchar,
			SqlType::Timetz,
			SqlType::Inet,
			SqlType::Cidr,
			SqlType::MacAddr,
			SqlType::Oid,
			SqlType::Regclass,
			SqlType::Record,
		];
		for ty in scalars {
			let name = ty.name().expect("a scalar has a name");
			assert_eq!(SqlType::from_name(&name), ty, "{name}");
			let array = SqlType::array_of(ty).expect("a scalar has an array");
			let name = array.name().expect("an array has a name");
			assert_eq!(SqlType::from_name(&name), array, "{name}");
		}
		assert_eq!(SqlType::Unknown.name(), None);
		assert_eq!(SqlType::Int8.name().as_deref(), Some("bigint"));
		assert_eq!(
			SqlType::array_of(SqlType::Text).and_then(|t| t.name()).as_deref(),
			Some("text[]")
		);
	}

	fn pg(sql: &str) -> (BoundSelect, DialectPolicy) {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let policy = DialectPolicy::postgres();
		(
			crate::binder::pgsql::bind_select(select, &EmptyCatalog, &policy, None).expect("bind"),
			policy,
		)
	}

	fn my(sql: &str) -> (BoundSelect, DialectPolicy) {
		let stmts = surrealdb_mysql::parse_statements(sql).expect("parse");
		let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let policy = DialectPolicy::mysql();
		(
			crate::binder::mysql::bind_select(select, &EmptyCatalog, &policy, None).expect("bind"),
			policy,
		)
	}

	#[test]
	fn a_wildcard_is_known_only_from_the_rows() {
		for (bound, policy) in [
			pg("SELECT * FROM person"),
			pg("SELECT *, age FROM person"),
			my("SELECT * FROM person"),
			my("SELECT p.* FROM person p"),
		] {
			assert!(bound.has_dynamic_columns());
			assert!(bound.describe_static(&EmptyCatalog, &policy).is_none());
			assert!(bound.describe(&EmptyCatalog, &policy).has_dynamic_columns());
		}
	}

	#[test]
	fn a_column_named_star_is_a_column_like_any_other() {
		for (bound, policy) in [my("SELECT age, `*` FROM person"), my("SELECT p.`*` FROM person p")]
		{
			assert!(!bound.has_dynamic_columns());
			let descriptor = bound.describe_static(&EmptyCatalog, &policy).expect("static");
			assert!(!descriptor.has_dynamic_columns());
			assert_eq!(descriptor.columns.last().map(|c| c.name.as_str()), Some("*"));
		}
	}

	#[test]
	fn a_column_aliased_star_is_a_column_like_any_other() {
		for (bound, policy) in
			[pg("SELECT age, name AS \"*\" FROM person"), my("SELECT age, name AS `*` FROM person")]
		{
			assert!(!bound.has_dynamic_columns());
			let descriptor = bound.describe_static(&EmptyCatalog, &policy).expect("static");
			assert!(!descriptor.has_dynamic_columns());
			let names: Vec<&str> = descriptor.columns.iter().map(|c| c.name.as_str()).collect();
			assert_eq!(names, ["age", "*"]);
		}
	}
}

#[cfg(test)]
mod declared_type_tests {
	use super::*;
	use crate::catalog::{BinderCatalog, MemoryCatalog};

	#[test]
	fn scalar_type_hints_survive_aliases_without_enumerating_columns() {
		let catalog = MemoryCatalog::new()
			.with_column_type("people", "age", SqlType::Int8)
			.with_column_type("people", "enabled", SqlType::Bool);
		assert!(catalog.get_table(None, "people").is_none());
		let stmts = surrealdb_pgsql::parse_statements(
			"SELECT p.age, p.enabled FROM people p WHERE p.age = $2 AND $1 IS NULL",
		)
		.unwrap();
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("SELECT");
		};
		let policy = DialectPolicy::postgres();
		let bound = crate::binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
		let desc = bound.describe(&catalog, &policy);
		assert_eq!(
			desc.columns.iter().map(|col| col.ty).collect::<Vec<_>>(),
			vec![SqlType::Int8, SqlType::Bool]
		);
		assert_eq!(
			bound.describe_parameters(&catalog).param_types,
			vec![SqlType::Unknown, SqlType::Int8]
		);
	}
}
