use std::collections::{BTreeSet, HashMap};

use surrealdb_sqlplan::declared_type::DeclaredSqlType;
use surrealdb_sqlplan::descriptor::{
	VECTOR_ARRAY_OID, VECTOR_OID, composite_type_oid, enum_array_type_oid, enum_type_oid,
};
use surrealdb_sqlplan::reported_type::pg_reported_type;
use surrealdb_types::{Number, Value};

/// The Postgres wire types emitted by the listener, with their catalog OIDs.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) enum PgType {
	Bool,
	Bytea,
	Char,
	Name,
	Int8,
	Int2,
	Int4,
	Text,
	Oid,
	Json,
	Cidr,
	Float4,
	Float8,
	MacAddr,
	Inet,
	Bpchar,
	Varchar,
	Date,
	Time,
	Timestamp,
	Timestamptz,
	Interval,
	Timetz,
	Numeric,
	Regclass,
	Record,
	Uuid,
	Jsonb,
	/// The result type of a function that returns nothing; its value is an empty string.
	Void,
	/// The `vector` type of the pgvector extension.
	Vector,
	/// The types below travel in text format only. A value of one is its canonical string; a
	/// binary parameter or column of one is refused until its codec exists.
	Money,
	Bit,
	Varbit,
	Xml,
	Tsvector,
	Tsquery,
	Jsonpath,
	Macaddr8,
	Regproc,
	Regprocedure,
	Regoper,
	Regoperator,
	Regtype,
	Regnamespace,
	Regrole,
	Regconfig,
	Regdictionary,
	Regcollation,
	Xid,
	Cid,
	Xid8,
	PgLsn,
	Tid,
	PgSnapshot,
	TxidSnapshot,
	Point,
	Lseg,
	Path,
	/// PostgreSQL `box`. Named apart from `Box`, which the enum itself uses.
	PgBox,
	Polygon,
	Line,
	Circle,
	Int4range,
	Numrange,
	Tsrange,
	Tstzrange,
	Daterange,
	Int8range,
	Int4multirange,
	Nummultirange,
	Tsmultirange,
	Tstzmultirange,
	Datemultirange,
	Int8multirange,
	/// A user-defined enum type, by its database-scoped id. A value travels as its label.
	Enum(u32),
	/// A user-defined composite type, by its database-scoped id, with the wire type of each field
	/// in declaration order. A value travels as a record of its fields.
	Composite {
		id: u32,
		fields: Vec<(String, PgType)>,
	},
	Array(Box<PgType>),
}

impl PgType {
	pub(super) fn oid(&self) -> i32 {
		match self {
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
			Self::Timestamptz => 1184,
			Self::Interval => 1186,
			Self::Timetz => 1266,
			Self::Numeric => 1700,
			Self::Regclass => 2205,
			Self::Record => 2249,
			Self::Uuid => 2950,
			Self::Jsonb => 3802,
			Self::Void => 2278,
			Self::Vector => VECTOR_OID,
			Self::Money => 790,
			Self::Bit => 1560,
			Self::Varbit => 1562,
			Self::Xml => 142,
			Self::Tsvector => 3614,
			Self::Tsquery => 3615,
			Self::Jsonpath => 4072,
			Self::Macaddr8 => 774,
			Self::Regproc => 24,
			Self::Regprocedure => 2202,
			Self::Regoper => 2203,
			Self::Regoperator => 2204,
			Self::Regtype => 2206,
			Self::Regnamespace => 4089,
			Self::Regrole => 4096,
			Self::Regconfig => 3734,
			Self::Regdictionary => 3769,
			Self::Regcollation => 4191,
			Self::Xid => 28,
			Self::Cid => 29,
			Self::Xid8 => 5069,
			Self::PgLsn => 3220,
			Self::Tid => 27,
			Self::PgSnapshot => 5038,
			Self::TxidSnapshot => 2970,
			Self::Point => 600,
			Self::Lseg => 601,
			Self::Path => 602,
			Self::PgBox => 603,
			Self::Polygon => 604,
			Self::Line => 628,
			Self::Circle => 718,
			Self::Int4range => 3904,
			Self::Numrange => 3906,
			Self::Tsrange => 3908,
			Self::Tstzrange => 3910,
			Self::Daterange => 3912,
			Self::Int8range => 3926,
			Self::Int4multirange => 4451,
			Self::Nummultirange => 4532,
			Self::Tsmultirange => 4533,
			Self::Tstzmultirange => 4534,
			Self::Datemultirange => 4535,
			Self::Int8multirange => 4536,
			Self::Enum(id) => enum_type_oid(*id) as i32,
			Self::Composite {
				id,
				..
			} => composite_type_oid(*id) as i32,
			// Every dimension of an array has the OID of its element's array type.
			Self::Array(elem) if matches!(elem.as_ref(), Self::Array(_)) => elem.oid(),
			Self::Array(elem) => match elem.as_ref() {
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
				Self::Timestamptz => 1185,
				Self::Interval => 1187,
				Self::Numeric => 1231,
				Self::Timetz => 1270,
				Self::Json => 199,
				Self::Jsonb => 3807,
				Self::Uuid => 2951,
				Self::Regclass => 2210,
				Self::Vector => VECTOR_ARRAY_OID,
				Self::Enum(id) => enum_array_type_oid(*id) as i32,
				Self::Money => 791,
				Self::Bit => 1561,
				Self::Varbit => 1563,
				Self::Xml => 143,
				Self::Tsvector => 3643,
				Self::Tsquery => 3645,
				Self::Jsonpath => 4073,
				Self::Macaddr8 => 775,
				Self::Regproc => 1008,
				Self::Regprocedure => 2207,
				Self::Regoper => 2208,
				Self::Regoperator => 2209,
				Self::Regtype => 2211,
				Self::Regnamespace => 4090,
				Self::Regrole => 4097,
				Self::Regconfig => 3735,
				Self::Regdictionary => 3770,
				Self::Regcollation => 4192,
				Self::Xid => 1011,
				Self::Cid => 1012,
				Self::Xid8 => 271,
				Self::PgLsn => 3221,
				Self::Tid => 1010,
				Self::PgSnapshot => 5039,
				Self::TxidSnapshot => 2949,
				Self::Point => 1017,
				Self::Lseg => 1018,
				Self::Path => 1019,
				Self::PgBox => 1020,
				Self::Polygon => 1027,
				Self::Line => 629,
				Self::Circle => 719,
				Self::Int4range => 3905,
				Self::Numrange => 3907,
				Self::Tsrange => 3909,
				Self::Tstzrange => 3911,
				Self::Daterange => 3913,
				Self::Int8range => 3927,
				Self::Int4multirange => 6150,
				Self::Nummultirange => 6151,
				Self::Tsmultirange => 6152,
				Self::Tstzmultirange => 6153,
				Self::Datemultirange => 6155,
				Self::Int8multirange => 6157,
				Self::Record
				| Self::Void
				| Self::Array(_)
				| Self::Composite {
					..
				} => 2277, // anyarray
			},
		}
	}

	/// The `pg_type.typlen` value: fixed byte width, or -1 for variable-length types.
	pub(super) fn typlen(&self) -> i16 {
		match self {
			Self::Bool | Self::Char => 1,
			Self::Int2 => 2,
			Self::Int4 | Self::Float4 | Self::Date | Self::Oid | Self::Regclass | Self::Void => 4,
			Self::MacAddr => 6,
			Self::Int8 | Self::Float8 | Self::Timestamp | Self::Timestamptz | Self::Time => 8,
			Self::Timetz => 12,
			Self::Interval | Self::Uuid => 16,
			Self::Name => 64,
			Self::Regproc
			| Self::Regprocedure
			| Self::Regoper
			| Self::Regoperator
			| Self::Regtype
			| Self::Regnamespace
			| Self::Regrole
			| Self::Regconfig
			| Self::Regdictionary
			| Self::Regcollation
			| Self::Xid
			| Self::Cid => 4,
			Self::Money | Self::Macaddr8 | Self::Xid8 | Self::PgLsn => 8,
			Self::Tid => 6,
			Self::Point => 16,
			Self::Line | Self::Circle => 24,
			Self::Lseg | Self::PgBox => 32,
			Self::Numeric
			| Self::Text
			| Self::Varchar
			| Self::Bpchar
			| Self::Bytea
			| Self::Json
			| Self::Jsonb
			| Self::Inet
			| Self::Cidr
			| Self::Vector
			| Self::Bit
			| Self::Varbit
			| Self::Xml
			| Self::Tsvector
			| Self::Tsquery
			| Self::Jsonpath
			| Self::PgSnapshot
			| Self::TxidSnapshot
			| Self::Path
			| Self::Polygon
			| Self::Int4range
			| Self::Numrange
			| Self::Tsrange
			| Self::Tstzrange
			| Self::Daterange
			| Self::Int8range
			| Self::Int4multirange
			| Self::Nummultirange
			| Self::Tsmultirange
			| Self::Tstzmultirange
			| Self::Datemultirange
			| Self::Int8multirange
			| Self::Enum(_)
			| Self::Composite {
				..
			}
			| Self::Record
			| Self::Array(_) => -1,
		}
	}

	#[allow(dead_code)]
	pub(super) fn from_oid(oid: i32) -> Option<Self> {
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
			1184 => Some(Self::Timestamptz),
			1186 => Some(Self::Interval),
			1266 => Some(Self::Timetz),
			1700 => Some(Self::Numeric),
			2205 => Some(Self::Regclass),
			2249 => Some(Self::Record),
			2950 => Some(Self::Uuid),
			3802 => Some(Self::Jsonb),
			VECTOR_OID => Some(Self::Vector),
			790 => Some(Self::Money),
			1560 => Some(Self::Bit),
			1562 => Some(Self::Varbit),
			142 => Some(Self::Xml),
			3614 => Some(Self::Tsvector),
			3615 => Some(Self::Tsquery),
			4072 => Some(Self::Jsonpath),
			774 => Some(Self::Macaddr8),
			24 => Some(Self::Regproc),
			2202 => Some(Self::Regprocedure),
			2203 => Some(Self::Regoper),
			2204 => Some(Self::Regoperator),
			2206 => Some(Self::Regtype),
			4089 => Some(Self::Regnamespace),
			4096 => Some(Self::Regrole),
			3734 => Some(Self::Regconfig),
			3769 => Some(Self::Regdictionary),
			4191 => Some(Self::Regcollation),
			28 => Some(Self::Xid),
			29 => Some(Self::Cid),
			5069 => Some(Self::Xid8),
			3220 => Some(Self::PgLsn),
			27 => Some(Self::Tid),
			5038 => Some(Self::PgSnapshot),
			2970 => Some(Self::TxidSnapshot),
			600 => Some(Self::Point),
			601 => Some(Self::Lseg),
			602 => Some(Self::Path),
			603 => Some(Self::PgBox),
			604 => Some(Self::Polygon),
			628 => Some(Self::Line),
			718 => Some(Self::Circle),
			3904 => Some(Self::Int4range),
			3906 => Some(Self::Numrange),
			3908 => Some(Self::Tsrange),
			3910 => Some(Self::Tstzrange),
			3912 => Some(Self::Daterange),
			3926 => Some(Self::Int8range),
			4451 => Some(Self::Int4multirange),
			4532 => Some(Self::Nummultirange),
			4533 => Some(Self::Tsmultirange),
			4534 => Some(Self::Tstzmultirange),
			4535 => Some(Self::Datemultirange),
			4536 => Some(Self::Int8multirange),
			// Arrays
			1000 => Some(Self::Array(Box::new(Self::Bool))),
			1001 => Some(Self::Array(Box::new(Self::Bytea))),
			1002 => Some(Self::Array(Box::new(Self::Char))),
			1003 => Some(Self::Array(Box::new(Self::Name))),
			1005 => Some(Self::Array(Box::new(Self::Int2))),
			1007 => Some(Self::Array(Box::new(Self::Int4))),
			1009 => Some(Self::Array(Box::new(Self::Text))),
			1014 => Some(Self::Array(Box::new(Self::Bpchar))),
			1015 => Some(Self::Array(Box::new(Self::Varchar))),
			1016 => Some(Self::Array(Box::new(Self::Int8))),
			1021 => Some(Self::Array(Box::new(Self::Float4))),
			1022 => Some(Self::Array(Box::new(Self::Float8))),
			1028 => Some(Self::Array(Box::new(Self::Oid))),
			1040 => Some(Self::Array(Box::new(Self::MacAddr))),
			1041 => Some(Self::Array(Box::new(Self::Inet))),
			651 => Some(Self::Array(Box::new(Self::Cidr))),
			1115 => Some(Self::Array(Box::new(Self::Timestamp))),
			1182 => Some(Self::Array(Box::new(Self::Date))),
			1183 => Some(Self::Array(Box::new(Self::Time))),
			1185 => Some(Self::Array(Box::new(Self::Timestamptz))),
			1187 => Some(Self::Array(Box::new(Self::Interval))),
			1231 => Some(Self::Array(Box::new(Self::Numeric))),
			1270 => Some(Self::Array(Box::new(Self::Timetz))),
			199 => Some(Self::Array(Box::new(Self::Json))),
			3807 => Some(Self::Array(Box::new(Self::Jsonb))),
			2951 => Some(Self::Array(Box::new(Self::Uuid))),
			2210 => Some(Self::Array(Box::new(Self::Regclass))),
			VECTOR_ARRAY_OID => Some(Self::Array(Box::new(Self::Vector))),
			791 => Some(Self::Array(Box::new(Self::Money))),
			1561 => Some(Self::Array(Box::new(Self::Bit))),
			1563 => Some(Self::Array(Box::new(Self::Varbit))),
			143 => Some(Self::Array(Box::new(Self::Xml))),
			3643 => Some(Self::Array(Box::new(Self::Tsvector))),
			3645 => Some(Self::Array(Box::new(Self::Tsquery))),
			4073 => Some(Self::Array(Box::new(Self::Jsonpath))),
			775 => Some(Self::Array(Box::new(Self::Macaddr8))),
			1008 => Some(Self::Array(Box::new(Self::Regproc))),
			2207 => Some(Self::Array(Box::new(Self::Regprocedure))),
			2208 => Some(Self::Array(Box::new(Self::Regoper))),
			2209 => Some(Self::Array(Box::new(Self::Regoperator))),
			2211 => Some(Self::Array(Box::new(Self::Regtype))),
			4090 => Some(Self::Array(Box::new(Self::Regnamespace))),
			4097 => Some(Self::Array(Box::new(Self::Regrole))),
			3735 => Some(Self::Array(Box::new(Self::Regconfig))),
			3770 => Some(Self::Array(Box::new(Self::Regdictionary))),
			4192 => Some(Self::Array(Box::new(Self::Regcollation))),
			1011 => Some(Self::Array(Box::new(Self::Xid))),
			1012 => Some(Self::Array(Box::new(Self::Cid))),
			271 => Some(Self::Array(Box::new(Self::Xid8))),
			3221 => Some(Self::Array(Box::new(Self::PgLsn))),
			1010 => Some(Self::Array(Box::new(Self::Tid))),
			5039 => Some(Self::Array(Box::new(Self::PgSnapshot))),
			2949 => Some(Self::Array(Box::new(Self::TxidSnapshot))),
			1017 => Some(Self::Array(Box::new(Self::Point))),
			1018 => Some(Self::Array(Box::new(Self::Lseg))),
			1019 => Some(Self::Array(Box::new(Self::Path))),
			1020 => Some(Self::Array(Box::new(Self::PgBox))),
			1027 => Some(Self::Array(Box::new(Self::Polygon))),
			629 => Some(Self::Array(Box::new(Self::Line))),
			719 => Some(Self::Array(Box::new(Self::Circle))),
			3905 => Some(Self::Array(Box::new(Self::Int4range))),
			3907 => Some(Self::Array(Box::new(Self::Numrange))),
			3909 => Some(Self::Array(Box::new(Self::Tsrange))),
			3911 => Some(Self::Array(Box::new(Self::Tstzrange))),
			3913 => Some(Self::Array(Box::new(Self::Daterange))),
			3927 => Some(Self::Array(Box::new(Self::Int8range))),
			6150 => Some(Self::Array(Box::new(Self::Int4multirange))),
			6151 => Some(Self::Array(Box::new(Self::Nummultirange))),
			6152 => Some(Self::Array(Box::new(Self::Tsmultirange))),
			6153 => Some(Self::Array(Box::new(Self::Tstzmultirange))),
			6155 => Some(Self::Array(Box::new(Self::Datemultirange))),
			6157 => Some(Self::Array(Box::new(Self::Int8multirange))),
			2277 => Some(Self::Array(Box::new(Self::Text))),
			_ => None,
		}
	}

	/// The wire type of a described result column: the type of its enum where it holds the labels
	/// of a named one, and otherwise the type its values have.
	pub(super) fn from_result_column(column: &surrealdb_sqlplan::ResultColumn) -> Self {
		use surrealdb_sqlplan::EnumShape;
		if let Some(composite) = &column.composite {
			return Self::Composite {
				id: composite.id,
				fields: composite
					.fields
					.iter()
					.map(|(name, ty)| (name.clone(), Self::from_sql_type(*ty)))
					.collect(),
			};
		}
		match &column.enum_type {
			Some(surrealdb_sqlplan::ResultEnum {
				id: Some(id),
				shape: EnumShape::Scalar,
				..
			}) => Self::Enum(*id),
			Some(surrealdb_sqlplan::ResultEnum {
				id: Some(id),
				shape: EnumShape::Array,
				..
			}) => Self::Array(Box::new(Self::Enum(*id))),
			Some(_) | None => Self::from_sql_type(column.ty),
		}
	}

	pub(super) fn from_sql_type(ty: surrealdb_sqlplan::SqlType) -> Self {
		match ty {
			surrealdb_sqlplan::SqlType::Bool => Self::Bool,
			surrealdb_sqlplan::SqlType::Bytea => Self::Bytea,
			surrealdb_sqlplan::SqlType::Char => Self::Char,
			surrealdb_sqlplan::SqlType::Name => Self::Name,
			surrealdb_sqlplan::SqlType::Int2 => Self::Int2,
			surrealdb_sqlplan::SqlType::Int4 => Self::Int4,
			surrealdb_sqlplan::SqlType::Int8 => Self::Int8,
			surrealdb_sqlplan::SqlType::Float4 => Self::Float4,
			surrealdb_sqlplan::SqlType::Float8 => Self::Float8,
			surrealdb_sqlplan::SqlType::Numeric => Self::Numeric,
			// PostgreSQL has no unsigned integer, so a `u64` is announced as the `numeric` that
			// holds it.
			surrealdb_sqlplan::SqlType::UInt8 => Self::Numeric,
			surrealdb_sqlplan::SqlType::Text => Self::Text,
			surrealdb_sqlplan::SqlType::Varchar => Self::Varchar,
			surrealdb_sqlplan::SqlType::Bpchar => Self::Bpchar,
			surrealdb_sqlplan::SqlType::Date => Self::Date,
			surrealdb_sqlplan::SqlType::Time => Self::Time,
			surrealdb_sqlplan::SqlType::Timetz => Self::Timetz,
			surrealdb_sqlplan::SqlType::Timestamp => Self::Timestamp,
			surrealdb_sqlplan::SqlType::TimestampTz => Self::Timestamptz,
			surrealdb_sqlplan::SqlType::Interval => Self::Interval,
			surrealdb_sqlplan::SqlType::Uuid => Self::Uuid,
			surrealdb_sqlplan::SqlType::Json => Self::Json,
			surrealdb_sqlplan::SqlType::Array(elem) => {
				Self::Array(Box::new(Self::from_sql_type(*elem)))
			}
			surrealdb_sqlplan::SqlType::Inet => Self::Inet,
			surrealdb_sqlplan::SqlType::Cidr => Self::Cidr,
			surrealdb_sqlplan::SqlType::MacAddr => Self::MacAddr,
			surrealdb_sqlplan::SqlType::Oid => Self::Oid,
			surrealdb_sqlplan::SqlType::Regclass => Self::Regclass,
			surrealdb_sqlplan::SqlType::Record => Self::Record,
			// A vector is reported, and encoded, as the array of its element type.
			surrealdb_sqlplan::SqlType::Int2Vector => Self::Array(Box::new(Self::Int2)),
			surrealdb_sqlplan::SqlType::OidVector => Self::Array(Box::new(Self::Oid)),
			surrealdb_sqlplan::SqlType::Vector => Self::Vector,
			surrealdb_sqlplan::SqlType::Unknown => Self::Text,
			surrealdb_sqlplan::SqlType::Money => Self::Money,
			surrealdb_sqlplan::SqlType::PgLsn => Self::PgLsn,
			surrealdb_sqlplan::SqlType::MacAddr8 => Self::Macaddr8,
			surrealdb_sqlplan::SqlType::Bit => Self::Bit,
			surrealdb_sqlplan::SqlType::Varbit => Self::Varbit,
			surrealdb_sqlplan::SqlType::Point => Self::Point,
			surrealdb_sqlplan::SqlType::Lseg => Self::Lseg,
			surrealdb_sqlplan::SqlType::Path => Self::Path,
			surrealdb_sqlplan::SqlType::PgBox => Self::PgBox,
			surrealdb_sqlplan::SqlType::Polygon => Self::Polygon,
			surrealdb_sqlplan::SqlType::Line => Self::Line,
			surrealdb_sqlplan::SqlType::Circle => Self::Circle,
			surrealdb_sqlplan::SqlType::Range(element) => Self::range(element, false),
			surrealdb_sqlplan::SqlType::Multirange(element) => Self::range(element, true),
			// The MySQL types travel as the nearest PostgreSQL type.
			surrealdb_sqlplan::SqlType::Year => Self::Int4,
			surrealdb_sqlplan::SqlType::MySqlBit(_) => Self::Numeric,
			surrealdb_sqlplan::SqlType::Geometry(_) => Self::Json,
		}
	}

	/// The wire type of a range (`multi` false) or multirange of the element kind.
	pub(super) fn range(element: surrealdb_sqlplan::RangeElement, multi: bool) -> Self {
		use surrealdb_sqlplan::RangeElement;
		match (element, multi) {
			(RangeElement::Int4, false) => Self::Int4range,
			(RangeElement::Int8, false) => Self::Int8range,
			(RangeElement::Numeric, false) => Self::Numrange,
			(RangeElement::Timestamp, false) => Self::Tsrange,
			(RangeElement::Timestamptz, false) => Self::Tstzrange,
			(RangeElement::Date, false) => Self::Daterange,
			(RangeElement::Int4, true) => Self::Int4multirange,
			(RangeElement::Int8, true) => Self::Int8multirange,
			(RangeElement::Numeric, true) => Self::Nummultirange,
			(RangeElement::Timestamp, true) => Self::Tsmultirange,
			(RangeElement::Timestamptz, true) => Self::Tstzmultirange,
			(RangeElement::Date, true) => Self::Datemultirange,
		}
	}

	/// The element kind of a range or multirange type, and whether it is a multirange. `None` for
	/// every other type.
	pub(super) fn range_element(&self) -> Option<(surrealdb_sqlplan::RangeElement, bool)> {
		use surrealdb_sqlplan::RangeElement;
		[
			RangeElement::Int4,
			RangeElement::Int8,
			RangeElement::Numeric,
			RangeElement::Timestamp,
			RangeElement::Timestamptz,
			RangeElement::Date,
		]
		.into_iter()
		.flat_map(|element| [(element, false), (element, true)])
		.find(|(element, multi)| Self::range(*element, *multi) == *self)
	}
}

#[derive(Debug, Clone)]
pub(super) struct PgColumn {
	pub(super) name: String,
	pub(super) ty: PgType,
	/// The type modifier RowDescription reports: the declared length, precision or scale of the
	/// column's SQL type, and `-1` for none.
	pub(super) typmod: i32,
}

impl PgColumn {
	/// The column a planner's result column describes: typed as its declared SQL type when the
	/// wire carries the column's values as that type, and otherwise as
	/// [`PgType::from_result_column`] types it.
	pub(super) fn of_result_column(column: &surrealdb_sqlplan::ResultColumn) -> Self {
		let declared = column
			.declared
			.as_ref()
			.filter(|_| column.enum_type.is_none() && column.composite.is_none());
		// A `json` or `jsonb` column keeps the type its kind maps to: a write through the wire can
		// leave a parameter's JSON text in it as a string, which that type sends as the text it is.
		match declared
			.and_then(declared_wire_type)
			.filter(|declared| !matches!(declared.ty, PgType::Json | PgType::Jsonb))
		{
			Some(declared) => Self {
				name: column.name.clone(),
				ty: declared.ty,
				typmod: declared.typmod,
			},
			None => Self {
				name: column.name.clone(),
				ty: PgType::from_result_column(column),
				typmod: -1,
			},
		}
	}

	/// The length a text value of this column is padded to with spaces: a `character(n)`.
	fn blank_padded_to(&self) -> Option<usize> {
		if matches!(self.ty, PgType::Bpchar) && self.typmod >= 4 {
			usize::try_from(self.typmod - 4).ok()
		} else {
			None
		}
	}
}

/// The types the schema declares for the columns of a `*` result, by column name.
#[derive(Debug, Clone, Default)]
pub(super) struct WildcardTypes {
	/// The wire type each declared column's kind maps to.
	pub(super) kinds: HashMap<String, PgType>,
	/// The wire type of each column declared with a SQL type the wire carries.
	pub(super) declared: HashMap<String, DeclaredWireType>,
}

impl WildcardTypes {
	/// The wire types of the columns whose kinds map to `kinds` and whose SQL types are
	/// `declared`.
	pub(super) fn of(
		kinds: &HashMap<String, surrealdb_sqlplan::SqlType>,
		declared: &HashMap<String, DeclaredSqlType>,
	) -> Self {
		Self {
			kinds: kinds
				.iter()
				.map(|(name, ty)| (name.clone(), PgType::from_sql_type(*ty)))
				.collect(),
			declared: declared
				.iter()
				.filter_map(|(name, declared)| {
					declared_wire_type(declared)
						.filter(|wire| !matches!(wire.ty, PgType::Json | PgType::Jsonb))
						.map(|wire| (name.clone(), wire))
				})
				.collect(),
		}
	}

	/// Forget every declaration.
	pub(super) fn clear(&mut self) {
		self.kinds.clear();
		self.declared.clear();
	}

	/// The names of the declared columns, each once for each declaration it has.
	pub(super) fn names(&self) -> impl Iterator<Item = &String> {
		self.kinds.keys().chain(self.declared.keys())
	}
}

impl From<HashMap<String, PgType>> for WildcardTypes {
	fn from(kinds: HashMap<String, PgType>) -> Self {
		Self {
			kinds,
			declared: HashMap::new(),
		}
	}
}

/// Whether a column declared with the SQL type whose wire type is `declared` holds values the
/// wire carries as that type: `inferred` is the type of its non-null values (`None` when it
/// holds none). A value of another type, which rows written before the declaration can hold,
/// keeps the column typed from its values.
fn declared_admits(declared: &PgType, inferred: Option<&PgType>) -> bool {
	use PgType::*;
	let Some(inferred) = inferred else {
		return true;
	};
	if declared == inferred {
		return true;
	}
	let integer = |ty: &PgType| matches!(ty, Int2 | Int4 | Int8);
	match (declared, inferred) {
		(Array(declared), Array(inferred)) => declared_admits(declared, Some(inferred)),
		// A text-shaped type holds its canonical text.
		(
			Varchar | Bpchar | Name | Char | Bit | Varbit | Xml | Tsvector | Tsquery | Jsonpath
			| Macaddr8 | Tid | PgSnapshot | TxidSnapshot | Inet | Cidr | MacAddr,
			Text,
		) => true,
		// The object identifier and transaction id types hold their numbers.
		(
			Oid | Regclass | Regproc | Regprocedure | Regoper | Regoperator | Regtype
			| Regnamespace | Regrole | Regconfig | Regdictionary | Regcollation | Xid | Cid,
			inferred,
		) => integer(inferred),
		(Xid8, inferred) => integer(inferred) || *inferred == Numeric,
		(Json | Jsonb, _) => true,
		(Timetz, Time) => true,
		(declared, inferred) => {
			let number = |ty: &PgType| integer(ty) || matches!(ty, Float4 | Float8 | Numeric);
			number(declared)
				&& number(inferred)
				&& exact_in(declared, inferred)
				&& promote(declared.clone(), inferred.clone()) == *declared
		}
	}
}

/// The wire type of a column declared with a SQL type.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct DeclaredWireType {
	pub(super) ty: PgType,
	pub(super) typmod: i32,
}

/// The wire type and modifier of a column declared `declared`, or `None` when the wire does not
/// carry the column's values as that type and the column is typed as its kind is.
///
/// The type is the declared PostgreSQL type, or the nearest PostgreSQL type of a type declared in
/// another dialect. A type is carried when the encoders write the values the column's kind holds
/// in that type's text form: the built-in scalar types, the text-shaped types whose values are
/// their canonical text, and the object identifier and transaction id types, whose values are
/// their numbers. `money`, `pg_lsn`, the geometric types, the ranges, and the types that have a
/// text form only (`tsvector`, `tsquery`, `tid` and the snapshots) are not, and their columns are
/// typed by their kind.
///
/// A column declared with several array dimensions is an array nested once per dimension, the
/// shape its values have.
pub(super) fn declared_wire_type(declared: &DeclaredSqlType) -> Option<DeclaredWireType> {
	let reported = pg_reported_type(declared)?;
	let mut ty = PgType::from_oid(reported.oid)?;
	if !carries_declared_values(&ty) {
		return None;
	}
	for _ in 1..declared.array_dimensions().len() {
		ty = PgType::Array(Box::new(ty));
	}
	Some(DeclaredWireType {
		ty,
		typmod: reported.typmod,
	})
}

/// Whether the encoders write the values of a column declared as `ty` in `ty`'s text form; see
/// [`declared_wire_type`].
fn carries_declared_values(ty: &PgType) -> bool {
	match ty {
		PgType::Array(element) => carries_declared_values(element),
		PgType::Bool
		| PgType::Bytea
		| PgType::Char
		| PgType::Name
		| PgType::Int8
		| PgType::Int2
		| PgType::Int4
		| PgType::Text
		| PgType::Oid
		| PgType::Json
		| PgType::Cidr
		| PgType::Float4
		| PgType::Float8
		| PgType::MacAddr
		| PgType::Inet
		| PgType::Bpchar
		| PgType::Varchar
		| PgType::Date
		| PgType::Time
		| PgType::Timestamp
		| PgType::Timestamptz
		| PgType::Interval
		| PgType::Timetz
		| PgType::Numeric
		| PgType::Regclass
		| PgType::Uuid
		| PgType::Jsonb
		| PgType::Vector
		| PgType::Bit
		| PgType::Varbit
		| PgType::Xml
		| PgType::Jsonpath
		| PgType::Macaddr8
		| PgType::Regproc
		| PgType::Regprocedure
		| PgType::Regoper
		| PgType::Regoperator
		| PgType::Regtype
		| PgType::Regnamespace
		| PgType::Regrole
		| PgType::Regconfig
		| PgType::Regdictionary
		| PgType::Regcollation
		| PgType::Xid
		| PgType::Cid
		| PgType::Xid8 => true,
		PgType::Tsvector
		| PgType::Tsquery
		| PgType::Tid
		| PgType::PgSnapshot
		| PgType::TxidSnapshot
		| PgType::Record
		| PgType::Void
		| PgType::Money
		| PgType::PgLsn
		| PgType::Point
		| PgType::Lseg
		| PgType::Path
		| PgType::PgBox
		| PgType::Polygon
		| PgType::Line
		| PgType::Circle
		| PgType::Int4range
		| PgType::Numrange
		| PgType::Tsrange
		| PgType::Tstzrange
		| PgType::Daterange
		| PgType::Int8range
		| PgType::Int4multirange
		| PgType::Nummultirange
		| PgType::Tsmultirange
		| PgType::Tstzmultirange
		| PgType::Datemultirange
		| PgType::Int8multirange
		| PgType::Enum(_)
		| PgType::Composite {
			..
		} => false,
	}
}

/// Pad each text value of a `character(n)` column with spaces to `n` characters, as PostgreSQL
/// returns them in both formats. A value already `n` characters long or longer is unchanged.
fn pad_fixed_width(columns: &[PgColumn], rows: &mut [Vec<Option<Value>>]) {
	for (index, column) in columns.iter().enumerate() {
		let Some(width) = column.blank_padded_to() else {
			continue;
		};
		for row in rows.iter_mut() {
			if let Some(Some(Value::String(text))) = row.get_mut(index) {
				let length = text.chars().count();
				if length < width {
					let mut padded = String::with_capacity(text.len() + width - length);
					padded.push_str(text);
					padded.extend(std::iter::repeat_n(' ', width - length));
					*text = padded;
				}
			}
		}
	}
}

/// A statement result reshaped for the wire: typed columns plus rows of
/// optional cells, where `None` encodes SQL NULL.
#[derive(Debug)]
pub(super) struct ResultShape {
	pub(super) columns: Vec<PgColumn>,
	pub(super) rows: Vec<Vec<Option<Value>>>,
}

/// The wire type a single value naturally maps to; `None` for NONE/NULL.
pub(super) fn base_type(value: &Value) -> Option<PgType> {
	match value {
		Value::None | Value::Null => None,
		Value::Bool(_) => Some(PgType::Bool),
		Value::Number(n) => Some(number_type(n)),
		Value::String(_) => Some(PgType::Text),
		Value::Datetime(_) => Some(PgType::Timestamptz),
		Value::Date(_) => Some(PgType::Date),
		Value::Time(_) => Some(PgType::Time),
		Value::Timestamp(_) => Some(PgType::Timestamp),
		Value::Duration(_) | Value::Interval(_) => Some(PgType::Interval),
		Value::Uuid(_) => Some(PgType::Uuid),
		Value::Bytes(_) => Some(PgType::Bytea),
		Value::Array(arr) => {
			let inner = arr.as_slice();
			if inner.is_empty() {
				Some(PgType::Array(Box::new(PgType::Text)))
			} else {
				let elem_ty = column_type(inner.iter().map(base_type));
				// An array of arrays is a multi-dimensional Postgres array when it is rectangular,
				// with one array of the element type per dimension. A ragged one has no such
				// array, so it travels as JSON.
				if matches!(elem_ty, PgType::Array(_)) && rectangular_dims(inner).is_none() {
					Some(PgType::Jsonb)
				} else {
					Some(PgType::Array(Box::new(elem_ty)))
				}
			}
		}
		Value::RecordId(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::File(_)
		| Value::Table(_) => Some(PgType::Text),
		Value::Object(_) | Value::Set(_) | Value::Geometry(_) => Some(PgType::Jsonb),
	}
}

/// The lengths of the dimensions of `arr` when it is rectangular: each element is a non-empty
/// array of the dimensions of the others, or no element is an array. `None` when the array is
/// empty, ragged, or mixes arrays with other values, since a Postgres array has one length per
/// dimension.
pub(super) fn rectangular_dims(arr: &[Value]) -> Option<Vec<usize>> {
	if arr.is_empty() {
		return None;
	}
	let nested = arr.iter().filter(|value| matches!(value, Value::Array(_))).count();
	if nested == 0 {
		return Some(vec![arr.len()]);
	}
	if nested != arr.len() {
		return None;
	}
	let mut inner: Option<Vec<usize>> = None;
	for value in arr {
		let Value::Array(sub) = value else {
			return None;
		};
		let dims = rectangular_dims(sub.as_slice())?;
		if inner.as_ref().is_some_and(|inner| *inner != dims) {
			return None;
		}
		inner = Some(dims);
	}
	let mut dims = vec![arr.len()];
	dims.extend(inner?);
	Some(dims)
}

/// The wire type of a number: the width of a sized integer or float, the 64-bit types for `int`
/// and `float`, and `numeric` for a `decimal` and a `u64`, which no integer type holds.
fn number_type(number: &Number) -> PgType {
	match number {
		Number::I8(_) | Number::U8(_) | Number::I16(_) => PgType::Int2,
		Number::U16(_) | Number::I32(_) => PgType::Int4,
		Number::U32(_) | Number::Int(_) => PgType::Int8,
		Number::F16(_) | Number::F32(_) => PgType::Float4,
		Number::Float(_) => PgType::Float8,
		Number::U64(_) | Number::Decimal(_) | Number::BigDecimal(_) => PgType::Numeric,
	}
}

/// Widen two cell types into a column type both can encode to.
fn promote(a: PgType, b: PgType) -> PgType {
	if a == b {
		return a;
	}
	use PgType::*;
	match (a, b) {
		// Integer ladder
		(Int2, Int4) | (Int4, Int2) => Int4,
		(Int2 | Int4, Int8) | (Int8, Int2 | Int4) => Int8,
		// Int to float
		// An `integer` is not exact in a `real`, so the pair is a `double precision`.
		(Int2, Float4) | (Float4, Int2) => Float4,
		(Int2 | Int4 | Int8 | Float4, Float8)
		| (Float8, Int2 | Int4 | Int8 | Float4)
		| (Int4 | Int8, Float4)
		| (Float4, Int4 | Int8) => Float8,
		// Number to numeric
		(Int2 | Int4 | Int8 | Float4 | Float8, Numeric)
		| (Numeric, Int2 | Int4 | Int8 | Float4 | Float8) => Numeric,
		// Strings
		(Char | Bpchar | Varchar, Text) | (Text, Char | Bpchar | Varchar) => Text,
		(Bpchar, Varchar) | (Varchar, Bpchar) => Varchar,
		// Temporal
		(Date, Timestamp) | (Timestamp, Date) => Timestamp,
		(Date | Timestamp, Timestamptz) | (Timestamptz, Date | Timestamp) => Timestamptz,
		(Time, Timetz) | (Timetz, Time) => Timetz,
		// Arrays
		(Array(elem_a), Array(elem_b)) => Array(Box::new(promote(*elem_a, *elem_b))),
		// Fallback
		_ => Jsonb,
	}
}

fn column_type(cells: impl Iterator<Item = Option<PgType>>) -> PgType {
	cells.flatten().reduce(promote).unwrap_or(PgType::Text)
}

fn cell(value: Value) -> Option<Value> {
	if matches!(value, Value::None | Value::Null) {
		None
	} else {
		Some(value)
	}
}

/// Reshape one statement result for the wire.
///
/// A result whose rows are all objects becomes a multi-column result set: the
/// columns are the union of the objects' keys (key-sorted — `Object` is a
/// `BTreeMap`, so projection order is not preserved), each typed by promoting
/// the types of the values present in it. Any other result becomes a single
/// `value` column with one row per array element, or a single row for a
/// scalar.
pub(super) fn shape_result(value: Value) -> ResultShape {
	shape_result_declared(value, &WildcardTypes::default())
}

/// Whether every value of the number type `inferred` is exactly a value of the number type
/// `declared`: an integer is exact in an integer or `numeric` declaration, and a float only in a
/// float one, so a declared `real` or `double precision` never rounds an integer or a decimal.
fn exact_in(declared: &PgType, inferred: &PgType) -> bool {
	use PgType::*;
	if let (Array(declared), Array(inferred)) = (declared, inferred) {
		return exact_in(declared, inferred);
	}
	let integer = |ty: &PgType| matches!(ty, Int2 | Int4 | Int8);
	let float = |ty: &PgType| matches!(ty, Float4 | Float8);
	(integer(declared) && integer(inferred))
		|| (*declared == Numeric && (integer(inferred) || *inferred == Numeric))
		|| (float(declared) && float(inferred))
}

/// The type of a `*` column the schema declares `declared`, given the type `inferred` from its
/// non-null values (`None` when it holds none). A numeric declaration types the column when it
/// can hold every value present, so a column of declared width answers with that width whether
/// or not its rows hold a value. A value wider than the declaration, as rows written before the
/// column was declared or a declaration changed since the statement was prepared can hold,
/// keeps the column typed from its values, so no value is narrowed to fit. `elementless_arrays`
/// says the column holds arrays with no element to type, which `inferred` leaves out.
fn declared_column_type(
	declared: Option<&PgType>,
	inferred: Option<PgType>,
	elementless_arrays: bool,
) -> PgType {
	use PgType::*;
	fn number(ty: &PgType) -> bool {
		if let Array(elem) = ty {
			number(elem)
		} else {
			matches!(ty, Int2 | Int4 | Int8 | Float4 | Float8 | Numeric)
		}
	}
	match (declared, inferred) {
		// A declaration that is no array cannot type a column of arrays without an element.
		(Some(declared), None) if elementless_arrays && !matches!(declared, Array(_)) => {
			Array(Box::new(Text))
		}
		(Some(declared), None) if number(declared) => declared.clone(),
		// Arrays that are empty or hold only NULLs have no element type of their own.
		(_, None) if elementless_arrays => Array(Box::new(Text)),
		(Some(declared), Some(inferred))
			if number(declared)
				&& number(&inferred)
				&& exact_in(declared, &inferred)
				&& promote(declared.clone(), inferred.clone()) == *declared =>
		{
			declared.clone()
		}
		(_, inferred) => inferred.unwrap_or(Text),
	}
}

/// [`shape_result`] for a result whose columns come from a `*` over one table: a column named in
/// `declared` takes the type the schema declares for it when the values it holds are numbers, and
/// every other column is typed from its values.
pub(super) fn shape_result_declared(value: Value, declared: &WildcardTypes) -> ResultShape {
	let values = match value {
		Value::Array(arr) => arr.into_inner(),
		v @ (Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Object(_)
		| Value::Set(_)) => vec![v],
	};
	if !values.is_empty() && values.iter().all(|v| matches!(v, Value::Object(_))) {
		let mut objects = Vec::with_capacity(values.len());
		for value in values {
			if let Value::Object(obj) = value {
				objects.push(obj.into_inner());
			}
		}
		let mut names = BTreeSet::new();
		for obj in &objects {
			for key in obj.keys() {
				if !names.contains(key) {
					names.insert(key.clone());
				}
			}
		}
		let names: Vec<String> = names.into_iter().collect();
		let mut rows = Vec::with_capacity(objects.len());
		for mut obj in objects {
			let row: Vec<Option<Value>> =
				names.iter().map(|name| obj.remove(name).and_then(cell)).collect();
			rows.push(row);
		}
		let columns = names
			.into_iter()
			.enumerate()
			.map(|(i, name)| {
				let elementless = |value: &Value| {
					matches!(value, Value::Array(items)
						if items.iter().all(|item| matches!(item, Value::None | Value::Null)))
				};
				let elementless_arrays =
					rows.iter().any(|row| row[i].as_ref().is_some_and(elementless));
				let inferred = rows
					.iter()
					.filter_map(|row| row[i].as_ref().filter(|value| !elementless(value)))
					.filter_map(base_type)
					.reduce(promote);
				// Arrays without an element are left out of the type only beside other arrays: next
				// to a value that is no array, the column mixes shapes and is typed from every
				// cell.
				let inferred = if elementless_arrays
					&& inferred.as_ref().is_some_and(|ty| !matches!(ty, PgType::Array(_)))
				{
					rows.iter()
						.filter_map(|row| row[i].as_ref().and_then(base_type))
						.reduce(promote)
				} else {
					inferred
				};
				let sql_declared = declared.declared.get(&name).filter(|wire| {
					(!elementless_arrays || matches!(wire.ty, PgType::Array(_)))
						&& declared_admits(&wire.ty, inferred.as_ref())
				});
				match sql_declared {
					Some(wire) => PgColumn {
						typmod: wire.typmod,
						ty: wire.ty.clone(),
						name,
					},
					None => PgColumn {
						typmod: -1,
						ty: declared_column_type(
							declared.kinds.get(&name),
							inferred,
							elementless_arrays,
						),
						name,
					},
				}
			})
			.collect::<Vec<_>>();
		let mut rows = rows;
		pad_fixed_width(&columns, &mut rows);
		ResultShape {
			columns,
			rows,
		}
	} else {
		let rows: Vec<Vec<Option<Value>>> = values.into_iter().map(|v| vec![cell(v)]).collect();
		let ty = column_type(rows.iter().map(|row| row[0].as_ref().and_then(base_type)));
		ResultShape {
			columns: vec![PgColumn {
				typmod: -1,
				name: "value".to_string(),
				ty,
			}],
			rows,
		}
	}
}

/// Reshape a statement result using a static result descriptor from the query planner.
pub(super) fn shape_result_with_descriptor(
	descriptor: &surrealdb_sqlplan::ResultDescriptor,
	value: Value,
) -> ResultShape {
	let values = match value {
		Value::Array(arr) => arr.into_inner(),
		v @ (Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Object(_)
		| Value::Set(_)) => vec![v],
	};
	let columns: Vec<PgColumn> =
		descriptor.columns.iter().map(PgColumn::of_result_column).collect();

	let mut rows = Vec::with_capacity(values.len());
	for value in values {
		if let Value::Object(obj) = value {
			let obj_map = obj.into_inner();
			let row: Vec<Option<Value>> = descriptor
				.columns
				.iter()
				.enumerate()
				.map(|(i, col)| {
					// A column the planner keyed is read from that field alone: another column
					// may share its name, and a missing field is an absent value, not a hint to
					// read a neighbour.
					let val = match &col.key {
						Some(key) => obj_map.get(key),
						None => obj_map
							.get(&col.name)
							.or_else(|| {
								col.original_column.as_ref().and_then(|orig| obj_map.get(orig))
							})
							.or_else(|| {
								if obj_map.len() == descriptor.columns.len() {
									obj_map.values().nth(i)
								} else {
									None
								}
							}),
					};
					val.cloned().and_then(cell)
				})
				.collect();
			rows.push(row);
		} else if descriptor.columns.len() == 1 {
			rows.push(vec![cell(value)]);
		}
	}
	pad_fixed_width(&columns, &mut rows);

	ResultShape {
		columns,
		rows,
	}
}

/// Reshape a result as a single `result jsonb` column, one row per top-level
/// array element (or a single row for a scalar). Used by the extended
/// protocol's driver-prepared path, where column types cannot be known before
/// execution so every value is delivered as JSON.
pub(super) fn shape_result_jsonb(value: Value) -> ResultShape {
	let values = match value {
		Value::Array(arr) => arr.into_inner(),
		v @ (Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Object(_)
		| Value::Set(_)) => vec![v],
	};
	let rows = values.into_iter().map(|v| vec![cell(v)]).collect();
	ResultShape {
		columns: vec![PgColumn {
			typmod: -1,
			name: "result".to_string(),
			ty: PgType::Jsonb,
		}],
		rows,
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::{Number, Object, Value};

	use super::*;

	#[test]
	fn a_declared_float_never_rounds_an_integer_value() {
		let declared = HashMap::from([("f".to_owned(), PgType::Float8)]);
		let row = |value: Value| Value::Array(vec![obj(&[("f", value)])].into());
		let integer = shape_result_declared(
			row(Value::Number(Number::Int(9_007_199_254_740_993))),
			&WildcardTypes::from(declared.clone()),
		);
		assert_eq!(integer.columns[0].ty, PgType::Int8);
		let float = shape_result_declared(
			row(Value::Number(Number::Float(1.5))),
			&WildcardTypes::from(declared),
		);
		assert_eq!(float.columns[0].ty, PgType::Float8);
	}

	#[test]
	fn an_empty_array_beside_a_scalar_keeps_the_column_a_mix() {
		let empty = Value::Array(Vec::<Value>::new().into());
		let one = Value::Number(Number::Int(1));
		let shape = shape_result_declared(
			Value::Array(vec![obj(&[("v", empty)]), obj(&[("v", one)])].into()),
			&WildcardTypes::default(),
		);
		assert_eq!(shape.columns[0].ty, PgType::Jsonb);
	}

	#[test]
	fn a_scalar_declaration_does_not_type_empty_arrays() {
		let declared = HashMap::from([("n".to_owned(), PgType::Int4)]);
		let empty = Value::Array(Vec::<Value>::new().into());
		let shape = shape_result_declared(
			Value::Array(vec![obj(&[("n", empty)])].into()),
			&WildcardTypes::from(declared),
		);
		assert_eq!(shape.columns[0].ty, PgType::Array(Box::new(PgType::Text)));
	}

	#[test]
	fn a_text_array_is_not_taken_for_an_empty_one() {
		let declared = HashMap::from([("n".to_owned(), PgType::Array(Box::new(PgType::Int4)))]);
		let strings = Value::Array(vec![Value::String("a".into())].into());
		let shape = shape_result_declared(
			Value::Array(vec![obj(&[("n", strings)])].into()),
			&WildcardTypes::from(declared),
		);
		assert_eq!(shape.columns[0].ty, PgType::Array(Box::new(PgType::Text)));
	}

	#[test]
	fn an_integer_and_a_real_widen_to_double_precision() {
		assert_eq!(promote(PgType::Int4, PgType::Float4), PgType::Float8);
		assert_eq!(promote(PgType::Float4, PgType::Int8), PgType::Float8);
		assert_eq!(promote(PgType::Int2, PgType::Float4), PgType::Float4);
	}

	#[test]
	fn a_declared_numeric_array_column_with_empty_arrays_is_typed_as_declared() {
		let declared = HashMap::from([("n".to_owned(), PgType::Array(Box::new(PgType::Int4)))]);
		let empty = Value::Array(Vec::<Value>::new().into());
		let shape = shape_result_declared(
			Value::Array(vec![obj(&[("n", empty)])].into()),
			&WildcardTypes::from(declared),
		);
		assert_eq!(shape.columns[0].ty, PgType::Array(Box::new(PgType::Int4)));
	}

	fn obj(entries: &[(&str, Value)]) -> Value {
		let mut object = Object::new();
		for (key, value) in entries {
			object.insert(key.to_string(), value.clone());
		}
		Value::Object(object)
	}

	fn int_array(values: &[i64]) -> Value {
		Value::Array(
			values.iter().map(|i| Value::Number(Number::Int(*i))).collect::<Vec<_>>().into(),
		)
	}

	#[test]
	fn a_declared_column_holding_only_nulls_keeps_its_declared_type() {
		let declared = HashMap::from([("n".to_string(), PgType::Int4)]);
		let rows =
			Value::Array(vec![obj(&[("n", Value::Null)]), obj(&[("n", Value::Null)])].into());
		assert_eq!(
			shape_result_declared(rows, &WildcardTypes::from(declared.clone())).columns[0].ty,
			PgType::Int4
		);
		let rows = Value::Array(vec![obj(&[("n", Value::Number(Number::I32(7)))])].into());
		assert_eq!(
			shape_result_declared(rows, &WildcardTypes::from(declared)).columns[0].ty,
			PgType::Int4
		);
	}

	#[test]
	fn a_declared_column_narrower_than_its_values_is_typed_from_them() {
		let declared = HashMap::from([("n".to_string(), PgType::Int4)]);
		let wide = Value::Number(Number::Int(i64::from(i32::MAX) + 1));
		let rows = Value::Array(vec![obj(&[("n", wide)])].into());
		assert_eq!(
			shape_result_declared(rows, &WildcardTypes::from(declared)).columns[0].ty,
			PgType::Int8
		);
		let declared = HashMap::from([("n".to_string(), PgType::Int8)]);
		let rows = Value::Array(vec![obj(&[("n", Value::Number(Number::I32(7)))])].into());
		assert_eq!(
			shape_result_declared(rows, &WildcardTypes::from(declared)).columns[0].ty,
			PgType::Int8
		);
	}

	#[test]
	fn a_rectangular_array_of_arrays_is_a_multidimensional_array() {
		let grid = Value::Array(vec![int_array(&[1, 2]), int_array(&[3, 4])].into());
		let int8_grid = PgType::Array(Box::new(PgType::Array(Box::new(PgType::Int8))));
		assert_eq!(base_type(&grid), Some(int8_grid.clone()));
		assert_eq!(int8_grid.oid(), 1016);
		let shape = shape_result(Value::Array(vec![obj(&[("grid", grid)])].into()));
		assert_eq!(shape.columns[0].ty, int8_grid);
		let cell = shape.rows[0][0].clone().expect("a cell");
		let text = crate::encode::encode_text(cell, &shape.columns[0].ty).unwrap();
		assert_eq!(String::from_utf8(text).unwrap(), "{{1,2},{3,4}}");
	}

	#[test]
	fn a_ragged_array_of_arrays_is_jsonb() {
		let jagged = Value::Array(vec![int_array(&[1]), int_array(&[2, 3])].into());
		assert_eq!(base_type(&jagged), Some(PgType::Jsonb));
		let empty_row = Value::Array(vec![int_array(&[1]), int_array(&[])].into());
		assert_eq!(base_type(&empty_row), Some(PgType::Jsonb));
	}

	#[test]
	fn a_rectangular_array_of_arrays_has_the_oid_of_a_one_dimensional_array() {
		let grid = Value::Array(vec![int_array(&[1, 2]), int_array(&[3, 4])].into());
		let ty = base_type(&grid).expect("a type");
		assert_eq!(ty.oid(), PgType::Array(Box::new(PgType::Int8)).oid());
		let Value::Array(rows) = &grid else {
			panic!("an array");
		};
		assert_eq!(rectangular_dims(rows.as_slice()), Some(vec![2, 2]));
		let flat = [Value::Number(Number::Int(1)), Value::Number(Number::Int(2))];
		assert_eq!(rectangular_dims(&flat), Some(vec![2]));
		assert_eq!(rectangular_dims(&[]), None);
	}

	#[test]
	fn jagged_and_mixed_nesting_is_jsonb_too() {
		let jagged = Value::Array(vec![int_array(&[1]), int_array(&[2, 3])].into());
		assert_eq!(base_type(&jagged), Some(PgType::Jsonb));
		let mixed = Value::Array(vec![Value::Number(Number::Int(1)), int_array(&[2])].into());
		assert_eq!(base_type(&mixed), Some(PgType::Array(Box::new(PgType::Jsonb))));
		let text =
			crate::encode::encode_text(mixed, &PgType::Array(Box::new(PgType::Jsonb))).unwrap();
		assert_eq!(String::from_utf8(text).unwrap(), "{1,[2]}");
	}

	#[test]
	fn a_flat_array_keeps_its_element_type() {
		assert_eq!(base_type(&int_array(&[1, 2])), Some(PgType::Array(Box::new(PgType::Int8))));
	}

	#[test]
	fn promotion_ladder() {
		assert_eq!(promote(PgType::Int8, PgType::Int8), PgType::Int8);
		assert_eq!(promote(PgType::Int8, PgType::Float8), PgType::Float8);
		assert_eq!(promote(PgType::Float8, PgType::Numeric), PgType::Numeric);
		assert_eq!(promote(PgType::Int8, PgType::Numeric), PgType::Numeric);
		assert_eq!(promote(PgType::Int8, PgType::Text), PgType::Jsonb);
		assert_eq!(promote(PgType::Uuid, PgType::Bool), PgType::Jsonb);
	}

	#[test]
	fn objects_become_columns() {
		let value = Value::Array(
			vec![
				obj(&[("id", Value::Number(Number::Int(1))), ("name", Value::String("a".into()))]),
				obj(&[
					("id", Value::Number(Number::Int(2))),
					("age", Value::Number(Number::Int(30))),
				]),
			]
			.into(),
		);
		let shape = shape_result(value);
		let names: Vec<&str> = shape.columns.iter().map(|c| c.name.as_str()).collect();
		assert_eq!(names, vec!["age", "id", "name"]);
		assert_eq!(shape.columns[0].ty, PgType::Int8);
		assert_eq!(shape.columns[1].ty, PgType::Int8);
		assert_eq!(shape.columns[2].ty, PgType::Text);
		assert_eq!(shape.rows.len(), 2);
		assert!(shape.rows[0][0].is_none());
		assert!(shape.rows[1][2].is_none());
	}

	#[test]
	fn mixed_numbers_promote() {
		let value = Value::Array(
			vec![
				obj(&[("n", Value::Number(Number::Int(1)))]),
				obj(&[("n", Value::Number(Number::Float(1.5)))]),
			]
			.into(),
		);
		let shape = shape_result(value);
		assert_eq!(shape.columns[0].ty, PgType::Float8);
	}

	#[test]
	fn scalars_become_value_column() {
		let shape = shape_result(Value::Array(
			vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))].into(),
		));
		assert_eq!(shape.columns.len(), 1);
		assert_eq!(shape.columns[0].name, "value");
		assert_eq!(shape.columns[0].ty, PgType::Int8);
		assert_eq!(shape.rows.len(), 2);
	}

	#[test]
	fn empty_result_defaults_to_text() {
		let shape = shape_result(Value::Array(Vec::<Value>::new().into()));
		assert_eq!(shape.columns.len(), 1);
		assert_eq!(shape.columns[0].ty, PgType::Text);
		assert!(shape.rows.is_empty());
	}

	#[test]
	fn all_null_column_defaults_to_text() {
		let value =
			Value::Array(vec![obj(&[("x", Value::Null)]), obj(&[("x", Value::None)])].into());
		let shape = shape_result(value);
		assert_eq!(shape.columns[0].ty, PgType::Text);
		assert!(shape.rows.iter().all(|row| row[0].is_none()));
	}
	#[test]
	fn a_named_enum_column_has_the_oid_of_its_type_and_an_array_of_it_the_array_oid() {
		let column = |shape| surrealdb_sqlplan::ResultColumn {
			name: "m".to_owned(),
			enum_type: Some(surrealdb_sqlplan::ResultEnum {
				id: Some(3),
				labels: vec!["sad".to_owned()],
				shape,
			}),
			..Default::default()
		};
		let scalar = PgType::from_result_column(&column(surrealdb_sqlplan::EnumShape::Scalar));
		assert_eq!(scalar.oid(), (1 << 21) + 3);
		let array = PgType::from_result_column(&column(surrealdb_sqlplan::EnumShape::Array));
		assert_eq!(array.oid(), (1 << 21) + (1 << 20) + 3);
	}

	#[test]
	fn an_inline_enum_column_travels_as_text() {
		let column = surrealdb_sqlplan::ResultColumn {
			name: "m".to_owned(),
			enum_type: Some(surrealdb_sqlplan::ResultEnum {
				id: None,
				labels: vec!["a".to_owned()],
				shape: surrealdb_sqlplan::EnumShape::Scalar,
			}),
			ty: surrealdb_sqlplan::SqlType::Text,
			..Default::default()
		};
		assert_eq!(PgType::from_result_column(&column), PgType::Text);
	}

	/// Every text-only type with its OID, element array OID, and `typlen`, as PostgreSQL 17 lists
	/// them in `pg_type`.
	fn text_only_types() -> Vec<(PgType, i32, i32, i16)> {
		vec![
			(PgType::Money, 790, 791, 8),
			(PgType::Bit, 1560, 1561, -1),
			(PgType::Varbit, 1562, 1563, -1),
			(PgType::Xml, 142, 143, -1),
			(PgType::Tsvector, 3614, 3643, -1),
			(PgType::Tsquery, 3615, 3645, -1),
			(PgType::Jsonpath, 4072, 4073, -1),
			(PgType::Macaddr8, 774, 775, 8),
			(PgType::Regproc, 24, 1008, 4),
			(PgType::Regprocedure, 2202, 2207, 4),
			(PgType::Regoper, 2203, 2208, 4),
			(PgType::Regoperator, 2204, 2209, 4),
			(PgType::Regtype, 2206, 2211, 4),
			(PgType::Regnamespace, 4089, 4090, 4),
			(PgType::Regrole, 4096, 4097, 4),
			(PgType::Regconfig, 3734, 3735, 4),
			(PgType::Regdictionary, 3769, 3770, 4),
			(PgType::Regcollation, 4191, 4192, 4),
			(PgType::Xid, 28, 1011, 4),
			(PgType::Cid, 29, 1012, 4),
			(PgType::Xid8, 5069, 271, 8),
			(PgType::PgLsn, 3220, 3221, 8),
			(PgType::Tid, 27, 1010, 6),
			(PgType::PgSnapshot, 5038, 5039, -1),
			(PgType::TxidSnapshot, 2970, 2949, -1),
			(PgType::Point, 600, 1017, 16),
			(PgType::Lseg, 601, 1018, 32),
			(PgType::Path, 602, 1019, -1),
			(PgType::PgBox, 603, 1020, 32),
			(PgType::Polygon, 604, 1027, -1),
			(PgType::Line, 628, 629, 24),
			(PgType::Circle, 718, 719, 24),
			(PgType::Int4range, 3904, 3905, -1),
			(PgType::Numrange, 3906, 3907, -1),
			(PgType::Tsrange, 3908, 3909, -1),
			(PgType::Tstzrange, 3910, 3911, -1),
			(PgType::Daterange, 3912, 3913, -1),
			(PgType::Int8range, 3926, 3927, -1),
			(PgType::Int4multirange, 4451, 6150, -1),
			(PgType::Nummultirange, 4532, 6151, -1),
			(PgType::Tsmultirange, 4533, 6152, -1),
			(PgType::Tstzmultirange, 4534, 6153, -1),
			(PgType::Datemultirange, 4535, 6155, -1),
			(PgType::Int8multirange, 4536, 6157, -1),
		]
	}

	#[test]
	fn text_only_types_round_trip_their_oids() {
		for (ty, oid, array_oid, typlen) in text_only_types() {
			assert_eq!(ty.oid(), oid, "{ty:?}");
			assert_eq!(ty.typlen(), typlen, "{ty:?}");
			assert_eq!(PgType::from_oid(oid), Some(ty.clone()), "{ty:?}");
			let array = PgType::Array(Box::new(ty.clone()));
			assert_eq!(array.oid(), array_oid, "array of {ty:?}");
			assert_eq!(PgType::from_oid(array_oid), Some(array), "array of {ty:?}");
		}
	}

	#[test]
	fn text_only_types_have_distinct_oids() {
		let mut seen = BTreeSet::new();
		for (_, oid, array_oid, _) in text_only_types() {
			assert!(seen.insert(oid), "duplicate oid {oid}");
			assert!(seen.insert(array_oid), "duplicate array oid {array_oid}");
		}
	}

	#[test]
	fn text_only_types_send_a_string_as_its_text() {
		let text =
			crate::encode::encode_text(Value::String("(1,2)".into()), &PgType::Point).unwrap();
		assert_eq!(text, b"(1,2)");
		let range =
			crate::encode::encode_text(Value::String("[1,5)".into()), &PgType::Int4range).unwrap();
		assert_eq!(range, b"[1,5)");
	}

	#[test]
	fn text_only_types_refuse_the_binary_format() {
		let err = crate::encode::encode_binary(Value::String("'a'".into()), &PgType::Tsvector)
			.expect_err("tsvector has no binary codec");
		assert!(err.to_string().contains("request the text format"), "{err}");
		// An empty array has no element to encode, so only an element reaches the refusal.
		let array = PgType::Array(Box::new(PgType::Tsvector));
		let elements = Value::Array(vec![Value::String("'a'".into())].into());
		assert!(crate::encode::encode_binary(elements, &array).is_err());
	}

	#[test]
	fn the_carried_text_shaped_and_identifier_types_have_a_binary_form() {
		let bits = crate::encode::encode_binary(Value::String("101".into()), &PgType::Bit).unwrap();
		assert_eq!(bits, [0, 0, 0, 3, 0b1010_0000]);
		let varbit =
			crate::encode::encode_binary(Value::String("1".into()), &PgType::Varbit).unwrap();
		assert_eq!(varbit, [0, 0, 0, 1, 0x80]);
		assert!(crate::encode::encode_binary(Value::String("12".into()), &PgType::Bit).is_err());
		let xml = crate::encode::encode_binary(Value::String("<a/>".into()), &PgType::Xml).unwrap();
		assert_eq!(xml, b"<a/>");
		let path =
			crate::encode::encode_binary(Value::String("$.a".into()), &PgType::Jsonpath).unwrap();
		assert_eq!(path, b"\x01$.a");
		let xid =
			crate::encode::encode_binary(Value::Number(Number::U32(7)), &PgType::Xid).unwrap();
		assert_eq!(xid, [0, 0, 0, 7]);
		let xid8 =
			crate::encode::encode_binary(Value::Number(Number::U64(u64::MAX)), &PgType::Xid8)
				.unwrap();
		assert_eq!(xid8, [0xff; 8]);
		assert!(
			crate::encode::encode_binary(Value::Number(Number::Int(-1)), &PgType::Regtype).is_err()
		);
	}

	fn pg_declared(text: &str) -> DeclaredSqlType {
		DeclaredSqlType::parse(surrealdb_sqlplan::SqlTypeDialect::PostgreSql, text)
			.expect("a PostgreSQL type")
	}

	fn my_declared(text: &str) -> DeclaredSqlType {
		DeclaredSqlType::parse(surrealdb_sqlplan::SqlTypeDialect::MySql, text)
			.expect("a MySQL type")
	}

	fn declared_column(
		name: &str,
		ty: surrealdb_sqlplan::SqlType,
		declared: DeclaredSqlType,
	) -> surrealdb_sqlplan::ResultColumn {
		surrealdb_sqlplan::ResultColumn {
			name: name.to_owned(),
			ty,
			declared: Some(declared),
			..Default::default()
		}
	}

	#[test]
	fn a_declared_column_is_described_by_its_sql_type_and_modifier() {
		use surrealdb_sqlplan::SqlType;
		let cases = [
			(declared_column("v", SqlType::Text, pg_declared("varchar(20)")), PgType::Varchar, 24),
			(declared_column("c", SqlType::Text, pg_declared("char(5)")), PgType::Bpchar, 9),
			(
				declared_column("n", SqlType::Numeric, pg_declared("numeric(10,2)")),
				PgType::Numeric,
				(10 << 16 | 2) + 4,
			),
			(
				declared_column("t", SqlType::Timestamp, pg_declared("timestamp(3)")),
				PgType::Timestamp,
				3,
			),
			(declared_column("b", SqlType::Text, pg_declared("bit(8)")), PgType::Bit, 8),
			(declared_column("i", SqlType::Text, pg_declared("inet")), PgType::Inet, -1),
			(
				declared_column("a", SqlType::Array(&SqlType::Text), pg_declared("varchar(4)[]")),
				PgType::Array(Box::new(PgType::Varchar)),
				8,
			),
			// A MySQL type is described as the nearest PostgreSQL type.
			(
				declared_column("d", SqlType::Timestamp, my_declared("datetime(3)")),
				PgType::Timestamp,
				3,
			),
			(declared_column("y", SqlType::Int4, my_declared("year")), PgType::Int2, -1),
			// A type whose values the wire does not carry in its own form keeps the kind's type.
			(declared_column("m", SqlType::Numeric, pg_declared("money")), PgType::Numeric, -1),
			(declared_column("r", SqlType::Unknown, pg_declared("int4range")), PgType::Text, -1),
			// A type with no binary form is described as text, which every format carries.
			(declared_column("v", SqlType::Unknown, pg_declared("tsvector")), PgType::Text, -1),
			(declared_column("g", SqlType::Unknown, pg_declared("tid")), PgType::Text, -1),
			// Every dimension of a declared array nests the type once.
			(
				declared_column(
					"m",
					SqlType::Array(&SqlType::Array(&SqlType::Int4)),
					pg_declared("int[][]"),
				),
				PgType::Array(Box::new(PgType::Array(Box::new(PgType::Int4)))),
				-1,
			),
		];
		for (column, ty, typmod) in cases {
			let described = PgColumn::of_result_column(&column);
			assert_eq!((described.ty, described.typmod), (ty, typmod), "{}", column.name);
		}
	}

	#[test]
	fn a_row_description_carries_the_type_modifier() {
		let mut buf = bytes::BytesMut::new();
		let column = PgColumn::of_result_column(&declared_column(
			"v",
			surrealdb_sqlplan::SqlType::Text,
			pg_declared("varchar(20)"),
		));
		crate::msg::write_row_description(&mut buf, &[column], |_| 0);
		// Tag, length, count, the name, the table oid and attribute number, then the type oid,
		// its length and the modifier.
		let field = &buf[7 + 2..];
		assert_eq!(i32::from_be_bytes(field[6..10].try_into().unwrap()), 1043);
		assert_eq!(i32::from_be_bytes(field[12..16].try_into().unwrap()), 24);
	}

	#[test]
	fn a_character_column_is_padded_to_its_length() {
		let descriptor = surrealdb_sqlplan::ResultDescriptor {
			columns: vec![
				declared_column("c", surrealdb_sqlplan::SqlType::Text, pg_declared("char(5)")),
				declared_column("v", surrealdb_sqlplan::SqlType::Text, pg_declared("varchar(5)")),
			],
		};
		let row = obj(&[("c", Value::String("ab".into())), ("v", Value::String("ab".into()))]);
		let shape = shape_result_with_descriptor(&descriptor, Value::Array(vec![row].into()));
		assert_eq!(shape.rows[0][0], Some(Value::String("ab   ".into())));
		assert_eq!(shape.rows[0][1], Some(Value::String("ab".into())));
		let text =
			crate::encode::encode_text(shape.rows[0][0].clone().unwrap(), &shape.columns[0].ty)
				.unwrap();
		assert_eq!(text, b"ab   ");
		let binary =
			crate::encode::encode_binary(shape.rows[0][0].clone().unwrap(), &shape.columns[0].ty)
				.unwrap();
		assert_eq!(binary, b"ab   ");
		// A value as long as the column, or with characters of several bytes, keeps its length in
		// characters.
		let row = obj(&[("c", Value::String("éé".into())), ("v", Value::Null)]);
		let shape = shape_result_with_descriptor(&descriptor, Value::Array(vec![row].into()));
		assert_eq!(shape.rows[0][0], Some(Value::String("éé   ".into())));
	}

	#[test]
	fn a_star_column_takes_its_declared_sql_type_when_its_values_fit() {
		let declared = HashMap::from([
			("c".to_owned(), pg_declared("char(3)")),
			("n".to_owned(), pg_declared("numeric(10,2)")),
			("b".to_owned(), pg_declared("bit(2)")),
		]);
		let kinds = HashMap::from([("n".to_owned(), surrealdb_sqlplan::SqlType::Numeric)]);
		let types = WildcardTypes::of(&kinds, &declared);
		let row = obj(&[
			("c", Value::String("x".into())),
			("n", Value::Number(Number::Int(2))),
			("b", Value::Number(Number::Int(1))),
		]);
		let shape = shape_result_declared(Value::Array(vec![row].into()), &types);
		let described: Vec<_> =
			shape.columns.iter().map(|c| (c.name.as_str(), c.ty.clone(), c.typmod)).collect();
		assert_eq!(
			described,
			[
				// A number is no bit string, so the column is typed from its values.
				("b", PgType::Int8, -1),
				("c", PgType::Bpchar, 7),
				("n", PgType::Numeric, (10 << 16 | 2) + 4),
			]
		);
		assert_eq!(shape.rows[0][1], Some(Value::String("x  ".into())));
	}
}
