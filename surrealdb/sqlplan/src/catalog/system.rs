//! PostgreSQL and standard SQL system catalog schemas and synthetic views (P-05).
//!
//! Provides static table schemas and synthetic rows for PostgreSQL system catalogs
//! (`pg_type`, `pg_class`, `pg_namespace`, `pg_attribute`, `pg_am`, `pg_database`,
//! `pg_proc`, `pg_index`, `pg_settings`, `pg_roles`, `pg_user`, `pg_range`, `pg_enum`,
//! `pg_description`, `pg_constraint`, `pg_foreign_data_wrapper`, `pg_foreign_server`,
//! `pg_collation`) and ANSI/ISO `information_schema` views (`tables`, `columns`,
//! `schemata`, `views`).
//!
//! These synthetic tables are essential for database GUI tools (DBeaver, DataGrip,
//! pgAdmin, Metabase) and ORMs (Prisma, Hasura, SQLAlchemy, Hibernate) to discover
//! schemas, tables, columns, indexes, types, and server configuration.

use surrealdb_expr::expr::logical::{LogicalExpr, LogicalLiteral};
use surrealdb_strand::Strand;

use super::relations::{
	IndexEntry, default_opclass, oid_of_table, user_constraints, user_indexes,
	visible_serial_columns, with_visible_sequence_names,
};
use super::{ColumnInfo, TableSchema, UserTypes, kind_type_name};
use crate::catalog::{CompositeType, DomainType};
use crate::descriptor::{composite_relation_oid, composite_type_oid, domain_type_oid};
use crate::dialect::SqlDialect;
use crate::reported_type::{mysql_reported_type, pg_reported_type};

// --- Helper constructors for LogicalExpr rows ---

#[inline]
fn str_val(s: impl AsRef<str>) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::String(Strand::from(s.as_ref())))
}

#[inline]
fn int_val(i: i64) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::Integer(i))
}

#[inline]
fn bool_val(b: bool) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::Bool(b))
}

#[inline]
fn float_val(f: f64) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::Float(f))
}

#[inline]
fn null_val() -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::Null)
}

fn int_array(values: &[i64]) -> LogicalExpr {
	LogicalExpr::Array(values.iter().map(|v| int_val(*v)).collect())
}

#[inline]
fn row(fields: Vec<(&'static str, LogicalExpr)>) -> LogicalExpr {
	LogicalExpr::Object(fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

/// Normalizes a table name and optional schema to canonical (schema, table) parts.
fn normalize_table_name<'a>(schema: Option<&'a str>, name: &'a str) -> (Option<&'a str>, &'a str) {
	if let Some(rest) = name.strip_prefix("pg_catalog.") {
		(Some("pg_catalog"), rest)
	} else if let Some(rest) = name.strip_prefix("information_schema.") {
		(Some("information_schema"), rest)
	} else {
		(schema, name)
	}
}

/// Returns true if the relation refers to a known system catalog or information_schema table.
pub fn is_system_catalog_table(schema: Option<&str>, name: &str) -> bool {
	let (norm_schema, norm_name) = normalize_table_name(schema, name);
	match norm_schema {
		Some("pg_catalog") => {
			matches!(
				norm_name,
				"pg_type"
					| "pg_class" | "pg_namespace"
					| "pg_attribute"
					| "pg_am" | "pg_database"
					| "pg_proc" | "pg_index"
					| "pg_settings" | "pg_roles"
					| "pg_user" | "pg_range"
					| "pg_enum" | "pg_description"
					| "pg_constraint"
					| "pg_sequences"
					| "pg_attrdef" | "pg_opclass"
					| "pg_policies" | "pg_policy"
					| "pg_statistic_ext"
					| "pg_inherits" | "pg_publication"
					| "pg_publication_rel"
					| "pg_publication_namespace"
					| "pg_stat_user_indexes"
					| "pg_views" | "pg_matviews"
					| "pg_language" | "pg_extension"
					| "pg_foreign_data_wrapper"
					| "pg_foreign_server"
					| "pg_collation"
			)
		}
		Some("information_schema") => {
			matches!(
				norm_name,
				"tables"
					| "columns" | "schemata"
					| "domains" | "views"
					| "sequences" | "table_constraints"
					| "constraint_column_usage"
					| "column_statistics"
			)
		}
		None => {
			matches!(
				norm_name,
				"pg_type"
					| "pg_class" | "pg_namespace"
					| "pg_attribute"
					| "pg_am" | "pg_database"
					| "pg_proc" | "pg_index"
					| "pg_settings" | "pg_roles"
					| "pg_user" | "pg_range"
					| "pg_enum" | "pg_description"
					| "pg_constraint"
					| "pg_sequences"
					| "pg_attrdef" | "pg_opclass"
					| "pg_policies" | "pg_policy"
					| "pg_statistic_ext"
					| "pg_inherits" | "pg_publication"
					| "pg_publication_rel"
					| "pg_publication_namespace"
					| "pg_stat_user_indexes"
					| "pg_views" | "pg_matviews"
					| "pg_language" | "pg_extension"
					| "pg_foreign_data_wrapper"
					| "pg_foreign_server"
					| "pg_collation"
			)
		}
		_ => false,
	}
}

const ATTRDEF_OID_BASE: i64 = 3_000_000;

/// The oid of the first sequence relation `pg_class` lists, in a range of its own beside those
/// of the indexes, constraints, defaults and row types.
const SEQUENCE_OID_BASE: i64 = 5_000_000;

/// The end of the oids the user enum types and their array types take.
const ENUM_OID_RANGE_END: i64 =
	(crate::descriptor::ENUM_TYPE_OID_BASE + 2 * crate::descriptor::ENUM_ARRAY_OID_OFFSET) as i64;

/// The oid of the first table row type: the oid of the composite row type of the table at
/// `position` among the user tables is this plus `position`, in its `pg_class` row's `reltype` and
/// its `pg_type` row's `oid`. Row types share `pg_type` with the user enum types and their arrays,
/// so the range starts where theirs ends.
const TABLE_TYPE_OID_BASE: i64 = ENUM_OID_RANGE_END;

// The table row types, which start past the enum types and their arrays, end below the sequences.
const _: () = assert!(TABLE_TYPE_OID_BASE < SEQUENCE_OID_BASE);

/// The sort-key stride `EnumDefinition` leaves between labels declared together, so that
/// `enumsortorder` of such labels counts 1, 2, 3, and a label inserted between two takes a
/// fraction.
const ENUM_SORT_ORDER_UNIT: u64 = 1 << 32;

/// The element type oid of an enum array type oid.
fn array_elem(array_oid: i64) -> u32 {
	(array_oid as u32).wrapping_sub(crate::descriptor::ENUM_ARRAY_OID_OFFSET)
}

/// The default expression of a column numbered by `sequence`, as `pg_get_expr` reads it back.
fn nextval_default(sequence: &str) -> String {
	format!("nextval('{}'::regclass)", super::relations::quote_ident(sequence))
}

/// The operator class of each column of `index`, by the type of the column.
fn index_opclasses(tables: &[TableSchema], index: &IndexEntry) -> Vec<i64> {
	let table = usize::try_from(index.table_oid - oid_of_table(0))
		.ok()
		.and_then(|position| tables.get(position));
	index
		.attnums
		.iter()
		.map(|attnum| {
			let column =
				table.and_then(|table| table.columns.get(usize::try_from(*attnum - 1).ok()?));
			default_opclass(column.and_then(|column| column.data_type.as_deref())).0
		})
		.collect()
}

/// The columns of the system relations that hold a boolean, which a client reads as one rather
/// than as text.
const BOOLEAN_COLUMNS: &[&str] = &[
	"attbyval",
	"atthasdef",
	"atthasmissing",
	"attisdropped",
	"attislocal",
	"attnotnull",
	"collisdeterministic",
	"condeferrable",
	"condeferred",
	"conislocal",
	"connoinherit",
	"convalidated",
	"cycle",
	"datallowconn",
	"datistemplate",
	"hasindexes",
	"indcheckxmin",
	"indimmediate",
	"indisclustered",
	"indisexclusion",
	"indislive",
	"indisprimary",
	"indisready",
	"indisreplident",
	"indisunique",
	"indisvalid",
	"ispopulated",
	"lanispl",
	"lanpltrusted",
	"extrelocatable",
	"opcdefault",
	"pending_restart",
	"proisstrict",
	"proleakproof",
	"proretset",
	"prosecdef",
	"relforcerowsecurity",
	"relhasindex",
	"relhasrules",
	"relhassubclass",
	"relhastriggers",
	"relispartition",
	"relispartitioned",
	"relispopulated",
	"relisshared",
	"relrowsecurity",
	"rolbypassrls",
	"rolcanlogin",
	"rolcreatedb",
	"rolcreaterole",
	"rolinherit",
	"rolreplication",
	"rolsuper",
	"typbyval",
	"typisdefined",
	"typispreferred",
	"typnotnull",
	"usebypassrls",
	"usecatupd",
	"usecreatedb",
	"userepl",
	"usesuper",
];

/// The declared type of the system relations' columns that are not text, as PostgreSQL declares
/// them, by column name; a column not listed is text. A client that reads a column as a number or
/// a one-character code relies on the type the wire reports.
fn system_column_type(column: &str) -> Option<&'static str> {
	Some(match column {
		"oid" | "relnamespace" | "reltype" | "reloftype" | "relowner" | "relam" | "relfilenode"
		| "reltablespace" | "reltoastrelid" | "relrewrite" | "attrelid" | "atttypid"
		| "typnamespace" | "typowner" | "typrelid" | "typelem" | "typarray" | "typbasetype"
		| "typcollation" | "connamespace" | "conrelid" | "contypid" | "conindid" | "confrelid"
		| "indexrelid" | "indrelid" | "adrelid" | "nspowner" | "enumtypid" | "objoid"
		| "classoid" | "opcmethod" | "opcnamespace" | "opcowner" | "opcfamily" | "opcintype"
		| "opckeytype" | "extowner" | "extnamespace" | "lanowner" | "lanplcallfoid"
		| "laninline" | "lanvalidator" | "rngtypid" | "rngsubtype" | "pronamespace"
		| "proowner" | "prolang" | "prorettype" | "datdba" | "dattablespace" | "collnamespace"
		| "collowner" | "attcollation" => "oid",
		"attnum" | "attlen" | "relnatts" | "relchecks" | "typlen" | "adnum" | "indnatts"
		| "indnkeyatts" | "pronargs" | "pronargdefaults" | "attndims" | "objsubid" => "int2",
		"atttypmod"
		| "typtypmod"
		| "typndims"
		| "attinhcount"
		| "attstattarget"
		| "encoding"
		| "datconnlimit"
		| "rolconnlimit"
		| "ordinal_position"
		| "numeric_precision"
		| "numeric_scale"
		| "numeric_precision_radix"
		| "datetime_precision"
		| "character_maximum_length"
		| "character_octet_length" => "int4",
		"relkind" | "relpersistence" | "relreplident" | "typtype" | "typcategory" | "typdelim"
		| "typalign" | "typstorage" | "contype" | "confupdtype" | "confdeltype"
		| "confmatchtype" | "attstorage" | "attidentity" | "attgenerated" | "attalign"
		| "amtype" | "prokind" | "provolatile" | "proparallel" => "\"char\"",
		"conkey" | "confkey" => "int2[]",
		"indkey" | "indoption" => "int2vector",
		"indclass" => "oidvector",
		"data_type" | "is_nullable" | "is_identity" => "varchar",
		_ => return None,
	})
}

/// How the system views name a column that holds the labels of an enum: its `data_type` and its
/// MySQL-style `column_type` (`enum('a','b')`, `set('a','b')`), with `udt` set to the name of a
/// named enum's type. Both are `None` for a column that holds none.
fn enum_column_names(
	held: Option<&crate::catalog::EnumColumn>,
	user_enums: &[crate::catalog::EnumType],
	udt: &mut String,
) -> (Option<&'static str>, Option<String>) {
	use crate::catalog::EnumShape;
	let Some(held) = held else {
		return (None, None);
	};
	let labels: Vec<String> = match &held.reference {
		crate::catalog::EnumRef::Named(name) => {
			match user_enums.iter().find(|ty| ty.name == *name) {
				Some(ty) => {
					*udt = match held.shape {
						EnumShape::Scalar | EnumShape::Set => ty.name.clone(),
						EnumShape::Array => format!("_{}", ty.name),
					};
					ty.labels.iter().map(|l| l.label.clone()).collect()
				}
				None => Vec::new(),
			}
		}
		crate::catalog::EnumRef::Inline(labels) => labels.clone(),
	};
	let list = labels
		.iter()
		.map(|label| format!("'{}'", label.replace('\'', "''")))
		.collect::<Vec<_>>()
		.join(",");
	let named = matches!(held.reference, crate::catalog::EnumRef::Named(_));
	match (held.shape, named) {
		(EnumShape::Scalar, true) => (Some("USER-DEFINED"), Some(format!("enum({list})"))),
		(EnumShape::Scalar, false) => (Some("enum"), Some(format!("enum({list})"))),
		(EnumShape::Set, _) => (Some("set"), Some(format!("set({list})"))),
		(EnumShape::Array, _) => (Some("ARRAY"), Some(format!("enum({list})[]"))),
	}
}

/// What `information_schema.columns` lists for a column declared with a SQL type, in the
/// dialect of the statement that reads it.
struct DeclaredColumnFacts {
	data_type: String,
	column_type: String,
	/// `udt_name`, which only PostgreSQL names.
	udt_name: Option<String>,
	character_maximum_length: Option<i64>,
	character_octet_length: Option<i64>,
	numeric_precision: Option<i64>,
	numeric_precision_radix: Option<i64>,
	numeric_scale: Option<i64>,
	datetime_precision: Option<i64>,
}

impl DeclaredColumnFacts {
	/// The facts of `column` for a statement of `dialect`, or `None` when the column has no
	/// declared SQL type that dialect reports, or holds an enum or a composite, which the views
	/// name by their own types.
	fn of(column: &ColumnInfo, dialect: SqlDialect) -> Option<Self> {
		if column.enum_held.is_some() || column.composite.is_some() || column.domain.is_some() {
			return None;
		}
		let declared = column.declared.as_ref()?;
		match dialect {
			SqlDialect::Postgres => {
				let reported = pg_reported_type(declared)?;
				Some(Self {
					data_type: reported.data_type,
					column_type: reported.format,
					udt_name: Some(reported.udt_name),
					character_maximum_length: reported.character_maximum_length,
					character_octet_length: reported.character_octet_length,
					numeric_precision: reported.numeric_precision,
					numeric_precision_radix: reported.numeric_precision_radix,
					numeric_scale: reported.numeric_scale,
					datetime_precision: reported.datetime_precision,
				})
			}
			SqlDialect::MySql => {
				let reported = mysql_reported_type(declared)?;
				Some(Self {
					data_type: reported.data_type,
					column_type: reported.column_type,
					udt_name: None,
					character_maximum_length: reported.character_maximum_length,
					character_octet_length: reported.character_octet_length,
					numeric_precision: reported.numeric_precision,
					numeric_precision_radix: None,
					numeric_scale: reported.numeric_scale,
					datetime_precision: reported.datetime_precision,
				})
			}
		}
	}
}

/// The base type of a domain as PostgreSQL reports it: `pg_type.typbasetype` and the
/// `information_schema.domains` columns that describe it.
struct DomainBase {
	/// The `pg_type` OID of the base type: the array type's for an array base.
	oid: i64,
	/// `information_schema.domains.data_type`: the type's name, `ARRAY`, or `USER-DEFINED` for a
	/// user-defined base.
	data_type: String,
	/// `information_schema.domains.udt_name`: the `pg_type.typname` of the base.
	udt_name: String,
	/// The full report of a base that is a built-in type, which carries its length, precision
	/// and scale.
	reported: Option<crate::reported_type::PgReportedType>,
}

impl DomainBase {
	/// The base type of `domain`. A base naming a user-defined enum, domain or composite type is
	/// that type. A built-in base is reported as a column declared with the same type is. A base
	/// the declared text does not name is reported by the kind its values have, and as `text` when
	/// that names none.
	fn of(
		domain: &DomainType,
		user_enums: &[crate::catalog::EnumType],
		domains: &[DomainType],
		composites: &[CompositeType],
	) -> Self {
		if let surrealdb_expr::expr::Kind::Named(name) = &domain.base
			&& let Some(oid) = named_enum_oid(Some(name.as_str()), user_enums)
				.or_else(|| domain_attribute_oid(Some(name.as_str()), domains))
				.or_else(|| composite_attribute_oid(Some(name.as_str()), composites))
		{
			return Self {
				oid,
				data_type: "USER-DEFINED".to_owned(),
				udt_name: name.clone(),
				reported: None,
			};
		}
		if let Ok(declared) = crate::declared_type::DeclaredSqlType::parse(
			crate::SqlTypeDialect::PostgreSql,
			&domain.base_type,
		) && let Some(reported) = pg_reported_type(&declared)
		{
			return Self {
				oid: i64::from(reported.oid),
				data_type: reported.data_type.clone(),
				udt_name: reported.udt_name.clone(),
				reported: Some(reported),
			};
		}
		let name = kind_type_name(&domain.base).unwrap_or_else(|| "text".to_owned());
		Self {
			oid: attribute_type_oid(&name),
			udt_name: crate::SqlType::from_name(&name)
				.udt_name()
				.unwrap_or_else(|| "text".to_owned()),
			data_type: name,
			reported: None,
		}
	}
}

/// The type a composite attribute of `kind` names, alone or with `none` and `null`: an attribute
/// is stored as a nullable kind.
fn attribute_named_type(kind: &surrealdb_expr::expr::Kind) -> Option<&str> {
	use surrealdb_expr::expr::Kind;
	if let Kind::Named(name) = kind {
		return Some(name.as_str());
	}
	let Kind::Either(kinds) = kind else {
		return None;
	};
	let mut rest = kinds.iter().filter(|kind| !matches!(kind, Kind::None | Kind::Null));
	if let (Some(Kind::Named(name)), None) = (rest.next(), rest.next()) {
		Some(name.as_str())
	} else {
		None
	}
}

/// The `pg_attribute.attlen` of a type with OID `oid`: its fixed width, or -1 when it is variable.
fn attribute_len(oid: i64) -> i64 {
	match oid {
		16 => 1,
		21 => 2,
		23 | 700 => 4,
		20 | 701 => 8,
		2950 => 16,
		_ => -1,
	}
}

/// The `pg_type.typcategory` of a base type with OID `oid`, as PostgreSQL categorises it.
fn type_category(oid: i64) -> char {
	match oid {
		16 => 'B',
		18 | 19 | 25 | 1042 | 1043 => 'S',
		20 | 21 | 23 | 26 | 700 | 701 | 1700 => 'N',
		1082 | 1083 | 1114 | 1184 | 1266 => 'D',
		1186 => 'T',
		_ => 'U',
	}
}

/// The `atttypid` of a column declared with the user-defined domain `name`: the OID of the domain.
fn domain_attribute_oid(name: Option<&str>, domains: &[DomainType]) -> Option<i64> {
	let name = name?;
	let domain = domains.iter().find(|domain| domain.name == name)?;
	Some(i64::from(domain_type_oid(domain.id)))
}

/// The `atttypid` of a column declared with the user-defined composite type `name`: the OID of the
/// composite type.
fn composite_attribute_oid(name: Option<&str>, composites: &[CompositeType]) -> Option<i64> {
	let name = name?;
	let composite = composites.iter().find(|composite| composite.name == name)?;
	Some(i64::from(composite_type_oid(composite.id)))
}

/// The `atttypid` of a composite attribute of the user-defined enum `name`: the OID of its type.
fn named_enum_oid(name: Option<&str>, user_enums: &[crate::catalog::EnumType]) -> Option<i64> {
	let name = name?;
	let ty = user_enums.iter().find(|ty| ty.name == name)?;
	Some(i64::from(crate::descriptor::enum_type_oid(ty.id)))
}

/// The `atttypid` of a column that holds the labels of a named enum: the OID of its type, or of the
/// array of it.
fn enum_attribute_oid(
	held: Option<&crate::catalog::EnumColumn>,
	user_enums: &[crate::catalog::EnumType],
) -> Option<i64> {
	use crate::catalog::EnumShape;
	let held = held?;
	let crate::catalog::EnumRef::Named(name) = &held.reference else {
		return None;
	};
	let ty = user_enums.iter().find(|ty| ty.name == *name)?;
	Some(i64::from(match held.shape {
		EnumShape::Scalar | EnumShape::Set => crate::descriptor::enum_type_oid(ty.id),
		EnumShape::Array => crate::descriptor::enum_array_type_oid(ty.id),
	}))
}

/// Helper to build a `TableSchema` with column names and ordinal indices.
fn make_schema(schema_name: Option<&str>, table_name: &str, cols: &[&'static str]) -> TableSchema {
	let mut s = TableSchema::new(
		table_name,
		cols.iter()
			.enumerate()
			.map(|(i, &c)| {
				let column = ColumnInfo::new(c, i);
				if BOOLEAN_COLUMNS.contains(&c) {
					column.with_type("boolean")
				} else if let Some(ty) = system_column_type(c) {
					column.with_type(ty)
				} else {
					column
				}
			})
			.collect(),
	);
	if let Some(sn) = schema_name {
		s = s.with_schema(sn);
	}
	s
}

/// Look up schema definition for system catalog or information_schema tables.
pub fn get_system_table_schema(schema: Option<&str>, name: &str) -> Option<TableSchema> {
	let (norm_schema, norm_name) = normalize_table_name(schema, name);
	match (norm_schema, norm_name) {
		(Some("pg_catalog") | None, "pg_type") => Some(make_schema(
			Some("pg_catalog"),
			"pg_type",
			&[
				"oid",
				"typname",
				"typnamespace",
				"typowner",
				"typlen",
				"typbyval",
				"typtype",
				"typcategory",
				"typispreferred",
				"typisdefined",
				"typdelim",
				"typrelid",
				"typelem",
				"typarray",
				"typinput",
				"typoutput",
				"typreceive",
				"typsend",
				"typalign",
				"typstorage",
				"typnotnull",
				"typbasetype",
				"typtypmod",
				"typndims",
				"typcollation",
				"typdefaultbin",
				"typdefault",
				"typacl",
			],
		)),
		(Some("pg_catalog") | None, "pg_class") => Some(make_schema(
			Some("pg_catalog"),
			"pg_class",
			&[
				"oid",
				"relname",
				"relnamespace",
				"reltype",
				"reloftype",
				"relowner",
				"relam",
				"relfilenode",
				"reltablespace",
				"relpages",
				"reltuples",
				"relallvisible",
				"reltoastrelid",
				"relhasindex",
				"relisshared",
				"relpersistence",
				"relkind",
				"relnatts",
				"relchecks",
				"relhasrules",
				"relhastriggers",
				"relhassubclass",
				"relrowsecurity",
				"relforcerowsecurity",
				"relispopulated",
				"relreplident",
				"relispartitioned",
				"relispartition",
				"relrewrite",
				"relfrozenxid",
				"relminmxid",
				"relacl",
				"reloptions",
				"relpartbound",
			],
		)),
		(Some("pg_catalog") | None, "pg_namespace") => Some(make_schema(
			Some("pg_catalog"),
			"pg_namespace",
			&["oid", "nspname", "nspowner", "nspacl"],
		)),
		(Some("pg_catalog") | None, "pg_attribute") => Some(make_schema(
			Some("pg_catalog"),
			"pg_attribute",
			&[
				"attrelid",
				"attname",
				"atttypid",
				"attstattarget",
				"attlen",
				"attnum",
				"attndims",
				"attcacheoff",
				"atttypmod",
				"attbyval",
				"attstorage",
				"attalign",
				"attnotnull",
				"atthasdef",
				"atthasmissing",
				"attidentity",
				"attgenerated",
				"attisdropped",
				"attislocal",
				"attinhcount",
				"attcollation",
				"attacl",
				"attoptions",
				"attfdwoptions",
				"attmissingval",
			],
		)),
		(Some("pg_catalog") | None, "pg_am") => Some(make_schema(
			Some("pg_catalog"),
			"pg_am",
			&["oid", "amname", "amhandler", "amtype"],
		)),
		(Some("pg_catalog") | None, "pg_database") => Some(make_schema(
			Some("pg_catalog"),
			"pg_database",
			&[
				"oid",
				"datname",
				"datdba",
				"encoding",
				"datcollate",
				"datctype",
				"datistemplate",
				"datallowconn",
				"datconnlimit",
				"datlastsysoid",
				"datfrozenxid",
				"datminmxid",
				"dattablespace",
				"datacl",
			],
		)),
		(Some("pg_catalog") | None, "pg_proc") => Some(make_schema(
			Some("pg_catalog"),
			"pg_proc",
			&[
				"oid",
				"proname",
				"pronamespace",
				"proowner",
				"prolang",
				"procost",
				"prorows",
				"provariadic",
				"prosupport",
				"prokind",
				"prosecdef",
				"proleakproof",
				"proisstrict",
				"proretset",
				"provolatile",
				"proparallel",
				"pronargs",
				"pronargdefaults",
				"prorettype",
				"proargtypes",
				"proallargtypes",
				"proargmodes",
				"proargnames",
				"proargdefaults",
				"protrftypes",
				"prosrc",
				"probin",
				"prosqlbody",
				"proconfig",
				"proacl",
			],
		)),
		(Some("pg_catalog") | None, "pg_index") => Some(make_schema(
			Some("pg_catalog"),
			"pg_index",
			&[
				"indexrelid",
				"indrelid",
				"indnatts",
				"indnkeyatts",
				"indisunique",
				"indisprimary",
				"indisexclusion",
				"indimmediate",
				"indisclustered",
				"indisvalid",
				"indcheckxmin",
				"indisready",
				"indislive",
				"indisreplident",
				"indkey",
				"indcollation",
				"indclass",
				"indoption",
				"indexprs",
				"indpred",
			],
		)),
		(Some("pg_catalog") | None, "pg_settings") => Some(make_schema(
			Some("pg_catalog"),
			"pg_settings",
			&[
				"name",
				"setting",
				"unit",
				"category",
				"short_desc",
				"extra_desc",
				"context",
				"vartype",
				"source",
				"min_val",
				"max_val",
				"enumvals",
				"boot_val",
				"reset_val",
				"sourcefile",
				"sourceline",
				"pending_restart",
			],
		)),
		(Some("pg_catalog") | None, "pg_roles") => Some(make_schema(
			Some("pg_catalog"),
			"pg_roles",
			&[
				"oid",
				"rolname",
				"rolsuper",
				"rolinherit",
				"rolcreaterole",
				"rolcreatedb",
				"rolcanlogin",
				"rolreplication",
				"rolconnlimit",
				"rolpassword",
				"rolvaliduntil",
				"rolbypassrls",
				"rolconfig",
			],
		)),
		(Some("pg_catalog") | None, "pg_user") => Some(make_schema(
			Some("pg_catalog"),
			"pg_user",
			&[
				"usesysid",
				"usename",
				"usesuper",
				"usecreatedb",
				"valuntil",
				"usecatupd",
				"userepl",
				"usebypassrls",
				"passwd",
			],
		)),
		(Some("pg_catalog") | None, "pg_range") => Some(make_schema(
			Some("pg_catalog"),
			"pg_range",
			&[
				"rngtypid",
				"rngsubtype",
				"rngmultitypid",
				"rngcollation",
				"rngsubopc",
				"rngcanonical",
				"rngsubdiff",
			],
		)),
		(Some("pg_catalog") | None, "pg_sequences") => Some(make_schema(
			Some("pg_catalog"),
			"pg_sequences",
			&[
				"schemaname",
				"sequencename",
				"sequenceowner",
				"data_type",
				"start_value",
				"min_value",
				"max_value",
				"increment_by",
				"cycle",
				"cache_size",
				"last_value",
			],
		)),
		(Some("pg_catalog") | None, "pg_attrdef") => Some(make_schema(
			Some("pg_catalog"),
			"pg_attrdef",
			&["oid", "adrelid", "adnum", "adbin"],
		)),
		(Some("pg_catalog") | None, "pg_opclass") => Some(make_schema(
			Some("pg_catalog"),
			"pg_opclass",
			&[
				"oid",
				"opcmethod",
				"opcname",
				"opcnamespace",
				"opcowner",
				"opcfamily",
				"opcintype",
				"opcdefault",
				"opckeytype",
			],
		)),
		(Some("pg_catalog") | None, "pg_policies") => Some(make_schema(
			Some("pg_catalog"),
			"pg_policies",
			&[
				"schemaname",
				"tablename",
				"policyname",
				"permissive",
				"roles",
				"cmd",
				"qual",
				"with_check",
			],
		)),
		(Some("pg_catalog") | None, "pg_statistic_ext") => Some(make_schema(
			Some("pg_catalog"),
			"pg_statistic_ext",
			&[
				"oid",
				"stxrelid",
				"stxname",
				"stxnamespace",
				"stxowner",
				"stxstattarget",
				"stxkeys",
				"stxkind",
				"stxexprs",
			],
		)),
		(Some("pg_catalog") | None, "pg_inherits") => Some(make_schema(
			Some("pg_catalog"),
			"pg_inherits",
			&["inhrelid", "inhparent", "inhseqno", "inhdetachpending"],
		)),
		(Some("pg_catalog") | None, "pg_publication") => Some(make_schema(
			Some("pg_catalog"),
			"pg_publication",
			&[
				"oid",
				"pubname",
				"pubowner",
				"puballtables",
				"pubinsert",
				"pubupdate",
				"pubdelete",
				"pubtruncate",
				"pubviaroot",
			],
		)),
		(Some("pg_catalog") | None, "pg_publication_rel") => Some(make_schema(
			Some("pg_catalog"),
			"pg_publication_rel",
			&["oid", "prpubid", "prrelid", "prqual", "prattrs"],
		)),
		(Some("pg_catalog") | None, "pg_publication_namespace") => Some(make_schema(
			Some("pg_catalog"),
			"pg_publication_namespace",
			&["oid", "pnpubid", "pnnspid"],
		)),
		(Some("pg_catalog") | None, "pg_policy") => Some(make_schema(
			Some("pg_catalog"),
			"pg_policy",
			&[
				"oid",
				"polname",
				"polrelid",
				"polcmd",
				"polpermissive",
				"polroles",
				"polqual",
				"polwithcheck",
			],
		)),
		(Some("pg_catalog") | None, "pg_stat_user_indexes") => Some(make_schema(
			Some("pg_catalog"),
			"pg_stat_user_indexes",
			&[
				"relid",
				"indexrelid",
				"schemaname",
				"relname",
				"indexrelname",
				"idx_scan",
				"last_idx_scan",
				"idx_tup_read",
				"idx_tup_fetch",
			],
		)),
		(Some("information_schema"), "table_constraints") => Some(make_schema(
			Some("information_schema"),
			"table_constraints",
			&[
				"constraint_catalog",
				"constraint_schema",
				"constraint_name",
				"table_catalog",
				"table_schema",
				"table_name",
				"constraint_type",
				"is_deferrable",
				"initially_deferred",
				"enforced",
				"nulls_distinct",
			],
		)),
		(Some("information_schema"), "constraint_column_usage") => Some(make_schema(
			Some("information_schema"),
			"constraint_column_usage",
			&[
				"table_catalog",
				"table_schema",
				"table_name",
				"column_name",
				"constraint_catalog",
				"constraint_schema",
				"constraint_name",
			],
		)),
		(Some("pg_catalog") | None, "pg_language") => Some(make_schema(
			Some("pg_catalog"),
			"pg_language",
			&[
				"oid",
				"lanname",
				"lanowner",
				"lanispl",
				"lanpltrusted",
				"lanplcallfoid",
				"laninline",
				"lanvalidator",
				"lanacl",
			],
		)),
		(Some("pg_catalog") | None, "pg_extension") => Some(make_schema(
			Some("pg_catalog"),
			"pg_extension",
			&[
				"oid",
				"extname",
				"extowner",
				"extnamespace",
				"extrelocatable",
				"extversion",
				"extconfig",
				"extcondition",
			],
		)),
		(Some("pg_catalog") | None, "pg_views") => Some(make_schema(
			Some("pg_catalog"),
			"pg_views",
			&["schemaname", "viewname", "viewowner", "definition"],
		)),
		(Some("pg_catalog") | None, "pg_matviews") => Some(make_schema(
			Some("pg_catalog"),
			"pg_matviews",
			&[
				"schemaname",
				"matviewname",
				"matviewowner",
				"tablespace",
				"hasindexes",
				"ispopulated",
				"definition",
			],
		)),
		(Some("pg_catalog") | None, "pg_enum") => Some(make_schema(
			Some("pg_catalog"),
			"pg_enum",
			&["oid", "enumtypid", "enumsortorder", "enumlabel"],
		)),
		(Some("pg_catalog") | None, "pg_description") => Some(make_schema(
			Some("pg_catalog"),
			"pg_description",
			&["objoid", "classoid", "objsubid", "description"],
		)),
		(Some("pg_catalog") | None, "pg_constraint") => Some(make_schema(
			Some("pg_catalog"),
			"pg_constraint",
			&[
				"oid",
				"conname",
				"connamespace",
				"contype",
				"condeferrable",
				"condeferred",
				"convalidated",
				"conrelid",
				"contypid",
				"conindid",
				"conparentid",
				"confrelid",
				"confupdtype",
				"confdeltype",
				"confmatchtype",
				"conislocal",
				"coninhcount",
				"connoinherit",
				"conkey",
				"confkey",
				"conpfeqop",
				"conppeqop",
				"conffeqop",
				"conexclop",
				"conbin",
			],
		)),
		(Some("pg_catalog") | None, "pg_foreign_data_wrapper") => Some(make_schema(
			Some("pg_catalog"),
			"pg_foreign_data_wrapper",
			&["oid", "fdwname", "fdwowner", "fdwhandler", "fdwvalidator", "fdwacl", "fdwoptions"],
		)),
		(Some("pg_catalog") | None, "pg_foreign_server") => Some(make_schema(
			Some("pg_catalog"),
			"pg_foreign_server",
			&[
				"oid",
				"srvname",
				"srvowner",
				"srvfdw",
				"srvtype",
				"srvversion",
				"srvacl",
				"srvoptions",
			],
		)),
		(Some("pg_catalog") | None, "pg_collation") => Some(make_schema(
			Some("pg_catalog"),
			"pg_collation",
			&[
				"oid",
				"collname",
				"collnamespace",
				"collowner",
				"collprovider",
				"collisdeterministic",
				"collencoding",
				"collcollate",
				"collctype",
				"colliculocale",
				"collversion",
			],
		)),
		(Some("information_schema"), "tables") => Some(make_schema(
			Some("information_schema"),
			"tables",
			&[
				"table_catalog",
				"table_schema",
				"table_name",
				"table_type",
				"self_referencing_column_name",
				"reference_generation",
				"user_defined_type_catalog",
				"user_defined_type_schema",
				"user_defined_type_name",
				"is_insertable_into",
				"is_typed",
				"commit_action",
			],
		)),
		(Some("information_schema"), "domains") => Some(make_schema(
			Some("information_schema"),
			"domains",
			&[
				"domain_catalog",
				"domain_schema",
				"domain_name",
				"data_type",
				"character_maximum_length",
				"character_octet_length",
				"character_set_catalog",
				"character_set_schema",
				"character_set_name",
				"collation_catalog",
				"collation_schema",
				"collation_name",
				"numeric_precision",
				"numeric_precision_radix",
				"numeric_scale",
				"datetime_precision",
				"interval_type",
				"interval_precision",
				"domain_default",
				"udt_catalog",
				"udt_schema",
				"udt_name",
				"scope_catalog",
				"scope_schema",
				"scope_name",
				"maximum_cardinality",
				"dtd_identifier",
			],
		)),
		(Some("information_schema"), "columns") => Some(make_schema(
			Some("information_schema"),
			"columns",
			&[
				"table_catalog",
				"table_schema",
				"table_name",
				"column_name",
				"ordinal_position",
				"column_default",
				"is_nullable",
				"data_type",
				"character_maximum_length",
				"character_octet_length",
				"numeric_precision",
				"numeric_precision_radix",
				"numeric_scale",
				"datetime_precision",
				"interval_type",
				"interval_precision",
				"character_set_catalog",
				"character_set_schema",
				"character_set_name",
				"collation_catalog",
				"collation_schema",
				"collation_name",
				"domain_catalog",
				"domain_schema",
				"domain_name",
				"udt_catalog",
				"udt_schema",
				"udt_name",
				"scope_catalog",
				"scope_schema",
				"scope_name",
				"maximum_cardinality",
				"dtd_identifier",
				"is_self_referencing",
				"is_identity",
				"identity_generation",
				"identity_start",
				"identity_increment",
				"identity_maximum",
				"identity_minimum",
				"identity_cycle",
				"is_generated",
				"generation_expression",
				"is_updatable",
				"column_type",
			],
		)),
		(Some("information_schema"), "column_statistics") => Some(make_schema(
			Some("information_schema"),
			"column_statistics",
			&["schema_name", "table_name", "column_name", "histogram"],
		)),
		(Some("information_schema"), "schemata") => Some(make_schema(
			Some("information_schema"),
			"schemata",
			&[
				"catalog_name",
				"schema_name",
				"schema_owner",
				"default_character_set_catalog",
				"default_character_set_schema",
				"default_character_set_name",
				"sql_path",
			],
		)),
		(Some("information_schema"), "sequences") => Some(make_schema(
			Some("information_schema"),
			"sequences",
			&[
				"sequence_catalog",
				"sequence_schema",
				"sequence_name",
				"data_type",
				"numeric_precision",
				"numeric_precision_radix",
				"numeric_scale",
				"start_value",
				"minimum_value",
				"maximum_value",
				"increment",
				"cycle_option",
			],
		)),
		(Some("information_schema"), "views") => Some(make_schema(
			Some("information_schema"),
			"views",
			&[
				"table_catalog",
				"table_schema",
				"table_name",
				"view_definition",
				"check_option",
				"is_updatable",
				"is_insertable_into",
				"is_trigger_updatable",
				"is_trigger_deletable",
				"is_trigger_insertable_into",
			],
		)),
		_ => None,
	}
}

/// Whether the rows of this system relation list the user's tables, which
/// [`get_system_table_rows`] takes as its `user_tables`: `information_schema.tables` and
/// `columns`, `sequences`, `pg_class`, `pg_type`, `pg_attribute`, `pg_index`, `pg_constraint`,
/// `pg_sequences`, `pg_attrdef`, `pg_stat_user_indexes`, `information_schema.table_constraints` and
/// `information_schema.constraint_column_usage`.
pub fn lists_user_tables(schema: Option<&str>, name: &str) -> bool {
	matches!(
		normalize_table_name(schema, name),
		(
			Some("pg_catalog") | None,
			"pg_class"
				| "pg_type" | "pg_attribute"
				| "pg_index" | "pg_constraint"
				| "pg_sequences"
				| "pg_attrdef"
				| "pg_stat_user_indexes",
		) | (
			Some("information_schema"),
			"tables" | "columns" | "sequences" | "table_constraints" | "constraint_column_usage",
		)
	)
}

/// Generates synthetic rows for the requested system catalog or information_schema table, which
/// name `database` as their catalog.
pub fn get_system_table_rows(
	schema: Option<&str>,
	name: &str,
	user_tables: &[TableSchema],
	database: &str,
) -> Option<Vec<LogicalExpr>> {
	get_system_table_rows_with_enums(schema, name, user_tables, &[], database)
}

/// [`get_system_table_rows`], where `pg_type` and `pg_enum` also hold the user-defined enum types
/// `user_enums`.
pub fn get_system_table_rows_with_enums(
	schema: Option<&str>,
	name: &str,
	user_tables: &[TableSchema],
	user_enums: &[crate::catalog::EnumType],
	database: &str,
) -> Option<Vec<LogicalExpr>> {
	let types = UserTypes {
		enums: user_enums.to_vec(),
		..UserTypes::default()
	};
	get_system_table_rows_with_types(schema, name, user_tables, &types, database)
}

/// [`get_system_table_rows`], where the catalog views also hold the user-defined types `types`:
/// `pg_type` lists its enums, domains and composite types, `pg_class` and `pg_attribute` the
/// relations of its composite types, and `information_schema.domains` its domains.
pub fn get_system_table_rows_with_types(
	schema: Option<&str>,
	name: &str,
	user_tables: &[TableSchema],
	types: &UserTypes,
	database: &str,
) -> Option<Vec<LogicalExpr>> {
	get_system_table_rows_in(schema, name, user_tables, types, database, SqlDialect::Postgres)
}

/// [`get_system_table_rows_with_types`] for a statement of `dialect`: a column declared with a
/// SQL type is listed by `information_schema.columns` as that type, or as the nearest type of
/// `dialect` when it is declared in the other one.
pub fn get_system_table_rows_in(
	schema: Option<&str>,
	name: &str,
	user_tables: &[TableSchema],
	types: &UserTypes,
	database: &str,
	dialect: SqlDialect,
) -> Option<Vec<LogicalExpr>> {
	let user_enums = types.enums.as_slice();
	let domains = types.domains.as_slice();
	let composites = types.composites.as_slice();
	let visible_tables = with_visible_sequence_names(user_tables);
	let user_tables = &*visible_tables;
	let (norm_schema, norm_name) = normalize_table_name(schema, name);
	match (norm_schema, norm_name) {
		(Some("pg_catalog") | None, "pg_namespace") => Some(vec![
			row(vec![
				("oid", int_val(11)),
				("nspname", str_val("pg_catalog")),
				("nspowner", int_val(10)),
				("nspacl", null_val()),
			]),
			row(vec![
				("oid", int_val(2200)),
				("nspname", str_val("public")),
				("nspowner", int_val(10)),
				("nspacl", null_val()),
			]),
			row(vec![
				("oid", int_val(11000)),
				("nspname", str_val("information_schema")),
				("nspowner", int_val(10)),
				("nspacl", null_val()),
			]),
			row(vec![
				("oid", int_val(99)),
				("nspname", str_val("pg_toast")),
				("nspowner", int_val(10)),
				("nspacl", null_val()),
			]),
		]),

		(Some("pg_catalog") | None, "pg_database") => {
			let mut rows = vec![row(vec![
				("oid", int_val(13757)),
				("datname", str_val(database)),
				("datdba", int_val(10)),
				("encoding", int_val(6)),
				("datcollate", str_val("en_US.UTF-8")),
				("datctype", str_val("en_US.UTF-8")),
				("datistemplate", bool_val(false)),
				("datallowconn", bool_val(true)),
				("datconnlimit", int_val(-1)),
				("datlastsysoid", int_val(0)),
				("datfrozenxid", int_val(0)),
				("datminmxid", int_val(0)),
				("dattablespace", int_val(1663)),
				("datacl", null_val()),
			])];
			// The database the statement runs in is the first row; the compatibility row is
			// omitted when it would repeat that name.
			if database != "postgres" {
				rows.push(row(vec![
					("oid", int_val(1)),
					("datname", str_val("postgres")),
					("datdba", int_val(10)),
					("encoding", int_val(6)),
					("datcollate", str_val("en_US.UTF-8")),
					("datctype", str_val("en_US.UTF-8")),
					("datistemplate", bool_val(false)),
					("datallowconn", bool_val(true)),
					("datconnlimit", int_val(-1)),
					("datlastsysoid", int_val(0)),
					("datfrozenxid", int_val(0)),
					("datminmxid", int_val(0)),
					("dattablespace", int_val(1663)),
					("datacl", null_val()),
				]));
			}
			Some(rows)
		}

		(Some("pg_catalog") | None, "pg_roles") => Some(vec![
			row(vec![
				("oid", int_val(10)),
				("rolname", str_val("root")),
				("rolsuper", bool_val(true)),
				("rolinherit", bool_val(true)),
				("rolcreaterole", bool_val(true)),
				("rolcreatedb", bool_val(true)),
				("rolcanlogin", bool_val(true)),
				("rolreplication", bool_val(true)),
				("rolconnlimit", int_val(-1)),
				("rolpassword", null_val()),
				("rolvaliduntil", null_val()),
				("rolbypassrls", bool_val(true)),
				("rolconfig", null_val()),
			]),
			row(vec![
				("oid", int_val(11)),
				("rolname", str_val("postgres")),
				("rolsuper", bool_val(true)),
				("rolinherit", bool_val(true)),
				("rolcreaterole", bool_val(true)),
				("rolcreatedb", bool_val(true)),
				("rolcanlogin", bool_val(true)),
				("rolreplication", bool_val(true)),
				("rolconnlimit", int_val(-1)),
				("rolpassword", null_val()),
				("rolvaliduntil", null_val()),
				("rolbypassrls", bool_val(true)),
				("rolconfig", null_val()),
			]),
		]),

		(Some("pg_catalog") | None, "pg_user") => Some(vec![
			row(vec![
				("usesysid", int_val(10)),
				("usename", str_val("root")),
				("usesuper", bool_val(true)),
				("usecreatedb", bool_val(true)),
				("valuntil", null_val()),
				("usecatupd", bool_val(true)),
				("userepl", bool_val(true)),
				("usebypassrls", bool_val(true)),
				("passwd", null_val()),
			]),
			row(vec![
				("usesysid", int_val(11)),
				("usename", str_val("postgres")),
				("usesuper", bool_val(true)),
				("usecreatedb", bool_val(true)),
				("valuntil", null_val()),
				("usecatupd", bool_val(true)),
				("userepl", bool_val(true)),
				("usebypassrls", bool_val(true)),
				("passwd", null_val()),
			]),
		]),

		(Some("pg_catalog") | None, "pg_am") => Some(vec![
			row(vec![
				("oid", int_val(403)),
				("amname", str_val("btree")),
				("amhandler", int_val(334)),
				("amtype", str_val("i")),
			]),
			row(vec![
				("oid", int_val(405)),
				("amname", str_val("hash")),
				("amhandler", int_val(335)),
				("amtype", str_val("i")),
			]),
			row(vec![
				("oid", int_val(783)),
				("amname", str_val("gist")),
				("amhandler", int_val(784)),
				("amtype", str_val("i")),
			]),
			row(vec![
				("oid", int_val(2742)),
				("amname", str_val("gin")),
				("amhandler", int_val(2743)),
				("amtype", str_val("i")),
			]),
			row(vec![
				("oid", int_val(4000)),
				("amname", str_val("spgist")),
				("amhandler", int_val(4001)),
				("amtype", str_val("i")),
			]),
			row(vec![
				("oid", int_val(3580)),
				("amname", str_val("brin")),
				("amhandler", int_val(3581)),
				("amtype", str_val("i")),
			]),
		]),

		(Some("pg_catalog") | None, "pg_type") => {
			let vector_oid = i64::from(crate::descriptor::VECTOR_OID);
			let vector_array_oid = i64::from(crate::descriptor::VECTOR_ARRAY_OID);
			let types = [
				(16, "bool", 1, true, 'b', 'B', 0, 1000),
				(17, "bytea", -1, false, 'b', 'U', 0, 1001),
				(18, "char", 1, true, 'b', 'S', 0, 1002),
				(19, "name", 64, false, 'b', 'S', 0, 1003),
				(20, "int8", 8, true, 'b', 'N', 0, 1016),
				(21, "int2", 2, true, 'b', 'N', 0, 1005),
				(23, "int4", 4, true, 'b', 'N', 0, 1007),
				(24, "regproc", 4, true, 'b', 'N', 0, 1008),
				(25, "text", -1, false, 'b', 'S', 0, 1009),
				(26, "oid", 4, true, 'b', 'N', 0, 1028),
				(27, "tid", 6, false, 'b', 'U', 0, 1010),
				(28, "xid", 4, true, 'b', 'U', 0, 1011),
				(29, "cid", 4, true, 'b', 'U', 0, 1012),
				(114, "json", -1, false, 'b', 'U', 0, 199),
				(650, "cidr", -1, false, 'b', 'I', 0, 651),
				(700, "float4", 4, true, 'b', 'N', 0, 1021),
				(701, "float8", 8, true, 'b', 'N', 0, 1022),
				(705, "unknown", -2, false, 'p', 'X', 0, 0),
				(829, "macaddr", 6, false, 'b', 'U', 0, 1040),
				(869, "inet", -1, false, 'b', 'I', 0, 1041),
				(1042, "bpchar", -1, false, 'b', 'S', 0, 1014),
				(1043, "varchar", -1, false, 'b', 'S', 0, 1015),
				(1082, "date", 4, true, 'b', 'D', 0, 1182),
				(1083, "time", 8, true, 'b', 'D', 0, 1183),
				(1114, "timestamp", 8, true, 'b', 'D', 0, 1115),
				(1184, "timestamptz", 8, true, 'b', 'D', 0, 1185),
				(1186, "interval", 16, false, 'b', 'T', 0, 1187),
				(1266, "timetz", 12, false, 'b', 'D', 0, 1270),
				(1700, "numeric", -1, false, 'b', 'N', 0, 1231),
				(2205, "regclass", 4, true, 'b', 'N', 0, 2210),
				(2249, "record", -1, false, 'p', 'P', 0, 2287),
				(2278, "void", 4, true, 'p', 'P', 0, 0),
				(2950, "uuid", 16, false, 'b', 'U', 0, 2951),
				(3802, "jsonb", -1, false, 'b', 'U', 0, 3807),
				(3904, "int4range", -1, false, 'r', 'R', 23, 3905),
				(3906, "numrange", -1, false, 'r', 'R', 1700, 3907),
				(3908, "tsrange", -1, false, 'r', 'R', 1114, 3909),
				(3910, "tstzrange", -1, false, 'r', 'R', 1184, 3911),
				(3912, "daterange", -1, false, 'r', 'R', 1082, 3913),
				(3926, "int8range", -1, false, 'r', 'R', 20, 3927),
				// The `vector` type of the pgvector extension, which clients find by its name.
				(vector_oid, "vector", -1, false, 'b', 'U', 0, vector_array_oid),
				// The types the wire layer names with an OID of their own, each with the array
				// type of it.
				(790, "money", 8, true, 'b', 'N', 0, 791),
				(1560, "bit", -1, false, 'b', 'V', 0, 1561),
				(1562, "varbit", -1, false, 'b', 'V', 0, 1563),
				(142, "xml", -1, false, 'b', 'U', 0, 143),
				(3614, "tsvector", -1, false, 'b', 'U', 0, 3643),
				(3615, "tsquery", -1, false, 'b', 'U', 0, 3645),
				(4072, "jsonpath", -1, false, 'b', 'U', 0, 4073),
				(774, "macaddr8", 8, false, 'b', 'U', 0, 775),
				(2202, "regprocedure", 4, true, 'b', 'N', 0, 2207),
				(2203, "regoper", 4, true, 'b', 'N', 0, 2208),
				(2204, "regoperator", 4, true, 'b', 'N', 0, 2209),
				(2206, "regtype", 4, true, 'b', 'N', 0, 2211),
				(4089, "regnamespace", 4, true, 'b', 'N', 0, 4090),
				(4096, "regrole", 4, true, 'b', 'N', 0, 4097),
				(3734, "regconfig", 4, true, 'b', 'N', 0, 3735),
				(3769, "regdictionary", 4, true, 'b', 'N', 0, 3770),
				(4191, "regcollation", 4, true, 'b', 'N', 0, 4192),
				(5069, "xid8", 8, true, 'b', 'U', 0, 271),
				(3220, "pg_lsn", 8, true, 'b', 'U', 0, 3221),
				(5038, "pg_snapshot", -1, false, 'b', 'U', 0, 5039),
				(2970, "txid_snapshot", -1, false, 'b', 'U', 0, 2949),
				(600, "point", 16, false, 'b', 'G', 0, 1017),
				(601, "lseg", 32, false, 'b', 'G', 0, 1018),
				(602, "path", -1, false, 'b', 'G', 0, 1019),
				(603, "box", 32, false, 'b', 'G', 0, 1020),
				(604, "polygon", -1, false, 'b', 'G', 0, 1027),
				(628, "line", 24, false, 'b', 'G', 0, 629),
				(718, "circle", 24, false, 'b', 'G', 0, 719),
				(4451, "int4multirange", -1, false, 'm', 'R', 0, 6150),
				(4532, "nummultirange", -1, false, 'm', 'R', 0, 6151),
				(4533, "tsmultirange", -1, false, 'm', 'R', 0, 6152),
				(4534, "tstzmultirange", -1, false, 'm', 'R', 0, 6153),
				(4535, "datemultirange", -1, false, 'm', 'R', 0, 6155),
				(4536, "int8multirange", -1, false, 'm', 'R', 0, 6157),
				// Array types
				(1000, "_bool", -1, false, 'b', 'A', 16, 0),
				(1001, "_bytea", -1, false, 'b', 'A', 17, 0),
				(1002, "_char", -1, false, 'b', 'A', 18, 0),
				(1003, "_name", -1, false, 'b', 'A', 19, 0),
				(1005, "_int2", -1, false, 'b', 'A', 21, 0),
				(1007, "_int4", -1, false, 'b', 'A', 23, 0),
				(1008, "_regproc", -1, false, 'b', 'A', 24, 0),
				(1009, "_text", -1, false, 'b', 'A', 25, 0),
				(1014, "_bpchar", -1, false, 'b', 'A', 1042, 0),
				(1015, "_varchar", -1, false, 'b', 'A', 1043, 0),
				(1016, "_int8", -1, false, 'b', 'A', 20, 0),
				(1021, "_float4", -1, false, 'b', 'A', 700, 0),
				(1022, "_float8", -1, false, 'b', 'A', 701, 0),
				(1028, "_oid", -1, false, 'b', 'A', 26, 0),
				(1182, "_date", -1, false, 'b', 'A', 1082, 0),
				(1183, "_time", -1, false, 'b', 'A', 1083, 0),
				(1115, "_timestamp", -1, false, 'b', 'A', 1114, 0),
				(1185, "_timestamptz", -1, false, 'b', 'A', 1184, 0),
				(1231, "_numeric", -1, false, 'b', 'A', 1700, 0),
				(2951, "_uuid", -1, false, 'b', 'A', 2950, 0),
				(3807, "_jsonb", -1, false, 'b', 'A', 3802, 0),
				(vector_array_oid, "_vector", -1, false, 'b', 'A', vector_oid, 0),
				(1010, "_tid", -1, false, 'b', 'A', 27, 0),
				(1011, "_xid", -1, false, 'b', 'A', 28, 0),
				(1012, "_cid", -1, false, 'b', 'A', 29, 0),
				(199, "_json", -1, false, 'b', 'A', 114, 0),
				(651, "_cidr", -1, false, 'b', 'A', 650, 0),
				(1040, "_macaddr", -1, false, 'b', 'A', 829, 0),
				(1041, "_inet", -1, false, 'b', 'A', 869, 0),
				(1187, "_interval", -1, false, 'b', 'A', 1186, 0),
				(1270, "_timetz", -1, false, 'b', 'A', 1266, 0),
				(2210, "_regclass", -1, false, 'b', 'A', 2205, 0),
				(2287, "_record", -1, false, 'p', 'P', 2249, 0),
				(3905, "_int4range", -1, false, 'b', 'A', 3904, 0),
				(3907, "_numrange", -1, false, 'b', 'A', 3906, 0),
				(3909, "_tsrange", -1, false, 'b', 'A', 3908, 0),
				(3911, "_tstzrange", -1, false, 'b', 'A', 3910, 0),
				(3913, "_daterange", -1, false, 'b', 'A', 3912, 0),
				(3927, "_int8range", -1, false, 'b', 'A', 3926, 0),
				(791, "_money", -1, false, 'b', 'A', 790, 0),
				(1561, "_bit", -1, false, 'b', 'A', 1560, 0),
				(1563, "_varbit", -1, false, 'b', 'A', 1562, 0),
				(143, "_xml", -1, false, 'b', 'A', 142, 0),
				(3643, "_tsvector", -1, false, 'b', 'A', 3614, 0),
				(3645, "_tsquery", -1, false, 'b', 'A', 3615, 0),
				(4073, "_jsonpath", -1, false, 'b', 'A', 4072, 0),
				(775, "_macaddr8", -1, false, 'b', 'A', 774, 0),
				(2207, "_regprocedure", -1, false, 'b', 'A', 2202, 0),
				(2208, "_regoper", -1, false, 'b', 'A', 2203, 0),
				(2209, "_regoperator", -1, false, 'b', 'A', 2204, 0),
				(2211, "_regtype", -1, false, 'b', 'A', 2206, 0),
				(4090, "_regnamespace", -1, false, 'b', 'A', 4089, 0),
				(4097, "_regrole", -1, false, 'b', 'A', 4096, 0),
				(3735, "_regconfig", -1, false, 'b', 'A', 3734, 0),
				(3770, "_regdictionary", -1, false, 'b', 'A', 3769, 0),
				(4192, "_regcollation", -1, false, 'b', 'A', 4191, 0),
				(271, "_xid8", -1, false, 'b', 'A', 5069, 0),
				(3221, "_pg_lsn", -1, false, 'b', 'A', 3220, 0),
				(5039, "_pg_snapshot", -1, false, 'b', 'A', 5038, 0),
				(2949, "_txid_snapshot", -1, false, 'b', 'A', 2970, 0),
				(1017, "_point", -1, false, 'b', 'A', 600, 0),
				(1018, "_lseg", -1, false, 'b', 'A', 601, 0),
				(1019, "_path", -1, false, 'b', 'A', 602, 0),
				(1020, "_box", -1, false, 'b', 'A', 603, 0),
				(1027, "_polygon", -1, false, 'b', 'A', 604, 0),
				(629, "_line", -1, false, 'b', 'A', 628, 0),
				(719, "_circle", -1, false, 'b', 'A', 718, 0),
				(6150, "_int4multirange", -1, false, 'b', 'A', 4451, 0),
				(6151, "_nummultirange", -1, false, 'b', 'A', 4532, 0),
				(6152, "_tsmultirange", -1, false, 'b', 'A', 4533, 0),
				(6153, "_tstzmultirange", -1, false, 'b', 'A', 4534, 0),
				(6155, "_datemultirange", -1, false, 'b', 'A', 4535, 0),
				(6157, "_int8multirange", -1, false, 'b', 'A', 4536, 0),
			];

			let rows = types
				.iter()
				.map(|&(oid, name, len, byval, typtype, cat, elem, arr)| {
					row(vec![
						("oid", int_val(oid)),
						("typname", str_val(name)),
						(
							"typnamespace",
							// The vector type is installed in the `public` schema.
							int_val(if oid == vector_oid || oid == vector_array_oid {
								2200
							} else {
								11
							}),
						),
						("typowner", int_val(10)),
						("typlen", int_val(len)),
						("typbyval", bool_val(byval)),
						("typtype", str_val(typtype.to_string())),
						("typcategory", str_val(cat.to_string())),
						("typispreferred", bool_val(oid == 23 || oid == 701 || oid == 25)),
						("typisdefined", bool_val(true)),
						("typdelim", str_val(",")),
						("typrelid", int_val(0)),
						("typelem", int_val(elem)),
						("typarray", int_val(arr)),
						("typinput", str_val(format!("{name}in"))),
						("typoutput", str_val(format!("{name}out"))),
						("typreceive", str_val(format!("{name}recv"))),
						("typsend", str_val(format!("{name}send"))),
						("typalign", str_val("i")),
						("typstorage", str_val("p")),
						("typnotnull", bool_val(false)),
						("typbasetype", int_val(0)),
						("typtypmod", int_val(-1)),
						("typndims", int_val(0)),
						("typcollation", int_val(0)),
						("typdefaultbin", null_val()),
						("typdefault", null_val()),
						("typacl", null_val()),
					])
				})
				.chain(user_enums.iter().flat_map(|en| {
					let oid = i64::from(crate::descriptor::enum_type_oid(en.id));
					let array_oid = i64::from(crate::descriptor::enum_array_type_oid(en.id));
					let enum_row = |oid: i64, name: String, array: bool| {
						row(vec![
							("oid", int_val(oid)),
							("typname", str_val(&name)),
							("typnamespace", int_val(2200)),
							("typowner", int_val(10)),
							(
								"typlen",
								int_val(if array {
									-1
								} else {
									4
								}),
							),
							("typbyval", bool_val(!array)),
							(
								"typtype",
								str_val(if array {
									"b"
								} else {
									"e"
								}),
							),
							(
								"typcategory",
								str_val(if array {
									"A"
								} else {
									"E"
								}),
							),
							("typispreferred", bool_val(false)),
							("typisdefined", bool_val(true)),
							("typdelim", str_val(",")),
							("typrelid", int_val(0)),
							(
								"typelem",
								int_val(if array {
									i64::from(array_elem(oid))
								} else {
									0
								}),
							),
							(
								"typarray",
								int_val(if array {
									0
								} else {
									array_oid
								}),
							),
							(
								"typinput",
								str_val(if array {
									"array_in"
								} else {
									"enum_in"
								}),
							),
							(
								"typoutput",
								str_val(if array {
									"array_out"
								} else {
									"enum_out"
								}),
							),
							(
								"typreceive",
								str_val(if array {
									"array_recv"
								} else {
									"enum_recv"
								}),
							),
							(
								"typsend",
								str_val(if array {
									"array_send"
								} else {
									"enum_send"
								}),
							),
							("typalign", str_val("i")),
							("typstorage", str_val("p")),
							("typnotnull", bool_val(false)),
							("typbasetype", int_val(0)),
							("typtypmod", int_val(-1)),
							("typndims", int_val(0)),
							("typcollation", int_val(0)),
							("typdefaultbin", null_val()),
							("typdefault", null_val()),
							("typacl", null_val()),
						])
					};
					[
						enum_row(oid, en.name.clone(), false),
						enum_row(array_oid, format!("_{}", en.name), true),
					]
				}))
				.chain(user_tables.iter().enumerate().map(|(position, table)| {
					row(vec![
						("oid", int_val(TABLE_TYPE_OID_BASE + position as i64)),
						("typname", str_val(&table.name)),
						("typnamespace", int_val(2200)),
						("typowner", int_val(10)),
						("typlen", int_val(-1)),
						("typbyval", bool_val(false)),
						("typtype", str_val("c")),
						("typcategory", str_val("C")),
						("typispreferred", bool_val(false)),
						("typisdefined", bool_val(true)),
						("typdelim", str_val(",")),
						("typrelid", int_val(16384 + position as i64)),
						("typelem", int_val(0)),
						("typarray", int_val(0)),
						("typinput", str_val("record_in")),
						("typoutput", str_val("record_out")),
						("typreceive", str_val("record_recv")),
						("typsend", str_val("record_send")),
						("typalign", str_val("d")),
						("typstorage", str_val("x")),
						("typnotnull", bool_val(false)),
						("typbasetype", int_val(0)),
						("typtypmod", int_val(-1)),
						("typndims", int_val(0)),
						("typcollation", int_val(0)),
						("typdefaultbin", null_val()),
						("typdefault", null_val()),
						("typacl", null_val()),
					])
				}))
				.chain(domains.iter().map(|domain| {
					let domain_base = DomainBase::of(domain, user_enums, domains, composites);
					let base = domain_base.oid;
					let base_typmod = domain_base
						.reported
						.as_ref()
						.map_or(-1, |reported| i64::from(reported.typmod));
					let base_len = attribute_len(base);
					row(vec![
						("oid", int_val(i64::from(domain_type_oid(domain.id)))),
						("typname", str_val(&domain.name)),
						("typnamespace", int_val(2200)),
						("typowner", int_val(10)),
						("typlen", int_val(base_len)),
						("typbyval", bool_val(matches!(base_len, 1 | 2 | 4 | 8))),
						("typtype", str_val("d")),
						("typcategory", str_val(type_category(base).to_string())),
						("typispreferred", bool_val(false)),
						("typisdefined", bool_val(true)),
						("typdelim", str_val(",")),
						("typrelid", int_val(0)),
						("typelem", int_val(0)),
						("typarray", int_val(0)),
						("typinput", str_val("domain_in")),
						("typoutput", str_val("domain_out")),
						("typreceive", str_val("domain_recv")),
						("typsend", str_val("domain_send")),
						("typalign", str_val("i")),
						("typstorage", str_val("p")),
						("typnotnull", bool_val(domain.not_null)),
						("typbasetype", int_val(base)),
						("typtypmod", int_val(base_typmod)),
						("typndims", int_val(0)),
						("typcollation", int_val(0)),
						("typdefaultbin", null_val()),
						("typdefault", null_val()),
						("typacl", null_val()),
					])
				}))
				.chain(composites.iter().map(|composite| {
					row(vec![
						("oid", int_val(i64::from(composite_type_oid(composite.id)))),
						("typname", str_val(&composite.name)),
						("typnamespace", int_val(2200)),
						("typowner", int_val(10)),
						("typlen", int_val(-1)),
						("typbyval", bool_val(false)),
						("typtype", str_val("c")),
						("typcategory", str_val("C")),
						("typispreferred", bool_val(false)),
						("typisdefined", bool_val(true)),
						("typdelim", str_val(",")),
						("typrelid", int_val(i64::from(composite_relation_oid(composite.id)))),
						("typelem", int_val(0)),
						("typarray", int_val(0)),
						("typinput", str_val("record_in")),
						("typoutput", str_val("record_out")),
						("typreceive", str_val("record_recv")),
						("typsend", str_val("record_send")),
						("typalign", str_val("d")),
						("typstorage", str_val("x")),
						("typnotnull", bool_val(false)),
						("typbasetype", int_val(0)),
						("typtypmod", int_val(-1)),
						("typndims", int_val(0)),
						("typcollation", int_val(0)),
						("typdefaultbin", null_val()),
						("typdefault", null_val()),
						("typacl", null_val()),
					])
				}))
				.collect();
			Some(rows)
		}

		(Some("pg_catalog") | None, "pg_class") => {
			let mut rows = Vec::new();

			// Built-in pg_catalog relations
			let pg_catalog_tables = [
				(1247, "pg_type", 11, 'r', 28),
				(1259, "pg_class", 11, 'r', 33),
				(2615, "pg_namespace", 11, 'r', 4),
				(1249, "pg_attribute", 11, 'r', 25),
				(2601, "pg_am", 11, 'r', 4),
				(1262, "pg_database", 11, 'r', 14),
				(1255, "pg_proc", 11, 'r', 30),
				(2610, "pg_index", 11, 'r', 20),
				(2616, "pg_settings", 11, 'r', 17),
				(2842, "pg_roles", 11, 'r', 13),
				(2843, "pg_user", 11, 'r', 9),
				(3541, "pg_range", 11, 'r', 7),
				(3501, "pg_enum", 11, 'r', 4),
				(2609, "pg_description", 11, 'r', 4),
				(2606, "pg_constraint", 11, 'r', 25),
				(2328, "pg_foreign_data_wrapper", 11, 'r', 7),
				(1417, "pg_foreign_server", 11, 'r', 8),
				(3456, "pg_collation", 11, 'r', 11),
			];
			// The relations the catalog serves besides, numbered after the information-schema
			// views.
			let served = [
				(11101, "pg_sequences", 'v'),
				(11102, "pg_attrdef", 'r'),
				(11103, "pg_opclass", 'r'),
				(11104, "pg_policies", 'v'),
				(11105, "pg_stat_user_indexes", 'v'),
				(11106, "pg_views", 'v'),
				(11107, "pg_extension", 'r'),
				(11108, "pg_language", 'r'),
				(11109, "pg_matviews", 'v'),
			]
			.map(|(oid, name, kind)| {
				let natts = get_system_table_schema(Some("pg_catalog"), name)
					.map_or(0, |table| table.columns.len() as i64);
				(oid, name, 11, kind, natts)
			});
			let pg_catalog_tables = pg_catalog_tables.into_iter().chain(served);

			for (oid, tname, nsp, kind, natts) in pg_catalog_tables {
				rows.push(row(vec![
					("oid", int_val(oid)),
					("relname", str_val(tname)),
					("relnamespace", int_val(nsp)),
					("reltype", int_val(0)),
					("reloftype", int_val(0)),
					("relowner", int_val(10)),
					("relam", int_val(0)),
					("relfilenode", int_val(0)),
					("reltablespace", int_val(0)),
					("relpages", int_val(0)),
					("reltuples", float_val(0.0)),
					("relallvisible", int_val(0)),
					("reltoastrelid", int_val(0)),
					("relhasindex", bool_val(false)),
					("relisshared", bool_val(false)),
					("relpersistence", str_val("p")),
					("relkind", str_val(kind.to_string())),
					("relnatts", int_val(natts)),
					("relchecks", int_val(0)),
					("relhasrules", bool_val(false)),
					("relhastriggers", bool_val(false)),
					("relhassubclass", bool_val(false)),
					("relrowsecurity", bool_val(false)),
					("relforcerowsecurity", bool_val(false)),
					("relispopulated", bool_val(true)),
					("relreplident", str_val("d")),
					("relispartitioned", bool_val(false)),
					("relispartition", bool_val(false)),
					("relrewrite", int_val(0)),
					("relfrozenxid", int_val(0)),
					("relminmxid", int_val(0)),
					("relacl", null_val()),
					("reloptions", null_val()),
					("relpartbound", null_val()),
				]));
			}

			// Built-in information_schema views
			let attribute_count = |name: &str| {
				get_system_table_schema(Some("information_schema"), name)
					.map_or(0, |table| table.columns.len() as i64)
			};
			let info_schema_views = [
				(11001, "tables", 11000, 'v', 12),
				(11002, "columns", 11000, 'v', 44),
				(11003, "schemata", 11000, 'v', 7),
				(11004, "views", 11000, 'v', 10),
				(11005, "sequences", 11000, 'v', attribute_count("sequences")),
				(11006, "table_constraints", 11000, 'v', attribute_count("table_constraints")),
				(
					11007,
					"constraint_column_usage",
					11000,
					'v',
					attribute_count("constraint_column_usage"),
				),
			];

			for (oid, tname, nsp, kind, natts) in info_schema_views {
				rows.push(row(vec![
					("oid", int_val(oid)),
					("relname", str_val(tname)),
					("relnamespace", int_val(nsp)),
					("reltype", int_val(0)),
					("reloftype", int_val(0)),
					("relowner", int_val(10)),
					("relam", int_val(0)),
					("relfilenode", int_val(0)),
					("reltablespace", int_val(0)),
					("relpages", int_val(0)),
					("reltuples", float_val(0.0)),
					("relallvisible", int_val(0)),
					("reltoastrelid", int_val(0)),
					("relhasindex", bool_val(false)),
					("relisshared", bool_val(false)),
					("relpersistence", str_val("p")),
					("relkind", str_val(kind.to_string())),
					("relnatts", int_val(natts)),
					("relchecks", int_val(0)),
					("relhasrules", bool_val(false)),
					("relhastriggers", bool_val(false)),
					("relhassubclass", bool_val(false)),
					("relrowsecurity", bool_val(false)),
					("relforcerowsecurity", bool_val(false)),
					("relispopulated", bool_val(true)),
					("relreplident", str_val("d")),
					("relispartitioned", bool_val(false)),
					("relispartition", bool_val(false)),
					("relrewrite", int_val(0)),
					("relfrozenxid", int_val(0)),
					("relminmxid", int_val(0)),
					("relacl", null_val()),
					("reloptions", null_val()),
					("relpartbound", null_val()),
				]));
			}

			let indexes = user_indexes(user_tables);

			// Dynamic user tables
			for (idx, ut) in user_tables.iter().enumerate() {
				let oid = 16384 + idx as i64;
				rows.push(row(vec![
					("oid", int_val(oid)),
					("relname", str_val(&ut.name)),
					("relnamespace", int_val(2200)), // public
					("reltype", int_val(TABLE_TYPE_OID_BASE + idx as i64)),
					("reloftype", int_val(0)),
					("relowner", int_val(10)),
					("relam", int_val(0)),
					("relfilenode", int_val(0)),
					("reltablespace", int_val(0)),
					("relpages", int_val(0)),
					("reltuples", float_val(0.0)),
					("relallvisible", int_val(0)),
					("reltoastrelid", int_val(0)),
					("relhasindex", bool_val(indexes.iter().any(|index| index.table_oid == oid))),
					("relisshared", bool_val(false)),
					("relpersistence", str_val("p")),
					("relkind", str_val("r")),
					("relnatts", int_val(ut.columns.len() as i64)),
					("relchecks", int_val(0)),
					("relhasrules", bool_val(false)),
					("relhastriggers", bool_val(false)),
					("relhassubclass", bool_val(false)),
					("relrowsecurity", bool_val(false)),
					("relforcerowsecurity", bool_val(false)),
					("relispopulated", bool_val(true)),
					("relreplident", str_val("d")),
					("relispartitioned", bool_val(false)),
					("relispartition", bool_val(false)),
					("relrewrite", int_val(0)),
					("relfrozenxid", int_val(0)),
					("relminmxid", int_val(0)),
					("relacl", null_val()),
					("reloptions", null_val()),
					("relpartbound", null_val()),
				]));
			}

			// The relation of each composite type, which its `pg_type.typrelid` names.
			for composite in composites {
				rows.push(row(vec![
					("oid", int_val(i64::from(composite_relation_oid(composite.id)))),
					("relname", str_val(&composite.name)),
					("relnamespace", int_val(2200)),
					("reltype", int_val(i64::from(composite_type_oid(composite.id)))),
					("reloftype", int_val(0)),
					("relowner", int_val(10)),
					("relam", int_val(0)),
					("relfilenode", int_val(0)),
					("reltablespace", int_val(0)),
					("relpages", int_val(0)),
					("reltuples", float_val(0.0)),
					("relallvisible", int_val(0)),
					("reltoastrelid", int_val(0)),
					("relhasindex", bool_val(false)),
					("relisshared", bool_val(false)),
					("relpersistence", str_val("p")),
					("relkind", str_val("c")),
					("relnatts", int_val(composite.fields.len() as i64)),
					("relchecks", int_val(0)),
					("relhasrules", bool_val(false)),
					("relhastriggers", bool_val(false)),
					("relhassubclass", bool_val(false)),
					("relrowsecurity", bool_val(false)),
					("relforcerowsecurity", bool_val(false)),
					("relispopulated", bool_val(true)),
					("relreplident", str_val("n")),
					("relispartitioned", bool_val(false)),
					("relispartition", bool_val(false)),
					("relrewrite", int_val(0)),
					("relfrozenxid", int_val(0)),
					("relminmxid", int_val(0)),
					("relacl", null_val()),
					("reloptions", null_val()),
					("relpartbound", null_val()),
				]));
			}

			// Index relations of the user tables
			for index in &indexes {
				rows.push(row(vec![
					("oid", int_val(index.oid)),
					("relname", str_val(&index.name)),
					("relnamespace", int_val(2200)),
					("reltype", int_val(0)),
					("reloftype", int_val(0)),
					("relowner", int_val(10)),
					("relam", int_val(403)),
					("relfilenode", int_val(0)),
					("reltablespace", int_val(0)),
					("relpages", int_val(0)),
					("reltuples", float_val(0.0)),
					("relallvisible", int_val(0)),
					("reltoastrelid", int_val(0)),
					("relhasindex", bool_val(false)),
					("relisshared", bool_val(false)),
					("relpersistence", str_val("p")),
					("relkind", str_val("i")),
					("relnatts", int_val(index.attnums.len() as i64)),
					("relchecks", int_val(0)),
					("relhasrules", bool_val(false)),
					("relhastriggers", bool_val(false)),
					("relhassubclass", bool_val(false)),
					("relrowsecurity", bool_val(false)),
					("relforcerowsecurity", bool_val(false)),
					("relispopulated", bool_val(true)),
					("relreplident", str_val("n")),
					("relispartitioned", bool_val(false)),
					("relispartition", bool_val(false)),
					("relrewrite", int_val(0)),
					("relfrozenxid", int_val(0)),
					("relminmxid", int_val(0)),
					("relacl", null_val()),
					("reloptions", null_val()),
					("relpartbound", null_val()),
				]));
			}

			// The sequence relations of the serial columns, as `pg_sequences` lists them.
			let sequences = user_tables.iter().flat_map(|table| {
				table.columns.iter().filter_map(|column| column.sequence.as_ref())
			});
			for (position, sequence) in sequences.enumerate() {
				rows.push(row(vec![
					("oid", int_val(SEQUENCE_OID_BASE + position as i64)),
					("relname", str_val(sequence)),
					("relnamespace", int_val(2200)),
					("reltype", int_val(0)),
					("reloftype", int_val(0)),
					("relowner", int_val(10)),
					("relam", int_val(0)),
					("relfilenode", int_val(0)),
					("reltablespace", int_val(0)),
					("relpages", int_val(0)),
					("reltuples", float_val(0.0)),
					("relallvisible", int_val(0)),
					("reltoastrelid", int_val(0)),
					("relhasindex", bool_val(false)),
					("relisshared", bool_val(false)),
					("relpersistence", str_val("p")),
					("relkind", str_val("S")),
					("relnatts", int_val(3)),
					("relchecks", int_val(0)),
					("relhasrules", bool_val(false)),
					("relhastriggers", bool_val(false)),
					("relhassubclass", bool_val(false)),
					("relrowsecurity", bool_val(false)),
					("relforcerowsecurity", bool_val(false)),
					("relispopulated", bool_val(true)),
					("relreplident", str_val("n")),
					("relispartitioned", bool_val(false)),
					("relispartition", bool_val(false)),
					("relrewrite", int_val(0)),
					("relfrozenxid", int_val(0)),
					("relminmxid", int_val(0)),
					("relacl", null_val()),
					("reloptions", null_val()),
					("relpartbound", null_val()),
				]));
			}

			Some(rows)
		}

		(Some("pg_catalog") | None, "pg_attribute") => {
			let mut rows = Vec::new();

			// Populate attributes for dynamic user tables
			for (t_idx, ut) in user_tables.iter().enumerate() {
				let relid = 16384 + t_idx as i64;
				for (col_idx, col) in ut.columns.iter().enumerate() {
					// A column declared with a SQL type has that type and its modifier.
					let reported = col.declared.as_ref().and_then(pg_reported_type);
					// A domain column reports the domain's OID here, and its base type's length.
					let base_oid = match &reported {
						Some(reported) => i64::from(reported.oid),
						None => attribute_type_oid(col.data_type.as_deref().unwrap_or("text")),
					};
					let atttypid = enum_attribute_oid(col.enum_held.as_ref(), user_enums)
						.or_else(|| domain_attribute_oid(col.domain.as_deref(), domains))
						.or_else(|| composite_attribute_oid(col.composite.as_deref(), composites))
						.unwrap_or(base_oid);
					let atttypmod = match &reported {
						Some(reported) if atttypid == base_oid => i64::from(reported.typmod),
						Some(_) | None => -1,
					};
					let attlen = attribute_len(base_oid);
					// PostgreSQL records the declared dimensions of an array column, which Drizzle
					// reads to tell `text[]` from `text`.
					let attndims = match (&reported, &col.declared) {
						(Some(_), Some(declared)) => {
							i64::try_from(declared.array_dimensions().len()).unwrap_or(1)
						}
						(Some(_), None) | (None, _) => i64::from(matches!(
							crate::SqlType::from_name(col.data_type.as_deref().unwrap_or("text")),
							crate::SqlType::Array(_)
						)),
					};
					rows.push(row(vec![
						("attrelid", int_val(relid)),
						("attname", str_val(&col.name)),
						("atttypid", int_val(atttypid)),
						("attstattarget", int_val(-1)),
						("attlen", int_val(attlen)),
						("attnum", int_val((col_idx + 1) as i64)),
						("attndims", int_val(attndims)),
						("attcacheoff", int_val(-1)),
						("atttypmod", int_val(atttypmod)),
						("attbyval", bool_val(attlen > 0)),
						("attstorage", str_val("p")),
						("attalign", str_val("i")),
						("attnotnull", bool_val(!col.is_nullable)),
						("atthasdef", bool_val(col.sequence.is_some())),
						("atthasmissing", bool_val(false)),
						("attidentity", str_val("")),
						("attgenerated", str_val("")),
						("attisdropped", bool_val(false)),
						("attislocal", bool_val(true)),
						("attinhcount", int_val(0)),
						("attcollation", int_val(0)),
						("attacl", null_val()),
						("attoptions", null_val()),
						("attfdwoptions", null_val()),
						("attmissingval", null_val()),
					]));
				}
			}

			// The fields of each composite type, as the attributes of its relation.
			for composite in composites {
				let relid = i64::from(composite_relation_oid(composite.id));
				for (index, (field, kind)) in composite.fields.iter().enumerate() {
					// An attribute declared with a SQL type has that type and its modifier; an
					// attribute of an enum, a domain or a composite type has the OID of that type.
					let reported = composite.declared_type(field).and_then(pg_reported_type);
					let base_oid = match &reported {
						Some(reported) => i64::from(reported.oid),
						None => {
							attribute_type_oid(kind_type_name(kind).as_deref().unwrap_or("text"))
						}
					};
					let named = attribute_named_type(kind);
					let atttypid = named_enum_oid(named, user_enums)
						.or_else(|| domain_attribute_oid(named, domains))
						.or_else(|| composite_attribute_oid(named, composites))
						.unwrap_or(base_oid);
					let atttypmod = match &reported {
						Some(reported) if atttypid == base_oid => i64::from(reported.typmod),
						Some(_) | None => -1,
					};
					let attlen = attribute_len(base_oid);
					rows.push(row(vec![
						("attrelid", int_val(relid)),
						("attname", str_val(field)),
						("atttypid", int_val(atttypid)),
						("attstattarget", int_val(-1)),
						("attlen", int_val(attlen)),
						("attnum", int_val((index + 1) as i64)),
						("attndims", int_val(0)),
						("attcacheoff", int_val(-1)),
						("atttypmod", int_val(atttypmod)),
						("attbyval", bool_val(attlen > 0)),
						("attstorage", str_val("p")),
						("attalign", str_val("i")),
						("attnotnull", bool_val(false)),
						("atthasdef", bool_val(false)),
						("atthasmissing", bool_val(false)),
						("attidentity", str_val("")),
						("attgenerated", str_val("")),
						("attisdropped", bool_val(false)),
						("attislocal", bool_val(true)),
						("attinhcount", int_val(0)),
						("attcollation", int_val(0)),
						("attacl", null_val()),
						("attoptions", null_val()),
						("attfdwoptions", null_val()),
						("attmissingval", null_val()),
					]));
				}
			}

			Some(rows)
		}

		(Some("pg_catalog") | None, "pg_settings") => {
			let settings = [
				("server_version", "16.0", "Preset", "PostgreSQL version"),
				("server_encoding", "UTF8", "Preset", "Server character set"),
				("client_encoding", "UTF8", "Client", "Client character set"),
				("application_name", "", "Client", "Client application name"),
				("TimeZone", "UTC", "Client", "Session timezone"),
				("DateStyle", "ISO, MDY", "Client", "Date display style"),
				("standard_conforming_strings", "on", "Client", "Literal backslash interpretation"),
				("integer_datetimes", "on", "Preset", "64-bit integer datetimes"),
				("extra_float_digits", "1", "Client", "Extra precision for floating point numbers"),
				("bytea_output", "hex", "Client", "Bytea output format"),
				("search_path", "\"$user\", public", "Client", "Schema search path"),
				("default_transaction_read_only", "off", "Client", "Read-only transaction mode"),
				(
					"transaction_isolation",
					"read committed",
					"Client",
					"Transaction isolation level",
				),
				("max_connections", "100", "Preset", "Maximum client connections"),
				("block_size", "8192", "Preset", "Disk block size in bytes"),
				("segment_size", "131072", "Preset", "Disk segment size in blocks"),
				("wal_block_size", "8192", "Preset", "WAL disk block size in bytes"),
				("session_replication_role", "origin", "Client", "Replication trigger role"),
				("check_function_bodies", "on", "Client", "Function body validation"),
				("default_tablespace", "", "Client", "Default tablespace"),
			];

			let rows = settings
				.iter()
				.map(|&(name, val, cat, desc)| {
					row(vec![
						("name", str_val(name)),
						("setting", str_val(val)),
						("unit", null_val()),
						("category", str_val(cat)),
						("short_desc", str_val(desc)),
						("extra_desc", null_val()),
						("context", str_val("user")),
						("vartype", str_val("string")),
						("source", str_val("default")),
						("min_val", null_val()),
						("max_val", null_val()),
						("enumvals", null_val()),
						("boot_val", str_val(val)),
						("reset_val", str_val(val)),
						("sourcefile", null_val()),
						("sourceline", null_val()),
						("pending_restart", bool_val(false)),
					])
				})
				.collect();
			Some(rows)
		}

		(Some("pg_catalog") | None, "pg_proc") => {
			let procs = [
				(10001, "version", 25, 0),
				(10002, "current_schema", 25, 0),
				(10003, "current_schemas", 1009, 1),
				(10004, "current_database", 25, 0),
				(10005, "current_setting", 25, 1),
				(10006, "current_user", 25, 0),
				(10007, "session_user", 25, 0),
				(10008, "user", 25, 0),
				(10009, "pg_backend_pid", 23, 0),
				(10010, "pg_is_in_recovery", 16, 0),
				(10011, "pg_typeof", 25, 1),
				(10012, "pg_get_userbyid", 25, 1),
				(10013, "format_type", 25, 2),
				(10014, "pg_table_is_visible", 16, 1),
				(10015, "pg_type_is_visible", 16, 1),
				(10016, "pg_function_is_visible", 16, 1),
				(10017, "obj_description", 25, 2),
				(10018, "col_description", 25, 2),
				(10019, "shobj_description", 25, 2),
				(10020, "pg_get_expr", 25, 2),
				(10021, "pg_relation_size", 20, 1),
				(10022, "pg_total_relation_size", 20, 1),
				(10023, "has_schema_privilege", 16, 2),
				(10024, "has_table_privilege", 16, 2),
				(10025, "has_database_privilege", 16, 2),
				(10026, "has_column_privilege", 16, 3),
				(2147, "count", 20, 1),
				(2108, "sum", 1700, 1),
				(2101, "avg", 1700, 1),
				(2132, "min", 2283, 1),
				(2116, "max", 2283, 1),
				(870, "lower", 25, 1),
				(871, "upper", 25, 1),
				(1381, "length", 23, 1),
				(3058, "concat", 25, 1),
				(2111, "coalesce", 2283, 2),
				(1299, "now", 1184, 0),
			];

			let rows = procs
				.iter()
				.map(|&(oid, name, rettype, nargs)| {
					row(vec![
						("oid", int_val(oid)),
						("proname", str_val(name)),
						("pronamespace", int_val(11)),
						("proowner", int_val(10)),
						("prolang", int_val(12)),
						("procost", float_val(1.0)),
						("prorows", float_val(0.0)),
						("provariadic", int_val(0)),
						("prosupport", int_val(0)),
						("prokind", str_val("f")),
						("prosecdef", bool_val(false)),
						("proleakproof", bool_val(false)),
						("proisstrict", bool_val(false)),
						("proretset", bool_val(false)),
						("provolatile", str_val("s")),
						("proparallel", str_val("s")),
						("pronargs", int_val(nargs)),
						("pronargdefaults", int_val(0)),
						("prorettype", int_val(rettype)),
						("proargtypes", str_val("")),
						("proallargtypes", null_val()),
						("proargmodes", null_val()),
						("proargnames", null_val()),
						("proargdefaults", null_val()),
						("protrftypes", null_val()),
						("prosrc", str_val(name)),
						("probin", null_val()),
						("prosqlbody", null_val()),
						("proconfig", null_val()),
						("proacl", null_val()),
					])
				})
				.collect();
			Some(rows)
		}

		(Some("pg_catalog") | None, "pg_range") => Some(vec![
			row(vec![
				("rngtypid", int_val(3904)),
				("rngsubtype", int_val(23)),
				("rngmultitypid", int_val(0)),
				("rngcollation", int_val(0)),
				("rngsubopc", int_val(0)),
				("rngcanonical", int_val(0)),
				("rngsubdiff", int_val(0)),
			]),
			row(vec![
				("rngtypid", int_val(3906)),
				("rngsubtype", int_val(1700)),
				("rngmultitypid", int_val(0)),
				("rngcollation", int_val(0)),
				("rngsubopc", int_val(0)),
				("rngcanonical", int_val(0)),
				("rngsubdiff", int_val(0)),
			]),
			row(vec![
				("rngtypid", int_val(3908)),
				("rngsubtype", int_val(1114)),
				("rngmultitypid", int_val(0)),
				("rngcollation", int_val(0)),
				("rngsubopc", int_val(0)),
				("rngcanonical", int_val(0)),
				("rngsubdiff", int_val(0)),
			]),
			row(vec![
				("rngtypid", int_val(3910)),
				("rngsubtype", int_val(1184)),
				("rngmultitypid", int_val(0)),
				("rngcollation", int_val(0)),
				("rngsubopc", int_val(0)),
				("rngcanonical", int_val(0)),
				("rngsubdiff", int_val(0)),
			]),
			row(vec![
				("rngtypid", int_val(3912)),
				("rngsubtype", int_val(1082)),
				("rngmultitypid", int_val(0)),
				("rngcollation", int_val(0)),
				("rngsubopc", int_val(0)),
				("rngcanonical", int_val(0)),
				("rngsubdiff", int_val(0)),
			]),
			row(vec![
				("rngtypid", int_val(3926)),
				("rngsubtype", int_val(20)),
				("rngmultitypid", int_val(0)),
				("rngcollation", int_val(0)),
				("rngsubopc", int_val(0)),
				("rngcanonical", int_val(0)),
				("rngsubdiff", int_val(0)),
			]),
		]),

		(Some("pg_catalog") | None, "pg_collation") => Some(vec![
			row(vec![
				("oid", int_val(100)),
				("collname", str_val("default")),
				("collnamespace", int_val(11)),
				("collowner", int_val(10)),
				("collprovider", str_val("c")),
				("collisdeterministic", bool_val(true)),
				("collencoding", int_val(-1)),
				("collcollate", str_val("default")),
				("collctype", str_val("default")),
				("colliculocale", null_val()),
				("collversion", null_val()),
			]),
			row(vec![
				("oid", int_val(950)),
				("collname", str_val("C")),
				("collnamespace", int_val(11)),
				("collowner", int_val(10)),
				("collprovider", str_val("c")),
				("collisdeterministic", bool_val(true)),
				("collencoding", int_val(-1)),
				("collcollate", str_val("C")),
				("collctype", str_val("C")),
				("colliculocale", null_val()),
				("collversion", null_val()),
			]),
			row(vec![
				("oid", int_val(951)),
				("collname", str_val("POSIX")),
				("collnamespace", int_val(11)),
				("collowner", int_val(10)),
				("collprovider", str_val("c")),
				("collisdeterministic", bool_val(true)),
				("collencoding", int_val(-1)),
				("collcollate", str_val("POSIX")),
				("collctype", str_val("POSIX")),
				("colliculocale", null_val()),
				("collversion", null_val()),
			]),
		]),

		(Some("pg_catalog") | None, "pg_index") => Some(
			user_indexes(user_tables)
				.into_iter()
				.map(|index| {
					let natts = index.attnums.len() as i64;
					row(vec![
						("indexrelid", int_val(index.oid)),
						("indrelid", int_val(index.table_oid)),
						("indnatts", int_val(natts)),
						("indnkeyatts", int_val(natts)),
						("indisunique", bool_val(index.unique)),
						("indisprimary", bool_val(index.primary)),
						("indisexclusion", bool_val(false)),
						("indimmediate", bool_val(true)),
						("indisclustered", bool_val(false)),
						("indisvalid", bool_val(true)),
						("indcheckxmin", bool_val(false)),
						("indisready", bool_val(true)),
						("indislive", bool_val(true)),
						("indisreplident", bool_val(false)),
						("indkey", int_array(&index.attnums)),
						("indcollation", null_val()),
						("indclass", int_array(&index_opclasses(user_tables, &index))),
						("indoption", int_array(&vec![0; index.attnums.len()])),
						("indexprs", null_val()),
						("indpred", null_val()),
					])
				})
				.collect(),
		),

		(Some("pg_catalog") | None, "pg_constraint") => Some(
			user_constraints(user_tables)
				.into_iter()
				.map(|constraint| {
					let foreign = constraint.kind == 'f';
					row(vec![
						("oid", int_val(constraint.oid)),
						("conname", str_val(&constraint.name)),
						("connamespace", int_val(2200)),
						("contype", str_val(constraint.kind.to_string())),
						("condeferrable", bool_val(false)),
						("condeferred", bool_val(false)),
						("convalidated", bool_val(true)),
						("conrelid", int_val(constraint.table_oid)),
						("contypid", int_val(0)),
						("conindid", int_val(constraint.index_oid)),
						("conparentid", int_val(0)),
						("confrelid", int_val(constraint.referenced_table_oid)),
						(
							"confupdtype",
							str_val(if foreign {
								"a"
							} else {
								" "
							}),
						),
						(
							"confdeltype",
							str_val(if foreign {
								constraint.on_delete.pg_code().to_string()
							} else {
								" ".to_owned()
							}),
						),
						(
							"confmatchtype",
							str_val(if foreign {
								"s"
							} else {
								" "
							}),
						),
						("conislocal", bool_val(true)),
						("coninhcount", int_val(0)),
						("connoinherit", bool_val(true)),
						("conkey", int_array(&constraint.attnums)),
						(
							"confkey",
							if foreign {
								int_array(&constraint.referenced_attnums)
							} else {
								null_val()
							},
						),
						("conpfeqop", null_val()),
						("conppeqop", null_val()),
						("conffeqop", null_val()),
						("conexclop", null_val()),
						("conbin", null_val()),
					])
				})
				.collect(),
		),

		// One sequence per serial or auto-increment column of the user's tables, as the SQL DDL
		// defines it: an increasing `bigint` sequence from 1 that does not cycle, owned by the
		// connecting user.
		(Some("pg_catalog") | None, "pg_sequences") => Some(
			user_tables
				.iter()
				.flat_map(|table| {
					let schema = table.schema.as_deref().unwrap_or("public");
					table.columns.iter().filter_map(move |column| {
						let sequence = column.sequence.as_deref()?;
						Some(row(vec![
							("schemaname", str_val(schema)),
							("sequencename", str_val(sequence)),
							("sequenceowner", str_val("root")),
							("data_type", str_val("bigint")),
							("start_value", int_val(1)),
							("min_value", int_val(1)),
							("max_value", int_val(i64::MAX)),
							("increment_by", int_val(1)),
							("cycle", bool_val(false)),
							("cache_size", int_val(1)),
							("last_value", null_val()),
						]))
					})
				})
				.collect(),
		),

		(Some("information_schema"), "sequences") => Some(
			user_tables
				.iter()
				.flat_map(|table| {
					let schema = table.schema.as_deref().unwrap_or("public");
					table.columns.iter().filter_map(move |column| {
						let sequence = column.sequence.as_deref()?;
						Some(row(vec![
							("sequence_catalog", str_val(database)),
							("sequence_schema", str_val(schema)),
							("sequence_name", str_val(sequence)),
							("data_type", str_val("bigint")),
							("numeric_precision", int_val(64)),
							("numeric_precision_radix", int_val(2)),
							("numeric_scale", int_val(0)),
							("start_value", str_val("1")),
							("minimum_value", str_val("1")),
							("maximum_value", str_val(i64::MAX.to_string())),
							("increment", str_val("1")),
							("cycle_option", str_val("NO")),
						]))
					})
				})
				.collect(),
		),

		// A default for each column a sequence numbers; the other columns have none.
		(Some("pg_catalog") | None, "pg_attrdef") => Some(
			visible_serial_columns(user_tables)
				.into_iter()
				.enumerate()
				.map(|(i, column)| {
					row(vec![
						("oid", int_val(ATTRDEF_OID_BASE + i as i64)),
						("adrelid", int_val(column.table_oid)),
						("adnum", int_val(column.attnum)),
						("adbin", str_val(nextval_default(&column.sequence))),
					])
				})
				.collect(),
		),

		// The default B-tree operator class of each type the views list.
		(Some("pg_catalog") | None, "pg_opclass") => Some(
			[
				(3124, "int8_ops", 20),
				(1978, "int4_ops", 23),
				(1979, "int2_ops", 21),
				(424, "bool_ops", 16),
				(3122, "float8_ops", 701),
				(3121, "float4_ops", 700),
				(1988, "numeric_ops", 1700),
				(3126, "text_ops", 25),
			]
			.into_iter()
			.map(|(oid, name, intype)| {
				row(vec![
					("oid", int_val(oid)),
					("opcmethod", int_val(403)),
					("opcname", str_val(name)),
					("opcnamespace", int_val(11)),
					("opcowner", int_val(10)),
					("opcfamily", int_val(0)),
					("opcintype", int_val(intype)),
					("opcdefault", bool_val(true)),
					("opckeytype", int_val(0)),
				])
			})
			.collect(),
		),

		// Row-level security policies, extended statistics, table inheritance and publications are
		// not supported, so these catalogs are always empty.
		(
			Some("pg_catalog") | None,
			"pg_policies"
			| "pg_policy"
			| "pg_statistic_ext"
			| "pg_inherits"
			| "pg_publication"
			| "pg_publication_rel"
			| "pg_publication_namespace",
		) => Some(Vec::new()),

		// The languages a function of the catalog is written in: its own (`internal`), C and SQL.
		(Some("pg_catalog") | None, "pg_language") => Some(
			[(12, "internal", false, false), (13, "c", false, false), (14, "sql", false, true)]
				.into_iter()
				.map(|(oid, name, ispl, trusted)| {
					row(vec![
						("oid", int_val(oid)),
						("lanname", str_val(name)),
						("lanowner", int_val(10)),
						("lanispl", bool_val(ispl)),
						("lanpltrusted", bool_val(trusted)),
						("lanplcallfoid", int_val(0)),
						("laninline", int_val(0)),
						(
							"lanvalidator",
							int_val(if oid == 12 {
								2246
							} else {
								0
							}),
						),
						("lanacl", null_val()),
					])
				})
				.collect(),
		),

		// No extension can be installed.
		(Some("pg_catalog") | None, "pg_extension") => Some(Vec::new()),

		// A view is listed with the tables, as a relation of kind `r`, so no relation is a view
		// here.
		(Some("pg_catalog") | None, "pg_views") => Some(Vec::new()),

		// SurrealDB has no materialized views, so none is ever listed.
		(Some("pg_catalog") | None, "pg_matviews") => Some(Vec::new()),

		(Some("pg_catalog") | None, "pg_stat_user_indexes") => Some(
			user_indexes(user_tables)
				.into_iter()
				.filter_map(|index| {
					let position = usize::try_from(index.table_oid - oid_of_table(0)).ok()?;
					let table = user_tables.get(position)?;
					Some(row(vec![
						("relid", int_val(index.table_oid)),
						("indexrelid", int_val(index.oid)),
						("schemaname", str_val(table.schema.as_deref().unwrap_or("public"))),
						("relname", str_val(&table.name)),
						("indexrelname", str_val(&index.name)),
						("idx_scan", int_val(0)),
						("last_idx_scan", null_val()),
						("idx_tup_read", int_val(0)),
						("idx_tup_fetch", int_val(0)),
					]))
				})
				.collect(),
		),

		(Some("information_schema"), "table_constraints") => Some(
			user_constraints(user_tables)
				.into_iter()
				.filter_map(|constraint| {
					let position = usize::try_from(constraint.table_oid - oid_of_table(0)).ok()?;
					let table = user_tables.get(position)?;
					let schema = table.schema.as_deref().unwrap_or("public");
					let kind = match constraint.kind {
						'p' => "PRIMARY KEY",
						'u' => "UNIQUE",
						'f' => "FOREIGN KEY",
						_ => "CHECK",
					};
					Some(row(vec![
						("constraint_catalog", str_val(database)),
						("constraint_schema", str_val(schema)),
						("constraint_name", str_val(&constraint.name)),
						("table_catalog", str_val(database)),
						("table_schema", str_val(schema)),
						("table_name", str_val(&table.name)),
						("constraint_type", str_val(kind)),
						("is_deferrable", str_val("NO")),
						("initially_deferred", str_val("NO")),
						("enforced", str_val("YES")),
						("nulls_distinct", null_val()),
					]))
				})
				.collect(),
		),

		// The columns a constraint constrains; for a foreign key, the columns it references.
		(Some("information_schema"), "constraint_column_usage") => Some(
			user_constraints(user_tables)
				.into_iter()
				.flat_map(|constraint| {
					let (table_oid, attnums) = if constraint.kind == 'f' {
						(constraint.referenced_table_oid, constraint.referenced_attnums.clone())
					} else {
						(constraint.table_oid, constraint.attnums.clone())
					};
					let name = constraint.name;
					let position = usize::try_from(table_oid - oid_of_table(0)).ok();
					let table = position.and_then(|position| user_tables.get(position));
					attnums.into_iter().filter_map(move |attnum| {
						let table = table?;
						let column = table.columns.get(usize::try_from(attnum - 1).ok()?)?;
						let schema = table.schema.as_deref().unwrap_or("public");
						Some(row(vec![
							("table_catalog", str_val(database)),
							("table_schema", str_val(schema)),
							("table_name", str_val(&table.name)),
							("column_name", str_val(&column.name)),
							("constraint_catalog", str_val(database)),
							("constraint_schema", str_val(schema)),
							("constraint_name", str_val(&name)),
						]))
					})
				})
				.collect(),
		),

		(Some("pg_catalog") | None, "pg_enum") => Some(
			user_enums
				.iter()
				.flat_map(|en| {
					en.labels.iter().map(move |label| {
						row(vec![
							(
								"oid",
								int_val(i64::from(crate::descriptor::enum_label_oid(
									en.id,
									label.ident,
								))),
							),
							(
								"enumtypid",
								int_val(i64::from(crate::descriptor::enum_type_oid(en.id))),
							),
							(
								"enumsortorder",
								float_val(label.order as f64 / ENUM_SORT_ORDER_UNIT as f64),
							),
							("enumlabel", str_val(&label.label)),
						])
					})
				})
				.collect(),
		),

		// Empty standard tables returning 0 rows but valid schema
		(
			Some("pg_catalog") | None,
			"pg_description" | "pg_foreign_data_wrapper" | "pg_foreign_server",
		) => Some(Vec::new()),

		// information_schema.tables
		(Some("information_schema"), "tables") => {
			let mut rows = Vec::new();

			// Built-in pg_catalog relations
			let pg_catalog_names = [
				"pg_type",
				"pg_class",
				"pg_namespace",
				"pg_attribute",
				"pg_am",
				"pg_database",
				"pg_proc",
				"pg_index",
				"pg_settings",
				"pg_roles",
				"pg_user",
				"pg_range",
				"pg_enum",
				"pg_description",
				"pg_constraint",
				"pg_foreign_data_wrapper",
				"pg_foreign_server",
				"pg_collation",
				"pg_attrdef",
				"pg_extension",
				"pg_language",
				"pg_opclass",
				"pg_policies",
				"pg_sequences",
				"pg_stat_user_indexes",
				"pg_views",
				"pg_matviews",
			];
			for tname in pg_catalog_names {
				rows.push(row(vec![
					("table_catalog", str_val(database)),
					("table_schema", str_val("pg_catalog")),
					("table_name", str_val(tname)),
					("table_type", str_val("BASE TABLE")),
					("self_referencing_column_name", null_val()),
					("reference_generation", null_val()),
					("user_defined_type_catalog", null_val()),
					("user_defined_type_schema", null_val()),
					("user_defined_type_name", null_val()),
					("is_insertable_into", str_val("NO")),
					("is_typed", str_val("NO")),
					("commit_action", null_val()),
				]));
			}

			// Built-in information_schema views
			for vname in &[
				"tables",
				"columns",
				"schemata",
				"views",
				"sequences",
				"table_constraints",
				"constraint_column_usage",
			] {
				rows.push(row(vec![
					("table_catalog", str_val(database)),
					("table_schema", str_val("information_schema")),
					("table_name", str_val(*vname)),
					("table_type", str_val("VIEW")),
					("self_referencing_column_name", null_val()),
					("reference_generation", null_val()),
					("user_defined_type_catalog", null_val()),
					("user_defined_type_schema", null_val()),
					("user_defined_type_name", null_val()),
					("is_insertable_into", str_val("NO")),
					("is_typed", str_val("NO")),
					("commit_action", null_val()),
				]));
			}

			// Dynamic user tables
			for ut in user_tables {
				let schema_name = ut.schema.as_deref().unwrap_or("public");
				rows.push(row(vec![
					("table_catalog", str_val(database)),
					("table_schema", str_val(schema_name)),
					("table_name", str_val(&ut.name)),
					("table_type", str_val("BASE TABLE")),
					("self_referencing_column_name", null_val()),
					("reference_generation", null_val()),
					("user_defined_type_catalog", null_val()),
					("user_defined_type_schema", null_val()),
					("user_defined_type_name", null_val()),
					("is_insertable_into", str_val("YES")),
					("is_typed", str_val("NO")),
					("commit_action", null_val()),
				]));
			}

			Some(rows)
		}

		// information_schema.columns
		(Some("information_schema"), "domains") => Some(
			domains
				.iter()
				.map(|domain| {
					let base = DomainBase::of(domain, user_enums, domains, composites);
					let reported = base.reported.as_ref();
					let fact = |f: fn(&crate::reported_type::PgReportedType) -> Option<i64>| {
						reported.and_then(f).map_or_else(null_val, int_val)
					};
					row(vec![
						("domain_catalog", str_val(database)),
						("domain_schema", str_val("public")),
						("domain_name", str_val(&domain.name)),
						("data_type", str_val(&base.data_type)),
						("character_maximum_length", fact(|r| r.character_maximum_length)),
						("character_octet_length", fact(|r| r.character_octet_length)),
						("character_set_catalog", null_val()),
						("character_set_schema", null_val()),
						("character_set_name", null_val()),
						("collation_catalog", null_val()),
						("collation_schema", null_val()),
						("collation_name", null_val()),
						("numeric_precision", fact(|r| r.numeric_precision)),
						("numeric_precision_radix", fact(|r| r.numeric_precision_radix)),
						("numeric_scale", fact(|r| r.numeric_scale)),
						("datetime_precision", fact(|r| r.datetime_precision)),
						("interval_type", null_val()),
						("interval_precision", null_val()),
						("domain_default", null_val()),
						("udt_catalog", str_val(database)),
						(
							"udt_schema",
							str_val(if base.data_type == "USER-DEFINED" {
								"public"
							} else {
								"pg_catalog"
							}),
						),
						("udt_name", str_val(&base.udt_name)),
						("scope_catalog", null_val()),
						("scope_schema", null_val()),
						("scope_name", null_val()),
						("maximum_cardinality", null_val()),
						("dtd_identifier", str_val("1")),
					])
				})
				.collect(),
		),

		(Some("information_schema"), "columns") => {
			let mut rows = Vec::new();

			for ut in user_tables {
				let schema_name = ut.schema.as_deref().unwrap_or("public");
				for (col_idx, col) in ut.columns.iter().enumerate() {
					let dt = col.data_type.as_deref().unwrap_or("text");
					let sql_type = crate::SqlType::from_name(dt);
					let mut udt = sql_type.udt_name().unwrap_or_else(|| "text".to_string());
					let (enum_data_type, enum_column_type) =
						enum_column_names(col.enum_held.as_ref(), user_enums, &mut udt);
					if let Some(composite) = &col.composite {
						udt.clone_from(composite);
					}
					// PostgreSQL reports every array column as `ARRAY`; the element is only
					// visible through `udt_name` (`_text`) and `format_type` (`text[]`).
					let dt = if matches!(sql_type, crate::SqlType::Array(_)) {
						"ARRAY"
					} else if col.composite.is_some() {
						"USER-DEFINED"
					} else {
						enum_data_type.unwrap_or(dt)
					};
					let column_type = enum_column_type.unwrap_or_else(|| dt.to_owned());
					let (precision, radix, scale) = match udt.as_str() {
						"int2" => (int_val(16), int_val(2), int_val(0)),
						"int4" => (int_val(32), int_val(2), int_val(0)),
						"int8" => (int_val(64), int_val(2), int_val(0)),
						"float4" => (int_val(24), int_val(2), null_val()),
						"float8" => (int_val(53), int_val(2), null_val()),
						_ => (null_val(), null_val(), null_val()),
					};
					let declared = DeclaredColumnFacts::of(col, dialect);
					let (dt, column_type, udt) = match &declared {
						Some(facts) => (
							facts.data_type.as_str(),
							facts.column_type.clone(),
							facts.udt_name.clone().unwrap_or(udt),
						),
						None => (dt, column_type, udt),
					};
					let fact = |pick: fn(&DeclaredColumnFacts) -> Option<i64>| {
						declared.as_ref().and_then(pick).map_or_else(null_val, int_val)
					};
					let (precision, radix, scale) = match &declared {
						Some(_) => (
							fact(|facts| facts.numeric_precision),
							fact(|facts| facts.numeric_precision_radix),
							fact(|facts| facts.numeric_scale),
						),
						None => (precision, radix, scale),
					};
					rows.push(row(vec![
						("table_catalog", str_val(database)),
						("table_schema", str_val(schema_name)),
						("table_name", str_val(&ut.name)),
						("column_name", str_val(&col.name)),
						("ordinal_position", int_val((col_idx + 1) as i64)),
						(
							"column_default",
							col.sequence.as_deref().map_or_else(null_val, |sequence| {
								str_val(nextval_default(sequence))
							}),
						),
						(
							"is_nullable",
							str_val(if col.is_nullable {
								"YES"
							} else {
								"NO"
							}),
						),
						("data_type", str_val(dt)),
						("character_maximum_length", fact(|facts| facts.character_maximum_length)),
						("character_octet_length", fact(|facts| facts.character_octet_length)),
						("numeric_precision", precision),
						("numeric_precision_radix", radix),
						("numeric_scale", scale),
						("datetime_precision", fact(|facts| facts.datetime_precision)),
						("interval_type", null_val()),
						("interval_precision", null_val()),
						("character_set_catalog", null_val()),
						("character_set_schema", null_val()),
						("character_set_name", null_val()),
						("collation_catalog", null_val()),
						("collation_schema", null_val()),
						("collation_name", null_val()),
						(
							"domain_catalog",
							col.domain.as_ref().map_or_else(null_val, |_| str_val(database)),
						),
						(
							"domain_schema",
							col.domain.as_ref().map_or_else(null_val, |_| str_val("public")),
						),
						("domain_name", col.domain.as_ref().map_or_else(null_val, str_val)),
						("udt_catalog", str_val(database)),
						("udt_schema", str_val("pg_catalog")),
						("udt_name", str_val(udt)),
						("scope_catalog", null_val()),
						("scope_schema", null_val()),
						("scope_name", null_val()),
						("maximum_cardinality", null_val()),
						("dtd_identifier", str_val((col_idx + 1).to_string())),
						("is_self_referencing", str_val("NO")),
						("is_identity", str_val("NO")),
						("identity_generation", null_val()),
						("identity_start", null_val()),
						("identity_increment", null_val()),
						("identity_maximum", null_val()),
						("identity_minimum", null_val()),
						("identity_cycle", str_val("NO")),
						("is_generated", str_val("NEVER")),
						("generation_expression", null_val()),
						("is_updatable", str_val("YES")),
						("column_type", str_val(column_type)),
					]));
				}
			}

			Some(rows)
		}

		// information_schema.schemata
		(Some("information_schema"), "schemata") => Some(vec![
			row(vec![
				("catalog_name", str_val(database)),
				("schema_name", str_val("pg_catalog")),
				("schema_owner", str_val("root")),
				("default_character_set_catalog", null_val()),
				("default_character_set_schema", null_val()),
				("default_character_set_name", null_val()),
				("sql_path", null_val()),
			]),
			row(vec![
				("catalog_name", str_val(database)),
				("schema_name", str_val("public")),
				("schema_owner", str_val("root")),
				("default_character_set_catalog", null_val()),
				("default_character_set_schema", null_val()),
				("default_character_set_name", null_val()),
				("sql_path", null_val()),
			]),
			row(vec![
				("catalog_name", str_val(database)),
				("schema_name", str_val("information_schema")),
				("schema_owner", str_val("root")),
				("default_character_set_catalog", null_val()),
				("default_character_set_schema", null_val()),
				("default_character_set_name", null_val()),
				("sql_path", null_val()),
			]),
			row(vec![
				("catalog_name", str_val(database)),
				("schema_name", str_val("pg_toast")),
				("schema_owner", str_val("root")),
				("default_character_set_catalog", null_val()),
				("default_character_set_schema", null_val()),
				("default_character_set_name", null_val()),
				("sql_path", null_val()),
			]),
		]),

		// No column has a histogram.
		(Some("information_schema"), "column_statistics") => Some(Vec::new()),

		// information_schema.views
		(Some("information_schema"), "views") => Some(
			[
				"tables",
				"columns",
				"schemata",
				"views",
				"sequences",
				"table_constraints",
				"constraint_column_usage",
			]
			.iter()
			.map(|&vname| {
				row(vec![
					("table_catalog", str_val(database)),
					("table_schema", str_val("information_schema")),
					("table_name", str_val(vname)),
					("view_definition", str_val(format!("SELECT * FROM {vname}"))),
					("check_option", str_val("NONE")),
					("is_updatable", str_val("NO")),
					("is_insertable_into", str_val("NO")),
					("is_trigger_updatable", str_val("NO")),
					("is_trigger_deletable", str_val("NO")),
					("is_trigger_insertable_into", str_val("NO")),
				])
			})
			.collect(),
		),

		_ => None,
	}
}

/// The `pg_type` OID of a column whose `information_schema.columns.data_type` is `data_type`.
/// `jsonb` has its own OID (the engine's JSON type is otherwise reported as `json`); a type with
/// no PostgreSQL counterpart is `text`.
fn attribute_type_oid(data_type: &str) -> i64 {
	match data_type {
		"jsonb" => 3802,
		"jsonb[]" => 3807,
		_ => match crate::SqlType::from_name(data_type).to_pg_oid() {
			0 => 25,
			oid => oid as i64,
		},
	}
}

/// The names `to_regtype` resolves, and the OIDs of the types they spell.
pub(crate) const REGTYPE_NAMES: &[(&str, i32)] = &[
	("bool", 16),
	("boolean", 16),
	("bytea", 17),
	("int8", 20),
	("bigint", 20),
	("int2", 21),
	("smallint", 21),
	("int4", 23),
	("integer", 23),
	("int", 23),
	("text", 25),
	("oid", 26),
	("json", 114),
	("float4", 700),
	("real", 700),
	("float8", 701),
	("double precision", 701),
	("varchar", 1043),
	("character varying", 1043),
	("date", 1082),
	("timestamp", 1114),
	("timestamptz", 1184),
	("numeric", 1700),
	("uuid", 2950),
	("jsonb", 3802),
	("vector", crate::descriptor::VECTOR_OID),
	("_vector", crate::descriptor::VECTOR_ARRAY_OID),
	("public.vector", crate::descriptor::VECTOR_OID),
	("public._vector", crate::descriptor::VECTOR_ARRAY_OID),
];

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn exactly_the_relations_that_list_user_tables_say_so() {
		let table = [
			TableSchema::new(
				"emp",
				vec![
					crate::ColumnInfo::new("id", 0).with_sequence("emp_id_seq"),
					crate::ColumnInfo::new("dept", 1),
				],
			)
			.with_foreign_keys(vec![crate::ForeignKeyInfo {
				column: "dept".to_owned(),
				target_table: "dept".to_owned(),
				on_delete: crate::ReferentialAction::NoAction,
			}]),
			TableSchema::new("dept", vec![crate::ColumnInfo::new("id", 0)]),
		];
		let names = [
			(None, "pg_sequences"),
			(None, "pg_attrdef"),
			(None, "pg_stat_user_indexes"),
			(None, "pg_opclass"),
			(None, "pg_policies"),
			(None, "pg_matviews"),
			(Some("information_schema"), "table_constraints"),
			(Some("information_schema"), "constraint_column_usage"),
			(None, "pg_index"),
			(None, "pg_constraint"),
			(None, "pg_class"),
			(Some("pg_catalog"), "pg_attribute"),
			(None, "pg_catalog.pg_class"),
			(Some("information_schema"), "tables"),
			(None, "information_schema.columns"),
			(None, "pg_type"),
			(None, "pg_settings"),
			(Some("information_schema"), "schemata"),
			(Some("information_schema"), "views"),
			(None, "pg_namespace"),
			(None, "emp"),
		];
		for (schema, name) in names {
			let with = get_system_table_rows(schema, name, &table, "test");
			let without = get_system_table_rows(schema, name, &[], "test");
			let differs = with.as_ref().map(Vec::len) != without.as_ref().map(Vec::len);
			assert_eq!(lists_user_tables(schema, name), differs, "{schema:?} {name}");
		}
	}
	fn sequence_table() -> [TableSchema; 1] {
		[TableSchema::new(
			"Item",
			vec![
				crate::ColumnInfo::new("id", 0).with_sequence("Item_id_seq"),
				crate::ColumnInfo::new("label", 1),
			],
		)
		.with_schema("public")]
	}

	fn field<'a>(row: &'a LogicalExpr, name: &str) -> &'a LogicalExpr {
		let LogicalExpr::Object(fields) = row else {
			panic!("a row is an object");
		};
		&fields.iter().find(|(key, _)| key == name).expect("the row has the field").1
	}

	/// A table whose `column` is numbered by `sequence`.
	fn numbered(table: &str, column: &str, sequence: &str) -> TableSchema {
		TableSchema::new(
			table,
			vec![
				crate::ColumnInfo::new("id", 0),
				crate::ColumnInfo::new(column, 1).with_sequence(sequence),
			],
		)
		.with_schema("public")
	}

	fn sequence_names(tables: &[TableSchema]) -> Vec<String> {
		let rows = get_system_table_rows(None, "pg_sequences", tables, "test").unwrap();
		rows.iter()
			.map(|row| {
				let LogicalExpr::Literal(LogicalLiteral::String(name)) = field(row, "sequencename")
				else {
					panic!("a sequence is named by a string");
				};
				name.to_string()
			})
			.collect()
	}

	#[test]
	fn the_sequence_the_ddl_defines_for_a_column_is_listed_as_postgresql_names_it() {
		let tables = [
			numbered("Counter", "id", "Counter.id"),
			numbered("s.t", "n", "s.t.n"),
			numbered("t", "a b", "t.`a b`"),
		];
		assert_eq!(sequence_names(&tables), ["Counter_id_seq", "s_t_n_seq", "t_a b_seq"]);

		let serial = crate::catalog::relations::serial_columns(&tables);
		let names: Vec<_> = serial.iter().map(|c| c.sequence.as_str()).collect();
		assert_eq!(names, ["Counter_id_seq", "s_t_n_seq", "t_a b_seq"]);
	}

	#[test]
	fn a_sequence_the_ddl_did_not_define_for_the_column_keeps_its_name() {
		// `draws` is named in a `DEFAULT`; the column of `t` takes the conventional name, which
		// the other sequence does not hold.
		let tables = [numbered("t", "id", "draws"), numbered("t2", "id", "t2.id")];
		assert_eq!(sequence_names(&tables), ["draws", "t2_id_seq"]);

		// One that holds the conventional name keeps it, and the column the DDL numbered takes the
		// next free one.
		let tables = [numbered("t", "id", "t.id"), numbered("u", "id", "t_id_seq")];
		assert_eq!(sequence_names(&tables), ["t_id_seq1", "t_id_seq"]);
	}

	#[test]
	fn columns_whose_conventional_names_collide_get_distinct_names() {
		// `a_b` with `c` and `a` with `b_c` both run together as `a_b_c_seq`. The names depend on
		// the tables alone, not on the order they are listed in.
		let one = [numbered("a_b", "c", "a_b.c"), numbered("a", "b_c", "a.b_c")];
		let other = [numbered("a", "b_c", "a.b_c"), numbered("a_b", "c", "a_b.c")];
		assert_eq!(sequence_names(&one), ["a_b_c_seq1", "a_b_c_seq"]);
		assert_eq!(sequence_names(&other), ["a_b_c_seq", "a_b_c_seq1"]);

		let serial = crate::catalog::relations::serial_columns(&one);
		let names: Vec<_> =
			serial.iter().map(|c| (c.table.as_str(), c.sequence.as_str())).collect();
		assert_eq!(names, [("a_b", "a_b_c_seq1"), ("a", "a_b_c_seq")]);

		// Naming the names again changes nothing.
		let named = crate::catalog::relations::with_visible_sequence_names(&one).into_owned();
		assert_eq!(
			crate::catalog::relations::with_visible_sequence_names(&named).as_ref(),
			named.as_slice()
		);
	}

	#[test]
	fn pg_sequences_lists_the_sequence_of_each_serial_column() {
		let rows = get_system_table_rows(None, "pg_sequences", &sequence_table(), "test").unwrap();
		assert_eq!(rows.len(), 1);
		assert_eq!(field(&rows[0], "schemaname"), &str_val("public"));
		assert_eq!(field(&rows[0], "sequencename"), &str_val("Item_id_seq"));
		assert_eq!(field(&rows[0], "start_value"), &int_val(1));
		assert_eq!(field(&rows[0], "increment_by"), &int_val(1));
		assert_eq!(field(&rows[0], "cycle"), &bool_val(false));
		assert!(get_system_table_rows(None, "pg_sequences", &[], "test").unwrap().is_empty());
	}

	#[test]
	fn a_serial_column_has_a_default_and_pg_class_lists_the_information_schema_views() {
		let attributes =
			get_system_table_rows(None, "pg_attribute", &sequence_table(), "test").unwrap();
		let serial = attributes
			.iter()
			.find(|row| field(row, "attname") == &str_val("id"))
			.expect("the serial column");
		assert_eq!(field(serial, "atthasdef"), &bool_val(true));
		let plain = attributes
			.iter()
			.find(|row| field(row, "attname") == &str_val("label"))
			.expect("the plain column");
		assert_eq!(field(plain, "atthasdef"), &bool_val(false));
		let classes = get_system_table_rows(None, "pg_class", &[], "test").unwrap();
		for view in ["sequences", "table_constraints", "constraint_column_usage"] {
			assert!(classes.iter().any(|row| field(row, "relname") == &str_val(view)), "{view}");
		}
	}

	#[test]
	fn pg_database_lists_each_name_once_and_pg_class_lists_the_served_relations() {
		for (database, expected) in [("postgres", 1), ("shop", 2)] {
			let rows = get_system_table_rows(None, "pg_database", &[], database).unwrap();
			assert_eq!(rows.len(), expected, "{database}");
			assert!(rows.iter().any(|row| field(row, "datname") == &str_val(database)));
		}
		let classes = get_system_table_rows(None, "pg_class", &[], "test").unwrap();
		for relation in
			["pg_sequences", "pg_attrdef", "pg_opclass", "pg_views", "pg_matviews", "pg_language"]
		{
			assert!(
				classes.iter().any(|row| field(row, "relname") == &str_val(relation)),
				"{relation}"
			);
		}
	}

	#[test]
	fn pg_matviews_has_the_columns_of_postgres_and_lists_no_materialized_view() {
		let expected = [
			("schemaname", None),
			("matviewname", None),
			("matviewowner", None),
			("tablespace", None),
			("hasindexes", Some("boolean")),
			("ispopulated", Some("boolean")),
			("definition", None),
		];
		for schema in [None, Some("pg_catalog")] {
			assert!(is_system_catalog_table(schema, "pg_matviews"), "{schema:?}");
			let table = get_system_table_schema(schema, "pg_matviews").expect("a schema");
			assert_eq!(table.schema.as_deref(), Some("pg_catalog"));
			assert_eq!(table.columns.len(), expected.len());
			for (position, (name, data_type)) in expected.iter().enumerate() {
				let column = &table.columns[position];
				assert_eq!(column.name, *name);
				assert_eq!(column.data_type.as_deref(), *data_type, "{name}");
			}
		}

		let tables = [
			TableSchema::new("emp", vec![crate::ColumnInfo::new("id", 0)]),
			TableSchema::new("dept", vec![crate::ColumnInfo::new("id", 0)]),
		];
		for schema in [None, Some("pg_catalog")] {
			for user_tables in [&tables[..], &[]] {
				let rows = get_system_table_rows(schema, "pg_matviews", user_tables, "test");
				assert_eq!(rows, Some(Vec::new()), "{schema:?}");
			}
		}
		assert_eq!(
			get_system_table_rows(None, "pg_catalog.pg_matviews", &tables, "test"),
			Some(vec![])
		);
	}

	#[test]
	fn pg_matviews_is_listed_among_the_relations_of_the_catalog() {
		let classes = get_system_table_rows(None, "pg_class", &[], "test").unwrap();
		let class = classes
			.iter()
			.find(|row| field(row, "relname") == &str_val("pg_matviews"))
			.expect("a pg_class row");
		assert_eq!(field(class, "relnatts"), &int_val(7));
		assert_eq!(field(class, "relkind"), &str_val("v"));
		let oids: Vec<_> = classes.iter().map(|row| field(row, "oid").clone()).collect();
		let distinct: std::collections::HashSet<_> =
			oids.iter().map(|oid| format!("{oid:?}")).collect();
		assert_eq!(oids.len(), distinct.len(), "every pg_class oid is unique");
		let tables =
			get_system_table_rows(Some("information_schema"), "tables", &[], "test").unwrap();
		assert!(tables.iter().any(|row| {
			field(row, "table_schema") == &str_val("pg_catalog")
				&& field(row, "table_name") == &str_val("pg_matviews")
		}));
	}

	#[test]
	fn the_sequences_and_views_of_the_catalog_are_listed_where_clients_look() {
		let classes = get_system_table_rows(None, "pg_class", &sequence_table(), "test").unwrap();
		let sequence = classes
			.iter()
			.find(|row| field(row, "relname") == &str_val("Item_id_seq"))
			.expect("a pg_class row for the sequence");
		assert_eq!(field(sequence, "relkind"), &str_val("S"));
		let views =
			get_system_table_rows(Some("information_schema"), "views", &[], "test").unwrap();
		for view in ["sequences", "table_constraints", "constraint_column_usage"] {
			assert!(views.iter().any(|row| field(row, "table_name") == &str_val(view)), "{view}");
		}
	}

	#[test]
	fn pg_attrdef_defaults_a_serial_column_to_the_next_value_of_its_sequence() {
		let rows = get_system_table_rows(None, "pg_attrdef", &sequence_table(), "test").unwrap();
		assert_eq!(rows.len(), 1);
		assert_eq!(field(&rows[0], "adrelid"), &int_val(oid_of_table(0)));
		assert_eq!(field(&rows[0], "adnum"), &int_val(1));
		assert_eq!(field(&rows[0], "adbin"), &str_val("nextval('\"Item_id_seq\"'::regclass)"));
	}

	#[test]
	fn table_constraints_name_the_kind_of_each_constraint() {
		let tables = [
			TableSchema::new("a", vec![crate::ColumnInfo::new("id", 0)]),
			TableSchema::new(
				"b",
				vec![crate::ColumnInfo::new("id", 0), crate::ColumnInfo::new("a_id", 1)],
			)
			.with_foreign_keys(vec![crate::ForeignKeyInfo {
				column: "a_id".to_owned(),
				target_table: "a".to_owned(),
				on_delete: crate::ReferentialAction::Cascade,
			}]),
		];
		let rows =
			get_system_table_rows(Some("information_schema"), "table_constraints", &tables, "test")
				.unwrap();
		let kinds: Vec<_> = rows.iter().map(|row| field(row, "constraint_type").clone()).collect();
		assert_eq!(kinds, [str_val("PRIMARY KEY"), str_val("PRIMARY KEY"), str_val("FOREIGN KEY")]);
		let usage = get_system_table_rows(
			Some("information_schema"),
			"constraint_column_usage",
			&tables,
			"test",
		)
		.unwrap();
		let foreign = usage
			.iter()
			.find(|row| field(row, "constraint_name") == &str_val("b_a_id_fkey"))
			.expect("the foreign key uses a column");
		assert_eq!(field(foreign, "table_name"), &str_val("a"));
		assert_eq!(field(foreign, "column_name"), &str_val("id"));
	}

	#[test]
	fn the_catalog_views_name_the_database_the_statement_runs_in() {
		let tables = [TableSchema::new("t", vec![crate::ColumnInfo::new("a", 0)])];
		for name in ["tables", "columns"] {
			let rows =
				get_system_table_rows(Some("information_schema"), name, &tables, "shop").unwrap();
			assert!(!rows.is_empty(), "{name}");
			for row in rows {
				assert_eq!(field(&row, "table_catalog"), &str_val("shop"), "{name}");
			}
		}
	}

	#[test]
	fn array_columns_are_reported_as_array_with_the_element_in_udt_name() {
		let column = |name: &str, ty: &str, ordinal| {
			let mut column = crate::ColumnInfo::new(name, ordinal);
			column.data_type = Some(ty.to_owned());
			column
		};
		let tables = [TableSchema::new(
			"t",
			vec![
				column("tags", "text[]", 0),
				column("scores", "bigint[]", 1),
				column("n", "bigint", 2),
			],
		)];
		let rows =
			get_system_table_rows(Some("information_schema"), "columns", &tables, "test").unwrap();
		let of = |name: &str| {
			let row = rows
				.iter()
				.find(|row| field(row, "column_name") == &str_val(name))
				.expect("a row for the column");
			(
				field(row, "data_type").clone(),
				field(row, "udt_name").clone(),
				field(row, "udt_schema").clone(),
			)
		};
		assert_eq!(of("tags"), (str_val("ARRAY"), str_val("_text"), str_val("pg_catalog")));
		assert_eq!(of("scores"), (str_val("ARRAY"), str_val("_int8"), str_val("pg_catalog")));
		assert_eq!(of("n"), (str_val("bigint"), str_val("int8"), str_val("pg_catalog")));
	}

	#[test]
	fn boolean_catalog_columns_are_typed_as_boolean() {
		let index = get_system_table_schema(None, "pg_index").unwrap();
		assert_eq!(
			index.find_column("indisunique", false).and_then(|c| c.data_type.as_deref()),
			Some("boolean")
		);
		assert_eq!(
			index.find_column("indrelid", false).and_then(|c| c.data_type.as_deref()),
			Some("oid")
		);
	}

	/// A table `people` with a domain column `rank` over `integer`, and a composite column `home`,
	/// with the domain `posint` (id 3) and the composite `address` (id 2) that they name.
	fn user_types_fixture() -> (Vec<TableSchema>, UserTypes) {
		let people = TableSchema::new(
			"people",
			vec![
				crate::ColumnInfo::new("id", 0),
				crate::ColumnInfo::new("rank", 1).with_type("integer").with_domain("posint"),
				crate::ColumnInfo::new("home", 2).with_type("jsonb").with_composite("address"),
			],
		)
		.with_schema("public");
		let types = UserTypes {
			enums: Vec::new(),
			domains: vec![DomainType {
				name: "posint".to_owned(),
				id: 3,
				base: surrealdb_expr::expr::Kind::I32,
				base_type: "int4".to_owned(),
				not_null: true,
				default: None,
				checks: Vec::new(),
			}],
			composites: vec![CompositeType {
				name: "address".to_owned(),
				id: 2,
				fields: vec![
					("street".to_owned(), surrealdb_expr::expr::Kind::String),
					("zip".to_owned(), surrealdb_expr::expr::Kind::I32),
				],
				declared: vec![(
					"street".to_owned(),
					crate::declared_type::DeclaredSqlType::parse(
						crate::SqlTypeDialect::PostgreSql,
						"varchar(40)",
					)
					.expect("varchar parses"),
				)],
			}],
		};
		(vec![people], types)
	}

	/// The row of `rows` whose `column` holds `value`.
	fn row_where<'a>(
		rows: &'a [LogicalExpr],
		column: &str,
		value: &LogicalExpr,
	) -> &'a LogicalExpr {
		rows.iter()
			.find(|row| field(row, column) == value)
			.unwrap_or_else(|| panic!("no row with {column} = {value:?}"))
	}

	fn typed_rows(name: &str, tables: &[TableSchema], types: &UserTypes) -> Vec<LogicalExpr> {
		get_system_table_rows_with_types(None, name, tables, types, "test").unwrap()
	}

	#[test]
	fn a_domain_is_a_pg_type_over_its_base_type() {
		let (tables, types) = user_types_fixture();
		let rows = typed_rows("pg_type", &tables, &types);
		let domain = row_where(&rows, "typname", &str_val("posint"));
		let oid = i64::from(crate::descriptor::domain_type_oid(3));
		assert_eq!(field(domain, "oid"), &int_val(oid));
		assert_eq!(field(domain, "typtype"), &str_val("d"));
		assert_eq!(field(domain, "typbasetype"), &int_val(23));
		assert_eq!(field(domain, "typnotnull"), &bool_val(true));
		assert_eq!(field(domain, "typlen"), &int_val(4));
		assert_eq!(field(domain, "typarray"), &int_val(0));
	}

	#[test]
	fn a_composite_is_a_pg_type_whose_relation_holds_its_fields() {
		let (tables, types) = user_types_fixture();
		let rows = typed_rows("pg_type", &tables, &types);
		let composite = row_where(&rows, "typname", &str_val("address"));
		let relation = i64::from(crate::descriptor::composite_relation_oid(2));
		assert_eq!(
			field(composite, "oid"),
			&int_val(i64::from(crate::descriptor::composite_type_oid(2)))
		);
		assert_eq!(field(composite, "typtype"), &str_val("c"));
		assert_eq!(field(composite, "typrelid"), &int_val(relation));

		let classes = typed_rows("pg_class", &tables, &types);
		let class = row_where(&classes, "relname", &str_val("address"));
		assert_eq!(field(class, "oid"), &int_val(relation));
		assert_eq!(field(class, "relkind"), &str_val("c"));
		assert_eq!(field(class, "relnatts"), &int_val(2));
		assert_eq!(
			field(class, "reltype"),
			&int_val(i64::from(crate::descriptor::composite_type_oid(2)))
		);

		let attributes = typed_rows("pg_attribute", &tables, &types);
		let fields: Vec<_> =
			attributes.iter().filter(|row| field(row, "attrelid") == &int_val(relation)).collect();
		let names: Vec<_> = fields.iter().map(|row| field(row, "attname").clone()).collect();
		assert_eq!(names, vec![str_val("street"), str_val("zip")]);
		// An attribute declared with a SQL type has that type and its modifier.
		assert_eq!(field(fields[0], "atttypid"), &int_val(1043));
		assert_eq!(field(fields[0], "atttypmod"), &int_val(44));
		assert_eq!(field(fields[1], "atttypid"), &int_val(23));
		assert_eq!(field(fields[1], "atttypmod"), &int_val(-1));
		assert_eq!(field(fields[1], "attnum"), &int_val(2));
	}

	#[test]
	fn a_domain_column_is_attributed_to_the_domain_and_a_composite_column_to_the_composite() {
		let (tables, types) = user_types_fixture();
		let attributes = typed_rows("pg_attribute", &tables, &types);
		let rank = attributes
			.iter()
			.find(|row| field(row, "attname") == &str_val("rank"))
			.expect("the rank attribute");
		assert_eq!(
			field(rank, "atttypid"),
			&int_val(i64::from(crate::descriptor::domain_type_oid(3)))
		);
		assert_eq!(field(rank, "attlen"), &int_val(4));
		let home = attributes
			.iter()
			.find(|row| field(row, "attname") == &str_val("home"))
			.expect("the home attribute");
		assert_eq!(
			field(home, "atttypid"),
			&int_val(i64::from(crate::descriptor::composite_type_oid(2)))
		);
		assert_eq!(field(home, "attlen"), &int_val(-1));
	}

	#[test]
	fn information_schema_names_the_domain_of_a_column_and_lists_the_domains() {
		let (tables, types) = user_types_fixture();
		let columns = typed_rows_of_schema("columns", &tables, &types);
		let rank = row_where(&columns, "column_name", &str_val("rank"));
		assert_eq!(field(rank, "domain_name"), &str_val("posint"));
		assert_eq!(field(rank, "domain_schema"), &str_val("public"));
		assert_eq!(field(rank, "data_type"), &str_val("integer"));
		let id = row_where(&columns, "column_name", &str_val("id"));
		assert_eq!(field(id, "domain_name"), &null_val());
		let home = row_where(&columns, "column_name", &str_val("home"));
		assert_eq!(field(home, "data_type"), &str_val("USER-DEFINED"));
		assert_eq!(field(home, "udt_name"), &str_val("address"));
		assert_eq!(field(home, "domain_name"), &null_val());

		let domains = typed_rows_of_schema("domains", &tables, &types);
		assert_eq!(domains.len(), 1);
		let posint = &domains[0];
		assert_eq!(field(posint, "domain_name"), &str_val("posint"));
		assert_eq!(field(posint, "data_type"), &str_val("integer"));
		assert_eq!(field(posint, "udt_name"), &str_val("int4"));
	}

	fn typed_rows_of_schema(
		name: &str,
		tables: &[TableSchema],
		types: &UserTypes,
	) -> Vec<LogicalExpr> {
		get_system_table_rows_with_types(Some("information_schema"), name, tables, types, "test")
			.unwrap()
	}

	#[test]
	fn user_type_oids_are_disjoint_from_the_other_ranges_and_fit_a_signed_word() {
		use crate::descriptor::{
			COMPOSITE_RELATION_OID_BASE, COMPOSITE_TYPE_OID_BASE, DOMAIN_TYPE_OID_BASE,
			ENUM_ARRAY_OID_OFFSET, ENUM_LABEL_OID_STRIDE, ENUM_TYPE_OID_BASE, USER_TYPE_ID_LIMIT,
			composite_relation_oid, composite_type_oid, domain_type_oid, enum_label_oid,
		};
		let last = USER_TYPE_ID_LIMIT - 1;
		// The domain range ends where the composite type range begins, and so on up the space.
		assert!(domain_type_oid(last) < COMPOSITE_TYPE_OID_BASE);
		assert!(composite_type_oid(last) < COMPOSITE_RELATION_OID_BASE);
		assert!(composite_relation_oid(last) < 1 << 31, "a relation oid must be a positive i32");
		// Clear of the enum types and their arrays, the table row types and the sequences.
		let enums_end = ENUM_TYPE_OID_BASE + 2 * ENUM_ARRAY_OID_OFFSET;
		assert!(DOMAIN_TYPE_OID_BASE >= enums_end);
		let table_and_sequence_oids_end = 6_000_000;
		assert!(DOMAIN_TYPE_OID_BASE >= table_and_sequence_oids_end);
		// The table row types are `pg_type` rows too, so they start past the last enum array.
		let last_enum_array = crate::descriptor::enum_array_type_oid(ENUM_ARRAY_OID_OFFSET - 1);
		assert!(TABLE_TYPE_OID_BASE > i64::from(last_enum_array));
		assert!(
			TABLE_TYPE_OID_BASE + i64::from(USER_TYPE_ID_LIMIT) < i64::from(DOMAIN_TYPE_OID_BASE)
		);
		// Below the enum label oids, which start at 3e9 and take one block per enum.
		assert!(composite_relation_oid(last) < enum_label_oid(0, 0));
		let last_enum = ENUM_ARRAY_OID_OFFSET - 1;
		assert_eq!(enum_label_oid(0, ENUM_LABEL_OID_STRIDE - 1) + 1, enum_label_oid(1, 0));
		assert!(enum_label_oid(last_enum, ENUM_LABEL_OID_STRIDE - 1) < u32::MAX);
		assert_ne!(domain_type_oid(0), composite_type_oid(0));
	}
}
