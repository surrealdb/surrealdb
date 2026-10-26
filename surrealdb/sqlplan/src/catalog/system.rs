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
	IndexEntry, default_opclass, oid_of_table, serial_columns, user_constraints, user_indexes,
};
use super::{ColumnInfo, TableSchema};

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
					| "pg_policies" | "pg_stat_user_indexes"
					| "pg_views" | "pg_language"
					| "pg_extension"
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
					| "views" | "sequences"
					| "table_constraints"
					| "constraint_column_usage"
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
					| "pg_policies" | "pg_stat_user_indexes"
					| "pg_views" | "pg_language"
					| "pg_extension"
					| "pg_foreign_data_wrapper"
					| "pg_foreign_server"
					| "pg_collation"
			)
		}
		_ => false,
	}
}

const ATTRDEF_OID_BASE: i64 = 3_000_000;

/// The oid of the composite row type of the table at `position` among the user tables: the
/// `reltype` of its `pg_class` row and the `oid` of its `pg_type` row.
/// The oid of the first sequence relation `pg_class` lists, in a range of its own beside those
/// of the indexes, constraints, defaults and row types.
const SEQUENCE_OID_BASE: i64 = 5_000_000;

const TABLE_TYPE_OID_BASE: i64 = 4_000_000;

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
			],
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
					let atttypid = attribute_type_oid(col.data_type.as_deref().unwrap_or("text"));
					let attlen = match atttypid {
						16 => 1,
						21 => 2,
						23 | 700 => 4,
						20 | 701 => 8,
						2950 => 16,
						_ => -1,
					};
					// PostgreSQL records the declared dimensions of an array column, which Drizzle
					// reads to tell `text[]` from `text`.
					let attndims = i64::from(matches!(
						crate::SqlType::from_name(col.data_type.as_deref().unwrap_or("text")),
						crate::SqlType::Array(_)
					));
					rows.push(row(vec![
						("attrelid", int_val(relid)),
						("attname", str_val(&col.name)),
						("atttypid", int_val(atttypid)),
						("attstattarget", int_val(-1)),
						("attlen", int_val(attlen)),
						("attnum", int_val((col_idx + 1) as i64)),
						("attndims", int_val(attndims)),
						("attcacheoff", int_val(-1)),
						("atttypmod", int_val(-1)),
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
			serial_columns(user_tables)
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

		// No row-level security policy is ever defined.
		(Some("pg_catalog") | None, "pg_policies") => Some(Vec::new()),

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

		// Empty standard tables returning 0 rows but valid schema
		(
			Some("pg_catalog") | None,
			"pg_enum" | "pg_description" | "pg_foreign_data_wrapper" | "pg_foreign_server",
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
		(Some("information_schema"), "columns") => {
			let mut rows = Vec::new();

			for ut in user_tables {
				let schema_name = ut.schema.as_deref().unwrap_or("public");
				for (col_idx, col) in ut.columns.iter().enumerate() {
					let dt = col.data_type.as_deref().unwrap_or("text");
					let sql_type = crate::SqlType::from_name(dt);
					let udt = sql_type.udt_name().unwrap_or_else(|| "text".to_string());
					// PostgreSQL reports every array column as `ARRAY`; the element is only
					// visible through `udt_name` (`_text`) and `format_type` (`text[]`).
					let dt = if matches!(sql_type, crate::SqlType::Array(_)) {
						"ARRAY"
					} else {
						dt
					};
					let (precision, radix, scale) = match udt.as_str() {
						"int2" => (int_val(16), int_val(2), int_val(0)),
						"int4" => (int_val(32), int_val(2), int_val(0)),
						"int8" => (int_val(64), int_val(2), int_val(0)),
						"float4" => (int_val(24), int_val(2), null_val()),
						"float8" => (int_val(53), int_val(2), null_val()),
						_ => (null_val(), null_val(), null_val()),
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
						("character_maximum_length", null_val()),
						("character_octet_length", null_val()),
						("numeric_precision", precision),
						("numeric_precision_radix", radix),
						("numeric_scale", scale),
						("datetime_precision", null_val()),
						("interval_type", null_val()),
						("interval_precision", null_val()),
						("character_set_catalog", null_val()),
						("character_set_schema", null_val()),
						("character_set_name", null_val()),
						("collation_catalog", null_val()),
						("collation_schema", null_val()),
						("collation_name", null_val()),
						("domain_catalog", null_val()),
						("domain_schema", null_val()),
						("domain_name", null_val()),
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
		for relation in ["pg_sequences", "pg_attrdef", "pg_opclass", "pg_views", "pg_language"] {
			assert!(
				classes.iter().any(|row| field(row, "relname") == &str_val(relation)),
				"{relation}"
			);
		}
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
}
