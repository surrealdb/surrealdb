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
		Some("pg_catalog") => matches!(
			norm_name,
			"pg_type"
				| "pg_class" | "pg_namespace"
				| "pg_attribute"
				| "pg_am" | "pg_database"
				| "pg_proc" | "pg_index"
				| "pg_settings"
				| "pg_roles" | "pg_user"
				| "pg_range" | "pg_enum"
				| "pg_description"
				| "pg_constraint"
				| "pg_foreign_data_wrapper"
				| "pg_foreign_server"
				| "pg_collation"
		),
		Some("information_schema") => {
			matches!(norm_name, "tables" | "columns" | "schemata" | "views")
		}
		None => matches!(
			norm_name,
			"pg_type"
				| "pg_class" | "pg_namespace"
				| "pg_attribute"
				| "pg_am" | "pg_database"
				| "pg_proc" | "pg_index"
				| "pg_settings"
				| "pg_roles" | "pg_user"
				| "pg_range" | "pg_enum"
				| "pg_description"
				| "pg_constraint"
				| "pg_foreign_data_wrapper"
				| "pg_foreign_server"
				| "pg_collation"
		),
		_ => false,
	}
}

/// Helper to build a `TableSchema` with column names and ordinal indices.
fn make_schema(schema_name: Option<&str>, table_name: &str, cols: &[&'static str]) -> TableSchema {
	let mut s = TableSchema::new(
		table_name,
		cols.iter().enumerate().map(|(i, &c)| ColumnInfo::new(c, i)).collect(),
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

/// Generates synthetic rows for the requested system catalog or information_schema table.
pub fn get_system_table_rows(
	schema: Option<&str>,
	name: &str,
	user_tables: &[TableSchema],
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

		(Some("pg_catalog") | None, "pg_database") => Some(vec![
			row(vec![
				("oid", int_val(13757)),
				("datname", str_val("test")),
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
			]),
			row(vec![
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
			]),
		]),

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
			];

			let rows = types
				.iter()
				.map(|&(oid, name, len, byval, typtype, cat, elem, arr)| {
					row(vec![
						("oid", int_val(oid)),
						("typname", str_val(name)),
						("typnamespace", int_val(11)),
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
					("relrewrite", int_val(0)),
					("relfrozenxid", int_val(0)),
					("relminmxid", int_val(0)),
					("relacl", null_val()),
					("reloptions", null_val()),
					("relpartbound", null_val()),
				]));
			}

			// Built-in information_schema views
			let info_schema_views = [
				(11001, "tables", 11000, 'v', 12),
				(11002, "columns", 11000, 'v', 44),
				(11003, "schemata", 11000, 'v', 7),
				(11004, "views", 11000, 'v', 10),
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
					("relrewrite", int_val(0)),
					("relfrozenxid", int_val(0)),
					("relminmxid", int_val(0)),
					("relacl", null_val()),
					("reloptions", null_val()),
					("relpartbound", null_val()),
				]));
			}

			// Dynamic user tables
			for (idx, ut) in user_tables.iter().enumerate() {
				let oid = 16384 + idx as i64;
				rows.push(row(vec![
					("oid", int_val(oid)),
					("relname", str_val(&ut.name)),
					("relnamespace", int_val(2200)), // public
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
					let (atttypid, attlen) = match col.data_type.as_deref() {
						Some("int" | "integer" | "int4") => (23, 4),
						Some("bigint" | "int8") => (20, 8),
						Some("smallint" | "int2") => (21, 2),
						Some("bool" | "boolean") => (16, 1),
						Some("float" | "float8" | "double") => (701, 8),
						Some("uuid") => (2950, 16),
						_ => (25, -1), // text
					};
					rows.push(row(vec![
						("attrelid", int_val(relid)),
						("attname", str_val(&col.name)),
						("atttypid", int_val(atttypid)),
						("attstattarget", int_val(-1)),
						("attlen", int_val(attlen)),
						("attnum", int_val((col_idx + 1) as i64)),
						("attndims", int_val(0)),
						("attcacheoff", int_val(-1)),
						("atttypmod", int_val(-1)),
						("attbyval", bool_val(attlen > 0)),
						("attstorage", str_val("p")),
						("attalign", str_val("i")),
						("attnotnull", bool_val(!col.is_nullable)),
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

		// Empty standard tables returning 0 rows but valid schema
		(
			Some("pg_catalog") | None,
			"pg_index"
			| "pg_enum"
			| "pg_description"
			| "pg_constraint"
			| "pg_foreign_data_wrapper"
			| "pg_foreign_server",
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
			];
			for tname in pg_catalog_names {
				rows.push(row(vec![
					("table_catalog", str_val("test")),
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
			for vname in &["tables", "columns", "schemata", "views"] {
				rows.push(row(vec![
					("table_catalog", str_val("test")),
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
					("table_catalog", str_val("test")),
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
					rows.push(row(vec![
						("table_catalog", str_val("test")),
						("table_schema", str_val(schema_name)),
						("table_name", str_val(&ut.name)),
						("column_name", str_val(&col.name)),
						("ordinal_position", int_val((col_idx + 1) as i64)),
						("column_default", null_val()),
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
						("numeric_precision", null_val()),
						("numeric_precision_radix", null_val()),
						("numeric_scale", null_val()),
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
						("udt_catalog", str_val("test")),
						("udt_schema", str_val("pg_catalog")),
						("udt_name", str_val(dt)),
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
				("catalog_name", str_val("test")),
				("schema_name", str_val("pg_catalog")),
				("schema_owner", str_val("root")),
				("default_character_set_catalog", null_val()),
				("default_character_set_schema", null_val()),
				("default_character_set_name", null_val()),
				("sql_path", null_val()),
			]),
			row(vec![
				("catalog_name", str_val("test")),
				("schema_name", str_val("public")),
				("schema_owner", str_val("root")),
				("default_character_set_catalog", null_val()),
				("default_character_set_schema", null_val()),
				("default_character_set_name", null_val()),
				("sql_path", null_val()),
			]),
			row(vec![
				("catalog_name", str_val("test")),
				("schema_name", str_val("information_schema")),
				("schema_owner", str_val("root")),
				("default_character_set_catalog", null_val()),
				("default_character_set_schema", null_val()),
				("default_character_set_name", null_val()),
				("sql_path", null_val()),
			]),
			row(vec![
				("catalog_name", str_val("test")),
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
			["tables", "columns", "schemata", "views"]
				.iter()
				.map(|&vname| {
					row(vec![
						("table_catalog", str_val("test")),
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
