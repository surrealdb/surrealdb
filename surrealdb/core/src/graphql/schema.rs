//! GraphQL schema generation and type conversion.
//!
//! This is the main orchestration module for the GraphQL subsystem.  The entry
//! point is [`generate_schema`], which reads database metadata (tables, fields,
//! functions, access definitions) and produces a complete
//! `async_graphql::dynamic::Schema`.
//!
//! ## Key responsibilities
//!
//! - **Schema orchestration** -- assembles Query, Mutation, and type definitions by delegating to
//!   [`super::tables`], [`super::mutations`], [`super::functions`], and [`super::auth`].
//! - **Type mapping** -- [`kind_to_type`] converts SurrealDB `Kind` descriptors to GraphQL
//!   `TypeRef`s; [`graphql_to_sql_kind`] does the reverse conversion for input values.
//! - **Value conversion** -- [`sql_value_to_graphql_value`] converts SurrealDB runtime values to
//!   GraphQL output values.
//! - **Geometry support** -- registers GeoJSON-compatible output and input types for all geometry
//!   variants.
//! - **Custom scalars** -- registers scalars like `uuid`, `decimal`, `datetime`, `duration`,
//!   `bytes`, `object`, `any`, `JSON`, and `null`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
	Enum, Field, FieldFuture, FieldValue, InputObject, InputValue, Interface, InterfaceField,
	Object, Scalar, Schema, Type, TypeRef, Union,
};
use async_graphql::{Name, Value as GraphqlValue};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use serde_json::Number;
use surrealdb_kvs::TransactionType;
use surrealdb_types::ToSql;

use super::auth::add_auth_mutations;
use super::error::{GraphqlError, resolver_error};
#[cfg(debug_assertions)]
use super::ext::ValidatorExt;
use crate::catalog::providers::{AuthorisationProvider, DatabaseProvider, TableProvider};
use crate::catalog::{
	DatabaseId, FieldDefinition, GraphQLConfig, GraphQLFunctionsConfig, GraphQLIntrospectionConfig,
	GraphQLTablesConfig, NamespaceId,
};
use crate::dbs::Session;
use crate::expr::kind::{GeometryKind, KindLiteral};
use crate::expr::{Expr, Kind, Literal};
use crate::graphql::error::{internal_error, schema_error, type_error};
use crate::graphql::functions::process_fns;
use crate::graphql::mutations::process_mutations;
use crate::graphql::relations::collect_relations;
use crate::graphql::subscriptions::process_subscriptions;
use crate::graphql::tables::{process_tbs, register_filter_helper_types};
use crate::kvs::{Datastore, Transaction};
use crate::val::{
	Array as SurArray, Geometry as SurGeometry, Number as SurNumber, Object as SurObject,
	RecordId as SurRecordId, RecordIdKey as SurRecordIdKey, Set as SurSet, TableName,
	Value as SurValue,
};

/// Default maximum query nesting depth when `DEFINE CONFIG GRAPHQL` leaves `DEPTH` unset.
pub(crate) const DEFAULT_DEPTH_LIMIT: usize = 16;

/// Default maximum query complexity when `DEFINE CONFIG GRAPHQL` leaves `COMPLEXITY` unset.
pub(crate) const DEFAULT_COMPLEXITY_LIMIT: usize = 1000;

/// Set of table names exposed by the generated GraphQL schema, stored in schema data
/// so runtime resolvers can refuse to dereference or return records from unexposed tables.
#[derive(Clone)]
pub(crate) struct ExposedTables(pub(crate) Arc<HashSet<TableName>>);

/// Rewrites `Kind::Record(tables)` variants so that only tables present in
/// `exposed_tables` are retained as concrete GraphQL table types.
///
/// When any referenced table is excluded from the GraphQL schema, the record
/// kind becomes nullable (wrapping in `Kind::Either` with `Kind::None`) so runtime
/// resolvers can return `null` for records belonging to excluded tables, and
/// widens to `Kind::Record(vec![])` when all target tables are excluded rather
/// than referencing an unregistered GraphQL object type.
#[allow(clippy::wildcard_enum_match_arm)]
pub(crate) fn sanitize_kind_for_exposed_tables(
	kind: Kind,
	exposed_tables: &HashSet<TableName>,
) -> Kind {
	match kind {
		Kind::Record(mut tables) => {
			if !tables.is_empty() {
				let prev_len = tables.len();
				tables.retain(|t| exposed_tables.contains(t));
				if tables.len() < prev_len {
					return Kind::Either(vec![Kind::None, Kind::Record(tables)]);
				}
			}
			Kind::Record(tables)
		}
		Kind::Array(inner, len) => {
			Kind::Array(Box::new(sanitize_kind_for_exposed_tables(*inner, exposed_tables)), len)
		}
		Kind::Set(inner, len) => {
			Kind::Set(Box::new(sanitize_kind_for_exposed_tables(*inner, exposed_tables)), len)
		}
		Kind::Either(kinds) => {
			let mut out = Vec::with_capacity(kinds.len());
			for k in kinds {
				let sanitized = sanitize_kind_for_exposed_tables(k, exposed_tables);
				if let Kind::Either(inner) = sanitized {
					for ik in inner {
						if !out.contains(&ik) {
							out.push(ik);
						}
					}
				} else if !out.contains(&sanitized) {
					out.push(sanitized);
				}
			}
			Kind::Either(out)
		}
		other => other,
	}
}

/// Shared context for accessing table metadata during schema generation.
///
/// Groups the transaction, namespace/database identifiers, and datastore
/// reference that are threaded through `process_tbs` and `process_mutations`.
pub(crate) struct SchemaContext<'a> {
	pub tx: &'a Transaction,
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub datastore: &'a Arc<Datastore>,
	/// The kind a value of each type of the database has, by the type's name.
	pub named_types: HashMap<String, Kind>,
}

impl SchemaContext<'_> {
	/// The field definitions of the table `tb`, each with its kind read through the types it
	/// names ([`resolve_named_kinds`]).
	pub(crate) async fn table_fields(
		&self,
		tb: &TableName,
	) -> Result<Arc<[FieldDefinition]>, GraphqlError> {
		let fds = self.tx.all_tb_fields(self.ns, self.db, tb, None).await?;
		Ok(resolve_field_kinds(fds, &self.named_types))
	}
}

/// The kind a value of each type of the database `ns`/`db` has, by the type's name.
async fn named_type_kinds(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
) -> Result<HashMap<String, Kind>, GraphqlError> {
	use crate::catalog::providers::TypeProvider;
	let types = tx.all_db_types(ns, db, None).await?;
	Ok(types.iter().map(|definition| (definition.name.to_string(), definition.kind())).collect())
}

/// `kind` with each named type it holds replaced by the kind of its definition in `types`, read
/// the same way through the types that kind names, so that a field of a type maps to GraphQL as
/// the type's body does: an alias as what it names, and a union of string labels as the union of
/// string literals it holds. A union a replacement places in a union is merged into it. A type
/// `types` does not hold, and one past [`crate::catalog::UNION_ALIAS_DEPTH`] types deep, stays
/// named and maps to `any`.
pub(crate) fn resolve_named_kinds(kind: &Kind, types: &HashMap<String, Kind>) -> Kind {
	fn resolve(kind: &Kind, types: &HashMap<String, Kind>, depth: usize) -> Kind {
		if let Kind::Either(members) = kind {
			let mut merged = Vec::with_capacity(members.len());
			for member in members {
				let resolved = resolve(member, types, depth);
				if let Kind::Either(inner) = resolved {
					merged.extend(inner);
				} else {
					merged.push(resolved);
				}
			}
			return Kind::Either(merged);
		}
		if let Kind::Array(inner, len) = kind {
			return Kind::Array(Box::new(resolve(inner, types, depth)), *len);
		}
		if let Kind::Set(inner, len) = kind {
			return Kind::Set(Box::new(resolve(inner, types, depth)), *len);
		}
		kind.replace_named_types(&mut |name| {
			let body = types.get(name)?;
			let depth = depth.checked_sub(1)?;
			Some(resolve(body, types, depth))
		})
	}
	resolve(kind, types, crate::catalog::UNION_ALIAS_DEPTH)
}

/// `fds` with the kind of each field resolved by [`resolve_named_kinds`]. The definitions are
/// returned as they are when none of their kinds names a type.
pub(crate) fn resolve_field_kinds(
	fds: Arc<[FieldDefinition]>,
	types: &HashMap<String, Kind>,
) -> Arc<[FieldDefinition]> {
	if !fds.iter().any(|fd| fd.field_kind.as_ref().is_some_and(Kind::has_named_type)) {
		return fds;
	}
	fds.iter()
		.map(|fd| {
			let mut fd = fd.clone();
			if let Some(kind) = fd.field_kind.as_ref()
				&& kind.has_named_type()
			{
				fd.field_kind = Some(resolve_named_kinds(kind, types));
			}
			fd
		})
		.collect()
}

/// Generate a complete GraphQL schema from database metadata.
///
/// Reads table definitions, field types, function signatures, and access
/// definitions for the namespace/database specified in `session`, then builds
/// a dynamic `async_graphql::Schema` with:
///
/// - A **Query** root containing list and get fields for each table, plus custom function fields
///   and a generic `_get` field.
/// - An optional **Mutation** root containing CRUD operations for each table and authentication
///   mutations (`signIn`, `signUp`).
/// - All supporting types: table Object types, filter/order inputs, scalars, geometry types, enums,
///   unions, and interfaces.
///
/// The `graphql_config` controls which tables and functions are exposed, and
/// sets depth/complexity limits and introspection behaviour.
pub async fn generate_schema(
	datastore: &Arc<Datastore>,
	session: &Session,
	graphql_config: GraphQLConfig,
) -> Result<Schema, GraphqlError> {
	let kvs = datastore;
	let tx = kvs.transaction(TransactionType::Read).await?;
	let ns = session.ns.as_ref().ok_or(GraphqlError::UnspecifiedNamespace)?;
	let db = session.db.as_ref().ok_or(GraphqlError::UnspecifiedDatabase)?;

	let db_def = match tx.get_db_by_name(ns, db, None).await? {
		Some(db) => db,
		None => return Err(GraphqlError::NotConfigured),
	};

	// Get all tables
	let tbs = if let GraphQLTablesConfig::None = graphql_config.tables {
		None
	} else {
		let tbs = tx.all_tb(db_def.namespace_id, db_def.database_id, None).await?;
		// Lightweight relations are skipped: their edges are record-less,
		// and the GraphQL layer's resolvers assume stored records. They
		// stay reachable through the tables they connect.
		let tbs: Arc<[_]> = tbs
			.iter()
			.filter(|t| crate::kvs::lightweight::lightweight_relation(&t.table_type).is_none())
			.cloned()
			.collect::<Vec<_>>()
			.into();

		match graphql_config.tables {
			GraphQLTablesConfig::None => None,
			GraphQLTablesConfig::Auto => Some(tbs),
			GraphQLTablesConfig::Include(inc) => Some(
				tbs.iter()
					.filter(|t| inc.iter().any(|x| x.as_str() == t.name.as_str()))
					.cloned()
					.collect(),
			),
			GraphQLTablesConfig::Exclude(exc) => Some(
				tbs.iter()
					.filter(|t| !exc.iter().any(|x| x.as_str() == t.name.as_str()))
					.cloned()
					.collect(),
			),
		}
	};

	// Get all functions
	let fns = if let GraphQLFunctionsConfig::None = graphql_config.functions {
		None
	} else {
		let fns = tx.all_db_functions(db_def.namespace_id, db_def.database_id, None).await?;
		match graphql_config.functions {
			GraphQLFunctionsConfig::None => None,
			GraphQLFunctionsConfig::Auto => Some(fns),
			GraphQLFunctionsConfig::Include(inc) => Some(
				fns.iter()
					.filter(|f| inc.iter().any(|n| n.as_str() == f.name.as_str()))
					.cloned()
					.collect(),
			),
			GraphQLFunctionsConfig::Exclude(exc) => Some(
				fns.iter()
					.filter(|f| !exc.iter().any(|n| n.as_str() == f.name.as_str()))
					.cloned()
					.collect(),
			),
		}
	};

	let mut query = Object::new("Query");
	let mut types: Vec<Type> = Vec::new();

	trace!(ns, db, ?tbs, ?fns, "generating schema");

	let has_tables = tbs.as_ref().is_some_and(|t| !t.is_empty());
	let has_fns = fns.as_ref().is_some_and(|f| !f.is_empty());

	// Check if there's anything to expose via GraphQL
	if !has_tables && !has_fns {
		return Err(schema_error(
			"no items found in database: GraphQL requires at least one table or function",
		));
	}

	// Collect relation info from table definitions for relation field generation
	let relations = match &tbs {
		Some(tbs) => collect_relations(tbs),
		None => Vec::new(),
	};

	let named_types = named_type_kinds(&tx, db_def.namespace_id, db_def.database_id).await?;
	let schema_ctx = SchemaContext {
		tx: &tx,
		ns: db_def.namespace_id,
		db: db_def.database_id,
		datastore,
		named_types,
	};

	let mut mutation_obj: Option<Object> = None;
	let mut subscription_obj = None;
	let exposed_table_names: Arc<HashSet<TableName>> = Arc::new(
		tbs.as_ref().map_or_else(HashSet::new, |tbs| tbs.iter().map(|t| t.name.clone()).collect()),
	);

	match tbs {
		Some(ref tbs) if !tbs.is_empty() => {
			let mut table_fields: HashMap<TableName, Arc<[FieldDefinition]>> = HashMap::new();
			query = process_tbs(
				Arc::clone(tbs),
				query,
				&mut types,
				&schema_ctx,
				&relations,
				&mut table_fields,
			)
			.await?;

			// Generate mutations for all tables
			mutation_obj = Some(process_mutations(Arc::clone(tbs), &mut types, &schema_ctx).await?);
			subscription_obj = process_subscriptions(&tbs[..], &table_fields);
		}
		_ => {}
	}

	// Generate auth mutations (signIn/signUp) from access definitions
	{
		let accesses = tx.all_db_accesses(db_def.namespace_id, db_def.database_id, None).await?;
		if !accesses.is_empty() {
			let mut auth_mutation = mutation_obj.take().unwrap_or_else(|| Object::new("Mutation"));
			auth_mutation = add_auth_mutations(auth_mutation, &accesses, ns, db, datastore);
			mutation_obj = Some(auth_mutation);
		}
	}

	if let Some(fns) = fns {
		// A function's argument and return kinds map through the types they name, as a field's do.
		let fns: Arc<[crate::catalog::FunctionDefinition]> = fns
			.iter()
			.map(|fnd| {
				let mut fnd = fnd.clone();
				for (_, kind) in &mut fnd.args {
					*kind = resolve_named_kinds(kind, &schema_ctx.named_types);
				}
				if let Some(kind) = fnd.returns.as_mut() {
					*kind = resolve_named_kinds(kind, &schema_ctx.named_types);
				}
				fnd
			})
			.collect();
		query = process_fns(fns, query, &mut types, datastore, &exposed_table_names).await?;
	}

	// Register all geometry-related types (enum, object types, union, input types)
	register_geometry_types(&mut types);

	// Register shared helper types for the advanced filter operators
	// (`nearest`, `similarity`, `matches`, `call`) — see #7312.
	register_filter_helper_types(&mut types);

	trace!("current Query object for schema: {:?}", query);

	let has_mutation = mutation_obj.is_some();
	let mutation_name = if has_mutation {
		Some("Mutation")
	} else {
		None
	};
	let subscription_name = if subscription_obj.is_some() {
		Some("Subscription")
	} else {
		None
	};

	let mut schema = Schema::build("Query", mutation_name, subscription_name).register(query);

	// Apply depth and complexity limits from the GraphQL config, falling back
	// to bounded defaults when unset.
	let depth = graphql_config.depth_limit.map_or(DEFAULT_DEPTH_LIMIT, |d| d as usize);
	let complexity =
		graphql_config.complexity_limit.map_or(DEFAULT_COMPLEXITY_LIMIT, |c| c as usize);
	schema = schema
		.limit_depth(depth)
		.limit_complexity(complexity)
		.data(ExposedTables(exposed_table_names));
	// Disable introspection when configured to do so
	if matches!(graphql_config.introspection, GraphQLIntrospectionConfig::None) {
		schema = schema.disable_introspection();
	}

	if let Some(mutation) = mutation_obj {
		schema = schema.register(mutation);
	}
	if let Some(subscription) = subscription_obj {
		schema = schema.register(subscription);
	}
	for ty in types {
		trace!("adding type: {ty:?}");
		schema = schema.register(ty);
	}

	macro_rules! scalar_debug_validated {
		($schema:ident, $name:expr_2021, $kind:expr_2021) => {
			scalar_debug_validated!(
				$schema,
				$name,
				$kind,
				::std::option::Option::<&str>::None,
				::std::option::Option::<&str>::None
			)
		};
		($schema:ident, $name:expr_2021, $kind:expr_2021, $desc:literal) => {
			scalar_debug_validated!(
				$schema,
				$name,
				$kind,
				std::option::Option::Some($desc),
				std::option::Option::<&str>::None
			)
		};
		($schema:ident, $name:expr_2021, $kind:expr_2021, $desc:literal, $url:literal) => {
			scalar_debug_validated!(
				$schema,
				$name,
				$kind,
				std::option::Option::Some($desc),
				Some($url)
			)
		};
		($schema:ident, $name:expr_2021, $kind:expr_2021, $desc:expr_2021, $url:expr_2021) => {{
			let new_type = Type::Scalar({
				let mut tmp = Scalar::new($name);
				if let Some(desc) = $desc {
					tmp = tmp.description(desc);
				}
				if let Some(url) = $url {
					tmp = tmp.specified_by_url(url);
				}
				#[cfg(debug_assertions)]
				tmp.add_validator(|v| graphql_to_sql_kind(v, $kind).is_ok());
				tmp
			});
			$schema = $schema.register(new_type);
		}};
	}

	scalar_debug_validated!(
		schema,
		"uuid",
		Kind::Uuid,
		"String encoded UUID",
		"https://datatracker.ietf.org/doc/html/rfc4122"
	);

	// `decimal` stands for both `decimal` and `bigdecimal`, so it admits the wider of the two;
	// the field's own kind refuses a value too wide for a `decimal` when it is converted.
	scalar_debug_validated!(schema, "decimal", Kind::BigDecimal);
	scalar_debug_validated!(schema, "number", Kind::Number);
	scalar_debug_validated!(schema, "u32", Kind::U32);
	scalar_debug_validated!(schema, "u64", Kind::U64);
	scalar_debug_validated!(schema, "null", Kind::Null);
	scalar_debug_validated!(schema, "datetime", Kind::Datetime);
	scalar_debug_validated!(schema, "duration", Kind::Duration);
	scalar_debug_validated!(schema, "date", Kind::Date, "ISO 8601 calendar date, YYYY-MM-DD");
	scalar_debug_validated!(
		schema,
		"time",
		Kind::Time,
		"Time of day without a zone, HH:MM:SS[.fffffffff], from 00:00:00 to 24:00:00"
	);
	scalar_debug_validated!(
		schema,
		"timestamp",
		Kind::Timestamp,
		"Date and time without a zone, YYYY-MM-DDTHH:MM:SS[.fffffffff]"
	);
	scalar_debug_validated!(
		schema,
		"interval",
		Kind::Interval,
		"Calendar interval of months, days and microseconds, in ISO 8601 or PostgreSQL text"
	);
	scalar_debug_validated!(schema, "bytes", Kind::Bytes);
	scalar_debug_validated!(schema, "object", Kind::Object);
	scalar_debug_validated!(schema, "any", Kind::Any);

	// JSON scalar: accepts arbitrary JSON values (including objects and arrays)
	// as input arguments. Used for dynamic data like auth variables.
	schema = schema.register(Type::Scalar(
		Scalar::new("JSON")
			.description("Arbitrary JSON value (object, array, string, number, or boolean)"),
	));

	let id_interface =
		Interface::new("record").field(InterfaceField::new("id", TypeRef::named_nn(TypeRef::ID)));
	schema = schema.register(id_interface);

	let relation_interface = Interface::new("relation")
		.field(InterfaceField::new("id", TypeRef::named_nn(TypeRef::ID)))
		.field(InterfaceField::new("in", TypeRef::named_nn("record")))
		.field(InterfaceField::new("out", TypeRef::named_nn("record")))
		.implement("record");
	schema = schema.register(relation_interface);

	schema
		.finish()
		.map_err(|e| schema_error(format!("there was an error generating schema: {e:?}")))
}

/// Convert a SurrealDB runtime value to a GraphQL output value.
///
/// Handles all common value types: booleans, numbers (int/float/decimal),
/// strings, datetimes, durations, UUIDs, arrays, objects, geometry, bytes
/// (base64-encoded), and record IDs.  Unrecognised variants fall back to
/// their string representation.
pub(crate) fn sql_value_to_graphql_value(v: SurValue) -> Result<GraphqlValue, GraphqlError> {
	let out = match v {
		SurValue::None => GraphqlValue::Null,
		SurValue::Null => GraphqlValue::Null,
		SurValue::Bool(b) => GraphqlValue::Boolean(b),
		SurValue::Number(n) => match n {
			SurNumber::Int(i) => GraphqlValue::Number(i.into()),
			SurNumber::I8(i) => GraphqlValue::Number((i as i64).into()),
			SurNumber::I16(i) => GraphqlValue::Number((i as i64).into()),
			SurNumber::I32(i) => GraphqlValue::Number((i as i64).into()),
			SurNumber::U8(i) => GraphqlValue::Number((i as i64).into()),
			SurNumber::U16(i) => GraphqlValue::Number((i as i64).into()),
			SurNumber::U32(i) => GraphqlValue::Number((i as i64).into()),
			SurNumber::U64(i) => GraphqlValue::Number(i.into()),
			// GraphQL numbers are finite, and `NaN`, `Infinity` and
			// `-Infinity` are all answers SurrealQL gives — the extremes of no
			// values, a mean of none. None of them has a number to send, so
			// they are sent as null rather than failing the whole query.
			SurNumber::Float(f) => match Number::from_f64(f) {
				Some(n) => GraphqlValue::Number(n),
				None => GraphqlValue::Null,
			},
			SurNumber::F16(f) => match Number::from_f64(f.to_f64()) {
				Some(n) => GraphqlValue::Number(n),
				None => GraphqlValue::Null,
			},
			SurNumber::F32(f) => match f
				.is_finite()
				.then(|| f.to_string().parse::<f64>().ok())
				.flatten()
				.and_then(Number::from_f64)
			{
				Some(n) => GraphqlValue::Number(n),
				None => GraphqlValue::Null,
			},
			num @ SurNumber::Decimal(_) => GraphqlValue::String(num.to_string()),
			// A bigdecimal is sent as its plain decimal text, without the literal suffix that its
			// `Display` form carries.
			SurNumber::BigDecimal(d) => GraphqlValue::String(d.to_string()),
		},
		SurValue::String(s) => GraphqlValue::String(s.into_string()),
		SurValue::Duration(d) => GraphqlValue::String(d.to_string()),
		SurValue::Datetime(d) => GraphqlValue::String(d.to_rfc3339()),
		SurValue::Date(d) => GraphqlValue::String(d.to_string()),
		SurValue::Time(d) => GraphqlValue::String(d.to_string()),
		SurValue::Timestamp(d) => GraphqlValue::String(d.to_string()),
		SurValue::Interval(d) => GraphqlValue::String(d.to_string()),
		SurValue::Uuid(uuid) => GraphqlValue::String(uuid.to_string()),
		SurValue::Array(a) => {
			let items: Result<Vec<GraphqlValue>, GraphqlError> =
				a.into_iter().map(sql_value_to_graphql_value).collect();
			GraphqlValue::List(items?)
		}
		SurValue::Object(o) => {
			let entries: Result<IndexMap<Name, GraphqlValue>, GraphqlError> =
				o.0.into_iter()
					.map(|(k, v)| sql_value_to_graphql_value(v).map(|gv| (Name::new(k), gv)))
					.collect();
			GraphqlValue::Object(entries?)
		}
		SurValue::Geometry(ref g) => return geometry_to_graphql_object(g),
		SurValue::Bytes(b) => {
			use base64::Engine;
			GraphqlValue::String(base64::engine::general_purpose::STANDARD.encode(b.into_inner()))
		}
		SurValue::RecordId(t) => GraphqlValue::String(record_id_to_raw(&t)),
		// Fallback: convert unhandled variants to their string representation
		// rather than returning an error, to be resilient to new Value variants
		v @ (SurValue::Set(_)
		| SurValue::Table(_)
		| SurValue::File(_)
		| SurValue::Regex(_)
		| SurValue::Range(_)
		| SurValue::Closure(_)) => GraphqlValue::String(v.to_raw_string()),
	};
	Ok(out)
}

/// Convert a SurrealDB runtime value to GraphQL, using field kind metadata when available.
///
/// For string-literal `Either` kinds, this emits a GraphQL enum token instead
/// of a raw string so enum fields resolve correctly.
pub(crate) fn sql_value_to_graphql_value_with_kind(
	v: SurValue,
	kind: Option<&Kind>,
	enum_scope: Option<&str>,
) -> Result<GraphqlValue, GraphqlError> {
	if let SurValue::String(s) = &v
		&& let Some(Kind::Either(ks)) = kind
	{
		for k in ks {
			if let Kind::Literal(KindLiteral::String(lit)) = k
				&& lit.as_str() == s.as_str()
			{
				return Ok(GraphqlValue::Enum(Name::new(literal_enum_item_name(enum_scope, lit))));
			}
		}
	}
	sql_value_to_graphql_value(v)
}

/// Formats a `RecordId` as a raw string without SQL-specific escaping.
///
/// Produces `table:key` where the key is the raw value (no backtick/angle-bracket
/// escaping). This is the format GraphQL clients expect, e.g. `"person:alice"`,
/// `"item:1"`.
fn record_id_to_raw(t: &SurRecordId) -> String {
	let key_str = match &t.key {
		SurRecordIdKey::Number(n) => n.to_string(),
		SurRecordIdKey::String(s) => s.to_string(),
		SurRecordIdKey::Uuid(u) => u.to_string(),
		// For complex keys (arrays, objects, ranges), fall back to SQL format
		SurRecordIdKey::Array(_) | SurRecordIdKey::Object(_) | SurRecordIdKey::Range(_) => {
			t.key.to_sql()
		}
	};
	format!("{}:{}", t.table, key_str)
}

fn sanitize_graphql_identifier_component(raw: &str) -> String {
	let mut out = String::with_capacity(raw.len() + 1);
	let mut last_was_underscore = false;
	for c in raw.chars() {
		if c.is_ascii_alphanumeric() {
			out.push(c.to_ascii_uppercase());
			last_was_underscore = false;
		} else if !last_was_underscore && !out.is_empty() {
			out.push('_');
			last_was_underscore = true;
		}
	}
	while out.ends_with('_') {
		out.pop();
	}
	if out.is_empty() || !out.as_bytes()[0].is_ascii_alphabetic() {
		out.insert(0, 'V');
	}
	out
}

fn literal_enum_item_name(scope: Option<&str>, literal: &str) -> String {
	let literal_component = sanitize_graphql_identifier_component(literal);
	match scope {
		Some(s) if !s.is_empty() => {
			let scope_component = sanitize_graphql_identifier_component(s);
			format!("{scope_component}_{literal_component}")
		}
		_ => format!("VALUE_{literal_component}"),
	}
}

fn literal_enum_type_name(scope: Option<&str>, literals: &[String]) -> String {
	let parts: Vec<String> =
		literals.iter().map(|s| sanitize_graphql_identifier_component(s)).collect();
	let joined = parts.join("_OR_");
	match scope {
		Some(s) if !s.is_empty() => {
			let scope_component = sanitize_graphql_identifier_component(s);
			format!("{scope_component}_ENUM_{joined}")
		}
		_ => format!("Literal_{joined}"),
	}
}

fn enum_token_to_literal(ks: &[Kind], token: &str, enum_scope: Option<&str>) -> Option<String> {
	for kind in ks {
		if let Kind::Literal(KindLiteral::String(lit)) = kind {
			let expected = literal_enum_item_name(enum_scope, lit);
			if expected == token {
				return Some(lit.as_str().to_owned());
			}
		}
	}
	None
}

fn literal_to_base_kind(literal: &KindLiteral) -> Kind {
	match literal {
		KindLiteral::String(_) => Kind::String,
		KindLiteral::Integer(_) => Kind::Int,
		KindLiteral::Float(_) => Kind::Float,
		KindLiteral::Decimal(_) => Kind::Decimal,
		KindLiteral::Duration(_) => Kind::Duration,
		KindLiteral::Bool(_) => Kind::Bool,
		KindLiteral::Object(_) => Kind::Object,
		KindLiteral::Array(kinds) => {
			let len = Some(kinds.len() as u64);
			if let Some(first) = kinds.first()
				&& kinds.iter().all(|k| k == first)
			{
				Kind::Array(Box::new(first.clone()), len)
			} else {
				// tuple-like literal arrays are represented as array<any> for GraphQL then
				// validated
				Kind::Array(Box::new(Kind::Any), len)
			}
		}
	}
}

/// Map a SurrealDB [`Kind`] to a GraphQL [`TypeRef`].
///
/// This is the central type-mapping function: it translates SurrealDB's type
/// system (int, float, string, record, array, geometry, either/union, etc.)
/// into the corresponding `async_graphql` dynamic type references.
///
/// - `is_input` controls whether the type is used in an input position (e.g. mutation arguments) or
///   an output position (e.g. query return types). Geometry types produce `*Input` type names in
///   input context.
/// - Union / Either types with multiple members are registered as GraphQL `Union` types;
///   single-member eithers are simplified.
/// - `option<T>` (represented as `Either([None, T])`) produces a nullable type reference.
pub fn kind_to_type(
	kind: Kind,
	types: &mut Vec<Type>,
	is_input: bool,
) -> Result<TypeRef, GraphqlError> {
	kind_to_type_with_enum_prefix(kind, types, is_input, None)
}

/// Maximum allowed depth of nested `array<...>` types in the generated GraphQL
/// schema. Beyond this depth the inner type is replaced with the `JSON` scalar
/// so that the resulting `ofType` chain stays within the 7-level limit enforced
/// by the standard GraphQL introspection query.
///
/// Worst case for a single array layer is `NonNull(List(NonNull(T)))` which adds
/// 2 `ofType` hops; depth 3 produces a 7-hop chain (`[[[T!]!]!]!`), which is the
/// hard ceiling. Anything past depth 3 collapses to a single `JSON!` instead.
const MAX_ARRAY_DEPTH: usize = 3;

/// Map a SurrealDB [`Kind`] to a GraphQL [`TypeRef`], with optional enum naming scope.
///
/// When `enum_scope` is provided, string-literal unions (`Kind::Either` with
/// `Kind::Literal(String)`) use this scope when generating enum type/item names.
pub fn kind_to_type_with_enum_prefix(
	kind: Kind,
	types: &mut Vec<Type>,
	is_input: bool,
	enum_scope: Option<&str>,
) -> Result<TypeRef, GraphqlError> {
	kind_to_type_inner(kind, types, is_input, enum_scope, 0)
}

fn kind_to_type_inner(
	kind: Kind,
	types: &mut Vec<Type>,
	is_input: bool,
	enum_scope: Option<&str>,
	array_depth: usize,
) -> Result<TypeRef, GraphqlError> {
	let optional = kind.can_be_none();
	let out_ty = match kind {
		Kind::Any | Kind::Named(_) => TypeRef::named("any"),
		Kind::None => TypeRef::named("none"),
		Kind::Null => TypeRef::named("null"),
		Kind::Bool => TypeRef::named(TypeRef::BOOLEAN),
		Kind::Bytes => TypeRef::named("bytes"),
		Kind::Datetime => TypeRef::named("datetime"),
		Kind::Date => TypeRef::named("date"),
		Kind::Time => TypeRef::named("time"),
		Kind::Timestamp => TypeRef::named("timestamp"),
		Kind::Interval => TypeRef::named("interval"),
		Kind::Enum(_) => TypeRef::named(TypeRef::STRING),
		Kind::Decimal | Kind::BigDecimal => TypeRef::named("decimal"),
		Kind::Duration => TypeRef::named("duration"),
		Kind::Float | Kind::F16 | Kind::F32 => TypeRef::named(TypeRef::FLOAT),
		Kind::Int | Kind::I8 | Kind::I16 | Kind::I32 | Kind::U8 | Kind::U16 => {
			TypeRef::named(TypeRef::INT)
		}
		Kind::U32 => TypeRef::named("u32"),
		Kind::U64 => TypeRef::named("u64"),
		Kind::Number => TypeRef::named("number"),
		Kind::Object => TypeRef::named("object"),
		Kind::Regex => return Err(schema_error("Kind::Regex is not yet supported")),
		Kind::String => TypeRef::named(TypeRef::STRING),
		Kind::Uuid => TypeRef::named("uuid"),
		Kind::Table(ref _t) => TypeRef::named(kind.to_sql()),
		Kind::Record(mut tables) => match tables.len() {
			0 => TypeRef::named("record"),
			1 => TypeRef::named(tables.pop().expect("single table in record kind").into_string()),
			_ => {
				let ty_name = tables.join("_or_");

				let mut tmp_union = Union::new(ty_name.clone())
					.description(format!("A record which is one of: {}", tables.join(", ")));
				for n in tables {
					tmp_union = tmp_union.possible_type(n.into_string());
				}

				types.push(Type::Union(tmp_union));
				TypeRef::named(ty_name)
			}
		},
		Kind::Geometry(ref geo_kinds) => {
			if is_input {
				// Input context: return InputObject type names
				match geo_kinds.len() {
					0 => TypeRef::named("GeometryInput"),
					1 => TypeRef::named(geometry_kind_to_graphql_input_type_name(&geo_kinds[0])),
					_ => {
						// GraphQL doesn't support union input types, so we use
						// the unified GeometryInput for multi-kind geometry fields
						TypeRef::named("GeometryInput")
					}
				}
			} else {
				// Output context: return Object type / Union names
				match geo_kinds.len() {
					0 => TypeRef::named("Geometry"),
					1 => TypeRef::named(geometry_kind_to_graphql_type_name(&geo_kinds[0])),
					_ => {
						// Create a partial union of the allowed geometry types
						let names: Vec<&str> =
							geo_kinds.iter().map(geometry_kind_to_graphql_type_name).collect();
						let ty_name = names.join("_or_");

						let mut partial_union = Union::new(ty_name.clone()).description(format!(
							"A geometry which is one of: {}",
							names.join(", ")
						));
						for name in &names {
							partial_union = partial_union.possible_type(*name);
						}
						types.push(Type::Union(partial_union));
						TypeRef::named(ty_name)
					}
				}
			}
		}
		Kind::Either(ks) => {
			// A `null` member admits a stored NULL just as `none` admits a
			// missing value, so either makes the field nullable.
			let optional = optional || ks.iter().any(|k| matches!(k, Kind::Null));
			// Strip None/Null from the Either variants — they are already handled
			// by the `optional` flag at the top of kind_to_type() which makes the
			// resulting TypeRef nullable. This prevents spurious unions like
			// `none_or_foo` for `option<record<foo>>`.
			let ks: Vec<Kind> =
				ks.into_iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).collect();

			// If nothing remains after stripping None/Null, it's just a nullable null.
			if ks.is_empty() {
				return Ok(TypeRef::named("null"));
			}

			// If only one kind remains after stripping None/Null, delegate directly
			// to avoid creating a single-member union.
			if ks.len() == 1 {
				let inner = ks.into_iter().next().expect("checked len == 1");
				let inner_ty = kind_to_type_inner(inner, types, is_input, enum_scope, array_depth)?;
				// Unwrap any NonNull wrapper — the outer optional flag will re-apply
				// nullability as needed.
				return Ok(match optional {
					true => unwrap_type(inner_ty),
					false => inner_ty,
				});
			}

			let (ls, others): (Vec<Kind>, Vec<Kind>) =
				ks.into_iter().partition(|k| matches!(k, Kind::Literal(KindLiteral::String(_))));

			let enum_ty = if !ls.is_empty() {
				let vals: Vec<String> = ls
					.into_iter()
					.map(|l| {
						let Kind::Literal(KindLiteral::String(out)) = l else {
							unreachable!(
								"just checked that this is a Kind::Literal(Literal::String(_))"
							);
						};
						out.into_string()
					})
					.collect();

				let enum_name = literal_enum_type_name(enum_scope, &vals);
				let enum_items: Vec<String> =
					vals.iter().map(|v| literal_enum_item_name(enum_scope, v)).collect();

				let mut tmp = Enum::new(enum_name);
				tmp = tmp.items(enum_items);

				let enum_ty = tmp.type_name().to_string();

				types.push(Type::Enum(tmp));
				if others.is_empty() {
					return Ok(nullability(TypeRef::named(enum_ty), optional));
				}
				Some(enum_ty)
			} else {
				None
			};

			let pos_names: Result<Vec<TypeRef>, GraphqlError> = others
				.into_iter()
				.map(|k| kind_to_type_inner(k, types, is_input, enum_scope, array_depth))
				.collect();
			let pos_names: Vec<String> = pos_names?.into_iter().map(|tr| tr.to_string()).collect();
			let ty_name = pos_names.join("_or_");

			let mut tmp_union = Union::new(ty_name.clone());
			for n in pos_names {
				tmp_union = tmp_union.possible_type(n);
			}

			if let Some(ty) = enum_ty {
				tmp_union = tmp_union.possible_type(ty);
			}

			types.push(Type::Union(tmp_union));
			return Ok(nullability(TypeRef::named(ty_name), optional));
		}
		Kind::Set(_, _) => return Err(schema_error("Kind::Set is not yet supported")),
		Kind::Array(k, _) => {
			// Cap nested-array depth so the resulting chain stays within the
			// 7-level `ofType` limit of the standard GraphQL introspection
			// query. Each `array<…>` layer adds two `ofType` hops
			// (`NonNull(List(…))`), so `MAX_ARRAY_DEPTH = 3` permits up to
			// `[[[T!]!]!]!` (7 hops to a named leaf). When the *current* layer
			// would push us past that, the entire remaining sub-tree collapses
			// to the `JSON` scalar — a single named type, contributing no
			// extra hops. The wire format is unchanged: nested numeric arrays
			// continue to be encoded as JSON arrays.
			if array_depth >= MAX_ARRAY_DEPTH {
				TypeRef::named("JSON")
			} else {
				TypeRef::List(Box::new(kind_to_type_inner(
					*k,
					types,
					is_input,
					enum_scope,
					array_depth + 1,
				)?))
			}
		}
		Kind::Function(_, _) => return Err(schema_error("Kind::Function is not yet supported")),
		Kind::Range => return Err(schema_error("Kind::Range is not yet supported")),
		Kind::Literal(literal) => {
			return kind_to_type_inner(
				literal_to_base_kind(&literal),
				types,
				is_input,
				enum_scope,
				array_depth,
			);
		}
		Kind::File(_) => return Err(schema_error("Kind::File is not yet supported")),
	};

	let out = match optional {
		true => out_ty,
		false => TypeRef::NonNull(Box::new(out_ty)),
	};
	Ok(out)
}

/// `ty` as nullable when the kind admits `none`, otherwise non-null.
fn nullability(ty: TypeRef, optional: bool) -> TypeRef {
	match optional {
		true => ty,
		false => TypeRef::NonNull(Box::new(ty)),
	}
}

/// Strip `NonNull` wrappers from a [`TypeRef`], making it nullable.
///
/// Used when generating update input types (all fields become optional) and
/// when simplifying `option<T>` unions.
pub fn unwrap_type(ty: TypeRef) -> TypeRef {
	if let TypeRef::NonNull(t) = ty {
		unwrap_type(*t)
	} else {
		ty
	}
}

/// Render a [`TypeRef`] as a string suitable for use in a GraphQL identifier.
///
/// `TypeRef::Display` produces shapes like `[child!]!` that contain brackets
/// and exclamation marks — invalid characters for a GraphQL Name. This helper
/// flattens nullability and list nesting into underscore-joined alphabetic
/// segments so callers can build derived type names like
/// `_filter_list_child` from `array<record<child>>`.
pub fn filter_type_name(ty: &TypeRef) -> String {
	match ty {
		TypeRef::Named(n) => n.to_string(),
		TypeRef::NonNull(inner) => filter_type_name(inner),
		TypeRef::List(inner) => format!("list_{}", filter_type_name(inner)),
	}
}

/// Try to convert a GraphQL value using one kind from an `Either` type list.
///
/// Special token arms:
/// - `Kind::Array` / `Array` -- iterates all `Kind::Array(_, _)` variants in `$ks`.
/// - `Record` -- iterates all `Kind::Record(_)` variants in `$ks`.
/// - `AllNumbers` -- expands to tries for Int, Float, Decimal, Number.
/// - Any other expression -- checks containment then attempts conversion.
///
/// On success, `return Ok(out)` exits the enclosing function immediately.
macro_rules! either_try_kind {
	($ks:ident, $val:expr_2021, Kind::Array) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr_2021, Array) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr_2021, Record) => {
		for rec_kind in $ks.iter().filter(|k| matches!(k, Kind::Record(_))).cloned() {
			either_try_kind!($ks, $val, rec_kind);
		}
	};
	($ks:ident, $val:expr_2021, AllNumbers) => {
		either_try_kind!($ks, $val, Kind::Int);
		either_try_kind!($ks, $val, Kind::I8);
		either_try_kind!($ks, $val, Kind::I16);
		either_try_kind!($ks, $val, Kind::I32);
		either_try_kind!($ks, $val, Kind::U8);
		either_try_kind!($ks, $val, Kind::U16);
		either_try_kind!($ks, $val, Kind::U32);
		either_try_kind!($ks, $val, Kind::U64);
		either_try_kind!($ks, $val, Kind::Float);
		either_try_kind!($ks, $val, Kind::F16);
		either_try_kind!($ks, $val, Kind::F32);
		either_try_kind!($ks, $val, Kind::Decimal);
		either_try_kind!($ks, $val, Kind::Number);
	};
	($ks:ident, $val:expr_2021, $kind:expr_2021) => {
		if $ks.contains(&$kind) {
			if let Ok(out) = graphql_to_sql_kind($val, $kind) {
				return Ok(out);
			}
		}
	};
}

macro_rules! either_try_kinds {
	($ks:ident, $val:expr_2021, $($kind:tt),+) => {
		$(either_try_kind!($ks, $val, $kind));+
	};
}

macro_rules! any_try_kind {
	($val:expr_2021, $kind:expr_2021) => {
		if let Ok(out) = graphql_to_sql_kind($val, $kind) {
			return Ok(out);
		}
	};
}
macro_rules! any_try_kinds {
	($val:expr_2021, $($kind:tt),+) => {
		$(any_try_kind!($val, $kind));+
	};
}

/// Convert a static RecordIdKeyLit to RecordIdKey
/// Only works for static literals (no expressions)
fn convert_static_record_id_key(
	key: crate::expr::RecordIdKeyLit,
) -> Result<SurRecordIdKey, GraphqlError> {
	use crate::expr::RecordIdKeyLit;
	match key {
		RecordIdKeyLit::Number(n) => Ok(SurRecordIdKey::Number(n)),
		RecordIdKeyLit::String(s) => Ok(SurRecordIdKey::String(s)),
		RecordIdKeyLit::Uuid(u) => Ok(SurRecordIdKey::Uuid(u)),
		RecordIdKeyLit::Array(exprs) => {
			let vals: Result<Vec<SurValue>, GraphqlError> =
				exprs.into_iter().map(convert_static_expr).collect();
			Ok(SurRecordIdKey::Array(SurArray(vals?)))
		}
		RecordIdKeyLit::Object(entries) => {
			// Collect into `BTreeMap<Strand, _>` directly so the
			// final `SurObject::from(map)` resolves to the zero-cost
			// `From<BTreeMap<Strand, Value>> for Object` impl
			// (a single field move). Going via
			// `BTreeMap<String, _>` would force a `Strand::into_string`
			// heap allocation per key and then re-convert every key
			// back to `Strand` inside `From<BTreeMap<String, Value>>`.
			let mut map = BTreeMap::new();
			for entry in entries {
				map.insert(entry.key, convert_static_expr(entry.value)?);
			}
			Ok(SurRecordIdKey::Object(SurObject::from(map)))
		}
		RecordIdKeyLit::Generate(_) => Err(resolver_error(
			"Generated RecordId keys (rand(), ulid(), uuid()) are not supported in GraphQL inputs",
		)),
		RecordIdKeyLit::Range(_) => {
			Err(resolver_error("RecordId key ranges are not supported in GraphQL"))
		}
	}
}

/// Convert a static Expr to Value
/// Only works for static literals (no parameters, idioms, function calls, etc.)
fn convert_static_expr(expr: Expr) -> Result<SurValue, GraphqlError> {
	if let Expr::Literal(lit) = expr {
		convert_static_literal(lit)
	} else if let Expr::Table(t) = expr {
		Ok(SurValue::Table(t))
	} else {
		Err(resolver_error("Only literal values are supported in GraphQL inputs"))
	}
}

/// Convert a static Literal to Value
/// Only works for static literals (no expressions that need evaluation)
fn convert_static_literal(lit: Literal) -> Result<SurValue, GraphqlError> {
	match lit {
		Literal::None => Ok(SurValue::None),
		Literal::Null => Ok(SurValue::Null),
		Literal::Bool(b) => Ok(SurValue::Bool(b)),
		Literal::Float(f) => Ok(SurValue::Number(SurNumber::Float(f))),
		Literal::Integer(i) => Ok(SurValue::Number(SurNumber::Int(i))),
		Literal::Decimal(d) => Ok(SurValue::Number(SurNumber::Decimal(d))),
		Literal::BigDecimal(d) => Ok(SurValue::Number(SurNumber::BigDecimal(d))),
		Literal::I8(v) => Ok(SurValue::Number(SurNumber::I8(v))),
		Literal::I16(v) => Ok(SurValue::Number(SurNumber::I16(v))),
		Literal::I32(v) => Ok(SurValue::Number(SurNumber::I32(v))),
		Literal::U8(v) => Ok(SurValue::Number(SurNumber::U8(v))),
		Literal::U16(v) => Ok(SurValue::Number(SurNumber::U16(v))),
		Literal::U32(v) => Ok(SurValue::Number(SurNumber::U32(v))),
		Literal::U64(v) => Ok(SurValue::Number(SurNumber::U64(v))),
		Literal::F16(v) => Ok(SurValue::Number(SurNumber::F16(v))),
		Literal::F32(v) => Ok(SurValue::Number(SurNumber::F32(v))),
		Literal::String(s) => Ok(SurValue::String(s)),
		Literal::Bytes(b) => Ok(SurValue::Bytes(b)),
		Literal::Regex(r) => Ok(SurValue::Regex(r)),
		Literal::RecordId(record_id_lit) => {
			let key = convert_static_record_id_key(record_id_lit.key)?;
			Ok(SurValue::RecordId(SurRecordId {
				table: record_id_lit.table,
				key,
			}))
		}
		Literal::Array(exprs) => {
			let vals: Result<Vec<SurValue>, GraphqlError> =
				exprs.into_iter().map(convert_static_expr).collect();
			Ok(SurValue::Array(SurArray(vals?)))
		}
		Literal::Set(exprs) => {
			let vals: Result<Vec<SurValue>, GraphqlError> =
				exprs.into_iter().map(convert_static_expr).collect();
			Ok(SurValue::Set(SurSet::from(vals?)))
		}
		Literal::Object(entries) => {
			// See `convert_static_record_id_key` for why the
			// accumulator is `BTreeMap<Strand, _>` rather than
			// `BTreeMap<String, _>`.
			let mut map = BTreeMap::new();
			for entry in entries {
				map.insert(entry.key, convert_static_expr(entry.value)?);
			}
			Ok(SurValue::Object(SurObject::from(map)))
		}
		Literal::Duration(d) => Ok(SurValue::Duration(d)),
		Literal::Datetime(dt) => Ok(SurValue::Datetime(dt)),
		Literal::Date(v) => Ok(SurValue::Date(v)),
		Literal::Time(v) => Ok(SurValue::Time(v)),
		Literal::Timestamp(v) => Ok(SurValue::Timestamp(v)),
		Literal::Interval(v) => Ok(SurValue::Interval(v)),
		Literal::Uuid(u) => Ok(SurValue::Uuid(u)),
		Literal::Geometry(g) => Ok(SurValue::Geometry(g)),
		Literal::File(f) => Ok(SurValue::File(f)),
		Literal::UnboundedRange => {
			Err(resolver_error("Unbounded ranges are not supported in GraphQL"))
		}
	}
}

/// Convert a GraphQL input value to a SurrealDB value, guided by the expected [`Kind`].
///
/// This is the reverse of [`sql_value_to_graphql_value`]: it takes a GraphQL input
/// value (from a query argument, filter, or mutation input) and converts it to
/// the appropriate SurrealDB value type.  The `kind` parameter tells the function
/// what type to expect, enabling proper parsing of strings as datetimes, record
/// IDs, durations, etc.
///
/// For `Kind::Any`, the function uses heuristics: strings are tried as datetime,
/// duration, uuid, then parsed as SurrealQL expressions.  For `Kind::Either`,
/// each constituent kind is tried in turn until one succeeds.
pub(crate) fn graphql_to_sql_kind(
	val: &GraphqlValue,
	kind: Kind,
) -> Result<SurValue, GraphqlError> {
	graphql_to_sql_kind_with_scope(val, kind, None)
}

fn gql_int_input<T>(
	val: &GraphqlValue,
	kind: Kind,
	lit_extract: impl FnOnce(Literal) -> Option<T>,
	wrap: impl FnOnce(T) -> SurNumber,
) -> Result<SurValue, GraphqlError>
where
	T: TryFrom<i64> + TryFrom<u64>,
{
	use crate::syn;
	if let GraphqlValue::Number(n) = val {
		if let Some(v) = n
			.as_i64()
			.and_then(|i| T::try_from(i).ok())
			.or_else(|| n.as_u64().and_then(|u| T::try_from(u).ok()))
		{
			Ok(SurValue::Number(wrap(v)))
		} else {
			Err(type_error(kind, val))
		}
	} else if let GraphqlValue::String(s) = val {
		let Expr::Literal(lit) = syn::expr(s)? else {
			return Err(type_error(kind, val));
		};
		if let Literal::Integer(i) = lit {
			let v = T::try_from(i).map_err(|_| type_error(kind, val))?;
			Ok(SurValue::Number(wrap(v)))
		} else if let Some(v) = lit_extract(lit) {
			Ok(SurValue::Number(wrap(v)))
		} else {
			Err(type_error(kind, val))
		}
	} else {
		Err(type_error(kind, val))
	}
}

/// Like [`graphql_to_sql_kind`], but with an optional `enum_scope` for precise
/// reverse-mapping of scoped enum tokens back to their original string literals.
pub(crate) fn graphql_to_sql_kind_with_scope(
	val: &GraphqlValue,
	kind: Kind,
	enum_scope: Option<&str>,
) -> Result<SurValue, GraphqlError> {
	use crate::syn;
	match kind {
		Kind::Any | Kind::Named(_) => match val {
			GraphqlValue::String(s) => {
				use Kind::*;
				// Detect the temporal/uuid formats the datastore cannot coerce
				// from a plain string, so nested `datetime`/`duration`/`uuid`
				// subfields inside `object`-typed fields still work. These
				// checks are format-guarded and never match a plain string
				// like "9000" or "10:00". Any other string is kept verbatim —
				// GraphQL input strings are data, not embedded SurrealQL, so we
				// must not re-parse them (doing so turned "9000" into the
				// integer 9000 and broke coercion against `TYPE string`).
				any_try_kinds!(val, Datetime, Duration, Uuid);
				Ok(SurValue::String(s.as_str().into()))
			}
			GraphqlValue::Null => Ok(SurValue::Null),
			obj @ GraphqlValue::Object(_) => graphql_to_sql_kind(obj, Kind::Object),
			num @ GraphqlValue::Number(_) => graphql_to_sql_kind(num, Kind::Number),
			GraphqlValue::Boolean(b) => Ok(SurValue::Bool(*b)),
			bin @ GraphqlValue::Binary(_) => graphql_to_sql_kind(bin, Kind::Bytes),
			GraphqlValue::Enum(s) => Ok(SurValue::String(s.as_str().into())),
			arr @ GraphqlValue::List(_) => {
				graphql_to_sql_kind(arr, Kind::Array(Box::new(Kind::Any), None))
			}
		},
		Kind::None => {
			if let GraphqlValue::Null = val {
				Ok(SurValue::None)
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Null => {
			if let GraphqlValue::Null = val {
				Ok(SurValue::Null)
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Bool => {
			if let GraphqlValue::Boolean(b) = val {
				Ok(SurValue::Bool(*b))
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Bytes => {
			if let GraphqlValue::Binary(b) = val {
				Ok(SurValue::Bytes(bytes::Bytes::copy_from_slice(b).into()))
			} else if let GraphqlValue::String(s) = val {
				use base64::Engine;
				let decoded = base64::engine::general_purpose::STANDARD
					.decode(s.as_bytes())
					.map_err(|_| type_error(kind, val))?;
				Ok(SurValue::Bytes(bytes::Bytes::from(decoded).into()))
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Date => {
			if let GraphqlValue::String(s) = val
				&& let Ok(v) = s.parse::<surrealdb_types::Date>()
			{
				Ok(SurValue::Date(v.into()))
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Time => {
			if let GraphqlValue::String(s) = val
				&& let Ok(v) = s.parse::<surrealdb_types::Time>()
			{
				Ok(SurValue::Time(v.into()))
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Timestamp => {
			if let GraphqlValue::String(s) = val
				&& let Ok(v) = s.parse::<surrealdb_types::Timestamp>()
			{
				Ok(SurValue::Timestamp(v.into()))
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Interval => {
			if let GraphqlValue::String(s) = val
				&& let Ok(v) = s.parse::<surrealdb_types::Interval>()
			{
				Ok(SurValue::Interval(v.into()))
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Datetime => {
			if let GraphqlValue::String(s) = val {
				match syn::datetime(s) {
					Ok(dt) => Ok(SurValue::Datetime(dt.into())),
					Err(_) => Err(type_error(kind, val)),
				}
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::BigDecimal => {
			// A bigdecimal is carried as its exact decimal text, as a decimal is.
			let text = match val {
				GraphqlValue::Number(n) => n.to_string(),
				GraphqlValue::String(s) => s.clone(),
				GraphqlValue::Null
				| GraphqlValue::Boolean(_)
				| GraphqlValue::Binary(_)
				| GraphqlValue::Enum(_)
				| GraphqlValue::List(_)
				| GraphqlValue::Object(_) => return Err(type_error(kind, val)),
			};
			match text.parse::<surrealdb_types::BigDecimal>() {
				Ok(d) => Ok(SurValue::Number(SurNumber::BigDecimal(Box::new(d)))),
				Err(_) => Err(type_error(kind, val)),
			}
		}
		Kind::Decimal => {
			if let GraphqlValue::Number(n) = val {
				if let Some(int) = n.as_i64() {
					Ok(SurValue::Number(SurNumber::Decimal(int.into())))
				} else if let Some(d) = n.as_f64().and_then(Decimal::from_f64) {
					Ok(SurValue::Number(SurNumber::Decimal(d)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SurValue::Number(SurNumber::Decimal(uint.into())))
				} else {
					Err(type_error(kind, val))
				}
			} else if let GraphqlValue::String(s) = val {
				// Decimal values serialize to GraphQL as plain decimal strings
				// (see `sql_value_to_graphql_value`), so accept that format
				// directly before falling back to a SurrealQL decimal literal
				// such as `"1.5dec"`.
				if let Ok(d) = s.parse::<Decimal>() {
					return Ok(SurValue::Number(SurNumber::Decimal(d)));
				}
				let decimal_expr: Expr = syn::expr(s)?;

				if let Expr::Literal(Literal::Decimal(d)) = decimal_expr {
					Ok(SurValue::Number(SurNumber::Decimal(d)))
				} else {
					Err(type_error(kind, val))
				}
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Duration => {
			if let GraphqlValue::String(s) = val {
				match syn::duration(s) {
					Ok(d) => Ok(SurValue::Duration(d.into())),
					Err(_) => Err(type_error(kind, val)),
				}
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Float => {
			if let GraphqlValue::Number(n) = val {
				if let Some(i) = n.as_i64() {
					Ok(SurValue::Number(SurNumber::Float(i as f64)))
				} else if let Some(f) = n.as_f64() {
					Ok(SurValue::Number(SurNumber::Float(f)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SurValue::Number(SurNumber::Float(uint as f64)))
				} else {
					unreachable!("serde_json::Number must be either i64, u64 or f64")
				}
			} else if let GraphqlValue::String(s) = val {
				let float_expr: Expr = syn::expr(s)?;

				if let Expr::Literal(Literal::Float(f)) = float_expr {
					Ok(SurValue::Number(SurNumber::Float(f)))
				} else {
					Err(type_error(kind, val))
				}
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::F16 => {
			let f = if let GraphqlValue::Number(n) = val {
				let num = if let Some(i) = n.as_i64() {
					SurNumber::Int(i)
				} else if let Some(f) = n.as_f64() {
					SurNumber::Float(f)
				} else if let Some(uint) = n.as_u64() {
					SurNumber::U64(uint)
				} else {
					unreachable!("serde_json::Number must be either i64, u64 or f64")
				};
				num.as_f16().ok_or_else(|| type_error(kind, val))?
			} else if let GraphqlValue::String(s) = val {
				let float_expr: Expr = syn::expr(s)?;
				if let Expr::Literal(Literal::F16(f)) = float_expr {
					f
				} else if let Expr::Literal(Literal::Float(f)) = float_expr {
					SurNumber::Float(f).as_f16().ok_or_else(|| type_error(kind, val))?
				} else {
					return Err(type_error(kind, val));
				}
			} else {
				return Err(type_error(kind, val));
			};
			Ok(SurValue::Number(SurNumber::F16(f)))
		}
		Kind::F32 => {
			let f = if let GraphqlValue::Number(n) = val {
				let num = if let Some(i) = n.as_i64() {
					SurNumber::Int(i)
				} else if let Some(f) = n.as_f64() {
					SurNumber::Float(f)
				} else if let Some(uint) = n.as_u64() {
					SurNumber::U64(uint)
				} else {
					unreachable!("serde_json::Number must be either i64, u64 or f64")
				};
				num.as_f32().ok_or_else(|| type_error(kind, val))?
			} else if let GraphqlValue::String(s) = val {
				let float_expr: Expr = syn::expr(s)?;
				if let Expr::Literal(Literal::F32(f)) = float_expr {
					f
				} else if let Expr::Literal(Literal::Float(f)) = float_expr {
					SurNumber::Float(f).as_f32().ok_or_else(|| type_error(kind, val))?
				} else {
					return Err(type_error(kind, val));
				}
			} else {
				return Err(type_error(kind, val));
			};
			Ok(SurValue::Number(SurNumber::F32(f)))
		}
		Kind::I8 => gql_int_input(
			val,
			kind,
			|l| {
				if let Literal::I8(v) = l {
					Some(v)
				} else {
					None
				}
			},
			SurNumber::I8,
		),
		Kind::I16 => gql_int_input(
			val,
			kind,
			|l| {
				if let Literal::I16(v) = l {
					Some(v)
				} else {
					None
				}
			},
			SurNumber::I16,
		),
		Kind::I32 => gql_int_input(
			val,
			kind,
			|l| {
				if let Literal::I32(v) = l {
					Some(v)
				} else {
					None
				}
			},
			SurNumber::I32,
		),
		Kind::Int => gql_int_input(val, kind, |_| None, SurNumber::Int),
		Kind::U8 => gql_int_input(
			val,
			kind,
			|l| {
				if let Literal::U8(v) = l {
					Some(v)
				} else {
					None
				}
			},
			SurNumber::U8,
		),
		Kind::U16 => gql_int_input(
			val,
			kind,
			|l| {
				if let Literal::U16(v) = l {
					Some(v)
				} else {
					None
				}
			},
			SurNumber::U16,
		),
		Kind::U32 => gql_int_input(
			val,
			kind,
			|l| {
				if let Literal::U32(v) = l {
					Some(v)
				} else {
					None
				}
			},
			SurNumber::U32,
		),
		Kind::U64 => gql_int_input(
			val,
			kind,
			|l| {
				if let Literal::U64(v) = l {
					Some(v)
				} else {
					None
				}
			},
			SurNumber::U64,
		),
		Kind::Number => {
			if let GraphqlValue::Number(n) = val {
				if let Some(i) = n.as_i64() {
					Ok(SurValue::Number(SurNumber::Int(i)))
				} else if let Some(f) = n.as_f64() {
					Ok(SurValue::Number(SurNumber::Float(f)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SurValue::Number(SurNumber::Decimal(uint.into())))
				} else {
					unreachable!("serde_json::Number must be either i64, u64 or f64")
				}
			} else if let GraphqlValue::String(s) = val {
				let number_expr: Expr = syn::expr(s)?;

				if let Expr::Literal(Literal::Integer(i)) = number_expr {
					Ok(SurValue::Number(SurNumber::Int(i)))
				} else if let Expr::Literal(Literal::Float(f)) = number_expr {
					Ok(SurValue::Number(SurNumber::Float(f)))
				} else if let Expr::Literal(Literal::Decimal(d)) = number_expr {
					Ok(SurValue::Number(SurNumber::Decimal(d)))
				} else if let Expr::Literal(Literal::I8(v)) = number_expr {
					Ok(SurValue::Number(SurNumber::I8(v)))
				} else if let Expr::Literal(Literal::I16(v)) = number_expr {
					Ok(SurValue::Number(SurNumber::I16(v)))
				} else if let Expr::Literal(Literal::I32(v)) = number_expr {
					Ok(SurValue::Number(SurNumber::I32(v)))
				} else if let Expr::Literal(Literal::U8(v)) = number_expr {
					Ok(SurValue::Number(SurNumber::U8(v)))
				} else if let Expr::Literal(Literal::U16(v)) = number_expr {
					Ok(SurValue::Number(SurNumber::U16(v)))
				} else if let Expr::Literal(Literal::U32(v)) = number_expr {
					Ok(SurValue::Number(SurNumber::U32(v)))
				} else if let Expr::Literal(Literal::U64(v)) = number_expr {
					Ok(SurValue::Number(SurNumber::U64(v)))
				} else if let Expr::Literal(Literal::F16(v)) = number_expr {
					Ok(SurValue::Number(SurNumber::F16(v)))
				} else if let Expr::Literal(Literal::F32(v)) = number_expr {
					Ok(SurValue::Number(SurNumber::F32(v)))
				} else {
					Err(type_error(kind, val))
				}
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Object => {
			if let GraphqlValue::Object(o) = val {
				let out: Result<BTreeMap<String, SurValue>, GraphqlError> = o
					.iter()
					.map(|(k, v)| {
						graphql_to_sql_kind(v, Kind::Any).map(|sqlv| (k.to_string(), sqlv))
					})
					.collect();
				Ok(SurValue::Object(out?.into()))
			} else if let GraphqlValue::String(s) = val {
				let expr = syn::expr_legacy_strand(s.as_str())?;
				let expr: Expr = expr;

				if let Expr::Literal(Literal::Object(o)) = expr {
					// See `convert_static_record_id_key` for
					// why the accumulator is
					// `BTreeMap<Strand, _>`.
					let mut map = BTreeMap::new();
					for entry in o {
						map.insert(entry.key, convert_static_expr(entry.value)?);
					}
					Ok(SurValue::Object(SurObject::from(map)))
				} else {
					Err(type_error(kind, val))
				}
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::String => {
			if let GraphqlValue::String(s) = val {
				Ok(SurValue::String(s.as_str().into()))
			} else if let GraphqlValue::Enum(s) = val {
				Ok(SurValue::String(s.as_str().into()))
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Uuid => {
			if let GraphqlValue::String(s) = val {
				match s.parse::<uuid::Uuid>() {
					Ok(u) => Ok(SurValue::Uuid(u.into())),
					Err(_) => Err(type_error(kind, val)),
				}
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Table(ref ts) => {
			if let GraphqlValue::String(s) = val {
				match ts.contains(&s.as_str().into()) {
					true => Ok(SurValue::Table(TableName::new(s.clone()))),
					false => Err(type_error(kind, val)),
				}
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Record(ref ts) => {
			if let GraphqlValue::String(s) = val {
				let record_id_expr = syn::expr(s)?;
				let record_id_expr: Expr = record_id_expr;

				if let Expr::Literal(Literal::RecordId(t)) = record_id_expr {
					// Empty `ts` means "any record table" — accept all.
					let table_allowed = ts.is_empty() || ts.contains(&t.table);
					if table_allowed {
						let key = convert_static_record_id_key(t.key)?;
						Ok(SurValue::RecordId(SurRecordId {
							table: t.table,
							key,
						}))
					} else {
						Err(type_error(kind, val))
					}
				} else {
					Err(type_error(kind, val))
				}
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Geometry(ref geo_kinds) => {
			if let GraphqlValue::Object(obj) = val {
				let geometry = graphql_geometry_from_object(obj, geo_kinds)?;
				Ok(SurValue::Geometry(geometry))
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Either(ks) => {
			// Parser-built unions are already flat, so only pay the
			// `Kind::either` flatten + de-dupe cost when a nested variant is
			// actually present. Flattening may collapse the union to a single
			// variant; recurse in that case.
			let ks = if ks.iter().any(|k| matches!(k, Kind::Either(_))) {
				let flat = Kind::either(ks);
				let Kind::Either(ks) = flat else {
					return graphql_to_sql_kind_with_scope(val, flat, enum_scope);
				};
				ks
			} else {
				ks
			};
			use Kind::*;

			match val {
				GraphqlValue::Null => {
					if ks.contains(&Kind::None) {
						Ok(SurValue::None)
					} else if ks.contains(&Kind::Null) {
						Ok(SurValue::Null)
					} else {
						Err(type_error(Kind::Either(ks), val))
					}
				}
				num @ GraphqlValue::Number(_) => {
					either_try_kind!(ks, num, AllNumbers);
					Err(type_error(Kind::Either(ks), val))
				}
				string @ GraphqlValue::String(_) => {
					either_try_kinds!(
						ks, string, Datetime, Duration, AllNumbers, Uuid, Record, Array, Any,
						String
					);
					either_try_kind!(ks, string, Kind::Object);
					Err(type_error(Kind::Either(ks), val))
				}
				bool @ GraphqlValue::Boolean(_) => {
					either_try_kind!(ks, bool, Kind::Bool);
					Err(type_error(Kind::Either(ks), val))
				}
				GraphqlValue::Binary(_) => {
					Err(resolver_error("binary input for Either is not yet supported"))
				}
				GraphqlValue::Enum(n) => {
					if let Some(literal) = enum_token_to_literal(&ks, n.as_str(), enum_scope) {
						return Ok(SurValue::String(literal.into()));
					}
					either_try_kind!(ks, &GraphqlValue::String(n.to_string()), Kind::String);
					Err(type_error(Kind::Either(ks), val))
				}
				list @ GraphqlValue::List(_) => {
					either_try_kind!(ks, list, Kind::Array);
					Err(type_error(Kind::Either(ks), val))
				}
				obj @ GraphqlValue::Object(_) => {
					// Try geometry kinds first (geometry inputs are objects)
					for geo_kind in ks.iter().filter(|k| matches!(k, Kind::Geometry(_))).cloned() {
						if let Ok(out) = graphql_to_sql_kind(obj, geo_kind) {
							return Ok(out);
						}
					}
					either_try_kind!(ks, obj, Kind::Object);
					Err(type_error(Kind::Either(ks), val))
				}
			}
		}
		Kind::Set(_k, _n) => Err(resolver_error("Sets are not yet supported")),
		Kind::Array(ref k, n) => {
			if let GraphqlValue::List(l) = val {
				let list_iter = l.iter().map(|v| graphql_to_sql_kind(v, *k.to_owned()));
				let list: Result<Vec<SurValue>, GraphqlError> = list_iter.collect();

				match (list, n) {
					(Ok(l), Some(n)) => {
						if l.len() as u64 == n {
							Ok(l.into())
						} else {
							Err(type_error(kind, val))
						}
					}
					(Ok(l), None) => Ok(l.into()),
					(Err(e), _) => Err(e),
				}
			} else {
				Err(type_error(kind, val))
			}
		}
		Kind::Function(_, _) => Err(resolver_error("Functions are not yet supported")),
		Kind::Range => Err(resolver_error("Ranges are not yet supported")),
		Kind::Literal(literal) => {
			let base_kind = literal_to_base_kind(&literal);
			let converted = graphql_to_sql_kind_with_scope(val, base_kind, enum_scope)?;
			if literal.validate_value(&converted) {
				Ok(converted)
			} else {
				Err(type_error(Kind::Literal(literal), val))
			}
		}
		Kind::Regex => Err(resolver_error("Regexes are not yet supported")),
		Kind::File(_) => Err(resolver_error("Files are not yet supported")),
		Kind::Enum(_) => graphql_to_sql_kind_with_scope(val, Kind::String, enum_scope),
	}
}

// ---------------------------------------------------------------------------
// Geometry support: helpers, type registration, and conversion
// ---------------------------------------------------------------------------

/// Map a `GeometryKind` to the corresponding GraphQL output Object type name.
pub(crate) fn geometry_kind_to_graphql_type_name(kind: &GeometryKind) -> &'static str {
	match kind {
		GeometryKind::Point => "GeometryPoint",
		GeometryKind::Line => "GeometryLineString",
		GeometryKind::Polygon => "GeometryPolygon",
		GeometryKind::MultiPoint => "GeometryMultiPoint",
		GeometryKind::MultiLine => "GeometryMultiLineString",
		GeometryKind::MultiPolygon => "GeometryMultiPolygon",
		GeometryKind::Collection => "GeometryCollection",
	}
}

/// Map a `GeometryKind` to the corresponding GraphQL InputObject type name.
fn geometry_kind_to_graphql_input_type_name(kind: &GeometryKind) -> &'static str {
	match kind {
		GeometryKind::Point => "GeometryPointInput",
		GeometryKind::Line => "GeometryLineStringInput",
		GeometryKind::Polygon => "GeometryPolygonInput",
		GeometryKind::MultiPoint => "GeometryMultiPointInput",
		GeometryKind::MultiLine => "GeometryMultiLineStringInput",
		GeometryKind::MultiPolygon => "GeometryMultiPolygonInput",
		GeometryKind::Collection => "GeometryCollectionInput",
	}
}

/// Map a `Geometry` value to the GraphQL Object type name for that variant.
pub(crate) fn geometry_graphql_type_name(g: &SurGeometry) -> &'static str {
	match g {
		SurGeometry::Point(_) => "GeometryPoint",
		SurGeometry::Line(_) => "GeometryLineString",
		SurGeometry::Polygon(_) => "GeometryPolygon",
		SurGeometry::MultiPoint(_) => "GeometryMultiPoint",
		SurGeometry::MultiLine(_) => "GeometryMultiLineString",
		SurGeometry::MultiPolygon(_) => "GeometryMultiPolygon",
		SurGeometry::Collection(_) => "GeometryCollection",
	}
}

/// Build a geometry Object type for variants that have `coordinates`.
///
/// Creates an Object type with:
/// - `type: GeometryType!` (fixed enum value)
/// - `coordinates: JSON!` — exposed as the `JSON` scalar rather than a deeply nested
///   `[[[[Float!]!]!]!]!` chain. The latter exceeds the standard 7-level introspection `ofType`
///   chain (the spec's canonical introspection query stops at 7), which causes strict clients such
///   as Postman to fail schema validation as soon as any field is typed `geometry`. Returning
///   `JSON` keeps the value shape (nested arrays of numbers) intact at runtime while keeping the
///   introspection chain at a single hop.
fn make_geometry_object_type(obj_name: &str, geojson_type: &'static str) -> Object {
	let coords_ty = TypeRef::named_nn("JSON");
	Object::new(obj_name)
		.field(Field::new("type", TypeRef::named_nn("GeometryType"), {
			move |_ctx| {
				FieldFuture::new(async move {
					Ok(Some(FieldValue::value(GraphqlValue::Enum(Name::new(geojson_type)))))
				})
			}
		}))
		.field(Field::new("coordinates", coords_ty, |ctx| {
			FieldFuture::new(async move {
				let g = ctx.parent_value.try_downcast_ref::<SurGeometry>()?;
				let coords = g.as_coordinates();
				let graphql_coords = sql_value_to_graphql_value(coords)?;
				Ok(Some(FieldValue::value(graphql_coords)))
			})
		}))
}

/// Build the `GeometryCollection` Object type, which uses `geometries` instead
/// of `coordinates`.
fn make_geometry_collection_type() -> Object {
	Object::new("GeometryCollection")
		.field(Field::new("type", TypeRef::named_nn("GeometryType"), |_ctx| {
			FieldFuture::new(async move {
				Ok(Some(FieldValue::value(GraphqlValue::Enum(Name::new("GeometryCollection")))))
			})
		}))
		.field(Field::new("geometries", TypeRef::named_nn_list_nn("Geometry"), |ctx| {
			FieldFuture::new(async move {
				let g = ctx.parent_value.try_downcast_ref::<SurGeometry>()?;
				if let SurGeometry::Collection(geometries) = g {
					let items: Vec<FieldValue> = geometries
						.iter()
						.map(|g| {
							let type_name = geometry_graphql_type_name(g);
							FieldValue::owned_any(g.clone()).with_type(type_name)
						})
						.collect();
					Ok(Some(FieldValue::list(items)))
				} else {
					Err(internal_error("Expected GeometryCollection value").into())
				}
			})
		}))
}

/// Register all geometry-related types into the types list and return types
/// that must be registered directly on the Schema builder (enum, union).
///
/// Types registered into `types` vec:
/// - Object types: `GeometryPoint`, `GeometryLineString`, `GeometryPolygon`, `GeometryMultiPoint`,
///   `GeometryMultiLineString`, `GeometryMultiPolygon`, `GeometryCollection`
///
/// Types that need `schema.register()`:
/// - Enum: `GeometryType`
/// - Union: `Geometry`
/// - InputObject types for each variant + unified `GeometryInput`
pub(crate) fn register_geometry_types(types: &mut Vec<Type>) {
	// GeometryType enum
	types.push(Type::Enum(
		Enum::new("GeometryType")
			.description("GeoJSON geometry type discriminator")
			.item("Point")
			.item("LineString")
			.item("Polygon")
			.item("MultiPoint")
			.item("MultiLineString")
			.item("MultiPolygon")
			.item("GeometryCollection"),
	));

	// Per-variant output Object types. `coordinates` is exposed as a `JSON` scalar
	// regardless of variant — see `make_geometry_object_type` for the rationale
	// (avoiding the 7-level introspection `ofType` chain limit).
	types.push(Type::Object(make_geometry_object_type("GeometryPoint", "Point")));
	types.push(Type::Object(make_geometry_object_type("GeometryLineString", "LineString")));
	types.push(Type::Object(make_geometry_object_type("GeometryPolygon", "Polygon")));
	types.push(Type::Object(make_geometry_object_type("GeometryMultiPoint", "MultiPoint")));
	types.push(Type::Object(make_geometry_object_type(
		"GeometryMultiLineString",
		"MultiLineString",
	)));
	types.push(Type::Object(make_geometry_object_type("GeometryMultiPolygon", "MultiPolygon")));
	types.push(Type::Object(make_geometry_collection_type()));

	// Geometry union (covers all variants)
	types.push(Type::Union(
		Union::new("Geometry")
			.description("A GeoJSON geometry – one of Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, or GeometryCollection")
			.possible_type("GeometryPoint")
			.possible_type("GeometryLineString")
			.possible_type("GeometryPolygon")
			.possible_type("GeometryMultiPoint")
			.possible_type("GeometryMultiLineString")
			.possible_type("GeometryMultiPolygon")
			.possible_type("GeometryCollection"),
	));

	// Per-variant InputObject types. `coordinates` is `JSON!` rather than a deeply
	// nested float array to keep introspection within the standard 7-level chain
	// limit. The runtime parser (`graphql_coords_to_sur_value`) still walks nested
	// arrays of numbers from the JSON value, so the wire format is unchanged.
	let json_nn = || TypeRef::named_nn("JSON");
	types.push(Type::InputObject(
		InputObject::new("GeometryPointInput")
			.description("GeoJSON Point input – coordinates is [lng, lat]")
			.field(InputValue::new("coordinates", json_nn())),
	));
	types.push(Type::InputObject(
		InputObject::new("GeometryLineStringInput")
			.description("GeoJSON LineString input")
			.field(InputValue::new("coordinates", json_nn())),
	));
	types.push(Type::InputObject(
		InputObject::new("GeometryPolygonInput")
			.description("GeoJSON Polygon input")
			.field(InputValue::new("coordinates", json_nn())),
	));
	types.push(Type::InputObject(
		InputObject::new("GeometryMultiPointInput")
			.description("GeoJSON MultiPoint input")
			.field(InputValue::new("coordinates", json_nn())),
	));
	types.push(Type::InputObject(
		InputObject::new("GeometryMultiLineStringInput")
			.description("GeoJSON MultiLineString input")
			.field(InputValue::new("coordinates", json_nn())),
	));
	types.push(Type::InputObject(
		InputObject::new("GeometryMultiPolygonInput")
			.description("GeoJSON MultiPolygon input")
			.field(InputValue::new("coordinates", json_nn())),
	));
	types.push(Type::InputObject(
		InputObject::new("GeometryCollectionInput")
			.description("GeoJSON GeometryCollection input")
			.field(InputValue::new("geometries", TypeRef::named_nn_list_nn("GeometryInput"))),
	));

	// Unified GeometryInput (for fields typed as `geometry` without specific variant)
	types.push(Type::InputObject(
		InputObject::new("GeometryInput")
			.description(
				"Generic GeoJSON geometry input. Use `type` to select the variant, \
				 `coordinates` for coordinate-based types, `geometries` for GeometryCollection.",
			)
			.field(InputValue::new("type", TypeRef::named_nn("GeometryType")))
			.field(InputValue::new("coordinates", TypeRef::named("JSON")))
			.field(InputValue::new("geometries", TypeRef::named_list("GeometryInput"))),
	));
}

/// Convert a GraphQL coordinate value (nested arrays of numbers) to a SurrealDB
/// `Value` suitable for `Geometry::array_to_*` helpers.
fn graphql_coords_to_sur_value(val: &GraphqlValue) -> Result<SurValue, GraphqlError> {
	if let GraphqlValue::Number(n) = val {
		let f = n
			.as_f64()
			.ok_or_else(|| resolver_error("Invalid coordinate: expected finite number"))?;
		Ok(SurValue::Number(SurNumber::Float(f)))
	} else if let GraphqlValue::List(items) = val {
		let vals: Result<Vec<SurValue>, GraphqlError> =
			items.iter().map(graphql_coords_to_sur_value).collect();
		Ok(vals?.into())
	} else {
		Err(resolver_error("Expected number or array in geometry coordinates"))
	}
}

/// Convert a GraphQL geometry input Object (GeoJSON format) to a SurrealDB
/// `Geometry` value.
///
/// For typed inputs (e.g. `GeometryPointInput`), the `type` field is optional
/// and the variant is inferred from `expected_kind`. For the unified
/// `GeometryInput`, the `type` field is required.
fn graphql_geometry_from_object(
	obj: &IndexMap<Name, GraphqlValue>,
	expected_kind: &[GeometryKind],
) -> Result<SurGeometry, GraphqlError> {
	// Determine the geometry type: from explicit `type` field or from expected_kind
	let geo_type: &str = if let Some(ty) = obj.get("type") {
		if let GraphqlValue::Enum(s) = ty {
			s.as_str()
		} else if let GraphqlValue::String(s) = ty {
			s.as_str()
		} else {
			return Err(resolver_error("Geometry 'type' must be a GeometryType enum value"));
		}
	} else if expected_kind.len() == 1 {
		// Infer from the single expected kind
		match &expected_kind[0] {
			GeometryKind::Point => "Point",
			GeometryKind::Line => "LineString",
			GeometryKind::Polygon => "Polygon",
			GeometryKind::MultiPoint => "MultiPoint",
			GeometryKind::MultiLine => "MultiLineString",
			GeometryKind::MultiPolygon => "MultiPolygon",
			GeometryKind::Collection => "GeometryCollection",
		}
	} else {
		return Err(resolver_error(
			"Geometry input must include a 'type' field when multiple geometry types are allowed",
		));
	};

	// Validate that the type is allowed by the expected kinds
	if !expected_kind.is_empty() {
		let kind = match geo_type {
			"Point" => GeometryKind::Point,
			"LineString" => GeometryKind::Line,
			"Polygon" => GeometryKind::Polygon,
			"MultiPoint" => GeometryKind::MultiPoint,
			"MultiLineString" => GeometryKind::MultiLine,
			"MultiPolygon" => GeometryKind::MultiPolygon,
			"GeometryCollection" => GeometryKind::Collection,
			other => return Err(resolver_error(format!("Unknown geometry type: {other}"))),
		};
		if !expected_kind.contains(&kind) {
			return Err(resolver_error(format!(
				"Geometry type '{geo_type}' is not allowed here; expected one of: {}",
				expected_kind
					.iter()
					.map(|k| geometry_kind_to_graphql_type_name(k))
					.collect::<Vec<_>>()
					.join(", ")
			)));
		}
	}

	// Parse based on type
	match geo_type {
		"Point" => {
			let coords = obj
				.get("coordinates")
				.ok_or_else(|| resolver_error("Point requires 'coordinates' field"))?;
			let sur_coords = graphql_coords_to_sur_value(coords)?;
			SurGeometry::array_to_point(&sur_coords)
				.map(SurGeometry::Point)
				.ok_or_else(|| resolver_error("Invalid Point coordinates: expected [lng, lat]"))
		}
		"LineString" => {
			let coords = obj
				.get("coordinates")
				.ok_or_else(|| resolver_error("LineString requires 'coordinates' field"))?;
			let sur_coords = graphql_coords_to_sur_value(coords)?;
			SurGeometry::array_to_line(&sur_coords).map(SurGeometry::Line).ok_or_else(|| {
				resolver_error("Invalid LineString coordinates: expected [[lng, lat], ...]")
			})
		}
		"Polygon" => {
			let coords = obj
				.get("coordinates")
				.ok_or_else(|| resolver_error("Polygon requires 'coordinates' field"))?;
			let sur_coords = graphql_coords_to_sur_value(coords)?;
			SurGeometry::array_to_polygon(&sur_coords).map(SurGeometry::Polygon).ok_or_else(|| {
				resolver_error("Invalid Polygon coordinates: expected [[[lng, lat], ...], ...]")
			})
		}
		"MultiPoint" => {
			let coords = obj
				.get("coordinates")
				.ok_or_else(|| resolver_error("MultiPoint requires 'coordinates' field"))?;
			let sur_coords = graphql_coords_to_sur_value(coords)?;
			SurGeometry::array_to_multipoint(&sur_coords).map(SurGeometry::MultiPoint).ok_or_else(
				|| resolver_error("Invalid MultiPoint coordinates: expected [[lng, lat], ...]"),
			)
		}
		"MultiLineString" => {
			let coords = obj
				.get("coordinates")
				.ok_or_else(|| resolver_error("MultiLineString requires 'coordinates' field"))?;
			let sur_coords = graphql_coords_to_sur_value(coords)?;
			SurGeometry::array_to_multiline(&sur_coords).map(SurGeometry::MultiLine).ok_or_else(
				|| {
					resolver_error(
						"Invalid MultiLineString coordinates: expected [[[lng, lat], ...], ...]",
					)
				},
			)
		}
		"MultiPolygon" => {
			let coords = obj
				.get("coordinates")
				.ok_or_else(|| resolver_error("MultiPolygon requires 'coordinates' field"))?;
			let sur_coords = graphql_coords_to_sur_value(coords)?;
			SurGeometry::array_to_multipolygon(&sur_coords)
				.map(SurGeometry::MultiPolygon)
				.ok_or_else(|| resolver_error("Invalid MultiPolygon coordinates"))
		}
		"GeometryCollection" => {
			let graphql_geometries = obj
				.get("geometries")
				.ok_or_else(|| resolver_error("GeometryCollection requires 'geometries' field"))?;
			let GraphqlValue::List(list) = graphql_geometries else {
				return Err(resolver_error("GeometryCollection 'geometries' must be an array"));
			};
			let mut geometries = Vec::with_capacity(list.len());
			for item in list {
				if let GraphqlValue::Object(sub_obj) = item {
					// Recursively parse each sub-geometry (allow any type)
					geometries.push(graphql_geometry_from_object(sub_obj, &[])?);
				} else {
					return Err(resolver_error(
						"Each item in 'geometries' must be a geometry object",
					));
				}
			}
			Ok(SurGeometry::Collection(geometries))
		}
		other => Err(resolver_error(format!("Unknown geometry type: {other}"))),
	}
}

/// Convert a SurrealDB `Geometry` value to a `GraphqlValue::Object` in GeoJSON format.
///
/// Used by `sql_value_to_graphql_value` for geometry values in arrays / nested objects
/// where we cannot use `FieldValue::owned_any`.
pub(crate) fn geometry_to_graphql_object(g: &SurGeometry) -> Result<GraphqlValue, GraphqlError> {
	let mut map = IndexMap::new();
	map.insert(Name::new("type"), GraphqlValue::Enum(Name::new(g.as_type())));

	if let SurGeometry::Collection(geometries) = g {
		let graphql_geometries: Result<Vec<GraphqlValue>, GraphqlError> =
			geometries.iter().map(geometry_to_graphql_object).collect();
		map.insert(Name::new("geometries"), GraphqlValue::List(graphql_geometries?));
	} else {
		let coords = g.as_coordinates();
		let graphql_coords = sql_value_to_graphql_value(coords)?;
		map.insert(Name::new("coordinates"), graphql_coords);
	}

	Ok(GraphqlValue::Object(map))
}

#[cfg(test)]
mod tests {
	use async_graphql::Number;

	use super::*;

	#[test]
	fn a_named_type_maps_as_its_body() {
		let mood = Kind::Either(vec![
			Kind::Literal(KindLiteral::String("sad".into())),
			Kind::Literal(KindLiteral::String("ok".into())),
		]);
		let named = |name: &str| Kind::Named(name.into());
		let types: HashMap<String, Kind> = [
			("mood".to_owned(), mood.clone()),
			("feeling".to_owned(), named("mood")),
			("maybe_mood".to_owned(), Kind::Either(vec![Kind::None, Kind::Null, named("mood")])),
			("small".to_owned(), Kind::Int),
		]
		.into_iter()
		.collect();
		// An alias of a union is the union; a domain that may be empty is the union beside NONE
		// and NULL, merged into one union.
		assert_eq!(resolve_named_kinds(&named("feeling"), &types), mood);
		let Kind::Either(members) = resolve_named_kinds(&named("maybe_mood"), &types) else {
			panic!("a domain of a union resolves to a union");
		};
		assert_eq!(members.len(), 4);
		assert_eq!(
			resolve_named_kinds(&Kind::Array(Box::new(named("small")), None), &types),
			Kind::Array(Box::new(Kind::Int), None)
		);
		assert_eq!(resolve_named_kinds(&named("unknown"), &types), named("unknown"));

		let mut graphql_types = Vec::new();
		let scope = Some("t_f");
		let union = kind_to_type_with_enum_prefix(
			resolve_named_kinds(&named("feeling"), &types),
			&mut graphql_types,
			false,
			scope,
		)
		.unwrap();
		assert_eq!(union.to_string(), "T_F_ENUM_SAD_OR_OK!");
		let optional = kind_to_type_with_enum_prefix(
			resolve_named_kinds(&named("maybe_mood"), &types),
			&mut graphql_types,
			false,
			scope,
		)
		.unwrap();
		assert_eq!(optional.to_string(), "T_F_ENUM_SAD_OR_OK");
		let small =
			kind_to_type(resolve_named_kinds(&named("small"), &types), &mut graphql_types, false)
				.unwrap();
		assert_eq!(small.to_string(), "Int!");
	}

	#[test]
	fn either_flattens_nested_variants() {
		// `Kind::Either` nested inside another `Kind::Either` must still resolve
		// correctly. The parser normally flattens these, but the runtime path
		// must also handle them defensively for programmatically built kinds.
		let kind = Kind::Either(vec![Kind::Either(vec![Kind::Int, Kind::String]), Kind::Bool]);

		let int_val = GraphqlValue::Number(Number::from(7));
		assert_eq!(graphql_to_sql_kind(&int_val, kind.clone()).unwrap(), SurValue::from(7i64));

		let str_val = GraphqlValue::String("hi".to_owned());
		assert_eq!(graphql_to_sql_kind(&str_val, kind.clone()).unwrap(), SurValue::from("hi"));

		let bool_val = GraphqlValue::Boolean(true);
		assert_eq!(graphql_to_sql_kind(&bool_val, kind).unwrap(), SurValue::from(true));
	}

	#[test]
	fn either_collapses_single_variant_after_dedup() {
		// After flattening + de-duplication, an `Either` may collapse to a
		// single variant. The runtime path should recurse and convert against
		// that bare kind rather than failing.
		let kind = Kind::Either(vec![Kind::Int, Kind::Either(vec![Kind::Int, Kind::Int])]);

		let val = GraphqlValue::Number(Number::from(42));
		assert_eq!(graphql_to_sql_kind(&val, kind).unwrap(), SurValue::from(42i64));
	}

	#[test]
	fn decimal_string_round_trips() {
		// `sql_value_to_graphql_value` serializes decimals as plain decimal
		// strings, so `graphql_to_sql_kind` must accept that exact format back —
		// the debug-build scalar validator rejects every decimal field
		// response otherwise. The SurrealQL literal form keeps working.
		let decimal = SurValue::Number(SurNumber::Decimal("19.99".parse().unwrap()));

		let serialized = sql_value_to_graphql_value(decimal.clone()).unwrap();
		assert_eq!(serialized, GraphqlValue::String("19.99".to_owned()));
		assert_eq!(graphql_to_sql_kind(&serialized, Kind::Decimal).unwrap(), decimal);

		let literal = GraphqlValue::String("19.99dec".to_owned());
		assert_eq!(graphql_to_sql_kind(&literal, Kind::Decimal).unwrap(), decimal);
	}

	#[test]
	fn bigdecimal_string_round_trips() {
		let big = SurValue::Number(SurNumber::BigDecimal(Box::new(
			"123456789012345678901234567890.5".parse().unwrap(),
		)));

		let serialized = sql_value_to_graphql_value(big.clone()).unwrap();
		assert_eq!(serialized, GraphqlValue::String("123456789012345678901234567890.5".to_owned()));
		assert_eq!(graphql_to_sql_kind(&serialized, Kind::BigDecimal).unwrap(), big);
	}

	#[test]
	fn u32_and_u64_use_custom_scalars_and_round_trip_full_range() {
		let mut types = Vec::new();

		let u32_ty = kind_to_type(Kind::U32, &mut types, false).unwrap();
		assert_eq!(u32_ty.to_string(), "u32!");

		let u64_ty = kind_to_type(Kind::U64, &mut types, false).unwrap();
		assert_eq!(u64_ty.to_string(), "u64!");

		let u32_max = SurValue::Number(SurNumber::U32(u32::MAX));
		let gql_u32 = sql_value_to_graphql_value(u32_max.clone()).unwrap();
		assert_eq!(gql_u32, GraphqlValue::Number(Number::from(u32::MAX)));
		assert_eq!(graphql_to_sql_kind(&gql_u32, Kind::U32).unwrap(), u32_max);

		let u64_max = SurValue::Number(SurNumber::U64(u64::MAX));
		let gql_u64 = sql_value_to_graphql_value(u64_max.clone()).unwrap();
		assert_eq!(gql_u64, GraphqlValue::Number(Number::from(u64::MAX)));
		assert_eq!(graphql_to_sql_kind(&gql_u64, Kind::U64).unwrap(), u64_max);
	}

	#[test]
	fn sanitize_kind_strips_unexposed_record_tables() {
		let mut exposed = HashSet::new();
		exposed.insert(TableName::from("public"));

		let secret_only = Kind::Record(vec![TableName::from("secret")]);
		assert_eq!(
			sanitize_kind_for_exposed_tables(secret_only, &exposed),
			Kind::Either(vec![Kind::None, Kind::Record(vec![])]),
		);

		let mixed = Kind::Record(vec![TableName::from("public"), TableName::from("secret")]);
		assert_eq!(
			sanitize_kind_for_exposed_tables(mixed, &exposed),
			Kind::Either(vec![Kind::None, Kind::Record(vec![TableName::from("public")])]),
		);

		let option_secret =
			Kind::Either(vec![Kind::None, Kind::Record(vec![TableName::from("secret")])]);
		assert_eq!(
			sanitize_kind_for_exposed_tables(option_secret, &exposed),
			Kind::Either(vec![Kind::None, Kind::Record(vec![])]),
		);
	}

	#[tokio::test]
	async fn excluded_table_record_references_build_and_do_not_leak_at_runtime() {
		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		for res in ds
			.execute(
				"
				DEFINE NAMESPACE test;
				DEFINE DATABASE test;
				DEFINE TABLE secret SCHEMAFULL;
				DEFINE FIELD token ON secret TYPE string;
				DEFINE TABLE public SCHEMAFULL;
				DEFINE FIELD title ON public TYPE string;
				DEFINE FIELD secret_ref ON public TYPE option<record<secret>>;
				DEFINE FUNCTION fn::get_secret() -> record<secret> { RETURN secret:1; };
				DEFINE CONFIG GRAPHQL TABLES EXCLUDE secret FUNCTIONS AUTO;
				CREATE secret:1 SET token = 'top-secret';
				CREATE public:1 SET title = 'hello', secret_ref = secret:1;
				",
				&sess,
				None,
			)
			.await
			.unwrap()
		{
			res.result.unwrap();
		}

		let resp = crate::graphql::exec::execute_request(
			&ds,
			&sess,
			"{ public(id: \"1\") { title secret_ref { id } } _get(id: \"secret:1\") { id } fn_get_secret { id } }".to_string(),
			serde_json::Value::Null,
			None,
		)
		.await
		.unwrap();
		assert!(resp.get("errors").is_none(), "unexpected GraphQL errors: {resp}");
		let data = &resp["data"];
		assert_eq!(data["public"]["title"], serde_json::json!("hello"));
		assert_eq!(data["public"]["secret_ref"], serde_json::Value::Null);
		assert_eq!(data["_get"], serde_json::Value::Null);
		assert_eq!(data["fn_get_secret"], serde_json::Value::Null);
	}
}
