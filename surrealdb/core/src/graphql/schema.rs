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

use std::collections::{BTreeMap, HashMap};
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
use crate::kvs::{Datastore, LockType, Transaction, TransactionType};
use crate::val::{
	Array as SurArray, Geometry as SurGeometry, Number as SurNumber, Object as SurObject,
	RecordId as SurRecordId, RecordIdKey as SurRecordIdKey, Set as SurSet, TableName,
	Value as SurValue,
};

/// Shared context for accessing table metadata during schema generation.
///
/// Groups the transaction, namespace/database identifiers, and datastore
/// reference that are threaded through `process_tbs` and `process_mutations`.
pub(crate) struct SchemaContext<'a> {
	pub tx: &'a Transaction,
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub datastore: &'a Arc<Datastore>,
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
	let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
	let ns = session.ns.as_ref().ok_or(GraphqlError::UnspecifiedNamespace)?;
	let db = session.db.as_ref().ok_or(GraphqlError::UnspecifiedDatabase)?;

	let db_def = match tx.get_db_by_name(ns, db, None).await? {
		Some(db) => db,
		None => return Err(GraphqlError::NotConfigured),
	};

	// Get all tables
	let tbs = match graphql_config.tables {
		GraphQLTablesConfig::None => None,
		_ => {
			let tbs = tx.all_tb(db_def.namespace_id, db_def.database_id, None).await?;

			match graphql_config.tables {
				GraphQLTablesConfig::None => None,
				GraphQLTablesConfig::Auto => Some(tbs),
				GraphQLTablesConfig::Include(inc) => {
					Some(tbs.iter().filter(|t| inc.contains(&t.name)).cloned().collect())
				}
				GraphQLTablesConfig::Exclude(exc) => {
					Some(tbs.iter().filter(|t| !exc.contains(&t.name)).cloned().collect())
				}
			}
		}
	};

	// Get all functions
	let fns = match graphql_config.functions {
		GraphQLFunctionsConfig::None => None,
		_ => {
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

	let schema_ctx = SchemaContext {
		tx: &tx,
		ns: db_def.namespace_id,
		db: db_def.database_id,
		datastore,
	};

	let mut mutation_obj: Option<Object> = None;
	let mut subscription_obj = None;

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
		query = process_fns(fns, query, &mut types, datastore).await?;
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

	// Apply depth and complexity limits from the GraphQL config
	if let Some(depth) = graphql_config.depth_limit {
		schema = schema.limit_depth(depth as usize);
	}
	if let Some(complexity) = graphql_config.complexity_limit {
		schema = schema.limit_complexity(complexity as usize);
	}
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
			scalar_debug_validated!($schema, $name, $kind, std::option::Option::Some($desc), None)
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

	scalar_debug_validated!(schema, "decimal", Kind::Decimal);
	scalar_debug_validated!(schema, "number", Kind::Number);
	scalar_debug_validated!(schema, "null", Kind::Null);
	scalar_debug_validated!(schema, "datetime", Kind::Datetime);
	scalar_debug_validated!(schema, "duration", Kind::Duration);
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
			SurNumber::Float(f) => GraphqlValue::Number(
				Number::from_f64(f)
					.ok_or(resolver_error("unimplemented: graceful NaN and Inf handling"))?,
			),
			num @ SurNumber::Decimal(_) => GraphqlValue::String(num.to_string()),
		},
		SurValue::String(s) => GraphqlValue::String(s.into_string()),
		SurValue::Duration(d) => GraphqlValue::String(d.to_string()),
		SurValue::Datetime(d) => GraphqlValue::String(d.to_rfc3339()),
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
		v => GraphqlValue::String(v.to_raw_string()),
	};
	Ok(out)
}

/// Strip an `option<…>` wrapper, which is represented as an `Either` carrying a
/// `None` variant: `option<array<T>>` collapses to `array<T>`.
///
/// A union with more than one meaningful variant is returned untouched — that
/// deliberately includes `option<'A' | 'B'>`, which must stay an `Either` so it
/// is still recognised as a literal union.
fn strip_optional(kind: &Kind) -> &Kind {
	let Kind::Either(ks) = kind else {
		return kind;
	};
	let mut variants = ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null));
	match (variants.next(), variants.next()) {
		(Some(only), None) => only,
		_ => kind,
	}
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
	// `strip_optional` is needed to avoid the `Either([None, Array(Either([…]))])` case hitting
	// the literal-lookup arm (and consequentially falling through) due to the outer `Either`.
	match (v, kind.map(strip_optional)) {
		// Convert element-wise so the enum mapping survives the array layer.
		// `kind_to_type_inner` registers one enum for the *element* kind and
		// passes `enum_scope` through the array unchanged, so the elements are
		// scoped identically to a bare field of that kind.
		(SurValue::Array(items), Some(Kind::Array(inner, _))) => items
			.into_iter()
			.map(|item| sql_value_to_graphql_value_with_kind(item, Some(inner), enum_scope))
			.collect::<Result<Vec<_>, GraphqlError>>()
			.map(GraphqlValue::List),
		(v, Some(Kind::Either(ks))) => {
			if let SurValue::String(s) = &v {
				for k in ks {
					if let Kind::Literal(KindLiteral::String(lit)) = k
						&& lit.as_str() == s.as_str()
					{
						return Ok(GraphqlValue::Enum(Name::new(literal_enum_item_name(
							enum_scope, lit,
						))));
					}
				}
			}
			sql_value_to_graphql_value(v)
		}
		(v, _) => sql_value_to_graphql_value(v),
	}
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
		_ => t.key.to_sql(),
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
		Kind::Any => TypeRef::named("any"),
		Kind::None => TypeRef::named("none"),
		Kind::Null => TypeRef::named("null"),
		Kind::Bool => TypeRef::named(TypeRef::BOOLEAN),
		Kind::Bytes => TypeRef::named("bytes"),
		Kind::Datetime => TypeRef::named("datetime"),
		Kind::Decimal => TypeRef::named("decimal"),
		Kind::Duration => TypeRef::named("duration"),
		Kind::Float => TypeRef::named(TypeRef::FLOAT),
		Kind::Int => TypeRef::named(TypeRef::INT),
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
				Some(enum_ty)
			} else {
				None
			};

			match (enum_ty, others.is_empty()) {
				// Every member was a string literal, so the field *is* the enum
				// and there is nothing to union it with. Yielded here rather
				// than returned early so that it still picks up the `optional`
				// wrapping at the end of this function: a non-optional literal
				// union must be emitted as `<Enum>!`, exactly like every other
				// non-optional kind (issue #7452).
				(Some(enum_ty), true) => TypeRef::named(enum_ty),
				(enum_ty, _) => {
					let pos_names: Result<Vec<TypeRef>, GraphqlError> = others
						.into_iter()
						.map(|k| kind_to_type_inner(k, types, is_input, enum_scope, array_depth))
						.collect();
					let pos_names: Vec<String> =
						pos_names?.into_iter().map(|tr| tr.to_string()).collect();
					let ty_name = pos_names.join("_or_");

					let mut tmp_union = Union::new(ty_name.clone());
					for n in pos_names {
						tmp_union = tmp_union.possible_type(n);
					}

					if let Some(ty) = enum_ty {
						tmp_union = tmp_union.possible_type(ty);
					}

					types.push(Type::Union(tmp_union));
					TypeRef::named(ty_name)
				}
			}
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

/// Strip `NonNull` wrappers from a [`TypeRef`], making it nullable.
///
/// Used when generating update input types (all fields become optional) and
/// when simplifying `option<T>` unions.
pub fn unwrap_type(ty: TypeRef) -> TypeRef {
	match ty {
		TypeRef::NonNull(t) => unwrap_type(*t),
		_ => ty,
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
		either_try_kind!($ks, $val, Kind::Float);
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
	match expr {
		Expr::Literal(lit) => convert_static_literal(lit),
		Expr::Table(t) => Ok(SurValue::Table(t)),
		_ => Err(resolver_error("Only literal values are supported in GraphQL inputs")),
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

/// Like [`graphql_to_sql_kind`], but with an optional `enum_scope` for precise
/// reverse-mapping of scoped enum tokens back to their original string literals.
pub(crate) fn graphql_to_sql_kind_with_scope(
	val: &GraphqlValue,
	kind: Kind,
	enum_scope: Option<&str>,
) -> Result<SurValue, GraphqlError> {
	use crate::syn;
	match kind {
		Kind::Any => match val {
			GraphqlValue::String(s) => {
				use Kind::*;
				any_try_kinds!(val, Datetime, Duration, Uuid);
				if let Ok(expr) = syn::expr_legacy_strand(s.as_str())
					&& let Ok(out) = convert_static_expr(expr.into())
				{
					return Ok(out);
				}
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
		Kind::None => match val {
			GraphqlValue::Null => Ok(SurValue::None),
			_ => Err(type_error(kind, val)),
		},
		Kind::Null => match val {
			GraphqlValue::Null => Ok(SurValue::Null),
			_ => Err(type_error(kind, val)),
		},
		Kind::Bool => match val {
			GraphqlValue::Boolean(b) => Ok(SurValue::Bool(*b)),
			_ => Err(type_error(kind, val)),
		},
		Kind::Bytes => match val {
			GraphqlValue::Binary(b) => Ok(SurValue::Bytes(bytes::Bytes::copy_from_slice(b).into())),
			GraphqlValue::String(s) => {
				use base64::Engine;
				let decoded = base64::engine::general_purpose::STANDARD
					.decode(s.as_bytes())
					.map_err(|_| type_error(kind, val))?;
				Ok(SurValue::Bytes(bytes::Bytes::from(decoded).into()))
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Datetime => match val {
			GraphqlValue::String(s) => match syn::datetime(s) {
				Ok(dt) => Ok(SurValue::Datetime(dt.into())),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Decimal => match val {
			GraphqlValue::Number(n) => {
				if let Some(int) = n.as_i64() {
					Ok(SurValue::Number(SurNumber::Decimal(int.into())))
				} else if let Some(d) = n.as_f64().and_then(Decimal::from_f64) {
					Ok(SurValue::Number(SurNumber::Decimal(d)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SurValue::Number(SurNumber::Decimal(uint.into())))
				} else {
					Err(type_error(kind, val))
				}
			}
			GraphqlValue::String(s) => {
				// Decimal values serialize to GraphQL as plain decimal strings
				// (see `sql_value_to_graphql_value`), so accept that format
				// directly before falling back to a SurrealQL decimal literal
				// such as `"1.5dec"`.
				if let Ok(d) = s.parse::<Decimal>() {
					return Ok(SurValue::Number(SurNumber::Decimal(d)));
				}
				let decimal_expr: Expr = syn::expr(s)?.into();

				match decimal_expr {
					Expr::Literal(Literal::Decimal(d)) => {
						Ok(SurValue::Number(SurNumber::Decimal(d)))
					}
					_ => Err(type_error(kind, val)),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Duration => match val {
			GraphqlValue::String(s) => match syn::duration(s) {
				Ok(d) => Ok(SurValue::Duration(d.into())),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Float => match val {
			GraphqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					Ok(SurValue::Number(SurNumber::Float(i as f64)))
				} else if let Some(f) = n.as_f64() {
					Ok(SurValue::Number(SurNumber::Float(f)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SurValue::Number(SurNumber::Float(uint as f64)))
				} else {
					unreachable!("serde_json::Number must be either i64, u64 or f64")
				}
			}
			GraphqlValue::String(s) => {
				let float_expr: Expr = syn::expr(s)?.into();

				match float_expr {
					Expr::Literal(Literal::Float(f)) => Ok(SurValue::Number(SurNumber::Float(f))),
					_ => Err(type_error(kind, val)),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Int => match val {
			GraphqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					Ok(SurValue::Number(SurNumber::Int(i)))
				} else {
					Err(type_error(kind, val))
				}
			}
			GraphqlValue::String(s) => {
				let int_expr: Expr = syn::expr(s)?.into();

				match int_expr {
					Expr::Literal(Literal::Integer(i)) => Ok(SurValue::Number(SurNumber::Int(i))),
					_ => Err(type_error(kind, val)),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Number => match val {
			GraphqlValue::Number(n) => {
				if let Some(i) = n.as_i64() {
					Ok(SurValue::Number(SurNumber::Int(i)))
				} else if let Some(f) = n.as_f64() {
					Ok(SurValue::Number(SurNumber::Float(f)))
				} else if let Some(uint) = n.as_u64() {
					Ok(SurValue::Number(SurNumber::Decimal(uint.into())))
				} else {
					unreachable!("serde_json::Number must be either i64, u64 or f64")
				}
			}
			GraphqlValue::String(s) => {
				let number_expr: Expr = syn::expr(s)?.into();

				match number_expr {
					Expr::Literal(Literal::Integer(i)) => Ok(SurValue::Number(SurNumber::Int(i))),
					Expr::Literal(Literal::Float(f)) => Ok(SurValue::Number(SurNumber::Float(f))),
					Expr::Literal(Literal::Decimal(d)) => {
						Ok(SurValue::Number(SurNumber::Decimal(d)))
					}
					_ => Err(type_error(kind, val)),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Object => match val {
			GraphqlValue::Object(o) => {
				let out: Result<BTreeMap<String, SurValue>, GraphqlError> = o
					.iter()
					.map(|(k, v)| {
						graphql_to_sql_kind(v, Kind::Any).map(|sqlv| (k.to_string(), sqlv))
					})
					.collect();
				Ok(SurValue::Object(out?.into()))
			}
			GraphqlValue::String(s) => {
				let expr = syn::expr_legacy_strand(s.as_str())?;
				let expr: Expr = expr.into();

				match expr {
					Expr::Literal(Literal::Object(o)) => {
						// See `convert_static_record_id_key` for
						// why the accumulator is
						// `BTreeMap<Strand, _>`.
						let mut map = BTreeMap::new();
						for entry in o {
							map.insert(entry.key, convert_static_expr(entry.value)?);
						}
						Ok(SurValue::Object(SurObject::from(map)))
					}
					_ => Err(type_error(kind, val)),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::String => match val {
			GraphqlValue::String(s) => Ok(SurValue::String(s.as_str().into())),
			GraphqlValue::Enum(s) => Ok(SurValue::String(s.as_str().into())),
			_ => Err(type_error(kind, val)),
		},
		Kind::Uuid => match val {
			GraphqlValue::String(s) => match s.parse::<uuid::Uuid>() {
				Ok(u) => Ok(SurValue::Uuid(u.into())),
				Err(_) => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Table(ref ts) => match val {
			GraphqlValue::String(s) => match ts.contains(&s.as_str().into()) {
				true => Ok(SurValue::Table(TableName::new(s.clone()))),
				false => Err(type_error(kind, val)),
			},
			_ => Err(type_error(kind, val)),
		},
		Kind::Record(ref ts) => match val {
			GraphqlValue::String(s) => {
				let record_id_expr = syn::expr(s)?;
				let record_id_expr: Expr = record_id_expr.into();

				match record_id_expr {
					Expr::Literal(Literal::RecordId(t)) => {
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
					}
					_ => Err(type_error(kind, val)),
				}
			}
			_ => Err(type_error(kind, val)),
		},
		Kind::Geometry(ref geo_kinds) => match val {
			GraphqlValue::Object(obj) => {
				let geometry = graphql_geometry_from_object(obj, geo_kinds)?;
				Ok(SurValue::Geometry(geometry))
			}
			_ => Err(type_error(kind, val)),
		},
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
					// Same shape as `either_try_kind!(ks, list, Kind::Array)`,
					// but threading `enum_scope` through — the macro cannot see
					// it (a `macro_rules!` body resolves free identifiers at its
					// definition site), and without it the elements of an
					// `option<array<'A' | 'B'>>` lose their enum mapping.
					for arr_kind in ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
						if let Ok(out) = graphql_to_sql_kind_with_scope(list, arr_kind, enum_scope)
						{
							return Ok(out);
						}
					}
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
		Kind::Array(ref k, n) => match val {
			GraphqlValue::List(l) => {
				// Carry `enum_scope` into the elements: the element kind is what
				// registered the enum, so dropping the scope here leaves the
				// reverse mapping unable to recognise its own tokens.
				let list_iter =
					l.iter().map(|v| graphql_to_sql_kind_with_scope(v, *k.to_owned(), enum_scope));
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
			}
			_ => Err(type_error(kind, val)),
		},
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
				match g {
					SurGeometry::Collection(geometries) => {
						let items: Vec<FieldValue> = geometries
							.iter()
							.map(|g| {
								let type_name = geometry_graphql_type_name(g);
								FieldValue::owned_any(g.clone()).with_type(type_name)
							})
							.collect();
						Ok(Some(FieldValue::list(items)))
					}
					_ => Err(internal_error("Expected GeometryCollection value").into()),
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
	match val {
		GraphqlValue::Number(n) => {
			let f = n
				.as_f64()
				.ok_or_else(|| resolver_error("Invalid coordinate: expected finite number"))?;
			Ok(SurValue::Number(SurNumber::Float(f)))
		}
		GraphqlValue::List(items) => {
			let vals: Result<Vec<SurValue>, GraphqlError> =
				items.iter().map(graphql_coords_to_sur_value).collect();
			Ok(vals?.into())
		}
		_ => Err(resolver_error("Expected number or array in geometry coordinates")),
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
		match ty {
			GraphqlValue::Enum(s) => s.as_str(),
			GraphqlValue::String(s) => s.as_str(),
			_ => return Err(resolver_error("Geometry 'type' must be a GeometryType enum value")),
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
			let list = match graphql_geometries {
				GraphqlValue::List(l) => l,
				_ => {
					return Err(resolver_error("GeometryCollection 'geometries' must be an array"));
				}
			};
			let mut geometries = Vec::with_capacity(list.len());
			for item in list {
				match item {
					GraphqlValue::Object(sub_obj) => {
						// Recursively parse each sub-geometry (allow any type)
						geometries.push(graphql_geometry_from_object(sub_obj, &[])?);
					}
					_ => {
						return Err(resolver_error(
							"Each item in 'geometries' must be a geometry object",
						));
					}
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

	match g {
		SurGeometry::Collection(geometries) => {
			let graphql_geometries: Result<Vec<GraphqlValue>, GraphqlError> =
				geometries.iter().map(geometry_to_graphql_object).collect();
			map.insert(Name::new("geometries"), GraphqlValue::List(graphql_geometries?));
		}
		_ => {
			let coords = g.as_coordinates();
			let graphql_coords = sql_value_to_graphql_value(coords)?;
			map.insert(Name::new("coordinates"), graphql_coords);
		}
	}

	Ok(GraphqlValue::Object(map))
}

#[cfg(test)]
mod tests {
	use async_graphql::Number;

	use super::*;

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

	/// `'ACTIVE' | 'BANNED'`, wrapped in `option<…>` when `optional`.
	fn status_union(optional: bool) -> Kind {
		let mut variants = Vec::new();
		if optional {
			variants.push(Kind::None);
		}
		variants.push(Kind::Literal(KindLiteral::String("ACTIVE".into())));
		variants.push(Kind::Literal(KindLiteral::String("BANNED".into())));
		Kind::Either(variants)
	}

	/// Name of the one enum type a `kind_to_type*` call registered, so the
	/// assertions below tie the returned `TypeRef` to the actual registration
	/// instead of restating the generated name.
	fn only_registered_enum(types: &[Type]) -> String {
		let names: Vec<&str> = types
			.iter()
			.filter_map(|t| match t {
				Type::Enum(e) => Some(e.type_name()),
				_ => None,
			})
			.collect();
		assert_eq!(names.len(), 1, "expected exactly one registered enum, got {names:?}");
		names[0].to_owned()
	}

	#[test]
	fn literal_union_derives_non_null_like_every_other_kind() {
		// The `NonNull` wrapper is applied in a single place, at the end of
		// `kind_to_type_inner`. The literal-union arm used to return the enum's
		// `TypeRef` early and skip it, so `TYPE 'A' | 'B'` came out nullable
		// while `TYPE string` correctly came out `String!` (issue #7452).
		let mut types = Vec::new();
		let ty = kind_to_type_with_enum_prefix(
			status_union(false),
			&mut types,
			false,
			Some("person_status"),
		)
		.unwrap();

		let rendered = ty.to_string();
		let TypeRef::NonNull(inner) = ty else {
			panic!("a non-optional literal union should be NonNull, got `{rendered}`");
		};
		assert_eq!(inner.to_string(), only_registered_enum(&types));
	}

	#[test]
	fn optional_literal_union_stays_nullable() {
		// The other half of the same contract: `optional` is derived from the
		// kind, which keeps its `None` variant, so `option<'A' | 'B'>` must not
		// pick up a `NonNull` wrapper. Guards against "fixing" the case above by
		// wrapping unconditionally.
		let mut types = Vec::new();
		let ty = kind_to_type_with_enum_prefix(
			status_union(true),
			&mut types,
			false,
			Some("person_mood"),
		)
		.unwrap();

		assert_eq!(ty.to_string(), only_registered_enum(&types));
	}

	/// `array<'ACTIVE' | 'BANNED'>`, wrapped in `option<…>` when `optional`.
	fn status_union_array(optional: bool) -> Kind {
		let array = Kind::Array(Box::new(status_union(false)), None);
		if optional {
			Kind::Either(vec![Kind::None, array])
		} else {
			array
		}
	}

	#[test]
	fn enum_values_survive_an_array_layer_in_both_directions() {
		// `kind_to_type` registers one enum for the *element* kind and passes
		// the scope through the array unchanged, so both converters have to do
		// the same. They used to stop at the array: reading produced raw
		// strings that async-graphql rejected as invalid enum items, and
		// writing produced tokens the reverse mapping no longer recognised.
		for optional in [false, true] {
			let kind = status_union_array(optional);
			let stored = SurValue::Array(SurArray(vec![SurValue::from("BANNED")]));

			let out = sql_value_to_graphql_value_with_kind(
				stored.clone(),
				Some(&kind),
				Some("person_status"),
			)
			.unwrap();
			assert_eq!(
				out,
				GraphqlValue::List(vec![GraphqlValue::Enum(Name::new("PERSON_STATUS_BANNED"))]),
				"reading array<'A' | 'B'> (optional: {optional})"
			);

			// And the token round-trips back to the stored string.
			assert_eq!(
				graphql_to_sql_kind_with_scope(&out, kind, Some("person_status")).unwrap(),
				stored,
				"writing array<'A' | 'B'> (optional: {optional})"
			);
		}
	}

	#[test]
	fn scalar_literal_union_conversion_is_unchanged() {
		// The array handling is layered on top of the scalar path, not in place
		// of it: a bare `'A' | 'B'` must still map string to token and back.
		let kind = status_union(false);

		let out =
			sql_value_to_graphql_value_with_kind(SurValue::from("ACTIVE"), Some(&kind), Some("s"))
				.unwrap();
		assert_eq!(out, GraphqlValue::Enum(Name::new("S_ACTIVE")));
		assert_eq!(
			graphql_to_sql_kind_with_scope(&out, kind, Some("s")).unwrap(),
			SurValue::from("ACTIVE")
		);
	}
}
